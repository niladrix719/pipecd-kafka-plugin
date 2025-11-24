package deployment

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	sdk "github.com/pipe-cd/piped-plugin-sdk-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/niladrix719/pipecd-kafka-plugin/config"
	"github.com/niladrix719/pipecd-kafka-plugin/provider"
)

// fakeLogPersister records what a stage wrote to the deployment log.
type fakeLogPersister struct {
	mu    sync.Mutex
	lines []string
}

func (f *fakeLogPersister) add(line string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lines = append(f.lines, line)
}

func (f *fakeLogPersister) Write(log []byte) (int, error) { f.add(string(log)); return len(log), nil }
func (f *fakeLogPersister) Info(log string)               { f.add(log) }
func (f *fakeLogPersister) Infof(format string, a ...interface{}) {
	f.add(fmt.Sprintf(format, a...))
}
func (f *fakeLogPersister) Success(log string) { f.add(log) }
func (f *fakeLogPersister) Successf(format string, a ...interface{}) {
	f.add(fmt.Sprintf(format, a...))
}
func (f *fakeLogPersister) Error(log string) { f.add(log) }
func (f *fakeLogPersister) Errorf(format string, a ...interface{}) {
	f.add(fmt.Sprintf(format, a...))
}

func (f *fakeLogPersister) text() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return strings.Join(f.lines, "\n")
}

// fakeMetadataStore captures stage metadata instead of sending it to piped.
type fakeMetadataStore struct{ stored map[string]string }

func (f *fakeMetadataStore) PutStageMetadataMulti(_ context.Context, metadata map[string]string) error {
	if f.stored == nil {
		f.stored = map[string]string{}
	}
	for k, v := range metadata {
		f.stored[k] = v
	}
	return nil
}

// harness wires a plugin to an in-memory cluster and registry.
type harness struct {
	plugin   *Plugin
	cluster  *provider.FakeCluster
	registry *provider.FakeRegistry
	lp       *fakeLogPersister
	metadata *fakeMetadataStore
	target   *sdk.DeployTarget[config.DeployTargetConfig]
}

func newHarness(t *testing.T, targetCfg config.DeployTargetConfig, existing ...provider.TopicState) *harness {
	t.Helper()

	cluster := provider.NewFakeCluster(existing...)
	registry := provider.NewFakeRegistry()
	if targetCfg.BootstrapServers == nil {
		targetCfg.BootstrapServers = []string{"localhost:9092"}
	}
	if targetCfg.SchemaRegistry == nil {
		targetCfg.SchemaRegistry = &config.SchemaRegistryConfig{URL: "http://localhost:8081"}
	}

	return &harness{
		plugin: &Plugin{
			newCluster:  func(config.DeployTargetConfig) (provider.Cluster, error) { return cluster, nil },
			newRegistry: func(config.SchemaRegistryConfig) (provider.Registry, error) { return registry, nil },
		},
		cluster:  cluster,
		registry: registry,
		lp:       &fakeLogPersister{},
		metadata: &fakeMetadataStore{},
		target:   &sdk.DeployTarget[config.DeployTargetConfig]{Name: "test", Config: targetCfg},
	}
}

// source builds a deployment source from an application directory on disk.
func source(t *testing.T, files map[string]string, spec config.ApplicationConfigSpec) sdk.DeploymentSource[config.ApplicationConfigSpec] {
	t.Helper()

	dir := t.TempDir()
	for rel, content := range files {
		p := filepath.Join(dir, rel)
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, []byte(content), 0o644))
	}
	return sdk.DeploymentSource[config.ApplicationConfigSpec]{
		ApplicationDirectory: dir,
		ApplicationConfig:    &sdk.ApplicationConfig[config.ApplicationConfigSpec]{Spec: &spec},
	}
}

func (h *harness) input(target sdk.DeploymentSource[config.ApplicationConfigSpec], stageConfig string) stageInput {
	return stageInput{
		lp:           h.lp,
		metadata:     h.metadata,
		stageConfig:  []byte(stageConfig),
		target:       h.target,
		targetSource: target,
	}
}

func ownAll() config.ApplicationConfigSpec {
	return config.ApplicationConfigSpec{Ownership: []string{"*"}}
}

const ordersTopic = `
name: orders
partitions: 12
replicationFactor: 3
config:
  retention.ms: "604800000"
`

func TestExecutePlan(t *testing.T) {
	t.Parallel()

	t.Run("reports the changes without applying them", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t, config.DeployTargetConfig{})
		src := source(t, map[string]string{"topics/orders.yaml": ordersTopic}, ownAll())

		got := h.plugin.executePlan(context.Background(), h.input(src, "{}"))

		assert.Equal(t, sdk.StageStatusSuccess, got.Status)
		assert.Contains(t, h.lp.text(), "create topic orders (12 partitions, replication factor 3)")
		assert.Empty(t, h.cluster.Calls, "the plan stage must not change the cluster")
		assert.Equal(t, "1", h.metadata.stored[metadataKeyChanges])
		assert.Equal(t, "0", h.metadata.stored[metadataKeyBlocked])
	})

	t.Run("fails when the plan is blocked", func(t *testing.T) {
		t.Parallel()

		// The topic exists with fewer partitions, and the target forbids growth.
		h := newHarness(t, config.DeployTargetConfig{}, provider.TopicState{Name: "orders", Partitions: 6, ReplicationFactor: 3})
		src := source(t, map[string]string{"topics/orders.yaml": ordersTopic}, ownAll())

		got := h.plugin.executePlan(context.Background(), h.input(src, "{}"))

		assert.Equal(t, sdk.StageStatusFailure, got.Status)
		assert.Contains(t, h.lp.text(), "allowPartitionIncrease is false")
		assert.Contains(t, h.lp.text(), "Nothing has been applied")
		assert.Empty(t, h.cluster.Calls)
	})

	t.Run("exits early on no changes when asked", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t, config.DeployTargetConfig{}, provider.TopicState{
			Name: "orders", Partitions: 12, ReplicationFactor: 3,
			Config: map[string]string{"retention.ms": "604800000"},
		})
		src := source(t, map[string]string{"topics/orders.yaml": ordersTopic}, ownAll())

		got := h.plugin.executePlan(context.Background(), h.input(src, `{"exitOnNoChanges":true}`))

		assert.Equal(t, sdk.StageStatusExited, got.Status)
	})

	t.Run("warns about permitted irreversible changes", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t, config.DeployTargetConfig{AllowPartitionIncrease: true},
			provider.TopicState{Name: "orders", Partitions: 6, ReplicationFactor: 3, Config: map[string]string{"retention.ms": "604800000"}})
		src := source(t, map[string]string{"topics/orders.yaml": ordersTopic}, ownAll())

		got := h.plugin.executePlan(context.Background(), h.input(src, "{}"))

		assert.Equal(t, sdk.StageStatusSuccess, got.Status)
		assert.Contains(t, h.lp.text(), "cannot be undone by a rollback")
		assert.Equal(t, "1", h.metadata.stored[metadataKeyIrreversible])
	})
}

func TestExecuteApply(t *testing.T) {
	t.Parallel()

	t.Run("creates the declared topic", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t, config.DeployTargetConfig{})
		src := source(t, map[string]string{"topics/orders.yaml": ordersTopic}, ownAll())

		got := h.plugin.executeApply(context.Background(), h.input(src, "{}"))

		assert.Equal(t, sdk.StageStatusSuccess, got.Status)
		topic, ok := h.cluster.Topic("orders")
		require.True(t, ok)
		assert.Equal(t, 12, topic.Partitions)
		assert.Equal(t, "604800000", topic.Config["retention.ms"])
		assert.Equal(t, "1", h.metadata.stored[metadataKeyApplied])
	})

	t.Run("refuses to apply a blocked plan", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t, config.DeployTargetConfig{}, provider.TopicState{Name: "orders", Partitions: 6, ReplicationFactor: 3})
		src := source(t, map[string]string{"topics/orders.yaml": ordersTopic}, ownAll())

		got := h.plugin.executeApply(context.Background(), h.input(src, "{}"))

		assert.Equal(t, sdk.StageStatusFailure, got.Status)
		assert.Empty(t, h.cluster.Calls)
		assert.Contains(t, h.lp.text(), "Nothing will be applied")
	})

	t.Run("reports how far a partial apply got", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t, config.DeployTargetConfig{})
		h.cluster.FailOn["create:b"] = fmt.Errorf("broker is down")
		src := source(t, map[string]string{
			"topics/a.yaml": "name: a\npartitions: 1\nreplicationFactor: 1\n",
			"topics/b.yaml": "name: b\npartitions: 1\nreplicationFactor: 1\n",
			"topics/c.yaml": "name: c\npartitions: 1\nreplicationFactor: 1\n",
		}, ownAll())

		got := h.plugin.executeApply(context.Background(), h.input(src, "{}"))

		assert.Equal(t, sdk.StageStatusFailure, got.Status)
		// An apply is not atomic, and the log has to say so.
		assert.Contains(t, h.lp.text(), "1 of 3 change(s) were applied before failing")
		_, created := h.cluster.Topic("a")
		assert.True(t, created)
		_, reached := h.cluster.Topic("c")
		assert.False(t, reached, "the apply should have stopped at the failure")
	})

	t.Run("continueOnError keeps going past a failure", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t, config.DeployTargetConfig{})
		h.cluster.FailOn["create:b"] = fmt.Errorf("broker is down")
		src := source(t, map[string]string{
			"topics/a.yaml": "name: a\npartitions: 1\nreplicationFactor: 1\n",
			"topics/b.yaml": "name: b\npartitions: 1\nreplicationFactor: 1\n",
			"topics/c.yaml": "name: c\npartitions: 1\nreplicationFactor: 1\n",
		}, ownAll())

		got := h.plugin.executeApply(context.Background(), h.input(src, `{"continueOnError":true}`))

		assert.Equal(t, sdk.StageStatusFailure, got.Status)
		_, reached := h.cluster.Topic("c")
		assert.True(t, reached, "c should have been created despite b failing")
	})
}

func TestExecuteRegisterSchema(t *testing.T) {
	t.Parallel()

	const topicWithSchema = `
name: orders
partitions: 1
replicationFactor: 1
schema:
  subject: orders-value
  file: orders.avsc
`

	t.Run("registers a new version", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t, config.DeployTargetConfig{})
		src := source(t, map[string]string{
			"topics/orders.yaml":  topicWithSchema,
			"schemas/orders.avsc": `{"type":"record","name":"Order","fields":[]}`,
		}, config.ApplicationConfigSpec{Ownership: []string{"*"}, SchemasDir: "schemas"})

		got := h.plugin.executeRegisterSchema(context.Background(), h.input(src, "{}"))

		assert.Equal(t, sdk.StageStatusSuccess, got.Status)
		assert.Equal(t, []string{"orders-value"}, h.registry.Registered)
		assert.Contains(t, h.lp.text(), "registered subject orders-value as version 1")
		// Registration must not touch topics; that is the apply stage's job.
		assert.Empty(t, h.cluster.Calls)
	})

	t.Run("refuses an incompatible schema", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t, config.DeployTargetConfig{})
		h.registry.Seed("orders-value", `{"old":true}`)
		h.registry.Incompatible["orders-value"] = []string{"field total was removed"}

		src := source(t, map[string]string{
			"topics/orders.yaml":  topicWithSchema,
			"schemas/orders.avsc": `{"new":true}`,
		}, config.ApplicationConfigSpec{Ownership: []string{"*"}, SchemasDir: "schemas"})

		got := h.plugin.executeRegisterSchema(context.Background(), h.input(src, "{}"))

		assert.Equal(t, sdk.StageStatusFailure, got.Status)
		assert.Empty(t, h.registry.Registered)
		assert.Contains(t, h.lp.text(), "field total was removed")
	})
}

func TestExecuteRollback(t *testing.T) {
	t.Parallel()

	t.Run("restores the previous config", func(t *testing.T) {
		t.Parallel()

		// The cluster is on the new value; the previous revision wanted the old one.
		h := newHarness(t, config.DeployTargetConfig{}, provider.TopicState{
			Name: "orders", Partitions: 12, ReplicationFactor: 3,
			Config: map[string]string{"retention.ms": "999"},
		})
		running := source(t, map[string]string{"topics/orders.yaml": ordersTopic}, ownAll())

		in := h.input(sdk.DeploymentSource[config.ApplicationConfigSpec]{}, "{}")
		in.runningSource = running

		got := h.plugin.executeRollback(context.Background(), in)

		assert.Equal(t, sdk.StageStatusSuccess, got.Status)
		topic, _ := h.cluster.Topic("orders")
		assert.Equal(t, "604800000", topic.Config["retention.ms"])
		assert.Contains(t, h.lp.text(), "matches the previously running revision")
	})

	t.Run("says plainly what it cannot undo", func(t *testing.T) {
		t.Parallel()

		// The apply grew the topic; the previous revision had fewer partitions,
		// and Kafka cannot shrink it back.
		h := newHarness(t, config.DeployTargetConfig{AllowPartitionIncrease: true}, provider.TopicState{
			Name: "orders", Partitions: 24, ReplicationFactor: 3,
			Config: map[string]string{"retention.ms": "604800000"},
		})
		running := source(t, map[string]string{"topics/orders.yaml": ordersTopic}, ownAll())

		in := h.input(sdk.DeploymentSource[config.ApplicationConfigSpec]{}, "{}")
		in.runningSource = running

		got := h.plugin.executeRollback(context.Background(), in)

		assert.Equal(t, sdk.StageStatusFailure, got.Status)
		assert.Contains(t, h.lp.text(), "cannot reduce a partition count")
		topic, _ := h.cluster.Topic("orders")
		assert.Equal(t, 24, topic.Partitions, "the partition count must be left alone")
	})

	t.Run("no previous revision is not a failure", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t, config.DeployTargetConfig{})
		got := h.plugin.executeRollback(context.Background(), h.input(sdk.DeploymentSource[config.ApplicationConfigSpec]{}, "{}"))

		assert.Equal(t, sdk.StageStatusSuccess, got.Status)
		assert.Contains(t, h.lp.text(), "no previously running revision")
	})
}

func TestBuildPipelineSyncStagesAppendsRollback(t *testing.T) {
	t.Parallel()

	got, err := (&Plugin{}).BuildPipelineSyncStages(context.Background(), &config.Config{}, &sdk.BuildPipelineSyncStagesInput{
		Request: sdk.BuildPipelineSyncStagesRequest{
			Stages: []sdk.StageConfig{{Index: 0, Name: StagePlan}, {Index: 1, Name: StageApply}},
		},
	})
	require.NoError(t, err)

	require.Len(t, got.Stages, 3)
	assert.Equal(t, StageRollback, got.Stages[2].Name)
	assert.True(t, got.Stages[2].Rollback)
	// The rollback stage reuses an index from the request.
	assert.Equal(t, 0, got.Stages[2].Index)
}

func TestFetchDefinedStages(t *testing.T) {
	t.Parallel()

	assert.ElementsMatch(t,
		[]string{StagePlan, StageRegisterSchema, StageApply, StageRollback},
		(&Plugin{}).FetchDefinedStages())
}

func TestSingleTarget(t *testing.T) {
	t.Parallel()

	one := &sdk.DeployTarget[config.DeployTargetConfig]{Name: "prod"}

	got, err := singleTarget([]*sdk.DeployTarget[config.DeployTargetConfig]{one})
	require.NoError(t, err)
	assert.Equal(t, "prod", got.Name)

	_, err = singleTarget(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exactly one Kafka cluster")

	// An application's topics live on one cluster; applying one plan to several
	// would need a plan and a rollback per cluster.
	_, err = singleTarget([]*sdk.DeployTarget[config.DeployTargetConfig]{one, one})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "2 deploy targets")
}
