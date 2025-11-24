package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/niladrix719/pipecd-kafka-plugin/config"
	"github.com/niladrix719/pipecd-kafka-plugin/provider"
)

func topic(name string, partitions int, cfg map[string]string) provider.Topic {
	return provider.Topic{Name: name, Partitions: partitions, ReplicationFactor: 3, Config: cfg}
}

func state(name string, partitions int, cfg map[string]string) provider.TopicState {
	if cfg == nil {
		cfg = map[string]string{}
	}
	return provider.TopicState{Name: name, Partitions: partitions, ReplicationFactor: 3, Config: cfg}
}

// input builds a plan input owning everything, with no rails enabled.
func input(desired []provider.Topic, actual ...provider.TopicState) Input {
	byName := make(map[string]provider.TopicState, len(actual))
	for _, s := range actual {
		byName[s.Name] = s
	}
	return Input{
		Desired: desired,
		Actual:  byName,
		Spec:    config.ApplicationConfigSpec{Ownership: []string{"*"}},
		Target:  config.DeployTargetConfig{},
	}
}

func build(t *testing.T, in Input) *Plan {
	t.Helper()
	got, err := Build(context.Background(), in)
	require.NoError(t, err)
	return got
}

func kinds(changes []Change) []ChangeKind {
	out := make([]ChangeKind, 0, len(changes))
	for _, c := range changes {
		out = append(out, c.Kind)
	}
	return out
}

func TestBuildCreatesMissingTopics(t *testing.T) {
	t.Parallel()

	got := build(t, input([]provider.Topic{topic("orders", 12, map[string]string{"retention.ms": "1000"})}))

	require.Len(t, got.Changes, 1)
	assert.Equal(t, CreateTopic, got.Changes[0].Kind)
	assert.Equal(t, "orders", got.Changes[0].Topic)
	assert.Equal(t, 12, got.Changes[0].Desired.Partitions)
	assert.Equal(t, "1000", got.Changes[0].Desired.Config["retention.ms"])
	assert.Empty(t, got.Blocked)
}

func TestBuildNoChangesWhenAlreadyMatching(t *testing.T) {
	t.Parallel()

	cfg := map[string]string{"retention.ms": "1000"}
	got := build(t, input([]provider.Topic{topic("orders", 12, cfg)}, state("orders", 12, cfg)))

	assert.True(t, got.Empty())
	assert.Equal(t, "No changes. The cluster already matches the desired state.", got.Render())
}

func TestBuildConfigDiff(t *testing.T) {
	t.Parallel()

	desired := map[string]string{"retention.ms": "2000", "cleanup.policy": "compact"}
	actual := map[string]string{"retention.ms": "1000", "segment.ms": "500"}

	got := build(t, input([]provider.Topic{topic("orders", 12, desired)}, state("orders", 12, actual)))

	require.Len(t, got.Changes, 1)
	change := got.Changes[0]
	assert.Equal(t, UpdateTopicConfig, change.Kind)
	// Changed and newly-declared keys are set.
	assert.Equal(t, map[string]string{"retention.ms": "2000", "cleanup.policy": "compact"}, change.ConfigChange.Set)
	// A key that is no longer declared goes back to the broker default.
	assert.Equal(t, []string{"segment.ms"}, change.ConfigChange.Delete)
	// The previous values are kept so a rollback can restore them.
	assert.Equal(t, map[string]string{"retention.ms": "1000", "segment.ms": "500"}, change.ConfigBefore)
	assert.Contains(t, change.Describe(), "retention.ms: 1000 -> 2000")
	assert.Contains(t, change.Describe(), "cleanup.policy: unset -> compact")
	assert.Contains(t, change.Describe(), "segment.ms: 500 -> broker default")
}

func TestBuildPartitionIncrease(t *testing.T) {
	t.Parallel()

	t.Run("blocked unless the target allows it", func(t *testing.T) {
		t.Parallel()

		got := build(t, input([]provider.Topic{topic("orders", 24, nil)}, state("orders", 12, nil)))

		assert.Empty(t, got.Changes)
		require.Len(t, got.Blocked, 1)
		assert.Contains(t, got.Blocked[0].Reason, "allowPartitionIncrease is false")
		assert.Contains(t, got.Blocked[0].Reason, "cannot be undone")
	})

	t.Run("allowed when the target opts in", func(t *testing.T) {
		t.Parallel()

		in := input([]provider.Topic{topic("orders", 24, nil)}, state("orders", 12, nil))
		in.Target.AllowPartitionIncrease = true

		got := build(t, in)

		require.Len(t, got.Changes, 1)
		assert.Equal(t, IncreasePartitions, got.Changes[0].Kind)
		assert.Equal(t, 12, got.Changes[0].FromPartitions)
		assert.Equal(t, 24, got.Changes[0].ToPartitions)
		// Permitted, but still not undoable.
		assert.Len(t, got.Irreversible(), 1)
	})
}

func TestBuildPartitionDecreaseIsAlwaysBlocked(t *testing.T) {
	t.Parallel()

	in := input([]provider.Topic{topic("orders", 6, nil)}, state("orders", 12, nil))
	// Even with every rail opened, Kafka cannot do this.
	in.Target.AllowPartitionIncrease = true
	in.Target.AllowTopicDeletion = true

	got := build(t, in)

	assert.Empty(t, got.Changes)
	require.Len(t, got.Blocked, 1)
	assert.Contains(t, got.Blocked[0].Reason, "cannot reduce a partition count")
}

func TestBuildReplicationFactorChangeIsBlocked(t *testing.T) {
	t.Parallel()

	desired := provider.Topic{Name: "orders", Partitions: 12, ReplicationFactor: 5}
	got := build(t, input([]provider.Topic{desired}, state("orders", 12, nil)))

	require.Len(t, got.Blocked, 1)
	assert.Contains(t, got.Blocked[0].Reason, "partition reassignment")
}

func TestBuildDeletion(t *testing.T) {
	t.Parallel()

	t.Run("blocked unless the target allows it", func(t *testing.T) {
		t.Parallel()

		got := build(t, input(nil, state("legacy", 3, nil)))

		assert.Empty(t, got.Changes)
		require.Len(t, got.Blocked, 1)
		assert.Contains(t, got.Blocked[0].Reason, "allowTopicDeletion is false")
	})

	t.Run("allowed when the target opts in", func(t *testing.T) {
		t.Parallel()

		in := input(nil, state("legacy", 3, nil))
		in.Target.AllowTopicDeletion = true

		got := build(t, in)

		require.Len(t, got.Changes, 1)
		assert.Equal(t, DeleteTopic, got.Changes[0].Kind)
		assert.Equal(t, "legacy", got.Changes[0].Topic)
	})

	t.Run("topics outside ownership are left alone", func(t *testing.T) {
		t.Parallel()

		in := input([]provider.Topic{topic("orders.created", 1, nil)}, state("someone-elses-topic", 3, nil))
		in.Spec.Ownership = []string{"orders.*"}
		in.Target.AllowTopicDeletion = true

		got := build(t, in)

		// Only the create; the unowned topic is invisible to this application.
		assert.Equal(t, []ChangeKind{CreateTopic}, kinds(got.Changes))
		assert.Empty(t, got.Blocked)
	})
}

func TestBuildProtectedTopics(t *testing.T) {
	t.Parallel()

	t.Run("a declared topic that is protected blocks the plan", func(t *testing.T) {
		t.Parallel()

		in := input([]provider.Topic{topic("__consumer_offsets", 50, nil)})
		in.Target.ProtectedTopics = []string{"__*"}

		got := build(t, in)

		assert.Empty(t, got.Changes)
		require.Len(t, got.Blocked, 1)
		assert.Contains(t, got.Blocked[0].Reason, "protected pattern")
	})

	t.Run("an undeclared protected topic is never deleted", func(t *testing.T) {
		t.Parallel()

		in := input(nil, state("orders.dlq", 3, nil))
		in.Target.AllowTopicDeletion = true
		in.Target.ProtectedTopics = []string{"*.dlq"}

		got := build(t, in)

		assert.True(t, got.Empty(), "a protected topic should be neither deleted nor blocked")
	})
}

func TestBuildOrdersDeletionsLast(t *testing.T) {
	t.Parallel()

	in := input([]provider.Topic{topic("new", 1, nil)}, state("old", 1, nil))
	in.Target.AllowTopicDeletion = true

	got := build(t, in)

	assert.Equal(t, []ChangeKind{CreateTopic, DeleteTopic}, kinds(got.Changes))
}

// withSchema returns a topic declaring a schema.
func withSchema(name, subject, body string) provider.Topic {
	t := topic(name, 1, nil)
	t.Schema = &provider.Schema{Subject: subject, File: "s.avsc", Body: body}
	return t
}

func TestBuildSchemaChanges(t *testing.T) {
	t.Parallel()

	t.Run("a new subject is registered", func(t *testing.T) {
		t.Parallel()

		in := input([]provider.Topic{withSchema("orders", "orders-value", `{"v":1}`)}, state("orders", 1, nil))
		in.Registry = provider.NewFakeRegistry()

		got := build(t, in)

		require.Len(t, got.Changes, 1)
		assert.Equal(t, RegisterSchema, got.Changes[0].Kind)
		assert.Equal(t, 0, got.Changes[0].CurrentVersion)
		assert.Contains(t, got.Changes[0].Describe(), "first version")
	})

	t.Run("an unchanged schema is not re-registered", func(t *testing.T) {
		t.Parallel()

		registry := provider.NewFakeRegistry()
		registry.Seed("orders-value", `{"v":1}`)

		in := input([]provider.Topic{withSchema("orders", "orders-value", "  {\"v\":1}\n")}, state("orders", 1, nil))
		in.Registry = registry

		got := build(t, in)

		assert.True(t, got.Empty(), "whitespace-only differences should not produce a new version")
	})

	t.Run("a changed schema registers a new version", func(t *testing.T) {
		t.Parallel()

		registry := provider.NewFakeRegistry()
		registry.Seed("orders-value", `{"v":1}`)

		in := input([]provider.Topic{withSchema("orders", "orders-value", `{"v":2}`)}, state("orders", 1, nil))
		in.Registry = registry

		got := build(t, in)

		require.Len(t, got.Changes, 1)
		assert.Equal(t, 1, got.Changes[0].CurrentVersion)
		assert.Contains(t, got.Changes[0].Describe(), "replacing version 1")
	})

	t.Run("an incompatible schema blocks the plan", func(t *testing.T) {
		t.Parallel()

		registry := provider.NewFakeRegistry()
		registry.Seed("orders-value", `{"v":1}`)
		registry.Incompatible["orders-value"] = []string{"reader field total is missing a default value"}

		in := input([]provider.Topic{withSchema("orders", "orders-value", `{"v":2}`)}, state("orders", 1, nil))
		in.Registry = registry

		got := build(t, in)

		assert.Empty(t, got.Changes)
		require.Len(t, got.Blocked, 1)
		assert.Contains(t, got.Blocked[0].Reason, "not compatible with version 1")
		// The registry's own explanation is passed through.
		assert.Contains(t, got.Blocked[0].Reason, "missing a default value")
	})

	t.Run("declaring a schema with no registry is an error", func(t *testing.T) {
		t.Parallel()

		in := input([]provider.Topic{withSchema("orders", "orders-value", `{"v":1}`)}, state("orders", 1, nil))
		in.Registry = nil

		_, err := Build(context.Background(), in)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no schemaRegistry configured")
	})
}

func TestPlanPartitioning(t *testing.T) {
	t.Parallel()

	registry := provider.NewFakeRegistry()
	in := input([]provider.Topic{withSchema("orders", "orders-value", `{"v":1}`)})
	in.Registry = registry

	got := build(t, in)

	// The apply stage takes the topic changes, the registration stage the rest.
	assert.Equal(t, []ChangeKind{CreateTopic}, kinds(got.TopicChanges()))
	assert.Equal(t, []ChangeKind{RegisterSchema}, kinds(got.SchemaChanges()))
	assert.Len(t, got.Changes, 2)
}
