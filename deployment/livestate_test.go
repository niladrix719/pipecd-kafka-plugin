package deployment

import (
	"context"
	"testing"

	sdk "github.com/pipe-cd/piped-plugin-sdk-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/niladrix719/pipecd-kafka-plugin/config"
	"github.com/niladrix719/pipecd-kafka-plugin/provider"
)

// livestateInput wraps a deployment source the way piped's periodic drift check
// does. Only the source is read, so the client and logger are left unset.
func livestateInput(src sdk.DeploymentSource[config.ApplicationConfigSpec]) *sdk.GetLivestateInput[config.ApplicationConfigSpec] {
	return &sdk.GetLivestateInput[config.ApplicationConfigSpec]{
		Request: sdk.GetLivestateRequest[config.ApplicationConfigSpec]{
			ApplicationID:    "app-1",
			ApplicationName:  "orders",
			DeploymentSource: src,
		},
	}
}

func (h *harness) livestate(t *testing.T, src sdk.DeploymentSource[config.ApplicationConfigSpec]) *sdk.GetLivestateResponse {
	t.Helper()

	got, err := h.plugin.GetLivestate(context.Background(), nil,
		[]*sdk.DeployTarget[config.DeployTargetConfig]{h.target}, livestateInput(src))
	require.NoError(t, err)
	return got
}

// resourceByName finds one resource in a live state.
func resourceByName(t *testing.T, live sdk.ApplicationLiveState, name string) sdk.ResourceState {
	t.Helper()

	for _, r := range live.Resources {
		if r.Name == name {
			return r
		}
	}
	t.Fatalf("no resource named %q in %+v", name, live.Resources)
	return sdk.ResourceState{}
}

func TestGetLivestate(t *testing.T) {
	t.Parallel()

	t.Run("reports synced when the cluster matches", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t, config.DeployTargetConfig{}, provider.TopicState{
			Name: "orders", Partitions: 12, ReplicationFactor: 3,
			Config: map[string]string{"retention.ms": "604800000"},
		})
		src := source(t, map[string]string{"topics/orders.yaml": ordersTopic}, ownAll())

		got := h.livestate(t, src)

		assert.Equal(t, sdk.ApplicationSyncStateSynced, got.SyncState.Status)
		assert.Empty(t, h.cluster.Calls, "drift detection must not change the cluster")

		orders := resourceByName(t, got.LiveState, "orders")
		assert.Equal(t, sdk.ResourceHealthStateHealthy, orders.HealthStatus)
		assert.Equal(t, resourceTypeTopic, orders.ResourceType)
		assert.Equal(t, "test", orders.DeployTarget)
		assert.Equal(t, "12", orders.ResourceMetadata["partitions"])
		assert.Equal(t, "3", orders.ResourceMetadata["replicationFactor"])
		assert.Equal(t, "604800000", orders.ResourceMetadata["config.retention.ms"])
	})

	t.Run("reports drift when a declared topic is missing", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t, config.DeployTargetConfig{})
		src := source(t, map[string]string{"topics/orders.yaml": ordersTopic}, ownAll())

		got := h.livestate(t, src)

		assert.Equal(t, sdk.ApplicationSyncStateOutOfSync, got.SyncState.Status)
		assert.Contains(t, got.SyncState.ShortReason, "1 change(s) would be needed")
		assert.Contains(t, got.SyncState.Reason, "create topic orders (12 partitions, replication factor 3)")

		orders := resourceByName(t, got.LiveState, "orders")
		assert.Equal(t, sdk.ResourceHealthStateUnhealthy, orders.HealthStatus)
		assert.Contains(t, orders.HealthDescription, "not present on the cluster")
	})

	t.Run("reports drift the deploy target would refuse to close", func(t *testing.T) {
		t.Parallel()

		// The topic shrank out of the repository, but deletion is not permitted,
		// so no deployment would remove it. That is still drift.
		h := newHarness(t, config.DeployTargetConfig{}, provider.TopicState{Name: "legacy", Partitions: 1, ReplicationFactor: 1})
		src := source(t, map[string]string{"topics/orders.yaml": ordersTopic}, ownAll())

		got := h.livestate(t, src)

		assert.Equal(t, sdk.ApplicationSyncStateOutOfSync, got.SyncState.Status)
		assert.Contains(t, got.SyncState.ShortReason, "1 more are blocked by this deploy target")
		assert.Contains(t, got.SyncState.Reason, "allowTopicDeletion is false")

		legacy := resourceByName(t, got.LiveState, "legacy")
		assert.Equal(t, sdk.ResourceHealthStateHealthy, legacy.HealthStatus, "the topic itself is fine; only the repository disagrees")
		assert.Contains(t, legacy.HealthDescription, "no longer declared")
	})

	t.Run("leaves topics owned by other applications out", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t, config.DeployTargetConfig{},
			provider.TopicState{Name: "orders", Partitions: 12, ReplicationFactor: 3, Config: map[string]string{"retention.ms": "604800000"}},
			provider.TopicState{Name: "payments.audit", Partitions: 1, ReplicationFactor: 1},
		)
		src := source(t, map[string]string{"topics/orders.yaml": ordersTopic},
			config.ApplicationConfigSpec{Ownership: []string{"orders*"}})

		got := h.livestate(t, src)

		assert.Equal(t, sdk.ApplicationSyncStateSynced, got.SyncState.Status)
		require.Len(t, got.LiveState.Resources, 1)
		assert.Equal(t, "orders", got.LiveState.Resources[0].Name)
	})

	t.Run("does not compare when drift detection is disabled", func(t *testing.T) {
		t.Parallel()

		disabled := false
		h := newHarness(t, config.DeployTargetConfig{DriftDetectionEnabled: &disabled})
		src := source(t, map[string]string{"topics/orders.yaml": ordersTopic}, ownAll())

		got := h.livestate(t, src)

		assert.Equal(t, sdk.ApplicationSyncStateUnknown, got.SyncState.Status)
		assert.Contains(t, got.SyncState.ShortReason, "disabled")
		// The live state is still worth reporting.
		assert.Len(t, got.LiveState.Resources, 1)
	})

	t.Run("reports an unreadable desired state as invalid config", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t, config.DeployTargetConfig{}, provider.TopicState{Name: "orders", Partitions: 12, ReplicationFactor: 3})
		src := source(t, map[string]string{"topics/orders.yaml": "name: orders\npartitions: nope\n"}, ownAll())

		got := h.livestate(t, src)

		assert.Equal(t, sdk.ApplicationSyncStateInvalidConfig, got.SyncState.Status)
		assert.Contains(t, got.SyncState.Reason, "topics/orders.yaml")
	})

	t.Run("reports the registry as unknown rather than as drift", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t, config.DeployTargetConfig{}, provider.TopicState{Name: "orders", Partitions: 1, ReplicationFactor: 1})
		h.registry.Err = assert.AnError
		src := source(t, map[string]string{
			"topics/orders.yaml":  topicWithSchema,
			"schemas/orders.avsc": `{"type":"record","name":"Order","fields":[]}`,
		}, config.ApplicationConfigSpec{Ownership: []string{"*"}, SchemasDir: "schemas"})

		got := h.livestate(t, src)

		assert.Equal(t, sdk.ApplicationSyncStateUnknown, got.SyncState.Status)
		assert.Contains(t, got.SyncState.ShortReason, "could not be compared")
	})
}

func TestGetLivestateSchemaSubjects(t *testing.T) {
	t.Parallel()

	files := map[string]string{
		"topics/orders.yaml":  topicWithSchema,
		"schemas/orders.avsc": `{"type":"record","name":"Order","fields":[]}`,
	}
	spec := config.ApplicationConfigSpec{Ownership: []string{"*"}, SchemasDir: "schemas"}

	t.Run("reports the registered version", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t, config.DeployTargetConfig{}, provider.TopicState{Name: "orders", Partitions: 1, ReplicationFactor: 1})
		h.registry.Seed("orders-value", `{"type":"record","name":"Order","fields":[]}`)
		src := source(t, files, spec)

		got := h.livestate(t, src)

		assert.Equal(t, sdk.ApplicationSyncStateSynced, got.SyncState.Status)

		subject := resourceByName(t, got.LiveState, "orders-value")
		assert.Equal(t, sdk.ResourceHealthStateHealthy, subject.HealthStatus)
		assert.Equal(t, resourceTypeSubject, subject.ResourceType)
		assert.Equal(t, []string{"topic/orders"}, subject.ParentIDs)
		assert.Equal(t, "1", subject.ResourceMetadata["version"])
		assert.Equal(t, "AVRO", subject.ResourceMetadata["type"])
	})

	t.Run("marks a subject that was never registered", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t, config.DeployTargetConfig{}, provider.TopicState{Name: "orders", Partitions: 1, ReplicationFactor: 1})
		src := source(t, files, spec)

		got := h.livestate(t, src)

		assert.Equal(t, sdk.ApplicationSyncStateOutOfSync, got.SyncState.Status)

		subject := resourceByName(t, got.LiveState, "orders-value")
		assert.Equal(t, sdk.ResourceHealthStateUnhealthy, subject.HealthStatus)
		assert.Contains(t, subject.HealthDescription, "not registered yet")
	})
}
