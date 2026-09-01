//go:build integration

// make up
// go test -tags integration ./provider/...
package provider

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/niladrix719/pipecd-kafka-plugin/config"
)

func brokers() []string {
	if v := os.Getenv("KAFKA_BOOTSTRAP_SERVERS"); v != "" {
		return []string{v}
	}
	return []string{"localhost:9092"}
}

func registryURL() string {
	if v := os.Getenv("KAFKA_SCHEMA_REGISTRY_URL"); v != "" {
		return v
	}
	return "http://localhost:8081"
}

// uniqueName keeps parallel runs and reruns from colliding on the cluster.
func uniqueName(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func TestClusterLifecycle(t *testing.T) {
	cluster, err := NewCluster(config.DeployTargetConfig{BootstrapServers: brokers()})
	require.NoError(t, err)
	defer cluster.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	name := uniqueName("plugin-test")
	t.Cleanup(func() { _ = cluster.DeleteTopic(context.Background(), name) })

	// Create.
	require.NoError(t, cluster.CreateTopic(ctx, TopicState{
		Name:              name,
		Partitions:        3,
		ReplicationFactor: 1,
		Config:            map[string]string{"retention.ms": "604800000"},
	}))

	topics, err := cluster.ListTopics(ctx)
	require.NoError(t, err)
	created, ok := topics[name]
	require.True(t, ok, "the topic should exist after being created")
	assert.Equal(t, 3, created.Partitions)
	assert.Equal(t, 1, created.ReplicationFactor)
	assert.Equal(t, "604800000", created.Config["retention.ms"])

	// Only explicitly set configs are reported: a topic has dozens of broker
	// defaults, and treating those as managed state would make every plan noisy.
	assert.NotContains(t, created.Config, "segment.bytes")

	// Update the config, and clear a key back to the broker default.
	require.NoError(t, cluster.AlterTopicConfig(ctx, name, ConfigChange{
		Set:    map[string]string{"cleanup.policy": "compact"},
		Delete: []string{"retention.ms"},
	}))

	topics, err = cluster.ListTopics(ctx)
	require.NoError(t, err)
	updated := topics[name]
	assert.Equal(t, "compact", updated.Config["cleanup.policy"])
	assert.NotContains(t, updated.Config, "retention.ms")

	// Grow the partition count.
	require.NoError(t, cluster.IncreasePartitions(ctx, name, 6))
	topics, err = cluster.ListTopics(ctx)
	require.NoError(t, err)
	assert.Equal(t, 6, topics[name].Partitions)

	// Kafka refuses to shrink it, which is the premise the plugin is built on.
	err = cluster.IncreasePartitions(ctx, name, 3)
	assert.Error(t, err, "Kafka must not allow a partition count to be reduced")

	// Delete.
	require.NoError(t, cluster.DeleteTopic(ctx, name))
	topics, err = cluster.ListTopics(ctx)
	require.NoError(t, err)
	assert.NotContains(t, topics, name)
}

func TestRegistryLifecycle(t *testing.T) {
	registry, err := NewRegistry(config.SchemaRegistryConfig{URL: registryURL()})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	subject := uniqueName("plugin-test-value")

	// An unknown subject is "does not exist", not an error.
	_, exists, err := registry.LatestSchema(ctx, subject)
	require.NoError(t, err)
	assert.False(t, exists)

	// Nothing can be incompatible with a subject that has no versions.
	result, err := registry.CheckCompatibility(ctx, subject, Schema{Body: `{"type":"record","name":"R","fields":[]}`})
	require.NoError(t, err)
	assert.True(t, result.Compatible)

	v1 := `{"type":"record","name":"R","fields":[{"name":"a","type":"string"}]}`
	registered, err := registry.RegisterSchema(ctx, subject, Schema{Subject: subject, Body: v1})
	require.NoError(t, err)
	assert.Equal(t, 1, registered.Version)
	t.Cleanup(func() { _ = registry.SoftDeleteVersion(context.Background(), subject, 1) })

	latest, exists, err := registry.LatestSchema(ctx, subject)
	require.NoError(t, err)
	require.True(t, exists)
	assert.Equal(t, 1, latest.Version)

	// Adding a field with a default stays backward compatible.
	compatible := `{"type":"record","name":"R","fields":[{"name":"a","type":"string"},{"name":"b","type":"string","default":""}]}`
	result, err = registry.CheckCompatibility(ctx, subject, Schema{Body: compatible})
	require.NoError(t, err)
	assert.True(t, result.Compatible)

	// Removing a field does not.
	incompatible := `{"type":"record","name":"R","fields":[{"name":"c","type":"int"}]}`
	result, err = registry.CheckCompatibility(ctx, subject, Schema{Body: incompatible})
	require.NoError(t, err)
	assert.False(t, result.Compatible, "dropping a required field must not be backward compatible")
}
