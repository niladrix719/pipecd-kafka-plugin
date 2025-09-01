package plan

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/niladrix719/pipecd-kafka-plugin/provider"
)

func TestChangeKindReversible(t *testing.T) {
	t.Parallel()

	// This table is the plugin's central claim about Kafka. If it changes,
	// rollback behaviour changes with it.
	assert.True(t, CreateTopic.Reversible())
	assert.True(t, UpdateTopicConfig.Reversible())
	assert.True(t, RegisterSchema.Reversible())
	assert.False(t, IncreasePartitions.Reversible(), "Kafka cannot lower a partition count")
	assert.False(t, DeleteTopic.Reversible(), "a deleted topic's data is gone")
}

func TestChangeKindString(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "create topic", CreateTopic.String())
	assert.Equal(t, "increase partitions", IncreasePartitions.String())
	assert.Equal(t, "unknown", ChangeKind(99).String())
}

func TestRender(t *testing.T) {
	t.Parallel()

	p := &Plan{
		Changes: []Change{
			{Kind: CreateTopic, Topic: "orders", Desired: &provider.TopicState{Partitions: 12, ReplicationFactor: 3}},
			{Kind: IncreasePartitions, Topic: "events", FromPartitions: 6, ToPartitions: 12},
		},
		Blocked: []Blocked{{Reason: "topic legacy is no longer declared, but allowTopicDeletion is false"}},
	}

	got := p.Render()

	assert.Contains(t, got, "Plan: 1 to create topic, 1 to increase partitions.")
	assert.Contains(t, got, "+ create topic orders")
	assert.Contains(t, got, "! increase partitions of topic events from 6 to 12")
	assert.Contains(t, got, "These changes cannot be undone by a rollback:")
	assert.Contains(t, got, "Blocked (1)")
	assert.Contains(t, got, "x topic legacy is no longer declared")
}

func TestRenderEmpty(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "No changes. The cluster already matches the desired state.", (&Plan{}).Render())
}
