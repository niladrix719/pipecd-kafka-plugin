package provider

import "context"

// TopicState is a topic's actual state on the cluster, in the same terms as the
// desired state so the two can be compared directly.
type TopicState struct {
	Name              string
	Partitions        int
	ReplicationFactor int
	// Config holds only the explicitly set (dynamic) topic configs.
	Config map[string]string
}

// ConfigChange is an incremental alteration to a topic's config.
type ConfigChange struct {
	Set map[string]string
	// Delete are the keys to reset to the broker default.
	Delete []string
}

// Empty reports whether the change would alter nothing.
func (c ConfigChange) Empty() bool { return len(c.Set) == 0 && len(c.Delete) == 0 }

// Cluster is the subset of Kafka admin operations this plugin needs.
type Cluster interface {
	ListTopics(ctx context.Context) (map[string]TopicState, error)
	CreateTopic(ctx context.Context, topic TopicState) error
	AlterTopicConfig(ctx context.Context, name string, change ConfigChange) error
	// IncreasePartitions raises a topic's partition count. Kafka cannot lower
	// it, so this is one-way.
	IncreasePartitions(ctx context.Context, name string, count int) error
	DeleteTopic(ctx context.Context, name string) error
	Close()
}

// CompatibilityResult is the registry's verdict on a proposed schema.
type CompatibilityResult struct {
	Compatible bool
	// Messages explains an incompatibility, in the registry's own words.
	Messages []string
}

// RegisteredSchema is a version of a subject that exists in the registry.
type RegisteredSchema struct {
	Subject string
	Version int
	ID      int
	Body    string
	Type    string
}

// Registry is the subset of schema registry operations this plugin needs.
type Registry interface {
	// CheckCompatibility asks whether a schema could be registered against the
	// subject's latest version without breaking compatibility.
	CheckCompatibility(ctx context.Context, subject string, schema Schema) (CompatibilityResult, error)
	// LatestSchema returns the current version of a subject. The boolean is
	// false when the subject does not exist yet.
	LatestSchema(ctx context.Context, subject string) (RegisteredSchema, bool, error)
	RegisterSchema(ctx context.Context, subject string, schema Schema) (RegisteredSchema, error)
	// SoftDeleteVersion soft-deletes one version of a subject, which is the
	// closest thing the registry offers to undoing a registration.
	SoftDeleteVersion(ctx context.Context, subject string, version int) error
}
