package plan

import (
	"fmt"
	"sort"
	"strings"

	"github.com/niladrix719/pipecd-kafka-plugin/provider"
)

// ChangeKind is the kind of operation a change performs.
type ChangeKind int

const (
	// CreateTopic creates a topic that does not exist yet.
	// Reversible by deleting it, if the target permits deletion.
	CreateTopic ChangeKind = iota
	// UpdateTopicConfig sets or clears dynamic topic configs.
	// Reversible by restoring the previous values.
	UpdateTopicConfig
	// IncreasePartitions raises a topic's partition count.
	// NOT reversible: Kafka cannot lower a partition count, and the change
	// alters which partition a given key hashes to.
	IncreasePartitions
	// DeleteTopic deletes a topic that is no longer declared.
	// NOT reversible: the data is gone.
	DeleteTopic
	// RegisterSchema registers a new version of a subject.
	// Reversible only in the weak sense that the version can be soft-deleted;
	// consumers that already read it are not un-affected.
	RegisterSchema
)

// Reversible reports whether a rollback could undo a change of this kind.
func (k ChangeKind) Reversible() bool {
	switch k {
	case CreateTopic, UpdateTopicConfig, RegisterSchema:
		return true
	case IncreasePartitions, DeleteTopic:
		return false
	default:
		return false
	}
}

func (k ChangeKind) String() string {
	switch k {
	case CreateTopic:
		return "create topic"
	case UpdateTopicConfig:
		return "update config"
	case IncreasePartitions:
		return "increase partitions"
	case DeleteTopic:
		return "delete topic"
	case RegisterSchema:
		return "register schema"
	default:
		return "unknown"
	}
}

// Change is one operation in a plan.
type Change struct {
	Kind ChangeKind
	// Topic is the topic the change applies to. For a schema change it is the
	// topic that declared the subject.
	Topic string

	// Desired is the intended topic state, set for CreateTopic.
	Desired *provider.TopicState
	// Actual is the current topic state, set for DeleteTopic.
	Actual *provider.TopicState

	// ConfigChange is the alteration to apply, set for UpdateTopicConfig.
	ConfigChange provider.ConfigChange
	// ConfigBefore records the previous values of every key the change touches,
	// so a rollback can restore them.
	ConfigBefore map[string]string

	// FromPartitions and ToPartitions are set for IncreasePartitions.
	FromPartitions int
	ToPartitions   int

	// Subject, Schema and Compatibility are set for RegisterSchema.
	Subject string
	Schema  *provider.Schema
	// CurrentVersion is the subject's latest version before this change, or 0
	// when the subject does not exist yet.
	CurrentVersion int
	Compatibility  provider.CompatibilityResult
}

// Describe renders a one-line summary of the change.
func (c Change) Describe() string {
	switch c.Kind {
	case CreateTopic:
		return fmt.Sprintf("create topic %s (%d partitions, replication factor %d)", c.Topic, c.Desired.Partitions, c.Desired.ReplicationFactor)
	case UpdateTopicConfig:
		return fmt.Sprintf("update config of topic %s (%s)", c.Topic, describeConfigChange(c.ConfigChange, c.ConfigBefore))
	case IncreasePartitions:
		return fmt.Sprintf("increase partitions of topic %s from %d to %d", c.Topic, c.FromPartitions, c.ToPartitions)
	case DeleteTopic:
		return fmt.Sprintf("delete topic %s (%d partitions)", c.Topic, c.Actual.Partitions)
	case RegisterSchema:
		if c.CurrentVersion == 0 {
			return fmt.Sprintf("register the first version of subject %s (topic %s)", c.Subject, c.Topic)
		}
		return fmt.Sprintf("register a new version of subject %s, replacing version %d (topic %s)", c.Subject, c.CurrentVersion, c.Topic)
	default:
		return "unknown change"
	}
}

// describeConfigChange renders the individual key transitions, so the log shows
// what a value is moving from and to rather than only that it moved.
func describeConfigChange(change provider.ConfigChange, before map[string]string) string {
	parts := make([]string, 0, len(change.Set)+len(change.Delete))

	keys := make([]string, 0, len(change.Set))
	for key := range change.Set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if old, ok := before[key]; ok {
			parts = append(parts, fmt.Sprintf("%s: %s -> %s", key, old, change.Set[key]))
			continue
		}
		parts = append(parts, fmt.Sprintf("%s: unset -> %s", key, change.Set[key]))
	}

	deletes := append([]string(nil), change.Delete...)
	sort.Strings(deletes)
	for _, key := range deletes {
		parts = append(parts, fmt.Sprintf("%s: %s -> broker default", key, before[key]))
	}

	return strings.Join(parts, ", ")
}

// Blocked is a change that the plan refuses to carry out, and why.
type Blocked struct {
	Change Change
	Reason string
}
