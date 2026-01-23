package plan

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/niladrix719/pipecd-kafka-plugin/config"
	"github.com/niladrix719/pipecd-kafka-plugin/provider"
)

// Plan is the full set of changes between a desired and an actual state.
type Plan struct {
	// Changes are the operations that would be applied, in a safe order.
	Changes []Change
	// Blocked are operations the plan refuses to carry out. A plan with any
	// blocked change must not be applied.
	Blocked []Blocked
}

// Input is everything needed to build a plan.
type Input struct {
	// Desired is the application's declared state.
	Desired []provider.Topic
	// Actual is every topic currently on the cluster, keyed by name.
	Actual map[string]provider.TopicState
	// Spec is the application-scoped config, which carries the ownership scope.
	Spec config.ApplicationConfigSpec
	// Target is the deploy-target config, which carries the safety rails.
	Target config.DeployTargetConfig
	// Registry is used to check schema compatibility. It may be nil, in which
	// case declaring a schema is an error.
	Registry provider.Registry
	// CompatibilityOverride replaces each subject's configured compatibility
	// level for this plan. Empty means the subject's own level is used.
	CompatibilityOverride string
}

// Build compares the desired state against the actual state and returns the
// changes that would close the gap.
func Build(ctx context.Context, in Input) (*Plan, error) {
	p := &Plan{}

	desiredByName := make(map[string]provider.Topic, len(in.Desired))
	for _, topic := range in.Desired {
		desiredByName[topic.Name] = topic
	}

	// Creations, updates and partition increases, in declaration order.
	for _, topic := range in.Desired {
		if pattern, protected := protectedBy(topic.Name, in.Target.ProtectedTopics); protected {
			p.block(Change{Kind: UpdateTopicConfig, Topic: topic.Name},
				fmt.Sprintf("topic %s matches the protected pattern %q on this deploy target", topic.Name, pattern))
			continue
		}

		actual, exists := in.Actual[topic.Name]
		if !exists {
			p.add(Change{Kind: CreateTopic, Topic: topic.Name, Desired: desiredState(topic)})
		} else {
			p.compareExisting(topic, actual, in.Target)
		}

		if err := p.planSchema(ctx, topic, in); err != nil {
			return nil, err
		}
	}

	// Deletions last: a topic is only deleted once everything else has landed.
	for _, name := range sortedNames(in.Actual) {
		if _, stillDeclared := desiredByName[name]; stillDeclared {
			continue
		}
		// Only topics this application owns are candidates for deletion. Every
		// other topic on a shared cluster belongs to someone else.
		if !provider.Matches(name, in.Spec.Ownership) {
			continue
		}
		if pattern, protected := protectedBy(name, in.Target.ProtectedTopics); protected {
			// Protected topics are invisible to deletion rather than blocking
			// the plan: they are meant to be left alone, not managed.
			_ = pattern
			continue
		}

		actual := in.Actual[name]
		change := Change{Kind: DeleteTopic, Topic: name, Actual: &actual}
		if !in.Target.AllowTopicDeletion {
			p.block(change, fmt.Sprintf("topic %s is no longer declared, but allowTopicDeletion is false on this deploy target", name))
			continue
		}
		p.add(change)
	}

	return p, nil
}

// compareExisting plans the changes needed to move an existing topic to its
// desired state.
func (p *Plan) compareExisting(topic provider.Topic, actual provider.TopicState, target config.DeployTargetConfig) {
	// Replication factor changes need a partition reassignment plan, which is a
	// different and much heavier operation than anything else here. Refuse
	// clearly rather than silently ignoring the field.
	if topic.ReplicationFactor != actual.ReplicationFactor {
		p.block(Change{Kind: UpdateTopicConfig, Topic: topic.Name},
			fmt.Sprintf("topic %s declares replication factor %d but has %d; changing it requires a partition reassignment, which this plugin does not perform",
				topic.Name, topic.ReplicationFactor, actual.ReplicationFactor))
	}

	switch {
	case topic.Partitions > actual.Partitions:
		change := Change{
			Kind:           IncreasePartitions,
			Topic:          topic.Name,
			FromPartitions: actual.Partitions,
			ToPartitions:   topic.Partitions,
		}
		if !target.AllowPartitionIncrease {
			p.block(change, fmt.Sprintf("topic %s would grow from %d to %d partitions, but allowPartitionIncrease is false on this deploy target. This cannot be undone, and it changes which partition a key hashes to",
				topic.Name, actual.Partitions, topic.Partitions))
		} else {
			p.add(change)
		}
	case topic.Partitions < actual.Partitions:
		p.block(Change{Kind: IncreasePartitions, Topic: topic.Name, FromPartitions: actual.Partitions, ToPartitions: topic.Partitions},
			fmt.Sprintf("topic %s declares %d partitions but has %d; Kafka cannot reduce a partition count. Recreating the topic would destroy its data, so this must be resolved by hand",
				topic.Name, topic.Partitions, actual.Partitions))
	}

	if change, before := diffConfig(topic.Config, actual.Config); !change.Empty() {
		p.add(Change{
			Kind:         UpdateTopicConfig,
			Topic:        topic.Name,
			ConfigChange: change,
			ConfigBefore: before,
		})
	}
}

// planSchema plans the schema registration for a topic, if it declares one.
func (p *Plan) planSchema(ctx context.Context, topic provider.Topic, in Input) error {
	if topic.Schema == nil {
		return nil
	}
	if in.Registry == nil {
		return fmt.Errorf("topic %q declares a schema but this deploy target has no schemaRegistry configured", topic.Name)
	}

	latest, exists, err := in.Registry.LatestSchema(ctx, topic.Schema.Subject)
	if err != nil {
		return err
	}
	// An unchanged schema needs no new version. Registries normalise whitespace
	// on their side, so compare the trimmed text.
	if exists && strings.TrimSpace(latest.Body) == strings.TrimSpace(topic.Schema.Body) {
		return nil
	}

	change := Change{
		Kind:    RegisterSchema,
		Topic:   topic.Name,
		Subject: topic.Schema.Subject,
		Schema:  topic.Schema,
	}
	if exists {
		change.CurrentVersion = latest.Version
	}

	result, err := in.Registry.CheckCompatibility(ctx, topic.Schema.Subject, *topic.Schema)
	if err != nil {
		return err
	}
	change.Compatibility = result

	if !result.Compatible {
		reason := fmt.Sprintf("the new schema for subject %s is not compatible with version %d", topic.Schema.Subject, change.CurrentVersion)
		if len(result.Messages) > 0 {
			reason = fmt.Sprintf("%s: %s", reason, strings.Join(result.Messages, "; "))
		}
		p.block(change, reason)
		return nil
	}

	p.add(change)
	return nil
}

func (p *Plan) add(change Change) { p.Changes = append(p.Changes, change) }
func (p *Plan) block(change Change, why string) {
	p.Blocked = append(p.Blocked, Blocked{Change: change, Reason: why})
}

// Empty reports whether the plan would change nothing.
func (p *Plan) Empty() bool { return len(p.Changes) == 0 && len(p.Blocked) == 0 }

// HasBlocked reports whether any change was refused.
func (p *Plan) HasBlocked() bool { return len(p.Blocked) > 0 }

// TopicChanges returns the changes applied by the apply stage.
func (p *Plan) TopicChanges() []Change {
	return p.filter(func(k ChangeKind) bool { return k != RegisterSchema })
}

// SchemaChanges returns the changes applied by the schema registration stage.
func (p *Plan) SchemaChanges() []Change {
	return p.filter(func(k ChangeKind) bool { return k == RegisterSchema })
}

// Irreversible returns the changes that a rollback could not undo.
func (p *Plan) Irreversible() []Change {
	return p.filter(func(k ChangeKind) bool { return !k.Reversible() })
}

func (p *Plan) filter(keep func(ChangeKind) bool) []Change {
	changes := make([]Change, 0, len(p.Changes))
	for _, change := range p.Changes {
		if keep(change.Kind) {
			changes = append(changes, change)
		}
	}
	return changes
}

// diffConfig returns the change needed to move actual to desired, together with
// the previous value of every key it touches.
func diffConfig(desired, actual map[string]string) (provider.ConfigChange, map[string]string) {
	change := provider.ConfigChange{Set: map[string]string{}}
	before := map[string]string{}

	for key, want := range desired {
		if got, ok := actual[key]; !ok || got != want {
			change.Set[key] = want
			if ok {
				before[key] = got
			}
		}
	}

	// A key that was set explicitly and is no longer declared goes back to the
	// broker default rather than being left behind.
	for key, got := range actual {
		if _, declared := desired[key]; !declared {
			change.Delete = append(change.Delete, key)
			before[key] = got
		}
	}
	sort.Strings(change.Delete)

	if len(change.Set) == 0 {
		change.Set = nil
	}
	return change, before
}

func protectedBy(name string, patterns []string) (string, bool) {
	for _, pattern := range patterns {
		if provider.Matches(name, []string{pattern}) {
			return pattern, true
		}
	}
	return "", false
}

func desiredState(topic provider.Topic) *provider.TopicState {
	config := make(map[string]string, len(topic.Config))
	for key, value := range topic.Config {
		config[key] = value
	}
	return &provider.TopicState{
		Name:              topic.Name,
		Partitions:        topic.Partitions,
		ReplicationFactor: topic.ReplicationFactor,
		Config:            config,
	}
}

func sortedNames(topics map[string]provider.TopicState) []string {
	names := make([]string, 0, len(topics))
	for name := range topics {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
