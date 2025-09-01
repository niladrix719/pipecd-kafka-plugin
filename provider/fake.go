package provider

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strings"
	"sync"
)

// FakeCluster is an in-memory Cluster for tests, so the plan, apply and
// rollback logic can be exercised without a broker.
//
// It enforces the rules that matter to this plugin: partition counts only go
// up, and a deleted topic is really gone.
type FakeCluster struct {
	mu     sync.Mutex
	topics map[string]TopicState

	// Calls records every mutating operation, in order, as a readable string.
	Calls []string
	// FailOn makes the operation on a given topic fail, keyed by "kind:topic".
	FailOn map[string]error
	// Closed reports whether Close was called.
	Closed bool
}

// NewFakeCluster returns a cluster preloaded with the given topics.
func NewFakeCluster(topics ...TopicState) *FakeCluster {
	byName := make(map[string]TopicState, len(topics))
	for _, topic := range topics {
		if topic.Config == nil {
			topic.Config = map[string]string{}
		}
		byName[topic.Name] = topic
	}
	return &FakeCluster{topics: byName, FailOn: map[string]error{}}
}

// ListTopics implements Cluster.
func (c *FakeCluster) ListTopics(context.Context) (map[string]TopicState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make(map[string]TopicState, len(c.topics))
	for name, topic := range c.topics {
		topic.Config = maps.Clone(topic.Config)
		out[name] = topic
	}
	return out, nil
}

// CreateTopic implements Cluster.
func (c *FakeCluster) CreateTopic(_ context.Context, topic TopicState) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.fail("create", topic.Name); err != nil {
		return err
	}
	if _, exists := c.topics[topic.Name]; exists {
		return fmt.Errorf("topic %s already exists", topic.Name)
	}
	if topic.Config == nil {
		topic.Config = map[string]string{}
	}
	c.topics[topic.Name] = topic
	c.Calls = append(c.Calls, fmt.Sprintf("create %s partitions=%d rf=%d", topic.Name, topic.Partitions, topic.ReplicationFactor))
	return nil
}

// AlterTopicConfig implements Cluster.
func (c *FakeCluster) AlterTopicConfig(_ context.Context, name string, change ConfigChange) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.fail("alter", name); err != nil {
		return err
	}
	topic, exists := c.topics[name]
	if !exists {
		return fmt.Errorf("topic %s does not exist", name)
	}

	for key, value := range change.Set {
		topic.Config[key] = value
	}
	for _, key := range change.Delete {
		delete(topic.Config, key)
	}
	c.topics[name] = topic

	keys := make([]string, 0, len(change.Set))
	for key := range change.Set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	c.Calls = append(c.Calls, fmt.Sprintf("alter %s set=[%s] delete=[%s]", name, strings.Join(keys, " "), strings.Join(change.Delete, " ")))
	return nil
}

// IncreasePartitions implements Cluster.
func (c *FakeCluster) IncreasePartitions(_ context.Context, name string, count int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.fail("partitions", name); err != nil {
		return err
	}
	topic, exists := c.topics[name]
	if !exists {
		return fmt.Errorf("topic %s does not exist", name)
	}
	if count < topic.Partitions {
		return fmt.Errorf("cannot reduce the partitions of topic %s from %d to %d", name, topic.Partitions, count)
	}
	topic.Partitions = count
	c.topics[name] = topic
	c.Calls = append(c.Calls, fmt.Sprintf("partitions %s -> %d", name, count))
	return nil
}

// DeleteTopic implements Cluster.
func (c *FakeCluster) DeleteTopic(_ context.Context, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.fail("delete", name); err != nil {
		return err
	}
	if _, exists := c.topics[name]; !exists {
		return fmt.Errorf("topic %s does not exist", name)
	}
	delete(c.topics, name)
	c.Calls = append(c.Calls, fmt.Sprintf("delete %s", name))
	return nil
}

// Close implements Cluster.
func (c *FakeCluster) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Closed = true
}

// Topic returns the current state of a topic, for assertions.
func (c *FakeCluster) Topic(name string) (TopicState, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	topic, ok := c.topics[name]
	topic.Config = maps.Clone(topic.Config)
	return topic, ok
}

func (c *FakeCluster) fail(kind, topic string) error {
	if err, ok := c.FailOn[kind+":"+topic]; ok {
		return err
	}
	return nil
}

// FakeRegistry is an in-memory Registry for tests.
type FakeRegistry struct {
	mu       sync.Mutex
	versions map[string][]RegisteredSchema

	// Incompatible marks subjects whose next registration is rejected.
	Incompatible map[string][]string
	// Registered records the subjects registered, in order.
	Registered []string
	// Err, when set, is returned by every method.
	Err error
}

// NewFakeRegistry returns an empty registry.
func NewFakeRegistry() *FakeRegistry {
	return &FakeRegistry{versions: map[string][]RegisteredSchema{}, Incompatible: map[string][]string{}}
}

// Seed adds an existing version of a subject.
func (r *FakeRegistry) Seed(subject, body string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	next := len(r.versions[subject]) + 1
	r.versions[subject] = append(r.versions[subject], RegisteredSchema{
		Subject: subject,
		Version: next,
		ID:      100 + next,
		Body:    body,
		Type:    "AVRO",
	})
}

// CheckCompatibility implements Registry.
func (r *FakeRegistry) CheckCompatibility(_ context.Context, subject string, _ Schema) (CompatibilityResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.Err != nil {
		return CompatibilityResult{}, r.Err
	}
	if messages, bad := r.Incompatible[subject]; bad {
		return CompatibilityResult{Compatible: false, Messages: messages}, nil
	}
	return CompatibilityResult{Compatible: true}, nil
}

// LatestSchema implements Registry.
func (r *FakeRegistry) LatestSchema(_ context.Context, subject string) (RegisteredSchema, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.Err != nil {
		return RegisteredSchema{}, false, r.Err
	}
	versions := r.versions[subject]
	if len(versions) == 0 {
		return RegisteredSchema{}, false, nil
	}
	return versions[len(versions)-1], true, nil
}

// RegisterSchema implements Registry.
func (r *FakeRegistry) RegisterSchema(_ context.Context, subject string, schema Schema) (RegisteredSchema, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.Err != nil {
		return RegisteredSchema{}, r.Err
	}
	next := len(r.versions[subject]) + 1
	registered := RegisteredSchema{
		Subject: subject,
		Version: next,
		ID:      100 + next,
		Body:    schema.Body,
		Type:    schema.SchemaType(),
	}
	r.versions[subject] = append(r.versions[subject], registered)
	r.Registered = append(r.Registered, subject)
	return registered, nil
}

// SoftDeleteVersion implements Registry.
func (r *FakeRegistry) SoftDeleteVersion(_ context.Context, subject string, version int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.Err != nil {
		return r.Err
	}
	kept := make([]RegisteredSchema, 0, len(r.versions[subject]))
	for _, schema := range r.versions[subject] {
		if schema.Version != version {
			kept = append(kept, schema)
		}
	}
	r.versions[subject] = kept
	return nil
}

var (
	_ Cluster  = (*FakeCluster)(nil)
	_ Registry = (*FakeRegistry)(nil)
)
