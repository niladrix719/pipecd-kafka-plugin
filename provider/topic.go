// Package provider reads the desired state of an application from its
// repository and talks to the Kafka cluster and schema registry that hold the
// actual state.
package provider

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/niladrix719/pipecd-kafka-plugin/config"
)

// Topic is one topic's desired state, as declared in the repository.
type Topic struct {
	Name              string            `yaml:"name"`
	Partitions        int               `yaml:"partitions"`
	ReplicationFactor int               `yaml:"replicationFactor"`
	Config            map[string]string `yaml:"config"`
	Schema            *Schema           `yaml:"schema"`

	// SourceFile is the file this topic was declared in, used in error
	// messages so an operator knows which file to fix.
	SourceFile string `yaml:"-"`
}

// Schema is the desired schema for a topic's records.
type Schema struct {
	// Subject is the registry subject. Conventionally "<topic>-value".
	Subject string `yaml:"subject"`
	// File is the schema file, relative to the application directory.
	File string `yaml:"file"`
	// Type is AVRO, PROTOBUF or JSON. Defaults to AVRO.
	Type string `yaml:"type"`
	// Compatibility is the level this schema must satisfy against the existing
	// versions of the subject, such as BACKWARD. Empty means the subject's own
	// configured level is used.
	Compatibility string `yaml:"compatibility"`

	// Body is the schema text, read from File when the topics are loaded.
	Body string `yaml:"-"`
}

// LoadTopics reads every topic definition for an application.
//
// appDir is the application directory piped checked out; spec is the
// application-scoped plugin config naming the directories and ownership.
func LoadTopics(appDir string, spec config.ApplicationConfigSpec) ([]Topic, error) {
	topicsDir := filepath.Join(appDir, spec.TopicsDirOrDefault())

	entries, err := os.ReadDir(topicsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("the topics directory %s does not exist", spec.TopicsDirOrDefault())
		}
		return nil, fmt.Errorf("reading the topics directory: %w", err)
	}

	byName := make(map[string]Topic)
	topics := make([]Topic, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !isYAML(entry.Name()) {
			continue
		}
		rel := filepath.Join(spec.TopicsDirOrDefault(), entry.Name())

		parsed, err := parseTopicFile(filepath.Join(topicsDir, entry.Name()), rel)
		if err != nil {
			return nil, err
		}
		for _, topic := range parsed {
			applyDefaults(&topic, spec.Defaults)
			if err := topic.validate(); err != nil {
				return nil, err
			}
			if previous, ok := byName[topic.Name]; ok {
				return nil, fmt.Errorf("topic %q is declared twice, in %s and in %s", topic.Name, previous.SourceFile, topic.SourceFile)
			}
			if err := topic.loadSchemaBody(appDir, spec); err != nil {
				return nil, err
			}
			byName[topic.Name] = topic
			topics = append(topics, topic)
		}
	}

	// A topic outside the application's ownership would be created but never
	// seen again by a later plan, so treat it as the typo it almost always is.
	for _, topic := range topics {
		if !Matches(topic.Name, spec.Ownership) {
			return nil, fmt.Errorf("topic %q in %s is not covered by this application's ownership patterns %v", topic.Name, topic.SourceFile, spec.Ownership)
		}
	}

	sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })
	return topics, nil
}

// parseTopicFile reads one file, which may hold a single topic or a YAML stream
// of several.
func parseTopicFile(fullPath, relPath string) ([]Topic, error) {
	data, err := os.ReadFile(fullPath)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", relPath, err)
	}

	var topics []Topic
	decoder := yaml.NewDecoder(strings.NewReader(string(data)))
	for {
		var topic Topic
		err := decoder.Decode(&topic)
		if err != nil {
			// io.EOF ends the stream; anything else is a malformed document.
			if err.Error() == "EOF" {
				break
			}
			return nil, fmt.Errorf("parsing %s: %w", relPath, err)
		}
		// A document that is entirely comments decodes to the zero value.
		if topic.Name == "" && topic.Partitions == 0 && topic.Config == nil {
			continue
		}
		topic.SourceFile = relPath
		topics = append(topics, topic)
	}
	return topics, nil
}

// applyDefaults fills in anything the topic did not set itself.
func applyDefaults(topic *Topic, defaults config.TopicDefaults) {
	if topic.Partitions == 0 {
		topic.Partitions = defaults.Partitions
	}
	if topic.ReplicationFactor == 0 {
		topic.ReplicationFactor = defaults.ReplicationFactor
	}
	if len(defaults.Config) == 0 {
		return
	}
	if topic.Config == nil {
		topic.Config = make(map[string]string, len(defaults.Config))
	}
	for key, value := range defaults.Config {
		if _, ok := topic.Config[key]; !ok {
			topic.Config[key] = value
		}
	}
}

func (t *Topic) validate() error {
	if t.Name == "" {
		return fmt.Errorf("a topic in %s has no name", t.SourceFile)
	}
	if t.Partitions <= 0 {
		return fmt.Errorf("topic %q in %s must set partitions to a positive number", t.Name, t.SourceFile)
	}
	if t.ReplicationFactor <= 0 {
		return fmt.Errorf("topic %q in %s must set replicationFactor to a positive number", t.Name, t.SourceFile)
	}
	if t.Schema == nil {
		return nil
	}
	if t.Schema.Subject == "" {
		return fmt.Errorf("topic %q in %s declares a schema with no subject", t.Name, t.SourceFile)
	}
	if t.Schema.File == "" {
		return fmt.Errorf("topic %q in %s declares a schema with no file", t.Name, t.SourceFile)
	}
	switch strings.ToUpper(t.Schema.Type) {
	case "", "AVRO", "PROTOBUF", "JSON":
	default:
		return fmt.Errorf("topic %q in %s declares an unknown schema type %q: must be AVRO, PROTOBUF or JSON", t.Name, t.SourceFile, t.Schema.Type)
	}
	return nil
}

// loadSchemaBody reads the schema file referenced by a topic.
//
// The path is resolved against the schemas directory when one is configured,
// and against the application directory otherwise. Either way it must stay
// inside the application directory.
func (t *Topic) loadSchemaBody(appDir string, spec config.ApplicationConfigSpec) error {
	if t.Schema == nil {
		return nil
	}

	base := appDir
	if spec.SchemasDir != "" {
		base = filepath.Join(appDir, spec.SchemasDir)
	}
	full := filepath.Join(base, t.Schema.File)

	// filepath.Join cleans the path, so a "../" escape shows up here.
	rel, err := filepath.Rel(appDir, full)
	if err != nil || strings.HasPrefix(rel, "..") {
		return fmt.Errorf("topic %q in %s references a schema outside the application directory: %s", t.Name, t.SourceFile, t.Schema.File)
	}

	body, err := os.ReadFile(full)
	if err != nil {
		return fmt.Errorf("reading the schema for topic %q declared in %s: %w", t.Name, t.SourceFile, err)
	}
	t.Schema.Body = string(body)
	return nil
}

// SchemaType returns the schema type, defaulting to AVRO.
func (s *Schema) SchemaType() string {
	if s.Type == "" {
		return "AVRO"
	}
	return strings.ToUpper(s.Type)
}

// Matches reports whether a topic name matches any of the glob patterns. No
// patterns means nothing matches, so an application with no ownership manages
// no topics rather than all of them.
func Matches(name string, patterns []string) bool {
	for _, pattern := range patterns {
		if ok, err := path.Match(pattern, name); err == nil && ok {
			return true
		}
	}
	return false
}

func isYAML(name string) bool {
	ext := strings.ToLower(filepath.Ext(name))
	return ext == ".yaml" || ext == ".yml"
}
