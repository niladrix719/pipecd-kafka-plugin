package config

import (
	"fmt"
	"os"
	"strings"
)

// Config is the plugin-scoped configuration, set under plugins[].config in the
// piped config.
type Config struct{}

// DeployTargetConfig describes one Kafka cluster and what piped is permitted to
// do to it.
type DeployTargetConfig struct {
	// BootstrapServers is the list of broker addresses.
	BootstrapServers []string `json:"bootstrapServers"`
	// ClientID identifies this piped to the cluster in broker logs and quotas.
	ClientID string     `json:"clientID,omitempty"`
	TLS      TLSConfig  `json:"tls,omitempty"`
	SASL     SASLConfig `json:"sasl,omitempty"`
	// SchemaRegistry is optional: a cluster may have no registry, in which case
	// topics may not declare schemas.
	SchemaRegistry *SchemaRegistryConfig `json:"schemaRegistry,omitempty"`

	// AllowTopicDeletion permits deleting a topic that is no longer defined.
	// Deleting a topic destroys its data and cannot be rolled back.
	AllowTopicDeletion bool `json:"allowTopicDeletion"`
	// AllowPartitionIncrease permits raising a topic's partition count.
	// Partition counts can only ever increase, so this cannot be rolled back.
	AllowPartitionIncrease bool `json:"allowPartitionIncrease"`
	// ProtectedTopics are glob patterns that this piped must never modify,
	// whatever the application config says. Internal topics belong here.
	ProtectedTopics []string `json:"protectedTopics,omitempty"`

	// DriftDetectionEnabled reports whether the live cluster state should be
	// compared against the desired state outside of a deployment.
	DriftDetectionEnabled *bool `json:"driftDetectionEnabled" default:"true"`
}

// TLSConfig configures transport security to the brokers.
type TLSConfig struct {
	Enabled bool `json:"enabled"`
	// CAFile is a PEM bundle used to verify the broker certificates.
	CAFile string `json:"caFile,omitempty"`
	// CertFile and KeyFile enable mutual TLS.
	CertFile string `json:"certFile,omitempty"`
	KeyFile  string `json:"keyFile,omitempty"`
	// InsecureSkipVerify disables certificate verification. Intended for local
	// clusters with self-signed certificates, never for production.
	InsecureSkipVerify bool `json:"insecureSkipVerify,omitempty"`
}

// SASLConfig configures authentication to the brokers.
type SASLConfig struct {
	// Mechanism is PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512. Empty disables SASL.
	Mechanism string `json:"mechanism,omitempty"`
	Username  string `json:"username,omitempty"`
	// Password holds the secret inline. Prefer PasswordFile.
	Password string `json:"password,omitempty"`
	// PasswordFile is a path to a file holding the password, so the secret does
	// not have to be written into the piped config.
	PasswordFile string `json:"passwordFile,omitempty"`
}

// SchemaRegistryConfig points at a Confluent-compatible schema registry.
type SchemaRegistryConfig struct {
	URL                string `json:"url"`
	Username           string `json:"username,omitempty"`
	Password           string `json:"password,omitempty"`
	PasswordFile       string `json:"passwordFile,omitempty"`
	CAFile             string `json:"caFile,omitempty"`
	InsecureSkipVerify bool   `json:"insecureSkipVerify,omitempty"`
}

// ApplicationConfigSpec is the application-scoped config, describing which part
// of a shared cluster this application owns and where its desired state lives.
type ApplicationConfigSpec struct {
	// TopicsDir holds the topic definition files, relative to the application
	// directory.
	TopicsDir string `json:"topicsDir,omitempty"`
	// SchemasDir holds schema files referenced by topics, relative to the
	// application directory.
	SchemasDir string `json:"schemasDir,omitempty"`
	// Ownership are glob patterns naming the topics this application manages.
	Ownership []string `json:"ownership,omitempty"`
	// Defaults are applied to any topic in this application that does not set
	// them itself.
	Defaults TopicDefaults `json:"defaults,omitempty"`
}

// TopicDefaults are application-wide defaults for topic definitions.
type TopicDefaults struct {
	Partitions        int               `json:"partitions,omitempty"`
	ReplicationFactor int               `json:"replicationFactor,omitempty"`
	Config            map[string]string `json:"config,omitempty"`
}

const defaultTopicsDir = "topics"

// TopicsDirOrDefault returns the configured topics directory, or the default.
func (s *ApplicationConfigSpec) TopicsDirOrDefault() string {
	if s.TopicsDir == "" {
		return defaultTopicsDir
	}
	return s.TopicsDir
}

// PlanStageOptions configures a KAFKA_PLAN stage.
type PlanStageOptions struct {
	// ExitOnNoChanges ends the pipeline successfully when the plan is empty,
	// so a no-op deployment does not run the remaining stages.
	ExitOnNoChanges bool `json:"exitOnNoChanges,omitempty"`
}

// RegisterSchemaStageOptions configures a KAFKA_REGISTER_SCHEMA stage.
type RegisterSchemaStageOptions struct {
	// Compatibility overrides the compatibility level checked against, for every
	// subject in this application. Empty means each subject's own configured
	// level is used.
	Compatibility string `json:"compatibility,omitempty"`
}

// ApplyStageOptions configures a KAFKA_APPLY stage.
type ApplyStageOptions struct {
	// ContinueOnError keeps applying independent changes after one fails,
	// instead of stopping at the first failure.
	ContinueOnError bool `json:"continueOnError,omitempty"`
}

// Validate checks a deploy target config for mistakes that would otherwise
// surface as confusing connection errors.
func (c *DeployTargetConfig) Validate() error {
	if len(c.BootstrapServers) == 0 {
		return fmt.Errorf("bootstrapServers must not be empty")
	}
	if err := c.SASL.validate(); err != nil {
		return err
	}
	if c.SchemaRegistry != nil {
		if err := c.SchemaRegistry.validate(); err != nil {
			return err
		}
	}
	return nil
}

// Enabled reports whether SASL authentication is configured.
func (c *SASLConfig) Enabled() bool { return c.Mechanism != "" }

func (c *SASLConfig) validate() error {
	if !c.Enabled() {
		return nil
	}
	switch strings.ToUpper(c.Mechanism) {
	case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
	default:
		return fmt.Errorf("unknown sasl.mechanism %q: must be PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512", c.Mechanism)
	}
	if c.Username == "" {
		return fmt.Errorf("sasl.username must be set when sasl.mechanism is set")
	}
	return validateSecret("sasl", c.Password, c.PasswordFile, true)
}

// ResolvePassword returns the SASL password, reading it from disk when
// passwordFile is used.
func (c *SASLConfig) ResolvePassword() (string, error) {
	return resolveSecret("sasl", c.Password, c.PasswordFile)
}

func (c *SchemaRegistryConfig) validate() error {
	if c.URL == "" {
		return fmt.Errorf("schemaRegistry.url must not be empty")
	}
	// The registry may be unauthenticated, so a missing secret is fine here.
	return validateSecret("schemaRegistry", c.Password, c.PasswordFile, false)
}

// ResolvePassword returns the registry password, reading it from disk when
// passwordFile is used. It returns an empty string when neither is set.
func (c *SchemaRegistryConfig) ResolvePassword() (string, error) {
	if c.Password == "" && c.PasswordFile == "" {
		return "", nil
	}
	return resolveSecret("schemaRegistry", c.Password, c.PasswordFile)
}

// validateSecret rejects the two ways of supplying a secret being used at once,
// and optionally requires that one of them is present.
func validateSecret(scope, inline, file string, required bool) error {
	if inline != "" && file != "" {
		return fmt.Errorf("%s.password and %s.passwordFile are mutually exclusive", scope, scope)
	}
	if required && inline == "" && file == "" {
		return fmt.Errorf("either %s.password or %s.passwordFile must be set", scope, scope)
	}
	return nil
}

// resolveSecret returns an inline secret, or the trimmed contents of the file
// holding it.
func resolveSecret(scope, inline, file string) (string, error) {
	if inline != "" {
		return inline, nil
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return "", fmt.Errorf("reading %s.passwordFile: %w", scope, err)
	}
	secret := strings.TrimSpace(string(data))
	if secret == "" {
		return "", fmt.Errorf("%s.passwordFile %s is empty", scope, file)
	}
	return secret, nil
}
