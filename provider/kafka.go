package provider

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/niladrix719/pipecd-kafka-plugin/config"
)

// kafkaCluster is the Cluster implementation backed by a real broker.
type kafkaCluster struct {
	client *kgo.Client
	admin  *kadm.Client
}

// NewCluster connects to the cluster described by a deploy target.
func NewCluster(cfg config.DeployTargetConfig) (Cluster, error) {
	opts := []kgo.Opt{kgo.SeedBrokers(cfg.BootstrapServers...)}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}

	if cfg.TLS.Enabled {
		tlsConfig, err := buildTLSConfig(cfg.TLS)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}

	if cfg.SASL.Enabled() {
		mechanism, err := buildSASL(cfg.SASL)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(mechanism))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("connecting to the Kafka cluster: %w", err)
	}
	return &kafkaCluster{client: client, admin: kadm.NewClient(client)}, nil
}

func buildTLSConfig(cfg config.TLSConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: cfg.InsecureSkipVerify, //nolint:gosec // opt-in, for local clusters with self-signed certificates
	}

	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("reading tls.caFile: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("tls.caFile %s contains no usable certificates", cfg.CAFile)
		}
		tlsConfig.RootCAs = pool
	}

	if cfg.CertFile != "" || cfg.KeyFile != "" {
		if cfg.CertFile == "" || cfg.KeyFile == "" {
			return nil, fmt.Errorf("tls.certFile and tls.keyFile must be set together")
		}
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("loading the client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

func buildSASL(cfg config.SASLConfig) (sasl.Mechanism, error) {
	password, err := cfg.ResolvePassword()
	if err != nil {
		return nil, err
	}
	switch strings.ToUpper(cfg.Mechanism) {
	case "PLAIN":
		return plain.Auth{User: cfg.Username, Pass: password}.AsMechanism(), nil
	case "SCRAM-SHA-256":
		return scram.Auth{User: cfg.Username, Pass: password}.AsSha256Mechanism(), nil
	case "SCRAM-SHA-512":
		return scram.Auth{User: cfg.Username, Pass: password}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("unknown sasl.mechanism %q", cfg.Mechanism)
	}
}

// ListTopics implements Cluster.
func (c *kafkaCluster) ListTopics(ctx context.Context) (map[string]TopicState, error) {
	details, err := c.admin.ListTopics(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing topics: %w", err)
	}

	names := make([]string, 0, len(details))
	states := make(map[string]TopicState, len(details))
	for _, detail := range details {
		if detail.Err != nil {
			return nil, fmt.Errorf("listing topic %s: %w", detail.Topic, detail.Err)
		}
		names = append(names, detail.Topic)
		states[detail.Topic] = TopicState{
			Name:              detail.Topic,
			Partitions:        len(detail.Partitions),
			ReplicationFactor: detail.Partitions.NumReplicas(),
			Config:            map[string]string{},
		}
	}
	if len(names) == 0 {
		return states, nil
	}

	resources, err := c.admin.DescribeTopicConfigs(ctx, names...)
	if err != nil {
		return nil, fmt.Errorf("describing topic configs: %w", err)
	}
	for _, resource := range resources {
		if resource.Err != nil {
			return nil, fmt.Errorf("describing the config of topic %s: %w", resource.Name, resource.Err)
		}
		state, ok := states[resource.Name]
		if !ok {
			continue
		}
		for _, entry := range resource.Configs {
			// Only explicitly set configs count as state we manage. Broker
			// defaults and static configs would otherwise appear as drift.
			if entry.Source != kmsg.ConfigSourceDynamicTopicConfig || entry.Value == nil {
				continue
			}
			state.Config[entry.Key] = *entry.Value
		}
		states[resource.Name] = state
	}

	return states, nil
}

// CreateTopic implements Cluster.
func (c *kafkaCluster) CreateTopic(ctx context.Context, topic TopicState) error {
	configs := make(map[string]*string, len(topic.Config))
	for key, value := range topic.Config {
		configs[key] = kadm.StringPtr(value)
	}

	resp, err := c.admin.CreateTopic(ctx, int32(topic.Partitions), int16(topic.ReplicationFactor), configs, topic.Name)
	if err != nil {
		return fmt.Errorf("creating topic %s: %w", topic.Name, err)
	}
	if resp.Err != nil {
		return fmt.Errorf("creating topic %s: %w", topic.Name, resp.Err)
	}
	return nil
}

// AlterTopicConfig implements Cluster.
func (c *kafkaCluster) AlterTopicConfig(ctx context.Context, name string, change ConfigChange) error {
	if change.Empty() {
		return nil
	}

	alters := make([]kadm.AlterConfig, 0, len(change.Set)+len(change.Delete))
	for key, value := range change.Set {
		alters = append(alters, kadm.AlterConfig{Op: kadm.SetConfig, Name: key, Value: kadm.StringPtr(value)})
	}
	for _, key := range change.Delete {
		alters = append(alters, kadm.AlterConfig{Op: kadm.DeleteConfig, Name: key})
	}

	responses, err := c.admin.AlterTopicConfigs(ctx, alters, name)
	if err != nil {
		return fmt.Errorf("altering the config of topic %s: %w", name, err)
	}
	for _, resp := range responses {
		if resp.Err != nil {
			return fmt.Errorf("altering the config of topic %s: %w", name, resp.Err)
		}
	}
	return nil
}

// IncreasePartitions implements Cluster.
func (c *kafkaCluster) IncreasePartitions(ctx context.Context, name string, count int) error {
	responses, err := c.admin.UpdatePartitions(ctx, count, name)
	if err != nil {
		return fmt.Errorf("increasing the partitions of topic %s: %w", name, err)
	}
	for _, resp := range responses {
		if resp.Err != nil {
			return fmt.Errorf("increasing the partitions of topic %s: %w", name, resp.Err)
		}
	}
	return nil
}

// DeleteTopic implements Cluster.
func (c *kafkaCluster) DeleteTopic(ctx context.Context, name string) error {
	responses, err := c.admin.DeleteTopics(ctx, name)
	if err != nil {
		return fmt.Errorf("deleting topic %s: %w", name, err)
	}
	for _, resp := range responses {
		if resp.Err != nil {
			return fmt.Errorf("deleting topic %s: %w", name, resp.Err)
		}
	}
	return nil
}

// Close implements Cluster.
func (c *kafkaCluster) Close() { c.client.Close() }
