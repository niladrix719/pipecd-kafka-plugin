package provider

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/niladrix719/pipecd-kafka-plugin/config"
)

// latestVersion asks the registry for a subject's most recent version.
const latestVersion = -1

// schemaRegistry is the Registry implementation backed by a Confluent-compatible
// schema registry.
type schemaRegistry struct {
	client *sr.Client
}

var _ Registry = (*schemaRegistry)(nil)

// NewRegistry connects to the schema registry described by a deploy target.
func NewRegistry(cfg config.SchemaRegistryConfig) (Registry, error) {
	opts := []sr.ClientOpt{sr.URLs(cfg.URL)}

	password, err := cfg.ResolvePassword()
	if err != nil {
		return nil, err
	}
	if cfg.Username != "" || password != "" {
		opts = append(opts, sr.BasicAuth(cfg.Username, password))
	}

	if cfg.CAFile != "" || cfg.InsecureSkipVerify {
		httpClient, err := registryHTTPClient(cfg)
		if err != nil {
			return nil, err
		}
		opts = append(opts, sr.HTTPClient(httpClient))
	}

	client, err := sr.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("connecting to the schema registry: %w", err)
	}
	return &schemaRegistry{client: client}, nil
}

func registryHTTPClient(cfg config.SchemaRegistryConfig) (*http.Client, error) {
	tlsConfig := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: cfg.InsecureSkipVerify, //nolint:gosec // opt-in, for local registries with self-signed certificates
	}
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("reading schemaRegistry.caFile: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("schemaRegistry.caFile %s contains no usable certificates", cfg.CAFile)
		}
		tlsConfig.RootCAs = pool
	}
	return &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}}, nil
}

func (r *schemaRegistry) CheckCompatibility(ctx context.Context, subject string, schema Schema) (CompatibilityResult, error) {
	result, err := r.client.CheckCompatibility(ctx, subject, latestVersion, toSRSchema(schema))
	if err != nil {
		// A subject with no versions yet cannot be incompatible with anything.
		if isNotFound(err) {
			return CompatibilityResult{Compatible: true}, nil
		}
		return CompatibilityResult{}, fmt.Errorf("checking the compatibility of subject %s: %w", subject, err)
	}
	return CompatibilityResult{Compatible: result.Is, Messages: result.Messages}, nil
}

func (r *schemaRegistry) LatestSchema(ctx context.Context, subject string) (RegisteredSchema, bool, error) {
	got, err := r.client.SchemaByVersion(ctx, subject, latestVersion)
	if err != nil {
		if isNotFound(err) {
			return RegisteredSchema{}, false, nil
		}
		return RegisteredSchema{}, false, fmt.Errorf("reading the latest version of subject %s: %w", subject, err)
	}
	return RegisteredSchema{
		Subject: subject,
		Version: got.Version,
		ID:      got.ID,
		Body:    got.Schema.Schema,
		Type:    got.Schema.Type.String(),
	}, true, nil
}

func (r *schemaRegistry) RegisterSchema(ctx context.Context, subject string, schema Schema) (RegisteredSchema, error) {
	created, err := r.client.CreateSchema(ctx, subject, toSRSchema(schema))
	if err != nil {
		return RegisteredSchema{}, fmt.Errorf("registering a schema for subject %s: %w", subject, err)
	}
	return RegisteredSchema{
		Subject: subject,
		Version: created.Version,
		ID:      created.ID,
		Body:    created.Schema.Schema,
		Type:    created.Schema.Type.String(),
	}, nil
}

func (r *schemaRegistry) SoftDeleteVersion(ctx context.Context, subject string, version int) error {
	if err := r.client.DeleteSchema(ctx, subject, version, sr.SoftDelete); err != nil {
		return fmt.Errorf("soft-deleting version %d of subject %s: %w", version, subject, err)
	}
	return nil
}

// toSRSchema converts a desired schema into the registry client's shape.
func toSRSchema(schema Schema) sr.Schema {
	return sr.Schema{Schema: schema.Body, Type: schemaType(schema.SchemaType())}
}

func schemaType(name string) sr.SchemaType {
	switch name {
	case "PROTOBUF":
		return sr.TypeProtobuf
	case "JSON":
		return sr.TypeJSON
	default:
		return sr.TypeAvro
	}
}

// isNotFound reports whether the registry answered "no such subject or version",
// which this plugin treats as "does not exist yet" rather than as a failure.
func isNotFound(err error) bool {
	var responseErr *sr.ResponseError
	if errors.As(err, &responseErr) {
		return responseErr.StatusCode == http.StatusNotFound
	}
	return false
}
