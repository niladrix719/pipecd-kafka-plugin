package provider

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/niladrix719/pipecd-kafka-plugin/config"
)

// writeApp materialises an application directory from a path -> content map.
func writeApp(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for rel, content := range files {
		p := filepath.Join(dir, rel)
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, []byte(content), 0o644))
	}
	return dir
}

func ownAll() config.ApplicationConfigSpec {
	return config.ApplicationConfigSpec{Ownership: []string{"*"}}
}

func TestLoadTopics(t *testing.T) {
	t.Parallel()

	dir := writeApp(t, map[string]string{
		"topics/orders.yaml": `
name: orders
partitions: 12
replicationFactor: 3
config:
  retention.ms: "604800000"
`,
		"topics/payments.yml": `
name: payments
partitions: 6
replicationFactor: 3
`,
		"topics/notes.txt": "ignored",
	})

	got, err := LoadTopics(dir, ownAll())
	require.NoError(t, err)

	// Sorted by name, and the non-YAML file is skipped.
	require.Len(t, got, 2)
	assert.Equal(t, "orders", got[0].Name)
	assert.Equal(t, 12, got[0].Partitions)
	assert.Equal(t, map[string]string{"retention.ms": "604800000"}, got[0].Config)
	assert.Equal(t, "topics/orders.yaml", got[0].SourceFile)
	assert.Equal(t, "payments", got[1].Name)
}

func TestLoadTopicsMultiDocumentFile(t *testing.T) {
	t.Parallel()

	dir := writeApp(t, map[string]string{
		"topics/all.yaml": `
name: a
partitions: 1
replicationFactor: 1
---
name: b
partitions: 2
replicationFactor: 1
`,
	})

	got, err := LoadTopics(dir, ownAll())
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "a", got[0].Name)
	assert.Equal(t, "b", got[1].Name)
}

func TestLoadTopicsAppliesDefaults(t *testing.T) {
	t.Parallel()

	dir := writeApp(t, map[string]string{
		"topics/orders.yaml": `
name: orders
config:
  retention.ms: "1"
`,
	})

	got, err := LoadTopics(dir, config.ApplicationConfigSpec{
		Ownership: []string{"*"},
		Defaults: config.TopicDefaults{
			Partitions:        6,
			ReplicationFactor: 3,
			Config:            map[string]string{"min.insync.replicas": "2", "retention.ms": "999"},
		},
	})
	require.NoError(t, err)

	require.Len(t, got, 1)
	assert.Equal(t, 6, got[0].Partitions)
	assert.Equal(t, 3, got[0].ReplicationFactor)
	// The topic's own value wins over the default.
	assert.Equal(t, "1", got[0].Config["retention.ms"])
	assert.Equal(t, "2", got[0].Config["min.insync.replicas"])
}

func TestLoadTopicsLoadsSchemaBody(t *testing.T) {
	t.Parallel()

	dir := writeApp(t, map[string]string{
		"topics/orders.yaml": `
name: orders
partitions: 1
replicationFactor: 1
schema:
  subject: orders-value
  file: orders.avsc
`,
		"schemas/orders.avsc": `{"type":"record","name":"Order","fields":[]}`,
	})

	got, err := LoadTopics(dir, config.ApplicationConfigSpec{Ownership: []string{"*"}, SchemasDir: "schemas"})
	require.NoError(t, err)

	require.Len(t, got, 1)
	require.NotNil(t, got[0].Schema)
	assert.Contains(t, got[0].Schema.Body, `"name":"Order"`)
	assert.Equal(t, "AVRO", got[0].Schema.SchemaType())
}

func TestLoadTopicsRejects(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		name    string
		files   map[string]string
		spec    config.ApplicationConfigSpec
		wantErr string
	}{
		{
			name:    "missing topics directory",
			files:   map[string]string{"README.md": "x"},
			spec:    ownAll(),
			wantErr: "does not exist",
		},
		{
			name:    "no name",
			files:   map[string]string{"topics/a.yaml": "partitions: 1\nreplicationFactor: 1\n"},
			spec:    ownAll(),
			wantErr: "has no name",
		},
		{
			name:    "no partitions",
			files:   map[string]string{"topics/a.yaml": "name: a\nreplicationFactor: 1\n"},
			spec:    ownAll(),
			wantErr: "must set partitions",
		},
		{
			name:    "no replication factor",
			files:   map[string]string{"topics/a.yaml": "name: a\npartitions: 1\n"},
			spec:    ownAll(),
			wantErr: "must set replicationFactor",
		},
		{
			name: "duplicate topic across files",
			files: map[string]string{
				"topics/a.yaml": "name: dup\npartitions: 1\nreplicationFactor: 1\n",
				"topics/b.yaml": "name: dup\npartitions: 1\nreplicationFactor: 1\n",
			},
			spec:    ownAll(),
			wantErr: "declared twice",
		},
		{
			name:    "outside ownership",
			files:   map[string]string{"topics/a.yaml": "name: other.thing\npartitions: 1\nreplicationFactor: 1\n"},
			spec:    config.ApplicationConfigSpec{Ownership: []string{"orders.*"}},
			wantErr: "not covered by this application's ownership",
		},
		{
			name: "schema escaping the application directory",
			files: map[string]string{
				"topics/a.yaml": "name: a\npartitions: 1\nreplicationFactor: 1\nschema:\n  subject: s\n  file: ../../secret.avsc\n",
			},
			spec:    ownAll(),
			wantErr: "outside the application directory",
		},
		{
			name: "schema with no subject",
			files: map[string]string{
				"topics/a.yaml": "name: a\npartitions: 1\nreplicationFactor: 1\nschema:\n  file: a.avsc\n",
			},
			spec:    ownAll(),
			wantErr: "no subject",
		},
		{
			name: "unknown schema type",
			files: map[string]string{
				"topics/a.yaml": "name: a\npartitions: 1\nreplicationFactor: 1\nschema:\n  subject: s\n  file: a.avsc\n  type: thrift\n",
			},
			spec:    ownAll(),
			wantErr: "unknown schema type",
		},
		{
			name:    "malformed yaml",
			files:   map[string]string{"topics/a.yaml": "name: [unclosed\n"},
			spec:    ownAll(),
			wantErr: "parsing",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := LoadTopics(writeApp(t, tc.files), tc.spec)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestMatches(t *testing.T) {
	t.Parallel()

	assert.True(t, Matches("orders.created", []string{"orders.*"}))
	assert.True(t, Matches("anything", []string{"nope", "*"}))
	assert.False(t, Matches("payments.made", []string{"orders.*"}))
	// No patterns means no ownership, not total ownership.
	assert.False(t, Matches("orders", nil))
}
