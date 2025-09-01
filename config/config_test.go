package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeployTargetValidate(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		name    string
		cfg     DeployTargetConfig
		wantErr string
	}{
		{
			name: "minimal valid target",
			cfg:  DeployTargetConfig{BootstrapServers: []string{"localhost:9092"}},
		},
		{
			name:    "no brokers",
			cfg:     DeployTargetConfig{},
			wantErr: "bootstrapServers must not be empty",
		},
		{
			name: "unknown sasl mechanism",
			cfg: DeployTargetConfig{
				BootstrapServers: []string{"localhost:9092"},
				SASL:             SASLConfig{Mechanism: "KERBEROS", Username: "u", Password: "p"},
			},
			wantErr: "unknown sasl.mechanism",
		},
		{
			name: "sasl without a username",
			cfg: DeployTargetConfig{
				BootstrapServers: []string{"localhost:9092"},
				SASL:             SASLConfig{Mechanism: "PLAIN", Password: "p"},
			},
			wantErr: "sasl.username must be set",
		},
		{
			name: "sasl without a secret",
			cfg: DeployTargetConfig{
				BootstrapServers: []string{"localhost:9092"},
				SASL:             SASLConfig{Mechanism: "PLAIN", Username: "u"},
			},
			wantErr: "either sasl.password or sasl.passwordFile",
		},
		{
			name: "both secret forms at once",
			cfg: DeployTargetConfig{
				BootstrapServers: []string{"localhost:9092"},
				SASL:             SASLConfig{Mechanism: "PLAIN", Username: "u", Password: "p", PasswordFile: "/f"},
			},
			wantErr: "mutually exclusive",
		},
		{
			name: "registry without a url",
			cfg: DeployTargetConfig{
				BootstrapServers: []string{"localhost:9092"},
				SchemaRegistry:   &SchemaRegistryConfig{},
			},
			wantErr: "schemaRegistry.url must not be empty",
		},
		{
			name: "unauthenticated registry is fine",
			cfg: DeployTargetConfig{
				BootstrapServers: []string{"localhost:9092"},
				SchemaRegistry:   &SchemaRegistryConfig{URL: "http://localhost:8081"},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.cfg.Validate()
			if tc.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestResolvePassword(t *testing.T) {
	t.Parallel()

	t.Run("inline", func(t *testing.T) {
		t.Parallel()

		got, err := (&SASLConfig{Password: "inline"}).ResolvePassword()
		require.NoError(t, err)
		assert.Equal(t, "inline", got)
	})

	t.Run("from a file, trailing newline trimmed", func(t *testing.T) {
		t.Parallel()

		p := filepath.Join(t.TempDir(), "pw")
		require.NoError(t, os.WriteFile(p, []byte("secret\n"), 0o600))

		got, err := (&SASLConfig{PasswordFile: p}).ResolvePassword()
		require.NoError(t, err)
		assert.Equal(t, "secret", got)
	})

	t.Run("missing file", func(t *testing.T) {
		t.Parallel()

		_, err := (&SASLConfig{PasswordFile: filepath.Join(t.TempDir(), "absent")}).ResolvePassword()
		assert.Error(t, err)
	})

	t.Run("empty file", func(t *testing.T) {
		t.Parallel()

		p := filepath.Join(t.TempDir(), "pw")
		require.NoError(t, os.WriteFile(p, []byte("  \n"), 0o600))

		_, err := (&SASLConfig{PasswordFile: p}).ResolvePassword()
		assert.Error(t, err)
	})

	t.Run("an unauthenticated registry resolves to nothing", func(t *testing.T) {
		t.Parallel()

		got, err := (&SchemaRegistryConfig{URL: "http://localhost:8081"}).ResolvePassword()
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}

func TestTopicsDirOrDefault(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "topics", (&ApplicationConfigSpec{}).TopicsDirOrDefault())
	assert.Equal(t, "kafka", (&ApplicationConfigSpec{TopicsDir: "kafka"}).TopicsDirOrDefault())
}

func TestSASLEnabled(t *testing.T) {
	t.Parallel()

	assert.False(t, (&SASLConfig{}).Enabled())
	assert.True(t, (&SASLConfig{Mechanism: "PLAIN"}).Enabled())
}
