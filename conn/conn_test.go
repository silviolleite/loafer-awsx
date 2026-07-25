package conn_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/conn"
	"github.com/silviolleite/loafer-awsx/errors"
)

func TestNewMissingRegionReturnsErrEmptyRegion(t *testing.T) {
	tests := []struct {
		name string
		opts []conn.Option
	}{
		{name: "no options"},
		{name: "empty region", opts: []conn.Option{conn.WithRegion("")}},
		{name: "credentials without region", opts: []conn.Option{conn.WithAccessKey("id", "secret")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := conn.New(context.Background(), tt.opts...)

			require.Error(t, err)
			assert.ErrorIs(t, err, errors.ErrEmptyRegion)
			assert.Empty(t, cfg.Region)
		})
	}
}

func TestNewDefaultRetryCountIsTen(t *testing.T) {
	cfg, err := conn.New(context.Background(), conn.WithRegion("us-east-1"))

	require.NoError(t, err)
	require.NotNil(t, cfg.Retryer)
	assert.Equal(t, 10, cfg.Retryer().MaxAttempts())
}

func TestNewSetsRegion(t *testing.T) {
	cfg, err := conn.New(context.Background(), conn.WithRegion("eu-west-1"))

	require.NoError(t, err)
	assert.Equal(t, "eu-west-1", cfg.Region)
}

func TestNewRetryCountOverride(t *testing.T) {
	cfg, err := conn.New(
		context.Background(),
		conn.WithRegion("us-east-1"),
		conn.WithRetryCount(3),
	)

	require.NoError(t, err)
	require.NotNil(t, cfg.Retryer)
	assert.Equal(t, 3, cfg.Retryer().MaxAttempts())
}

func TestNewEndpointOverride(t *testing.T) {
	const endpoint = "http://localhost:4566"

	cfg, err := conn.New(
		context.Background(),
		conn.WithRegion("us-east-1"),
		conn.WithEndpoint(endpoint),
	)

	require.NoError(t, err)
	require.NotNil(t, cfg.BaseEndpoint)
	assert.Equal(t, endpoint, *cfg.BaseEndpoint)
}

func TestNewNoEndpointLeavesBaseEndpointNil(t *testing.T) {
	cfg, err := conn.New(context.Background(), conn.WithRegion("us-east-1"))

	require.NoError(t, err)
	assert.Nil(t, cfg.BaseEndpoint)
}

func TestNewStaticCredentials(t *testing.T) {
	cfg, err := conn.New(
		context.Background(),
		conn.WithRegion("us-east-1"),
		conn.WithAccessKey("AKIA", "secret"),
	)

	require.NoError(t, err)

	creds, err := cfg.Credentials.Retrieve(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "AKIA", creds.AccessKeyID)
	assert.Equal(t, "secret", creds.SecretAccessKey)
	assert.Empty(t, creds.SessionToken)
}

func TestNewSessionTokenIsApplied(t *testing.T) {
	cfg, err := conn.New(
		context.Background(),
		conn.WithRegion("us-east-1"),
		conn.WithAccessKey("AKIA", "secret"),
		conn.WithSessionToken("token"),
	)

	require.NoError(t, err)

	creds, err := cfg.Credentials.Retrieve(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "token", creds.SessionToken)
}

func TestNewStaticCredentialsTakePrecedenceOverProfile(t *testing.T) {
	cfg, err := conn.New(
		context.Background(),
		conn.WithRegion("us-east-1"),
		conn.WithProfile("nonexistent-profile"),
		conn.WithAccessKey("AKIA", "secret"),
		conn.WithSessionToken("token"),
	)

	require.NoError(t, err)

	creds, err := cfg.Credentials.Retrieve(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "AKIA", creds.AccessKeyID)
	assert.Equal(t, "secret", creds.SecretAccessKey)
	assert.Equal(t, "token", creds.SessionToken)
}

func TestNewProfileIsApplied(t *testing.T) {
	dir := t.TempDir()
	credsFile := filepath.Join(dir, "credentials")
	require.NoError(t, os.WriteFile(
		credsFile,
		[]byte("[custom]\naws_access_key_id = PROFILEKEY\naws_secret_access_key = profilesecret\n"),
		0o600,
	))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", credsFile)
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "config"))

	cfg, err := conn.New(
		context.Background(),
		conn.WithRegion("us-east-1"),
		conn.WithProfile("custom"),
	)

	require.NoError(t, err)

	creds, err := cfg.Credentials.Retrieve(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "PROFILEKEY", creds.AccessKeyID)
	assert.Equal(t, "profilesecret", creds.SecretAccessKey)
}

func TestNewOptionsCompose(t *testing.T) {
	const endpoint = "http://localhost:4566"

	cfg, err := conn.New(
		context.Background(),
		conn.WithRegion("sa-east-1"),
		conn.WithAccessKey("AKIA", "secret"),
		conn.WithSessionToken("token"),
		conn.WithEndpoint(endpoint),
		conn.WithRetryCount(7),
	)

	require.NoError(t, err)
	assert.Equal(t, "sa-east-1", cfg.Region)
	require.NotNil(t, cfg.BaseEndpoint)
	assert.Equal(t, endpoint, *cfg.BaseEndpoint)
	require.NotNil(t, cfg.Retryer)
	assert.Equal(t, 7, cfg.Retryer().MaxAttempts())

	creds, err := cfg.Credentials.Retrieve(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "AKIA", creds.AccessKeyID)
	assert.Equal(t, "token", creds.SessionToken)
}

func TestNewIgnoresNilOptions(t *testing.T) {
	cfg, err := conn.New(
		context.Background(),
		nil,
		conn.WithRegion("us-east-1"),
		nil,
	)

	require.NoError(t, err)
	assert.Equal(t, "us-east-1", cfg.Region)
}

func TestNewRetryCountMatchesConfiguredValue(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.UintRange(1, 100).Draw(rt, "retryCount")

		cfg, err := conn.New(
			context.Background(),
			conn.WithRegion("us-east-1"),
			conn.WithRetryCount(n),
		)

		require.NoError(rt, err)
		require.NotNil(rt, cfg.Retryer)
		assert.Equal(rt, int(n), cfg.Retryer().MaxAttempts())
	})
}

func TestNewRegionIsPreserved(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		region := rapid.StringMatching(`[a-z]{2}-[a-z]{4,9}-[1-9]`).Draw(rt, "region")

		cfg, err := conn.New(context.Background(), conn.WithRegion(region))

		require.NoError(rt, err)
		assert.Equal(rt, region, cfg.Region)
	})
}
