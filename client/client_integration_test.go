//go:build integration

package client_test

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/silviolleite/loafer-awsx/client"
	"github.com/silviolleite/loafer-awsx/conn"
	liberrors "github.com/silviolleite/loafer-awsx/errors"
)

const (
	integrationRegion      = "us-east-1"
	integrationAccessKey   = "test"
	integrationSecretKey   = "test"
	integrationUnreachable = "http://127.0.0.1:1"
	integrationPingTimeout = 2 * time.Second
)

func integrationEndpoint() string {
	if v := os.Getenv("MINISTACK_ENDPOINT"); v != "" {
		return v
	}
	return "http://localhost:4566"
}

func integrationConfig(t *testing.T, endpoint string) aws.Config {
	t.Helper()

	cfg, err := conn.New(context.Background(),
		conn.WithRegion(integrationRegion),
		conn.WithAccessKey(integrationAccessKey, integrationSecretKey),
		conn.WithEndpoint(endpoint),
		conn.WithRetryCount(1),
	)
	require.NoError(t, err)

	transport := awshttp.NewBuildableClient().GetTransport()
	t.Cleanup(transport.CloseIdleConnections)
	cfg.HTTPClient = &http.Client{Transport: transport}

	return cfg
}

type integrationConstructor func(context.Context, aws.Config, ...client.Option) (any, error)

func integrationConstructors() map[string]integrationConstructor {
	return map[string]integrationConstructor{
		"sqs": func(ctx context.Context, cfg aws.Config, opts ...client.Option) (any, error) {
			return client.NewSQS(ctx, cfg, opts...)
		},
		"sns": func(ctx context.Context, cfg aws.Config, opts ...client.Option) (any, error) {
			return client.NewSNS(ctx, cfg, opts...)
		},
		"scheduler": func(ctx context.Context, cfg aws.Config, opts ...client.Option) (any, error) {
			return client.NewScheduler(ctx, cfg, opts...)
		},
	}
}

func TestIntegrationConstructorsPassConnectivityCheck(t *testing.T) {
	cases := []struct {
		name string
		opts []client.Option
	}{
		{name: "default options"},
		{name: "zero retry limit", opts: []client.Option{client.WithPingRetryLimit(0)}},
	}

	for _, tc := range cases {
		for name, newClient := range integrationConstructors() {
			t.Run(tc.name+"/"+name, func(t *testing.T) {
				cfg := integrationConfig(t, integrationEndpoint())

				got, err := newClient(context.Background(), cfg, tc.opts...)

				require.NoError(t, err)
				assert.NotNil(t, got)
			})
		}
	}
}

func TestIntegrationConstructorsFailConnectivityCheck(t *testing.T) {
	for name, newClient := range integrationConstructors() {
		t.Run(name, func(t *testing.T) {
			cfg := integrationConfig(t, integrationUnreachable)

			got, err := newClient(context.Background(), cfg,
				client.WithPingTimeout(integrationPingTimeout),
				client.WithPingRetryLimit(1),
			)

			require.ErrorIs(t, err, liberrors.ErrPingFailed)
			assert.Nil(t, got)
		})
	}
}
