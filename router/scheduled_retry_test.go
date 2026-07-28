package router_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/silviolleite/loafer-awsx/errors"
	"github.com/silviolleite/loafer-awsx/router"
)

func validScheduledRetry(extra ...router.ScheduledRetryOption) router.Option {
	opts := []router.ScheduledRetryOption{
		router.WithSchedulerIdentity("arn:aws:sqs:us-east-1:123456789012:orders.fifo", "arn:aws:iam::123456789012:role/scheduler"),
		router.WithScheduledDLQ("https://sqs.us-east-1.amazonaws.com/123456789012/orders-dlq"),
		router.WithMaxRetryCount(5),
		router.WithBackoff(1000*time.Millisecond, 2000*time.Millisecond),
	}
	opts = append(opts, extra...)
	return router.WithScheduledRetry(opts...)
}

func TestRetryModelDefault(t *testing.T) {
	r, err := router.New("orders", noopHandler)

	require.NoError(t, err)
	assert.Equal(t, router.VisibilityRetryModel, r.RetryModel())
	assert.Nil(t, r.ScheduledRetry())
}

func TestWithRetryModelExplicitVisibility(t *testing.T) {
	r, err := router.New("orders", noopHandler, router.WithRetryModel(router.VisibilityRetryModel))

	require.NoError(t, err)
	assert.Equal(t, router.VisibilityRetryModel, r.RetryModel())
}

func TestWithRetryModelRejectsInvalidValue(t *testing.T) {
	tests := []struct {
		name  string
		model router.RetryModel
	}{
		{name: "negative", model: router.RetryModel(-1)},
		{name: "above range", model: router.RetryModel(99)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := router.New("orders", noopHandler, router.WithRetryModel(tt.model))

			require.Error(t, err)
			assert.ErrorIs(t, err, errors.ErrInvalidOption)
			assert.ErrorIs(t, err, errors.ErrScheduledRetryConfig)
			assert.Nil(t, r)
		})
	}
}

func TestWithScheduledRetryValidationErrors(t *testing.T) {
	tests := []struct {
		opt  router.Option
		name string
	}{
		{
			name: "missing scheduler identity",
			opt: router.WithScheduledRetry(
				router.WithScheduledDLQ("https://sqs.us-east-1.amazonaws.com/123456789012/orders-dlq"),
				router.WithMaxRetryCount(5),
				router.WithBackoff(1000*time.Millisecond, 2000*time.Millisecond),
			),
		},
		{
			name: "missing target queue arn",
			opt: router.WithScheduledRetry(
				router.WithSchedulerIdentity("", "arn:aws:iam::123456789012:role/scheduler"),
				router.WithScheduledDLQ("https://sqs.us-east-1.amazonaws.com/123456789012/orders-dlq"),
				router.WithMaxRetryCount(5),
				router.WithBackoff(1000*time.Millisecond, 2000*time.Millisecond),
			),
		},
		{
			name: "missing execution role arn",
			opt: router.WithScheduledRetry(
				router.WithSchedulerIdentity("arn:aws:sqs:us-east-1:123456789012:orders.fifo", ""),
				router.WithScheduledDLQ("https://sqs.us-east-1.amazonaws.com/123456789012/orders-dlq"),
				router.WithMaxRetryCount(5),
				router.WithBackoff(1000*time.Millisecond, 2000*time.Millisecond),
			),
		},
		{
			name: "missing dlq destination",
			opt: router.WithScheduledRetry(
				router.WithSchedulerIdentity("arn:aws:sqs:us-east-1:123456789012:orders.fifo", "arn:aws:iam::123456789012:role/scheduler"),
				router.WithMaxRetryCount(5),
				router.WithBackoff(1000*time.Millisecond, 2000*time.Millisecond),
			),
		},
		{
			name: "max retry count negative",
			opt:  validScheduledRetry(router.WithMaxRetryCount(-1)),
		},
		{
			name: "base backoff below minimum",
			opt:  validScheduledRetry(router.WithBackoff(500*time.Microsecond, 2000*time.Millisecond)),
		},
		{
			name: "max backoff above guard",
			opt:  validScheduledRetry(router.WithBackoff(1000*time.Millisecond, 25*time.Hour)),
		},
		{
			name: "max backoff below base",
			opt:  validScheduledRetry(router.WithBackoff(2000*time.Millisecond, 1000*time.Millisecond)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := router.New("orders", noopHandler, tt.opt)

			require.Error(t, err)
			assert.ErrorIs(t, err, errors.ErrInvalidOption)
			assert.ErrorIs(t, err, errors.ErrScheduledRetryConfig)
			assert.Nil(t, r)
		})
	}
}

func TestWithScheduledRetryConflictsWithDLQ(t *testing.T) {
	tests := []struct {
		name string
		opts []router.Option
	}{
		{
			name: "scheduled retry then dlq",
			opts: []router.Option{validScheduledRetry(), router.WithDLQ(3)},
		},
		{
			name: "dlq then scheduled retry",
			opts: []router.Option{router.WithDLQ(3), validScheduledRetry()},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := router.New("orders", noopHandler, tt.opts...)

			require.Error(t, err)
			assert.ErrorIs(t, err, errors.ErrInvalidOption)
			assert.ErrorIs(t, err, errors.ErrScheduledRetryConfig)
			assert.Nil(t, r)
		})
	}
}

func TestWithScheduledRetryBaseBackoffDefault(t *testing.T) {
	r, err := router.New(
		"orders",
		noopHandler,
		validScheduledRetry(router.WithBackoff(0, 2000*time.Millisecond)),
	)

	require.NoError(t, err)
	require.NotNil(t, r.ScheduledRetry())
	assert.Equal(t, 1000*time.Millisecond, r.ScheduledRetry().BaseBackoff)
}

func TestWithScheduledRetryValidBuild(t *testing.T) {
	r, err := router.New(
		"orders",
		noopHandler,
		router.WithScheduledRetry(
			router.WithSchedulerIdentity("arn:aws:sqs:us-east-1:123456789012:orders.fifo", "arn:aws:iam::123456789012:role/scheduler"),
			router.WithScheduledDLQ("https://sqs.us-east-1.amazonaws.com/123456789012/orders-dlq"),
			router.WithMaxRetryCount(7),
			router.WithBackoff(500*time.Millisecond, 30*time.Second),
		),
	)

	require.NoError(t, err)
	assert.Equal(t, router.ScheduledRetryModel, r.RetryModel())

	cfg := r.ScheduledRetry()
	require.NotNil(t, cfg)
	assert.Equal(t, "arn:aws:sqs:us-east-1:123456789012:orders.fifo", cfg.TargetQueueARN)
	assert.Equal(t, "arn:aws:iam::123456789012:role/scheduler", cfg.ExecutionRoleARN)
	assert.Equal(t, "https://sqs.us-east-1.amazonaws.com/123456789012/orders-dlq", cfg.DLQQueueURL)
	assert.Equal(t, 7, cfg.MaxRetryCount)
	assert.Equal(t, 500*time.Millisecond, cfg.BaseBackoff)
	assert.Equal(t, 30*time.Second, cfg.MaxBackoff)
}
