package router_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/errors"
	"github.com/silviolleite/loafer-awsx/middleware"
	"github.com/silviolleite/loafer-awsx/router"
)

func noopHandler(ctx context.Context, msg middleware.Message) error {
	return nil
}

func TestNewValidationErrors(t *testing.T) {
	tests := []struct {
		wantErr   error
		handler   middleware.Handler
		name      string
		queueName string
	}{
		{name: "empty queue name", queueName: "", handler: noopHandler, wantErr: errors.ErrEmptyQueueName},
		{name: "nil handler", queueName: "queue", handler: nil, wantErr: errors.ErrNoHandler},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := router.New(tt.queueName, tt.handler)

			require.Error(t, err)
			assert.ErrorIs(t, err, tt.wantErr)
			assert.Nil(t, r)
		})
	}
}

func TestNewDefaults(t *testing.T) {
	r, err := router.New("orders", noopHandler)

	require.NoError(t, err)
	assert.Equal(t, "orders", r.QueueName())
	assert.NotNil(t, r.Handler())
	assert.Nil(t, r.Middlewares())
	assert.Equal(t, 5, r.WorkerPoolSize())
	assert.Equal(t, int32(10), r.MaxMessages())
	assert.Equal(t, int32(10), r.WaitTimeSeconds())
	assert.Equal(t, int32(30), r.VisibilityTimeout())
	assert.Equal(t, 2, r.ExtensionLimit())
	assert.Equal(t, router.Parallel, r.RunMode())
	assert.Nil(t, r.CustomGroupFields())
	assert.Nil(t, r.DLQ())
}

func TestNewIgnoresNilOptions(t *testing.T) {
	r, err := router.New("orders", noopHandler, nil, router.WithWorkerPoolSize(3), nil)

	require.NoError(t, err)
	assert.Equal(t, 3, r.WorkerPoolSize())
}

func TestNewOptionsCompose(t *testing.T) {
	called := false
	mw := func(next middleware.Handler) middleware.Handler {
		return func(ctx context.Context, msg middleware.Message) error {
			return next(ctx, msg)
		}
	}
	onDLQ := func(ctx context.Context, msg middleware.Message) { called = true }

	r, err := router.New(
		"orders.fifo",
		noopHandler,
		router.WithWorkerPoolSize(8),
		router.WithMaxMessages(5),
		router.WithWaitTimeSeconds(20),
		router.WithVisibilityTimeout(45),
		router.WithExtensionLimit(4),
		router.WithRunMode(router.PerGroupID),
		router.WithCustomGroupFields("tenant", "region"),
		router.WithMiddleware(mw),
		router.WithDLQ(3, router.WithOnDLQ(onDLQ)),
	)

	require.NoError(t, err)
	assert.Equal(t, 8, r.WorkerPoolSize())
	assert.Equal(t, int32(5), r.MaxMessages())
	assert.Equal(t, int32(20), r.WaitTimeSeconds())
	assert.Equal(t, int32(45), r.VisibilityTimeout())
	assert.Equal(t, 4, r.ExtensionLimit())
	assert.Equal(t, router.PerGroupID, r.RunMode())
	assert.Equal(t, []string{"tenant", "region"}, r.CustomGroupFields())
	assert.Len(t, r.Middlewares(), 1)

	dlq := r.DLQ()
	require.NotNil(t, dlq)
	assert.Equal(t, 3, dlq.MaxReceiveCount)
	require.NotNil(t, dlq.OnDLQ)
	dlq.OnDLQ(context.Background(), nil)
	assert.True(t, called)
}

func TestWithVisibilityTimeoutClamp(t *testing.T) {
	tests := []struct {
		name  string
		input int32
		want  int32
	}{
		{name: "below minimum", input: 5, want: 11},
		{name: "at minimum", input: 11, want: 11},
		{name: "zero", input: 0, want: 11},
		{name: "negative", input: -3, want: 11},
		{name: "above minimum", input: 12, want: 12},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := router.New("orders", noopHandler, router.WithVisibilityTimeout(tt.input))

			require.NoError(t, err)
			assert.Equal(t, tt.want, r.VisibilityTimeout())
		})
	}
}

func TestOptionValidationErrors(t *testing.T) {
	tests := []struct {
		opt  router.Option
		name string
	}{
		{name: "worker pool zero", opt: router.WithWorkerPoolSize(0)},
		{name: "worker pool negative", opt: router.WithWorkerPoolSize(-1)},
		{name: "max messages zero", opt: router.WithMaxMessages(0)},
		{name: "max messages above limit", opt: router.WithMaxMessages(11)},
		{name: "wait time negative", opt: router.WithWaitTimeSeconds(-1)},
		{name: "wait time above limit", opt: router.WithWaitTimeSeconds(21)},
		{name: "extension limit negative", opt: router.WithExtensionLimit(-1)},
		{name: "unknown run mode", opt: router.WithRunMode(router.Mode(99))},
		{name: "dlq zero receive count", opt: router.WithDLQ(0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := router.New("orders", noopHandler, tt.opt)

			require.Error(t, err)
			assert.ErrorIs(t, err, errors.ErrInvalidOption)
			assert.Nil(t, r)
		})
	}
}

func TestWithExtensionLimitZeroAllowed(t *testing.T) {
	r, err := router.New("orders", noopHandler, router.WithExtensionLimit(0))

	require.NoError(t, err)
	assert.Equal(t, 0, r.ExtensionLimit())
}

func TestWithCustomGroupFieldsFiltersEmpty(t *testing.T) {
	r, err := router.New("orders", noopHandler, router.WithCustomGroupFields("a", "", "b"))

	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, r.CustomGroupFields())
}

func TestWithMiddlewareIgnoresNil(t *testing.T) {
	mw := func(next middleware.Handler) middleware.Handler { return next }

	r, err := router.New("orders", noopHandler, router.WithMiddleware(nil, mw, nil))

	require.NoError(t, err)
	assert.Len(t, r.Middlewares(), 1)
}

func TestWithDLQNilOptionIgnored(t *testing.T) {
	r, err := router.New("orders", noopHandler, router.WithDLQ(2, nil))

	require.NoError(t, err)
	require.NotNil(t, r.DLQ())
	assert.Nil(t, r.DLQ().OnDLQ)
}

func TestWithOnDLQNilKeepsCallbackUnset(t *testing.T) {
	r, err := router.New("orders", noopHandler, router.WithDLQ(2, router.WithOnDLQ(nil)))

	require.NoError(t, err)
	require.NotNil(t, r.DLQ())
	assert.Nil(t, r.DLQ().OnDLQ)
}

func TestModeString(t *testing.T) {
	tests := []struct {
		name string
		want string
		mode router.Mode
	}{
		{name: "parallel", mode: router.Parallel, want: "Parallel"},
		{name: "per group id", mode: router.PerGroupID, want: "PerGroupID"},
		{name: "unknown", mode: router.Mode(42), want: "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.mode.String())
		})
	}
}

func TestNewVisibilityTimeoutClampProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		seconds := rapid.Int32Range(-1000, 43200).Draw(rt, "seconds")

		r, err := router.New("orders", noopHandler, router.WithVisibilityTimeout(seconds))

		require.NoError(rt, err)
		assert.GreaterOrEqual(rt, r.VisibilityTimeout(), int32(11))
		if seconds > 11 {
			assert.Equal(rt, seconds, r.VisibilityTimeout())
		}
	})
}

func TestNewValidOptionsPreservedProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		workers := rapid.IntRange(1, 1000).Draw(rt, "workers")
		maxMessages := rapid.Int32Range(1, 10).Draw(rt, "maxMessages")
		waitTime := rapid.Int32Range(0, 20).Draw(rt, "waitTime")
		extensionLimit := rapid.IntRange(0, 100).Draw(rt, "extensionLimit")
		mode := router.Mode(rapid.IntRange(0, 1).Draw(rt, "mode"))

		r, err := router.New(
			"orders",
			noopHandler,
			router.WithWorkerPoolSize(workers),
			router.WithMaxMessages(maxMessages),
			router.WithWaitTimeSeconds(waitTime),
			router.WithExtensionLimit(extensionLimit),
			router.WithRunMode(mode),
		)

		require.NoError(rt, err)
		assert.Equal(rt, workers, r.WorkerPoolSize())
		assert.Equal(rt, maxMessages, r.MaxMessages())
		assert.Equal(rt, waitTime, r.WaitTimeSeconds())
		assert.Equal(rt, extensionLimit, r.ExtensionLimit())
		assert.Equal(rt, mode, r.RunMode())
	})
}
