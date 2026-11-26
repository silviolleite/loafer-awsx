package router_test

import (
	"context"
	"math"
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

func TestWithDeleteBatch(t *testing.T) {
	tests := []struct {
		name string
		opts []router.Option
		want bool
	}{
		{name: "default disabled", opts: nil, want: false},
		{name: "option enables", opts: []router.Option{router.WithDeleteBatch()}, want: true},
		{name: "with scheduled retry", opts: []router.Option{router.WithDeleteBatch(), validScheduledRetry()}, want: true},
		{name: "scheduled retry first", opts: []router.Option{validScheduledRetry(), router.WithDeleteBatch()}, want: true},
		{name: "with dlq", opts: []router.Option{router.WithDeleteBatch(), router.WithDLQ(3)}, want: true},
		{name: "with per group id", opts: []router.Option{router.WithRunMode(router.PerGroupID), router.WithDeleteBatch()}, want: true},
		{name: "with parallel", opts: []router.Option{router.WithRunMode(router.Parallel), router.WithDeleteBatch()}, want: true},
		{
			name: "with visibility retry model",
			opts: []router.Option{router.WithRetryModel(router.VisibilityRetryModel), router.WithDeleteBatch()},
			want: true,
		},
		{
			name: "with per group id and scheduled retry",
			opts: []router.Option{router.WithRunMode(router.PerGroupID), validScheduledRetry(), router.WithDeleteBatch()},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := router.New("orders", noopHandler, tt.opts...)

			require.NoError(t, err)
			assert.Equal(t, tt.want, r.DeleteBatch())
		})
	}
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

// Feature: consumer-flow-control, Property 1: For any integer n, router.New with WithPrefetch(n) succeeds iff n >= 0 and reports Prefetch() == n, and with WithPollers(n) succeeds iff n >= 1 and reports Pollers() == n; failures match errors.ErrInvalidOption; without the options Prefetch() == MaxMessages and Pollers() == 1.
func TestNewFlowControlOptionsProperty(t *testing.T) {
	drawN := func(rt *rapid.T) int {
		return rapid.OneOf(rapid.Int(), rapid.SampledFrom([]int{-1, 0, 1})).Draw(rt, "n")
	}

	t.Run("prefetch", func(t *testing.T) {
		rapid.Check(t, func(rt *rapid.T) {
			n := drawN(rt)

			r, err := router.New("orders", noopHandler, router.WithPrefetch(n))

			if n < 0 {
				require.Error(rt, err)
				assert.ErrorIs(rt, err, errors.ErrInvalidOption)
				assert.Nil(rt, r)
				return
			}
			require.NoError(rt, err)
			assert.Equal(rt, n, r.Prefetch())
			assert.Equal(rt, 1, r.Pollers())
		})
	})

	t.Run("pollers", func(t *testing.T) {
		rapid.Check(t, func(rt *rapid.T) {
			n := drawN(rt)

			r, err := router.New("orders", noopHandler, router.WithPollers(n))

			if n < 1 {
				require.Error(rt, err)
				assert.ErrorIs(rt, err, errors.ErrInvalidOption)
				assert.Nil(rt, r)
				return
			}
			require.NoError(rt, err)
			assert.Equal(rt, n, r.Pollers())
			assert.Equal(rt, 10, r.Prefetch())
		})
	})

	t.Run("prefetch defaults to max messages", func(t *testing.T) {
		rapid.Check(t, func(rt *rapid.T) {
			maxMessages := rapid.Int32Range(1, 10).Draw(rt, "maxMessages")
			workers := rapid.IntRange(1, 1000).Draw(rt, "workers")
			maxMessagesLast := rapid.Bool().Draw(rt, "maxMessagesLast")
			opts := []router.Option{router.WithWorkerPoolSize(workers), router.WithRunMode(router.PerGroupID)}
			if maxMessagesLast {
				opts = append(opts, router.WithMaxMessages(maxMessages))
			} else {
				opts = append([]router.Option{router.WithMaxMessages(maxMessages)}, opts...)
			}

			r, err := router.New("orders", noopHandler, opts...)

			require.NoError(rt, err)
			assert.Equal(rt, int(maxMessages), r.Prefetch())
			assert.Equal(rt, maxMessages, r.MaxMessages())
			assert.Equal(rt, 1, r.Pollers())
		})
	})

	t.Run("explicit prefetch is independent of max messages", func(t *testing.T) {
		rapid.Check(t, func(rt *rapid.T) {
			prefetch := rapid.IntRange(0, 1000).Draw(rt, "prefetch")
			pollers := rapid.IntRange(1, 64).Draw(rt, "pollers")
			maxMessages := rapid.Int32Range(1, 10).Draw(rt, "maxMessages")
			opts := []router.Option{router.WithPrefetch(prefetch), router.WithPollers(pollers), router.WithMaxMessages(maxMessages)}
			if rapid.Bool().Draw(rt, "maxMessagesFirst") {
				opts = []router.Option{router.WithMaxMessages(maxMessages), router.WithPollers(pollers), router.WithPrefetch(prefetch)}
			}

			r, err := router.New("orders", noopHandler, opts...)

			require.NoError(rt, err)
			assert.Equal(rt, prefetch, r.Prefetch())
			assert.Equal(rt, pollers, r.Pollers())
			assert.Equal(rt, maxMessages, r.MaxMessages())
		})
	})
}

func TestFlowControlDefaults(t *testing.T) {
	tests := []struct {
		name         string
		opts         []router.Option
		wantPrefetch int
		wantPollers  int
	}{
		{name: "no options", wantPrefetch: 10, wantPollers: 1},
		{name: "max messages one", opts: []router.Option{router.WithMaxMessages(1)}, wantPrefetch: 1, wantPollers: 1},
		{name: "prefetch zero", opts: []router.Option{router.WithPrefetch(0)}, wantPrefetch: 0, wantPollers: 1},
		{
			name:         "prefetch zero before max messages",
			opts:         []router.Option{router.WithPrefetch(0), router.WithMaxMessages(3)},
			wantPrefetch: 0,
			wantPollers:  1,
		},
		{
			name:         "last prefetch wins",
			opts:         []router.Option{router.WithPrefetch(7), router.WithPrefetch(2)},
			wantPrefetch: 2,
			wantPollers:  1,
		},
		{
			name:         "last pollers wins",
			opts:         []router.Option{router.WithPollers(7), router.WithPollers(2)},
			wantPrefetch: 10,
			wantPollers:  2,
		},
		{name: "pollers one", opts: []router.Option{router.WithPollers(1)}, wantPrefetch: 10, wantPollers: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := router.New("orders", noopHandler, tt.opts...)

			require.NoError(t, err)
			assert.Equal(t, tt.wantPrefetch, r.Prefetch())
			assert.Equal(t, tt.wantPollers, r.Pollers())
		})
	}
}

func TestFlowControlOptionValidationErrors(t *testing.T) {
	tests := []struct {
		opt  router.Option
		name string
	}{
		{name: "prefetch negative one", opt: router.WithPrefetch(-1)},
		{name: "prefetch min int", opt: router.WithPrefetch(math.MinInt)},
		{name: "pollers zero", opt: router.WithPollers(0)},
		{name: "pollers negative one", opt: router.WithPollers(-1)},
		{name: "pollers min int", opt: router.WithPollers(math.MinInt)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := router.New("orders", noopHandler, router.WithPrefetch(3), router.WithPollers(2), tt.opt)

			require.Error(t, err)
			assert.ErrorIs(t, err, errors.ErrInvalidOption)
			assert.Nil(t, r)
		})
	}
}

func TestFlowControlOptionsCombine(t *testing.T) {
	tests := []struct {
		name            string
		opts            []router.Option
		wantMode        router.Mode
		wantRetryModel  router.RetryModel
		wantDeleteBatch bool
		wantDLQ         bool
	}{
		{name: "parallel", opts: []router.Option{router.WithRunMode(router.Parallel)}, wantMode: router.Parallel},
		{name: "per group id", opts: []router.Option{router.WithRunMode(router.PerGroupID)}, wantMode: router.PerGroupID},
		{
			name:           "visibility retry model",
			opts:           []router.Option{router.WithRetryModel(router.VisibilityRetryModel)},
			wantRetryModel: router.VisibilityRetryModel,
		},
		{name: "scheduled retry", opts: []router.Option{validScheduledRetry()}, wantRetryModel: router.ScheduledRetryModel},
		{name: "delete batch", opts: []router.Option{router.WithDeleteBatch()}, wantDeleteBatch: true},
		{name: "dlq", opts: []router.Option{router.WithDLQ(3)}, wantDLQ: true},
		{
			name:            "per group id with scheduled retry and delete batch",
			opts:            []router.Option{router.WithRunMode(router.PerGroupID), validScheduledRetry(), router.WithDeleteBatch()},
			wantMode:        router.PerGroupID,
			wantRetryModel:  router.ScheduledRetryModel,
			wantDeleteBatch: true,
		},
		{
			name:            "per group id with dlq and delete batch",
			opts:            []router.Option{router.WithRunMode(router.PerGroupID), router.WithDLQ(5), router.WithDeleteBatch()},
			wantMode:        router.PerGroupID,
			wantDeleteBatch: true,
			wantDLQ:         true,
		},
	}

	placements := []struct {
		name  string
		build func(flow, other []router.Option) []router.Option
	}{
		{name: "flow control first", build: func(flow, other []router.Option) []router.Option { return append(flow, other...) }},
		{name: "flow control last", build: func(flow, other []router.Option) []router.Option { return append(other, flow...) }},
	}

	for _, tt := range tests {
		for _, p := range placements {
			t.Run(tt.name+"/"+p.name, func(t *testing.T) {
				flow := []router.Option{router.WithPrefetch(0), router.WithPollers(4)}
				other := append([]router.Option(nil), tt.opts...)

				r, err := router.New("orders.fifo", noopHandler, p.build(flow, other)...)

				require.NoError(t, err)
				assert.Equal(t, 0, r.Prefetch())
				assert.Equal(t, 4, r.Pollers())
				assert.Equal(t, tt.wantMode, r.RunMode())
				assert.Equal(t, tt.wantRetryModel, r.RetryModel())
				assert.Equal(t, tt.wantDeleteBatch, r.DeleteBatch())
				assert.Equal(t, tt.wantDLQ, r.DLQ() != nil)
			})
		}
	}
}
