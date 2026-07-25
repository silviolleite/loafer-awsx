package middleware_test

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/middleware"
)

func recorder(label string, trace *[]string) middleware.Middleware {
	return func(next middleware.Handler) middleware.Handler {
		return func(ctx context.Context, msg middleware.Message) error {
			*trace = append(*trace, label)
			return next(ctx, msg)
		}
	}
}

func TestChainExecutionOrder(t *testing.T) {
	tests := []struct {
		name   string
		labels []string
		want   []string
	}{
		{
			name:   "empty chain runs only the handler",
			labels: nil,
			want:   []string{"handler"},
		},
		{
			name:   "single middleware wraps the handler",
			labels: []string{"A"},
			want:   []string{"A", "handler"},
		},
		{
			name:   "three middlewares run outermost first",
			labels: []string{"A", "B", "C"},
			want:   []string{"A", "B", "C", "handler"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var trace []string

			mws := make([]middleware.Middleware, 0, len(tt.labels))
			for _, label := range tt.labels {
				mws = append(mws, recorder(label, &trace))
			}

			handler := func(ctx context.Context, msg middleware.Message) error {
				trace = append(trace, "handler")
				return nil
			}

			wrapped := middleware.Chain(mws...)(handler)

			require.NoError(t, wrapped(context.Background(), nil))
			assert.Equal(t, tt.want, trace)
		})
	}
}

func TestChainEmptyReturnsHandlerUnchanged(t *testing.T) {
	handler := func(ctx context.Context, msg middleware.Message) error {
		return nil
	}

	wrapped := middleware.Chain()(handler)

	assert.Equal(t,
		reflect.ValueOf(handler).Pointer(),
		reflect.ValueOf(wrapped).Pointer(),
	)
}

func TestChainPropagatesHandlerError(t *testing.T) {
	sentinel := errors.New("boom")

	handler := func(ctx context.Context, msg middleware.Message) error {
		return sentinel
	}

	var trace []string
	wrapped := middleware.Chain(recorder("A", &trace))(handler)

	err := wrapped(context.Background(), nil)

	assert.ErrorIs(t, err, sentinel)
	assert.Equal(t, []string{"A"}, trace)
}

func TestChainShortCircuitsOnMiddlewareError(t *testing.T) {
	sentinel := errors.New("blocked")

	blocker := func(next middleware.Handler) middleware.Handler {
		return func(ctx context.Context, msg middleware.Message) error {
			return sentinel
		}
	}

	handlerCalled := false
	handler := func(ctx context.Context, msg middleware.Message) error {
		handlerCalled = true
		return nil
	}

	wrapped := middleware.Chain(blocker)(handler)

	err := wrapped(context.Background(), nil)

	assert.ErrorIs(t, err, sentinel)
	assert.False(t, handlerCalled)
}

func TestChainCompositionOrderProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(0, 12).Draw(rt, "n")

		var trace []string
		labels := make([]string, n)
		mws := make([]middleware.Middleware, n)
		for i := range n {
			labels[i] = strconv.Itoa(i)
			mws[i] = recorder(labels[i], &trace)
		}

		handler := func(ctx context.Context, msg middleware.Message) error {
			trace = append(trace, "handler")
			return nil
		}

		wrapped := middleware.Chain(mws...)(handler)
		require.NoError(rt, wrapped(context.Background(), nil))

		want := append(append([]string{}, labels...), "handler")
		assert.Equal(rt, want, trace)
	})
}
