package router

import (
	"context"

	"github.com/silviolleite/loafer-awsx/middleware"
)

// DLQConfig holds Dead Letter Queue configuration for a route. It describes
// when a message should be routed to a dead letter destination and an optional
// callback invoked at that time.
type DLQConfig struct {
	OnDLQ           func(ctx context.Context, msg middleware.Message)
	MaxReceiveCount int
}

// DLQOption configures optional DLQConfig behavior.
type DLQOption func(*DLQConfig)

// WithOnDLQ sets a callback invoked when a message is sent to the DLQ. Passing a
// nil function leaves the current callback unchanged.
func WithOnDLQ(fn func(ctx context.Context, msg middleware.Message)) DLQOption {
	return func(c *DLQConfig) {
		if fn == nil {
			return
		}
		c.OnDLQ = fn
	}
}
