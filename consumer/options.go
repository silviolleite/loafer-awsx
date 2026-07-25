package consumer

import (
	"log/slog"
	"time"

	"github.com/silviolleite/loafer-awsx/middleware"
)

// defaultRetryTimeout is the wait applied after a failed ReceiveMessage before
// the polling loop tries again. It is used when WithRetryTimeout is not set.
const defaultRetryTimeout = 5 * time.Second

// Option configures a Consumer during construction. Options are applied in
// order by New.
type Option func(*Consumer)

// DLQMetric increments the loafer_messages_dlq_total counter for the named
// route. It is the observe-only hook the consumer invokes once for every
// message detected as exhausted (handler error with ApproximateReceiveCount at
// or above the route's MaxReceiveCount).
//
// It is wired through WithDLQMetric and is expected to be backed by the counter
// registered by the Metrics middleware, so it is supplied only when metrics are
// enabled. A nil DLQMetric means metrics are disabled and the counter is left
// untouched.
type DLQMetric func(routeName string)

// WithLogger sets the structured logger used by the consumer, its dispatcher,
// and its visibility manager. A nil logger is ignored so the consumer keeps its
// no-op default and callers never have to guard against nil.
func WithLogger(log *slog.Logger) Option {
	return func(c *Consumer) {
		if log != nil {
			c.log = log
		}
	}
}

// WithRetryTimeout sets the wait applied after a failed ReceiveMessage before
// the polling loop retries. A non-positive duration is ignored so the consumer
// keeps its default of 5 seconds.
func WithRetryTimeout(d time.Duration) Option {
	return func(c *Consumer) {
		if d > 0 {
			c.retryTimeout = d
		}
	}
}

// WithGlobalMiddleware appends middlewares applied outermost to every message,
// ahead of the route-level middlewares. They run first on the way in and last
// on the way out. Multiple calls accumulate in order.
func WithGlobalMiddleware(mws ...middleware.Middleware) Option {
	return func(c *Consumer) {
		c.globalMiddlewares = append(c.globalMiddlewares, mws...)
	}
}

// WithDLQMetric sets the incrementer used to record loafer_messages_dlq_total
// when a message is observed as exhausted. It should be wired only when the
// Metrics middleware is enabled and backed by the same registered counter. A
// nil incrementer is ignored so the consumer keeps DLQ metric reporting off and
// callers never have to guard against nil.
func WithDLQMetric(inc DLQMetric) Option {
	return func(c *Consumer) {
		if inc != nil {
			c.dlqMetric = inc
		}
	}
}
