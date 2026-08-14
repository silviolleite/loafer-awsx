package consumer

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/silviolleite/loafer-awsx/middleware"
)

// defaultRetryTimeout is the wait applied after a failed ReceiveMessage before
// the polling loop tries again. It is used when WithRetryTimeout is not set.
const defaultRetryTimeout = 5 * time.Second

// Option configures a Consumer during construction. Options are applied in
// order by New and may return an error to abort construction; New wraps any
// option error with errors.ErrInvalidOption.
type Option func(*Consumer) error

// WithLogger sets the structured logger used by the consumer, its dispatcher,
// and its visibility manager. A nil logger is ignored so the consumer keeps its
// no-op default and callers never have to guard against nil.
func WithLogger(log *slog.Logger) Option {
	return func(c *Consumer) error {
		if log != nil {
			c.log = log
		}
		return nil
	}
}

// WithRetryTimeout sets the wait applied after a failed ReceiveMessage before
// the polling loop retries. The duration must be positive; a non-positive value
// fails construction with an error wrapping errors.ErrInvalidOption.
func WithRetryTimeout(d time.Duration) Option {
	return func(c *Consumer) error {
		if d <= 0 {
			return fmt.Errorf("retry timeout must be positive, got %s", d)
		}
		c.retryTimeout = d
		return nil
	}
}

// WithGlobalMiddleware appends middlewares applied outermost to every message,
// ahead of the route-level middlewares. They run first on the way in and last
// on the way out. Multiple calls accumulate in order and nil middlewares are
// ignored.
func WithGlobalMiddleware(mws ...middleware.Middleware) Option {
	return func(c *Consumer) error {
		for _, mw := range mws {
			if mw != nil {
				c.globalMiddlewares = append(c.globalMiddlewares, mw)
			}
		}
		return nil
	}
}

// WithMetrics sets the recorder used to report per-route message outcomes: the
// observe-only DLQ counter under the Visibility model and the success, retry,
// and dead-letter counters under the Scheduled Retry model. It should be wired
// only when the Metrics middleware is enabled and backed by the same registered
// counters. A nil recorder is ignored so the consumer keeps metric reporting off
// and callers never have to guard against nil.
func WithMetrics(rec MetricsRecorder) Option {
	return func(c *Consumer) error {
		if rec != nil {
			c.metrics = rec
		}
		return nil
	}
}

// WithSchedulerClient sets the EventBridge Scheduler client used to create
// retry schedules under the Scheduled Retry model. It should be wired for
// routes that select the Scheduled Retry model. A nil client is ignored so the
// consumer keeps its default and callers never have to guard against nil.
func WithSchedulerClient(client SchedulerClient) Option {
	return func(c *Consumer) error {
		if client != nil {
			c.schedulerClient = client
		}
		return nil
	}
}
