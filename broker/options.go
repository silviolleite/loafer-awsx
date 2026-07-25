package broker

import (
	"log/slog"
	"time"

	"github.com/silviolleite/loafer-awsx/middleware"
)

// defaultRetryTimeout is the wait each consumer applies after a failed
// ReceiveMessage before retrying. It is used when WithRetryTimeout is not set.
const defaultRetryTimeout = 5 * time.Second

// Option configures a Broker during construction. Options are applied in order
// by New.
type Option func(*Broker)

// WithLogger sets the structured logger used by the broker and forwarded to
// every consumer it starts. A nil logger is ignored so the broker keeps its
// default stdout logger and callers never have to guard against nil.
func WithLogger(log *slog.Logger) Option {
	return func(b *Broker) {
		if log != nil {
			b.log = log
		}
	}
}

// WithRetryTimeout sets the wait each consumer applies after a failed
// ReceiveMessage before retrying. A non-positive duration is ignored so the
// broker keeps its default of 5 seconds.
func WithRetryTimeout(d time.Duration) Option {
	return func(b *Broker) {
		if d > 0 {
			b.retryTimeout = d
		}
	}
}

// WithShutdownTimeout sets the maximum time Run waits for in-flight consumers
// to finish after the context is canceled. A non-positive duration is ignored.
//
// When this option is not set, the broker waits indefinitely for consumers to
// drain their in-flight messages, so a slow-but-healthy consumer is never
// abandoned on shutdown. Set a positive duration to bound that wait.
func WithShutdownTimeout(d time.Duration) Option {
	return func(b *Broker) {
		if d > 0 {
			b.shutdownTimeout = d
		}
	}
}

// WithMiddleware appends global middlewares applied outermost to every message
// across all consumers, ahead of any route-level middlewares. They run first on
// the way in and last on the way out. Multiple calls accumulate in order.
func WithMiddleware(mws ...middleware.Middleware) Option {
	return func(b *Broker) {
		b.globalMiddlewares = append(b.globalMiddlewares, mws...)
	}
}
