package broker

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/silviolleite/loafer-awsx/middleware"
)

// defaultRetryTimeout is the wait each consumer applies after a failed
// ReceiveMessage before retrying. It is used when WithRetryTimeout is not set.
const defaultRetryTimeout = 5 * time.Second

// Option configures a Broker during construction. Options are applied in order
// by New and may return an error to abort construction; New wraps any option
// error with errors.ErrInvalidOption.
type Option func(*Broker) error

// WithLogger sets the structured logger used by the broker and forwarded to
// every consumer it starts. A nil logger is ignored so the broker keeps its
// default stdout logger and callers never have to guard against nil.
func WithLogger(log *slog.Logger) Option {
	return func(b *Broker) error {
		if log != nil {
			b.log = log
		}
		return nil
	}
}

// WithRetryTimeout sets the wait each consumer applies after a failed
// ReceiveMessage before retrying. The duration must be positive; a non-positive
// value fails construction with an error wrapping errors.ErrInvalidOption.
func WithRetryTimeout(d time.Duration) Option {
	return func(b *Broker) error {
		if d <= 0 {
			return fmt.Errorf("retry timeout must be positive, got %s", d)
		}
		b.retryTimeout = d
		return nil
	}
}

// WithShutdownTimeout sets the maximum time Run waits for in-flight consumers
// to finish after the context is canceled. The duration must be positive; a
// non-positive value fails construction with an error wrapping
// errors.ErrInvalidOption.
//
// When this option is not set, the broker waits indefinitely for consumers to
// drain their in-flight messages, so a slow-but-healthy consumer is never
// abandoned on shutdown. Set a positive duration to bound that wait.
func WithShutdownTimeout(d time.Duration) Option {
	return func(b *Broker) error {
		if d <= 0 {
			return fmt.Errorf("shutdown timeout must be positive, got %s", d)
		}
		b.shutdownTimeout = d
		return nil
	}
}

// WithGlobalMiddleware appends global middlewares applied outermost to every
// message across all consumers, ahead of any route-level middlewares. They run
// first on the way in and last on the way out. Multiple calls accumulate in
// order and nil middlewares are ignored.
func WithGlobalMiddleware(mws ...middleware.Middleware) Option {
	return func(b *Broker) error {
		for _, mw := range mws {
			if mw != nil {
				b.globalMiddlewares = append(b.globalMiddlewares, mw)
			}
		}
		return nil
	}
}
