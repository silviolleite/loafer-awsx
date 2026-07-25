package router

import (
	"fmt"

	"github.com/silviolleite/loafer-awsx/middleware"
)

// maxReceiveMessages is the largest batch size AWS SQS accepts on a single
// ReceiveMessage call.
const maxReceiveMessages = int32(10)

// maxWaitTimeSeconds is the largest long-polling wait time AWS SQS accepts.
const maxWaitTimeSeconds = int32(20)

// WithWorkerPoolSize sets the number of worker goroutines. The value must be
// greater than zero. The default is 5.
func WithWorkerPoolSize(n int) Option {
	return func(r *Route) error {
		if n < 1 {
			return fmt.Errorf("worker pool size must be greater than zero, got %d", n)
		}
		r.workerPoolSize = n
		return nil
	}
}

// WithMaxMessages sets the maximum number of messages requested per SQS receive
// call. The value must be within the inclusive range [1, 10]. The default is 10.
func WithMaxMessages(n int32) Option {
	return func(r *Route) error {
		if n < 1 || n > maxReceiveMessages {
			return fmt.Errorf("max messages must be between 1 and %d, got %d", maxReceiveMessages, n)
		}
		r.maxMessages = n
		return nil
	}
}

// WithWaitTimeSeconds sets the long-polling wait time in seconds. The value must
// be within the inclusive range [0, 20]. The default is 10.
func WithWaitTimeSeconds(n int32) Option {
	return func(r *Route) error {
		if n < 0 || n > maxWaitTimeSeconds {
			return fmt.Errorf("wait time seconds must be between 0 and %d, got %d", maxWaitTimeSeconds, n)
		}
		r.waitTimeSeconds = n
		return nil
	}
}

// WithVisibilityTimeout sets the visibility timeout in seconds. Values at or
// below the minimum of 11 are clamped up to 11 during construction. The default
// is 30.
func WithVisibilityTimeout(seconds int32) Option {
	return func(r *Route) error {
		r.visibilityTimeout = seconds
		return nil
	}
}

// WithExtensionLimit sets how many times the visibility timeout may be extended
// while a message is processed. The value must not be negative. The default is 2.
func WithExtensionLimit(n int) Option {
	return func(r *Route) error {
		if n < 0 {
			return fmt.Errorf("extension limit must not be negative, got %d", n)
		}
		r.extensionLimit = n
		return nil
	}
}

// WithRunMode sets the message dispatch strategy. The mode must be one of the
// defined values. The default is Parallel.
func WithRunMode(mode Mode) Option {
	return func(r *Route) error {
		if !mode.valid() {
			return fmt.Errorf("unknown run mode %d", int(mode))
		}
		r.runMode = mode
		return nil
	}
}

// WithCustomGroupFields sets the fields extracted from the message used to
// derive the group key for PerGroupID routing. Nil and empty entries are
// ignored.
func WithCustomGroupFields(fields ...string) Option {
	return func(r *Route) error {
		cleaned := make([]string, 0, len(fields))
		for _, f := range fields {
			if f == "" {
				continue
			}
			cleaned = append(cleaned, f)
		}
		r.customGroupFields = cleaned
		return nil
	}
}

// WithMiddleware appends route-level middleware to the chain in the order
// provided. Nil middlewares are ignored.
func WithMiddleware(mws ...middleware.Middleware) Option {
	return func(r *Route) error {
		for _, mw := range mws {
			if mw == nil {
				continue
			}
			r.middlewares = append(r.middlewares, mw)
		}
		return nil
	}
}

// WithDLQ enables Dead Letter Queue observability for the route. maxReceiveCount
// must be greater than zero and should mirror the source queue's SQS redrive
// policy maxReceiveCount. The redrive destination is owned by the SQS queue
// redrive policy; this option does not move messages and only enables DLQ
// observability through logs, metrics, and the OnDLQ callback. Additional DLQ
// options refine the configuration.
func WithDLQ(maxReceiveCount int, opts ...DLQOption) Option {
	return func(r *Route) error {
		if maxReceiveCount < 1 {
			return fmt.Errorf("dlq max receive count must be greater than zero, got %d", maxReceiveCount)
		}

		cfg := &DLQConfig{
			MaxReceiveCount: maxReceiveCount,
		}
		for _, opt := range opts {
			if opt == nil {
				continue
			}
			opt(cfg)
		}

		r.dlqConfig = cfg
		return nil
	}
}
