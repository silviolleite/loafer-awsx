package router

import (
	"github.com/silviolleite/loafer-awsx/errors"
	"github.com/silviolleite/loafer-awsx/middleware"
)

// Route-level configuration defaults applied before user options.
const (
	defaultWorkerPoolSize    = 5
	defaultMaxMessages       = int32(10)
	defaultWaitTimeSeconds   = int32(10)
	defaultVisibilityTimeout = int32(30)
	defaultExtensionLimit    = 2

	// minVisibilityTimeout is the smallest visibility timeout the consumer can
	// operate with; smaller values are clamped up to this bound.
	minVisibilityTimeout = int32(11)
)

// Route holds the configuration for a single SQS queue consumer. It is an
// immutable value object: all fields are unexported and exposed through
// getters. Routes carry no runtime state.
type Route struct {
	handler           middleware.Handler
	dlqConfig         *DLQConfig
	scheduledRetry    *ScheduledRetryConfig
	queueName         string
	middlewares       []middleware.Middleware
	customGroupFields []string
	workerPoolSize    int
	extensionLimit    int
	runMode           Mode
	maxMessages       int32
	waitTimeSeconds   int32
	visibilityTimeout int32
	retryModel        RetryModel
}

// Option configures a Route. Options are applied in order and may return an
// error to abort construction.
type Option func(*Route) error

// New creates a Route with the given queue name, handler, and options. It seeds
// library defaults, applies the options in order, and finally clamps the
// visibility timeout to its minimum.
//
// New returns errors.ErrEmptyQueueName when queueName is empty and
// errors.ErrNoHandler when handler is nil. Option failures are wrapped with
// errors.ErrInvalidOption.
func New(queueName string, handler middleware.Handler, opts ...Option) (*Route, error) {
	if queueName == "" {
		return nil, errors.ErrEmptyQueueName
	}
	if handler == nil {
		return nil, errors.ErrNoHandler
	}

	r := &Route{
		queueName:         queueName,
		handler:           handler,
		workerPoolSize:    defaultWorkerPoolSize,
		maxMessages:       defaultMaxMessages,
		waitTimeSeconds:   defaultWaitTimeSeconds,
		visibilityTimeout: defaultVisibilityTimeout,
		extensionLimit:    defaultExtensionLimit,
		runMode:           Parallel,
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(r); err != nil {
			return nil, errors.Wrap(errors.ErrInvalidOption, err)
		}
	}

	if r.visibilityTimeout <= minVisibilityTimeout {
		r.visibilityTimeout = minVisibilityTimeout
	}

	return r, nil
}

// QueueName returns the configured queue name.
func (r *Route) QueueName() string {
	return r.queueName
}

// Handler returns the configured handler.
func (r *Route) Handler() middleware.Handler {
	return r.handler
}

// Middlewares returns the route-level middleware chain.
func (r *Route) Middlewares() []middleware.Middleware {
	return r.middlewares
}

// WorkerPoolSize returns the configured worker pool size.
func (r *Route) WorkerPoolSize() int {
	return r.workerPoolSize
}

// MaxMessages returns the max messages requested per SQS receive call.
func (r *Route) MaxMessages() int32 {
	return r.maxMessages
}

// WaitTimeSeconds returns the long-polling wait time in seconds.
func (r *Route) WaitTimeSeconds() int32 {
	return r.waitTimeSeconds
}

// VisibilityTimeout returns the visibility timeout in seconds.
func (r *Route) VisibilityTimeout() int32 {
	return r.visibilityTimeout
}

// ExtensionLimit returns the maximum number of visibility extensions.
func (r *Route) ExtensionLimit() int {
	return r.extensionLimit
}

// RunMode returns the dispatch mode.
func (r *Route) RunMode() Mode {
	return r.runMode
}

// CustomGroupFields returns the fields used to derive the group key for
// PerGroupID routing.
func (r *Route) CustomGroupFields() []string {
	return r.customGroupFields
}

// DLQ returns the DLQ configuration, or nil when the route has no DLQ.
func (r *Route) DLQ() *DLQConfig {
	return r.dlqConfig
}

// RetryModel returns the configured retry model. It defaults to
// VisibilityRetryModel when no retry model is configured.
func (r *Route) RetryModel() RetryModel {
	return r.retryModel
}

// ScheduledRetry returns the Scheduled Retry configuration, or nil when the
// route uses the Visibility model.
func (r *Route) ScheduledRetry() *ScheduledRetryConfig {
	return r.scheduledRetry
}
