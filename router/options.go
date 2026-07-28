package router

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/silviolleite/loafer-awsx/errors"
	"github.com/silviolleite/loafer-awsx/middleware"
)

// maxReceiveMessages is the largest batch size AWS SQS accepts on a single
// ReceiveMessage call.
const maxReceiveMessages = int32(10)

// maxWaitTimeSeconds is the largest long-polling wait time AWS SQS accepts.
const maxWaitTimeSeconds = int32(20)

// Scheduled Retry model configuration bounds.
const (
	// defaultBaseBackoff is the base Backoff_Delay applied when none is
	// configured (Req 4.5).
	defaultBaseBackoff = 1000 * time.Millisecond

	// minBackoff is the smallest permitted base or maximum Backoff_Delay.
	minBackoff = 1 * time.Millisecond

	// maxBackoffGuard is the library-defined maximum backoff guard of
	// 86,400,000 milliseconds (24 hours).
	maxBackoffGuard = 24 * time.Hour

	// minMaxRetryCount and maxMaxRetryCount bound the inclusive Max_Retry_Count
	// range [0, 2147483647].
	minMaxRetryCount = 0
	maxMaxRetryCount = math.MaxInt32
)

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
		if r.scheduledRetry != nil {
			return errors.Wrap(errors.ErrScheduledRetryConfig,
				fmt.Errorf("observe-only WithDLQ conflicts with the Scheduled Retry model DLQ destination"))
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

// WithRetryModel sets the per-route retry model. The value must be exactly one
// of VisibilityRetryModel or ScheduledRetryModel; any other value is rejected at
// construction with an error wrapping errors.ErrScheduledRetryConfig that
// identifies the invalid value (Req 1.1, 1.4).
func WithRetryModel(m RetryModel) Option {
	return func(r *Route) error {
		if m != VisibilityRetryModel && m != ScheduledRetryModel {
			return errors.Wrap(errors.ErrScheduledRetryConfig,
				fmt.Errorf("unknown retry model %d", int(m)))
		}
		r.retryModel = m
		return nil
	}
}

// WithScheduledRetry selects the Scheduled Retry model and attaches a validated
// ScheduledRetryConfig assembled from the given sub-options. The base
// Backoff_Delay defaults to 1000ms when unset (Req 4.5). The assembled
// configuration is validated at construction; any error wraps
// errors.ErrScheduledRetryConfig and identifies the offending value, and the
// route does not start the Scheduled Retry model on failure.
//
// Configuring both this option and the observe-only WithDLQ on the same route is
// a configuration error regardless of the order the options are applied
// (Req 11.3).
func WithScheduledRetry(opts ...ScheduledRetryOption) Option {
	return func(r *Route) error {
		cfg := &ScheduledRetryConfig{}
		for _, opt := range opts {
			if opt == nil {
				continue
			}
			opt(cfg)
		}

		if cfg.BaseBackoff == 0 {
			cfg.BaseBackoff = defaultBaseBackoff
		}

		if err := validateScheduledRetry(r, cfg); err != nil {
			return errors.Wrap(errors.ErrScheduledRetryConfig, err)
		}

		r.retryModel = ScheduledRetryModel
		r.scheduledRetry = cfg
		return nil
	}
}

// validateScheduledRetry enforces the Scheduled Retry model configuration rules,
// returning an error that names the offending value. It does not wrap the error
// with the sentinel; the caller performs the wrapping.
func validateScheduledRetry(r *Route, cfg *ScheduledRetryConfig) error {
	if r.dlqConfig != nil {
		return fmt.Errorf("scheduled Retry model DLQ conflicts with the observe-only WithDLQ")
	}

	var missing []string
	if cfg.TargetQueueARN == "" {
		missing = append(missing, "target Entry_Queue ARN")
	}
	if cfg.ExecutionRoleARN == "" {
		missing = append(missing, "execution role ARN")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing scheduler identity: %s", strings.Join(missing, ", "))
	}

	if cfg.DLQQueueURL == "" {
		return fmt.Errorf("missing DLQ destination")
	}

	if cfg.MaxRetryCount < minMaxRetryCount || cfg.MaxRetryCount > maxMaxRetryCount {
		return fmt.Errorf("max retry count must be within [%d, %d], got %d",
			minMaxRetryCount, maxMaxRetryCount, cfg.MaxRetryCount)
	}

	if cfg.BaseBackoff < minBackoff || cfg.BaseBackoff > maxBackoffGuard {
		return fmt.Errorf("base backoff must be within [%s, %s], got %s",
			minBackoff, maxBackoffGuard, cfg.BaseBackoff)
	}
	if cfg.MaxBackoff < minBackoff || cfg.MaxBackoff > maxBackoffGuard {
		return fmt.Errorf("max backoff must be within [%s, %s], got %s",
			minBackoff, maxBackoffGuard, cfg.MaxBackoff)
	}
	if cfg.MaxBackoff < cfg.BaseBackoff {
		return fmt.Errorf("max backoff %s must be greater than or equal to base backoff %s",
			cfg.MaxBackoff, cfg.BaseBackoff)
	}

	return nil
}

// WithSchedulerIdentity sets the EventBridge Scheduler identity used to create
// retry schedules: the target Entry_Queue ARN and the execution role ARN. Both
// are required; a missing item is named individually at construction (Req 9.3,
// 9.4).
func WithSchedulerIdentity(targetQueueARN, executionRoleARN string) ScheduledRetryOption {
	return func(c *ScheduledRetryConfig) {
		c.TargetQueueARN = targetQueueARN
		c.ExecutionRoleARN = executionRoleARN
	}
}

// WithScheduledDLQ sets the DLQ destination queue URL used by the Scheduled Retry
// model for messages whose Retry_Count exceeds Max_Retry_Count. It is required
// (Req 5.5, 5.6).
func WithScheduledDLQ(dlqQueueURL string) ScheduledRetryOption {
	return func(c *ScheduledRetryConfig) {
		c.DLQQueueURL = dlqQueueURL
	}
}

// WithMaxRetryCount sets the inclusive Max_Retry_Count threshold after which a
// message is routed to the DLQ instead of being rescheduled. It must be within
// the inclusive range [0, 2147483647] (Req 5.5, 5.7).
func WithMaxRetryCount(n int) ScheduledRetryOption {
	return func(c *ScheduledRetryConfig) {
		c.MaxRetryCount = n
	}
}

// WithBackoff sets the base and maximum Backoff_Delay for the Scheduled Retry
// model. Both must be within the inclusive range [1ms, 86,400,000ms] and
// maxDelay must be greater than or equal to base. When base is left unset it
// defaults to 1000ms (Req 4.1, 4.5, 4.6, 4.7).
func WithBackoff(base, maxDelay time.Duration) ScheduledRetryOption {
	return func(c *ScheduledRetryConfig) {
		c.BaseBackoff = base
		c.MaxBackoff = maxDelay
	}
}
