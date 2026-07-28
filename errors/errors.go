package errors

import "fmt"

// Sentinel errors returned across loafer-go v3. They are comparable with the
// standard library errors.Is so callers can branch on specific failure modes
// without depending on error message strings.
var (
	// ErrNoRoute indicates that no routes were registered with the broker.
	ErrNoRoute = New("no routes registered")
	// ErrNoSQSClient indicates that a required SQS client was not provided.
	ErrNoSQSClient = New("sqs client is nil")
	// ErrNoHandler indicates that a route was created without a handler.
	ErrNoHandler = New("handler is nil")
	// ErrGetMessage indicates a failure while receiving messages from SQS.
	ErrGetMessage = New("failed to receive messages")
	// ErrQueueResolve indicates a failure while resolving a queue URL.
	ErrQueueResolve = New("failed to resolve queue url")
	// ErrNoSNSClient indicates that a required SNS client was not provided.
	ErrNoSNSClient = New("sns client is nil")
	// ErrEmptyInput indicates that a required publish input was nil or empty.
	ErrEmptyInput = New("input must be filled")
	// ErrMaxBatchSize indicates that a batch exceeded the maximum allowed size.
	ErrMaxBatchSize = New("batch size exceeds the maximum allowed")
	// ErrEmptyRegion indicates that a required AWS region was not provided.
	ErrEmptyRegion = New("region must be filled")
	// ErrEmptyQueueName indicates that a required queue name was not provided.
	ErrEmptyQueueName = New("queue name must be filled")
	// ErrInvalidOption indicates that a functional option received an invalid value.
	ErrInvalidOption = New("invalid option")
	// ErrEmptyFields indicates that a required fields map was empty.
	ErrEmptyFields = New("fields must be filled")
	// ErrPanic indicates that a handler panicked and the panic was recovered.
	ErrPanic = New("handler panicked")
	// ErrRetryScheduleCreate indicates that creating an EventBridge Scheduler
	// schedule for a scheduled retry failed; the original message is retained.
	ErrRetryScheduleCreate = New("failed to create retry schedule")
	// ErrDLQPublish indicates that publishing a message to the dead-letter queue
	// failed; the original message is retained.
	ErrDLQPublish = New("failed to publish to dead-letter queue")
	// ErrScheduledRetryConfig indicates that a Scheduled-model route
	// configuration is invalid or incomplete.
	ErrScheduledRetryConfig = New("invalid scheduled retry configuration")
	// ErrNoSchedulerClient indicates that a Scheduled-model route was given to a
	// consumer without an EventBridge Scheduler client, so scheduled retries
	// cannot be created; the consumer must not begin consuming.
	ErrNoSchedulerClient = New("scheduler client is nil")
)

// New returns a new error that formats as the given text. It is a thin wrapper
// around the standard library so callers within loafer-go do not need to import
// both this package and the standard errors package.
func New(text string) error {
	return &sentinelError{msg: text}
}

// sentinelError is an immutable error value used for the package sentinels.
type sentinelError struct {
	msg string
}

// Error implements the error interface.
func (e *sentinelError) Error() string {
	return e.msg
}

// Wrap combines a sentinel error with an underlying error into a single error
// that reports both in its message and remains matchable with errors.Is against
// either one. When err is nil the sentinel is returned unchanged; when sentinel
// is nil the underlying err is returned unchanged. If both are nil, nil is
// returned. The returned error never loses either cause because it unwraps to
// both via the multi-error support in fmt.Errorf.
func Wrap(sentinel, err error) error {
	switch {
	case sentinel == nil && err == nil:
		return nil
	case sentinel == nil:
		return err
	case err == nil:
		return sentinel
	default:
		return fmt.Errorf("%w: %w", sentinel, err)
	}
}
