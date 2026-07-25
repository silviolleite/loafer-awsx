package consumer

import (
	"context"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/silviolleite/loafer-awsx/logger"
)

const (
	// visibilityRenewalMargin is the number of seconds subtracted from the
	// route visibility timeout to derive the extension interval, ensuring the
	// visibility is renewed shortly before it would otherwise expire.
	visibilityRenewalMargin = int32(10)

	// maxVisibilityTimeout is the largest visibility timeout accepted by AWS SQS
	// (12 hours). Any computed value above this is clamped down.
	//
	// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ChangeMessageVisibility.html
	maxVisibilityTimeout = int32(12 * 60 * 60)

	// minVisibilityTimeout is the smallest visibility timeout accepted by AWS
	// SQS. Negative computed values are clamped up to this bound.
	minVisibilityTimeout = int32(0)
)

// visibilityManager extends the visibility timeout of a single in-flight
// message while its handler is running. It renews the timeout on a fixed
// interval derived from the route visibility timeout, reacts to dispatch and
// backoff signals, honors an extension limit, and stops on context
// cancellation. All ChangeMessageVisibility values are clamped to the AWS SQS
// bounds.
type visibilityManager struct {
	client            SQSClient
	log               *slog.Logger
	queueURL          string
	sleepInterval     time.Duration
	visibilityTimeout int32
	extensionLimit    int
}

// newVisibilityManager builds a visibilityManager for a queue. visibilityTimeout
// and extensionLimit come from the route configuration. A nil logger is
// replaced with a no-op logger so callers never have to guard against nil.
func newVisibilityManager(
	client SQSClient,
	queueURL string,
	visibilityTimeout int32,
	extensionLimit int,
	log *slog.Logger,
) *visibilityManager {
	if log == nil {
		log = logger.NewNoOp()
	}

	return &visibilityManager{
		client:            client,
		log:               log,
		queueURL:          queueURL,
		visibilityTimeout: visibilityTimeout,
		extensionLimit:    extensionLimit,
	}
}

// interval returns the delay between visibility extensions. When an explicit
// sleepInterval is set (used by tests for determinism) it takes precedence;
// otherwise the interval is (visibilityTimeout - 10) seconds, guarded against
// non-positive values so the underlying ticker never panics.
func (v *visibilityManager) interval() time.Duration {
	if v.sleepInterval > 0 {
		return v.sleepInterval
	}

	seconds := v.visibilityTimeout - visibilityRenewalMargin
	if seconds < 1 {
		seconds = 1
	}

	return time.Duration(seconds) * time.Second
}

// run drives the extension loop for msg until the handler dispatches, a backoff
// is requested, the extension limit is exhausted, or the context is canceled.
//
// The first tick renews the visibility to the route visibility timeout; each
// subsequent tick increments it by the route value. When a backoff signal
// arrives the visibility is set to the backoff duration and the loop stops. run
// is intended to be launched as a goroutine, one per in-flight message, and is
// guaranteed to return so it never leaks.
func (v *visibilityManager) run(ctx context.Context, msg *message) {
	ticker := time.NewTicker(v.interval())
	defer ticker.Stop()

	var count int
	extension := v.visibilityTimeout

	for {
		if count > v.extensionLimit {
			return
		}

		select {
		case <-ctx.Done():
			return
		case delay := <-msg.backoffSignal():
			v.changeVisibility(ctx, msg, int32(delay.Seconds()))
			return
		case <-msg.dispatchSignal():
			return
		case <-ticker.C:
			if count > 0 {
				extension += v.visibilityTimeout
			}
			v.changeVisibility(ctx, msg, extension)
			count++
		}
	}
}

// changeVisibility issues a ChangeMessageVisibility call for msg, clamping
// timeout to the AWS SQS [0, 43200] range. A failure is logged at Error level
// and swallowed so a transient API error never crashes the manager.
func (v *visibilityManager) changeVisibility(ctx context.Context, msg *message, timeout int32) {
	if timeout < minVisibilityTimeout {
		timeout = minVisibilityTimeout
	}
	if timeout > maxVisibilityTimeout {
		timeout = maxVisibilityTimeout
	}

	_, err := v.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(v.queueURL),
		ReceiptHandle:     aws.String(msg.Identifier()),
		VisibilityTimeout: timeout,
	})
	if err != nil {
		v.log.Error("failed to change message visibility",
			slog.String("queue_url", v.queueURL),
			slog.String("receipt_handle", msg.Identifier()),
			slog.Int("visibility_timeout", int(timeout)),
			slog.Any("error", err),
		)
	}
}
