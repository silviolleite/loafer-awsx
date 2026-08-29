package consumer

import (
	"context"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/silviolleite/loafer-awsx/errors"
	"github.com/silviolleite/loafer-awsx/logger"
	"github.com/silviolleite/loafer-awsx/middleware"
	"github.com/silviolleite/loafer-awsx/router"
)

// allAttributes requests every custom message attribute on each receive.
var allAttributes = []string{"All"}

// allSystemAttributes requests every system attribute on each receive.
var allSystemAttributes = []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll}

// Consumer polls a single SQS queue and drives the message lifecycle for one
// route. It resolves the queue URL, receives messages in batches with long
// polling, and hands each message to a worker-pool dispatcher that runs the
// middleware-wrapped handler and applies the outcome. A Consumer is created
// with New and run with Run; it carries no exported mutable state and a single
// Consumer is intended to be run once at a time.
type Consumer struct {
	client            SQSClient
	route             *router.Route
	log               *slog.Logger
	metrics           MetricsRecorder
	schedulerClient   SchedulerClient
	globalMiddlewares []middleware.Middleware
	retryTimeout      time.Duration
}

// New builds a Consumer for the given SQS client and route. The consumer
// defaults to a no-op logger and a 5 second retry timeout; both are overridable
// through options. Nil options are ignored.
//
// New returns errors.ErrNoSQSClient when client is nil and errors.ErrNoRoute
// when route is nil, so a misconfigured consumer fails fast at construction
// rather than panicking during Run. Option failures are wrapped with
// errors.ErrInvalidOption.
func New(client SQSClient, route *router.Route, opts ...Option) (*Consumer, error) {
	if client == nil {
		return nil, errors.ErrNoSQSClient
	}
	if route == nil {
		return nil, errors.ErrNoRoute
	}

	c := &Consumer{
		client:       client,
		route:        route,
		log:          logger.NewNoOp(),
		retryTimeout: defaultRetryTimeout,
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(c); err != nil {
			return nil, errors.Wrap(errors.ErrInvalidOption, err)
		}
	}

	return c, nil
}

// Run resolves the queue URL, starts the worker pool, and enters the polling
// loop until ctx is canceled.
//
// It fails fast: if the queue URL cannot be resolved Run returns immediately
// with an error wrapping errors.ErrQueueResolve and never starts any goroutine.
// Once polling starts, a receive error is logged and retried after the
// configured retry timeout, with the wait honoring ctx cancellation. On ctx
// cancellation Run stops polling, tears down the dispatcher (closing the worker
// channels and waiting for all in-flight messages and visibility goroutines to
// finish), and returns nil for a clean shutdown.
func (c *Consumer) Run(ctx context.Context) error {
	scheduled := c.route.RetryModel() == router.ScheduledRetryModel

	// A Scheduled-model route cannot create retry schedules without a scheduler
	// client, so fail fast before resolving the queue URL or starting any
	// goroutine: consumption must never begin in this misconfiguration.
	if scheduled && c.schedulerClient == nil {
		return errors.ErrNoSchedulerClient
	}

	queueURL, err := c.resolveQueueURL(ctx)
	if err != nil {
		return err
	}

	var vm *visibilityManager
	if scheduled {
		vm = newScheduledVisibilityManager(c.client, queueURL, c.route.VisibilityTimeout(), c.route.ExtensionLimit(), c.log)
	} else {
		vm = newVisibilityManager(c.client, queueURL, c.route.VisibilityTimeout(), c.route.ExtensionLimit(), c.log)
	}

	d := newDispatcher(c.client, c.route, queueURL, vm, c.schedulerClient, c.log, c.globalMiddlewares...)

	// The recorder serves both models: the Visibility model reports the
	// observe-only DLQ counter and the Scheduled Retry model reports success,
	// retry, and dead-letter outcomes. Each code path calls only the methods it
	// owns, so a single recorder is wired unconditionally.
	d.metrics = c.metrics

	d.start(ctx)
	defer d.stop()

	c.poll(ctx, queueURL, d)

	return nil
}

// resolveQueueURL looks up the route queue URL by name. Any failure, including a
// missing URL in the response, is wrapped with errors.ErrQueueResolve so callers
// can branch on the failure mode with errors.Is.
func (c *Consumer) resolveQueueURL(ctx context.Context) (string, error) {
	out, err := c.client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(c.route.QueueName()),
	})
	if err != nil {
		return "", errors.Wrap(errors.ErrQueueResolve, err)
	}
	if out == nil || out.QueueUrl == nil {
		return "", errors.ErrQueueResolve
	}

	return *out.QueueUrl, nil
}

// poll runs the receive loop until ctx is canceled. Each iteration requests a
// batch of messages with all attributes and dispatches every received message
// to the worker pool. A receive error is logged and followed by a retry-timeout
// wait; the loop returns as soon as ctx is canceled, whether at the top of the
// loop, during the retry wait, or while dispatching.
//
// Each batch shares one group barrier, attached to every message before
// dispatch. The barrier is nil unless the route uses PerGroupID dispatch with
// the Visibility retry model; when non-nil it lets the dispatcher hold back the
// tail of a FIFO group whose head failed, preserving order.
//
// The receive request sets VisibilityTimeout to the route value so each message
// starts hidden for the configured duration. This lets the visibility manager
// skip the initial ChangeMessageVisibility call — it only extends after the
// first tick when processing is still ongoing — avoiding an extra API call (and
// its cost) for messages that complete quickly.
func (c *Consumer) poll(ctx context.Context, queueURL string, d *dispatcher) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		out, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:                    aws.String(queueURL),
			MaxNumberOfMessages:         c.route.MaxMessages(),
			WaitTimeSeconds:             c.route.WaitTimeSeconds(),
			VisibilityTimeout:           c.route.VisibilityTimeout(),
			MessageAttributeNames:       allAttributes,
			MessageSystemAttributeNames: allSystemAttributes,
		})
		if err != nil {
			if !c.retryAfterError(ctx, queueURL, err) {
				return
			}
			continue
		}

		barrier := d.newBatchBarrier()
		for i := range out.Messages {
			msg := newMessage(out.Messages[i])
			msg.barrier = barrier
			d.dispatch(ctx, msg)
		}
	}
}

// retryAfterError handles a failed receive. When ctx is already canceled it
// returns false immediately without logging so a shutdown-induced error is not
// reported as a failure. Otherwise it logs the error and waits the configured
// retry timeout, returning true to retry or false if ctx is canceled during the
// wait.
func (c *Consumer) retryAfterError(ctx context.Context, queueURL string, err error) bool {
	if ctx.Err() != nil {
		return false
	}

	c.log.Error("failed to receive messages",
		slog.String("queue_url", queueURL),
		slog.Any("error", errors.Wrap(errors.ErrGetMessage, err)),
	)

	timer := time.NewTimer(c.retryTimeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
