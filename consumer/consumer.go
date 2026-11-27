package consumer

import (
	"context"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/silviolleite/loafer-awsx/errors"
	"github.com/silviolleite/loafer-awsx/logger"
	"github.com/silviolleite/loafer-awsx/middleware"
	"github.com/silviolleite/loafer-awsx/router"
)

// Consumer polls a single SQS queue and drives the message lifecycle for one
// route. It resolves the queue URL, receives messages with long polling only
// while it has capacity to start them soon, and hands each message to a worker
// pool that runs the middleware-wrapped handler and applies the outcome. A
// single visibility manager keeps every received message invisible from the
// moment it was received until it reaches an outcome. A Consumer is created
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

// Run resolves the queue URL, starts the visibility manager and the worker
// pool, and runs Route.Pollers() polling loops until ctx is canceled. The
// consumer never holds more than WorkerPoolSize plus Prefetch received messages
// that have not reached an outcome.
//
// It fails fast, before resolving the queue URL and without starting any
// goroutine, with errors.ErrNoSchedulerClient when a Scheduled-model route has
// no scheduler client, and with errors.ErrDeleteBatchUnsupported when the route
// enables router.WithDeleteBatch but the SQS client does not implement
// BatchDeleteClient. If the queue URL cannot be resolved Run returns
// immediately with an error wrapping errors.ErrQueueResolve and never starts
// any goroutine.
//
// Once polling starts, a receive error is logged and retried after the
// configured retry timeout, with the wait honoring ctx cancellation. On ctx
// cancellation Run stops polling, waits for every message whose handler is
// running to reach its outcome, and releases every received message that has
// not started by setting its visibility timeout to zero, so it is redelivered
// at once instead of after its visibility timeout. On a batched-delete route it
// then sends every pending delete. The final deletes and visibility changes are
// not canceled by ctx; each request is bounded by its own timeout. Run returns
// nil for a clean shutdown, after every goroutine it started has exited.
func (c *Consumer) Run(ctx context.Context) error {
	scheduled := c.route.RetryModel() == router.ScheduledRetryModel

	// A Scheduled-model route cannot create retry schedules without a scheduler
	// client, so fail fast before resolving the queue URL or starting any
	// goroutine: consumption must never begin in this misconfiguration.
	if scheduled && c.schedulerClient == nil {
		return errors.ErrNoSchedulerClient
	}

	batchClient, err := c.batchDeleteClient()
	if err != nil {
		return err
	}

	queueURL, err := c.resolveQueueURL(ctx)
	if err != nil {
		return err
	}

	// The visibility manager runs on a context detached from ctx: messages
	// still running after cancellation keep receiving Extensions until they
	// finish. It is closed after every worker has exited.
	vm := newVisibilityManager(c.client, queueURL, c.route.VisibilityTimeout(), c.route.ExtensionLimit(), c.log)
	vm.start(context.WithoutCancel(ctx))
	defer vm.close()

	d := newDispatcher(c.client, c.route, queueURL, vm, c.schedulerClient, c.log, c.globalMiddlewares...)

	// The recorder serves both models: the Visibility model reports the
	// observe-only DLQ counter and the Scheduled Retry model reports success,
	// retry, and dead-letter outcomes. Each code path calls only the methods it
	// owns, so a single recorder is wired unconditionally.
	d.metrics = c.metrics

	// Senders get a context detached from ctx so deletes committed by messages
	// still running at shutdown are still sent; the batcher is closed only after
	// every worker has exited.
	if batchClient != nil {
		d.batcher = newDeleteBatcher(batchClient, queueURL, d.workerPoolSize*int(c.route.MaxMessages()), c.log)
		d.batcher.start(context.WithoutCancel(ctx), d.workerPoolSize)
	}

	d.startWorkers(ctx)
	c.runPollers(ctx, queueURL, d)
	d.stopWorkers()
	d.releasePending()

	if d.batcher != nil {
		d.batcher.close()
	}

	return nil
}

// batchDeleteClient returns the SQS client as a BatchDeleteClient when the route
// enables batched deletes, and nil when it does not. It returns
// errors.ErrDeleteBatchUnsupported when the route enables batched deletes but
// the client does not implement DeleteMessageBatch.
func (c *Consumer) batchDeleteClient() (BatchDeleteClient, error) {
	if !c.route.DeleteBatch() {
		return nil, nil
	}

	bc, ok := c.client.(BatchDeleteClient)
	if !ok {
		return nil, errors.ErrDeleteBatchUnsupported
	}

	return bc, nil
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
