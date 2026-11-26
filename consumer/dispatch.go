package consumer

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/silviolleite/loafer-awsx/idgen"
	"github.com/silviolleite/loafer-awsx/logger"
	"github.com/silviolleite/loafer-awsx/middleware"
	"github.com/silviolleite/loafer-awsx/router"
)

const (
	// messageGroupIDKey is the SQS system attribute that carries the FIFO
	// message group identifier used to derive the PerGroupID routing key.
	messageGroupIDKey = "MessageGroupId"

	// groupKeySeparator joins the message group ID with any custom group
	// fields when building the routing key. It is unlikely to appear inside an
	// individual key component, keeping distinct groups from colliding.
	groupKeySeparator = "\x00"

	// approximateReceiveCountKey is the SQS system attribute that reports how
	// many times a message has been received. It drives DLQ exhaustion
	// detection: once it reaches the route's configured MaxReceiveCount, AWS SQS
	// is about to redrive the message to its dead letter queue.
	approximateReceiveCountKey = "ApproximateReceiveCount"

	// maxReceiveBatch is the AWS SQS limit on messages per ReceiveMessage call.
	maxReceiveBatch = 10

	// fifoQueueSuffix ends the name, and so the URL, of every SQS FIFO queue.
	fifoQueueSuffix = ".fifo"

	// outcomeTimeout bounds the outcome requests of a message whose handler
	// finished after the run context was canceled.
	outcomeTimeout = 5 * time.Second
)

// dispatcher owns the per-route worker pool and the admission state shared with
// the pollers. Workers take Pending_Messages from the work source, run the
// middleware-wrapped handler, and apply the outcome (delete on success, leave on
// error, change visibility on backoff). Every admitted message holds one credit
// of the credit pool until it reaches an outcome, so the number of messages the
// consumer holds is bounded by WorkerPoolSize plus Prefetch.
//
// The work source depends on the run mode: Parallel uses a sharedQueue that any
// idle worker drains, PerGroupID uses a groupScheduler that keeps at most one
// message per group active and hands any ready group to any idle worker.
//
// On a route without batched deletes, batcher is nil and every Commit is a
// synchronous DeleteMessage issued from the worker. On a route configured with
// router.WithDeleteBatch, batcher is set before the workers start and every
// Commit hands the receipt handle to the deleteBatcher.
//
// A dispatcher is created with newDispatcher, its workers are started with
// startWorkers and awaited with stopWorkers, and the messages still pending at
// shutdown are released with releasePending.
type dispatcher struct {
	client         SQSClient
	handler        middleware.Handler
	visibility     *visibilityManager
	credits        *creditPool
	idle           *idleWorkers
	src            workSource
	log            *slog.Logger
	dlq            *router.DLQConfig
	metrics        MetricsRecorder
	retryScheduler *retryScheduler
	dlqPublisher   *dlqPublisher
	batcher        *deleteBatcher
	scheduledRetry *router.ScheduledRetryConfig
	queueURL       string
	queueName      string
	mode           router.Mode
	retryModel     router.RetryModel
	workerPoolSize int
	workers        sync.WaitGroup
	isFIFO         bool
	receiveBatch   int32
}

// newDispatcher builds a dispatcher for a route. The route handler is wrapped
// with the global middlewares (outermost) followed by the route middlewares,
// preserving first-is-outermost semantics. queueURL is the resolved URL of the
// route queue and vm is the visibility manager that tracks every admitted
// message. A nil logger is replaced with a no-op logger.
//
// The credit pool and the work source are sized to WorkerPoolSize plus
// Prefetch, and each receive asks for min(MaxMessages, Prefetch+1) messages.
// Bounding the batch by Prefetch+1 guarantees that a poller can always start a
// receive while any worker is idle: with fewer than WorkerPoolSize messages
// admitted, at least Prefetch+1 credits are free.
//
// schedulerClient is the EventBridge Scheduler client used only by the Scheduled
// Retry model. The retryScheduler and dlqPublisher collaborators are constructed
// solely when the route selects ScheduledRetryModel, a validated
// ScheduledRetryConfig is present, and schedulerClient is non-nil; for the
// Visibility model schedulerClient is ignored and no scheduler or DLQ collaborator
// is created.
func newDispatcher(
	client SQSClient,
	route *router.Route,
	queueURL string,
	vm *visibilityManager,
	schedulerClient SchedulerClient,
	log *slog.Logger,
	globalMiddlewares ...middleware.Middleware,
) *dispatcher {
	if log == nil {
		log = logger.NewNoOp()
	}

	mws := make([]middleware.Middleware, 0, len(globalMiddlewares)+len(route.Middlewares()))
	mws = append(mws, globalMiddlewares...)
	mws = append(mws, route.Middlewares()...)
	handler := middleware.Chain(mws...)(route.Handler())

	workers := max(route.WorkerPoolSize(), 1)
	capacity := workers + max(route.Prefetch(), 0)

	prefetch := max(route.Prefetch(), 0)
	receiveBatch := int32(max(min(int(route.MaxMessages()), prefetch+1, maxReceiveBatch), 1))

	idle := newIdleWorkers()
	var src workSource = newSharedQueue(capacity, idle)
	if route.RunMode() == router.PerGroupID {
		src = newGroupScheduler(capacity, route.CustomGroupFields(), idle)
	}

	d := &dispatcher{
		client:         client,
		handler:        handler,
		visibility:     vm,
		credits:        newCreditPool(capacity),
		idle:           idle,
		src:            src,
		log:            log,
		dlq:            route.DLQ(),
		queueURL:       queueURL,
		queueName:      route.QueueName(),
		mode:           route.RunMode(),
		retryModel:     route.RetryModel(),
		workerPoolSize: workers,
		receiveBatch:   receiveBatch,
		isFIFO:         strings.HasSuffix(queueURL, fifoQueueSuffix),
	}

	if cfg := route.ScheduledRetry(); route.RetryModel() == router.ScheduledRetryModel && cfg != nil && schedulerClient != nil {
		d.scheduledRetry = cfg
		d.retryScheduler = newRetryScheduler(
			schedulerClient,
			cfg.TargetQueueARN,
			cfg.ExecutionRoleARN,
			queueURL,
			idgen.NewRandom(),
		)
		d.dlqPublisher = newDLQPublisher(client, cfg.DLQQueueURL, idgen.NewRandom())
	}

	return d
}

// admit hands the messages of one receive to the work source in a single step,
// after the visibility manager starts tracking each of them. The caller holds
// one credit per message; each is returned when its message reaches an
// outcome.
func (d *dispatcher) admit(msgs ...*message) {
	for _, msg := range msgs {
		d.visibility.track(msg)
	}
	d.src.submit(msgs...)
}

// startWorkers launches WorkerPoolSize workers. Each returns once ctx is done.
func (d *dispatcher) startWorkers(ctx context.Context) {
	for range d.workerPoolSize {
		d.workers.Go(func() { d.worker(ctx) })
	}
}

// stopWorkers waits for every worker to return. Workers return once ctx is done
// and the message each one is running has reached its outcome.
func (d *dispatcher) stopWorkers() {
	d.workers.Wait()
}

// worker takes messages from the work source and processes them until ctx is
// done. A message taken after ctx was canceled is released instead of run.
func (d *dispatcher) worker(ctx context.Context) {
	for {
		msg, ok := d.src.take(ctx)
		if !ok {
			return
		}

		if ctx.Err() != nil {
			d.visibility.release(msg)
			d.credits.release(1)
			d.hold(d.src.complete(msg, false))
			continue
		}

		if !d.visibility.begin(msg, time.Now()) {
			d.skipExpired(ctx, msg)
			d.hold(d.src.complete(msg, d.retryModel == router.VisibilityRetryModel))
			continue
		}

		failed := d.process(ctx, msg)
		d.hold(d.src.complete(msg, failed))
	}
}

// skipExpired finishes a message whose visibility timeout ran out before a
// worker could start it. SQS may already have delivered it to another consumer,
// so its handler is not run and it is left for redelivery. Because a pending
// message is extended while it waits, this only happens when its Extensions
// failed or the route disables them, so it is logged at Error with the
// identifiers needed to follow the message across deliveries. The skip is
// counted through the optional SkipRecorder, and the observe-only DLQ path runs
// as it does after a handler error, because the receive that was just skipped
// may be the last one before SQS redrives the message. Under the Visibility
// retry model the caller holds the rest of its group, which keeps group order.
func (d *dispatcher) skipExpired(ctx context.Context, msg *message) {
	d.visibility.untrack(msg)
	defer d.credits.release(1)

	d.log.Error("skipping message whose visibility timeout expired before it started",
		slog.String("queue_url", d.queueURL),
		slog.String("receipt_handle", msg.Identifier()),
		slog.String("sqs_message_id", msg.sqsMessageID()),
		slog.String("group_id", msg.SystemAttributeByKey(messageGroupIDKey)),
		slog.Int("receive_count", receiveCount(msg)),
	)

	if rec, ok := d.metrics.(SkipRecorder); ok {
		emitMetric(d.log, rec.IncSkipped, d.queueName)
	}

	d.observeDLQ(ctx, msg)
}

// releasePending makes every message still pending visible again at once and
// returns its credit. It is called after every poller and worker has returned.
func (d *dispatcher) releasePending() {
	pending := d.src.drain()
	if len(pending) == 0 {
		return
	}
	d.visibility.release(pending...)
	d.credits.release(len(pending))
}

// process runs the middleware-wrapped handler for msg and applies the outcome.
// It reports whether the outcome is a failure that must hold the rest of the
// message group, which is the case for a handler error or a backoff under the
// Visibility retry model. Outcomes:
//   - backoff (Visibility model): the visibility manager changes the message
//     visibility to the backoff delay; the message is not deleted.
//   - error, message exhausted: when a DLQ route option is configured and the
//     message's ApproximateReceiveCount has reached MaxReceiveCount, the message
//     is treated as exhausted. Observability signals are emitted and the message
//     is left in the source queue so AWS SQS performs the redrive natively; the
//     library never publishes or deletes it for DLQ purposes.
//   - error: the error is logged with message body, group ID, and receipt
//     handle, and the message is left in the queue for redelivery.
//   - success: the message is deleted from the queue.
//   - Scheduled Retry model: processScheduled decides the outcome.
//
// In every case the message stops being tracked for visibility and its credit
// is returned. When the run context was canceled while the handler ran, the
// outcome is still applied, through outcomeContext.
func (d *dispatcher) process(ctx context.Context, msg *message) bool {
	err := d.handler(ctx, msg)

	ctx, cancel := outcomeContext(ctx)
	defer cancel()

	if d.retryModel == router.ScheduledRetryModel {
		d.visibility.untrack(msg)
		d.processScheduled(ctx, msg, err)
		d.credits.release(1)
		return false
	}

	if msg.BackedOff() {
		d.visibility.backoff(msg, msg.backoffDelay())
		d.credits.release(1)
		return true
	}

	d.visibility.untrack(msg)
	defer d.credits.release(1)

	if err != nil {
		if d.observeDLQ(ctx, msg) {
			return true
		}

		d.log.Error("handler returned an error",
			slog.String("queue_url", d.queueURL),
			slog.String("receipt_handle", msg.Identifier()),
			slog.String("group_id", msg.SystemAttributeByKey(messageGroupIDKey)),
			slog.String("body", string(msg.Body())),
			slog.Any("error", err),
		)
		return true
	}

	d.deleteMessage(ctx, msg)
	return false
}

// outcomeContext returns the context used to apply a handler outcome. While
// ctx is live it is returned unchanged. Once ctx is canceled, which happens
// when the consumer shuts down while a handler is still running, the outcome
// requests (delete, retry schedule, DLQ publish) run on a context detached from
// that cancellation and bounded by outcomeTimeout, so a message that finished
// is still committed instead of being redelivered.
func outcomeContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx.Err() == nil {
		return ctx, func() {}
	}
	return context.WithTimeout(context.WithoutCancel(ctx), outcomeTimeout)
}

// finish stops tracking msg for visibility and returns its credit.
func (d *dispatcher) finish(msg *message) {
	d.visibility.untrack(msg)
	d.credits.release(1)
}

// hold finishes every message held back because an earlier message of its
// group failed. A held message never runs its handler and is not deleted.
//
// On a FIFO queue it is released with a visibility timeout of zero: SQS does
// not deliver it while the failed message of its group is in flight, and
// delivers the group in order once that message is visible again, so the group
// is retried when the failed message's backoff or visibility timeout ends
// rather than when the latest extension of a held message runs out. On any
// other queue SQS gives no group ordering, so the held message keeps its
// current visibility to stay behind the failed one.
func (d *dispatcher) hold(msgs []*message) {
	for _, msg := range msgs {
		d.log.Warn("holding message to preserve FIFO group order; earlier message in group failed",
			slog.String("queue_url", d.queueURL),
			slog.String("receipt_handle", msg.Identifier()),
			slog.String("group_id", msg.SystemAttributeByKey(messageGroupIDKey)),
		)
		if d.isFIFO {
			d.visibility.release(msg)
			d.credits.release(1)
			continue
		}
		d.finish(msg)
	}
}

// processScheduled applies the Scheduled Retry model outcome for a message after
// its handler returns. A backoff request never changes the message visibility
// under this model:
//
//   - Failure (handler error OR requested backoff): the current retry count is
//     parsed from the message (defaulting to 0) and the next count is
//     current+1. When the next count is at or below MaxRetryCount a one-time
//     retry schedule is created for the computed backoff; on success the
//     original message is deleted and the retry metric is emitted. When the next
//     count exceeds MaxRetryCount the message is published to the DLQ; on success
//     the original message is deleted and the dead-letter metric is emitted. If
//     either orchestration step fails the error is logged and the message is
//     retained (not deleted) for redelivery after its visibility timeout.
//   - Success: the message is deleted and the success metric is emitted. The
//     library performs no success-side publishing; that is the handler's
//     responsibility.
func (d *dispatcher) processScheduled(ctx context.Context, msg *message, err error) {
	if err != nil || msg.BackedOff() {
		current := parseRetryCount(msg, d.log)
		next := current + 1

		if next <= d.scheduledRetry.MaxRetryCount {
			backoff := computeBackoff(next, d.scheduledRetry.BaseBackoff, d.scheduledRetry.MaxBackoff)
			if scheduleErr := d.retryScheduler.schedule(ctx, msg, next, backoff); scheduleErr != nil {
				d.log.Error("failed to create retry schedule; retaining message",
					slog.String("queue_url", d.queueURL),
					slog.String("receipt_handle", msg.Identifier()),
					slog.String("group_id", msg.SystemAttributeByKey(messageGroupIDKey)),
					slog.Int("retry_count", next),
					slog.Any("error", scheduleErr),
				)
				return
			}

			d.deleteMessage(ctx, msg)
			if d.metrics != nil {
				emitMetric(d.log, d.metrics.IncRetry, d.queueName)
			}
			return
		}

		if publishErr := d.dlqPublisher.publish(ctx, msg, next); publishErr != nil {
			d.log.Error("failed to publish message to DLQ; retaining message",
				slog.String("queue_url", d.queueURL),
				slog.String("receipt_handle", msg.Identifier()),
				slog.String("group_id", msg.SystemAttributeByKey(messageGroupIDKey)),
				slog.Int("retry_count", next),
				slog.Any("error", publishErr),
			)
			return
		}

		d.deleteMessage(ctx, msg)
		if d.metrics != nil {
			emitMetric(d.log, d.metrics.IncDeadLetter, d.queueName)
		}
		return
	}

	d.deleteMessage(ctx, msg)
	if d.metrics != nil {
		emitMetric(d.log, d.metrics.IncSuccess, d.queueName)
	}
}

// observeDLQ emits observe-only dead-letter signals for a message whose handler
// returned an error, or that was skipped because its visibility timeout ran out
// before it started, and reports whether it treated the message as exhausted.
//
// It is a no-op returning false unless a DLQ route option is configured and the
// message's ApproximateReceiveCount has reached the route's MaxReceiveCount. The
// DLQ model is strictly observe-only: AWS SQS performs the actual redrive per
// its own redrive policy, so the message is left untouched in the source queue —
// it is never published to any destination and never deleted for DLQ purposes.
//
// When the message is exhausted it logs at Error level (receipt handle under
// message_id, SQS MessageId, queue name, receive count), increments loafer_messages_dlq_total through the
// injected metric incrementer when the Metrics middleware is enabled, invokes
// the optional OnDLQ callback, and returns true.
func (d *dispatcher) observeDLQ(ctx context.Context, msg *message) bool {
	if d.dlq == nil {
		return false
	}

	receiveCount := receiveCount(msg)
	if receiveCount < d.dlq.MaxReceiveCount {
		return false
	}

	d.log.Error("message exhausted; left in source queue for AWS SQS redrive",
		slog.String("queue_name", d.queueName),
		slog.String("message_id", msg.Identifier()),
		slog.String("sqs_message_id", msg.sqsMessageID()),
		slog.Int("receive_count", receiveCount),
	)

	if d.metrics != nil {
		emitMetric(d.log, d.metrics.IncDLQ, d.queueName)
	}

	if d.dlq.OnDLQ != nil {
		d.dlq.OnDLQ(ctx, msg)
	}

	return true
}

// receiveCount returns the message's ApproximateReceiveCount system attribute as
// an int. A missing or non-numeric value yields zero, so a message that never
// reports a receive count is never treated as exhausted.
func receiveCount(msg *message) int {
	n, err := strconv.Atoi(msg.SystemAttributeByKey(approximateReceiveCountKey))
	if err != nil {
		return 0
	}
	return n
}

// deleteMessage commits msg, removing it from the queue. When batched deletes
// are enabled it hands the receipt handle to the deleteBatcher and returns
// without waiting for the delete to complete; it blocks only while the
// Pending_Delete queue is full. Delete failures on that path are logged by the
// batcher. Otherwise it issues a synchronous DeleteMessage; a failure is logged
// at Error level and swallowed so a transient API error never crashes the
// worker. Either way a failed delete leaves the message in the queue for
// redelivery after its visibility timeout.
func (d *dispatcher) deleteMessage(ctx context.Context, msg *message) {
	if d.batcher != nil {
		d.batcher.enqueue(msg.Identifier())
		return
	}

	_, err := d.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(d.queueURL),
		ReceiptHandle: aws.String(msg.Identifier()),
	})
	if err != nil {
		d.log.Error("failed to delete message",
			slog.String("queue_url", d.queueURL),
			slog.String("receipt_handle", msg.Identifier()),
			slog.Any("error", err),
		)
	}
}
