package consumer

import (
	"context"
	"hash/fnv"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"

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
)

// dispatcher owns the per-route worker pool. It fans received messages out to a
// fixed set of worker goroutines, each draining a buffered channel, and drives
// the full processing lifecycle of every message: starting the visibility
// extension goroutine, invoking the middleware-wrapped handler, and applying
// the outcome (delete on success, leave on error, change visibility on
// backoff).
//
// A dispatcher is created with newDispatcher, started with start, fed with
// dispatch, and torn down with stop. start, dispatch, and stop must be called
// from a single goroutine (the polling loop); the worker and visibility
// goroutines they manage are internally synchronized and leak-free.
type dispatcher struct {
	client            SQSClient
	handler           middleware.Handler
	visibility        *visibilityManager
	log               *slog.Logger
	dlq               *router.DLQConfig
	metrics           MetricsRecorder
	retryScheduler    *retryScheduler
	dlqPublisher      *dlqPublisher
	scheduledRetry    *router.ScheduledRetryConfig
	queueURL          string
	queueName         string
	channels          []chan *message
	randIntN          func(int) int
	customGroupFields []string
	mode              router.Mode
	retryModel        router.RetryModel
	workerPoolSize    int
	bufferSize        int
	wg                sync.WaitGroup
}

// newDispatcher builds a dispatcher for a route. The route handler is wrapped
// with the global middlewares (outermost) followed by the route middlewares,
// preserving first-is-outermost semantics. queueURL is the resolved URL of the
// route queue and vm is the shared visibility manager used to extend visibility
// while each message is in flight. A nil logger is replaced with a no-op logger.
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

	workers := route.WorkerPoolSize()
	if workers < 1 {
		workers = 1
	}

	buffer := int(route.MaxMessages())
	if buffer < 1 {
		buffer = 1
	}

	d := &dispatcher{
		client:            client,
		handler:           handler,
		visibility:        vm,
		log:               log,
		dlq:               route.DLQ(),
		queueURL:          queueURL,
		queueName:         route.QueueName(),
		randIntN:          rand.IntN,
		customGroupFields: route.CustomGroupFields(),
		mode:              route.RunMode(),
		retryModel:        route.RetryModel(),
		workerPoolSize:    workers,
		bufferSize:        buffer,
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

// start launches the worker pool. It allocates one buffered channel and one
// worker goroutine per configured worker, each processing messages until its
// channel is closed by stop. start must be called exactly once before dispatch.
func (d *dispatcher) start(ctx context.Context) {
	d.channels = make([]chan *message, d.workerPoolSize)
	for i := range d.channels {
		d.channels[i] = make(chan *message, d.bufferSize)
		d.wg.Add(1)
		go d.worker(ctx, d.channels[i])
	}
}

// dispatch assigns msg to a worker and enqueues it for processing. The worker
// index is chosen by run mode: Parallel picks a random worker, PerGroupID picks
// a worker by hashing the message group key so that a given group is always
// handled by the same worker. dispatch returns early without enqueuing when ctx
// is canceled, so it never blocks past shutdown.
func (d *dispatcher) dispatch(ctx context.Context, msg *message) {
	index := d.workerIndex(msg)

	select {
	case <-ctx.Done():
	case d.channels[index] <- msg:
	}
}

// stop closes every worker channel and waits for all worker and visibility
// goroutines to finish. After stop returns no goroutine started by the
// dispatcher remains, satisfying the leak-free guarantee. stop must be called
// exactly once after the polling loop stops feeding dispatch.
func (d *dispatcher) stop() {
	for _, ch := range d.channels {
		close(ch)
	}
	d.wg.Wait()
}

// worker drains ch, processing each message in turn until the channel is closed.
func (d *dispatcher) worker(ctx context.Context, ch <-chan *message) {
	defer d.wg.Done()
	for msg := range ch {
		d.process(ctx, msg)
	}
}

// process runs the full lifecycle for a single message. In PerGroupID mode under
// the Visibility retry model, when an earlier message of this message's FIFO
// group has already failed in the same batch, the message is held back before
// any work starts: its handler never runs and it is left in the queue so the
// group is redelivered in order. Otherwise it starts a visibility extension
// goroutine, invokes the middleware-wrapped handler, and then applies the
// outcome:
//   - backoff: the message is left in the queue; the visibility manager
//     consumes the backoff signal and changes visibility to the backoff
//     duration. The message is not deleted. In PerGroupID mode the group is
//     marked failed so the rest of the group in the batch is held back.
//   - error, message exhausted: when a DLQ route option is configured and the
//     message's ApproximateReceiveCount has reached MaxReceiveCount, the message
//     is treated as exhausted. Observability signals are emitted and the message
//     is left in the source queue so AWS SQS performs the redrive natively; the
//     library never publishes or deletes it for DLQ purposes.
//   - error: the error is logged with message body, group ID, and receipt
//     handle, and the message is left in the queue for redelivery. In PerGroupID
//     mode the group is marked failed so the rest of the group in the batch is
//     held back.
//   - success: the message is deleted from the queue.
//
// The visibility goroutine is tracked by the dispatcher wait group and always
// terminates: on backoff it stops after reading the backoff signal, otherwise
// Dispatch closes the dispatch signal to stop it immediately.
func (d *dispatcher) process(ctx context.Context, msg *message) {
	if d.orderedGroups() && msg.barrier.failed(msg.groupKey) {
		d.holdForOrder(msg)
		return
	}

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.visibility.run(ctx, msg)
	}()

	err := d.handler(ctx, msg)

	if d.retryModel == router.ScheduledRetryModel {
		d.processScheduled(ctx, msg, err)
		return
	}

	if msg.BackedOff() {
		d.failGroup(msg)
		return
	}

	msg.Dispatch()

	if err != nil {
		d.failGroup(msg)

		if d.observeDLQ(ctx, msg) {
			return
		}

		d.log.Error("handler returned an error",
			slog.String("queue_url", d.queueURL),
			slog.String("receipt_handle", msg.Identifier()),
			slog.String("group_id", msg.SystemAttributeByKey(messageGroupIDKey)),
			slog.String("body", string(msg.Body())),
			slog.Any("error", err),
		)
		return
	}

	d.deleteMessage(ctx, msg)
}

// failGroup records the message's group as failed on the batch barrier so that,
// in PerGroupID mode, every later message of the same group in the batch is held
// back to preserve order. It is a no-op unless per-group ordering is active.
func (d *dispatcher) failGroup(msg *message) {
	if d.orderedGroups() {
		msg.barrier.fail(msg.groupKey)
	}
}

// holdForOrder handles a message whose FIFO group already failed earlier in the
// batch. To preserve group order the message must not run its handler and must
// not be deleted: it is left in the queue with its receive-time visibility
// timeout so AWS SQS redelivers the whole group in order once the failed head
// becomes visible again. No visibility goroutine is started for a held message,
// so nothing is leaked.
func (d *dispatcher) holdForOrder(msg *message) {
	d.log.Warn("holding message to preserve FIFO group order; earlier message in group failed",
		slog.String("queue_url", d.queueURL),
		slog.String("receipt_handle", msg.Identifier()),
		slog.String("group_id", msg.SystemAttributeByKey(messageGroupIDKey)),
	)
}

// processScheduled applies the Scheduled Retry model outcome for a message after
// its handler returns. It first calls msg.Dispatch to stop the visibility
// goroutine so no backoff-driven ChangeMessageVisibility is issued, then:
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
	msg.Dispatch()

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
// returned an error and reports whether it treated the message as exhausted.
//
// It is a no-op returning false unless a DLQ route option is configured and the
// message's ApproximateReceiveCount has reached the route's MaxReceiveCount. The
// DLQ model is strictly observe-only: AWS SQS performs the actual redrive per
// its own redrive policy, so the message is left untouched in the source queue —
// it is never published to any destination and never deleted for DLQ purposes.
//
// When the message is exhausted it logs at Error level (message identifier,
// queue name, receive count), increments loafer_messages_dlq_total through the
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

// deleteMessage removes msg from the queue. A failure is logged at Error level
// and swallowed so a transient API error never crashes the worker.
func (d *dispatcher) deleteMessage(ctx context.Context, msg *message) {
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

// workerIndex selects the worker that will process msg. In Parallel mode the
// worker is chosen at random; in PerGroupID mode it is chosen by hashing the
// message group key so equal keys always map to the same worker. In PerGroupID
// mode the computed group key is also cached on the message so the group
// barrier can key on it without recomputing.
func (d *dispatcher) workerIndex(msg *message) int {
	if d.mode == router.PerGroupID {
		key := d.groupKey(msg)
		msg.groupKey = key
		return d.groupIndex(key)
	}
	return d.randIntN(d.workerPoolSize)
}

// orderedGroups reports whether per-group FIFO ordering must be preserved on
// failure. PerGroupID dispatch exists to keep each message group in order, so
// under the Visibility retry model the group barrier is always engaged: a failed
// message holds back the rest of its group. The Scheduled Retry model unblocks
// groups by design and is therefore excluded.
func (d *dispatcher) orderedGroups() bool {
	return d.mode == router.PerGroupID &&
		d.retryModel == router.VisibilityRetryModel
}

// newBatchBarrier returns the group barrier shared by a received batch, or nil
// when per-group ordering is not preserved (any mode other than PerGroupID, or
// the Scheduled Retry model). A nil barrier disables all hold-back behavior, so
// the fast path pays nothing.
func (d *dispatcher) newBatchBarrier() *groupBarrier {
	if !d.orderedGroups() {
		return nil
	}
	return newGroupBarrier()
}

// groupIndex maps a group key to a worker index using a stable 32-bit FNV-1a
// hash modulo the worker pool size, yielding deterministic per-group affinity.
func (d *dispatcher) groupIndex(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(d.workerPoolSize))
}

// groupKey builds the routing key for PerGroupID mode by joining the message
// group ID with the configured custom group fields. Each field is read from the
// native SQS user message attributes first (where SNS places them under raw
// message delivery) and falls back to the SNS-envelope attributes, so custom
// group fields work under both delivery modes. The separator keeps distinct
// field combinations from colliding.
func (d *dispatcher) groupKey(msg *message) string {
	parts := make([]string, 0, len(d.customGroupFields)+1)
	parts = append(parts, msg.SystemAttributeByKey(messageGroupIDKey))
	for _, field := range d.customGroupFields {
		value := msg.UserMessageAttribute(field)
		if value == "" {
			value = msg.Attribute(field)
		}
		parts = append(parts, value)
	}
	return strings.Join(parts, groupKeySeparator)
}
