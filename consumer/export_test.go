package consumer

import (
	"container/heap"
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// MaxDeleteBatch exposes maxDeleteBatch to external tests.
const MaxDeleteBatch = maxDeleteBatch

// DeleteRequestTimeout exposes deleteRequestTimeout to external tests.
const DeleteRequestTimeout = deleteRequestTimeout

// DeleteBatcher exposes the unexported deleteBatcher to external tests.
type DeleteBatcher struct {
	b *deleteBatcher
}

// NewDeleteBatcher wraps newDeleteBatcher.
func NewDeleteBatcher(client BatchDeleteClient, queueURL string, capacity int, log *slog.Logger) *DeleteBatcher {
	return &DeleteBatcher{b: newDeleteBatcher(client, queueURL, capacity, log)}
}

// Start wraps deleteBatcher.start.
func (d *DeleteBatcher) Start(ctx context.Context, senders int) {
	d.b.start(ctx, senders)
}

// Enqueue wraps deleteBatcher.enqueue.
func (d *DeleteBatcher) Enqueue(handle string) {
	d.b.enqueue(handle)
}

// Close wraps deleteBatcher.close.
func (d *DeleteBatcher) Close() {
	d.b.close()
}

// Send wraps deleteBatcher.send.
func (d *DeleteBatcher) Send(ctx context.Context, batch []string) {
	d.b.send(ctx, batch)
}

// MaxVisibilityBatch exposes the per-request entry limit of
// ChangeMessageVisibilityBatch to external tests.
const MaxVisibilityBatch = maxVisibilityBatch

// VisibilityRequestTimeout exposes the per-request visibility timeout bound to
// external tests.
const VisibilityRequestTimeout = visibilityRequestTimeout

// TestMessage aliases the internal message type so external tests can build and
// inspect messages handed to internal components.
type TestMessage = message

// NewTestMessage builds a message with the given receipt handle, MessageGroupId
// system attribute, native user string attributes, and raw body.
func NewTestMessage(handle, groupID string, userAttrs map[string]string, body string) *TestMessage {
	raw := types.Message{
		ReceiptHandle: aws.String(handle),
		Body:          aws.String(body),
		Attributes:    map[string]string{},
	}
	if groupID != "" {
		raw.Attributes[messageGroupIDKey] = groupID
	}
	if len(userAttrs) > 0 {
		raw.MessageAttributes = make(map[string]types.MessageAttributeValue, len(userAttrs))
		for k, v := range userAttrs {
			raw.MessageAttributes[k] = types.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(v),
			}
		}
	}
	return newMessage(raw)
}

// GroupKey exposes the PerGroupID Group_Key derivation to external tests.
func GroupKey(msg *TestMessage, fields []string) string {
	return groupKey(msg, fields)
}

// CreditPool aliases the internal credit pool for external tests.
type CreditPool = creditPool

// NewCreditPool builds a credit pool with the given capacity.
func NewCreditPool(capacity int) *CreditPool {
	return newCreditPool(capacity)
}

// Acquire takes n credits, blocking until available or ctx is done.
func (p *creditPool) Acquire(ctx context.Context, n int) error {
	_, err := p.acquireUpTo(ctx, n, nil)
	return err
}

// Release returns n credits.
func (p *creditPool) Release(n int) {
	p.release(n)
}

// Available reports the number of free credits.
func (p *creditPool) Available() int {
	return len(p.tokens)
}

// Capacity reports the total number of credits.
func (p *creditPool) Capacity() int {
	return cap(p.tokens)
}

// WorkSource wraps an internal workSource for external tests.
type WorkSource struct {
	ws workSource
}

// NewSharedQueue builds the Parallel-mode work source with the given capacity.
func NewSharedQueue(capacity int) *WorkSource {
	return &WorkSource{ws: newSharedQueue(capacity, nil)}
}

// NewGroupScheduler builds the PerGroupID-mode work source with the given
// capacity and custom group fields.
func NewGroupScheduler(capacity int, fields []string) *WorkSource {
	return &WorkSource{ws: newGroupScheduler(capacity, fields, nil)}
}

// Submit adds a Pending_Message.
func (w *WorkSource) Submit(msg *TestMessage) {
	w.ws.submit(msg)
}

// Take blocks until a message can be started or ctx is done.
func (w *WorkSource) Take(ctx context.Context) (*TestMessage, bool) {
	return w.ws.take(ctx)
}

// Complete reports an Outcome and returns the held messages.
func (w *WorkSource) Complete(msg *TestMessage, failed bool) []*TestMessage {
	return w.ws.complete(msg, failed)
}

// Drain removes and returns every Pending_Message.
func (w *WorkSource) Drain() []*TestMessage {
	return w.ws.drain()
}

// GroupCount reports how many groups a group scheduler keeps state for, or -1
// for a work source that is not a group scheduler.
func (w *WorkSource) GroupCount() int {
	gs, ok := w.ws.(*groupScheduler)
	if !ok {
		return -1
	}
	gs.mu.Lock()
	defer gs.mu.Unlock()
	return len(gs.groups)
}

// VisibilityResult is the outcome of one change sent by a VisibilitySender.
type VisibilityResult struct {
	Err         error
	Code        string
	Message     string
	Failed      bool
	SenderFault bool
	PerEntry    bool
}

// VisibilitySender wraps an internal visibilitySender for external tests.
type VisibilitySender struct {
	s visibilitySender
}

// NewVisibilitySender builds the sender the consumer would pick for client.
func NewVisibilitySender(client SQSClient, queueURL string, log *slog.Logger) *VisibilitySender {
	return &VisibilitySender{s: newVisibilitySender(client, queueURL, log)}
}

// IsBatch reports whether the sender uses ChangeMessageVisibilityBatch.
func (v *VisibilitySender) IsBatch() bool {
	_, ok := v.s.(*batchVisibilitySender)
	return ok
}

// Send sends one change per message with the matching timeout and returns one
// result per change, in order.
func (v *VisibilitySender) Send(ctx context.Context, msgs []*TestMessage, timeouts []int32) []VisibilityResult {
	changes := make([]visibilityChange, len(msgs))
	for i, m := range msgs {
		changes[i] = visibilityChange{msg: m, timeout: timeouts[i]}
	}
	failures := v.s.send(ctx, changes)
	out := make([]VisibilityResult, len(failures))
	for i, f := range failures {
		if f == nil {
			continue
		}
		out[i] = VisibilityResult{
			Failed:      true,
			Err:         f.err,
			Code:        f.code,
			Message:     f.message,
			SenderFault: f.senderFault,
			PerEntry:    f.isPerEntry,
		}
	}
	return out
}

// RenewalMargin exposes how long before its deadline an Extension becomes due.
const RenewalMargin = renewalMargin

// CoalesceWindow exposes how far ahead of its due instant an Extension may be
// sent to share a batch.
const CoalesceWindow = coalesceWindow

// ExtensionRetryDelay exposes the spacing between retries of a failed
// Extension.
const ExtensionRetryDelay = extensionRetryDelay

// StartMargin exposes the least visibility a message with no extension left
// must have to start.
const StartMargin = startMargin

// MaxConcurrentVisibilityRequests exposes how many visibility requests the
// manager keeps in flight at once.
const MaxConcurrentVisibilityRequests = maxConcurrentVisibilityRequests

// MaxVisibilitySeconds exposes the 12-hour visibility ceiling in seconds.
const MaxVisibilitySeconds = maxVisibilitySeconds

// SetReceivedAt sets the Receive_Time of a test message.
func SetReceivedAt(msg *TestMessage, at time.Time) {
	msg.receivedAt = at
}

// BackoffDelay returns the delay recorded by the last Backoff call.
func BackoffDelay(msg *TestMessage) time.Duration {
	return msg.backoffDelay()
}

// VisibilityManager wraps the internal visibility manager for external tests.
type VisibilityManager struct {
	vm *visibilityManager
}

// NewVisibilityManager builds a visibility manager for client and queueURL with
// the given visibility timeout in seconds and extension limit.
func NewVisibilityManager(
	client SQSClient,
	queueURL string,
	visibilityTimeout int32,
	extensionLimit int,
	log *slog.Logger,
) *VisibilityManager {
	return &VisibilityManager{vm: newVisibilityManager(client, queueURL, visibilityTimeout, extensionLimit, log)}
}

// Start launches the manager goroutine on ctx.
func (v *VisibilityManager) Start(ctx context.Context) {
	v.vm.start(ctx)
}

// Close flushes queued changes and stops the manager goroutine.
func (v *VisibilityManager) Close() {
	v.vm.close()
}

// Track starts tracking msg from its Receive_Time.
func (v *VisibilityManager) Track(msg *TestMessage) {
	v.vm.track(msg)
}

// TrackActive starts tracking msg from its Receive_Time and marks it active at
// that instant, as if a worker had started it right after the receive. It does
// what track does, but creates the entry already active under the same lock, so
// the manager goroutine can never send an uncounted pending Extension between
// the track and the start.
func (v *VisibilityManager) TrackActive(msg *TestMessage) {
	v.vm.mu.Lock()
	v.vm.untrackLocked(msg)
	if msg.receivedAt.IsZero() {
		msg.receivedAt = time.Now()
	}
	e := &trackedEntry{
		msg:      msg,
		deadline: msg.receivedAt.Add(v.vm.visibility),
		index:    -1,
		isActive: true,
	}
	e.due = e.deadline.Add(-renewalMargin)
	v.vm.entries[msg] = e
	if v.vm.extensionLimit > 0 {
		heap.Push(&v.vm.heap, e)
	}
	v.vm.mu.Unlock()
	v.vm.signal()
}

// Untrack stops tracking msg.
func (v *VisibilityManager) Untrack(msg *TestMessage) {
	v.vm.untrack(msg)
}

// Backoff stops tracking msg and queues a change to delay.
func (v *VisibilityManager) Backoff(msg *TestMessage, delay time.Duration) {
	v.vm.backoff(msg, delay)
}

// Release stops tracking msgs and queues a change to zero for each.
func (v *VisibilityManager) Release(msgs ...*TestMessage) {
	v.vm.release(msgs...)
}

// Tracked reports how many messages the manager tracks.
func (v *VisibilityManager) Tracked() int {
	v.vm.mu.Lock()
	defer v.vm.mu.Unlock()
	return len(v.vm.entries)
}

// IdleWorkers aliases the internal idle-worker tracker for external tests.
type IdleWorkers = idleWorkers

// NewIdleWorkers builds an idle-worker tracker with no idle worker.
func NewIdleWorkers() *IdleWorkers {
	return newIdleWorkers()
}

// Enter records a worker that starts waiting with nothing to run.
func (w *idleWorkers) Enter() {
	w.enter()
}

// Leave records that a worker stopped waiting.
func (w *idleWorkers) Leave() {
	w.leave()
}

// IsAny reports whether a worker is waiting with nothing to run.
func (w *idleWorkers) IsAny() bool {
	return w.isAny()
}

// AcquireUpTo takes between 1 and n credits, stopping early once it holds a
// credit and idle reports an idle worker.
func (p *creditPool) AcquireUpTo(ctx context.Context, n int, idle *IdleWorkers) (int, error) {
	return p.acquireUpTo(ctx, n, idle)
}

// NewSharedQueueWithIdle builds the Parallel-mode work source that records idle
// workers in idle.
func NewSharedQueueWithIdle(capacity int, idle *IdleWorkers) *WorkSource {
	return &WorkSource{ws: newSharedQueue(capacity, idle)}
}

// NewGroupSchedulerWithIdle builds the PerGroupID-mode work source that records
// idle workers in idle.
func NewGroupSchedulerWithIdle(capacity int, fields []string, idle *IdleWorkers) *WorkSource {
	return &WorkSource{ws: newGroupScheduler(capacity, fields, idle)}
}

// IsExpired reports whether msg is tracked and must not be started at now,
// without marking it active.
func (v *VisibilityManager) IsExpired(msg *TestMessage, now time.Time) bool {
	v.vm.mu.Lock()
	defer v.vm.mu.Unlock()
	e, ok := v.vm.entries[msg]
	return ok && v.vm.isExpiredLocked(e, now)
}

// Begin wraps visibilityManager.begin: it reports whether msg may start at now
// and, when it may, marks it active.
func (v *VisibilityManager) Begin(msg *TestMessage, now time.Time) bool {
	return v.vm.begin(msg, now)
}

// SetMessageID sets the SQS MessageId of a test message.
func SetMessageID(msg *TestMessage, id string) {
	msg.original.MessageId = &id
}

// SetReceiveCount sets the ApproximateReceiveCount system attribute of a test
// message.
func SetReceiveCount(msg *TestMessage, n int) {
	if msg.original.Attributes == nil {
		msg.original.Attributes = map[string]string{}
	}
	msg.original.Attributes[approximateReceiveCountKey] = strconv.Itoa(n)
}
