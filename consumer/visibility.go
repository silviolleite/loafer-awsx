package consumer

import (
	"container/heap"
	"context"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/silviolleite/loafer-awsx/logger"
)

const (
	// renewalMargin is how long before its Visibility_Deadline an Extension of
	// a Tracked_Message becomes due.
	renewalMargin = 10 * time.Second

	// coalesceWindow is how far ahead of its due instant an Extension may be
	// sent so that it shares a batch request with an Extension already due.
	coalesceWindow = 5 * time.Second

	// extensionRetryDelay spaces out retries of a failed Extension so a
	// persistent failure does not turn the manager into a busy loop.
	extensionRetryDelay = time.Second

	// maxVisibilitySeconds is the AWS SQS ceiling on a visibility timeout. It
	// also bounds the total time a message can stay invisible, counted from
	// its receive.
	maxVisibilitySeconds = int64(12 * 60 * 60)
)

// changeKind tells the manager how to apply the result of a sent change.
type changeKind int

const (
	// changeImmediate is a backoff change or a shutdown release.
	changeImmediate changeKind = iota
	// changeExtension is a deadline-driven Extension of a Tracked_Message.
	changeExtension
)

// trackedEntry is the visibility state of one Tracked_Message.
type trackedEntry struct {
	msg        *message
	deadline   time.Time
	due        time.Time
	extensions int
	index      int
}

// sentChange pairs a change with the data needed to apply its result.
type sentChange struct {
	entry  *trackedEntry
	sentAt time.Time
	change visibilityChange
	kind   changeKind
}

// visibilityManager is the single per-Consumer component that keeps every
// admitted message invisible while it waits and while it runs.
//
// Each message is tracked from its Receive_Time: its Visibility_Deadline starts
// at Receive_Time plus the visibility timeout, and its next Extension is due
// renewalMargin before that deadline. One goroutine keeps the tracked entries
// in a min-heap ordered by due instant, sleeps until the earliest one, and then
// sends every Extension due within coalesceWindow in batch requests of up to
// ten entries. The k-th Extension of a message sets its visibility timeout to k
// times the route visibility timeout, capped so the message never stays
// invisible longer than 12 hours after its receive. At most extensionLimit
// Extensions are sent per message.
//
// Backoff changes and shutdown releases are queued as immediate changes and
// sent by the same goroutine, so they are batched as well. Untracking a message
// removes it from the heap at once, so no later request includes it.
//
// All state is guarded by mu. Callers only take mu for short critical sections
// and never wait for an AWS request; the goroutine sends requests outside the
// lock. The goroutine runs on a context that the caller detaches from the
// Consumer run context, because Active_Messages still need Extensions after
// cancellation, and every request is bounded by visibilityRequestTimeout.
type visibilityManager struct {
	send              visibilitySender
	log               *slog.Logger
	entries           map[*message]*trackedEntry
	wake              chan struct{}
	queueURL          string
	heap              entryHeap
	immediate         []visibilityChange
	visibility        time.Duration
	extensionLimit    int
	wg                sync.WaitGroup
	visibilitySeconds int32
	mu                sync.Mutex
	isClosing         bool
}

// newVisibilityManager builds a manager for one queue. It sends changes through
// ChangeMessageVisibilityBatch when client implements BatchVisibilityClient and
// through ChangeMessageVisibility otherwise. visibilityTimeout is the route
// visibility timeout in seconds and extensionLimit the maximum number of
// Extensions per message. A nil logger is replaced with a no-op logger.
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
		send:              newVisibilitySender(client, queueURL, log),
		log:               log,
		entries:           make(map[*message]*trackedEntry),
		wake:              make(chan struct{}, 1),
		queueURL:          queueURL,
		visibility:        time.Duration(visibilityTimeout) * time.Second,
		visibilitySeconds: visibilityTimeout,
		extensionLimit:    extensionLimit,
	}
}

// start launches the manager goroutine. ctx must not be canceled by the
// Consumer run context; the goroutine stops only through close.
func (v *visibilityManager) start(ctx context.Context) {
	v.wg.Add(1)
	go v.run(ctx)
}

// close sends every queued backoff change and release, then stops the
// goroutine and waits for it to exit. Messages still tracked are not extended
// any further.
func (v *visibilityManager) close() {
	v.mu.Lock()
	v.isClosing = true
	v.mu.Unlock()
	v.signal()
	v.wg.Wait()
}

// track starts tracking msg with a Visibility_Deadline of its Receive_Time
// plus the visibility timeout. A message without a Receive_Time is stamped with
// the current time. Tracking a message again replaces its previous entry.
func (v *visibilityManager) track(msg *message) {
	v.mu.Lock()
	v.untrackLocked(msg)
	if msg.receivedAt.IsZero() {
		msg.receivedAt = time.Now()
	}
	e := &trackedEntry{
		msg:      msg,
		deadline: msg.receivedAt.Add(v.visibility),
		index:    -1,
	}
	e.due = e.deadline.Add(-renewalMargin)
	v.entries[msg] = e
	if v.extensionLimit > 0 {
		heap.Push(&v.heap, e)
	}
	v.mu.Unlock()
	v.signal()
}

// untrack stops tracking msg. After untrack returns, no request built later
// includes msg unless it is queued again through backoff or release.
func (v *visibilityManager) untrack(msg *message) {
	v.mu.Lock()
	v.untrackLocked(msg)
	v.mu.Unlock()
}

// backoff stops tracking msg and queues a change of its visibility timeout to
// delay, in whole seconds clamped to [0, maxVisibilitySeconds].
func (v *visibilityManager) backoff(msg *message, delay time.Duration) {
	v.mu.Lock()
	v.untrackLocked(msg)
	v.immediate = append(v.immediate, visibilityChange{msg: msg, timeout: backoffSeconds(delay)})
	v.mu.Unlock()
	v.signal()
}

// release stops tracking every msg and queues a change of its visibility
// timeout to zero, making it visible again at once.
func (v *visibilityManager) release(msgs ...*message) {
	if len(msgs) == 0 {
		return
	}
	v.mu.Lock()
	for _, msg := range msgs {
		v.untrackLocked(msg)
		v.immediate = append(v.immediate, visibilityChange{msg: msg})
	}
	v.mu.Unlock()
	v.signal()
}

// isExpired reports whether msg is tracked and its Visibility_Deadline is not
// after now, which means SQS may already have made it visible again.
func (v *visibilityManager) isExpired(msg *message, now time.Time) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	e, ok := v.entries[msg]
	return ok && !e.deadline.After(now)
}

// untrackLocked removes msg from the entries and the heap. v.mu must be held.
func (v *visibilityManager) untrackLocked(msg *message) {
	e, ok := v.entries[msg]
	if !ok {
		return
	}
	delete(v.entries, msg)
	if e.index >= 0 {
		heap.Remove(&v.heap, e.index)
	}
}

// signal wakes the goroutine without blocking.
func (v *visibilityManager) signal() {
	select {
	case v.wake <- struct{}{}:
	default:
	}
}

// run is the manager loop. It sends every change that is ready, then sleeps
// until the next Extension is due or until it is woken, and returns once close
// was called and no immediate change is left.
func (v *visibilityManager) run(ctx context.Context) {
	defer v.wg.Done()

	timer := time.NewTimer(time.Hour)
	defer timer.Stop()

	for {
		batch, next, isDone := v.collect(time.Now())
		if len(batch) > 0 {
			v.dispatch(ctx, batch)
			continue
		}
		if isDone {
			return
		}

		if next.IsZero() {
			<-v.wake
			continue
		}

		timer.Reset(time.Until(next))
		select {
		case <-v.wake:
		case <-timer.C:
		}
	}
}

// collect takes every queued immediate change and every Extension that is due
// or due within coalesceWindow of now. It reports the next due instant, or the
// zero time when nothing is tracked, and whether the loop should stop.
func (v *visibilityManager) collect(now time.Time) ([]sentChange, time.Time, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()

	batch := make([]sentChange, 0, len(v.immediate))
	for _, change := range v.immediate {
		batch = append(batch, sentChange{change: change, kind: changeImmediate, sentAt: now})
	}
	v.immediate = v.immediate[:0]

	if v.isClosing {
		return batch, time.Time{}, len(batch) == 0
	}

	if len(v.heap) > 0 && !v.heap[0].due.After(now) {
		batch = v.collectDueLocked(now, batch)
	}

	if len(batch) > 0 || len(v.heap) == 0 {
		return batch, time.Time{}, false
	}

	return batch, v.heap[0].due, false
}

// collectDueLocked pops every entry due within coalesceWindow of now and turns
// the ones that can still move their deadline later into Extensions. v.mu must
// be held.
func (v *visibilityManager) collectDueLocked(now time.Time, batch []sentChange) []sentChange {
	horizon := now.Add(coalesceWindow)
	var deferred []*trackedEntry

	for len(v.heap) > 0 && !v.heap[0].due.After(horizon) {
		e, ok := heap.Pop(&v.heap).(*trackedEntry)
		if !ok {
			continue
		}

		timeout, canExtend := v.nextTimeout(e, now)
		if !canExtend {
			if e.due.After(now) {
				deferred = append(deferred, e)
			}
			continue
		}

		batch = append(batch, sentChange{
			entry:  e,
			change: visibilityChange{msg: e.msg, timeout: timeout},
			kind:   changeExtension,
			sentAt: now,
		})
	}

	for _, e := range deferred {
		heap.Push(&v.heap, e)
	}

	return batch
}

// nextTimeout returns the visibility timeout of the next Extension of e and
// whether sending it now moves the deadline later. The k-th Extension asks for
// k times the visibility timeout, capped by what is left of the 12-hour budget
// counted from the message receive.
func (v *visibilityManager) nextTimeout(e *trackedEntry, now time.Time) (int32, bool) {
	k := int64(e.extensions + 1)
	want := k * int64(v.visibilitySeconds)

	elapsed := now.Sub(e.msg.receivedAt)
	elapsedSeconds := int64((elapsed + time.Second - 1) / time.Second)
	remaining := maxVisibilitySeconds - elapsedSeconds

	timeout := min(want, remaining)
	if timeout <= 0 {
		return 0, false
	}
	if !now.Add(time.Duration(timeout) * time.Second).After(e.deadline) {
		return 0, false
	}

	return int32(timeout), true
}

// dispatch sends batch in requests of up to maxVisibilityBatch changes and
// applies each result.
func (v *visibilityManager) dispatch(ctx context.Context, batch []sentChange) {
	for start := 0; start < len(batch); start += maxVisibilityBatch {
		chunk := batch[start:min(start+maxVisibilityBatch, len(batch))]

		changes := make([]visibilityChange, len(chunk))
		for i := range chunk {
			changes[i] = chunk[i].change
		}

		failures := v.send.send(ctx, changes)
		v.apply(chunk, failures)
	}
}

// apply records the result of each sent change. A successful Extension moves
// the deadline and, below the limit, schedules the next one. A failed
// Extension keeps its deadline and is retried after extensionRetryDelay while
// the deadline has not passed. Failures are logged, except for Extensions of
// messages untracked while the request was in flight.
func (v *visibilityManager) apply(chunk []sentChange, failures []*visibilityFailure) {
	now := time.Now()

	v.mu.Lock()
	defer v.mu.Unlock()

	for i := range chunk {
		sc := &chunk[i]
		var failure *visibilityFailure
		if i < len(failures) {
			failure = failures[i]
		}

		if sc.kind == changeImmediate {
			if failure != nil {
				v.logFailure(sc.change, failure)
			}
			continue
		}

		e := sc.entry
		if v.entries[e.msg] != e {
			continue
		}

		if failure != nil {
			v.logFailure(sc.change, failure)
			if retryAt := now.Add(extensionRetryDelay); retryAt.Before(e.deadline) {
				e.due = retryAt
				heap.Push(&v.heap, e)
			}
			continue
		}

		e.extensions++
		e.deadline = sc.sentAt.Add(time.Duration(sc.change.timeout) * time.Second)
		e.due = e.deadline.Add(-renewalMargin)
		if e.extensions < v.extensionLimit {
			heap.Push(&v.heap, e)
		}
	}
}

// logFailure writes one Error record for a failed visibility change.
func (v *visibilityManager) logFailure(change visibilityChange, failure *visibilityFailure) {
	changeAttrs := []any{
		slog.String("queue_url", v.queueURL),
		slog.String("receipt_handle", change.msg.Identifier()),
		slog.Int("visibility_timeout", int(change.timeout)),
	}
	attrs := slices.Concat(changeAttrs, failure.logAttrs())
	v.log.Error("failed to change message visibility", attrs...)
}

// backoffSeconds converts a backoff delay to whole seconds clamped to
// [0, maxVisibilitySeconds].
func backoffSeconds(delay time.Duration) int32 {
	seconds := int64(delay / time.Second)
	switch {
	case seconds < 0:
		return 0
	case seconds > maxVisibilitySeconds:
		return int32(maxVisibilitySeconds)
	default:
		return int32(seconds)
	}
}

// entryHeap is a min-heap of tracked entries ordered by due instant. It
// implements heap.Interface and keeps each entry's index current so entries
// can be removed in O(log n).
type entryHeap []*trackedEntry

// Len reports the number of entries.
func (h entryHeap) Len() int { return len(h) }

// Less orders entries by due instant.
func (h entryHeap) Less(i, j int) bool { return h[i].due.Before(h[j].due) }

// Swap swaps two entries and updates their indexes.
func (h entryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

// Push appends an entry; heap.Push restores the order.
func (h *entryHeap) Push(x any) {
	e, ok := x.(*trackedEntry)
	if !ok {
		return
	}
	e.index = len(*h)
	*h = append(*h, e)
}

// Pop removes the last entry; heap.Pop moves the minimum there first.
func (h *entryHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	e.index = -1
	*h = old[:n-1]
	return e
}
