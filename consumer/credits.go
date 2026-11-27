package consumer

import (
	"context"
	"sync/atomic"
)

// creditPool bounds the number of Admitted_Messages a Consumer holds. One
// credit is held for every message returned by ReceiveMessage until that
// message reaches an Outcome.
//
// Acquirers are serialized through turn so that a multi-credit acquire is never
// interleaved with another one. Without it, two pollers could each hold part of
// the capacity while nothing in flight is left to free more, and both would
// wait forever.
type creditPool struct {
	tokens chan struct{}
	turn   chan struct{}
}

// newCreditPool builds a pool holding capacity free credits. A capacity below 1
// is raised to 1 so a pool can always admit at least one message.
func newCreditPool(capacity int) *creditPool {
	if capacity < 1 {
		capacity = 1
	}

	p := &creditPool{
		tokens: make(chan struct{}, capacity),
		turn:   make(chan struct{}, 1),
	}
	for range capacity {
		p.tokens <- struct{}{}
	}

	return p
}

// acquireUpTo takes between 1 and n credits and reports how many it took. It
// waits for n credits, except that once it holds at least one credit it stops
// waiting as soon as idle reports a worker with nothing to run, so an idle
// worker never waits for a full batch. A nil idle always waits for n credits.
// On cancellation it returns the credits taken so far to the pool and reports
// ctx.Err().
func (p *creditPool) acquireUpTo(ctx context.Context, n int, idle *idleWorkers) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case p.turn <- struct{}{}:
	}
	defer func() { <-p.turn }()

	taken := 0
	for taken < n {
		select {
		case <-p.tokens:
			taken++
			continue
		default:
		}

		if taken > 0 && idle.isAny() {
			return taken, nil
		}

		select {
		case <-ctx.Done():
			p.release(taken)
			return 0, ctx.Err()
		case <-p.tokens:
			taken++
		case <-idle.wake():
		}
	}

	return taken, nil
}

// release returns n credits to the pool. Callers release exactly the credits
// they acquired. release never blocks: credits beyond the pool capacity are
// dropped, so an accounting mistake can never deadlock a worker.
func (p *creditPool) release(n int) {
	for range n {
		select {
		case p.tokens <- struct{}{}:
		default:
			return
		}
	}
}

// idleWorkers counts the workers that are waiting for work while none is ready
// for them, and wakes a poller waiting for credits when one starts waiting. A
// nil *idleWorkers reports no idle worker and never wakes anyone.
type idleWorkers struct {
	signal chan struct{}
	count  atomic.Int32
}

// newIdleWorkers builds an idleWorkers with no idle worker.
func newIdleWorkers() *idleWorkers {
	return &idleWorkers{signal: make(chan struct{}, 1)}
}

// enter records a worker that starts waiting with nothing ready to run and
// wakes a waiting poller.
func (w *idleWorkers) enter() {
	if w == nil {
		return
	}
	w.count.Add(1)
	select {
	case w.signal <- struct{}{}:
	default:
	}
}

// leave records that a worker stopped waiting.
func (w *idleWorkers) leave() {
	if w == nil {
		return
	}
	w.count.Add(-1)
}

// isAny reports whether at least one worker is waiting with nothing to run.
func (w *idleWorkers) isAny() bool {
	return w != nil && w.count.Load() > 0
}

// wake returns the channel signaled when a worker starts waiting, or nil (which
// blocks forever in a select) for a nil receiver.
func (w *idleWorkers) wake() <-chan struct{} {
	if w == nil {
		return nil
	}
	return w.signal
}
