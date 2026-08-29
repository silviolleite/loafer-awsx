package consumer

import "sync"

// groupBarrier tracks the FIFO message groups that have failed within a single
// received batch. Under PerGroupID dispatch with the Visibility retry model, it
// lets the dispatcher hold back every later message of a failed group so the
// group is redelivered and reprocessed in order, rather than deleting the
// group's tail out of order.
//
// A groupBarrier is created per received batch by the polling loop and shared
// by every worker that processes that batch's messages. Because distinct groups
// may be handled by distinct workers concurrently, all access is guarded by a
// mutex. All methods are nil-safe: a nil groupBarrier reports no failures and
// records none, so callers never have to guard against a disabled barrier.
type groupBarrier struct {
	poisoned map[string]struct{}
	mu       sync.Mutex
}

// newGroupBarrier builds an empty groupBarrier ready to record failed groups.
func newGroupBarrier() *groupBarrier {
	return &groupBarrier{poisoned: make(map[string]struct{})}
}

// fail records key as a failed group for the batch. It is a no-op on a nil
// receiver.
func (b *groupBarrier) fail(key string) {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.poisoned[key] = struct{}{}
	b.mu.Unlock()
}

// failed reports whether key was already recorded as a failed group for the
// batch. It returns false on a nil receiver.
func (b *groupBarrier) failed(key string) bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	_, ok := b.poisoned[key]
	b.mu.Unlock()
	return ok
}
