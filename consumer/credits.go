package consumer

import "context"

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

// acquire takes n credits, blocking until they are all available or ctx is
// done. On cancellation it returns the credits taken so far to the pool and
// reports ctx.Err(), so a canceled acquire never changes the number of free
// credits.
func (p *creditPool) acquire(ctx context.Context, n int) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.turn <- struct{}{}:
	}
	defer func() { <-p.turn }()

	for taken := range n {
		select {
		case <-ctx.Done():
			p.release(taken)
			return ctx.Err()
		case <-p.tokens:
		}
	}

	return nil
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
