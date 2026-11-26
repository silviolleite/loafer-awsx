package consumer

import "context"

// workSource hands Pending_Messages to workers. A poller submits every message
// it admits, and workers take messages, run them, and report completion.
//
// Implementations size their internal queues to the credit capacity, and a
// message is submitted only while its credit is held, so submit never blocks.
// sharedQueue serves the Parallel run mode and groupScheduler the PerGroupID
// run mode.
type workSource interface {
	// submit adds the admitted messages of one receive as Pending, all at
	// once: a worker never observes part of a receive, so a failure in a group
	// holds every later message of that group from the same receive.
	submit(msgs ...*message)
	// take blocks until a Pending_Message can be started or ctx is done. ok is
	// false when ctx is done.
	take(ctx context.Context) (msg *message, ok bool)
	// complete reports that a taken message reached an Outcome. failed requests
	// a group Hold; the returned messages are the held Pending_Messages of the
	// same group, which the caller must finish without running them.
	complete(msg *message, failed bool) (held []*message)
	// drain removes and returns every Pending_Message. It is called only after
	// every poller and every worker has returned.
	drain() []*message
}

// sharedQueue is the Parallel-mode workSource: one channel drained by every
// worker, so any idle worker takes the next message.
type sharedQueue struct {
	ch   chan *message
	idle *idleWorkers
}

var _ workSource = (*sharedQueue)(nil)

// newSharedQueue builds a sharedQueue whose channel holds capacity messages. A
// capacity below 1 is raised to 1. idle, which may be nil, records the workers
// waiting in take while the queue is empty.
func newSharedQueue(capacity int, idle *idleWorkers) *sharedQueue {
	if capacity < 1 {
		capacity = 1
	}
	return &sharedQueue{ch: make(chan *message, capacity), idle: idle}
}

// submit enqueues msgs. It does not block while the caller holds a credit for
// each message, because the channel capacity equals the credit capacity.
func (q *sharedQueue) submit(msgs ...*message) {
	for _, msg := range msgs {
		q.ch <- msg
	}
}

// take returns the next Pending_Message, or ok false once ctx is done. A
// worker that finds the queue empty counts as idle while it waits.
func (q *sharedQueue) take(ctx context.Context) (*message, bool) {
	select {
	case msg := <-q.ch:
		return msg, true
	default:
	}

	q.idle.enter()
	defer q.idle.leave()

	select {
	case <-ctx.Done():
		return nil, false
	case msg := <-q.ch:
		return msg, true
	}
}

// complete is a no-op: Parallel mode never holds messages.
func (q *sharedQueue) complete(*message, bool) []*message {
	return nil
}

// drain returns every message still queued, without blocking.
func (q *sharedQueue) drain() []*message {
	var out []*message
	for {
		select {
		case msg := <-q.ch:
			out = append(out, msg)
		default:
			return out
		}
	}
}
