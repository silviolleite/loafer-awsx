package consumer

import (
	"context"
	"slices"
	"strings"
	"sync"
)

// groupState holds the Pending_Messages of one Group_Key in receive order and
// whether the group currently has an Active_Message or sits in the ready queue.
type groupState struct {
	key     string
	pending []*message
	active  bool
	queued  bool
}

// groupScheduler is the PerGroupID-mode workSource. It keeps a FIFO of
// Pending_Messages per Group_Key and a ready queue of groups that have pending
// work and no Active_Message. Any idle worker takes the next ready group, so a
// group is never bound to a fixed worker, and at most one message per group is
// active at a time, which preserves the group's order.
//
// After a message completes, its group goes back to the tail of the ready queue
// when it still has pending messages, which keeps groups fair. The ready
// channel is sized to the credit capacity: a group is queued at most once and
// only while it has an admitted message, so the send never blocks.
type groupScheduler struct {
	groups       map[string]*groupState
	ready        chan *groupState
	idle         *idleWorkers
	customFields []string
	mu           sync.Mutex
}

var _ workSource = (*groupScheduler)(nil)

// newGroupScheduler builds a groupScheduler for up to capacity admitted
// messages, deriving Group_Keys from customFields. A capacity below 1 is raised
// to 1. idle, which may be nil, records the workers waiting in take while no
// group is ready, including when every pending message belongs to a group that
// is already running.
func newGroupScheduler(capacity int, customFields []string, idle *idleWorkers) *groupScheduler {
	if capacity < 1 {
		capacity = 1
	}
	return &groupScheduler{
		groups:       make(map[string]*groupState),
		ready:        make(chan *groupState, capacity),
		idle:         idle,
		customFields: customFields,
	}
}

// submit computes the Group_Key of each message, appends the messages to their
// groups in order, and queues every group that is neither active nor already
// queued. All messages are added under one lock, so a worker that fails a
// message of a group always sees the rest of that group from the same receive
// and holds it.
func (s *groupScheduler) submit(msgs ...*message) {
	for _, msg := range msgs {
		msg.groupKey = groupKey(msg, s.customFields)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, msg := range msgs {
		g, ok := s.groups[msg.groupKey]
		if !ok {
			g = &groupState{key: msg.groupKey}
			s.groups[msg.groupKey] = g
		}
		g.pending = append(g.pending, msg)
	}
	for _, msg := range msgs {
		s.enqueueLocked(s.groups[msg.groupKey])
	}
}

// take waits for a ready group and starts its oldest Pending_Message.
func (s *groupScheduler) take(ctx context.Context) (*message, bool) {
	for {
		g, ok := s.nextReady(ctx)
		if !ok {
			return nil, false
		}

		s.mu.Lock()
		g.queued = false
		if len(g.pending) == 0 {
			s.mu.Unlock()
			continue
		}
		msg := g.pending[0]
		g.pending[0] = nil
		g.pending = g.pending[1:]
		g.active = true
		s.mu.Unlock()

		return msg, true
	}
}

// nextReady returns the next ready group, counting the worker as idle while it
// waits for one. It reports false once ctx is done.
func (s *groupScheduler) nextReady(ctx context.Context) (*groupState, bool) {
	select {
	case g := <-s.ready:
		return g, true
	default:
	}

	s.idle.enter()
	defer s.idle.leave()

	select {
	case <-ctx.Done():
		return nil, false
	case g := <-s.ready:
		return g, true
	}
}

// complete marks the group of msg inactive. When failed is true, every
// Pending_Message of the group is removed and returned as held. A group that
// still has pending messages is queued again; an empty group is discarded.
func (s *groupScheduler) complete(msg *message, failed bool) []*message {
	s.mu.Lock()
	defer s.mu.Unlock()

	g, ok := s.groups[msg.groupKey]
	if !ok {
		return nil
	}
	g.active = false

	var held []*message
	if failed {
		held = g.pending
		g.pending = nil
	}

	if len(g.pending) == 0 {
		delete(s.groups, g.key)
		return held
	}
	s.enqueueLocked(g)

	return held
}

// drain removes every Pending_Message of every group, ordered by Group_Key and
// then by receive order, and resets the scheduler.
func (s *groupScheduler) drain() []*message {
	s.mu.Lock()
	defer s.mu.Unlock()

	keys := make([]string, 0, len(s.groups))
	for k := range s.groups {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	var out []*message
	for _, k := range keys {
		out = append(out, s.groups[k].pending...)
	}
	clear(s.groups)

	for {
		select {
		case <-s.ready:
		default:
			return out
		}
	}
}

// enqueueLocked sends g to the ready queue when it has pending work, is not
// active, and is not already queued. s.mu must be held.
func (s *groupScheduler) enqueueLocked(g *groupState) {
	if g.active || g.queued || len(g.pending) == 0 {
		return
	}
	g.queued = true
	s.ready <- g
}

// groupKey builds the PerGroupID routing key by joining the message group ID
// with the configured custom group fields. Each field is read from the native
// SQS user message attributes first (where SNS places them under raw message
// delivery) and falls back to the SNS-envelope attributes, so custom group
// fields work under both delivery modes. The separator keeps distinct field
// combinations from colliding.
func groupKey(msg *message, fields []string) string {
	parts := make([]string, 0, len(fields)+1)
	parts = append(parts, msg.SystemAttributeByKey(messageGroupIDKey))
	for _, field := range fields {
		value := msg.UserMessageAttribute(field)
		if value == "" {
			value = msg.Attribute(field)
		}
		parts = append(parts, value)
	}
	return strings.Join(parts, groupKeySeparator)
}
