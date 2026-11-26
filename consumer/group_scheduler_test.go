package consumer_test

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/consumer"
)

const groupKeySeparator = "\x00"

type groupedMessage struct {
	msg   *consumer.TestMessage
	group string
}

func drawGroupedMessages(t *rapid.T) []groupedMessage {
	groups := rapid.IntRange(1, 8).Draw(t, "groups")
	assignment := rapid.SliceOfN(rapid.IntRange(0, groups-1), 1, 50).Draw(t, "assignment")
	out := make([]groupedMessage, len(assignment))
	for i, g := range assignment {
		group := fmt.Sprintf("g-%d", g)
		out[i] = groupedMessage{
			msg:   consumer.NewTestMessage(fmt.Sprintf("m-%d", i), group, nil, ""),
			group: group,
		}
	}
	return out
}

func messageHandles(msgs []*consumer.TestMessage) []string {
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, m.Identifier())
	}
	return out
}

func snsEnvelopeBody(t require.TestingT, attrs map[string]string) string {
	type attribute struct {
		Type  string `json:"Type"`
		Value string `json:"Value"`
	}
	envelope := struct {
		MessageAttributes map[string]attribute `json:"MessageAttributes"`
		Message           string               `json:"Message"`
	}{
		MessageAttributes: make(map[string]attribute, len(attrs)),
		Message:           "payload",
	}
	for k, v := range attrs {
		envelope.MessageAttributes[k] = attribute{Type: "String", Value: v}
	}
	body, err := json.Marshal(envelope)
	require.NoError(t, err)
	return string(body)
}

func TestNewGroupScheduler(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		name     string
		capacity int
	}{
		{name: "negative capacity is raised to one", capacity: -3},
		{name: "zero capacity is raised to one", capacity: 0},
		{name: "capacity of one", capacity: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := consumer.NewGroupScheduler(tt.capacity, nil)
				msg := consumer.NewTestMessage("only", "g", nil, "")

				s.Submit(msg)
				got, ok := s.Take(context.Background())

				require.True(t, ok)
				assert.Same(t, msg, got)
				assert.Equal(t, 1, s.GroupCount())
				assert.Empty(t, s.Complete(got, false))
				assert.Zero(t, s.GroupCount())
			})
		})
	}
}

func TestGroupScheduler_TakeCanceledContext(t *testing.T) {
	s := consumer.NewGroupScheduler(4, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got, ok := s.Take(ctx)

	assert.False(t, ok)
	assert.Nil(t, got)
}

func TestGroupScheduler_TakeBlockedUntilCanceled(t *testing.T) {
	defer goleak.VerifyNone(t)

	synctest.Test(t, func(t *testing.T) {
		s := consumer.NewGroupScheduler(4, nil)
		ctx, cancel := context.WithCancel(context.Background())
		results := make(chan bool, 1)

		go func() {
			_, ok := s.Take(ctx)
			results <- ok
		}()
		synctest.Wait()
		require.Empty(t, results)
		cancel()

		assert.False(t, <-results)
	})
}

func TestGroupScheduler_RequeuesGroupAfterComplete(t *testing.T) {
	defer goleak.VerifyNone(t)

	synctest.Test(t, func(t *testing.T) {
		s := consumer.NewGroupScheduler(4, nil)
		first := consumer.NewTestMessage("a-1", "a", nil, "")
		second := consumer.NewTestMessage("a-2", "a", nil, "")
		s.Submit(first)
		s.Submit(second)
		taken, ok := s.Take(context.Background())
		require.True(t, ok)
		require.Same(t, first, taken)
		results := make(chan *consumer.TestMessage, 1)

		go func() {
			msg, _ := s.Take(context.Background())
			results <- msg
		}()
		synctest.Wait()
		require.Empty(t, results)

		assert.Empty(t, s.Complete(first, false))

		assert.Same(t, second, <-results)
		assert.Equal(t, 1, s.GroupCount())
		assert.Empty(t, s.Complete(second, false))
		assert.Zero(t, s.GroupCount())
	})
}

func TestGroupScheduler_CompleteHoldsPendingTailOfFailedGroup(t *testing.T) {
	tests := []struct {
		name       string
		wantHeld   []string
		wantDrain  []string
		wantGroups int
		failed     bool
	}{
		{
			name:       "failure holds the pending tail of its group",
			failed:     true,
			wantHeld:   []string{"a-2", "a-3"},
			wantDrain:  []string{"b-2"},
			wantGroups: 1,
		},
		{
			name:       "success holds nothing",
			failed:     false,
			wantHeld:   []string{},
			wantDrain:  []string{"a-2", "a-3", "b-2"},
			wantGroups: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := consumer.NewGroupScheduler(8, nil)
			for _, h := range []string{"a-1", "a-2", "b-1", "a-3", "b-2"} {
				s.Submit(consumer.NewTestMessage(h, h[:1], nil, ""))
			}
			a1, ok := s.Take(context.Background())
			require.True(t, ok)
			require.Equal(t, "a-1", a1.Identifier())
			b1, ok := s.Take(context.Background())
			require.True(t, ok)
			require.Equal(t, "b-1", b1.Identifier())

			held := s.Complete(a1, tt.failed)

			assert.Equal(t, tt.wantHeld, messageHandles(held))
			assert.Equal(t, tt.wantGroups, s.GroupCount())
			assert.Empty(t, s.Complete(b1, false))
			assert.Equal(t, tt.wantDrain, messageHandles(s.Drain()))
			assert.Zero(t, s.GroupCount())
		})
	}
}

func TestGroupScheduler_CompleteUnknownMessage(t *testing.T) {
	tests := []struct {
		name   string
		failed bool
	}{
		{name: "success", failed: false},
		{name: "failure", failed: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := consumer.NewGroupScheduler(4, nil)
			s.Submit(consumer.NewTestMessage("known", "a", nil, ""))

			held := s.Complete(consumer.NewTestMessage("unknown", "z", nil, ""), tt.failed)

			assert.Nil(t, held)
			assert.Equal(t, 1, s.GroupCount())
		})
	}
}

func TestGroupScheduler_TakeSkipsGroupEmptiedWhileQueued(t *testing.T) {
	s := consumer.NewGroupScheduler(4, nil)
	a1 := consumer.NewTestMessage("a-1", "a", nil, "")
	a2 := consumer.NewTestMessage("a-2", "a", nil, "")
	b1 := consumer.NewTestMessage("b-1", "b", nil, "")
	s.Submit(a1)
	s.Submit(a2)
	taken, ok := s.Take(context.Background())
	require.True(t, ok)
	require.Same(t, a1, taken)
	require.Empty(t, s.Complete(a1, false))
	require.Equal(t, []string{"a-2"}, messageHandles(s.Complete(a1, true)))
	s.Submit(b1)

	got, ok := s.Take(context.Background())

	require.True(t, ok)
	assert.Same(t, b1, got)
}

func TestGroupScheduler_Drain(t *testing.T) {
	s := consumer.NewGroupScheduler(8, nil)
	for _, h := range []string{"b-1", "a-1", "b-2", "c-1", "a-2", "b-3"} {
		s.Submit(consumer.NewTestMessage(h, h[:1], nil, ""))
	}
	active, ok := s.Take(context.Background())
	require.True(t, ok)
	require.Equal(t, "b-1", active.Identifier())
	require.Equal(t, 3, s.GroupCount())

	drained := s.Drain()

	assert.Equal(t, []string{"a-1", "a-2", "b-2", "b-3", "c-1"}, messageHandles(drained))
	assert.Zero(t, s.GroupCount())
	assert.Empty(t, s.Drain())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, ok = s.Take(ctx)
	assert.False(t, ok)
}

func TestGroupScheduler_DrainEmpty(t *testing.T) {
	s := consumer.NewGroupScheduler(4, nil)

	assert.Empty(t, s.Drain())
	assert.Zero(t, s.GroupCount())
}

func TestWorkSource_GroupCount(t *testing.T) {
	tests := []struct {
		ws   *consumer.WorkSource
		name string
		want int
	}{
		{name: "shared queue is not a group scheduler", ws: consumer.NewSharedQueue(4), want: -1},
		{name: "empty group scheduler", ws: consumer.NewGroupScheduler(4, nil), want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.ws.GroupCount())
		})
	}
}

type groupRunRecorder struct {
	active     map[string]bool
	starts     map[string][]string
	violations []string
	finished   int
	mu         sync.Mutex
}

func (r *groupRunRecorder) start(group, handle string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active[group] {
		r.violations = append(r.violations, handle)
	}
	r.active[group] = true
	r.starts[group] = append(r.starts[group], handle)
}

func (r *groupRunRecorder) finish(group string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.active[group] = false
	r.finished++
	return r.finished
}

// Feature: consumer-flow-control, Property 6: For any sequence of submitted messages over any set of Group_Keys and any number of Workers, at most one message per Group_Key is active at a time, and for each Group_Key the handlers start in submission order.
func TestGroupScheduler_ExclusivityAndOrder(t *testing.T) {
	defer goleak.VerifyNone(t)

	rapid.Check(t, func(t *rapid.T) {
		msgs := drawGroupedMessages(t)
		workers := rapid.IntRange(1, 8).Draw(t, "workers")
		s := consumer.NewGroupScheduler(len(msgs), nil)
		groupOf := make(map[string]string, len(msgs))
		wantOrder := make(map[string][]string)
		for _, m := range msgs {
			groupOf[m.msg.Identifier()] = m.group
			wantOrder[m.group] = append(wantOrder[m.group], m.msg.Identifier())
		}
		rec := &groupRunRecorder{active: map[string]bool{}, starts: map[string][]string{}}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var wg sync.WaitGroup

		for range workers {
			wg.Go(func() {
				for {
					msg, ok := s.Take(ctx)
					if !ok {
						return
					}
					group := groupOf[msg.Identifier()]
					rec.start(group, msg.Identifier())
					done := rec.finish(group)
					s.Complete(msg, false)
					if done == len(msgs) {
						cancel()
					}
				}
			})
		}
		for _, m := range msgs {
			s.Submit(m.msg)
		}
		wg.Wait()

		assert.Empty(t, rec.violations)
		assert.Equal(t, wantOrder, rec.starts)
		assert.Equal(t, len(msgs), rec.finished)
		assert.Zero(t, s.GroupCount())
		assert.Empty(t, s.Drain())
	})
}

type attributePlacement int

const (
	placeNowhere attributePlacement = iota
	placeNative
	placeEnvelope
	placeBoth
)

func TestGroupKey(t *testing.T) {
	tests := []struct {
		native   map[string]string
		envelope map[string]string
		name     string
		groupID  string
		want     string
		fields   []string
	}{
		{name: "no fields yields the group id", groupID: "g", want: "g"},
		{name: "no group id and no fields", want: ""},
		{
			name:    "native attribute",
			groupID: "g", fields: []string{"tenant"},
			native: map[string]string{"tenant": "n"},
			want:   "g" + groupKeySeparator + "n",
		},
		{
			name:    "envelope attribute",
			groupID: "g", fields: []string{"tenant"},
			envelope: map[string]string{"tenant": "e"},
			want:     "g" + groupKeySeparator + "e",
		},
		{
			name:    "native takes precedence over envelope",
			groupID: "g", fields: []string{"tenant"},
			native:   map[string]string{"tenant": "n"},
			envelope: map[string]string{"tenant": "e"},
			want:     "g" + groupKeySeparator + "n",
		},
		{
			name:    "empty native value falls back to envelope",
			groupID: "g", fields: []string{"tenant"},
			native:   map[string]string{"tenant": ""},
			envelope: map[string]string{"tenant": "e"},
			want:     "g" + groupKeySeparator + "e",
		},
		{
			name:    "missing field contributes an empty part",
			groupID: "g", fields: []string{"tenant", "region"},
			native: map[string]string{"region": "r"},
			want:   "g" + groupKeySeparator + groupKeySeparator + "r",
		},
		{
			name:   "standard queue without group id",
			fields: []string{"tenant"},
			native: map[string]string{"tenant": "n"},
			want:   groupKeySeparator + "n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := consumer.NewTestMessage("h", tt.groupID, tt.native, snsEnvelopeBody(t, tt.envelope))

			assert.Equal(t, tt.want, consumer.GroupKey(msg, tt.fields))
		})
	}
}

func TestGroupKeyDistinguishesFieldCombinations(t *testing.T) {
	fields := []string{"a", "b"}
	left := consumer.NewTestMessage("l", "g", map[string]string{"a": "x", "b": "yz"}, "")
	right := consumer.NewTestMessage("r", "g", map[string]string{"a": "xy", "b": "z"}, "")

	assert.NotEqual(t, consumer.GroupKey(left, fields), consumer.GroupKey(right, fields))
}

// Feature: consumer-flow-control, Property 8: For any MessageGroupId, custom field names, and attribute values placed in the native user attributes, the SNS envelope, both, or neither, the Group_Key equals the separator-joined MessageGroupId and field values, with native values taking precedence.
func TestGroupKeyDerivation(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		groupID := rapid.String().Draw(t, "groupID")
		fields := rapid.SliceOfNDistinct(rapid.StringMatching(`[A-Za-z][A-Za-z0-9_.-]{0,15}`), 0, 5, rapid.ID[string]).
			Draw(t, "fields")
		native := map[string]string{}
		envelope := map[string]string{}
		parts := []string{groupID}
		for _, f := range fields {
			placement := attributePlacement(rapid.IntRange(int(placeNowhere), int(placeBoth)).Draw(t, "placement"))
			nativeValue := rapid.StringN(1, 20, -1).Draw(t, "native")
			envelopeValue := rapid.StringN(1, 20, -1).Draw(t, "envelope")
			want := ""
			switch placement {
			case placeNowhere:
			case placeNative:
				native[f] = nativeValue
				want = nativeValue
			case placeEnvelope:
				envelope[f] = envelopeValue
				want = envelopeValue
			case placeBoth:
				native[f] = nativeValue
				envelope[f] = envelopeValue
				want = nativeValue
			}
			parts = append(parts, want)
		}
		msg := consumer.NewTestMessage("h", groupID, native, snsEnvelopeBody(t, envelope))

		assert.Equal(t, strings.Join(parts, groupKeySeparator), consumer.GroupKey(msg, fields))
	})
}

type schedulerStepKind int

const (
	stepSubmit schedulerStepKind = iota
	stepTake
	stepComplete
)

type schedulerStep struct {
	pick   int
	failed bool
}

type schedulerModel struct {
	s        *consumer.WorkSource
	c        *collectedFailures
	pending  map[string][]*consumer.TestMessage
	active   map[string]*consumer.TestMessage
	groupOf  map[*consumer.TestMessage]string
	msgs     []groupedMessage
	next     int
	outcomes int
}

func newSchedulerModel(c *collectedFailures, msgs []groupedMessage) *schedulerModel {
	m := &schedulerModel{
		s:       consumer.NewGroupScheduler(len(msgs), nil),
		c:       c,
		pending: map[string][]*consumer.TestMessage{},
		active:  map[string]*consumer.TestMessage{},
		groupOf: make(map[*consumer.TestMessage]string, len(msgs)),
		msgs:    msgs,
	}
	for _, gm := range msgs {
		m.groupOf[gm.msg] = gm.group
	}
	return m
}

func (m *schedulerModel) readyGroups() int {
	n := 0
	for g, p := range m.pending {
		if len(p) > 0 && m.active[g] == nil {
			n++
		}
	}
	return n
}

func (m *schedulerModel) enabled() []schedulerStepKind {
	var kinds []schedulerStepKind
	if m.next < len(m.msgs) {
		kinds = append(kinds, stepSubmit)
	}
	if m.readyGroups() > 0 {
		kinds = append(kinds, stepTake)
	}
	if len(m.active) > 0 {
		kinds = append(kinds, stepComplete)
	}
	return kinds
}

func (m *schedulerModel) submit() {
	gm := m.msgs[m.next]
	m.next++
	m.s.Submit(gm.msg)
	m.pending[gm.group] = append(m.pending[gm.group], gm.msg)
}

func (m *schedulerModel) take() bool {
	msg, ok := m.s.Take(context.Background())
	if !assert.True(m.c, ok) {
		return false
	}
	group := m.groupOf[msg]
	if !assert.Nil(m.c, m.active[group]) ||
		!assert.NotEmpty(m.c, m.pending[group]) ||
		!assert.Same(m.c, m.pending[group][0], msg) {
		return false
	}
	m.pending[group] = m.pending[group][1:]
	m.active[group] = msg
	return true
}

func (m *schedulerModel) complete(pick int, failed bool) bool {
	groups := slices.Sorted(maps.Keys(m.active))
	group := groups[pick%len(groups)]
	msg := m.active[group]
	delete(m.active, group)

	held := m.s.Complete(msg, failed)

	m.outcomes++
	if !failed {
		return assert.Empty(m.c, held)
	}
	want := messageHandles(m.pending[group])
	m.outcomes += len(want)
	m.pending[group] = nil
	return assert.Equal(m.c, want, messageHandles(held))
}

func (m *schedulerModel) trackedGroups() int {
	groups := map[string]bool{}
	for g, p := range m.pending {
		if len(p) > 0 {
			groups[g] = true
		}
	}
	for g := range m.active {
		groups[g] = true
	}
	return len(groups)
}

func (m *schedulerModel) step(kind schedulerStepKind, st schedulerStep) bool {
	ok := true
	switch kind {
	case stepSubmit:
		m.submit()
	case stepTake:
		ok = m.take()
	case stepComplete:
		ok = m.complete(st.pick, st.failed)
	}
	return ok && assert.Equal(m.c, m.trackedGroups(), m.s.GroupCount())
}

func (m *schedulerModel) run(steps []schedulerStep) {
	for _, st := range steps {
		kinds := m.enabled()
		if len(kinds) == 0 {
			break
		}
		if !m.step(kinds[st.pick%len(kinds)], st) {
			return
		}
	}
	for kinds := m.enabled(); len(kinds) > 0; kinds = m.enabled() {
		if !m.step(kinds[0], schedulerStep{}) {
			return
		}
	}

	assert.Equal(m.c, len(m.msgs), m.outcomes)
	assert.Zero(m.c, m.s.GroupCount())
	assert.Empty(m.c, m.s.Drain())
}

// Feature: consumer-flow-control, Property 10: For any script, once every message of a Group_Key reaches an Outcome, the scheduler holds no state for that key.
func TestGroupScheduler_DiscardsSettledGroups(t *testing.T) {
	defer goleak.VerifyNone(t)

	rapid.Check(t, func(rt *rapid.T) {
		msgs := drawGroupedMessages(rt)
		steps := rapid.SliceOfN(rapid.Custom(func(t *rapid.T) schedulerStep {
			return schedulerStep{
				pick:   rapid.IntRange(0, 1<<10).Draw(t, "pick"),
				failed: rapid.Bool().Draw(t, "failed"),
			}
		}), 0, 150).Draw(rt, "steps")
		failures := &collectedFailures{}

		synctest.Test(t, func(*testing.T) {
			newSchedulerModel(failures, msgs).run(steps)
		})

		if len(failures.msgs) > 0 {
			rt.Fatalf("%s", strings.Join(failures.msgs, "\n"))
		}
	})
}
