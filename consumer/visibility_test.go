package consumer_test

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/consumer"
	"github.com/silviolleite/loafer-awsx/fake"
)

const vmQuiet = 13 * time.Hour

type vmSend struct {
	at      time.Time
	handle  string
	timeout int32
}

type vmRequest struct {
	at      time.Time
	entries []vmSend
	epoch   int
}

type vmRespond func(call int, params *sqs.ChangeMessageVisibilityBatchInput) (*sqs.ChangeMessageVisibilityBatchOutput, error)

type vmRecorder struct {
	client   *fake.SQSClient
	respond  vmRespond
	requests []vmRequest
	epoch    int
	mu       sync.Mutex
}

func newVMRecorder(respond vmRespond) *vmRecorder {
	r := &vmRecorder{respond: respond}
	r.client = &fake.SQSClient{ChangeMessageVisibilityBatchFunc: r.changeBatch}
	return r
}

func (r *vmRecorder) changeBatch(
	_ context.Context,
	params *sqs.ChangeMessageVisibilityBatchInput,
	_ ...func(*sqs.Options),
) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
	now := time.Now()
	req := vmRequest{at: now, entries: make([]vmSend, len(params.Entries))}
	for i, e := range params.Entries {
		req.entries[i] = vmSend{at: now, handle: aws.ToString(e.ReceiptHandle), timeout: e.VisibilityTimeout}
	}

	r.mu.Lock()
	req.epoch = r.epoch
	call := len(r.requests)
	r.requests = append(r.requests, req)
	r.mu.Unlock()

	if r.respond == nil {
		return &sqs.ChangeMessageVisibilityBatchOutput{}, nil
	}
	return r.respond(call, params)
}

func (r *vmRecorder) setEpoch(epoch int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.epoch = epoch
}

func (r *vmRecorder) snapshot() []vmRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.requests)
}

func (r *vmRecorder) sendsByHandle() map[string][]vmSend {
	out := make(map[string][]vmSend)
	for _, req := range r.snapshot() {
		for _, e := range req.entries {
			out[e.handle] = append(out[e.handle], e)
		}
	}
	return out
}

func (r *vmRecorder) sends() []vmSend {
	var out []vmSend
	for _, req := range r.snapshot() {
		out = append(out, req.entries...)
	}
	return out
}

func vmPairs(req vmRequest) []visibilityPair {
	out := make([]visibilityPair, len(req.entries))
	for i, e := range req.entries {
		out[i] = visibilityPair{handle: e.handle, timeout: e.timeout}
	}
	return out
}

func vmHandles(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("msg-%03d", i)
	}
	return out
}

func vmMessages(n int) []*consumer.TestMessage {
	return visibilityMessages(vmHandles(n)...)
}

func startVM(client consumer.SQSClient, vt int32, limit int, log *slog.Logger) *consumer.VisibilityManager {
	vm := consumer.NewVisibilityManager(client, visibilityQueueURL, vt, limit, log)
	vm.Start(context.Background())
	return vm
}

func vmSeconds(s int32) time.Duration {
	return time.Duration(s) * time.Second
}

func vmMillis(ms int64) time.Duration {
	return time.Duration(ms) * time.Millisecond
}

func vmCeilSeconds(d time.Duration) int64 {
	return int64((d + time.Second - 1) / time.Second)
}

func vmBackoffSeconds(d time.Duration) int32 {
	return int32(min(max(int64(d/time.Second), 0), consumer.MaxVisibilitySeconds))
}

func vmSleepUntil(at time.Time) {
	time.Sleep(time.Until(at))
}

func vmOrder(values []int64) []int {
	order := make([]int, len(values))
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(a, b int) int { return cmp.Compare(values[a], values[b]) })
	return order
}

func vmMaxAttempts() int {
	return int(consumer.RenewalMargin / consumer.ExtensionRetryDelay)
}

// Feature: consumer-flow-control, Property 11: For any VT >= 11s, ExtensionLimit >= 1, and wait before handler start, the first Extension for a message still unfinished is sent no later than receivedAt + VT - margin, and no Extension is sent earlier than deadline - margin - coalesceWindow.
func TestVisibilityManager_ExtensionTimingFromReceiveTime(t *testing.T) {
	defer goleak.VerifyNone(t)

	rapid.Check(t, func(rt *rapid.T) {
		vt := rapid.Int32Range(11, 600).Draw(rt, "visibilityTimeout")
		limit := rapid.IntRange(1, 5).Draw(rt, "extensionLimit")
		n := rapid.IntRange(1, 12).Draw(rt, "messages")
		trackAfter := rapid.SliceOfN(rapid.Int64Range(0, 120_000), n, n).Draw(rt, "trackAfterMillis")
		waits := rapid.SliceOfN(rapid.Int64Range(0, int64(vt)*1000+30_000), n, n).Draw(rt, "waitMillis")
		msgs := vmMessages(n)
		rec := newVMRecorder(nil)
		var base time.Time

		synctest.Test(t, func(*testing.T) {
			base = time.Now()
			vm := startVM(rec.client, vt, limit, nil)
			for _, i := range vmOrder(trackAfter) {
				vmSleepUntil(base.Add(vmMillis(trackAfter[i])))
				consumer.SetReceivedAt(msgs[i], time.Now().Add(-vmMillis(waits[i])))
				vm.Track(msgs[i])
				synctest.Wait()
			}
			time.Sleep(vmQuiet)
			vm.Close()
		})

		sends := rec.sendsByHandle()
		for i, msg := range msgs {
			trackAt := base.Add(vmMillis(trackAfter[i]))
			deadline := trackAt.Add(-vmMillis(waits[i])).Add(vmSeconds(vt))
			got := sends[msg.Identifier()]
			require.NotEmpty(rt, got, "message %d got no extension", i)

			latest := deadline.Add(-consumer.RenewalMargin)
			if trackAt.After(latest) {
				latest = trackAt
			}
			assert.False(rt, got[0].at.After(latest), "message %d: first extension at %v, latest %v", i, got[0].at, latest)

			for k, s := range got {
				earliest := deadline.Add(-consumer.RenewalMargin - consumer.CoalesceWindow)
				assert.False(rt, s.at.Before(earliest), "message %d extension %d at %v before %v", i, k+1, s.at, earliest)
				deadline = s.at.Add(vmSeconds(s.timeout))
			}
		}
	})
}

// Feature: consumer-flow-control, Property 12: For any ExtensionLimit = n >= 0 and a handler that outlives every Extension, exactly n Extensions are sent for the message, and the k-th carries timeout min(k * VT, 43200 - elapsed).
func TestVisibilityManager_ExtensionLimitIsExact(t *testing.T) {
	defer goleak.VerifyNone(t)

	rapid.Check(t, func(rt *rapid.T) {
		vt := rapid.Int32Range(11, 600).Draw(rt, "visibilityTimeout")
		limit := rapid.IntRange(0, 5).Draw(rt, "extensionLimit")
		wait := vmMillis(rapid.Int64Range(0, int64(vt+60)*1000).Draw(rt, "waitMillis"))
		msg := vmMessages(1)[0]
		rec := newVMRecorder(nil)
		var trackAt time.Time

		synctest.Test(t, func(*testing.T) {
			vm := startVM(rec.client, vt, limit, nil)
			trackAt = time.Now()
			consumer.SetReceivedAt(msg, trackAt.Add(-wait))
			vm.Track(msg)
			time.Sleep(vmQuiet)
			vm.Close()
		})

		receivedAt := trackAt.Add(-wait)
		got := rec.sendsByHandle()[msg.Identifier()]
		require.Len(rt, got, limit)
		assert.Len(rt, rec.snapshot(), limit)

		deadline := receivedAt.Add(vmSeconds(vt))
		for k, s := range got {
			due := deadline.Add(-consumer.RenewalMargin)
			if trackAt.After(due) {
				due = trackAt
			}
			assert.WithinDuration(rt, due, s.at, 0, "extension %d", k+1)

			want := min(int64(k+1)*int64(vt), consumer.MaxVisibilitySeconds-vmCeilSeconds(s.at.Sub(receivedAt)))
			assert.Equal(rt, int32(want), s.timeout, "extension %d", k+1)
			deadline = s.at.Add(vmSeconds(s.timeout))
		}
	})
}

// Feature: consumer-flow-control, Property 13: For any VT and ExtensionLimit (including values whose k * VT exceeds 43200), no visibility request makes a message invisible past receivedAt + 43200 s.
func TestVisibilityManager_TwelveHourCeiling(t *testing.T) {
	defer goleak.VerifyNone(t)

	rapid.Check(t, func(rt *rapid.T) {
		vt := rapid.Int32Range(1, int32(consumer.MaxVisibilitySeconds)).Draw(rt, "visibilityTimeout")
		limit := rapid.IntRange(0, 12).Draw(rt, "extensionLimit")
		n := rapid.IntRange(1, 6).Draw(rt, "messages")
		waits := rapid.SliceOfN(rapid.Int64Range(0, (consumer.MaxVisibilitySeconds+100)*1000), n, n).Draw(rt, "waitMillis")
		msgs := vmMessages(n)
		rec := newVMRecorder(nil)
		var base time.Time

		synctest.Test(t, func(*testing.T) {
			base = time.Now()
			vm := startVM(rec.client, vt, limit, nil)
			for i, msg := range msgs {
				consumer.SetReceivedAt(msg, base.Add(-vmMillis(waits[i])))
				vm.Track(msg)
				synctest.Wait()
			}
			time.Sleep(vmQuiet)
			vm.Close()
		})

		ceiling := time.Duration(consumer.MaxVisibilitySeconds) * time.Second
		sends := rec.sendsByHandle()
		for i, msg := range msgs {
			limitAt := base.Add(-vmMillis(waits[i])).Add(ceiling)
			got := sends[msg.Identifier()]
			assert.LessOrEqual(rt, len(got), limit, "message %d", i)
			for k, s := range got {
				assert.Positive(rt, s.timeout, "message %d extension %d", i, k+1)
				assert.False(rt, s.at.Add(vmSeconds(s.timeout)).After(limitAt),
					"message %d extension %d at %v with timeout %d passes %v", i, k+1, s.at, s.timeout, limitAt)
			}
		}
	})
}

type vmOpKind int

const (
	vmOpTrack vmOpKind = iota
	vmOpUntrack
	vmOpBackoff
	vmOpRelease
	vmOpSleep
)

type vmOp struct {
	picks []int
	delay time.Duration
	kind  vmOpKind
	pick  int
}

type vmEpoch struct {
	immediate map[string][]int32
	tracked   []bool
}

func drawVMOps(t *rapid.T, n int, vt int32) []vmOp {
	return rapid.SliceOfN(rapid.Custom(func(t *rapid.T) vmOp {
		return vmOp{
			kind:  vmOpKind(rapid.IntRange(int(vmOpTrack), int(vmOpSleep)).Draw(t, "kind")),
			pick:  rapid.IntRange(0, n-1).Draw(t, "pick"),
			picks: rapid.SliceOfNDistinct(rapid.IntRange(0, n-1), 0, n, rapid.ID[int]).Draw(t, "picks"),
			delay: vmMillis(rapid.Int64Range(-60_000, 2*int64(vt)*1000).Draw(t, "delayMillis")),
		}
	}), 1, 40).Draw(t, "ops")
}

func runVMOps(vm *consumer.VisibilityManager, rec *vmRecorder, msgs []*consumer.TestMessage, ops []vmOp) []vmEpoch {
	tracked := make([]bool, len(msgs))
	everTracked := make([]bool, len(msgs))
	epochs := make([]vmEpoch, len(ops))

	for i, op := range ops {
		rec.setEpoch(i)
		immediate := make(map[string][]int32)
		switch op.kind {
		case vmOpTrack:
			if !everTracked[op.pick] {
				consumer.SetReceivedAt(msgs[op.pick], time.Now().Add(-max(op.delay, 0)))
				vm.Track(msgs[op.pick])
				everTracked[op.pick], tracked[op.pick] = true, true
			}
		case vmOpUntrack:
			vm.Untrack(msgs[op.pick])
			tracked[op.pick] = false
		case vmOpBackoff:
			vm.Backoff(msgs[op.pick], op.delay)
			tracked[op.pick] = false
			h := msgs[op.pick].Identifier()
			immediate[h] = append(immediate[h], vmBackoffSeconds(op.delay))
		case vmOpRelease:
			selected := make([]*consumer.TestMessage, len(op.picks))
			for j, p := range op.picks {
				selected[j] = msgs[p]
				tracked[p] = false
				immediate[msgs[p].Identifier()] = append(immediate[msgs[p].Identifier()], 0)
			}
			vm.Release(selected...)
		case vmOpSleep:
			time.Sleep(max(op.delay, 0))
		}
		synctest.Wait()
		epochs[i] = vmEpoch{tracked: slices.Clone(tracked), immediate: immediate}
	}
	rec.setEpoch(len(ops))

	return epochs
}

// Feature: consumer-flow-control, Property 14: For any script, after untrack(msg) returns, no subsequently built visibility request contains msg, except its own backoff or Release change.
func TestVisibilityManager_NoRequestAfterUntrack(t *testing.T) {
	defer goleak.VerifyNone(t)

	rapid.Check(t, func(rt *rapid.T) {
		vt := rapid.Int32Range(11, 120).Draw(rt, "visibilityTimeout")
		limit := rapid.IntRange(0, 5).Draw(rt, "extensionLimit")
		n := rapid.IntRange(1, 8).Draw(rt, "messages")
		ops := drawVMOps(rt, n, vt)
		msgs := vmMessages(n)
		index := make(map[string]int, n)
		for i, m := range msgs {
			index[m.Identifier()] = i
		}
		rec := newVMRecorder(nil)
		var epochs []vmEpoch

		synctest.Test(t, func(*testing.T) {
			vm := startVM(rec.client, vt, limit, nil)
			epochs = runVMOps(vm, rec, msgs, ops)
			vm.Close()
		})

		byEpoch := make(map[int][]vmRequest)
		for _, req := range rec.snapshot() {
			byEpoch[req.epoch] = append(byEpoch[req.epoch], req)
		}

		for i, ep := range epochs {
			remaining := make(map[string][]int32, len(ep.immediate))
			for h, timeouts := range ep.immediate {
				remaining[h] = slices.Clone(timeouts)
			}
			for _, req := range byEpoch[i] {
				for _, e := range req.entries {
					if ep.tracked[index[e.handle]] {
						continue
					}
					pos := slices.Index(remaining[e.handle], e.timeout)
					if !assert.GreaterOrEqual(rt, pos, 0, "op %d (%v): request contains untracked %s with timeout %d", i, ops[i].kind, e.handle, e.timeout) {
						continue
					}
					remaining[e.handle] = slices.Delete(remaining[e.handle], pos, pos+1)
				}
			}
			for h, left := range remaining {
				assert.Empty(rt, left, "op %d: change for %s was not sent", i, h)
			}
		}
		assert.Empty(rt, byEpoch[len(ops)])
	})
}

// Feature: consumer-flow-control, Property 16: For any set of tracked messages whose due instants fall within one Coalesce_Window of the earliest due instant, their Extensions are sent in ceil(count / 10) requests.
func TestVisibilityManager_Coalescing(t *testing.T) {
	defer goleak.VerifyNone(t)

	rapid.Check(t, func(rt *rapid.T) {
		vt := rapid.Int32Range(11, 600).Draw(rt, "visibilityTimeout")
		n := rapid.IntRange(1, 40).Draw(rt, "messages")
		spread := min(consumer.CoalesceWindow, vmSeconds(vt)-consumer.RenewalMargin-time.Millisecond)
		offsets := rapid.SliceOfN(rapid.Int64Range(0, spread.Milliseconds()), n, n).Draw(rt, "offsetMillis")
		msgs := vmMessages(n)
		rec := newVMRecorder(nil)
		var base time.Time

		synctest.Test(t, func(*testing.T) {
			base = time.Now()
			vm := startVM(rec.client, vt, 1, nil)
			for _, i := range vmOrder(offsets) {
				vmSleepUntil(base.Add(vmMillis(offsets[i])))
				consumer.SetReceivedAt(msgs[i], time.Now())
				vm.Track(msgs[i])
				synctest.Wait()
			}
			time.Sleep(vmQuiet)
			vm.Close()
		})

		first := base.Add(vmMillis(slices.Min(offsets))).Add(vmSeconds(vt) - consumer.RenewalMargin)
		reqs := rec.snapshot()
		assert.Len(rt, reqs, (n+consumer.MaxVisibilityBatch-1)/consumer.MaxVisibilityBatch)

		seen := make(map[string]int, n)
		for _, req := range reqs {
			assert.WithinDuration(rt, first, req.at, 0)
			assert.LessOrEqual(rt, len(req.entries), consumer.MaxVisibilityBatch)
			for _, e := range req.entries {
				seen[e.handle]++
				assert.Equal(rt, vt, e.timeout)
			}
		}
		for _, m := range msgs {
			assert.Equal(rt, 1, seen[m.Identifier()], "message %s", m.Identifier())
		}
	})
}

type vmBatchOutcome int

const (
	vmBatchOK vmBatchOutcome = iota
	vmBatchError
	vmBatchNil
	vmBatchFailed
)

type vmResponseSpec struct {
	unknownIDs  []string
	code        string
	message     string
	kind        vmBatchOutcome
	failMask    uint16
	dupMask     uint16
	untrackMask uint16
	senderFault bool
}

type vmEntryOutcome struct {
	code        string
	message     string
	failed      bool
	untracked   bool
	perEntry    bool
	senderFault bool
}

type vmFailureScript struct {
	vm       *consumer.VisibilityManager
	byHandle map[string]*consumer.TestMessage
	specs    []vmResponseSpec
	outcomes [][]vmEntryOutcome
	unknown  int
	mu       sync.Mutex
}

func drawVMResponseSpecs(t *rapid.T) []vmResponseSpec {
	unknownID := rapid.OneOf(
		rapid.SampledFrom([]string{"", "-1", "10", "01", " 1", "+1", "1.0", "99999999999999999999"}),
		rapid.StringMatching(`[a-z_]{1,8}`),
	)
	return rapid.SliceOfN(rapid.Custom(func(t *rapid.T) vmResponseSpec {
		return vmResponseSpec{
			kind:        vmBatchOutcome(rapid.IntRange(int(vmBatchOK), int(vmBatchFailed)).Draw(t, "kind")),
			failMask:    rapid.Uint16().Draw(t, "failMask"),
			dupMask:     rapid.Uint16().Draw(t, "dupMask"),
			untrackMask: rapid.Uint16().Draw(t, "untrackMask") & rapid.Uint16().Draw(t, "untrackMask2"),
			unknownIDs:  rapid.SliceOfN(unknownID, 0, 3).Draw(t, "unknownIDs"),
			code:        rapid.StringMatching(`[A-Za-z.]{0,16}`).Draw(t, "code"),
			message:     rapid.String().Draw(t, "message"),
			senderFault: rapid.Bool().Draw(t, "senderFault"),
		}
	}), 1, 20).Draw(t, "responses")
}

func (s *vmFailureScript) respond(call int, params *sqs.ChangeMessageVisibilityBatchInput) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
	spec := s.specs[call%len(s.specs)]
	outcomes := make([]vmEntryOutcome, len(params.Entries))
	var failed []types.BatchResultErrorEntry

	for i, e := range params.Entries {
		bit := uint16(1) << i
		o := &outcomes[i]
		if spec.untrackMask&bit != 0 {
			o.untracked = true
			s.vm.Untrack(s.byHandle[aws.ToString(e.ReceiptHandle)])
		}
		switch spec.kind {
		case vmBatchError:
			o.failed = true
		case vmBatchFailed:
			if spec.failMask&bit == 0 {
				continue
			}
			o.failed, o.perEntry = true, true
			o.code, o.message, o.senderFault = spec.code, spec.message, spec.senderFault
			entry := types.BatchResultErrorEntry{
				Id:          e.Id,
				Code:        aws.String(spec.code),
				Message:     aws.String(spec.message),
				SenderFault: spec.senderFault,
			}
			failed = append(failed, entry)
			if spec.dupMask&bit != 0 {
				failed = append(failed, entry)
			}
		}
	}

	s.mu.Lock()
	s.outcomes = append(s.outcomes, outcomes)
	if spec.kind == vmBatchFailed {
		s.unknown += len(spec.unknownIDs)
	}
	s.mu.Unlock()

	switch spec.kind {
	case vmBatchError:
		return nil, errVisibilityRequest
	case vmBatchNil:
		return nil, nil
	case vmBatchFailed:
		for _, id := range spec.unknownIDs {
			entry := types.BatchResultErrorEntry{Code: aws.String(spec.code)}
			if id != "" {
				entry.Id = aws.String(id)
			}
			failed = append(failed, entry)
		}
		return &sqs.ChangeMessageVisibilityBatchOutput{Failed: failed}, nil
	default:
		return &sqs.ChangeMessageVisibilityBatchOutput{}, nil
	}
}

type vmLog struct {
	handle      string
	code        string
	message     string
	timeout     int64
	senderFault bool
	perEntry    bool
	requestErr  bool
}

func vmExpectedLog(e vmSend, o vmEntryOutcome) vmLog {
	if o.perEntry {
		return vmLog{handle: e.handle, timeout: int64(e.timeout), code: o.code, message: o.message, senderFault: o.senderFault, perEntry: true}
	}
	return vmLog{handle: e.handle, timeout: int64(e.timeout), requestErr: true}
}

func vmFailureLogs(t assert.TestingT, h *fake.LogHandler) ([]vmLog, int) {
	var logs []vmLog
	unknown := 0
	for _, r := range visibilityFailureRecords(h) {
		assert.Equal(t, slog.LevelError, r.Level)
		assert.Equal(t, visibilityQueueURL, r.Attrs["queue_url"])
		if _, ok := r.Attrs["id"]; ok {
			unknown++
			continue
		}
		l := vmLog{}
		l.handle, _ = r.Attrs["receipt_handle"].(string)
		l.timeout, _ = r.Attrs["visibility_timeout"].(int64)
		if code, ok := r.Attrs["code"].(string); ok {
			l.perEntry, l.code = true, code
			l.senderFault, _ = r.Attrs["sender_fault"].(bool)
			l.message, _ = r.Attrs["error"].(string)
		} else {
			err, _ := r.Attrs["error"].(error)
			l.requestErr = errors.Is(err, errVisibilityRequest)
		}
		logs = append(logs, l)
	}
	return logs, unknown
}

type vmExtensionModel struct {
	deadline time.Time
	retryBy  time.Time
	k        int
	done     bool
}

type vmFailureCase struct {
	immediate map[string]int32
	receipts  map[string]time.Time
	vt        int32
	limit     int
}

func checkVMFailureRun(t assert.TestingT, c vmFailureCase, reqs []vmRequest, outcomes [][]vmEntryOutcome) ([]vmLog, int) {
	models := make(map[string]*vmExtensionModel, len(c.receipts))
	for h, at := range c.receipts {
		models[h] = &vmExtensionModel{deadline: at.Add(vmSeconds(c.vt))}
	}
	immediateSeen := make(map[string]int, len(c.immediate))
	var want []vmLog
	untracked := 0

	if !assert.Len(t, outcomes, len(reqs)) {
		return nil, 0
	}
	for r, req := range reqs {
		for i, e := range req.entries {
			o := outcomes[r][i]
			if timeout, ok := c.immediate[e.handle]; ok {
				immediateSeen[e.handle]++
				assert.Equal(t, timeout, e.timeout, "immediate change for %s", e.handle)
				if o.failed {
					want = append(want, vmExpectedLog(e, o))
				}
				continue
			}
			m := models[e.handle]
			if !assert.NotNil(t, m, "unknown handle %s", e.handle) || !assert.False(t, m.done, "request %d sent for finished %s", r, e.handle) {
				continue
			}
			assert.Equal(t, int32(m.k+1)*c.vt, e.timeout, "extension %d of %s", m.k+1, e.handle)
			assert.True(t, e.at.Before(m.deadline), "%s sent at %v after deadline %v", e.handle, e.at, m.deadline)
			assert.False(t, e.at.Before(m.deadline.Add(-consumer.RenewalMargin-consumer.CoalesceWindow)), "%s sent too early", e.handle)
			if !m.retryBy.IsZero() {
				assert.False(t, e.at.After(m.retryBy), "%s retried at %v, want by %v", e.handle, e.at, m.retryBy)
				m.retryBy = time.Time{}
			}
			switch {
			case o.untracked:
				m.done = true
				untracked++
			case o.failed:
				want = append(want, vmExpectedLog(e, o))
				if retryAt := e.at.Add(consumer.ExtensionRetryDelay); retryAt.Before(m.deadline) {
					m.retryBy = retryAt
				} else {
					m.done = true
				}
			default:
				m.k++
				m.deadline = e.at.Add(vmSeconds(e.timeout))
				m.done = m.k == c.limit
			}
		}
	}

	for h := range c.immediate {
		assert.Equal(t, 1, immediateSeen[h], "immediate change for %s", h)
	}
	for h, m := range models {
		assert.True(t, m.done, "%s left with pending extensions", h)
	}
	return want, untracked
}

// Feature: consumer-flow-control, Property 17: For any batch response (request error, nil output, arbitrary Failed entries including unknown ids), the manager does not panic, logs exactly one Error record per failed still-tracked or immediate change with the required attributes, and leaves the deadline of failed Extensions unchanged.
func TestVisibilityManager_BatchFailureReporting(t *testing.T) {
	defer goleak.VerifyNone(t)

	rapid.Check(t, func(rt *rapid.T) {
		vt := rapid.Int32Range(11, 120).Draw(rt, "visibilityTimeout")
		limit := rapid.IntRange(1, 4).Draw(rt, "extensionLimit")
		trackedN := rapid.IntRange(1, 14).Draw(rt, "tracked")
		immediateN := rapid.IntRange(0, 12).Draw(rt, "immediate")
		waits := rapid.SliceOfN(rapid.Int64Range(0, 5_000), trackedN, trackedN).Draw(rt, "waitMillis")
		delays := rapid.SliceOfN(rapid.Int64Range(-5_000, int64(vmQuiet/time.Millisecond)), immediateN, immediateN).Draw(rt, "backoffMillis")
		isRelease := rapid.SliceOfN(rapid.Bool(), immediateN, immediateN).Draw(rt, "release")
		msgs := vmMessages(trackedN + immediateN)
		script := &vmFailureScript{specs: drawVMResponseSpecs(rt), byHandle: make(map[string]*consumer.TestMessage, len(msgs))}
		for _, m := range msgs {
			script.byHandle[m.Identifier()] = m
		}
		rec := newVMRecorder(script.respond)
		logs := fake.NewLogHandler()
		c := vmFailureCase{vt: vt, limit: limit, immediate: map[string]int32{}, receipts: map[string]time.Time{}}
		tracked := -1

		synctest.Test(t, func(*testing.T) {
			script.vm = startVM(rec.client, vt, limit, slog.New(logs))
			base := time.Now()
			for i, m := range msgs[:trackedN] {
				consumer.SetReceivedAt(m, base.Add(-vmMillis(waits[i])))
				c.receipts[m.Identifier()] = base.Add(-vmMillis(waits[i]))
				script.vm.Track(m)
				synctest.Wait()
			}
			var releases []*consumer.TestMessage
			for i, m := range msgs[trackedN:] {
				if isRelease[i] {
					releases = append(releases, m)
					c.immediate[m.Identifier()] = 0
					continue
				}
				script.vm.Backoff(m, vmMillis(delays[i]))
				c.immediate[m.Identifier()] = vmBackoffSeconds(vmMillis(delays[i]))
				synctest.Wait()
			}
			script.vm.Release(releases...)
			synctest.Wait()
			time.Sleep(vmQuiet)
			script.vm.Close()
			tracked = script.vm.Tracked()
		})

		script.mu.Lock()
		outcomes, unknown := script.outcomes, script.unknown
		script.mu.Unlock()

		want, untracked := checkVMFailureRun(rt, c, rec.snapshot(), outcomes)
		got, gotUnknown := vmFailureLogs(rt, logs)
		assert.ElementsMatch(rt, want, got)
		assert.Equal(rt, unknown, gotUnknown)
		assert.Equal(rt, trackedN-untracked, tracked)
	})
}

func TestVisibilityManager_Backoff(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		name  string
		delay time.Duration
		want  int32
	}{
		{name: "negative delay", delay: -5 * time.Second, want: 0},
		{name: "one nanosecond below zero", delay: -time.Nanosecond, want: 0},
		{name: "zero", delay: 0, want: 0},
		{name: "sub-second", delay: 999 * time.Millisecond, want: 0},
		{name: "fractional seconds truncate", delay: 1500 * time.Millisecond, want: 1},
		{name: "whole seconds", delay: 30 * time.Second, want: 30},
		{name: "exactly twelve hours", delay: 12 * time.Hour, want: int32(consumer.MaxVisibilitySeconds)},
		{name: "above twelve hours", delay: 12*time.Hour + time.Second, want: int32(consumer.MaxVisibilitySeconds)},
		{name: "maximum duration", delay: time.Duration(math.MaxInt64), want: int32(consumer.MaxVisibilitySeconds)},
		{name: "minimum duration", delay: time.Duration(math.MinInt64), want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				rec := newVMRecorder(nil)
				vm := startVM(rec.client, 30, 3, nil)
				msg := vmMessages(1)[0]
				vm.Track(msg)
				time.Sleep(3 * time.Second)
				at := time.Now()

				vm.Backoff(msg, tt.delay)
				synctest.Wait()

				reqs := rec.snapshot()
				require.Len(t, reqs, 1)
				assert.WithinDuration(t, at, reqs[0].at, 0)
				assert.Equal(t, []visibilityPair{{handle: msg.Identifier(), timeout: tt.want}}, vmPairs(reqs[0]))
				assert.Zero(t, vm.Tracked())

				time.Sleep(vmQuiet)
				assert.Len(t, rec.snapshot(), 1)
				vm.Close()
			})
		})
	}
}

func TestVisibilityManager_BackoffClampProperty(t *testing.T) {
	defer goleak.VerifyNone(t)

	rapid.Check(t, func(rt *rapid.T) {
		delay := time.Duration(rapid.Int64().Draw(rt, "delay"))
		rec := newVMRecorder(nil)
		msg := vmMessages(1)[0]

		synctest.Test(t, func(*testing.T) {
			vm := startVM(rec.client, 30, 1, nil)
			vm.Backoff(msg, delay)
			vm.Close()
		})

		reqs := rec.snapshot()
		require.Len(rt, reqs, 1)
		require.Len(rt, reqs[0].entries, 1)
		got := reqs[0].entries[0].timeout
		assert.GreaterOrEqual(rt, got, int32(0))
		assert.LessOrEqual(rt, int64(got), consumer.MaxVisibilitySeconds)
		assert.Equal(rt, vmBackoffSeconds(delay), got)
	})
}

func TestVisibilityManager_Release(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		name  string
		sizes []int
		count int
	}{
		{name: "no messages", count: 0, sizes: nil},
		{name: "one message", count: 1, sizes: []int{1}},
		{name: "several messages", count: 3, sizes: []int{3}},
		{name: "exactly one full batch", count: 10, sizes: []int{10}},
		{name: "one more than a batch", count: 11, sizes: []int{10, 1}},
		{name: "many messages", count: 25, sizes: []int{10, 10, 5}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				rec := newVMRecorder(nil)
				vm := startVM(rec.client, 30, 3, nil)
				msgs := vmMessages(tt.count)
				for _, m := range msgs {
					vm.Track(m)
				}
				synctest.Wait()
				at := time.Now()

				vm.Release(msgs...)
				synctest.Wait()

				reqs := rec.snapshot()
				require.Len(t, reqs, len(tt.sizes))
				var released []string
				for i, req := range reqs {
					assert.WithinDuration(t, at, req.at, 0)
					assert.Len(t, req.entries, tt.sizes[i])
					for _, e := range req.entries {
						assert.Zero(t, e.timeout)
						released = append(released, e.handle)
					}
				}
				assert.ElementsMatch(t, vmHandles(tt.count), released)
				assert.Zero(t, vm.Tracked())

				time.Sleep(vmQuiet)
				assert.Len(t, rec.snapshot(), len(tt.sizes))
				vm.Close()
			})
		})
	}
}

func TestVisibilityManager_CloseFlushesQueuedChanges(t *testing.T) {
	defer goleak.VerifyNone(t)

	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		rec := newVMRecorder(func(call int, _ *sqs.ChangeMessageVisibilityBatchInput) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
			if call == 0 {
				<-gate
			}
			return &sqs.ChangeMessageVisibilityBatchOutput{}, nil
		})
		vm := startVM(rec.client, 30, 3, nil)
		msgs := vmMessages(5)
		vm.Track(msgs[4])
		vm.Backoff(msgs[0], 5*time.Second)
		synctest.Wait()
		vm.Backoff(msgs[1], 7*time.Second)
		vm.Release(msgs[2], msgs[3])

		closed := make(chan struct{})
		go func() {
			vm.Close()
			close(closed)
		}()
		synctest.Wait()
		select {
		case <-closed:
			assert.Fail(t, "Close returned while a request was in flight")
		default:
		}

		close(gate)
		<-closed

		reqs := rec.snapshot()
		require.Len(t, reqs, 2)
		assert.Equal(t, []visibilityPair{{handle: msgs[0].Identifier(), timeout: 5}}, vmPairs(reqs[0]))
		assert.Equal(t, []visibilityPair{
			{handle: msgs[1].Identifier(), timeout: 7},
			{handle: msgs[2].Identifier(), timeout: 0},
			{handle: msgs[3].Identifier(), timeout: 0},
		}, vmPairs(reqs[1]))
		assert.Equal(t, 1, vm.Tracked())

		time.Sleep(vmQuiet)
		assert.Len(t, rec.snapshot(), 2)
	})
}

func TestVisibilityManager_Close(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		name        string
		started     bool
		trackCount  int
		backoffs    int
		wantTracked int
	}{
		{name: "never started", started: false, trackCount: 1, backoffs: 1, wantTracked: 1},
		{name: "started and idle", started: true},
		{name: "started with tracked messages left", started: true, trackCount: 3, wantTracked: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				rec := newVMRecorder(nil)
				vm := consumer.NewVisibilityManager(rec.client, visibilityQueueURL, 30, 3, nil)
				if tt.started {
					vm.Start(context.Background())
				}
				msgs := vmMessages(tt.trackCount + tt.backoffs)
				for _, m := range msgs[:tt.trackCount] {
					vm.Track(m)
				}
				for _, m := range msgs[tt.trackCount:] {
					vm.Backoff(m, time.Minute)
				}
				synctest.Wait()

				vm.Close()

				time.Sleep(vmQuiet)
				assert.Empty(t, rec.snapshot())
				assert.Equal(t, tt.wantTracked, vm.Tracked())
			})
		})
	}
}

func TestVisibilityManager_NilLogger(t *testing.T) {
	defer goleak.VerifyNone(t)

	synctest.Test(t, func(t *testing.T) {
		rec := newVMRecorder(func(int, *sqs.ChangeMessageVisibilityBatchInput) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
			return nil, errVisibilityRequest
		})
		vm := startVM(rec.client, 30, 2, nil)
		msgs := vmMessages(2)
		vm.Track(msgs[0])
		vm.Backoff(msgs[1], time.Minute)

		assert.NotPanics(t, func() { time.Sleep(vmQuiet) })
		vm.Close()

		sends := rec.sendsByHandle()
		assert.Len(t, sends[msgs[0].Identifier()], vmMaxAttempts())
		assert.Len(t, sends[msgs[1].Identifier()], 1)
	})
}

func TestVisibilityManager_Tracked(t *testing.T) {
	defer goleak.VerifyNone(t)

	for _, limit := range []int{0, 2} {
		t.Run(fmt.Sprintf("extension limit %d", limit), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				rec := newVMRecorder(nil)
				vm := startVM(rec.client, 30, limit, nil)
				msgs := vmMessages(4)
				assert.Zero(t, vm.Tracked())

				for _, m := range msgs[:3] {
					vm.Track(m)
				}
				assert.Equal(t, 3, vm.Tracked())

				vm.Untrack(msgs[3])
				assert.Equal(t, 3, vm.Tracked())

				vm.Untrack(msgs[0])
				assert.Equal(t, 2, vm.Tracked())

				vm.Untrack(msgs[0])
				assert.Equal(t, 2, vm.Tracked())

				vm.Backoff(msgs[1], time.Second)
				assert.Equal(t, 1, vm.Tracked())

				vm.Release(msgs[2])
				assert.Zero(t, vm.Tracked())

				synctest.Wait()
				time.Sleep(vmQuiet)
				vm.Close()

				assert.Equal(t, []visibilityPair{
					{handle: msgs[1].Identifier(), timeout: 1},
					{handle: msgs[2].Identifier(), timeout: 0},
				}, vmPairs(vmRequest{entries: rec.sends()}))
			})
		})
	}
}

func TestVisibilityManager_RetryAfterFailure(t *testing.T) {
	defer goleak.VerifyNone(t)

	rapid.Check(t, func(rt *rapid.T) {
		vt := rapid.Int32Range(11, 600).Draw(rt, "visibilityTimeout")
		failures := rapid.IntRange(0, 15).Draw(rt, "failures")
		perEntry := rapid.Bool().Draw(rt, "perEntry")
		rec := newVMRecorder(func(call int, _ *sqs.ChangeMessageVisibilityBatchInput) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
			switch {
			case call >= failures:
				return &sqs.ChangeMessageVisibilityBatchOutput{}, nil
			case perEntry:
				return &sqs.ChangeMessageVisibilityBatchOutput{Failed: []types.BatchResultErrorEntry{
					{Id: aws.String("0"), Code: aws.String("InternalError"), Message: aws.String("boom"), SenderFault: true},
				}}, nil
			default:
				return nil, errVisibilityRequest
			}
		})
		logs := fake.NewLogHandler()
		msg := vmMessages(1)[0]
		var base time.Time

		synctest.Test(t, func(*testing.T) {
			base = time.Now()
			vm := startVM(rec.client, vt, 1, slog.New(logs))
			vm.Track(msg)
			time.Sleep(vmQuiet)
			vm.Close()
		})

		attempts := min(failures+1, vmMaxAttempts())
		due := base.Add(vmSeconds(vt) - consumer.RenewalMargin)
		got := rec.sends()
		require.Len(rt, got, attempts)
		for i, s := range got {
			assert.WithinDuration(rt, due.Add(time.Duration(i)*consumer.ExtensionRetryDelay), s.at, 0, "attempt %d", i+1)
			assert.Equal(rt, vt, s.timeout, "attempt %d", i+1)
		}

		records, unknown := vmFailureLogs(rt, logs)
		assert.Zero(rt, unknown)
		want := vmLog{handle: msg.Identifier(), timeout: int64(vt), requestErr: true}
		if perEntry {
			want = vmLog{handle: msg.Identifier(), timeout: int64(vt), code: "InternalError", message: "boom", senderFault: true, perEntry: true}
		}
		assert.ElementsMatch(rt, slices.Repeat([]vmLog{want}, min(failures, vmMaxAttempts())), records)
	})
}

func TestVisibilityManager_UntrackedDuringFlight(t *testing.T) {
	defer goleak.VerifyNone(t)

	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		rec := newVMRecorder(func(call int, _ *sqs.ChangeMessageVisibilityBatchInput) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
			if call > 0 {
				return &sqs.ChangeMessageVisibilityBatchOutput{}, nil
			}
			<-gate
			return &sqs.ChangeMessageVisibilityBatchOutput{Failed: []types.BatchResultErrorEntry{
				{Id: aws.String("0"), Code: aws.String("InternalError")},
				{Id: aws.String("1"), Code: aws.String("InternalError")},
			}}, nil
		})
		logs := fake.NewLogHandler()
		vm := startVM(rec.client, 30, 2, slog.New(logs))
		msgs := vmMessages(2)
		base := time.Now()
		vm.Track(msgs[0])
		vm.Track(msgs[1])

		time.Sleep(20 * time.Second)
		synctest.Wait()
		vm.Untrack(msgs[0])
		close(gate)
		time.Sleep(vmQuiet)
		vm.Close()

		reqs := rec.snapshot()
		require.Len(t, reqs, 3)
		assert.Equal(t, []visibilityPair{{handle: msgs[0].Identifier(), timeout: 30}, {handle: msgs[1].Identifier(), timeout: 30}}, vmPairs(reqs[0]))
		assert.Equal(t, []visibilityPair{{handle: msgs[1].Identifier(), timeout: 30}}, vmPairs(reqs[1]))
		assert.Equal(t, []visibilityPair{{handle: msgs[1].Identifier(), timeout: 60}}, vmPairs(reqs[2]))
		assert.WithinDuration(t, base.Add(21*time.Second), reqs[1].at, 0)
		assert.WithinDuration(t, base.Add(41*time.Second), reqs[2].at, 0)

		records, _ := vmFailureLogs(t, logs)
		assert.Equal(t, []vmLog{{handle: msgs[1].Identifier(), timeout: 30, code: "InternalError", perEntry: true}}, records)
		assert.Equal(t, 1, vm.Tracked())
	})
}

type vmSingleRecorder struct {
	sends []vmSend
	err   error
	mu    sync.Mutex
}

func (r *vmSingleRecorder) client() consumer.SQSClient {
	return standardClient(&fake.SQSClient{
		ChangeMessageVisibilityFunc: func(
			_ context.Context,
			params *sqs.ChangeMessageVisibilityInput,
			_ ...func(*sqs.Options),
		) (*sqs.ChangeMessageVisibilityOutput, error) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.sends = append(r.sends, vmSend{at: time.Now(), handle: aws.ToString(params.ReceiptHandle), timeout: params.VisibilityTimeout})
			if r.err != nil {
				return nil, r.err
			}
			return &sqs.ChangeMessageVisibilityOutput{}, nil
		},
	})
}

func (r *vmSingleRecorder) snapshot() []vmSend {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.sends)
}

type vmFallbackScript struct {
	waits     []int64
	vt        int32
	limit     int
	releaseAt time.Duration
	released  int
}

func runVMFallbackScript(t *testing.T, client consumer.SQSClient, s vmFallbackScript) time.Time {
	var base time.Time
	synctest.Test(t, func(*testing.T) {
		base = time.Now()
		vm := startVM(client, s.vt, s.limit, nil)
		msgs := vmMessages(len(s.waits))
		for i, m := range msgs {
			consumer.SetReceivedAt(m, base.Add(-vmMillis(s.waits[i])))
			vm.Track(m)
			synctest.Wait()
		}
		time.Sleep(s.releaseAt)
		vm.Backoff(msgs[0], s.releaseAt)
		vm.Release(msgs[1 : 1+s.released]...)
		synctest.Wait()
		time.Sleep(vmQuiet)
		vm.Close()
	})
	return base
}

func vmRelativeSends(base time.Time, sends []vmSend) []visibilityPair {
	out := make([]visibilityPair, len(sends))
	for i, s := range sends {
		out[i] = visibilityPair{handle: fmt.Sprintf("%s@%v", s.handle, s.at.Sub(base)), timeout: s.timeout}
	}
	return out
}

func TestVisibilityManager_SingleClientFallback(t *testing.T) {
	defer goleak.VerifyNone(t)

	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(1, 12).Draw(rt, "messages")
		vt := rapid.Int32Range(11, 120).Draw(rt, "visibilityTimeout")
		s := vmFallbackScript{
			vt:        vt,
			limit:     rapid.IntRange(0, 3).Draw(rt, "extensionLimit"),
			waits:     rapid.SliceOfN(rapid.Int64Range(0, int64(vt)*1000), n, n).Draw(rt, "waitMillis"),
			releaseAt: vmMillis(rapid.Int64Range(0, 3*int64(vt)*1000).Draw(rt, "releaseAtMillis")),
			released:  rapid.IntRange(0, n-1).Draw(rt, "released"),
		}
		batch := newVMRecorder(nil)
		single := &vmSingleRecorder{}

		batchBase := runVMFallbackScript(t, batch.client, s)
		singleBase := runVMFallbackScript(t, single.client(), s)

		want := vmRelativeSends(batchBase, batch.sends())
		got := vmRelativeSends(singleBase, single.snapshot())
		require.NotEmpty(rt, want)
		assert.ElementsMatch(rt, want, got)
	})
}

func TestVisibilityManager_SingleClientFailureIsLogged(t *testing.T) {
	defer goleak.VerifyNone(t)

	synctest.Test(t, func(t *testing.T) {
		single := &vmSingleRecorder{err: errVisibilityRequest}
		logs := fake.NewLogHandler()
		vm := startVM(single.client(), 30, 1, slog.New(logs))
		msg := vmMessages(1)[0]

		vm.Backoff(msg, 5*time.Second)
		synctest.Wait()
		vm.Close()

		assert.Equal(t, []visibilityPair{{handle: msg.Identifier(), timeout: 5}}, vmPairs(vmRequest{entries: single.snapshot()}))
		records, unknown := vmFailureLogs(t, logs)
		assert.Zero(t, unknown)
		assert.Equal(t, []vmLog{{handle: msg.Identifier(), timeout: 5, requestErr: true}}, records)
	})
}

func TestVisibilityManager_ZeroReceivedAtIsStampedAtTrack(t *testing.T) {
	defer goleak.VerifyNone(t)

	rapid.Check(t, func(rt *rapid.T) {
		vt := rapid.Int32Range(11, 600).Draw(rt, "visibilityTimeout")
		trackAfter := vmMillis(rapid.Int64Range(0, int64(vmQuiet/time.Millisecond)).Draw(rt, "trackAfterMillis"))
		rec := newVMRecorder(nil)
		msg := vmMessages(1)[0]
		var trackAt time.Time

		synctest.Test(t, func(*testing.T) {
			vm := startVM(rec.client, vt, 1, nil)
			time.Sleep(trackAfter)
			trackAt = time.Now()
			vm.Track(msg)
			time.Sleep(vmQuiet)
			vm.Close()
		})

		got := rec.sends()
		require.Len(rt, got, 1)
		assert.WithinDuration(rt, trackAt.Add(vmSeconds(vt)-consumer.RenewalMargin), got[0].at, 0)
		assert.Equal(rt, vt, got[0].timeout)
	})
}

func TestVisibilityManager_ExtensionNotSent(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		name  string
		wait  time.Duration
		vt    int32
		limit int
	}{
		{name: "extension limit zero", vt: 30, limit: 0},
		{name: "received twelve hours ago", vt: 30, limit: 3, wait: time.Duration(consumer.MaxVisibilitySeconds) * time.Second},
		{name: "received more than twelve hours ago", vt: 30, limit: 3, wait: 13 * time.Hour},
		{name: "visibility timeout equal to renewal margin", vt: 10, limit: 3},
		{name: "visibility timeout below renewal margin", vt: 5, limit: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				rec := newVMRecorder(nil)
				vm := startVM(rec.client, tt.vt, tt.limit, nil)
				msg := vmMessages(1)[0]
				consumer.SetReceivedAt(msg, time.Now().Add(-tt.wait))

				vm.Track(msg)
				time.Sleep(vmQuiet)
				vm.Close()

				assert.Empty(t, rec.snapshot())
				assert.Equal(t, 1, vm.Tracked())
			})
		})
	}
}

func TestVisibilityManager_CoalesceWindowEdges(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		name    string
		offset  time.Duration
		wantAt  []time.Duration
		wantLen []int
		vt      int32
	}{
		{name: "second due at the window edge", vt: 60, offset: consumer.CoalesceWindow, wantAt: []time.Duration{50 * time.Second}, wantLen: []int{2}},
		{
			name: "second due past the window", vt: 60, offset: consumer.CoalesceWindow + time.Millisecond,
			wantAt: []time.Duration{50 * time.Second, 55*time.Second + time.Millisecond}, wantLen: []int{1, 1},
		},
		{
			name: "second would not move its deadline later", vt: 11, offset: 2 * time.Second,
			wantAt: []time.Duration{time.Second, 3 * time.Second}, wantLen: []int{1, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				rec := newVMRecorder(nil)
				vm := startVM(rec.client, tt.vt, 1, nil)
				msgs := vmMessages(2)
				base := time.Now()
				consumer.SetReceivedAt(msgs[0], base)
				vm.Track(msgs[0])
				time.Sleep(tt.offset)
				consumer.SetReceivedAt(msgs[1], time.Now())
				vm.Track(msgs[1])
				time.Sleep(vmQuiet)
				vm.Close()

				reqs := rec.snapshot()
				require.Len(t, reqs, len(tt.wantAt))
				for i, req := range reqs {
					assert.WithinDuration(t, base.Add(tt.wantAt[i]), req.at, 0, "request %d", i)
					assert.Len(t, req.entries, tt.wantLen[i], "request %d", i)
					for _, e := range req.entries {
						assert.Equal(t, tt.vt, e.timeout)
					}
				}
			})
		})
	}
}
