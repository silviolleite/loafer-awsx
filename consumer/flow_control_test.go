package consumer_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/consumer"
	"github.com/silviolleite/loafer-awsx/fake"
	"github.com/silviolleite/loafer-awsx/middleware"
	"github.com/silviolleite/loafer-awsx/router"
)

const (
	flowQueueName       = "flow-queue"
	flowQueueURL        = "https://sqs.us-east-1.amazonaws.com/123456789012/flow-queue"
	flowFIFOQueueName   = "flow-queue.fifo"
	flowFIFOQueueURL    = "https://sqs.us-east-1.amazonaws.com/123456789012/flow-queue.fifo"
	flowDLQURL          = "https://sqs.us-east-1.amazonaws.com/123456789012/flow-dlq"
	holdWarnMessage     = "holding message to preserve FIFO group order; earlier message in group failed"
	handlerErrorMessage = "handler returned an error"
	exhaustedMessage    = "message exhausted; left in source queue for AWS SQS redrive"
	maxReceiveBatch     = 10
)

var (
	errFlowHandler       = errors.New("flow handler failed")
	errFlowOrchestration = errors.New("flow orchestration failed")
)

type flowOutcome int

const (
	flowSuccess flowOutcome = iota
	flowError
	flowBackoff
)

type flowTrigger int

const (
	triggerSettled flowTrigger = iota
	triggerHandlerStart
	triggerReceive
)

type flowMessage struct {
	handle             string
	body               string
	groupID            string
	backoff            time.Duration
	index              int
	outcome            flowOutcome
	retryCount         int
	receiveCount       int
	orchestrationFails bool
}

func (m flowMessage) sqsMessage() types.Message {
	return types.Message{
		ReceiptHandle: aws.String(m.handle),
		Body:          aws.String(m.body),
		Attributes: map[string]string{
			"MessageGroupId":          m.groupID,
			"ApproximateReceiveCount": strconv.Itoa(m.receiveCount),
		},
		MessageAttributes: map[string]types.MessageAttributeValue{
			"retry_count": {DataType: aws.String("Number"), StringValue: aws.String(strconv.Itoa(m.retryCount))},
		},
	}
}

type flowScenario struct {
	messages           []flowMessage
	chunks             []int
	retryModel         router.RetryModel
	runMode            router.Mode
	trigger            flowTrigger
	triggerAt          int
	workers            int
	prefetch           int
	pollers            int
	maxRetryCount      int
	dlqMaxReceiveCount int
	maxMessages        int32
	batchVisibility    bool
	deleteBatch        bool
	gated              bool
	fifo               bool
}

func (s flowScenario) queueName() string {
	if s.fifo {
		return flowFIFOQueueName
	}
	return flowQueueName
}

func (s flowScenario) queueURL() string {
	if s.fifo {
		return flowFIFOQueueURL
	}
	return flowQueueURL
}

func (s flowScenario) receiveBatch() int32 {
	return min(s.maxMessages, int32(min(s.prefetch+1, maxReceiveBatch)))
}

func (s flowScenario) routeOptions() []router.Option {
	opts := []router.Option{
		router.WithWorkerPoolSize(s.workers),
		router.WithPrefetch(s.prefetch),
		router.WithPollers(s.pollers),
		router.WithMaxMessages(s.maxMessages),
		router.WithRunMode(s.runMode),
		router.WithExtensionLimit(0),
	}
	if s.retryModel == router.ScheduledRetryModel {
		opts = append(opts, router.WithScheduledRetry(
			router.WithSchedulerIdentity("arn:aws:sqs:us-east-1:123456789012:flow-queue", "arn:aws:iam::123456789012:role/scheduler"),
			router.WithScheduledDLQ(flowDLQURL),
			router.WithMaxRetryCount(s.maxRetryCount),
			router.WithBackoff(time.Second, 2*time.Second),
		))
	}
	if s.dlqMaxReceiveCount > 0 {
		opts = append(opts, router.WithDLQ(s.dlqMaxReceiveCount))
	}
	if s.deleteBatch {
		opts = append(opts, router.WithDeleteBatch())
	}
	return opts
}

func (s flowScenario) failingBodies() map[string]bool {
	out := make(map[string]bool)
	for _, m := range s.messages {
		if m.orchestrationFails {
			out[m.body] = true
		}
	}
	return out
}

func (s flowScenario) expectedHeld() map[string]bool {
	held := make(map[string]bool)
	if s.runMode != router.PerGroupID || s.retryModel != router.VisibilityRetryModel {
		return held
	}
	failed := make(map[string]bool)
	for _, m := range s.messages {
		if failed[m.groupID] {
			held[m.handle] = true
			continue
		}
		if m.outcome != flowSuccess {
			failed[m.groupID] = true
		}
	}
	return held
}

func drawFlowMessages(t *rapid.T, n int) []flowMessage {
	groups := rapid.IntRange(1, 4).Draw(t, "groups")
	outcomes := rapid.SampledFrom([]flowOutcome{flowSuccess, flowError, flowBackoff})
	out := make([]flowMessage, n)
	for i := range out {
		id := strconv.Itoa(i)
		out[i] = flowMessage{
			handle:             "flow-receipt-" + id,
			body:               "flow-body-" + id,
			groupID:            "group-" + strconv.Itoa(rapid.IntRange(0, groups-1).Draw(t, "group")),
			index:              i,
			outcome:            outcomes.Draw(t, "outcome"),
			backoff:            drawBackoffDelay(t, "backoff"),
			retryCount:         rapid.IntRange(0, 4).Draw(t, "retryCount"),
			receiveCount:       rapid.IntRange(1, 4).Draw(t, "receiveCount"),
			orchestrationFails: rapid.Bool().Draw(t, "orchestrationFails"),
		}
	}
	return out
}

func drawFlowScenario(t *rapid.T) flowScenario {
	s := flowScenario{
		retryModel:      rapid.SampledFrom([]router.RetryModel{router.VisibilityRetryModel, router.ScheduledRetryModel}).Draw(t, "retryModel"),
		runMode:         rapid.SampledFrom([]router.Mode{router.Parallel, router.PerGroupID}).Draw(t, "runMode"),
		workers:         rapid.IntRange(1, 4).Draw(t, "workers"),
		prefetch:        rapid.IntRange(0, 4).Draw(t, "prefetch"),
		pollers:         rapid.IntRange(1, 3).Draw(t, "pollers"),
		maxMessages:     rapid.Int32Range(1, maxReceiveBatch).Draw(t, "maxMessages"),
		maxRetryCount:   rapid.IntRange(0, 3).Draw(t, "maxRetryCount"),
		batchVisibility: rapid.Bool().Draw(t, "batchVisibility"),
		fifo:            rapid.Bool().Draw(t, "fifo"),
	}
	s.deleteBatch = s.batchVisibility && rapid.Bool().Draw(t, "deleteBatch")
	if s.retryModel == router.VisibilityRetryModel {
		s.dlqMaxReceiveCount = rapid.IntRange(0, 3).Draw(t, "dlqMaxReceiveCount")
	}
	s.messages = drawFlowMessages(t, rapid.IntRange(0, 20).Draw(t, "messages"))
	s.chunks = rapid.SliceOfN(rapid.IntRange(0, maxReceiveBatch), 1, 5).Draw(t, "chunks")
	s.chunks[len(s.chunks)-1] = max(s.chunks[len(s.chunks)-1], 1)
	return s
}

func gateFlowScenario(s flowScenario) flowScenario {
	s.gated = true
	s.trigger = triggerSettled
	s.prefetch = len(s.messages) + s.pollers*maxReceiveBatch
	return s
}

type flowProbe struct {
	cancel       context.CancelFunc
	scripts      map[string]flowMessage
	received     map[string]int
	started      map[string]int
	finished     map[string]int
	active       map[string]int
	admitted     chan struct{}
	queue        []flowMessage
	startOrder   []string
	scenario     flowScenario
	overlaps     int
	receiveCalls int
	drained      int
	starts       int
	settled      int
	mu           sync.Mutex
}

func newFlowProbe(s flowScenario, cancel context.CancelFunc) *flowProbe {
	p := &flowProbe{
		cancel:   cancel,
		scripts:  make(map[string]flowMessage, len(s.messages)),
		received: make(map[string]int),
		started:  make(map[string]int),
		finished: make(map[string]int),
		active:   make(map[string]int),
		admitted: make(chan struct{}),
		queue:    slices.Clone(s.messages),
		scenario: s,
	}
	for _, m := range s.messages {
		p.scripts[m.handle] = m
	}
	return p
}

func (p *flowProbe) cancelIfSettledLocked() {
	if len(p.queue) == 0 && p.settled == len(p.scenario.messages) {
		p.cancel()
	}
}

func (p *flowProbe) receive(ctx context.Context, params *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	p.mu.Lock()
	call := p.receiveCalls
	p.receiveCalls++
	if len(p.queue) == 0 {
		p.drained++
		if p.scenario.gated && p.drained == p.scenario.pollers {
			close(p.admitted)
		}
		p.cancelIfSettledLocked()
		p.mu.Unlock()
		<-ctx.Done()
		return nil, ctx.Err()
	}

	n := min(len(p.queue), int(params.MaxNumberOfMessages), p.scenario.chunks[call%len(p.scenario.chunks)])
	msgs := make([]types.Message, n)
	for i := range n {
		msgs[i] = p.queue[i].sqsMessage()
		p.received[p.queue[i].handle]++
	}
	p.queue = p.queue[n:]
	if p.scenario.trigger == triggerReceive && call == p.scenario.triggerAt {
		p.cancel()
	}
	p.mu.Unlock()

	return &sqs.ReceiveMessageOutput{Messages: msgs}, nil
}

func (p *flowProbe) start(m flowMessage) {
	p.mu.Lock()
	p.started[m.handle]++
	p.startOrder = append(p.startOrder, m.handle)
	p.active[m.groupID]++
	if p.active[m.groupID] > 1 && p.scenario.runMode == router.PerGroupID {
		p.overlaps++
	}
	if p.scenario.trigger == triggerHandlerStart && p.starts == p.scenario.triggerAt {
		p.cancel()
	}
	p.starts++
	p.mu.Unlock()

	if p.scenario.gated {
		<-p.admitted
	}
}

func (p *flowProbe) finish(m flowMessage) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.finished[m.handle]++
	p.active[m.groupID]--
	p.settled++
	p.cancelIfSettledLocked()
}

func (p *flowProbe) hold() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.settled++
	p.cancelIfSettledLocked()
}

func (p *flowProbe) handler(_ context.Context, m middleware.Message) error {
	script := p.scripts[m.Identifier()]
	p.start(script)
	defer p.finish(script)

	switch script.outcome {
	case flowError:
		return errFlowHandler
	case flowBackoff:
		if msg, ok := m.(consumer.Message); ok {
			msg.Backoff(script.backoff)
		}
	default:
	}
	return nil
}

type holdSignal struct {
	*fake.LogHandler
	onHold func()
}

func (h holdSignal) Handle(ctx context.Context, r slog.Record) error {
	err := h.LogHandler.Handle(ctx, r)
	if r.Level == slog.LevelWarn && r.Message == holdWarnMessage {
		h.onHold()
	}
	return err
}

type flowRun struct {
	err   error
	probe *flowProbe
	sqs   *fake.SQSClient
	sched *fake.SchedulerClient
	logs  *fake.LogHandler
}

func newFlowScheduler(failing map[string]bool) *fake.SchedulerClient {
	return &fake.SchedulerClient{
		CreateScheduleFunc: func(_ context.Context, params *scheduler.CreateScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.CreateScheduleOutput, error) {
			if failing[scheduledBody(params)] {
				return nil, errFlowOrchestration
			}
			return &scheduler.CreateScheduleOutput{}, nil
		},
	}
}

func scheduledBody(params *scheduler.CreateScheduleInput) string {
	var request struct {
		MessageBody string `json:"MessageBody"`
	}
	if params.Target == nil || json.Unmarshal([]byte(aws.ToString(params.Target.Input)), &request) != nil {
		return ""
	}
	return request.MessageBody
}

func runFlowScenario(t require.TestingT, s flowScenario) flowRun {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	failing := s.failingBodies()
	run := flowRun{
		probe: newFlowProbe(s, cancel),
		sched: newFlowScheduler(failing),
		logs:  fake.NewLogHandler(),
	}
	run.sqs = &fake.SQSClient{
		GetQueueUrlFunc: func(_ context.Context, _ *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
			return &sqs.GetQueueUrlOutput{QueueUrl: aws.String(s.queueURL())}, nil
		},
		ReceiveMessageFunc: run.probe.receive,
		SendMessageFunc: func(_ context.Context, params *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			if failing[aws.ToString(params.MessageBody)] {
				return nil, errFlowOrchestration
			}
			return &sqs.SendMessageOutput{}, nil
		},
	}

	var client consumer.SQSClient = run.sqs
	if s.batchVisibility {
		run.sqs.ChangeMessageVisibilityBatchFunc = succeedVisibilityBatch
	} else {
		client = standardClient(run.sqs)
	}

	route, err := router.New(s.queueName(), run.probe.handler, s.routeOptions()...)
	require.NoError(t, err)
	logger := slog.New(holdSignal{LogHandler: run.logs, onHold: run.probe.hold})
	c, err := consumer.New(client, route, consumer.WithSchedulerClient(run.sched), consumer.WithLogger(logger))
	require.NoError(t, err)

	run.err = c.Run(ctx)
	return run
}

type flowObservation struct {
	deletes   map[string]int
	changes   map[string][]int32
	schedules map[string]int
	published map[string]int
	holds     map[string][]fake.LogRecord
	errorLogs map[string][]string
}

func observeFlow(run flowRun) flowObservation {
	obs := flowObservation{
		deletes:   make(map[string]int),
		changes:   visibilityChangesByHandle(run.sqs),
		schedules: make(map[string]int),
		published: make(map[string]int),
		holds:     make(map[string][]fake.LogRecord),
		errorLogs: make(map[string][]string),
	}
	for _, call := range run.sqs.DeleteMessageCalls() {
		obs.deletes[aws.ToString(call.ReceiptHandle)]++
	}
	for _, call := range run.sqs.DeleteMessageBatchCalls() {
		for _, entry := range call.Entries {
			obs.deletes[aws.ToString(entry.ReceiptHandle)]++
		}
	}
	for _, call := range run.sched.CreateScheduleCalls() {
		obs.schedules[scheduledBody(call)]++
	}
	for _, call := range run.sqs.SendMessageCalls() {
		if aws.ToString(call.QueueUrl) == flowDLQURL {
			obs.published[aws.ToString(call.MessageBody)]++
		}
	}
	for _, rec := range run.logs.Records() {
		switch {
		case rec.Level == slog.LevelWarn && rec.Message == holdWarnMessage:
			handle, _ := rec.Attrs["receipt_handle"].(string)
			obs.holds[handle] = append(obs.holds[handle], rec)
		case rec.Level == slog.LevelError && rec.Message == handlerErrorMessage:
			handle, _ := rec.Attrs["receipt_handle"].(string)
			obs.errorLogs[handle] = append(obs.errorLogs[handle], rec.Message)
		case rec.Level == slog.LevelError && rec.Message == exhaustedMessage:
			handle, _ := rec.Attrs["message_id"].(string)
			obs.errorLogs[handle] = append(obs.errorLogs[handle], rec.Message)
		}
	}
	return obs
}

func assertFlowOutcomes(t assert.TestingT, s flowScenario, run flowRun) flowObservation {
	assert.NoError(t, run.err)
	obs := observeFlow(run)
	p := run.probe

	assert.Zero(t, p.overlaps)
	assertFlowClientPaths(t, s, run)
	if s.pollers == 1 && s.runMode == router.PerGroupID {
		assertGroupStartOrder(t, p)
	}
	for _, m := range s.messages {
		assertMessageOutcome(t, s, p, obs, m)
	}
	return obs
}

func assertFlowClientPaths(t assert.TestingT, s flowScenario, run flowRun) {
	if s.batchVisibility {
		assert.Empty(t, run.sqs.ChangeMessageVisibilityCalls())
	} else {
		assert.Empty(t, run.sqs.ChangeMessageVisibilityBatchCalls())
	}
	if s.deleteBatch {
		assert.Empty(t, run.sqs.DeleteMessageCalls())
	} else {
		assert.Empty(t, run.sqs.DeleteMessageBatchCalls())
	}
	assertReceiveRequestSizes(t, s, run.sqs.ReceiveMessageCalls())
	for _, call := range run.sqs.ChangeMessageVisibilityCalls() {
		assert.Equal(t, s.queueURL(), aws.ToString(call.QueueUrl))
	}
	for _, call := range run.sqs.ChangeMessageVisibilityBatchCalls() {
		assert.Equal(t, s.queueURL(), aws.ToString(call.QueueUrl))
		assert.LessOrEqual(t, len(call.Entries), maxReceiveBatch)
	}
}

func assertReceiveRequestSizes(t assert.TestingT, s flowScenario, calls []*sqs.ReceiveMessageInput) {
	batch := s.receiveBatch()
	if !assert.NotEmpty(t, calls) {
		return
	}
	assert.True(t, slices.ContainsFunc(calls, func(in *sqs.ReceiveMessageInput) bool {
		return in.MaxNumberOfMessages == batch
	}))
	for _, call := range calls {
		assert.GreaterOrEqual(t, call.MaxNumberOfMessages, int32(1))
		assert.LessOrEqual(t, call.MaxNumberOfMessages, batch)
		if s.gated {
			assert.Equal(t, batch, call.MaxNumberOfMessages)
		}
	}
}

func assertGroupStartOrder(t assert.TestingT, p *flowProbe) {
	last := make(map[string]int)
	for _, handle := range p.startOrder {
		m := p.scripts[handle]
		if prev, ok := last[m.groupID]; ok {
			assert.Less(t, prev, m.index, handle)
		}
		last[m.groupID] = m.index
	}
}

func assertMessageOutcome(t assert.TestingT, s flowScenario, p *flowProbe, obs flowObservation, m flowMessage) {
	h := m.handle
	switch {
	case p.received[h] == 0:
		assert.Zero(t, p.started[h], h)
		assertUntouched(t, obs, m)
		assert.Empty(t, obs.changes[h], h)
	case p.started[h] > 0:
		assert.Equal(t, 1, p.received[h], h)
		assert.Equal(t, 1, p.started[h], h)
		assert.Equal(t, 1, p.finished[h], h)
		assert.Empty(t, obs.holds[h], h)
		if s.retryModel == router.ScheduledRetryModel {
			assertScheduledOutcome(t, s, obs, m)
		} else {
			assertVisibilityOutcome(t, s, obs, m)
		}
	case len(obs.holds[h]) > 0:
		assert.Equal(t, 1, p.received[h], h)
		assertHeldOutcome(t, s, p, obs, m)
	default:
		assert.Equal(t, 1, p.received[h], h)
		assertUntouched(t, obs, m)
		assert.Equal(t, []int32{0}, obs.changes[h], h)
	}
}

func assertUntouched(t assert.TestingT, obs flowObservation, m flowMessage) {
	assert.Zero(t, obs.deletes[m.handle], m.handle)
	assert.Zero(t, obs.schedules[m.body], m.handle)
	assert.Zero(t, obs.published[m.body], m.handle)
	assert.Empty(t, obs.holds[m.handle], m.handle)
	assert.Empty(t, obs.errorLogs[m.handle], m.handle)
}

func assertVisibilityOutcome(t assert.TestingT, s flowScenario, obs flowObservation, m flowMessage) {
	h := m.handle
	assert.Zero(t, obs.schedules[m.body], h)
	assert.Zero(t, obs.published[m.body], h)

	switch m.outcome {
	case flowSuccess:
		assert.Equal(t, 1, obs.deletes[h], h)
		assert.Empty(t, obs.changes[h], h)
		assert.Empty(t, obs.errorLogs[h], h)
	case flowError:
		want := handlerErrorMessage
		if s.dlqMaxReceiveCount > 0 && m.receiveCount >= s.dlqMaxReceiveCount {
			want = exhaustedMessage
		}
		assert.Zero(t, obs.deletes[h], h)
		assert.Empty(t, obs.changes[h], h)
		assert.Equal(t, []string{want}, obs.errorLogs[h], h)
	case flowBackoff:
		assert.Zero(t, obs.deletes[h], h)
		assert.Equal(t, []int32{expectedBackoffTimeout(m.backoff)}, obs.changes[h], h)
		assert.Empty(t, obs.errorLogs[h], h)
	}
}

func assertScheduledOutcome(t assert.TestingT, s flowScenario, obs flowObservation, m flowMessage) {
	wantDeletes, wantSchedules, wantPublished := 1, 0, 0
	if m.outcome != flowSuccess {
		if m.retryCount+1 <= s.maxRetryCount {
			wantSchedules = 1
		} else {
			wantPublished = 1
		}
		if m.orchestrationFails {
			wantDeletes = 0
		}
	}

	assert.Empty(t, obs.changes[m.handle], m.handle)
	assert.Equal(t, wantDeletes, obs.deletes[m.handle], m.handle)
	assert.Equal(t, wantSchedules, obs.schedules[m.body], m.handle)
	assert.Equal(t, wantPublished, obs.published[m.body], m.handle)
}

func assertHeldOutcome(t assert.TestingT, s flowScenario, p *flowProbe, obs flowObservation, m flowMessage) {
	h := m.handle
	assert.Equal(t, router.PerGroupID, s.runMode, h)
	assert.Equal(t, router.VisibilityRetryModel, s.retryModel, h)
	assert.Zero(t, obs.deletes[h], h)
	if s.fifo {
		assert.Equal(t, []int32{0}, obs.changes[h], h)
	} else {
		assert.Empty(t, obs.changes[h], h)
	}
	assert.Zero(t, obs.schedules[m.body], h)
	assert.Zero(t, obs.published[m.body], h)
	assert.True(t, hasFailedPeer(s, p, m), h)

	if assert.Len(t, obs.holds[h], 1, h) {
		attrs := obs.holds[h][0].Attrs
		assert.Equal(t, s.queueURL(), attrs["queue_url"], h)
		assert.Equal(t, m.groupID, attrs["group_id"], h)
	}
}

func hasFailedPeer(s flowScenario, p *flowProbe, m flowMessage) bool {
	for _, peer := range s.messages {
		if peer.groupID != m.groupID || peer.index == m.index || p.started[peer.handle] == 0 || peer.outcome == flowSuccess {
			continue
		}
		if s.pollers > 1 || peer.index < m.index {
			return true
		}
	}
	return false
}

// Feature: consumer-flow-control, Property 4: For any configuration, receive script, and handler outcome script (success, error, backoff, scheduled success/failure), each admitted message reaches exactly one Outcome, and after shutdown available() == capacity.
func TestRun_PropertyEveryAdmittedMessageReachesOneOutcome(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		defer goleak.VerifyNone(rt)

		s := drawFlowScenario(rt)
		if rapid.Bool().Draw(rt, "gated") {
			s = gateFlowScenario(s)
		}

		run := runFlowScenario(rt, s)
		obs := assertFlowOutcomes(rt, s, run)

		for _, m := range s.messages {
			assert.Equal(rt, 1, run.probe.received[m.handle], m.handle)
			assert.True(rt, run.probe.started[m.handle] == 1 || len(obs.holds[m.handle]) == 1, m.handle)
		}
	})
}

type groupWorkScript struct {
	groupOf map[string]string
	order   []string
	picks   []int
	workers int
}

func newGroupWorkScript(workers int, groups []int, picks []int) groupWorkScript {
	s := groupWorkScript{
		groupOf: make(map[string]string, len(groups)),
		order:   make([]string, len(groups)),
		picks:   picks,
		workers: workers,
	}
	for i, g := range groups {
		handle := "work-" + strconv.Itoa(i)
		s.order[i] = handle
		s.groupOf[handle] = "group-" + strconv.Itoa(g)
	}
	return s
}

func drawGroupWorkScript(t *rapid.T) groupWorkScript {
	workers := rapid.IntRange(1, 16).Draw(t, "workers")
	groupCount := rapid.IntRange(workers, workers+4).Draw(t, "groupCount")
	var groups []int
	for g := range groupCount {
		for range 1 + rapid.IntRange(0, 2).Draw(t, "extra") {
			groups = append(groups, g)
		}
	}
	groups = rapid.Permutation(groups).Draw(t, "receiveOrder")
	picks := rapid.SliceOfN(rapid.IntRange(0, 1<<10), len(groups), len(groups)).Draw(t, "picks")
	return newGroupWorkScript(workers, groups, picks)
}

type groupWorkProbe struct {
	running map[string]bool
	queue   []string
	mu      sync.Mutex
}

func (p *groupWorkProbe) snapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.running))
	for h := range p.running {
		out = append(out, h)
	}
	slices.Sort(out)
	return out
}

func (p *groupWorkProbe) receive(ctx context.Context, params *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	p.mu.Lock()
	n := min(len(p.queue), int(params.MaxNumberOfMessages))
	handles := p.queue[:n]
	p.queue = p.queue[n:]
	p.mu.Unlock()

	if n == 0 {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return messageBatch(handles...), nil
}

func groupWorkClient(s groupWorkScript, p *groupWorkProbe) *fake.SQSClient {
	return &fake.SQSClient{
		GetQueueUrlFunc: func(_ context.Context, _ *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
			return &sqs.GetQueueUrlOutput{QueueUrl: aws.String(flowQueueURL)}, nil
		},
		ReceiveMessageFunc: func(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			out, err := p.receive(ctx, params, optFns...)
			if out != nil {
				for i := range out.Messages {
					out.Messages[i].Attributes = map[string]string{"MessageGroupId": s.groupOf[aws.ToString(out.Messages[i].ReceiptHandle)]}
				}
			}
			return out, err
		},
	}
}

func runGroupWork(c *collectedFailures, s groupWorkScript) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gates := make(map[string]chan struct{}, len(s.order))
	for _, h := range s.order {
		gates[h] = make(chan struct{})
	}
	probe := &groupWorkProbe{running: make(map[string]bool), queue: slices.Clone(s.order)}
	handler := func(_ context.Context, m middleware.Message) error {
		h := m.Identifier()
		probe.mu.Lock()
		probe.running[h] = true
		probe.mu.Unlock()
		<-gates[h]
		probe.mu.Lock()
		delete(probe.running, h)
		probe.mu.Unlock()
		return nil
	}

	client := groupWorkClient(s, probe)
	route, err := router.New(flowQueueName, handler,
		router.WithRunMode(router.PerGroupID),
		router.WithWorkerPoolSize(s.workers),
		router.WithPrefetch(len(s.order)+maxReceiveBatch),
		router.WithExtensionLimit(0),
	)
	if !assert.NoError(c, err) {
		return
	}
	cons, err := consumer.New(client, route)
	if !assert.NoError(c, err) {
		return
	}

	done := make(chan error, 1)
	go func() { done <- cons.Run(ctx) }()

	closed := make(map[string]bool)
	defer func() {
		for h, gate := range gates {
			if !closed[h] {
				close(gate)
			}
		}
		cancel()
		assert.NoError(c, <-done)
		assert.Len(c, client.DeleteMessageCalls(), len(s.order))
	}()

	stepGroupWork(c, s, probe, gates, closed)
}

func stepGroupWork(c *collectedFailures, s groupWorkScript, probe *groupWorkProbe, gates map[string]chan struct{}, closed map[string]bool) {
	remaining := make(map[string][]string)
	for _, h := range s.order {
		remaining[s.groupOf[h]] = append(remaining[s.groupOf[h]], h)
	}

	for step := range s.order {
		synctest.Wait()
		running := probe.snapshot()
		if !assert.Len(c, running, min(s.workers, len(remaining))) {
			return
		}
		for _, h := range running {
			if !assert.Equal(c, remaining[s.groupOf[h]][0], h) {
				return
			}
		}

		pick := running[s.picks[step]%len(running)]
		close(gates[pick])
		closed[pick] = true
		g := s.groupOf[pick]
		remaining[g] = remaining[g][1:]
		if len(remaining[g]) == 0 {
			delete(remaining, g)
		}
	}

	synctest.Wait()
	assert.Empty(c, probe.snapshot())
}

// Feature: consumer-flow-control, Property 7: For any WorkerPoolSize = w and g >= w distinct Group_Keys whose handlers block, w handlers are running at the same time. No Group_Key is bound to a fixed Worker.
func TestRun_PropertyGroupWorkConservation(t *testing.T) {
	defer goleak.VerifyNone(t)

	rapid.Check(t, func(rt *rapid.T) {
		s := drawGroupWorkScript(rt)
		failures := &collectedFailures{}

		synctest.Test(t, func(*testing.T) {
			runGroupWork(failures, s)
		})

		if len(failures.msgs) > 0 {
			rt.Fatalf("%s", strings.Join(failures.msgs, "\n"))
		}
	})
}

func TestRun_GroupSchedulingAvoidsHeadOfLineBlocking(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		name    string
		groups  []int
		picks   []int
		workers int
	}{
		{name: "second message of a group runs beside a blocked group", workers: 2, groups: []int{0, 1, 1}, picks: []int{1, 1, 0}},
		{name: "freed worker takes another ready group", workers: 2, groups: []int{0, 0, 1, 2}, picks: []int{1, 1, 0, 0}},
		{name: "single worker serves every group in turn", workers: 1, groups: []int{0, 1, 0, 2}, picks: []int{0, 0, 0, 0}},
		{name: "more workers than groups", workers: 4, groups: []int{0, 0, 1}, picks: []int{0, 0, 0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			failures := &collectedFailures{}

			synctest.Test(t, func(*testing.T) {
				runGroupWork(failures, newGroupWorkScript(tt.workers, tt.groups, tt.picks))
			})

			assert.Empty(t, failures.msgs)
		})
	}
}

// Feature: consumer-flow-control, Property 9: For any group script under the Visibility_Retry_Model in which message i of a group fails (error or backoff), no Pending_Message of that group submitted before the failure runs its handler and none is deleted; on a FIFO queue each held message receives exactly one visibility change with timeout 0, on other queues none. Each held message produces exactly one Warn record. Under the Scheduled_Retry_Model no message is held.
func TestRun_PropertyHoldOnFailure(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		defer goleak.VerifyNone(rt)

		s := drawFlowScenario(rt)
		s.runMode = router.PerGroupID
		s.pollers = 1
		s.messages = drawFlowMessages(rt, rapid.IntRange(1, 20).Draw(rt, "groupMessages"))
		s = gateFlowScenario(s)

		run := runFlowScenario(rt, s)
		obs := assertFlowOutcomes(rt, s, run)

		held := s.expectedHeld()
		for _, m := range s.messages {
			if held[m.handle] {
				assert.Zero(rt, run.probe.started[m.handle], m.handle)
				assert.Zero(rt, obs.deletes[m.handle], m.handle)
				assert.Len(rt, obs.holds[m.handle], 1, m.handle)
				if s.fifo {
					assert.Equal(rt, []int32{0}, obs.changes[m.handle], m.handle)
				} else {
					assert.Empty(rt, obs.changes[m.handle], m.handle)
				}
				continue
			}
			assert.Equal(rt, 1, run.probe.started[m.handle], m.handle)
			assert.Empty(rt, obs.holds[m.handle], m.handle)
		}
		if s.retryModel == router.ScheduledRetryModel {
			assert.Empty(rt, held)
			assert.Empty(rt, obs.holds)
		}
	})
}

func TestRun_HoldOnFailureExamples(t *testing.T) {
	defer goleak.VerifyNone(t)

	group := func(outcomes ...flowOutcome) []flowMessage {
		out := make([]flowMessage, len(outcomes))
		for i, o := range outcomes {
			id := strconv.Itoa(i)
			out[i] = flowMessage{handle: "hold-" + id, body: "hold-body-" + id, groupID: "g", index: i, outcome: o, backoff: time.Minute}
		}
		return out
	}

	tests := []struct {
		name       string
		messages   []flowMessage
		wantHeld   []string
		wantRun    []string
		retryModel router.RetryModel
		fifo       bool
	}{
		{
			name:       "error holds the tail of the group",
			messages:   group(flowSuccess, flowError, flowSuccess, flowSuccess),
			retryModel: router.VisibilityRetryModel,
			wantRun:    []string{"hold-0", "hold-1"},
			wantHeld:   []string{"hold-2", "hold-3"},
		},
		{
			name:       "backoff holds the tail of the group",
			messages:   group(flowBackoff, flowSuccess),
			retryModel: router.VisibilityRetryModel,
			wantRun:    []string{"hold-0"},
			wantHeld:   []string{"hold-1"},
		},
		{
			name:       "failure of the last message holds nothing",
			messages:   group(flowSuccess, flowError),
			retryModel: router.VisibilityRetryModel,
			wantRun:    []string{"hold-0", "hold-1"},
		},
		{
			name:       "scheduled model never holds",
			messages:   group(flowError, flowBackoff, flowSuccess),
			retryModel: router.ScheduledRetryModel,
			wantRun:    []string{"hold-0", "hold-1", "hold-2"},
		},
		{
			name:       "error on a fifo queue releases the tail of the group",
			messages:   group(flowSuccess, flowError, flowSuccess, flowSuccess),
			retryModel: router.VisibilityRetryModel,
			fifo:       true,
			wantRun:    []string{"hold-0", "hold-1"},
			wantHeld:   []string{"hold-2", "hold-3"},
		},
		{
			name:       "backoff on a fifo queue releases the tail of the group",
			messages:   group(flowBackoff, flowSuccess),
			retryModel: router.VisibilityRetryModel,
			fifo:       true,
			wantRun:    []string{"hold-0"},
			wantHeld:   []string{"hold-1"},
		},
		{
			name:       "scheduled model on a fifo queue never holds",
			messages:   group(flowError, flowBackoff, flowSuccess),
			retryModel: router.ScheduledRetryModel,
			fifo:       true,
			wantRun:    []string{"hold-0", "hold-1", "hold-2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := gateFlowScenario(flowScenario{
				messages:        tt.messages,
				chunks:          []int{maxReceiveBatch},
				retryModel:      tt.retryModel,
				runMode:         router.PerGroupID,
				workers:         2,
				pollers:         1,
				maxMessages:     maxReceiveBatch,
				maxRetryCount:   3,
				batchVisibility: true,
				fifo:            tt.fifo,
			})

			run := runFlowScenario(t, s)
			obs := assertFlowOutcomes(t, s, run)

			assert.ElementsMatch(t, tt.wantRun, run.probe.startOrder)
			held := make([]string, 0, len(obs.holds))
			for h := range obs.holds {
				held = append(held, h)
			}
			assert.ElementsMatch(t, tt.wantHeld, held)
			for _, h := range tt.wantHeld {
				if tt.fifo {
					assert.Equal(t, []int32{0}, obs.changes[h], h)
				} else {
					assert.Empty(t, obs.changes[h], h)
				}
			}
		})
	}
}

// Feature: consumer-flow-control, Property 19: For any configuration and any cancellation instant, Consumer.Run returns only after every Active_Message reaches its Outcome. Every Pending_Message that was not held receives exactly one visibility change with timeout 0 and never runs its handler. No goroutine started by Run remains.
func TestRun_PropertyShutdownReleasesPending(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		defer goleak.VerifyNone(rt)

		s := drawFlowScenario(rt)
		s.trigger = rapid.SampledFrom([]flowTrigger{triggerHandlerStart, triggerReceive}).Draw(rt, "trigger")
		s.triggerAt = rapid.IntRange(0, len(s.messages)).Draw(rt, "triggerAt")

		run := runFlowScenario(rt, s)
		assertFlowOutcomes(rt, s, run)

		for h, n := range run.probe.started {
			assert.Equal(rt, n, run.probe.finished[h], h)
		}
	})
}

func TestRun_ShutdownWaitsForActiveMessage(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		name       string
		runMode    router.Mode
		retryModel router.RetryModel
		batch      bool
	}{
		{name: "parallel visibility batch client", runMode: router.Parallel, retryModel: router.VisibilityRetryModel, batch: true},
		{name: "per group visibility single client", runMode: router.PerGroupID, retryModel: router.VisibilityRetryModel},
		{name: "parallel scheduled single client", runMode: router.Parallel, retryModel: router.ScheduledRetryModel},
		{name: "per group scheduled batch client", runMode: router.PerGroupID, retryModel: router.ScheduledRetryModel, batch: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				runShutdownWaitsForActive(t, tt.runMode, tt.retryModel, tt.batch)
			})
		})
	}
}

func runShutdownWaitsForActive(t *testing.T, runMode router.Mode, retryModel router.RetryModel, batch bool) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	const active = "receipt-active"
	pending := []string{"receipt-pending-0", "receipt-pending-1", "receipt-pending-2"}
	proceed := make(chan struct{})
	var mu sync.Mutex
	var handled []string

	handler := func(_ context.Context, m middleware.Message) error {
		mu.Lock()
		handled = append(handled, m.Identifier())
		mu.Unlock()
		if m.Identifier() == active {
			cancel()
			<-proceed
		}
		return nil
	}

	var receives int
	client := &fake.SQSClient{
		GetQueueUrlFunc: func(_ context.Context, _ *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
			return &sqs.GetQueueUrlOutput{QueueUrl: aws.String(flowQueueURL)}, nil
		},
		ReceiveMessageFunc: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			receives++
			if receives > 1 {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			out := messageBatch(append([]string{active}, pending...)...)
			for i := range out.Messages {
				out.Messages[i].Attributes = map[string]string{"MessageGroupId": "group-" + strconv.Itoa(i)}
			}
			return out, nil
		},
	}
	var sqsClient consumer.SQSClient = client
	if !batch {
		sqsClient = standardClient(client)
	}

	s := flowScenario{workers: 1, prefetch: len(pending), pollers: 1, maxMessages: 4, runMode: runMode, retryModel: retryModel, maxRetryCount: 1}
	route, err := router.New(flowQueueName, handler, s.routeOptions()...)
	require.NoError(t, err)
	c, err := consumer.New(sqsClient, route, consumer.WithSchedulerClient(&fake.SchedulerClient{}))
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	synctest.Wait()

	require.Empty(t, done)
	assert.Empty(t, client.DeleteMessageCalls())

	close(proceed)
	require.NoError(t, <-done)

	assert.Equal(t, []string{active}, handled)
	assert.Equal(t, []string{active}, deleteMessageHandles(client.DeleteMessageCalls()))
	want := make(map[string][]int32, len(pending))
	for _, h := range pending {
		want[h] = []int32{0}
	}
	assert.Equal(t, want, visibilityChangesByHandle(client))
}

// Feature: consumer-flow-control, Property 20: For any script run with a client that does not implement BatchVisibilityClient, the Consumer runs without error and every visibility change is sent through ChangeMessageVisibility with the same timeouts the batch path would use.
func TestRun_PropertyFallbackWithoutBatchClient(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		defer goleak.VerifyNone(rt)

		s := drawFlowScenario(rt)
		s.batchVisibility = false
		s.deleteBatch = false
		s.trigger = rapid.SampledFrom([]flowTrigger{triggerSettled, triggerHandlerStart, triggerReceive}).Draw(rt, "trigger")
		s.triggerAt = rapid.IntRange(0, len(s.messages)).Draw(rt, "triggerAt")

		single := runFlowScenario(rt, s)
		assertFlowOutcomes(rt, s, single)
		assert.Empty(rt, single.sqs.ChangeMessageVisibilityBatchCalls())

		gated := s
		gated.pollers = 1
		gated = gateFlowScenario(gated)
		fallback := runFlowScenario(rt, gated)
		assertFlowOutcomes(rt, gated, fallback)

		gated.batchVisibility = true
		batched := runFlowScenario(rt, gated)
		assertFlowOutcomes(rt, gated, batched)

		assert.Empty(rt, fallback.sqs.ChangeMessageVisibilityBatchCalls())
		assert.Equal(rt, visibilityChangesByHandle(batched.sqs), visibilityChangesByHandle(fallback.sqs))
	})
}

func TestRun_DeleteBatchWithPollersPerGroupID(t *testing.T) {
	defer goleak.VerifyNone(t)

	messages := make([]flowMessage, 24)
	for i := range messages {
		id := strconv.Itoa(i)
		messages[i] = flowMessage{handle: "batch-" + id, body: "batch-body-" + id, groupID: "group-" + strconv.Itoa(i%3), index: i}
	}

	tests := []struct {
		name         string
		trigger      flowTrigger
		wantDeleted  int
		wantReleased bool
	}{
		{name: "every success is deleted through DeleteMessageBatch", trigger: triggerSettled, wantDeleted: len(messages)},
		{name: "shutdown flushes started deletes and releases pending", trigger: triggerHandlerStart, wantDeleted: 1, wantReleased: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := flowScenario{
				messages:        messages,
				chunks:          []int{maxReceiveBatch},
				retryModel:      router.VisibilityRetryModel,
				runMode:         router.PerGroupID,
				trigger:         tt.trigger,
				workers:         1,
				prefetch:        len(messages),
				pollers:         2,
				maxMessages:     maxReceiveBatch,
				batchVisibility: true,
				deleteBatch:     true,
			}

			run := runFlowScenario(t, s)
			obs := assertFlowOutcomes(t, s, run)

			assert.Empty(t, run.sqs.DeleteMessageCalls())
			batches := run.sqs.DeleteMessageBatchCalls()
			require.NotEmpty(t, batches)
			deleted := 0
			for _, call := range batches {
				assert.Equal(t, flowQueueURL, aws.ToString(call.QueueUrl))
				assert.LessOrEqual(t, len(call.Entries), maxReceiveBatch)
				deleted += len(call.Entries)
			}
			assert.Equal(t, tt.wantDeleted, deleted)
			assert.Len(t, obs.deletes, deleted)
			if tt.wantReleased {
				assert.Len(t, run.probe.startOrder, 1)
				assert.NotEmpty(t, obs.changes)
			} else {
				assert.Empty(t, obs.changes)
				assert.Len(t, obs.deletes, len(messages))
			}
		})
	}
}

const (
	skipLogMessage        = "skipping message whose visibility timeout expired before it started"
	metricPanicLogMessage = "metric hook panicked; recovered and continuing"
	skipActiveHandle      = "receipt-active"
	skipPendingHandle     = "receipt-pending"
)

var errSkipVisibility = errors.New("visibility change failed")

type skipRecorderKind int

const (
	skipRecorderNone skipRecorderKind = iota
	skipRecorderPlain
	skipRecorderCounting
	skipRecorderPanicking
)

type skipSignals struct {
	skipped []string
	dlq     []string
	onDLQ   []string
	mu      sync.Mutex
}

func (s *skipSignals) add(dst *[]string, v string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	*dst = append(*dst, v)
}

func (s *skipSignals) snapshot() ([]string, []string, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.skipped), slices.Clone(s.dlq), slices.Clone(s.onDLQ)
}

type plainSkipRecorder struct {
	signals *skipSignals
}

func (r plainSkipRecorder) IncDLQ(route string) { r.signals.add(&r.signals.dlq, route) }

func (plainSkipRecorder) IncSuccess(string) {}

func (plainSkipRecorder) IncRetry(string) {}

func (plainSkipRecorder) IncDeadLetter(string) {}

type countingSkipRecorder struct {
	plainSkipRecorder
}

func (r countingSkipRecorder) IncSkipped(route string) { r.signals.add(&r.signals.skipped, route) }

type panickingSkipRecorder struct {
	plainSkipRecorder
}

func (r panickingSkipRecorder) IncSkipped(route string) {
	r.signals.add(&r.signals.skipped, route)
	panic("skip recorder failed")
}

var (
	_ consumer.SkipRecorder = countingSkipRecorder{}
	_ consumer.SkipRecorder = panickingSkipRecorder{}
)

func (k skipRecorderKind) recorder(signals *skipSignals) consumer.MetricsRecorder {
	plain := plainSkipRecorder{signals: signals}
	switch k {
	case skipRecorderPlain:
		return plain
	case skipRecorderCounting:
		return countingSkipRecorder{plainSkipRecorder: plain}
	case skipRecorderPanicking:
		return panickingSkipRecorder{plainSkipRecorder: plain}
	default:
		return nil
	}
}

type skipScenario struct {
	messageID      string
	groupID        string
	runMode        router.Mode
	recorder       skipRecorderKind
	receiveCount   int
	dlqThreshold   int
	vt             int32
	failExtensions bool
}

func drawSkipScenario(t *rapid.T) skipScenario {
	return skipScenario{
		messageID:      rapid.StringMatching(`[a-f0-9-]{0,36}`).Draw(t, "messageID"),
		groupID:        rapid.StringMatching(`[a-z0-9]{0,8}`).Draw(t, "groupID"),
		runMode:        rapid.SampledFrom([]router.Mode{router.Parallel, router.PerGroupID}).Draw(t, "runMode"),
		recorder:       skipRecorderKind(rapid.IntRange(int(skipRecorderNone), int(skipRecorderPanicking)).Draw(t, "recorder")),
		receiveCount:   rapid.IntRange(1, 10).Draw(t, "receiveCount"),
		dlqThreshold:   rapid.IntRange(0, 10).Draw(t, "dlqThreshold"),
		vt:             rapid.Int32Range(11, 120).Draw(t, "visibilityTimeout"),
		failExtensions: rapid.Bool().Draw(t, "failExtensions"),
	}
}

func (s skipScenario) isExhausted() bool {
	return s.dlqThreshold > 0 && s.receiveCount >= s.dlqThreshold
}

func (s skipScenario) pendingMessage() types.Message {
	m := types.Message{
		ReceiptHandle: aws.String(skipPendingHandle),
		Body:          aws.String("pending"),
		Attributes:    map[string]string{"ApproximateReceiveCount": strconv.Itoa(s.receiveCount)},
	}
	if s.messageID != "" {
		m.MessageId = aws.String(s.messageID)
	}
	if s.groupID != "" {
		m.Attributes["MessageGroupId"] = s.groupID
	}
	return m
}

func (s skipScenario) routeOptions(signals *skipSignals) []router.Option {
	opts := []router.Option{
		router.WithWorkerPoolSize(1),
		router.WithPrefetch(1),
		router.WithMaxMessages(2),
		router.WithVisibilityTimeout(s.vt),
		router.WithRunMode(s.runMode),
	}
	if !s.failExtensions {
		opts = append(opts, router.WithExtensionLimit(0))
	}
	if s.dlqThreshold > 0 {
		opts = append(opts, router.WithDLQ(s.dlqThreshold, router.WithOnDLQ(func(_ context.Context, m middleware.Message) {
			signals.add(&signals.onDLQ, m.Identifier())
		})))
	}
	return opts
}

type skipRun struct {
	err             error
	client          *fake.SQSClient
	logs            *fake.LogHandler
	signals         *skipSignals
	handledEarly    []string
	handled         []string
	visibilityCalls int
}

func runSkipScenario(t *testing.T, s skipScenario) skipRun {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	run := skipRun{logs: fake.NewLogHandler(), signals: &skipSignals{}}
	release := make(chan struct{})
	var mu sync.Mutex
	handler := func(_ context.Context, m middleware.Message) error {
		mu.Lock()
		run.handled = append(run.handled, m.Identifier())
		mu.Unlock()
		if m.Identifier() == skipActiveHandle {
			<-release
		}
		return nil
	}

	var receives int
	run.client = &fake.SQSClient{
		GetQueueUrlFunc: func(_ context.Context, _ *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
			return &sqs.GetQueueUrlOutput{QueueUrl: aws.String(flowQueueURL)}, nil
		},
		ReceiveMessageFunc: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			receives++
			if receives > 1 {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			active := types.Message{
				ReceiptHandle: aws.String(skipActiveHandle),
				Body:          aws.String("active"),
				Attributes:    map[string]string{"MessageGroupId": "active-group"},
			}
			return &sqs.ReceiveMessageOutput{Messages: []types.Message{active, s.pendingMessage()}}, nil
		},
	}
	if s.failExtensions {
		run.client.ChangeMessageVisibilityBatchFunc = func(
			context.Context, *sqs.ChangeMessageVisibilityBatchInput, ...func(*sqs.Options),
		) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
			return nil, errSkipVisibility
		}
	}

	route, err := router.New(flowQueueName, handler, s.routeOptions(run.signals)...)
	require.NoError(t, err)
	opts := []consumer.Option{consumer.WithLogger(slog.New(run.logs))}
	if rec := s.recorder.recorder(run.signals); rec != nil {
		opts = append(opts, consumer.WithMetrics(rec))
	}
	c, err := consumer.New(run.client, route, opts...)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	time.Sleep(time.Duration(s.vt)*time.Second + time.Second)
	synctest.Wait()

	mu.Lock()
	run.handledEarly = slices.Clone(run.handled)
	mu.Unlock()
	run.visibilityCalls = len(run.client.ChangeMessageVisibilityBatchCalls()) + len(run.client.ChangeMessageVisibilityCalls())

	close(release)
	synctest.Wait()
	cancel()
	run.err = <-done
	return run
}

func logsWithMessage(h *fake.LogHandler, level slog.Level, message string) []fake.LogRecord {
	var out []fake.LogRecord
	for _, r := range h.Records() {
		if r.Level == level && r.Message == message {
			out = append(out, r)
		}
	}
	return out
}

func assertSkipSignals(t assert.TestingT, s skipScenario, run skipRun) {
	assert.NoError(t, run.err)
	assert.Equal(t, []string{skipActiveHandle}, run.handledEarly)
	assert.Equal(t, []string{skipActiveHandle}, run.handled)
	assert.Equal(t, []string{skipActiveHandle}, deleteMessageHandles(run.client.DeleteMessageCalls()))
	if s.failExtensions {
		assert.Positive(t, run.visibilityCalls)
	} else {
		assert.Zero(t, run.visibilityCalls)
	}

	skips := logsWithMessage(run.logs, slog.LevelError, skipLogMessage)
	if assert.Len(t, skips, 1) {
		assert.Equal(t, map[string]any{
			"queue_url":      flowQueueURL,
			"receipt_handle": skipPendingHandle,
			"sqs_message_id": s.messageID,
			"group_id":       s.groupID,
			"receive_count":  int64(s.receiveCount),
		}, skips[0].Attrs)
	}
	assert.Empty(t, logsWithMessage(run.logs, slog.LevelWarn, skipLogMessage))

	skipped, dlq, onDLQ := run.signals.snapshot()
	switch s.recorder {
	case skipRecorderCounting, skipRecorderPanicking:
		assert.Equal(t, []string{flowQueueName}, skipped)
	default:
		assert.Empty(t, skipped)
	}

	panics := logsWithMessage(run.logs, slog.LevelError, metricPanicLogMessage)
	if s.recorder == skipRecorderPanicking {
		if assert.Len(t, panics, 1) {
			assert.Equal(t, flowQueueName, panics[0].Attrs["route_name"])
			assert.Equal(t, "skip recorder failed", panics[0].Attrs["panic"])
		}
	} else {
		assert.Empty(t, panics)
	}

	exhausted := logsWithMessage(run.logs, slog.LevelError, exhaustedMessage)
	if !s.isExhausted() {
		assert.Empty(t, exhausted)
		assert.Empty(t, dlq)
		assert.Empty(t, onDLQ)
		return
	}
	if assert.Len(t, exhausted, 1) {
		assert.Equal(t, map[string]any{
			"queue_name":     flowQueueName,
			"message_id":     skipPendingHandle,
			"sqs_message_id": s.messageID,
			"receive_count":  int64(s.receiveCount),
		}, exhausted[0].Attrs)
	}
	assert.Equal(t, []string{skipPendingHandle}, onDLQ)
	if s.recorder == skipRecorderNone {
		assert.Empty(t, dlq)
	} else {
		assert.Equal(t, []string{flowQueueName}, dlq)
	}
}

// Feature: consumer-flow-control, Property 22: For any Skipped message (Extensions failing, or ExtensionLimit = 0 with a wait past the deadline), the handler never runs, exactly one Error record carries the queue URL, receipt handle, sqs_message_id, group id, and receive count, IncSkipped is invoked once when the recorder implements SkipRecorder and never otherwise, and the observe-only DLQ signals fire exactly when the receive count reached the configured MaxReceiveCount.
func TestRun_PropertySkipSignals(t *testing.T) {
	defer goleak.VerifyNone(t)

	rapid.Check(t, func(rt *rapid.T) {
		s := drawSkipScenario(rt)
		var run skipRun
		synctest.Test(t, func(t *testing.T) {
			run = runSkipScenario(t, s)
		})
		assertSkipSignals(rt, s, run)
	})
}

func TestRun_SkipSignalsExamples(t *testing.T) {
	defer goleak.VerifyNone(t)

	base := skipScenario{messageID: "sqs-id-1", groupID: "group-1", runMode: router.Parallel, receiveCount: 3, vt: 30}
	tests := []struct {
		modify func(*skipScenario)
		name   string
	}{
		{name: "no recorder and no dlq", modify: func(*skipScenario) {}},
		{name: "plain recorder without skip recorder", modify: func(s *skipScenario) { s.recorder = skipRecorderPlain }},
		{name: "skip recorder", modify: func(s *skipScenario) { s.recorder = skipRecorderCounting }},
		{name: "panicking skip recorder", modify: func(s *skipScenario) { s.recorder = skipRecorderPanicking }},
		{name: "failing extensions", modify: func(s *skipScenario) { s.failExtensions = true; s.recorder = skipRecorderCounting }},
		{name: "receive count below threshold", modify: func(s *skipScenario) { s.dlqThreshold = 4; s.recorder = skipRecorderCounting }},
		{name: "receive count at threshold", modify: func(s *skipScenario) { s.dlqThreshold = 3; s.recorder = skipRecorderCounting }},
		{name: "receive count above threshold without recorder", modify: func(s *skipScenario) { s.dlqThreshold = 2 }},
		{name: "panicking skip recorder still emits dlq", modify: func(s *skipScenario) { s.dlqThreshold = 1; s.recorder = skipRecorderPanicking }},
		{name: "missing message and group ids", modify: func(s *skipScenario) { s.messageID = ""; s.groupID = ""; s.recorder = skipRecorderPlain }},
		{name: "per group id", modify: func(s *skipScenario) { s.runMode = router.PerGroupID; s.recorder = skipRecorderCounting }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := base
			tt.modify(&s)
			synctest.Test(t, func(t *testing.T) {
				run := runSkipScenario(t, s)
				assertSkipSignals(t, s, run)
			})
		})
	}
}

var (
	errModelReceiptHandle = errors.New("receipt handle is invalid")
	errModelNotInFlight   = errors.New("message is not in flight")
)

type modelMessage struct {
	visibleAt time.Time
	id        string
	groupID   string
	handle    string
	receives  int
	deleted   bool
}

type sqsModel struct {
	onDrained func()
	byHandle  map[string]*modelMessage
	messages  []*modelMessage
	deleted   int
	mu        sync.Mutex
}

func newSQSModel(n int, groups int, onDrained func()) *sqsModel {
	m := &sqsModel{onDrained: onDrained, byHandle: make(map[string]*modelMessage)}
	for i := range n {
		msg := &modelMessage{id: "message-" + strconv.Itoa(i)}
		if groups > 0 {
			msg.groupID = "group-" + strconv.Itoa(i%groups)
		}
		m.messages = append(m.messages, msg)
	}
	return m
}

func (m *sqsModel) client() *fake.SQSClient {
	return &fake.SQSClient{
		GetQueueUrlFunc: func(_ context.Context, _ *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
			return &sqs.GetQueueUrlOutput{QueueUrl: aws.String(flowQueueURL)}, nil
		},
		ReceiveMessageFunc:          m.receive,
		DeleteMessageFunc:           m.delete,
		ChangeMessageVisibilityFunc: m.changeVisibility,
	}
}

func (m *sqsModel) take(limit int32, visibility int32) ([]types.Message, time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	var (
		out  []types.Message
		next time.Time
	)
	for _, msg := range m.messages {
		if msg.deleted {
			continue
		}
		if msg.visibleAt.After(now) {
			if next.IsZero() || msg.visibleAt.Before(next) {
				next = msg.visibleAt
			}
			continue
		}
		if int32(len(out)) == limit {
			continue
		}
		delete(m.byHandle, msg.handle)
		msg.receives++
		msg.handle = msg.id + "#" + strconv.Itoa(msg.receives)
		msg.visibleAt = now.Add(time.Duration(visibility) * time.Second)
		m.byHandle[msg.handle] = msg
		attrs := map[string]string{"ApproximateReceiveCount": strconv.Itoa(msg.receives)}
		if msg.groupID != "" {
			attrs["MessageGroupId"] = msg.groupID
		}
		out = append(out, types.Message{
			MessageId:     aws.String(msg.id),
			ReceiptHandle: aws.String(msg.handle),
			Body:          aws.String(msg.id),
			Attributes:    attrs,
		})
	}
	return out, next
}

func (m *sqsModel) receive(ctx context.Context, params *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	waitUntil := time.Now().Add(time.Duration(params.WaitTimeSeconds) * time.Second)
	for {
		out, next := m.take(params.MaxNumberOfMessages, params.VisibilityTimeout)
		if len(out) > 0 || !time.Now().Before(waitUntil) {
			return &sqs.ReceiveMessageOutput{Messages: out}, nil
		}
		until := waitUntil
		if !next.IsZero() && next.Before(until) {
			until = next
		}
		timer := time.NewTimer(time.Until(until))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (m *sqsModel) inFlightLocked(handle string) (*modelMessage, error) {
	msg, ok := m.byHandle[handle]
	if !ok || msg.deleted {
		return nil, errModelReceiptHandle
	}
	if !msg.visibleAt.After(time.Now()) {
		return nil, errModelNotInFlight
	}
	return msg, nil
}

func (m *sqsModel) changeVisibility(
	_ context.Context, params *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options),
) (*sqs.ChangeMessageVisibilityOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	msg, err := m.inFlightLocked(aws.ToString(params.ReceiptHandle))
	if err != nil {
		return nil, err
	}
	msg.visibleAt = time.Now().Add(time.Duration(params.VisibilityTimeout) * time.Second)
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func (m *sqsModel) delete(_ context.Context, params *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	m.mu.Lock()
	msg, ok := m.byHandle[aws.ToString(params.ReceiptHandle)]
	if !ok || msg.deleted {
		m.mu.Unlock()
		return nil, errModelReceiptHandle
	}
	msg.deleted = true
	m.deleted++
	drained := m.deleted == len(m.messages)
	m.mu.Unlock()

	if drained {
		m.onDrained()
	}
	return &sqs.DeleteMessageOutput{}, nil
}

func (m *sqsModel) receiveCounts() map[string]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]int, len(m.messages))
	for _, msg := range m.messages {
		out[msg.id] = msg.receives
	}
	return out
}

func (m *sqsModel) deletedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.deleted
}

func TestRun_SlowSingleWorkerNeverSkipsPendingMessages(t *testing.T) {
	defer goleak.VerifyNone(t)

	const (
		messages    = 30
		handlerTime = 5 * time.Second
		timeLimit   = 3 * time.Minute
	)
	tests := []struct {
		name   string
		opts   []router.Option
		groups int
	}{
		{name: "parallel"},
		{name: "per group id", opts: []router.Option{router.WithRunMode(router.PerGroupID)}, groups: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()

				model := newSQSModel(messages, tt.groups, cancel)
				var mu sync.Mutex
				handled := make(map[string]int, messages)
				handler := func(_ context.Context, m middleware.Message) error {
					mu.Lock()
					handled[string(m.Body())]++
					mu.Unlock()
					time.Sleep(handlerTime)
					return nil
				}

				opts := append([]router.Option{router.WithWorkerPoolSize(1), router.WithVisibilityTimeout(11)}, tt.opts...)
				route, err := router.New(flowQueueName, handler, opts...)
				require.NoError(t, err)
				logs := fake.NewLogHandler()
				c, err := consumer.New(model.client(), route, consumer.WithLogger(slog.New(logs)))
				require.NoError(t, err)

				go func() {
					select {
					case <-ctx.Done():
					case <-time.After(timeLimit):
						cancel()
					}
				}()
				start := time.Now()
				require.NoError(t, c.Run(ctx))
				elapsed := time.Since(start)

				want := make(map[string]int, messages)
				for i := range messages {
					want["message-"+strconv.Itoa(i)] = 1
				}
				assert.Equal(t, messages, model.deletedCount())
				assert.Equal(t, want, handled)
				assert.Equal(t, want, model.receiveCounts())
				assert.LessOrEqual(t, elapsed, timeLimit)
				assert.Empty(t, logsWithMessage(logs, slog.LevelError, skipLogMessage))
				assert.Empty(t, logsWithMessage(logs, slog.LevelError, "failed to change message visibility"))
			})
		})
	}
}
