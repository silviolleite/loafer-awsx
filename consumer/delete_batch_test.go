package consumer_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
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
	verrors "github.com/silviolleite/loafer-awsx/errors"
	"github.com/silviolleite/loafer-awsx/fake"
	"github.com/silviolleite/loafer-awsx/middleware"
	"github.com/silviolleite/loafer-awsx/router"
)

const (
	commitQueueName = "orders-queue"
	commitQueueURL  = "https://sqs.us-east-1.amazonaws.com/123456789012/orders-queue"
	commitBackoff   = 30 * time.Second
)

var (
	errCommitHandler       = errors.New("handler failed")
	errCommitOrchestration = errors.New("orchestration failed")
)

type commitOutcome int

const (
	outcomeSuccess commitOutcome = iota
	outcomeError
	outcomeBackoff
)

type commitMessage struct {
	handle             string
	body               string
	groupID            string
	outcome            commitOutcome
	retryCount         int
	receiveCount       int
	orchestrationFails bool
}

type commitScenario struct {
	batches            [][]commitMessage
	retryModel         router.RetryModel
	runMode            router.Mode
	workers            int
	maxRetryCount      int
	dlqMaxReceiveCount int
	maxMessages        int32
}

type commitRun struct {
	sqs       *fake.SQSClient
	scheduler *fake.SchedulerClient
}

type sqsOnlyClient struct {
	consumer.SQSClient
}

func standardClient(c *fake.SQSClient) consumer.SQSClient {
	return sqsOnlyClient{SQSClient: c}
}

func batchCapableClient(c *fake.SQSClient) consumer.SQSClient {
	return c
}

func drawCommitScenario(t *rapid.T) commitScenario {
	s := commitScenario{
		retryModel:    rapid.SampledFrom([]router.RetryModel{router.VisibilityRetryModel, router.ScheduledRetryModel}).Draw(t, "retryModel"),
		runMode:       rapid.SampledFrom([]router.Mode{router.Parallel, router.PerGroupID}).Draw(t, "runMode"),
		workers:       rapid.IntRange(1, 4).Draw(t, "workers"),
		maxMessages:   rapid.Int32Range(1, 10).Draw(t, "maxMessages"),
		maxRetryCount: rapid.IntRange(0, 3).Draw(t, "maxRetryCount"),
	}
	if s.retryModel == router.VisibilityRetryModel {
		s.dlqMaxReceiveCount = rapid.IntRange(0, 3).Draw(t, "dlqMaxReceiveCount")
	}

	groups := rapid.IntRange(1, 4).Draw(t, "groups")
	outcomes := rapid.SampledFrom([]commitOutcome{outcomeSuccess, outcomeError, outcomeBackoff})
	s.batches = make([][]commitMessage, rapid.IntRange(1, 3).Draw(t, "batchCount"))
	next := 0
	for b := range s.batches {
		size := rapid.IntRange(0, int(s.maxMessages)).Draw(t, "batchSize")
		s.batches[b] = make([]commitMessage, size)
		for i := range s.batches[b] {
			id := strconv.Itoa(next)
			next++
			s.batches[b][i] = commitMessage{
				handle:             "receipt-" + id,
				body:               "body-" + id,
				groupID:            "group-" + strconv.Itoa(rapid.IntRange(0, groups-1).Draw(t, "group")),
				outcome:            outcomes.Draw(t, "outcome"),
				retryCount:         rapid.IntRange(0, 4).Draw(t, "retryCount"),
				receiveCount:       rapid.IntRange(1, 4).Draw(t, "receiveCount"),
				orchestrationFails: rapid.Bool().Draw(t, "orchestrationFails"),
			}
		}
	}
	return s
}

func (s commitScenario) messages() []commitMessage {
	var out []commitMessage
	for _, batch := range s.batches {
		out = append(out, batch...)
	}
	return out
}

func (s commitScenario) expectedCommitted() []string {
	var out []string
	for _, batch := range s.batches {
		failedGroups := make(map[string]bool)
		for _, m := range batch {
			if s.commits(m, failedGroups) {
				out = append(out, m.handle)
			}
		}
	}
	return out
}

func (s commitScenario) commits(m commitMessage, failedGroups map[string]bool) bool {
	if s.retryModel == router.ScheduledRetryModel {
		return m.outcome == outcomeSuccess || !m.orchestrationFails
	}
	ordered := s.runMode == router.PerGroupID
	if ordered && failedGroups[m.groupID] {
		return false
	}
	if m.outcome == outcomeSuccess {
		return true
	}
	if ordered {
		failedGroups[m.groupID] = true
	}
	return false
}

func (s commitScenario) routeOptions() []router.Option {
	opts := []router.Option{
		router.WithWorkerPoolSize(s.workers),
		router.WithMaxMessages(s.maxMessages),
		router.WithRunMode(s.runMode),
	}
	if s.retryModel == router.ScheduledRetryModel {
		opts = append(opts, router.WithScheduledRetry(
			router.WithSchedulerIdentity("arn:aws:sqs:us-east-1:123456789012:orders-queue", "arn:aws:iam::123456789012:role/scheduler"),
			router.WithScheduledDLQ("https://sqs.us-east-1.amazonaws.com/123456789012/orders-dlq"),
			router.WithMaxRetryCount(s.maxRetryCount),
			router.WithBackoff(time.Second, 2*time.Second),
		))
	}
	if s.dlqMaxReceiveCount > 0 {
		opts = append(opts, router.WithDLQ(s.dlqMaxReceiveCount))
	}
	return opts
}

func (s commitScenario) handler() middleware.Handler {
	outcomes := make(map[string]commitOutcome)
	for _, m := range s.messages() {
		outcomes[m.body] = m.outcome
	}
	return func(_ context.Context, m middleware.Message) error {
		switch outcomes[string(m.Body())] {
		case outcomeError:
			return errCommitHandler
		case outcomeBackoff:
			m.(consumer.Message).Backoff(commitBackoff)
			return nil
		default:
			return nil
		}
	}
}

func (s commitScenario) failingBodies() map[string]bool {
	out := make(map[string]bool)
	for _, m := range s.messages() {
		if m.orchestrationFails {
			out[m.body] = true
		}
	}
	return out
}

func (m commitMessage) sqsMessage() types.Message {
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

func newCommitSQSClient(s commitScenario, cancel context.CancelFunc) *fake.SQSClient {
	failing := s.failingBodies()
	var calls atomic.Int64
	return &fake.SQSClient{
		GetQueueUrlFunc: func(_ context.Context, _ *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
			return &sqs.GetQueueUrlOutput{QueueUrl: aws.String(commitQueueURL)}, nil
		},
		ReceiveMessageFunc: func(_ context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			n := int(calls.Add(1) - 1)
			if n >= len(s.batches) {
				cancel()
				return &sqs.ReceiveMessageOutput{}, nil
			}
			msgs := make([]types.Message, len(s.batches[n]))
			for i, m := range s.batches[n] {
				msgs[i] = m.sqsMessage()
			}
			return &sqs.ReceiveMessageOutput{Messages: msgs}, nil
		},
		SendMessageFunc: func(_ context.Context, params *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			if failing[aws.ToString(params.MessageBody)] {
				return nil, errCommitOrchestration
			}
			return &sqs.SendMessageOutput{}, nil
		},
	}
}

func newCommitScheduler(s commitScenario) *fake.SchedulerClient {
	failing := s.failingBodies()
	return &fake.SchedulerClient{
		CreateScheduleFunc: func(
			_ context.Context,
			params *scheduler.CreateScheduleInput,
			_ ...func(*scheduler.Options),
		) (*scheduler.CreateScheduleOutput, error) {
			var request struct {
				MessageBody string `json:"MessageBody"`
			}
			if err := json.Unmarshal([]byte(aws.ToString(params.Target.Input)), &request); err != nil {
				return nil, err
			}
			if failing[request.MessageBody] {
				return nil, errCommitOrchestration
			}
			return &scheduler.CreateScheduleOutput{}, nil
		},
	}
}

func runCommitScenario(
	t require.TestingT,
	s commitScenario,
	wrap func(*fake.SQSClient) consumer.SQSClient,
	routeOpts []router.Option,
	consumerOpts ...consumer.Option,
) commitRun {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	run := commitRun{
		sqs:       newCommitSQSClient(s, cancel),
		scheduler: newCommitScheduler(s),
	}

	route, err := router.New(commitQueueName, s.handler(), append(s.routeOptions(), routeOpts...)...)
	require.NoError(t, err)

	opts := append([]consumer.Option{consumer.WithSchedulerClient(run.scheduler)}, consumerOpts...)
	c, err := consumer.New(wrap(run.sqs), route, opts...)
	require.NoError(t, err)

	require.NoError(t, c.Run(ctx))
	return run
}

func deleteMessageHandles(calls []*sqs.DeleteMessageInput) []string {
	out := make([]string, 0, len(calls))
	for _, call := range calls {
		out = append(out, aws.ToString(call.ReceiptHandle))
	}
	return out
}

func deleteMessageQueueURLs(calls []*sqs.DeleteMessageInput) []string {
	out := make([]string, 0, len(calls))
	for _, call := range calls {
		out = append(out, aws.ToString(call.QueueUrl))
	}
	return out
}

func assertSynchronousCommits(t assert.TestingT, s commitScenario, run commitRun) {
	calls := run.sqs.DeleteMessageCalls()
	assert.Empty(t, run.sqs.DeleteMessageBatchCalls())
	assert.ElementsMatch(t, s.expectedCommitted(), deleteMessageHandles(calls))
	for _, url := range deleteMessageQueueURLs(calls) {
		assert.Equal(t, commitQueueURL, url)
	}
}

func TestSQSOnlyClient_IsNotBatchDeleteClient(t *testing.T) {
	var client consumer.SQSClient = standardClient(&fake.SQSClient{})

	_, ok := client.(consumer.BatchDeleteClient)

	assert.False(t, ok)
}

func TestRun_StandardRouteCommitsSynchronously(t *testing.T) {
	visibility := func(runMode router.Mode, batches ...[]commitMessage) commitScenario {
		return commitScenario{
			batches:     batches,
			retryModel:  router.VisibilityRetryModel,
			runMode:     runMode,
			workers:     2,
			maxMessages: 10,
		}
	}
	scheduled := func(maxRetryCount int, batches ...[]commitMessage) commitScenario {
		return commitScenario{
			batches:       batches,
			retryModel:    router.ScheduledRetryModel,
			runMode:       router.Parallel,
			workers:       2,
			maxMessages:   10,
			maxRetryCount: maxRetryCount,
		}
	}
	msg := func(id, groupID string, outcome commitOutcome) commitMessage {
		return commitMessage{handle: "receipt-" + id, body: "body-" + id, groupID: groupID, outcome: outcome, receiveCount: 1}
	}
	failing := func(m commitMessage) commitMessage {
		m.orchestrationFails = true
		return m
	}
	withRetryCount := func(m commitMessage, n int) commitMessage {
		m.retryCount = n
		return m
	}

	tests := []struct {
		name     string
		want     []string
		scenario commitScenario
	}{
		{
			name:     "empty batch deletes nothing",
			scenario: visibility(router.Parallel, []commitMessage{}),
		},
		{
			name: "visibility success deletes and failures stay",
			scenario: visibility(router.Parallel, []commitMessage{
				msg("1", "g", outcomeSuccess),
				msg("2", "g", outcomeError),
				msg("3", "g", outcomeBackoff),
			}),
			want: []string{"receipt-1"},
		},
		{
			name: "per group id holds the tail of a failed group",
			scenario: visibility(router.PerGroupID, []commitMessage{
				msg("1", "a", outcomeSuccess),
				msg("2", "a", outcomeError),
				msg("3", "a", outcomeSuccess),
				msg("4", "b", outcomeSuccess),
			}),
			want: []string{"receipt-1", "receipt-4"},
		},
		{
			name: "per group id barrier resets on the next batch",
			scenario: visibility(router.PerGroupID,
				[]commitMessage{msg("1", "a", outcomeBackoff), msg("2", "a", outcomeSuccess)},
				[]commitMessage{msg("3", "a", outcomeSuccess)},
			),
			want: []string{"receipt-3"},
		},
		{
			name: "scheduled commits success, retry, and dead letter",
			scenario: scheduled(1, []commitMessage{
				msg("1", "g", outcomeSuccess),
				msg("2", "g", outcomeError),
				withRetryCount(msg("3", "g", outcomeBackoff), 1),
			}),
			want: []string{"receipt-1", "receipt-2", "receipt-3"},
		},
		{
			name: "scheduled retains messages whose orchestration fails",
			scenario: scheduled(1, []commitMessage{
				failing(msg("1", "g", outcomeSuccess)),
				failing(msg("2", "g", outcomeError)),
				failing(withRetryCount(msg("3", "g", outcomeBackoff), 1)),
			}),
			want: []string{"receipt-1"},
		},
	}

	clients := []struct {
		wrap func(*fake.SQSClient) consumer.SQSClient
		name string
	}{
		{name: "sqs only client", wrap: standardClient},
		{name: "batch capable client", wrap: batchCapableClient},
	}

	for _, tt := range tests {
		for _, cl := range clients {
			t.Run(tt.name+"/"+cl.name, func(t *testing.T) {
				defer goleak.VerifyNone(t)

				run := runCommitScenario(t, tt.scenario, cl.wrap, nil)

				assert.ElementsMatch(t, tt.want, tt.scenario.expectedCommitted())
				assertSynchronousCommits(t, tt.scenario, run)
			})
		}
	}
}

// Feature: opportunistic-delete-batch, Property 3: For any batch of messages and handler outcomes on a Standard_Route, the consumer makes zero DeleteMessageBatch calls and exactly one DeleteMessage call per committed message, and works with a client that implements only consumer.SQSClient.
func TestRun_PropertyStandardRouteKeepsSynchronousPath(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		defer goleak.VerifyNone(rt)

		s := drawCommitScenario(rt)
		wrap := standardClient
		if rapid.Bool().Draw(rt, "batchCapableClient") {
			wrap = batchCapableClient
		}

		run := runCommitScenario(rt, s, wrap, nil)

		assertSynchronousCommits(rt, s, run)
		assert.Len(rt, run.sqs.GetQueueURLCalls(), 1)
	})
}

// Feature: opportunistic-delete-batch, Property 4: For any batch of messages with generated handler outcomes (success, error, backoff), retry model, and run mode, the set of receipt handles removed on a Delete_Batch_Route equals the set removed by DeleteMessage on an otherwise identical Standard_Route, and a Delete_Batch_Route makes zero DeleteMessage calls. In particular, errored, backed-off, and held messages are never removed under the Visibility_Retry_Model.
func TestRun_PropertyCommittedSetIdenticalOnBothPaths(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		defer goleak.VerifyNone(rt)

		s := drawCommitScenario(rt)

		batched := runCommitScenario(rt, s, batchCapableClient, []router.Option{router.WithDeleteBatch()})
		standard := runCommitScenario(rt, s, standardClient, nil)

		batchCalls := batched.sqs.DeleteMessageBatchCalls()
		batchRemoved := sentHandles(batchCalls)
		standardRemoved := deleteMessageHandles(standard.sqs.DeleteMessageCalls())

		assert.Empty(rt, batched.sqs.DeleteMessageCalls())
		assert.Empty(rt, standard.sqs.DeleteMessageBatchCalls())
		assert.ElementsMatch(rt, standardRemoved, batchRemoved)
		assert.ElementsMatch(rt, s.expectedCommitted(), batchRemoved)
		assert.ElementsMatch(rt, s.expectedCommitted(), standardRemoved)
		for _, call := range batchCalls {
			assert.Equal(rt, commitQueueURL, aws.ToString(call.QueueUrl))
		}
	})
}

const (
	metricSuccess    = "IncSuccess"
	metricRetry      = "IncRetry"
	metricDeadLetter = "IncDeadLetter"
	metricDLQ        = "IncDLQ"
)

type metricKey struct {
	method string
	route  string
}

type metricCounter struct {
	counts map[metricKey]int
	mu     sync.Mutex
}

func newMetricCounter() *metricCounter {
	return &metricCounter{counts: make(map[metricKey]int)}
}

func (c *metricCounter) inc(method string) func(string) {
	return func(route string) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.counts[metricKey{method: method, route: route}]++
	}
}

func (c *metricCounter) recorder() consumer.MetricsRecorder {
	return recorderFunc{
		dlq:        c.inc(metricDLQ),
		success:    c.inc(metricSuccess),
		retry:      c.inc(metricRetry),
		deadLetter: c.inc(metricDeadLetter),
	}
}

func (c *metricCounter) snapshot() map[metricKey]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return maps.Clone(c.counts)
}

func drawMetricsScenario(t *rapid.T) commitScenario {
	s := drawCommitScenario(t)
	if s.retryModel == router.VisibilityRetryModel {
		s.dlqMaxReceiveCount = rapid.IntRange(1, 4).Draw(t, "dlqSignalMaxReceiveCount")
	}
	return s
}

func (s commitScenario) expectedMetrics() map[metricKey]int {
	out := make(map[metricKey]int)
	for _, batch := range s.batches {
		failedGroups := make(map[string]bool)
		for _, m := range batch {
			if method := s.metricFor(m, failedGroups); method != "" {
				out[metricKey{method: method, route: commitQueueName}]++
			}
		}
	}
	return out
}

func (s commitScenario) metricFor(m commitMessage, failedGroups map[string]bool) string {
	if s.retryModel == router.ScheduledRetryModel {
		switch {
		case m.outcome == outcomeSuccess:
			return metricSuccess
		case m.orchestrationFails:
			return ""
		case m.retryCount+1 <= s.maxRetryCount:
			return metricRetry
		default:
			return metricDeadLetter
		}
	}
	ordered := s.runMode == router.PerGroupID
	if ordered && failedGroups[m.groupID] {
		return ""
	}
	if m.outcome == outcomeSuccess {
		return ""
	}
	if ordered {
		failedGroups[m.groupID] = true
	}
	if m.outcome == outcomeError && s.dlqMaxReceiveCount > 0 && m.receiveCount >= s.dlqMaxReceiveCount {
		return metricDLQ
	}
	return ""
}

// Feature: opportunistic-delete-batch, Property 8: For any batch with generated handler outcomes under either retry model, the counts of success, retry, dead-letter, and observe-only DLQ metrics on a Delete_Batch_Route equal those on an otherwise identical Standard_Route.
func TestRun_PropertyMetricsParityOnBothPaths(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		defer goleak.VerifyNone(rt)

		s := drawMetricsScenario(rt)
		batchedMetrics := newMetricCounter()
		standardMetrics := newMetricCounter()

		deleteBatch := []router.Option{router.WithDeleteBatch()}
		runCommitScenario(rt, s, batchCapableClient, deleteBatch, consumer.WithMetrics(batchedMetrics.recorder()))
		runCommitScenario(rt, s, standardClient, nil, consumer.WithMetrics(standardMetrics.recorder()))

		batched := batchedMetrics.snapshot()
		standard := standardMetrics.snapshot()

		assert.Equal(rt, standard, batched)
		assert.Equal(rt, s.expectedMetrics(), batched)
		assert.Equal(rt, s.expectedMetrics(), standard)
	})
}

type shutdownDelete struct {
	ctxErr      error
	handles     []string
	runCanceled bool
	afterReturn bool
}

type shutdownRecorder struct {
	calls    []shutdownDelete
	mu       sync.Mutex
	returned atomic.Bool
}

func (r *shutdownRecorder) deleteBatchFunc(
	runCtx context.Context,
) func(context.Context, *sqs.DeleteMessageBatchInput, ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
	return func(ctx context.Context, params *sqs.DeleteMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
		call := shutdownDelete{
			ctxErr:      ctx.Err(),
			handles:     sentHandles([]*sqs.DeleteMessageBatchInput{params}),
			runCanceled: runCtx.Err() != nil,
			afterReturn: r.returned.Load(),
		}
		r.mu.Lock()
		r.calls = append(r.calls, call)
		r.mu.Unlock()
		return &sqs.DeleteMessageBatchOutput{}, nil
	}
}

func (r *shutdownRecorder) snapshot() []shutdownDelete {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]shutdownDelete(nil), r.calls...)
}

func drawShutdownScenario(t *rapid.T) (commitScenario, map[string]bool) {
	s := drawCommitScenario(t)
	head := commitMessage{
		handle:             "receipt-head",
		body:               "body-head",
		groupID:            "group-0",
		outcome:            rapid.SampledFrom([]commitOutcome{outcomeSuccess, outcomeError, outcomeBackoff}).Draw(t, "headOutcome"),
		retryCount:         rapid.IntRange(0, 4).Draw(t, "headRetryCount"),
		receiveCount:       rapid.IntRange(1, 4).Draw(t, "headReceiveCount"),
		orchestrationFails: rapid.Bool().Draw(t, "headOrchestrationFails"),
	}
	msgs := append([]commitMessage{head}, s.messages()...)
	msgs = msgs[:min(len(msgs), int(s.maxMessages))]
	s.batches = [][]commitMessage{msgs}

	gated := map[string]bool{head.body: true}
	for _, m := range msgs[1:] {
		gated[m.body] = rapid.Bool().Draw(t, "gated")
	}
	return s, gated
}

func gateHandlers(gated map[string]bool, head string, headStarted chan<- struct{}, release <-chan struct{}) middleware.Middleware {
	return func(next middleware.Handler) middleware.Handler {
		return func(ctx context.Context, m middleware.Message) error {
			body := string(m.Body())
			if body == head {
				close(headStarted)
			}
			if gated[body] {
				<-release
			}
			return next(ctx, m)
		}
	}
}

func (s commitScenario) gatedCommitted(gated map[string]bool) []string {
	committed := make(map[string]bool)
	for _, h := range s.expectedCommitted() {
		committed[h] = true
	}
	var out []string
	for _, m := range s.messages() {
		if gated[m.body] && committed[m.handle] {
			out = append(out, m.handle)
		}
	}
	return out
}

// Feature: opportunistic-delete-batch, Property 6: For any set of handles enqueued before and after the run context is canceled (while workers are still finishing), when Consumer.Run returns every handle has been sent, every request was made with a context that was not canceled at call time, and no Sender goroutine remains.
func TestRun_PropertyShutdownFlushesAndLeaksNothing(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		s, gated := drawShutdownScenario(rt)
		batch := s.batches[0]

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		headStarted := make(chan struct{})
		release := make(chan struct{})
		shutdown := sync.OnceFunc(func() {
			cancel()
			close(release)
		})

		rec := &shutdownRecorder{}
		client := newCommitSQSClient(s, cancel)
		var receives atomic.Int64
		client.ReceiveMessageFunc = func(_ context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			if receives.Add(1) == 1 {
				msgs := make([]types.Message, len(batch))
				for i, m := range batch {
					msgs[i] = m.sqsMessage()
				}
				return &sqs.ReceiveMessageOutput{Messages: msgs}, nil
			}
			<-headStarted
			shutdown()
			return &sqs.ReceiveMessageOutput{}, nil
		}
		client.DeleteMessageBatchFunc = rec.deleteBatchFunc(ctx)

		routeOpts := append(s.routeOptions(),
			router.WithDeleteBatch(),
			router.WithMiddleware(gateHandlers(gated, batch[0].body, headStarted, release)),
		)
		route, err := router.New(commitQueueName, s.handler(), routeOpts...)
		require.NoError(rt, err)
		c, err := consumer.New(client, route, consumer.WithSchedulerClient(newCommitScheduler(s)))
		require.NoError(rt, err)

		require.NoError(rt, c.Run(ctx))
		rec.returned.Store(true)
		sentAtReturn := len(client.DeleteMessageBatchCalls())

		goleak.VerifyNone(rt)

		calls := rec.snapshot()
		assert.Len(rt, client.DeleteMessageBatchCalls(), sentAtReturn)
		assert.Len(rt, calls, sentAtReturn)
		assert.Empty(rt, client.DeleteMessageCalls())
		assert.ElementsMatch(rt, s.expectedCommitted(), sentHandles(client.DeleteMessageBatchCalls()))

		var sentAfterCancel []string
		for _, call := range calls {
			assert.NoError(rt, call.ctxErr)
			assert.False(rt, call.afterReturn)
			if call.runCanceled {
				sentAfterCancel = append(sentAfterCancel, call.handles...)
			}
		}
		assert.Subset(rt, sentAfterCancel, s.gatedCommitted(gated))
	})
}

func drawFailFastRouteOptions(t *rapid.T) []router.Option {
	s := commitScenario{
		retryModel:    rapid.SampledFrom([]router.RetryModel{router.VisibilityRetryModel, router.ScheduledRetryModel}).Draw(t, "retryModel"),
		runMode:       rapid.SampledFrom([]router.Mode{router.Parallel, router.PerGroupID}).Draw(t, "runMode"),
		workers:       rapid.IntRange(1, 8).Draw(t, "workers"),
		maxMessages:   rapid.Int32Range(1, 10).Draw(t, "maxMessages"),
		maxRetryCount: rapid.IntRange(0, 5).Draw(t, "maxRetryCount"),
	}
	if s.retryModel == router.VisibilityRetryModel {
		s.dlqMaxReceiveCount = rapid.IntRange(0, 5).Draw(t, "dlqMaxReceiveCount")
	}
	opts := append(s.routeOptions(),
		router.WithWaitTimeSeconds(rapid.Int32Range(0, 20).Draw(t, "waitTimeSeconds")),
		router.WithVisibilityTimeout(rapid.Int32Range(0, 120).Draw(t, "visibilityTimeout")),
		router.WithExtensionLimit(rapid.IntRange(0, 5).Draw(t, "extensionLimit")),
	)
	at := rapid.IntRange(0, len(opts)).Draw(t, "deleteBatchPosition")
	return slices.Insert(opts, at, router.WithDeleteBatch())
}

func runFailFast(
	ctx context.Context,
	t require.TestingT,
	routeOpts []router.Option,
	consumerOpts ...consumer.Option,
) (*fake.SQSClient, error) {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	client := newCommitSQSClient(commitScenario{}, cancel)
	route, err := router.New(commitQueueName, commitScenario{}.handler(), routeOpts...)
	require.NoError(t, err)
	require.True(t, route.DeleteBatch())

	c, err := consumer.New(standardClient(client), route, consumerOpts...)
	require.NoError(t, err)

	return client, c.Run(runCtx)
}

func assertNoSQSCalls(t assert.TestingT, client *fake.SQSClient) {
	assert.Empty(t, client.GetQueueURLCalls())
	assert.Empty(t, client.ReceiveMessageCalls())
	assert.Empty(t, client.DeleteMessageCalls())
	assert.Empty(t, client.DeleteMessageBatchCalls())
	assert.Empty(t, client.ChangeMessageVisibilityCalls())
	assert.Empty(t, client.SendMessageCalls())
}

func TestRun_DeleteBatchUnsupportedFailsFast(t *testing.T) {
	scheduled := commitScenario{retryModel: router.ScheduledRetryModel, runMode: router.Parallel, workers: 2, maxMessages: 10}.routeOptions()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		ctx          context.Context
		wantErr      error
		name         string
		routeOpts    []router.Option
		withSchedule bool
	}{
		{
			name:      "visibility parallel route",
			ctx:       context.Background(),
			routeOpts: []router.Option{router.WithDeleteBatch()},
			wantErr:   verrors.ErrDeleteBatchUnsupported,
		},
		{
			name:      "visibility per group id route with dlq",
			ctx:       context.Background(),
			routeOpts: []router.Option{router.WithRunMode(router.PerGroupID), router.WithDLQ(3), router.WithDeleteBatch()},
			wantErr:   verrors.ErrDeleteBatchUnsupported,
		},
		{
			name:         "scheduled route with scheduler client",
			ctx:          context.Background(),
			routeOpts:    append(slices.Clone(scheduled), router.WithDeleteBatch()),
			withSchedule: true,
			wantErr:      verrors.ErrDeleteBatchUnsupported,
		},
		{
			name:      "scheduled route without scheduler client reports the scheduler first",
			ctx:       context.Background(),
			routeOpts: append([]router.Option{router.WithDeleteBatch()}, scheduled...),
			wantErr:   verrors.ErrNoSchedulerClient,
		},
		{
			name:      "already canceled context",
			ctx:       canceled,
			routeOpts: []router.Option{router.WithDeleteBatch()},
			wantErr:   verrors.ErrDeleteBatchUnsupported,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer goleak.VerifyNone(t)

			sched := &fake.SchedulerClient{}
			var opts []consumer.Option
			if tt.withSchedule {
				opts = append(opts, consumer.WithSchedulerClient(sched))
			}

			client, err := runFailFast(tt.ctx, t, tt.routeOpts, opts...)

			assert.ErrorIs(t, err, tt.wantErr)
			assertNoSQSCalls(t, client)
			assert.Empty(t, sched.CreateScheduleCalls())
		})
	}
}

// Feature: opportunistic-delete-batch, Property 7: For any Delete_Batch_Route and any client that implements consumer.SQSClient but not consumer.BatchDeleteClient, Consumer.Run returns an error matching errors.ErrDeleteBatchUnsupported and the client records zero GetQueueUrl and zero ReceiveMessage calls.
func TestRun_PropertyUnsupportedClientFailsFast(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		defer goleak.VerifyNone(rt)

		sched := &fake.SchedulerClient{}
		client, err := runFailFast(context.Background(), rt, drawFailFastRouteOptions(rt), consumer.WithSchedulerClient(sched))

		assert.ErrorIs(rt, err, verrors.ErrDeleteBatchUnsupported)
		assertNoSQSCalls(rt, client)
		assert.Empty(rt, sched.CreateScheduleCalls())
	})
}

func failAllEntries(params *sqs.DeleteMessageBatchInput) *sqs.DeleteMessageBatchOutput {
	out := &sqs.DeleteMessageBatchOutput{}
	for _, entry := range params.Entries {
		out.Failed = append(out.Failed, types.BatchResultErrorEntry{
			Id:          entry.Id,
			Code:        aws.String("ReceiptHandleIsInvalid"),
			SenderFault: true,
		})
	}
	return out
}

func drawFailedDeleteScenario(t *rapid.T) commitScenario {
	s := drawCommitScenario(t)
	s.retryModel = router.VisibilityRetryModel
	for b := range s.batches {
		for i := range s.batches[b] {
			if s.batches[b][i].outcome == outcomeBackoff {
				s.batches[b][i].outcome = outcomeSuccess
			}
		}
	}
	return s
}

func deleteFailureHandles(t assert.TestingT, records []fake.LogRecord) []string {
	var out []string
	for _, rec := range records {
		if rec.Message != "failed to delete message" {
			continue
		}
		assert.Equal(t, slog.LevelError, rec.Level)
		handle, _ := rec.Attrs["receipt_handle"].(string)
		out = append(out, handle)
	}
	return out
}

func TestRun_DeleteBatchFailureLeavesMessagesInQueue(t *testing.T) {
	tests := []struct {
		respond func(*sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error)
		name    string
	}{
		{
			name: "request error",
			respond: func(*sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error) {
				return nil, errCommitOrchestration
			},
		},
		{
			name: "every entry failed",
			respond: func(params *sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error) {
				return failAllEntries(params), nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rapid.Check(t, func(rt *rapid.T) {
				defer goleak.VerifyNone(rt)

				s := drawFailedDeleteScenario(rt)
				handler := fake.NewLogHandler()
				failing := func(c *fake.SQSClient) consumer.SQSClient {
					c.DeleteMessageBatchFunc = func(
						_ context.Context, params *sqs.DeleteMessageBatchInput, _ ...func(*sqs.Options),
					) (*sqs.DeleteMessageBatchOutput, error) {
						return tt.respond(params)
					}
					return c
				}

				run := runCommitScenario(rt, s, failing, []router.Option{router.WithDeleteBatch()}, consumer.WithLogger(slog.New(handler)))

				want := s.expectedCommitted()
				assert.Empty(rt, run.sqs.DeleteMessageCalls())
				assert.Empty(rt, run.sqs.ChangeMessageVisibilityCalls())
				assert.ElementsMatch(rt, want, sentHandles(run.sqs.DeleteMessageBatchCalls()))
				assert.ElementsMatch(rt, want, deleteFailureHandles(rt, handler.Records()))
			})
		})
	}
}

func groupMessages(first, count int, groupID string) []types.Message {
	msgs := make([]types.Message, count)
	for i := range msgs {
		id := strconv.Itoa(first + i)
		msgs[i] = types.Message{
			ReceiptHandle: aws.String("receipt-" + id),
			Body:          aws.String("receipt-" + id),
			Attributes:    map[string]string{"MessageGroupId": groupID},
		}
	}
	return msgs
}

func receiptHandles(batches [][]types.Message) []string {
	var out []string
	for _, batch := range batches {
		for _, m := range batch {
			out = append(out, aws.ToString(m.ReceiptHandle))
		}
	}
	return out
}

func scriptedReceive(
	batches [][]types.Message,
) func(context.Context, *sqs.ReceiveMessageInput, ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	var calls atomic.Int64
	return func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
		n := int(calls.Add(1) - 1)
		if n < len(batches) {
			return &sqs.ReceiveMessageOutput{Messages: batches[n]}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
}

type handledRecorder struct {
	handles []string
	mu      sync.Mutex
}

func (r *handledRecorder) handler(_ context.Context, m middleware.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handles = append(r.handles, string(m.Body()))
	return nil
}

func (r *handledRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.handles)
}

type batchRun struct {
	client  *fake.SQSClient
	handled *handledRecorder
	done    chan error
	cancel  context.CancelFunc
}

func startBatchRun(
	t *testing.T,
	batches [][]types.Message,
	deleteBatch func(context.Context, *sqs.DeleteMessageBatchInput, ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error),
	routeOpts []router.Option,
	consumerOpts ...consumer.Option,
) batchRun {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	run := batchRun{
		client: &fake.SQSClient{
			GetQueueUrlFunc: func(context.Context, *sqs.GetQueueUrlInput, ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
				return &sqs.GetQueueUrlOutput{QueueUrl: aws.String(commitQueueURL)}, nil
			},
			ReceiveMessageFunc:     scriptedReceive(batches),
			DeleteMessageBatchFunc: deleteBatch,
		},
		handled: &handledRecorder{},
		done:    make(chan error, 1),
		cancel:  cancel,
	}
	route, err := router.New(commitQueueName, run.handled.handler, append(routeOpts, router.WithDeleteBatch())...)
	require.NoError(t, err)
	c, err := consumer.New(run.client, route, consumerOpts...)
	require.NoError(t, err)
	go func() { run.done <- c.Run(ctx) }()
	return run
}

func TestRun_DeleteBatcherSizedByWorkersAndMaxMessages(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		name        string
		workers     int
		maxMessages int32
	}{
		{name: "one worker one message", workers: 1, maxMessages: 1},
		{name: "one worker three messages", workers: 1, maxMessages: 3},
		{name: "one worker ten messages", workers: 1, maxMessages: 10},
		{name: "two workers one message", workers: 2, maxMessages: 1},
		{name: "two workers three messages", workers: 2, maxMessages: 3},
		{name: "four workers ten messages", workers: 4, maxMessages: 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				maxMessages := int(tt.maxMessages)
				capacity := tt.workers * maxMessages
				needed := tt.workers*consumer.MaxDeleteBatch + capacity + 2*maxMessages + 2
				batches := make([][]types.Message, (needed+maxMessages-1)/maxMessages)
				for i := range batches {
					batches[i] = groupMessages(i*maxMessages, maxMessages, "single-group")
				}
				release := make(chan struct{})
				var inFlight atomic.Int64
				run := startBatchRun(t, batches, gatedDeleteBatch(release, &inFlight), []router.Option{
					router.WithWorkerPoolSize(tt.workers),
					router.WithMaxMessages(tt.maxMessages),
					router.WithRunMode(router.PerGroupID),
				})
				defer run.cancel()

				synctest.Wait()

				calls := run.client.DeleteMessageBatchCalls()
				assert.Equal(t, int64(tt.workers), inFlight.Load())
				assert.Len(t, calls, tt.workers)
				assert.Len(t, run.handled.snapshot(), len(sentHandles(calls))+capacity+1)
				assert.Empty(t, run.done)

				close(release)
				synctest.Wait()
				run.cancel()

				require.NoError(t, <-run.done)
				all := receiptHandles(batches)
				assert.ElementsMatch(t, all, run.handled.snapshot())
				assert.ElementsMatch(t, all, sentHandles(run.client.DeleteMessageBatchCalls()))
				assert.Empty(t, run.client.DeleteMessageCalls())
			})
		})
	}
}

func handleCounts(handles []string) map[string]int {
	out := make(map[string]int, len(handles))
	for _, h := range handles {
		out[h]++
	}
	return out
}

func shutdownCases() []struct {
	name     string
	workers  int
	messages int
} {
	return []struct {
		name     string
		workers  int
		messages int
	}{
		{name: "one worker one message", workers: 1, messages: 1},
		{name: "one worker full batch", workers: 1, messages: 10},
		{name: "two workers partial batch", workers: 2, messages: 7},
		{name: "four workers full batch", workers: 4, messages: 10},
	}
}

func TestRun_DeleteBatchShutdownWaitsForBlockedSenders(t *testing.T) {
	defer goleak.VerifyNone(t)

	for _, tt := range shutdownCases() {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				batches := [][]types.Message{groupMessages(0, tt.messages, "group")}
				release := make(chan struct{})
				var inFlight atomic.Int64
				run := startBatchRun(t, batches, gatedDeleteBatch(release, &inFlight), []router.Option{
					router.WithWorkerPoolSize(tt.workers),
					router.WithMaxMessages(consumer.MaxDeleteBatch),
				})
				defer run.cancel()

				synctest.Wait()
				require.Len(t, run.handled.snapshot(), tt.messages)
				run.cancel()
				synctest.Wait()

				assert.Empty(t, run.done)
				assert.Positive(t, inFlight.Load())
				assert.LessOrEqual(t, inFlight.Load(), int64(tt.workers))

				close(release)

				require.NoError(t, <-run.done)
				want := handleCounts(receiptHandles(batches))
				assert.Equal(t, want, handleCounts(sentHandles(run.client.DeleteMessageBatchCalls())))
				assert.Zero(t, inFlight.Load())
			})
		})
	}
}

func TestRun_DeleteBatchShutdownBoundsStalledDeletes(t *testing.T) {
	defer goleak.VerifyNone(t)

	stalled := func(ctx context.Context, _ *sqs.DeleteMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	for _, tt := range shutdownCases() {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				batches := [][]types.Message{groupMessages(0, tt.messages, "group")}
				logs := fake.NewLogHandler()
				run := startBatchRun(t, batches, stalled, []router.Option{
					router.WithWorkerPoolSize(tt.workers),
					router.WithMaxMessages(consumer.MaxDeleteBatch),
				}, consumer.WithLogger(slog.New(logs)))
				defer run.cancel()

				synctest.Wait()
				require.Len(t, run.handled.snapshot(), tt.messages)
				canceledAt := time.Now()
				run.cancel()

				require.NoError(t, <-run.done)
				elapsed := time.Since(canceledAt)
				rounds := (tt.messages + tt.workers - 1) / tt.workers
				assert.GreaterOrEqual(t, elapsed, consumer.DeleteRequestTimeout)
				assert.LessOrEqual(t, elapsed, time.Duration(rounds)*consumer.DeleteRequestTimeout)
				assert.Zero(t, elapsed%consumer.DeleteRequestTimeout)

				var logged []string
				for _, rec := range logs.Records() {
					assert.Equal(t, "failed to delete message", rec.Message)
					assert.Equal(t, slog.LevelError, rec.Level)
					err, _ := rec.Attrs["error"].(error)
					assert.ErrorIs(t, err, context.DeadlineExceeded)
					handle, _ := rec.Attrs["receipt_handle"].(string)
					logged = append(logged, handle)
				}
				want := receiptHandles(batches)
				assert.ElementsMatch(t, want, logged)
				assert.ElementsMatch(t, want, sentHandles(run.client.DeleteMessageBatchCalls()))
				assert.Empty(t, run.client.DeleteMessageCalls())
			})
		})
	}
}
