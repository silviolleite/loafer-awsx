//go:build integration

package consumer_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/silviolleite/loafer-awsx/conn"
	"github.com/silviolleite/loafer-awsx/consumer"
	"github.com/silviolleite/loafer-awsx/fake"
	"github.com/silviolleite/loafer-awsx/middleware"
	"github.com/silviolleite/loafer-awsx/router"
)

const (
	testRegion    = "us-east-1"
	testAccessKey = "test"
	testSecretKey = "test"
)

func endpoint() string {
	if v := os.Getenv("MINISTACK_ENDPOINT"); v != "" {
		return v
	}
	return "http://localhost:4566"
}

func newIntegrationConfig(t *testing.T) aws.Config {
	t.Helper()

	cfg, err := conn.New(context.Background(),
		conn.WithRegion(testRegion),
		conn.WithAccessKey(testAccessKey, testSecretKey),
		conn.WithEndpoint(endpoint()),
		conn.WithRetryCount(1),
	)
	require.NoError(t, err)

	transport := awshttp.NewBuildableClient().GetTransport()
	t.Cleanup(transport.CloseIdleConnections)
	cfg.HTTPClient = &http.Client{Transport: transport}

	return cfg
}

func newSQSClient(t *testing.T) *sqs.Client {
	t.Helper()
	return sqs.NewFromConfig(newIntegrationConfig(t))
}

func newSchedulerClient(t *testing.T) *scheduler.Client {
	t.Helper()
	return scheduler.NewFromConfig(newIntegrationConfig(t))
}

func createStandardQueue(t *testing.T, client *sqs.Client) string {
	t.Helper()
	return createQueue(t, client, uniqueName("standard"), nil)
}

func createFIFOQueue(t *testing.T, client *sqs.Client) string {
	t.Helper()
	return createQueue(t, client, uniqueName("group")+".fifo", map[string]string{
		"FifoQueue":                 "true",
		"ContentBasedDeduplication": "true",
	})
}

func createQueue(t *testing.T, client *sqs.Client, name string, attrs map[string]string) string {
	t.Helper()

	out, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{
		QueueName:  aws.String(name),
		Attributes: attrs,
	})
	require.NoError(t, err)
	require.NotNil(t, out.QueueUrl)

	url := *out.QueueUrl
	t.Cleanup(func() {
		_, _ = client.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: aws.String(url)})
	})

	return url
}

func queueNameFromURL(t *testing.T, client *sqs.Client, url string) string {
	t.Helper()

	out, err := client.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(url),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)

	arn := out.Attributes[string(types.QueueAttributeNameQueueArn)]
	require.NotEmpty(t, arn)

	for i := len(arn) - 1; i >= 0; i-- {
		if arn[i] == ':' {
			return arn[i+1:]
		}
	}
	return arn
}

var uniqueNameSeq atomic.Uint64

func uniqueName(prefix string) string {
	return fmt.Sprintf("loafer-it-%s-%d-%d", prefix, time.Now().UnixNano(), uniqueNameSeq.Add(1))
}

func sendMessage(t *testing.T, client *sqs.Client, url string, body any, groupID string) {
	t.Helper()

	raw, err := json.Marshal(body)
	require.NoError(t, err)

	in := &sqs.SendMessageInput{
		QueueUrl:    aws.String(url),
		MessageBody: aws.String(string(raw)),
	}
	if groupID != "" {
		in.MessageGroupId = aws.String(groupID)
	}

	_, err = client.SendMessage(context.Background(), in)
	require.NoError(t, err)
}

func waitQueueDrained(t *testing.T, client *sqs.Client, url string) {
	t.Helper()

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		out, err := client.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
			QueueUrl: aws.String(url),
			AttributeNames: []types.QueueAttributeName{
				types.QueueAttributeNameApproximateNumberOfMessages,
				types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
			},
		})
		require.NoError(c, err)
		assert.Equal(c, "0", out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)])
		assert.Equal(c, "0", out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)])
	}, 30*time.Second, 250*time.Millisecond)
}

func startConsumer(t *testing.T, c *consumer.Consumer) (context.CancelFunc, <-chan error) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- c.Run(ctx)
		close(done)
	}()

	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Error("consumer Run did not return during cleanup")
		}
	})

	return cancel, done
}

func runConsumer(t *testing.T, client consumer.SQSClient, route *router.Route) (context.CancelFunc, <-chan error) {
	t.Helper()

	c, err := consumer.New(client, route)
	require.NoError(t, err)

	return startConsumer(t, c)
}

func stopConsumer(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("consumer Run did not return after cancellation")
	}
}

func itNewRoute(t *testing.T, name string, handler middleware.Handler, opts ...router.Option) *router.Route {
	t.Helper()
	route, err := router.New(name, handler, opts...)
	require.NoError(t, err)
	return route
}

func TestIntegrationReceiveProcessDelete(t *testing.T) {
	client := newSQSClient(t)
	url := createStandardQueue(t, client)
	name := queueNameFromURL(t, client, url)

	var processed atomic.Int32
	handler := func(_ context.Context, msg middleware.Message) error {
		var payload map[string]string
		if err := msg.Decode(&payload); err != nil {
			return err
		}
		if payload["event"] == "created" {
			processed.Add(1)
		}
		return nil
	}

	sendMessage(t, client, url, map[string]string{"event": "created"}, "")

	route := itNewRoute(t, name, handler,
		router.WithWaitTimeSeconds(1),
		router.WithVisibilityTimeout(11),
	)
	cancel, done := runConsumer(t, client, route)

	require.Eventually(t, func() bool { return processed.Load() == 1 }, 30*time.Second, 200*time.Millisecond)
	waitQueueDrained(t, client, url)

	stopConsumer(t, cancel, done)
	assert.Equal(t, int32(1), processed.Load())
}

func TestIntegrationReceiveErrorRedelivery(t *testing.T) {
	client := newSQSClient(t)
	url := createStandardQueue(t, client)
	name := queueNameFromURL(t, client, url)

	var calls atomic.Int32
	handler := func(_ context.Context, _ middleware.Message) error {
		calls.Add(1)
		return fmt.Errorf("handler failed")
	}

	sendMessage(t, client, url, map[string]string{"event": "retry"}, "")

	route := itNewRoute(t, name, handler,
		router.WithWaitTimeSeconds(1),
		router.WithVisibilityTimeout(11),
		router.WithExtensionLimit(0),
	)
	cancel, done := runConsumer(t, client, route)

	require.Eventually(t, func() bool { return calls.Load() >= 2 }, 45*time.Second, 250*time.Millisecond)

	stopConsumer(t, cancel, done)
	assert.GreaterOrEqual(t, calls.Load(), int32(2))
}

func TestIntegrationReceiveBackoffVisibilityChange(t *testing.T) {
	client := newSQSClient(t)
	url := createStandardQueue(t, client)
	name := queueNameFromURL(t, client, url)

	var calls atomic.Int32
	handler := func(_ context.Context, msg middleware.Message) error {
		n := calls.Add(1)
		if n == 1 {
			backoffer, ok := msg.(consumer.Message)
			require.True(t, ok)
			backoffer.Backoff(2 * time.Second)
			return fmt.Errorf("backing off")
		}
		return nil
	}

	sendMessage(t, client, url, map[string]string{"event": "backoff"}, "")

	route := itNewRoute(t, name, handler,
		router.WithWaitTimeSeconds(1),
		router.WithVisibilityTimeout(30),
	)
	cancel, done := runConsumer(t, client, route)

	start := time.Now()
	require.Eventually(t, func() bool { return calls.Load() >= 2 }, 20*time.Second, 200*time.Millisecond)
	assert.Less(t, time.Since(start), 20*time.Second)
	waitQueueDrained(t, client, url)

	stopConsumer(t, cancel, done)
}

func TestIntegrationVisibilityTimeoutExtension(t *testing.T) {
	client := newSQSClient(t)
	url := createStandardQueue(t, client)
	name := queueNameFromURL(t, client, url)

	var calls atomic.Int32
	finished := make(chan struct{}, 1)
	handler := func(_ context.Context, _ middleware.Message) error {
		if calls.Add(1) == 1 {
			time.Sleep(14 * time.Second)
			finished <- struct{}{}
		}
		return nil
	}

	sendMessage(t, client, url, map[string]string{"event": "slow"}, "")

	route := itNewRoute(t, name, handler,
		router.WithWaitTimeSeconds(1),
		router.WithVisibilityTimeout(11),
		router.WithExtensionLimit(10),
	)
	cancel, done := runConsumer(t, client, route)

	select {
	case <-finished:
	case <-time.After(30 * time.Second):
		t.Fatal("slow handler never completed")
	}

	time.Sleep(2 * time.Second)
	assert.Equal(t, int32(1), calls.Load())
	waitQueueDrained(t, client, url)

	stopConsumer(t, cancel, done)
}

func TestIntegrationPerGroupIDRoutingConsistency(t *testing.T) {
	client := newSQSClient(t)
	url := createFIFOQueue(t, client)
	name := queueNameFromURL(t, client, url)

	groups := []string{"group-a", "group-b", "group-c"}
	perGroup := 5

	var mu sync.Mutex
	observed := map[string][]int{}
	var total atomic.Int32

	handler := func(_ context.Context, msg middleware.Message) error {
		var payload struct {
			Group string `json:"group"`
			Seq   int    `json:"seq"`
		}
		if err := msg.Decode(&payload); err != nil {
			return err
		}
		mu.Lock()
		observed[payload.Group] = append(observed[payload.Group], payload.Seq)
		mu.Unlock()
		total.Add(1)
		return nil
	}

	for _, g := range groups {
		for seq := 0; seq < perGroup; seq++ {
			sendMessage(t, client, url, map[string]any{"group": g, "seq": seq}, g)
		}
	}

	route := itNewRoute(t, name, handler,
		router.WithWaitTimeSeconds(1),
		router.WithVisibilityTimeout(30),
		router.WithWorkerPoolSize(5),
		router.WithMaxMessages(10),
		router.WithRunMode(router.PerGroupID),
	)
	cancel, done := runConsumer(t, client, route)

	require.Eventually(t, func() bool {
		return total.Load() == int32(len(groups)*perGroup)
	}, 45*time.Second, 250*time.Millisecond)

	stopConsumer(t, cancel, done)

	mu.Lock()
	defer mu.Unlock()
	for _, g := range groups {
		seqs := observed[g]
		require.Len(t, seqs, perGroup)
		for i := range seqs {
			assert.Equal(t, i, seqs[i])
		}
	}
}

func runScheduledConsumer(
	t *testing.T,
	client consumer.SQSClient,
	route *router.Route,
	sched consumer.SchedulerClient,
) (context.CancelFunc, <-chan error) {
	t.Helper()

	c, err := consumer.New(client, route, consumer.WithSchedulerClient(sched))
	require.NoError(t, err)

	return startConsumer(t, c)
}

func schedulerIdentity(t *testing.T, client *sqs.Client, url string) (string, string) {
	t.Helper()

	out, err := client.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(url),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)

	arn := out.Attributes[string(types.QueueAttributeNameQueueArn)]
	require.NotEmpty(t, arn)

	return arn, "arn:aws:iam::000000000000:role/loafer-scheduler"
}

func TestIntegrationScheduledRetryCreatesScheduleAndDeletesOriginal(t *testing.T) {
	client := newSQSClient(t)
	entryURL := createFIFOQueue(t, client)
	dlqURL := createFIFOQueue(t, client)
	name := queueNameFromURL(t, client, entryURL)
	targetARN, roleARN := schedulerIdentity(t, client, entryURL)

	sched := &fake.SchedulerClient{}

	handler := func(_ context.Context, _ middleware.Message) error {
		return fmt.Errorf("handler failed")
	}

	sendMessage(t, client, entryURL, map[string]string{"event": "scheduled"}, "group-scheduled")

	route := itNewRoute(t, name, handler,
		router.WithWaitTimeSeconds(1),
		router.WithVisibilityTimeout(11),
		router.WithScheduledRetry(
			router.WithSchedulerIdentity(targetARN, roleARN),
			router.WithScheduledDLQ(dlqURL),
			router.WithMaxRetryCount(3),
			router.WithBackoff(1*time.Second, 10*time.Second),
		),
	)
	cancel, done := runScheduledConsumer(t, client, route, sched)

	require.Eventually(t, func() bool {
		return len(sched.CreateScheduleCalls()) >= 1
	}, 45*time.Second, 250*time.Millisecond)
	waitQueueDrained(t, client, entryURL)

	stopConsumer(t, cancel, done)
	assert.GreaterOrEqual(t, len(sched.CreateScheduleCalls()), 1)
}

func TestIntegrationScheduledExhaustedGoesToDLQ(t *testing.T) {
	const body = `{"event":"exhausted"}`

	cases := []struct {
		attributes map[string]types.MessageAttributeValue
		name       string
		wantRetry  string
		maxRetry   int
	}{
		{
			name:      "max retry count zero",
			wantRetry: "1",
		},
		{
			name: "retry count at max",
			attributes: map[string]types.MessageAttributeValue{
				"retry_count": {DataType: aws.String("Number"), StringValue: aws.String("3")},
			},
			wantRetry: "4",
			maxRetry:  3,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := newSQSClient(t)
			rec := recordingScheduler(t, newSchedulerClient(t))
			entryURL := createFIFOQueue(t, client)
			dlqURL := createFIFOQueue(t, client)
			name := queueNameFromURL(t, client, entryURL)
			targetARN, roleARN := schedulerIdentity(t, client, entryURL)

			handler := func(_ context.Context, _ middleware.Message) error {
				return fmt.Errorf("handler failed")
			}

			_, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{
				QueueUrl:          aws.String(entryURL),
				MessageBody:       aws.String(body),
				MessageGroupId:    aws.String("group-exhausted"),
				MessageAttributes: tc.attributes,
			})
			require.NoError(t, err)

			route := itNewRoute(t, name, handler,
				router.WithWaitTimeSeconds(1),
				router.WithVisibilityTimeout(11),
				router.WithScheduledRetry(
					router.WithSchedulerIdentity(targetARN, roleARN),
					router.WithScheduledDLQ(dlqURL),
					router.WithMaxRetryCount(tc.maxRetry),
					router.WithBackoff(time.Hour, 2*time.Hour),
				),
			)
			cancel, done := runScheduledConsumer(t, client, route, rec)

			var received []types.Message
			require.Eventually(t, func() bool {
				out, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
					QueueUrl:              aws.String(dlqURL),
					MaxNumberOfMessages:   1,
					WaitTimeSeconds:       1,
					MessageAttributeNames: []string{"All"},
				})
				if err != nil {
					return false
				}
				received = append(received, out.Messages...)
				return len(received) >= 1
			}, 45*time.Second, 250*time.Millisecond)
			waitQueueDrained(t, client, entryURL)

			stopConsumer(t, cancel, done)
			assert.Empty(t, rec.CreateScheduleCalls())
			require.Len(t, received, 1)
			assert.Equal(t, body, aws.ToString(received[0].Body))
			retry := received[0].MessageAttributes["retry_count"]
			assert.Equal(t, "Number", aws.ToString(retry.DataType))
			assert.Equal(t, tc.wantRetry, aws.ToString(retry.StringValue))
		})
	}
}

type countingSQSClient struct {
	*sqs.Client
	deletes      atomic.Int64
	batches      atomic.Int64
	batchEntries atomic.Int64
}

func (c *countingSQSClient) DeleteMessage(
	ctx context.Context,
	params *sqs.DeleteMessageInput,
	optFns ...func(*sqs.Options),
) (*sqs.DeleteMessageOutput, error) {
	c.deletes.Add(1)
	return c.Client.DeleteMessage(ctx, params, optFns...)
}

func (c *countingSQSClient) DeleteMessageBatch(
	ctx context.Context,
	params *sqs.DeleteMessageBatchInput,
	optFns ...func(*sqs.Options),
) (*sqs.DeleteMessageBatchOutput, error) {
	c.batches.Add(1)
	c.batchEntries.Add(int64(len(params.Entries)))
	return c.Client.DeleteMessageBatch(ctx, params, optFns...)
}

func TestIntegrationDeleteBatchRemovesMessages(t *testing.T) {
	const (
		total  = 25
		groups = 5
	)

	tests := []struct {
		create    func(*testing.T, *sqs.Client) string
		group     func(int) string
		routeOpts []router.Option
		name      string
		fifo      bool
	}{
		{
			name:   "standard queue",
			create: createStandardQueue,
			group:  func(int) string { return "" },
		},
		{
			name:      "fifo queue",
			create:    createFIFOQueue,
			group:     func(seq int) string { return fmt.Sprintf("group-%d", seq%groups) },
			routeOpts: []router.Option{router.WithRunMode(router.PerGroupID)},
			fifo:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newSQSClient(t)
			url := tt.create(t, client)
			name := queueNameFromURL(t, client, url)

			var mu sync.Mutex
			seen := map[int]int{}
			order := map[string][]int{}
			handler := func(_ context.Context, msg middleware.Message) error {
				var payload struct {
					Seq int `json:"seq"`
				}
				if err := msg.Decode(&payload); err != nil {
					return err
				}
				mu.Lock()
				seen[payload.Seq]++
				group := tt.group(payload.Seq)
				order[group] = append(order[group], payload.Seq)
				mu.Unlock()
				return nil
			}

			for seq := range total {
				sendMessage(t, client, url, map[string]int{"seq": seq}, tt.group(seq))
			}

			opts := append([]router.Option{
				router.WithWaitTimeSeconds(1),
				router.WithVisibilityTimeout(30),
				router.WithWorkerPoolSize(5),
				router.WithMaxMessages(10),
				router.WithDeleteBatch(),
			}, tt.routeOpts...)
			route := itNewRoute(t, name, handler, opts...)
			counting := &countingSQSClient{Client: client}
			cancel, done := runConsumer(t, counting, route)

			require.Eventually(t, func() bool {
				mu.Lock()
				defer mu.Unlock()
				return len(seen) == total
			}, 45*time.Second, 250*time.Millisecond)
			waitQueueDrained(t, client, url)

			stopConsumer(t, cancel, done)

			assert.Zero(t, counting.deletes.Load())
			assert.GreaterOrEqual(t, counting.batches.Load(), int64(1))

			mu.Lock()
			defer mu.Unlock()
			require.Len(t, seen, total)
			if !tt.fifo {
				assert.GreaterOrEqual(t, counting.batchEntries.Load(), int64(total))
				for seq := range total {
					assert.GreaterOrEqual(t, seen[seq], 1, "seq %d", seq)
				}
				return
			}
			assert.Equal(t, int64(total), counting.batchEntries.Load())
			for seq := range total {
				assert.Equal(t, 1, seen[seq], "seq %d", seq)
			}
			require.Len(t, order, groups)
			for group, seqs := range order {
				assert.IsIncreasing(t, seqs, "group %s", group)
			}
		})
	}
}

type scheduledAttribute struct {
	DataType    string `json:"DataType"`
	StringValue string `json:"StringValue"`
}

type scheduledSendMessage struct {
	MessageAttributes      map[string]scheduledAttribute `json:"MessageAttributes"`
	QueueURL               string                        `json:"QueueUrl"`
	MessageBody            string                        `json:"MessageBody"`
	MessageGroupID         string                        `json:"MessageGroupId"`
	MessageDeduplicationID string                        `json:"MessageDeduplicationId"`
}

func recordingScheduler(t *testing.T, api *scheduler.Client) *fake.SchedulerClient {
	t.Helper()

	rec := &fake.SchedulerClient{CreateScheduleFunc: api.CreateSchedule}
	t.Cleanup(func() {
		var notFound *schedulertypes.ResourceNotFoundException
		for _, call := range rec.CreateScheduleCalls() {
			_, err := api.DeleteSchedule(context.Background(), &scheduler.DeleteScheduleInput{Name: call.Name})
			if errors.As(err, &notFound) {
				continue
			}
			assert.NoError(t, err)
		}
	})

	return rec
}

func scheduledStringAttributes(n int) map[string]types.MessageAttributeValue {
	attrs := make(map[string]types.MessageAttributeValue, n)
	for i := range n {
		attrs[fmt.Sprintf("attr-%d", i)] = types.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(fmt.Sprintf("value-%d", i)),
		}
	}
	return attrs
}

func TestIntegrationScheduledRetryCreatesRealSchedule(t *testing.T) {
	const (
		body          = `{"event":"scheduled-real","id":7}`
		groupID       = "group-real-schedule"
		originalDedup = "orig-dedup"
		base          = time.Hour
		maxRetry      = 3
		maxAttributes = 10
	)

	cases := []struct {
		attributes     map[string]types.MessageAttributeValue
		name           string
		wantRetry      string
		delay          time.Duration
		wantAttributes int
	}{
		{
			name: "first retry keeps user attributes",
			attributes: map[string]types.MessageAttributeValue{
				"tenant": {DataType: aws.String("String"), StringValue: aws.String("acme")},
			},
			wantRetry:      "1",
			delay:          base,
			wantAttributes: 2,
		},
		{
			name:           "ten user attributes are capped to make room for retry count",
			attributes:     scheduledStringAttributes(maxAttributes),
			wantRetry:      "1",
			delay:          base,
			wantAttributes: maxAttributes,
		},
		{
			name: "retry count one below max is scheduled",
			attributes: map[string]types.MessageAttributeValue{
				"tenant":      {DataType: aws.String("String"), StringValue: aws.String("acme")},
				"retry_count": {DataType: aws.String("Number"), StringValue: aws.String(strconv.Itoa(maxRetry - 1))},
			},
			wantRetry:      strconv.Itoa(maxRetry),
			delay:          2 * base,
			wantAttributes: 2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := newSQSClient(t)
			sched := newSchedulerClient(t)
			rec := recordingScheduler(t, sched)
			entryURL := createFIFOQueue(t, client)
			dlqURL := createFIFOQueue(t, client)
			name := queueNameFromURL(t, client, entryURL)
			targetARN, roleARN := schedulerIdentity(t, client, entryURL)

			userAttributes := map[string]string{}
			for key, value := range tc.attributes {
				if key != "retry_count" {
					userAttributes[key] = aws.ToString(value.StringValue)
				}
			}

			handler := func(_ context.Context, _ middleware.Message) error {
				return errors.New("handler failed")
			}

			before := time.Now().UTC()
			_, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{
				QueueUrl:               aws.String(entryURL),
				MessageBody:            aws.String(body),
				MessageGroupId:         aws.String(groupID),
				MessageDeduplicationId: aws.String(originalDedup),
				MessageAttributes:      tc.attributes,
			})
			require.NoError(t, err)

			route := itNewRoute(t, name, handler,
				router.WithWaitTimeSeconds(1),
				router.WithVisibilityTimeout(11),
				router.WithScheduledRetry(
					router.WithSchedulerIdentity(targetARN, roleARN),
					router.WithScheduledDLQ(dlqURL),
					router.WithMaxRetryCount(maxRetry),
					router.WithBackoff(base, 2*base),
				),
			)
			cancel, done := runScheduledConsumer(t, client, route, rec)

			require.Eventually(t, func() bool {
				return len(rec.CreateScheduleCalls()) >= 1
			}, 45*time.Second, 250*time.Millisecond)
			waitQueueDrained(t, client, entryURL)
			stopConsumer(t, cancel, done)
			after := time.Now().UTC()

			dlq, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
				QueueUrl:            aws.String(dlqURL),
				MaxNumberOfMessages: 1,
				WaitTimeSeconds:     1,
			})
			require.NoError(t, err)
			assert.Empty(t, dlq.Messages)

			calls := rec.CreateScheduleCalls()
			require.Len(t, calls, 1)
			out, err := sched.GetSchedule(context.Background(), &scheduler.GetScheduleInput{Name: calls[0].Name})
			require.NoError(t, err)

			expression := aws.ToString(out.ScheduleExpression)
			require.Regexp(t, `^at\(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\)$`, expression)
			fireAt, err := time.Parse("at(2006-01-02T15:04:05)", expression)
			require.NoError(t, err)
			assert.WithinRange(t, fireAt, before.Add(tc.delay).Truncate(time.Second), after.Add(tc.delay).Truncate(time.Second))
			assert.Contains(t, []string{"", "UTC"}, aws.ToString(out.ScheduleExpressionTimezone))
			assert.Equal(t, schedulertypes.ActionAfterCompletionDelete, out.ActionAfterCompletion)
			require.NotNil(t, out.FlexibleTimeWindow)
			assert.Equal(t, schedulertypes.FlexibleTimeWindowModeOff, out.FlexibleTimeWindow.Mode)
			require.NotNil(t, out.Target)
			assert.Equal(t, "arn:aws:scheduler:::aws-sdk:sqs:sendMessage", aws.ToString(out.Target.Arn))
			assert.Equal(t, roleARN, aws.ToString(out.Target.RoleArn))

			var input scheduledSendMessage
			require.NoError(t, json.Unmarshal([]byte(aws.ToString(out.Target.Input)), &input))
			assert.Equal(t, entryURL, input.QueueURL)
			assert.Equal(t, body, input.MessageBody)
			assert.Equal(t, groupID, input.MessageGroupID)
			assert.NotEmpty(t, input.MessageDeduplicationID)
			assert.NotEqual(t, originalDedup, input.MessageDeduplicationID)
			assert.Equal(t, aws.ToString(calls[0].Name), input.MessageDeduplicationID)
			assert.Len(t, input.MessageAttributes, tc.wantAttributes)
			assert.Equal(t, scheduledAttribute{DataType: "Number", StringValue: tc.wantRetry}, input.MessageAttributes["retry_count"])
			for key, attr := range input.MessageAttributes {
				if key == "retry_count" {
					continue
				}
				require.Contains(t, userAttributes, key)
				assert.Equal(t, scheduledAttribute{DataType: "String", StringValue: userAttributes[key]}, attr, key)
			}
		})
	}
}
