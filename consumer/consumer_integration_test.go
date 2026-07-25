//go:build integration

package consumer_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/silviolleite/loafer-awsx/conn"
	"github.com/silviolleite/loafer-awsx/consumer"
	"github.com/silviolleite/loafer-awsx/middleware"
	"github.com/silviolleite/loafer-awsx/router"
)

const (
	testRegion    = "us-east-1"
	testAccessKey = "test"
	testSecretKey = "test"
)

func endpoint() string {
	if v := os.Getenv("LOCALSTACK_ENDPOINT"); v != "" {
		return v
	}
	return "http://localhost:4566"
}

func newSQSClient(t *testing.T) *sqs.Client {
	t.Helper()

	cfg, err := conn.New(context.Background(),
		conn.WithRegion(testRegion),
		conn.WithAccessKey(testAccessKey, testSecretKey),
		conn.WithEndpoint(endpoint()),
		conn.WithRetryCount(1),
	)
	require.NoError(t, err)

	return sqs.NewFromConfig(cfg)
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

func uniqueName(prefix string) string {
	return fmt.Sprintf("loafer-it-%s-%d", prefix, time.Now().UnixNano())
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

	require.Eventually(t, func() bool {
		out, err := client.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
			QueueUrl: aws.String(url),
			AttributeNames: []types.QueueAttributeName{
				types.QueueAttributeNameApproximateNumberOfMessages,
				types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
			},
		})
		if err != nil {
			return false
		}
		visible := out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)]
		notVisible := out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)]
		return visible == "0" && notVisible == "0"
	}, 30*time.Second, 250*time.Millisecond)
}

func runConsumer(t *testing.T, client consumer.SQSClient, route *router.Route) (context.CancelFunc, <-chan error) {
	t.Helper()

	c, err := consumer.New(client, route)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	return cancel, done
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
