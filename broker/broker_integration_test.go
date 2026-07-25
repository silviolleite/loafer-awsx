//go:build integration

package broker_test

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
	"go.uber.org/goleak"

	"github.com/silviolleite/loafer-awsx/broker"
	"github.com/silviolleite/loafer-awsx/conn"
	"github.com/silviolleite/loafer-awsx/middleware"
	"github.com/silviolleite/loafer-awsx/router"
)

const (
	integrationRegion    = "us-east-1"
	integrationAccessKey = "test"
	integrationSecretKey = "test"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
		goleak.IgnoreAnyFunction("net/http.(*http2ClientConn).readLoop"),
	)
}

func integrationEndpoint() string {
	if v := os.Getenv("LOCALSTACK_ENDPOINT"); v != "" {
		return v
	}
	return "http://localhost:4566"
}

func integrationClient(t *testing.T) *sqs.Client {
	t.Helper()

	cfg, err := conn.New(context.Background(),
		conn.WithRegion(integrationRegion),
		conn.WithAccessKey(integrationAccessKey, integrationSecretKey),
		conn.WithEndpoint(integrationEndpoint()),
		conn.WithRetryCount(1),
	)
	require.NoError(t, err)

	return sqs.NewFromConfig(cfg)
}

func integrationUniqueName(prefix string) string {
	return fmt.Sprintf("loafer-it-broker-%s-%d", prefix, time.Now().UnixNano())
}

func integrationCreateQueue(t *testing.T, client *sqs.Client, name string, attrs map[string]string) string {
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

func integrationStandardQueue(t *testing.T, client *sqs.Client) string {
	t.Helper()
	return integrationCreateQueue(t, client, integrationUniqueName("standard"), nil)
}

func integrationFIFOQueue(t *testing.T, client *sqs.Client) string {
	t.Helper()
	return integrationCreateQueue(t, client, integrationUniqueName("group")+".fifo", map[string]string{
		"FifoQueue":                 "true",
		"ContentBasedDeduplication": "true",
	})
}

func integrationQueueName(t *testing.T, client *sqs.Client, url string) string {
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

func integrationSend(t *testing.T, client *sqs.Client, url string, body any, groupID string) {
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

func integrationWaitDrained(t *testing.T, client *sqs.Client, url string) {
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

func TestIntegrationBrokerFullLifecycle(t *testing.T) {
	client := integrationClient(t)

	ordersURL := integrationStandardQueue(t, client)
	paymentsURL := integrationStandardQueue(t, client)
	ordersName := integrationQueueName(t, client, ordersURL)
	paymentsName := integrationQueueName(t, client, paymentsURL)

	var orders, payments atomic.Int32
	ordersHandler := func(_ context.Context, _ middleware.Message) error {
		orders.Add(1)
		return nil
	}
	paymentsHandler := func(_ context.Context, _ middleware.Message) error {
		payments.Add(1)
		return nil
	}

	integrationSend(t, client, ordersURL, map[string]string{"event": "order"}, "")
	integrationSend(t, client, ordersURL, map[string]string{"event": "order"}, "")
	integrationSend(t, client, paymentsURL, map[string]string{"event": "payment"}, "")

	routes := []*router.Route{
		newRoute(t, ordersName, ordersHandler, router.WithWaitTimeSeconds(1), router.WithVisibilityTimeout(11)),
		newRoute(t, paymentsName, paymentsHandler, router.WithWaitTimeSeconds(1), router.WithVisibilityTimeout(11)),
	}

	b, err := broker.New(client, routes, broker.WithLogger(noopLogger()))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()

	require.Eventually(t, func() bool {
		return orders.Load() == 2 && payments.Load() == 1
	}, 30*time.Second, 250*time.Millisecond)

	integrationWaitDrained(t, client, ordersURL)
	integrationWaitDrained(t, client, paymentsURL)

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("broker Run did not return after cancellation")
	}
}

func TestIntegrationBrokerGracefulShutdownInFlight(t *testing.T) {
	client := integrationClient(t)
	url := integrationStandardQueue(t, client)
	name := integrationQueueName(t, client, url)

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var completed atomic.Bool

	handler := func(_ context.Context, _ middleware.Message) error {
		started <- struct{}{}
		<-release
		completed.Store(true)
		return nil
	}

	integrationSend(t, client, url, map[string]string{"event": "inflight"}, "")

	routes := []*router.Route{
		newRoute(t, name, handler, router.WithWaitTimeSeconds(1), router.WithVisibilityTimeout(30)),
	}

	b, err := broker.New(client, routes,
		broker.WithLogger(noopLogger()),
		broker.WithShutdownTimeout(30*time.Second),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()

	select {
	case <-started:
	case <-time.After(30 * time.Second):
		t.Fatal("handler never started")
	}

	cancel()
	assert.False(t, completed.Load())
	close(release)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("broker Run did not return after in-flight completion")
	}

	assert.True(t, completed.Load())
}

func TestIntegrationBrokerPerGroupIDConsistency(t *testing.T) {
	client := integrationClient(t)
	url := integrationFIFOQueue(t, client)
	name := integrationQueueName(t, client, url)

	groups := []string{"alpha", "beta", "gamma"}
	perGroup := 6

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
			integrationSend(t, client, url, map[string]any{"group": g, "seq": seq}, g)
		}
	}

	routes := []*router.Route{
		newRoute(t, name, handler,
			router.WithWaitTimeSeconds(1),
			router.WithVisibilityTimeout(30),
			router.WithWorkerPoolSize(5),
			router.WithMaxMessages(10),
			router.WithRunMode(router.PerGroupID),
		),
	}

	b, err := broker.New(client, routes, broker.WithLogger(noopLogger()))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()

	require.Eventually(t, func() bool {
		return total.Load() == int32(len(groups)*perGroup)
	}, 45*time.Second, 250*time.Millisecond)

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("broker Run did not return after cancellation")
	}

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
