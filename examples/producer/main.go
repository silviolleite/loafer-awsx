// Command producer demonstrates publishing messages to SNS topics with
// loafer-go v3.
//
// It covers the four common publishing shapes:
//
//  1. Single publish to a standard topic.
//  2. Batch publish (up to ten messages) to a standard topic.
//  3. Single publish to a FIFO topic, with the MessageGroupId and
//     MessageDeduplicationId auto-generated from the message attributes.
//  4. Batch publish to a FIFO topic.
//
// For FIFO topics the producer is configured with ID generators: a
// deterministic key-based generator derives the MessageGroupId from selected
// attributes (so messages about the same entity share a group and stay
// ordered), and a random generator supplies a unique MessageDeduplicationId per
// message. Auto-generation only kicks in for FIFO topics (ARNs ending in
// ".fifo") and only when the caller does not set the value explicitly.
//
// Run it against real SNS topics or a local AWS-compatible endpoint (for
// example LocalStack) by adjusting the region, credentials, endpoint, account
// ID, and topic names below.
package main

import (
	"context"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sns"

	"github.com/silviolleite/loafer-awsx/conn"
	"github.com/silviolleite/loafer-awsx/idgen"
	"github.com/silviolleite/loafer-awsx/producer"
)

const (
	// region and accountID are used to build topic ARNs. Replace them with your
	// AWS region and account ID (LocalStack commonly uses 000000000000).
	region    = "us-east-1"
	accountID = "000000000000"

	// standardTopic and fifoTopic are the destination topic names. FIFO topic
	// names must end with ".fifo".
	standardTopic = "example-standard-topic"
	fifoTopic     = "example-fifo-topic.fifo"

	// ordersTopic carries OrderPlaced-shaped messages consumed by the typed
	// example. It has its own topic so the typed queue only ever receives the
	// order payloads its codec knows how to decode.
	ordersTopic = "example-orders-topic"

	// scheduledTopic fans messages out to the FIFO Scheduled Retry Entry_Queue
	// (example-scheduled.fifo). Publishing here seeds that queue so the
	// fifo-scheduled-retry consumer has input to exercise the scheduled-retry
	// and DLQ paths. FIFO topic names must end with ".fifo".
	scheduledTopic = "example-scheduled-topic.fifo"

	// publishTimeout bounds the whole publishing sequence.
	publishTimeout = 15 * time.Second
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
	defer cancel()

	// Step 1: build the AWS connection. The static credentials and endpoint are
	// convenient for local development against LocalStack and can be dropped in
	// favor of the default AWS credential chain in production.
	cfg, err := conn.New(ctx,
		conn.WithRegion(region),
		conn.WithAccessKey("test", "test"),
		conn.WithEndpoint("http://localhost:4566"),
	)
	if err != nil {
		log.Fatalf("failed to create AWS config: %v", err)
	}

	// Step 2: create the SNS client. *sns.Client satisfies producer.SNSClient.
	snsClient := sns.NewFromConfig(cfg)

	// Step 3: create the producer with FIFO ID generators. The key-based
	// generator builds a stable MessageGroupId from the "tenant_id" and
	// "order_id" attributes, keeping messages about the same order in one group.
	// The random generator produces a unique MessageDeduplicationId per message.
	// These generators are only consulted for FIFO topics and only when the
	// corresponding ID is not set explicitly on the input.
	p, err := producer.New(snsClient,
		producer.WithGroupIDGenerator(idgen.NewKeyBased(
			idgen.WithFields("tenant_id", "order_id"),
		)),
		producer.WithDeduplicationIDGenerator(idgen.NewRandom()),
	)
	if err != nil {
		log.Fatalf("failed to create producer: %v", err)
	}

	// Build the destination ARNs from their components.
	standardARN := producer.BuildTopicARN(region, accountID, standardTopic)
	fifoARN := producer.BuildTopicARN(region, accountID, fifoTopic)
	ordersARN := producer.BuildTopicARN(region, accountID, ordersTopic)
	scheduledARN := producer.BuildTopicARN(region, accountID, scheduledTopic)

	publishStandardSingle(ctx, p, standardARN)
	publishStandardBatch(ctx, p, standardARN)
	publishFIFOSingle(ctx, p, fifoARN)
	publishFIFOBatch(ctx, p, fifoARN)
	publishOrder(ctx, p, ordersARN)
	publishScheduledRetry(ctx, p, scheduledARN)

	log.Println("all messages published")
}

// publishOrder publishes a single OrderPlaced-shaped message to the orders
// topic. The typed example subscribes its queue to this topic and decodes the
// JSON body into its OrderPlaced struct, so the payload here must match that
// shape (order_id, customer_id, total, and a list of items with sku/quantity).
func publishOrder(ctx context.Context, p *producer.Producer, topicARN string) {
	const order = `{"order_id":"o-1001","customer_id":"c-42","total":129.99,` +
		`"items":[{"sku":"abc","quantity":2},{"sku":"xyz","quantity":1}]}`

	msgID, err := p.Publish(ctx, &producer.PublishInput{
		TopicARN:   topicARN,
		Message:    order,
		Attributes: map[string]string{"event_type": "order.placed"},
	})
	if err != nil {
		log.Fatalf("order publish failed: %v", err)
	}

	log.Printf("order publish ok: message_id=%s", msgID)
}

// publishStandardSingle publishes one message to a standard topic. No FIFO
// fields are needed, so GroupID and DeduplicationID are left unset.
func publishStandardSingle(ctx context.Context, p *producer.Producer, topicARN string) {
	msgID, err := p.Publish(ctx, &producer.PublishInput{
		TopicARN:   topicARN,
		Message:    `{"id":"u-1001","text":"user created"}`,
		Attributes: map[string]string{"event_type": "user.created"},
	})
	if err != nil {
		log.Fatalf("standard single publish failed: %v", err)
	}

	log.Printf("standard single publish ok: message_id=%s", msgID)
}

// publishStandardBatch publishes several messages to a standard topic in a
// single request. Each entry needs an ID unique within the batch so the result
// can be correlated back to the request entry.
func publishStandardBatch(ctx context.Context, p *producer.Producer, topicARN string) {
	out, err := p.PublishBatch(ctx, &producer.PublishBatchInput{
		TopicARN: topicARN,
		Messages: []*producer.PublishBatchEntry{
			{
				ID:         "1",
				Message:    `{"id":"u-1001","text":"user updated"}`,
				Attributes: map[string]string{"event_type": "user.updated"},
			},
			{
				ID:         "2",
				Message:    `{"id":"u-1002","text":"user deleted"}`,
				Attributes: map[string]string{"event_type": "user.deleted"},
			},
		},
	})
	if err != nil {
		log.Fatalf("standard batch publish failed: %v", err)
	}

	logBatchOutput("standard", out)
}

// publishFIFOSingle publishes one message to a FIFO topic. GroupID and
// DeduplicationID are intentionally left empty: because the topic ARN ends in
// ".fifo", the producer auto-generates them from the Attributes using the
// configured generators.
func publishFIFOSingle(ctx context.Context, p *producer.Producer, topicARN string) {
	msgID, err := p.Publish(ctx, &producer.PublishInput{
		TopicARN: topicARN,
		Message:  `{"event":"order.placed","order_id":"o-42"}`,
		Attributes: map[string]string{
			"tenant_id": "acme",
			"order_id":  "o-42",
		},
	})
	if err != nil {
		log.Fatalf("fifo single publish failed: %v", err)
	}

	log.Printf("fifo single publish ok: message_id=%s", msgID)
}

// publishFIFOBatch publishes a batch to a FIFO topic. As with the single FIFO
// publish, the MessageGroupId and MessageDeduplicationId are derived per entry
// from each entry's Attributes. Entries sharing the same tenant_id and order_id
// land in the same group and preserve their relative order.
func publishFIFOBatch(ctx context.Context, p *producer.Producer, topicARN string) {
	out, err := p.PublishBatch(ctx, &producer.PublishBatchInput{
		TopicARN: topicARN,
		Messages: []*producer.PublishBatchEntry{
			{
				ID:      "1",
				Message: `{"event":"order.item_added","order_id":"o-42","sku":"abc"}`,
				Attributes: map[string]string{
					"tenant_id": "acme",
					"order_id":  "o-42",
				},
			},
			{
				ID:      "2",
				Message: `{"event":"order.item_added","order_id":"o-42","sku":"xyz"}`,
				Attributes: map[string]string{
					"tenant_id": "acme",
					"order_id":  "o-42",
				},
			},
		},
	})
	if err != nil {
		log.Fatalf("fifo batch publish failed: %v", err)
	}

	logBatchOutput("fifo", out)
}

// publishScheduledRetry publishes a small FIFO batch to the Scheduled Retry
// topic, seeding the example-scheduled.fifo Entry_Queue consumed by the
// fifo-scheduled-retry example. As with the other FIFO publishes, GroupID and
// DeduplicationID are left empty so the producer auto-generates them from the
// Attributes: the key-based generator derives the MessageGroupId from tenant_id
// and order_id, and the random generator supplies a unique
// MessageDeduplicationId. The scheduled-retry consumer's handler always fails,
// so these messages exercise the scheduled-retry and DLQ paths.
func publishScheduledRetry(ctx context.Context, p *producer.Producer, topicARN string) {
	out, err := p.PublishBatch(ctx, &producer.PublishBatchInput{
		TopicARN: topicARN,
		Messages: []*producer.PublishBatchEntry{
			{
				ID:      "1",
				Message: `{"event":"payment.retry","order_id":"o-77"}`,
				Attributes: map[string]string{
					"tenant_id": "acme",
					"order_id":  "o-77",
				},
			},
			{
				ID:      "2",
				Message: `{"event":"payment.retry","order_id":"o-88"}`,
				Attributes: map[string]string{
					"tenant_id": "acme",
					"order_id":  "o-88",
				},
			},
		},
	})
	if err != nil {
		log.Fatalf("scheduled-retry batch publish failed: %v", err)
	}

	logBatchOutput("scheduled-retry", out)
}

// logBatchOutput reports the per-entry outcome of a batch publish.
func logBatchOutput(label string, out *producer.PublishBatchOutput) {
	for _, s := range out.Successful {
		log.Printf("%s batch publish ok: entry_id=%s message_id=%s", label, s.EntryID, s.MessageID)
	}
	for _, f := range out.Failed {
		log.Printf("%s batch publish failed: entry_id=%s error=%v", label, f.EntryID, f.Err)
	}
}
