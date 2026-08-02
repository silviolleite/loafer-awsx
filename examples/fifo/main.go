// Command fifo demonstrates ordered consumption from an SQS FIFO queue with
// loafer-go v3.
//
// FIFO queues guarantee ordering within a message group. To preserve that
// ordering while still processing multiple groups concurrently, the route runs
// in router.PerGroupID mode: every message is dispatched to a worker chosen by
// hashing its group key, so all messages sharing a group key are handled by the
// same worker in the order they arrive. The group key is derived from the
// custom group fields configured on the route (here "tenant_id" and
// "order_id"), which are read from the message attributes.
//
// It shows the full wiring of a FIFO consumer application:
//
//  1. Building an AWS connection (aws.Config) with the conn package.
//  2. Writing a handler that inspects the group attributes.
//  3. Declaring a route in PerGroupID mode with custom group fields.
//  4. Creating an SQS client and a broker that owns the route.
//  5. Running the broker with graceful shutdown driven by OS signals.
//
// Run it against a real FIFO queue or a local AWS-compatible endpoint (for
// example LocalStack) by adjusting the region, credentials, endpoint, and queue
// name below. Note the required ".fifo" suffix on the queue name.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/silviolleite/loafer-awsx/broker"
	"github.com/silviolleite/loafer-awsx/client"
	"github.com/silviolleite/loafer-awsx/conn"
	"github.com/silviolleite/loafer-awsx/logger"
	"github.com/silviolleite/loafer-awsx/middleware"
	"github.com/silviolleite/loafer-awsx/router"
)

// queueName is the SQS FIFO queue this example consumes from. FIFO queue names
// must end with the ".fifo" suffix. Replace it with the name of a FIFO queue
// that exists in your AWS account or local environment.
const queueName = "example-fifo-queue.fifo"

// Group attribute keys used to derive the per-group ordering key. They must
// match the message attributes your producers set on each message.
const (
	attrTenantID = "tenant_id"
	attrOrderID  = "order_id"
)

func main() {
	// Step 5 (part 1): derive a context that is canceled when the process
	// receives an interrupt (Ctrl+C) or a SIGTERM, giving the broker a chance
	// to drain in-flight messages before returning.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Step 1: build the AWS connection. The static credentials and endpoint are
	// convenient for local development against LocalStack and can be dropped in
	// favor of the default AWS credential chain in production.
	cfg, err := conn.New(ctx,
		conn.WithRegion("us-east-1"),
		conn.WithAccessKey("test", "test"),
		conn.WithEndpoint("http://localhost:4566"),
	)
	if err != nil {
		log.Fatalf("failed to create AWS config: %v", err)
	}

	// Step 4 (part 1): create the SQS client from the AWS config with the client
	// constructor. It returns a consumer.SQSClient the broker requires without
	// importing the AWS SDK sqs package, and validates connectivity during
	// construction (a lightweight ListQueues ping), so a construction error may
	// also signal a connectivity failure. Pass client.WithoutConnectivityCheck()
	// to skip that validation for local runs where the ListQueues permission or
	// endpoint is unavailable.
	sqsClient, err := client.NewSQS(ctx, cfg)
	if err != nil {
		log.Fatalf("failed to create SQS client (construction or connectivity): %v", err)
	}

	// Step 3: declare the FIFO route. WithRunMode(router.PerGroupID) tells the
	// consumer to pin each group key to a single worker so ordering is
	// preserved within a group. WithCustomGroupFields lists the message
	// attributes combined (together with the SQS MessageGroupId) to form that
	// group key. Messages that share the same tenant_id and order_id are always
	// processed by the same worker, in order.
	route, err := router.New(queueName, handleMessage,
		router.WithRunMode(router.PerGroupID),
		router.WithCustomGroupFields(attrTenantID, attrOrderID),
		// A small worker pool still preserves per-group ordering while allowing
		// distinct groups to be processed in parallel.
		router.WithWorkerPoolSize(5),
	)
	if err != nil {
		log.Fatalf("failed to create route: %v", err)
	}

	// Step 4 (part 2): create the broker that owns the SQS client and the route.
	b, err := broker.New(sqsClient, []*router.Route{route},
		broker.WithLogger(logger.New()),
	)
	if err != nil {
		log.Fatalf("failed to create broker: %v", err)
	}

	// Step 5 (part 2): run the broker until every consumer stops. A nil return
	// means a clean, graceful shutdown.
	log.Printf("starting broker, consuming from FIFO queue %q (press Ctrl+C to stop)", queueName)
	if err := b.Run(ctx); err != nil {
		log.Fatalf("broker stopped with error: %v", err)
	}

	log.Println("broker stopped gracefully")
}

// handleMessage processes a single FIFO message. It reads the group attributes
// that make up the ordering key and decodes the JSON body. Returning a non-nil
// error leaves the message on the queue for redelivery; because the queue is
// FIFO, redelivery preserves the group's ordering.
func handleMessage(_ context.Context, msg middleware.Message) error {
	// The group attributes arrive as native SQS user message attributes because
	// the FIFO subscription uses raw message delivery, so they are read through
	// UserMessageAttribute rather than the SNS-envelope Attribute accessor.
	tenantID := msg.UserMessageAttribute(attrTenantID)
	orderID := msg.UserMessageAttribute(attrOrderID)

	var payload struct {
		Event string `json:"event"`
	}
	if err := msg.Decode(&payload); err != nil {
		return err
	}

	log.Printf("received FIFO message: tenant=%q order=%q event=%q",
		tenantID, orderID, payload.Event)

	// A nil return acknowledges successful processing; the consumer then
	// deletes the message, allowing the next message in the group to be
	// delivered.
	return nil
}
