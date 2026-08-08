// Command typed demonstrates strongly-typed message handling with loafer-awsx.
//
// Instead of decoding the raw message body by hand inside the handler, the
// typed package adapts a strongly-typed handler function into the standard
// middleware.Handler contract. A typed.Codec is responsible for turning the raw
// body into a value of the handler's type; here typed.JSONCodec decodes the
// JSON body into an OrderPlaced struct before the handler runs.
//
// It shows the full wiring of a typed consumer application:
//
//  1. Building an AWS connection (aws.Config) with the conn package.
//  2. Defining the message type and a typed handler for it.
//  3. Wrapping the typed handler with typed.WrapHandler and typed.JSONCodec.
//  4. Declaring a route with the wrapped handler and running a broker.
//  5. Graceful shutdown driven by OS signals.
//
// Run it against a real queue or a local AWS-compatible endpoint (for example
// LocalStack) by adjusting the region, credentials, endpoint, and queue name
// below.
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
	"github.com/silviolleite/loafer-awsx/router"
	"github.com/silviolleite/loafer-awsx/typed"
)

// queueName is the SQS queue this example consumes from. Replace it with the
// name of a queue that exists in your AWS account or local environment.
const queueName = "example-typed-queue"

// OrderPlaced is the strongly-typed payload this example expects on the queue.
// The struct tags describe the JSON shape the producers must send.
type OrderPlaced struct {
	OrderID    string  `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	Items      []Item  `json:"items"`
	Total      float64 `json:"total"`
}

// Item is a single line item of an OrderPlaced message. It demonstrates that
// the JSON codec decodes nested structures transparently.
type Item struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

func main() {
	// Step 5: derive a context canceled on interrupt (Ctrl+C) or SIGTERM so the
	// broker can drain in-flight messages before returning.
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

	// Step 2 (part 2): create the SQS client from the AWS config with the client
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

	// Step 3: wrap the typed handler. typed.WrapHandler takes a codec and a
	// strongly-typed function and returns a standard middleware.Handler. The
	// JSONCodec decodes the raw body into an OrderPlaced value; if decoding
	// fails the error is returned to the consumer and handleOrder is not called.
	handler := typed.WrapHandler(typed.JSONCodec[OrderPlaced]{}, handleOrder)

	// Step 4 (part 1): declare the route using the wrapped handler. From the
	// route's perspective this is an ordinary handler; the decoding happens
	// inside the wrapper.
	route, err := router.New(queueName, handler)
	if err != nil {
		log.Fatalf("failed to create route: %v", err)
	}

	// Step 4 (part 2): create and run the broker.
	b, err := broker.New(sqsClient, []*router.Route{route},
		broker.WithLogger(logger.New()),
	)
	if err != nil {
		log.Fatalf("failed to create broker: %v", err)
	}

	log.Printf("starting broker, consuming typed messages from queue %q (press Ctrl+C to stop)", queueName)
	if err := b.Run(ctx); err != nil {
		log.Fatalf("broker stopped with error: %v", err)
	}

	log.Println("broker stopped gracefully")
}

// handleOrder is a strongly-typed handler. It receives an already-decoded
// OrderPlaced value, so it can work with domain types directly instead of
// unmarshaling JSON. Returning a non-nil error leaves the message on the queue
// for redelivery.
func handleOrder(_ context.Context, order OrderPlaced) error {
	log.Printf("received order: id=%q customer=%q total=%.2f items=%d",
		order.OrderID, order.CustomerID, order.Total, len(order.Items))

	// A nil return acknowledges successful processing; the consumer then
	// deletes the message from the queue.
	return nil
}
