// Command basic demonstrates standard SQS queue consumption with loafer-awsx.
//
// It shows the full wiring of a consumer application:
//
//  1. Building an AWS connection (aws.Config) with the conn package.
//  2. Writing a simple message handler.
//  3. Declaring a route with library defaults using the router package.
//  4. Creating an SQS client and a broker that owns the routes.
//  5. Running the broker with graceful shutdown driven by OS signals.
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
	"github.com/silviolleite/loafer-awsx/middleware"
	"github.com/silviolleite/loafer-awsx/router"
)

// queueName is the SQS queue this example consumes from. Replace it with the
// name of a queue that exists in your AWS account or local environment.
const queueName = "example-basic-queue"

func main() {
	// Step 5 (part 1): derive a context that is canceled when the process
	// receives an interrupt (Ctrl+C) or a SIGTERM. Passing this context to
	// broker.Run gives us graceful shutdown: the broker stops polling and lets
	// in-flight messages drain before returning.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Step 1: build the AWS connection. conn.New returns an aws.Config
	// configured from the supplied options. WithRegion is required; the static
	// credentials and endpoint below are convenient for local development
	// against LocalStack and can be dropped in favor of the default AWS
	// credential chain (profiles, environment variables, IAM roles) in
	// production.
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

	// Step 3: declare the route. router.New binds a queue name to a handler and
	// seeds sensible defaults (worker pool size, batch size, long-poll wait,
	// and visibility timeout), so a route needs no extra options to work.
	route, err := router.New(queueName, handleMessage)
	if err != nil {
		log.Fatalf("failed to create route: %v", err)
	}

	// Step 4 (part 2): create the broker. It owns the SQS client and the set of
	// routes, and starts one consumer per route when Run is called. WithLogger
	// attaches a structured stdout logger shared by every consumer.
	b, err := broker.New(sqsClient, []*router.Route{route},
		broker.WithLogger(logger.New()),
	)
	if err != nil {
		log.Fatalf("failed to create broker: %v", err)
	}

	// Step 5 (part 2): run the broker. Run blocks until every consumer stops,
	// either because a consumer failed to start or because ctx was canceled by
	// an OS signal. A nil return means a clean, graceful shutdown.
	log.Printf("starting broker, consuming from queue %q (press Ctrl+C to stop)", queueName)
	if err := b.Run(ctx); err != nil {
		log.Fatalf("broker stopped with error: %v", err)
	}

	log.Println("broker stopped gracefully")
}

// handleMessage is a simple message handler. It receives the processing context
// and the message, and returns a non-nil error to signal failure (which leaves
// the message on the queue for redelivery). Here it simply decodes the body
// into a struct and logs it.
//
// Step 2: the handler signature is middleware.Handler, i.e.
// func(ctx context.Context, msg middleware.Message) error.
func handleMessage(_ context.Context, msg middleware.Message) error {
	// payload models the expected JSON body of the message. Adjust it to match
	// the shape your producers send.
	var payload struct {
		ID   string `json:"id"`
		Text string `json:"text"`
	}

	if err := msg.Decode(&payload); err != nil {
		// Returning the error keeps the message on the queue so it can be
		// retried or eventually routed to a dead-letter queue.
		return err
	}

	log.Printf("received message: id=%q text=%q", payload.ID, payload.Text)

	// A nil return acknowledges successful processing; the consumer then
	// deletes the message from the queue.
	return nil
}
