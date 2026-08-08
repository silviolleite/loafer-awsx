// Command fifo-scheduled-retry demonstrates the Scheduled Retry model for an
// SQS FIFO queue with loafer-awsx.
//
// FIFO queues preserve ordering within a message group, which makes the default
// "leave the message on the queue and retry in place" behavior problematic: a
// single failing message would block every later message that shares its group.
// The Scheduled Retry model avoids that head-of-line blocking. When a handler
// returns an error the consumer deletes the original message and asks
// EventBridge Scheduler to re-publish it to the same Entry_Queue after a
// computed backoff delay, so ordering for other groups keeps flowing while the
// failed message is retried later.
//
// It shows the full wiring of a FIFO Scheduled Retry consumer application:
//
//  1. Building an AWS connection (aws.Config) with the conn package.
//  2. Creating the SQS and EventBridge Scheduler clients with the client
//     constructors, which validate connectivity during construction.
//  3. Declaring a FIFO route that selects the Scheduled Retry model with the
//     scheduler identity, DLQ destination, max retry count, and backoff bounds.
//  4. Wiring the scheduler client and the observability hooks into a consumer
//     built directly with consumer.New (the broker does not forward these
//     consumer options, so a scheduled route is run through consumer.New).
//  5. Running the consumer with graceful shutdown driven by OS signals.
//
// Required resources (provisioned by the example Terraform config in
// examples/terraform via `make provision`; create them yourself when running
// against another environment):
//
//   - An Entry_Queue: the FIFO queue this example consumes from and that the
//     scheduler re-publishes to. Because retries are re-published messages, the
//     queue must use explicit deduplication (ContentBasedDeduplication disabled;
//     producers and the retry publisher supply their own MessageDeduplicationId)
//     so a retry is never dropped as a duplicate.
//   - A DLQ: a separate FIFO queue that receives messages whose Retry_Count
//     exceeds Max_Retry_Count.
//   - An execution role: an IAM role EventBridge Scheduler assumes to deliver
//     the schedule. It needs iam:PassRole (to pass itself), scheduler:CreateSchedule,
//     and sqs:SendMessage on the Entry_Queue.
//
// Run it against a real environment or a local AWS-compatible endpoint (for
// example LocalStack) by adjusting the region, credentials, endpoint, and the
// placeholder ARNs and URLs below. Note the required ".fifo" suffix on FIFO
// queue names.
//
// LocalStack note: LocalStack only mocks EventBridge Scheduler and never fires
// the schedules this consumer creates, so the retry re-delivery and DLQ loop
// does not complete on its own locally. Run the examples/localscheduler helper
// alongside this consumer to emulate the scheduler firing and close the loop.
// Against real AWS the helper is unnecessary — EventBridge Scheduler fires the
// schedules itself.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/silviolleite/loafer-awsx/client"
	"github.com/silviolleite/loafer-awsx/conn"
	"github.com/silviolleite/loafer-awsx/consumer"
	"github.com/silviolleite/loafer-awsx/logger"
	"github.com/silviolleite/loafer-awsx/middleware"
	"github.com/silviolleite/loafer-awsx/router"
)

// queueName is the SQS FIFO Entry_Queue this example consumes from. FIFO queue
// names must end with the ".fifo" suffix. Replace it with a FIFO queue that
// exists in your AWS account or local environment.
const queueName = "example-scheduled.fifo"

// Placeholder identifiers for the Scheduled Retry model. Replace each of them
// with the real ARNs and URL from your environment before running.
const (
	// targetQueueARN is the ARN of the Entry_Queue the scheduler re-publishes
	// failed messages to.
	targetQueueARN = "arn:aws:sqs:us-east-1:000000000000:example-scheduled.fifo"
	// executionRoleARN is the IAM role EventBridge Scheduler assumes to deliver
	// each retry schedule. It needs iam:PassRole, scheduler:CreateSchedule, and
	// sqs:SendMessage on the Entry_Queue.
	executionRoleARN = "arn:aws:iam::000000000000:role/loafer-scheduler"
	// dlqQueueURL is the URL of the FIFO DLQ that receives messages whose
	// Retry_Count exceeds Max_Retry_Count.
	dlqQueueURL = "http://localhost:4566/000000000000/example-scheduled-dlq.fifo"
)

func main() {
	// Step 5 (part 1): derive a context that is canceled when the process
	// receives an interrupt (Ctrl+C) or a SIGTERM, giving the consumer a chance
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

	// Step 2: create the SQS client and the EventBridge Scheduler client from
	// the AWS config with the client constructors. They return interfaces
	// usable by the consumer without importing the AWS SDK service packages and
	// validate connectivity during construction (an SQS ListQueues and a
	// Scheduler ListSchedules ping), so a construction error may also signal a
	// connectivity failure. Pass client.WithoutConnectivityCheck() to skip that
	// validation for local runs where the List* endpoints or permissions are
	// unavailable.
	sqsClient, err := client.NewSQS(ctx, cfg)
	if err != nil {
		log.Fatalf("failed to create SQS client (construction or connectivity): %v", err)
	}
	schedClient, err := client.NewScheduler(ctx, cfg)
	if err != nil {
		log.Fatalf("failed to create Scheduler client (construction or connectivity): %v", err)
	}

	// Step 3: declare the FIFO route with the Scheduled Retry model.
	// WithScheduledRetry switches the route from in-place redelivery to
	// scheduler-driven retries. Its nested options configure:
	//   - WithSchedulerIdentity: the target Entry_Queue ARN and the IAM role the
	//     scheduler assumes to deliver the retry.
	//   - WithScheduledDLQ: the DLQ that receives exhausted messages.
	//   - WithMaxRetryCount: how many retries to attempt before dead-lettering.
	//   - WithBackoff: the base and maximum delay bounding the exponential
	//     backoff applied between retries.
	route, err := router.New(queueName, handleMessage,
		router.WithScheduledRetry(
			router.WithSchedulerIdentity(targetQueueARN, executionRoleARN),
			router.WithScheduledDLQ(dlqQueueURL),
			router.WithMaxRetryCount(3),
			router.WithBackoff(time.Second, 30*time.Second),
		),
	)
	if err != nil {
		log.Fatalf("failed to create route: %v", err)
	}

	// Step 4: build the consumer directly with consumer.New. A scheduled route
	// needs the EventBridge Scheduler client and the observability hooks, which
	// are consumer-level options, so it is run through consumer.New rather than
	// a broker. WithSchedulerClient supplies the scheduler used to create retry
	// schedules; the metric hooks log each outcome (success, retry, dead-letter)
	// so the scheduled-retry path is easy to observe when running the example.
	c, err := consumer.New(sqsClient, route,
		consumer.WithSchedulerClient(schedClient),
		consumer.WithLogger(logger.New()),
		consumer.WithSuccessMetric(func(route string) {
			log.Printf("success: route=%q message processed and deleted", route)
		}),
		consumer.WithRetryMetric(func(route string) {
			log.Printf("retry: route=%q retry schedule created", route)
		}),
		consumer.WithDeadLetterMetric(func(route string) {
			log.Printf("dead-letter: route=%q exhausted message sent to DLQ", route)
		}),
	)
	if err != nil {
		log.Fatalf("failed to create consumer: %v", err)
	}

	// Step 5 (part 2): run the consumer until it stops. A nil return means a
	// clean, graceful shutdown.
	log.Printf("starting consumer, consuming from FIFO queue %q with scheduled retry (press Ctrl+C to stop)", queueName)
	if err := c.Run(ctx); err != nil {
		log.Fatalf("consumer stopped with error: %v", err)
	}

	log.Println("consumer stopped gracefully")
}

// handleMessage always returns an error to exercise the Scheduled Retry path:
// the consumer deletes the original message and schedules a re-publish to the
// Entry_Queue after the computed backoff. Once Retry_Count exceeds the route's
// Max_Retry_Count the message is forwarded to the DLQ instead. Replace the body
// with real processing that returns nil on success.
func handleMessage(_ context.Context, msg middleware.Message) error {
	id := msg.Identifier()
	log.Printf("received message %q, forcing failure to trigger a scheduled retry", id)
	return fmt.Errorf("simulated processing failure for message %q", id)
}
