// Package client provides constructors that build AWS SQS, SNS, and
// EventBridge Scheduler service clients from an aws.Config produced by the conn
// package, so that consuming applications do not need to import the AWS SDK for
// Go v2 service packages directly.
//
// Each constructor validates connectivity during construction: before returning
// a client, it issues a lightweight, read-only request (the "Ping") to confirm
// the client can reach its AWS service with valid credentials. The validation
// uses a dedicated timeout and retry budget that are independent of the request
// retry policy carried by the aws.Config, defaulting to a 3-second timeout and
// 2 retries. Both values are overridable through functional options, and the
// validation can be disabled entirely for environments where the read-only
// permission is not granted or where offline construction is required.
//
// The constructors return the library's own minimal interface types
// (consumer.SQSClient, producer.SNSClient, consumer.SchedulerClient) rather than
// the concrete SDK types, keeping the AWS SDK an internal implementation detail
// while remaining directly usable by the broker, consumer, and producer.
package client
