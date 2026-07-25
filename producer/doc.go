// Package producer publishes messages to AWS SNS topics. It supports standard
// and FIFO topics through a minimal SNSClient interface satisfied by the
// aws-sdk-go-v2 *sns.Client, and exposes a helper to build topic ARNs.
package producer
