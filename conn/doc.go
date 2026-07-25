// Package conn provides a factory for AWS SDK v2 configuration.
//
// It encapsulates credential resolution, endpoint configuration, retry
// policies, and profile loading behind a functional-options API, producing an
// aws.Config that is reusable across SQS and SNS clients.
package conn
