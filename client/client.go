package client

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/silviolleite/loafer-awsx/consumer"
	"github.com/silviolleite/loafer-awsx/producer"
)

// The unexported constructor seams (newSQS / newSNS / newScheduler) are exported
// to the external client_test package per-constructor in export_test.go, so the
// build stays green as each constructor is implemented.

// sqsAPI is the internal surface the SQS constructor depends on: the operations
// the consumer and broker require plus the read-only ListQueues call used for
// connectivity validation during construction.
type sqsAPI interface {
	consumer.SQSClient
	ListQueues(ctx context.Context, params *sqs.ListQueuesInput, optFns ...func(*sqs.Options)) (*sqs.ListQueuesOutput, error)
}

// snsAPI is the internal surface the SNS constructor depends on: the operations
// the producer requires plus the read-only ListTopics call used for connectivity
// validation during construction.
type snsAPI interface {
	producer.SNSClient
	ListTopics(ctx context.Context, params *sns.ListTopicsInput, optFns ...func(*sns.Options)) (*sns.ListTopicsOutput, error)
}

// schedulerAPI is the internal surface the Scheduler constructor depends on: the
// operations the consumer requires plus the read-only ListSchedules call used for
// connectivity validation during construction.
type schedulerAPI interface {
	consumer.SchedulerClient
	ListSchedules(
		ctx context.Context,
		params *scheduler.ListSchedulesInput,
		optFns ...func(*scheduler.Options),
	) (*scheduler.ListSchedulesOutput, error)
}

// NewSQS builds an SQS client from cfg, validates connectivity during
// construction unless disabled, and returns a client usable by the broker and
// consumer without importing the AWS SDK sqs package. It returns an error
// wrapping ErrInvalidOption for an invalid option, or ErrPingFailed when the
// connectivity validation fails, and never returns both a nil client and a nil
// error.
//
//nolint:gocritic // cfg is taken by value to match sqs.NewFromConfig and the AWS SDK idiom.
func NewSQS(ctx context.Context, cfg aws.Config, opts ...Option) (consumer.SQSClient, error) {
	return newSQS(ctx, sqs.NewFromConfig(cfg), opts...)
}

// newSQS applies the options, optionally validates connectivity through the
// read-only ListQueues call with the SDK retryer disabled, and returns the api
// typed as consumer.SQSClient or the wrapped error.
func newSQS(ctx context.Context, api sqsAPI, opts ...Option) (consumer.SQSClient, error) {
	o, err := buildOptions(opts...)
	if err != nil {
		return nil, err
	}
	if o.connectivityCheck {
		do := func(ctx context.Context) error {
			_, err := api.ListQueues(ctx, &sqs.ListQueuesInput{MaxResults: aws.Int32(1)},
				func(o *sqs.Options) { o.Retryer = aws.NopRetryer{} })
			return err
		}
		if err := validate(ctx, o.pingConfig(), do); err != nil {
			return nil, err
		}
	}
	return api, nil
}

// NewSNS builds an SNS client from cfg, validates connectivity during
// construction unless disabled, and returns a client usable by the producer
// without importing the AWS SDK sns package. It returns an error wrapping
// ErrInvalidOption for an invalid option, or ErrPingFailed when the
// connectivity validation fails, and never returns both a nil client and a nil
// error.
//
//nolint:gocritic // cfg is taken by value to match sns.NewFromConfig and the AWS SDK idiom.
func NewSNS(ctx context.Context, cfg aws.Config, opts ...Option) (producer.SNSClient, error) {
	return newSNS(ctx, sns.NewFromConfig(cfg), opts...)
}

// newSNS applies the options, optionally validates connectivity through the
// read-only ListTopics call with the SDK retryer disabled, and returns the api
// typed as producer.SNSClient or the wrapped error.
func newSNS(ctx context.Context, api snsAPI, opts ...Option) (producer.SNSClient, error) {
	o, err := buildOptions(opts...)
	if err != nil {
		return nil, err
	}
	if o.connectivityCheck {
		do := func(ctx context.Context) error {
			_, err := api.ListTopics(ctx, &sns.ListTopicsInput{},
				func(o *sns.Options) { o.Retryer = aws.NopRetryer{} })
			return err
		}
		if err := validate(ctx, o.pingConfig(), do); err != nil {
			return nil, err
		}
	}
	return api, nil
}

// NewScheduler builds an EventBridge Scheduler client from cfg, validates
// connectivity during construction unless disabled, and returns a client usable
// by the consumer for scheduled retries without importing the AWS SDK scheduler
// package. It returns an error wrapping ErrInvalidOption for an invalid option,
// or ErrPingFailed when the connectivity validation fails, and never returns
// both a nil client and a nil error.
//
//nolint:gocritic // cfg is taken by value to match scheduler.NewFromConfig and the AWS SDK idiom.
func NewScheduler(ctx context.Context, cfg aws.Config, opts ...Option) (consumer.SchedulerClient, error) {
	return newScheduler(ctx, scheduler.NewFromConfig(cfg), opts...)
}

// newScheduler applies the options, optionally validates connectivity through
// the read-only ListSchedules call with the SDK retryer disabled, and returns
// the api typed as consumer.SchedulerClient or the wrapped error.
func newScheduler(ctx context.Context, api schedulerAPI, opts ...Option) (consumer.SchedulerClient, error) {
	o, err := buildOptions(opts...)
	if err != nil {
		return nil, err
	}
	if o.connectivityCheck {
		do := func(ctx context.Context) error {
			_, err := api.ListSchedules(ctx, &scheduler.ListSchedulesInput{MaxResults: aws.Int32(1)},
				func(o *scheduler.Options) { o.Retryer = aws.NopRetryer{} })
			return err
		}
		if err := validate(ctx, o.pingConfig(), do); err != nil {
			return nil, err
		}
	}
	return api, nil
}
