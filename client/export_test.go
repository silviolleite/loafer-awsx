package client

import (
	"context"
	"time"

	"github.com/silviolleite/loafer-awsx/consumer"
	"github.com/silviolleite/loafer-awsx/producer"
)

// SQSAPI is an exported alias of the unexported sqsAPI interface so the external
// client_test package can supply a fake implementation to NewSQSForTest.
type SQSAPI = sqsAPI

// NewSQSForTest drives the unexported newSQS seam with an injected sqsAPI, so
// the external client_test package can exercise the constructor without building
// a real aws.Config or reaching the network.
func NewSQSForTest(ctx context.Context, api SQSAPI, opts ...Option) (consumer.SQSClient, error) {
	return newSQS(ctx, api, opts...)
}

// SNSAPI is an exported alias of the unexported snsAPI interface so the external
// client_test package can supply a fake implementation to NewSNSForTest.
type SNSAPI = snsAPI

// NewSNSForTest drives the unexported newSNS seam with an injected snsAPI, so
// the external client_test package can exercise the constructor without building
// a real aws.Config or reaching the network.
func NewSNSForTest(ctx context.Context, api SNSAPI, opts ...Option) (producer.SNSClient, error) {
	return newSNS(ctx, api, opts...)
}

// OptionsView is a read-only projection of the internal options struct exposed
// to the external client_test package so tests can assert the effect of options
// without depending on unexported fields.
type OptionsView struct {
	PingTimeout       time.Duration
	PingRetryLimit    uint
	ConnectivityCheck bool
}

// BuildOptions applies opts through the internal buildOptions and returns a
// read-only view of the resulting configuration for tests.
func BuildOptions(opts ...Option) (*OptionsView, error) {
	o, err := buildOptions(opts...)
	if err != nil {
		return nil, err
	}
	cfg := o.pingConfig()
	return &OptionsView{
		PingTimeout:       cfg.timeout,
		PingRetryLimit:    cfg.retryLimit,
		ConnectivityCheck: o.connectivityCheck,
	}, nil
}

// RecordingOption returns an Option that invokes record when applied, letting
// tests observe the order in which options are applied.
func RecordingOption(record func()) Option {
	return func(*options) error {
		record()
		return nil
	}
}

// Validate exposes the internal validation runner to the external client_test
// package. It builds a pingConfig from timeout and retryLimit and drives the
// unexported validate seam with the supplied do closure.
func Validate(ctx context.Context, timeout time.Duration, retryLimit uint, do func(context.Context) error) error {
	return validate(ctx, pingConfig{timeout: timeout, retryLimit: retryLimit}, do)
}

// SchedulerAPI is an exported alias of the unexported schedulerAPI interface so
// the external client_test package can supply a fake implementation to
// NewSchedulerForTest.
type SchedulerAPI = schedulerAPI

// NewSchedulerForTest drives the unexported newScheduler seam with an injected
// schedulerAPI, so the external client_test package can exercise the constructor
// without building a real aws.Config or reaching the network.
func NewSchedulerForTest(ctx context.Context, api SchedulerAPI, opts ...Option) (consumer.SchedulerClient, error) {
	return newScheduler(ctx, api, opts...)
}
