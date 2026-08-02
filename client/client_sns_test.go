package client_test

import (
	"context"
	stderrors "errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/client"
	"github.com/silviolleite/loafer-awsx/errors"
)

type fakeSNS struct {
	listTopicsErr   error
	listTopicsCalls int
}

func (f *fakeSNS) ListTopics(context.Context, *sns.ListTopicsInput, ...func(*sns.Options)) (*sns.ListTopicsOutput, error) {
	f.listTopicsCalls++
	if f.listTopicsErr != nil {
		return nil, f.listTopicsErr
	}
	return &sns.ListTopicsOutput{}, nil
}

func (f *fakeSNS) Publish(context.Context, *sns.PublishInput, ...func(*sns.Options)) (*sns.PublishOutput, error) {
	return &sns.PublishOutput{}, nil
}

func (f *fakeSNS) PublishBatch(context.Context, *sns.PublishBatchInput, ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
	return &sns.PublishBatchOutput{}, nil
}

func TestNewSNSHealthyPingReturnsClient(t *testing.T) {
	api := &fakeSNS{}

	got, err := client.NewSNSForTest(context.Background(), api)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatalf("client = nil, want non-nil")
	}
	if api.listTopicsCalls != 1 {
		t.Errorf("listTopicsCalls = %d, want 1", api.listTopicsCalls)
	}
}

func TestNewSNSFailingPingReturnsError(t *testing.T) {
	api := &fakeSNS{listTopicsErr: errPing}

	got, err := client.NewSNSForTest(context.Background(), api)
	if got != nil {
		t.Errorf("client = %v, want nil", got)
	}
	if !stderrors.Is(err, errors.ErrPingFailed) {
		t.Errorf("err = %v, want match ErrPingFailed", err)
	}
	if !stderrors.Is(err, errPing) {
		t.Errorf("err = %v, want unwrap to underlying cause", err)
	}
}

func TestNewSNSInvalidOptionReturnsError(t *testing.T) {
	api := &fakeSNS{}

	got, err := client.NewSNSForTest(context.Background(), api, client.WithPingTimeout(0))
	if got != nil {
		t.Errorf("client = %v, want nil", got)
	}
	if !stderrors.Is(err, errors.ErrInvalidOption) {
		t.Errorf("err = %v, want match ErrInvalidOption", err)
	}
	if api.listTopicsCalls != 0 {
		t.Errorf("listTopicsCalls = %d, want 0", api.listTopicsCalls)
	}
}

func TestNewSNSWithoutConnectivityCheckSkipsPing(t *testing.T) {
	api := &fakeSNS{listTopicsErr: errPing}

	got, err := client.NewSNSForTest(context.Background(), api, client.WithoutConnectivityCheck())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatalf("client = nil, want non-nil")
	}
	if api.listTopicsCalls != 0 {
		t.Errorf("listTopicsCalls = %d, want 0", api.listTopicsCalls)
	}
}

func TestNewSNSNeverNeither(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		pingFails := rapid.Bool().Draw(rt, "pingFails")
		invalidOption := rapid.Bool().Draw(rt, "invalidOption")
		skipCheck := rapid.Bool().Draw(rt, "skipCheck")

		api := &fakeSNS{}
		if pingFails {
			api.listTopicsErr = errPing
		}

		var opts []client.Option
		if invalidOption {
			opts = append(opts, client.WithPingTimeout(0))
		}
		if skipCheck {
			opts = append(opts, client.WithoutConnectivityCheck())
		}

		got, err := client.NewSNSForTest(context.Background(), api, opts...)

		if (got != nil) == (err != nil) {
			rt.Fatalf("client=%v err=%v, want exactly one non-nil", got, err)
		}
	})
}
