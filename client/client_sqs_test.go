package client_test

import (
	"context"
	stderrors "errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/client"
	"github.com/silviolleite/loafer-awsx/errors"
)

type fakeSQS struct {
	listQueuesErr   error
	listQueuesCalls int
}

func (f *fakeSQS) ListQueues(context.Context, *sqs.ListQueuesInput, ...func(*sqs.Options)) (*sqs.ListQueuesOutput, error) {
	f.listQueuesCalls++
	if f.listQueuesErr != nil {
		return nil, f.listQueuesErr
	}
	return &sqs.ListQueuesOutput{}, nil
}

func (f *fakeSQS) ReceiveMessage(context.Context, *sqs.ReceiveMessageInput, ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	return &sqs.ReceiveMessageOutput{}, nil
}

func (f *fakeSQS) DeleteMessage(context.Context, *sqs.DeleteMessageInput, ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	return &sqs.DeleteMessageOutput{}, nil
}

func (f *fakeSQS) ChangeMessageVisibility(context.Context, *sqs.ChangeMessageVisibilityInput, ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *fakeSQS) GetQueueUrl(context.Context, *sqs.GetQueueUrlInput, ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	return &sqs.GetQueueUrlOutput{}, nil
}

func (f *fakeSQS) SendMessage(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	return &sqs.SendMessageOutput{}, nil
}

func TestNewSQSHealthyPingReturnsClient(t *testing.T) {
	api := &fakeSQS{}

	got, err := client.NewSQSForTest(context.Background(), api)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatalf("client = nil, want non-nil")
	}
	if api.listQueuesCalls != 1 {
		t.Errorf("listQueuesCalls = %d, want 1", api.listQueuesCalls)
	}
}

func TestNewSQSFailingPingReturnsError(t *testing.T) {
	api := &fakeSQS{listQueuesErr: errPing}

	got, err := client.NewSQSForTest(context.Background(), api)
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

func TestNewSQSInvalidOptionReturnsError(t *testing.T) {
	api := &fakeSQS{}

	got, err := client.NewSQSForTest(context.Background(), api, client.WithPingTimeout(0))
	if got != nil {
		t.Errorf("client = %v, want nil", got)
	}
	if !stderrors.Is(err, errors.ErrInvalidOption) {
		t.Errorf("err = %v, want match ErrInvalidOption", err)
	}
	if api.listQueuesCalls != 0 {
		t.Errorf("listQueuesCalls = %d, want 0", api.listQueuesCalls)
	}
}

func TestNewSQSWithoutConnectivityCheckSkipsPing(t *testing.T) {
	api := &fakeSQS{listQueuesErr: errPing}

	got, err := client.NewSQSForTest(context.Background(), api, client.WithoutConnectivityCheck())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatalf("client = nil, want non-nil")
	}
	if api.listQueuesCalls != 0 {
		t.Errorf("listQueuesCalls = %d, want 0", api.listQueuesCalls)
	}
}

func TestNewSQSNeverNeither(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		pingFails := rapid.Bool().Draw(rt, "pingFails")
		invalidOption := rapid.Bool().Draw(rt, "invalidOption")
		skipCheck := rapid.Bool().Draw(rt, "skipCheck")

		api := &fakeSQS{}
		if pingFails {
			api.listQueuesErr = errPing
		}

		var opts []client.Option
		if invalidOption {
			opts = append(opts, client.WithPingTimeout(0))
		}
		if skipCheck {
			opts = append(opts, client.WithoutConnectivityCheck())
		}

		got, err := client.NewSQSForTest(context.Background(), api, opts...)

		if (got != nil) == (err != nil) {
			rt.Fatalf("client=%v err=%v, want exactly one non-nil", got, err)
		}
	})
}
