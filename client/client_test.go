package client_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.uber.org/goleak"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/client"
	"github.com/silviolleite/loafer-awsx/consumer"
	"github.com/silviolleite/loafer-awsx/producer"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type budgetSQS struct {
	*fakeSQS
	failN uint
	calls uint
}

func (b *budgetSQS) ListQueues(context.Context, *sqs.ListQueuesInput, ...func(*sqs.Options)) (*sqs.ListQueuesOutput, error) {
	b.calls++
	if b.calls <= b.failN {
		return nil, errPing
	}
	return &sqs.ListQueuesOutput{}, nil
}

type budgetSNS struct {
	*fakeSNS
	failN uint
	calls uint
}

func (b *budgetSNS) ListTopics(context.Context, *sns.ListTopicsInput, ...func(*sns.Options)) (*sns.ListTopicsOutput, error) {
	b.calls++
	if b.calls <= b.failN {
		return nil, errPing
	}
	return &sns.ListTopicsOutput{}, nil
}

type budgetScheduler struct {
	*fakeScheduler
	failN uint
	calls uint
}

func (b *budgetScheduler) ListSchedules(context.Context, *scheduler.ListSchedulesInput, ...func(*scheduler.Options)) (*scheduler.ListSchedulesOutput, error) {
	b.calls++
	if b.calls <= b.failN {
		return nil, errPing
	}
	return &scheduler.ListSchedulesOutput{}, nil
}

func TestConstructorsReturnInterfaceTypes(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		skipCheck := rapid.Bool().Draw(rt, "skipCheck")
		retryLimit := rapid.UintRange(0, 5).Draw(rt, "retryLimit")

		opts := []client.Option{client.WithPingRetryLimit(retryLimit)}
		if skipCheck {
			opts = append(opts, client.WithoutConnectivityCheck())
		}

		var sqsClient consumer.SQSClient
		sqsClient, sqsErr := client.NewSQSForTest(context.Background(), &fakeSQS{}, opts...)
		if sqsErr != nil {
			rt.Fatalf("NewSQS error = %v, want nil", sqsErr)
		}
		if sqsClient == nil {
			rt.Fatalf("NewSQS client = nil, want non-nil consumer.SQSClient")
		}

		var snsClient producer.SNSClient
		snsClient, snsErr := client.NewSNSForTest(context.Background(), &fakeSNS{}, opts...)
		if snsErr != nil {
			rt.Fatalf("NewSNS error = %v, want nil", snsErr)
		}
		if snsClient == nil {
			rt.Fatalf("NewSNS client = nil, want non-nil producer.SNSClient")
		}

		var schedulerClient consumer.SchedulerClient
		schedulerClient, schedErr := client.NewSchedulerForTest(context.Background(), &fakeScheduler{}, opts...)
		if schedErr != nil {
			rt.Fatalf("NewScheduler error = %v, want nil", schedErr)
		}
		if schedulerClient == nil {
			rt.Fatalf("NewScheduler client = nil, want non-nil consumer.SchedulerClient")
		}
	})
}

func TestPublicConstructorsReturnInterfaceTypes(t *testing.T) {
	var cfg aws.Config

	var sqsClient consumer.SQSClient
	sqsClient, err := client.NewSQS(context.Background(), cfg, client.WithoutConnectivityCheck())
	if err != nil {
		t.Fatalf("NewSQS error = %v, want nil", err)
	}
	if sqsClient == nil {
		t.Fatalf("NewSQS client = nil, want non-nil consumer.SQSClient")
	}

	var snsClient producer.SNSClient
	snsClient, err = client.NewSNS(context.Background(), cfg, client.WithoutConnectivityCheck())
	if err != nil {
		t.Fatalf("NewSNS error = %v, want nil", err)
	}
	if snsClient == nil {
		t.Fatalf("NewSNS client = nil, want non-nil producer.SNSClient")
	}

	var schedulerClient consumer.SchedulerClient
	schedulerClient, err = client.NewScheduler(context.Background(), cfg, client.WithoutConnectivityCheck())
	if err != nil {
		t.Fatalf("NewScheduler error = %v, want nil", err)
	}
	if schedulerClient == nil {
		t.Fatalf("NewScheduler client = nil, want non-nil consumer.SchedulerClient")
	}
}

func TestConstructorsRetryBudgetIndependentOfConfigRetryer(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		k := rapid.UintRange(0, 10).Draw(rt, "failures")
		retryLimit := rapid.UintRange(0, 10).Draw(rt, "retryLimit")
		wantCalls := min(k+1, retryLimit+1)

		sqsAPI := &budgetSQS{fakeSQS: &fakeSQS{}, failN: k}
		if _, err := client.NewSQSForTest(context.Background(), sqsAPI, client.WithPingRetryLimit(retryLimit)); (err == nil) != (k <= retryLimit) {
			rt.Fatalf("NewSQS err = %v, k=%d retryLimit=%d", err, k, retryLimit)
		}
		if sqsAPI.calls != wantCalls {
			rt.Fatalf("SQS calls = %d, want %d (k=%d, retryLimit=%d)", sqsAPI.calls, wantCalls, k, retryLimit)
		}

		snsAPI := &budgetSNS{fakeSNS: &fakeSNS{}, failN: k}
		if _, err := client.NewSNSForTest(context.Background(), snsAPI, client.WithPingRetryLimit(retryLimit)); (err == nil) != (k <= retryLimit) {
			rt.Fatalf("NewSNS err = %v, k=%d retryLimit=%d", err, k, retryLimit)
		}
		if snsAPI.calls != wantCalls {
			rt.Fatalf("SNS calls = %d, want %d (k=%d, retryLimit=%d)", snsAPI.calls, wantCalls, k, retryLimit)
		}

		schedAPI := &budgetScheduler{fakeScheduler: &fakeScheduler{}, failN: k}
		if _, err := client.NewSchedulerForTest(context.Background(), schedAPI, client.WithPingRetryLimit(retryLimit)); (err == nil) != (k <= retryLimit) {
			rt.Fatalf("NewScheduler err = %v, k=%d retryLimit=%d", err, k, retryLimit)
		}
		if schedAPI.calls != wantCalls {
			rt.Fatalf("Scheduler calls = %d, want %d (k=%d, retryLimit=%d)", schedAPI.calls, wantCalls, k, retryLimit)
		}
	})
}
