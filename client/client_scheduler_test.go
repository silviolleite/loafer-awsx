package client_test

import (
	"context"
	stderrors "errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/client"
	"github.com/silviolleite/loafer-awsx/errors"
)

type fakeScheduler struct {
	listSchedulesErr   error
	listSchedulesCalls int
}

func (f *fakeScheduler) ListSchedules(context.Context, *scheduler.ListSchedulesInput, ...func(*scheduler.Options)) (*scheduler.ListSchedulesOutput, error) {
	f.listSchedulesCalls++
	if f.listSchedulesErr != nil {
		return nil, f.listSchedulesErr
	}
	return &scheduler.ListSchedulesOutput{}, nil
}

func (f *fakeScheduler) CreateSchedule(context.Context, *scheduler.CreateScheduleInput, ...func(*scheduler.Options)) (*scheduler.CreateScheduleOutput, error) {
	return &scheduler.CreateScheduleOutput{}, nil
}

func TestNewSchedulerHealthyPingReturnsClient(t *testing.T) {
	api := &fakeScheduler{}

	got, err := client.NewSchedulerForTest(context.Background(), api)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatalf("client = nil, want non-nil")
	}
	if api.listSchedulesCalls != 1 {
		t.Errorf("listSchedulesCalls = %d, want 1", api.listSchedulesCalls)
	}
}

func TestNewSchedulerFailingPingReturnsError(t *testing.T) {
	api := &fakeScheduler{listSchedulesErr: errPing}

	got, err := client.NewSchedulerForTest(context.Background(), api)
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

func TestNewSchedulerInvalidOptionReturnsError(t *testing.T) {
	api := &fakeScheduler{}

	got, err := client.NewSchedulerForTest(context.Background(), api, client.WithPingTimeout(0))
	if got != nil {
		t.Errorf("client = %v, want nil", got)
	}
	if !stderrors.Is(err, errors.ErrInvalidOption) {
		t.Errorf("err = %v, want match ErrInvalidOption", err)
	}
	if api.listSchedulesCalls != 0 {
		t.Errorf("listSchedulesCalls = %d, want 0", api.listSchedulesCalls)
	}
}

func TestNewSchedulerWithoutConnectivityCheckSkipsPing(t *testing.T) {
	api := &fakeScheduler{listSchedulesErr: errPing}

	got, err := client.NewSchedulerForTest(context.Background(), api, client.WithoutConnectivityCheck())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatalf("client = nil, want non-nil")
	}
	if api.listSchedulesCalls != 0 {
		t.Errorf("listSchedulesCalls = %d, want 0", api.listSchedulesCalls)
	}
}

func TestNewSchedulerNeverNeither(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		pingFails := rapid.Bool().Draw(rt, "pingFails")
		invalidOption := rapid.Bool().Draw(rt, "invalidOption")
		skipCheck := rapid.Bool().Draw(rt, "skipCheck")

		api := &fakeScheduler{}
		if pingFails {
			api.listSchedulesErr = errPing
		}

		var opts []client.Option
		if invalidOption {
			opts = append(opts, client.WithPingTimeout(0))
		}
		if skipCheck {
			opts = append(opts, client.WithoutConnectivityCheck())
		}

		got, err := client.NewSchedulerForTest(context.Background(), api, opts...)

		if (got != nil) == (err != nil) {
			rt.Fatalf("client=%v err=%v, want exactly one non-nil", got, err)
		}
	})
}
