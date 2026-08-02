package client_test

import (
	stderrors "errors"
	"testing"
	"time"

	"context"

	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/client"
	"github.com/silviolleite/loafer-awsx/errors"
)

var errPing = stderrors.New("ping boom")

func TestValidateRetryBudgetBound(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		k := rapid.UintRange(0, 20).Draw(rt, "failures")
		retryLimit := rapid.UintRange(0, 20).Draw(rt, "retryLimit")

		var calls uint
		do := func(context.Context) error {
			calls++
			if calls <= k {
				return errPing
			}
			return nil
		}

		err := client.Validate(context.Background(), time.Hour, retryLimit, do)

		wantCalls := min(k+1, retryLimit+1)
		if calls != wantCalls {
			rt.Fatalf("calls = %d, want %d (k=%d, retryLimit=%d)", calls, wantCalls, k, retryLimit)
		}

		if k <= retryLimit {
			if err != nil {
				rt.Fatalf("err = %v, want nil (k=%d, retryLimit=%d)", err, k, retryLimit)
			}
			return
		}
		if !stderrors.Is(err, errors.ErrPingFailed) {
			rt.Fatalf("err = %v, want match ErrPingFailed", err)
		}
		if !stderrors.Is(err, errPing) {
			rt.Fatalf("err = %v, want unwrap to last cause", err)
		}
	})
}

func TestValidateSuccessStopsFurtherAttempts(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		retryLimit := rapid.UintRange(0, 20).Draw(rt, "retryLimit")
		k := rapid.UintRange(0, retryLimit).Draw(rt, "failuresWithinBudget")

		var calls uint
		do := func(context.Context) error {
			calls++
			if calls <= k {
				return errPing
			}
			return nil
		}

		err := client.Validate(context.Background(), time.Hour, retryLimit, do)
		if err != nil {
			rt.Fatalf("err = %v, want nil", err)
		}
		if calls != k+1 {
			rt.Fatalf("calls = %d, want %d", calls, k+1)
		}
	})
}

func TestValidateFailureWrapsErrPingFailed(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		retryLimit := rapid.UintRange(0, 20).Draw(rt, "retryLimit")

		var calls uint
		do := func(context.Context) error {
			calls++
			return errPing
		}

		err := client.Validate(context.Background(), time.Hour, retryLimit, do)
		if calls != retryLimit+1 {
			rt.Fatalf("calls = %d, want %d", calls, retryLimit+1)
		}
		if !stderrors.Is(err, errors.ErrPingFailed) {
			rt.Fatalf("err = %v, want match ErrPingFailed", err)
		}
		if !stderrors.Is(err, errPing) {
			rt.Fatalf("err = %v, want unwrap to underlying cause", err)
		}
	})
}

func TestValidateCanceledContextSchedulesNoAttempts(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		retryLimit := rapid.UintRange(0, 20).Draw(rt, "retryLimit")

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		var calls uint
		do := func(context.Context) error {
			calls++
			return nil
		}

		err := client.Validate(ctx, time.Hour, retryLimit, do)
		if calls != 0 {
			rt.Fatalf("calls = %d, want 0", calls)
		}
		if !stderrors.Is(err, errors.ErrPingFailed) {
			rt.Fatalf("err = %v, want match ErrPingFailed", err)
		}
		if !stderrors.Is(err, context.Canceled) {
			rt.Fatalf("err = %v, want match context.Canceled", err)
		}
	})
}

func TestValidateInFlightRequestNotAborted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var doErr error
	do := func(reqCtx context.Context) error {
		cancel()
		doErr = reqCtx.Err()
		return nil
	}

	err := client.Validate(ctx, time.Hour, 2, do)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if doErr != nil {
		t.Fatalf("in-flight request context error = %v, want nil", doErr)
	}
}
