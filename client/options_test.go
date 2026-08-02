package client_test

import (
	"testing"
	"time"

	stderrors "errors"

	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/client"
	"github.com/silviolleite/loafer-awsx/errors"
)

func TestBuildOptionsDefaults(t *testing.T) {
	view, err := client.BuildOptions()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if view.PingTimeout != 3*time.Second {
		t.Errorf("PingTimeout = %s, want 3s", view.PingTimeout)
	}
	if view.PingRetryLimit != 2 {
		t.Errorf("PingRetryLimit = %d, want 2", view.PingRetryLimit)
	}
	if !view.ConnectivityCheck {
		t.Errorf("ConnectivityCheck = false, want true")
	}
}

func TestBuildOptionsAppliesInOrder(t *testing.T) {
	var order []int
	opts := []client.Option{
		client.RecordingOption(func() { order = append(order, 1) }),
		client.RecordingOption(func() { order = append(order, 2) }),
		client.RecordingOption(func() { order = append(order, 3) }),
	}
	if _, err := client.BuildOptions(opts...); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []int{1, 2, 3}
	if len(order) != len(want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
}

func TestBuildOptionsSkipsNilOption(t *testing.T) {
	var applied bool
	view, err := client.BuildOptions(
		nil,
		client.RecordingOption(func() { applied = true }),
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !applied {
		t.Errorf("non-nil option was not applied")
	}
	if view == nil {
		t.Fatalf("view is nil")
	}
}

func TestWithPingRetryLimitOverridesDefault(t *testing.T) {
	view, err := client.BuildOptions(client.WithPingRetryLimit(7))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if view.PingRetryLimit != 7 {
		t.Errorf("PingRetryLimit = %d, want 7", view.PingRetryLimit)
	}
}

func TestWithoutConnectivityCheckDisablesValidation(t *testing.T) {
	view, err := client.BuildOptions(client.WithoutConnectivityCheck())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if view.ConnectivityCheck {
		t.Errorf("ConnectivityCheck = true, want false")
	}
}

func TestWithPingTimeoutRejectsNonPositive(t *testing.T) {
	tests := []struct {
		name string
		d    time.Duration
	}{
		{name: "zero", d: 0},
		{name: "negative", d: -time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			view, err := client.BuildOptions(client.WithPingTimeout(tt.d))
			if view != nil {
				t.Errorf("view = %+v, want nil", view)
			}
			if !stderrors.Is(err, errors.ErrInvalidOption) {
				t.Errorf("err = %v, want match ErrInvalidOption", err)
			}
		})
	}
}

func TestWithPingTimeoutAcceptsPositive(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		ns := rapid.Int64Range(1, int64(time.Hour)).Draw(rt, "timeoutNanos")
		d := time.Duration(ns)

		view, err := client.BuildOptions(client.WithPingTimeout(d))
		if err != nil {
			rt.Fatalf("unexpected error for d=%s: %v", d, err)
		}
		if view.PingTimeout != d {
			rt.Errorf("PingTimeout = %s, want %s", view.PingTimeout, d)
		}
	})
}

func TestWithPingTimeoutRejectsNonPositiveProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		ns := rapid.Int64Range(int64(-time.Hour), 0).Draw(rt, "timeoutNanos")
		d := time.Duration(ns)

		view, err := client.BuildOptions(client.WithPingTimeout(d))
		if view != nil {
			rt.Errorf("view = %+v, want nil for d=%s", view, d)
		}
		if !stderrors.Is(err, errors.ErrInvalidOption) {
			rt.Errorf("err = %v, want match ErrInvalidOption for d=%s", err, d)
		}
	})
}

func TestWithPingRetryLimitApplied(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.Uint32().Draw(rt, "retryLimit")

		view, err := client.BuildOptions(client.WithPingRetryLimit(uint(n)))
		if err != nil {
			rt.Fatalf("unexpected error for n=%d: %v", n, err)
		}
		if view.PingRetryLimit != uint(n) {
			rt.Errorf("PingRetryLimit = %d, want %d", view.PingRetryLimit, n)
		}
	})
}
