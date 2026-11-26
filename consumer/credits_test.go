package consumer_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/consumer"
)

func TestNewCreditPool(t *testing.T) {
	tests := []struct {
		name     string
		capacity int
		want     int
	}{
		{name: "negative capacity is raised to one", capacity: -3, want: 1},
		{name: "zero capacity is raised to one", capacity: 0, want: 1},
		{name: "capacity of one", capacity: 1, want: 1},
		{name: "larger capacity", capacity: 64, want: 64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := consumer.NewCreditPool(tt.capacity)

			assert.Equal(t, tt.want, p.Capacity())
			assert.Equal(t, tt.want, p.Available())
		})
	}
}

func TestNewCreditPoolStartsFull(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		capacity := rapid.IntRange(-1<<16, 256).Draw(t, "capacity")

		p := consumer.NewCreditPool(capacity)

		want := max(capacity, 1)
		assert.Equal(t, want, p.Capacity())
		assert.Equal(t, want, p.Available())
	})
}

func TestCreditPool_AcquireZeroCredits(t *testing.T) {
	p := consumer.NewCreditPool(3)

	require.NoError(t, p.Acquire(context.Background(), 0))
	assert.Equal(t, 3, p.Available())
}

func TestCreditPool_AcquireAndRelease(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		capacity := rapid.IntRange(1, 64).Draw(t, "capacity")
		n := rapid.IntRange(0, capacity).Draw(t, "n")
		p := consumer.NewCreditPool(capacity)

		require.NoError(t, p.Acquire(context.Background(), n))
		assert.Equal(t, capacity-n, p.Available())

		p.Release(n)
		assert.Equal(t, capacity, p.Available())
	})
}

type creditOpKind int

const (
	creditAcquire creditOpKind = iota
	creditRelease
	creditAcquireCanceled
	creditAcquireCanceledWhileBlocked
)

type creditOp struct {
	kind creditOpKind
	pick int
}

type collectedFailures struct {
	msgs []string
}

func (c *collectedFailures) Errorf(format string, args ...any) {
	c.msgs = append(c.msgs, fmt.Sprintf(format, args...))
}

func drawCreditOps(t *rapid.T) []creditOp {
	return rapid.SliceOfN(rapid.Custom(func(t *rapid.T) creditOp {
		return creditOp{
			kind: creditOpKind(rapid.IntRange(int(creditAcquire), int(creditAcquireCanceledWhileBlocked)).Draw(t, "kind")),
			pick: rapid.IntRange(0, 1<<20).Draw(t, "pick"),
		}
	}), 1, 60).Draw(t, "ops")
}

func runCreditOps(c *collectedFailures, capacity int, ops []creditOp) {
	p := consumer.NewCreditPool(capacity)
	held := 0

	for _, op := range ops {
		free := capacity - held
		switch op.kind {
		case creditAcquire:
			if free == 0 {
				continue
			}
			n := 1 + op.pick%free
			if !assert.NoError(c, p.Acquire(context.Background(), n)) {
				return
			}
			held += n
		case creditRelease:
			if held == 0 {
				continue
			}
			n := 1 + op.pick%held
			p.Release(n)
			held -= n
		case creditAcquireCanceled:
			n := 1 + op.pick%capacity
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := p.Acquire(ctx, n)
			if err == nil {
				if !assert.LessOrEqual(c, n, free) {
					return
				}
				held += n
				break
			}
			if !assert.ErrorIs(c, err, context.Canceled) || !assert.Equal(c, free, p.Available()) {
				return
			}
		case creditAcquireCanceledWhileBlocked:
			if free == capacity {
				continue
			}
			n := free + 1 + op.pick%(capacity-free)
			if !acquireCanceledWhileBlocked(c, p, n, free) {
				return
			}
		}

		if !assert.GreaterOrEqual(c, p.Available(), 0) ||
			!assert.LessOrEqual(c, p.Available(), capacity) ||
			!assert.Equal(c, capacity-held, p.Available()) {
			return
		}
	}

	p.Release(held)
	assert.Equal(c, capacity, p.Available())
}

func acquireCanceledWhileBlocked(c *collectedFailures, p *consumer.CreditPool, n, free int) bool {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)

	go func() { errc <- p.Acquire(ctx, n) }()
	synctest.Wait()
	blocked := assert.Empty(c, errc) && assert.Zero(c, p.Available())
	cancel()

	return assert.ErrorIs(c, <-errc, context.Canceled) &&
		assert.Equal(c, free, p.Available()) &&
		blocked
}

// Feature: consumer-flow-control, Property 2: For any capacity c >= 1 and any interleaving of serialized acquire(n <= c) and matching release calls, available() stays within [0, c], after every acquired credit is released available() == c, and a canceled acquire leaves available() unchanged.
func TestCreditPool_CreditConservation(t *testing.T) {
	defer goleak.VerifyNone(t)

	rapid.Check(t, func(rt *rapid.T) {
		capacity := rapid.IntRange(1, 64).Draw(rt, "capacity")
		ops := drawCreditOps(rt)
		failures := &collectedFailures{}

		synctest.Test(t, func(*testing.T) {
			runCreditOps(failures, capacity, ops)
		})

		if len(failures.msgs) > 0 {
			rt.Fatalf("%s", strings.Join(failures.msgs, "\n"))
		}
	})
}

func TestCreditPool_AcquireFullCapacityContention(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		name     string
		capacity int
	}{
		{name: "single credit", capacity: 1},
		{name: "two credits", capacity: 2},
		{name: "many credits", capacity: 64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := consumer.NewCreditPool(tt.capacity)
				errs := make(chan error, 2)

				for range 2 {
					go func() { errs <- p.Acquire(context.Background(), tt.capacity) }()
				}
				synctest.Wait()

				require.Len(t, errs, 1)
				require.NoError(t, <-errs)
				assert.Zero(t, p.Available())

				p.Release(tt.capacity)
				synctest.Wait()

				require.Len(t, errs, 1)
				require.NoError(t, <-errs)
				assert.Zero(t, p.Available())

				p.Release(tt.capacity)
				assert.Equal(t, tt.capacity, p.Available())
			})
		})
	}
}

func TestCreditPool_AcquireBlockedReturnsPartialCredits(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		newCtx  func() (context.Context, context.CancelFunc)
		wantErr error
		name    string
		cancel  bool
	}{
		{
			name: "canceled context",
			newCtx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			cancel:  true,
			wantErr: context.Canceled,
		},
		{
			name: "expired deadline",
			newCtx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), time.Minute)
			},
			wantErr: context.DeadlineExceeded,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const capacity, preHeld = 5, 2
				p := consumer.NewCreditPool(capacity)
				require.NoError(t, p.Acquire(context.Background(), preHeld))
				ctx, cancel := tt.newCtx()
				defer cancel()
				errc := make(chan error, 1)

				go func() { errc <- p.Acquire(ctx, capacity) }()
				synctest.Wait()

				require.Empty(t, errc)
				assert.Zero(t, p.Available())

				if tt.cancel {
					cancel()
				}
				err := <-errc

				require.ErrorIs(t, err, tt.wantErr)
				assert.Equal(t, capacity-preHeld, p.Available())

				p.Release(preHeld)
				assert.Equal(t, capacity, p.Available())
			})
		})
	}
}

func TestCreditPool_AcquireWaitingForTurnHonorsContext(t *testing.T) {
	defer goleak.VerifyNone(t)

	synctest.Test(t, func(t *testing.T) {
		const capacity = 4
		p := consumer.NewCreditPool(capacity)
		require.NoError(t, p.Acquire(context.Background(), 1))
		first := make(chan error, 1)

		go func() { first <- p.Acquire(context.Background(), capacity) }()
		synctest.Wait()
		require.Zero(t, p.Available())

		canceled, cancelNow := context.WithCancel(context.Background())
		cancelNow()
		require.ErrorIs(t, p.Acquire(canceled, 1), context.Canceled)
		assert.Zero(t, p.Available())

		waiting, cancelWaiting := context.WithCancel(context.Background())
		second := make(chan error, 1)
		go func() { second <- p.Acquire(waiting, 1) }()
		synctest.Wait()
		require.Empty(t, second)
		cancelWaiting()
		require.ErrorIs(t, <-second, context.Canceled)
		assert.Zero(t, p.Available())

		p.Release(1)
		require.NoError(t, <-first)
		assert.Zero(t, p.Available())

		p.Release(capacity)
		assert.Equal(t, capacity, p.Available())
	})
}
