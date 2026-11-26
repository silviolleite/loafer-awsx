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

type idleState int

const (
	idleNil idleState = iota
	idleNone
	idleOne
)

func newIdle(state idleState) *consumer.IdleWorkers {
	if state == idleNil {
		return nil
	}
	idle := consumer.NewIdleWorkers()
	if state == idleOne {
		idle.Enter()
	}
	return idle
}

type acquireResult struct {
	err error
	got int
}

func TestCreditPool_AcquireUpTo(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		name     string
		capacity int
		held     int
		n        int
		idle     idleState
		release  int
		want     int
	}{
		{name: "every credit free without idle tracker", capacity: 5, n: 3, idle: idleNil, want: 3},
		{name: "every credit free with no idle worker", capacity: 5, n: 5, idle: idleNone, want: 5},
		{name: "every credit free with an idle worker", capacity: 5, n: 4, idle: idleOne, want: 4},
		{name: "exactly n free with an idle worker", capacity: 5, held: 2, n: 3, idle: idleOne, want: 3},
		{name: "fewer free with an idle worker returns what is free", capacity: 5, held: 3, n: 4, idle: idleOne, want: 2},
		{name: "single free credit with an idle worker", capacity: 10, held: 9, n: 10, idle: idleOne, want: 1},
		{name: "fewer free without idle tracker waits for n", capacity: 5, held: 3, n: 4, idle: idleNil, release: 2, want: 4},
		{name: "fewer free with no idle worker waits for n", capacity: 5, held: 3, n: 4, idle: idleNone, release: 2, want: 4},
		{name: "nothing free with an idle worker returns the first released", capacity: 3, held: 3, n: 3, idle: idleOne, release: 1, want: 1},
		{name: "nothing free without idle tracker waits for n", capacity: 3, held: 3, n: 2, idle: idleNil, release: 3, want: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := consumer.NewCreditPool(tt.capacity)
				require.NoError(t, p.Acquire(context.Background(), tt.held))
				idle := newIdle(tt.idle)
				results := make(chan acquireResult, 1)

				go func() {
					got, err := p.AcquireUpTo(context.Background(), tt.n, idle)
					results <- acquireResult{got: got, err: err}
				}()
				synctest.Wait()

				if tt.release > 0 {
					require.Empty(t, results)
					assert.Zero(t, p.Available())
					p.Release(tt.release)
				}
				res := <-results

				require.NoError(t, res.err)
				assert.Equal(t, tt.want, res.got)
				assert.Equal(t, tt.capacity-tt.held+tt.release-tt.want, p.Available())

				p.Release(tt.held - tt.release + tt.want)
				assert.Equal(t, tt.capacity, p.Available())
			})
		})
	}
}

func TestCreditPool_AcquireUpToIdleWorkerWakesBlockedAcquire(t *testing.T) {
	defer goleak.VerifyNone(t)

	synctest.Test(t, func(t *testing.T) {
		const capacity, held = 4, 2
		p := consumer.NewCreditPool(capacity)
		require.NoError(t, p.Acquire(context.Background(), held))
		idle := consumer.NewIdleWorkers()
		results := make(chan acquireResult, 1)

		go func() {
			got, err := p.AcquireUpTo(context.Background(), capacity, idle)
			results <- acquireResult{got: got, err: err}
		}()
		synctest.Wait()
		require.Empty(t, results)
		require.Zero(t, p.Available())

		idle.Enter()
		res := <-results

		require.NoError(t, res.err)
		assert.Equal(t, capacity-held, res.got)
		assert.Zero(t, p.Available())

		p.Release(capacity)
		assert.Equal(t, capacity, p.Available())
	})
}

func TestCreditPool_AcquireUpToStaleWakeKeepsWaiting(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		name string
		held int
	}{
		{name: "stale wake while holding part of the credits", held: 2},
		{name: "stale wake while holding no credit", held: 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const capacity = 4
				p := consumer.NewCreditPool(capacity)
				require.NoError(t, p.Acquire(context.Background(), tt.held))
				idle := consumer.NewIdleWorkers()
				idle.Enter()
				idle.Leave()
				require.False(t, idle.IsAny())
				results := make(chan acquireResult, 1)

				go func() {
					got, err := p.AcquireUpTo(context.Background(), capacity, idle)
					results <- acquireResult{got: got, err: err}
				}()
				synctest.Wait()
				require.Empty(t, results)
				assert.Zero(t, p.Available())

				p.Release(tt.held)
				res := <-results

				require.NoError(t, res.err)
				assert.Equal(t, capacity, res.got)
				assert.Zero(t, p.Available())

				p.Release(capacity)
				assert.Equal(t, capacity, p.Available())
			})
		})
	}
}

func TestCreditPool_AcquireUpToCanceled(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		name      string
		idle      idleState
		held      int
		preCancel bool
	}{
		{name: "already canceled context takes nothing", idle: idleOne, preCancel: true},
		{name: "already canceled context with nil idle takes nothing", idle: idleNil, held: 1, preCancel: true},
		{name: "canceled while holding part of the credits returns them", idle: idleNone, held: 2},
		{name: "canceled while holding part of the credits with nil idle returns them", idle: idleNil, held: 3},
		{name: "canceled while holding nothing with an idle worker", idle: idleOne, held: 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const capacity = 4
				p := consumer.NewCreditPool(capacity)
				require.NoError(t, p.Acquire(context.Background(), tt.held))
				idle := newIdle(tt.idle)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if tt.preCancel {
					cancel()
				}
				results := make(chan acquireResult, 1)

				go func() {
					got, err := p.AcquireUpTo(ctx, capacity, idle)
					results <- acquireResult{got: got, err: err}
				}()
				synctest.Wait()

				if !tt.preCancel {
					require.Empty(t, results)
					assert.Zero(t, p.Available())
					cancel()
				}
				res := <-results

				require.ErrorIs(t, res.err, context.Canceled)
				assert.Zero(t, res.got)
				assert.Equal(t, capacity-tt.held, p.Available())

				p.Release(tt.held)
				assert.Equal(t, capacity, p.Available())
			})
		})
	}
}

func TestCreditPool_AcquireUpToWaitingForTurnHonorsContext(t *testing.T) {
	defer goleak.VerifyNone(t)

	synctest.Test(t, func(t *testing.T) {
		const capacity = 3
		p := consumer.NewCreditPool(capacity)
		require.NoError(t, p.Acquire(context.Background(), 1))
		first := make(chan error, 1)
		go func() { first <- p.Acquire(context.Background(), capacity) }()
		synctest.Wait()
		require.Zero(t, p.Available())

		idle := consumer.NewIdleWorkers()
		idle.Enter()
		ctx, cancel := context.WithCancel(context.Background())
		results := make(chan acquireResult, 1)
		go func() {
			got, err := p.AcquireUpTo(ctx, 1, idle)
			results <- acquireResult{got: got, err: err}
		}()
		synctest.Wait()
		require.Empty(t, results)

		cancel()
		res := <-results

		require.ErrorIs(t, res.err, context.Canceled)
		assert.Zero(t, res.got)

		p.Release(1)
		require.NoError(t, <-first)
		p.Release(capacity)
		assert.Equal(t, capacity, p.Available())
	})
}

func runAcquireUpTo(c *collectedFailures, capacity, held, n int, state idleState) {
	p := consumer.NewCreditPool(capacity)
	if !assert.NoError(c, p.Acquire(context.Background(), held)) {
		return
	}
	idle := newIdle(state)
	free := capacity - held
	results := make(chan acquireResult, 1)

	go func() {
		got, err := p.AcquireUpTo(context.Background(), n, idle)
		results <- acquireResult{got: got, err: err}
	}()
	synctest.Wait()

	want, released := n, 0
	switch {
	case free >= n:
	case state == idleOne && free > 0:
		want = free
	case state == idleOne:
		want, released = 1, 1
	default:
		released = n - free
	}

	if released > 0 {
		if !assert.Empty(c, results) || !assert.Zero(c, p.Available()) {
			p.Release(capacity)
			<-results
			return
		}
		p.Release(released)
	}
	res := <-results

	if !assert.NoError(c, res.err) ||
		!assert.Equal(c, want, res.got) ||
		!assert.Equal(c, free+released-want, p.Available()) {
		return
	}
	p.Release(held - released + res.got)
	assert.Equal(c, capacity, p.Available())
}

func TestCreditPool_AcquireUpToTakesFreeCreditsProperty(t *testing.T) {
	defer goleak.VerifyNone(t)

	rapid.Check(t, func(rt *rapid.T) {
		capacity := rapid.IntRange(1, 64).Draw(rt, "capacity")
		held := rapid.IntRange(0, capacity).Draw(rt, "held")
		n := rapid.IntRange(1, capacity).Draw(rt, "n")
		state := idleState(rapid.IntRange(int(idleNil), int(idleOne)).Draw(rt, "idle"))
		failures := &collectedFailures{}

		synctest.Test(t, func(*testing.T) {
			runAcquireUpTo(failures, capacity, held, n, state)
		})

		if len(failures.msgs) > 0 {
			rt.Fatalf("%s", strings.Join(failures.msgs, "\n"))
		}
	})
}

func TestIdleWorkers_NilReceiver(t *testing.T) {
	var idle *consumer.IdleWorkers

	assert.NotPanics(t, func() {
		idle.Enter()
		idle.Leave()
	})
	assert.False(t, idle.IsAny())
}

func TestIdleWorkers_NewHasNoIdleWorker(t *testing.T) {
	assert.False(t, consumer.NewIdleWorkers().IsAny())
}

func TestIdleWorkers_Counting(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		ops := rapid.SliceOfN(rapid.Bool(), 0, 100).Draw(rt, "enter")
		idle := consumer.NewIdleWorkers()
		count := 0

		for _, enter := range ops {
			if enter {
				idle.Enter()
				count++
			} else {
				idle.Leave()
				count--
			}
			assert.Equal(rt, count > 0, idle.IsAny())
		}
	})
}

func TestIdleWorkers_ConcurrentEnterLeave(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		name    string
		workers int
	}{
		{name: "one worker", workers: 1},
		{name: "many workers", workers: 64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idle := consumer.NewIdleWorkers()
			entered := make(chan struct{}, tt.workers)
			leave := make(chan struct{})
			done := make(chan struct{}, tt.workers)

			for range tt.workers {
				go func() {
					idle.Enter()
					entered <- struct{}{}
					<-leave
					idle.Leave()
					done <- struct{}{}
				}()
			}
			for range tt.workers {
				<-entered
			}
			assert.True(t, idle.IsAny())

			close(leave)
			for range tt.workers {
				<-done
			}
			assert.False(t, idle.IsAny())
		})
	}
}
