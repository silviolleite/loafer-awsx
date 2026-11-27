package consumer_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/consumer"
)

func newHandleMessages(n int) []*consumer.TestMessage {
	msgs := make([]*consumer.TestMessage, n)
	for i := range n {
		msgs[i] = consumer.NewTestMessage(fmt.Sprintf("m-%d", i), "", nil, "")
	}
	return msgs
}

func TestNewSharedQueue(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		name     string
		capacity int
	}{
		{name: "negative capacity is raised to one", capacity: -3},
		{name: "zero capacity is raised to one", capacity: 0},
		{name: "capacity of one", capacity: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				q := consumer.NewSharedQueue(tt.capacity)
				msg := consumer.NewTestMessage("only", "", nil, "")

				q.Submit(msg)
				got, ok := q.Take(context.Background())

				require.True(t, ok)
				assert.Same(t, msg, got)
			})
		})
	}
}

func TestSharedQueue_TakeInSubmissionOrder(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(1, 50).Draw(t, "n")
		msgs := newHandleMessages(n)
		q := consumer.NewSharedQueue(n)

		for _, m := range msgs {
			q.Submit(m)
		}

		for _, want := range msgs {
			got, ok := q.Take(context.Background())
			require.True(t, ok)
			assert.Same(t, want, got)
		}
		assert.Empty(t, q.Drain())
	})
}

func TestSharedQueue_Take(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		name   string
		submit bool
		want   bool
	}{
		{name: "blocked take returns the next submitted message", submit: true, want: true},
		{name: "blocked take returns false once the context is canceled", submit: false, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				q := consumer.NewSharedQueue(1)
				msg := consumer.NewTestMessage("h", "", nil, "")
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				type result struct {
					msg *consumer.TestMessage
					ok  bool
				}
				results := make(chan result, 1)

				go func() {
					got, ok := q.Take(ctx)
					results <- result{msg: got, ok: ok}
				}()
				synctest.Wait()
				require.Empty(t, results)

				if tt.submit {
					q.Submit(msg)
				} else {
					cancel()
				}
				got := <-results

				assert.Equal(t, tt.want, got.ok)
				if tt.want {
					assert.Same(t, msg, got.msg)
				} else {
					assert.Nil(t, got.msg)
				}
			})
		})
	}
}

func TestSharedQueue_TakeCanceledContext(t *testing.T) {
	q := consumer.NewSharedQueue(4)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got, ok := q.Take(ctx)

	assert.False(t, ok)
	assert.Nil(t, got)
}

func TestSharedQueue_Complete(t *testing.T) {
	tests := []struct {
		name   string
		failed bool
	}{
		{name: "success never holds", failed: false},
		{name: "failure never holds", failed: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := consumer.NewSharedQueue(4)
			msgs := newHandleMessages(3)
			for _, m := range msgs {
				q.Submit(m)
			}
			taken, ok := q.Take(context.Background())
			require.True(t, ok)

			assert.Nil(t, q.Complete(taken, tt.failed))
			assert.Equal(t, msgs[1:], q.Drain())
		})
	}
}

func TestSharedQueue_Drain(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(0, 50).Draw(t, "n")
		taken := rapid.IntRange(0, n).Draw(t, "taken")
		msgs := newHandleMessages(n)
		q := consumer.NewSharedQueue(max(n, 1))
		for _, m := range msgs {
			q.Submit(m)
		}
		for range taken {
			_, ok := q.Take(context.Background())
			require.True(t, ok)
		}

		drained := q.Drain()

		if taken == n {
			assert.Empty(t, drained)
		} else {
			assert.Equal(t, msgs[taken:], drained)
		}
		assert.Empty(t, q.Drain())
	})
}

type takeResult struct {
	msg *consumer.TestMessage
	ok  bool
}

func startTake(ctx context.Context, src *consumer.WorkSource) <-chan takeResult {
	results := make(chan takeResult, 1)
	go func() {
		msg, ok := src.Take(ctx)
		results <- takeResult{msg: msg, ok: ok}
	}()
	return results
}

func TestSharedQueue_TakeMarksIdleOnlyWhileBlocked(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		name     string
		queued   int
		cancel   bool
		wantIdle bool
	}{
		{name: "ready message is taken without becoming idle", queued: 1},
		{name: "empty queue marks the worker idle until a message arrives", wantIdle: true},
		{name: "empty queue marks the worker idle until canceled", wantIdle: true, cancel: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				idle := consumer.NewIdleWorkers()
				q := consumer.NewSharedQueueWithIdle(2, idle)
				msg := consumer.NewTestMessage("h", "", nil, "")
				for range tt.queued {
					q.Submit(msg)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				results := startTake(ctx, q)
				synctest.Wait()

				assert.Equal(t, tt.wantIdle, idle.IsAny())
				if tt.wantIdle {
					require.Empty(t, results)
					if tt.cancel {
						cancel()
					} else {
						q.Submit(msg)
					}
				}
				got := <-results

				assert.Equal(t, !tt.cancel, got.ok)
				assert.False(t, idle.IsAny())
			})
		})
	}
}

func TestSharedQueue_TakeWithNilIdle(t *testing.T) {
	defer goleak.VerifyNone(t)

	synctest.Test(t, func(t *testing.T) {
		q := consumer.NewSharedQueueWithIdle(1, nil)
		msg := consumer.NewTestMessage("h", "", nil, "")

		results := startTake(context.Background(), q)
		synctest.Wait()
		require.Empty(t, results)
		q.Submit(msg)
		got := <-results

		require.True(t, got.ok)
		assert.Same(t, msg, got.msg)
	})
}

func runSharedQueueIdle(c *collectedFailures, workers, queued int) {
	idle := consumer.NewIdleWorkers()
	q := consumer.NewSharedQueueWithIdle(max(workers, queued), idle)
	for _, m := range newHandleMessages(queued) {
		q.Submit(m)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	results := make([]<-chan takeResult, workers)
	for i := range workers {
		results[i] = startTake(ctx, q)
	}
	synctest.Wait()

	assert.Equal(c, workers > queued, idle.IsAny())

	cancel()
	taken := 0
	for _, r := range results {
		if (<-r).ok {
			taken++
		}
	}
	assert.Equal(c, min(workers, queued), taken)
	assert.False(c, idle.IsAny())
}

func TestSharedQueue_IdleIffMoreWorkersThanMessages(t *testing.T) {
	defer goleak.VerifyNone(t)

	rapid.Check(t, func(rt *rapid.T) {
		workers := rapid.IntRange(1, 16).Draw(rt, "workers")
		queued := rapid.IntRange(0, 16).Draw(rt, "queued")
		failures := &collectedFailures{}

		synctest.Test(t, func(*testing.T) {
			runSharedQueueIdle(failures, workers, queued)
		})

		if len(failures.msgs) > 0 {
			rt.Fatalf("%s", strings.Join(failures.msgs, "\n"))
		}
	})
}
