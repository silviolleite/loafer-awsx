package consumer_test

import (
	"context"
	"fmt"
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
