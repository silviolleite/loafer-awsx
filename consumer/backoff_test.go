package consumer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

// Feature: fifo-scheduled-retry, Property 1: Backoff is bounded, monotonic, and seeded by base
func TestComputeBackoffBoundedMonotonicSeeded(t *testing.T) {
	const maxGuardMs = 86_400_000

	rapid.Check(t, func(t *rapid.T) {
		baseMs := rapid.OneOf(
			rapid.Just(1),
			rapid.Just(maxGuardMs),
			rapid.IntRange(1, maxGuardMs),
		).Draw(t, "baseMs")
		maxMs := rapid.OneOf(
			rapid.Just(baseMs),
			rapid.Just(maxGuardMs),
			rapid.IntRange(baseMs, maxGuardMs),
		).Draw(t, "maxMs")

		base := time.Duration(baseMs) * time.Millisecond
		max := time.Duration(maxMs) * time.Millisecond

		i := rapid.IntRange(1, 1000).Draw(t, "i")
		j := rapid.IntRange(i, 1000).Draw(t, "j")

		bi := computeBackoff(i, base, max)
		bj := computeBackoff(j, base, max)

		assert.LessOrEqual(t, bi, bj)
		assert.GreaterOrEqual(t, bi, minBackoff)
		assert.LessOrEqual(t, bi, max)
		assert.GreaterOrEqual(t, bj, minBackoff)
		assert.LessOrEqual(t, bj, max)

		clamped := base
		if clamped < minBackoff {
			clamped = minBackoff
		}
		if clamped > max {
			clamped = max
		}
		assert.Equal(t, clamped, computeBackoff(1, base, max))
	})
}

// Feature: fifo-scheduled-retry, Property 6: Schedule invocation time equals now plus backoff
func TestScheduleAtEqualsNowPlusBackoff(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		nowUnix := rapid.Int64Range(0, 4_102_444_800).Draw(t, "nowUnix")
		nowNanos := rapid.Int64Range(0, int64(time.Second)-1).Draw(t, "nowNanos")
		now := time.Unix(nowUnix, nowNanos).UTC()

		backoffMs := rapid.Int64Range(1, 86_400_000).Draw(t, "backoffMs")
		backoff := time.Duration(backoffMs) * time.Millisecond

		got := scheduleAt(now, backoff)
		want := now.Add(backoff)

		diff := want.Sub(got)
		if diff < 0 {
			diff = -diff
		}
		assert.LessOrEqual(t, diff, time.Second)
	})
}
