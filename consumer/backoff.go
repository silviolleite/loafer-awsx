package consumer

import "time"

// minBackoff is the smallest backoff delay the Scheduled Retry model will ever
// use. Every computed delay is clamped up to this bound so a retry is never
// scheduled with a zero or negative delay.
const minBackoff = time.Millisecond

// computeBackoff returns the delay for the given retry attempt, where attempt 1
// is the first retry. The delay grows exponentially as base * 2^(attempt-1),
// is clamped to the inclusive range [1ms, maxDelay], and is monotonically
// non-decreasing in attempt.
//
// The exponential growth is computed by repeated doubling rather than a single
// shift, and the running value is compared against maxDelay before each
// doubling, so the computation returns maxDelay as soon as the next step would
// exceed it. This keeps the result bounded and avoids any integer overflow
// regardless of how large attempt is.
//
// Inputs are normalized defensively: base and maxDelay below 1ms are raised to
// 1ms, a base greater than maxDelay is lowered to maxDelay, and an attempt below
// 1 is treated as the first retry.
func computeBackoff(attempt int, base, maxDelay time.Duration) time.Duration {
	if maxDelay < minBackoff {
		maxDelay = minBackoff
	}
	if base < minBackoff {
		base = minBackoff
	}
	if base > maxDelay {
		base = maxDelay
	}
	if attempt < 1 {
		attempt = 1
	}

	delay := base
	for i := 1; i < attempt; i++ {
		// Check against maxDelay before doubling to keep the value bounded and
		// to avoid overflow: if doubling would meet or exceed maxDelay, return it.
		if delay > maxDelay/2 {
			return maxDelay
		}
		delay *= 2
	}

	if delay > maxDelay {
		return maxDelay
	}
	if delay < minBackoff {
		return minBackoff
	}

	return delay
}

// scheduleAt returns the one-time schedule fire time as now plus backoff,
// truncated to whole seconds to match the granularity of the EventBridge
// Scheduler at() expression. The truncation keeps the configured invocation
// time within one second of now + backoff.
func scheduleAt(now time.Time, backoff time.Duration) time.Time {
	return now.Add(backoff).Truncate(time.Second)
}
