package consumer

import "log/slog"

// MetricsRecorder receives per-route outcome counters emitted by the consumer.
// Under the Scheduled Retry model the consumer invokes IncSuccess when a handler
// succeeds and the original message is deleted, IncRetry when a retry schedule
// is created successfully, and IncDeadLetter when an exhausted message is
// published to the DLQ successfully. Under the observe-only DLQ path the
// consumer invokes IncDLQ once for every message detected as exhausted.
//
// Implementations are typically backed by the counters registered by the
// Metrics middleware and are wired through WithMetrics. A nil recorder disables
// metric reporting so callers never have to guard against nil. All methods must
// be safe for concurrent use and must not panic; a panicking method is recovered
// and logged so the message outcome always completes.
type MetricsRecorder interface {
	// IncDLQ increments the observe-only dead-letter counter for the route.
	IncDLQ(routeName string)
	// IncSuccess increments the success counter for the route.
	IncSuccess(routeName string)
	// IncRetry increments the scheduled-retry counter for the route.
	IncRetry(routeName string)
	// IncDeadLetter increments the dead-letter-publish counter for the route.
	IncDeadLetter(routeName string)
}

// emitMetric invokes inc for the named route, guarding the call with a recover
// so a panicking recorder can never abort the message outcome.
//
// When inc is nil the call is a no-op, so callers never have to guard against
// disabled metrics. If inc panics, the panic is recovered and logged at Error
// level through the provided logger and then swallowed, allowing the message
// outcome (delete, schedule, or DLQ publish) to complete without retention or
// reprocessing.
func emitMetric(log *slog.Logger, inc func(routeName string), routeName string) {
	if inc == nil {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			log.Error("metric hook panicked; recovered and continuing",
				slog.String("route_name", routeName),
				slog.Any("panic", r),
			)
		}
	}()

	inc(routeName)
}
