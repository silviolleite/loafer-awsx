package consumer

import "log/slog"

// emitMetric invokes the given metric hook for the named route, guarding the
// call with a recover so a panicking hook can never abort the message outcome.
//
// The hook is any per-route metric incrementer (SuccessMetric, RetryMetric, or
// DeadLetterMetric, all of which are func(string)). When hook is nil the call
// is a no-op, so callers never have to guard against disabled metrics. If the
// hook panics, the panic is recovered and logged at Error level through the
// provided logger and then swallowed, allowing the message outcome (delete,
// schedule, or DLQ publish) to complete without retention or reprocessing.
func emitMetric(log *slog.Logger, hook func(routeName string), routeName string) {
	if hook == nil {
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

	hook(routeName)
}
