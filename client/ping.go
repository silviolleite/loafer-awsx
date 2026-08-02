package client

import (
	"context"

	"github.com/silviolleite/loafer-awsx/errors"
)

// validate executes do until it succeeds or the retry budget is exhausted,
// bounding the total duration by cfg.timeout. It stops scheduling new attempts
// when ctx is canceled but lets an in-flight request finish, because do runs on
// a context detached from ctx cancellation yet still bounded by the timeout.
func validate(ctx context.Context, cfg pingConfig, do func(context.Context) error) error {
	budgetCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.timeout)
	defer cancel()

	var lastErr error
	for attempt := uint(0); attempt <= cfg.retryLimit; attempt++ {
		if err := ctx.Err(); err != nil {
			return errors.Wrap(errors.ErrPingFailed, err)
		}
		err := do(budgetCtx)
		if err == nil {
			return nil
		}
		lastErr = err
	}
	return errors.Wrap(errors.ErrPingFailed, lastErr)
}
