package middleware

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"

	"github.com/silviolleite/loafer-awsx/errors"
)

// Recovery returns a Middleware that guards a Handler against panics. When the
// wrapped handler panics, the recovered value and the stack trace captured via
// runtime/debug.Stack are logged at Error level through the supplied logger and
// the panic is converted into an error rather than being allowed to crash the
// process.
//
// The returned error wraps errors.ErrPanic together with the recovered value,
// so callers can both read the panic value from the error message and match it
// with errors.Is against errors.ErrPanic. Handlers that return normally are
// passed through unchanged, preserving their error (including nil on success).
func Recovery(log *slog.Logger) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, msg Message) (err error) {
			defer func() {
				if r := recover(); r != nil {
					stack := debug.Stack()
					attrs := []any{
						"panic", r,
						"stack", string(stack),
					}
					if msg != nil {
						attrs = append(attrs, logKeyMessageID, msg.Identifier())
					}
					log.Error("panic recovered", attrs...)
					err = errors.Wrap(errors.ErrPanic, fmt.Errorf("%v", r))
				}
			}()

			return next(ctx, msg)
		}
	}
}
