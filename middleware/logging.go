package middleware

import (
	"context"
	"log/slog"
	"time"
)

// Log attribute keys shared by the logging and recovery middlewares.
const (
	logKeyMessageID = "message_id"
	logKeyDuration  = "duration"
	logKeyError     = "error"
)

// Logging returns a Middleware that records the lifecycle of message
// processing through the supplied logger.
//
// For every message it logs three key moments:
//   - Receipt is logged at Info level before the wrapped handler runs, carrying
//     the message identifier under the "message_id" key.
//   - Successful completion is logged at Info level with the processing
//     duration under the "duration" key.
//   - Failure is logged at Error level with both the processing duration and
//     the returned error under the "duration" and "error" keys.
//
// The error returned by the wrapped handler is always propagated unchanged, so
// the middleware is transparent to callers. A nil message is tolerated: in that
// case the "message_id" field is omitted from the emitted records.
func Logging(log *slog.Logger) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, msg Message) error {
			var idAttrs []any
			if msg != nil {
				idAttrs = append(idAttrs, logKeyMessageID, msg.Identifier())
			}

			log.Info("message received", idAttrs...)

			start := time.Now()
			err := next(ctx, msg)
			duration := time.Since(start)

			if err != nil {
				attrs := append([]any{logKeyDuration, duration, logKeyError, err}, idAttrs...)
				log.Error("message processing failed", attrs...)
				return err
			}

			attrs := append([]any{logKeyDuration, duration}, idAttrs...)
			log.Info("message processed", attrs...)
			return err
		}
	}
}
