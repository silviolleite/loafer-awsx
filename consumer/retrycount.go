package consumer

import (
	"log/slog"
	"strconv"
)

// retryCountAttribute is the native SQS user message attribute that records how
// many delivery attempts have failed for a message under the Scheduled Retry
// model. Its value is a base-10 non-negative integer.
const retryCountAttribute = "retry_count"

// parseRetryCount reads the native retry_count user message attribute and
// returns it as a non-negative integer. It defaults to 0 when the attribute is
// absent or when its value does not parse as a non-negative base-10 integer,
// logging the malformed value through log so callers can observe corrupt
// attributes. A nil log is tolerated by relying on the dispatcher's no-op
// default logger being supplied by callers.
func parseRetryCount(msg *message, log *slog.Logger) int {
	raw := msg.UserMessageAttribute(retryCountAttribute)
	if raw == "" {
		return 0
	}

	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		log.Error("malformed retry_count attribute; defaulting to 0",
			slog.String("receipt_handle", msg.Identifier()),
			slog.String("retry_count", raw),
		)
		return 0
	}

	return value
}
