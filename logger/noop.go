package logger

import "log/slog"

// NewNoOp returns a *slog.Logger backed by a discard handler. Every record is
// dropped, making it a safe default when no logging output is desired.
func NewNoOp() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}
