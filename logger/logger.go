package logger

import (
	"log/slog"
	"os"
)

// New returns a default *slog.Logger that writes structured, leveled output to
// stdout using a slog.TextHandler on os.Stdout.
func New() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, nil))
}
