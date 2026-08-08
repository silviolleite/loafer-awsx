// Package logger provides constructors for the standard library *slog.Logger
// used throughout loafer-awsx.
//
// New returns a logger writing structured, leveled output to stdout via a
// slog.TextHandler, while NewNoOp returns a logger that discards all output.
// The library uses the standard library *slog.Logger type everywhere and does
// not define a custom logging interface.
package logger
