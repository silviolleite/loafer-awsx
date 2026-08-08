// Package fake provides configurable test doubles for the core loafer-awsx
// interfaces used across package tests: Message (consumer.Message and
// middleware.Message), SQSClient (consumer.SQSClient), and SNSClient
// (producer.SNSClient). It also provides LogHandler, a capturing slog.Handler
// that tests wrap in a *slog.Logger via slog.New to assert on emitted logs; no
// custom logger interface is faked because the library uses the standard
// library *slog.Logger directly.
package fake
