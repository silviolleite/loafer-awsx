package middleware

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	// tracerName is the instrumentation scope name used when acquiring a
	// Tracer from the configured TracerProvider.
	tracerName = "loafer-go"

	// spanNamePrefix is prepended to the route name to form the span name
	// following the "loafer.process/<route>" pattern.
	spanNamePrefix = "loafer.process/"

	// messagingSystem is the value reported for the messaging.system span
	// attribute, identifying AWS SQS as the messaging backend.
	messagingSystem = "aws_sqs"
)

// otelConfig holds the resolved configuration for the OTel middleware.
type otelConfig struct {
	tracerProvider trace.TracerProvider
}

// OTelOption configures the OpenTelemetry middleware.
type OTelOption func(*otelConfig)

// WithTracerProvider sets a custom trace.TracerProvider for the OTel
// middleware. When this option is not supplied, the middleware falls back to
// the globally registered provider returned by otel.GetTracerProvider.
func WithTracerProvider(tp trace.TracerProvider) OTelOption {
	return func(cfg *otelConfig) {
		if tp != nil {
			cfg.tracerProvider = tp
		}
	}
}

// loadOTelConfig builds an otelConfig from the supplied options, defaulting the
// TracerProvider to the global provider when no override is given.
func loadOTelConfig(opts ...OTelOption) otelConfig {
	cfg := otelConfig{tracerProvider: otel.GetTracerProvider()}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// OTel returns a Middleware that creates a distributed tracing span for each
// message processing operation.
//
// For every message it starts a span named "loafer.process/<routeName>" with
// SpanKind Consumer and records the following attributes:
//   - messaging.system ("aws_sqs")
//   - messaging.destination.name (the route name)
//   - messaging.message.id (the message identifier)
//
// The span context is propagated to the wrapped handler through the returned
// context. When the handler returns an error, the error is recorded as a span
// event and the span status is set to Error; otherwise the status is set to Ok.
// The handler's error is always propagated unchanged, keeping the middleware
// transparent to callers. A nil message is tolerated: in that case the
// messaging.message.id attribute is omitted.
func OTel(routeName string, opts ...OTelOption) Middleware {
	cfg := loadOTelConfig(opts...)

	return func(next Handler) Handler {
		return func(ctx context.Context, msg Message) error {
			tracer := cfg.tracerProvider.Tracer(tracerName)

			attrs := []attribute.KeyValue{
				attribute.String("messaging.system", messagingSystem),
				attribute.String("messaging.destination.name", routeName),
			}
			if msg != nil {
				attrs = append(attrs, attribute.String("messaging.message.id", msg.Identifier()))
			}

			ctx, span := tracer.Start(ctx, spanNamePrefix+routeName,
				trace.WithAttributes(attrs...),
				trace.WithSpanKind(trace.SpanKindConsumer),
			)
			defer span.End()

			err := next(ctx, msg)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return err
			}

			span.SetStatus(codes.Ok, "")
			return nil
		}
	}
}
