package middleware_test

import (
	"context"
	stderrors "errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/middleware"
)

func newRecordingProvider() (*sdktrace.TracerProvider, *tracetest.SpanRecorder) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	return tp, sr
}

func attrValue(attrs []attribute.KeyValue, key string) (attribute.Value, bool) {
	for _, kv := range attrs {
		if string(kv.Key) == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

func TestOTelRecordsSuccessSpan(t *testing.T) {
	tp, sr := newRecordingProvider()

	handler := func(context.Context, middleware.Message) error {
		return nil
	}

	wrapped := middleware.OTel("orders", middleware.WithTracerProvider(tp))(handler)

	err := wrapped(context.Background(), stubMessage{id: "msg-1"})

	require.NoError(t, err)

	spans := sr.Ended()
	require.Len(t, spans, 1)

	span := spans[0]
	assert.Equal(t, "loafer.process/orders", span.Name())
	assert.Equal(t, trace.SpanKindConsumer, span.SpanKind())
	assert.Equal(t, codes.Ok, span.Status().Code)
	assert.Empty(t, span.Events())

	system, ok := attrValue(span.Attributes(), "messaging.system")
	require.True(t, ok)
	assert.Equal(t, "aws_sqs", system.AsString())

	dest, ok := attrValue(span.Attributes(), "messaging.destination.name")
	require.True(t, ok)
	assert.Equal(t, "orders", dest.AsString())

	id, ok := attrValue(span.Attributes(), "messaging.message.id")
	require.True(t, ok)
	assert.Equal(t, "msg-1", id.AsString())
}

func TestOTelRecordsErrorSpan(t *testing.T) {
	tp, sr := newRecordingProvider()
	sentinel := stderrors.New("handler failure")

	handler := func(context.Context, middleware.Message) error {
		return sentinel
	}

	wrapped := middleware.OTel("payments", middleware.WithTracerProvider(tp))(handler)

	err := wrapped(context.Background(), stubMessage{id: "msg-2"})

	require.ErrorIs(t, err, sentinel)

	spans := sr.Ended()
	require.Len(t, spans, 1)

	span := spans[0]
	assert.Equal(t, "loafer.process/payments", span.Name())
	assert.Equal(t, codes.Error, span.Status().Code)
	assert.Equal(t, sentinel.Error(), span.Status().Description)

	events := span.Events()
	require.Len(t, events, 1)
	assert.Equal(t, "exception", events[0].Name)

	msgAttr, ok := attrValue(events[0].Attributes, "exception.message")
	require.True(t, ok)
	assert.Equal(t, sentinel.Error(), msgAttr.AsString())
}

func TestOTelPropagatesSpanContextToHandler(t *testing.T) {
	tp, _ := newRecordingProvider()

	var recording bool
	handler := func(ctx context.Context, _ middleware.Message) error {
		recording = trace.SpanFromContext(ctx).IsRecording()
		return nil
	}

	wrapped := middleware.OTel("ctx", middleware.WithTracerProvider(tp))(handler)

	err := wrapped(context.Background(), stubMessage{id: "msg-ctx"})

	require.NoError(t, err)
	assert.True(t, recording)
}

func TestOTelHandlesNilMessage(t *testing.T) {
	tp, sr := newRecordingProvider()

	handler := func(context.Context, middleware.Message) error {
		return nil
	}

	wrapped := middleware.OTel("nilroute", middleware.WithTracerProvider(tp))(handler)

	err := wrapped(context.Background(), nil)

	require.NoError(t, err)

	spans := sr.Ended()
	require.Len(t, spans, 1)

	_, ok := attrValue(spans[0].Attributes(), "messaging.message.id")
	assert.False(t, ok)

	dest, ok := attrValue(spans[0].Attributes(), "messaging.destination.name")
	require.True(t, ok)
	assert.Equal(t, "nilroute", dest.AsString())
}

func TestOTelDefaultsToGlobalProvider(t *testing.T) {
	previous := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(previous) })

	tp, sr := newRecordingProvider()
	otel.SetTracerProvider(tp)

	handler := func(context.Context, middleware.Message) error {
		return nil
	}

	wrapped := middleware.OTel("global")(handler)

	err := wrapped(context.Background(), stubMessage{id: "msg-global"})

	require.NoError(t, err)

	spans := sr.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, "loafer.process/global", spans[0].Name())
}

func TestOTelWithLinkFromContextCreatesRootAndLink(t *testing.T) {
	tp, sr := newRecordingProvider()

	parentTracer := tp.Tracer("parent")
	parentCtx, parentSpan := parentTracer.Start(context.Background(), "producer")
	parentSC := parentSpan.SpanContext()
	parentSpan.End()

	handler := func(context.Context, middleware.Message) error {
		return nil
	}

	wrapped := middleware.OTel("linked",
		middleware.WithTracerProvider(tp),
		middleware.WithLinkFromContext(),
	)(handler)

	err := wrapped(parentCtx, stubMessage{id: "msg-link"})

	require.NoError(t, err)

	spans := sr.Ended()
	require.Len(t, spans, 2)

	var processing sdktrace.ReadOnlySpan
	for _, s := range spans {
		if s.Name() == "loafer.process/linked" {
			processing = s
		}
	}
	require.NotNil(t, processing)

	assert.NotEqual(t, parentSC.TraceID(), processing.SpanContext().TraceID())
	assert.False(t, processing.Parent().IsValid())

	links := processing.Links()
	require.Len(t, links, 1)
	assert.Equal(t, parentSC.TraceID(), links[0].SpanContext.TraceID())
	assert.Equal(t, parentSC.SpanID(), links[0].SpanContext.SpanID())
}

func TestOTelWithLinkFromContextWithoutParentStartsRootWithoutLink(t *testing.T) {
	tp, sr := newRecordingProvider()

	handler := func(context.Context, middleware.Message) error {
		return nil
	}

	wrapped := middleware.OTel("rootless",
		middleware.WithTracerProvider(tp),
		middleware.WithLinkFromContext(),
	)(handler)

	err := wrapped(context.Background(), stubMessage{id: "msg-noparent"})

	require.NoError(t, err)

	spans := sr.Ended()
	require.Len(t, spans, 1)

	assert.Equal(t, "loafer.process/rootless", spans[0].Name())
	assert.False(t, spans[0].Parent().IsValid())
	assert.Empty(t, spans[0].Links())
}

func TestOTelDefaultInheritsParentTrace(t *testing.T) {
	tp, sr := newRecordingProvider()

	parentTracer := tp.Tracer("parent")
	parentCtx, parentSpan := parentTracer.Start(context.Background(), "producer")
	parentSC := parentSpan.SpanContext()
	parentSpan.End()

	handler := func(context.Context, middleware.Message) error {
		return nil
	}

	wrapped := middleware.OTel("inherited", middleware.WithTracerProvider(tp))(handler)

	err := wrapped(parentCtx, stubMessage{id: "msg-inherit"})

	require.NoError(t, err)

	spans := sr.Ended()
	require.Len(t, spans, 2)

	var processing sdktrace.ReadOnlySpan
	for _, s := range spans {
		if s.Name() == "loafer.process/inherited" {
			processing = s
		}
	}
	require.NotNil(t, processing)

	assert.Equal(t, parentSC.TraceID(), processing.SpanContext().TraceID())
	assert.Equal(t, parentSC.SpanID(), processing.Parent().SpanID())
	assert.Empty(t, processing.Links())
}

func TestOTelWithNilTracerProviderFallsBack(t *testing.T) {
	previous := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(previous) })

	tp, sr := newRecordingProvider()
	otel.SetTracerProvider(tp)

	handler := func(context.Context, middleware.Message) error {
		return nil
	}

	wrapped := middleware.OTel("fallback", middleware.WithTracerProvider(nil))(handler)

	err := wrapped(context.Background(), stubMessage{id: "msg-fb"})

	require.NoError(t, err)

	spans := sr.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, "loafer.process/fallback", spans[0].Name())
}

func TestOTelSpanReflectsHandlerOutcomeProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		route := rapid.StringMatching(`[a-zA-Z0-9_.\-/]{1,20}`).Draw(rt, "route")
		id := rapid.String().Draw(rt, "messageID")
		fail := rapid.Bool().Draw(rt, "fail")
		errMsg := rapid.StringN(1, 30, 30).Draw(rt, "errMsg")

		tp, sr := newRecordingProvider()

		var want error
		if fail {
			want = stderrors.New(errMsg)
		}

		handler := func(context.Context, middleware.Message) error {
			return want
		}

		wrapped := middleware.OTel(route, middleware.WithTracerProvider(tp))(handler)

		err := wrapped(context.Background(), stubMessage{id: id})

		spans := sr.Ended()
		require.Len(rt, spans, 1)

		span := spans[0]
		assert.Equal(rt, "loafer.process/"+route, span.Name())
		assert.Equal(rt, trace.SpanKindConsumer, span.SpanKind())

		system, ok := attrValue(span.Attributes(), "messaging.system")
		require.True(rt, ok)
		assert.Equal(rt, "aws_sqs", system.AsString())

		dest, ok := attrValue(span.Attributes(), "messaging.destination.name")
		require.True(rt, ok)
		assert.Equal(rt, route, dest.AsString())

		msgID, ok := attrValue(span.Attributes(), "messaging.message.id")
		require.True(rt, ok)
		assert.Equal(rt, id, msgID.AsString())

		if fail {
			assert.ErrorIs(rt, err, want)
			assert.Equal(rt, codes.Error, span.Status().Code)
			assert.Equal(rt, errMsg, span.Status().Description)
			require.Len(rt, span.Events(), 1)
			assert.Equal(rt, "exception", span.Events()[0].Name)
		} else {
			assert.NoError(rt, err)
			assert.Equal(rt, codes.Ok, span.Status().Code)
			assert.Empty(rt, span.Events())
		}
	})
}
