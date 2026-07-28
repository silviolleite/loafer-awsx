package consumer

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/silviolleite/loafer-awsx/logger"
)

func TestEmitMetricNilHookIsNoOp(t *testing.T) {
	assert.NotPanics(t, func() {
		emitMetric(logger.NewNoOp(), nil, "route-a")
	})
}

func TestEmitMetricInvokesHookWithRouteName(t *testing.T) {
	var calls int
	var got string
	hook := func(routeName string) {
		calls++
		got = routeName
	}

	emitMetric(logger.NewNoOp(), hook, "route-b")

	assert.Equal(t, 1, calls)
	assert.Equal(t, "route-b", got)
}

func TestEmitMetricRecoversAndLogsPanickingHook(t *testing.T) {
	handler := &captureHandler{}
	hook := func(_ string) {
		panic("boom")
	}

	assert.NotPanics(t, func() {
		emitMetric(slog.New(handler), hook, "route-c")
	})

	assert.Equal(t, 1, handler.errorCount())

	attrs := handler.errorAttrs()
	assert.Len(t, attrs, 1)
	assert.Equal(t, "route-c", attrs[0]["route_name"])
	assert.Equal(t, "boom", attrs[0]["panic"])
}
