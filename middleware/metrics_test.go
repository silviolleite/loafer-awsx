package middleware_test

import (
	"context"
	stderrors "errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/middleware"
)

func gather(t require.TestingT, reg *prometheus.Registry) []*dto.MetricFamily {
	families, err := reg.Gather()
	require.NoError(t, err)
	return families
}

func metricByLabels(families []*dto.MetricFamily, name string, labels map[string]string) *dto.Metric {
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		for _, metric := range fam.GetMetric() {
			if labelsMatch(metric, labels) {
				return metric
			}
		}
	}
	return nil
}

func labelsMatch(metric *dto.Metric, labels map[string]string) bool {
	pairs := make(map[string]string, len(metric.GetLabel()))
	for _, lp := range metric.GetLabel() {
		pairs[lp.GetName()] = lp.GetValue()
	}
	for key, value := range labels {
		if pairs[key] != value {
			return false
		}
	}
	return true
}

func counterValue(families []*dto.MetricFamily, name string, labels map[string]string) float64 {
	metric := metricByLabels(families, name, labels)
	if metric == nil {
		return 0
	}
	return metric.GetCounter().GetValue()
}

func gaugeValue(families []*dto.MetricFamily, name string, labels map[string]string) float64 {
	metric := metricByLabels(families, name, labels)
	if metric == nil {
		return 0
	}
	return metric.GetGauge().GetValue()
}

func histogramCount(families []*dto.MetricFamily, name string, labels map[string]string) uint64 {
	metric := metricByLabels(families, name, labels)
	if metric == nil {
		return 0
	}
	return metric.GetHistogram().GetSampleCount()
}

func assertAlreadyRegistered(t *testing.T, reg *prometheus.Registry, c prometheus.Collector) {
	t.Helper()
	err := reg.Register(c)
	var are prometheus.AlreadyRegisteredError
	require.ErrorAs(t, err, &are)
}

func TestMetricsRegistersAllCollectors(t *testing.T) {
	reg := prometheus.NewRegistry()

	middleware.Metrics("orders", middleware.WithMetricsRegisterer(reg))

	assertAlreadyRegistered(t, reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "loafer_messages_received_total",
		Help: "Total messages received",
	}, []string{"route"}))

	assertAlreadyRegistered(t, reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "loafer_messages_processed_total",
		Help: "Total messages processed",
	}, []string{"route", "status"}))

	assertAlreadyRegistered(t, reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "loafer_messages_errors_total",
		Help: "Total message processing errors",
	}, []string{"route"}))

	assertAlreadyRegistered(t, reg, prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "loafer_message_processing_duration_seconds",
		Help:    "Message processing duration",
		Buckets: prometheus.DefBuckets,
	}, []string{"route"}))

	assertAlreadyRegistered(t, reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "loafer_messages_inflight",
		Help: "Messages currently being processed",
	}, []string{"route"}))

	assertAlreadyRegistered(t, reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "loafer_messages_dlq_total",
		Help: "Total messages observed as exhausted (receive count reached maxReceiveCount; redriven by AWS SQS)",
	}, []string{"route"}))
}

func TestMetricsSuccessUpdatesCollectors(t *testing.T) {
	reg := prometheus.NewRegistry()

	handler := func(context.Context, middleware.Message) error {
		return nil
	}

	wrapped := middleware.Metrics("orders", middleware.WithMetricsRegisterer(reg))(handler)

	err := wrapped(context.Background(), stubMessage{id: "msg-1"})
	require.NoError(t, err)

	families := gather(t, reg)
	route := map[string]string{"route": "orders"}

	assert.Equal(t, float64(1), counterValue(families, "loafer_messages_received_total", route))
	assert.Equal(t, float64(1), counterValue(families, "loafer_messages_processed_total", map[string]string{"route": "orders", "status": "success"}))
	assert.Equal(t, float64(0), counterValue(families, "loafer_messages_processed_total", map[string]string{"route": "orders", "status": "error"}))
	assert.Equal(t, float64(0), counterValue(families, "loafer_messages_errors_total", route))
	assert.Equal(t, float64(0), gaugeValue(families, "loafer_messages_inflight", route))
	assert.Equal(t, uint64(1), histogramCount(families, "loafer_message_processing_duration_seconds", route))
}

func TestMetricsErrorUpdatesCollectors(t *testing.T) {
	reg := prometheus.NewRegistry()
	sentinel := stderrors.New("handler failure")

	handler := func(context.Context, middleware.Message) error {
		return sentinel
	}

	wrapped := middleware.Metrics("payments", middleware.WithMetricsRegisterer(reg))(handler)

	err := wrapped(context.Background(), stubMessage{id: "msg-2"})
	require.ErrorIs(t, err, sentinel)

	families := gather(t, reg)
	route := map[string]string{"route": "payments"}

	assert.Equal(t, float64(1), counterValue(families, "loafer_messages_received_total", route))
	assert.Equal(t, float64(1), counterValue(families, "loafer_messages_errors_total", route))
	assert.Equal(t, float64(1), counterValue(families, "loafer_messages_processed_total", map[string]string{"route": "payments", "status": "error"}))
	assert.Equal(t, float64(0), counterValue(families, "loafer_messages_processed_total", map[string]string{"route": "payments", "status": "success"}))
	assert.Equal(t, float64(0), gaugeValue(families, "loafer_messages_inflight", route))
	assert.Equal(t, uint64(1), histogramCount(families, "loafer_message_processing_duration_seconds", route))
}

func TestMetricsInflightGaugeTracksProcessing(t *testing.T) {
	reg := prometheus.NewRegistry()
	route := map[string]string{"route": "inflight"}

	var during float64
	handler := func(context.Context, middleware.Message) error {
		during = gaugeValue(gather(t, reg), "loafer_messages_inflight", route)
		return nil
	}

	wrapped := middleware.Metrics("inflight", middleware.WithMetricsRegisterer(reg))(handler)

	err := wrapped(context.Background(), stubMessage{id: "msg-inflight"})
	require.NoError(t, err)

	assert.Equal(t, float64(1), during)
	assert.Equal(t, float64(0), gaugeValue(gather(t, reg), "loafer_messages_inflight", route))
}

func TestMetricsInflightDecrementsOnPanic(t *testing.T) {
	reg := prometheus.NewRegistry()
	route := map[string]string{"route": "panicky"}

	handler := func(context.Context, middleware.Message) error {
		panic("boom")
	}

	wrapped := middleware.Metrics("panicky", middleware.WithMetricsRegisterer(reg))(handler)

	assert.Panics(t, func() {
		_ = wrapped(context.Background(), stubMessage{id: "msg-panic"})
	})

	assert.Equal(t, float64(0), gaugeValue(gather(t, reg), "loafer_messages_inflight", route))
}

func TestMetricsCustomRegistererIsolatesCollectors(t *testing.T) {
	regA := prometheus.NewRegistry()
	regB := prometheus.NewRegistry()

	handler := func(context.Context, middleware.Message) error {
		return nil
	}

	wrappedA := middleware.Metrics("routeA", middleware.WithMetricsRegisterer(regA))(handler)
	require.NoError(t, wrappedA(context.Background(), stubMessage{id: "a"}))

	route := map[string]string{"route": "routeA"}
	assert.Equal(t, float64(1), counterValue(gather(t, regA), "loafer_messages_received_total", route))
	assert.Equal(t, float64(0), counterValue(gather(t, regB), "loafer_messages_received_total", route))
}

func TestMetricsNilRegistererFallsBackToDefault(t *testing.T) {
	handler := func(context.Context, middleware.Message) error {
		return nil
	}

	assert.NotPanics(t, func() {
		wrapped := middleware.Metrics("default-fallback", middleware.WithMetricsRegisterer(nil))(handler)
		_ = wrapped(context.Background(), stubMessage{id: "d"})
	})
}

func TestMetricsDuplicateRegistrationDoesNotPanic(t *testing.T) {
	reg := prometheus.NewRegistry()

	handler := func(context.Context, middleware.Message) error {
		return nil
	}

	assert.NotPanics(t, func() {
		first := middleware.Metrics("dup", middleware.WithMetricsRegisterer(reg))(handler)
		second := middleware.Metrics("dup", middleware.WithMetricsRegisterer(reg))(handler)
		require.NoError(t, first(context.Background(), stubMessage{id: "1"}))
		require.NoError(t, second(context.Background(), stubMessage{id: "2"}))
	})

	assert.Equal(t, float64(2), counterValue(gather(t, reg), "loafer_messages_received_total", map[string]string{"route": "dup"}))
}

func TestMetricsHandlesNilMessage(t *testing.T) {
	reg := prometheus.NewRegistry()

	handler := func(context.Context, middleware.Message) error {
		return nil
	}

	wrapped := middleware.Metrics("nilmsg", middleware.WithMetricsRegisterer(reg))(handler)

	err := wrapped(context.Background(), nil)
	require.NoError(t, err)

	assert.Equal(t, float64(1), counterValue(gather(t, reg), "loafer_messages_received_total", map[string]string{"route": "nilmsg"}))
}

func TestMetricsCollectorValuesReflectOutcomesProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		reg := prometheus.NewRegistry()
		route := rapid.StringMatching(`[a-zA-Z0-9_]{1,20}`).Draw(rt, "route")
		outcomes := rapid.SliceOfN(rapid.Bool(), 1, 30).Draw(rt, "outcomes")

		sentinel := stderrors.New("boom")
		next := func(fail bool) middleware.Handler {
			return func(context.Context, middleware.Message) error {
				if fail {
					return sentinel
				}
				return nil
			}
		}

		var successes, failures float64
		for _, fail := range outcomes {
			wrapped := middleware.Metrics(route, middleware.WithMetricsRegisterer(reg))(next(fail))
			err := wrapped(context.Background(), stubMessage{id: "id"})
			if fail {
				failures++
				require.ErrorIs(rt, err, sentinel)
			} else {
				successes++
				require.NoError(rt, err)
			}
		}

		families := gather(rt, reg)
		routeLabels := map[string]string{"route": route}
		total := float64(len(outcomes))

		assert.Equal(rt, total, counterValue(families, "loafer_messages_received_total", routeLabels))
		assert.Equal(rt, successes, counterValue(families, "loafer_messages_processed_total", map[string]string{"route": route, "status": "success"}))
		assert.Equal(rt, failures, counterValue(families, "loafer_messages_processed_total", map[string]string{"route": route, "status": "error"}))
		assert.Equal(rt, failures, counterValue(families, "loafer_messages_errors_total", routeLabels))
		assert.Equal(rt, float64(0), gaugeValue(families, "loafer_messages_inflight", routeLabels))
		assert.Equal(rt, uint64(total), histogramCount(families, "loafer_message_processing_duration_seconds", routeLabels))
	})
}
