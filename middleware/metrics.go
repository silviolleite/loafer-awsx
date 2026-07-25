package middleware

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metric names exported by the Metrics middleware. They follow the Prometheus
// naming conventions with the "loafer" namespace prefix.
const (
	metricReceived  = "loafer_messages_received_total"
	metricProcessed = "loafer_messages_processed_total"
	metricErrors    = "loafer_messages_errors_total"
	metricDuration  = "loafer_message_processing_duration_seconds"
	metricInflight  = "loafer_messages_inflight"
	metricDLQ       = "loafer_messages_dlq_total"
)

// Metric label names and values used by the Metrics middleware.
const (
	labelRoute  = "route"
	labelStatus = "status"

	statusSuccess = "success"
	statusError   = "error"
)

// metricsConfig holds the resolved configuration for the Metrics middleware.
type metricsConfig struct {
	registerer prometheus.Registerer
}

// MetricsOption configures the Metrics middleware.
type MetricsOption func(*metricsConfig)

// WithMetricsRegisterer sets a custom prometheus.Registerer for the Metrics
// middleware. When this option is not supplied, or is supplied with a nil
// registerer, the middleware falls back to prometheus.DefaultRegisterer.
func WithMetricsRegisterer(r prometheus.Registerer) MetricsOption {
	return func(cfg *metricsConfig) {
		if r != nil {
			cfg.registerer = r
		}
	}
}

// loadMetricsConfig builds a metricsConfig from the supplied options,
// defaulting the registerer to prometheus.DefaultRegisterer when no valid
// override is given.
func loadMetricsConfig(opts ...MetricsOption) metricsConfig {
	cfg := metricsConfig{registerer: prometheus.DefaultRegisterer}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.registerer == nil {
		cfg.registerer = prometheus.DefaultRegisterer
	}
	return cfg
}

// Metrics returns a Middleware that instruments message processing with
// Prometheus collectors, all labeled by the supplied route name:
//
//   - loafer_messages_received_total (counter): incremented for every message.
//   - loafer_messages_processed_total (counter, "status" label): incremented
//     with status "success" or "error" once processing completes.
//   - loafer_messages_errors_total (counter): incremented when the wrapped
//     handler returns an error.
//   - loafer_message_processing_duration_seconds (histogram): observes the
//     elapsed handler processing time in seconds.
//   - loafer_messages_inflight (gauge): incremented when processing begins and
//     decremented when it completes.
//   - loafer_messages_dlq_total (counter): registered here so it is available
//     to the consumer's dead-letter-queue observability; it is not incremented
//     by this middleware.
//
// Collectors are registered on the configured registerer using a safe,
// idempotent registration that reuses any previously registered collector,
// so constructing the middleware for multiple routes (or more than once for
// the same registerer) never panics on duplicate registration. The wrapped
// handler's error is always propagated unchanged, keeping the middleware
// transparent to callers.
func Metrics(routeName string, opts ...MetricsOption) Middleware {
	cfg := loadMetricsConfig(opts...)

	received := registerCounterVec(cfg.registerer, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricReceived,
		Help: "Total messages received",
	}, []string{labelRoute}))

	processed := registerCounterVec(cfg.registerer, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricProcessed,
		Help: "Total messages processed",
	}, []string{labelRoute, labelStatus}))

	errorsTotal := registerCounterVec(cfg.registerer, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricErrors,
		Help: "Total message processing errors",
	}, []string{labelRoute}))

	duration := registerHistogramVec(cfg.registerer, prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    metricDuration,
		Help:    "Message processing duration",
		Buckets: prometheus.DefBuckets,
	}, []string{labelRoute}))

	inflight := registerGaugeVec(cfg.registerer, prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: metricInflight,
		Help: "Messages currently being processed",
	}, []string{labelRoute}))

	// Registered so the collector is available to the consumer's DLQ
	// observability; not incremented by this middleware.
	_ = registerCounterVec(cfg.registerer, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricDLQ,
		Help: "Total messages observed as exhausted (receive count reached maxReceiveCount; redriven by AWS SQS)",
	}, []string{labelRoute}))

	return func(next Handler) Handler {
		return func(ctx context.Context, msg Message) error {
			received.WithLabelValues(routeName).Inc()
			inflight.WithLabelValues(routeName).Inc()
			defer inflight.WithLabelValues(routeName).Dec()

			start := time.Now()
			err := next(ctx, msg)
			elapsed := time.Since(start).Seconds()

			duration.WithLabelValues(routeName).Observe(elapsed)

			if err != nil {
				errorsTotal.WithLabelValues(routeName).Inc()
				processed.WithLabelValues(routeName, statusError).Inc()
				return err
			}

			processed.WithLabelValues(routeName, statusSuccess).Inc()
			return nil
		}
	}
}

// registerCounterVec registers c with the registerer and returns the usable
// collector. When an equivalent collector is already registered, the existing
// one is returned instead, avoiding a panic on duplicate registration.
func registerCounterVec(r prometheus.Registerer, c *prometheus.CounterVec) *prometheus.CounterVec {
	if err := r.Register(c); err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			if existing, ok := are.ExistingCollector.(*prometheus.CounterVec); ok {
				return existing
			}
		}
	}
	return c
}

// registerHistogramVec registers h with the registerer and returns the usable
// collector, reusing an already registered equivalent collector when present.
func registerHistogramVec(r prometheus.Registerer, h *prometheus.HistogramVec) *prometheus.HistogramVec {
	if err := r.Register(h); err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			if existing, ok := are.ExistingCollector.(*prometheus.HistogramVec); ok {
				return existing
			}
		}
	}
	return h
}

// registerGaugeVec registers g with the registerer and returns the usable
// collector, reusing an already registered equivalent collector when present.
func registerGaugeVec(r prometheus.Registerer, g *prometheus.GaugeVec) *prometheus.GaugeVec {
	if err := r.Register(g); err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			if existing, ok := are.ExistingCollector.(*prometheus.GaugeVec); ok {
				return existing
			}
		}
	}
	return g
}
