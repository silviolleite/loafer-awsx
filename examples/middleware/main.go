// Command middleware demonstrates observability middleware with loafer-go v3.
//
// It wires a consumer with a full middleware stack applied globally by the
// broker (outermost first):
//
//   - Recovery converts a panicking handler into an error instead of crashing
//     the process.
//   - Logging records message receipt, processing duration, and outcome.
//   - Metrics instruments processing with Prometheus collectors, exposed here
//     on an HTTP /metrics endpoint backed by a dedicated registry.
//   - OTel creates an OpenTelemetry span per message, reporting to a
//     TracerProvider from the OTel SDK.
//
// It shows the full wiring of an observable consumer application:
//
//  1. Building an AWS connection (aws.Config) with the conn package.
//  2. Setting up a Prometheus registry and an OTel TracerProvider.
//  3. Serving the Prometheus /metrics endpoint over HTTP.
//  4. Declaring a route and a broker with the middleware stack.
//  5. Running the broker with graceful shutdown driven by OS signals, then
//     cleanly shutting down the metrics server and tracer provider.
//
// Run it against a real queue or a local AWS-compatible endpoint (for example
// LocalStack) by adjusting the region, credentials, endpoint, and queue name
// below.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/silviolleite/loafer-awsx/broker"
	"github.com/silviolleite/loafer-awsx/conn"
	"github.com/silviolleite/loafer-awsx/logger"
	"github.com/silviolleite/loafer-awsx/middleware"
	"github.com/silviolleite/loafer-awsx/router"
)

const (
	// queueName is the SQS queue this example consumes from. Replace it with
	// the name of a queue that exists in your AWS account or local environment.
	queueName = "example-middleware-queue"

	// metricsAddr is the address the Prometheus /metrics endpoint listens on.
	metricsAddr = ":9090"

	// shutdownGracePeriod bounds how long the metrics server and tracer
	// provider are given to shut down cleanly.
	shutdownGracePeriod = 5 * time.Second
)

func main() {
	// Step 5 (part 1): derive a context canceled on interrupt (Ctrl+C) or
	// SIGTERM so the broker can drain in-flight messages before returning.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// A shared structured logger used by the broker and the Logging and
	// Recovery middlewares.
	lg := logger.New()

	// Step 1: build the AWS connection. The static credentials and endpoint are
	// convenient for local development against LocalStack and can be dropped in
	// favor of the default AWS credential chain in production.
	cfg, err := conn.New(ctx,
		conn.WithRegion("us-east-1"),
		conn.WithAccessKey("test", "test"),
		conn.WithEndpoint("http://localhost:4566"),
	)
	if err != nil {
		lg.Error("failed to create AWS config", "error", err)
		os.Exit(1)
	}

	sqsClient := sqs.NewFromConfig(cfg)

	// Step 2 (part 1): create a dedicated Prometheus registry. Using a custom
	// registry (instead of the global default) keeps the example self-contained
	// and lets the Metrics middleware register its collectors in isolation.
	registry := prometheus.NewRegistry()

	// Step 2 (part 2): create an OTel TracerProvider from the SDK. In a real
	// deployment you would attach a span exporter (OTLP, Jaeger, etc.); here the
	// provider is created without an exporter so the example stays dependency
	// free while still exercising the OTel middleware.
	tracerProvider := sdktrace.NewTracerProvider()

	// Step 3: serve the Prometheus metrics over HTTP in the background.
	metricsServer := &http.Server{
		Addr:              metricsAddr,
		Handler:           promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
		ReadHeaderTimeout: shutdownGracePeriod,
	}
	go func() {
		lg.Info("serving Prometheus metrics", "addr", metricsAddr, "path", "/metrics")
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			lg.Error("metrics server stopped with error", "error", err)
		}
	}()

	// Step 4 (part 1): declare the route.
	route, err := router.New(queueName, handleMessage)
	if err != nil {
		lg.Error("failed to create route", "error", err)
		os.Exit(1)
	}

	// Step 4 (part 2): create the broker with the global middleware stack. The
	// broker applies these middlewares outermost, in order, to every message
	// across all routes: Recovery wraps everything so a panic never escapes,
	// then Logging, then Metrics, then OTel closest to the handler.
	b, err := broker.New(sqsClient, []*router.Route{route},
		broker.WithLogger(lg),
		broker.WithMiddleware(
			middleware.Recovery(lg),
			middleware.Logging(lg),
			middleware.Metrics(queueName, middleware.WithMetricsRegisterer(registry)),
			middleware.OTel(queueName, middleware.WithTracerProvider(tracerProvider)),
		),
	)
	if err != nil {
		lg.Error("failed to create broker", "error", err)
		os.Exit(1)
	}

	// Step 5 (part 2): run the broker until every consumer stops.
	lg.Info("starting broker", "queue", queueName)
	runErr := b.Run(ctx)

	// Step 5 (part 3): shut down the auxiliary components cleanly. A fresh
	// context is used because the parent context is already canceled by the
	// signal that triggered shutdown.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGracePeriod)
	defer cancel()

	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		lg.Error("failed to shut down metrics server", "error", err)
	}
	if err := tracerProvider.Shutdown(shutdownCtx); err != nil {
		lg.Error("failed to shut down tracer provider", "error", err)
	}

	if runErr != nil {
		lg.Error("broker stopped with error", "error", runErr)
		os.Exit(1)
	}

	lg.Info("broker stopped gracefully")
}

// handleMessage is a simple handler used to exercise the middleware stack. It
// decodes the JSON body; the surrounding middlewares handle logging, metrics,
// tracing, and panic recovery. Returning a non-nil error leaves the message on
// the queue and is reflected in the error metrics, logs, and span status.
func handleMessage(_ context.Context, msg middleware.Message) error {
	var payload struct {
		ID   string `json:"id"`
		Text string `json:"text"`
	}
	if err := msg.Decode(&payload); err != nil {
		return err
	}

	// A nil return acknowledges successful processing; the consumer then
	// deletes the message from the queue.
	return nil
}
