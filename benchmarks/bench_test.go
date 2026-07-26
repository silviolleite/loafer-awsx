package benchmarks

import (
	"context"
	"testing"

	jc "github.com/justcodes/loafer-go/v2"
	jcsqs "github.com/justcodes/loafer-go/v2/aws/sqs"

	awsxbroker "github.com/silviolleite/loafer-awsx/broker"
	awsxlog "github.com/silviolleite/loafer-awsx/logger"
	awsxmw "github.com/silviolleite/loafer-awsx/middleware"
	awsxrouter "github.com/silviolleite/loafer-awsx/router"
)

// benchWorkers is the worker-pool size used for every library and mode so the
// comparison holds the concurrency knob constant.
const benchWorkers = 8

// fifoGroups is the number of distinct MessageGroupId values used by the FIFO
// benchmarks. It is larger than benchWorkers so groups spread across the pool.
const fifoGroups int64 = 64

func awsxNoopHandler(context.Context, awsxmw.Message) error { return nil }

func jcNoopHandler(context.Context, jc.Message) error { return nil }

// benchAWSX drives loafer-awsx end to end: it seeds b.N messages, runs the
// broker until every message has been deleted, then stops the clock and shuts
// the broker down. groups > 0 selects FIFO PerGroupID routing.
func benchAWSX(b *testing.B, groups int64) {
	b.Helper()
	b.ReportAllocs()

	client := newBenchClient(int64(b.N), groups)

	opts := []awsxrouter.Option{awsxrouter.WithWorkerPoolSize(benchWorkers)}
	if groups > 0 {
		opts = append(opts, awsxrouter.WithRunMode(awsxrouter.PerGroupID))
	}

	route, err := awsxrouter.New("bench-queue", awsxNoopHandler, opts...)
	if err != nil {
		b.Fatalf("router.New: %v", err)
	}

	broker, err := awsxbroker.New(client, []*awsxrouter.Route{route}, awsxbroker.WithLogger(awsxlog.NewNoOp()))
	if err != nil {
		b.Fatalf("broker.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})

	b.ResetTimer()
	go func() {
		_ = broker.Run(ctx)
		close(runDone)
	}()
	<-client.done
	b.StopTimer()

	cancel()
	<-runDone

	reportThroughput(b)
}

// benchJC drives github.com/justcodes/loafer-go through the same lifecycle.
func benchJC(b *testing.B, groups int64) {
	b.Helper()
	b.ReportAllocs()

	client := newBenchClient(int64(b.N), groups)

	opts := []func(*jcsqs.RouteConfig){jcsqs.RouteWithWorkerPoolSize(benchWorkers)}
	if groups > 0 {
		opts = append(opts, jcsqs.RouteWithRunMode(jc.PerGroupID))
	}

	route := jcsqs.NewRoute(&jcsqs.Config{
		SQSClient: client,
		Handler:   jcNoopHandler,
		QueueName: "bench-queue",
	}, opts...)

	manager := jc.NewManager(&jc.Config{Logger: jc.LoggerFunc(func(...any) {})})
	manager.RegisterRoute(route)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})

	b.ResetTimer()
	go func() {
		_ = manager.Run(ctx)
		close(runDone)
	}()
	<-client.done
	b.StopTimer()

	cancel()
	<-runDone

	reportThroughput(b)
}

// reportThroughput adds a messages-per-second metric derived from the timed
// window so the results table can quote throughput directly.
func reportThroughput(b *testing.B) {
	b.Helper()
	seconds := b.Elapsed().Seconds()
	if seconds > 0 {
		b.ReportMetric(float64(b.N)/seconds, "msg/s")
	}
}

func BenchmarkStandardLoaferAWSX(b *testing.B) { benchAWSX(b, 0) }

func BenchmarkStandardLoaferGo(b *testing.B) { benchJC(b, 0) }

func BenchmarkFIFOLoaferAWSX(b *testing.B) { benchAWSX(b, fifoGroups) }

func BenchmarkFIFOLoaferGo(b *testing.B) { benchJC(b, fifoGroups) }
