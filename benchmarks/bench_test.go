package benchmarks

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	jc "github.com/justcodes/loafer-go/v2"
	jcsqs "github.com/justcodes/loafer-go/v2/aws/sqs"

	awsxbroker "github.com/silviolleite/loafer-awsx/broker"
	awsxconsumer "github.com/silviolleite/loafer-awsx/consumer"
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

// simulatedDeleteRTT is the per-request delete latency the delete benchmarks
// use to approximate an SQS round trip.
const simulatedDeleteRTT = 2 * time.Millisecond

var _ awsxconsumer.BatchDeleteClient = (*benchClient)(nil)

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

// benchAWSXDelete drives loafer-awsx through the benchAWSX lifecycle with every
// delete request delayed by latency. batch enables opportunistic delete
// batching on the route. Besides msg/s it reports delete-calls/msg,
// receive_reqs/msg and, for batch runs, entries/batch.
func benchAWSXDelete(b *testing.B, groups int64, latency time.Duration, batch bool) {
	b.Helper()
	b.ReportAllocs()

	client := newBenchClient(int64(b.N), groups)
	client.deleteLatency = latency

	opts := []awsxrouter.Option{awsxrouter.WithWorkerPoolSize(benchWorkers)}
	if groups > 0 {
		opts = append(opts, awsxrouter.WithRunMode(awsxrouter.PerGroupID))
	}
	if batch {
		opts = append(opts, awsxrouter.WithDeleteBatch())
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
	reportDeleteCalls(b, client, batch)
}

// reportDeleteCalls adds the delete and receive requests per message and, for
// batch runs, the average entries per DeleteMessageBatch request.
func reportDeleteCalls(b *testing.B, client *benchClient, batch bool) {
	b.Helper()
	b.ReportMetric(float64(atomic.LoadInt64(&client.deleteCalls))/float64(b.N), "delete-calls/msg")
	b.ReportMetric(float64(atomic.LoadInt64(&client.receiveCalls))/float64(b.N), "receive_reqs/msg")
	if !batch {
		return
	}
	if calls := atomic.LoadInt64(&client.batchCalls); calls > 0 {
		b.ReportMetric(float64(atomic.LoadInt64(&client.batchEntries))/float64(calls), "entries/batch")
	}
}

// benchDeleteLatencies runs benchAWSXDelete once without delete latency and
// once with simulatedDeleteRTT, as latency=0 and latency=2ms sub-benchmarks.
func benchDeleteLatencies(b *testing.B, groups int64, batch bool) {
	b.Helper()
	b.Run("latency=0", func(b *testing.B) { benchAWSXDelete(b, groups, 0, batch) })
	b.Run("latency=2ms", func(b *testing.B) { benchAWSXDelete(b, groups, simulatedDeleteRTT, batch) })
}

func BenchmarkDeleteStandardSync(b *testing.B) { benchDeleteLatencies(b, 0, false) }

func BenchmarkDeleteStandardBatch(b *testing.B) { benchDeleteLatencies(b, 0, true) }

func BenchmarkDeleteFIFOSync(b *testing.B) { benchDeleteLatencies(b, fifoGroups, false) }

func BenchmarkDeleteFIFOBatch(b *testing.B) { benchDeleteLatencies(b, fifoGroups, true) }

// Flow-control scenario: the production-like route the flow-control spec was
// written for, with a simulated handler and receive round trip.
const (
	flowWorkers          = 30
	flowMaxMessages      = 10
	simulatedHandlerWork = 2 * time.Millisecond
	simulatedReceiveRTT  = 5 * time.Millisecond
)

// identifiable is the part of the consumer message the flow-control handler
// uses to look up the receive instant.
type identifiable interface {
	Identifier() string
}

// benchFlowControl drives loafer-awsx with flowWorkers workers, MaxMessages
// flowMaxMessages, the given number of pollers, a simulated handler duration,
// and a simulated receive round trip. groups > 0 selects FIFO PerGroupID
// routing. Besides msg/s it reports receive and visibility-change requests per
// message and the mean wait between receive and handler start.
func benchFlowControl(b *testing.B, groups int64, pollers int) {
	b.Helper()
	b.ReportAllocs()

	client := newBenchClient(int64(b.N), groups)
	client.receiveLatency = simulatedReceiveRTT
	client.recordReceivedAt = true

	var waitNanos, started atomic.Int64
	handler := func(ctx context.Context, m awsxmw.Message) error {
		if msg, ok := m.(identifiable); ok {
			if wait, ok := client.waitSince(msg.Identifier()); ok {
				waitNanos.Add(int64(wait))
				started.Add(1)
			}
		}
		return sleepCtx(ctx, simulatedHandlerWork)
	}

	opts := []awsxrouter.Option{
		awsxrouter.WithWorkerPoolSize(flowWorkers),
		awsxrouter.WithMaxMessages(flowMaxMessages),
		awsxrouter.WithPollers(pollers),
	}
	if groups > 0 {
		opts = append(opts, awsxrouter.WithRunMode(awsxrouter.PerGroupID))
	}

	route, err := awsxrouter.New("bench-queue", handler, opts...)
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
	n := float64(b.N)
	b.ReportMetric(float64(atomic.LoadInt64(&client.receiveCalls))/n, "receive_reqs/msg")
	b.ReportMetric(float64(atomic.LoadInt64(&client.visibilityCalls))/n, "visibility_reqs/msg")
	if s := started.Load(); s > 0 {
		b.ReportMetric(float64(waitNanos.Load())/float64(s)/float64(time.Millisecond), "queue_wait_ms")
	}
}

func BenchmarkFlowControl(b *testing.B) {
	for _, mode := range []struct {
		name   string
		groups int64
	}{
		{name: "parallel", groups: 0},
		{name: "pergroup", groups: fifoGroups},
	} {
		for _, pollers := range []int{1, 2} {
			name := mode.name + "/workers=30/max=10/pollers=" + strconv.Itoa(pollers)
			b.Run(name, func(b *testing.B) { benchFlowControl(b, mode.groups, pollers) })
		}
	}
}
