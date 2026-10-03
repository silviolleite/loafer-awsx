package consumer_test

import (
	"context"
	"errors"
	"log/slog"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/consumer"
	"github.com/silviolleite/loafer-awsx/fake"
	"github.com/silviolleite/loafer-awsx/logger"
)

type batcherScenario struct {
	queueURL  string
	handles   []string
	producers [][]string
	latencies []int
	capacity  int
	senders   int
}

func drawBatcherScenario(t *rapid.T) batcherScenario {
	return drawBatcherScenarioOfSize(t, 0, 200)
}

func drawBatcherScenarioOfSize(t *rapid.T, minHandles, maxHandles int) batcherScenario {
	handles := rapid.SliceOfNDistinct(rapid.StringN(1, 64, -1), minHandles, maxHandles, rapid.ID[string]).Draw(t, "handles")
	producerCount := rapid.IntRange(1, 8).Draw(t, "producers")
	assignment := rapid.SliceOfN(rapid.IntRange(0, producerCount-1), len(handles), len(handles)).Draw(t, "assignment")
	producers := make([][]string, producerCount)
	for i, h := range handles {
		producers[assignment[i]] = append(producers[assignment[i]], h)
	}
	return batcherScenario{
		queueURL:  rapid.StringMatching(`https://sqs\.[a-z0-9-]{1,16}\.amazonaws\.com/[0-9]{12}/[A-Za-z0-9_-]{1,80}`).Draw(t, "queueURL"),
		handles:   handles,
		producers: producers,
		latencies: rapid.SliceOfN(rapid.IntRange(0, 16), 1, 32).Draw(t, "latencies"),
		capacity:  rapid.IntRange(1, 64).Draw(t, "capacity"),
		senders:   rapid.IntRange(1, 8).Draw(t, "senders"),
	}
}

func yieldingDeleteBatch(
	latencies []int,
) func(context.Context, *sqs.DeleteMessageBatchInput, ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
	var calls atomic.Int64
	return func(_ context.Context, params *sqs.DeleteMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
		n := calls.Add(1) - 1
		for range latencies[int(n%int64(len(latencies)))] {
			runtime.Gosched()
		}
		out := &sqs.DeleteMessageBatchOutput{
			Successful: make([]types.DeleteMessageBatchResultEntry, 0, len(params.Entries)),
		}
		for _, entry := range params.Entries {
			out.Successful = append(out.Successful, types.DeleteMessageBatchResultEntry{Id: entry.Id})
		}
		return out, nil
	}
}

func runBatcher(ctx context.Context, client *fake.SQSClient, s batcherScenario) {
	b := consumer.NewDeleteBatcher(client, s.queueURL, s.capacity, logger.NewNoOp())
	b.Start(ctx, s.senders)

	var producers sync.WaitGroup
	for _, handles := range s.producers {
		producers.Go(func() {
			for _, h := range handles {
				b.Enqueue(h)
			}
		})
	}
	producers.Wait()
	b.Close()
}

func sentHandles(calls []*sqs.DeleteMessageBatchInput) []string {
	var out []string
	for _, call := range calls {
		for _, entry := range call.Entries {
			out = append(out, aws.ToString(entry.ReceiptHandle))
		}
	}
	return out
}

// Feature: opportunistic-delete-batch, Property 1: For any sequence of receipt handles enqueued concurrently by any number of producers, and any number of Senders ≥ 1, after close returns the multiset of ReceiptHandles across all DeleteMessageBatch requests equals the multiset of enqueued handles.
func TestDeleteBatcher_PropertyEveryHandleSentExactlyOnce(t *testing.T) {
	defer goleak.VerifyNone(t)

	rapid.Check(t, func(rt *rapid.T) {
		s := drawBatcherScenario(rt)
		client := &fake.SQSClient{DeleteMessageBatchFunc: yieldingDeleteBatch(s.latencies)}

		runBatcher(t.Context(), client, s)

		assert.ElementsMatch(rt, s.handles, sentHandles(client.DeleteMessageBatchCalls()))
	})
}

// Feature: opportunistic-delete-batch, Property 2: For any enqueue sequence and Sender count, every DeleteMessageBatch request has between 1 and 10 entries inclusive, entry Ids that are pairwise distinct and match ^[A-Za-z0-9_-]{1,80}$, QueueUrl equal to the route queue URL, and each entry ReceiptHandle equal to an enqueued handle.
func TestDeleteBatcher_PropertyEveryRequestWellFormed(t *testing.T) {
	defer goleak.VerifyNone(t)

	idPattern := regexp.MustCompile(`^[A-Za-z0-9_-]{1,80}$`)
	tests := []struct {
		name     string
		minCount int
		maxCount int
	}{
		{name: "one handle", minCount: 1, maxCount: 1},
		{name: "nine handles", minCount: 9, maxCount: 9},
		{name: "ten handles", minCount: 10, maxCount: 10},
		{name: "eleven handles", minCount: 11, maxCount: 11},
		{name: "twenty handles", minCount: 20, maxCount: 20},
		{name: "any number of handles", minCount: 0, maxCount: 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rapid.Check(t, func(rt *rapid.T) {
				s := drawBatcherScenarioOfSize(rt, tt.minCount, tt.maxCount)
				client := &fake.SQSClient{DeleteMessageBatchFunc: yieldingDeleteBatch(s.latencies)}

				runBatcher(t.Context(), client, s)

				enqueued := make(map[string]struct{}, len(s.handles))
				for _, h := range s.handles {
					enqueued[h] = struct{}{}
				}
				for _, call := range client.DeleteMessageBatchCalls() {
					assert.Equal(rt, s.queueURL, aws.ToString(call.QueueUrl))
					assert.GreaterOrEqual(rt, len(call.Entries), 1)
					assert.LessOrEqual(rt, len(call.Entries), consumer.MaxDeleteBatch)
					ids := make(map[string]struct{}, len(call.Entries))
					for _, entry := range call.Entries {
						id := aws.ToString(entry.Id)
						assert.Regexp(rt, idPattern, id)
						assert.NotContains(rt, ids, id)
						ids[id] = struct{}{}
						assert.Contains(rt, enqueued, aws.ToString(entry.ReceiptHandle))
					}
				}
			})
		})
	}
}

type deleteResponse struct {
	out  *sqs.DeleteMessageBatchOutput
	err  error
	want []map[string]any
}

func drawOptionalString(t *rapid.T, label string) *string {
	return rapid.OneOf(rapid.Just[*string](nil), rapid.Map(rapid.String(), aws.String)).Draw(t, label)
}

func drawUnknownID(t *rapid.T, n int, label string) *string {
	known := make(map[string]struct{}, n)
	for i := range n {
		known[strconv.Itoa(i)] = struct{}{}
	}
	id := rapid.OneOf(
		rapid.Just(""),
		rapid.StringMatching(`[A-Za-z_-][A-Za-z0-9_-]{0,15}`),
		rapid.Map(rapid.IntRange(n, 1<<20), strconv.Itoa),
		rapid.Map(rapid.IntRange(0, n-1), func(i int) string { return "0" + strconv.Itoa(i) }),
		rapid.Map(rapid.IntRange(0, n-1), func(i int) string { return "+" + strconv.Itoa(i) }),
		rapid.String().Filter(func(s string) bool {
			_, ok := known[s]
			return !ok
		}),
	).Draw(t, label)
	return rapid.OneOf(rapid.Just[*string](nil), rapid.Just(aws.String(id))).Draw(t, label+"Ptr")
}

func drawRequestError(t *rapid.T, queueURL string, batch []string) deleteResponse {
	err := errors.New(rapid.String().Draw(t, "requestError"))
	want := make([]map[string]any, len(batch))
	for i, handle := range batch {
		want[i] = map[string]any{"queue_url": queueURL, "receipt_handle": handle, "error": err}
	}
	return deleteResponse{err: err, want: want}
}

func drawNilOutput(_ *rapid.T, _ string, _ []string) deleteResponse {
	return deleteResponse{want: []map[string]any{}}
}

func drawFailedEntries(t *rapid.T, queueURL string, batch []string) deleteResponse {
	n := len(batch)
	failedIdx := rapid.SliceOfNDistinct(rapid.IntRange(0, n-1), 0, n, rapid.ID[int]).Draw(t, "failedIndexes")
	unknownCount := rapid.IntRange(0, 5).Draw(t, "unknownCount")

	failed := make([]types.BatchResultErrorEntry, 0, len(failedIdx)+unknownCount)
	isFailed := make(map[int]bool, len(failedIdx))
	for _, i := range failedIdx {
		isFailed[i] = true
		failed = append(failed, types.BatchResultErrorEntry{
			Id:          aws.String(strconv.Itoa(i)),
			Code:        drawOptionalString(t, "knownCode"),
			Message:     drawOptionalString(t, "knownMessage"),
			SenderFault: rapid.Bool().Draw(t, "knownSenderFault"),
		})
	}
	for range unknownCount {
		failed = append(failed, types.BatchResultErrorEntry{
			Id:          drawUnknownID(t, n, "unknownID"),
			Code:        drawOptionalString(t, "unknownCode"),
			Message:     drawOptionalString(t, "unknownMessage"),
			SenderFault: rapid.Bool().Draw(t, "unknownSenderFault"),
		})
	}
	failed = rapid.Permutation(failed).Draw(t, "failedOrder")

	successful := make([]types.DeleteMessageBatchResultEntry, 0, n-len(failedIdx))
	for i := range n {
		if !isFailed[i] {
			successful = append(successful, types.DeleteMessageBatchResultEntry{Id: aws.String(strconv.Itoa(i))})
		}
	}

	index := make(map[string]int, n)
	for i := range n {
		index[strconv.Itoa(i)] = i
	}
	want := make([]map[string]any, len(failed))
	for k, entry := range failed {
		attrs := map[string]any{
			"queue_url":    queueURL,
			"code":         aws.ToString(entry.Code),
			"sender_fault": entry.SenderFault,
			"error":        aws.ToString(entry.Message),
		}
		if i, ok := index[aws.ToString(entry.Id)]; ok && entry.Id != nil {
			attrs["receipt_handle"] = batch[i]
		} else {
			attrs["id"] = aws.ToString(entry.Id)
		}
		want[k] = attrs
	}

	return deleteResponse{
		out:  &sqs.DeleteMessageBatchOutput{Failed: failed, Successful: successful},
		want: want,
	}
}

// Feature: opportunistic-delete-batch, Property 5: For any batch and any generated response, consisting of a request error, a nil output, or an output whose Failed entries reference any subset of sent Ids plus arbitrary unknown Ids with nil or non-nil fields, send does not panic, logs exactly one Error record with message "failed to delete message" per failed entry (all entries on a request error), includes the receipt handle and queue URL for every known entry, and logs nothing for successful entries.
func TestDeleteBatcher_PropertyFailuresLoggedOncePerEntry(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		draw      func(*rapid.T, string, []string) deleteResponse
		name      string
		nilLogger bool
	}{
		{name: "request error", draw: drawRequestError},
		{name: "nil output", draw: drawNilOutput},
		{name: "failed entries", draw: drawFailedEntries},
		{name: "request error with nil logger", draw: drawRequestError, nilLogger: true},
		{name: "failed entries with nil logger", draw: drawFailedEntries, nilLogger: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rapid.Check(t, func(rt *rapid.T) {
				queueURL := rapid.String().Draw(rt, "queueURL")
				batch := rapid.SliceOfN(rapid.String(), 1, consumer.MaxDeleteBatch).Draw(rt, "batch")
				resp := tt.draw(rt, queueURL, batch)
				client := &fake.SQSClient{
					DeleteMessageBatchFunc: func(
						context.Context, *sqs.DeleteMessageBatchInput, ...func(*sqs.Options),
					) (*sqs.DeleteMessageBatchOutput, error) {
						return resp.out, resp.err
					},
				}
				handler := fake.NewLogHandler()
				var log *slog.Logger
				if !tt.nilLogger {
					log = slog.New(handler)
				}
				b := consumer.NewDeleteBatcher(client, queueURL, 1, log)

				assert.NotPanics(rt, func() { b.Send(t.Context(), batch) })

				assert.Len(rt, client.DeleteMessageBatchCalls(), 1)
				if tt.nilLogger {
					return
				}
				records := handler.Records()
				got := make([]map[string]any, len(records))
				for i, rec := range records {
					assert.Equal(rt, slog.LevelError, rec.Level)
					assert.Equal(rt, "failed to delete message", rec.Message)
					got[i] = rec.Attrs
				}
				assert.Equal(rt, resp.want, got)
			})
		})
	}
}

func gatedDeleteBatch(
	release <-chan struct{},
	inFlight *atomic.Int64,
) func(context.Context, *sqs.DeleteMessageBatchInput, ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
	return func(_ context.Context, params *sqs.DeleteMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
		inFlight.Add(1)
		defer inFlight.Add(-1)
		<-release
		out := &sqs.DeleteMessageBatchOutput{}
		for _, entry := range params.Entries {
			out.Successful = append(out.Successful, types.DeleteMessageBatchResultEntry{Id: entry.Id})
		}
		return out, nil
	}
}

func occupySenders(t *testing.T, b *consumer.DeleteBatcher, inFlight *atomic.Int64, senders int) []string {
	t.Helper()
	handles := make([]string, senders)
	for i := range senders {
		handles[i] = "busy-" + strconv.Itoa(i)
		b.Enqueue(handles[i])
		synctest.Wait()
		assert.Equal(t, int64(i+1), inFlight.Load())
	}
	return handles
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestDeleteBatcher_SendsWithoutWaitingForMoreHandles(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		name  string
		count int
	}{
		{name: "one handle", count: 1},
		{name: "two handles", count: 2},
		{name: "more handles than one batch holds", count: consumer.MaxDeleteBatch + 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				start := time.Now()
				observed := make(chan time.Time, tt.count)
				client := &fake.SQSClient{
					DeleteMessageBatchFunc: func(
						context.Context, *sqs.DeleteMessageBatchInput, ...func(*sqs.Options),
					) (*sqs.DeleteMessageBatchOutput, error) {
						observed <- time.Now()
						return &sqs.DeleteMessageBatchOutput{}, nil
					},
				}
				b := consumer.NewDeleteBatcher(client, "queue", tt.count, logger.NewNoOp())
				b.Start(context.Background(), 1)

				for i := range tt.count {
					handle := "handle-" + strconv.Itoa(i)
					b.Enqueue(handle)
					synctest.Wait()

					calls := client.DeleteMessageBatchCalls()
					if assert.Len(t, calls, i+1) {
						assert.Equal(t, []string{handle}, sentHandles(calls[i:]))
					}
					assert.Equal(t, start, <-observed)
				}

				b.Close()
				assert.Equal(t, start, time.Now())
			})
		})
	}
}

func TestDeleteBatcher_RunsExactlySendersConcurrentRequests(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		name    string
		senders int
	}{
		{name: "one sender", senders: 1},
		{name: "two senders", senders: 2},
		{name: "four senders", senders: 4},
		{name: "sixteen senders", senders: 16},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				var inFlight atomic.Int64
				client := &fake.SQSClient{DeleteMessageBatchFunc: gatedDeleteBatch(release, &inFlight)}
				capacity := tt.senders * consumer.MaxDeleteBatch
				b := consumer.NewDeleteBatcher(client, "queue", capacity, logger.NewNoOp())
				b.Start(context.Background(), tt.senders)

				want := occupySenders(t, b, &inFlight, tt.senders)
				for i := range capacity {
					handle := "queued-" + strconv.Itoa(i)
					want = append(want, handle)
					b.Enqueue(handle)
				}
				synctest.Wait()

				assert.Equal(t, int64(tt.senders), inFlight.Load())
				assert.Len(t, client.DeleteMessageBatchCalls(), tt.senders)

				close(release)
				b.Close()

				calls := client.DeleteMessageBatchCalls()
				assert.Zero(t, inFlight.Load())
				assert.ElementsMatch(t, want, sentHandles(calls))
				drained := calls[min(tt.senders, len(calls)):]
				batches := capacity / consumer.MaxDeleteBatch
				if tt.senders == 1 {
					assert.Len(t, calls, 1+batches)
					for _, call := range drained {
						assert.Len(t, call.Entries, consumer.MaxDeleteBatch)
					}
					return
				}
				assert.GreaterOrEqual(t, len(drained), batches)
				assert.LessOrEqual(t, len(drained), batches+tt.senders-1)
			})
		})
	}
}

func requestSizes(calls []*sqs.DeleteMessageBatchInput) []int {
	sizes := make([]int, len(calls))
	for i, call := range calls {
		sizes[i] = len(call.Entries)
	}
	return sizes
}

func TestDeleteBatcher_DrainsQueuedHandlesIntoFullBatches(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		name   string
		want   []int
		queued int
	}{
		{name: "one queued handle", queued: 1, want: []int{1}},
		{name: "nine queued handles", queued: 9, want: []int{9}},
		{name: "ten queued handles", queued: 10, want: []int{10}},
		{name: "eleven queued handles", queued: 11, want: []int{10, 1}},
		{name: "twenty queued handles", queued: 20, want: []int{10, 10}},
		{name: "twenty one queued handles", queued: 21, want: []int{10, 10, 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				var inFlight atomic.Int64
				client := &fake.SQSClient{DeleteMessageBatchFunc: gatedDeleteBatch(release, &inFlight)}
				b := consumer.NewDeleteBatcher(client, "queue", tt.queued, logger.NewNoOp())
				b.Start(context.Background(), 1)

				busy := occupySenders(t, b, &inFlight, 1)
				queued := make([]string, tt.queued)
				for i := range queued {
					queued[i] = "queued-" + strconv.Itoa(i)
					b.Enqueue(queued[i])
				}
				synctest.Wait()

				close(release)
				b.Close()

				calls := client.DeleteMessageBatchCalls()
				if assert.Len(t, calls, 1+len(tt.want)) {
					assert.Equal(t, busy, sentHandles(calls[:1]))
					assert.Equal(t, tt.want, requestSizes(calls[1:]))
					assert.Equal(t, queued, sentHandles(calls[1:]))
				}
			})
		})
	}
}

func TestDeleteBatcher_EnqueueBlocksOnlyWhenQueueIsFull(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		name     string
		senders  int
		capacity int
	}{
		{name: "one sender with capacity one", senders: 1, capacity: 1},
		{name: "one sender with capacity of one batch", senders: 1, capacity: consumer.MaxDeleteBatch},
		{name: "two senders with small capacity", senders: 2, capacity: 3},
		{name: "four senders with capacity above one batch", senders: 4, capacity: 25},
		{name: "eight senders with large capacity", senders: 8, capacity: 64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				var inFlight atomic.Int64
				client := &fake.SQSClient{DeleteMessageBatchFunc: gatedDeleteBatch(release, &inFlight)}
				b := consumer.NewDeleteBatcher(client, "queue", tt.capacity, logger.NewNoOp())
				b.Start(context.Background(), tt.senders)

				want := occupySenders(t, b, &inFlight, tt.senders)
				queued := make([]string, tt.capacity)
				for i := range queued {
					queued[i] = "queued-" + strconv.Itoa(i)
				}
				want = append(want, queued...)
				filled := make(chan struct{})
				go func() {
					defer close(filled)
					for _, h := range queued {
						b.Enqueue(h)
					}
				}()
				synctest.Wait()
				assert.True(t, isClosed(filled))

				overflow := "overflow"
				want = append(want, overflow)
				overflowed := make(chan struct{})
				go func() {
					defer close(overflowed)
					b.Enqueue(overflow)
				}()
				synctest.Wait()
				assert.False(t, isClosed(overflowed))
				assert.Equal(t, int64(tt.senders), inFlight.Load())

				release <- struct{}{}
				synctest.Wait()
				assert.True(t, isClosed(overflowed))

				close(release)
				<-filled
				<-overflowed
				b.Close()

				assert.ElementsMatch(t, want, sentHandles(client.DeleteMessageBatchCalls()))
			})
		})
	}
}

func TestDeleteBatcher_RequestContextIsBoundedAndDetached(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		parent func() context.Context
		name   string
	}{
		{name: "background context", parent: context.Background},
		{
			name: "detached from a canceled context",
			parent: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return context.WithoutCancel(ctx)
			},
		},
		{
			name: "detached from an expired context",
			parent: func() context.Context {
				ctx, cancel := context.WithDeadline(context.Background(), time.Now())
				defer cancel()
				return context.WithoutCancel(ctx)
			},
		},
	}

	type observation struct {
		deadline    time.Time
		at          time.Time
		errAtCall   error
		errAtExpiry error
		elapsed     time.Duration
		hasDeadline bool
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				observed := make(chan observation, 1)
				client := &fake.SQSClient{
					DeleteMessageBatchFunc: func(
						ctx context.Context, _ *sqs.DeleteMessageBatchInput, _ ...func(*sqs.Options),
					) (*sqs.DeleteMessageBatchOutput, error) {
						var o observation
						o.at = time.Now()
						o.deadline, o.hasDeadline = ctx.Deadline()
						o.errAtCall = ctx.Err()
						<-ctx.Done()
						o.elapsed = time.Since(o.at)
						o.errAtExpiry = ctx.Err()
						observed <- o
						return nil, ctx.Err()
					},
				}
				b := consumer.NewDeleteBatcher(client, "queue", 1, logger.NewNoOp())
				b.Start(tt.parent(), 1)

				b.Enqueue("handle")
				b.Close()

				o := <-observed
				assert.True(t, o.hasDeadline)
				assert.Positive(t, o.deadline.Sub(o.at))
				assert.LessOrEqual(t, o.deadline.Sub(o.at), consumer.DeleteRequestTimeout)
				assert.NoError(t, o.errAtCall)
				assert.Equal(t, consumer.DeleteRequestTimeout, o.elapsed)
				assert.ErrorIs(t, o.errAtExpiry, context.DeadlineExceeded)
			})
		})
	}
}

func TestDeleteBatcher_FailedDeleteIsNotRetried(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		respond func(*sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error)
		name    string
	}{
		{
			name: "request error",
			respond: func(*sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error) {
				return nil, errors.New("request failed")
			},
		},
		{
			name: "every entry failed",
			respond: func(params *sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error) {
				out := &sqs.DeleteMessageBatchOutput{}
				for _, entry := range params.Entries {
					out.Failed = append(out.Failed, types.BatchResultErrorEntry{
						Id:          entry.Id,
						Code:        aws.String("ReceiptHandleIsInvalid"),
						SenderFault: true,
					})
				}
				return out, nil
			},
		},
		{
			name: "first entry failed",
			respond: func(params *sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error) {
				out := &sqs.DeleteMessageBatchOutput{
					Failed: []types.BatchResultErrorEntry{{Id: params.Entries[0].Id, Code: aws.String("InternalError")}},
				}
				for _, entry := range params.Entries[1:] {
					out.Successful = append(out.Successful, types.DeleteMessageBatchResultEntry{Id: entry.Id})
				}
				return out, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rapid.Check(t, func(rt *rapid.T) {
				s := drawBatcherScenarioOfSize(rt, 1, 50)
				client := &fake.SQSClient{
					DeleteMessageBatchFunc: func(
						_ context.Context, params *sqs.DeleteMessageBatchInput, _ ...func(*sqs.Options),
					) (*sqs.DeleteMessageBatchOutput, error) {
						return tt.respond(params)
					},
				}

				runBatcher(t.Context(), client, s)

				assert.ElementsMatch(rt, s.handles, sentHandles(client.DeleteMessageBatchCalls()))
			})
		})
	}
}
