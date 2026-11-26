package consumer_test

import (
	"context"
	"errors"
	"log/slog"
	"regexp"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/consumer"
	"github.com/silviolleite/loafer-awsx/fake"
)

const (
	visibilityQueueURL   = "https://sqs.us-east-1.amazonaws.com/123456789012/orders"
	maxVisibilityTimeout = 43200
	visibilityFailureMsg = "failed to change message visibility"
)

var (
	errVisibilityRequest = errors.New("visibility request failed")
	batchEntryIDPattern  = regexp.MustCompile(`^[A-Za-z0-9_-]{1,80}$`)
)

type visibilityPair struct {
	handle  string
	timeout int32
}

func visibilityMessages(handles ...string) []*consumer.TestMessage {
	msgs := make([]*consumer.TestMessage, len(handles))
	for i, h := range handles {
		msgs[i] = consumer.NewTestMessage(h, "", nil, "body-"+h)
	}
	return msgs
}

func visibilityTimeouts(n int) []int32 {
	timeouts := make([]int32, n)
	for i := range timeouts {
		timeouts[i] = int32(i * 30)
	}
	return timeouts
}

func drawVisibilityChanges(t *rapid.T) ([]*consumer.TestMessage, []int32) {
	handles := rapid.SliceOfNDistinct(rapid.StringN(1, 64, -1), 1, consumer.MaxVisibilityBatch, rapid.ID[string]).Draw(t, "handles")
	timeouts := rapid.SliceOfN(rapid.Int32Range(0, maxVisibilityTimeout), len(handles), len(handles)).Draw(t, "timeouts")
	return visibilityMessages(handles...), timeouts
}

func expectedPairs(msgs []*consumer.TestMessage, timeouts []int32) map[visibilityPair]int {
	pairs := make(map[visibilityPair]int, len(msgs))
	for i, m := range msgs {
		pairs[visibilityPair{handle: m.Identifier(), timeout: timeouts[i]}]++
	}
	return pairs
}

func failedBatchOutput(entries ...types.BatchResultErrorEntry) func(
	context.Context, *sqs.ChangeMessageVisibilityBatchInput, ...func(*sqs.Options),
) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
	return func(context.Context, *sqs.ChangeMessageVisibilityBatchInput, ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
		return &sqs.ChangeMessageVisibilityBatchOutput{Failed: entries}, nil
	}
}

func visibilityFailureRecords(h *fake.LogHandler) []fake.LogRecord {
	var out []fake.LogRecord
	for _, r := range h.Records() {
		if r.Message == visibilityFailureMsg {
			out = append(out, r)
		}
	}
	return out
}

func assertVisibilityResults(t *testing.T, want, got []consumer.VisibilityResult) {
	t.Helper()
	require.Len(t, got, len(want))
	for i := range want {
		if want[i].Err != nil {
			assert.ErrorIs(t, got[i].Err, want[i].Err, "result %d", i)
		} else {
			assert.NoError(t, got[i].Err, "result %d", i)
		}
		w, g := want[i], got[i]
		w.Err, g.Err = nil, nil
		assert.Equal(t, w, g, "result %d", i)
	}
}

// Feature: consumer-flow-control, Property 15: For any set of due changes, every ChangeMessageVisibilityBatch request has 1-10 entries, unique ids matching ^[A-Za-z0-9_-]{1,80}$, and the route queue URL, and every due change appears in exactly one entry.
func TestVisibilitySender_BatchShape(t *testing.T) {
	t.Run("batch", func(t *testing.T) {
		rapid.Check(t, func(rt *rapid.T) {
			queueURL := rapid.StringN(1, 128, -1).Draw(rt, "queueURL")
			msgs, timeouts := drawVisibilityChanges(rt)
			client := &fake.SQSClient{ChangeMessageVisibilityBatchFunc: succeedVisibilityBatch}
			sender := consumer.NewVisibilitySender(client, queueURL, nil)

			results := sender.Send(context.Background(), msgs, timeouts)

			calls := client.ChangeMessageVisibilityBatchCalls()
			require.Len(rt, calls, 1)
			assert.Empty(rt, client.ChangeMessageVisibilityCalls())
			req := calls[0]
			assert.Equal(rt, queueURL, aws.ToString(req.QueueUrl))
			require.NotEmpty(rt, req.Entries)
			assert.LessOrEqual(rt, len(req.Entries), consumer.MaxVisibilityBatch)

			ids := make(map[string]struct{}, len(req.Entries))
			got := make(map[visibilityPair]int, len(req.Entries))
			for _, e := range req.Entries {
				id := aws.ToString(e.Id)
				assert.Regexp(rt, batchEntryIDPattern, id)
				assert.NotContains(rt, ids, id)
				ids[id] = struct{}{}
				got[visibilityPair{handle: aws.ToString(e.ReceiptHandle), timeout: e.VisibilityTimeout}]++
			}
			assert.Equal(rt, expectedPairs(msgs, timeouts), got)
			assert.Equal(rt, make([]consumer.VisibilityResult, len(msgs)), results)
		})
	})

	t.Run("single", func(t *testing.T) {
		rapid.Check(t, func(rt *rapid.T) {
			queueURL := rapid.StringN(1, 128, -1).Draw(rt, "queueURL")
			msgs, timeouts := drawVisibilityChanges(rt)
			client := &fake.SQSClient{}
			sender := consumer.NewVisibilitySender(standardClient(client), queueURL, nil)

			results := sender.Send(context.Background(), msgs, timeouts)

			calls := client.ChangeMessageVisibilityCalls()
			require.Len(rt, calls, len(msgs))
			assert.Empty(rt, client.ChangeMessageVisibilityBatchCalls())
			got := make(map[visibilityPair]int, len(calls))
			for _, c := range calls {
				assert.Equal(rt, queueURL, aws.ToString(c.QueueUrl))
				got[visibilityPair{handle: aws.ToString(c.ReceiptHandle), timeout: c.VisibilityTimeout}]++
			}
			assert.Equal(rt, expectedPairs(msgs, timeouts), got)
			assert.Equal(rt, make([]consumer.VisibilityResult, len(msgs)), results)
		})
	})
}

func TestVisibilitySender_IsBatch(t *testing.T) {
	tests := []struct {
		client consumer.SQSClient
		name   string
		want   bool
	}{
		{name: "batch capable client", client: &fake.SQSClient{}, want: true},
		{name: "sqs only client", client: standardClient(&fake.SQSClient{}), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, consumer.NewVisibilitySender(tt.client, visibilityQueueURL, nil).IsBatch())
		})
	}
}

func TestVisibilitySender_EmptyInput(t *testing.T) {
	tests := []struct {
		wrap func(*fake.SQSClient) consumer.SQSClient
		msgs []*consumer.TestMessage
		name string
	}{
		{name: "batch nil", wrap: batchCapableClient},
		{name: "batch empty", wrap: batchCapableClient, msgs: []*consumer.TestMessage{}},
		{name: "single nil", wrap: standardClient},
		{name: "single empty", wrap: standardClient, msgs: []*consumer.TestMessage{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &fake.SQSClient{}
			sender := consumer.NewVisibilitySender(tt.wrap(client), visibilityQueueURL, nil)

			results := sender.Send(context.Background(), tt.msgs, nil)

			assert.Empty(t, results)
			assert.Empty(t, client.ChangeMessageVisibilityBatchCalls())
			assert.Empty(t, client.ChangeMessageVisibilityCalls())
		})
	}
}

func TestVisibilitySender_BatchResponses(t *testing.T) {
	requestFailed := consumer.VisibilityResult{Failed: true, Err: errVisibilityRequest}

	tests := []struct {
		respond func(context.Context, *sqs.ChangeMessageVisibilityBatchInput, ...func(*sqs.Options)) (
			*sqs.ChangeMessageVisibilityBatchOutput, error)
		name       string
		want       []consumer.VisibilityResult
		loggedIDs  []string
		loggedCode []string
	}{
		{
			name: "request error fails every change",
			respond: func(context.Context, *sqs.ChangeMessageVisibilityBatchInput, ...func(*sqs.Options)) (
				*sqs.ChangeMessageVisibilityBatchOutput, error) {
				return nil, errVisibilityRequest
			},
			want: []consumer.VisibilityResult{requestFailed, requestFailed, requestFailed},
		},
		{
			name: "nil output and nil error succeed",
			respond: func(context.Context, *sqs.ChangeMessageVisibilityBatchInput, ...func(*sqs.Options)) (
				*sqs.ChangeMessageVisibilityBatchOutput, error) {
				return nil, nil
			},
			want: make([]consumer.VisibilityResult, 3),
		},
		{
			name:    "empty failed list succeeds",
			respond: failedBatchOutput(),
			want:    make([]consumer.VisibilityResult, 3),
		},
		{
			name: "failed entries map back by id",
			respond: failedBatchOutput(
				types.BatchResultErrorEntry{
					Id: aws.String("2"), Code: aws.String("ReceiptHandleIsInvalid"), Message: aws.String("bad handle"), SenderFault: true,
				},
				types.BatchResultErrorEntry{Id: aws.String("0"), Code: aws.String("InternalError")},
			),
			want: []consumer.VisibilityResult{
				{Failed: true, Code: "InternalError", PerEntry: true},
				{},
				{Failed: true, Code: "ReceiptHandleIsInvalid", Message: "bad handle", SenderFault: true, PerEntry: true},
			},
		},
		{
			name: "out of range id is logged and ignored",
			respond: failedBatchOutput(
				types.BatchResultErrorEntry{Id: aws.String("99"), Code: aws.String("InternalError")},
			),
			want:       make([]consumer.VisibilityResult, 3),
			loggedIDs:  []string{"99"},
			loggedCode: []string{"InternalError"},
		},
		{
			name: "non numeric id is logged and ignored",
			respond: failedBatchOutput(
				types.BatchResultErrorEntry{Id: aws.String("x"), Code: aws.String("InternalError")},
			),
			want:       make([]consumer.VisibilityResult, 3),
			loggedIDs:  []string{"x"},
			loggedCode: []string{"InternalError"},
		},
		{
			name: "non canonical id is logged and ignored",
			respond: failedBatchOutput(
				types.BatchResultErrorEntry{Id: aws.String("01"), Code: aws.String("InternalError")},
			),
			want:       make([]consumer.VisibilityResult, 3),
			loggedIDs:  []string{"01"},
			loggedCode: []string{"InternalError"},
		},
		{
			name: "negative id is logged and ignored",
			respond: failedBatchOutput(
				types.BatchResultErrorEntry{Id: aws.String("-1")},
			),
			want:       make([]consumer.VisibilityResult, 3),
			loggedIDs:  []string{"-1"},
			loggedCode: []string{""},
		},
		{
			name:       "nil id is logged and ignored",
			respond:    failedBatchOutput(types.BatchResultErrorEntry{}),
			want:       make([]consumer.VisibilityResult, 3),
			loggedIDs:  []string{""},
			loggedCode: []string{""},
		},
		{
			name: "valid and invalid ids are handled independently",
			respond: failedBatchOutput(
				types.BatchResultErrorEntry{Id: aws.String("3"), Code: aws.String("A")},
				types.BatchResultErrorEntry{Id: aws.String("1"), Code: aws.String("B"), Message: aws.String("m")},
				types.BatchResultErrorEntry{Id: aws.String("x"), Code: aws.String("C")},
			),
			want: []consumer.VisibilityResult{
				{},
				{Failed: true, Code: "B", Message: "m", PerEntry: true},
				{},
			},
			loggedIDs:  []string{"3", "x"},
			loggedCode: []string{"A", "C"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &fake.SQSClient{ChangeMessageVisibilityBatchFunc: tt.respond}
			logs := fake.NewLogHandler()
			sender := consumer.NewVisibilitySender(client, visibilityQueueURL, slog.New(logs))
			msgs := visibilityMessages("a", "b", "c")

			results := sender.Send(context.Background(), msgs, visibilityTimeouts(len(msgs)))

			assertVisibilityResults(t, tt.want, results)
			require.Len(t, client.ChangeMessageVisibilityBatchCalls(), 1)

			records := visibilityFailureRecords(logs)
			require.Len(t, records, len(tt.loggedIDs))
			for i, r := range records {
				assert.Equal(t, slog.LevelError, r.Level)
				assert.Equal(t, tt.loggedIDs[i], r.Attrs["id"])
				assert.Equal(t, tt.loggedCode[i], r.Attrs["code"])
				assert.Equal(t, visibilityQueueURL, r.Attrs["queue_url"])
			}
		})
	}
}

func TestVisibilitySender_BatchFailedEntriesProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		msgs, timeouts := drawVisibilityChanges(rt)
		n := len(msgs)
		failedIdx := rapid.SliceOfDistinct(rapid.IntRange(0, n-1), rapid.ID[int]).Draw(rt, "failedIdx")
		unknownIDs := rapid.SliceOfN(rapid.SampledFrom([]string{"99", "x", "01", "-1", "", "10"}), 0, 5).Draw(rt, "unknownIDs")

		want := make([]consumer.VisibilityResult, n)
		var failed []types.BatchResultErrorEntry
		for _, i := range failedIdx {
			code := rapid.StringN(0, 16, -1).Draw(rt, "code")
			msg := rapid.StringN(0, 32, -1).Draw(rt, "message")
			senderFault := rapid.Bool().Draw(rt, "senderFault")
			failed = append(failed, types.BatchResultErrorEntry{
				Id: aws.String(strconv.Itoa(i)), Code: aws.String(code), Message: aws.String(msg), SenderFault: senderFault,
			})
			want[i] = consumer.VisibilityResult{Failed: true, Code: code, Message: msg, SenderFault: senderFault, PerEntry: true}
		}
		for _, id := range unknownIDs {
			failed = append(failed, types.BatchResultErrorEntry{Id: aws.String(id)})
		}
		failed = rapid.Permutation(failed).Draw(rt, "order")

		client := &fake.SQSClient{ChangeMessageVisibilityBatchFunc: failedBatchOutput(failed...)}
		logs := fake.NewLogHandler()
		sender := consumer.NewVisibilitySender(client, visibilityQueueURL, slog.New(logs))

		results := sender.Send(context.Background(), msgs, timeouts)

		assert.Equal(rt, want, results)
		assert.Len(rt, visibilityFailureRecords(logs), len(unknownIDs))
	})
}

func TestVisibilitySender_SingleResponses(t *testing.T) {
	tests := []struct {
		failing map[string]error
		name    string
		want    []consumer.VisibilityResult
	}{
		{
			name: "every call succeeds",
			want: make([]consumer.VisibilityResult, 3),
		},
		{
			name:    "one call fails",
			failing: map[string]error{"b": errVisibilityRequest},
			want:    []consumer.VisibilityResult{{}, {Failed: true, Err: errVisibilityRequest}, {}},
		},
		{
			name:    "every call fails",
			failing: map[string]error{"a": errVisibilityRequest, "b": errVisibilityRequest, "c": errVisibilityRequest},
			want: []consumer.VisibilityResult{
				{Failed: true, Err: errVisibilityRequest},
				{Failed: true, Err: errVisibilityRequest},
				{Failed: true, Err: errVisibilityRequest},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &fake.SQSClient{
				ChangeMessageVisibilityFunc: func(_ context.Context, in *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (
					*sqs.ChangeMessageVisibilityOutput, error) {
					return &sqs.ChangeMessageVisibilityOutput{}, tt.failing[aws.ToString(in.ReceiptHandle)]
				},
			}
			logs := fake.NewLogHandler()
			sender := consumer.NewVisibilitySender(standardClient(client), visibilityQueueURL, slog.New(logs))
			msgs := visibilityMessages("a", "b", "c")

			results := sender.Send(context.Background(), msgs, visibilityTimeouts(len(msgs)))

			assertVisibilityResults(t, tt.want, results)
			assert.Len(t, client.ChangeMessageVisibilityCalls(), len(msgs))
			assert.Empty(t, client.ChangeMessageVisibilityBatchCalls())
		})
	}
}

func TestVisibilitySender_RequestDeadline(t *testing.T) {
	tests := []struct {
		name           string
		wrap           func(*fake.SQSClient) consumer.SQSClient
		parentDeadline time.Duration
		want           time.Duration
	}{
		{name: "batch without parent deadline", wrap: batchCapableClient, want: consumer.VisibilityRequestTimeout},
		{
			name: "batch with longer parent deadline", wrap: batchCapableClient,
			parentDeadline: 2 * consumer.VisibilityRequestTimeout, want: consumer.VisibilityRequestTimeout,
		},
		{
			name: "batch with shorter parent deadline", wrap: batchCapableClient,
			parentDeadline: time.Second, want: time.Second,
		},
		{name: "single without parent deadline", wrap: standardClient, want: consumer.VisibilityRequestTimeout},
		{
			name: "single with longer parent deadline", wrap: standardClient,
			parentDeadline: 2 * consumer.VisibilityRequestTimeout, want: consumer.VisibilityRequestTimeout,
		},
		{
			name: "single with shorter parent deadline", wrap: standardClient,
			parentDeadline: time.Second, want: time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var mu sync.Mutex
				var remaining []time.Duration
				var hasDeadline []bool
				record := func(ctx context.Context) {
					d, ok := ctx.Deadline()
					mu.Lock()
					defer mu.Unlock()
					hasDeadline = append(hasDeadline, ok)
					remaining = append(remaining, time.Until(d))
				}
				client := &fake.SQSClient{
					ChangeMessageVisibilityBatchFunc: func(ctx context.Context, _ *sqs.ChangeMessageVisibilityBatchInput, _ ...func(*sqs.Options)) (
						*sqs.ChangeMessageVisibilityBatchOutput, error) {
						record(ctx)
						return nil, nil
					},
					ChangeMessageVisibilityFunc: func(ctx context.Context, _ *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (
						*sqs.ChangeMessageVisibilityOutput, error) {
						record(ctx)
						return nil, nil
					},
				}
				ctx := context.Background()
				if tt.parentDeadline > 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, tt.parentDeadline)
					defer cancel()
				}
				sender := consumer.NewVisibilitySender(tt.wrap(client), visibilityQueueURL, nil)
				msgs := visibilityMessages("a", "b")

				sender.Send(ctx, msgs, visibilityTimeouts(len(msgs)))

				require.NotEmpty(t, remaining)
				for i := range remaining {
					assert.True(t, hasDeadline[i])
					assert.Equal(t, tt.want, remaining[i])
					assert.LessOrEqual(t, remaining[i], consumer.VisibilityRequestTimeout)
				}
			})
		})
	}
}

func TestVisibilitySender_CanceledContext(t *testing.T) {
	tests := []struct {
		wrap func(*fake.SQSClient) consumer.SQSClient
		name string
	}{
		{name: "batch", wrap: batchCapableClient},
		{name: "single", wrap: standardClient},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &fake.SQSClient{
				ChangeMessageVisibilityBatchFunc: func(ctx context.Context, _ *sqs.ChangeMessageVisibilityBatchInput, _ ...func(*sqs.Options)) (
					*sqs.ChangeMessageVisibilityBatchOutput, error) {
					return nil, ctx.Err()
				},
				ChangeMessageVisibilityFunc: func(ctx context.Context, _ *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (
					*sqs.ChangeMessageVisibilityOutput, error) {
					return nil, ctx.Err()
				},
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			sender := consumer.NewVisibilitySender(tt.wrap(client), visibilityQueueURL, nil)
			msgs := visibilityMessages("a", "b")

			results := sender.Send(ctx, msgs, visibilityTimeouts(len(msgs)))

			assertVisibilityResults(t, []consumer.VisibilityResult{
				{Failed: true, Err: context.Canceled},
				{Failed: true, Err: context.Canceled},
			}, results)
		})
	}
}

func TestVisibilitySender_NilLogger(t *testing.T) {
	client := &fake.SQSClient{
		ChangeMessageVisibilityBatchFunc: failedBatchOutput(
			types.BatchResultErrorEntry{Id: aws.String("x"), Code: aws.String("InternalError")},
			types.BatchResultErrorEntry{Id: aws.String("0"), Code: aws.String("InternalError")},
		),
	}
	sender := consumer.NewVisibilitySender(client, visibilityQueueURL, nil)
	msgs := visibilityMessages("a", "b")

	var results []consumer.VisibilityResult
	require.NotPanics(t, func() {
		results = sender.Send(context.Background(), msgs, visibilityTimeouts(len(msgs)))
	})

	assertVisibilityResults(t, []consumer.VisibilityResult{
		{Failed: true, Code: "InternalError", PerEntry: true},
		{},
	}, results)
}
