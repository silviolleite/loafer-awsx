package fake_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/fake"
)

var errDeleteBatch = errors.New("delete batch failed")

func drawBatchInput(t *rapid.T) *sqs.DeleteMessageBatchInput {
	ids := rapid.SliceOfNDistinct(rapid.StringMatching(`[A-Za-z0-9_-]{1,80}`), 0, 10, rapid.ID[string]).Draw(t, "ids")
	entries := make([]types.DeleteMessageBatchRequestEntry, len(ids))
	for i, id := range ids {
		entries[i] = types.DeleteMessageBatchRequestEntry{
			Id:            aws.String(id),
			ReceiptHandle: aws.String(rapid.String().Draw(t, "receiptHandle")),
		}
	}
	return &sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(rapid.String().Draw(t, "queueURL")),
		Entries:  entries,
	}
}

func entryIDs(entries []types.DeleteMessageBatchRequestEntry) []string {
	out := make([]string, len(entries))
	for i, entry := range entries {
		out[i] = aws.ToString(entry.Id)
	}
	return out
}

func resultIDs(entries []types.DeleteMessageBatchResultEntry) []string {
	out := make([]string, len(entries))
	for i, entry := range entries {
		out[i] = aws.ToString(entry.Id)
	}
	return out
}

func TestSQSClient_DeleteMessageBatchDefaultReportsEverySuccess(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		client := &fake.SQSClient{}
		in := drawBatchInput(rt)

		out, err := client.DeleteMessageBatch(context.Background(), in)

		require.NoError(rt, err)
		require.NotNil(rt, out)
		assert.Equal(rt, entryIDs(in.Entries), resultIDs(out.Successful))
		assert.Empty(rt, out.Failed)
		assert.Equal(rt, []*sqs.DeleteMessageBatchInput{in}, client.DeleteMessageBatchCalls())
	})
}

func TestSQSClient_DeleteMessageBatchDefaultEdgeInputs(t *testing.T) {
	tests := []struct {
		in   *sqs.DeleteMessageBatchInput
		name string
	}{
		{name: "nil input"},
		{name: "no entries", in: &sqs.DeleteMessageBatchInput{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &fake.SQSClient{}

			out, err := client.DeleteMessageBatch(context.Background(), tt.in)

			require.NoError(t, err)
			require.NotNil(t, out)
			assert.Empty(t, out.Successful)
			assert.Empty(t, out.Failed)
			assert.Equal(t, []*sqs.DeleteMessageBatchInput{tt.in}, client.DeleteMessageBatchCalls())
		})
	}
}

func TestSQSClient_DeleteMessageBatchFuncOverridesDefault(t *testing.T) {
	type key struct{}
	failed := &sqs.DeleteMessageBatchOutput{
		Failed: []types.BatchResultErrorEntry{{Id: aws.String("0"), Code: aws.String("InternalError")}},
	}

	tests := []struct {
		out    *sqs.DeleteMessageBatchOutput
		err    error
		name   string
		optFns int
	}{
		{name: "error", err: errDeleteBatch},
		{name: "nil output", optFns: 1},
		{name: "failed entries", out: failed, optFns: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), key{}, tt.name)
			in := &sqs.DeleteMessageBatchInput{
				QueueUrl: aws.String("queue"),
				Entries:  []types.DeleteMessageBatchRequestEntry{{Id: aws.String("0"), ReceiptHandle: aws.String("handle")}},
			}
			optFns := make([]func(*sqs.Options), tt.optFns)
			for i := range optFns {
				optFns[i] = func(*sqs.Options) {}
			}
			var gotCtx context.Context
			var gotIn *sqs.DeleteMessageBatchInput
			var gotOptFns int
			client := &fake.SQSClient{
				DeleteMessageBatchFunc: func(
					ctx context.Context, params *sqs.DeleteMessageBatchInput, optFns ...func(*sqs.Options),
				) (*sqs.DeleteMessageBatchOutput, error) {
					gotCtx, gotIn, gotOptFns = ctx, params, len(optFns)
					return tt.out, tt.err
				},
			}

			out, err := client.DeleteMessageBatch(ctx, in, optFns...)

			assert.ErrorIs(t, err, tt.err)
			assert.Same(t, tt.out, out)
			assert.Equal(t, tt.name, gotCtx.Value(key{}))
			assert.Same(t, in, gotIn)
			assert.Equal(t, tt.optFns, gotOptFns)
			assert.Equal(t, []*sqs.DeleteMessageBatchInput{in}, client.DeleteMessageBatchCalls())
		})
	}
}

func TestSQSClient_DeleteMessageBatchCallsReturnsCopy(t *testing.T) {
	client := &fake.SQSClient{}
	first := &sqs.DeleteMessageBatchInput{QueueUrl: aws.String("first")}
	mutated := &sqs.DeleteMessageBatchInput{QueueUrl: aws.String("mutated")}
	second := &sqs.DeleteMessageBatchInput{QueueUrl: aws.String("second")}
	_, err := client.DeleteMessageBatch(context.Background(), first)
	require.NoError(t, err)

	calls := client.DeleteMessageBatchCalls()
	require.Len(t, calls, 1)
	calls[0] = mutated

	_, err = client.DeleteMessageBatch(context.Background(), second)
	require.NoError(t, err)

	assert.Equal(t, []*sqs.DeleteMessageBatchInput{mutated}, calls)
	assert.Equal(t, []*sqs.DeleteMessageBatchInput{first, second}, client.DeleteMessageBatchCalls())
}

func TestSQSClient_DeleteMessageBatchRecordsConcurrentCalls(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		name       string
		goroutines int
	}{
		{name: "one goroutine", goroutines: 1},
		{name: "eight goroutines", goroutines: 8},
		{name: "sixty four goroutines", goroutines: 64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &fake.SQSClient{}
			inputs := make([]*sqs.DeleteMessageBatchInput, tt.goroutines)
			for i := range inputs {
				inputs[i] = &sqs.DeleteMessageBatchInput{QueueUrl: aws.String("queue-" + strconv.Itoa(i))}
			}

			var wg sync.WaitGroup
			for _, in := range inputs {
				wg.Go(func() {
					_, err := client.DeleteMessageBatch(context.Background(), in)
					assert.NoError(t, err)
				})
			}
			wg.Wait()

			assert.ElementsMatch(t, inputs, client.DeleteMessageBatchCalls())
		})
	}
}

var errVisibilityBatch = errors.New("change visibility batch failed")

func drawVisibilityBatchInput(t *rapid.T) *sqs.ChangeMessageVisibilityBatchInput {
	ids := rapid.SliceOfNDistinct(rapid.StringMatching(`[A-Za-z0-9_-]{1,80}`), 0, 10, rapid.ID[string]).Draw(t, "ids")
	entries := make([]types.ChangeMessageVisibilityBatchRequestEntry, len(ids))
	for i, id := range ids {
		entries[i] = types.ChangeMessageVisibilityBatchRequestEntry{
			Id:                aws.String(id),
			ReceiptHandle:     aws.String(rapid.String().Draw(t, "receiptHandle")),
			VisibilityTimeout: rapid.Int32Range(0, 43200).Draw(t, "visibilityTimeout"),
		}
	}
	return &sqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: aws.String(rapid.String().Draw(t, "queueURL")),
		Entries:  entries,
	}
}

func visibilityEntryIDs(entries []types.ChangeMessageVisibilityBatchRequestEntry) []string {
	out := make([]string, len(entries))
	for i, entry := range entries {
		out[i] = aws.ToString(entry.Id)
	}
	return out
}

func visibilityResultIDs(entries []types.ChangeMessageVisibilityBatchResultEntry) []string {
	out := make([]string, len(entries))
	for i, entry := range entries {
		out[i] = aws.ToString(entry.Id)
	}
	return out
}

func TestSQSClient_ChangeMessageVisibilityBatchDefaultReportsEverySuccess(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		client := &fake.SQSClient{}
		in := drawVisibilityBatchInput(rt)

		out, err := client.ChangeMessageVisibilityBatch(context.Background(), in)

		require.NoError(rt, err)
		require.NotNil(rt, out)
		assert.Equal(rt, visibilityEntryIDs(in.Entries), visibilityResultIDs(out.Successful))
		assert.Empty(rt, out.Failed)
		assert.Equal(rt, []*sqs.ChangeMessageVisibilityBatchInput{in}, client.ChangeMessageVisibilityBatchCalls())
	})
}

func TestSQSClient_ChangeMessageVisibilityBatchDefaultEdgeInputs(t *testing.T) {
	tests := []struct {
		in   *sqs.ChangeMessageVisibilityBatchInput
		name string
	}{
		{name: "nil input"},
		{name: "no entries", in: &sqs.ChangeMessageVisibilityBatchInput{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &fake.SQSClient{}

			out, err := client.ChangeMessageVisibilityBatch(context.Background(), tt.in)

			require.NoError(t, err)
			require.NotNil(t, out)
			assert.Empty(t, out.Successful)
			assert.Empty(t, out.Failed)
			assert.Equal(t, []*sqs.ChangeMessageVisibilityBatchInput{tt.in}, client.ChangeMessageVisibilityBatchCalls())
		})
	}
}

func TestSQSClient_ChangeMessageVisibilityBatchFuncOverridesDefault(t *testing.T) {
	type key struct{}
	failed := &sqs.ChangeMessageVisibilityBatchOutput{
		Failed: []types.BatchResultErrorEntry{{Id: aws.String("0"), Code: aws.String("ReceiptHandleIsInvalid")}},
	}
	partial := &sqs.ChangeMessageVisibilityBatchOutput{
		Successful: []types.ChangeMessageVisibilityBatchResultEntry{{Id: aws.String("1")}},
		Failed:     []types.BatchResultErrorEntry{{Id: aws.String("0"), Code: aws.String("InternalError"), SenderFault: true}},
	}

	tests := []struct {
		out    *sqs.ChangeMessageVisibilityBatchOutput
		err    error
		name   string
		optFns int
	}{
		{name: "error", err: errVisibilityBatch},
		{name: "nil output", optFns: 1},
		{name: "failed entries", out: failed, optFns: 2},
		{name: "partial success", out: partial},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), key{}, tt.name)
			in := &sqs.ChangeMessageVisibilityBatchInput{
				QueueUrl: aws.String("queue"),
				Entries: []types.ChangeMessageVisibilityBatchRequestEntry{
					{Id: aws.String("0"), ReceiptHandle: aws.String("handle-0"), VisibilityTimeout: 30},
					{Id: aws.String("1"), ReceiptHandle: aws.String("handle-1"), VisibilityTimeout: 60},
				},
			}
			optFns := make([]func(*sqs.Options), tt.optFns)
			for i := range optFns {
				optFns[i] = func(*sqs.Options) {}
			}
			var gotCtx context.Context
			var gotIn *sqs.ChangeMessageVisibilityBatchInput
			var gotOptFns int
			client := &fake.SQSClient{
				ChangeMessageVisibilityBatchFunc: func(
					ctx context.Context, params *sqs.ChangeMessageVisibilityBatchInput, optFns ...func(*sqs.Options),
				) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
					gotCtx, gotIn, gotOptFns = ctx, params, len(optFns)
					return tt.out, tt.err
				},
			}

			out, err := client.ChangeMessageVisibilityBatch(ctx, in, optFns...)

			assert.ErrorIs(t, err, tt.err)
			assert.Same(t, tt.out, out)
			assert.Equal(t, tt.name, gotCtx.Value(key{}))
			assert.Same(t, in, gotIn)
			assert.Equal(t, tt.optFns, gotOptFns)
			assert.Equal(t, []*sqs.ChangeMessageVisibilityBatchInput{in}, client.ChangeMessageVisibilityBatchCalls())
		})
	}
}

func TestSQSClient_ChangeMessageVisibilityBatchCallsReturnsCopy(t *testing.T) {
	client := &fake.SQSClient{}
	first := &sqs.ChangeMessageVisibilityBatchInput{QueueUrl: aws.String("first")}
	mutated := &sqs.ChangeMessageVisibilityBatchInput{QueueUrl: aws.String("mutated")}
	second := &sqs.ChangeMessageVisibilityBatchInput{QueueUrl: aws.String("second")}
	_, err := client.ChangeMessageVisibilityBatch(context.Background(), first)
	require.NoError(t, err)

	calls := client.ChangeMessageVisibilityBatchCalls()
	require.Len(t, calls, 1)
	calls[0] = mutated

	_, err = client.ChangeMessageVisibilityBatch(context.Background(), second)
	require.NoError(t, err)

	assert.Equal(t, []*sqs.ChangeMessageVisibilityBatchInput{mutated}, calls)
	assert.Equal(t, []*sqs.ChangeMessageVisibilityBatchInput{first, second}, client.ChangeMessageVisibilityBatchCalls())
}

func TestSQSClient_ChangeMessageVisibilityBatchCallsInOrderProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		client := &fake.SQSClient{}
		inputs := rapid.SliceOfN(rapid.Custom(drawVisibilityBatchInput), 0, 20).Draw(rt, "inputs")

		for _, in := range inputs {
			_, err := client.ChangeMessageVisibilityBatch(context.Background(), in)
			require.NoError(rt, err)
		}

		calls := client.ChangeMessageVisibilityBatchCalls()
		require.Len(rt, calls, len(inputs))
		for i := range inputs {
			assert.Same(rt, inputs[i], calls[i])
		}
		assert.Empty(rt, client.DeleteMessageBatchCalls())
		assert.Empty(rt, client.ChangeMessageVisibilityCalls())
	})
}

func TestSQSClient_ChangeMessageVisibilityBatchRecordsConcurrentCalls(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		name       string
		goroutines int
		withFunc   bool
	}{
		{name: "one goroutine", goroutines: 1},
		{name: "eight goroutines", goroutines: 8},
		{name: "sixty four goroutines", goroutines: 64},
		{name: "sixty four goroutines with func", goroutines: 64, withFunc: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &fake.SQSClient{}
			if tt.withFunc {
				client.ChangeMessageVisibilityBatchFunc = func(
					context.Context, *sqs.ChangeMessageVisibilityBatchInput, ...func(*sqs.Options),
				) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
					return &sqs.ChangeMessageVisibilityBatchOutput{}, nil
				}
			}
			inputs := make([]*sqs.ChangeMessageVisibilityBatchInput, tt.goroutines)
			for i := range inputs {
				inputs[i] = &sqs.ChangeMessageVisibilityBatchInput{
					QueueUrl: aws.String("queue-" + strconv.Itoa(i)),
					Entries: []types.ChangeMessageVisibilityBatchRequestEntry{
						{Id: aws.String(strconv.Itoa(i)), ReceiptHandle: aws.String("handle"), VisibilityTimeout: 30},
					},
				}
			}

			var wg sync.WaitGroup
			for _, in := range inputs {
				wg.Go(func() {
					_, err := client.ChangeMessageVisibilityBatch(context.Background(), in)
					assert.NoError(t, err)
					_ = client.ChangeMessageVisibilityBatchCalls()
				})
			}
			wg.Wait()

			assert.ElementsMatch(t, inputs, client.ChangeMessageVisibilityBatchCalls())
		})
	}
}

var errReplay = errors.New("change visibility failed")

func replayExpectedCalls(in *sqs.ChangeMessageVisibilityBatchInput) []*sqs.ChangeMessageVisibilityInput {
	out := make([]*sqs.ChangeMessageVisibilityInput, len(in.Entries))
	for i, entry := range in.Entries {
		out[i] = &sqs.ChangeMessageVisibilityInput{
			QueueUrl:          in.QueueUrl,
			ReceiptHandle:     entry.ReceiptHandle,
			VisibilityTimeout: entry.VisibilityTimeout,
		}
	}
	return out
}

func TestSQSClient_ChangeMessageVisibilityBatchReplaysSingleCalls(t *testing.T) {
	type key struct{}
	in := &sqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: aws.String("queue"),
		Entries: []types.ChangeMessageVisibilityBatchRequestEntry{
			{Id: aws.String("0"), ReceiptHandle: aws.String("handle-0"), VisibilityTimeout: 30},
			{Id: aws.String("1"), ReceiptHandle: aws.String("handle-1"), VisibilityTimeout: 60},
			{Id: aws.String("2"), ReceiptHandle: aws.String("handle-2"), VisibilityTimeout: 0},
		},
	}

	tests := []struct {
		in            *sqs.ChangeMessageVisibilityBatchInput
		failing       map[string]bool
		name          string
		wantSucceeded []string
		wantFailed    []string
		wantCalls     []*sqs.ChangeMessageVisibilityInput
	}{
		{
			name:          "every single call succeeds",
			in:            in,
			wantSucceeded: []string{"0", "1", "2"},
			wantCalls:     replayExpectedCalls(in),
		},
		{
			name:          "failed single calls become failed entries",
			in:            in,
			failing:       map[string]bool{"handle-0": true, "handle-2": true},
			wantSucceeded: []string{"1"},
			wantFailed:    []string{"0", "2"},
			wantCalls:     replayExpectedCalls(in),
		},
		{
			name:       "every single call fails",
			in:         in,
			failing:    map[string]bool{"handle-0": true, "handle-1": true, "handle-2": true},
			wantFailed: []string{"0", "1", "2"},
			wantCalls:  replayExpectedCalls(in),
		},
		{name: "nil input sends nothing"},
		{name: "no entries sends nothing", in: &sqs.ChangeMessageVisibilityBatchInput{QueueUrl: aws.String("queue")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), key{}, tt.name)
			optFns := []func(*sqs.Options){func(*sqs.Options) {}, func(*sqs.Options) {}}
			var gotCtxValues []any
			var gotOptFns []int
			client := &fake.SQSClient{
				ChangeMessageVisibilityFunc: func(
					ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options),
				) (*sqs.ChangeMessageVisibilityOutput, error) {
					gotCtxValues = append(gotCtxValues, ctx.Value(key{}))
					gotOptFns = append(gotOptFns, len(optFns))
					if tt.failing[aws.ToString(params.ReceiptHandle)] {
						return nil, errReplay
					}
					return &sqs.ChangeMessageVisibilityOutput{}, nil
				},
			}

			out, err := client.ChangeMessageVisibilityBatch(ctx, tt.in, optFns...)

			require.NoError(t, err)
			require.NotNil(t, out)
			assert.Equal(t, tt.wantSucceeded, nilIfEmptyIDs(visibilityResultIDs(out.Successful)))
			failedIDs := make([]string, 0, len(out.Failed))
			for _, f := range out.Failed {
				failedIDs = append(failedIDs, aws.ToString(f.Id))
				assert.Equal(t, "FakeChangeMessageVisibilityError", aws.ToString(f.Code))
				assert.Equal(t, errReplay.Error(), aws.ToString(f.Message))
				assert.False(t, f.SenderFault)
			}
			assert.Equal(t, tt.wantFailed, nilIfEmptyIDs(failedIDs))
			assert.Equal(t, tt.wantCalls, nilIfEmptyCalls(client.ChangeMessageVisibilityCalls()))
			assert.Empty(t, client.ChangeMessageVisibilityBatchCalls())
			for i := range gotCtxValues {
				assert.Equal(t, tt.name, gotCtxValues[i])
				assert.Equal(t, len(optFns), gotOptFns[i])
			}
			assert.Len(t, gotCtxValues, len(tt.wantCalls))
		})
	}
}

func TestSQSClient_ChangeMessageVisibilityBatchFuncTakesPrecedenceOverReplay(t *testing.T) {
	in := &sqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: aws.String("queue"),
		Entries: []types.ChangeMessageVisibilityBatchRequestEntry{
			{Id: aws.String("0"), ReceiptHandle: aws.String("handle-0"), VisibilityTimeout: 30},
		},
	}
	want := &sqs.ChangeMessageVisibilityBatchOutput{
		Successful: []types.ChangeMessageVisibilityBatchResultEntry{{Id: aws.String("0")}},
	}
	singleCalls := 0
	client := &fake.SQSClient{
		ChangeMessageVisibilityFunc: func(
			context.Context, *sqs.ChangeMessageVisibilityInput, ...func(*sqs.Options),
		) (*sqs.ChangeMessageVisibilityOutput, error) {
			singleCalls++
			return nil, errReplay
		},
		ChangeMessageVisibilityBatchFunc: func(
			context.Context, *sqs.ChangeMessageVisibilityBatchInput, ...func(*sqs.Options),
		) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
			return want, nil
		},
	}

	out, err := client.ChangeMessageVisibilityBatch(context.Background(), in)

	require.NoError(t, err)
	assert.Same(t, want, out)
	assert.Zero(t, singleCalls)
	assert.Empty(t, client.ChangeMessageVisibilityCalls())
	assert.Equal(t, []*sqs.ChangeMessageVisibilityBatchInput{in}, client.ChangeMessageVisibilityBatchCalls())
}

func TestSQSClient_ChangeMessageVisibilityBatchReplayPartitionsEntries(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		in := drawVisibilityBatchInput(rt)
		failing := make(map[int]bool, len(in.Entries))
		for i := range in.Entries {
			failing[i] = rapid.Bool().Draw(rt, "fails")
		}
		call := 0
		client := &fake.SQSClient{
			ChangeMessageVisibilityFunc: func(
				context.Context, *sqs.ChangeMessageVisibilityInput, ...func(*sqs.Options),
			) (*sqs.ChangeMessageVisibilityOutput, error) {
				defer func() { call++ }()
				if failing[call] {
					return nil, errReplay
				}
				return &sqs.ChangeMessageVisibilityOutput{}, nil
			},
		}

		out, err := client.ChangeMessageVisibilityBatch(context.Background(), in)

		require.NoError(rt, err)
		var wantSucceeded, wantFailed []string
		for i, entry := range in.Entries {
			if failing[i] {
				wantFailed = append(wantFailed, aws.ToString(entry.Id))
			} else {
				wantSucceeded = append(wantSucceeded, aws.ToString(entry.Id))
			}
		}
		failedIDs := make([]string, 0, len(out.Failed))
		for _, f := range out.Failed {
			failedIDs = append(failedIDs, aws.ToString(f.Id))
		}
		assert.Equal(rt, wantSucceeded, nilIfEmptyIDs(visibilityResultIDs(out.Successful)))
		assert.Equal(rt, wantFailed, nilIfEmptyIDs(failedIDs))
		assert.Equal(rt, replayExpectedCalls(in), client.ChangeMessageVisibilityCalls())
		assert.Empty(rt, client.ChangeMessageVisibilityBatchCalls())
	})
}

func nilIfEmptyIDs(ids []string) []string {
	if len(ids) == 0 {
		return nil
	}
	return ids
}

func nilIfEmptyCalls(calls []*sqs.ChangeMessageVisibilityInput) []*sqs.ChangeMessageVisibilityInput {
	if len(calls) == 0 {
		return nil
	}
	return calls
}
