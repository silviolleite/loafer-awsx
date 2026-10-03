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
