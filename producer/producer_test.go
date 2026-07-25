package producer_test

import (
	"context"
	stderrors "errors"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/errors"
	"github.com/silviolleite/loafer-awsx/producer"
)

var _ producer.SNSClient = (*sns.Client)(nil)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type fakeSNSClient struct {
	err               error
	batchErr          error
	publishInput      *sns.PublishInput
	output            *sns.PublishOutput
	publishBatchInput *sns.PublishBatchInput
	batchOutput       *sns.PublishBatchOutput
	calls             int
	batchCalls        int
	mu                sync.Mutex
}

func (f *fakeSNSClient) Publish(_ context.Context, params *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.publishInput = params
	f.calls++
	return f.output, f.err
}

func (f *fakeSNSClient) PublishBatch(_ context.Context, params *sns.PublishBatchInput, _ ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.publishBatchInput = params
	f.batchCalls++
	return f.batchOutput, f.batchErr
}

func TestProducer_Publish_EmptyInput(t *testing.T) {
	testCases := []struct {
		input *producer.PublishInput
		name  string
	}{
		{
			name:  "nil input",
			input: nil,
		},
		{
			name:  "empty message",
			input: &producer.PublishInput{TopicARN: "arn:aws:sns:us-east-1:000:topic"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := &fakeSNSClient{}
			p, err := producer.New(client)
			require.NoError(t, err)

			id, err := p.Publish(context.Background(), tc.input)

			assert.ErrorIs(t, err, errors.ErrEmptyInput)
			assert.Empty(t, id)
			assert.Equal(t, 0, client.calls)
		})
	}
}

func TestProducer_Publish_Success(t *testing.T) {
	testCases := []struct {
		name         string
		input        *producer.PublishInput
		wantGroupID  *string
		wantDedupID  *string
		wantAttrKeys []string
	}{
		{
			name: "standard message",
			input: &producer.PublishInput{
				TopicARN: "arn:aws:sns:us-east-1:000:topic",
				Message:  "hello",
			},
		},
		{
			name: "fifo group id",
			input: &producer.PublishInput{
				TopicARN: "arn:aws:sns:us-east-1:000:topic.fifo",
				Message:  "hello",
				GroupID:  "group-1",
			},
			wantGroupID: aws.String("group-1"),
		},
		{
			name: "deduplication id",
			input: &producer.PublishInput{
				TopicARN:        "arn:aws:sns:us-east-1:000:topic.fifo",
				Message:         "hello",
				DeduplicationID: "dedup-1",
			},
			wantDedupID: aws.String("dedup-1"),
		},
		{
			name: "message attributes",
			input: &producer.PublishInput{
				TopicARN:   "arn:aws:sns:us-east-1:000:topic",
				Message:    "hello",
				Attributes: map[string]string{"key1": "value1", "key2": "value2"},
			},
			wantAttrKeys: []string{"key1", "key2"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := &fakeSNSClient{output: &sns.PublishOutput{MessageId: aws.String("msg-123")}}
			p, err := producer.New(client)
			require.NoError(t, err)

			id, err := p.Publish(context.Background(), tc.input)

			require.NoError(t, err)
			assert.Equal(t, "msg-123", id)
			assert.Equal(t, 1, client.calls)
			require.NotNil(t, client.publishInput)
			assert.Equal(t, tc.input.TopicARN, aws.ToString(client.publishInput.TopicArn))
			assert.Equal(t, tc.input.Message, aws.ToString(client.publishInput.Message))
			assert.Equal(t, aws.ToString(tc.wantGroupID), aws.ToString(client.publishInput.MessageGroupId))
			assert.Equal(t, aws.ToString(tc.wantDedupID), aws.ToString(client.publishInput.MessageDeduplicationId))

			for _, key := range tc.wantAttrKeys {
				attr, ok := client.publishInput.MessageAttributes[key]
				require.True(t, ok)
				assert.Equal(t, "String", aws.ToString(attr.DataType))
				assert.Equal(t, tc.input.Attributes[key], aws.ToString(attr.StringValue))
			}

			if len(tc.wantAttrKeys) == 0 {
				assert.Nil(t, client.publishInput.MessageAttributes)
			}
		})
	}
}

func TestProducer_Publish_ClientError(t *testing.T) {
	wantErr := stderrors.New("sns failure")
	client := &fakeSNSClient{err: wantErr}
	p, err := producer.New(client)
	require.NoError(t, err)

	id, err := p.Publish(context.Background(), &producer.PublishInput{
		TopicARN: "arn:aws:sns:us-east-1:000:topic",
		Message:  "hello",
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, wantErr)
	assert.Empty(t, id)
}

func TestProducer_Publish_NilOutput(t *testing.T) {
	testCases := []struct {
		output *sns.PublishOutput
		name   string
	}{
		{
			name:   "nil output",
			output: nil,
		},
		{
			name:   "nil message id",
			output: &sns.PublishOutput{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := &fakeSNSClient{output: tc.output}
			p, err := producer.New(client)
			require.NoError(t, err)

			id, err := p.Publish(context.Background(), &producer.PublishInput{
				TopicARN: "arn:aws:sns:us-east-1:000:topic",
				Message:  "hello",
			})

			require.NoError(t, err)
			assert.Empty(t, id)
		})
	}
}

func TestProducer_New_IgnoresNilOption(t *testing.T) {
	client := &fakeSNSClient{output: &sns.PublishOutput{MessageId: aws.String("msg-1")}}
	p, err := producer.New(client, nil)
	require.NoError(t, err)

	id, err := p.Publish(context.Background(), &producer.PublishInput{
		TopicARN: "arn:aws:sns:us-east-1:000:topic",
		Message:  "hello",
	})

	require.NoError(t, err)
	assert.Equal(t, "msg-1", id)
}

func TestProducer_New_NilClient(t *testing.T) {
	p, err := producer.New(nil)

	assert.Nil(t, p)
	assert.ErrorIs(t, err, errors.ErrNoSNSClient)
}

func TestBuildTopicARN(t *testing.T) {
	testCases := []struct {
		name      string
		region    string
		accountID string
		topicName string
		want      string
	}{
		{
			name:      "standard topic",
			region:    "us-east-1",
			accountID: "000000000000",
			topicName: "my_topic",
			want:      "arn:aws:sns:us-east-1:000000000000:my_topic",
		},
		{
			name:      "fifo topic",
			region:    "eu-west-1",
			accountID: "123456789012",
			topicName: "orders.fifo",
			want:      "arn:aws:sns:eu-west-1:123456789012:orders.fifo",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, producer.BuildTopicARN(tc.region, tc.accountID, tc.topicName))
		})
	}
}

func TestBuildTopicARN_Property(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		region := rapid.StringMatching(`[a-z0-9-]+`).Draw(t, "region")
		accountID := rapid.StringMatching(`[0-9]+`).Draw(t, "accountID")
		topicName := rapid.StringMatching(`[a-zA-Z0-9_.-]+`).Draw(t, "topicName")

		arn := producer.BuildTopicARN(region, accountID, topicName)

		assert.Equal(t, "arn:aws:sns:"+region+":"+accountID+":"+topicName, arn)
	})
}

func TestProducer_Publish_AttributePassthrough_Property(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		attrs := rapid.MapOfN(
			rapid.StringMatching(`[a-zA-Z][a-zA-Z0-9_]*`),
			rapid.String(),
			1, 10,
		).Draw(t, "attributes")

		client := &fakeSNSClient{output: &sns.PublishOutput{MessageId: aws.String("msg")}}
		p, err := producer.New(client)
		require.NoError(t, err)

		_, err = p.Publish(context.Background(), &producer.PublishInput{
			TopicARN:   "arn:aws:sns:us-east-1:000:topic",
			Message:    "payload",
			Attributes: attrs,
		})

		require.NoError(t, err)
		require.Len(t, client.publishInput.MessageAttributes, len(attrs))
		for k, v := range attrs {
			got, ok := client.publishInput.MessageAttributes[k]
			require.True(t, ok)
			assert.Equal(t, "String", aws.ToString(got.DataType))
			assert.Equal(t, v, aws.ToString(got.StringValue))
			assert.IsType(t, types.MessageAttributeValue{}, got)
		}
	})
}

func TestProducer_Publish_Concurrent(t *testing.T) {
	client := &fakeSNSClient{output: &sns.PublishOutput{MessageId: aws.String("msg-concurrent")}}
	p, err := producer.New(client)
	require.NoError(t, err)

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			id, err := p.Publish(context.Background(), &producer.PublishInput{
				TopicARN: "arn:aws:sns:us-east-1:000:topic",
				Message:  "hello",
				GroupID:  "group",
			})
			assert.NoError(t, err)
			assert.Equal(t, "msg-concurrent", id)
		}()
	}

	wg.Wait()
	assert.Equal(t, goroutines, client.calls)
}

func TestProducer_PublishBatch_EmptyInput(t *testing.T) {
	testCases := []struct {
		input *producer.PublishBatchInput
		name  string
	}{
		{
			name:  "nil input",
			input: nil,
		},
		{
			name:  "nil messages",
			input: &producer.PublishBatchInput{TopicARN: "arn:aws:sns:us-east-1:000:topic"},
		},
		{
			name:  "empty messages",
			input: &producer.PublishBatchInput{TopicARN: "arn:aws:sns:us-east-1:000:topic", Messages: []*producer.PublishBatchEntry{}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := &fakeSNSClient{}
			p, err := producer.New(client)
			require.NoError(t, err)

			out, err := p.PublishBatch(context.Background(), tc.input)

			assert.ErrorIs(t, err, errors.ErrEmptyInput)
			assert.Nil(t, out)
			assert.Equal(t, 0, client.batchCalls)
		})
	}
}

func TestProducer_PublishBatch_MaxBatchSize(t *testing.T) {
	messages := make([]*producer.PublishBatchEntry, 11)
	for i := range messages {
		messages[i] = &producer.PublishBatchEntry{ID: "id", Message: "msg"}
	}

	client := &fakeSNSClient{}
	p, err := producer.New(client)
	require.NoError(t, err)

	out, err := p.PublishBatch(context.Background(), &producer.PublishBatchInput{
		TopicARN: "arn:aws:sns:us-east-1:000:topic",
		Messages: messages,
	})

	assert.ErrorIs(t, err, errors.ErrMaxBatchSize)
	assert.Nil(t, out)
	assert.Equal(t, 0, client.batchCalls)
}

func TestProducer_PublishBatch_Success(t *testing.T) {
	client := &fakeSNSClient{batchOutput: &sns.PublishBatchOutput{
		Successful: []types.PublishBatchResultEntry{
			{Id: aws.String("e1"), MessageId: aws.String("m1")},
			{Id: aws.String("e2"), MessageId: aws.String("m2")},
		},
	}}
	p, err := producer.New(client)
	require.NoError(t, err)

	out, err := p.PublishBatch(context.Background(), &producer.PublishBatchInput{
		TopicARN: "arn:aws:sns:us-east-1:000:topic",
		Messages: []*producer.PublishBatchEntry{
			{ID: "e1", Message: "hello"},
			{ID: "e2", Message: "world"},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Empty(t, out.Failed)
	require.Len(t, out.Successful, 2)
	assert.Equal(t, "e1", out.Successful[0].EntryID)
	assert.Equal(t, "m1", out.Successful[0].MessageID)
	assert.Equal(t, "e2", out.Successful[1].EntryID)
	assert.Equal(t, "m2", out.Successful[1].MessageID)

	assert.Equal(t, 1, client.batchCalls)
	require.NotNil(t, client.publishBatchInput)
	assert.Equal(t, "arn:aws:sns:us-east-1:000:topic", aws.ToString(client.publishBatchInput.TopicArn))
	require.Len(t, client.publishBatchInput.PublishBatchRequestEntries, 2)
	assert.Equal(t, "e1", aws.ToString(client.publishBatchInput.PublishBatchRequestEntries[0].Id))
	assert.Equal(t, "hello", aws.ToString(client.publishBatchInput.PublishBatchRequestEntries[0].Message))
}

func TestProducer_PublishBatch_FailedMapping(t *testing.T) {
	client := &fakeSNSClient{batchOutput: &sns.PublishBatchOutput{
		Failed: []types.BatchResultErrorEntry{
			{Id: aws.String("e1"), Code: aws.String("InternalError"), Message: aws.String("boom")},
		},
	}}
	p, err := producer.New(client)
	require.NoError(t, err)

	out, err := p.PublishBatch(context.Background(), &producer.PublishBatchInput{
		TopicARN: "arn:aws:sns:us-east-1:000:topic",
		Messages: []*producer.PublishBatchEntry{{ID: "e1", Message: "hello"}},
	})

	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Empty(t, out.Successful)
	require.Len(t, out.Failed, 1)
	assert.Equal(t, "e1", out.Failed[0].EntryID)
	require.Error(t, out.Failed[0].Err)
	assert.Contains(t, out.Failed[0].Err.Error(), "InternalError")
	assert.Contains(t, out.Failed[0].Err.Error(), "boom")
}

func TestProducer_PublishBatch_MixedResults(t *testing.T) {
	client := &fakeSNSClient{batchOutput: &sns.PublishBatchOutput{
		Successful: []types.PublishBatchResultEntry{
			{Id: aws.String("e1"), MessageId: aws.String("m1")},
		},
		Failed: []types.BatchResultErrorEntry{
			{Id: aws.String("e2"), Code: aws.String("Throttling"), Message: aws.String("slow down")},
		},
	}}
	p, err := producer.New(client)
	require.NoError(t, err)

	out, err := p.PublishBatch(context.Background(), &producer.PublishBatchInput{
		TopicARN: "arn:aws:sns:us-east-1:000:topic",
		Messages: []*producer.PublishBatchEntry{
			{ID: "e1", Message: "hello"},
			{ID: "e2", Message: "world"},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, out)
	require.Len(t, out.Successful, 1)
	require.Len(t, out.Failed, 1)
	assert.Equal(t, "e1", out.Successful[0].EntryID)
	assert.Equal(t, "e2", out.Failed[0].EntryID)
}

func TestProducer_PublishBatch_ClientError(t *testing.T) {
	wantErr := stderrors.New("sns batch failure")
	client := &fakeSNSClient{batchErr: wantErr}
	p, err := producer.New(client)
	require.NoError(t, err)

	out, err := p.PublishBatch(context.Background(), &producer.PublishBatchInput{
		TopicARN: "arn:aws:sns:us-east-1:000:topic",
		Messages: []*producer.PublishBatchEntry{{ID: "e1", Message: "hello"}},
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, wantErr)
	assert.Nil(t, out)
}

func TestProducer_PublishBatch_NilOutput(t *testing.T) {
	client := &fakeSNSClient{batchOutput: nil}
	p, err := producer.New(client)
	require.NoError(t, err)

	out, err := p.PublishBatch(context.Background(), &producer.PublishBatchInput{
		TopicARN: "arn:aws:sns:us-east-1:000:topic",
		Messages: []*producer.PublishBatchEntry{{ID: "e1", Message: "hello"}},
	})

	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Empty(t, out.Successful)
	assert.Empty(t, out.Failed)
}

func TestProducer_PublishBatch_FIFOAndAttributes(t *testing.T) {
	client := &fakeSNSClient{batchOutput: &sns.PublishBatchOutput{}}
	p, err := producer.New(client)
	require.NoError(t, err)

	_, err = p.PublishBatch(context.Background(), &producer.PublishBatchInput{
		TopicARN: "arn:aws:sns:us-east-1:000:topic.fifo",
		Messages: []*producer.PublishBatchEntry{
			{
				ID:              "e1",
				Message:         "hello",
				GroupID:         "group-1",
				DeduplicationID: "dedup-1",
				Attributes:      map[string]string{"key1": "value1"},
			},
			{
				ID:      "e2",
				Message: "world",
			},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, client.publishBatchInput)
	entries := client.publishBatchInput.PublishBatchRequestEntries
	require.Len(t, entries, 2)

	assert.Equal(t, "group-1", aws.ToString(entries[0].MessageGroupId))
	assert.Equal(t, "dedup-1", aws.ToString(entries[0].MessageDeduplicationId))
	attr, ok := entries[0].MessageAttributes["key1"]
	require.True(t, ok)
	assert.Equal(t, "String", aws.ToString(attr.DataType))
	assert.Equal(t, "value1", aws.ToString(attr.StringValue))

	assert.Nil(t, entries[1].MessageGroupId)
	assert.Nil(t, entries[1].MessageDeduplicationId)
	assert.Nil(t, entries[1].MessageAttributes)
}

func TestProducer_PublishBatch_Passthrough_Property(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(1, 10).Draw(t, "n")
		entries := make([]*producer.PublishBatchEntry, 0, n)
		for i := 0; i < n; i++ {
			entries = append(entries, &producer.PublishBatchEntry{
				ID:              rapid.StringMatching(`[a-zA-Z0-9_-]+`).Draw(t, "id"),
				Message:         rapid.String().Draw(t, "message"),
				GroupID:         rapid.StringMatching(`[a-zA-Z0-9_-]*`).Draw(t, "groupID"),
				DeduplicationID: rapid.StringMatching(`[a-zA-Z0-9_-]*`).Draw(t, "dedupID"),
				Attributes: rapid.MapOfN(
					rapid.StringMatching(`[a-zA-Z][a-zA-Z0-9_]*`),
					rapid.String(),
					0, 5,
				).Draw(t, "attributes"),
			})
		}

		client := &fakeSNSClient{batchOutput: &sns.PublishBatchOutput{}}
		p, err := producer.New(client)
		require.NoError(t, err)

		_, err = p.PublishBatch(context.Background(), &producer.PublishBatchInput{
			TopicARN: "arn:aws:sns:us-east-1:000:topic",
			Messages: entries,
		})

		require.NoError(t, err)
		got := client.publishBatchInput.PublishBatchRequestEntries
		require.Len(t, got, len(entries))

		for i, e := range entries {
			assert.Equal(t, e.ID, aws.ToString(got[i].Id))
			assert.Equal(t, e.Message, aws.ToString(got[i].Message))
			assert.Equal(t, e.GroupID, aws.ToString(got[i].MessageGroupId))
			assert.Equal(t, e.DeduplicationID, aws.ToString(got[i].MessageDeduplicationId))
			require.Len(t, got[i].MessageAttributes, len(e.Attributes))
			for k, v := range e.Attributes {
				attr, ok := got[i].MessageAttributes[k]
				require.True(t, ok)
				assert.Equal(t, "String", aws.ToString(attr.DataType))
				assert.Equal(t, v, aws.ToString(attr.StringValue))
			}
		}
	})
}

func TestProducer_PublishBatch_ResultMapping_Property(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		successIDs := rapid.SliceOfN(rapid.StringMatching(`[a-zA-Z0-9_-]+`), 0, 10).Draw(t, "successIDs")
		failIDs := rapid.SliceOfN(rapid.StringMatching(`[a-zA-Z0-9_-]+`), 0, 10).Draw(t, "failIDs")

		successful := make([]types.PublishBatchResultEntry, 0, len(successIDs))
		for _, id := range successIDs {
			successful = append(successful, types.PublishBatchResultEntry{
				Id:        aws.String(id),
				MessageId: aws.String("msg-" + id),
			})
		}

		failed := make([]types.BatchResultErrorEntry, 0, len(failIDs))
		for _, id := range failIDs {
			failed = append(failed, types.BatchResultErrorEntry{
				Id:      aws.String(id),
				Code:    aws.String("Code"),
				Message: aws.String("msg"),
			})
		}

		client := &fakeSNSClient{batchOutput: &sns.PublishBatchOutput{
			Successful: successful,
			Failed:     failed,
		}}
		p, err := producer.New(client)
		require.NoError(t, err)

		out, err := p.PublishBatch(context.Background(), &producer.PublishBatchInput{
			TopicARN: "arn:aws:sns:us-east-1:000:topic",
			Messages: []*producer.PublishBatchEntry{{ID: "e1", Message: "hello"}},
		})

		require.NoError(t, err)
		require.Len(t, out.Successful, len(successful))
		require.Len(t, out.Failed, len(failed))
		for i, s := range successful {
			assert.Equal(t, aws.ToString(s.Id), out.Successful[i].EntryID)
			assert.Equal(t, aws.ToString(s.MessageId), out.Successful[i].MessageID)
		}
		for i, f := range failed {
			assert.Equal(t, aws.ToString(f.Id), out.Failed[i].EntryID)
			require.Error(t, out.Failed[i].Err)
		}
	})
}

type stubGenerator struct {
	err    error
	value  string
	fields []map[string]string
	calls  int
	mu     sync.Mutex
}

func (s *stubGenerator) Generate(_ context.Context, fields map[string]string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.fields = append(s.fields, fields)
	return s.value, s.err
}

func TestProducer_Publish_GeneratesGroupID(t *testing.T) {
	gen := &stubGenerator{value: "generated-group"}
	client := &fakeSNSClient{output: &sns.PublishOutput{MessageId: aws.String("msg")}}
	p, err := producer.New(client, producer.WithGroupIDGenerator(gen))
	require.NoError(t, err)

	_, err = p.Publish(context.Background(), &producer.PublishInput{
		TopicARN:   "arn:aws:sns:us-east-1:000:topic.fifo",
		Message:    "hello",
		Attributes: map[string]string{"tenant": "acme"},
	})

	require.NoError(t, err)
	assert.Equal(t, 1, gen.calls)
	assert.Equal(t, "generated-group", aws.ToString(client.publishInput.MessageGroupId))
	require.Len(t, gen.fields, 1)
	assert.Equal(t, map[string]string{"tenant": "acme"}, gen.fields[0])
}

func TestProducer_Publish_GeneratesDeduplicationID(t *testing.T) {
	gen := &stubGenerator{value: "generated-dedup"}
	client := &fakeSNSClient{output: &sns.PublishOutput{MessageId: aws.String("msg")}}
	p, err := producer.New(client, producer.WithDeduplicationIDGenerator(gen))
	require.NoError(t, err)

	_, err = p.Publish(context.Background(), &producer.PublishInput{
		TopicARN:   "arn:aws:sns:us-east-1:000:topic.fifo",
		Message:    "hello",
		Attributes: map[string]string{"order": "1"},
	})

	require.NoError(t, err)
	assert.Equal(t, 1, gen.calls)
	assert.Equal(t, "generated-dedup", aws.ToString(client.publishInput.MessageDeduplicationId))
}

func TestProducer_Publish_ExplicitIDsSkipGenerators(t *testing.T) {
	groupGen := &stubGenerator{value: "generated-group"}
	dedupGen := &stubGenerator{value: "generated-dedup"}
	client := &fakeSNSClient{output: &sns.PublishOutput{MessageId: aws.String("msg")}}
	p, err := producer.New(client,
		producer.WithGroupIDGenerator(groupGen),
		producer.WithDeduplicationIDGenerator(dedupGen),
	)
	require.NoError(t, err)

	_, err = p.Publish(context.Background(), &producer.PublishInput{
		TopicARN:        "arn:aws:sns:us-east-1:000:topic.fifo",
		Message:         "hello",
		GroupID:         "explicit-group",
		DeduplicationID: "explicit-dedup",
		Attributes:      map[string]string{"tenant": "acme"},
	})

	require.NoError(t, err)
	assert.Equal(t, 0, groupGen.calls)
	assert.Equal(t, 0, dedupGen.calls)
	assert.Equal(t, "explicit-group", aws.ToString(client.publishInput.MessageGroupId))
	assert.Equal(t, "explicit-dedup", aws.ToString(client.publishInput.MessageDeduplicationId))
}

func TestProducer_Publish_GroupIDGeneratorError(t *testing.T) {
	wantErr := stderrors.New("group gen failure")
	gen := &stubGenerator{err: wantErr}
	client := &fakeSNSClient{output: &sns.PublishOutput{MessageId: aws.String("msg")}}
	p, err := producer.New(client, producer.WithGroupIDGenerator(gen))
	require.NoError(t, err)

	id, err := p.Publish(context.Background(), &producer.PublishInput{
		TopicARN:   "arn:aws:sns:us-east-1:000:topic.fifo",
		Message:    "hello",
		Attributes: map[string]string{"tenant": "acme"},
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, wantErr)
	assert.Empty(t, id)
	assert.Equal(t, 0, client.calls)
}

func TestProducer_Publish_DeduplicationIDGeneratorError(t *testing.T) {
	wantErr := stderrors.New("dedup gen failure")
	gen := &stubGenerator{err: wantErr}
	client := &fakeSNSClient{output: &sns.PublishOutput{MessageId: aws.String("msg")}}
	p, err := producer.New(client, producer.WithDeduplicationIDGenerator(gen))
	require.NoError(t, err)

	id, err := p.Publish(context.Background(), &producer.PublishInput{
		TopicARN:   "arn:aws:sns:us-east-1:000:topic.fifo",
		Message:    "hello",
		Attributes: map[string]string{"order": "1"},
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, wantErr)
	assert.Empty(t, id)
	assert.Equal(t, 0, client.calls)
}

func TestProducer_New_NilGeneratorsAreNoOp(t *testing.T) {
	client := &fakeSNSClient{output: &sns.PublishOutput{MessageId: aws.String("msg")}}
	p, err := producer.New(client,
		producer.WithGroupIDGenerator(nil),
		producer.WithDeduplicationIDGenerator(nil),
	)
	require.NoError(t, err)

	_, err = p.Publish(context.Background(), &producer.PublishInput{
		TopicARN:   "arn:aws:sns:us-east-1:000:topic.fifo",
		Message:    "hello",
		Attributes: map[string]string{"tenant": "acme"},
	})

	require.NoError(t, err)
	assert.Nil(t, client.publishInput.MessageGroupId)
	assert.Nil(t, client.publishInput.MessageDeduplicationId)
}

func TestProducer_PublishBatch_GeneratesIDsPerEntry(t *testing.T) {
	groupGen := &stubGenerator{value: "gen-group"}
	dedupGen := &stubGenerator{value: "gen-dedup"}
	client := &fakeSNSClient{batchOutput: &sns.PublishBatchOutput{}}
	p, err := producer.New(client,
		producer.WithGroupIDGenerator(groupGen),
		producer.WithDeduplicationIDGenerator(dedupGen),
	)
	require.NoError(t, err)

	_, err = p.PublishBatch(context.Background(), &producer.PublishBatchInput{
		TopicARN: "arn:aws:sns:us-east-1:000:topic.fifo",
		Messages: []*producer.PublishBatchEntry{
			{ID: "e1", Message: "hello", Attributes: map[string]string{"tenant": "acme"}},
			{ID: "e2", Message: "world", GroupID: "explicit", DeduplicationID: "explicit-dedup"},
		},
	})

	require.NoError(t, err)
	entries := client.publishBatchInput.PublishBatchRequestEntries
	require.Len(t, entries, 2)

	assert.Equal(t, "gen-group", aws.ToString(entries[0].MessageGroupId))
	assert.Equal(t, "gen-dedup", aws.ToString(entries[0].MessageDeduplicationId))

	assert.Equal(t, "explicit", aws.ToString(entries[1].MessageGroupId))
	assert.Equal(t, "explicit-dedup", aws.ToString(entries[1].MessageDeduplicationId))

	assert.Equal(t, 1, groupGen.calls)
	assert.Equal(t, 1, dedupGen.calls)
}

func TestProducer_PublishBatch_GeneratorErrorAborts(t *testing.T) {
	wantErr := stderrors.New("batch gen failure")
	gen := &stubGenerator{err: wantErr}
	client := &fakeSNSClient{batchOutput: &sns.PublishBatchOutput{}}
	p, err := producer.New(client, producer.WithGroupIDGenerator(gen))
	require.NoError(t, err)

	out, err := p.PublishBatch(context.Background(), &producer.PublishBatchInput{
		TopicARN: "arn:aws:sns:us-east-1:000:topic.fifo",
		Messages: []*producer.PublishBatchEntry{
			{ID: "e1", Message: "hello", Attributes: map[string]string{"tenant": "acme"}},
		},
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, wantErr)
	assert.Nil(t, out)
	assert.Equal(t, 0, client.batchCalls)
}

func TestProducer_Publish_ExplicitGroupSkipsGeneratorOnly_Property(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		explicitGroup := rapid.Bool().Draw(t, "explicitGroup")
		explicitDedup := rapid.Bool().Draw(t, "explicitDedup")

		groupGen := &stubGenerator{value: "gen-group"}
		dedupGen := &stubGenerator{value: "gen-dedup"}
		client := &fakeSNSClient{output: &sns.PublishOutput{MessageId: aws.String("msg")}}
		p, err := producer.New(client,
			producer.WithGroupIDGenerator(groupGen),
			producer.WithDeduplicationIDGenerator(dedupGen),
		)
		require.NoError(t, err)

		in := &producer.PublishInput{
			TopicARN:   "arn:aws:sns:us-east-1:000:topic.fifo",
			Message:    "hello",
			Attributes: map[string]string{"tenant": "acme"},
		}
		if explicitGroup {
			in.GroupID = "explicit-group"
		}
		if explicitDedup {
			in.DeduplicationID = "explicit-dedup"
		}

		_, err = p.Publish(context.Background(), in)
		require.NoError(t, err)

		if explicitGroup {
			assert.Equal(t, 0, groupGen.calls)
			assert.Equal(t, "explicit-group", aws.ToString(client.publishInput.MessageGroupId))
		} else {
			assert.Equal(t, 1, groupGen.calls)
			assert.Equal(t, "gen-group", aws.ToString(client.publishInput.MessageGroupId))
		}

		if explicitDedup {
			assert.Equal(t, 0, dedupGen.calls)
			assert.Equal(t, "explicit-dedup", aws.ToString(client.publishInput.MessageDeduplicationId))
		} else {
			assert.Equal(t, 1, dedupGen.calls)
			assert.Equal(t, "gen-dedup", aws.ToString(client.publishInput.MessageDeduplicationId))
		}
	})
}

func TestProducer_Publish_StandardTopicSkipsGenerators(t *testing.T) {
	groupGen := &stubGenerator{value: "gen-group"}
	dedupGen := &stubGenerator{value: "gen-dedup"}
	client := &fakeSNSClient{output: &sns.PublishOutput{MessageId: aws.String("msg")}}
	p, err := producer.New(client,
		producer.WithGroupIDGenerator(groupGen),
		producer.WithDeduplicationIDGenerator(dedupGen),
	)
	require.NoError(t, err)

	id, err := p.Publish(context.Background(), &producer.PublishInput{
		TopicARN:   "arn:aws:sns:us-east-1:000:topic",
		Message:    "hello",
		Attributes: map[string]string{"tenant": "acme"},
	})

	require.NoError(t, err)
	assert.Equal(t, "msg", id)
	assert.Equal(t, 1, client.calls)
	assert.Equal(t, 0, groupGen.calls)
	assert.Equal(t, 0, dedupGen.calls)
	assert.Nil(t, client.publishInput.MessageGroupId)
	assert.Nil(t, client.publishInput.MessageDeduplicationId)
}

func TestProducer_Publish_StandardTopicKeepsExplicitIDs(t *testing.T) {
	groupGen := &stubGenerator{value: "gen-group"}
	dedupGen := &stubGenerator{value: "gen-dedup"}
	client := &fakeSNSClient{output: &sns.PublishOutput{MessageId: aws.String("msg")}}
	p, err := producer.New(client,
		producer.WithGroupIDGenerator(groupGen),
		producer.WithDeduplicationIDGenerator(dedupGen),
	)
	require.NoError(t, err)

	id, err := p.Publish(context.Background(), &producer.PublishInput{
		TopicARN:        "arn:aws:sns:us-east-1:000:topic",
		Message:         "hello",
		GroupID:         "explicit-group",
		DeduplicationID: "explicit-dedup",
	})

	require.NoError(t, err)
	assert.Equal(t, "msg", id)
	assert.Equal(t, 0, groupGen.calls)
	assert.Equal(t, 0, dedupGen.calls)
	assert.Equal(t, "explicit-group", aws.ToString(client.publishInput.MessageGroupId))
	assert.Equal(t, "explicit-dedup", aws.ToString(client.publishInput.MessageDeduplicationId))
}

func TestProducer_PublishBatch_StandardTopicSkipsGenerators(t *testing.T) {
	groupGen := &stubGenerator{value: "gen-group"}
	dedupGen := &stubGenerator{value: "gen-dedup"}
	client := &fakeSNSClient{batchOutput: &sns.PublishBatchOutput{}}
	p, err := producer.New(client,
		producer.WithGroupIDGenerator(groupGen),
		producer.WithDeduplicationIDGenerator(dedupGen),
	)
	require.NoError(t, err)

	_, err = p.PublishBatch(context.Background(), &producer.PublishBatchInput{
		TopicARN: "arn:aws:sns:us-east-1:000:topic",
		Messages: []*producer.PublishBatchEntry{
			{ID: "e1", Message: "hello", Attributes: map[string]string{"tenant": "acme"}},
			{ID: "e2", Message: "world", GroupID: "explicit", DeduplicationID: "explicit-dedup"},
		},
	})

	require.NoError(t, err)
	assert.Equal(t, 0, groupGen.calls)
	assert.Equal(t, 0, dedupGen.calls)

	entries := client.publishBatchInput.PublishBatchRequestEntries
	require.Len(t, entries, 2)

	assert.Nil(t, entries[0].MessageGroupId)
	assert.Nil(t, entries[0].MessageDeduplicationId)

	assert.Equal(t, "explicit", aws.ToString(entries[1].MessageGroupId))
	assert.Equal(t, "explicit-dedup", aws.ToString(entries[1].MessageDeduplicationId))
}

func TestProducer_Publish_FIFOGateProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		fifo := rapid.Bool().Draw(t, "fifo")
		topicARN := "arn:aws:sns:us-east-1:000:topic"
		if fifo {
			topicARN += ".fifo"
		}

		groupGen := &stubGenerator{value: "gen-group"}
		dedupGen := &stubGenerator{value: "gen-dedup"}
		client := &fakeSNSClient{output: &sns.PublishOutput{MessageId: aws.String("msg")}}
		p, err := producer.New(client,
			producer.WithGroupIDGenerator(groupGen),
			producer.WithDeduplicationIDGenerator(dedupGen),
		)
		require.NoError(t, err)

		_, err = p.Publish(context.Background(), &producer.PublishInput{
			TopicARN:   topicARN,
			Message:    "hello",
			Attributes: map[string]string{"tenant": "acme"},
		})
		require.NoError(t, err)

		if fifo {
			assert.Equal(t, 1, groupGen.calls)
			assert.Equal(t, 1, dedupGen.calls)
			assert.Equal(t, "gen-group", aws.ToString(client.publishInput.MessageGroupId))
			assert.Equal(t, "gen-dedup", aws.ToString(client.publishInput.MessageDeduplicationId))
		} else {
			assert.Equal(t, 0, groupGen.calls)
			assert.Equal(t, 0, dedupGen.calls)
			assert.Nil(t, client.publishInput.MessageGroupId)
			assert.Nil(t, client.publishInput.MessageDeduplicationId)
		}
	})
}

func TestProducer_PublishBatch_FIFOGateProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		fifo := rapid.Bool().Draw(t, "fifo")
		topicARN := "arn:aws:sns:us-east-1:000:topic"
		if fifo {
			topicARN += ".fifo"
		}

		groupGen := &stubGenerator{value: "gen-group"}
		dedupGen := &stubGenerator{value: "gen-dedup"}
		client := &fakeSNSClient{batchOutput: &sns.PublishBatchOutput{}}
		p, err := producer.New(client,
			producer.WithGroupIDGenerator(groupGen),
			producer.WithDeduplicationIDGenerator(dedupGen),
		)
		require.NoError(t, err)

		_, err = p.PublishBatch(context.Background(), &producer.PublishBatchInput{
			TopicARN: topicARN,
			Messages: []*producer.PublishBatchEntry{
				{ID: "e1", Message: "hello", Attributes: map[string]string{"tenant": "acme"}},
			},
		})
		require.NoError(t, err)

		entry := client.publishBatchInput.PublishBatchRequestEntries[0]
		if fifo {
			assert.Equal(t, 1, groupGen.calls)
			assert.Equal(t, 1, dedupGen.calls)
			assert.Equal(t, "gen-group", aws.ToString(entry.MessageGroupId))
			assert.Equal(t, "gen-dedup", aws.ToString(entry.MessageDeduplicationId))
		} else {
			assert.Equal(t, 0, groupGen.calls)
			assert.Equal(t, 0, dedupGen.calls)
			assert.Nil(t, entry.MessageGroupId)
			assert.Nil(t, entry.MessageDeduplicationId)
		}
	})
}
