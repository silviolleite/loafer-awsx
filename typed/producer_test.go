package typed_test

import (
	"context"
	stderrors "errors"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/producer"
	"github.com/silviolleite/loafer-awsx/typed"
)

type fakeSNSClient struct {
	err          error
	publishInput *sns.PublishInput
	output       *sns.PublishOutput
	calls        int
	mu           sync.Mutex
}

func (f *fakeSNSClient) Publish(_ context.Context, params *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.publishInput = params
	f.calls++
	return f.output, f.err
}

func (f *fakeSNSClient) PublishBatch(_ context.Context, _ *sns.PublishBatchInput, _ ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
	return nil, nil
}

func (f *fakeSNSClient) snapshot() (*sns.PublishInput, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.publishInput, f.calls
}

type failingCodec struct {
	err error
}

func (c failingCodec) Encode(payload) ([]byte, error) {
	return nil, c.err
}

func (failingCodec) Decode([]byte) (payload, error) {
	return payload{}, nil
}

func newTypedProducer(t *testing.T, client producer.SNSClient) *typed.Producer[payload] {
	t.Helper()
	p, err := producer.New(client)
	require.NoError(t, err)
	return typed.NewProducer[payload](p, typed.JSONCodec[payload]{})
}

func TestTypedProducerEncodesValueBeforePublish(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		value := drawPayload(rt)

		codec := typed.JSONCodec[payload]{}
		encoded, err := codec.Encode(value)
		require.NoError(rt, err)

		client := &fakeSNSClient{output: &sns.PublishOutput{MessageId: aws.String("msg-id")}}
		p, err := producer.New(client)
		require.NoError(rt, err)
		tp := typed.NewProducer[payload](p, codec)

		id, err := tp.Publish(context.Background(), "arn:aws:sns:us-east-1:000000000000:topic", value)
		require.NoError(rt, err)
		assert.Equal(rt, "msg-id", id)

		input, calls := client.snapshot()
		require.Equal(rt, 1, calls)
		require.NotNil(rt, input)
		assert.Equal(rt, string(encoded), aws.ToString(input.Message))
		assert.Equal(rt, "arn:aws:sns:us-east-1:000000000000:topic", aws.ToString(input.TopicArn))
	})
}

func TestTypedProducerEncodeFailureDoesNotCallSNS(t *testing.T) {
	wantErr := stderrors.New("encode boom")
	client := &fakeSNSClient{output: &sns.PublishOutput{MessageId: aws.String("msg-id")}}
	p, err := producer.New(client)
	require.NoError(t, err)
	tp := typed.NewProducer[payload](p, failingCodec{err: wantErr})

	id, err := tp.Publish(context.Background(), "arn:topic", payload{})

	require.ErrorIs(t, err, wantErr)
	assert.Empty(t, id)
	_, calls := client.snapshot()
	assert.Equal(t, 0, calls)
}

func TestTypedProducerForwardsPublishOptions(t *testing.T) {
	client := &fakeSNSClient{output: &sns.PublishOutput{MessageId: aws.String("msg-id")}}
	tp := newTypedProducer(t, client)

	_, err := tp.Publish(
		context.Background(),
		"arn:aws:sns:us-east-1:000000000000:topic.fifo",
		payload{ID: "abc"},
		typed.WithGroupID("group-1"),
		typed.WithDeduplicationID("dedup-1"),
		typed.WithAttributes(map[string]string{"key": "value"}),
	)
	require.NoError(t, err)

	input, calls := client.snapshot()
	require.Equal(t, 1, calls)
	require.NotNil(t, input)
	assert.Equal(t, "group-1", aws.ToString(input.MessageGroupId))
	assert.Equal(t, "dedup-1", aws.ToString(input.MessageDeduplicationId))
	require.Contains(t, input.MessageAttributes, "key")
	assert.Equal(t, "value", aws.ToString(input.MessageAttributes["key"].StringValue))
	assert.Equal(t, "String", aws.ToString(input.MessageAttributes["key"].DataType))
}

func TestTypedProducerWithoutOptionsOmitsFIFOFields(t *testing.T) {
	client := &fakeSNSClient{output: &sns.PublishOutput{MessageId: aws.String("msg-id")}}
	tp := newTypedProducer(t, client)

	_, err := tp.Publish(context.Background(), "arn:topic", payload{ID: "abc"})
	require.NoError(t, err)

	input, _ := client.snapshot()
	require.NotNil(t, input)
	assert.Nil(t, input.MessageGroupId)
	assert.Nil(t, input.MessageDeduplicationId)
	assert.Empty(t, input.MessageAttributes)
}

func TestTypedProducerIgnoresNilOption(t *testing.T) {
	client := &fakeSNSClient{output: &sns.PublishOutput{MessageId: aws.String("msg-id")}}
	tp := newTypedProducer(t, client)

	id, err := tp.Publish(context.Background(), "arn:topic", payload{ID: "abc"}, nil)
	require.NoError(t, err)
	assert.Equal(t, "msg-id", id)
}

func TestTypedProducerPropagatesPublishError(t *testing.T) {
	wantErr := stderrors.New("sns failure")
	client := &fakeSNSClient{err: wantErr}
	tp := newTypedProducer(t, client)

	id, err := tp.Publish(context.Background(), "arn:topic", payload{ID: "abc"})

	require.ErrorIs(t, err, wantErr)
	assert.Empty(t, id)
}

func TestTypedProducerConcurrentPublish(t *testing.T) {
	client := &fakeSNSClient{output: &sns.PublishOutput{MessageId: aws.String("msg-id")}}
	tp := newTypedProducer(t, client)

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	errs := make([]error, goroutines)

	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = tp.Publish(
				context.Background(),
				"arn:aws:sns:us-east-1:000000000000:topic.fifo",
				payload{ID: "abc", Count: idx},
				typed.WithGroupID("group"),
			)
		}(i)
	}

	wg.Wait()

	for _, err := range errs {
		require.NoError(t, err)
	}

	_, calls := client.snapshot()
	assert.Equal(t, goroutines, calls)
}
