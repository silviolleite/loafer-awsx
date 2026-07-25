package typed

import (
	"context"

	"github.com/silviolleite/loafer-awsx/producer"
)

// Producer wraps a standard producer.Producer with type-safe encoding. It
// encodes a value of type T through its Codec before delegating the publish to
// the underlying producer. A Producer holds only immutable references after
// construction and adds no mutable shared state, so it is safe for concurrent
// use by multiple goroutines whenever the underlying producer.Producer is.
type Producer[T any] struct {
	producer *producer.Producer
	codec    Codec[T]
}

// NewProducer creates a typed Producer that encodes messages of type T with the
// supplied codec before publishing them through the given producer.Producer.
func NewProducer[T any](p *producer.Producer, codec Codec[T]) *Producer[T] {
	return &Producer[T]{
		producer: p,
		codec:    codec,
	}
}

// publishConfig accumulates the optional publish settings applied through
// PublishOption values before the underlying publish is performed.
type publishConfig struct {
	attributes      map[string]string
	groupID         string
	deduplicationID string
}

// PublishOption configures a typed publish operation.
type PublishOption func(*publishConfig)

// WithGroupID sets the MessageGroupId forwarded to the underlying producer for
// FIFO topics.
func WithGroupID(id string) PublishOption {
	return func(c *publishConfig) {
		c.groupID = id
	}
}

// WithDeduplicationID sets the MessageDeduplicationId forwarded to the
// underlying producer.
func WithDeduplicationID(id string) PublishOption {
	return func(c *publishConfig) {
		c.deduplicationID = id
	}
}

// WithAttributes sets the message attributes forwarded to the underlying
// producer as SNS message attributes.
func WithAttributes(attrs map[string]string) PublishOption {
	return func(c *publishConfig) {
		c.attributes = attrs
	}
}

// Publish encodes value with the configured codec and publishes the result to
// topicARN through the underlying producer, returning the SNS message ID on
// success.
//
// The value is encoded before any call to SNS: when encoding fails the error is
// returned immediately and the underlying producer is not invoked. The publish
// options (WithGroupID, WithDeduplicationID, WithAttributes) are forwarded to
// the underlying producer.Publish unchanged.
func (tp *Producer[T]) Publish(ctx context.Context, topicARN string, value T, opts ...PublishOption) (string, error) {
	encoded, err := tp.codec.Encode(value)
	if err != nil {
		return "", err
	}

	cfg := &publishConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
	}

	return tp.producer.Publish(ctx, &producer.PublishInput{
		TopicARN:        topicARN,
		Message:         string(encoded),
		GroupID:         cfg.groupID,
		DeduplicationID: cfg.deduplicationID,
		Attributes:      cfg.attributes,
	})
}
