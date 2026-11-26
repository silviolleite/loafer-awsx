package consumer

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/silviolleite/loafer-awsx/middleware"
)

// Message represents a received SQS message together with the controls needed
// to drive its processing lifecycle. It extends the minimal middleware.Message
// with JSON decoding helpers, visibility-timeout signaling, and backoff state.
type Message interface {
	middleware.Message

	// Decode unmarshals the raw message body into out using JSON.
	Decode(out any) error
	// DecodeMessage unmarshals the inner SNS Message field into out using JSON.
	DecodeMessage(out any) error
	// Identifier returns the message receipt handle.
	Identifier() string
	// TimeStamp returns the message timestamp.
	TimeStamp() time.Time
	// Dispatch marks processing as completed. It is idempotent. The consumer
	// stops managing the message visibility timeout as soon as the message
	// reaches an outcome, so handlers do not need to call it.
	Dispatch()
	// Backoff requests that redelivery of the message be delayed by delay and
	// marks the message as backed off. It never blocks; when it is called more
	// than once, the last delay wins. The consumer applies the delay when the
	// handler returns.
	Backoff(delay time.Duration)
	// BackedOff reports whether Backoff has been called for the message.
	BackedOff() bool
}

// Compile-time assertion that message satisfies the Message interface (and, by
// embedding, middleware.Message).
var _ Message = (*message)(nil)

// messageAttribute is the value shape SNS uses for each entry of the
// MessageAttributes map inside the SNS-to-SQS envelope.
type messageAttribute struct {
	Type  string `json:"Type"`
	Value string `json:"Value"`
}

// snsEnvelope models the JSON payload SNS writes into the SQS message body when
// a topic fans out to a queue. Only the fields consumed by the message
// accessors are declared.
type snsEnvelope struct {
	Timestamp         time.Time                   `json:"Timestamp"`
	MessageAttributes map[string]messageAttribute `json:"MessageAttributes"`
	Message           string                      `json:"Message"`
}

// message is the internal implementation of Message. It wraps a single SQS
// types.Message, lazily parsed SNS envelope data, the instant it was received,
// and the handler's backoff request.
//
// A message is safe for concurrent use: the backoff state is stored atomically
// and Dispatch is idempotent. receivedAt is written by the poller before the
// message is shared and only read afterwards.
type message struct {
	receivedAt   time.Time
	dispatched   chan struct{}
	original     types.Message
	envelope     snsEnvelope
	groupKey     string
	backoffNanos atomic.Int64
	dispatchOnce sync.Once
	backedOff    atomic.Bool
}

// newMessage builds a message from an SQS types.Message. The body is parsed as
// an SNS envelope; a malformed or empty body simply yields zero-value envelope
// fields rather than an error, mirroring the resilient behavior expected during
// polling.
func newMessage(m types.Message) *message {
	var envelope snsEnvelope
	if m.Body != nil {
		_ = json.Unmarshal([]byte(*m.Body), &envelope)
	}

	return &message{
		original:   m,
		envelope:   envelope,
		dispatched: make(chan struct{}),
	}
}

// Body returns the raw message body as a byte slice, or an empty slice when the
// body is absent.
func (m *message) Body() []byte {
	if m.original.Body != nil {
		return []byte(*m.original.Body)
	}
	return []byte{}
}

// Decode unmarshals the raw message body into out using JSON.
func (m *message) Decode(out any) error {
	return json.Unmarshal(m.Body(), out)
}

// DecodeMessage unmarshals the inner SNS Message field into out using JSON.
func (m *message) DecodeMessage(out any) error {
	return json.Unmarshal([]byte(m.envelope.Message), out)
}

// Attribute returns a single custom attribute by key, or the empty string when
// the key is absent.
func (m *message) Attribute(key string) string {
	attr, ok := m.envelope.MessageAttributes[key]
	if !ok {
		return ""
	}
	return attr.Value
}

// Attributes returns all custom attributes as a string map. The returned map is
// a fresh copy owned by the caller.
func (m *message) Attributes() map[string]string {
	attrs := make(map[string]string, len(m.envelope.MessageAttributes))
	for k, v := range m.envelope.MessageAttributes {
		attrs[k] = v.Value
	}
	return attrs
}

// SystemAttributeByKey returns a single system attribute by key, or the empty
// string when the key is absent.
func (m *message) SystemAttributeByKey(key string) string {
	value, ok := m.original.Attributes[key]
	if !ok {
		return ""
	}
	return value
}

// SystemAttributes returns all system attributes as a string map. The returned
// map is a fresh copy owned by the caller.
func (m *message) SystemAttributes() map[string]string {
	attrs := make(map[string]string, len(m.original.Attributes))
	for k, v := range m.original.Attributes {
		attrs[k] = v
	}
	return attrs
}

// UserMessageAttribute returns a single native SQS user message attribute by
// key, or the empty string when the key is absent or carries no string value.
// These are the attributes carried directly on the SQS message
// (types.Message.MessageAttributes), distinct from the SNS-envelope attributes
// exposed by Attribute and the SQS system attributes exposed by
// SystemAttributeByKey.
func (m *message) UserMessageAttribute(key string) string {
	attr, ok := m.original.MessageAttributes[key]
	if !ok || attr.StringValue == nil {
		return ""
	}
	return *attr.StringValue
}

// UserMessageAttributes returns all native SQS user message attributes as a
// string map. The returned map is a fresh copy owned by the caller. Attributes
// without a string value are omitted.
func (m *message) UserMessageAttributes() map[string]string {
	attrs := make(map[string]string, len(m.original.MessageAttributes))
	for k, v := range m.original.MessageAttributes {
		if v.StringValue == nil {
			continue
		}
		attrs[k] = *v.StringValue
	}
	return attrs
}

// Metadata returns message metadata as a string map. Metadata is sourced from
// the SNS envelope message attributes. The returned map is a fresh copy owned
// by the caller.
func (m *message) Metadata() map[string]string {
	return m.Attributes()
}

// Identifier returns the message receipt handle, or the empty string when it is
// absent.
func (m *message) Identifier() string {
	if m.original.ReceiptHandle == nil {
		return ""
	}
	return *m.original.ReceiptHandle
}

// Message returns the inner message content as a string.
func (m *message) Message() string {
	return m.envelope.Message
}

// TimeStamp returns the message timestamp taken from the SNS envelope.
func (m *message) TimeStamp() time.Time {
	return m.envelope.Timestamp
}

// Dispatch signals processing completion by closing the dispatch channel. It is
// idempotent: calling it more than once has no effect and never panics.
func (m *message) Dispatch() {
	m.dispatchOnce.Do(func() {
		close(m.dispatched)
	})
}

// Backoff marks the message as backed off and records delay. It never blocks;
// the last recorded delay is applied when the handler returns.
func (m *message) Backoff(delay time.Duration) {
	m.backoffNanos.Store(int64(delay))
	m.backedOff.Store(true)
}

// backoffDelay returns the delay recorded by the last Backoff call.
func (m *message) backoffDelay() time.Duration {
	return time.Duration(m.backoffNanos.Load())
}

// BackedOff reports whether Backoff has been called for the message.
func (m *message) BackedOff() bool {
	return m.backedOff.Load()
}
