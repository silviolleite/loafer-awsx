package fake

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/silviolleite/loafer-awsx/consumer"
)

// Compile-time assertion that Message satisfies the full consumer.Message
// interface, which embeds middleware.Message. Satisfying consumer.Message
// therefore also satisfies middleware.Message.
var _ consumer.Message = (*Message)(nil)

// Message is a configurable test double for the consumer.Message and
// middleware.Message interfaces. Every accessor returns a value taken from an
// exported field, so tests can construct a Message literal and assert on the
// behavior of code that consumes it. Mutating methods (Dispatch and Backoff)
// are recorded so tests can verify the message lifecycle.
//
// A zero Message is usable: accessors return zero values and the map accessors
// return empty, non-nil maps. A Message must be used through a pointer because
// its methods have pointer receivers.
type Message struct {
	TimeStampValue        time.Time
	MetadataValues        map[string]string
	AttributeValues       map[string]string
	SystemAttributeValues map[string]string
	DecodeFunc            func(out any) error
	DecodeMessageFunc     func(out any) error
	MessageValue          string
	IdentifierValue       string
	BodyData              []byte
	backoffDelays         []time.Duration
	dispatchCount         int
	mu                    sync.Mutex
	BackedOffValue        bool
	backedOff             bool
}

// Body returns the configured raw message body.
func (m *Message) Body() []byte {
	return m.BodyData
}

// Decode unmarshals the configured body into out. When DecodeFunc is set it is
// used instead, letting tests inject decoding errors or custom behavior.
func (m *Message) Decode(out any) error {
	if m.DecodeFunc != nil {
		return m.DecodeFunc(out)
	}
	return json.Unmarshal(m.BodyData, out)
}

// DecodeMessage unmarshals the configured inner message into out. When
// DecodeMessageFunc is set it is used instead.
func (m *Message) DecodeMessage(out any) error {
	if m.DecodeMessageFunc != nil {
		return m.DecodeMessageFunc(out)
	}
	return json.Unmarshal([]byte(m.MessageValue), out)
}

// Attribute returns the custom attribute for key, or the empty string when the
// key is absent.
func (m *Message) Attribute(key string) string {
	return m.AttributeValues[key]
}

// Attributes returns a copy of the configured custom attributes. The result is
// always non-nil.
func (m *Message) Attributes() map[string]string {
	return copyStringMap(m.AttributeValues)
}

// SystemAttributeByKey returns the system attribute for key, or the empty
// string when the key is absent.
func (m *Message) SystemAttributeByKey(key string) string {
	return m.SystemAttributeValues[key]
}

// SystemAttributes returns a copy of the configured system attributes. The
// result is always non-nil.
func (m *Message) SystemAttributes() map[string]string {
	return copyStringMap(m.SystemAttributeValues)
}

// Metadata returns a copy of the configured metadata. The result is always
// non-nil.
func (m *Message) Metadata() map[string]string {
	return copyStringMap(m.MetadataValues)
}

// Identifier returns the configured receipt handle.
func (m *Message) Identifier() string {
	return m.IdentifierValue
}

// Message returns the configured inner message content.
func (m *Message) Message() string {
	return m.MessageValue
}

// TimeStamp returns the configured message timestamp.
func (m *Message) TimeStamp() time.Time {
	return m.TimeStampValue
}

// Dispatch records that processing completed. It is safe to call concurrently
// and can be inspected with DispatchCount.
func (m *Message) Dispatch() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dispatchCount++
}

// DispatchCount reports how many times Dispatch has been called.
func (m *Message) DispatchCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.dispatchCount
}

// Backoff records a redelivery delay and marks the message as backed off. It is
// safe to call concurrently and the recorded delays can be inspected with
// BackoffDelays.
func (m *Message) Backoff(delay time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.backedOff = true
	m.backoffDelays = append(m.backoffDelays, delay)
}

// BackoffDelays returns a copy of the delays passed to Backoff, in call order.
func (m *Message) BackoffDelays() []time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]time.Duration, len(m.backoffDelays))
	copy(out, m.backoffDelays)
	return out
}

// BackedOff reports whether Backoff has been called. Before any call it returns
// BackedOffValue.
func (m *Message) BackedOff() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.backedOff || m.BackedOffValue
}

// copyStringMap returns a fresh, non-nil copy of src.
func copyStringMap(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
