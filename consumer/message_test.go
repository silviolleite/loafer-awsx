package consumer

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"pgregory.net/rapid"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
		goleak.IgnoreAnyFunction("net/http.(*http2ClientConn).readLoop"),
	)
}

type payload struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

func buildEnvelope(t *testing.T, inner string, attrs map[string]messageAttribute, ts time.Time) string {
	t.Helper()
	env := map[string]any{
		"Timestamp":         ts,
		"Message":           inner,
		"MessageAttributes": attrs,
	}
	b, err := json.Marshal(env)
	require.NoError(t, err)
	return string(b)
}

func TestNewMessageParsesEnvelope(t *testing.T) {
	ts := time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC)
	body := buildEnvelope(t, `{"name":"loafer","count":7}`, map[string]messageAttribute{
		"trace": {Type: "String", Value: "abc-123"},
	}, ts)

	tests := []struct {
		name string
		msg  types.Message
	}{
		{
			name: "full body",
			msg:  types.Message{Body: aws.String(body)},
		},
		{
			name: "nil body yields zero envelope",
			msg:  types.Message{},
		},
		{
			name: "malformed body yields zero envelope",
			msg:  types.Message{Body: aws.String("{not-json")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newMessage(tt.msg)
			require.NotNil(t, m)
			assert.NotNil(t, m.dispatched)
			assert.NotNil(t, m.backoffChannel)
			assert.Equal(t, 1, cap(m.backoffChannel))
		})
	}
}

func TestMessageBody(t *testing.T) {
	tests := []struct {
		name string
		msg  types.Message
		want []byte
	}{
		{
			name: "present body",
			msg:  types.Message{Body: aws.String("raw-body")},
			want: []byte("raw-body"),
		},
		{
			name: "nil body",
			msg:  types.Message{},
			want: []byte{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newMessage(tt.msg)
			assert.Equal(t, tt.want, m.Body())
		})
	}
}

func TestMessageDecode(t *testing.T) {
	body := buildEnvelope(t, "", nil, time.Time{})

	t.Run("decodes envelope body", func(t *testing.T) {
		m := newMessage(types.Message{Body: aws.String(body)})
		var out snsEnvelope
		require.NoError(t, m.Decode(&out))
		assert.Equal(t, "", out.Message)
	})

	t.Run("returns error on invalid target", func(t *testing.T) {
		m := newMessage(types.Message{Body: aws.String(`{"count":"x"}`)})
		var out payload
		assert.Error(t, m.Decode(&out))
	})
}

func TestMessageDecodeMessage(t *testing.T) {
	inner := `{"name":"loafer","count":7}`
	body := buildEnvelope(t, inner, nil, time.Time{})

	t.Run("decodes inner message", func(t *testing.T) {
		m := newMessage(types.Message{Body: aws.String(body)})
		var out payload
		require.NoError(t, m.DecodeMessage(&out))
		assert.Equal(t, payload{Name: "loafer", Count: 7}, out)
	})

	t.Run("returns error on invalid inner json", func(t *testing.T) {
		bad := buildEnvelope(t, "{bad", nil, time.Time{})
		m := newMessage(types.Message{Body: aws.String(bad)})
		var out payload
		assert.Error(t, m.DecodeMessage(&out))
	})
}

func TestMessageAttribute(t *testing.T) {
	body := buildEnvelope(t, "", map[string]messageAttribute{
		"trace":  {Type: "String", Value: "abc-123"},
		"region": {Type: "String", Value: "us-east-1"},
	}, time.Time{})
	m := newMessage(types.Message{Body: aws.String(body)})

	tests := []struct {
		name string
		key  string
		want string
	}{
		{name: "existing key", key: "trace", want: "abc-123"},
		{name: "second key", key: "region", want: "us-east-1"},
		{name: "missing key", key: "absent", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, m.Attribute(tt.key))
		})
	}
}

func TestMessageAttributes(t *testing.T) {
	body := buildEnvelope(t, "", map[string]messageAttribute{
		"trace":  {Type: "String", Value: "abc-123"},
		"region": {Type: "String", Value: "us-east-1"},
	}, time.Time{})
	m := newMessage(types.Message{Body: aws.String(body)})

	got := m.Attributes()
	assert.Equal(t, map[string]string{"trace": "abc-123", "region": "us-east-1"}, got)

	got["mutation"] = "x"
	assert.NotContains(t, m.Attributes(), "mutation")
}

func TestMessageSystemAttributeByKey(t *testing.T) {
	m := newMessage(types.Message{
		Body: aws.String(buildEnvelope(t, "", nil, time.Time{})),
		Attributes: map[string]string{
			"SentTimestamp":           "1700000000000",
			"ApproximateReceiveCount": "3",
		},
	})

	tests := []struct {
		name string
		key  string
		want string
	}{
		{name: "existing key", key: "ApproximateReceiveCount", want: "3"},
		{name: "missing key", key: "absent", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, m.SystemAttributeByKey(tt.key))
		})
	}
}

func TestMessageSystemAttributes(t *testing.T) {
	attrs := map[string]string{"SentTimestamp": "1700000000000", "ApproximateReceiveCount": "3"}
	m := newMessage(types.Message{
		Body:       aws.String(buildEnvelope(t, "", nil, time.Time{})),
		Attributes: attrs,
	})

	got := m.SystemAttributes()
	assert.Equal(t, attrs, got)

	got["mutation"] = "x"
	assert.NotContains(t, m.SystemAttributes(), "mutation")
}

func TestMessageMetadata(t *testing.T) {
	body := buildEnvelope(t, "", map[string]messageAttribute{
		"trace": {Type: "String", Value: "abc-123"},
	}, time.Time{})
	m := newMessage(types.Message{Body: aws.String(body)})

	assert.Equal(t, map[string]string{"trace": "abc-123"}, m.Metadata())
}

func TestMessageIdentifier(t *testing.T) {
	tests := []struct {
		name string
		msg  types.Message
		want string
	}{
		{
			name: "present receipt handle",
			msg:  types.Message{ReceiptHandle: aws.String("receipt-42")},
			want: "receipt-42",
		},
		{
			name: "nil receipt handle",
			msg:  types.Message{},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newMessage(tt.msg)
			assert.Equal(t, tt.want, m.Identifier())
		})
	}
}

func TestMessageMessage(t *testing.T) {
	inner := `{"name":"loafer"}`
	body := buildEnvelope(t, inner, nil, time.Time{})
	m := newMessage(types.Message{Body: aws.String(body)})
	assert.Equal(t, inner, m.Message())
}

func TestMessageTimeStamp(t *testing.T) {
	ts := time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC)
	body := buildEnvelope(t, "", nil, ts)
	m := newMessage(types.Message{Body: aws.String(body)})
	assert.True(t, ts.Equal(m.TimeStamp()))
}

func TestMessageDispatch(t *testing.T) {
	m := newMessage(types.Message{})

	m.Dispatch()

	select {
	case _, ok := <-m.dispatchSignal():
		assert.False(t, ok)
	default:
		t.Fatal("dispatch channel was not closed")
	}
}

func TestMessageDispatchIsIdempotent(t *testing.T) {
	m := newMessage(types.Message{})

	assert.NotPanics(t, func() {
		m.Dispatch()
		m.Dispatch()
		m.Dispatch()
	})
}

func TestMessageBackoff(t *testing.T) {
	m := newMessage(types.Message{})
	assert.False(t, m.BackedOff())

	m.Backoff(15 * time.Second)

	assert.True(t, m.BackedOff())
	select {
	case got := <-m.backoffSignal():
		assert.Equal(t, 15*time.Second, got)
	default:
		t.Fatal("backoff channel had no value")
	}
}

func TestMessageBackoffDoesNotBlock(t *testing.T) {
	m := newMessage(types.Message{})

	done := make(chan struct{})
	go func() {
		m.Backoff(time.Second)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Backoff blocked the caller")
	}
	assert.True(t, m.BackedOff())
}

func TestMessageDecodeRoundTripProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		want := payload{
			Name:  rapid.String().Draw(rt, "name"),
			Count: rapid.Int().Draw(rt, "count"),
		}
		inner, err := json.Marshal(want)
		require.NoError(rt, err)

		body := buildEnvelope(t, string(inner), nil, time.Time{})
		m := newMessage(types.Message{Body: aws.String(body)})

		var gotInner payload
		require.NoError(rt, m.DecodeMessage(&gotInner))
		assert.Equal(rt, want, gotInner)

		assert.Equal(rt, string(inner), m.Message())
	})
}

func TestMessageAttributesRoundTripProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		keys := rapid.SliceOfDistinct(
			rapid.StringMatching(`[a-zA-Z][a-zA-Z0-9_]{0,10}`),
			func(s string) string { return s },
		).Draw(rt, "keys")

		attrs := make(map[string]messageAttribute, len(keys))
		want := make(map[string]string, len(keys))
		for _, k := range keys {
			v := rapid.String().Draw(rt, "value-"+k)
			attrs[k] = messageAttribute{Type: "String", Value: v}
			want[k] = v
		}

		body := buildEnvelope(t, "", attrs, time.Time{})
		m := newMessage(types.Message{Body: aws.String(body)})

		if len(want) == 0 {
			assert.Empty(rt, m.Attributes())
			return
		}
		assert.Equal(rt, want, m.Attributes())
		for k, v := range want {
			assert.Equal(rt, v, m.Attribute(k))
		}
	})
}

func TestMessageUserMessageAttribute(t *testing.T) {
	m := newMessage(types.Message{
		MessageAttributes: map[string]types.MessageAttributeValue{
			"trace":  {DataType: aws.String("String"), StringValue: aws.String("abc-123")},
			"region": {DataType: aws.String("String"), StringValue: aws.String("us-east-1")},
			"binary": {DataType: aws.String("Binary")},
		},
	})

	tests := []struct {
		name string
		key  string
		want string
	}{
		{name: "existing key", key: "trace", want: "abc-123"},
		{name: "second key", key: "region", want: "us-east-1"},
		{name: "missing key", key: "absent", want: ""},
		{name: "nil string value", key: "binary", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, m.UserMessageAttribute(tt.key))
		})
	}
}

func TestMessageUserMessageAttributeEmptyMap(t *testing.T) {
	m := newMessage(types.Message{})
	assert.Equal(t, "", m.UserMessageAttribute("any"))
}

func TestMessageUserMessageAttributes(t *testing.T) {
	m := newMessage(types.Message{
		MessageAttributes: map[string]types.MessageAttributeValue{
			"trace":  {DataType: aws.String("String"), StringValue: aws.String("abc-123")},
			"region": {DataType: aws.String("String"), StringValue: aws.String("us-east-1")},
			"binary": {DataType: aws.String("Binary")},
		},
	})

	got := m.UserMessageAttributes()
	assert.Equal(t, map[string]string{"trace": "abc-123", "region": "us-east-1"}, got)

	got["mutation"] = "x"
	assert.NotContains(t, m.UserMessageAttributes(), "mutation")
}

func TestMessageUserMessageAttributesEmptyMap(t *testing.T) {
	m := newMessage(types.Message{})
	assert.Empty(t, m.UserMessageAttributes())
}
