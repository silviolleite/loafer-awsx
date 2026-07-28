package typed_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/middleware"
	"github.com/silviolleite/loafer-awsx/typed"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type bodyMessage struct {
	body []byte
}

func (bodyMessage) Decode(any) error                         { return nil }
func (bodyMessage) DecodeMessage(any) error                  { return nil }
func (bodyMessage) Attribute(string) string                  { return "" }
func (bodyMessage) Attributes() map[string]string            { return nil }
func (bodyMessage) UserMessageAttribute(string) string       { return "" }
func (bodyMessage) UserMessageAttributes() map[string]string { return nil }
func (bodyMessage) SystemAttributeByKey(string) string       { return "" }
func (bodyMessage) SystemAttributes() map[string]string      { return nil }
func (bodyMessage) Metadata() map[string]string              { return nil }
func (bodyMessage) Identifier() string                       { return "" }
func (m bodyMessage) Body() []byte                           { return m.body }
func (bodyMessage) Message() string                          { return "" }
func (bodyMessage) TimeStamp() time.Time                     { return time.Time{} }

var _ middleware.Message = bodyMessage{}

func TestWrapHandlerDecodesAndInvokesTypedHandler(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		original := drawPayload(rt)

		codec := typed.JSONCodec[payload]{}
		encoded, err := codec.Encode(original)
		require.NoError(rt, err)

		var (
			called   bool
			received payload
		)
		handler := typed.WrapHandler(codec, func(_ context.Context, msg payload) error {
			called = true
			received = msg
			return nil
		})

		err = handler(context.Background(), bodyMessage{body: encoded})
		require.NoError(rt, err)
		assert.True(rt, called)
		assert.Equal(rt, original, received)
	})
}

func TestWrapHandlerReturnsDecodeErrorWithoutCallingHandler(t *testing.T) {
	tests := []struct {
		name string
		body []byte
	}{
		{name: "truncated object", body: []byte("{")},
		{name: "type mismatch", body: []byte(`{"count":"nope"}`)},
		{name: "empty body", body: []byte("")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			handler := typed.WrapHandler(typed.JSONCodec[payload]{}, func(_ context.Context, _ payload) error {
				called = true
				return nil
			})

			err := handler(context.Background(), bodyMessage{body: tt.body})

			require.Error(t, err)
			assert.False(t, called)
		})
	}
}

func TestWrapHandlerPropagatesHandlerError(t *testing.T) {
	handlerErr := errors.New("handler boom")

	handler := typed.WrapHandler(typed.JSONCodec[payload]{}, func(_ context.Context, _ payload) error {
		return handlerErr
	})

	err := handler(context.Background(), bodyMessage{body: []byte(`{"id":"a"}`)})

	require.ErrorIs(t, err, handlerErr)
}

func TestWrapHandlerPassesContext(t *testing.T) {
	type ctxKey struct{}

	var got any
	handler := typed.WrapHandler(typed.JSONCodec[payload]{}, func(ctx context.Context, _ payload) error {
		got = ctx.Value(ctxKey{})
		return nil
	})

	ctx := context.WithValue(context.Background(), ctxKey{}, "value")
	err := handler(ctx, bodyMessage{body: []byte(`{}`)})

	require.NoError(t, err)
	assert.Equal(t, "value", got)
}
