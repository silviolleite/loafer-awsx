package middleware_test

import (
	"bytes"
	"context"
	stderrors "errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	liberrors "github.com/silviolleite/loafer-awsx/errors"
	"github.com/silviolleite/loafer-awsx/middleware"
)

type stubMessage struct {
	id string
}

func (s stubMessage) Decode(any) error                         { return nil }
func (s stubMessage) DecodeMessage(any) error                  { return nil }
func (s stubMessage) Attribute(string) string                  { return "" }
func (s stubMessage) Attributes() map[string]string            { return nil }
func (s stubMessage) UserMessageAttribute(string) string       { return "" }
func (s stubMessage) UserMessageAttributes() map[string]string { return nil }
func (s stubMessage) SystemAttributeByKey(string) string       { return "" }
func (s stubMessage) SystemAttributes() map[string]string      { return nil }
func (s stubMessage) Metadata() map[string]string              { return nil }
func (s stubMessage) Identifier() string                       { return s.id }
func (s stubMessage) Body() []byte                             { return nil }
func (s stubMessage) Message() string                          { return "" }
func (s stubMessage) TimeStamp() time.Time                     { return time.Time{} }

func newCapturingLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	return slog.New(handler), &buf
}

func TestRecoveryCatchesPanic(t *testing.T) {
	tests := []struct {
		name       string
		panicValue any
		wantSubstr string
	}{
		{"string panic", "boom", "boom"},
		{"error panic", stderrors.New("kaboom"), "kaboom"},
		{"int panic", 42, "42"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log, buf := newCapturingLogger()

			handler := func(context.Context, middleware.Message) error {
				panic(tt.panicValue)
			}

			wrapped := middleware.Recovery(log)(handler)

			err := wrapped(context.Background(), stubMessage{id: "msg-1"})

			require.Error(t, err)
			assert.ErrorIs(t, err, liberrors.ErrPanic)
			assert.Contains(t, err.Error(), tt.wantSubstr)

			out := buf.String()
			assert.Contains(t, out, "level=ERROR")
			assert.Contains(t, out, "panic recovered")
			assert.Contains(t, out, "stack=")
			assert.Contains(t, out, "goroutine")
			assert.Contains(t, out, "message_id=msg-1")
		})
	}
}

func TestRecoveryPassesThroughSuccess(t *testing.T) {
	log, buf := newCapturingLogger()

	handler := func(context.Context, middleware.Message) error {
		return nil
	}

	wrapped := middleware.Recovery(log)(handler)

	err := wrapped(context.Background(), stubMessage{id: "ok"})

	assert.NoError(t, err)
	assert.Empty(t, buf.String())
}

func TestRecoveryPassesThroughHandlerError(t *testing.T) {
	log, buf := newCapturingLogger()
	sentinel := stderrors.New("handler failure")

	handler := func(context.Context, middleware.Message) error {
		return sentinel
	}

	wrapped := middleware.Recovery(log)(handler)

	err := wrapped(context.Background(), stubMessage{id: "err"})

	assert.ErrorIs(t, err, sentinel)
	assert.NotErrorIs(t, err, liberrors.ErrPanic)
	assert.Empty(t, buf.String())
}

func TestRecoveryHandlesNilMessage(t *testing.T) {
	log, buf := newCapturingLogger()

	handler := func(context.Context, middleware.Message) error {
		panic("no message")
	}

	wrapped := middleware.Recovery(log)(handler)

	err := wrapped(context.Background(), nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, liberrors.ErrPanic)
	assert.Contains(t, err.Error(), "no message")

	out := buf.String()
	assert.Contains(t, out, "level=ERROR")
	assert.NotContains(t, out, "message_id=")
}

func TestRecoveryCatchesArbitraryPanicProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		value := rapid.String().Draw(rt, "panicValue")
		id := rapid.String().Draw(rt, "messageID")

		log, buf := newCapturingLogger()

		handler := func(context.Context, middleware.Message) error {
			panic(value)
		}

		wrapped := middleware.Recovery(log)(handler)

		err := wrapped(context.Background(), stubMessage{id: id})

		require.Error(rt, err)
		assert.True(rt, stderrors.Is(err, liberrors.ErrPanic))
		assert.Contains(rt, err.Error(), fmt.Sprintf("%v", value))
		assert.Contains(rt, buf.String(), "level=ERROR")
	})
}

func TestRecoveryPassesThroughResultProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		fail := rapid.Bool().Draw(rt, "fail")
		msg := rapid.String().Draw(rt, "errMsg")

		log, buf := newCapturingLogger()

		var want error
		if fail {
			want = stderrors.New(msg)
		}

		handler := func(context.Context, middleware.Message) error {
			return want
		}

		wrapped := middleware.Recovery(log)(handler)

		err := wrapped(context.Background(), stubMessage{id: "p"})

		if fail {
			assert.ErrorIs(rt, err, want)
		} else {
			assert.NoError(rt, err)
		}
		assert.Empty(rt, buf.String())
	})
}
