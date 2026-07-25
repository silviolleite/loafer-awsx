package middleware_test

import (
	"context"
	stderrors "errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/middleware"
)

type capturedRecord struct {
	attrs map[string]slog.Value
	msg   string
	level slog.Level
}

type captureHandler struct {
	mu      *sync.Mutex
	records *[]capturedRecord
}

func newCaptureLogger() (*slog.Logger, *[]capturedRecord) {
	records := &[]capturedRecord{}
	handler := captureHandler{mu: &sync.Mutex{}, records: records}
	return slog.New(handler), records
}

func (h captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h captureHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := make(map[string]slog.Value, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value
		return true
	})
	h.mu.Lock()
	*h.records = append(*h.records, capturedRecord{level: r.Level, msg: r.Message, attrs: attrs})
	h.mu.Unlock()
	return nil
}

func (h captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h captureHandler) WithGroup(string) slog.Handler { return h }

func TestLoggingLogsReceiptAndSuccess(t *testing.T) {
	log, records := newCaptureLogger()

	handler := func(context.Context, middleware.Message) error {
		return nil
	}

	wrapped := middleware.Logging(log)(handler)

	err := wrapped(context.Background(), stubMessage{id: "msg-1"})

	require.NoError(t, err)
	require.Len(t, *records, 2)

	receipt := (*records)[0]
	assert.Equal(t, slog.LevelInfo, receipt.level)
	assert.Equal(t, "message received", receipt.msg)
	require.Contains(t, receipt.attrs, "message_id")
	assert.Equal(t, "msg-1", receipt.attrs["message_id"].String())

	success := (*records)[1]
	assert.Equal(t, slog.LevelInfo, success.level)
	assert.Equal(t, "message processed", success.msg)
	assert.Equal(t, "msg-1", success.attrs["message_id"].String())
	require.Contains(t, success.attrs, "duration")
	assert.Equal(t, slog.KindDuration, success.attrs["duration"].Kind())
	assert.GreaterOrEqual(t, success.attrs["duration"].Duration(), time.Duration(0))
	assert.NotContains(t, success.attrs, "error")
}

func TestLoggingLogsError(t *testing.T) {
	log, records := newCaptureLogger()
	sentinel := stderrors.New("handler failure")

	handler := func(context.Context, middleware.Message) error {
		return sentinel
	}

	wrapped := middleware.Logging(log)(handler)

	err := wrapped(context.Background(), stubMessage{id: "msg-2"})

	require.ErrorIs(t, err, sentinel)
	require.Len(t, *records, 2)

	receipt := (*records)[0]
	assert.Equal(t, slog.LevelInfo, receipt.level)
	assert.Equal(t, "message received", receipt.msg)
	assert.Equal(t, "msg-2", receipt.attrs["message_id"].String())

	failure := (*records)[1]
	assert.Equal(t, slog.LevelError, failure.level)
	assert.Equal(t, "message processing failed", failure.msg)
	assert.Equal(t, "msg-2", failure.attrs["message_id"].String())
	require.Contains(t, failure.attrs, "duration")
	assert.Equal(t, slog.KindDuration, failure.attrs["duration"].Kind())
	assert.GreaterOrEqual(t, failure.attrs["duration"].Duration(), time.Duration(0))
	require.Contains(t, failure.attrs, "error")
	assert.Equal(t, sentinel, failure.attrs["error"].Any())
}

func TestLoggingNilMessageOmitsID(t *testing.T) {
	cases := []struct {
		err       error
		name      string
		wantMsg   string
		wantLevel slog.Level
	}{
		{name: "success", err: nil, wantLevel: slog.LevelInfo, wantMsg: "message processed"},
		{name: "error", err: stderrors.New("boom"), wantLevel: slog.LevelError, wantMsg: "message processing failed"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			log, records := newCaptureLogger()

			handler := func(context.Context, middleware.Message) error {
				return tc.err
			}

			wrapped := middleware.Logging(log)(handler)

			err := wrapped(context.Background(), nil)

			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}

			require.Len(t, *records, 2)
			for _, rec := range *records {
				assert.NotContains(t, rec.attrs, "message_id")
			}

			outcome := (*records)[1]
			assert.Equal(t, tc.wantLevel, outcome.level)
			assert.Equal(t, tc.wantMsg, outcome.msg)
			assert.Contains(t, outcome.attrs, "duration")
		})
	}
}

func TestLoggingPropagatesResultProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		id := rapid.String().Draw(rt, "messageID")
		fail := rapid.Bool().Draw(rt, "fail")
		errMsg := rapid.String().Draw(rt, "errMsg")

		log, records := newCaptureLogger()

		var want error
		if fail {
			want = stderrors.New(errMsg)
		}

		handler := func(context.Context, middleware.Message) error {
			return want
		}

		wrapped := middleware.Logging(log)(handler)

		err := wrapped(context.Background(), stubMessage{id: id})

		if fail {
			assert.ErrorIs(rt, err, want)
		} else {
			assert.NoError(rt, err)
		}

		require.Len(rt, *records, 2)

		receipt := (*records)[0]
		assert.Equal(rt, slog.LevelInfo, receipt.level)
		assert.Equal(rt, id, receipt.attrs["message_id"].String())

		outcome := (*records)[1]
		assert.Equal(rt, id, outcome.attrs["message_id"].String())
		assert.Contains(rt, outcome.attrs, "duration")
		assert.Equal(rt, slog.KindDuration, outcome.attrs["duration"].Kind())

		if fail {
			assert.Equal(rt, slog.LevelError, outcome.level)
			require.Contains(rt, outcome.attrs, "error")
			assert.Equal(rt, want, outcome.attrs["error"].Any())
		} else {
			assert.Equal(rt, slog.LevelInfo, outcome.level)
			assert.NotContains(rt, outcome.attrs, "error")
		}
	})
}
