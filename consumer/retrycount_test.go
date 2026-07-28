package consumer

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func messageWithRetryCount(present bool, value string) *message {
	msg := types.Message{ReceiptHandle: aws.String("receipt-handle")}
	if present {
		msg.MessageAttributes = map[string]types.MessageAttributeValue{
			retryCountAttribute: {
				DataType:    aws.String("String"),
				StringValue: aws.String(value),
			},
		}
	}
	return newMessage(msg)
}

// Feature: fifo-scheduled-retry, Property 2: Retry-count parsing defaults to zero
func TestParseRetryCountDefaultsToZero(t *testing.T) {
	log := discardLogger()

	rapid.Check(t, func(t *rapid.T) {
		kind := rapid.SampledFrom([]string{"absent", "valid", "arbitrary"}).Draw(t, "kind")

		var (
			msg  *message
			want int
		)

		switch kind {
		case "absent":
			msg = messageWithRetryCount(false, "")
			want = 0
		case "valid":
			n := rapid.IntRange(0, 2_147_483_647).Draw(t, "n")
			msg = messageWithRetryCount(true, strconv.Itoa(n))
			want = n
		default:
			raw := rapid.String().Draw(t, "raw")
			msg = messageWithRetryCount(true, raw)
			if parsed, err := strconv.Atoi(raw); err == nil && parsed >= 0 {
				want = parsed
			} else {
				want = 0
			}
		}

		assert.Equal(t, want, parseRetryCount(msg, log))
	})
}

func TestParseRetryCountValidValue(t *testing.T) {
	msg := messageWithRetryCount(true, "5")
	assert.Equal(t, 5, parseRetryCount(msg, discardLogger()))
}

func TestParseRetryCountAbsentAttribute(t *testing.T) {
	msg := messageWithRetryCount(false, "")
	assert.Equal(t, 0, parseRetryCount(msg, discardLogger()))
}

func TestParseRetryCountMalformedLogsEntry(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{name: "non numeric", raw: "not-a-number"},
		{name: "negative", raw: "-3"},
		{name: "float", raw: "1.5"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

			msg := messageWithRetryCount(true, tt.raw)

			assert.Equal(t, 0, parseRetryCount(msg, log))

			var entry map[string]any
			require.NoError(t, json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &entry))
			assert.Equal(t, slog.LevelError.String(), entry["level"])
			assert.Equal(t, tt.raw, entry["retry_count"])
			assert.Equal(t, "receipt-handle", entry["receipt_handle"])
		})
	}
}
