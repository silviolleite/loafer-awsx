package errors_test

import (
	stderrors "errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	liberrors "github.com/silviolleite/loafer-awsx/errors"
)

func TestSentinels(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"ErrNoRoute", liberrors.ErrNoRoute, "no routes registered"},
		{"ErrNoSQSClient", liberrors.ErrNoSQSClient, "sqs client is nil"},
		{"ErrNoHandler", liberrors.ErrNoHandler, "handler is nil"},
		{"ErrGetMessage", liberrors.ErrGetMessage, "failed to receive messages"},
		{"ErrQueueResolve", liberrors.ErrQueueResolve, "failed to resolve queue url"},
		{"ErrNoSNSClient", liberrors.ErrNoSNSClient, "sns client is nil"},
		{"ErrEmptyInput", liberrors.ErrEmptyInput, "input must be filled"},
		{"ErrMaxBatchSize", liberrors.ErrMaxBatchSize, "batch size exceeds the maximum allowed"},
		{"ErrEmptyRegion", liberrors.ErrEmptyRegion, "region must be filled"},
		{"ErrEmptyQueueName", liberrors.ErrEmptyQueueName, "queue name must be filled"},
		{"ErrInvalidOption", liberrors.ErrInvalidOption, "invalid option"},
		{"ErrEmptyFields", liberrors.ErrEmptyFields, "fields must be filled"},
		{"ErrPanic", liberrors.ErrPanic, "handler panicked"},
		{"ErrRetryScheduleCreate", liberrors.ErrRetryScheduleCreate, "failed to create retry schedule"},
		{"ErrDLQPublish", liberrors.ErrDLQPublish, "failed to publish to dead-letter queue"},
		{"ErrScheduledRetryConfig", liberrors.ErrScheduledRetryConfig, "invalid scheduled retry configuration"},
		{"ErrPingFailed", liberrors.ErrPingFailed, "connectivity ping failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Error(t, tt.err)
			assert.Equal(t, tt.want, tt.err.Error())
			assert.True(t, stderrors.Is(tt.err, tt.err))
		})
	}
}

func TestSentinelsAreDistinct(t *testing.T) {
	all := []error{
		liberrors.ErrNoRoute,
		liberrors.ErrNoSQSClient,
		liberrors.ErrNoHandler,
		liberrors.ErrGetMessage,
		liberrors.ErrQueueResolve,
		liberrors.ErrNoSNSClient,
		liberrors.ErrEmptyInput,
		liberrors.ErrMaxBatchSize,
		liberrors.ErrEmptyRegion,
		liberrors.ErrEmptyQueueName,
		liberrors.ErrInvalidOption,
		liberrors.ErrEmptyFields,
		liberrors.ErrPanic,
		liberrors.ErrRetryScheduleCreate,
		liberrors.ErrDLQPublish,
		liberrors.ErrScheduledRetryConfig,
		liberrors.ErrPingFailed,
	}

	for i := range all {
		for j := range all {
			if i == j {
				continue
			}
			assert.False(t, stderrors.Is(all[i], all[j]))
		}
	}
}

func TestScheduledRetrySentinelsMatchThroughWrap(t *testing.T) {
	cause := stderrors.New("boom")

	sentinels := []error{
		liberrors.ErrRetryScheduleCreate,
		liberrors.ErrDLQPublish,
		liberrors.ErrScheduledRetryConfig,
	}

	for _, sentinel := range sentinels {
		wrapped := liberrors.Wrap(sentinel, cause)
		require.Error(t, wrapped)
		assert.True(t, stderrors.Is(wrapped, sentinel))
		assert.True(t, stderrors.Is(wrapped, cause))
	}
}

func TestNew(t *testing.T) {
	err := liberrors.New("boom")
	require.Error(t, err)
	assert.Equal(t, "boom", err.Error())
	assert.False(t, stderrors.Is(err, liberrors.New("boom")))
}

func TestWrap(t *testing.T) {
	cause := stderrors.New("cause")

	tests := []struct {
		sentinel   error
		err        error
		name       string
		wantMsg    string
		wantNil    bool
		matchSent  bool
		matchErr   bool
		sameAsSent bool
		sameAsErr  bool
	}{
		{
			name:    "both nil",
			wantNil: true,
		},
		{
			name:      "sentinel nil returns err",
			err:       cause,
			wantMsg:   "cause",
			matchErr:  true,
			sameAsErr: true,
		},
		{
			name:       "err nil returns sentinel",
			sentinel:   liberrors.ErrEmptyInput,
			wantMsg:    "input must be filled",
			matchSent:  true,
			sameAsSent: true,
		},
		{
			name:      "both present",
			sentinel:  liberrors.ErrGetMessage,
			err:       cause,
			wantMsg:   "failed to receive messages: cause",
			matchSent: true,
			matchErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := liberrors.Wrap(tt.sentinel, tt.err)

			if tt.wantNil {
				assert.NoError(t, got)
				return
			}

			require.Error(t, got)
			assert.Equal(t, tt.wantMsg, got.Error())

			if tt.matchSent {
				assert.True(t, stderrors.Is(got, tt.sentinel))
			}
			if tt.matchErr {
				assert.True(t, stderrors.Is(got, tt.err))
			}
			if tt.sameAsSent {
				assert.Equal(t, tt.sentinel, got)
			}
			if tt.sameAsErr {
				assert.Equal(t, tt.err, got)
			}
		})
	}
}

func TestWrapMatchesBothCauses(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		sentinelMsg := rapid.String().Draw(t, "sentinelMsg")
		causeMsg := rapid.String().Draw(t, "causeMsg")

		sentinel := liberrors.New(sentinelMsg)
		cause := stderrors.New(causeMsg)

		got := liberrors.Wrap(sentinel, cause)

		require.Error(t, got)
		assert.True(t, stderrors.Is(got, sentinel))
		assert.True(t, stderrors.Is(got, cause))
		assert.Equal(t, fmt.Sprintf("%s: %s", sentinelMsg, causeMsg), got.Error())

		unrelated := stderrors.New("unrelated-" + sentinelMsg + causeMsg)
		assert.False(t, stderrors.Is(got, unrelated))
	})
}

func TestWrapNilBranches(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		msg := rapid.String().Draw(t, "msg")
		err := stderrors.New(msg)

		assert.Equal(t, err, liberrors.Wrap(nil, err))
		assert.Equal(t, err, liberrors.Wrap(err, nil))
		assert.NoError(t, liberrors.Wrap(nil, nil))
	})
}
