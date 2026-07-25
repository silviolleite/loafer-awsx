package logger_test

import (
	"bytes"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/logger"
)

func TestNewReturnsNonNilLogger(t *testing.T) {
	l := logger.New()

	require.NotNil(t, l)
	assert.IsType(t, &slog.Logger{}, l)
}

func TestNewEnabledAtAllLevels(t *testing.T) {
	l := logger.New()

	levels := []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError}
	for _, lvl := range levels {
		if lvl == slog.LevelDebug {
			assert.False(t, l.Enabled(t.Context(), lvl))
			continue
		}
		assert.True(t, l.Enabled(t.Context(), lvl))
	}
}

func TestNewWritesToStdout(t *testing.T) {
	original := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	t.Cleanup(func() { os.Stdout = original })

	logger.New().Info("hello", "k", "v")

	require.NoError(t, w.Close())
	out, err := io.ReadAll(r)
	require.NoError(t, err)

	got := string(out)
	assert.Contains(t, got, "level=INFO")
	assert.Contains(t, got, "msg=hello")
	assert.Contains(t, got, "k=v")
}

func TestNoOpReturnsNonNilLogger(t *testing.T) {
	l := logger.NewNoOp()

	require.NotNil(t, l)
	assert.IsType(t, &slog.Logger{}, l)
}

func TestNoOpDisabledAtAllLevels(t *testing.T) {
	l := logger.NewNoOp()

	levels := []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError}
	for _, lvl := range levels {
		assert.False(t, l.Enabled(t.Context(), lvl))
	}
}

func TestNoOpDiscardsAllOutput(t *testing.T) {
	original := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	t.Cleanup(func() { os.Stdout = original })

	l := logger.NewNoOp()
	assert.NotPanics(t, func() {
		l.Debug("d", "k", "v")
		l.Info("i", "k", "v")
		l.Warn("w", "k", "v")
		l.Error("e", "k", "v")
	})

	require.NoError(t, w.Close())
	out, err := io.ReadAll(r)
	require.NoError(t, err)

	assert.Empty(t, out)
}

func TestTextHandlerEmitsArbitraryPairs(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		msg := rapid.StringMatching(`[a-zA-Z0-9 ]{0,20}`).Draw(rt, "msg")
		n := rapid.IntRange(0, 8).Draw(rt, "n")

		keys := make([]string, n)
		values := make([]int, n)
		pairs := make([]any, 0, n*2)
		for i := 0; i < n; i++ {
			keys[i] = rapid.StringMatching(`[a-z]{1,5}`).Draw(rt, "key")
			values[i] = rapid.Int().Draw(rt, "value")
			pairs = append(pairs, keys[i], values[i])
		}

		var buf bytes.Buffer
		l := slog.New(slog.NewTextHandler(&buf, nil))
		l.Info(msg, pairs...)

		got := buf.String()
		assert.True(rt, strings.HasSuffix(got, "\n"))
		assert.Contains(rt, got, "level=INFO")
	})
}
