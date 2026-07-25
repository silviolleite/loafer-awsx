package fake

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// Compile-time assertion that LogHandler satisfies the slog.Handler interface.
var _ slog.Handler = (*LogHandler)(nil)

// LogRecord is a captured log entry. Attrs holds every attribute associated
// with the record, including handler attributes added through WithAttrs, with
// group names applied as dot-separated key prefixes.
type LogRecord struct {
	Time    time.Time
	Attrs   map[string]any
	Message string
	Level   slog.Level
}

// LogHandler is a capturing slog.Handler for tests. It records every handled
// record so tests can wrap it in a *slog.Logger via slog.New and later assert
// on the emitted level, message, and attributes. No custom logger interface is
// involved: the code under test keeps using the standard library *slog.Logger.
//
// Handlers derived through WithAttrs and WithGroup share the same underlying
// record store as the handler they were created from, so all records are
// visible from the root handler. LogHandler is safe for concurrent use.
type LogHandler struct {
	store        *logStore
	preformatted map[string]any
	groups       []string
	level        slog.Level
}

// logStore is the shared, concurrency-safe sink that accumulates records across
// all handlers derived from a single NewLogHandler call.
type logStore struct {
	records []LogRecord
	mu      sync.Mutex
}

// NewLogHandler returns a LogHandler that captures records at slog.LevelDebug
// and above, capturing every level a default logger emits.
func NewLogHandler() *LogHandler {
	return &LogHandler{
		store:        &logStore{},
		level:        slog.LevelDebug,
		preformatted: make(map[string]any),
	}
}

// SetLevel sets the minimum level the handler reports as enabled. It returns the
// handler to allow chaining during setup.
func (h *LogHandler) SetLevel(level slog.Level) *LogHandler {
	h.level = level
	return h
}

// Enabled reports whether records at the given level are captured.
func (h *LogHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

// Handle records the log entry, merging the handler's preformatted attributes
// with the record attributes and applying the active group prefix.
func (h *LogHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := make(map[string]any, len(h.preformatted)+r.NumAttrs())
	for k, v := range h.preformatted {
		attrs[k] = v
	}

	r.Attrs(func(a slog.Attr) bool {
		flattenAttr(attrs, h.groups, a)
		return true
	})

	rec := LogRecord{
		Time:    r.Time,
		Level:   r.Level,
		Message: r.Message,
		Attrs:   attrs,
	}

	h.store.mu.Lock()
	h.store.records = append(h.store.records, rec)
	h.store.mu.Unlock()

	return nil
}

// WithAttrs returns a new handler that shares this handler's record store and
// includes attrs, resolved under the currently active groups, on every future
// record.
func (h *LogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}

	preformatted := make(map[string]any, len(h.preformatted)+len(attrs))
	for k, v := range h.preformatted {
		preformatted[k] = v
	}
	for _, a := range attrs {
		flattenAttr(preformatted, h.groups, a)
	}

	return &LogHandler{
		store:        h.store,
		level:        h.level,
		groups:       h.groups,
		preformatted: preformatted,
	}
}

// WithGroup returns a new handler that shares this handler's record store and
// nests subsequent attributes under name. An empty name returns the handler
// unchanged, per the slog.Handler contract.
func (h *LogHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}

	groups := make([]string, len(h.groups), len(h.groups)+1)
	copy(groups, h.groups)
	groups = append(groups, name)

	return &LogHandler{
		store:        h.store,
		level:        h.level,
		groups:       groups,
		preformatted: h.preformatted,
	}
}

// Records returns a copy of every captured record, in the order they were
// handled.
func (h *LogHandler) Records() []LogRecord {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	out := make([]LogRecord, len(h.store.records))
	copy(out, h.store.records)
	return out
}

// Reset discards all captured records.
func (h *LogHandler) Reset() {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	h.store.records = nil
}

// flattenAttr resolves a and writes it into dst, joining the active group path
// with the attribute key using dots. Group-valued attributes are recursed into,
// extending the group path with the group key when it is non-empty. Empty
// attributes are ignored, matching the slog.Handler contract.
func flattenAttr(dst map[string]any, groups []string, a slog.Attr) {
	a.Value = a.Value.Resolve()
	if a.Equal(slog.Attr{}) {
		return
	}

	if a.Value.Kind() == slog.KindGroup {
		group := a.Value.Group()
		if len(group) == 0 {
			return
		}

		nested := groups
		if a.Key != "" {
			nested = append(append([]string{}, groups...), a.Key)
		}
		for _, ga := range group {
			flattenAttr(dst, nested, ga)
		}
		return
	}

	dst[qualifyKey(groups, a.Key)] = a.Value.Any()
}

// qualifyKey joins the group path and key with dots.
func qualifyKey(groups []string, key string) string {
	if len(groups) == 0 {
		return key
	}
	return strings.Join(groups, ".") + "." + key
}
