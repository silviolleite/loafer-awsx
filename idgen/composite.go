package idgen

import (
	"context"
	"sort"
	"strings"

	"github.com/silviolleite/loafer-awsx/errors"
)

// composite is a deterministic generator that joins the selected field values
// with the configured separator without hashing them. With a whitelist the
// values are joined in whitelist order; without one they are joined in stable
// sorted key order so the result never depends on map iteration order. The type
// is safe for concurrent use because it never mutates its configuration after
// construction.
type composite struct {
	config generatorConfig
}

// NewComposite builds a deterministic generator that joins the selected field
// values with the configured separator (a colon by default) to form a
// human-readable identifier without hashing. When WithFields is provided the
// values are joined in the given whitelist order; otherwise the field keys are
// sorted so equal field maps always yield the same identifier. The returned
// generator satisfies both GroupIDGenerator and DeduplicationIDGenerator. Nil
// options are ignored.
func NewComposite(opts ...Option) GroupIDGenerator {
	c := newConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&c)
		}
	}
	return &composite{config: c}
}

// Generate returns the joined identifier derived from fields. It returns
// errors.ErrEmptyFields when the fields map is empty or when a configured
// whitelist selects none of the present fields, since neither case yields any
// value to join.
func (g *composite) Generate(_ context.Context, fields map[string]string) (string, error) {
	return g.join(fields)
}

// join concatenates the selected field values with the configured separator. It
// returns errors.ErrEmptyFields when no value is selected so callers never
// produce an identifier from an empty input.
func (g *composite) join(fields map[string]string) (string, error) {
	if len(fields) == 0 {
		return "", errors.ErrEmptyFields
	}

	values := g.selectValues(fields)
	if len(values) == 0 {
		return "", errors.ErrEmptyFields
	}

	return strings.Join(values, g.config.separator), nil
}

// selectValues returns the field values to join. With no whitelist every value
// is returned in stable sorted key order; with a whitelist only the values for
// the listed keys that are present in fields are returned, preserving the
// whitelist order.
func (g *composite) selectValues(fields map[string]string) []string {
	if len(g.config.fields) == 0 {
		keys := make([]string, 0, len(fields))
		for key := range fields {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		values := make([]string, 0, len(keys))
		for _, key := range keys {
			values = append(values, fields[key])
		}
		return values
	}

	values := make([]string, 0, len(g.config.fields))
	for _, key := range g.config.fields {
		if value, ok := fields[key]; ok {
			values = append(values, value)
		}
	}
	return values
}
