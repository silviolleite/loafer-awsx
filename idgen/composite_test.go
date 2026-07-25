package idgen_test

import (
	stderrors "errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	verrors "github.com/silviolleite/loafer-awsx/errors"
	"github.com/silviolleite/loafer-awsx/idgen"
)

func TestCompositeGenerateEmptyFields(t *testing.T) {
	tests := []struct {
		fields map[string]string
		name   string
		opts   []idgen.Option
	}{
		{name: "nil map", fields: nil},
		{name: "empty map", fields: map[string]string{}},
		{
			name:   "whitelist selects nothing",
			opts:   []idgen.Option{idgen.WithFields("missing")},
			fields: map[string]string{"present": "value"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := idgen.NewComposite(tc.opts...)

			id, err := g.Generate(t.Context(), tc.fields)

			require.Error(t, err)
			assert.True(t, stderrors.Is(err, verrors.ErrEmptyFields))
			assert.Empty(t, id)
		})
	}
}

func TestCompositeGenerateJoinsInWhitelistOrder(t *testing.T) {
	tests := []struct {
		fields map[string]string
		name   string
		want   string
		opts   []idgen.Option
	}{
		{
			name:   "whitelist order preserved",
			opts:   []idgen.Option{idgen.WithFields("b", "a", "c")},
			fields: map[string]string{"a": "1", "b": "2", "c": "3"},
			want:   "2:1:3",
		},
		{
			name:   "custom separator",
			opts:   []idgen.Option{idgen.WithFields("a", "b"), idgen.WithSeparator("-")},
			fields: map[string]string{"a": "1", "b": "2"},
			want:   "1-2",
		},
		{
			name:   "whitelist skips absent keys",
			opts:   []idgen.Option{idgen.WithFields("a", "missing", "b")},
			fields: map[string]string{"a": "1", "b": "2"},
			want:   "1:2",
		},
		{
			name:   "no whitelist sorts keys",
			fields: map[string]string{"b": "2", "a": "1", "c": "3"},
			want:   "1:2:3",
		},
		{
			name:   "single field",
			opts:   []idgen.Option{idgen.WithFields("a")},
			fields: map[string]string{"a": "only"},
			want:   "only",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := idgen.NewComposite(tc.opts...)

			id, err := g.Generate(t.Context(), tc.fields)

			require.NoError(t, err)
			assert.Equal(t, tc.want, id)
		})
	}
}

func TestCompositeGenerateNilOptionIgnored(t *testing.T) {
	g := idgen.NewComposite(nil)

	id, err := g.Generate(t.Context(), map[string]string{"a": "1"})

	require.NoError(t, err)
	assert.Equal(t, "1", id)
}

func TestCompositeSatisfiesDeduplicationIDGenerator(t *testing.T) {
	var dedup idgen.DeduplicationIDGenerator = idgen.NewComposite(idgen.WithFields("a"))

	id, err := dedup.Generate(t.Context(), map[string]string{"a": "1"})

	require.NoError(t, err)
	assert.Equal(t, "1", id)
}

func TestCompositeGenerateOrderIndependentWithoutWhitelistProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		fields := fieldsGenerator().Draw(rt, "fields")
		g := idgen.NewComposite()

		first, err := g.Generate(rt.Context(), fields)
		require.NoError(rt, err)

		shuffled := make(map[string]string, len(fields))
		for k, v := range fields {
			shuffled[k] = v
		}
		second, err := g.Generate(rt.Context(), shuffled)
		require.NoError(rt, err)

		assert.Equal(rt, first, second)
	})
}

func TestCompositeGenerateIdempotentProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		fields := fieldsGenerator().Draw(rt, "fields")
		g := idgen.NewComposite()

		first, err := g.Generate(rt.Context(), fields)
		require.NoError(rt, err)
		second, err := g.Generate(rt.Context(), fields)
		require.NoError(rt, err)

		assert.Equal(rt, first, second)
	})
}
