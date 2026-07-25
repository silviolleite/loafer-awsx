package idgen_test

import (
	stderrors "errors"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	verrors "github.com/silviolleite/loafer-awsx/errors"
	"github.com/silviolleite/loafer-awsx/idgen"
)

func TestCompositeWithSuffixGenerateInvalidRange(t *testing.T) {
	tests := []struct {
		name string
		min  int
		max  int
	}{
		{name: "min greater than max", min: 5, max: 1},
		{name: "min greater than max by one", min: 2, max: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := idgen.NewCompositeWithSuffix(idgen.WithSuffixRange(tc.min, tc.max))

			id, err := g.Generate(t.Context(), map[string]string{"a": "1"})

			require.Error(t, err)
			assert.True(t, stderrors.Is(err, verrors.ErrInvalidOption))
			assert.Empty(t, id)
		})
	}
}

func TestCompositeWithSuffixGenerateEmptyFields(t *testing.T) {
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
			g := idgen.NewCompositeWithSuffix(tc.opts...)

			id, err := g.Generate(t.Context(), tc.fields)

			require.Error(t, err)
			assert.True(t, stderrors.Is(err, verrors.ErrEmptyFields))
			assert.Empty(t, id)
		})
	}
}

func TestCompositeWithSuffixGenerateFormat(t *testing.T) {
	g := idgen.NewCompositeWithSuffix(idgen.WithFields("a", "b"), idgen.WithSuffixRange(7, 7))

	id, err := g.Generate(t.Context(), map[string]string{"a": "1", "b": "2"})

	require.NoError(t, err)
	assert.Equal(t, "1:2:7", id)
}

func TestCompositeWithSuffixGenerateCustomSeparator(t *testing.T) {
	g := idgen.NewCompositeWithSuffix(
		idgen.WithFields("a", "b"),
		idgen.WithSeparator("-"),
		idgen.WithSuffixRange(3, 3),
	)

	id, err := g.Generate(t.Context(), map[string]string{"a": "1", "b": "2"})

	require.NoError(t, err)
	assert.Equal(t, "1-2-3", id)
}

func TestCompositeWithSuffixGenerateDefaultRange(t *testing.T) {
	g := idgen.NewCompositeWithSuffix(idgen.WithFields("a"))

	for range 200 {
		id, err := g.Generate(t.Context(), map[string]string{"a": "x"})
		require.NoError(t, err)

		require.True(t, strings.HasPrefix(id, "x:"))
		suffix, convErr := strconv.Atoi(strings.TrimPrefix(id, "x:"))
		require.NoError(t, convErr)
		assert.GreaterOrEqual(t, suffix, 1)
		assert.LessOrEqual(t, suffix, 20)
	}
}

func TestCompositeWithSuffixNilOptionIgnored(t *testing.T) {
	g := idgen.NewCompositeWithSuffix(nil)

	id, err := g.Generate(t.Context(), map[string]string{"a": "1"})

	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(id, "1:"))
}

func TestCompositeWithSuffixSatisfiesDeduplicationIDGenerator(t *testing.T) {
	var dedup idgen.DeduplicationIDGenerator = idgen.NewCompositeWithSuffix(
		idgen.WithFields("a"),
		idgen.WithSuffixRange(4, 4),
	)

	id, err := dedup.Generate(t.Context(), map[string]string{"a": "1"})

	require.NoError(t, err)
	assert.Equal(t, "1:4", id)
}

func TestCompositeWithSuffixGenerateSuffixWithinRangeProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		fields := fieldsGenerator().Draw(rt, "fields")
		separator := rapid.SampledFrom([]string{":", "#", "|", "::"}).Draw(rt, "separator")
		min := rapid.IntRange(-50, 50).Draw(rt, "min")
		span := rapid.IntRange(0, 100).Draw(rt, "span")
		max := min + span

		g := idgen.NewCompositeWithSuffix(
			idgen.WithSeparator(separator),
			idgen.WithSuffixRange(min, max),
		)

		id, err := g.Generate(rt.Context(), fields)
		require.NoError(rt, err)

		idx := strings.LastIndex(id, separator)
		require.NotEqual(rt, -1, idx)

		suffix, convErr := strconv.Atoi(id[idx+len(separator):])
		require.NoError(rt, convErr)
		assert.GreaterOrEqual(rt, suffix, min)
		assert.LessOrEqual(rt, suffix, max)
	})
}

func TestCompositeWithSuffixGeneratePrefixMatchesCompositeProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		fields := fieldsGenerator().Draw(rt, "fields")
		min := rapid.IntRange(1, 20).Draw(rt, "min")
		span := rapid.IntRange(0, 20).Draw(rt, "span")
		max := min + span

		prefixGen := idgen.NewComposite()
		prefix, err := prefixGen.Generate(rt.Context(), fields)
		require.NoError(rt, err)

		g := idgen.NewCompositeWithSuffix(idgen.WithSuffixRange(min, max))
		id, err := g.Generate(rt.Context(), fields)
		require.NoError(rt, err)

		idx := strings.LastIndex(id, ":")
		require.NotEqual(rt, -1, idx)
		assert.Equal(rt, prefix, id[:idx])
	})
}
