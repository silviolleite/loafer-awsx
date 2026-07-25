package idgen_test

import (
	stderrors "errors"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	verrors "github.com/silviolleite/loafer-awsx/errors"
	"github.com/silviolleite/loafer-awsx/idgen"
)

var (
	sha256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
	fnv64Pattern  = regexp.MustCompile(`^[0-9a-f]{1,16}$`)
)

func TestKeyBasedGenerateEmptyFields(t *testing.T) {
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
			g := idgen.NewKeyBased(tc.opts...)

			id, err := g.Generate(t.Context(), tc.fields)

			require.Error(t, err)
			assert.True(t, stderrors.Is(err, verrors.ErrEmptyFields))
			assert.Empty(t, id)
		})
	}
}

func TestKeyBasedGenerateProducesExpectedFormat(t *testing.T) {
	tests := []struct {
		pattern *regexp.Regexp
		name    string
		opts    []idgen.Option
	}{
		{name: "default is sha256", pattern: sha256Pattern},
		{name: "explicit sha256", opts: []idgen.Option{idgen.WithHashAlgorithm(idgen.SHA256)}, pattern: sha256Pattern},
		{name: "fnv64", opts: []idgen.Option{idgen.WithHashAlgorithm(idgen.FNV64)}, pattern: fnv64Pattern},
		{name: "unknown algorithm falls back to sha256", opts: []idgen.Option{idgen.WithHashAlgorithm(idgen.HashAlgorithm(42))}, pattern: sha256Pattern},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := idgen.NewKeyBased(tc.opts...)

			id, err := g.Generate(t.Context(), map[string]string{"a": "1", "b": "2"})

			require.NoError(t, err)
			assert.Regexp(t, tc.pattern, id)
		})
	}
}

func TestKeyBasedGenerateDifferentFieldsDifferentHash(t *testing.T) {
	g := idgen.NewKeyBased()

	first, err := g.Generate(t.Context(), map[string]string{"a": "1"})
	require.NoError(t, err)
	second, err := g.Generate(t.Context(), map[string]string{"a": "2"})
	require.NoError(t, err)

	assert.NotEqual(t, first, second)
}

func TestKeyBasedGenerateAlgorithmsDiffer(t *testing.T) {
	fields := map[string]string{"a": "1", "b": "2"}

	sha, err := idgen.NewKeyBased(idgen.WithHashAlgorithm(idgen.SHA256)).Generate(t.Context(), fields)
	require.NoError(t, err)
	fnv, err := idgen.NewKeyBased(idgen.WithHashAlgorithm(idgen.FNV64)).Generate(t.Context(), fields)
	require.NoError(t, err)

	assert.NotEqual(t, sha, fnv)
}

func TestKeyBasedGenerateSeparatorChangesOutput(t *testing.T) {
	fields := map[string]string{"a": "1", "b": "2"}

	def, err := idgen.NewKeyBased().Generate(t.Context(), fields)
	require.NoError(t, err)
	custom, err := idgen.NewKeyBased(idgen.WithSeparator("#")).Generate(t.Context(), fields)
	require.NoError(t, err)
	ignored, err := idgen.NewKeyBased(idgen.WithSeparator("")).Generate(t.Context(), fields)
	require.NoError(t, err)

	assert.NotEqual(t, def, custom)
	assert.Equal(t, def, ignored)
}

func TestKeyBasedGenerateWhitelistIgnoresOtherFields(t *testing.T) {
	g := idgen.NewKeyBased(idgen.WithFields("a"))

	withExtra, err := g.Generate(t.Context(), map[string]string{"a": "1", "b": "2"})
	require.NoError(t, err)
	onlyWhitelisted, err := g.Generate(t.Context(), map[string]string{"a": "1"})
	require.NoError(t, err)

	assert.Equal(t, withExtra, onlyWhitelisted)
}

func TestKeyBasedGenerateWhitelistOrderIndependent(t *testing.T) {
	fields := map[string]string{"a": "1", "b": "2"}

	forward, err := idgen.NewKeyBased(idgen.WithFields("a", "b")).Generate(t.Context(), fields)
	require.NoError(t, err)
	reverse, err := idgen.NewKeyBased(idgen.WithFields("b", "a")).Generate(t.Context(), fields)
	require.NoError(t, err)

	assert.Equal(t, forward, reverse)
}

func TestKeyBasedNilOptionIgnored(t *testing.T) {
	g := idgen.NewKeyBased(nil)

	id, err := g.Generate(t.Context(), map[string]string{"a": "1"})

	require.NoError(t, err)
	assert.Regexp(t, sha256Pattern, id)
}

func TestKeyBasedSatisfiesDeduplicationIDGenerator(t *testing.T) {
	var dedup idgen.DeduplicationIDGenerator = idgen.NewKeyBased()

	id, err := dedup.Generate(t.Context(), map[string]string{"a": "1"})

	require.NoError(t, err)
	assert.Regexp(t, sha256Pattern, id)
}

func fieldsGenerator() *rapid.Generator[map[string]string] {
	return rapid.MapOfN(
		rapid.StringMatching(`[a-z]{1,8}`),
		rapid.StringMatching(`[a-zA-Z0-9]{0,8}`),
		1, 8,
	)
}

func TestKeyBasedGenerateOrderIndependentProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		fields := fieldsGenerator().Draw(rt, "fields")
		algorithm := rapid.SampledFrom([]idgen.HashAlgorithm{idgen.SHA256, idgen.FNV64}).Draw(rt, "algorithm")

		g := idgen.NewKeyBased(idgen.WithHashAlgorithm(algorithm))

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

func TestKeyBasedGenerateIdempotentProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		fields := fieldsGenerator().Draw(rt, "fields")
		algorithm := rapid.SampledFrom([]idgen.HashAlgorithm{idgen.SHA256, idgen.FNV64}).Draw(rt, "algorithm")

		g := idgen.NewKeyBased(idgen.WithHashAlgorithm(algorithm))

		first, err := g.Generate(rt.Context(), fields)
		require.NoError(rt, err)
		second, err := g.Generate(rt.Context(), fields)
		require.NoError(rt, err)

		assert.Equal(rt, first, second)
	})
}

func TestKeyBasedGenerateDistinctFieldsDistinctHashProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		fields := fieldsGenerator().Draw(rt, "fields")
		g := idgen.NewKeyBased()

		base, err := g.Generate(rt.Context(), fields)
		require.NoError(rt, err)

		mutated := make(map[string]string, len(fields))
		for k, v := range fields {
			mutated[k] = v
		}
		for k, v := range mutated {
			mutated[k] = v + "x"
			break
		}

		changed, err := g.Generate(rt.Context(), mutated)
		require.NoError(rt, err)

		assert.NotEqual(rt, base, changed)
	})
}
