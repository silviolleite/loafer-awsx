package idgen_test

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/idgen"
)

var uuidV4Pattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestRandomGenerateValidUUIDV4(t *testing.T) {
	g := idgen.NewRandom()

	id, err := g.Generate(t.Context(), nil)

	require.NoError(t, err)
	assert.Regexp(t, uuidV4Pattern, id)
}

func TestRandomGenerateIgnoresFields(t *testing.T) {
	g := idgen.NewRandom()

	id, err := g.Generate(t.Context(), map[string]string{"a": "1", "b": "2"})

	require.NoError(t, err)
	assert.Regexp(t, uuidV4Pattern, id)
}

func TestRandomSatisfiesDeduplicationIDGenerator(t *testing.T) {
	var dedup idgen.DeduplicationIDGenerator = idgen.NewRandom()

	id, err := dedup.Generate(t.Context(), nil)

	require.NoError(t, err)
	assert.Regexp(t, uuidV4Pattern, id)
}

func TestRandomGenerateVersionAndVariantProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		fields := rapid.MapOfN(
			rapid.StringMatching(`[a-z]{1,8}`),
			rapid.StringMatching(`[a-z0-9]{0,8}`),
			0, 5,
		).Draw(rt, "fields")

		id, err := idgen.NewRandom().Generate(rt.Context(), fields)

		require.NoError(rt, err)
		assert.Regexp(rt, uuidV4Pattern, id)
	})
}

func TestRandomGenerateUniqueProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(2, 50).Draw(rt, "n")
		g := idgen.NewRandom()

		seen := make(map[string]struct{}, n)
		for range n {
			id, err := g.Generate(rt.Context(), nil)
			require.NoError(rt, err)
			_, dup := seen[id]
			assert.False(rt, dup)
			seen[id] = struct{}{}
		}
	})
}
