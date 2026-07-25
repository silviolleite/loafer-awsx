package idgen

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"

	"github.com/silviolleite/loafer-awsx/errors"
)

// keyBased is a deterministic generator. It builds a canonical string from the
// selected fields, sorted by key, and hashes it with the configured algorithm.
// Equal field maps always yield the same identifier regardless of map iteration
// order, and the type is safe for concurrent use because it never mutates its
// configuration after construction.
type keyBased struct {
	config generatorConfig
}

// NewKeyBased builds a deterministic generator that derives an identifier from
// message fields. It sorts the selected field keys, concatenates each key and
// value with the configured separator, and hashes the result with the
// configured algorithm (SHA256 by default). The returned generator satisfies
// both GroupIDGenerator and DeduplicationIDGenerator. Nil options are ignored.
func NewKeyBased(opts ...Option) GroupIDGenerator {
	c := newConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&c)
		}
	}
	return &keyBased{config: c}
}

// Generate returns the identifier derived from fields. It returns
// errors.ErrEmptyFields when the fields map is empty or when a configured
// whitelist selects none of the present fields, since neither case yields any
// data to hash. The output is deterministic: the same effective fields always
// produce the same identifier.
func (g *keyBased) Generate(_ context.Context, fields map[string]string) (string, error) {
	if len(fields) == 0 {
		return "", errors.ErrEmptyFields
	}

	keys := g.selectKeys(fields)
	if len(keys) == 0 {
		return "", errors.ErrEmptyFields
	}

	sort.Strings(keys)

	pairs := make([]string, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, key+g.config.separator+fields[key])
	}

	return g.hash(strings.Join(pairs, g.config.separator)), nil
}

// selectKeys returns the field keys to include in the canonical string. With no
// whitelist every field key is returned; with a whitelist only the listed keys
// that are present in fields are returned. The result is unsorted; Generate
// sorts it before use.
func (g *keyBased) selectKeys(fields map[string]string) []string {
	if len(g.config.fields) == 0 {
		keys := make([]string, 0, len(fields))
		for key := range fields {
			keys = append(keys, key)
		}
		return keys
	}

	keys := make([]string, 0, len(g.config.fields))
	for _, key := range g.config.fields {
		if _, ok := fields[key]; ok {
			keys = append(keys, key)
		}
	}
	return keys
}

// hash reduces the canonical field string to a lowercase hexadecimal digest
// using the configured algorithm.
func (g *keyBased) hash(canonical string) string {
	if g.config.algorithm == FNV64 {
		h := fnv.New64a()
		_, _ = h.Write([]byte(canonical))
		return strconv.FormatUint(h.Sum64(), 16)
	}

	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}
