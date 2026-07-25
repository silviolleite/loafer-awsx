package idgen

import "context"

// GroupIDGenerator produces a MessageGroupId for a message from its fields. A
// generator must be safe for concurrent use by multiple goroutines.
type GroupIDGenerator interface {
	// Generate returns the identifier derived from fields. Implementations
	// return an error when they cannot produce a value, for example when the
	// fields map is empty and the strategy requires field data.
	Generate(ctx context.Context, fields map[string]string) (string, error)
}

// DeduplicationIDGenerator produces a MessageDeduplicationId for a message from
// its fields. Its method set is identical to GroupIDGenerator, so a single
// concrete generator can satisfy both interfaces. A generator must be safe for
// concurrent use by multiple goroutines.
type DeduplicationIDGenerator interface {
	// Generate returns the identifier derived from fields. Implementations
	// return an error when they cannot produce a value, for example when the
	// fields map is empty and the strategy requires field data.
	Generate(ctx context.Context, fields map[string]string) (string, error)
}

// HashAlgorithm selects the digest used by the key-based generator to turn the
// canonical field string into a stable identifier.
type HashAlgorithm int

const (
	// SHA256 hashes the canonical field string with SHA-256 and encodes the
	// digest as lowercase hexadecimal. It is the default algorithm.
	SHA256 HashAlgorithm = iota
	// FNV64 hashes the canonical field string with the 64-bit FNV-1a function
	// and encodes the result as lowercase hexadecimal. It is faster and more
	// compact than SHA256 but not cryptographically strong.
	FNV64
)

// defaultSeparator joins each key/value pair of the canonical field string. It
// is used when WithSeparator is not provided.
const defaultSeparator = ":"

const (
	// defaultSuffixMin is the inclusive lower bound of the random suffix range
	// used by CompositeWithSuffix when WithSuffixRange is not provided.
	defaultSuffixMin = 1
	// defaultSuffixMax is the inclusive upper bound of the random suffix range
	// used by CompositeWithSuffix when WithSuffixRange is not provided.
	defaultSuffixMax = 20
)

// generatorConfig accumulates the configuration produced by the functional
// options before it is used to build a generator.
type generatorConfig struct {
	separator string
	fields    []string
	algorithm HashAlgorithm
	suffixMin int
	suffixMax int
}

// newConfig returns a generatorConfig seeded with library defaults: SHA256
// hashing, a colon separator, no field whitelist (all fields are used), and the
// [1, 20] inclusive suffix range used by CompositeWithSuffix.
func newConfig() generatorConfig {
	return generatorConfig{
		algorithm: SHA256,
		separator: defaultSeparator,
		suffixMin: defaultSuffixMin,
		suffixMax: defaultSuffixMax,
	}
}

// Option configures a key-based generator during construction. Options are
// applied in order by NewKeyBased and nil options are ignored.
type Option func(*generatorConfig)
