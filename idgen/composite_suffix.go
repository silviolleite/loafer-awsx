package idgen

import (
	"context"
	"math/rand/v2"
	"strconv"

	"github.com/silviolleite/loafer-awsx/errors"
)

// compositeWithSuffix builds on the composite strategy by appending a random
// numeric suffix drawn from the configured inclusive range. It distributes
// otherwise identical identifiers across group partitions while keeping the
// leading field values readable. The type is safe for concurrent use because it
// never mutates its configuration and math/rand/v2 top-level functions are safe
// for concurrent use.
type compositeWithSuffix struct {
	base composite
}

// NewCompositeWithSuffix builds a generator that joins the selected field
// values like NewComposite and then appends the separator and a random integer
// drawn uniformly from the configured inclusive range (WithSuffixRange, [1, 20]
// by default). The suffix spreads identical field combinations across group
// partitions. The returned generator satisfies both GroupIDGenerator and
// DeduplicationIDGenerator. Nil options are ignored.
func NewCompositeWithSuffix(opts ...Option) GroupIDGenerator {
	c := newConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&c)
		}
	}
	return &compositeWithSuffix{base: composite{config: c}}
}

// Generate returns the joined identifier followed by the separator and a random
// suffix in the format field1<sep>field2<sep>...<sep>N. It returns
// errors.ErrInvalidOption when the configured range is invalid (min greater than
// max) and errors.ErrEmptyFields when the fields map yields no value to join.
func (g *compositeWithSuffix) Generate(ctx context.Context, fields map[string]string) (string, error) {
	if g.base.config.suffixMin > g.base.config.suffixMax {
		return "", errors.ErrInvalidOption
	}

	prefix, err := g.base.join(fields)
	if err != nil {
		return "", err
	}

	suffix := g.randomSuffix()
	return prefix + g.base.config.separator + strconv.Itoa(suffix), nil
}

// randomSuffix returns a random integer uniformly distributed within the
// configured inclusive [min, max] range. The caller guarantees min is not
// greater than max.
func (g *compositeWithSuffix) randomSuffix() int {
	span := g.base.config.suffixMax - g.base.config.suffixMin + 1
	return g.base.config.suffixMin + rand.IntN(span)
}
