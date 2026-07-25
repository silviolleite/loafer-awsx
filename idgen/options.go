package idgen

// WithHashAlgorithm sets the digest used to hash the canonical field string.
// Unknown algorithms are ignored so the generator keeps its default of SHA256.
func WithHashAlgorithm(algorithm HashAlgorithm) Option {
	return func(c *generatorConfig) {
		switch algorithm {
		case SHA256, FNV64:
			c.algorithm = algorithm
		}
	}
}

// WithSeparator sets the string used to join key/value pairs when building the
// canonical field string. An empty separator is ignored so the generator keeps
// its default separator.
func WithSeparator(separator string) Option {
	return func(c *generatorConfig) {
		if separator != "" {
			c.separator = separator
		}
	}
}

// WithFields sets a field whitelist. When provided, only the listed fields that
// are present in the fields map are used to build the identifier; every other
// field is ignored. The selected fields are always applied in stable sorted
// order so the result is independent of the whitelist order and of map
// iteration order. When not provided, all fields are used. A nil or empty
// whitelist is ignored so the generator keeps using all fields.
func WithFields(fields ...string) Option {
	return func(c *generatorConfig) {
		if len(fields) > 0 {
			c.fields = append([]string(nil), fields...)
		}
	}
}

// WithSuffixRange sets the inclusive [minSuffix, maxSuffix] range from which
// NewCompositeWithSuffix draws its random numeric suffix. It defaults to
// [1, 20]. When minSuffix is greater than maxSuffix the range is invalid and
// the resulting generator returns errors.ErrInvalidOption on every Generate
// call. Other generators ignore this option.
func WithSuffixRange(minSuffix, maxSuffix int) Option {
	return func(c *generatorConfig) {
		c.suffixMin = minSuffix
		c.suffixMax = maxSuffix
	}
}
