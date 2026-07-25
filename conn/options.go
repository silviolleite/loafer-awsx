package conn

// options accumulates the configuration produced by the functional options
// before it is translated into AWS SDK config load overrides.
type options struct {
	region       string
	accessKey    string
	secretKey    string
	sessionToken string
	profile      string
	endpoint     string
	retryCount   int
}

// defaultRetryCount is the number of retry attempts used when the caller does
// not provide an explicit retry count.
const defaultRetryCount = 10

// newOptions returns an options value seeded with library defaults.
func newOptions() *options {
	return &options{retryCount: defaultRetryCount}
}

// hasStaticCredentials reports whether static credentials were supplied. Static
// credentials take precedence over a configured profile.
func (o *options) hasStaticCredentials() bool {
	return o.accessKey != "" && o.secretKey != ""
}

// WithRegion sets the AWS region. The region is required; New returns
// ErrEmptyRegion when it is left empty.
func WithRegion(region string) Option {
	return func(o *options) error {
		o.region = region
		return nil
	}
}

// WithAccessKey sets static credentials using the given access key and secret.
// When provided, static credentials take precedence over a configured profile.
func WithAccessKey(key, secret string) Option {
	return func(o *options) error {
		o.accessKey = key
		o.secretKey = secret
		return nil
	}
}

// WithSessionToken adds a session token to the static credentials. It is only
// applied when static credentials are also provided via WithAccessKey.
func WithSessionToken(token string) Option {
	return func(o *options) error {
		o.sessionToken = token
		return nil
	}
}

// WithProfile sets the shared config profile name used to resolve credentials
// when static credentials are not provided.
func WithProfile(profile string) Option {
	return func(o *options) error {
		o.profile = profile
		return nil
	}
}

// WithEndpoint sets a custom endpoint URL for all AWS requests. It is intended
// for local development against LocalStack or other AWS-compatible endpoints.
func WithEndpoint(url string) Option {
	return func(o *options) error {
		o.endpoint = url
		return nil
	}
}

// WithRetryCount sets the maximum number of retry attempts. When not provided
// the retry count defaults to 10.
func WithRetryCount(n uint) Option {
	return func(o *options) error {
		o.retryCount = int(n)
		return nil
	}
}
