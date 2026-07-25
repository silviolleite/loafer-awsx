package conn

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/silviolleite/loafer-awsx/errors"
)

// Option configures the AWS connection. Options are applied in order and may
// return an error to abort configuration.
type Option func(*options) error

// New creates an aws.Config from the provided options. It validates that a
// region was supplied, applies retry, credential, profile, and endpoint
// overrides, and delegates to the AWS SDK default configuration loader.
//
// New returns errors.ErrEmptyRegion when no region is provided. Static
// credentials supplied via WithAccessKey take precedence over a profile
// configured via WithProfile.
func New(ctx context.Context, opts ...Option) (aws.Config, error) {
	o := newOptions()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(o); err != nil {
			return aws.Config{}, errors.Wrap(errors.ErrInvalidOption, err)
		}
	}

	if o.region == "" {
		return aws.Config{}, errors.ErrEmptyRegion
	}

	loadOpts := buildLoadOptions(o)

	cfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return aws.Config{}, err
	}

	return cfg, nil
}

// buildLoadOptions translates the accumulated options into the slice of AWS SDK
// config load options that express the requested overrides.
func buildLoadOptions(o *options) []func(*awsconfig.LoadOptions) error {
	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(o.region),
		awsconfig.WithRetryer(func() aws.Retryer {
			return retry.NewStandard(func(so *retry.StandardOptions) {
				so.MaxAttempts = o.retryCount
			})
		}),
	}

	switch {
	case o.hasStaticCredentials():
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(o.accessKey, o.secretKey, o.sessionToken),
		))
	case o.profile != "":
		loadOpts = append(loadOpts, awsconfig.WithSharedConfigProfile(o.profile))
	}

	if o.endpoint != "" {
		loadOpts = append(loadOpts, awsconfig.WithBaseEndpoint(o.endpoint))
	}

	return loadOpts
}
