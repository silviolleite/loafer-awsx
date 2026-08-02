package client

import (
	"fmt"
	"time"

	"github.com/silviolleite/loafer-awsx/errors"
)

// Option configures a client constructor. Options are applied in the order they
// are supplied and may return an error to abort construction.
type Option func(*options) error

// options accumulates the connectivity-validation configuration produced by the
// functional options before it is consumed by a constructor.
type options struct {
	pingTimeout       time.Duration
	pingRetryLimit    uint
	connectivityCheck bool
}

const (
	// defaultPingTimeout is the total time budget applied to the connectivity
	// validation, including its retries, when WithPingTimeout is not supplied.
	defaultPingTimeout = 3 * time.Second
	// defaultPingRetryLimit is the number of retries the connectivity validation
	// performs beyond the initial attempt when WithPingRetryLimit is not supplied.
	defaultPingRetryLimit = uint(2)
)

// pingConfig carries the connectivity-validation timeout and retry budget to the
// validation runner.
type pingConfig struct {
	timeout    time.Duration
	retryLimit uint
}

// newOptions returns an options value seeded with the library defaults: a
// 3-second ping timeout, a retry limit of 2, and connectivity validation
// enabled.
func newOptions() *options {
	return &options{
		pingTimeout:       defaultPingTimeout,
		pingRetryLimit:    defaultPingRetryLimit,
		connectivityCheck: true,
	}
}

// pingConfig returns the connectivity-validation configuration derived from the
// current options.
func (o *options) pingConfig() pingConfig {
	return pingConfig{
		timeout:    o.pingTimeout,
		retryLimit: o.pingRetryLimit,
	}
}

// buildOptions applies opts in order, skipping nil options, and returns the
// resulting options. Any option error is wrapped with ErrInvalidOption and
// aborts construction.
func buildOptions(opts ...Option) (*options, error) {
	o := newOptions()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(o); err != nil {
			return nil, errors.Wrap(errors.ErrInvalidOption, err)
		}
	}
	return o, nil
}

// WithPingTimeout overrides the total time budget for the connectivity
// validation, including retries. The duration must be positive; a non-positive
// value fails construction with an error wrapping ErrInvalidOption.
func WithPingTimeout(d time.Duration) Option {
	return func(o *options) error {
		if d <= 0 {
			return fmt.Errorf("ping timeout must be positive, got %s", d)
		}
		o.pingTimeout = d
		return nil
	}
}

// WithPingRetryLimit overrides the maximum number of retries the connectivity
// validation performs beyond the initial attempt.
func WithPingRetryLimit(n uint) Option {
	return func(o *options) error {
		o.pingRetryLimit = n
		return nil
	}
}

// WithoutConnectivityCheck disables the connectivity validation performed during
// construction. Use it when the credentials lack the read-only permission the
// Ping requires, or to construct a client offline.
func WithoutConnectivityCheck() Option {
	return func(o *options) error {
		o.connectivityCheck = false
		return nil
	}
}
