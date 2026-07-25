// Package typed provides generic, type-safe message handling on top of the
// standard middleware.Handler contract. It eliminates manual JSON unmarshaling
// boilerplate by decoding the raw message body into a caller-supplied type
// through a Codec before invoking a strongly-typed handler function.
package typed

import (
	"context"

	"github.com/silviolleite/loafer-awsx/middleware"
)

// Codec defines encoding and decoding for a specific type T. Implementations
// must be safe for concurrent use and satisfy the round-trip property
// Decode(Encode(x)) == x for all values of T they support.
type Codec[T any] interface {
	// Encode serializes v into its byte representation.
	Encode(v T) ([]byte, error)
	// Decode deserializes data into a value of type T.
	Decode(data []byte) (T, error)
}

// WrapHandler converts a typed handler function into a standard
// middleware.Handler. The supplied codec decodes the raw message body into a
// value of type T before the typed handler is invoked. When decoding fails the
// error is returned to the consumer for standard error handling and the typed
// handler is not called.
func WrapHandler[T any](codec Codec[T], fn func(ctx context.Context, msg T) error) middleware.Handler {
	return func(ctx context.Context, msg middleware.Message) error {
		value, err := codec.Decode(msg.Body())
		if err != nil {
			return err
		}

		return fn(ctx, value)
	}
}
