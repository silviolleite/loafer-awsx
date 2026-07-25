package middleware

import (
	"context"
	"time"
)

// Handler is the function signature for message processing. It receives a
// context for cancellation and deadlines and the message to process, returning
// a non-nil error when processing fails.
type Handler func(ctx context.Context, msg Message) error

// Message is the minimal message interface required by middleware. It is a
// subset of the full consumer message and is declared here to avoid an import
// cycle between the middleware and consumer packages.
type Message interface {
	// Decode unmarshals the raw body into out using JSON.
	Decode(out any) error
	// DecodeMessage unmarshals the inner SNS Message field into out using JSON.
	DecodeMessage(out any) error
	// Attribute returns a custom attribute by key, or the empty string when
	// the key is absent.
	Attribute(key string) string
	// Attributes returns all custom attributes.
	Attributes() map[string]string
	// SystemAttributeByKey returns a system attribute by key, or the empty
	// string when the key is absent.
	SystemAttributeByKey(key string) string
	// SystemAttributes returns all system attributes.
	SystemAttributes() map[string]string
	// Metadata returns message metadata.
	Metadata() map[string]string
	// Identifier returns the receipt handle that uniquely identifies the
	// message for the current receive.
	Identifier() string
	// Body returns the raw message body.
	Body() []byte
	// Message returns the inner message content.
	Message() string
	// TimeStamp returns the message timestamp.
	TimeStamp() time.Time
}

// Middleware wraps a Handler with additional behavior and returns the wrapped
// Handler.
type Middleware func(Handler) Handler

// Chain composes multiple middlewares into a single Middleware using
// first-is-outermost semantics: the first middleware in the list becomes the
// outermost layer and therefore runs first on the way in and last on the way
// out.
//
//	Chain(A, B, C)(h) == A(B(C(h)))
//
// The resulting execution order on the way in is A → B → C → h. Calling Chain
// with no middlewares returns a Middleware that leaves the handler unchanged.
func Chain(mws ...Middleware) Middleware {
	return func(next Handler) Handler {
		for i := len(mws) - 1; i >= 0; i-- {
			next = mws[i](next)
		}
		return next
	}
}
