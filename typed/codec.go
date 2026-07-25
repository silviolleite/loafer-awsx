package typed

import "encoding/json"

// JSONCodec is a Codec implementation that serializes and deserializes values
// of type T using the standard library encoding/json package. The zero value
// is ready to use and safe for concurrent use.
type JSONCodec[T any] struct{}

// Encode serializes v into JSON bytes.
func (JSONCodec[T]) Encode(v T) ([]byte, error) {
	return json.Marshal(v)
}

// Decode deserializes JSON bytes into a value of type T. When data is not valid
// JSON for T, the zero value of T and a non-nil error are returned.
func (JSONCodec[T]) Decode(data []byte) (T, error) {
	var out T
	if err := json.Unmarshal(data, &out); err != nil {
		return out, err
	}

	return out, nil
}
