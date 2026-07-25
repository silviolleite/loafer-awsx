package typed_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/typed"
)

type inner struct {
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}

type payload struct {
	Meta   map[string]int `json:"meta"`
	ID     string         `json:"id"`
	Nested inner          `json:"nested"`
	Items  []inner        `json:"items"`
	Count  int            `json:"count"`
	Active bool           `json:"active"`
}

func drawInner(t *rapid.T, label string) inner {
	return inner{
		Name: rapid.String().Draw(t, label+".name"),
		Tags: rapid.SliceOf(rapid.String()).Draw(t, label+".tags"),
	}
}

func drawPayload(t *rapid.T) payload {
	items := rapid.SliceOfN(rapid.Custom(func(t *rapid.T) inner {
		return drawInner(t, "item")
	}), 0, 5).Draw(t, "items")

	return payload{
		ID:     rapid.String().Draw(t, "id"),
		Count:  rapid.Int().Draw(t, "count"),
		Active: rapid.Bool().Draw(t, "active"),
		Nested: drawInner(t, "nested"),
		Items:  items,
		Meta:   rapid.MapOf(rapid.String(), rapid.Int()).Draw(t, "meta"),
	}
}

func TestJSONCodecRoundTrip(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		original := drawPayload(rt)

		codec := typed.JSONCodec[payload]{}

		encoded, err := codec.Encode(original)
		require.NoError(rt, err)

		decoded, err := codec.Decode(encoded)
		require.NoError(rt, err)

		require.Equal(rt, original, decoded)
	})
}

func TestJSONCodecEncode(t *testing.T) {
	tests := []struct {
		name  string
		want  string
		value payload
	}{
		{
			name:  "zero value",
			value: payload{},
			want:  `{"id":"","count":0,"active":false,"nested":{"name":"","tags":null},"items":null,"meta":null}`,
		},
		{
			name: "nested slices and maps",
			value: payload{
				ID:     "abc",
				Count:  7,
				Active: true,
				Nested: inner{Name: "root", Tags: []string{"a", "b"}},
				Items:  []inner{{Name: "x", Tags: []string{"t"}}},
				Meta:   map[string]int{"k": 1},
			},
			want: `{"id":"abc","count":7,"active":true,"nested":{"name":"root","tags":["a","b"]},"items":[{"name":"x","tags":["t"]}],"meta":{"k":1}}`,
		},
	}

	codec := typed.JSONCodec[payload]{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := codec.Encode(tt.value)
			require.NoError(t, err)
			assert.JSONEq(t, tt.want, string(encoded))
		})
	}
}

func TestJSONCodecDecode(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    payload
		wantErr bool
	}{
		{
			name: "valid nested document",
			data: []byte(`{"id":"abc","count":7,"active":true,"nested":{"name":"root","tags":["a","b"]},"items":[{"name":"x","tags":["t"]}],"meta":{"k":1}}`),
			want: payload{
				ID:     "abc",
				Count:  7,
				Active: true,
				Nested: inner{Name: "root", Tags: []string{"a", "b"}},
				Items:  []inner{{Name: "x", Tags: []string{"t"}}},
				Meta:   map[string]int{"k": 1},
			},
		},
		{
			name:    "invalid json",
			data:    []byte("{"),
			want:    payload{},
			wantErr: true,
		},
		{
			name:    "type mismatch",
			data:    []byte(`{"count":"not-a-number"}`),
			want:    payload{},
			wantErr: true,
		},
	}

	codec := typed.JSONCodec[payload]{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decoded, err := codec.Decode(tt.data)
			if tt.wantErr {
				require.Error(t, err)
				assert.Equal(t, tt.want, decoded)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, decoded)
		})
	}
}
