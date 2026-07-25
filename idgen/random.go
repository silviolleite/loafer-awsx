package idgen

import (
	"context"
	"crypto/rand"
	"encoding/hex"
)

const (
	// uuidByteLen is the number of random bytes that back a UUID.
	uuidByteLen = 16
	// uuidVersionIndex is the byte holding the version nibble.
	uuidVersionIndex = 6
	// uuidVariantIndex is the byte holding the variant bits.
	uuidVariantIndex = 8
	// uuidVersionMask clears the high nibble of the version byte.
	uuidVersionMask = 0x0f
	// uuidVersion4 sets the high nibble of the version byte to 4.
	uuidVersion4 = 0x40
	// uuidVariantMask clears the two high bits of the variant byte.
	uuidVariantMask = 0x3f
	// uuidVariantRFC4122 sets the two high bits of the variant byte to 10.
	uuidVariantRFC4122 = 0x80
	// uuidStringLen is the length of the canonical 8-4-4-4-12 hyphenated form.
	uuidStringLen = 36
)

// random is a non-deterministic generator that ignores its input fields and
// returns a fresh RFC 4122 version 4 UUID on every call. It is safe for
// concurrent use because crypto/rand.Read is safe for concurrent use and the
// type holds no mutable state.
type random struct{}

// NewRandom builds a generator that returns a random UUID version 4 string on
// every call, ignoring the provided fields. The returned generator satisfies
// both GroupIDGenerator and DeduplicationIDGenerator.
func NewRandom() GroupIDGenerator {
	return random{}
}

// Generate returns a random UUID version 4 string in the canonical
// 8-4-4-4-12 hexadecimal form. The fields argument is ignored. It returns an
// error only when the system random source cannot be read.
func (random) Generate(_ context.Context, _ map[string]string) (string, error) {
	var b [uuidByteLen]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}

	b[uuidVersionIndex] = (b[uuidVersionIndex] & uuidVersionMask) | uuidVersion4
	b[uuidVariantIndex] = (b[uuidVariantIndex] & uuidVariantMask) | uuidVariantRFC4122

	return format(b), nil
}

// format renders the 16 UUID bytes in the canonical 8-4-4-4-12 hyphenated
// hexadecimal representation.
func format(b [uuidByteLen]byte) string {
	buf := make([]byte, 0, uuidStringLen)
	buf = hex.AppendEncode(buf, b[0:4])
	buf = append(buf, '-')
	buf = hex.AppendEncode(buf, b[4:6])
	buf = append(buf, '-')
	buf = hex.AppendEncode(buf, b[6:8])
	buf = append(buf, '-')
	buf = hex.AppendEncode(buf, b[8:10])
	buf = append(buf, '-')
	buf = hex.AppendEncode(buf, b[10:16])
	return string(buf)
}
