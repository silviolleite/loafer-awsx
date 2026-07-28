package consumer

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

// Feature: fifo-scheduled-retry, Property 3: Retry count increments by one on failure
func TestNextCountIncrementsByOne(t *testing.T) {
	log := discardLogger()

	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(0, 2_147_483_647).Draw(t, "n")
		msg := messageWithRetryCount(true, strconv.Itoa(n))
		assert.Equal(t, n+1, parseRetryCount(msg, log)+1)
	})
}
