package consumer

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

func TestGroupBarrierFailAndFailed(t *testing.T) {
	b := newGroupBarrier()

	assert.False(t, b.failed("a"))
	b.fail("a")
	assert.True(t, b.failed("a"))
	assert.False(t, b.failed("b"))
}

func TestGroupBarrierNilSafe(t *testing.T) {
	var b *groupBarrier

	assert.False(t, b.failed("a"))
	assert.NotPanics(t, func() { b.fail("a") })
	assert.False(t, b.failed("a"))
}

func TestGroupBarrierConcurrentAccess(t *testing.T) {
	b := newGroupBarrier()

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.fail("shared")
			_ = b.failed("shared")
		}()
	}
	wg.Wait()

	assert.True(t, b.failed("shared"))
}

func TestGroupBarrierIndependentKeysProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		keys := rapid.SliceOfNDistinct(
			rapid.StringMatching(`[a-z]{1,8}`),
			1, 12,
			func(s string) string { return s },
		).Draw(rt, "keys")

		b := newGroupBarrier()
		failIdx := rapid.IntRange(0, len(keys)-1).Draw(rt, "failIdx")
		b.fail(keys[failIdx])

		for i, k := range keys {
			assert.Equal(rt, i == failIdx, b.failed(k))
		}
	})
}
