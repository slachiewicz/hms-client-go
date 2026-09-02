package ha_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/hms-client-go/internal/ha"
)

// fakeClock is an injectable clock for tests: Cluster never sleeps, and no
// test in this file sleeps either, per AGENTS.md's testing conventions.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func TestCluster_StickyFailoverAndBackoff(t *testing.T) {
	t.Parallel()
	clock := newFakeClock()
	c := ha.New(2, false, clock.Now)

	idx, ok := c.Pick()
	require.True(t, ok)
	assert.Equal(t, 0, idx)

	c.MarkFailed(0)
	idx, ok = c.Pick()
	require.True(t, ok)
	assert.Equal(t, 1, idx, "active must fail over to the next healthy endpoint")

	c.MarkFailed(1)
	_, ok = c.Pick()
	assert.False(t, ok, "every endpoint is cooling")
	assert.ElementsMatch(t, []int{0, 1}, c.Cooling())

	// The first backoff's ceiling is minBackoff (1s); its full-jitter
	// draw is always < 1s, so advancing well past 1s guarantees both
	// cooldowns have expired regardless of the actual jittered value.
	clock.Advance(2 * time.Second)
	idx, ok = c.Pick()
	require.True(t, ok, "an endpoint recovers once its cooldown elapses, even without MarkHealthy")
	assert.Contains(t, []int{0, 1}, idx)
	assert.Empty(t, c.Cooling())

	// MarkHealthy resets backoff and clears cooldown immediately, ahead
	// of natural expiry.
	c.MarkFailed(0)
	assert.Contains(t, c.Cooling(), 0)
	c.MarkHealthy(0)
	assert.NotContains(t, c.Cooling(), 0)
}

func TestCluster_ConsecutiveFailuresDoubleTheCeiling(t *testing.T) {
	t.Parallel()
	clock := newFakeClock()
	c := ha.New(1, false, clock.Now)

	// Full jitter draws uniformly from [0, ceiling), so only the upper
	// bound is deterministic: advancing by the ceiling itself always
	// clears the cooldown, regardless of the actual draw. Each
	// assertion below only relies on that guaranteed upper bound, never
	// on a lower one.
	c.MarkFailed(0)
	assert.Contains(t, c.Cooling(), 0, "must be cooling immediately after a failure")
	clock.Advance(time.Second)
	assert.Empty(t, c.Cooling(), "1s ceiling must have cleared by 1s")

	// Second consecutive failure with no MarkHealthy in between: the
	// ceiling doubles to 2s, so waiting the doubled ceiling must still
	// be sufficient to clear it.
	c.MarkFailed(0)
	assert.Contains(t, c.Cooling(), 0, "must be cooling immediately after a second failure")
	clock.Advance(2 * time.Second)
	assert.Empty(t, c.Cooling(), "doubled 2s ceiling must have cleared by 2s")
}

func TestCluster_RandomOrderIsPermutation(t *testing.T) {
	t.Parallel()
	clock := newFakeClock()
	c := ha.New(5, true, clock.Now)

	visited := map[int]bool{}
	for i := 0; i < 5; i++ {
		idx, ok := c.Pick()
		require.True(t, ok)
		visited[idx] = true
		c.MarkFailed(idx)
	}
	assert.Len(t, visited, 5, "random order must visit every index exactly once before repeating")
	for i := 0; i < 5; i++ {
		assert.True(t, visited[i], "index %d missing from the pick sequence", i)
	}

	_, ok := c.Pick()
	assert.False(t, ok, "all five endpoints are now cooling")
}

func TestCluster_MarkHealthyOnUnfailedEndpointIsANoop(t *testing.T) {
	t.Parallel()
	clock := newFakeClock()
	c := ha.New(1, false, clock.Now)

	c.MarkHealthy(0)
	idx, ok := c.Pick()
	require.True(t, ok)
	assert.Equal(t, 0, idx)
	assert.Empty(t, c.Cooling())
}
