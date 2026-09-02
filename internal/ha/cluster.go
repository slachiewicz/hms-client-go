// Package ha implements the sticky-active endpoint selection, exponential
// backoff with full jitter, and recovery bookkeeping described in SPEC §4.2.
// It knows nothing about Thrift, transports, or the hms package; it is
// driven entirely through endpoint indexes chosen by the caller.
package ha

import (
	"crypto/rand"
	"math/big"
	"sort"
	"sync"
	"time"
)

// Backoff bounds applied by MarkFailed (SPEC §4.2 point 2): the first
// failure cools an endpoint for a random duration up to minBackoff: each
// consecutive failure (without an intervening MarkHealthy) doubles that
// ceiling, up to maxBackoff.
const (
	minBackoff = time.Second
	maxBackoff = 30 * time.Second
)

// Cluster tracks a fixed set of n endpoints, identified only by index
// (0..n-1), and chooses which one a call should use next: a sticky active
// endpoint that only moves on failure, per SPEC §4.2's "mirrors the Java
// HiveMetaStoreClient" policy. Cluster is safe for concurrent use.
type Cluster struct {
	mu sync.Mutex

	now func() time.Time

	// order is the fixed pick sequence: list order, or a permutation of
	// 0..n-1 when random is requested by New. pos indexes into order,
	// not into the endpoint's own index space.
	order []int
	// posOf is the inverse of order: posOf[idx] is the position of
	// endpoint idx within order.
	posOf []int
	pos   int

	// coolUntil[idx] is the zero Time when idx is not cooling, otherwise
	// the time at which its current cooldown ends.
	coolUntil []time.Time
	// level[idx] is the backoff ceiling used by idx's most recent
	// cooldown (0 before any failure or after MarkHealthy), so the next
	// consecutive failure can double it.
	level []time.Duration
}

// New returns a Cluster over n endpoints (indexed 0..n-1). When random is
// true the pick order is a random permutation of those indexes (SPEC §4.1);
// otherwise it is list order. now supplies the current time for every
// cooldown computation, so tests can inject a fake clock; production
// callers pass time.Now.
func New(n int, random bool, now func() time.Time) *Cluster {
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	if random {
		// Fisher-Yates, using cryptoRandInt63n so the pick order (and,
		// via it, which endpoint New's caller dials first) is not
		// predictable from one process to the next.
		for i := n - 1; i > 0; i-- {
			j := int(cryptoRandInt63n(int64(i + 1)))
			order[i], order[j] = order[j], order[i]
		}
	}
	posOf := make([]int, n)
	for p, idx := range order {
		posOf[idx] = p
	}
	return &Cluster{
		now:       now,
		order:     order,
		posOf:     posOf,
		coolUntil: make([]time.Time, n),
		level:     make([]time.Duration, n),
	}
}

// Pick returns the active endpoint's index. If the active endpoint is
// currently cooling, Pick itself advances the active endpoint to the
// first one, in pick order, that is not cooling -- which may be an
// endpoint whose earlier cooldown has simply elapsed on its own, with no
// intervening MarkHealthy -- so Pick alone, not only MarkFailed, can move
// which endpoint is active; once an endpoint is found not cooling, calling
// Pick again leaves it active until something (MarkFailed, or the next
// Pick finding it cooling again) moves it. Pick reports ok=false, leaving
// the active endpoint's position unchanged, only when every endpoint is
// cooling.
func (c *Cluster) Pick() (idx int, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	for i := 0; i < len(c.order); i++ {
		p := (c.pos + i) % len(c.order)
		if e := c.order[p]; !c.coolingAt(e, now) {
			c.pos = p
			return e, true
		}
	}
	return 0, false
}

// MarkFailed records a failure of endpoint idx: its cooldown ceiling
// doubles from its previous value (starting at minBackoff, capped at
// maxBackoff), a full-jitter duration within that ceiling is drawn, and the
// active endpoint advances to the next one, in pick order, that is not
// currently cooling. If every other endpoint is also cooling, the active
// endpoint is left unchanged (Pick will report ok=false until one recovers).
func (c *Cluster) MarkFailed(idx int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()

	ceiling := c.level[idx]
	if ceiling <= 0 {
		ceiling = minBackoff
	} else {
		ceiling *= 2
		if ceiling > maxBackoff {
			ceiling = maxBackoff
		}
	}
	c.level[idx] = ceiling
	jitter := time.Duration(cryptoRandInt63n(int64(ceiling)))
	c.coolUntil[idx] = now.Add(jitter)

	pos := c.posOf[idx]
	for i := 1; i <= len(c.order); i++ {
		p := (pos + i) % len(c.order)
		if e := c.order[p]; !c.coolingAt(e, now) {
			c.pos = p
			return
		}
	}
	// Every endpoint (including idx itself) is cooling; leave pos as is.
}

// MarkHealthy clears idx's cooldown and resets its backoff ceiling, so its
// next failure starts again at minBackoff. Both the successful-call path in
// Client.call and the recovery probe call this once they have confirmed an
// endpoint is usable again.
func (c *Cluster) MarkHealthy(idx int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.coolUntil[idx] = time.Time{}
	c.level[idx] = 0
}

// Cooling returns the indexes of every endpoint currently in cooldown, in
// ascending order. The recovery probe uses this to know which endpoints to
// re-test.
func (c *Cluster) Cooling() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	var out []int
	for idx := range c.coolUntil {
		if c.coolingAt(idx, now) {
			out = append(out, idx)
		}
	}
	sort.Ints(out)
	return out
}

// coolingAt reports whether idx is still cooling at now. c.mu must be held.
func (c *Cluster) coolingAt(idx int, now time.Time) bool {
	u := c.coolUntil[idx]
	return !u.IsZero() && now.Before(u)
}

// cryptoRandInt63n returns a uniform random int64 in [0, n) drawn from
// crypto/rand. Backoff jitter and pick-order shuffling are not
// security-sensitive, but using crypto/rand here (rather than math/rand or
// math/rand/v2, seeded or not) means gosec's G404 rule — which flags any
// use of a non-cryptographic generator, full stop — has nothing to flag,
// and n is always small enough (endpoint counts, or a duration in
// nanoseconds bounded by maxBackoff) that crypto/rand's extra cost is
// immaterial. n<=0 returns 0.
func cryptoRandInt63n(n int64) int64 {
	if n <= 0 {
		return 0
	}
	v, err := rand.Int(rand.Reader, big.NewInt(n))
	if err != nil {
		// crypto/rand failing is exceptionally rare (a broken system
		// entropy source); degrade to no jitter/no shuffle rather than
		// panicking.
		return 0
	}
	return v.Int64()
}
