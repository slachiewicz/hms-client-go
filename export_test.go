package hms

import (
	"context"
	"time"
)

var (
	WrapError       = wrapError
	IsUnknownMethod = isUnknownMethod
)

// NewTestConn returns a zero-value conn suitable for exercising the
// fallback-cache contract (ConnUseLegacy / ConnMarkLegacy) from a
// black-box test, without dialing a real transport.
func NewTestConn() *conn { return &conn{} }

// ConnUseLegacy and ConnMarkLegacy expose conn.useLegacy and
// conn.markLegacy to hms_test, since Task 11's retry/failover loop is the
// first in-package caller of the fallback cache they read and write.
func ConnUseLegacy(cn *conn, method string) bool { return cn.useLegacy(method) }
func ConnMarkLegacy(cn *conn, method string)     { cn.markLegacy(method) }

// ClientPoolSize and ClientMaxRetries expose a Client's effective, clamped
// config values (see config.clamp) to hms_test, since cfg is unexported.
func ClientPoolSize(c *Client) int   { return c.cfg.poolSize }
func ClientMaxRetries(c *Client) int { return c.cfg.maxRetries }

// ClientAcquire, ClientRelease, and ClientLiveConns expose Client's pool
// internals to hms_test, so it can drive and observe the exact
// acquire/release/Close interleavings the pool-lifecycle tests exercise
// without needing a call's fn to block on demand. Task 11 made the pool
// per-endpoint, so these take the endpoint index too; every pool-lifecycle
// test predating Task 11 used a single-endpoint Client, so idx 0 is always
// correct for them.
func ClientAcquire(c *Client, ctx context.Context, idx int) (*conn, error) {
	return c.acquire(ctx, idx)
}
func ClientRelease(c *Client, idx int, cn *conn) { c.release(idx, cn) }
func ClientLiveConns(c *Client, idx int) int32   { return c.pools[idx].live.Load() }

// ClientMarkFailed exposes the Client's ha.Cluster to hms_test, since
// Cluster is unexported (internal/ha). Task 11's recovery probe test uses
// it to force an endpoint into cooldown without needing to actually kill
// its server.
func ClientMarkFailed(c *Client, idx int) { c.cluster.MarkFailed(idx) }

// WithProbeIntervalForTest overrides the recovery probe's tick interval
// (default 30s), so ha_test.go can bound its waits to well under a second.
func WithProbeIntervalForTest(d time.Duration) Option { return withProbeInterval(d) }

// SetChunkSizeForTest overrides defaultChunkSize, GetTables' per-request
// chunk size, so a test can exercise chunking without needing thousands of
// fixture tables. Call the returned restore func (typically via defer) to
// put the original value back.
func SetChunkSizeForTest(n int) (restore func()) {
	old := defaultChunkSize
	defaultChunkSize = n
	return func() { defaultChunkSize = old }
}
