package hms

import "context"

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
// without needing a call's fn to block on demand.
func ClientAcquire(c *Client, ctx context.Context) (*conn, error) { return c.acquire(ctx) }
func ClientRelease(c *Client, cn *conn)                           { c.release(cn) }
func ClientLiveConns(c *Client) int32                             { return c.live.Load() }
