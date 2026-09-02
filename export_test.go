package hms

import (
	"context"
	"time"

	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
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

// ClientConnectTimeout exposes a Client's effective, clamped connect
// timeout (config.connectTimeout, see config.clamp and WithConnectTimeout)
// to hms_test, since cfg is unexported.
func ClientConnectTimeout(c *Client) time.Duration { return c.cfg.connectTimeout }

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

// ClientMarkFailed and ClientPick expose the Client's ha.Cluster to
// hms_test, since Cluster is unexported (internal/ha). ClientMarkFailed
// forces an endpoint into cooldown without needing to actually kill its
// server (the recovery-probe test); ClientPick observes which endpoint the
// cluster considers active (fix round 1's cancelled-caller test, which
// must confirm cancellation alone never moved it).
func ClientMarkFailed(c *Client, idx int) { c.cluster.MarkFailed(idx) }
func ClientPick(c *Client) (int, bool)    { return c.cluster.Pick() }

// ConfigWantsSetUgi exposes config.wantsSetUgi to hms_test: whether opts
// would make newConn issue set_ugi on a binary NOSASL dial. hmstest's fake
// server does not implement the SASL PLAIN handshake (see
// internal/transport/sasl.go), so the WithPlainAuth/WithUser gating this
// exercises cannot be driven end-to-end through a live New() call; this
// applies opts to a fresh config exactly as New does (including clamp) and
// reports the resulting decision directly.
func ConfigWantsSetUgi(opts ...Option) bool {
	cfg := newConfig()
	for _, o := range opts {
		o(cfg)
	}
	cfg.clamp()
	return cfg.wantsSetUgi()
}

// ConfigUgiUser exposes config.ugiUser, as New itself resolves it (see
// resolveUgiUser), to hms_test: WithUser's value if set, else the current
// OS user. It lets a test assert the default set_ugi identity a live dial
// records is exactly what New would have resolved, without duplicating
// os/user.Current's fallback logic in the test itself.
func ConfigUgiUser(opts ...Option) string {
	cfg := newConfig()
	for _, o := range opts {
		o(cfg)
	}
	cfg.clamp()
	cfg.resolveUgiUser()
	return cfg.ugiUser
}

// WithProbeIntervalForTest overrides the recovery probe's tick interval
// (default 30s), so ha_test.go can bound its waits to well under a second.
func WithProbeIntervalForTest(d time.Duration) Option { return withProbeInterval(d) }

// WithDialHookForTest returns a context derived from ctx that makes newConn
// call fn before doing any real dial work, when that ctx is the one passed
// to Client.acquire (e.g. via ClientAcquire). It lets a test hold a
// specific, acquire-triggered dial open for as long as it needs -- to race
// it against a concurrent Close, say -- without a real slow or fake server,
// and without a package-level hook that every parallel test's own dials
// would also trip.
func WithDialHookForTest(ctx context.Context, fn func()) context.Context {
	return context.WithValue(ctx, dialHookKey{}, fn)
}

// TableRaw, PartitionRaw and DatabaseRaw expose Table.raw, Partition.raw and
// Database.raw to hms_test, the round-trip fidelity snapshot
// tableFromThrift/partitionFromThrift/databaseFromThrift set and
// tableToThriftFrom/partitionToThriftFrom/databaseToThriftFrom build on (see
// Table's doc comment), since raw is unexported.
func TableRaw(t *Table) *hive_metastore.Table             { return t.raw }
func PartitionRaw(p *Partition) *hive_metastore.Partition { return p.raw }
func DatabaseRaw(d *Database) *hive_metastore.Database    { return d.raw }

// StripTableRaw and StripDatabaseRaw return a shallow copy of t/d with the
// round-trip fidelity snapshot cleared, for a black-box test that compares
// a whole Table or Database for equality (e.g. via testify's assert.Equal,
// which does not ignore unexported fields) without wanting that
// server-populated, unexported field to participate.
func StripTableRaw(t *Table) *Table {
	if t == nil {
		return nil
	}
	cp := *t
	cp.raw = nil
	return &cp
}

func StripDatabaseRaw(d *Database) *Database {
	if d == nil {
		return nil
	}
	cp := *d
	cp.raw = nil
	return &cp
}
