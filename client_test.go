package hms_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hms "github.com/slachiewicz/hms-client-go"
	"github.com/slachiewicz/hms-client-go/internal/hmstest"
)

func TestNew_ConnectsAndReadsVersion(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c, err := hms.New(context.Background(), srv.URI())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	v, err := c.ServerVersion(context.Background())
	require.NoError(t, err)
	assert.Equal(t, hms.HiveVersion{Major: 4, Minor: 0, Patch: 1, Raw: "4.0.1"}, v)
	assert.Contains(t, srv.Calls(), "getVersion", "ServerVersion must try the fb303 getVersion RPC")
}

func TestNew_RefusedEndpoint(t *testing.T) {
	t.Parallel()
	_, err := hms.New(context.Background(), "thrift://127.0.0.1:1")
	require.ErrorIs(t, err, hms.ErrUnavailable)
}

func TestNew_BadURI(t *testing.T) {
	t.Parallel()
	_, err := hms.New(context.Background(), "ftp://x")
	require.Error(t, err)
	require.ErrorIs(t, err, hms.ErrInvalidOperation)
}

func TestClient_Close(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c, err := hms.New(context.Background(), srv.URI())
	require.NoError(t, err)

	require.NoError(t, c.Close())
	// Close is idempotent.
	require.NoError(t, c.Close())

	_, err = c.GetConfigValue(context.Background(), "x", "y")
	require.ErrorIs(t, err, hms.ErrUnavailable)
}

// TestClient_ReleaseRacingCloseDoesNotLeakConn covers the fix for a race
// between release and Close: release used to check c.closed and, finding
// it false, send the conn into the idle channel; if Close's own drain had
// already run to completion in the meantime (because the conn was still
// on loan when Close checked), that conn would sit in idle forever,
// leaked (never closed, still counted in live). release and Close now
// share one mutex around the closed flag and every idle send/drain, so
// the outcome is deterministic regardless of which one wins: the conn is
// always closed and live always ends at 0.
func TestClient_ReleaseRacingCloseDoesNotLeakConn(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c, err := hms.New(context.Background(), srv.URI(), hms.WithPoolSize(1))
	require.NoError(t, err)

	// Take the pool's only conn on loan, mirroring a call whose fn is
	// still in flight when Close runs.
	cn, err := hms.ClientAcquire(c, context.Background(), 0)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = c.Close()
	}()
	go func() {
		defer wg.Done()
		hms.ClientRelease(c, 0, cn)
	}()
	wg.Wait()

	assert.Equal(t, int32(0), hms.ClientLiveConns(c, 0), "release racing Close must not leak the conn")
}

// TestClient_AcquireWakesOnClose covers the fix for acquire never waking
// on Close: a waiter blocked in acquire's final select (no idle conn, pool
// already at capacity) used to have no way to observe Close, so it blocked
// until its own context was done — forever, for a caller using
// context.Background(). acquire now also selects on closeCh, which Close
// closes exactly once.
func TestClient_AcquireWakesOnClose(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c, err := hms.New(context.Background(), srv.URI(), hms.WithPoolSize(1))
	require.NoError(t, err)

	// Hold the only pooled conn on loan so a second acquire has nowhere
	// to come from but Close.
	cn, err := hms.ClientAcquire(c, context.Background(), 0)
	require.NoError(t, err)

	waiterErr := make(chan error, 1)
	go func() {
		_, err := hms.ClientAcquire(c, context.Background(), 0)
		waiterErr <- err
	}()

	// Give the waiter goroutine a moment to actually park in acquire's
	// blocking select before Close runs, so this exercises the closeCh
	// wakeup rather than the closed-check fast path at acquire's start.
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, c.Close())

	select {
	case err := <-waiterErr:
		require.ErrorIs(t, err, hms.ErrUnavailable)
	case <-time.After(2 * time.Second):
		t.Fatal("acquire did not wake up promptly after Close")
	}

	hms.ClientRelease(c, 0, cn)
}

// TestNew_PoolSizeClamped covers the fix for WithPoolSize(0) hanging New:
// idle was an unbuffered channel in that case, so New's own unconditional
// send of the eagerly dialed conn (c.idle <- cn) blocked forever with
// nothing to receive it. A negative pool size panicked outright (make
// with a negative size). Both are now clamped to 1.
func TestNew_PoolSizeClamped(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)

	for _, n := range []int{0, -5} {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		c, err := hms.New(ctx, srv.URI(), hms.WithPoolSize(n))
		cancel()
		require.NoError(t, err)
		t.Cleanup(func() { _ = c.Close() })
		assert.Equal(t, 1, hms.ClientPoolSize(c))
	}
}

// TestNew_MaxRetriesClamped covers the fix clamping a non-positive
// WithMaxRetries to 1, so an RPC is always attempted at least once.
func TestNew_MaxRetriesClamped(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)

	for _, n := range []int{0, -3} {
		c := mustNew(t, srv.URI(), hms.WithMaxRetries(n))
		assert.Equal(t, 1, hms.ClientMaxRetries(c))
	}
}

// TestNew_ConnectTimeoutDefaultsToTimeout covers config.clamp's default for
// WithConnectTimeout (SPEC §5.1): left unset (or set to 0), it resolves to
// WithTimeout's value rather than staying 0 (which would mean "no dial-side
// timeout" -- a silent behavior change for callers who only ever tuned
// WithTimeout).
func TestNew_ConnectTimeoutDefaultsToTimeout(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)

	c := mustNew(t, srv.URI(), hms.WithTimeout(7*time.Second))
	assert.Equal(t, 7*time.Second, hms.ClientConnectTimeout(c))
}

// TestNew_ConnectTimeoutExplicit covers WithConnectTimeout overriding the
// WithTimeout-derived default.
func TestNew_ConnectTimeoutExplicit(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)

	c := mustNew(t, srv.URI(), hms.WithTimeout(7*time.Second), hms.WithConnectTimeout(3*time.Second))
	assert.Equal(t, 3*time.Second, hms.ClientConnectTimeout(c))
}

// TestClient_ContextExpiredMidRPCDiscardsConn covers the fix for do()
// releasing a conn whose fn failed only because the caller's own ctx was
// already past its deadline: fn's error classifies as ErrUnavailable (via
// ContextClient's ctx.Err() override, see internal/transport/ctxclient.go)
// but ctx.Err() != nil, so the old code took the release branch and would
// hand the next caller a conn that may still have a half-read response on
// the wire. do() must discard whenever classify(err) is ErrUnavailable,
// independent of ctx; MarkFailed/retry alone are gated on ctx.Err() == nil.
func TestClient_ContextExpiredMidRPCDiscardsConn(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c, err := hms.New(context.Background(), srv.URI(), hms.WithPoolSize(1))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	// New eagerly dials and pools one conn for endpoint 0.
	require.Equal(t, int32(1), hms.ClientLiveConns(c, 0))

	// A deadline already in the past makes ctx.Err() != nil (and its
	// conn's socket deadline already elapsed) before fn ever runs,
	// deterministically forcing the RPC to fail without racing a real
	// in-flight cancellation.
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Minute))
	defer cancel()

	_, err = c.GetAllDatabases(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, hms.ErrUnavailable)

	assert.Equal(t, int32(0), hms.ClientLiveConns(c, 0),
		"a ctx-expired RPC failure must discard the conn, not release it")

	// A fresh, valid call must still succeed: it dials a brand-new conn
	// rather than reusing anything left over from the failed call.
	_, err = c.GetAllDatabases(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int32(1), hms.ClientLiveConns(c, 0))
}

// TestNew_SetUgi_FirstCallOnConnection covers SPEC §3.1: over binary NOSASL,
// a configured WithUser (and WithUserGroups) makes newConn issue set_ugi
// once a connection dials, before any caller-initiated RPC on it.
func TestNew_SetUgi_FirstCallOnConnection(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	mustNew(t, srv.URI(), hms.WithUser("alice"), hms.WithUserGroups("eng"))

	// New's own eager dial is the only thing that has run on this
	// connection so far, so set_ugi must be both present and first.
	calls := srv.Calls()
	require.NotEmpty(t, calls)
	assert.Equal(t, "set_ugi", calls[0])
	assert.Equal(t, hmstest.SetUgiArgs{User: "alice", Groups: []string{"eng"}}, srv.LastArgs("set_ugi"))
}

// TestNew_SetUgi_OnePerConnection covers the fix's per-conn scope: each
// newly dialed connection issues its own set_ugi, not just the first one in
// the pool. WithPoolSize(2) lets a second conn dial without waiting for the
// first (already on loan) to be released, so two sequential acquires
// deterministically produce two dials and thus two set_ugi calls, without
// needing an actual goroutine race to force it.
func TestNew_SetUgi_OnePerConnection(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c, err := hms.New(context.Background(), srv.URI(), hms.WithUser("alice"), hms.WithPoolSize(2))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	cn0, err := hms.ClientAcquire(c, context.Background(), 0)
	require.NoError(t, err)
	cn1, err := hms.ClientAcquire(c, context.Background(), 0)
	require.NoError(t, err)
	hms.ClientRelease(c, 0, cn0)
	hms.ClientRelease(c, 0, cn1)

	count := 0
	for _, m := range srv.Calls() {
		if m == "set_ugi" {
			count++
		}
	}
	assert.Equal(t, 2, count, "each of the pool's two dialed connections must issue its own set_ugi")
}

// TestNew_SetUgi_NotCalledWithoutUser covers the fix's default behavior:
// with no WithUser configured, newConn never issues set_ugi.
func TestNew_SetUgi_NotCalledWithoutUser(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI())

	_, err := c.GetAllDatabases(context.Background())
	require.NoError(t, err)
	assert.NotContains(t, srv.Calls(), "set_ugi")
}

// TestConfigWantsSetUgi covers config.wantsSetUgi's gating (SPEC §3.1):
// set_ugi is wanted only with a configured WithUser and no WithPlainAuth.
// This is exercised directly, via export_test.go's ConfigWantsSetUgi,
// rather than through a live New() call: hmstest's fake server does not
// implement the SASL PLAIN handshake (see internal/transport/sasl.go), so
// WithPlainAuth against it fails at dial, before ever reaching the point
// this gate lives at -- that would prove nothing about the gate itself.
func TestConfigWantsSetUgi(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		opts []hms.Option
		want bool
	}{
		{"WithUser alone wants set_ugi", []hms.Option{hms.WithUser("alice")}, true},
		{"no WithUser does not want set_ugi", nil, false},
		{"WithPlainAuth suppresses set_ugi even with WithUser", []hms.Option{hms.WithUser("alice"), hms.WithPlainAuth("bob", "pw")}, false},
		{"WithPlainAuth alone does not want set_ugi", []hms.Option{hms.WithPlainAuth("bob", "pw")}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, hms.ConfigWantsSetUgi(tt.opts...))
		})
	}
}

func TestClient_GetConfigValue(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI())

	v, err := c.GetConfigValue(context.Background(), "does.not.exist", "fallback")
	require.NoError(t, err)
	assert.Equal(t, "fallback", v)
}

func TestServerVersion_InfersLineFromCatalogSupport(t *testing.T) {
	t.Parallel()
	// getVersion on a pre-4 metastore answers the metastore schema line
	// "3.0" rather than its actual release (see hmstest.versionString), so
	// ServerVersion must tell Hive 2.3 apart from Hive 3.x by probing
	// catalog support on the same connection (SPEC §2.3 Rule 1; Hive 2.3
	// predates catalogs, Hive 3.x has them).
	tests := []struct {
		name string
		v    hmstest.Version
		want hms.HiveVersion
	}{
		{"Hive23 has no catalogs, inferred as 2.3", hmstest.Hive23, hms.HiveVersion{Major: 2, Minor: 3, Raw: "3.0"}},
		{"Hive31 has catalogs, inferred as 3.0", hmstest.Hive31, hms.HiveVersion{Major: 3, Minor: 0, Raw: "3.0"}},
		{"Hive40 reports its real release directly", hmstest.Hive40, hms.HiveVersion{Major: 4, Minor: 0, Patch: 1, Raw: "4.0.1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			srv := hmstest.Start(t, tt.v)
			c := mustNew(t, srv.URI())

			v, err := c.ServerVersion(context.Background())
			require.NoError(t, err)
			assert.Equal(t, tt.want, v)
			assert.Contains(t, srv.Calls(), "getVersion")
		})
	}
}

func TestServerVersion_Hive23FallsBackToHiveMetastoreVersion(t *testing.T) {
	t.Parallel()
	// WithoutRPC("getVersion") simulates a server whose fb303 service
	// lacks getVersion, so ServerVersion must fall back to the
	// "hive.metastore.version" get_config_value key (Start no longer
	// seeds either config key itself; see versionString).
	srv := hmstest.Start(t, hmstest.Hive23, hmstest.WithoutRPC("getVersion"))
	srv.Store().Config["hive.metastore.version"] = "2.3.9"
	c := mustNew(t, srv.URI())

	v, err := c.ServerVersion(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, v.Major)
	assert.Equal(t, 3, v.Minor)
	assert.NotContains(t, srv.Calls(), "getVersion")
}

func TestServerVersion_NeitherConfigValueSet(t *testing.T) {
	t.Parallel()
	// WithoutRPC("getVersion") simulates a server whose fb303 service
	// lacks getVersion; Start no longer seeds either get_config_value
	// fallback key itself, so with getVersion also gone, all three
	// sources ServerVersion tries report nothing.
	srv := hmstest.Start(t, hmstest.Hive40, hmstest.WithoutRPC("getVersion"))
	c := mustNew(t, srv.URI())

	_, err := c.ServerVersion(context.Background())
	require.ErrorIs(t, err, hms.ErrNotSupported)
}

func TestParseHiveVersion(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		in      string
		want    hms.HiveVersion
		wantErr bool
	}{
		{"simple", "4.0.1", hms.HiveVersion{Major: 4, Minor: 0, Patch: 1, Raw: "4.0.1"}, false},
		{"vendor build", "3.1.3000.7.1.7.0-551", hms.HiveVersion{Major: 3, Minor: 1, Patch: 3000, Raw: "3.1.3000.7.1.7.0-551"}, false},
		{"garbage", "not-a-version", hms.HiveVersion{}, true},
		// Every Hive 3.1.x metastore's getVersion answers the schema line
		// "3.0" rather than a release number (see HiveVersion's doc
		// comment); ParseHiveVersion must accept the two-component form,
		// defaulting Patch to 0.
		{"two component (Hive 3.1's schema line)", "3.0", hms.HiveVersion{Major: 3, Minor: 0, Patch: 0, Raw: "3.0"}, false},
		{"too short", "4", hms.HiveVersion{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := hms.ParseHiveVersion(tt.in)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHiveVersion_String(t *testing.T) {
	t.Parallel()
	v, err := hms.ParseHiveVersion("4.0.1")
	require.NoError(t, err)
	assert.Equal(t, "4.0.1", v.String())
}
