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

func TestClient_GetConfigValue(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI())

	v, err := c.GetConfigValue(context.Background(), "does.not.exist", "fallback")
	require.NoError(t, err)
	assert.Equal(t, "fallback", v)
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
		{"too short", "4.0", hms.HiveVersion{}, true},
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
