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
	cn, err := hms.ClientAcquire(c, context.Background())
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = c.Close()
	}()
	go func() {
		defer wg.Done()
		hms.ClientRelease(c, cn)
	}()
	wg.Wait()

	assert.Equal(t, int32(0), hms.ClientLiveConns(c), "release racing Close must not leak the conn")
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
	cn, err := hms.ClientAcquire(c, context.Background())
	require.NoError(t, err)

	waiterErr := make(chan error, 1)
	go func() {
		_, err := hms.ClientAcquire(c, context.Background())
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

	hms.ClientRelease(c, cn)
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
	srv := hmstest.Start(t, hmstest.Hive23)
	c := mustNew(t, srv.URI())

	v, err := c.ServerVersion(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, v.Major)
	assert.Equal(t, 3, v.Minor)
}

func TestServerVersion_NeitherConfigValueSet(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI())

	// Clear the config values Start seeded, simulating a server that
	// reports neither.
	srv.Store().Config["hive.metastore.version"] = ""
	srv.Store().Config["metastore.version"] = ""

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
