package hms_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hms "github.com/slachiewicz/hms-client-go"
	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
	"github.com/slachiewicz/hms-client-go/internal/hmstest"
)

// TestHA_FailoverToSecondEndpoint covers SPEC §4.2 points 2 and 3: an
// idempotent RPC (get_all_databases) that fails after being sent retries on
// the next endpoint and succeeds there.
func TestHA_FailoverToSecondEndpoint(t *testing.T) {
	t.Parallel()
	srv1 := hmstest.Start(t, hmstest.Hive40, hmstest.WithFailNext(1))
	srv2 := hmstest.Start(t, hmstest.Hive40)

	c, err := hms.New(context.Background(), srv1.URI()+","+srv2.URI())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	_, err = c.GetAllDatabases(context.Background())
	require.NoError(t, err)
	assert.Contains(t, srv2.Calls(), "get_all_databases")
}

// TestHA_NewSkipsStoppedFirstEndpoint covers SPEC §4.2 point 1: New tries
// endpoints in cluster order and stays on the first one that answers, so a
// dead first endpoint does not fail New outright.
func TestHA_NewSkipsStoppedFirstEndpoint(t *testing.T) {
	t.Parallel()
	srv1 := hmstest.Start(t, hmstest.Hive40)
	uri1 := srv1.URI()
	srv1.Stop()

	srv2 := hmstest.Start(t, hmstest.Hive40)

	c, err := hms.New(context.Background(), uri1+","+srv2.URI())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	_, err = c.GetAllDatabases(context.Background())
	require.NoError(t, err)
	assert.Contains(t, srv2.Calls(), "get_all_databases")
}

// TestHA_NewFailsWhenEveryEndpointIsDown covers SPEC §4.2: New returns
// ErrUnavailable when no endpoint in the list answers.
func TestHA_NewFailsWhenEveryEndpointIsDown(t *testing.T) {
	t.Parallel()
	srv1 := hmstest.Start(t, hmstest.Hive40)
	uri1 := srv1.URI()
	srv1.Stop()

	srv2 := hmstest.Start(t, hmstest.Hive40)
	uri2 := srv2.URI()
	srv2.Stop()

	_, err := hms.New(context.Background(), uri1+","+uri2)
	require.Error(t, err)
	assert.ErrorIs(t, err, hms.ErrUnavailable)
}

// TestHA_NonIdempotentOpDoesNotFailOver covers SPEC §4.2 point 3: a
// non-idempotent RPC (create_database) that fails after being sent must not
// be retried on another endpoint, since the server may already have
// applied it.
func TestHA_NonIdempotentOpDoesNotFailOver(t *testing.T) {
	t.Parallel()
	srv1 := hmstest.Start(t, hmstest.Hive40, hmstest.WithFailNext(1))
	srv2 := hmstest.Start(t, hmstest.Hive40)

	c, err := hms.New(context.Background(), srv1.URI()+","+srv2.URI(), hms.WithMaxRetries(3))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	err = c.CreateDatabase(context.Background(), &hms.Database{Name: "d"})
	require.Error(t, err)
	assert.ErrorIs(t, err, hms.ErrUnavailable)
	assert.NotContains(t, srv2.Calls(), "create_database")
}

// TestHA_MaxRetriesOfOneNeverTriesTheSecondEndpoint covers SPEC §4.2 point
// 3's retry budget: with WithMaxRetries(1), even an idempotent op gets only
// one attempt.
func TestHA_MaxRetriesOfOneNeverTriesTheSecondEndpoint(t *testing.T) {
	t.Parallel()
	srv1 := hmstest.Start(t, hmstest.Hive40, hmstest.WithFailNext(1))
	srv2 := hmstest.Start(t, hmstest.Hive40)

	c, err := hms.New(context.Background(), srv1.URI()+","+srv2.URI(), hms.WithMaxRetries(1))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	_, err = c.GetAllDatabases(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, hms.ErrUnavailable)
	assert.Empty(t, srv2.Calls())
}

// TestHA_RecoveryProbeReenablesCooledEndpoint covers SPEC §4.2 point 4: the
// background probe re-tests cooling endpoints and marks a genuinely healthy
// one usable again. srv1 is stopped for real and stays down for the whole
// test; endpoint 1 (srv2) is forced into cooldown even though it is
// healthy, so only the probe's own getStatus success can grow its pool's
// live count (probeCooling is the only path that dials a fresh conn and
// hands it to a pool outside of an actual RPC; natural cooldown expiry
// alone, or endpoint 0's own unrelated cooldown, never touches it), which
// is why the assertion checks the pool rather than Pick.
//
// MarkFailed draws its cooldown from a full-jitter [0, ceiling) window
// (ceiling starting at 1s), so a single call before the Eventually loop
// could let the window expire before the 20ms probe ticker ever fires,
// making the test flake. Calling MarkFailed again on every Eventually
// poll (also every 20ms) keeps re-arming that window -- and, since each
// consecutive failure without an intervening MarkHealthy doubles the
// ceiling, quickly grows it well past the polling interval -- so a
// cooling window is essentially always open by the time the probe ticks.
func TestHA_RecoveryProbeReenablesCooledEndpoint(t *testing.T) {
	t.Parallel()
	srv1 := hmstest.Start(t, hmstest.Hive40)
	uri1 := srv1.URI()
	srv1.Stop()

	srv2 := hmstest.Start(t, hmstest.Hive40)

	c, err := hms.New(context.Background(), uri1+","+srv2.URI(), hms.WithProbeIntervalForTest(20*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	before := hms.ClientLiveConns(c, 1)
	require.Equal(t, int32(1), before, "New must have dialed and pooled the one conn to srv2 (endpoint 1)")

	require.Eventually(t, func() bool {
		hms.ClientMarkFailed(c, 1)
		return hms.ClientLiveConns(c, 1) > before
	}, 500*time.Millisecond, 20*time.Millisecond, "recovery probe must dial and pool a fresh conn to the healthy endpoint")
}

// TestHA_FailoverToSecondEndpoint_GetTable covers fix round 1, finding 1:
// GetTable's op is now the wire name "get_table_req", read through
// (*Client).read (idempotent), not (*Client).call, so a failure after the
// RPC was sent still fails over to the next endpoint -- the same guarantee
// TestHA_FailoverToSecondEndpoint already covers for get_all_databases, but
// exercised on a table read specifically, since that is exactly the bug
// fix round 1 found (table.go's op strings used to be the Go method names
// "GetTable" etc., which never matched the old "get_" string-prefix rule).
// The table is seeded directly into both servers' stores (bypassing
// CreateTable, a non-idempotent RPC that would itself consume srv1's
// WithFailNext budget), since either server may end up answering the read.
func TestHA_FailoverToSecondEndpoint_GetTable(t *testing.T) {
	t.Parallel()
	srv1 := hmstest.Start(t, hmstest.Hive40, hmstest.WithFailNext(1))
	srv2 := hmstest.Start(t, hmstest.Hive40)

	cat := "hive"
	tbl := &hive_metastore.Table{DbName: "db", TableName: "t", Owner: "me", CatName: &cat}
	srv1.Store().Tables["hive.db.t"] = tbl
	srv2.Store().Tables["hive.db.t"] = tbl

	c, err := hms.New(context.Background(), srv1.URI()+","+srv2.URI())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	got, err := c.GetTable(context.Background(), "db", "t")
	require.NoError(t, err)
	assert.Equal(t, "t", got.TableName)
	assert.Contains(t, srv2.Calls(), "get_table_req")
}

// TestHA_NonIdempotentDialFailureFailsOver covers fix round 1's Step 3: a
// non-idempotent op's dial/acquire failure (the request was never sent) is
// still retried on another endpoint, unlike a failure after the RPC
// started (TestHA_NonIdempotentOpDoesNotFailOver). New already pools one
// live conn to srv1; that conn is taken out of idle (without being
// released) so CreateDatabase's own acquire has to dial a fresh
// connection rather than reuse the already-established one -- otherwise
// stopping srv1 would only fail the RPC after it was sent, which is
// already covered by TestHA_NonIdempotentOpDoesNotFailOver, not the dial
// itself.
func TestHA_NonIdempotentDialFailureFailsOver(t *testing.T) {
	t.Parallel()
	srv1 := hmstest.Start(t, hmstest.Hive40)
	srv2 := hmstest.Start(t, hmstest.Hive40)

	c, err := hms.New(context.Background(), srv1.URI()+","+srv2.URI())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	stale, err := hms.ClientAcquire(c, context.Background(), 0)
	require.NoError(t, err)
	t.Cleanup(func() { hms.ClientRelease(c, 0, stale) })

	srv1.Stop()

	err = c.CreateDatabase(context.Background(), &hms.Database{Name: "d"})
	require.NoError(t, err)
	assert.Contains(t, srv2.Calls(), "create_database")
}

// TestHA_CancelledCallerWaitingInAcquireDoesNotMarkEndpointFailed covers
// fix round 1, finding 2: acquire can return the caller's own ctx
// cancellation (or errClosed) while blocked waiting for a pooled conn; that
// is not evidence the endpoint is unhealthy, so it must not cost the
// endpoint a MarkFailed. WithPoolSize(1) and a conn held on loan (mirroring
// a call whose fn is in flight) forces a second call's acquire to actually
// block; cancelling its ctx must return promptly without touching the
// cluster's view of the endpoint.
func TestHA_CancelledCallerWaitingInAcquireDoesNotMarkEndpointFailed(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)

	c, err := hms.New(context.Background(), srv.URI(), hms.WithPoolSize(1))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	// Hold the only pooled conn on loan, so a second call has nowhere
	// to come from but a release or ctx cancellation.
	cn, err := hms.ClientAcquire(c, context.Background(), 0)
	require.NoError(t, err)
	t.Cleanup(func() { hms.ClientRelease(c, 0, cn) })

	ctx, cancel := context.WithCancel(context.Background())
	waiterErr := make(chan error, 1)
	go func() {
		_, err := c.GetAllDatabases(ctx)
		waiterErr <- err
	}()

	// Give the waiter a moment to actually park in acquire's blocking
	// select before cancelling, so this exercises the ctx.Done() path
	// rather than a fast-path check before it ever blocked.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-waiterErr:
		require.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled) || errors.Is(err, hms.ErrUnavailable))
	case <-time.After(2 * time.Second):
		t.Fatal("call did not return promptly after ctx was cancelled")
	}

	idx, ok := hms.ClientPick(c)
	assert.True(t, ok)
	assert.Equal(t, 0, idx, "a caller's own ctx cancellation must not cool down the endpoint it was waiting on")
}
