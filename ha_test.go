package hms_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hms "github.com/slachiewicz/hms-client-go"
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

	hms.ClientMarkFailed(c, 1)

	require.Eventually(t, func() bool {
		return hms.ClientLiveConns(c, 1) > before
	}, 500*time.Millisecond, 20*time.Millisecond, "recovery probe must dial and pool a fresh conn to the healthy endpoint")
}
