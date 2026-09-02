package hms_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hms "github.com/slachiewicz/hms-client-go"
	"github.com/slachiewicz/hms-client-go/internal/hmstest"
)

// syncBuffer is a bytes.Buffer guarded by a mutex, so it is safe for a
// *slog.Logger to write into concurrently -- the background recovery-probe
// goroutine (client.go's recoveryProbe) can log at the same time a test's
// own goroutine issues an RPC.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestObserver_SingleCall_OneAttempt covers SPEC §5.10: a successful RPC
// with no retries invokes the WithRPCObserver function exactly once, with
// Attempt 1, the issuing endpoint, and a nil Err.
func TestObserver_SingleCall_OneAttempt(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)

	var mu sync.Mutex
	var got []hms.RPCInfo
	c := mustNew(t, srv.URI(), hms.WithRPCObserver(func(info hms.RPCInfo) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, info)
	}))

	_, err := c.GetAllDatabases(context.Background())
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, got, 1)
	assert.Equal(t, "get_all_databases", got[0].Method)
	assert.Equal(t, srv.Addr(), got[0].Endpoint)
	assert.Equal(t, 1, got[0].Attempt)
	assert.NoError(t, got[0].Err)
	assert.GreaterOrEqual(t, got[0].Duration, time.Duration(0))
}

// TestObserver_Failover_OnePerAttempt covers SPEC §5.10's retried-call case,
// reusing TestHA_FailoverToSecondEndpoint's scenario (ha_test.go): an
// idempotent RPC that fails after being sent on srv1 (WithFailNext(1))
// retries on srv2 and succeeds there. The observer must see exactly two
// RPCInfo values: attempt 1 against srv1 with a non-nil Err, attempt 2
// against srv2 with a nil Err.
func TestObserver_Failover_OnePerAttempt(t *testing.T) {
	t.Parallel()
	srv1 := hmstest.Start(t, hmstest.Hive40, hmstest.WithFailNext(1))
	srv2 := hmstest.Start(t, hmstest.Hive40)

	var mu sync.Mutex
	var got []hms.RPCInfo
	c := mustNew(t, srv1.URI()+","+srv2.URI(), hms.WithRPCObserver(func(info hms.RPCInfo) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, info)
	}))

	_, err := c.GetAllDatabases(context.Background())
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, got, 2)

	assert.Equal(t, "get_all_databases", got[0].Method)
	assert.Equal(t, srv1.Addr(), got[0].Endpoint)
	assert.Equal(t, 1, got[0].Attempt)
	assert.Error(t, got[0].Err)

	assert.Equal(t, "get_all_databases", got[1].Method)
	assert.Equal(t, srv2.Addr(), got[1].Endpoint)
	assert.Equal(t, 2, got[1].Attempt)
	assert.NoError(t, got[1].Err)
}

// TestObserver_AcquireFailure_ObservedPerAttempt covers the other half of
// "once per attempt" (SPEC §5.10): an attempt that could not even dial the
// endpoint is reported too, with the dial failure in Err, so an observer
// watching for an endpoint going down does not simply stop hearing about
// it.
func TestObserver_AcquireFailure_ObservedPerAttempt(t *testing.T) {
	t.Parallel()
	srv1 := hmstest.Start(t, hmstest.Hive40)
	srv2 := hmstest.Start(t, hmstest.Hive40)

	var mu sync.Mutex
	var got []hms.RPCInfo
	c := mustNew(t, srv1.URI()+","+srv2.URI(),
		hms.WithMaxRetries(2),
		hms.WithRPCObserver(func(info hms.RPCInfo) {
			mu.Lock()
			defer mu.Unlock()
			got = append(got, info)
		}))

	// Both endpoints are gone: the first attempt fails on the conn New
	// pooled, and the second -- on the other endpoint, whose pool is
	// empty -- never gets a connection at all.
	srv1.Stop()
	srv2.Stop()

	_, err := c.GetAllDatabases(context.Background())
	require.Error(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, got, 2, "one RPCInfo per attempt, including the one that never dialed")
	for i, info := range got {
		assert.Equal(t, "get_all_databases", info.Method, "attempt %d", i)
		assert.Equal(t, i+1, info.Attempt)
		assert.Error(t, info.Err, "an attempt that never got a connection reports why")
	}
	assert.NotEqual(t, got[0].Endpoint, got[1].Endpoint, "the second attempt moved to the other endpoint")
}

// TestObserver_PanicRecovered covers SPEC §5.10: a panic escaping the
// WithRPCObserver function must not propagate to the RPC's caller -- the
// call still succeeds -- and is instead recovered and logged through
// WithLogger's logger.
func TestObserver_PanicRecovered(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)

	buf := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(buf, nil))

	c := mustNew(t, srv.URI(),
		hms.WithLogger(logger),
		hms.WithRPCObserver(func(hms.RPCInfo) { panic("boom") }),
	)

	_, err := c.GetAllDatabases(context.Background())
	require.NoError(t, err, "a panicking observer must not fail the RPC it observed")

	assert.Contains(t, buf.String(), "observer panicked")
}

// TestLogger_FailoverLogsEndpointMarkedFailed covers SPEC §5.10: WithLogger
// logs a failover transition (MarkFailed) at slog.LevelInfo, reusing the
// same failover scenario as TestObserver_Failover_OnePerAttempt. Exactly
// one "endpoint marked failed" line is expected -- the scenario drives
// exactly one failed attempt against srv1 -- covering fix round 1's
// Cluster.MarkFailed/MarkHealthy bool: without it, do's success path would
// also log "endpoint marked healthy" once per successful call, but that is
// covered separately by TestLogger_RepeatedSuccessLogsNoHealthyTransition.
func TestLogger_FailoverLogsEndpointMarkedFailed(t *testing.T) {
	t.Parallel()
	srv1 := hmstest.Start(t, hmstest.Hive40, hmstest.WithFailNext(1))
	srv2 := hmstest.Start(t, hmstest.Hive40)

	buf := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(buf, nil))

	c := mustNew(t, srv1.URI()+","+srv2.URI(), hms.WithLogger(logger))

	_, err := c.GetAllDatabases(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 1, strings.Count(buf.String(), "endpoint marked failed"),
		"exactly one endpoint failed transition must be logged")
}

// TestLogger_RepeatedSuccessLogsNoHealthyTransition covers fix round 1's
// critical finding: markHealthy must log "endpoint marked healthy" only on
// a real failed-to-healthy transition (Cluster.MarkHealthy's bool), not on
// every successful call. Five successful calls against an endpoint that
// was never cooling must produce zero such lines.
func TestLogger_RepeatedSuccessLogsNoHealthyTransition(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)

	buf := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(buf, nil))

	c := mustNew(t, srv.URI(), hms.WithLogger(logger))

	for i := 0; i < 5; i++ {
		_, err := c.GetAllDatabases(context.Background())
		require.NoError(t, err)
	}

	assert.NotContains(t, buf.String(), "endpoint marked healthy")
}

// TestLogger_ProbeRecoveryLogsEndpointMarkedHealthyOnce covers fix round 1:
// once an endpoint's cooldown is cleared by a real recovery -- here, the
// background probe (probeCooling) confirming a forced-cooling endpoint is
// actually reachable -- exactly one "endpoint marked healthy" line is
// logged for that transition. hms.ClientMarkFailed forces cooldown
// directly on the cluster (bypassing Client.markFailed, so it logs
// nothing itself) exactly once, unlike
// TestHA_RecoveryProbeReenablesCooledEndpoint's (ha_test.go) repeated
// re-arming: re-arming here would risk the probe recovering and
// re-cooling the endpoint more than once, logging more than one
// transition. A single MarkFailed's cooldown ceiling is minBackoff (1s),
// so a 2s wait comfortably covers the full jittered [0, 1s) window plus
// at least one 20ms probe tick.
func TestLogger_ProbeRecoveryLogsEndpointMarkedHealthyOnce(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)

	buf := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(buf, nil))

	c := mustNew(t, srv.URI(), hms.WithLogger(logger), hms.WithProbeIntervalForTest(20*time.Millisecond))
	hms.ClientMarkFailed(c, 0)

	require.Eventually(t, func() bool {
		return strings.Contains(buf.String(), "endpoint marked healthy")
	}, 2*time.Second, 20*time.Millisecond, "recovery probe must eventually mark the endpoint healthy again")

	assert.Equal(t, 1, strings.Count(buf.String(), "endpoint marked healthy"),
		"exactly one endpoint healthy transition must be logged")
}

// TestWithLogger_NilIsSafe covers SPEC §5.10: passing a nil *slog.Logger to
// WithLogger must not panic -- it substitutes the same discarding default
// as never calling WithLogger at all.
func TestWithLogger_NilIsSafe(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)

	c := mustNew(t, srv.URI(), hms.WithLogger(nil))

	_, err := c.GetAllDatabases(context.Background())
	require.NoError(t, err)
}
