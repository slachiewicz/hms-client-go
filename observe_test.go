package hms_test

import (
	"bytes"
	"context"
	"log/slog"
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
// same failover scenario as TestObserver_Failover_OnePerAttempt.
func TestLogger_FailoverLogsEndpointMarkedFailed(t *testing.T) {
	t.Parallel()
	srv1 := hmstest.Start(t, hmstest.Hive40, hmstest.WithFailNext(1))
	srv2 := hmstest.Start(t, hmstest.Hive40)

	buf := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(buf, nil))

	c := mustNew(t, srv1.URI()+","+srv2.URI(), hms.WithLogger(logger))

	_, err := c.GetAllDatabases(context.Background())
	require.NoError(t, err)

	assert.Contains(t, buf.String(), "endpoint marked failed")
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
