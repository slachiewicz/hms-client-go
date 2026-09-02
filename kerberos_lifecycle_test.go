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

// TestClose_WaitsForInFlightDialBeforeClosingSession covers the fix for a
// race between acquire's dial and Close's Kerberos session teardown: acquire
// used to read c.closed, release the lock, and only then dial (newConn),
// with nothing tracking that dial as in-flight; Close drained the pools and
// waited only for the recovery-probe goroutine before calling
// krbSession.Close() (gokrb5's Destroy, which mutates the session's
// Credentials unlocked). A dial that slipped past the closed check just
// before Close ran could still be inside DialBinary's GSSAPI handshake,
// reading that same session, when Close tore it down out from under it.
//
// hmstest has no SASL GSSAPI handshake to race against directly (see
// ConfigWantsSetUgi's doc comment), so this exercises the same acquire/Close
// interleaving the fix actually serializes -- inFlightDials.Add(1) under mu,
// gated on closed, and Close's inFlightDials.Wait() before krbSession.Close()
// -- using WithDialHookForTest to hold a real, acquire-triggered dial open
// for as long as the test needs, in place of a slow GSSAPI handshake.
func TestClose_WaitsForInFlightDialBeforeClosingSession(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c, err := hms.New(context.Background(), srv.URI(), hms.WithPoolSize(2))
	require.NoError(t, err)

	// Take New's own eagerly dialed conn out of the pool, so the next
	// acquire on this endpoint must dial a second one (live=1 < poolSize=2)
	// rather than just reuse it.
	cn0, err := hms.ClientAcquire(c, context.Background(), 0)
	require.NoError(t, err)

	releaseHook := make(chan struct{})
	hookEntered := make(chan struct{})
	hookCtx := hms.WithDialHookForTest(context.Background(), func() {
		close(hookEntered)
		<-releaseHook
	})

	dialErr := make(chan error, 1)
	go func() {
		cn1, err := hms.ClientAcquire(c, hookCtx, 0)
		if err == nil {
			hms.ClientRelease(c, 0, cn1)
		}
		dialErr <- err
	}()

	// Wait for the second dial to actually be in flight (registered in
	// inFlightDials, parked in the hook) before racing Close against it.
	select {
	case <-hookEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("dial hook never entered")
	}

	closeErr := make(chan error, 1)
	go func() {
		closeErr <- c.Close()
	}()

	// Close must not return while the dial it has to wait for is still
	// parked in the hook: that is exactly the window in which the old code
	// would have gone ahead and torn down the Kerberos session.
	select {
	case <-closeErr:
		t.Fatal("Close returned before the in-flight dial finished")
	case <-time.After(200 * time.Millisecond):
	}

	close(releaseHook)

	select {
	case err := <-closeErr:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return promptly after the in-flight dial finished")
	}

	select {
	case err := <-dialErr:
		// The dial itself completes normally even though the client was
		// closed while it was in flight (see acquire's and Close's doc
		// comments); ClientRelease's own closed check discards the conn it
		// returns instead of pooling it.
		assert.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("the in-flight dial's acquire call never returned")
	}

	hms.ClientRelease(c, 0, cn0)
	assert.Equal(t, int32(0), hms.ClientLiveConns(c, 0), "both conns must be accounted for once Close and both acquire calls have finished")
}
