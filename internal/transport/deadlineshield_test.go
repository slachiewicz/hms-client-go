package transport

// This file is package transport (white-box), not transport_test, because
// deadlineShield is deliberately unexported: it is an internal safety valve
// for DialBinary, not part of the public API, so it is not worth exposing
// solely to satisfy the repo's usual black-box test convention.

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeadlineShield_SetDeadlineCallsAreNoOps proves the three deadline
// setters both return nil and never reach the wrapped conn: on a net.Pipe,
// a real SetDeadline in the past would make the peer's Read fail
// immediately, so an unaffected, still-blocked Read after the shield calls
// demonstrates the underlying conn's deadline was never touched.
func TestDeadlineShield_SetDeadlineCallsAreNoOps(t *testing.T) {
	t.Parallel()
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	shield := deadlineShield{client}
	past := time.Now().Add(-time.Hour)
	assert.NoError(t, shield.SetDeadline(past))
	assert.NoError(t, shield.SetReadDeadline(past))
	assert.NoError(t, shield.SetWriteDeadline(past))

	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 1)
		_, err := client.Read(buf)
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("read returned early (%v); deadlineShield leaked a deadline to the underlying conn", err)
	case <-time.After(100 * time.Millisecond):
		// Still blocked, as expected: the shield's calls did not reach client.
	}

	require.NoError(t, server.SetWriteDeadline(time.Now().Add(time.Second)))
	_, err := server.Write([]byte{1})
	require.NoError(t, err)
	require.NoError(t, <-done)
}
