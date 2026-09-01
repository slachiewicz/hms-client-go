package transport_test

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/hms-client-go/gen/fb303"
	"github.com/slachiewicz/hms-client-go/internal/transport"
)

// fbHandler implements fb303.FacebookService by embedding the interface and
// overriding only the method under test. sleep, when non-zero, delays
// GetStatus so tests can exercise ctx/timeout expiry against a slow RPC.
type fbHandler struct {
	fb303.FacebookService
	sleep time.Duration
}

func (h fbHandler) GetStatus(_ context.Context) (fb303.FbStatus, error) {
	if h.sleep > 0 {
		time.Sleep(h.sleep)
	}
	return fb303.FbStatus_ALIVE, nil
}

func startFB303Server(t *testing.T, sleep time.Duration) string {
	t.Helper()
	proc := fb303.NewFacebookServiceProcessor(&fbHandler{sleep: sleep})
	serverSocket, err := thrift.NewTServerSocket("127.0.0.1:0")
	require.NoError(t, err)
	require.NoError(t, serverSocket.Listen())
	server := thrift.NewTSimpleServer4(
		proc,
		serverSocket,
		thrift.NewTBufferedTransportFactory(8192),
		thrift.NewTBinaryProtocolFactoryConf(nil),
	)
	go func() { _ = server.Serve() }()
	t.Cleanup(func() { _ = server.Stop() })
	return serverSocket.Addr().String()
}

func TestDialBinary_ServesFacebookService(t *testing.T) {
	t.Parallel()
	addr := startFB303Server(t, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := transport.DialBinary(ctx, addr, transport.BinaryConfig{Timeout: 5 * time.Second})
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	status, err := fb303.NewFacebookServiceClient(conn.Client).GetStatus(ctx)
	require.NoError(t, err)
	assert.Equal(t, fb303.FbStatus_ALIVE, status)
}

func TestDialBinary_ConnectionRefused(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := transport.DialBinary(ctx, "127.0.0.1:1", transport.BinaryConfig{Timeout: 2 * time.Second})
	require.Error(t, err)
	assert.True(t, errors.Is(err, syscall.ECONNREFUSED), "want error wrapping ECONNREFUSED, got %v", err)
}

func TestDialBinary_ContextAlreadyCancelled(t *testing.T) {
	t.Parallel()
	// Dial a live, listening server: if DialBinary ignored ctx, the dial
	// would succeed, so this only passes when ctx is actually honored.
	addr := startFB303Server(t, 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := transport.DialBinary(ctx, addr, transport.BinaryConfig{})
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled), "want error wrapping context.Canceled, got %v", err)
}

// TestDialBinary_CtxDeadlineWinsOverTimeout proves that ContextClient's
// ctx-derived deadline actually governs the call end-to-end through
// DialBinary's real TSocket/TBufferedTransport/TBinaryProtocol stack. Before
// the deadlineShield fix, TSocket.Read/Write recomputed the conn's deadline
// from TConfiguration.SocketTimeout on every I/O operation, silently
// discarding the deadline ContextClient had set from ctx.
func TestDialBinary_CtxDeadlineWinsOverTimeout(t *testing.T) {
	t.Parallel()
	addr := startFB303Server(t, 300*time.Millisecond)

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dialCancel()
	conn, err := transport.DialBinary(dialCtx, addr, transport.BinaryConfig{Timeout: 10 * time.Second})
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	callCtx, callCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer callCancel()
	start := time.Now()
	_, err = fb303.NewFacebookServiceClient(conn.Client).GetStatus(callCtx)
	require.Error(t, err)
	assert.Less(t, time.Since(start), time.Second)
	assert.True(t, errors.Is(err, context.DeadlineExceeded), "want error wrapping context.DeadlineExceeded, got %v", err)
}

// TestDialBinary_FallbackTimeoutFires proves BinaryConfig.Timeout still
// bounds a call end-to-end through the real TSocket stack when ctx carries
// no deadline of its own.
func TestDialBinary_FallbackTimeoutFires(t *testing.T) {
	t.Parallel()
	addr := startFB303Server(t, 300*time.Millisecond)

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dialCancel()
	conn, err := transport.DialBinary(dialCtx, addr, transport.BinaryConfig{Timeout: 50 * time.Millisecond})
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	start := time.Now()
	_, err = fb303.NewFacebookServiceClient(conn.Client).GetStatus(context.Background())
	require.Error(t, err)
	assert.Less(t, time.Since(start), time.Second)
}
