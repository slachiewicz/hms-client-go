package transport_test

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
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

// startSaslNegotiator listens on a loopback port and, for each accepted
// connection, plays the server side of the SASL PLAIN handshake: it reads a
// START then OK negotiation frame and replies with reply. It returns an error
// channel that must be drained by the caller to detect any server-side errors.
// It proves DialBinary actually drives NewSaslPlain's Open over the wire,
// rather than exercising saslPlain in isolation as sasl_test.go does.
func startSaslNegotiator(t *testing.T, replyStatus byte, replyPayload []byte) (string, <-chan error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	errs := make(chan error, 1)

	go func() {
		defer close(errs)
		conn, err := ln.Accept()
		if err != nil {
			errs <- err
			return
		}
		defer func() { _ = conn.Close() }()

		for range 2 {
			var hdr [5]byte
			if _, err := io.ReadFull(conn, hdr[:]); err != nil {
				errs <- err
				return
			}
			n := binary.BigEndian.Uint32(hdr[1:])
			if _, err := io.ReadFull(conn, make([]byte, n)); err != nil {
				errs <- err
				return
			}
		}

		n := len(replyPayload)
		if n < 0 || n > 64<<20 {
			errs <- errors.New("reply payload too large")
			return
		}
		out := make([]byte, 5+n)
		out[0] = replyStatus
		binary.BigEndian.PutUint32(out[1:5], uint32(n))
		copy(out[5:], replyPayload)
		if _, err := conn.Write(out); err != nil {
			errs <- err
			return
		}
	}()

	return ln.Addr().String(), errs
}

// TestDialBinary_SaslPlainHandshakeSucceeds proves that setting
// BinaryConfig.PlainUser drives DialBinary to perform the SASL PLAIN
// handshake over the real TCP connection before returning.
func TestDialBinary_SaslPlainHandshakeSucceeds(t *testing.T) {
	t.Parallel()
	addr, errs := startSaslNegotiator(t, 5, nil) // saslComplete

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := transport.DialBinary(ctx, addr, transport.BinaryConfig{
		Timeout:       5 * time.Second,
		PlainUser:     "alice",
		PlainPassword: "s3cret",
	})
	require.NoError(t, err)
	assert.NoError(t, conn.Close())
	require.NoError(t, <-errs)
}

// TestDialBinary_SaslPlainRejectedFailsDial proves a SASL rejection during
// DialBinary surfaces as a dial error rather than being deferred to the
// first RPC.
func TestDialBinary_SaslPlainRejectedFailsDial(t *testing.T) {
	t.Parallel()
	addr, errs := startSaslNegotiator(t, 3, []byte("denied")) // saslBad

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := transport.DialBinary(ctx, addr, transport.BinaryConfig{
		Timeout:       5 * time.Second,
		PlainUser:     "alice",
		PlainPassword: "wrong",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "denied")
	require.NoError(t, <-errs)
}

// startSilentServer listens on a loopback port and accepts connections but
// never reads or writes anything on them, so a SASL PLAIN handshake against
// it blocks forever on recvNegotiate's read of the server's reply. It is
// used to prove DialBinary's handshake actually respects ctx/Timeout,
// covering the fix for the handshake previously running on the raw conn
// with no deadline at all (SocketTimeout is 0 and deadlineShield no-ops)
// before ContextClient exists to own it.
func startSilentServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		t.Cleanup(func() { _ = conn.Close() })
	}()

	return ln.Addr().String()
}

// TestDialBinary_SaslHandshakeRespectsContextDeadline proves a ctx deadline
// bounds the SASL PLAIN handshake itself, not just later RPCs.
func TestDialBinary_SaslHandshakeRespectsContextDeadline(t *testing.T) {
	t.Parallel()
	addr := startSilentServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := transport.DialBinary(ctx, addr, transport.BinaryConfig{
		Timeout:       5 * time.Second,
		PlainUser:     "alice",
		PlainPassword: "s3cret",
	})
	require.Error(t, err)
	assert.Less(t, time.Since(start), time.Second)

	var netErr net.Error
	isTimeout := errors.As(err, &netErr) && netErr.Timeout()
	assert.True(t, errors.Is(err, context.DeadlineExceeded) || isTimeout,
		"want error wrapping context.DeadlineExceeded or a net timeout, got %v", err)
}

// TestDialBinary_SaslHandshakeRespectsTimeoutFallback is
// TestDialBinary_SaslHandshakeRespectsContextDeadline's counterpart for
// BinaryConfig.Timeout, when ctx carries no deadline of its own.
func TestDialBinary_SaslHandshakeRespectsTimeoutFallback(t *testing.T) {
	t.Parallel()
	addr := startSilentServer(t)

	start := time.Now()
	_, err := transport.DialBinary(context.Background(), addr, transport.BinaryConfig{
		Timeout:       100 * time.Millisecond,
		PlainUser:     "alice",
		PlainPassword: "s3cret",
	})
	require.Error(t, err)
	assert.Less(t, time.Since(start), time.Second)

	var netErr net.Error
	isTimeout := errors.As(err, &netErr) && netErr.Timeout()
	assert.True(t, errors.Is(err, context.DeadlineExceeded) || isTimeout,
		"want error wrapping context.DeadlineExceeded or a net timeout, got %v", err)
}
