package transport_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"errors"
	"io"
	"math/big"
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

// TestDialBinary_SaslHandshakeRespectsConnectTimeoutFallback is
// TestDialBinary_SaslHandshakeRespectsContextDeadline's counterpart for
// BinaryConfig.ConnectTimeout, when ctx carries no deadline of its own.
func TestDialBinary_SaslHandshakeRespectsConnectTimeoutFallback(t *testing.T) {
	t.Parallel()
	addr := startSilentServer(t)

	start := time.Now()
	_, err := transport.DialBinary(context.Background(), addr, transport.BinaryConfig{
		ConnectTimeout: 100 * time.Millisecond,
		PlainUser:      "alice",
		PlainPassword:  "s3cret",
	})
	require.Error(t, err)
	assert.Less(t, time.Since(start), time.Second)

	var netErr net.Error
	isTimeout := errors.As(err, &netErr) && netErr.Timeout()
	assert.True(t, errors.Is(err, context.DeadlineExceeded) || isTimeout,
		"want error wrapping context.DeadlineExceeded or a net timeout, got %v", err)
}

// generateTestCert returns a fresh self-signed ECDSA certificate valid for
// host (as an IP SAN) and the *x509.CertPool a client must trust to accept
// it, for the TLS tests below.
func generateTestCert(t *testing.T, host string) (tls.Certificate, *x509.CertPool) {
	t.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: host},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP(host)},
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	require.NoError(t, err)

	leaf, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	pool := x509.NewCertPool()
	pool.AddCert(leaf)

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv, Leaf: leaf}, pool
}

// startFB303TLSServer is startFB303Server's TLS counterpart: it serves the
// same fb303 handler over a tls.NewListener-wrapped TCP listener, so
// DialBinary's TLS path is proven against a real TLS handshake and a real
// fb303 round trip, not a mock. thrift's own thrift.TSSLServerSocket
// (lib/go/thrift v0.24.0, ssl_server_socket.go) is deliberately not used
// here: its interrupted field is read by Accept and written by Interrupt
// with no synchronization, a genuine data race in the vendored library
// that -race catches the moment a TLS-serving TSimpleServer is Stop()'d --
// this hand-rolled accept loop sidesteps it by closing the net.Listener
// directly (a documented-safe way to unblock a concurrent Accept) instead.
func startFB303TLSServer(t *testing.T, sleep time.Duration, cert tls.Certificate) string {
	t.Helper()
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	ln := tls.NewListener(raw, &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	})
	t.Cleanup(func() { _ = ln.Close() })

	proc := fb303.NewFacebookServiceProcessor(&fbHandler{sleep: sleep})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer func() { _ = conn.Close() }()
				trans := thrift.NewTBufferedTransport(thrift.NewTSocketFromConnConf(conn, nil), bufferSizeForTest)
				proto := thrift.NewTBinaryProtocolConf(trans, nil)
				for {
					ok, err := proc.Process(context.Background(), proto, proto)
					if err != nil || !ok {
						return
					}
				}
			}(conn)
		}
	}()
	return ln.Addr().String()
}

// bufferSizeForTest mirrors transport.bufferSize (unexported), so the
// hand-rolled TLS server above buffers the same as DialBinary's real
// client-side transport.
const bufferSizeForTest = 8192

// TestDialBinary_TLSRoundTrip proves BinaryConfig.TLS drives a real TLS
// handshake before the binary protocol layer, end to end through a real
// fb303 GetStatus call.
func TestDialBinary_TLSRoundTrip(t *testing.T) {
	t.Parallel()
	cert, pool := generateTestCert(t, "127.0.0.1")
	addr := startFB303TLSServer(t, 0, cert)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := transport.DialBinary(ctx, addr, transport.BinaryConfig{
		Timeout:        5 * time.Second,
		ConnectTimeout: 5 * time.Second,
		TLS:            &tls.Config{RootCAs: pool, ServerName: "127.0.0.1"},
	})
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	status, err := fb303.NewFacebookServiceClient(conn.Client).GetStatus(ctx)
	require.NoError(t, err)
	assert.Equal(t, fb303.FbStatus_ALIVE, status)
}

// TestDialBinary_TLSServerNameMismatch proves a ServerName that does not
// match the server's certificate fails the handshake promptly (well before
// ConnectTimeout would elapse), rather than DialBinary silently accepting
// an unverified connection or hanging.
func TestDialBinary_TLSServerNameMismatch(t *testing.T) {
	t.Parallel()
	cert, pool := generateTestCert(t, "127.0.0.1")
	addr := startFB303TLSServer(t, 0, cert)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	_, err := transport.DialBinary(ctx, addr, transport.BinaryConfig{
		Timeout:        5 * time.Second,
		ConnectTimeout: 5 * time.Second,
		TLS:            &tls.Config{RootCAs: pool, ServerName: "not-the-cert-name.invalid"},
	})
	require.Error(t, err)
	assert.Less(t, time.Since(start), time.Second)
}

// TestDialBinary_TLSHandshakeRespectsContextCancellation proves a ctx
// cancellation during the TLS handshake unblocks DialBinary promptly,
// against a server that accepts the TCP connection but never sends
// anything (so the handshake would otherwise block forever reading the
// server's TLS response).
func TestDialBinary_TLSHandshakeRespectsContextCancellation(t *testing.T) {
	t.Parallel()
	_, pool := generateTestCert(t, "127.0.0.1")
	addr := startSilentServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := transport.DialBinary(ctx, addr, transport.BinaryConfig{
		Timeout: 5 * time.Second,
		TLS:     &tls.Config{RootCAs: pool, ServerName: "127.0.0.1"},
	})
	require.Error(t, err)
	assert.Less(t, time.Since(start), time.Second)
}

// TestDialBinary_TLSHandshakeRespectsConnectTimeoutFallback proves
// ConnectTimeout alone (ctx carrying no deadline of its own) bounds a TLS
// handshake against a server that accepts the connection but never
// completes it.
func TestDialBinary_TLSHandshakeRespectsConnectTimeoutFallback(t *testing.T) {
	t.Parallel()
	_, pool := generateTestCert(t, "127.0.0.1")
	addr := startSilentServer(t)

	start := time.Now()
	_, err := transport.DialBinary(context.Background(), addr, transport.BinaryConfig{
		Timeout:        5 * time.Second,
		ConnectTimeout: 100 * time.Millisecond,
		TLS:            &tls.Config{RootCAs: pool, ServerName: "127.0.0.1"},
	})
	require.Error(t, err)
	assert.Less(t, time.Since(start), time.Second)
}
