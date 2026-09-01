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
// overriding only the method under test.
type fbHandler struct{ fb303.FacebookService }

func (fbHandler) GetStatus(_ context.Context) (fb303.FbStatus, error) {
	return fb303.FbStatus_ALIVE, nil
}

func startFB303Server(t *testing.T) string {
	t.Helper()
	proc := fb303.NewFacebookServiceProcessor(&fbHandler{})
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
	addr := startFB303Server(t)

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
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := transport.DialBinary(ctx, "127.0.0.1:1", transport.BinaryConfig{})
	require.Error(t, err)
}
