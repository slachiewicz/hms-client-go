package transport_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/hms-client-go/internal/transport"
)

func TestContextClient_DeadlineFromContext(t *testing.T) {
	t.Parallel()
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	inner := &blockingReadClient{conn: client}
	cc := transport.NewContextClient(inner, client, time.Hour)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := cc.Call(ctx, "get_table", nil, nil)
	require.Error(t, err)
	assert.Less(t, time.Since(start), time.Second, "deadline must come from ctx, not the 1h fallback")
	var ne net.Error
	isTimeout := errors.As(err, &ne) && ne.Timeout()
	assert.True(t, errors.Is(err, context.DeadlineExceeded) || isTimeout,
		"error must be context.DeadlineExceeded or a net.Error with Timeout()==true, got %v", err)
}

func TestContextClient_CancelClosesInFlightRead(t *testing.T) {
	t.Parallel()
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	inner := &blockingReadClient{conn: client}
	cc := transport.NewContextClient(inner, client, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(30 * time.Millisecond); cancel() }()
	start := time.Now()
	_, err := cc.Call(ctx, "get_table", nil, nil)
	require.Error(t, err)
	assert.Less(t, time.Since(start), time.Second)
}

func TestContextClient_FallbackTimeoutWhenNoDeadline(t *testing.T) {
	t.Parallel()
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	inner := &blockingReadClient{conn: client}
	cc := transport.NewContextClient(inner, client, 40*time.Millisecond)
	start := time.Now()
	_, err := cc.Call(context.Background(), "get_table", nil, nil)
	require.Error(t, err)
	assert.Less(t, time.Since(start), time.Second)
}

type blockingReadClient struct{ conn net.Conn }

func (b *blockingReadClient) Call(_ context.Context, _ string, _, _ thrift.TStruct) (thrift.ResponseMeta, error) {
	buf := make([]byte, 1)
	_, err := b.conn.Read(buf)
	return thrift.ResponseMeta{}, err
}
