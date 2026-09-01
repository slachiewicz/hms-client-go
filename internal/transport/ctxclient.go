package transport

import (
	"context"
	"net"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

// ContextClient is a thrift.TClient that binds each call's context to the
// underlying net.Conn: the deadline is copied to the socket and cancellation
// closes the in-flight read/write by moving the deadline to now.
type ContextClient struct {
	inner   thrift.TClient
	conn    net.Conn
	timeout time.Duration
}

// NewContextClient wraps inner so that each Call's context deadline (or, if
// the context carries none, the fallback timeout) is applied to conn, and
// context cancellation unblocks any in-flight read or write on conn.
func NewContextClient(inner thrift.TClient, conn net.Conn, timeout time.Duration) *ContextClient {
	return &ContextClient{inner: inner, conn: conn, timeout: timeout}
}

// Call binds ctx to the connection's deadline for the duration of the call
// and delegates to the inner client.
func (c *ContextClient) Call(ctx context.Context, method string, args, result thrift.TStruct) (thrift.ResponseMeta, error) {
	deadline, ok := ctx.Deadline()
	if !ok && c.timeout > 0 {
		deadline = time.Now().Add(c.timeout)
	}
	if !deadline.IsZero() {
		_ = c.conn.SetDeadline(deadline)
	} else {
		_ = c.conn.SetDeadline(time.Time{})
	}
	stop := context.AfterFunc(ctx, func() { _ = c.conn.SetDeadline(time.Now()) })
	defer stop()

	meta, err := c.inner.Call(ctx, method, args, result)
	if err != nil && ctx.Err() != nil {
		// Report the context error so callers see Canceled/DeadlineExceeded.
		return meta, ctx.Err()
	}
	return meta, err
}
