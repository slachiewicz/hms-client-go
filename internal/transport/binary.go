package transport

import (
	"context"
	"net"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

const bufferSize = 8192

// BinaryConfig configures a binary Thrift-over-TCP connection.
type BinaryConfig struct {
	// Timeout bounds both connect and per-call socket I/O.
	Timeout time.Duration
	// PlainUser and PlainPassword select SASL PLAIN authentication when
	// PlainUser is non-empty; when empty the connection uses NOSASL. See
	// DialBinary.
	PlainUser     string
	PlainPassword string
}

// Conn is an open connection to a metastore, ready to bind to a generated
// Thrift client.
type Conn struct {
	// Client issues RPCs over the connection.
	Client thrift.TClient
	// Close releases the underlying transport.
	Close func() error
}

// deadlineShield wraps a net.Conn and turns its deadline setters into no-ops.
//
// thrift's TSocket.Read/Write (lib/go/thrift v0.24.0, socket.go pushDeadline)
// call SetDeadline/SetReadDeadline/SetWriteDeadline on every I/O operation,
// recomputing the deadline from TConfiguration.SocketTimeout (or clearing it
// when SocketTimeout is 0). Left unchecked, that overwrites the ctx-derived
// deadline ContextClient sets on the same conn before delegating the call,
// so the total-call deadline would only survive until the first Read/Write.
// TSocket is given a deadlineShield instead of the raw conn so its deadline
// calls have no effect; ContextClient holds the real conn and is the sole
// owner of its read/write deadlines.
type deadlineShield struct{ net.Conn }

// SetDeadline is a no-op: deadlines are owned by ContextClient, not TSocket.
func (deadlineShield) SetDeadline(time.Time) error { return nil }

// SetReadDeadline is a no-op: deadlines are owned by ContextClient, not TSocket.
func (deadlineShield) SetReadDeadline(time.Time) error { return nil }

// SetWriteDeadline is a no-op: deadlines are owned by ContextClient, not TSocket.
func (deadlineShield) SetWriteDeadline(time.Time) error { return nil }

// DialBinary opens a binary Thrift-over-TCP connection to hostPort. The
// returned Conn's Client applies cfg.Timeout as a per-call fallback and
// binds each call's context deadline to the socket via ContextClient, which
// is the sole owner of the connection's read/write deadlines (see
// deadlineShield); TSocket is never allowed to set them.
//
// When cfg.PlainUser is non-empty, DialBinary performs a SASL PLAIN
// handshake (see NewSaslPlain) before wrapping the transport for buffered
// I/O; on handshake failure the raw connection is closed and the error is
// returned unwrapped.
func DialBinary(ctx context.Context, hostPort string, cfg BinaryConfig) (*Conn, error) {
	d := net.Dialer{Timeout: cfg.Timeout}
	raw, err := d.DialContext(ctx, "tcp", hostPort)
	if err != nil {
		return nil, err
	}
	// SocketTimeout is intentionally 0: TSocket must not manage deadlines
	// itself (see deadlineShield). ConnectTimeout still applies since Open()
	// is never called on this TSocket (it is constructed from an already
	// connected conn).
	tcfg := &thrift.TConfiguration{SocketTimeout: 0, ConnectTimeout: cfg.Timeout}
	var trans thrift.TTransport = thrift.NewTSocketFromConnConf(deadlineShield{raw}, tcfg)
	if cfg.PlainUser != "" {
		trans = NewSaslPlain(trans, cfg.PlainUser, cfg.PlainPassword)
		if err := trans.Open(); err != nil {
			_ = raw.Close()
			return nil, err
		}
	}
	trans = thrift.NewTBufferedTransport(trans, bufferSize)
	proto := thrift.NewTBinaryProtocolConf(trans, tcfg)
	std := thrift.NewTStandardClient(proto, proto)
	return &Conn{
		Client: NewContextClient(std, raw, cfg.Timeout),
		Close:  trans.Close,
	}, nil
}
