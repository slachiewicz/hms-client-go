package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

const bufferSize = 8192

// BinaryConfig configures a binary Thrift-over-TCP connection.
type BinaryConfig struct {
	// Timeout bounds per-call socket I/O (applied by ContextClient as a
	// fallback when a call's context carries no deadline). It does not
	// bound dialing, the TLS handshake, or the SASL handshake; see
	// ConnectTimeout for those.
	Timeout time.Duration
	// ConnectTimeout bounds dialing (net.Dialer.Timeout), the TLS
	// handshake (when TLS is set), and the SASL handshake (when PlainUser
	// is set), as a fallback applied when ctx carries no deadline of its
	// own -- exactly as Timeout does for later per-call I/O. Zero means no
	// dial-side timeout beyond ctx.
	ConnectTimeout time.Duration
	// TLS, when non-nil, wraps the dialed socket in tls.Client(raw, TLS)
	// and completes its handshake before the SASL/binary protocol layers
	// are attached, for a server configured with metastore.use.SSL=true.
	// See DialBinary.
	TLS *tls.Config
	// PlainUser and PlainPassword select SASL PLAIN authentication when
	// PlainUser is non-empty; when empty the connection uses NOSASL. See
	// DialBinary.
	PlainUser     string
	PlainPassword string
	// Kerberos, when non-nil, selects SASL GSSAPI (Kerberos)
	// authentication, mutually exclusive with PlainUser. See DialBinary
	// and NewSaslGSSAPI.
	Kerberos *KerberosConfig
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
// deadlineShield); TSocket is never allowed to set them. ContextClient
// always holds the raw net.Conn, even when cfg.TLS wraps it: crypto/tls
// documents that (*tls.Conn).SetDeadline/SetReadDeadline/SetWriteDeadline
// simply delegate to the underlying conn's, so a deadline set on raw
// bounds the TLS conn's Read/Write too (its handshake state machine calls
// back into raw's Read/Write, which is what actually blocks); there is no
// TLS-specific deadline behavior ContextClient would otherwise need to
// reach through the TLS layer for.
//
// When cfg.TLS is non-nil, DialBinary wraps the dialed socket with
// tls.Client and completes its handshake, bound to ctx and cfg.ConnectTimeout
// (see the ConnectTimeout fallback described below), before the SASL/binary
// protocol layers are attached; on handshake failure the raw connection is
// closed and the error is returned unwrapped. The deadlineShield-wrapped
// TSocket is built over the TLS conn in that case, so cfg.PlainUser's SASL
// handshake and every later RPC run over the encrypted connection.
//
// When cfg.PlainUser is non-empty, DialBinary performs a SASL PLAIN
// handshake (see NewSaslPlain) before wrapping the transport for buffered
// I/O; on handshake failure the raw connection is closed and the error is
// returned unwrapped. When cfg.Kerberos is non-nil it performs a SASL
// GSSAPI handshake instead (see NewSaslGSSAPI), on the same terms.
// Configuring both is a caller error and fails before the socket is
// dialed, since a connection carries exactly one SASL identity.
//
// The Kerberos handshake's own KDC exchanges are the one piece of I/O the
// deadline below does not reach: they run against the KDC over gokrb5's
// own sockets, not the metastore connection, and are bounded by
// krb5.conf's kdc_timeout instead.
//
// The SASL handshake runs before ContextClient exists to own raw's
// deadlines (see deadlineShield), so DialBinary binds ctx to raw directly
// for the handshake's duration: it sets a deadline from ctx.Deadline()
// (falling back to time.Now().Add(cfg.ConnectTimeout) when
// cfg.ConnectTimeout > 0), and arranges for ctx cancellation to unblock any
// in-flight handshake I/O by moving that deadline to now, exactly as
// ContextClient.Call does for every later RPC. The deadline is cleared
// again once the handshake returns, so ContextClient starts from a clean
// slate.
func DialBinary(ctx context.Context, hostPort string, cfg BinaryConfig) (*Conn, error) {
	if cfg.PlainUser != "" && cfg.Kerberos != nil {
		return nil, errors.New("hms: WithPlainAuth and WithKerberos are mutually exclusive")
	}
	d := net.Dialer{Timeout: cfg.ConnectTimeout}
	raw, err := d.DialContext(ctx, "tcp", hostPort)
	if err != nil {
		return nil, err
	}

	conn := raw
	if cfg.TLS != nil {
		hctx := ctx
		if cfg.ConnectTimeout > 0 {
			var hcancel context.CancelFunc
			hctx, hcancel = context.WithTimeout(ctx, cfg.ConnectTimeout)
			defer hcancel()
		}
		tlsConn := tls.Client(raw, cfg.TLS)
		if err := tlsConn.HandshakeContext(hctx); err != nil {
			_ = raw.Close()
			return nil, err
		}
		conn = tlsConn
	}

	// SocketTimeout is intentionally 0: TSocket must not manage deadlines
	// itself (see deadlineShield). ConnectTimeout still applies since Open()
	// is never called on this TSocket (it is constructed from an already
	// connected conn).
	tcfg := &thrift.TConfiguration{SocketTimeout: 0, ConnectTimeout: cfg.ConnectTimeout}
	var trans thrift.TTransport = thrift.NewTSocketFromConnConf(deadlineShield{conn}, tcfg)

	var sasl SaslTransport
	switch {
	case cfg.Kerberos != nil:
		var err error
		if sasl, err = NewSaslGSSAPI(trans, hostPort, *cfg.Kerberos); err != nil {
			_ = raw.Close()
			return nil, err
		}
	case cfg.PlainUser != "":
		sasl = NewSaslPlain(trans, cfg.PlainUser, cfg.PlainPassword)
	}
	if sasl != nil {
		deadline, ok := ctx.Deadline()
		if !ok && cfg.ConnectTimeout > 0 {
			deadline = time.Now().Add(cfg.ConnectTimeout)
		}
		if !deadline.IsZero() {
			_ = raw.SetDeadline(deadline)
		}
		stop := context.AfterFunc(ctx, func() { _ = raw.SetDeadline(time.Now()) })
		err := sasl.OpenContext(ctx)
		stop()
		_ = raw.SetDeadline(time.Time{})
		if err != nil {
			_ = raw.Close()
			if ctx.Err() != nil {
				// Report the context error so callers see
				// Canceled/DeadlineExceeded rather than the net.Error
				// timeout the deadline flip above produced, mirroring
				// ContextClient.Call's identical override for every later
				// RPC.
				return nil, ctx.Err()
			}
			return nil, err
		}
		trans = sasl
	}
	trans = thrift.NewTBufferedTransport(trans, bufferSize)
	proto := thrift.NewTBinaryProtocolConf(trans, tcfg)
	std := thrift.NewTStandardClient(proto, proto)
	return &Conn{
		Client: NewContextClient(std, raw, cfg.Timeout),
		Close:  trans.Close,
	}, nil
}
