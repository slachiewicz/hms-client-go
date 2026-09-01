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
	// both are non-empty. Ignored until SASL support lands; when unset the
	// connection uses NOSASL. See DialBinary.
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

// DialBinary opens a binary Thrift-over-TCP connection to hostPort. The
// returned Conn's Client applies cfg.Timeout as a per-call fallback and
// binds each call's context deadline to the socket via ContextClient.
//
// PlainUser is currently ignored; Task 4 adds SASL PLAIN support keyed on
// cfg.PlainUser being non-empty.
func DialBinary(ctx context.Context, hostPort string, cfg BinaryConfig) (*Conn, error) {
	d := net.Dialer{Timeout: cfg.Timeout}
	raw, err := d.DialContext(ctx, "tcp", hostPort)
	if err != nil {
		return nil, err
	}
	tcfg := &thrift.TConfiguration{SocketTimeout: cfg.Timeout, ConnectTimeout: cfg.Timeout}
	var trans thrift.TTransport = thrift.NewTSocketFromConnConf(raw, tcfg)
	// Task 4 inserts SASL PLAIN here when cfg.PlainUser != "".
	trans = thrift.NewTBufferedTransport(trans, bufferSize)
	proto := thrift.NewTBinaryProtocolConf(trans, tcfg)
	std := thrift.NewTStandardClient(proto, proto)
	return &Conn{
		Client: NewContextClient(std, raw, cfg.Timeout),
		Close:  trans.Close,
	}, nil
}
