package hmstest

import (
	"context"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
)

const bufferSize = 8192

// clampInt32 converts n to int32, clamping rather than wrapping if it is
// out of range.
func clampInt32(n int) int32 {
	if n > math.MaxInt32 {
		n = math.MaxInt32
	}
	if n < math.MinInt32 {
		n = math.MinInt32
	}
	return int32(n)
}

// config accumulates the effect of the Options passed to Start.
type config struct {
	without  []string
	failNext int
}

// Option configures a Server started by Start.
type Option func(*config)

// WithoutRPC deletes the named Thrift methods from the server's processor
// map, so a client calling them observes a TApplicationException with
// TypeId() == thrift.UNKNOWN_METHOD, as a real server missing that RPC
// would produce.
func WithoutRPC(names ...string) Option {
	return func(c *config) { c.without = append(c.without, names...) }
}

// WithFailNext arranges for the server to close the accepted socket right
// after reading the next n RPCs' message headers, before replying. It
// simulates a server that drops the connection mid-request, for testing
// client retry and failover behavior (see Task 11).
func WithFailNext(n int) Option {
	return func(c *config) { c.failNext = n }
}

// removedRPCs lists the Thrift method names absent from the emulated
// version's processor map.
func removedRPCs(v Version) []string {
	switch v {
	case Hive23:
		return []string{
			"get_catalogs", "get_catalog", "create_catalog", "drop_catalog",
			"alter_partitions_req", "get_partitions_req",
		}
	case Hive31:
		return []string{"alter_partitions_req", "get_partitions_req"}
	default:
		return nil
	}
}

// versionString is the "hive.metastore.version" config value Start seeds
// for the emulated version.
func versionString(v Version) string {
	switch v {
	case Hive23:
		return "2.3.9"
	case Hive31:
		return "3.1.3"
	default:
		return "4.0.1"
	}
}

// Server is an in-process fake Hive Metastore Thrift server. Construct one
// with Start.
type Server struct {
	ln       net.Listener
	rec      *recorder
	store    *Store
	failNext int32

	conns     sync.Map // net.Conn -> struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// Start launches a fake Hive Metastore server emulating version v and
// registers t.Cleanup to stop it. It fails the test immediately (via
// require) if an Option or the version table names an RPC absent from the
// generated processor map, since that indicates a typo rather than an
// intentional removal.
func Start(t testing.TB, v Version, opts ...Option) *Server {
	t.Helper()

	cfg := config{}
	for _, o := range opts {
		o(&cfg)
	}

	store := NewStore()
	store.Config["hive.metastore.version"] = versionString(v)
	rec := &recorder{}

	proc := hive_metastore.NewThriftHiveMetastoreProcessor(&handler{v: v, store: store, rec: rec})
	for _, name := range removedRPCs(v) {
		_, ok := proc.ProcessorMap()[name]
		require.True(t, ok, "hmstest: RPC %q not found in processor map (version table)", name)
		delete(proc.ProcessorMap(), name)
	}
	for _, name := range cfg.without {
		_, ok := proc.ProcessorMap()[name]
		require.True(t, ok, "hmstest: RPC %q not found in processor map (WithoutRPC)", name)
		delete(proc.ProcessorMap(), name)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	s := &Server{ln: ln, rec: rec, store: store, failNext: clampInt32(cfg.failNext)}
	go s.serve(proc)
	t.Cleanup(s.Stop)
	return s
}

// Addr returns the server's listen address, "127.0.0.1:port".
func (s *Server) Addr() string {
	return s.ln.Addr().String()
}

// URI returns the server's address as a "thrift://" URI.
func (s *Server) URI() string {
	return "thrift://" + s.Addr()
}

// Stop closes the listener and every accepted connection, then waits for
// their handler goroutines to exit. It is idempotent.
func (s *Server) Stop() {
	s.closeOnce.Do(func() {
		_ = s.ln.Close()
		s.conns.Range(func(key, _ any) bool {
			_ = key.(net.Conn).Close()
			return true
		})
		s.wg.Wait()
	})
}

// Calls returns, in call order, the Thrift wire method names of every RPC
// handled so far. The returned slice is a fresh copy.
func (s *Server) Calls() []string {
	return s.rec.list()
}

// LastArgs returns the most recently observed argument value for the
// given Thrift wire method name (e.g. "get_table_req"), or nil if it was
// never called. For "_req" methods this is the generated request struct
// pointer; for older positional-argument methods it is either the sole
// argument or a small *Args struct documented alongside the method (for
// example DropTableArgs for "drop_table").
func (s *Server) LastArgs(method string) any {
	return s.rec.lastArgs(method)
}

// Store returns the server's in-memory state.
func (s *Server) Store() *Store {
	return s.store
}

// serve accepts connections until the listener is closed by Stop.
func (s *Server) serve(proc thrift.TProcessor) {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.conns.Store(conn, struct{}{})
		s.wg.Add(1)
		go s.handleConn(conn, proc)
	}
}

// handleConn services one client connection until it errs out or closes.
func (s *Server) handleConn(conn net.Conn, proc thrift.TProcessor) {
	defer s.wg.Done()
	defer s.conns.Delete(conn)
	defer func() { _ = conn.Close() }()

	tcfg := &thrift.TConfiguration{}
	trans := thrift.NewTBufferedTransport(thrift.NewTSocketFromConnConf(conn, tcfg), bufferSize)
	base := thrift.NewTBinaryProtocolConf(trans, tcfg)
	proto := &failInjectProtocol{TProtocol: base, srv: s, conn: conn}

	ctx := context.Background()
	for {
		ok, err := proc.Process(ctx, proto, proto)
		if !ok || err != nil {
			return
		}
	}
}

// failInjectProtocol wraps a TProtocol and, while the server's failNext
// counter is positive, closes the underlying connection right after
// reading a message header instead of letting the RPC proceed. It
// implements WithFailNext.
type failInjectProtocol struct {
	thrift.TProtocol
	srv  *Server
	conn net.Conn
}

// ReadMessageBegin reads the message header as usual, then, if the
// server's fail-next budget is still positive, consumes one unit of it,
// closes the connection, and reports the read as failed so the caller
// (thrift's generated Process loop) stops without replying.
func (p *failInjectProtocol) ReadMessageBegin(ctx context.Context) (string, thrift.TMessageType, int32, error) {
	name, typeID, seqID, err := p.TProtocol.ReadMessageBegin(ctx)
	if err != nil {
		return name, typeID, seqID, err
	}
	for {
		cur := atomic.LoadInt32(&p.srv.failNext)
		if cur <= 0 {
			return name, typeID, seqID, nil
		}
		if atomic.CompareAndSwapInt32(&p.srv.failNext, cur, cur-1) {
			_ = p.conn.Close()
			return name, typeID, seqID, thrift.NewTTransportException(thrift.END_OF_FILE, "hmstest: injected failure")
		}
	}
}
