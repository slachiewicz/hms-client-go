package hmstest

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"runtime/debug"
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

// versionString is the version string the emulated version reports from
// the fb303 getVersion RPC (see handler.GetVersion). It mirrors real
// servers: pre-4 metastores answer with the metastore schema line "3.0"
// rather than their release, on Hive23 as well as Hive31, and only a
// Hive40 server reports its actual release. Start no longer seeds this
// under "hive.metastore.version"/"metastore.version": a test wanting the
// get_config_value fallback path must seed those keys itself (see
// TestServerVersion_NeitherConfigValueSet in client_test.go).
func versionString(v Version) string {
	switch v {
	case Hive23, Hive31:
		return "3.0"
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
	tb       testing.TB // for reporting a handler panic from handleConn

	conns sync.Map // net.Conn -> struct{}
	wg    sync.WaitGroup

	mu      sync.Mutex // guards stopped; serializes it against serve's accept path
	stopped bool

	panicsMu sync.Mutex
	panics   []string
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
	// CreateDatabase (client.go) resolves an unset Database.LocationURI to
	// "<warehouse>/<db>.db". A real Hive 2.3 metastore predates catalogs,
	// so CreateDatabase reads the warehouse root from
	// get_config_value("hive.metastore.warehouse.dir", ...); seed that key
	// so the path is exercised without every Hive23 test having to set it
	// itself. A real Hive 3.1 metastore's get_config_value does not
	// resolve that key to its "metastore.warehouse.dir" alias the way
	// Hive 4's does -- it answers "" -- so CreateDatabase instead reads
	// the warehouse root from the resolved catalog's own LocationUri
	// (get_catalog) on any server that supports catalogs; mirror that
	// here by seeding the default "hive" catalog's location instead, and
	// deliberately leaving hive.metastore.warehouse.dir unset, so a
	// regression that made CreateDatabase depend on it again would fail
	// on Hive31 the way it does against a real server.
	if v == Hive23 {
		store.Config["hive.metastore.warehouse.dir"] = "file:///tmp/hms-warehouse"
	} else {
		store.Catalogs["hive"].LocationUri = "file:///tmp/hms-warehouse"
	}
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

	s := &Server{ln: ln, rec: rec, store: store, failNext: clampInt32(cfg.failNext), tb: t}
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
// their handler goroutines to exit. It is idempotent: a second (or
// concurrent) call returns immediately.
//
// Setting the stopped flag happens under the same mutex that serve locks
// around accepting a connection, so a connection accepted concurrently
// with Stop is either seen (and thus closed and waited for below) or
// rejected by serve itself before Stop returns; no accepted connection can
// be missed by both.
func (s *Server) Stop() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	s.mu.Unlock()

	_ = s.ln.Close()
	s.conns.Range(func(key, _ any) bool {
		_ = key.(net.Conn).Close()
		return true
	})
	s.wg.Wait()
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

// Panics returns, in the order they occurred, the messages recorded by
// handleConn's recover for a panic in an unimplemented (nil embedded
// ThriftHiveMetastore) or misbehaving handler method. The returned slice
// is a fresh copy. Tests use this to confirm a panic was caught rather
// than crashing the test binary.
func (s *Server) Panics() []string {
	s.panicsMu.Lock()
	defer s.panicsMu.Unlock()
	out := make([]string, len(s.panics))
	copy(out, s.panics)
	return out
}

func (s *Server) recordPanic(msg string) {
	s.panicsMu.Lock()
	s.panics = append(s.panics, msg)
	s.panicsMu.Unlock()
}

// serve accepts connections until the listener is closed by Stop. Every
// accepted connection is either registered for handling (under s.mu, so
// Stop cannot be running concurrently with a use of the registration
// state below) or, once Stop has flipped s.stopped, closed immediately
// without ever being handled — closing the accept-time race described on
// Stop.
func (s *Server) serve(proc thrift.TProcessor) {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}

		s.mu.Lock()
		if s.stopped {
			s.mu.Unlock()
			_ = conn.Close()
			return
		}
		s.conns.Store(conn, struct{}{})
		s.wg.Add(1)
		s.mu.Unlock()

		go s.handleConn(conn, proc)
	}
}

// handleConn services one client connection until it errs out or closes.
// A panic in a handler method (most commonly: an RPC that is routable in
// the Thrift processor map but not overridden by handler, so it
// dispatches to the embedded nil ThriftHiveMetastore) is recovered here
// rather than allowed to crash the whole test binary: it is recorded on
// the Server (see Panics) and reported to the testing.TB passed to Start
// via Errorf, then the connection is closed as usual by the deferred
// close below.
func (s *Server) handleConn(conn net.Conn, proc thrift.TProcessor) {
	defer s.wg.Done()
	defer s.conns.Delete(conn)
	defer func() { _ = conn.Close() }()
	defer func() {
		if r := recover(); r != nil {
			msg := fmt.Sprintf("%v\n%s", r, debug.Stack())
			s.recordPanic(msg)
			if s.tb != nil {
				s.tb.Errorf("hmstest: handler panic: %s", msg)
			}
		}
	}()

	tcfg := &thrift.TConfiguration{}
	trans := thrift.NewTBufferedTransport(thrift.NewTSocketFromConnConf(conn, tcfg), bufferSize)
	base := thrift.NewTBinaryProtocolConf(trans, tcfg)
	proto := &failInjectProtocol{TProtocol: base, srv: s, conn: conn}

	ctx := context.Background()
	for {
		ok, err := proc.Process(ctx, proto, proto)
		// Mirror thrift's own TSimpleServer.processRequests (lib/go/thrift
		// v0.24.0, simple_server.go): a generated Process function returns
		// (ok=true, err=<declared exception>) whenever it successfully
		// writes a checked/declared Thrift exception (e.g.
		// NoSuchObjectException, AlreadyExistsException) into the RPC
		// reply — that is a normal, successful round trip, not a
		// connection failure, and the connection remains good for further
		// calls. Treating a non-nil err as fatal here (as an earlier
		// version of this loop did) closed the connection right after
		// correctly replying to, say, a duplicate create_catalog, so the
		// next call on that (still-pooled) client connection saw an EOF
		// even though nothing had actually gone wrong.
		//
		// Only three conditions end the connection, exactly as upstream
		// does: the handler abandoning the request, a real transport
		// (I/O) failure, and Process itself reporting !ok. A
		// TApplicationException of type UNKNOWN_METHOD is also not fatal:
		// it means the generated dispatcher wrote a valid exception reply
		// for an RPC absent from this version's processor map (see
		// removedRPCs / WithoutRPC) — real Hive Metastore server
		// connections survive a probe for an RPC they don't support
		// (SPEC §2.3), so the fake server must too, or every client-side
		// fallback/catalog probe would kill the connection it just proved
		// it needed to keep using.
		if errors.Is(err, thrift.ErrAbandonRequest) {
			return
		}
		var transErr thrift.TTransportException
		if errors.As(err, &transErr) {
			return
		}
		var appErr thrift.TApplicationException
		if errors.As(err, &appErr) && appErr.TypeId() == thrift.UNKNOWN_METHOD {
			continue
		}
		if !ok {
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
