package hmstest_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
	"github.com/slachiewicz/hms-client-go/internal/hmstest"
	"github.com/slachiewicz/hms-client-go/internal/transport"
)

func ptr[T any](v T) *T { return &v }

// recordingTB wraps a testing.TB and records Errorf calls instead of
// forwarding them, so a test can assert that hmstest reported a handler
// panic (via tb.Errorf, from a non-test goroutine) without the recording
// test itself going red.
type recordingTB struct {
	testing.TB

	mu   sync.Mutex
	errs []string
}

func (r *recordingTB) Errorf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs = append(r.errs, fmt.Sprintf(format, args...))
}

func (r *recordingTB) Helper() {}

func (r *recordingTB) Errors() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.errs))
	copy(out, r.errs)
	return out
}

// dial opens a binary Thrift connection to addr and wraps it in a
// ThriftHiveMetastoreClient, closing the connection on test cleanup.
func dial(t *testing.T, addr string) *hive_metastore.ThriftHiveMetastoreClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := transport.DialBinary(ctx, addr, transport.BinaryConfig{Timeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return hive_metastore.NewThriftHiveMetastoreClient(conn.Client)
}

func TestServer_Hive40_GetCatalogs(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	client := dial(t, srv.Addr())

	resp, err := client.GetCatalogs(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"hive"}, resp.Names)
}

func TestServer_Hive23_GetCatalogsIsUnknownMethod(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive23)
	client := dial(t, srv.Addr())

	_, err := client.GetCatalogs(context.Background())
	require.Error(t, err)

	var appErr thrift.TApplicationException
	require.True(t, errors.As(err, &appErr), "expected a TApplicationException, got %T: %v", err, err)
	assert.Equal(t, int32(thrift.UNKNOWN_METHOD), appErr.TypeId())
}

func TestServer_Hive23_GetTableReq(t *testing.T) {
	t.Parallel()

	t.Run("explicit catName is rejected", func(t *testing.T) {
		t.Parallel()
		srv := hmstest.Start(t, hmstest.Hive23)
		client := dial(t, srv.Addr())

		_, err := client.GetTableReq(context.Background(), &hive_metastore.GetTableRequest{
			DbName:  "db",
			TblName: "tbl",
			CatName: ptr("hive"),
			Engine:  "hive",
			ID:      -1,
		})
		require.Error(t, err)
		var metaErr *hive_metastore.MetaException
		require.True(t, errors.As(err, &metaErr), "expected *MetaException, got %T: %v", err, err)
	})

	t.Run("nil catName and missing table is NoSuchObjectException", func(t *testing.T) {
		t.Parallel()
		srv := hmstest.Start(t, hmstest.Hive23)
		client := dial(t, srv.Addr())

		_, err := client.GetTableReq(context.Background(), &hive_metastore.GetTableRequest{
			DbName:  "db",
			TblName: "tbl",
			Engine:  "hive",
			ID:      -1,
		})
		require.Error(t, err)
		var nsoErr *hive_metastore.NoSuchObjectException
		require.True(t, errors.As(err, &nsoErr), "expected *NoSuchObjectException, got %T: %v", err, err)
	})
}

// TestServer_ConnectionSurvivesUnknownMethod proves handleConn keeps a
// client connection open after replying UNKNOWN_METHOD to an RPC absent
// from the emulated version's processor map: on Hive23, get_catalogs (not
// in the Hive23 processor map, see removedRPCs) is immediately followed by
// get_all_databases on the very same connection, and that second call must
// still succeed. Before the handleConn fix, any non-nil Process error
// (including the UNKNOWN_METHOD exception it had just written correctly)
// ended the connection, so this second call would see an EOF.
func TestServer_ConnectionSurvivesUnknownMethod(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive23)
	client := dial(t, srv.Addr())
	ctx := context.Background()

	_, err := client.GetCatalogs(ctx)
	require.Error(t, err)
	var appErr thrift.TApplicationException
	require.True(t, errors.As(err, &appErr), "expected a TApplicationException, got %T: %v", err, err)
	assert.Equal(t, int32(thrift.UNKNOWN_METHOD), appErr.TypeId())

	names, err := client.GetAllDatabases(ctx)
	require.NoError(t, err, "expected the connection to survive the UNKNOWN_METHOD reply")
	assert.Empty(t, names)
}

// TestServer_ConnectionSurvivesDeclaredException proves handleConn keeps a
// client connection open after a generated Process function reports a
// declared/checked Thrift exception (e.g. AlreadyExistsException): the
// generated create_catalog Process returns (ok=true, err=<the exception>)
// once it has successfully written that exception into the RPC reply,
// exactly mirroring thrift's own TSimpleServer (lib/go/thrift v0.24.0,
// simple_server.go processRequests), which only ends the connection on
// !ok, ErrAbandonRequest, or a real TTransportException — never merely
// because Process returned a non-nil err. A duplicate create_catalog is
// immediately followed here by get_catalogs on the same connection, and
// that second call must still succeed.
func TestServer_ConnectionSurvivesDeclaredException(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	client := dial(t, srv.Addr())
	ctx := context.Background()

	srv.Store().Catalogs["spark"] = &hive_metastore.Catalog{Name: "spark"}

	err := client.CreateCatalog(ctx, &hive_metastore.CreateCatalogRequest{
		Catalog: &hive_metastore.Catalog{Name: "spark"},
	})
	require.Error(t, err)
	var existsErr *hive_metastore.AlreadyExistsException
	require.True(t, errors.As(err, &existsErr), "expected *AlreadyExistsException, got %T: %v", err, err)

	resp, err := client.GetCatalogs(ctx)
	require.NoError(t, err, "expected the connection to survive the declared exception reply")
	assert.Contains(t, resp.Names, "spark")
}

func TestServer_CallsAndLastArgs(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	client := dial(t, srv.Addr())
	ctx := context.Background()

	_, err := client.GetCatalogs(ctx)
	require.NoError(t, err)

	req := &hive_metastore.GetTableRequest{DbName: "db", TblName: "tbl", Engine: "hive", ID: -1}
	_, err = client.GetTableReq(ctx, req)
	require.Error(t, err) // table does not exist; we only care about recording here

	assert.Contains(t, srv.Calls(), "get_catalogs")

	last, ok := srv.LastArgs("get_table_req").(*hive_metastore.GetTableRequest)
	require.True(t, ok, "LastArgs(get_table_req) has unexpected type %T", srv.LastArgs("get_table_req"))
	assert.Equal(t, "db", last.DbName)
	assert.Equal(t, "tbl", last.TblName)
}

func TestServer_WithoutRPC(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40, hmstest.WithoutRPC("get_config_value"))
	client := dial(t, srv.Addr())

	_, err := client.GetConfigValue(context.Background(), "hive.metastore.version", "")
	require.Error(t, err)

	var appErr thrift.TApplicationException
	require.True(t, errors.As(err, &appErr), "expected a TApplicationException, got %T: %v", err, err)
	assert.Equal(t, int32(thrift.UNKNOWN_METHOD), appErr.TypeId())
}

func TestServer_AddrAndURI(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	assert.Equal(t, "thrift://"+srv.Addr(), srv.URI())
}

// TestServer_HandlerPanicIsRecovered calls an RPC that the fake server
// routes (it is present in Hive40's processor map) but that handler.go
// does not override, so it dispatches to the embedded nil
// ThriftHiveMetastore and nil-panics. handleConn must recover it, close
// the connection, report it via tb.Errorf, and record it in Panics(),
// rather than crashing the test binary.
func TestServer_HandlerPanicIsRecovered(t *testing.T) {
	t.Parallel()
	rtb := &recordingTB{TB: t}
	srv := hmstest.Start(rtb, hmstest.Hive40)
	client := dial(t, srv.Addr())

	_, err := client.GetMetaConf(context.Background(), "x")
	require.Error(t, err, "expected the closed connection to surface as a client error")

	assert.Len(t, srv.Panics(), 1, "expected exactly one recorded panic")
	assert.Len(t, rtb.Errors(), 1, "expected the panic to be reported via tb.Errorf")
}

// TestServer_StopIsIdempotent verifies a second Stop call (e.g. one made
// explicitly by a test on top of the t.Cleanup registered by Start) does
// not block or panic.
func TestServer_StopIsIdempotent(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	srv.Stop()
	srv.Stop()
}

// TestServer_StopWithNoConnectionsReturnsPromptly guards against a Stop
// that waits forever (or races the accept loop) when no client ever
// connected.
func TestServer_StopWithNoConnectionsReturnsPromptly(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)

	done := make(chan struct{})
	go func() {
		srv.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return promptly with no connections")
	}
}

func TestServer_GetConfigValue(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive31)
	// Start no longer seeds this key itself (see versionString); seed it
	// directly to exercise GetConfigValue's plumbing.
	srv.Store().Config["hive.metastore.version"] = "3.1.3"
	client := dial(t, srv.Addr())

	v, err := client.GetConfigValue(context.Background(), "hive.metastore.version", "unset")
	require.NoError(t, err)
	assert.Equal(t, "3.1.3", v)

	v, err = client.GetConfigValue(context.Background(), "does.not.exist", "fallback")
	require.NoError(t, err)
	assert.Equal(t, "fallback", v)
}
