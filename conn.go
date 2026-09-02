package hms

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/slachiewicz/hms-client-go/gen/fb303"
	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
	"github.com/slachiewicz/hms-client-go/internal/transport"
)

// catalogSupport records what a conn knows about its server's support for
// Hive 3+ catalogs, probed lazily by supportsCatalogs.
type catalogSupport int32

// Values stored in conn.catalogs.
const (
	// catalogUnknown means supportsCatalogs has not probed yet.
	catalogUnknown catalogSupport = 0
	// catalogYes means the server answered get_catalogs successfully.
	catalogYes catalogSupport = 1
	// catalogNo means get_catalogs came back UNKNOWN_METHOD.
	catalogNo catalogSupport = 2
)

// conn is one live connection to a metastore endpoint. Every generated RPC
// method this client uses is bound into a func field at construction time;
// the generated ThriftHiveMetastoreClient itself is never stored, per
// AGENTS.md invariant #5.
type conn struct {
	// close releases the underlying transport.
	close func() error

	// fallback caches, per RPC method name, whether a prior call on this
	// conn observed UNKNOWN_METHOD and should be retried against the
	// method's legacy form (SPEC §2.3 Rules 3 and 4). Task 11 reads and
	// writes it through useLegacy / markLegacy.
	fallback sync.Map // string -> bool

	// catalogs records this conn's probed catalog support (see
	// catalogSupport); read and written with atomic ops since conns may be
	// shared across goroutines via the pool.
	catalogs atomic.Int32

	getCatalogs   func(ctx context.Context) (*hive_metastore.GetCatalogsResponse, error)
	getCatalog    func(ctx context.Context, req *hive_metastore.GetCatalogRequest) (*hive_metastore.GetCatalogResponse, error)
	createCatalog func(ctx context.Context, req *hive_metastore.CreateCatalogRequest) error
	dropCatalog   func(ctx context.Context, req *hive_metastore.DropCatalogRequest) error

	getAllDatabases func(ctx context.Context) ([]string, error)
	getDatabases    func(ctx context.Context, pattern string) ([]string, error)
	getDatabase     func(ctx context.Context, name string) (*hive_metastore.Database, error)
	createDatabase  func(ctx context.Context, db *hive_metastore.Database) error
	dropDatabase    func(ctx context.Context, name string, deleteData, cascade bool) error

	getAllTables             func(ctx context.Context, dbName string) ([]string, error)
	getTableReq              func(ctx context.Context, req *hive_metastore.GetTableRequest) (*hive_metastore.GetTableResult_, error)
	getTableObjectsByNameReq func(ctx context.Context, req *hive_metastore.GetTablesRequest) (*hive_metastore.GetTablesResult_, error)
	createTable              func(ctx context.Context, tbl *hive_metastore.Table) error
	alterTable               func(ctx context.Context, dbName, tblName string, newTbl *hive_metastore.Table) error
	dropTable                func(ctx context.Context, dbName, name string, deleteData bool) error

	getPartitions      func(ctx context.Context, dbName, tblName string, maxParts int16) ([]*hive_metastore.Partition, error)
	getPartitionsReq   func(ctx context.Context, req *hive_metastore.PartitionsRequest) (*hive_metastore.PartitionsResponse, error)
	getPartitionNames  func(ctx context.Context, dbName, tblName string, maxParts int16) ([]string, error)
	addPartitionsReq   func(ctx context.Context, req *hive_metastore.AddPartitionsRequest) (*hive_metastore.AddPartitionsResult_, error)
	alterPartitions    func(ctx context.Context, dbName, tblName string, parts []*hive_metastore.Partition) error
	alterPartitionsReq func(ctx context.Context, req *hive_metastore.AlterPartitionsRequest) (*hive_metastore.AlterPartitionsResponse, error)
	dropPartition      func(ctx context.Context, dbName, tblName string, partVals []string, deleteData bool) (bool, error)

	getConfigValue func(ctx context.Context, name, defaultValue string) (string, error)
	getStatus      func(ctx context.Context) (fb303.FbStatus, error)
}

// newConn dials ep and binds every generated RPC method this client uses
// into cn's func fields. The generated ThriftHiveMetastoreClient (g below)
// is local to this function and is never stored, per AGENTS.md invariant
// #5.
func newConn(ctx context.Context, ep transport.Endpoint, cfg *config) (*conn, error) {
	var tc *transport.Conn
	var err error
	switch ep.Scheme {
	case transport.SchemeThrift:
		tc, err = transport.DialBinary(ctx, ep.Host, transport.BinaryConfig{
			Timeout:       cfg.timeout,
			PlainUser:     cfg.plainUser,
			PlainPassword: cfg.plainPassword,
		})
	default:
		tc, err = transport.NewHTTP(ctx, ep.URL, transport.HTTPConfig{
			Client:      cfg.httpClient,
			Timeout:     cfg.timeout,
			BearerToken: cfg.bearerToken,
			User:        cfg.user,
			Headers:     cfg.httpHeaders,
			UserAgent:   userAgent(),
		})
	}
	if err != nil {
		return nil, err
	}

	g := hive_metastore.NewThriftHiveMetastoreClient(tc.Client)
	fb := fb303.NewFacebookServiceClient(tc.Client)

	return &conn{
		close: tc.Close,

		getCatalogs:   g.GetCatalogs,
		getCatalog:    g.GetCatalog,
		createCatalog: g.CreateCatalog,
		dropCatalog:   g.DropCatalog,

		getAllDatabases: g.GetAllDatabases,
		getDatabases:    g.GetDatabases,
		getDatabase:     g.GetDatabase,
		createDatabase:  g.CreateDatabase,
		dropDatabase:    g.DropDatabase,

		getAllTables:             g.GetAllTables,
		getTableReq:              g.GetTableReq,
		getTableObjectsByNameReq: g.GetTableObjectsByNameReq,
		createTable:              g.CreateTable,
		alterTable:               g.AlterTable,
		dropTable:                g.DropTable,

		getPartitions:      g.GetPartitions,
		getPartitionsReq:   g.GetPartitionsReq,
		getPartitionNames:  g.GetPartitionNames,
		addPartitionsReq:   g.AddPartitionsReq,
		alterPartitions:    g.AlterPartitions,
		alterPartitionsReq: g.AlterPartitionsReq,
		dropPartition:      g.DropPartition,

		getConfigValue: g.GetConfigValue,
		getStatus:      fb.GetStatus,
	}, nil
}

// useLegacy reports whether method previously observed UNKNOWN_METHOD on
// this conn and should be retried against its legacy form (SPEC §2.3 Rules
// 3 and 4).
func (cn *conn) useLegacy(method string) bool {
	v, ok := cn.fallback.Load(method)
	return ok && v.(bool)
}

// markLegacy records that method should be retried against its legacy form
// for the remaining lifetime of this conn.
func (cn *conn) markLegacy(method string) {
	cn.fallback.Store(method, true)
}

// tryReq runs req; on UNKNOWN_METHOD it records the fallback on this conn
// (keyed by method, the request-variant RPC's wire name, e.g.
// "get_partitions_req") and runs legacy instead. Subsequent calls on this
// conn for the same method go straight to legacy without retrying req
// (SPEC §2.3 Rules 3 and 4).
func (cn *conn) tryReq(ctx context.Context, method string, req, legacy func(context.Context) error) error {
	if cn.useLegacy(method) {
		return legacy(ctx)
	}
	err := req(ctx)
	if isUnknownMethod(err) {
		cn.markLegacy(method)
		return legacy(ctx)
	}
	return err
}

// supportsCatalogs reports whether this conn's server understands Hive 3+
// catalogs, probing get_catalogs at most once per conn (SPEC §2.3 Rule 1).
// UNKNOWN_METHOD is treated as "no" and cached; any other error propagates
// unclassified so the caller (via wrapError) reports it accurately.
func (cn *conn) supportsCatalogs(ctx context.Context) (bool, error) {
	switch catalogSupport(cn.catalogs.Load()) {
	case catalogYes:
		return true, nil
	case catalogNo:
		return false, nil
	case catalogUnknown:
		// Not yet probed; fall through to the probe below.
	}
	_, err := cn.getCatalogs(ctx)
	switch {
	case err == nil:
		cn.catalogs.Store(int32(catalogYes))
		return true, nil
	case isUnknownMethod(err):
		cn.catalogs.Store(int32(catalogNo))
		return false, nil
	default:
		return false, err
	}
}
