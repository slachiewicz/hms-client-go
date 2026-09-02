//go:build integration

// Package integration_test exercises the hms package against a real,
// running Hive Metastore server (SPEC.md §2 compatibility matrix, §5 API,
// §6 formats), as opposed to the in-process fake used by the rest of the
// test suite (internal/hmstest). It is driven by the environment contract
// below and is not run by "go test ./..." or "make check"; only "make
// test-docker" (build tag "integration") runs it, against a container
// started by .github/workflows/integration.yml.
//
// Environment contract:
//   - HMS_URIS: the endpoint(s) to connect to, e.g. "thrift://127.0.0.1:9083"
//     or "http://127.0.0.1:9083/metastore". Every test in this package
//     t.Skip's with a clear message when this is empty, so the package
//     also runs (as a no-op) in a Docker-less environment such as "go vet"
//     or "make check".
//   - HMS_EXPECT_VERSION: the connected server's expected version, one of
//     "2.3", "3.1", "4.0", "4.2". Required whenever HMS_URIS is set; it
//     selects which version-gated assertions apply (e.g. catalog support)
//     and is cross-checked against Client.ServerVersion.
//   - HMS_USER (optional): forwarded as hms.WithUser, for a server that
//     authenticates the caller (e.g. the HTTP-mode job, which runs as
//     "ci").
//   - HMS_KRB5_URIS, HMS_KRB5_PRINCIPAL, HMS_KRB5_KEYTAB (optional): a
//     Kerberized endpoint and the identity to reach it with, for
//     TestKerberos. No matrix job sets them yet; see envKrb5URIs.
package integration_test

import (
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hms "github.com/slachiewicz/hms-client-go"
)

// envURIs is the environment variable naming the metastore endpoint(s)
// under test. See the package doc comment's environment contract.
const envURIs = "HMS_URIS"

// envExpectVersion is the environment variable naming the connected
// server's expected version. See the package doc comment.
const envExpectVersion = "HMS_EXPECT_VERSION"

// envUser is the environment variable naming the principal to authenticate
// as, forwarded via hms.WithUser. See the package doc comment.
const envUser = "HMS_USER"

// envTLSURIs names the endpoint(s) of a metastore configured with
// metastore.use.SSL=true (SPEC §3.1), for TestTLS. None of the matrix
// images enable it yet (PLAN.md Slice 9 tracks a TLS-enabled leg as a
// follow-up), so this is always empty today and TestTLS always skips; it
// exists so that job has a test ready to enable once it lands.
const envTLSURIs = "HMS_TLS_URIS"

// envKrb5URIs names the endpoint(s) of a Kerberized metastore (SPEC §3.1,
// KERBEROS), for TestKerberos, and envKrb5Principal the client principal
// to authenticate as. No matrix image runs a KDC yet (PLAN.md Slice 14
// tracks a Kerberized leg as a follow-up), so these are always empty today
// and TestKerberos always skips; the test exists so that job has one ready
// to enable once the KDC sidecar lands. The credentials come from the
// ambient KRB5CCNAME credential cache unless envKrb5Keytab names a keytab.
const (
	envKrb5URIs      = "HMS_KRB5_URIS"
	envKrb5Principal = "HMS_KRB5_PRINCIPAL"
	envKrb5Keytab    = "HMS_KRB5_KEYTAB"
)

// dialTimeout bounds how long New (and thus every test's setup) waits to
// connect before failing, distinct from each RPC's own context below.
const dialTimeout = 30 * time.Second

// seq makes uniqueName's names unique across the concurrent subtests within
// this process, on top of the nanosecond timestamp it also carries.
var seq atomic.Int64

// requireHMSEnv reads and validates the environment contract, skipping the
// test when HMS_URIS is empty (so this package is a no-op without a
// running metastore) and failing it when HMS_URIS is set but
// HMS_EXPECT_VERSION is missing or not one of the recognized versions.
func requireHMSEnv(t *testing.T) (uris, expectVersion string) {
	t.Helper()
	uris = os.Getenv(envURIs)
	if uris == "" {
		t.Skipf("%s is not set; skipping integration test (see test/integration_test.go for the environment contract)", envURIs)
	}
	expectVersion = os.Getenv(envExpectVersion)
	switch expectVersion {
	case "2.3", "3.1", "4.0", "4.2":
	default:
		t.Fatalf("%s must be one of \"2.3\", \"3.1\", \"4.0\", \"4.2\" when %s is set; got %q", envExpectVersion, envURIs, expectVersion)
	}
	return uris, expectVersion
}

// dial connects a Client to HMS_URIS, applying HMS_USER (if set) before any
// caller-supplied opts, and registers t.Cleanup to close it.
func dial(t *testing.T, opts ...hms.Option) *hms.Client {
	t.Helper()
	uris, _ := requireHMSEnv(t)

	all := make([]hms.Option, 0, len(opts)+1)
	if u := os.Getenv(envUser); u != "" {
		all = append(all, hms.WithUser(u))
	}
	all = append(all, opts...)

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	c, err := hms.New(ctx, uris, all...)
	require.NoError(t, err, "connecting to %s", uris)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// uniqueName returns a name starting with prefix that no other call in this
// test binary run will produce, so parallel tests never collide over a
// shared database, catalog, or table name on the live metastore.
func uniqueName(prefix string) string {
	return fmt.Sprintf("%s%d_%d", prefix, time.Now().UnixNano(), seq.Add(1))
}

// createDB creates a database named name (optionally in catalog catName,
// when non-empty) and registers t.Cleanup to drop it (cascading, ignoring a
// missing database) once the test finishes.
func createDB(t *testing.T, c *hms.Client, ctx context.Context, name, catName string) {
	t.Helper()
	db := &hms.Database{Name: name, CatalogName: catName}
	require.NoError(t, c.CreateDatabase(ctx, db))

	var opts []hms.CatalogOption
	if catName != "" {
		opts = append(opts, hms.InCatalog(catName))
	}
	t.Cleanup(func() {
		_ = c.DropDatabase(context.Background(), name, true, true, true, opts...)
	})
}

// textStorage returns a minimal, valid StorageDescriptor at location using
// Hive's built-in text input/output formats and LazySimpleSerDe, sufficient
// to create a table or partition on any supported server version.
func textStorage(location string, cols []*hms.FieldSchema) *hms.StorageDescriptor {
	return &hms.StorageDescriptor{
		Location:     location,
		Columns:      cols,
		InputFormat:  "org.apache.hadoop.mapred.TextInputFormat",
		OutputFormat: "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
		SerDe: &hms.SerDeInfo{
			SerializationLib: "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
		},
	}
}

// decodeNotificationMessage returns ev.Message as plain text, undoing the
// gzip+base64 envelope a real metastore wraps it in when ev.MessageFormat
// is "gzip(json-2.0)" (Hive's GzipJSONMessageEncoder, the default message
// factory since Hive 4 -- verified by decompiling
// org.apache.hadoop.hive.metastore.messaging.json.gzip.Serializer from
// hive-standalone-metastore-server-4.2.1.jar: MessageFactory.getInstance
// registers it as "metastore.event.message.factory"'s default). SPEC.md
// §5.7 deliberately exposes Message/MessageFormat as opaque strings rather
// than decoding them in the client, so this stays test-only rather than
// becoming exported API; any other MessageFormat (e.g. Hive 2/3's
// "json-0.2") is returned unchanged.
func decodeNotificationMessage(t *testing.T, ev hms.NotificationEvent) string {
	t.Helper()
	if ev.MessageFormat != "gzip(json-2.0)" {
		return ev.Message
	}
	raw, err := base64.StdEncoding.DecodeString(ev.Message)
	require.NoError(t, err, "base64-decoding notification message")
	zr, err := gzip.NewReader(strings.NewReader(string(raw)))
	require.NoError(t, err, "opening gzip notification message")
	defer func() { _ = zr.Close() }()
	decoded, err := io.ReadAll(zr)
	require.NoError(t, err, "reading gzip notification message")
	return string(decoded)
}

// TestDatabases_CRUD covers CreateDatabase/GetDatabase/GetAllDatabases/
// AlterDatabase/DropDatabase, ErrAlreadyExists on a duplicate create, and
// ErrNotFound on a get/drop after the database is gone (SPEC.md §5.3).
func TestDatabases_CRUD(t *testing.T) {
	t.Parallel()
	c := dial(t)
	ctx := context.Background()

	name := uniqueName("it_db_")
	db := &hms.Database{
		Name:        name,
		Description: "hms-client-go integration test database",
		Parameters:  map[string]string{"created_by": "hms-client-go-integration"},
	}
	require.NoError(t, c.CreateDatabase(ctx, db))

	err := c.CreateDatabase(ctx, db)
	require.ErrorIs(t, err, hms.ErrAlreadyExists)

	got, err := c.GetDatabase(ctx, name)
	require.NoError(t, err)
	assert.Equal(t, name, got.Name)
	assert.Equal(t, db.Description, got.Description)
	assert.Equal(t, "hms-client-go-integration", got.Parameters["created_by"])

	names, err := c.GetAllDatabases(ctx)
	require.NoError(t, err)
	assert.Contains(t, names, name)

	// AlterDatabase (1.0 addition, SPEC.md §5.3): change a parameter and
	// the owner, and read them back.
	altered := *got
	altered.Parameters = map[string]string{"created_by": "hms-client-go-integration", "updated_by": "hms-client-go-integration"}
	altered.OwnerName = "hms-client-go-integration"
	require.NoError(t, c.AlterDatabase(ctx, name, &altered))

	gotAltered, err := c.GetDatabase(ctx, name)
	require.NoError(t, err)
	assert.Equal(t, "hms-client-go-integration", gotAltered.Parameters["updated_by"])
	assert.Equal(t, "hms-client-go-integration", gotAltered.OwnerName)

	require.NoError(t, c.DropDatabase(ctx, name, false, false, false))

	_, err = c.GetDatabase(ctx, name)
	require.ErrorIs(t, err, hms.ErrNotFound)

	err = c.DropDatabase(ctx, name, false, false, false)
	require.ErrorIs(t, err, hms.ErrNotFound)
	require.NoError(t, c.DropDatabase(ctx, name, false, false, true))
}

// TestTables_FormatBuildersAndLifecycle round-trips NewIcebergTable,
// NewDeltaTable, and NewHudiTable through CreateTable/GetTable (SPEC.md §6),
// then exercises AlterTable (a parameter change) and DropTable's ifExists
// semantics. It also asserts OwnerType defaults to PrincipalUser on create
// (SPEC.md §5.4, "1.0 addition"), and, on 4.x only, a smoke test that a
// further GetTable -> AlterTable round trip that changes nothing leaves the
// Iceberg table's modelled Parameters, Storage.SerDe, and TableType intact.
// This package is external to hms (no access to the raw/TableRaw internals
// package hms's own convert_internal_test.go exercises against the fake
// server), so it cannot assert survival of a field hms.Table does not
// model; that guarantee is the white-box tests' job, not this one's.
func TestTables_FormatBuildersAndLifecycle(t *testing.T) {
	t.Parallel()
	c := dial(t)
	_, expectVersion := requireHMSEnv(t)
	ctx := context.Background()

	dbName := uniqueName("it_tbldb_")
	createDB(t, c, ctx, dbName, "")

	cols := []*hms.FieldSchema{
		{Name: "id", Type: "bigint"},
		{Name: "name", Type: "string"},
	}

	iceberg := hms.NewIcebergTable(dbName, "iceberg_tbl", "file:///tmp/"+dbName+"/iceberg_tbl", "file:///tmp/"+dbName+"/iceberg_tbl/metadata/v1.metadata.json", cols)
	require.NoError(t, c.CreateTable(ctx, iceberg))
	gotIceberg, err := c.GetTable(ctx, dbName, "iceberg_tbl")
	require.NoError(t, err)
	assert.Equal(t, hms.IcebergSerDe, gotIceberg.Storage.SerDe.SerializationLib)
	assert.Equal(t, "ICEBERG", gotIceberg.Parameters[hms.ParamTableType])
	assert.Equal(t, hms.PrincipalUser, gotIceberg.OwnerType, "OwnerType defaults to PrincipalUser on create")

	// GetTableColumnStatistics (SPEC.md §5.8, 1.0 addition, read-only): a
	// freshly created table has no computed statistics yet on any
	// supported version, so this asserts the get_table_statistics_req RPC
	// path itself works everywhere -- an empty result and no error -- not
	// any particular statistic value.
	stats, err := c.GetTableColumnStatistics(ctx, dbName, "iceberg_tbl", []string{"id", "name"})
	require.NoError(t, err)
	assert.Empty(t, stats)

	if expectVersion == "4.0" || expectVersion == "4.2" {
		// Smoke test: a GetTable -> AlterTable round trip that changes
		// nothing must not disturb Parameters, Storage.SerDe, or
		// TableType. This does not exercise round-trip fidelity itself
		// (SPEC.md §5.4) -- every field checked here is one hms.Table
		// already models and would round-trip even without the internal
		// snapshot; see convert_internal_test.go's
		// TestTableRoundTrip_PreservesUnmodelledFields and
		// TestAlterTable_PreservesUnmodelledFields for that.
		wantParams := make(map[string]string, len(gotIceberg.Parameters))
		for k, v := range gotIceberg.Parameters {
			wantParams[k] = v
		}
		require.NoError(t, c.AlterTable(ctx, dbName, "iceberg_tbl", gotIceberg))
		afterFidelity, err := c.GetTable(ctx, dbName, "iceberg_tbl")
		require.NoError(t, err)
		// Hive 4 adds its own statistics parameters (numFiles, totalSize,
		// numFilesErasureCoded) on alter_table, so compare as a subset: every
		// parameter the client sent must come back unchanged.
		for k, v := range wantParams {
			assert.Equal(t, v, afterFidelity.Parameters[k], "parameter %q", k)
		}
		assert.Equal(t, gotIceberg.Storage.SerDe, afterFidelity.Storage.SerDe)
		assert.Equal(t, gotIceberg.TableType, afterFidelity.TableType)
	}

	delta := hms.NewDeltaTable(dbName, "delta_tbl", "file:///tmp/"+dbName+"/delta_tbl", cols)
	require.NoError(t, c.CreateTable(ctx, delta))
	gotDelta, err := c.GetTable(ctx, dbName, "delta_tbl")
	require.NoError(t, err)
	assert.Equal(t, hms.DeltaSerDe, gotDelta.Storage.SerDe.SerializationLib)
	assert.Equal(t, "delta", gotDelta.Parameters[hms.ParamSparkProvider])

	partKeys := []*hms.FieldSchema{{Name: "dt", Type: "string"}}
	hudi := hms.NewHudiTable(dbName, "hudi_tbl", "file:///tmp/"+dbName+"/hudi_tbl", cols, partKeys)
	require.NoError(t, c.CreateTable(ctx, hudi))
	gotHudi, err := c.GetTable(ctx, dbName, "hudi_tbl")
	require.NoError(t, err)
	assert.Equal(t, hms.HudiSerDe, gotHudi.Storage.SerDe.SerializationLib)
	assert.Equal(t, "hudi", gotHudi.Parameters[hms.ParamSparkProvider])
	require.Len(t, gotHudi.PartitionKeys, 1)
	assert.Equal(t, "dt", gotHudi.PartitionKeys[0].Name)

	// AlterTable: add a parameter to the Iceberg table and confirm it
	// sticks.
	altered := gotIceberg
	altered.Parameters["updated_by"] = "hms-client-go-integration"
	require.NoError(t, c.AlterTable(ctx, dbName, "iceberg_tbl", altered))
	gotAltered, err := c.GetTable(ctx, dbName, "iceberg_tbl")
	require.NoError(t, err)
	assert.Equal(t, "hms-client-go-integration", gotAltered.Parameters["updated_by"])

	// DropTable ifExists semantics.
	require.NoError(t, c.DropTable(ctx, dbName, "iceberg_tbl", false, false))
	err = c.DropTable(ctx, dbName, "iceberg_tbl", false, false)
	require.ErrorIs(t, err, hms.ErrNotFound)
	require.NoError(t, c.DropTable(ctx, dbName, "iceberg_tbl", false, true))
}

// TestPartitions_AddGetAlterDrop adds 1500 partitions to exercise
// AddPartitions' chunking (SPEC.md §2.3 Rule 5, defaultChunkSize=1000),
// reads them back both unbounded and with maxParts=10, lists their names,
// alters one's parameters, and drops it.
func TestPartitions_AddGetAlterDrop(t *testing.T) {
	t.Parallel()
	c := dial(t)
	ctx := context.Background()

	dbName := uniqueName("it_partdb_")
	createDB(t, c, ctx, dbName, "")

	const tableName = "parted"
	const partitionCount = 1500

	table := &hms.Table{
		DatabaseName:  dbName,
		TableName:     tableName,
		TableType:     hms.TableTypeManaged,
		PartitionKeys: []*hms.FieldSchema{{Name: "dt", Type: "string"}},
		Storage:       textStorage("file:///tmp/"+dbName+"/"+tableName, []*hms.FieldSchema{{Name: "id", Type: "bigint"}}),
	}
	require.NoError(t, c.CreateTable(ctx, table))

	parts := make([]*hms.Partition, partitionCount)
	for i := range parts {
		val := fmt.Sprintf("d%04d", i)
		parts[i] = &hms.Partition{
			DatabaseName: dbName,
			TableName:    tableName,
			Values:       []string{val},
			Storage:      textStorage(fmt.Sprintf("file:///tmp/%s/%s/dt=%s", dbName, tableName, val), nil),
		}
	}
	require.NoError(t, c.AddPartitions(ctx, dbName, tableName, parts, false))

	all, err := c.GetPartitions(ctx, dbName, tableName, -1)
	require.NoError(t, err)
	assert.Len(t, all, partitionCount)

	limited, err := c.GetPartitions(ctx, dbName, tableName, 10)
	require.NoError(t, err)
	assert.Len(t, limited, 10)

	names, err := c.GetPartitionNames(ctx, dbName, tableName, -1)
	require.NoError(t, err)
	assert.Len(t, names, partitionCount)

	// GetPartitionsByNames (1.0 addition, SPEC.md §5.5): look up a subset
	// of partitions by their computed names.
	wantNames := names[:5]
	byNames, err := c.GetPartitionsByNames(ctx, dbName, tableName, wantNames)
	require.NoError(t, err)
	assert.Len(t, byNames, 5)
	gotByNames := make(map[string]bool, len(byNames))
	for _, p := range byNames {
		require.Len(t, p.Values, 1)
		gotByNames["dt="+p.Values[0]] = true
	}
	for _, n := range wantNames {
		assert.True(t, gotByNames[n], "GetPartitionsByNames missing %s", n)
	}

	// GetPartitionsByFilter (1.0 addition, SPEC.md §5.5): a Hive
	// partition-filter expression on the "dt" key created above.
	byFilter, err := c.GetPartitionsByFilter(ctx, dbName, tableName, fmt.Sprintf("dt = '%s'", parts[0].Values[0]), -1)
	require.NoError(t, err)
	require.Len(t, byFilter, 1)
	assert.Equal(t, parts[0].Values, byFilter[0].Values)

	// GetPartitionNamesByValues (1.0 addition, SPEC.md §5.5): "dt" is this
	// table's only partition key, so a full value is itself the whole
	// "prefix".
	byValues, err := c.GetPartitionNamesByValues(ctx, dbName, tableName, []string{parts[1].Values[0]}, -1)
	require.NoError(t, err)
	assert.Equal(t, []string{"dt=" + parts[1].Values[0]}, byValues)

	altered := []*hms.Partition{all[0]}
	altered[0].Parameters = map[string]string{"altered_by": "hms-client-go-integration"}
	require.NoError(t, c.AlterPartitions(ctx, dbName, tableName, altered))

	afterAlter, err := c.GetPartitions(ctx, dbName, tableName, -1)
	require.NoError(t, err)
	var found bool
	for _, p := range afterAlter {
		if len(p.Values) == 1 && p.Values[0] == altered[0].Values[0] {
			found = true
			assert.Equal(t, "hms-client-go-integration", p.Parameters["altered_by"])
		}
	}
	assert.True(t, found, "altered partition %v not found after AlterPartitions", altered[0].Values)

	require.NoError(t, c.DropPartition(ctx, dbName, tableName, altered[0].Values, false, false))
	err = c.DropPartition(ctx, dbName, tableName, altered[0].Values, false, false)
	require.ErrorIs(t, err, hms.ErrNotFound)
	require.NoError(t, c.DropPartition(ctx, dbName, tableName, altered[0].Values, false, true))
}

// TestCatalogs covers SPEC.md §5.2 and §2.1's catalog row: on Hive 2.3,
// GetCatalogs must fail with ErrNotSupported; on 3.1 and 4.x, catalog and
// database (via InCatalog) round-trip.
func TestCatalogs(t *testing.T) {
	t.Parallel()
	c := dial(t)
	_, expectVersion := requireHMSEnv(t)
	ctx := context.Background()

	if expectVersion == "2.3" {
		_, err := c.GetCatalogs(ctx)
		require.ErrorIs(t, err, hms.ErrNotSupported)
		return
	}

	catName := uniqueName("it_cat_")
	cat := &hms.Catalog{
		Name:        catName,
		Description: "hms-client-go integration test catalog",
		LocationURI: "file:///tmp/" + catName,
	}
	require.NoError(t, c.CreateCatalog(ctx, cat))
	t.Cleanup(func() { _ = c.DropCatalog(context.Background(), catName, true) })

	gotCat, err := c.GetCatalog(ctx, catName)
	require.NoError(t, err)
	assert.Equal(t, catName, gotCat.Name)

	catNames, err := c.GetCatalogs(ctx)
	require.NoError(t, err)
	assert.Contains(t, catNames, catName)

	dbName := uniqueName("it_catdb_")
	createDB(t, c, ctx, dbName, catName)

	dbNames, err := c.GetAllDatabases(ctx, hms.InCatalog(catName))
	require.NoError(t, err)
	assert.Contains(t, dbNames, dbName)
}

// TestIdentity covers SPEC.md §3.1's set_ugi identity over binary NOSASL:
// dial's own hms.WithUser("ci") (see the dial helper and HMS_USER) must not
// break CreateDatabase/DropDatabase against any supported server version. A
// server that fills a created database's owner from the connection's UGI
// (observed on Hive 4.x) additionally reports OwnerName as HMS_USER; a
// server that does not (observed on Hive 2.3/3.1, which leave it unset)
// only has the success of the round trip asserted, since there is nothing
// else here for those versions to prove set_ugi actually ran.
func TestIdentity(t *testing.T) {
	t.Parallel()
	c := dial(t)
	_, expectVersion := requireHMSEnv(t)
	ctx := context.Background()

	user := os.Getenv(envUser)
	require.NotEmpty(t, user, "%s must be set for TestIdentity", envUser)

	name := uniqueName("it_identity_db_")
	createDB(t, c, ctx, name, "")

	got, err := c.GetDatabase(ctx, name)
	require.NoError(t, err)
	assert.Equal(t, name, got.Name)

	if expectVersion == "4.0" || expectVersion == "4.2" {
		assert.Equal(t, user, got.OwnerName, "Hive 4.x fills a created database's owner from the set_ugi identity")
	}
}

// TestFallbacks covers SPEC.md §2.3 Rules 3-4: on a server lacking
// get_partitions_req (Hive 2.3, 3.1), GetPartitions degrades to the legacy
// get_partitions RPC on UNKNOWN_METHOD, and the degraded probe result is
// cached per connection, so a second call on the same pooled connection
// succeeds without repeating the probe. On 4.x, get_partitions_req is used
// natively both times. Either way, both calls must succeed with the same
// result -- WithPoolSize(1) keeps every call on the same connection so the
// cache is actually exercised end to end, not just internally.
func TestFallbacks(t *testing.T) {
	t.Parallel()
	c := dial(t, hms.WithPoolSize(1))
	ctx := context.Background()

	dbName := uniqueName("it_fbdb_")
	createDB(t, c, ctx, dbName, "")

	const tableName = "fallback_tbl"
	table := &hms.Table{
		DatabaseName:  dbName,
		TableName:     tableName,
		TableType:     hms.TableTypeManaged,
		PartitionKeys: []*hms.FieldSchema{{Name: "dt", Type: "string"}},
		Storage:       textStorage("file:///tmp/"+dbName+"/"+tableName, []*hms.FieldSchema{{Name: "id", Type: "bigint"}}),
	}
	require.NoError(t, c.CreateTable(ctx, table))

	parts := []*hms.Partition{
		{DatabaseName: dbName, TableName: tableName, Values: []string{"a"}, Storage: textStorage("file:///tmp/"+dbName+"/"+tableName+"/dt=a", nil)},
		{DatabaseName: dbName, TableName: tableName, Values: []string{"b"}, Storage: textStorage("file:///tmp/"+dbName+"/"+tableName+"/dt=b", nil)},
	}
	require.NoError(t, c.AddPartitions(ctx, dbName, tableName, parts, false))

	first, err := c.GetPartitions(ctx, dbName, tableName, -1)
	require.NoError(t, err, "first GetPartitions call (probes/uses get_partitions_req)")
	assert.Len(t, first, len(parts))

	second, err := c.GetPartitions(ctx, dbName, tableName, -1)
	require.NoError(t, err, "second GetPartitions call (must reuse the cached probe result, not re-probe)")
	assert.Len(t, second, len(parts))
}

// TestServerVersion checks that Client.ServerVersion reports a version
// whose major component matches HMS_EXPECT_VERSION (SPEC.md §5.6).
//
// Only Major is compared, not Minor: fb303's getVersion does not always
// report the release, and every Hive 3.1.x metastore answers the
// metastore schema line "3.0" instead of a release number (see
// HiveVersion's and ParseHiveVersion's doc comments in types.go). The
// schema line and the release agree on Major but not necessarily on
// Minor, so asserting Minor here would make this test version-fragile for
// no real coverage gain; Minor >= 0 confirms it parsed as a version
// component at all.
func TestServerVersion(t *testing.T) {
	t.Parallel()
	c := dial(t)
	_, expectVersion := requireHMSEnv(t)
	expectMajor, _, _ := strings.Cut(expectVersion, ".")

	v, err := c.ServerVersion(context.Background())
	require.NoError(t, err)
	assert.Equal(t, expectMajor, strconv.Itoa(v.Major))
	assert.GreaterOrEqual(t, v.Minor, 0)
}

// TestNotifications covers SPEC.md §5.7's notification polling on every
// supported version: CurrentNotificationID taken before a CreateTable call
// must be strictly less than the ID of the CREATE_TABLE event that call
// produces, and GetNextNotifications(sinceID, 100, nil) must return that
// event with the created table's name in its Message. The real
// metastore's message body is JSON carrying (at least) "db" and "table"
// keys on every supported version; this only checks the table name appears
// in Message rather than parsing it, since the exact JSON shape is a Hive
// implementation detail this package does not otherwise depend on.
func TestNotifications(t *testing.T) {
	t.Parallel()
	c := dial(t)
	ctx := context.Background()

	dbName := uniqueName("it_notifdb_")
	createDB(t, c, ctx, dbName, "")

	sinceID, err := c.CurrentNotificationID(ctx)
	require.NoError(t, err)

	tableName := "it_notif_tbl"
	table := &hms.Table{
		DatabaseName: dbName,
		TableName:    tableName,
		TableType:    hms.TableTypeManaged,
		Storage:      textStorage("file:///tmp/"+dbName+"/"+tableName, []*hms.FieldSchema{{Name: "id", Type: "bigint"}}),
	}
	require.NoError(t, c.CreateTable(ctx, table))

	events, err := c.GetNextNotifications(ctx, sinceID, 100, nil)
	require.NoError(t, err)

	// A metastore with no notification listener answers both calls
	// successfully and reports nothing at all: CurrentNotificationID stays
	// 0 because NOTIFICATION_SEQUENCE was never advanced, and the event log
	// is empty. That is a server configuration this test cannot assert
	// against, not a client defect -- the listener is registered with
	// metastore.transactional.event.listeners (hive.metastore.transactional
	// .event.listeners before Hive 3), which the integration workflow sets
	// when the image has the hcatalog server extensions to load it from.
	if sinceID == 0 && len(events) == 0 {
		t.Skip("metastore has no DbNotificationListener configured (metastore.transactional.event.listeners)")
	}

	var found bool
	for _, ev := range events {
		assert.Greater(t, ev.ID, sinceID, "every returned event must be newer than sinceID")
		if ev.Type == "CREATE_TABLE" && strings.Contains(decodeNotificationMessage(t, ev), tableName) {
			found = true
		}
	}
	assert.True(t, found, "no CREATE_TABLE event for %s.%s found in %d events after %d", dbName, tableName, len(events), sinceID)
}

// TestACID covers SPEC.md §5.9's minimal ACID surface against a real
// metastore: open a transaction, lock SHARED_READ on the test db/table,
// confirm CheckLock reports the same ACQUIRED state Lock did, and commit;
// a second transaction exercises the abort path instead. Neither subtest
// calls Unlock on the transactional lock: a real metastore ties a lock
// acquired with TxnID set to that transaction's lifecycle and releases it
// automatically on commit_txn/abort_txn, rejecting an explicit unlock on
// it with TxnOpenException ("Unlocking locks associated with transaction
// not permitted") -- verified against a real Hive 4.2.1 server, which
// returned exactly that once the OperationType fix below (acid.go's
// lockOperationType) let the lock RPC itself succeed. Unlock's own
// contract (releasing a lock taken without a transaction) is exercised by
// TestACID_Heartbeat_TxnOnlyAndLockOnly and friends in acid_test.go
// against the in-process fake server. open_txns/lock/check_lock/
// commit_txn/abort_txn all exist on every supported version (Hive 2.3+,
// SPEC §5.9), so this is not version-gated.
func TestACID(t *testing.T) {
	t.Parallel()
	c := dial(t)
	ctx := context.Background()

	dbName := uniqueName("it_aciddb_")
	createDB(t, c, ctx, dbName, "")
	const tableName = "it_acid_tbl"

	t.Run("commit", func(t *testing.T) {
		txnID, err := c.OpenTransaction(ctx, "hms-client-go-integration", "localhost")
		require.NoError(t, err)
		assert.Greater(t, txnID, int64(0))

		resp, err := c.Lock(ctx, hms.LockRequest{
			Components: []hms.LockComponent{
				{Type: hms.LockTypeSharedRead, Level: hms.LockLevelTable, Database: dbName, Table: tableName},
			},
			TxnID: txnID,
			User:  "hms-client-go-integration",
			Host:  "localhost",
		})
		require.NoError(t, err)
		require.Equal(t, hms.LockStateAcquired, resp.State)
		require.Greater(t, resp.LockID, int64(0))

		checked, err := c.CheckLock(ctx, resp.LockID)
		require.NoError(t, err)
		assert.Equal(t, hms.LockStateAcquired, checked.State)
		assert.Equal(t, resp.LockID, checked.LockID)

		require.NoError(t, c.CommitTransaction(ctx, txnID))
	})

	t.Run("abort", func(t *testing.T) {
		txnID, err := c.OpenTransaction(ctx, "hms-client-go-integration", "localhost")
		require.NoError(t, err)

		resp, err := c.Lock(ctx, hms.LockRequest{
			Components: []hms.LockComponent{
				{Type: hms.LockTypeSharedRead, Level: hms.LockLevelTable, Database: dbName, Table: tableName},
			},
			TxnID: txnID,
			User:  "hms-client-go-integration",
			Host:  "localhost",
		})
		require.NoError(t, err)
		require.Equal(t, hms.LockStateAcquired, resp.State)

		require.NoError(t, c.AbortTransaction(ctx, txnID))

		err = c.CommitTransaction(ctx, txnID)
		require.ErrorIs(t, err, hms.ErrInvalidOperation, "committing an aborted transaction must fail")
	})
}

// TestTLS connects to a metastore configured with metastore.use.SSL=true
// via hms.WithTLS (SPEC §3.1) and confirms a basic RPC round-trips over
// the encrypted socket. It skips unless HMS_TLS_URIS is set; see
// envTLSURIs's doc comment.
func TestTLS(t *testing.T) {
	t.Parallel()
	uris := os.Getenv(envTLSURIs)
	if uris == "" {
		t.Skipf("%s is not set; skipping TLS integration test (see PLAN.md Slice 9)", envTLSURIs)
	}

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	c, err := hms.New(ctx, uris, hms.WithTLS(&tls.Config{}))
	require.NoError(t, err, "connecting to %s over TLS", uris)
	t.Cleanup(func() { _ = c.Close() })

	_, err = c.ServerVersion(context.Background())
	require.NoError(t, err)
}

// TestKerberos connects to a Kerberized metastore with hms.WithKerberos,
// which authenticates over SASL GSSAPI at QOP auth (SPEC §3.1), and
// confirms a basic RPC round-trips over the resulting connection. It skips
// unless HMS_KRB5_URIS is set; see envKrb5URIs's doc comment.
func TestKerberos(t *testing.T) {
	t.Parallel()
	uris := os.Getenv(envKrb5URIs)
	if uris == "" {
		t.Skipf("%s is not set; skipping Kerberos integration test (see PLAN.md Slice 14)", envKrb5URIs)
	}

	opts := []hms.Option{hms.WithKerberos(os.Getenv(envKrb5Principal))}
	if kt := os.Getenv(envKrb5Keytab); kt != "" {
		opts = []hms.Option{hms.WithKerberos(os.Getenv(envKrb5Principal), kt)}
	}

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	c, err := hms.New(ctx, uris, opts...)
	require.NoError(t, err, "connecting to %s with Kerberos", uris)
	t.Cleanup(func() { _ = c.Close() })

	_, err = c.ServerVersion(context.Background())
	require.NoError(t, err)
}
