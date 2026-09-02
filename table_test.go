package hms_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hms "github.com/slachiewicz/hms-client-go"
	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
	"github.com/slachiewicz/hms-client-go/internal/hmstest"
)

func TestTable_CreateGetRoundTrip(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		v    hmstest.Version
	}{
		{"hive23", hmstest.Hive23},
		{"hive31", hmstest.Hive31},
		{"hive40", hmstest.Hive40},
	}

	for _, tt := range tests {
		v, name := tt.v, tt.name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := hmstest.Start(t, v)
			c := mustNew(t, srv.URI())
			ctx := context.Background()

			table := &hms.Table{
				DatabaseName: "db",
				TableName:    "iceberg_tbl",
				Owner:        "me",
				Storage: &hms.StorageDescriptor{
					Columns: []*hms.FieldSchema{
						{Name: "id", Type: "bigint"},
						{Name: "data", Type: "string"},
					},
					Location:     "s3://bucket/db/iceberg_tbl",
					InputFormat:  "org.apache.iceberg.mr.hive.HiveIcebergInputFormat",
					OutputFormat: "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat",
					SerDe: &hms.SerDeInfo{
						Name:             "iceberg",
						SerializationLib: "org.apache.iceberg.mr.hive.HiveIcebergSerDe",
					},
					Parameters: map[string]string{"write.format.default": "parquet"},
				},
				PartitionKeys: []*hms.FieldSchema{{Name: "ds", Type: "string"}},
				Parameters:    map[string]string{"table_type": "ICEBERG"},
				TableType:     hms.TableTypeExternal,
			}
			require.NoError(t, c.CreateTable(ctx, table))

			got, err := c.GetTable(ctx, "db", "iceberg_tbl")
			require.NoError(t, err)
			require.False(t, got.CreateTime.IsZero())

			want := *table
			want.CatalogName = "hive"
			want.CreateTime = got.CreateTime
			// OwnerType defaults to PrincipalUser on write (1.0 addition;
			// see Table's doc comment) when left unset, as table is here.
			want.OwnerType = hms.PrincipalUser
			// got carries the round-trip fidelity snapshot GetTable
			// populates (Table's raw field); strip it before this
			// whole-struct equality check so it does not fail on an
			// unexported field want (a plain struct literal) never has.
			assert.Equal(t, &want, hms.StripTableRaw(got))

			args, ok := srv.LastArgs("get_table_req").(*hive_metastore.GetTableRequest)
			require.True(t, ok)
			if v == hmstest.Hive23 {
				assert.Nil(t, args.CatName)
			} else {
				require.NotNil(t, args.CatName)
				assert.Equal(t, "hive", *args.CatName)
			}
		})
	}
}

// TestAlterTable_PreservesUnmodelledFields is round-trip fidelity's
// black-box counterpart to TestTableRoundTrip_PreservesUnmodelledFields
// (convert_internal_test.go): it seeds a table carrying fields hms.Table
// has no field for directly into the Hive40 fixture's store (bypassing
// CreateTable), fetches it with GetTable, changes one Parameter, and
// AlterTables it back, then reads the store directly and confirms the
// unmodelled fields are still there -- proving the fidelity holds across
// an actual client round trip, not just the bare converter functions.
func TestAlterTable_PreservesUnmodelledFields(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI())
	ctx := context.Background()

	catName := "hive"
	rewriteEnabled := true
	id := int64(7)
	seed := hive_metastore.NewTable()
	seed.DbName = "db"
	seed.TableName = "t"
	seed.CatName = &catName
	seed.Parameters = map[string]string{"x": "1"}
	seed.RewriteEnabled = &rewriteEnabled
	seed.ID = &id
	seed.Privileges = &hive_metastore.PrincipalPrivilegeSet{
		UserPrivileges: map[string][]*hive_metastore.PrivilegeGrantInfo{
			"alice": {{Privilege: "SELECT"}},
		},
	}
	srv.SeedTable(seed)

	got, err := c.GetTable(ctx, "db", "t")
	require.NoError(t, err)

	got.Parameters["x"] = "2"
	require.NoError(t, c.AlterTable(ctx, "db", "t", got))

	// Read the persisted table back through the client rather than peeking
	// at srv.Store().Tables directly, which would race the store's own
	// lock against the handler goroutine servicing this connection; the
	// exported TableRaw test hook then reaches the fields hms.Table itself
	// has no field for, off the fresh GetTable response's own raw
	// snapshot.
	after, err := c.GetTable(ctx, "db", "t")
	require.NoError(t, err)
	stored := hms.TableRaw(after)
	require.NotNil(t, stored)
	assert.Equal(t, "2", stored.Parameters["x"], "the modelled field this test actually changed must still take effect")
	require.NotNil(t, stored.RewriteEnabled, "RewriteEnabled must survive: hms.Table has no field for it")
	assert.True(t, *stored.RewriteEnabled)
	require.NotNil(t, stored.ID, "ID must survive: hms.Table has no field for it")
	assert.Equal(t, int64(7), *stored.ID)
	require.NotNil(t, stored.Privileges, "Privileges must survive: hms.Table has no field for it")
	assert.Contains(t, stored.Privileges.UserPrivileges, "alice")
}

// TestCreateTable_DoesNotCarrySnapshot is the create-path counterpart to
// TestAlterTable_PreservesUnmodelledFields: the round-trip fidelity
// snapshot is scoped to the Alter* calls (SPEC §5.4), so a Table fetched
// with GetTable and then handed to CreateTable as the template for a new
// table must not carry the source table's server-assigned identity -- Id,
// TxnId, WriteId, Privileges -- onto the wire as the definition of the new
// one.
func TestCreateTable_DoesNotCarrySnapshot(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI())
	ctx := context.Background()

	catName := "hive"
	id := int64(7)
	txnID := int64(99)
	seed := hive_metastore.NewTable()
	seed.DbName = "db"
	seed.TableName = "source"
	seed.CatName = &catName
	seed.Parameters = map[string]string{"x": "1"}
	seed.ID = &id
	seed.TxnId = &txnID
	seed.WriteId = 5
	seed.Privileges = &hive_metastore.PrincipalPrivilegeSet{
		UserPrivileges: map[string][]*hive_metastore.PrivilegeGrantInfo{
			"alice": {{Privilege: "SELECT"}},
		},
	}
	srv.SeedTable(seed)

	got, err := c.GetTable(ctx, "db", "source")
	require.NoError(t, err)

	got.TableName = "copy"
	require.NoError(t, c.CreateTable(ctx, got))

	sent, ok := srv.LastArgs("create_table").(*hive_metastore.Table)
	require.True(t, ok, "create_table args have unexpected type %T", srv.LastArgs("create_table"))
	assert.Equal(t, "copy", sent.TableName)
	assert.Equal(t, "1", sent.Parameters["x"], "a modelled field still travels")
	assert.Nil(t, sent.ID, "the source table's id must not define the new table")
	assert.Nil(t, sent.TxnId, "the source table's transaction id must not define the new table")
	assert.Nil(t, sent.Privileges, "the source table's privileges must not define the new table")
	assert.Equal(t, int64(-1), sent.WriteId, "a created table's write id is unassigned, not the source's")
}

func TestGetTable_NotFound(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI())

	_, err := c.GetTable(context.Background(), "db", "nope")
	require.ErrorIs(t, err, hms.ErrNotFound)
}

func TestGetAllTables(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI())
	ctx := context.Background()

	for _, n := range []string{"b", "a", "c"} {
		require.NoError(t, c.CreateTable(ctx, &hms.Table{DatabaseName: "db", TableName: n}))
	}

	names, err := c.GetAllTables(ctx, "db")
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, names)
}

func TestGetTables_ChunkedRequestOrder(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI(), hms.WithChunkSize(2))
	ctx := context.Background()

	// Only t0, t2, and t4 exist; t1 and t3 are requested but unknown to
	// the server and must be silently skipped, in request order.
	for _, n := range []string{"t0", "t2", "t4"} {
		require.NoError(t, c.CreateTable(ctx, &hms.Table{DatabaseName: "db", TableName: n}))
	}

	tables, err := c.GetTables(ctx, "db", []string{"t0", "t1", "t2", "t3", "t4"})
	require.NoError(t, err)

	var got []string
	for _, tbl := range tables {
		got = append(got, tbl.TableName)
	}
	assert.Equal(t, []string{"t0", "t2", "t4"}, got)

	n := 0
	for _, call := range srv.Calls() {
		if call == "get_table_objects_by_name_req" {
			n++
		}
	}
	assert.Equal(t, 3, n)
}

func TestAlterTable_UpdatesParameters(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI())
	ctx := context.Background()

	table := &hms.Table{DatabaseName: "db", TableName: "t", Parameters: map[string]string{"x": "1"}}
	require.NoError(t, c.CreateTable(ctx, table))

	newTable := &hms.Table{DatabaseName: "db", TableName: "t", Parameters: map[string]string{"x": "2"}}
	require.NoError(t, c.AlterTable(ctx, "db", "t", newTable))

	got, err := c.GetTable(ctx, "db", "t")
	require.NoError(t, err)
	assert.Equal(t, "2", got.Parameters["x"])
}

// TestAlterTable_CatalogPrecedence covers SPEC §5.0's catalog resolution
// order for AlterTable, the 1.0 fix in decision item 3: before it,
// AlterTable looked only at its opts ...CatalogOption and ignored
// newTable.CatalogName entirely.
func TestAlterTable_CatalogPrecedence(t *testing.T) {
	t.Parallel()

	t.Run("newTable.CatalogName selects the catalog, InCatalog overrides it", func(t *testing.T) {
		t.Parallel()
		srv := hmstest.Start(t, hmstest.Hive40)
		c := mustNew(t, srv.URI())
		ctx := context.Background()

		require.NoError(t, c.CreateTable(ctx, &hms.Table{CatalogName: "spark", DatabaseName: "db", TableName: "t"}))

		newTable := &hms.Table{CatalogName: "spark", DatabaseName: "db", TableName: "t", Parameters: map[string]string{"x": "1"}}
		require.NoError(t, c.AlterTable(ctx, "db", "t", newTable))

		args, ok := srv.LastArgs("alter_table").(hmstest.AlterTableArgs)
		require.True(t, ok)
		require.NotNil(t, args.NewTbl.CatName)
		assert.Equal(t, "spark", *args.NewTbl.CatName)

		// A per-call InCatalog overrides the struct's own CatalogName.
		require.NoError(t, c.CreateTable(ctx, &hms.Table{DatabaseName: "db2", TableName: "t2"}))
		newTable2 := &hms.Table{CatalogName: "spark", DatabaseName: "db2", TableName: "t2"}
		require.NoError(t, c.AlterTable(ctx, "db2", "t2", newTable2, hms.InCatalog("hive")))

		args2, ok := srv.LastArgs("alter_table").(hmstest.AlterTableArgs)
		require.True(t, ok)
		require.NotNil(t, args2.NewTbl.CatName)
		assert.Equal(t, "hive", *args2.NewTbl.CatName)
	})

	t.Run("struct catalog on Hive23 returns ErrNotSupported without issuing the RPC", func(t *testing.T) {
		t.Parallel()
		srv := hmstest.Start(t, hmstest.Hive23)
		c := mustNew(t, srv.URI())
		ctx := context.Background()

		newTable := &hms.Table{CatalogName: "spark", DatabaseName: "db", TableName: "t"}
		err := c.AlterTable(ctx, "db", "t", newTable)
		require.ErrorIs(t, err, hms.ErrNotSupported)

		assert.NotContains(t, srv.Calls(), "alter_table")
	})
}

func TestDropTable(t *testing.T) {
	t.Parallel()

	t.Run("ifExists false on missing table is not found", func(t *testing.T) {
		t.Parallel()
		srv := hmstest.Start(t, hmstest.Hive40)
		c := mustNew(t, srv.URI())

		err := c.DropTable(context.Background(), "db", "nope", false, false)
		require.ErrorIs(t, err, hms.ErrNotFound)
	})

	t.Run("ifExists true on missing table is nil", func(t *testing.T) {
		t.Parallel()
		srv := hmstest.Start(t, hmstest.Hive40)
		c := mustNew(t, srv.URI())

		err := c.DropTable(context.Background(), "db", "nope", false, true)
		require.NoError(t, err)
	})

	t.Run("deleteData is forwarded", func(t *testing.T) {
		t.Parallel()
		srv := hmstest.Start(t, hmstest.Hive40)
		c := mustNew(t, srv.URI())
		ctx := context.Background()

		require.NoError(t, c.CreateTable(ctx, &hms.Table{DatabaseName: "db", TableName: "t"}))
		require.NoError(t, c.DropTable(ctx, "db", "t", true, false))

		args, ok := srv.LastArgs("drop_table").(hmstest.DropTableArgs)
		require.True(t, ok)
		assert.True(t, args.DeleteData)
	})
}
