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
			assert.Equal(t, &want, got)

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
	restore := hms.SetChunkSizeForTest(2)
	defer restore()

	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI())
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
