package hms_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hms "github.com/slachiewicz/hms-client-go"
	"github.com/slachiewicz/hms-client-go/internal/hmstest"
)

func TestNewIcebergTable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name               string
		dbName             string
		tableName          string
		location           string
		metadataLocation   string
		cols               []*hms.FieldSchema
		wantStorageHandler string
		wantSerDe          string
		wantInputFormat    string
		wantOutputFormat   string
		wantTableType      hms.TableType
		wantParamTableType string
		wantParamMetadata  string
		wantParamHandler   string
		wantParamExternal  string
		wantParamCatalog   string
	}{
		{
			name:               "basic iceberg table",
			dbName:             "db",
			tableName:          "tbl",
			location:           "s3://bucket/db/tbl",
			metadataLocation:   "s3://bucket/db/tbl/metadata",
			cols:               []*hms.FieldSchema{{Name: "id", Type: "bigint"}},
			wantStorageHandler: hms.IcebergStorageHandler,
			wantSerDe:          hms.IcebergSerDe,
			wantInputFormat:    hms.IcebergInputFormat,
			wantOutputFormat:   hms.IcebergOutputFormat,
			wantTableType:      hms.TableTypeExternal,
			wantParamTableType: "ICEBERG",
			wantParamMetadata:  "s3://bucket/db/tbl/metadata",
			wantParamHandler:   hms.IcebergStorageHandler,
			wantParamExternal:  "TRUE",
			wantParamCatalog:   "location_based_table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := hms.NewIcebergTable(tt.dbName, tt.tableName, tt.location, tt.metadataLocation, tt.cols)

			assert.Equal(t, tt.dbName, got.DatabaseName)
			assert.Equal(t, tt.tableName, got.TableName)
			assert.Equal(t, tt.wantTableType, got.TableType)
			assert.Equal(t, tt.location, got.Storage.Location)
			assert.Equal(t, tt.wantInputFormat, got.Storage.InputFormat)
			assert.Equal(t, tt.wantOutputFormat, got.Storage.OutputFormat)
			assert.Equal(t, tt.wantSerDe, got.Storage.SerDe.SerializationLib)
			assert.Equal(t, tt.cols, got.Storage.Columns)

			assert.Equal(t, tt.wantParamTableType, got.Parameters["table_type"])
			assert.Equal(t, tt.wantParamMetadata, got.Parameters["metadata_location"])
			assert.Equal(t, tt.wantParamHandler, got.Parameters["storage_handler"])
			assert.Equal(t, tt.wantParamExternal, got.Parameters["EXTERNAL"])
			assert.Equal(t, tt.wantParamCatalog, got.Parameters["iceberg.catalog"])

			// Verify storage_handler constant matches literal
			assert.Equal(t, "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler", got.Parameters["storage_handler"])
		})
	}
}

func TestNewDeltaTable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name               string
		dbName             string
		tableName          string
		location           string
		cols               []*hms.FieldSchema
		wantSerDe          string
		wantStorageHandler string
		wantTableType      hms.TableType
		wantParamProvider  string
		wantParamTableType string
		wantParamExternal  string
	}{
		{
			name:               "basic delta table",
			dbName:             "db",
			tableName:          "tbl",
			location:           "s3://bucket/db/tbl",
			cols:               []*hms.FieldSchema{{Name: "id", Type: "bigint"}},
			wantSerDe:          hms.DeltaSerDe,
			wantStorageHandler: hms.DeltaStorageHandler,
			wantTableType:      hms.TableTypeExternal,
			wantParamProvider:  "delta",
			wantParamTableType: "DELTA",
			wantParamExternal:  "TRUE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := hms.NewDeltaTable(tt.dbName, tt.tableName, tt.location, tt.cols)

			assert.Equal(t, tt.dbName, got.DatabaseName)
			assert.Equal(t, tt.tableName, got.TableName)
			assert.Equal(t, tt.wantTableType, got.TableType)
			assert.Equal(t, tt.location, got.Storage.Location)
			// Delta is registered by storage handler, not an input/output
			// format pair (SPEC §6).
			assert.Empty(t, got.Storage.InputFormat)
			assert.Empty(t, got.Storage.OutputFormat)
			assert.Equal(t, tt.wantSerDe, got.Storage.SerDe.SerializationLib)
			assert.Equal(t, tt.cols, got.Storage.Columns)
			assert.Equal(t, "1", got.Storage.SerDe.Parameters["serialization.format"])
			assert.Equal(t, tt.location, got.Storage.SerDe.Parameters["path"])

			assert.Equal(t, tt.wantParamProvider, got.Parameters["spark.sql.sources.provider"])
			assert.Equal(t, tt.wantParamTableType, got.Parameters["table_type"])
			assert.Equal(t, tt.wantParamExternal, got.Parameters["EXTERNAL"])
			assert.Equal(t, tt.wantStorageHandler, got.Parameters["storage_handler"])

			// Verify constants match literals
			assert.Equal(t, "io.delta.hive.DeltaStorageHandler", got.Parameters["storage_handler"])
			assert.Equal(t, "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", got.Storage.SerDe.SerializationLib)
		})
	}
}

func TestNewHudiTable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name              string
		dbName            string
		tableName         string
		location          string
		cols              []*hms.FieldSchema
		partitionKeys     []*hms.FieldSchema
		wantSerDe         string
		wantInputFormat   string
		wantOutputFormat  string
		wantTableType     hms.TableType
		wantParamProvider string
		wantParamExternal string
	}{
		{
			name:              "basic hudi table",
			dbName:            "db",
			tableName:         "tbl",
			location:          "s3://bucket/db/tbl",
			cols:              []*hms.FieldSchema{{Name: "id", Type: "bigint"}},
			partitionKeys:     []*hms.FieldSchema{{Name: "ds", Type: "string"}},
			wantSerDe:         hms.HudiSerDe,
			wantInputFormat:   hms.HudiInputFormat,
			wantOutputFormat:  hms.HudiOutputFormat,
			wantTableType:     hms.TableTypeExternal,
			wantParamProvider: "hudi",
			wantParamExternal: "TRUE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := hms.NewHudiTable(tt.dbName, tt.tableName, tt.location, tt.cols, tt.partitionKeys)

			assert.Equal(t, tt.dbName, got.DatabaseName)
			assert.Equal(t, tt.tableName, got.TableName)
			assert.Equal(t, tt.wantTableType, got.TableType)
			assert.Equal(t, tt.location, got.Storage.Location)
			assert.Equal(t, tt.wantInputFormat, got.Storage.InputFormat)
			assert.Equal(t, tt.wantOutputFormat, got.Storage.OutputFormat)
			assert.Equal(t, tt.wantSerDe, got.Storage.SerDe.SerializationLib)
			assert.Equal(t, tt.cols, got.Storage.Columns)
			assert.Equal(t, tt.partitionKeys, got.PartitionKeys)
			assert.Equal(t, tt.location, got.Storage.SerDe.Parameters["path"])

			assert.Equal(t, tt.wantParamProvider, got.Parameters["spark.sql.sources.provider"])
			assert.Equal(t, tt.wantParamExternal, got.Parameters["EXTERNAL"])

			// Verify constants match literals
			assert.Equal(t, "org.apache.hudi.hadoop.HoodieParquetInputFormat", got.Storage.InputFormat)
			assert.Equal(t, "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat", got.Storage.OutputFormat)
			assert.Equal(t, "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", got.Storage.SerDe.SerializationLib)
		})
	}
}

func TestSetIcebergMetadataLocation(t *testing.T) {
	t.Parallel()
	t.Run("moves current metadata_location to previous", func(t *testing.T) {
		t.Parallel()
		table := hms.NewIcebergTable("db", "tbl", "s3://bucket/db/tbl", "s3://bucket/db/tbl/metadata/v1", []*hms.FieldSchema{})
		assert.Equal(t, "s3://bucket/db/tbl/metadata/v1", table.Parameters["metadata_location"])

		hms.SetIcebergMetadataLocation(table, "s3://bucket/db/tbl/metadata/v2")
		assert.Equal(t, "s3://bucket/db/tbl/metadata/v2", table.Parameters["metadata_location"])
		assert.Equal(t, "s3://bucket/db/tbl/metadata/v1", table.Parameters["previous_metadata_location"])
	})

	t.Run("sets metadata_location when none exists", func(t *testing.T) {
		t.Parallel()
		table := &hms.Table{
			DatabaseName: "db",
			TableName:    "tbl",
			TableType:    hms.TableTypeExternal,
			Parameters:   map[string]string{"EXTERNAL": "TRUE"},
		}
		hms.SetIcebergMetadataLocation(table, "s3://bucket/db/tbl/metadata/v1")
		assert.Equal(t, "s3://bucket/db/tbl/metadata/v1", table.Parameters["metadata_location"])
		assert.NotContains(t, table.Parameters, "previous_metadata_location")
	})

	t.Run("initializes Parameters if nil", func(t *testing.T) {
		t.Parallel()
		table := &hms.Table{DatabaseName: "db", TableName: "tbl"}
		assert.Nil(t, table.Parameters)
		hms.SetIcebergMetadataLocation(table, "s3://bucket/db/tbl/metadata/v1")
		require.NotNil(t, table.Parameters)
		assert.Equal(t, "s3://bucket/db/tbl/metadata/v1", table.Parameters["metadata_location"])
	})

	t.Run("multiple rotations", func(t *testing.T) {
		t.Parallel()
		table := hms.NewIcebergTable("db", "tbl", "s3://bucket/db/tbl", "s3://bucket/db/tbl/metadata/v1", []*hms.FieldSchema{})
		hms.SetIcebergMetadataLocation(table, "s3://bucket/db/tbl/metadata/v2")
		hms.SetIcebergMetadataLocation(table, "s3://bucket/db/tbl/metadata/v3")

		assert.Equal(t, "s3://bucket/db/tbl/metadata/v3", table.Parameters["metadata_location"])
		assert.Equal(t, "s3://bucket/db/tbl/metadata/v2", table.Parameters["previous_metadata_location"])
	})
}

func TestIcebergTableRoundTrip(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI())
	ctx := context.Background()

	cols := []*hms.FieldSchema{
		{Name: "id", Type: "bigint"},
		{Name: "data", Type: "string"},
	}
	table := hms.NewIcebergTable("db", "iceberg_tbl", "s3://bucket/db/iceberg_tbl", "s3://bucket/db/iceberg_tbl/metadata", cols)

	require.NoError(t, c.CreateTable(ctx, table))

	got, err := c.GetTable(ctx, "db", "iceberg_tbl")
	require.NoError(t, err)

	assert.Equal(t, hms.TableTypeExternal, got.TableType)
	assert.Equal(t, "s3://bucket/db/iceberg_tbl", got.Storage.Location)
	assert.Equal(t, hms.IcebergInputFormat, got.Storage.InputFormat)
	assert.Equal(t, hms.IcebergOutputFormat, got.Storage.OutputFormat)
	assert.Equal(t, hms.IcebergSerDe, got.Storage.SerDe.SerializationLib)
	assert.Equal(t, "ICEBERG", got.Parameters["table_type"])
	assert.Equal(t, "s3://bucket/db/iceberg_tbl/metadata", got.Parameters["metadata_location"])
	assert.Equal(t, hms.IcebergStorageHandler, got.Parameters["storage_handler"])
	assert.Equal(t, "TRUE", got.Parameters["EXTERNAL"])
	assert.Equal(t, "location_based_table", got.Parameters["iceberg.catalog"])
	assert.Equal(t, cols, got.Storage.Columns)
}
