package hms_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hms "github.com/slachiewicz/hms-client-go"
	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
	"github.com/slachiewicz/hms-client-go/hmstest"
)

// TestGetTableColumnStatistics covers SPEC §5.8's GetTableColumnStatistics
// across every supported version: a seeded round trip (SeedColumnStats,
// since this package's GetTableColumnStatistics has no write counterpart to
// create fixtures through), an unknown column returning an empty result
// rather than an error, an empty columns argument returning (nil, nil)
// without issuing the RPC at all, and (on Hive23, which predates catalogs)
// that the resolved catalog is never written to the wire.
func TestGetTableColumnStatistics(t *testing.T) {
	t.Parallel()
	for _, tt := range partitionVersions {
		v, name := tt.v, tt.name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := hmstest.Start(t, v)
			c := mustNew(t, srv.URI())
			ctx := context.Background()

			require.NoError(t, c.CreateTable(ctx, &hms.Table{
				DatabaseName: "db",
				TableName:    "t",
				Storage: &hms.StorageDescriptor{Columns: []*hms.FieldSchema{
					{Name: "a", Type: "bigint"},
					{Name: "b", Type: "string"},
				}},
			}))

			low, high := int64(1), int64(100)
			srv.SeedColumnStats("db", "t",
				&hive_metastore.ColumnStatisticsObj{
					ColName: "a", ColType: "bigint",
					StatsData: &hive_metastore.ColumnStatisticsData{
						LongStats: &hive_metastore.LongColumnStatsData{LowValue: &low, HighValue: &high, NumNulls: 2, NumDVs: 50},
					},
				},
				&hive_metastore.ColumnStatisticsObj{
					ColName: "b", ColType: "string",
					StatsData: &hive_metastore.ColumnStatisticsData{
						StringStats: &hive_metastore.StringColumnStatsData{MaxColLen: 10, AvgColLen: 5.5, NumNulls: 1, NumDVs: 20},
					},
				},
			)

			got, err := c.GetTableColumnStatistics(ctx, "db", "t", []string{"a", "b"})
			require.NoError(t, err)
			require.Len(t, got, 2)

			assert.Equal(t, "a", got[0].ColumnName)
			assert.Equal(t, "bigint", got[0].ColumnType)
			require.NotNil(t, got[0].Long)
			require.NotNil(t, got[0].Long.LowValue)
			require.NotNil(t, got[0].Long.HighValue)
			assert.Equal(t, low, *got[0].Long.LowValue)
			assert.Equal(t, high, *got[0].Long.HighValue)
			assert.Equal(t, int64(2), got[0].Long.NumNulls)
			assert.Equal(t, int64(50), got[0].Long.NumDistinct)

			assert.Equal(t, "b", got[1].ColumnName)
			require.NotNil(t, got[1].String)
			assert.Equal(t, int64(10), got[1].String.MaxColLen)
			assert.InDelta(t, 5.5, got[1].String.AvgColLen, 0)
			assert.Equal(t, int64(1), got[1].String.NumNulls)
			assert.Equal(t, int64(20), got[1].String.NumDistinct)

			// A column name not seeded via SeedColumnStats is absent from
			// the result rather than an error.
			none, err := c.GetTableColumnStatistics(ctx, "db", "t", []string{"nope"})
			require.NoError(t, err)
			assert.Empty(t, none)

			// An empty columns argument returns (nil, nil) without issuing
			// the RPC at all.
			callsBefore := len(srv.Calls())
			none2, err := c.GetTableColumnStatistics(ctx, "db", "t", nil)
			require.NoError(t, err)
			assert.Nil(t, none2)
			assert.Len(t, srv.Calls(), callsBefore, "an empty columns argument must not issue get_table_statistics_req")

			args, ok := srv.LastArgs("get_table_statistics_req").(*hive_metastore.TableStatsRequest)
			require.True(t, ok)
			if v == hmstest.Hive23 {
				assert.Nil(t, args.CatName)
			} else {
				require.NotNil(t, args.CatName)
				assert.Equal(t, "hive", *args.CatName)
			}
			assert.Equal(t, "hive", args.Engine)
			assert.Equal(t, int64(-1), args.ID)
		})
	}
}
