package hms_test

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hms "github.com/slachiewicz/hms-client-go"
	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
	"github.com/slachiewicz/hms-client-go/internal/hmstest"
)

var partitionVersions = []struct {
	name string
	v    hmstest.Version
}{
	{"hive23", hmstest.Hive23},
	{"hive31", hmstest.Hive31},
	{"hive40", hmstest.Hive40},
}

// TestAddPartitions_Chunked deliberately does not call t.Parallel(): it
// mutates the shared, unexported defaultChunkSize package var via
// SetChunkSizeForTest, exactly as table_test.go's
// TestGetTables_ChunkedRequestOrder already does. Running either of them
// concurrently with any t.Parallel() test races on that var (confirmed
// with go test -race); Go only runs non-parallel top-level tests one at a
// time, strictly before the parallel batch executes, so leaving this one
// serial is what keeps it race-free without touching export_test.go.
func TestAddPartitions_Chunked(t *testing.T) {
	restore := hms.SetChunkSizeForTest(2)
	defer restore()

	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI())
	ctx := context.Background()

	require.NoError(t, c.CreateTable(ctx, &hms.Table{
		DatabaseName:  "db",
		TableName:     "t",
		PartitionKeys: []*hms.FieldSchema{{Name: "dt", Type: "string"}},
	}))

	parts := []*hms.Partition{
		{Values: []string{"2024-01-01"}},
		{Values: []string{"2024-01-02"}},
		{Values: []string{"2024-01-03"}},
		{Values: []string{"2024-01-04"}},
		{Values: []string{"2024-01-05"}},
	}
	require.NoError(t, c.AddPartitions(ctx, "db", "t", parts, false))

	n := 0
	for _, call := range srv.Calls() {
		if call == "add_partitions_req" {
			n++
		}
	}
	assert.Equal(t, 3, n)

	got, err := c.GetPartitions(ctx, "db", "t", -1)
	require.NoError(t, err)
	assert.Len(t, got, 5)
}

// TestAddPartitions_IfNotExists does not call t.Parallel() at the top
// level: its subtests call AddPartitions, which reads the shared
// defaultChunkSize package var (see TestAddPartitions_Chunked's comment
// above for why that var makes top-level parallelism unsafe here). Its
// subtests still run in parallel with each other; Go only releases them to
// actually execute once this function's own body (dispatching both t.Run
// calls) returns, which happens during the top-level serial dispatch phase,
// strictly before any top-level t.Parallel() test's body runs.
func TestAddPartitions_IfNotExists(t *testing.T) {
	t.Run("duplicate without ifNotExists fails", func(t *testing.T) {
		t.Parallel()
		srv := hmstest.Start(t, hmstest.Hive40)
		c := mustNew(t, srv.URI())
		ctx := context.Background()
		require.NoError(t, c.CreateTable(ctx, &hms.Table{
			DatabaseName:  "db",
			TableName:     "t",
			PartitionKeys: []*hms.FieldSchema{{Name: "dt", Type: "string"}},
		}))

		part := &hms.Partition{Values: []string{"2024-01-01"}}
		require.NoError(t, c.AddPartitions(ctx, "db", "t", []*hms.Partition{part}, false))

		err := c.AddPartitions(ctx, "db", "t", []*hms.Partition{part}, false)
		require.ErrorIs(t, err, hms.ErrAlreadyExists)
	})

	t.Run("duplicate with ifNotExists succeeds without duplicating", func(t *testing.T) {
		t.Parallel()
		srv := hmstest.Start(t, hmstest.Hive40)
		c := mustNew(t, srv.URI())
		ctx := context.Background()
		require.NoError(t, c.CreateTable(ctx, &hms.Table{
			DatabaseName:  "db",
			TableName:     "t",
			PartitionKeys: []*hms.FieldSchema{{Name: "dt", Type: "string"}},
		}))

		part := &hms.Partition{Values: []string{"2024-01-01"}}
		require.NoError(t, c.AddPartitions(ctx, "db", "t", []*hms.Partition{part}, true))
		require.NoError(t, c.AddPartitions(ctx, "db", "t", []*hms.Partition{part}, true))

		require.Len(t, srv.Store().Partitions["hive.db.t"], 1)
	})
}

// TestGetPartitions does not call t.Parallel() at the top level: it calls
// AddPartitions to seed fixtures, which reads defaultChunkSize (see
// TestAddPartitions_Chunked's comment above).
func TestGetPartitions(t *testing.T) {
	for _, tt := range partitionVersions {
		v, name := tt.v, tt.name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := hmstest.Start(t, v)
			c := mustNew(t, srv.URI())
			ctx := context.Background()

			require.NoError(t, c.CreateTable(ctx, &hms.Table{
				DatabaseName:  "db",
				TableName:     "t",
				PartitionKeys: []*hms.FieldSchema{{Name: "dt", Type: "string"}},
			}))
			for _, d := range []string{"2024-01-01", "2024-01-02", "2024-01-03"} {
				require.NoError(t, c.AddPartitions(ctx, "db", "t", []*hms.Partition{{Values: []string{d}}}, false))
			}

			all, err := c.GetPartitions(ctx, "db", "t", -1)
			require.NoError(t, err)
			assert.Len(t, all, 3)

			limited, err := c.GetPartitions(ctx, "db", "t", 2)
			require.NoError(t, err)
			assert.Len(t, limited, 2)

			// get_partitions_req is deleted outright from the fake server's
			// processor map on Hive23/Hive31 (see removedRPCs), so the
			// generic Thrift dispatcher answers UNKNOWN_METHOD without ever
			// reaching (*handler).GetPartitionsReq — srv.Calls() can never
			// show "get_partitions_req" for those versions, only the
			// legacy fallback it degrades to.
			calls := srv.Calls()
			if v == hmstest.Hive40 {
				assert.Contains(t, calls, "get_partitions_req")
				assert.NotContains(t, calls, "get_partitions")
			} else {
				assert.NotContains(t, calls, "get_partitions_req")
				assert.Contains(t, calls, "get_partitions")
			}
		})
	}
}

// TestGetPartitions_FallbackCached exercises the fallback cache end to end,
// through the public API only (WithPoolSize(1) keeps every call on the same
// conn, so any caching behavior is visible to a single conn's cache; see
// TestConn_FallbackCache in conn_test.go for a focused, direct unit test of
// useLegacy/markLegacy themselves). get_partitions_req is deleted outright
// from the fake server's processor map on Hive23/Hive31 (see removedRPCs),
// so the generic Thrift dispatcher answers UNKNOWN_METHOD without ever
// reaching (*handler).GetPartitionsReq — srv.Calls() can never show
// "get_partitions_req" for those versions, only the legacy fallback it
// degrades to, both on the first call (which discovers the fallback) and
// every call after (which the cache sends straight to legacy). This test
// therefore asserts the one thing that is observable here: both calls
// succeed and produce the same, correct RPC on the wire.
func TestGetPartitions_FallbackCached(t *testing.T) {
	t.Parallel()
	for _, tt := range partitionVersions {
		v, name := tt.v, tt.name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := hmstest.Start(t, v)
			c := mustNew(t, srv.URI(), hms.WithPoolSize(1))
			ctx := context.Background()

			require.NoError(t, c.CreateTable(ctx, &hms.Table{DatabaseName: "db", TableName: "t"}))
			baseline := len(srv.Calls())

			_, err := c.GetPartitions(ctx, "db", "t", -1)
			require.NoError(t, err)
			afterFirst := srv.Calls()[baseline:]

			baseline2 := len(srv.Calls())
			_, err = c.GetPartitions(ctx, "db", "t", -1)
			require.NoError(t, err)
			afterSecond := srv.Calls()[baseline2:]

			if v == hmstest.Hive40 {
				assert.Equal(t, []string{"get_partitions_req"}, afterFirst)
				assert.Equal(t, []string{"get_partitions_req"}, afterSecond)
			} else {
				assert.Equal(t, []string{"get_partitions"}, afterFirst)
				assert.Equal(t, []string{"get_partitions"}, afterSecond)
			}
		})
	}
}

func TestGetPartitions_ClampMaxParts(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI())
	ctx := context.Background()

	require.NoError(t, c.CreateTable(ctx, &hms.Table{DatabaseName: "db", TableName: "t"}))

	_, err := c.GetPartitions(ctx, "db", "t", math.MaxInt16+5)
	require.NoError(t, err)

	args, ok := srv.LastArgs("get_partitions_req").(*hive_metastore.PartitionsRequest)
	require.True(t, ok)
	assert.EqualValues(t, math.MaxInt16, args.MaxParts)
}

// TestGetPartitionNames does not call t.Parallel(): it calls AddPartitions
// to seed fixtures, which reads defaultChunkSize (see
// TestAddPartitions_Chunked's comment above).
func TestGetPartitionNames(t *testing.T) {
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI())
	ctx := context.Background()

	require.NoError(t, c.CreateTable(ctx, &hms.Table{
		DatabaseName: "db",
		TableName:    "t",
		PartitionKeys: []*hms.FieldSchema{
			{Name: "dt", Type: "string"},
			{Name: "region", Type: "string"},
		},
	}))
	require.NoError(t, c.AddPartitions(ctx, "db", "t", []*hms.Partition{
		{Values: []string{"2024-01-01", "eu"}},
		{Values: []string{"2024-01-02", "us"}},
	}, false))

	names, err := c.GetPartitionNames(ctx, "db", "t", -1)
	require.NoError(t, err)
	assert.Equal(t, []string{"dt=2024-01-01/region=eu", "dt=2024-01-02/region=us"}, names)
}

// TestAlterPartitions does not call t.Parallel() at the top level: it calls
// AddPartitions to seed fixtures, which reads defaultChunkSize (see
// TestAddPartitions_Chunked's comment above).
func TestAlterPartitions(t *testing.T) {
	for _, tt := range partitionVersions {
		v, name := tt.v, tt.name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := hmstest.Start(t, v)
			c := mustNew(t, srv.URI())
			ctx := context.Background()

			require.NoError(t, c.CreateTable(ctx, &hms.Table{
				DatabaseName:  "db",
				TableName:     "t",
				PartitionKeys: []*hms.FieldSchema{{Name: "dt", Type: "string"}},
			}))
			part := &hms.Partition{Values: []string{"2024-01-01"}, Parameters: map[string]string{"x": "1"}}
			require.NoError(t, c.AddPartitions(ctx, "db", "t", []*hms.Partition{part}, false))

			updated := &hms.Partition{Values: []string{"2024-01-01"}, Parameters: map[string]string{"x": "2"}}
			require.NoError(t, c.AlterPartitions(ctx, "db", "t", []*hms.Partition{updated}))

			got, err := c.GetPartitions(ctx, "db", "t", -1)
			require.NoError(t, err)
			require.Len(t, got, 1)
			assert.Equal(t, "2", got[0].Parameters["x"])

			calls := srv.Calls()
			if v == hmstest.Hive40 {
				assert.Contains(t, calls, "alter_partitions_req")
				assert.NotContains(t, calls, "alter_partitions")
			} else {
				assert.Contains(t, calls, "alter_partitions")
			}
		})
	}
}

// TestDropPartition does not call t.Parallel() at the top level: its
// "existing partition is removed" subtest calls AddPartitions, which reads
// defaultChunkSize (see TestAddPartitions_Chunked's comment above); since
// this test's own body would otherwise be deferred into the same global
// parallel batch as table_test.go's TestGetTables_ChunkedRequestOrder, that
// applies to the whole function, not just the one subtest.
func TestDropPartition(t *testing.T) {
	t.Run("ifExists false on missing partition is not found", func(t *testing.T) {
		t.Parallel()
		srv := hmstest.Start(t, hmstest.Hive40)
		c := mustNew(t, srv.URI())
		ctx := context.Background()
		require.NoError(t, c.CreateTable(ctx, &hms.Table{
			DatabaseName:  "db",
			TableName:     "t",
			PartitionKeys: []*hms.FieldSchema{{Name: "dt", Type: "string"}},
		}))

		err := c.DropPartition(ctx, "db", "t", []string{"2024-01-01"}, false, false)
		require.ErrorIs(t, err, hms.ErrNotFound)
	})

	t.Run("ifExists true on missing partition is nil", func(t *testing.T) {
		t.Parallel()
		srv := hmstest.Start(t, hmstest.Hive40)
		c := mustNew(t, srv.URI())
		ctx := context.Background()
		require.NoError(t, c.CreateTable(ctx, &hms.Table{
			DatabaseName:  "db",
			TableName:     "t",
			PartitionKeys: []*hms.FieldSchema{{Name: "dt", Type: "string"}},
		}))

		err := c.DropPartition(ctx, "db", "t", []string{"2024-01-01"}, false, true)
		require.NoError(t, err)
	})

	t.Run("existing partition is removed", func(t *testing.T) {
		t.Parallel()
		srv := hmstest.Start(t, hmstest.Hive40)
		c := mustNew(t, srv.URI())
		ctx := context.Background()
		require.NoError(t, c.CreateTable(ctx, &hms.Table{
			DatabaseName:  "db",
			TableName:     "t",
			PartitionKeys: []*hms.FieldSchema{{Name: "dt", Type: "string"}},
		}))
		require.NoError(t, c.AddPartitions(ctx, "db", "t", []*hms.Partition{{Values: []string{"2024-01-01"}}}, false))

		require.NoError(t, c.DropPartition(ctx, "db", "t", []string{"2024-01-01"}, false, false))

		got, err := c.GetPartitions(ctx, "db", "t", -1)
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}
