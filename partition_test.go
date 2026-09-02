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

func TestAddPartitions_Chunked(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI(), hms.WithPartitionBatchSize(2))
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

// TestAddPartitions_ChunkSizeDoesNotAffectBatching covers the fix to a
// consumer regression: WithChunkSize used to also govern AddPartitions'
// batch size, so a caller setting it only to bound GetTables/
// GetPartitionsByNames requests unknowingly shrank AddPartitions' batches
// too. WithChunkSize(2) alone must leave AddPartitions at its 1000-item
// default (a single add_partitions_req for five partitions); only
// WithPartitionBatchSize(2) governs AddPartitions' batching (three
// add_partitions_req calls for five partitions).
func TestAddPartitions_ChunkSizeDoesNotAffectBatching(t *testing.T) {
	t.Parallel()

	countAddPartitionsReq := func(calls []string) int {
		n := 0
		for _, call := range calls {
			if call == "add_partitions_req" {
				n++
			}
		}
		return n
	}

	parts := []*hms.Partition{
		{Values: []string{"2024-01-01"}},
		{Values: []string{"2024-01-02"}},
		{Values: []string{"2024-01-03"}},
		{Values: []string{"2024-01-04"}},
		{Values: []string{"2024-01-05"}},
	}

	t.Run("WithChunkSize(2) alone does not batch AddPartitions", func(t *testing.T) {
		t.Parallel()
		srv := hmstest.Start(t, hmstest.Hive40)
		c := mustNew(t, srv.URI(), hms.WithChunkSize(2))
		ctx := context.Background()

		require.NoError(t, c.CreateTable(ctx, &hms.Table{
			DatabaseName:  "db",
			TableName:     "t",
			PartitionKeys: []*hms.FieldSchema{{Name: "dt", Type: "string"}},
		}))
		require.NoError(t, c.AddPartitions(ctx, "db", "t", parts, false))

		assert.Equal(t, 1, countAddPartitionsReq(srv.Calls()))
	})

	t.Run("WithPartitionBatchSize(2) batches AddPartitions", func(t *testing.T) {
		t.Parallel()
		srv := hmstest.Start(t, hmstest.Hive40)
		c := mustNew(t, srv.URI(), hms.WithPartitionBatchSize(2))
		ctx := context.Background()

		require.NoError(t, c.CreateTable(ctx, &hms.Table{
			DatabaseName:  "db",
			TableName:     "t",
			PartitionKeys: []*hms.FieldSchema{{Name: "dt", Type: "string"}},
		}))
		require.NoError(t, c.AddPartitions(ctx, "db", "t", parts, false))

		assert.Equal(t, 3, countAddPartitionsReq(srv.Calls()))
	})
}

func TestAddPartitions_IfNotExists(t *testing.T) {
	t.Parallel()
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

func TestGetPartitions(t *testing.T) {
	t.Parallel()
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

func TestGetPartitionNames(t *testing.T) {
	t.Parallel()
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

func TestAlterPartitions(t *testing.T) {
	t.Parallel()
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
				assert.NotContains(t, calls, "alter_partitions_req")
				assert.Contains(t, calls, "alter_partitions")
			}
		})
	}
}

// TestAddPartitions_DoesNotCarrySnapshot pins two create-path rules at
// once (SPEC §5.4, §5.5): a Partition read from one table and added to
// another lands in the table AddPartitions names -- its own
// DatabaseName/TableName never override the call's arguments -- and it does
// not carry the source partition's server-assigned fields (WriteId,
// Privileges) with it, since the round-trip fidelity snapshot is scoped to
// AlterPartitions.
func TestAddPartitions_DoesNotCarrySnapshot(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI())
	ctx := context.Background()

	for _, name := range []string{"a", "b"} {
		require.NoError(t, c.CreateTable(ctx, &hms.Table{
			DatabaseName:  "db",
			TableName:     name,
			PartitionKeys: []*hms.FieldSchema{{Name: "dt", Type: "string"}},
		}))
	}

	// Seeded straight into the store, so the source partition carries the
	// fields hms.Partition has no field for -- exactly what a real server
	// would return from GetPartitions.
	seed := hive_metastore.NewPartition()
	seed.Values = []string{"2024-01-01"}
	seed.DbName = "db"
	seed.TableName = "a"
	seed.CatName = ptrTo("hive")
	seed.Parameters = map[string]string{"x": "1"}
	seed.WriteId = 5
	seed.Privileges = &hive_metastore.PrincipalPrivilegeSet{
		UserPrivileges: map[string][]*hive_metastore.PrivilegeGrantInfo{
			"alice": {{Privilege: "SELECT"}},
		},
	}
	srv.SeedPartitions("hive", "db", "a", []*hive_metastore.Partition{seed})

	fromA, err := c.GetPartitions(ctx, "db", "a", -1)
	require.NoError(t, err)
	require.Len(t, fromA, 1)
	require.NotNil(t, hms.PartitionRaw(fromA[0]), "GetPartitions must snapshot what it read")

	require.NoError(t, c.AddPartitions(ctx, "db", "b", fromA, false))

	req, ok := srv.LastArgs("add_partitions_req").(*hive_metastore.AddPartitionsRequest)
	require.True(t, ok, "add_partitions_req args have unexpected type %T", srv.LastArgs("add_partitions_req"))
	require.Len(t, req.Parts, 1)
	sent := req.Parts[0]
	assert.Equal(t, "db", sent.DbName)
	assert.Equal(t, "b", sent.TableName, "the partition must land in the table the call names, not the one it was read from")
	assert.Equal(t, "1", sent.Parameters["x"], "a modelled field still travels")
	assert.Equal(t, int64(-1), sent.WriteId, "an added partition's write id is unassigned, not the source's")
	assert.Nil(t, sent.Privileges, "the source partition's privileges must not define the new one")
}

func TestDropPartition(t *testing.T) {
	t.Parallel()
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

// setupPartitionedTable creates "db"."t", partitioned by "dt" and "region",
// with three partitions, and returns their partition names in creation
// order ("dt=2024-01-01/region=eu", ...).
func setupPartitionedTable(t *testing.T, c *hms.Client, ctx context.Context) []string {
	t.Helper()
	require.NoError(t, c.CreateTable(ctx, &hms.Table{
		DatabaseName: "db",
		TableName:    "t",
		PartitionKeys: []*hms.FieldSchema{
			{Name: "dt", Type: "string"},
			{Name: "region", Type: "string"},
		},
	}))
	values := [][]string{
		{"2024-01-01", "eu"},
		{"2024-01-02", "us"},
		{"2024-01-03", "eu"},
	}
	names := make([]string, len(values))
	for i, v := range values {
		require.NoError(t, c.AddPartitions(ctx, "db", "t", []*hms.Partition{{Values: v}}, false))
		names[i] = "dt=" + v[0] + "/region=" + v[1]
	}
	return names
}

func TestGetPartitionsByNames(t *testing.T) {
	t.Parallel()
	for _, tt := range partitionVersions {
		v, name := tt.v, tt.name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := hmstest.Start(t, v)
			c := mustNew(t, srv.URI())
			ctx := context.Background()

			names := setupPartitionedTable(t, c, ctx)

			got, err := c.GetPartitionsByNames(ctx, "db", "t", []string{names[0], names[2]})
			require.NoError(t, err)
			require.Len(t, got, 2)
			gotValues := [][]string{got[0].Values, got[1].Values}
			assert.Contains(t, gotValues, []string{"2024-01-01", "eu"})
			assert.Contains(t, gotValues, []string{"2024-01-03", "eu"})

			// A name the server doesn't recognize is silently skipped,
			// like GetTables does for an unknown table name.
			got2, err := c.GetPartitionsByNames(ctx, "db", "t", []string{names[0], "dt=nope/region=nope"})
			require.NoError(t, err)
			assert.Len(t, got2, 1)

			calls := srv.Calls()
			if v == hmstest.Hive40 {
				assert.Contains(t, calls, "get_partitions_by_names_req")
				assert.NotContains(t, calls, "get_partitions_by_names")
			} else {
				assert.NotContains(t, calls, "get_partitions_by_names_req")
				assert.Contains(t, calls, "get_partitions_by_names")
			}
		})
	}
}

// TestGetPartitionsByNames_Chunked mirrors TestAddPartitions_Chunked: with
// a chunk size of 2, five names are sent as three
// get_partitions_by_names_req requests, and the returned partitions are
// the concatenation of every chunk's results.
func TestGetPartitionsByNames_Chunked(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI(), hms.WithChunkSize(2))
	ctx := context.Background()

	require.NoError(t, c.CreateTable(ctx, &hms.Table{
		DatabaseName:  "db",
		TableName:     "t",
		PartitionKeys: []*hms.FieldSchema{{Name: "dt", Type: "string"}},
	}))
	dates := []string{"2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"}
	names := make([]string, len(dates))
	for i, d := range dates {
		require.NoError(t, c.AddPartitions(ctx, "db", "t", []*hms.Partition{{Values: []string{d}}}, false))
		names[i] = "dt=" + d
	}

	got, err := c.GetPartitionsByNames(ctx, "db", "t", names)
	require.NoError(t, err)
	assert.Len(t, got, 5)

	n := 0
	for _, call := range srv.Calls() {
		if call == "get_partitions_by_names_req" {
			n++
		}
	}
	assert.Equal(t, 3, n)
}

// TestGetPartitionsByNames_FallbackCached mirrors
// TestGetPartitions_FallbackCached: get_partitions_by_names_req is deleted
// outright from the fake server's processor map on Hive23/Hive31 (see
// removedRPCs), so both the discovering first call and the cached second
// call go straight to the legacy RPC; only the resulting wire call is
// observable here, not the discovery itself.
func TestGetPartitionsByNames_FallbackCached(t *testing.T) {
	t.Parallel()
	for _, tt := range partitionVersions {
		v, name := tt.v, tt.name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := hmstest.Start(t, v)
			c := mustNew(t, srv.URI(), hms.WithPoolSize(1))
			ctx := context.Background()

			require.NoError(t, c.CreateTable(ctx, &hms.Table{
				DatabaseName:  "db",
				TableName:     "t",
				PartitionKeys: []*hms.FieldSchema{{Name: "dt", Type: "string"}},
			}))
			require.NoError(t, c.AddPartitions(ctx, "db", "t", []*hms.Partition{{Values: []string{"2024-01-01"}}}, false))
			names := []string{"dt=2024-01-01"}

			baseline := len(srv.Calls())
			_, err := c.GetPartitionsByNames(ctx, "db", "t", names)
			require.NoError(t, err)
			afterFirst := srv.Calls()[baseline:]

			baseline2 := len(srv.Calls())
			_, err = c.GetPartitionsByNames(ctx, "db", "t", names)
			require.NoError(t, err)
			afterSecond := srv.Calls()[baseline2:]

			if v == hmstest.Hive40 {
				assert.Equal(t, []string{"get_partitions_by_names_req"}, afterFirst)
				assert.Equal(t, []string{"get_partitions_by_names_req"}, afterSecond)
			} else {
				assert.Equal(t, []string{"get_partitions_by_names"}, afterFirst)
				assert.Equal(t, []string{"get_partitions_by_names"}, afterSecond)
			}
		})
	}
}

func TestGetPartitionsByFilter(t *testing.T) {
	t.Parallel()
	for _, tt := range partitionVersions {
		v, name := tt.v, tt.name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := hmstest.Start(t, v)
			c := mustNew(t, srv.URI())
			ctx := context.Background()
			setupPartitionedTable(t, c, ctx)

			single, err := c.GetPartitionsByFilter(ctx, "db", "t", "dt = '2024-01-01'", -1)
			require.NoError(t, err)
			require.Len(t, single, 1)
			assert.Equal(t, []string{"2024-01-01", "eu"}, single[0].Values)

			multi, err := c.GetPartitionsByFilter(ctx, "db", "t", "region = 'eu' and dt = '2024-01-03'", -1)
			require.NoError(t, err)
			require.Len(t, multi, 1)
			assert.Equal(t, []string{"2024-01-03", "eu"}, multi[0].Values)

			eu, err := c.GetPartitionsByFilter(ctx, "db", "t", "region = 'eu'", -1)
			require.NoError(t, err)
			assert.Len(t, eu, 2)

			limited, err := c.GetPartitionsByFilter(ctx, "db", "t", "region = 'eu'", 1)
			require.NoError(t, err)
			assert.Len(t, limited, 1)

			assert.Contains(t, srv.Calls(), "get_partitions_by_filter")
			assert.NotContains(t, srv.Calls(), "get_partitions_by_filter_req")
		})
	}
}

// TestGetPartitionsByFilter_UnsupportedGrammar covers the fake server's
// documented limitation (internal/hmstest/handler.go parsePartitionFilter):
// only "key = 'value'" terms joined by "and" are supported, so an OR
// expression is rejected with a MetaException, classified as ErrMeta.
func TestGetPartitionsByFilter_UnsupportedGrammar(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI())
	ctx := context.Background()
	setupPartitionedTable(t, c, ctx)

	_, err := c.GetPartitionsByFilter(ctx, "db", "t", "dt = '2024-01-01' or region = 'us'", -1)
	require.ErrorIs(t, err, hms.ErrMeta)
}

func TestGetPartitionNamesByValues(t *testing.T) {
	t.Parallel()
	for _, tt := range partitionVersions {
		v, name := tt.v, tt.name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := hmstest.Start(t, v)
			c := mustNew(t, srv.URI())
			ctx := context.Background()
			setupPartitionedTable(t, c, ctx)

			// A fully specified prefix matches exactly one partition.
			exact, err := c.GetPartitionNamesByValues(ctx, "db", "t", []string{"2024-01-01", "eu"}, -1)
			require.NoError(t, err)
			assert.Equal(t, []string{"dt=2024-01-01/region=eu"}, exact)

			// An empty trailing value wildcards that position.
			byDate, err := c.GetPartitionNamesByValues(ctx, "db", "t", []string{"2024-01-03", ""}, -1)
			require.NoError(t, err)
			assert.Equal(t, []string{"dt=2024-01-03/region=eu"}, byDate)

			byRegion, err := c.GetPartitionNamesByValues(ctx, "db", "t", []string{"", "eu"}, -1)
			require.NoError(t, err)
			assert.Len(t, byRegion, 2)

			limited, err := c.GetPartitionNamesByValues(ctx, "db", "t", []string{"", "eu"}, 1)
			require.NoError(t, err)
			assert.Len(t, limited, 1)

			calls := srv.Calls()
			if v == hmstest.Hive40 {
				assert.Contains(t, calls, "get_partition_names_ps_req")
				assert.NotContains(t, calls, "get_partition_names_ps")
			} else {
				assert.NotContains(t, calls, "get_partition_names_ps_req")
				assert.Contains(t, calls, "get_partition_names_ps")
			}
		})
	}
}

// TestGetPartitionNamesByValues_FallbackCached mirrors
// TestGetPartitionsByNames_FallbackCached for get_partition_names_ps_req.
func TestGetPartitionNamesByValues_FallbackCached(t *testing.T) {
	t.Parallel()
	for _, tt := range partitionVersions {
		v, name := tt.v, tt.name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := hmstest.Start(t, v)
			c := mustNew(t, srv.URI(), hms.WithPoolSize(1))
			ctx := context.Background()
			setupPartitionedTable(t, c, ctx)

			baseline := len(srv.Calls())
			_, err := c.GetPartitionNamesByValues(ctx, "db", "t", []string{"2024-01-01", "eu"}, -1)
			require.NoError(t, err)
			afterFirst := srv.Calls()[baseline:]

			baseline2 := len(srv.Calls())
			_, err = c.GetPartitionNamesByValues(ctx, "db", "t", []string{"2024-01-01", "eu"}, -1)
			require.NoError(t, err)
			afterSecond := srv.Calls()[baseline2:]

			if v == hmstest.Hive40 {
				assert.Equal(t, []string{"get_partition_names_ps_req"}, afterFirst)
				assert.Equal(t, []string{"get_partition_names_ps_req"}, afterSecond)
			} else {
				assert.Equal(t, []string{"get_partition_names_ps"}, afterFirst)
				assert.Equal(t, []string{"get_partition_names_ps"}, afterSecond)
			}
		})
	}
}

func TestAlterDatabase(t *testing.T) {
	t.Parallel()
	for _, tt := range partitionVersions {
		v, name := tt.v, tt.name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := hmstest.Start(t, v)
			c := mustNew(t, srv.URI())
			ctx := context.Background()

			require.NoError(t, c.CreateDatabase(ctx, &hms.Database{
				Name:       "db",
				Parameters: map[string]string{"k": "v1"},
			}))

			altered := &hms.Database{
				Name:       "db",
				Parameters: map[string]string{"k": "v2"},
				OwnerName:  "alice",
			}
			require.NoError(t, c.AlterDatabase(ctx, "db", altered))

			got, err := c.GetDatabase(ctx, "db")
			require.NoError(t, err)
			assert.Equal(t, "v2", got.Parameters["k"])
			assert.Equal(t, "alice", got.OwnerName)

			args, ok := srv.LastArgs("alter_database").(hmstest.AlterDatabaseArgs)
			require.True(t, ok)
			assert.Equal(t, "v2", args.Db.Parameters["k"])
			// CreateTime is read-only and never written by AlterDatabase
			// (see databaseToThrift).
			assert.Nil(t, args.Db.CreateTime)
		})
	}
}

func TestAlterDatabase_MissingIsNotFound(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI())

	err := c.AlterDatabase(context.Background(), "nope", &hms.Database{Name: "nope"})
	require.ErrorIs(t, err, hms.ErrNotFound)
}

// TestDropPartitionsByNames covers G9's main path across every emulated
// version: drop_partitions_req exists on every version's IDL (SPEC §2.1),
// so it carries no legacy fallback and must appear on the wire on Hive23
// and Hive31 too, unlike alter_partitions_req/get_partitions_req and their
// siblings.
func TestDropPartitionsByNames(t *testing.T) {
	t.Parallel()
	for _, tt := range partitionVersions {
		v, name := tt.v, tt.name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := hmstest.Start(t, v)
			c := mustNew(t, srv.URI())
			ctx := context.Background()

			names := setupPartitionedTable(t, c, ctx)

			require.NoError(t, c.DropPartitionsByNames(ctx, "db", "t", []string{names[0], names[2]}, false, false))

			got, err := c.GetPartitions(ctx, "db", "t", -1)
			require.NoError(t, err)
			require.Len(t, got, 1)
			assert.Equal(t, []string{"2024-01-02", "us"}, got[0].Values)

			assert.Contains(t, srv.Calls(), "drop_partitions_req")

			args, ok := srv.LastArgs("drop_partitions_req").(*hive_metastore.DropPartitionsRequest)
			require.True(t, ok)
			assert.ElementsMatch(t, []string{names[0], names[2]}, args.Parts.Names)
			assert.Empty(t, args.Parts.Exprs)
			assert.False(t, args.NeedResult_)
			require.NotNil(t, args.DeleteData)
			assert.False(t, *args.DeleteData)
			assert.False(t, args.IfExists)
		})
	}
}

// TestDropPartitionsByNames_Chunked mirrors TestAddPartitions_Chunked: with
// a partition batch size of 2, five names are sent as three
// drop_partitions_req requests.
func TestDropPartitionsByNames_Chunked(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI(), hms.WithPartitionBatchSize(2))
	ctx := context.Background()

	require.NoError(t, c.CreateTable(ctx, &hms.Table{
		DatabaseName:  "db",
		TableName:     "t",
		PartitionKeys: []*hms.FieldSchema{{Name: "dt", Type: "string"}},
	}))

	dates := []string{"2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"}
	parts := make([]*hms.Partition, len(dates))
	names := make([]string, len(dates))
	for i, d := range dates {
		parts[i] = &hms.Partition{Values: []string{d}}
		names[i] = "dt=" + d
	}
	require.NoError(t, c.AddPartitions(ctx, "db", "t", parts, false))

	require.NoError(t, c.DropPartitionsByNames(ctx, "db", "t", names, false, false))

	n := 0
	for _, call := range srv.Calls() {
		if call == "drop_partitions_req" {
			n++
		}
	}
	assert.Equal(t, 3, n)

	got, err := c.GetPartitions(ctx, "db", "t", -1)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestDropPartitionsByNames_IfExists(t *testing.T) {
	t.Parallel()
	t.Run("ifExists false on missing name is not found", func(t *testing.T) {
		t.Parallel()
		srv := hmstest.Start(t, hmstest.Hive40)
		c := mustNew(t, srv.URI())
		ctx := context.Background()
		require.NoError(t, c.CreateTable(ctx, &hms.Table{
			DatabaseName:  "db",
			TableName:     "t",
			PartitionKeys: []*hms.FieldSchema{{Name: "dt", Type: "string"}},
		}))

		err := c.DropPartitionsByNames(ctx, "db", "t", []string{"dt=2024-01-01"}, false, false)
		require.ErrorIs(t, err, hms.ErrNotFound)
	})

	t.Run("ifExists true on missing name is nil", func(t *testing.T) {
		t.Parallel()
		srv := hmstest.Start(t, hmstest.Hive40)
		c := mustNew(t, srv.URI())
		ctx := context.Background()
		require.NoError(t, c.CreateTable(ctx, &hms.Table{
			DatabaseName:  "db",
			TableName:     "t",
			PartitionKeys: []*hms.FieldSchema{{Name: "dt", Type: "string"}},
		}))

		err := c.DropPartitionsByNames(ctx, "db", "t", []string{"dt=2024-01-01"}, false, true)
		require.NoError(t, err)
	})
}

// TestDropPartitions_BuildsNamesFromValues covers DropPartitions'
// GetTable-then-PartitionName construction (G9): it must send the escaped,
// lowercased-key names PartitionName computes, not the raw values, as
// drop_partitions_req's Parts.Names. ifExists=true means a name the fake
// server's own (unescaped) partitionName helper never produces is silently
// skipped rather than failing the whole call -- this test only asserts
// what was sent on the wire, not that the fake server matched it to a
// partition (see PartitionName's own unit tests, TestPartitionName, for
// the escaping's correctness).
func TestDropPartitions_BuildsNamesFromValues(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI())
	ctx := context.Background()

	require.NoError(t, c.CreateTable(ctx, &hms.Table{
		DatabaseName:  "db",
		TableName:     "t",
		PartitionKeys: []*hms.FieldSchema{{Name: "dt", Type: "string"}},
	}))

	err := c.DropPartitions(ctx, "db", "t", [][]string{{"2024-01-01"}, {"a/b"}}, false, true)
	require.NoError(t, err)

	args, ok := srv.LastArgs("drop_partitions_req").(*hive_metastore.DropPartitionsRequest)
	require.True(t, ok)
	assert.Equal(t, []string{"dt=2024-01-01", "dt=a%2Fb"}, args.Parts.Names)
}
