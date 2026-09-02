package hms_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hms "github.com/slachiewicz/hms-client-go"
	"github.com/slachiewicz/hms-client-go/internal/hmstest"
)

// sliceIdentity returns fs's backing array's address as a comparable
// value, so two []*FieldSchema slices can be checked for actually sharing
// one backing array (G11's column-list interning) rather than merely
// being equal.
func sliceIdentity(fs []*hms.FieldSchema) uintptr { return reflect.ValueOf(fs).Pointer() }

// countCalls returns how many of srv.Calls() equal method.
func countCalls(srv *hmstest.Server, method string) int {
	n := 0
	for _, call := range srv.Calls() {
		if call == method {
			n++
		}
	}
	return n
}

// --- GetPartitionsSeq ---

// TestGetPartitionsSeq_AllPartitionsInNameOrder covers G11's main path
// across every emulated version: GetPartitionsSeq must yield exactly the
// partitions setupPartitionedTable creates, in the same order
// GetPartitionNames itself returns them (creation order, per the fake
// server's store -- see partitionsMatchingNames in internal/hmstest).
func TestGetPartitionsSeq_AllPartitionsInNameOrder(t *testing.T) {
	t.Parallel()
	for _, tt := range partitionVersions {
		v, name := tt.v, tt.name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := hmstest.Start(t, v)
			c := mustNew(t, srv.URI())
			ctx := context.Background()

			names := setupPartitionedTable(t, c, ctx)

			var gotNames []string
			for p, err := range c.GetPartitionsSeq(ctx, "db", "t") {
				require.NoError(t, err)
				require.Len(t, p.Values, 2)
				gotNames = append(gotNames, "dt="+p.Values[0]+"/region="+p.Values[1])
			}
			assert.Equal(t, names, gotNames)

			assert.Equal(t, 1, countCalls(srv, "get_partition_names"))
		})
	}
}

// TestGetPartitionsSeq_EarlyBreakIssuesNoFurtherRPCs covers the memory-
// bounding contract's other half: stopping the range loop early must stop
// issuing RPCs immediately, not drain every remaining chunk in the
// background. With 6 partitions and WithChunkSize(2), yielding the first 3
// items requires chunk 1 (items 1-2) and chunk 2 (items 3-4, of which only
// the first is consumed before the break) -- 2 get_partitions_by_names_req
// calls; chunk 3 (items 5-6) must never be requested.
func TestGetPartitionsSeq_EarlyBreakIssuesNoFurtherRPCs(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI(), hms.WithChunkSize(2))
	ctx := context.Background()

	require.NoError(t, c.CreateTable(ctx, &hms.Table{
		DatabaseName:  "db",
		TableName:     "t",
		PartitionKeys: []*hms.FieldSchema{{Name: "dt", Type: "string"}},
	}))
	dates := []string{"2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-06"}
	parts := make([]*hms.Partition, len(dates))
	for i, d := range dates {
		parts[i] = &hms.Partition{Values: []string{d}}
	}
	require.NoError(t, c.AddPartitions(ctx, "db", "t", parts, false))

	n := 0
	for p, err := range c.GetPartitionsSeq(ctx, "db", "t") {
		require.NoError(t, err)
		require.NotNil(t, p)
		n++
		if n == 3 {
			break
		}
	}
	assert.Equal(t, 3, n)

	assert.Equal(t, 1, countCalls(srv, "get_partition_names"))
	assert.Equal(t, 2, countCalls(srv, "get_partitions_by_names_req"),
		"breaking after item 3 must not issue the third chunk's RPC")
}

// TestGetPartitionsSeq_ServerErrorYieldedOnce covers failure mid-stream:
// stopping the server deterministically fails only the next RPC, not the
// one that already produced the partitions consumed so far, because the
// stop happens synchronously inside the consumer's own callback for the
// last item of a chunk -- range-over-func runs the loop body (including
// this test's own srv.Stop() call) before GetPartitionsSeq's chunk loop
// ever attempts the next chunk's RPC.
func TestGetPartitionsSeq_ServerErrorYieldedOnce(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI(), hms.WithChunkSize(2))
	ctx := context.Background()

	require.NoError(t, c.CreateTable(ctx, &hms.Table{
		DatabaseName:  "db",
		TableName:     "t",
		PartitionKeys: []*hms.FieldSchema{{Name: "dt", Type: "string"}},
	}))
	dates := []string{"2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"}
	parts := make([]*hms.Partition, len(dates))
	for i, d := range dates {
		parts[i] = &hms.Partition{Values: []string{d}}
	}
	require.NoError(t, c.AddPartitions(ctx, "db", "t", parts, false))

	var got []*hms.Partition
	var seqErr error
	errCount := 0
	for p, err := range c.GetPartitionsSeq(ctx, "db", "t") {
		if err != nil {
			errCount++
			seqErr = err
			continue
		}
		got = append(got, p)
		if len(got) == 2 {
			srv.Stop()
		}
	}
	assert.Len(t, got, 2, "the chunk already fetched before the server stopped must still be yielded")
	assert.Equal(t, 1, errCount, "exactly one error must be yielded")
	require.Error(t, seqErr)
	assert.ErrorIs(t, seqErr, hms.ErrUnavailable)
}

// TestGetPartitionsSeq_CtxCancelledStopsSequence covers ctx cancellation
// between chunks: the chunk loop checks ctx.Err() before issuing each
// chunk's RPC, so cancelling ctx synchronously inside the consumer's own
// callback (the same technique TestGetPartitionsSeq_ServerErrorYieldedOnce
// uses) deterministically stops the sequence before the next chunk's RPC
// is ever attempted.
func TestGetPartitionsSeq_CtxCancelledStopsSequence(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI(), hms.WithChunkSize(2))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, c.CreateTable(ctx, &hms.Table{
		DatabaseName:  "db",
		TableName:     "t",
		PartitionKeys: []*hms.FieldSchema{{Name: "dt", Type: "string"}},
	}))
	dates := []string{"2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"}
	parts := make([]*hms.Partition, len(dates))
	for i, d := range dates {
		parts[i] = &hms.Partition{Values: []string{d}}
	}
	require.NoError(t, c.AddPartitions(ctx, "db", "t", parts, false))

	var got []*hms.Partition
	var seqErr error
	errCount := 0
	for p, err := range c.GetPartitionsSeq(ctx, "db", "t") {
		if err != nil {
			errCount++
			seqErr = err
			continue
		}
		got = append(got, p)
		if len(got) == 2 {
			cancel()
		}
	}
	assert.Len(t, got, 2)
	assert.Equal(t, 1, errCount)
	require.Error(t, seqErr)
	assert.ErrorIs(t, seqErr, context.Canceled)

	// 4 partitions at chunk size 2 is 2 chunks; both items consumed above
	// come from chunk 1 alone, so the cancellation check must stop the
	// loop before chunk 2's RPC is ever issued.
	assert.Equal(t, 1, countCalls(srv, "get_partitions_by_names_req"))
}

// TestGetPartitionsSeq_InternsAcrossChunks covers G11's column-list
// interning end to end, across chunk boundaries: with WithChunkSize(2)
// and three partitions whose columns are A, B, A (in that order), chunk 1
// holds the first A and B, chunk 2 holds the second A -- a different
// get_partitions_by_names_req call, converted by a separate
// partitionsFromThriftIntern invocation. The two A partitions must still
// come back sharing one []*FieldSchema, proving the interner is shared
// across GetPartitionsSeq's whole call, not reset per chunk; the B
// partition, sharing a chunk with the first A, must not share its slice.
func TestGetPartitionsSeq_InternsAcrossChunks(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI(), hms.WithChunkSize(2))
	ctx := context.Background()

	require.NoError(t, c.CreateTable(ctx, &hms.Table{
		DatabaseName:  "db",
		TableName:     "t",
		PartitionKeys: []*hms.FieldSchema{{Name: "dt", Type: "string"}},
	}))
	colsA := []*hms.FieldSchema{{Name: "a", Type: "string"}}
	colsB := []*hms.FieldSchema{{Name: "b", Type: "int"}}
	require.NoError(t, c.AddPartitions(ctx, "db", "t", []*hms.Partition{
		{Values: []string{"2024-01-01"}, Storage: &hms.StorageDescriptor{Columns: colsA}},
		{Values: []string{"2024-01-02"}, Storage: &hms.StorageDescriptor{Columns: colsB}},
		{Values: []string{"2024-01-03"}, Storage: &hms.StorageDescriptor{Columns: colsA}},
	}, false))

	var got []*hms.Partition
	for p, err := range c.GetPartitionsSeq(ctx, "db", "t") {
		require.NoError(t, err)
		got = append(got, p)
	}
	require.Len(t, got, 3)
	assert.Equal(t, 2, countCalls(srv, "get_partitions_by_names_req"), "3 partitions at chunk size 2 must be 2 chunks")

	assert.True(t, sliceIdentity(got[0].Storage.Columns) == sliceIdentity(got[2].Storage.Columns),
		"the two A partitions, in different chunks, must share one []*FieldSchema slice")
	assert.False(t, sliceIdentity(got[0].Storage.Columns) == sliceIdentity(got[1].Storage.Columns),
		"the A and B partitions must not share a slice")
}

// TestGetPartitionsSeq_NoConnectionHeldWhileConsumerRuns covers fix round
// 1's design review: GetPartitionsSeq used to run the names lookup and
// every chunk fetch inside one held connection, so a consumer that made
// another call on the same Client from inside the range loop body would
// block forever under WithPoolSize(1) -- the nested call waiting on the
// very connection GetPartitionsSeq itself was still holding. Each chunk's
// connection is now released as soon as that chunk's own fetch returns,
// before any of its partitions are yielded, so the nested call below must
// succeed promptly instead of deadlocking.
func TestGetPartitionsSeq_NoConnectionHeldWhileConsumerRuns(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI(), hms.WithPoolSize(1), hms.WithChunkSize(2))
	ctx := context.Background()

	require.NoError(t, c.CreateTable(ctx, &hms.Table{
		DatabaseName:  "db",
		TableName:     "t",
		PartitionKeys: []*hms.FieldSchema{{Name: "dt", Type: "string"}},
	}))
	dates := []string{"2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"}
	parts := make([]*hms.Partition, len(dates))
	for i, d := range dates {
		parts[i] = &hms.Partition{Values: []string{d}}
	}
	require.NoError(t, c.AddPartitions(ctx, "db", "t", parts, false))

	n := 0
	for p, err := range c.GetPartitionsSeq(ctx, "db", "t") {
		require.NoError(t, err)
		require.NotNil(t, p)
		n++

		// A short-timeout context bounds the wait: if GetPartitionsSeq
		// still held its own connection while this loop body runs,
		// acquire would block on WithPoolSize(1)'s single connection
		// until this deadline, and the nested call would fail with
		// context.DeadlineExceeded (wrapped as ErrUnavailable) instead of
		// succeeding.
		nestedCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, nestedErr := c.GetPartitionNames(nestedCtx, "db", "t", -1)
		cancel()
		require.NoError(t, nestedErr, "a nested call from inside the range loop body must not deadlock on the pool")

		assert.LessOrEqual(t, hms.ClientLiveConns(c, 0), int32(1))
	}
	assert.Equal(t, 4, n)
}

// TestGetPartitionsSeq_FetchesChunksLazily covers the actual, honest
// property behind G11's "streaming, memory-bounded" claim, in place of
// BenchmarkGetPartitionsSeq's allocs/op (which does not measure it; see
// that benchmark's doc comment in seq_bench_test.go): with WithChunkSize
// (100) over 2,000 partitions, each chunk's get_partitions_by_names_req is
// issued only when the range loop reaches that chunk, interleaved with
// the caller's own consumption, rather than every chunk being fetched
// eagerly before the first item is yielded. After yielding the n-th item,
// exactly ceil(n/100) chunk RPCs can have happened -- never more -- which
// is what keeps at most one chunk's worth of partitions materialised at a
// time instead of the whole 2,000.
func TestGetPartitionsSeq_FetchesChunksLazily(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI(), hms.WithChunkSize(100))
	ctx := context.Background()

	require.NoError(t, c.CreateTable(ctx, &hms.Table{
		DatabaseName:  "db",
		TableName:     "t",
		PartitionKeys: []*hms.FieldSchema{{Name: "dt", Type: "string"}},
	}))
	const total = 2000
	parts := make([]*hms.Partition, total)
	for i := range parts {
		parts[i] = &hms.Partition{Values: []string{fmt.Sprintf("d%04d", i)}}
	}
	require.NoError(t, c.AddPartitions(ctx, "db", "t", parts, false))

	n := 0
	for p, err := range c.GetPartitionsSeq(ctx, "db", "t") {
		require.NoError(t, err)
		require.NotNil(t, p)
		n++

		wantRPCs := (n + 99) / 100
		gotRPCs := countCalls(srv, "get_partitions_by_names_req")
		assert.Equal(t, wantRPCs, gotRPCs,
			"after yielding item %d, exactly %d chunk RPC(s) must have run, not more (got %d)", n, wantRPCs, gotRPCs)
	}
	assert.Equal(t, total, n)
	assert.Equal(t, 20, countCalls(srv, "get_partitions_by_names_req"))
}

// --- GetTablesSeq ---

// TestGetTablesSeq_AllTablesInNameOrder mirrors
// TestGetPartitionsSeq_AllPartitionsInNameOrder for GetTablesSeq: it must
// yield exactly the tables created, in GetAllTables' own (sorted) order,
// across every emulated version.
func TestGetTablesSeq_AllTablesInNameOrder(t *testing.T) {
	t.Parallel()
	for _, tt := range partitionVersions {
		v, name := tt.v, tt.name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := hmstest.Start(t, v)
			c := mustNew(t, srv.URI())
			ctx := context.Background()

			for _, n := range []string{"c", "a", "b"} {
				require.NoError(t, c.CreateTable(ctx, &hms.Table{DatabaseName: "db", TableName: n}))
			}

			var got []string
			for tbl, err := range c.GetTablesSeq(ctx, "db") {
				require.NoError(t, err)
				got = append(got, tbl.TableName)
			}
			assert.Equal(t, []string{"a", "b", "c"}, got)
			assert.Equal(t, 1, countCalls(srv, "get_all_tables"))
		})
	}
}

// TestGetTablesSeq_EarlyBreakIssuesNoFurtherRPCs mirrors
// TestGetPartitionsSeq_EarlyBreakIssuesNoFurtherRPCs for GetTablesSeq: 6
// tables at WithChunkSize(2), breaking after 3 items must issue only 2
// get_table_objects_by_name_req calls, never a third.
func TestGetTablesSeq_EarlyBreakIssuesNoFurtherRPCs(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI(), hms.WithChunkSize(2))
	ctx := context.Background()

	for i := range 6 {
		require.NoError(t, c.CreateTable(ctx, &hms.Table{DatabaseName: "db", TableName: fmt.Sprintf("t%d", i)}))
	}

	n := 0
	for tbl, err := range c.GetTablesSeq(ctx, "db") {
		require.NoError(t, err)
		require.NotNil(t, tbl)
		n++
		if n == 3 {
			break
		}
	}
	assert.Equal(t, 3, n)
	assert.Equal(t, 1, countCalls(srv, "get_all_tables"))
	assert.Equal(t, 2, countCalls(srv, "get_table_objects_by_name_req"))
}

// TestGetTablesSeq_ServerErrorYieldedOnce mirrors
// TestGetPartitionsSeq_ServerErrorYieldedOnce for GetTablesSeq.
func TestGetTablesSeq_ServerErrorYieldedOnce(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI(), hms.WithChunkSize(2))
	ctx := context.Background()

	for i := range 4 {
		require.NoError(t, c.CreateTable(ctx, &hms.Table{DatabaseName: "db", TableName: fmt.Sprintf("t%d", i)}))
	}

	var got []*hms.Table
	var seqErr error
	errCount := 0
	for tbl, err := range c.GetTablesSeq(ctx, "db") {
		if err != nil {
			errCount++
			seqErr = err
			continue
		}
		got = append(got, tbl)
		if len(got) == 2 {
			srv.Stop()
		}
	}
	assert.Len(t, got, 2)
	assert.Equal(t, 1, errCount)
	require.Error(t, seqErr)
	assert.ErrorIs(t, seqErr, hms.ErrUnavailable)
}

// TestGetTablesSeq_CtxCancelledStopsSequence mirrors
// TestGetPartitionsSeq_CtxCancelledStopsSequence for GetTablesSeq.
func TestGetTablesSeq_CtxCancelledStopsSequence(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI(), hms.WithChunkSize(2))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := range 4 {
		require.NoError(t, c.CreateTable(ctx, &hms.Table{DatabaseName: "db", TableName: fmt.Sprintf("t%d", i)}))
	}

	var got []*hms.Table
	var seqErr error
	errCount := 0
	for tbl, err := range c.GetTablesSeq(ctx, "db") {
		if err != nil {
			errCount++
			seqErr = err
			continue
		}
		got = append(got, tbl)
		if len(got) == 2 {
			cancel()
		}
	}
	assert.Len(t, got, 2)
	assert.Equal(t, 1, errCount)
	require.Error(t, seqErr)
	assert.ErrorIs(t, seqErr, context.Canceled)
}

// TestGetTablesSeq_InternsAcrossChunks mirrors
// TestGetPartitionsSeq_InternsAcrossChunks for GetTablesSeq: tables t0 and
// t2 (columns A) land in different chunks (t1, columns B, sits between
// them in the sorted name order GetAllTables returns), and must still
// come back sharing one []*FieldSchema.
func TestGetTablesSeq_InternsAcrossChunks(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI(), hms.WithChunkSize(2))
	ctx := context.Background()

	colsA := []*hms.FieldSchema{{Name: "a", Type: "string"}}
	colsB := []*hms.FieldSchema{{Name: "b", Type: "int"}}
	require.NoError(t, c.CreateTable(ctx, &hms.Table{DatabaseName: "db", TableName: "t0", Storage: &hms.StorageDescriptor{Columns: colsA}}))
	require.NoError(t, c.CreateTable(ctx, &hms.Table{DatabaseName: "db", TableName: "t1", Storage: &hms.StorageDescriptor{Columns: colsB}}))
	require.NoError(t, c.CreateTable(ctx, &hms.Table{DatabaseName: "db", TableName: "t2", Storage: &hms.StorageDescriptor{Columns: colsA}}))

	var got []*hms.Table
	for tbl, err := range c.GetTablesSeq(ctx, "db") {
		require.NoError(t, err)
		got = append(got, tbl)
	}
	require.Len(t, got, 3)
	assert.Equal(t, 2, countCalls(srv, "get_table_objects_by_name_req"))

	assert.True(t, sliceIdentity(got[0].Storage.Columns) == sliceIdentity(got[2].Storage.Columns),
		"t0 and t2, in different chunks, must share one []*FieldSchema slice")
	assert.False(t, sliceIdentity(got[0].Storage.Columns) == sliceIdentity(got[1].Storage.Columns),
		"t0 and t1 must not share a slice")
}
