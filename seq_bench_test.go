package hms_test

import (
	"context"
	"fmt"
	"testing"

	hms "github.com/slachiewicz/hms-client-go"
	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
	"github.com/slachiewicz/hms-client-go/internal/hmstest"
)

// benchPartitionFixture seeds a Hive40 fake server directly (bypassing
// CreateTable/AddPartitions' own RPC and conversion cost, which the
// benchmarks below are not measuring) with a table of partitionCount
// partitions, each carrying the same columnCount-column schema -- the
// common case column-list interning (G11) targets: every partition of a
// table almost always shares its table's own schema. The fake server
// still talks real Thrift-over-TCP (internal/hmstest.Start dials a real
// net.Listener), so every partition's Storage.Cols is decoded fresh off
// the wire on each RPC regardless of how the fixture itself shares Go
// objects in the store -- the fixture's own sharing does not itself
// explain any allocation difference the benchmarks below observe.
func benchPartitionFixture(b *testing.B, partitionCount, columnCount int) (c *hms.Client, ctx context.Context, dbName, tableName string) {
	b.Helper()
	srv := hmstest.Start(b, hmstest.Hive40)
	var err error
	c, err = hms.New(context.Background(), srv.URI())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = c.Close() })
	ctx = context.Background()
	dbName, tableName = "db", "t"

	cols := make([]*hive_metastore.FieldSchema, columnCount)
	for i := range cols {
		cols[i] = &hive_metastore.FieldSchema{Name: fmt.Sprintf("col%03d", i), Type: "string"}
	}

	tbl := hive_metastore.NewTable()
	tbl.DbName = dbName
	tbl.TableName = tableName
	tbl.PartitionKeys = []*hive_metastore.FieldSchema{{Name: "dt", Type: "string"}}
	srv.SeedTable(tbl)

	parts := make([]*hive_metastore.Partition, partitionCount)
	for i := range parts {
		p := hive_metastore.NewPartition()
		p.DbName = dbName
		p.TableName = tableName
		p.Values = []string{fmt.Sprintf("d%04d", i)}
		p.Sd = &hive_metastore.StorageDescriptor{Cols: cols}
		parts[i] = p
	}
	srv.SeedPartitions("hive", dbName, tableName, parts)

	return c, ctx, dbName, tableName
}

// BenchmarkGetPartitions and BenchmarkGetPartitionsSeq compare G11's
// streaming form against GetPartitions on a 2,000-partition x 100-column
// fixture, reporting allocs/op (run with -benchmem, or just -bench since
// this file calls b.ReportAllocs() itself): GetPartitionsSeq should show
// materially fewer allocations, both because it never holds the whole
// 2,000-partition result in memory at once (chunked at the default
// WithChunkSize=1000, so this fixture's response spans 2 chunks) and
// because column-list interning (G11) collapses every partition's
// 100-column Storage.Columns, identical across the whole table, into a
// single shared []*FieldSchema instead of 2,000 separate copies.
func BenchmarkGetPartitions(b *testing.B) {
	c, ctx, dbName, tableName := benchPartitionFixture(b, 2000, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parts, err := c.GetPartitions(ctx, dbName, tableName, -1)
		if err != nil {
			b.Fatal(err)
		}
		if len(parts) != 2000 {
			b.Fatalf("got %d partitions, want 2000", len(parts))
		}
	}
}

// BenchmarkGetPartitionsSeq is BenchmarkGetPartitions' counterpart for
// GetPartitionsSeq; see BenchmarkGetPartitions' doc comment for the
// comparison this pair is for.
func BenchmarkGetPartitionsSeq(b *testing.B) {
	c, ctx, dbName, tableName := benchPartitionFixture(b, 2000, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := 0
		for p, err := range c.GetPartitionsSeq(ctx, dbName, tableName) {
			if err != nil {
				b.Fatal(err)
			}
			if p == nil {
				b.Fatal("nil partition")
			}
			n++
		}
		if n != 2000 {
			b.Fatalf("got %d partitions, want 2000", n)
		}
	}
}
