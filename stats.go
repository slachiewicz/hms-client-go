package hms

import (
	"context"
	"time"

	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
)

// ColumnStatistics is one column's aggregate statistics, as returned by
// GetTableColumnStatistics (SPEC §5.8). Read-only in 1.0; writing
// statistics is out of scope.
type ColumnStatistics struct {
	// ColumnName and ColumnType echo the column as the server reports it.
	ColumnName string
	ColumnType string

	// Exactly one of the following is set, matching ColumnStatisticsData's
	// wire union (idl/hive_metastore.thrift): the field corresponding to
	// ColumnType's Hive category is non-nil, the rest are nil.
	Boolean   *BooleanColumnStats
	Long      *LongColumnStats
	Double    *DoubleColumnStats
	String    *StringColumnStats
	Binary    *BinaryColumnStats
	Decimal   *DecimalColumnStats
	Date      *DateColumnStats
	Timestamp *TimestampColumnStats
}

// BooleanColumnStats is ColumnStatisticsData's booleanStats arm.
type BooleanColumnStats struct{ NumTrues, NumFalses, NumNulls int64 }

// LongColumnStats is ColumnStatisticsData's longStats arm. LowValue and
// HighValue are nil when the server did not compute a bound, independently
// of one another.
type LongColumnStats struct {
	LowValue, HighValue   *int64
	NumNulls, NumDistinct int64
}

// DoubleColumnStats is ColumnStatisticsData's doubleStats arm. LowValue and
// HighValue are nil when the server did not compute a bound, independently
// of one another.
type DoubleColumnStats struct {
	LowValue, HighValue   *float64
	NumNulls, NumDistinct int64
}

// StringColumnStats is ColumnStatisticsData's stringStats arm.
type StringColumnStats struct {
	MaxColLen             int64
	AvgColLen             float64
	NumNulls, NumDistinct int64
}

// BinaryColumnStats is ColumnStatisticsData's binaryStats arm. Binary
// column statistics carry no distinct-value count on the wire.
type BinaryColumnStats struct {
	MaxColLen int64
	AvgColLen float64
	NumNulls  int64
}

// DecimalColumnStats is ColumnStatisticsData's decimalStats arm. LowValue
// and HighValue are nil when the server did not compute a bound,
// independently of one another.
type DecimalColumnStats struct {
	LowValue, HighValue   *Decimal
	NumNulls, NumDistinct int64
}

// DateColumnStats is ColumnStatisticsData's dateStats arm. LowValue and
// HighValue are UTC time.Time values at that day's midnight (converted
// from the wire's daysSinceEpoch), nil when the server did not compute a
// bound, independently of one another.
type DateColumnStats struct {
	LowValue, HighValue   *time.Time
	NumNulls, NumDistinct int64
}

// TimestampColumnStats is ColumnStatisticsData's timestampStats arm.
// LowValue and HighValue are UTC time.Time values (converted from the
// wire's secondsSinceEpoch), nil when the server did not compute a bound,
// independently of one another.
type TimestampColumnStats struct {
	LowValue, HighValue   *time.Time
	NumNulls, NumDistinct int64
}

// Decimal is the generated Decimal type's exported counterpart: an
// arbitrary-precision decimal as HMS represents it on the wire (unscaled
// two's-complement big-endian bytes plus a base-10 scale), used by
// DecimalColumnStats. This package does not interpret Unscaled/Scale into
// a numeric type of its own.
type Decimal struct {
	Unscaled []byte
	Scale    int16
}

// newTableStatsRequest builds a TableStatsRequest from
// hive_metastore.NewTableStatsRequest() rather than a bare struct literal,
// so the non-pointer "optional with default" fields Engine and ID (no
// equivalent on this package's exported API, and absent from the wire
// entirely pre-4.x; see GetTableColumnStatistics's doc comment) keep
// NewTableStatsRequest's defaults ("hive" and -1) instead of falling back
// to the Go zero values (see newGetTableRequest's identical treatment in
// table.go).
func newTableStatsRequest(dbName, tblName string, cat *string, columns []string) *hive_metastore.TableStatsRequest {
	req := hive_metastore.NewTableStatsRequest()
	req.DbName = dbName
	req.TblName = tblName
	req.ColNames = columns
	req.CatName = cat
	return req
}

// GetTableColumnStatistics returns the aggregate column statistics for
// columns of the table named tbl in database db (SPEC §5.8), wrapping
// get_table_statistics_req rather than the older per-call
// get_table_column_statistics, so the whole columns list is fetched in one
// round trip. An empty columns returns (nil, nil) without issuing the RPC:
// there is nothing to ask the server for. The result is in the server's own
// order (TableStatsResult_.TableStats), not necessarily columns' order; a
// column the server has no statistics for is simply absent from the
// result, not an error.
//
// get_table_statistics_req exists on every supported version (Hive 2.3+),
// verified against hive_metastore.thrift at rel/release-2.3.9,
// rel/release-3.1.3, and the 4.2.1 IDL this client is generated from, so
// this issues the request-variant RPC directly with no legacy fallback.
// TableStatsRequest's own fields differ across that same history --
// 2.3.9 declares only dbName/tblName/colNames, 3.1.3 adds catName, and
// 4.2.1 adds validWriteIdList/engine/id -- but a pre-4.x server's Thrift
// decoder silently skips a field it does not recognize (SPEC §5.8), and on
// Hive 2.3 the effective catalog resolves to nil exactly as every other
// catalog-scoped call resolves it (SPEC §5.0), so no catName is ever
// written to a server whose own IDL never declared one.
//
// Hive 4's metastore stores column statistics per computing engine
// (TableStatsRequest.Engine); this call always requests the "hive" engine's
// statistics (Engine's IDL default, via newTableStatsRequest, and never
// overridden here), so statistics Spark, Impala, or another engine
// computed and stored under its own engine name are not returned. An
// engine option is out of scope for 1.0.
func (c *Client) GetTableColumnStatistics(ctx context.Context, db, tbl string, columns []string, opts ...CatalogOption) ([]ColumnStatistics, error) {
	if len(columns) == 0 {
		return nil, nil
	}
	var out []ColumnStatistics
	err := c.read(ctx, "get_table_statistics_req", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
		resp, err := cn.getTableStatisticsReq(ctx, newTableStatsRequest(db, tbl, cat, columns))
		if err != nil {
			return err
		}
		out = columnStatisticsListFromThrift(resp.TableStats)
		return nil
	})
	return out, err
}
