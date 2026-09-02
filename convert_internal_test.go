package hms

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
	"github.com/slachiewicz/hms-client-go/internal/hmstest"
)

// serdeTypePtr returns a pointer to v, mirroring ptr (convert.go) for the
// generated SerdeType enum. Only test seeds need a *SerdeType literal, so
// this lives here rather than in convert.go.
func serdeTypePtr(v hive_metastore.SerdeType) *hive_metastore.SerdeType { return &v }

// TestTableToThrift_DefaultsOwnerTypeAndWriteId covers the fix for
// tableToThrift building a bare hive_metastore.Table{} literal, which left
// the non-pointer "optional with default" fields OwnerType and WriteId at
// their Go zero value (0) instead of the generated NewTable()'s defaults
// (PrincipalType_USER, -1): ownerType=0 is not a valid PrincipalType on the
// wire, and writeId=0 is a real write id rather than "unassigned". The
// exported Table type carries neither field, so every table this client
// builds is, from tableToThrift's point of view, always "unset" for both
// and must always get NewTable's defaults.
func TestTableToThrift_DefaultsOwnerTypeAndWriteId(t *testing.T) {
	t.Parallel()
	got := tableToThrift(&Table{DatabaseName: "d", TableName: "t"}, nil)
	require.NotNil(t, got)
	assert.Equal(t, hive_metastore.PrincipalType_USER, got.OwnerType)
	assert.Equal(t, int64(-1), got.WriteId)
}

// TestGetTable_PreservesNonDefaultOwnerType proves that a real wire round
// trip through the Hive40 fixture does not corrupt a non-default OwnerType
// already on the wire. The exported Table type has no field for OwnerType
// (tableToThrift always sends PrincipalType_USER; see
// TestTableToThrift_DefaultsOwnerTypeAndWriteId above), so this seeds the
// fixture's store directly with a table whose OwnerType is
// PrincipalType_ROLE (bypassing CreateTable/tableToThrift entirely,
// mirroring the direct-seed technique already used by
// TestGetDatabase_Hive40QualifiesNonDefaultCatalog in database_test.go),
// then fetches it back over an actual connection using the unexported
// get_table_req binding directly (this file is a white-box test, in
// package hms, precisely so it can reach that binding and tableToThrift
// without an exported shim) and asserts the value the client actually
// receives off the wire is unchanged.
func TestGetTable_PreservesNonDefaultOwnerType(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	ctx := context.Background()
	c, err := New(ctx, srv.URI())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	srv.Store().Tables["hive.db.t"] = &hive_metastore.Table{
		DbName:    "db",
		TableName: "t",
		OwnerType: hive_metastore.PrincipalType_ROLE,
		WriteId:   -1,
		CatName:   ptr("hive"),
	}

	cn, err := c.acquire(ctx, 0)
	require.NoError(t, err)
	defer c.release(0, cn)

	res, err := cn.getTableReq(ctx, newGetTableRequest("db", "t", ptr("hive")))
	require.NoError(t, err)
	assert.Equal(t, hive_metastore.PrincipalType_ROLE, res.Table.OwnerType)
}

// TestPartitionToThrift_DefaultsWriteId is partitionToThrift's counterpart
// to TestTableToThrift_DefaultsOwnerTypeAndWriteId above: it must also
// build from hive_metastore.NewPartition() rather than a bare struct
// literal, or the non-pointer "optional with default" WriteId field (no
// equivalent on the exported Partition type) would be left at the Go zero
// value 0 instead of NewPartition's default of -1 — writeId=0 is a real
// write id on the wire, not "unassigned".
func TestPartitionToThrift_DefaultsWriteId(t *testing.T) {
	t.Parallel()
	got := partitionToThrift(&Partition{Values: []string{"2024-01-01"}}, nil, "d", "t")
	require.NotNil(t, got)
	assert.Equal(t, int64(-1), got.WriteId)
}

// TestTableRoundTrip_PreservesUnmodelledFields covers G3, round-trip
// fidelity (SPEC §5.4): before Table.raw existed, the alter path too
// started from a bare hive_metastore.NewTable(), so a
// tableFromThrift -> tableToThriftFrom round trip silently dropped every field
// hms.Table has no field for -- Privileges, RewriteEnabled, Id, TxnId,
// AccessType, the capability lists, Temporary, CreationMetadata, and
// SkewedInfo's names/values -- exactly what a GetTable -> AlterTable call
// against a table Spark or Trino registered would do to it. seed is built
// with every modelled field also populated (and set to a value that itself
// survives the modelled conversion unchanged, e.g. the CatName passed back
// into tableToThrift matches seed.CatName), so this asserts whole-struct
// equality rather than field-by-field: if any field, modelled or not, come
// back different from seed, the round trip lost or altered something.
func TestTableRoundTrip_PreservesUnmodelledFields(t *testing.T) {
	t.Parallel()

	seed := hive_metastore.NewTable() // OwnerType: USER, WriteId: -1
	seed.DbName = "db"
	seed.TableName = "t"
	seed.Owner = "me"
	seed.CreateTime = 1700000000
	seed.LastAccessTime = 1700000100
	seed.Retention = 42
	seed.CatName = ptr("hive")
	seed.ViewOriginalText = "SELECT 1"
	seed.ViewExpandedText = "SELECT 1 FROM db.t"
	seed.TableType = "EXTERNAL_TABLE"
	seed.Parameters = map[string]string{"k": "v"}
	seed.PartitionKeys = []*hive_metastore.FieldSchema{{Name: "dt", Type: "string"}}
	stored := true
	seed.Sd = &hive_metastore.StorageDescriptor{
		SkewedInfo: &hive_metastore.SkewedInfo{
			SkewedColNames:  []string{"region"},
			SkewedColValues: [][]string{{"us"}, {"eu"}},
		},
		StoredAsSubDirectories: &stored,
		// SerdeInfo's Description/SerializerClass/DeserializerClass/
		// SerdeType have no field on hms.SerDeInfo (only Name,
		// SerializationLib, and Parameters do); storageToThrift/
		// serDeToThrift must thread the raw snapshot's SerdeInfo through
		// so these survive too, not just the Table/Partition-level
		// unmodelled fields below.
		SerdeInfo: &hive_metastore.SerDeInfo{
			Description:       ptr("a serde description"),
			SerializerClass:   ptr("com.example.Serializer"),
			DeserializerClass: ptr("com.example.Deserializer"),
			SerdeType:         serdeTypePtr(hive_metastore.SerdeType_HIVE),
		},
	}

	// Fields hms.Table has no field for. GroupPrivileges and
	// RolePrivileges are set to empty (non-nil) maps rather than left
	// nil: PrincipalPrivilegeSet declares all three as required Thrift
	// fields, and the wire round trip deepCopyThrift performs (a real
	// serialize/deserialize, not an in-memory copy) decodes a required
	// map field that was never sent as empty, not nil -- the same
	// convention copyStringMap documents for this package's own
	// converters, just observed here one level further down, inside a
	// field this package does not itself convert.
	seed.Privileges = &hive_metastore.PrincipalPrivilegeSet{
		UserPrivileges: map[string][]*hive_metastore.PrivilegeGrantInfo{
			"alice": {{Privilege: "SELECT", Grantor: "bob", GrantorType: hive_metastore.PrincipalType_USER}},
		},
		GroupPrivileges: map[string][]*hive_metastore.PrivilegeGrantInfo{},
		RolePrivileges:  map[string][]*hive_metastore.PrivilegeGrantInfo{},
	}
	seed.Temporary = true
	rewriteEnabled := true
	seed.RewriteEnabled = &rewriteEnabled
	seed.CreationMetadata = &hive_metastore.CreationMetadata{
		CatName:    "hive",
		DbName:     "db",
		TblName:    "t",
		TablesUsed: []string{"db.other"},
	}
	accessType := int8(1)
	seed.AccessType = &accessType
	seed.RequiredReadCapabilities = []string{"CONNECTORREAD"}
	seed.RequiredWriteCapabilities = []string{"CONNECTORWRITE"}
	id := int64(42)
	seed.ID = &id
	txnID := int64(99)
	seed.TxnId = &txnID

	got := tableToThriftFrom(tableFromThrift(seed), ptr("hive"))
	assert.Equal(t, seed, got)
}

// TestPartitionRoundTrip_PreservesUnmodelledFields is
// TestTableRoundTrip_PreservesUnmodelledFields's counterpart for Partition:
// before Partition.raw existed, partitionToThriftFrom always started from a
// bare hive_metastore.NewPartition(), so a
// partitionFromThrift -> partitionToThriftFrom round trip silently dropped
// every field hms.Partition has no field for -- LastAccessTime, Privileges,
// WriteId, IsStatsCompliant, ColStats, FileMetadata, and (one level down,
// inside Sd) StorageDescriptor.SerdeInfo's Description/SerializerClass/
// DeserializerClass/SerdeType -- exactly what a
// GetPartitions -> AlterPartitions call would do to it.
func TestPartitionRoundTrip_PreservesUnmodelledFields(t *testing.T) {
	t.Parallel()

	seed := hive_metastore.NewPartition() // WriteId: -1
	seed.Values = []string{"2024-01-01"}
	seed.DbName = "db"
	seed.TableName = "t"
	seed.CreateTime = 1700000000
	seed.LastAccessTime = 1700000100
	seed.CatName = ptr("hive")
	seed.Parameters = map[string]string{"k": "v"}
	seed.Sd = &hive_metastore.StorageDescriptor{
		SerdeInfo: &hive_metastore.SerDeInfo{
			Description:       ptr("a serde description"),
			SerializerClass:   ptr("com.example.Serializer"),
			DeserializerClass: ptr("com.example.Deserializer"),
			SerdeType:         serdeTypePtr(hive_metastore.SerdeType_HIVE),
		},
	}

	// Fields hms.Partition has no field for. See
	// TestTableRoundTrip_PreservesUnmodelledFields for why
	// GroupPrivileges/RolePrivileges are non-nil empty maps rather than
	// left nil.
	seed.Privileges = &hive_metastore.PrincipalPrivilegeSet{
		UserPrivileges: map[string][]*hive_metastore.PrivilegeGrantInfo{
			"alice": {{Privilege: "SELECT", Grantor: "bob", GrantorType: hive_metastore.PrincipalType_USER}},
		},
		GroupPrivileges: map[string][]*hive_metastore.PrivilegeGrantInfo{},
		RolePrivileges:  map[string][]*hive_metastore.PrivilegeGrantInfo{},
	}
	statsCompliant := true
	seed.IsStatsCompliant = &statsCompliant
	seed.ColStats = &hive_metastore.ColumnStatistics{
		StatsDesc: &hive_metastore.ColumnStatisticsDesc{
			IsTblLevel: false,
			DbName:     "db",
			TableName:  "t",
		},
		StatsObj: []*hive_metastore.ColumnStatisticsObj{},
	}

	got := partitionToThriftFrom(partitionFromThrift(seed), ptr("hive"), "db", "t")
	assert.Equal(t, seed, got)
}

// int64p and float64p are convert_internal_test.go's local counterparts to
// ptr (convert.go), for the pointer types column-stats fixtures need.
func int64p(v int64) *int64       { return &v }
func float64p(v float64) *float64 { return &v }

// TestColumnStatisticsFromThrift covers every ColumnStatisticsData union
// arm columnStatisticsFromThrift converts (SPEC §5.8), including a nil
// LowValue/HighValue on each arm that has them, proving every pointer
// field is nil-safe independently of its sibling.
func TestColumnStatisticsFromThrift(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		obj  *hive_metastore.ColumnStatisticsObj
		want ColumnStatistics
	}{
		{
			name: "boolean",
			obj: &hive_metastore.ColumnStatisticsObj{
				ColName: "c", ColType: "boolean",
				StatsData: &hive_metastore.ColumnStatisticsData{
					BooleanStats: &hive_metastore.BooleanColumnStatsData{NumTrues: 1, NumFalses: 2, NumNulls: 3},
				},
			},
			want: ColumnStatistics{ColumnName: "c", ColumnType: "boolean", Boolean: &BooleanColumnStats{NumTrues: 1, NumFalses: 2, NumNulls: 3}},
		},
		{
			name: "long with bounds",
			obj: &hive_metastore.ColumnStatisticsObj{
				ColName: "c", ColType: "bigint",
				StatsData: &hive_metastore.ColumnStatisticsData{
					LongStats: &hive_metastore.LongColumnStatsData{LowValue: int64p(1), HighValue: int64p(2), NumNulls: 3, NumDVs: 4},
				},
			},
			want: ColumnStatistics{ColumnName: "c", ColumnType: "bigint", Long: &LongColumnStats{LowValue: int64p(1), HighValue: int64p(2), NumNulls: 3, NumDistinct: 4}},
		},
		{
			name: "long nil bounds",
			obj: &hive_metastore.ColumnStatisticsObj{
				ColName: "c", ColType: "bigint",
				StatsData: &hive_metastore.ColumnStatisticsData{
					LongStats: &hive_metastore.LongColumnStatsData{NumNulls: 3, NumDVs: 4},
				},
			},
			want: ColumnStatistics{ColumnName: "c", ColumnType: "bigint", Long: &LongColumnStats{NumNulls: 3, NumDistinct: 4}},
		},
		{
			name: "double with bounds",
			obj: &hive_metastore.ColumnStatisticsObj{
				ColName: "c", ColType: "double",
				StatsData: &hive_metastore.ColumnStatisticsData{
					DoubleStats: &hive_metastore.DoubleColumnStatsData{LowValue: float64p(1.5), HighValue: float64p(2.5), NumNulls: 3, NumDVs: 4},
				},
			},
			want: ColumnStatistics{ColumnName: "c", ColumnType: "double", Double: &DoubleColumnStats{LowValue: float64p(1.5), HighValue: float64p(2.5), NumNulls: 3, NumDistinct: 4}},
		},
		{
			name: "double nil bounds",
			obj: &hive_metastore.ColumnStatisticsObj{
				ColName: "c", ColType: "double",
				StatsData: &hive_metastore.ColumnStatisticsData{
					DoubleStats: &hive_metastore.DoubleColumnStatsData{NumNulls: 3, NumDVs: 4},
				},
			},
			want: ColumnStatistics{ColumnName: "c", ColumnType: "double", Double: &DoubleColumnStats{NumNulls: 3, NumDistinct: 4}},
		},
		{
			name: "string",
			obj: &hive_metastore.ColumnStatisticsObj{
				ColName: "c", ColType: "string",
				StatsData: &hive_metastore.ColumnStatisticsData{
					StringStats: &hive_metastore.StringColumnStatsData{MaxColLen: 10, AvgColLen: 5.5, NumNulls: 1, NumDVs: 2},
				},
			},
			want: ColumnStatistics{ColumnName: "c", ColumnType: "string", String: &StringColumnStats{MaxColLen: 10, AvgColLen: 5.5, NumNulls: 1, NumDistinct: 2}},
		},
		{
			name: "binary",
			obj: &hive_metastore.ColumnStatisticsObj{
				ColName: "c", ColType: "binary",
				StatsData: &hive_metastore.ColumnStatisticsData{
					BinaryStats: &hive_metastore.BinaryColumnStatsData{MaxColLen: 10, AvgColLen: 5.5, NumNulls: 1},
				},
			},
			want: ColumnStatistics{ColumnName: "c", ColumnType: "binary", Binary: &BinaryColumnStats{MaxColLen: 10, AvgColLen: 5.5, NumNulls: 1}},
		},
		{
			name: "decimal with bounds",
			obj: &hive_metastore.ColumnStatisticsObj{
				ColName: "c", ColType: "decimal(10,2)",
				StatsData: &hive_metastore.ColumnStatisticsData{
					DecimalStats: &hive_metastore.DecimalColumnStatsData{
						LowValue:  &hive_metastore.Decimal{Unscaled: []byte{1}, Scale: 2},
						HighValue: &hive_metastore.Decimal{Unscaled: []byte{2}, Scale: 2},
						NumNulls:  3, NumDVs: 4,
					},
				},
			},
			want: ColumnStatistics{ColumnName: "c", ColumnType: "decimal(10,2)", Decimal: &DecimalColumnStats{
				LowValue: &Decimal{Unscaled: []byte{1}, Scale: 2}, HighValue: &Decimal{Unscaled: []byte{2}, Scale: 2},
				NumNulls: 3, NumDistinct: 4,
			}},
		},
		{
			name: "decimal nil bounds",
			obj: &hive_metastore.ColumnStatisticsObj{
				ColName: "c", ColType: "decimal(10,2)",
				StatsData: &hive_metastore.ColumnStatisticsData{
					DecimalStats: &hive_metastore.DecimalColumnStatsData{NumNulls: 3, NumDVs: 4},
				},
			},
			want: ColumnStatistics{ColumnName: "c", ColumnType: "decimal(10,2)", Decimal: &DecimalColumnStats{NumNulls: 3, NumDistinct: 4}},
		},
		{
			name: "date with bounds",
			obj: &hive_metastore.ColumnStatisticsObj{
				ColName: "c", ColType: "date",
				StatsData: &hive_metastore.ColumnStatisticsData{
					DateStats: &hive_metastore.DateColumnStatsData{
						LowValue:  &hive_metastore.Date{DaysSinceEpoch: 0},
						HighValue: &hive_metastore.Date{DaysSinceEpoch: 5},
						NumNulls:  1, NumDVs: 2,
					},
				},
			},
			want: ColumnStatistics{ColumnName: "c", ColumnType: "date", Date: &DateColumnStats{
				LowValue:  timePtr(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)),
				HighValue: timePtr(time.Date(1970, 1, 6, 0, 0, 0, 0, time.UTC)),
				NumNulls:  1, NumDistinct: 2,
			}},
		},
		{
			name: "date nil bounds",
			obj: &hive_metastore.ColumnStatisticsObj{
				ColName: "c", ColType: "date",
				StatsData: &hive_metastore.ColumnStatisticsData{
					DateStats: &hive_metastore.DateColumnStatsData{NumNulls: 1, NumDVs: 2},
				},
			},
			want: ColumnStatistics{ColumnName: "c", ColumnType: "date", Date: &DateColumnStats{NumNulls: 1, NumDistinct: 2}},
		},
		{
			name: "timestamp with bounds",
			obj: &hive_metastore.ColumnStatisticsObj{
				ColName: "c", ColType: "timestamp",
				StatsData: &hive_metastore.ColumnStatisticsData{
					TimestampStats: &hive_metastore.TimestampColumnStatsData{
						LowValue:  &hive_metastore.Timestamp{SecondsSinceEpoch: 0},
						HighValue: &hive_metastore.Timestamp{SecondsSinceEpoch: 3600},
						NumNulls:  1, NumDVs: 2,
					},
				},
			},
			want: ColumnStatistics{ColumnName: "c", ColumnType: "timestamp", Timestamp: &TimestampColumnStats{
				LowValue:  timePtr(time.Unix(0, 0).UTC()),
				HighValue: timePtr(time.Unix(3600, 0).UTC()),
				NumNulls:  1, NumDistinct: 2,
			}},
		},
		{
			name: "timestamp nil bounds",
			obj: &hive_metastore.ColumnStatisticsObj{
				ColName: "c", ColType: "timestamp",
				StatsData: &hive_metastore.ColumnStatisticsData{
					TimestampStats: &hive_metastore.TimestampColumnStatsData{NumNulls: 1, NumDVs: 2},
				},
			},
			want: ColumnStatistics{ColumnName: "c", ColumnType: "timestamp", Timestamp: &TimestampColumnStats{NumNulls: 1, NumDistinct: 2}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := columnStatisticsFromThrift(tt.obj)
			assert.Equal(t, tt.want, got)
		})
	}
}

// timePtr returns a pointer to t, mirroring ptr (convert.go) for time.Time
// literals in column-stats fixtures.
func timePtr(t time.Time) *time.Time { return &t }

// TestDecimalFromThrift_CopiesUnscaled covers the fix for
// decimalFromThrift assigning d.Unscaled directly instead of copying it
// (mirroring TestDatabaseFromThrift_CopiesParameters's rationale): the
// result must never alias the wire struct's slice.
func TestDecimalFromThrift_CopiesUnscaled(t *testing.T) {
	t.Parallel()
	unscaled := []byte{1, 2, 3}
	wire := &hive_metastore.Decimal{Unscaled: unscaled, Scale: 2}

	out := decimalFromThrift(wire)
	require.NotNil(t, out)
	require.Equal(t, unscaled, out.Unscaled)

	out.Unscaled[0] = 0xFF
	assert.Equal(t, byte(1), unscaled[0], "decimalFromThrift must not alias the wire struct's Unscaled slice")
}

// sliceIdentity returns fs's backing array's address as a comparable
// value (reflect.Value.Pointer, not assert.Equal: two distinct backing
// arrays holding equal *FieldSchema pointers would otherwise look
// identical to assert.Equal's reflect.DeepEqual), so two []*FieldSchema
// slices can be checked for actually sharing one backing array rather
// than merely being equal.
func sliceIdentity(fs []*FieldSchema) uintptr { return reflect.ValueOf(fs).Pointer() }

// TestPartitionsFromThrift_InternsIdenticalColumns covers G11's
// column-list interning (columnIntern, storageFromThriftIntern): within
// one partitionsFromThrift call, two partitions whose Storage.Cols are
// equal (Name/Type/Comment) end up sharing the exact same []*FieldSchema
// slice -- not merely an equal one -- while a partition with different
// columns gets its own, distinct slice.
func TestPartitionsFromThrift_InternsIdenticalColumns(t *testing.T) {
	t.Parallel()
	colsA := []*hive_metastore.FieldSchema{{Name: "a", Type: "string"}, {Name: "b", Type: "int"}}
	// colsA2 is field-for-field equal to colsA but a distinct slice and
	// distinct *FieldSchema values, so any sharing observed below can only
	// come from columnIntern, not from the two Partitions pointing at the
	// same wire struct to begin with.
	colsA2 := []*hive_metastore.FieldSchema{{Name: "a", Type: "string"}, {Name: "b", Type: "int"}}
	colsB := []*hive_metastore.FieldSchema{{Name: "c", Type: "string"}}

	p1 := hive_metastore.NewPartition()
	p1.Values = []string{"1"}
	p1.Sd = &hive_metastore.StorageDescriptor{Cols: colsA}
	p2 := hive_metastore.NewPartition()
	p2.Values = []string{"2"}
	p2.Sd = &hive_metastore.StorageDescriptor{Cols: colsA2}
	p3 := hive_metastore.NewPartition()
	p3.Values = []string{"3"}
	p3.Sd = &hive_metastore.StorageDescriptor{Cols: colsB}

	out := partitionsFromThrift([]*hive_metastore.Partition{p1, p2, p3})
	require.Len(t, out, 3)
	require.Len(t, out[0].Storage.Columns, 2)
	require.Len(t, out[1].Storage.Columns, 2)
	require.Len(t, out[2].Storage.Columns, 1)
	assert.Equal(t, out[0].Storage.Columns, out[1].Storage.Columns)

	assert.True(t, sliceIdentity(out[0].Storage.Columns) == sliceIdentity(out[1].Storage.Columns),
		"partitions with equal columns must share one []*FieldSchema slice")
	assert.False(t, sliceIdentity(out[0].Storage.Columns) == sliceIdentity(out[2].Storage.Columns),
		"partitions with different columns must not share a slice")
}

// TestPartitionsFromThriftIntern_SharesAcrossCalls covers the extension
// GetPartitionsByNames and GetPartitionsSeq rely on: passing the same
// columnIntern to more than one partitionsFromThriftIntern call (one per
// chunk) shares identical column lists across those calls too, not just
// within a single one.
func TestPartitionsFromThriftIntern_SharesAcrossCalls(t *testing.T) {
	t.Parallel()
	cols1 := []*hive_metastore.FieldSchema{{Name: "a", Type: "string"}}
	cols2 := []*hive_metastore.FieldSchema{{Name: "a", Type: "string"}}

	p1 := hive_metastore.NewPartition()
	p1.Values = []string{"1"}
	p1.Sd = &hive_metastore.StorageDescriptor{Cols: cols1}
	p2 := hive_metastore.NewPartition()
	p2.Values = []string{"2"}
	p2.Sd = &hive_metastore.StorageDescriptor{Cols: cols2}

	in := make(columnIntern)
	chunk1 := partitionsFromThriftIntern([]*hive_metastore.Partition{p1}, in)
	chunk2 := partitionsFromThriftIntern([]*hive_metastore.Partition{p2}, in)
	require.Len(t, chunk1, 1)
	require.Len(t, chunk2, 1)

	assert.True(t, sliceIdentity(chunk1[0].Storage.Columns) == sliceIdentity(chunk2[0].Storage.Columns),
		"two partitionsFromThriftIntern calls sharing one columnIntern must share equal columns' slice")
}

// TestTableFromThriftIntern_InternsIdenticalColumns mirrors
// TestPartitionsFromThrift_InternsIdenticalColumns for tableFromThriftIntern
// (GetTables/GetTablesSeq's own column-list interning): two tables sharing
// one columnIntern and equal Storage.Cols end up with the same
// []*FieldSchema slice, while a table with different columns does not.
func TestTableFromThriftIntern_InternsIdenticalColumns(t *testing.T) {
	t.Parallel()
	colsA := []*hive_metastore.FieldSchema{{Name: "a", Type: "string"}}
	colsA2 := []*hive_metastore.FieldSchema{{Name: "a", Type: "string"}}
	colsB := []*hive_metastore.FieldSchema{{Name: "b", Type: "int"}}

	t1 := hive_metastore.NewTable()
	t1.TableName = "t1"
	t1.Sd = &hive_metastore.StorageDescriptor{Cols: colsA}
	t2 := hive_metastore.NewTable()
	t2.TableName = "t2"
	t2.Sd = &hive_metastore.StorageDescriptor{Cols: colsA2}
	t3 := hive_metastore.NewTable()
	t3.TableName = "t3"
	t3.Sd = &hive_metastore.StorageDescriptor{Cols: colsB}

	in := make(columnIntern)
	out1 := tableFromThriftIntern(t1, in)
	out2 := tableFromThriftIntern(t2, in)
	out3 := tableFromThriftIntern(t3, in)

	assert.True(t, sliceIdentity(out1.Storage.Columns) == sliceIdentity(out2.Storage.Columns),
		"tables with equal columns must share one []*FieldSchema slice")
	assert.False(t, sliceIdentity(out1.Storage.Columns) == sliceIdentity(out3.Storage.Columns),
		"tables with different columns must not share a slice")
}
