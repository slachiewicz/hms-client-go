package hms

import (
	"context"
	"testing"

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
// fidelity (SPEC §5.4): before Table.raw existed, tableToThrift always
// started from a bare hive_metastore.NewTable(), so a
// tableFromThrift -> tableToThrift round trip silently dropped every field
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

	got := tableToThrift(tableFromThrift(seed), ptr("hive"))
	assert.Equal(t, seed, got)
}

// TestPartitionRoundTrip_PreservesUnmodelledFields is
// TestTableRoundTrip_PreservesUnmodelledFields's counterpart for Partition:
// before Partition.raw existed, partitionToThrift always started from a
// bare hive_metastore.NewPartition(), so a
// partitionFromThrift -> partitionToThrift round trip silently dropped
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

	got := partitionToThrift(partitionFromThrift(seed), ptr("hive"), "db", "t")
	assert.Equal(t, seed, got)
}
