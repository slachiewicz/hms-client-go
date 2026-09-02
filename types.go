package hms

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
)

// PrincipalType identifies the kind of principal that owns a database,
// table, or holds a privilege grant.
type PrincipalType int

// Supported principal types.
const (
	// PrincipalUser identifies an individual user.
	PrincipalUser PrincipalType = 1
	// PrincipalRole identifies a role.
	PrincipalRole PrincipalType = 2
	// PrincipalGroup identifies a group.
	PrincipalGroup PrincipalType = 3
)

// String returns the principal type's name, or "PrincipalType(<n>)" for an
// unrecognized value.
func (t PrincipalType) String() string {
	switch t {
	case PrincipalUser:
		return "USER"
	case PrincipalRole:
		return "ROLE"
	case PrincipalGroup:
		return "GROUP"
	default:
		return "PrincipalType(" + strconv.Itoa(int(t)) + ")"
	}
}

// TableType identifies the kind of table a Table describes.
type TableType string

// Supported table types.
const (
	// TableTypeManaged is a table whose data Hive owns and deletes on drop.
	TableTypeManaged TableType = "MANAGED_TABLE"
	// TableTypeExternal is a table over data Hive does not own.
	TableTypeExternal TableType = "EXTERNAL_TABLE"
	// TableTypeVirtualView is a SQL view.
	TableTypeVirtualView TableType = "VIRTUAL_VIEW"
	// TableTypeMaterializedView is a materialized SQL view.
	TableTypeMaterializedView TableType = "MATERIALIZED_VIEW"
)

// Catalog is a Hive Metastore catalog, the top-level namespace introduced in
// Hive 3 that groups databases (see SPEC §5.2).
type Catalog struct {
	// Name is the catalog's unique name.
	Name string
	// Description is a free-text description.
	Description string
	// LocationURI is the catalog's root storage location.
	LocationURI string
}

// Database is a Hive Metastore database (SPEC §5.3).
type Database struct {
	// CatalogName is the catalog the database belongs to.
	CatalogName string
	// Name is the database's unique name within its catalog.
	Name string
	// Description is a free-text description.
	Description string
	// LocationURI is the database's root storage location. When left
	// empty on a call to CreateDatabase, the client fills it in the way
	// Hive's own DDL path does before the server ever sees it (see
	// CreateDatabase).
	LocationURI string
	// Parameters holds arbitrary key/value metadata.
	Parameters map[string]string
	// OwnerName is the owning principal's name.
	OwnerName string
	// OwnerType is the kind of principal that owns the database.
	OwnerType PrincipalType
	// CreateTime is when the database was created (1.0 addition). It is
	// read-only: CreateDatabase never writes it, since the server assigns
	// it itself.
	CreateTime time.Time
}

// FieldSchema describes one column or partition key.
type FieldSchema struct {
	// Name is the column name.
	Name string
	// Type is the Hive type string (e.g. "string", "bigint").
	Type string
	// Comment is a free-text description of the column.
	Comment string
}

// SerDeInfo describes the serializer/deserializer used to read and write a
// table's data.
type SerDeInfo struct {
	// Name is the SerDe's name.
	Name string
	// SerializationLib is the fully qualified Java class implementing the
	// SerDe (e.g. "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe").
	SerializationLib string
	// Parameters holds arbitrary key/value metadata for the SerDe.
	Parameters map[string]string
}

// Order describes one column's sort direction within a sorted bucket.
type Order struct {
	// Column is the sorted column's name.
	Column string
	// Order is the sort direction: 1 for ascending, 0 for descending.
	Order int32
}

// SkewedInfo describes a table's or partition's skewed-storage optimization:
// which columns Hive considers skewed and which value combinations it keeps
// in dedicated storage locations (1.0 addition; SPEC §5.4).
//
// ColumnValueLocationMaps, the wire's third SkewedInfo field mapping each
// skewed value combination to its own storage location, has no field here:
// it is gated behind THRIFT-2063 (SPEC §1.1) and is dropped from the
// generated Thrift bindings before this package ever sees it. A value
// already on the wire is not lost across GetTable -> AlterTable even so;
// see "Round-trip fidelity" below.
type SkewedInfo struct {
	// ColumnNames lists the columns Hive considers skewed, in
	// ColumnValues' column order.
	ColumnNames []string
	// ColumnValues lists the skewed value combinations, one per skew,
	// each in ColumnNames order.
	ColumnValues [][]string
}

// StorageDescriptor describes where and how a table's or partition's data
// is stored on disk.
type StorageDescriptor struct {
	// Columns describes the data columns, in order.
	Columns []*FieldSchema
	// Location is the storage path (e.g. an HDFS or S3 URI).
	Location string
	// InputFormat is the fully qualified Java InputFormat class.
	InputFormat string
	// OutputFormat is the fully qualified Java OutputFormat class.
	OutputFormat string
	// Compressed reports whether the data is compressed.
	Compressed bool
	// NumBuckets is the number of hash buckets, or -1 if not bucketed.
	NumBuckets int32
	// SerDe describes how rows are serialized and deserialized.
	SerDe *SerDeInfo
	// BucketColumns lists the columns data is bucketed by.
	BucketColumns []string
	// SortColumns lists the columns each bucket is sorted by.
	SortColumns []*Order
	// Parameters holds arbitrary key/value metadata.
	Parameters map[string]string
	// StoredAsSubDirectories reports whether skewed values are stored in
	// their own subdirectories.
	StoredAsSubDirectories bool
	// Skewed describes the skewed-storage optimization applied to this
	// data, or nil if none (1.0 addition).
	Skewed *SkewedInfo
}

// Table is a Hive Metastore table (SPEC §5.4).
//
// A Table returned by GetTable or GetTables carries an internal snapshot of
// every field the generated Thrift Table has, including ones this struct
// does not model (e.g. Privileges, RewriteEnabled, Id, TxnId, AccessType,
// the capability lists). AlterTable starts from that snapshot and
// overwrites only the fields modelled below, so round-tripping a table
// fetched from the server through AlterTable does not silently reset the
// unmodelled ones (SPEC §5.4 "Round-trip fidelity"). Copying a Table value
// copies that snapshot's pointer, not its contents: the snapshot is shared
// and read-only, and a Table built directly (a struct literal, or one of
// the NewXxxTable constructors in formats.go) carries none, exactly as
// before this existed.
type Table struct {
	// CatalogName is the catalog the table's database belongs to.
	CatalogName string
	// DatabaseName is the name of the database the table belongs to.
	DatabaseName string
	// TableName is the table's unique name within its database.
	TableName string
	// Owner is the owning principal's name.
	Owner string
	// OwnerType is the kind of principal that owns the table (1.0
	// addition). A zero value is written to the wire as PrincipalUser,
	// matching Hive's own default.
	OwnerType PrincipalType
	// CreateTime is when the table was created.
	CreateTime time.Time
	// LastAccessTime is when the table was last accessed.
	LastAccessTime time.Time
	// Retention is the table's data retention period, in seconds.
	Retention int32
	// Storage describes where and how the table's data is stored.
	Storage *StorageDescriptor
	// PartitionKeys describes the table's partition columns, in order.
	PartitionKeys []*FieldSchema
	// Parameters holds arbitrary key/value metadata.
	Parameters map[string]string
	// ViewOriginalText is the original SQL text of a view definition.
	ViewOriginalText string
	// ViewExpandedText is the fully qualified SQL text of a view definition.
	ViewExpandedText string
	// TableType is the kind of table.
	TableType TableType

	// raw is a deep copy of the generated Thrift Table this value was
	// converted from (tableFromThrift), or nil for a Table this package
	// never read off the wire. tableToThrift starts from a fresh copy of
	// it, when present, so a field raw carries but this struct does not
	// model survives GetTable -> AlterTable unchanged. See the type's own
	// doc comment for the sharing/read-only contract.
	raw *hive_metastore.Table
}

// Partition is a Hive Metastore table partition (SPEC §5.5).
//
// Like Table, a Partition returned by the server carries an internal,
// read-only snapshot of every field the generated Thrift Partition has;
// AlterPartitions preserves whatever that snapshot carries but this struct
// does not model. See Table's doc comment for the full contract.
type Partition struct {
	// CatalogName is the catalog the partition's table belongs to.
	CatalogName string
	// DatabaseName is the name of the database the partition's table
	// belongs to.
	DatabaseName string
	// TableName is the name of the table the partition belongs to.
	TableName string
	// Values holds the partition's column values, in partition-key order.
	Values []string
	// CreateTime is when the partition was created.
	CreateTime time.Time
	// Storage describes where and how the partition's data is stored.
	Storage *StorageDescriptor
	// Parameters holds arbitrary key/value metadata.
	Parameters map[string]string

	// raw is a deep copy of the generated Thrift Partition this value was
	// converted from (partitionFromThrift), or nil for a Partition this
	// package never read off the wire. See Table.raw.
	raw *hive_metastore.Partition
}

// HiveVersion is a parsed Hive Metastore server version.
//
// The fb303 getVersion RPC that ServerVersion prefers does not always
// report the server's release number: pre-4 metastores answer with the
// metastore schema version line instead (every Hive 3.x release answers
// "3.0", and so does Hive 2.3.x). ServerVersion tells the two apart by
// probing catalog support on the connection and reports the inferred line
// as Major/Minor; see its doc comment. Callers that need the true 3.x
// patch release cannot get it from this RPC -- Raw always carries the
// server's literal answer.
type HiveVersion struct {
	// Major is the major version component.
	Major int
	// Minor is the minor version component.
	Minor int
	// Patch is the patch version component.
	Patch int
	// Raw is the original, unparsed version string as reported by the
	// server.
	Raw string
}

// String returns the version's raw, unparsed form as reported by the
// server.
func (v HiveVersion) String() string {
	return v.Raw
}

// ParseHiveVersion parses a Hive Metastore version string such as "4.0.1" or
// "3.1.3000.7.1.7.0-551" (a vendor-patched build), taking the first three
// dot-separated numeric components as Major, Minor, and Patch. A
// two-component string such as "3.0" is also accepted, with Patch defaulted
// to 0: getVersion reports the metastore schema line rather than the
// release on some servers (every 3.1.x release answers "3.0"; see
// HiveVersion's doc comment), and the schema line carries no patch
// component.
func ParseHiveVersion(s string) (HiveVersion, error) {
	raw := s
	// A vendor build may append a "-<suffix>" after the numeric components
	// (e.g. "3.1.3000.7.1.7.0-551"); only the numeric prefix is parsed.
	numeric, _, _ := strings.Cut(s, "-")
	parts := strings.Split(numeric, ".")
	if len(parts) < 2 {
		return HiveVersion{}, fmt.Errorf("hms: invalid Hive version %q: expected at least 2 dot-separated components", s)
	}
	want := min(len(parts), 3)
	nums := make([]int, want)
	for i := range want {
		n, err := strconv.Atoi(parts[i])
		if err != nil {
			return HiveVersion{}, fmt.Errorf("hms: invalid Hive version %q: %w", s, err)
		}
		nums[i] = n
	}
	v := HiveVersion{Major: nums[0], Minor: nums[1], Raw: raw}
	if want == 3 {
		v.Patch = nums[2]
	}
	return v, nil
}
