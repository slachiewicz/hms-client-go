package hms

import (
	"fmt"
	"strconv"
	"strings"
	"time"
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
}

// Table is a Hive Metastore table (SPEC §5.4).
type Table struct {
	// CatalogName is the catalog the table's database belongs to.
	CatalogName string
	// DatabaseName is the name of the database the table belongs to.
	DatabaseName string
	// TableName is the table's unique name within its database.
	TableName string
	// Owner is the owning principal's name.
	Owner string
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
}

// Partition is a Hive Metastore table partition (SPEC §5.5).
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
}

// HiveVersion is a parsed Hive Metastore server version.
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
// dot-separated numeric components as Major, Minor, and Patch.
func ParseHiveVersion(s string) (HiveVersion, error) {
	raw := s
	// A vendor build may append a "-<suffix>" after the numeric components
	// (e.g. "3.1.3000.7.1.7.0-551"); only the numeric prefix is parsed.
	numeric, _, _ := strings.Cut(s, "-")
	parts := strings.Split(numeric, ".")
	if len(parts) < 3 {
		return HiveVersion{}, fmt.Errorf("hms: invalid Hive version %q: expected at least 3 dot-separated components", s)
	}
	nums := make([]int, 3)
	for i := range 3 {
		n, err := strconv.Atoi(parts[i])
		if err != nil {
			return HiveVersion{}, fmt.Errorf("hms: invalid Hive version %q: %w", s, err)
		}
		nums[i] = n
	}
	return HiveVersion{Major: nums[0], Minor: nums[1], Patch: nums[2], Raw: raw}, nil
}
