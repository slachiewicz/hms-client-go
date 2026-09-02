package hms

// Iceberg class names and parameters.
const (
	// IcebergStorageHandler is the fully qualified Java class implementing
	// Iceberg's Hive integration storage handler.
	IcebergStorageHandler = "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler"
	// IcebergSerDe is the fully qualified Java class implementing Iceberg's
	// serializer/deserializer.
	IcebergSerDe = "org.apache.iceberg.mr.hive.HiveIcebergSerDe"
	// IcebergInputFormat is the fully qualified Java class implementing
	// Iceberg's input format.
	IcebergInputFormat = "org.apache.iceberg.mr.hive.HiveIcebergInputFormat"
	// IcebergOutputFormat is the fully qualified Java class implementing
	// Iceberg's output format.
	IcebergOutputFormat = "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat"
	// ParamMetadataLocation is the parameter key for Iceberg's metadata
	// location.
	ParamMetadataLocation = "metadata_location"
	// ParamPreviousMetadataLocation is the parameter key for Iceberg's
	// previous metadata location.
	ParamPreviousMetadataLocation = "previous_metadata_location"
	// ParamTableType is the parameter key for the table type (e.g.,
	// "ICEBERG", "DELTA").
	ParamTableType = "table_type"
	// ParamStorageHandler is the parameter key for the storage handler class.
	ParamStorageHandler = "storage_handler"
)

// Delta class names and parameters.
const (
	// DeltaInputFormat is the fully qualified Java class implementing Delta's
	// input format.
	DeltaInputFormat = "io.delta.hive.DeltaInputFormat"
	// DeltaOutputFormat is the fully qualified Java class implementing the
	// common Hive output format used by Delta.
	DeltaOutputFormat = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
	// DeltaSerDe is the fully qualified Java class implementing the common
	// Hive serializer/deserializer used by Delta.
	DeltaSerDe = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
)

// Hudi class names and parameters.
const (
	// HudiInputFormat is the fully qualified Java class implementing Hudi's
	// input format.
	HudiInputFormat = "org.apache.hudi.hadoop.HoodieParquetInputFormat"
	// HudiOutputFormat is the fully qualified Java class implementing the
	// common Hive output format used by Hudi.
	HudiOutputFormat = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
	// HudiSerDe is the fully qualified Java class implementing the Parquet
	// serializer/deserializer used by Hudi.
	HudiSerDe = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
)

// Common parameter keys.
const (
	// ParamSparkProvider is the parameter key for Spark's data source provider
	// (e.g., "delta", "hudi").
	ParamSparkProvider = "spark.sql.sources.provider"
	// ParamExternal is the parameter key indicating the table is external.
	ParamExternal = "EXTERNAL"
)

// NewIcebergTable constructs a new Iceberg table with the given database name,
// table name, storage location, metadata location, and columns. The returned
// table is configured as an EXTERNAL_TABLE with all required Iceberg parameters.
func NewIcebergTable(dbName, tableName, location, metadataLocation string, cols []*FieldSchema) *Table {
	return &Table{
		DatabaseName:  dbName,
		TableName:     tableName,
		TableType:     TableTypeExternal,
		PartitionKeys: []*FieldSchema{},
		Storage: &StorageDescriptor{
			Location:     location,
			Columns:      cols,
			InputFormat:  IcebergInputFormat,
			OutputFormat: IcebergOutputFormat,
			SerDe: &SerDeInfo{
				SerializationLib: IcebergSerDe,
				Parameters:       map[string]string{},
			},
			Parameters: map[string]string{},
		},
		Parameters: map[string]string{
			ParamStorageHandler:   IcebergStorageHandler,
			ParamTableType:        "ICEBERG",
			ParamMetadataLocation: metadataLocation,
			ParamExternal:         "TRUE",
		},
	}
}

// NewDeltaTable constructs a new Delta Lake table with the given database name,
// table name, storage location, and columns. The returned table is configured as
// an EXTERNAL_TABLE with all required Delta parameters.
func NewDeltaTable(dbName, tableName, location string, cols []*FieldSchema) *Table {
	return &Table{
		DatabaseName:  dbName,
		TableName:     tableName,
		TableType:     TableTypeExternal,
		PartitionKeys: []*FieldSchema{},
		Storage: &StorageDescriptor{
			Location:     location,
			Columns:      cols,
			InputFormat:  DeltaInputFormat,
			OutputFormat: DeltaOutputFormat,
			SerDe: &SerDeInfo{
				SerializationLib: DeltaSerDe,
				Parameters:       map[string]string{},
			},
			Parameters: map[string]string{},
		},
		Parameters: map[string]string{
			ParamSparkProvider: "delta",
			ParamTableType:     "DELTA",
			ParamExternal:      "TRUE",
		},
	}
}

// NewHudiTable constructs a new Apache Hudi table with the given database name,
// table name, storage location, columns, and partition keys. The returned table is
// configured as an EXTERNAL_TABLE with all required Hudi parameters.
func NewHudiTable(dbName, tableName, location string, cols []*FieldSchema, partitionKeys []*FieldSchema) *Table {
	return &Table{
		DatabaseName:  dbName,
		TableName:     tableName,
		TableType:     TableTypeExternal,
		PartitionKeys: partitionKeys,
		Storage: &StorageDescriptor{
			Location:     location,
			Columns:      cols,
			InputFormat:  HudiInputFormat,
			OutputFormat: HudiOutputFormat,
			SerDe: &SerDeInfo{
				SerializationLib: HudiSerDe,
				Parameters:       map[string]string{},
			},
			Parameters: map[string]string{},
		},
		Parameters: map[string]string{
			ParamSparkProvider: "hudi",
			ParamExternal:      "TRUE",
		},
	}
}

// SetIcebergMetadataLocation updates an Iceberg table's metadata location,
// moving the current metadata_location to previous_metadata_location if one
// exists. If the table's Parameters field is nil, it is initialized.
func SetIcebergMetadataLocation(t *Table, newLocation string) {
	if t.Parameters == nil {
		t.Parameters = map[string]string{}
	}
	if current, ok := t.Parameters[ParamMetadataLocation]; ok {
		t.Parameters[ParamPreviousMetadataLocation] = current
	}
	t.Parameters[ParamMetadataLocation] = newLocation
}
