package hms

import (
	"math"
	"time"

	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
)

// ptr returns a pointer to a copy of s. It exists so a string literal or
// value can be passed where the generated Thrift bindings require *string.
func ptr(s string) *string { return &s }

// deref returns *p, or the empty string when p is nil.
func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// catalogFromThrift converts a generated Catalog to the exported Catalog
// type. It returns nil for a nil input.
func catalogFromThrift(c *hive_metastore.Catalog) *Catalog {
	if c == nil {
		return nil
	}
	return &Catalog{
		Name:        c.Name,
		Description: deref(c.Description),
		LocationURI: c.LocationUri,
	}
}

// catalogToThrift converts the exported Catalog type to its generated wire
// representation. It returns nil for a nil input.
func catalogToThrift(cat *Catalog) *hive_metastore.Catalog {
	if cat == nil {
		return nil
	}
	out := &hive_metastore.Catalog{
		Name:        cat.Name,
		LocationUri: cat.LocationURI,
	}
	if cat.Description != "" {
		out.Description = ptr(cat.Description)
	}
	return out
}

// databaseFromThrift converts a generated Database to the exported Database
// type. cat is the effective catalog resolved for the call (see
// (*Client).resolveCat); when non-nil it always wins over the wire value so
// the returned Database.CatalogName reflects the catalog the caller actually
// asked about, even on a server (e.g. Hive 2.3) that does not report or
// understand catalogs. It returns nil for a nil input.
func databaseFromThrift(d *hive_metastore.Database, cat *string) *Database {
	if d == nil {
		return nil
	}
	out := &Database{
		Name:        d.Name,
		Description: d.Description,
		LocationURI: d.LocationUri,
		Parameters:  copyStringMap(d.Parameters),
		OwnerName:   deref(d.OwnerName),
	}
	if d.OwnerType != nil {
		out.OwnerType = PrincipalType(*d.OwnerType)
	}
	switch {
	case cat != nil:
		out.CatalogName = *cat
	case d.CatalogName != nil:
		out.CatalogName = *d.CatalogName
	default:
		out.CatalogName = defaultCatalog
	}
	return out
}

// databaseToThrift converts the exported Database type to its generated
// wire representation. cat is the effective catalog resolved for the call
// (see (*Client).resolveCat); it becomes the wire CatalogName field
// (possibly nil, when the connection is known not to support catalogs). It
// returns nil for a nil input.
func databaseToThrift(db *Database, cat *string) *hive_metastore.Database {
	if db == nil {
		return nil
	}
	out := &hive_metastore.Database{
		Name:        db.Name,
		Description: db.Description,
		LocationUri: db.LocationURI,
		Parameters:  copyStringMap(db.Parameters),
		CatalogName: cat,
	}
	if db.OwnerName != "" {
		out.OwnerName = ptr(db.OwnerName)
	}
	if db.OwnerType != 0 {
		pt := hive_metastore.PrincipalType(db.OwnerType)
		out.OwnerType = &pt
	}
	return out
}

// copyStringMap returns a shallow copy of m, so the returned value never
// aliases the generated struct's map. It returns nil for a nil or empty
// input: a non-optional Thrift map field round-trips a nil source map as a
// non-nil, zero-length one, and this keeps that indistinguishable from nil
// on this package's side of the conversion.
func copyStringMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// copyStrings returns a copy of s, so the returned value never aliases the
// generated struct's slice. It returns nil for a nil or empty input, for
// the same reason as copyStringMap.
func copyStrings(s []string) []string {
	if len(s) == 0 {
		return nil
	}
	out := make([]string, len(s))
	copy(out, s)
	return out
}

// timeFromUnix32 converts a Thrift int32 "seconds since epoch" field to a
// time.Time, per Hive's convention that 0 means unset.
func timeFromUnix32(s int32) time.Time {
	if s == 0 {
		return time.Time{}
	}
	return time.Unix(int64(s), 0)
}

// unix32FromTime converts t to a Thrift int32 "seconds since epoch" field,
// per Hive's convention that a zero time.Time means unset. A value outside
// the int32 range is clamped rather than silently wrapping (gosec G115).
func unix32FromTime(t time.Time) int32 {
	if t.IsZero() {
		return 0
	}
	u := t.Unix()
	switch {
	case u > math.MaxInt32:
		u = math.MaxInt32
	case u < math.MinInt32:
		u = math.MinInt32
	}
	return int32(u)
}

// fieldSchemaFromThrift converts a generated FieldSchema to the exported
// FieldSchema type. It returns nil for a nil input.
func fieldSchemaFromThrift(f *hive_metastore.FieldSchema) *FieldSchema {
	if f == nil {
		return nil
	}
	return &FieldSchema{Name: f.Name, Type: f.Type, Comment: f.Comment}
}

// fieldSchemaToThrift converts the exported FieldSchema type to its
// generated wire representation. It returns nil for a nil input.
func fieldSchemaToThrift(f *FieldSchema) *hive_metastore.FieldSchema {
	if f == nil {
		return nil
	}
	return &hive_metastore.FieldSchema{Name: f.Name, Type: f.Type, Comment: f.Comment}
}

// fieldSchemasFromThrift converts a slice of generated FieldSchema values.
// It returns nil for a nil or empty input (see copyStringMap).
func fieldSchemasFromThrift(fs []*hive_metastore.FieldSchema) []*FieldSchema {
	if len(fs) == 0 {
		return nil
	}
	out := make([]*FieldSchema, len(fs))
	for i, f := range fs {
		out[i] = fieldSchemaFromThrift(f)
	}
	return out
}

// fieldSchemasToThrift converts a slice of the exported FieldSchema type. It
// returns nil for a nil or empty input (see copyStringMap).
func fieldSchemasToThrift(fs []*FieldSchema) []*hive_metastore.FieldSchema {
	if len(fs) == 0 {
		return nil
	}
	out := make([]*hive_metastore.FieldSchema, len(fs))
	for i, f := range fs {
		out[i] = fieldSchemaToThrift(f)
	}
	return out
}

// serDeFromThrift converts a generated SerDeInfo to the exported SerDeInfo
// type. It returns nil for a nil input.
func serDeFromThrift(s *hive_metastore.SerDeInfo) *SerDeInfo {
	if s == nil {
		return nil
	}
	return &SerDeInfo{
		Name:             s.Name,
		SerializationLib: s.SerializationLib,
		Parameters:       copyStringMap(s.Parameters),
	}
}

// serDeToThrift converts the exported SerDeInfo type to its generated wire
// representation. It returns nil for a nil input.
func serDeToThrift(s *SerDeInfo) *hive_metastore.SerDeInfo {
	if s == nil {
		return nil
	}
	return &hive_metastore.SerDeInfo{
		Name:             s.Name,
		SerializationLib: s.SerializationLib,
		Parameters:       copyStringMap(s.Parameters),
	}
}

// orderFromThrift converts a generated Order to the exported Order type. It
// returns nil for a nil input.
func orderFromThrift(o *hive_metastore.Order) *Order {
	if o == nil {
		return nil
	}
	return &Order{Column: o.Col, Order: o.Order}
}

// orderToThrift converts the exported Order type to its generated wire
// representation. It returns nil for a nil input.
func orderToThrift(o *Order) *hive_metastore.Order {
	if o == nil {
		return nil
	}
	return &hive_metastore.Order{Col: o.Column, Order: o.Order}
}

// ordersFromThrift converts a slice of generated Order values. It returns
// nil for a nil or empty input (see copyStringMap).
func ordersFromThrift(os []*hive_metastore.Order) []*Order {
	if len(os) == 0 {
		return nil
	}
	out := make([]*Order, len(os))
	for i, o := range os {
		out[i] = orderFromThrift(o)
	}
	return out
}

// ordersToThrift converts a slice of the exported Order type. It returns
// nil for a nil or empty input (see copyStringMap).
func ordersToThrift(os []*Order) []*hive_metastore.Order {
	if len(os) == 0 {
		return nil
	}
	out := make([]*hive_metastore.Order, len(os))
	for i, o := range os {
		out[i] = orderToThrift(o)
	}
	return out
}

// storageFromThrift converts a generated StorageDescriptor to the exported
// StorageDescriptor type. It returns nil for a nil input.
func storageFromThrift(sd *hive_metastore.StorageDescriptor) *StorageDescriptor {
	if sd == nil {
		return nil
	}
	out := &StorageDescriptor{
		Columns:       fieldSchemasFromThrift(sd.Cols),
		Location:      sd.Location,
		InputFormat:   sd.InputFormat,
		OutputFormat:  sd.OutputFormat,
		Compressed:    sd.Compressed,
		NumBuckets:    sd.NumBuckets,
		SerDe:         serDeFromThrift(sd.SerdeInfo),
		BucketColumns: copyStrings(sd.BucketCols),
		SortColumns:   ordersFromThrift(sd.SortCols),
		Parameters:    copyStringMap(sd.Parameters),
	}
	if sd.StoredAsSubDirectories != nil {
		out.StoredAsSubDirectories = *sd.StoredAsSubDirectories
	}
	return out
}

// storageToThrift converts the exported StorageDescriptor type to its
// generated wire representation. It returns nil for a nil input.
func storageToThrift(sd *StorageDescriptor) *hive_metastore.StorageDescriptor {
	if sd == nil {
		return nil
	}
	out := &hive_metastore.StorageDescriptor{
		Cols:         fieldSchemasToThrift(sd.Columns),
		Location:     sd.Location,
		InputFormat:  sd.InputFormat,
		OutputFormat: sd.OutputFormat,
		Compressed:   sd.Compressed,
		NumBuckets:   sd.NumBuckets,
		SerdeInfo:    serDeToThrift(sd.SerDe),
		BucketCols:   copyStrings(sd.BucketColumns),
		SortCols:     ordersToThrift(sd.SortColumns),
		Parameters:   copyStringMap(sd.Parameters),
	}
	if sd.StoredAsSubDirectories {
		v := true
		out.StoredAsSubDirectories = &v
	}
	return out
}

// tableFromThrift converts a generated Table to the exported Table type. A
// nil wire CatName defaults to the "hive" catalog, matching Hive's own
// convention on a server that predates catalogs (Hive 2.3). It returns nil
// for a nil input.
func tableFromThrift(t *hive_metastore.Table) *Table {
	if t == nil {
		return nil
	}
	out := &Table{
		DatabaseName:     t.DbName,
		TableName:        t.TableName,
		Owner:            t.Owner,
		CreateTime:       timeFromUnix32(t.CreateTime),
		LastAccessTime:   timeFromUnix32(t.LastAccessTime),
		Retention:        t.Retention,
		Storage:          storageFromThrift(t.Sd),
		PartitionKeys:    fieldSchemasFromThrift(t.PartitionKeys),
		Parameters:       copyStringMap(t.Parameters),
		ViewOriginalText: t.ViewOriginalText,
		ViewExpandedText: t.ViewExpandedText,
		TableType:        TableType(t.TableType),
	}
	if t.CatName != nil {
		out.CatalogName = *t.CatName
	} else {
		out.CatalogName = defaultCatalog
	}
	return out
}

// tableToThrift converts the exported Table type to its generated wire
// representation. cat is the effective catalog resolved for the call (see
// (*Client).resolveCat); it becomes the wire CatName field (possibly nil,
// when the connection is known not to support catalogs), taking precedence
// over t.CatalogName (see (*Client).CreateTable, which folds a non-empty
// t.CatalogName into the resolveCat call that produces cat). It returns nil
// for a nil input.
//
// The result is built from hive_metastore.NewTable() rather than a bare
// struct literal so the non-pointer "optional with default" fields the
// exported Table type has no equivalent for — OwnerType and WriteId — keep
// NewTable's defaults (PrincipalType_USER and -1) instead of falling back
// to the Go zero value 0: ownerType=0 is not a valid PrincipalType on the
// wire, and writeId=0 is a real write id rather than "unassigned" (see
// NewTable's own IsSet checks, which compare against these same defaults).
func tableToThrift(t *Table, cat *string) *hive_metastore.Table {
	if t == nil {
		return nil
	}
	out := hive_metastore.NewTable()
	out.DbName = t.DatabaseName
	out.TableName = t.TableName
	out.Owner = t.Owner
	out.CreateTime = unix32FromTime(t.CreateTime)
	out.LastAccessTime = unix32FromTime(t.LastAccessTime)
	out.Retention = t.Retention
	out.PartitionKeys = fieldSchemasToThrift(t.PartitionKeys)
	out.Parameters = copyStringMap(t.Parameters)
	out.ViewOriginalText = t.ViewOriginalText
	out.ViewExpandedText = t.ViewExpandedText
	out.TableType = string(t.TableType)
	out.CatName = cat
	if t.Storage != nil {
		out.Sd = storageToThrift(t.Storage)
	}
	return out
}

// partitionFromThrift converts a generated Partition to the exported
// Partition type. A nil wire CatName defaults to the "hive" catalog,
// matching Hive's own convention on a server that predates catalogs (Hive
// 2.3). It returns nil for a nil input.
func partitionFromThrift(p *hive_metastore.Partition) *Partition {
	if p == nil {
		return nil
	}
	out := &Partition{
		DatabaseName: p.DbName,
		TableName:    p.TableName,
		Values:       copyStrings(p.Values),
		CreateTime:   timeFromUnix32(p.CreateTime),
		Storage:      storageFromThrift(p.Sd),
		Parameters:   copyStringMap(p.Parameters),
	}
	if p.CatName != nil {
		out.CatalogName = *p.CatName
	} else {
		out.CatalogName = defaultCatalog
	}
	return out
}

// partitionsFromThrift converts a slice of generated Partition values. It
// returns nil for a nil or empty input (see copyStringMap).
func partitionsFromThrift(ps []*hive_metastore.Partition) []*Partition {
	if len(ps) == 0 {
		return nil
	}
	out := make([]*Partition, len(ps))
	for i, p := range ps {
		out[i] = partitionFromThrift(p)
	}
	return out
}

// partitionToThrift converts the exported Partition type to its generated
// wire representation. cat is the effective catalog resolved for the call
// (see (*Client).resolveCat); it becomes the wire CatName field (possibly
// nil, when the connection is known not to support catalogs). dbName and
// tableName are the call's own arguments (e.g. AddPartitions' dbName,
// tableName parameters); they become the wire DbName/TableName fields
// unless p itself already names a database or table, which then takes
// precedence. It returns nil for a nil input.
//
// The result is built from hive_metastore.NewPartition() rather than a bare
// struct literal so the non-pointer "optional with default" WriteId field
// (which the exported Partition type has no equivalent for) keeps
// NewPartition's default of -1 instead of falling back to the Go zero
// value 0, which is a real write id rather than "unassigned" (see
// tableToThrift's identical treatment of Table.WriteId).
func partitionToThrift(p *Partition, cat *string, dbName, tableName string) *hive_metastore.Partition {
	if p == nil {
		return nil
	}
	out := hive_metastore.NewPartition()
	out.DbName = dbName
	out.TableName = tableName
	out.Values = copyStrings(p.Values)
	out.CreateTime = unix32FromTime(p.CreateTime)
	out.Parameters = copyStringMap(p.Parameters)
	out.CatName = cat
	if p.DatabaseName != "" {
		out.DbName = p.DatabaseName
	}
	if p.TableName != "" {
		out.TableName = p.TableName
	}
	if p.Storage != nil {
		out.Sd = storageToThrift(p.Storage)
	}
	return out
}

// partitionsToThrift converts a slice of the exported Partition type. It
// returns nil for a nil or empty input (see copyStringMap).
func partitionsToThrift(ps []*Partition, cat *string, dbName, tableName string) []*hive_metastore.Partition {
	if len(ps) == 0 {
		return nil
	}
	out := make([]*hive_metastore.Partition, len(ps))
	for i, p := range ps {
		out[i] = partitionToThrift(p, cat, dbName, tableName)
	}
	return out
}
