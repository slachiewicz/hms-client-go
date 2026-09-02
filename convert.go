package hms

import (
	"context"
	"math"
	"time"

	"github.com/apache/thrift/lib/go/thrift"

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

// deepCopySer and deepCopyDeser are pooled thrift (de)serializers shared by
// every deepCopyThrift call: thrift.TSerializer/TDeserializer allocate a
// 1KB TMemoryBuffer apiece (see NewTSerializer/NewTDeserializer), and every
// GetTable/GetPartitions/GetDatabase response pays for one of each just to
// build its round-trip fidelity snapshot, so pooling them (via thrift's own
// TSerializerPool/TDeserializerPool, which already handle the concurrency)
// avoids that allocation on every call instead of only avoiding it within
// one.
var (
	deepCopySer   = thrift.NewTSerializerPool(thrift.NewTSerializer)
	deepCopyDeser = thrift.NewTDeserializerPool(thrift.NewTDeserializer)
)

// deepCopyThrift populates dst with a field-complete copy of src by
// serialising src with a binary-protocol Thrift writer and deserialising
// the result into dst (via the pooled deepCopySer/deepCopyDeser above).
// This is used, rather than a field-by-field Go copy, to back the
// round-trip fidelity snapshot (Table.raw / Partition.raw / Database.raw,
// see tableFromThrift and their *ToThrift counterparts): a
// serialize/deserialize round trip is guaranteed to stay complete as the
// generated Thrift bindings gain fields across an IDL bump (a Go copy would
// need a matching update every time), and the cost is negligible next to
// the network round trip that always accompanies it. dst must already be
// built from the same generated NewXxx() constructor as src (see
// requests_internal_test.go's roundTrip, which this mirrors), so a field
// the source never set decodes back to that constructor's default rather
// than the Go zero value.
func deepCopyThrift(src, dst thrift.TStruct) error {
	b, err := deepCopySer.Write(context.Background(), src)
	if err != nil {
		return err
	}
	return deepCopyDeser.Read(context.Background(), dst, b)
}

// rawTable returns a deep copy of t (see deepCopyThrift) for storage as
// Table.raw, or nil if the copy fails. A failure here is not expected in
// practice -- t was just decoded off the wire (or built by this same
// package) by the same machinery the copy itself uses -- but when it does
// happen, returning nil rather than a partially-populated Table is what
// lets tableToThrift's rawTableOrNew degrade cleanly to the pre-snapshot
// NewTable()-based path below: exactly the same path a Table this package
// never read off the wire already takes, rather than caching a "snapshot"
// that never actually captured anything.
func rawTable(t *hive_metastore.Table) *hive_metastore.Table {
	out := hive_metastore.NewTable()
	if err := deepCopyThrift(t, out); err != nil {
		return nil
	}
	return out
}

// rawTableOrNew returns a fresh deep copy of raw (see deepCopyThrift) built
// from hive_metastore.NewTable(), or a bare NewTable() when raw is nil or
// the copy fails. raw is nil either because the Table it came from was
// never read off the wire, or because rawTable's own copy failed when it
// was (see rawTable); a failure of this copy is not additionally expected,
// since raw is already a well-formed in-memory struct, but tableToThrift
// must never propagate that failure as a corrupt partial copy either.
func rawTableOrNew(raw *hive_metastore.Table) *hive_metastore.Table {
	out := hive_metastore.NewTable()
	if raw == nil {
		return out
	}
	if err := deepCopyThrift(raw, out); err != nil {
		return hive_metastore.NewTable()
	}
	return out
}

// rawPartition is rawTable's counterpart for Partition.raw.
func rawPartition(p *hive_metastore.Partition) *hive_metastore.Partition {
	out := hive_metastore.NewPartition()
	if err := deepCopyThrift(p, out); err != nil {
		return nil
	}
	return out
}

// rawPartitionOrNew is rawTableOrNew's counterpart for Partition.raw.
func rawPartitionOrNew(raw *hive_metastore.Partition) *hive_metastore.Partition {
	out := hive_metastore.NewPartition()
	if raw == nil {
		return out
	}
	if err := deepCopyThrift(raw, out); err != nil {
		return hive_metastore.NewPartition()
	}
	return out
}

// rawDatabase is rawTable's counterpart for Database.raw.
func rawDatabase(d *hive_metastore.Database) *hive_metastore.Database {
	out := hive_metastore.NewDatabase()
	if err := deepCopyThrift(d, out); err != nil {
		return nil
	}
	return out
}

// rawDatabaseOrNew is rawTableOrNew's counterpart for Database.raw.
func rawDatabaseOrNew(raw *hive_metastore.Database) *hive_metastore.Database {
	out := hive_metastore.NewDatabase()
	if raw == nil {
		return out
	}
	if err := deepCopyThrift(raw, out); err != nil {
		return hive_metastore.NewDatabase()
	}
	return out
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
//
// The returned Database's raw field holds a deep copy of d (see
// deepCopyThrift), independent of d itself; see tableFromThrift's identical
// treatment and Table's doc comment for the round-trip fidelity contract.
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
		CreateTime:  timeFromUnix32Ptr(d.CreateTime),
		raw:         rawDatabase(d),
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
// wire representation for CreateDatabase, always starting from a bare
// hive_metastore.NewDatabase(). cat is the effective catalog resolved for
// the call (see (*Client).resolveCat); it becomes the wire CatalogName
// field (possibly nil, when the connection is known not to support
// catalogs). It returns nil for a nil input.
//
// A create never carries the round-trip fidelity snapshot forward, even
// when db came from databaseFromThrift: SPEC §5.4 scopes that snapshot to
// the Alter* calls, whose job is to preserve the server's own view of an
// existing object. Sending one object's server-assigned fields
// (CreateTime, Privileges, and anything a future IDL bump adds) as the
// definition of a new object would be a different thing entirely. Use
// databaseToThriftFrom for AlterDatabase.
func databaseToThrift(db *Database, cat *string) *hive_metastore.Database {
	if db == nil {
		return nil
	}
	return fillDatabase(hive_metastore.NewDatabase(), db, cat)
}

// databaseToThriftFrom is databaseToThrift for AlterDatabase: the result
// starts from a deep copy of db.raw (see deepCopyThrift) when db was itself
// produced by databaseFromThrift, or from a bare
// hive_metastore.NewDatabase() otherwise (rawDatabaseOrNew handles both),
// so a field raw carries but this package's Database does not model --
// Privileges, Type, ConnectorName, RemoteDbname, ManagedLocationUri, and
// any field a future IDL bump adds -- survives GetDatabase -> AlterDatabase
// unchanged (SPEC §5.4 "Round-trip fidelity"). Every field the exported
// Database type does model is then unconditionally overwritten, exactly as
// for a create.
//
// db.CreateTime is not one of those overwritten fields: it is read-only
// (see Database's doc comment), assigned by the server itself, so this
// never writes a CreateTime of its own. A db with no raw snapshot leaves
// the wire CreateTime field absent; a db that does carry one (i.e. one
// GetDatabase itself returned) echoes that original, server-assigned value
// forward instead, which is harmless since the field is immutable.
func databaseToThriftFrom(db *Database, cat *string) *hive_metastore.Database {
	if db == nil {
		return nil
	}
	return fillDatabase(rawDatabaseOrNew(db.raw), db, cat)
}

// fillDatabase overwrites every field the exported Database type models on
// out, which is either a bare NewDatabase() (create) or the round-trip
// fidelity snapshot (alter); see databaseToThrift and
// databaseToThriftFrom.
func fillDatabase(out *hive_metastore.Database, db *Database, cat *string) *hive_metastore.Database {
	out.Name = db.Name
	out.Description = db.Description
	out.LocationUri = db.LocationURI
	out.Parameters = copyStringMap(db.Parameters)
	out.CatalogName = cat
	if db.OwnerName != "" {
		out.OwnerName = ptr(db.OwnerName)
	} else {
		out.OwnerName = nil
	}
	if db.OwnerType != 0 {
		pt := hive_metastore.PrincipalType(db.OwnerType)
		out.OwnerType = &pt
	} else {
		out.OwnerType = nil
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

// copyStringSlices returns a deep copy of s, so neither the outer slice nor
// any of its elements aliases the source. It returns nil for a nil or empty
// input, for the same reason as copyStringMap.
func copyStringSlices(s [][]string) [][]string {
	if len(s) == 0 {
		return nil
	}
	out := make([][]string, len(s))
	for i, v := range s {
		out[i] = copyStrings(v)
	}
	return out
}

// skewedInfoFromThrift converts a generated SkewedInfo to the exported
// SkewedInfo type. ColumnValueLocationMaps has no source field to read from
// (SPEC §1.1: dropped from the generated IDL pending THRIFT-2063), so it is
// simply absent from the result -- and, unlike every other field this
// package does not model, genuinely lost rather than merely unexposed: the
// generated SkewedInfo struct has no field for it either, so there is
// nothing for Table.raw/Partition.raw to have captured (see Appendix A: the
// generated Read skips those wire bytes without storing them anywhere). It
// returns nil for a nil input.
func skewedInfoFromThrift(si *hive_metastore.SkewedInfo) *SkewedInfo {
	if si == nil {
		return nil
	}
	return &SkewedInfo{
		ColumnNames:  copyStrings(si.SkewedColNames),
		ColumnValues: copyStringSlices(si.SkewedColValues),
	}
}

// skewedInfoToThrift converts the exported SkewedInfo type to its generated
// wire representation. It returns nil for a nil input.
func skewedInfoToThrift(si *SkewedInfo) *hive_metastore.SkewedInfo {
	if si == nil {
		return nil
	}
	return &hive_metastore.SkewedInfo{
		SkewedColNames:  copyStrings(si.ColumnNames),
		SkewedColValues: copyStringSlices(si.ColumnValues),
	}
}

// timeFromUnix32 converts a Thrift int32 "seconds since epoch" field to a
// time.Time, per Hive's convention that 0 means unset.
func timeFromUnix32(s int32) time.Time {
	if s == 0 {
		return time.Time{}
	}
	return time.Unix(int64(s), 0)
}

// timeFromUnix32Ptr is timeFromUnix32's counterpart for a Thrift field
// declared optional on the wire (e.g. Database.CreateTime): a nil pointer
// (field never sent) and a present zero value are both "unset".
func timeFromUnix32Ptr(p *int32) time.Time {
	if p == nil {
		return time.Time{}
	}
	return timeFromUnix32(*p)
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
// representation. base, when non-nil, seeds the result -- typically the
// SerdeInfo already sitting inside the Table/Partition's own raw snapshot
// (see storageToThrift, tableToThrift) -- so a field this package does not
// model (Description, SerializerClass, DeserializerClass, SerdeType)
// survives; Name, SerializationLib, and Parameters are then unconditionally
// overwritten below regardless of what base held for them. A nil base (no
// raw snapshot available) behaves exactly as before this parameter
// existed. It returns nil for a nil s.
func serDeToThrift(s *SerDeInfo, base *hive_metastore.SerDeInfo) *hive_metastore.SerDeInfo {
	if s == nil {
		return nil
	}
	out := hive_metastore.NewSerDeInfo()
	if base != nil {
		cp := *base
		out = &cp
	}
	out.Name = s.Name
	out.SerializationLib = s.SerializationLib
	out.Parameters = copyStringMap(s.Parameters)
	return out
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
	out.Skewed = skewedInfoFromThrift(sd.SkewedInfo)
	return out
}

// storageToThrift converts the exported StorageDescriptor type to its
// generated wire representation. base, when non-nil, seeds the result --
// typically the Sd already sitting inside the Table/Partition's own raw
// snapshot (see tableToThrift, partitionToThrift) -- and base.SerdeInfo is
// threaded into serDeToThrift the same way, so a field neither
// StorageDescriptor nor SerDeInfo models survives. Every field this
// package's StorageDescriptor does model is then unconditionally
// overwritten below regardless of what base held for it -- today that is
// every generated StorageDescriptor field (Cols through SkewedInfo), so a
// non-nil base only actually matters one level down, inside SerdeInfo. A
// nil base (no raw snapshot available) behaves exactly as before this
// parameter existed. It returns nil for a nil sd.
func storageToThrift(sd *StorageDescriptor, base *hive_metastore.StorageDescriptor) *hive_metastore.StorageDescriptor {
	if sd == nil {
		return nil
	}
	out := hive_metastore.NewStorageDescriptor()
	var baseSerde *hive_metastore.SerDeInfo
	if base != nil {
		cp := *base
		out = &cp
		baseSerde = base.SerdeInfo
	}
	out.Cols = fieldSchemasToThrift(sd.Columns)
	out.Location = sd.Location
	out.InputFormat = sd.InputFormat
	out.OutputFormat = sd.OutputFormat
	out.Compressed = sd.Compressed
	out.NumBuckets = sd.NumBuckets
	out.SerdeInfo = serDeToThrift(sd.SerDe, baseSerde)
	out.BucketCols = copyStrings(sd.BucketColumns)
	out.SortCols = ordersToThrift(sd.SortColumns)
	out.Parameters = copyStringMap(sd.Parameters)
	if sd.StoredAsSubDirectories {
		v := true
		out.StoredAsSubDirectories = &v
	} else {
		out.StoredAsSubDirectories = nil
	}
	out.SkewedInfo = skewedInfoToThrift(sd.Skewed)
	return out
}

// tableFromThrift converts a generated Table to the exported Table type. A
// nil wire CatName defaults to the "hive" catalog, matching Hive's own
// convention on a server that predates catalogs (Hive 2.3). It returns nil
// for a nil input.
//
// The returned Table's raw field holds a deep copy of t (see
// deepCopyThrift), independent of t itself, so a caller mutating t after
// this call cannot corrupt the snapshot tableToThrift will later build on;
// see Table's doc comment for the round-trip fidelity contract this exists
// for.
func tableFromThrift(t *hive_metastore.Table) *Table {
	if t == nil {
		return nil
	}
	out := &Table{
		DatabaseName:     t.DbName,
		TableName:        t.TableName,
		Owner:            t.Owner,
		OwnerType:        PrincipalType(t.OwnerType),
		CreateTime:       timeFromUnix32(t.CreateTime),
		LastAccessTime:   timeFromUnix32(t.LastAccessTime),
		Retention:        t.Retention,
		Storage:          storageFromThrift(t.Sd),
		PartitionKeys:    fieldSchemasFromThrift(t.PartitionKeys),
		Parameters:       copyStringMap(t.Parameters),
		ViewOriginalText: t.ViewOriginalText,
		ViewExpandedText: t.ViewExpandedText,
		TableType:        TableType(t.TableType),
		raw:              rawTable(t),
	}
	if t.CatName != nil {
		out.CatalogName = *t.CatName
	} else {
		out.CatalogName = defaultCatalog
	}
	return out
}

// tableToThrift converts the exported Table type to its generated wire
// representation for CreateTable, always starting from a bare
// hive_metastore.NewTable(). cat is the effective catalog resolved for the
// call (see (*Client).resolveCat); it becomes the wire CatName field
// (possibly nil, when the connection is known not to support catalogs),
// taking precedence over t.CatalogName (see (*Client).CreateTable, which
// folds a non-empty t.CatalogName into the resolveCat call that produces
// cat). It returns nil for a nil input.
//
// A create never carries the round-trip fidelity snapshot forward, even
// when t came from tableFromThrift (a GetTable result reused as the
// template for a new table, say): SPEC §5.4 scopes that snapshot to the
// Alter* calls. Sending the source table's server-assigned identity --
// Id, TxnId, WriteId, Privileges -- as the definition of a different table
// would be a different thing entirely. Use tableToThriftFrom for
// AlterTable.
func tableToThrift(t *Table, cat *string) *hive_metastore.Table {
	if t == nil {
		return nil
	}
	return fillTable(hive_metastore.NewTable(), t, cat)
}

// tableToThriftFrom is tableToThrift for AlterTable: the result starts from
// a deep copy of t.raw (see deepCopyThrift) when t was itself produced by
// tableFromThrift, or from a bare hive_metastore.NewTable() otherwise
// (rawTableOrNew handles both), so a field raw carries but this package's
// Table does not model -- Privileges, RewriteEnabled, Id, TxnId,
// AccessType, the capability lists, and any field a future IDL bump adds --
// survives GetTable -> AlterTable unchanged (SPEC §5.4 "Round-trip
// fidelity"). Every field the exported Table type does model is then
// unconditionally overwritten, exactly as for a create.
func tableToThriftFrom(t *Table, cat *string) *hive_metastore.Table {
	if t == nil {
		return nil
	}
	return fillTable(rawTableOrNew(t.raw), t, cat)
}

// fillTable overwrites every field the exported Table type models on out,
// which is either a bare NewTable() (create) or the round-trip fidelity
// snapshot (alter); see tableToThrift and tableToThriftFrom. That includes
// the non-pointer "optional with default" fields the exported Table type
// has no equivalent for -- OwnerType and WriteId -- which keep NewTable's
// defaults (PrincipalType_USER and -1) instead of falling back to the Go
// zero value 0 when t.OwnerType is itself zero: writeId=0 is a real write
// id rather than "unassigned" (see NewTable's own IsSet checks, which
// compare against these same defaults).
func fillTable(out *hive_metastore.Table, t *Table, cat *string) *hive_metastore.Table {
	out.DbName = t.DatabaseName
	out.TableName = t.TableName
	out.Owner = t.Owner
	if t.OwnerType != 0 {
		out.OwnerType = hive_metastore.PrincipalType(t.OwnerType)
	} else {
		out.OwnerType = hive_metastore.PrincipalType_USER
	}
	out.CreateTime = unix32FromTime(t.CreateTime)
	out.LastAccessTime = unix32FromTime(t.LastAccessTime)
	out.Retention = t.Retention
	out.PartitionKeys = fieldSchemasToThrift(t.PartitionKeys)
	out.Parameters = copyStringMap(t.Parameters)
	out.ViewOriginalText = t.ViewOriginalText
	out.ViewExpandedText = t.ViewExpandedText
	out.TableType = string(t.TableType)
	out.CatName = cat
	out.Sd = storageToThrift(t.Storage, out.Sd)
	return out
}

// partitionFromThrift converts a generated Partition to the exported
// Partition type. A nil wire CatName defaults to the "hive" catalog,
// matching Hive's own convention on a server that predates catalogs (Hive
// 2.3). It returns nil for a nil input.
//
// The returned Partition's raw field holds a deep copy of p (see
// deepCopyThrift), independent of p itself; see tableFromThrift's identical
// treatment and Table's doc comment for the round-trip fidelity contract.
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
		raw:          rawPartition(p),
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
// wire representation for AddPartitions, always starting from a bare
// hive_metastore.NewPartition(). cat is the effective catalog resolved for
// the call (see (*Client).resolveCat); it becomes the wire CatName field
// (possibly nil, when the connection is known not to support catalogs).
// dbName and tableName are the call's own arguments (AddPartitions' dbName,
// tableName parameters) and always become the wire DbName/TableName
// fields: p.DatabaseName/p.TableName never override them, so a Partition
// read from one table and added to another lands in the table the call
// names rather than silently in the one it was read from. It returns nil
// for a nil input.
//
// A create never carries the round-trip fidelity snapshot forward, even
// when p came from partitionFromThrift: SPEC §5.4 scopes that snapshot to
// the Alter* calls, so the source partition's WriteId, Privileges, and
// stats are not offered as the definition of a new partition. WriteId
// therefore keeps NewPartition's default of -1 ("unassigned") rather than
// the source's own value. Use partitionToThriftFrom for AlterPartitions.
func partitionToThrift(p *Partition, cat *string, dbName, tableName string) *hive_metastore.Partition {
	if p == nil {
		return nil
	}
	return fillPartition(hive_metastore.NewPartition(), p, cat, dbName, tableName)
}

// partitionToThriftFrom is partitionToThrift for AlterPartitions: the
// result starts from a deep copy of p.raw (see deepCopyThrift) when p was
// itself produced by partitionFromThrift, or from a bare
// hive_metastore.NewPartition() otherwise (rawPartitionOrNew handles both),
// so a field raw carries but this package's Partition does not model --
// Privileges, WriteId, the stats fields, and any field a future IDL bump
// adds -- survives GetPartitions -> AlterPartitions unchanged (SPEC §5.4
// "Round-trip fidelity"). Every field the exported Partition type does
// model is then unconditionally overwritten, exactly as for an add,
// including DbName/TableName from the call's own arguments.
func partitionToThriftFrom(p *Partition, cat *string, dbName, tableName string) *hive_metastore.Partition {
	if p == nil {
		return nil
	}
	return fillPartition(rawPartitionOrNew(p.raw), p, cat, dbName, tableName)
}

// fillPartition overwrites every field the exported Partition type models
// on out, which is either a bare NewPartition() (add) or the round-trip
// fidelity snapshot (alter); see partitionToThrift and
// partitionToThriftFrom. The non-pointer "optional with default" WriteId
// field (which the exported Partition type has no equivalent for) is left
// as out carries it: NewPartition's default of -1 for an add, the server's
// own value for an alter -- 0 would be a real write id rather than
// "unassigned" (see fillTable's identical treatment of Table.WriteId).
func fillPartition(out *hive_metastore.Partition, p *Partition, cat *string, dbName, tableName string) *hive_metastore.Partition {
	out.DbName = dbName
	out.TableName = tableName
	out.Values = copyStrings(p.Values)
	out.CreateTime = unix32FromTime(p.CreateTime)
	out.Parameters = copyStringMap(p.Parameters)
	out.CatName = cat
	out.Sd = storageToThrift(p.Storage, out.Sd)
	return out
}

// partitionsToThrift converts a slice of the exported Partition type for
// AddPartitions (see partitionToThrift). It returns nil for a nil or empty
// input (see copyStringMap).
func partitionsToThrift(ps []*Partition, cat *string, dbName, tableName string) []*hive_metastore.Partition {
	return partitionsToThriftWith(partitionToThrift, ps, cat, dbName, tableName)
}

// partitionsToThriftFrom converts a slice of the exported Partition type
// for AlterPartitions (see partitionToThriftFrom). It returns nil for a nil
// or empty input (see copyStringMap).
func partitionsToThriftFrom(ps []*Partition, cat *string, dbName, tableName string) []*hive_metastore.Partition {
	return partitionsToThriftWith(partitionToThriftFrom, ps, cat, dbName, tableName)
}

// partitionsToThriftWith maps conv over ps, the one line partitionsToThrift
// and partitionsToThriftFrom would otherwise each spell out.
func partitionsToThriftWith(
	conv func(*Partition, *string, string, string) *hive_metastore.Partition,
	ps []*Partition, cat *string, dbName, tableName string,
) []*hive_metastore.Partition {
	if len(ps) == 0 {
		return nil
	}
	out := make([]*hive_metastore.Partition, len(ps))
	for i, p := range ps {
		out[i] = conv(p, cat, dbName, tableName)
	}
	return out
}

// notificationFromThrift converts a generated NotificationEvent to the
// exported NotificationEvent type (notification.go). EventTime is a
// required Thrift int32 "seconds since epoch" field -- unlike
// Database/Table/Partition's optional CreateTime fields (timeFromUnix32),
// there is no "0 means unset" convention to apply here, so it always
// converts to a real instant; the result is normalised to UTC, since Hive's
// server-side clock carries no zone information for this package to
// preserve otherwise. A nil wire CatName defaults to the "hive" catalog,
// matching Hive's own convention on a server that predates catalogs (Hive
// 2.3, where NotificationEvent.catName does not exist on the wire at all;
// it exists from 3.x onward. See notification.go's doc comment on
// newNotificationEventRequest for the same gap on the request side).
func notificationFromThrift(e *hive_metastore.NotificationEvent) NotificationEvent {
	out := NotificationEvent{
		ID:            e.EventId,
		Time:          time.Unix(int64(e.EventTime), 0).UTC(),
		Type:          e.EventType,
		DatabaseName:  deref(e.DbName),
		TableName:     deref(e.TableName),
		Message:       e.Message,
		MessageFormat: deref(e.MessageFormat),
	}
	if e.CatName != nil {
		out.CatalogName = *e.CatName
	} else {
		out.CatalogName = defaultCatalog
	}
	return out
}

// decimalFromThrift converts a generated Decimal to the exported Decimal
// type, copying Unscaled (see copyStrings) so the result never aliases the
// wire struct's slice. It returns nil for a nil input.
func decimalFromThrift(d *hive_metastore.Decimal) *Decimal {
	if d == nil {
		return nil
	}
	out := make([]byte, len(d.Unscaled))
	copy(out, d.Unscaled)
	return &Decimal{Unscaled: out, Scale: d.Scale}
}

// epoch is the Unix epoch at UTC midnight, the base dateFromThrift adds
// Date.DaysSinceEpoch to.
var epoch = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

// dateFromThrift converts a generated Date (whole days since the Unix
// epoch) to a UTC time.Time at that day's midnight. It returns nil for a
// nil input.
func dateFromThrift(d *hive_metastore.Date) *time.Time {
	if d == nil {
		return nil
	}
	t := epoch.AddDate(0, 0, int(d.DaysSinceEpoch))
	return &t
}

// timestampFromThrift converts a generated Timestamp (seconds since the
// Unix epoch) to a UTC time.Time. It returns nil for a nil input.
func timestampFromThrift(ts *hive_metastore.Timestamp) *time.Time {
	if ts == nil {
		return nil
	}
	t := time.Unix(ts.SecondsSinceEpoch, 0).UTC()
	return &t
}

// columnStatisticsFromThrift converts one generated ColumnStatisticsObj to
// the exported ColumnStatistics type: ColumnStatisticsData's active union
// arm (SPEC §5.8) selects which of Boolean/Long/.../Timestamp is
// populated; every LowValue/HighValue pointer is nil-safe, independently
// of its sibling (see decimalFromThrift, dateFromThrift,
// timestampFromThrift). histogram and bitVectors (raw binary sketches) are
// not exposed. A nil o.StatsData (not expected on the wire, since the
// field is Thrift-required) leaves every arm nil.
func columnStatisticsFromThrift(o *hive_metastore.ColumnStatisticsObj) ColumnStatistics {
	out := ColumnStatistics{ColumnName: o.ColName, ColumnType: o.ColType}
	d := o.StatsData
	if d == nil {
		return out
	}
	switch {
	case d.BooleanStats != nil:
		s := d.BooleanStats
		out.Boolean = &BooleanColumnStats{NumTrues: s.NumTrues, NumFalses: s.NumFalses, NumNulls: s.NumNulls}
	case d.LongStats != nil:
		s := d.LongStats
		out.Long = &LongColumnStats{LowValue: s.LowValue, HighValue: s.HighValue, NumNulls: s.NumNulls, NumDistinct: s.NumDVs}
	case d.DoubleStats != nil:
		s := d.DoubleStats
		out.Double = &DoubleColumnStats{LowValue: s.LowValue, HighValue: s.HighValue, NumNulls: s.NumNulls, NumDistinct: s.NumDVs}
	case d.StringStats != nil:
		s := d.StringStats
		out.String = &StringColumnStats{MaxColLen: s.MaxColLen, AvgColLen: s.AvgColLen, NumNulls: s.NumNulls, NumDistinct: s.NumDVs}
	case d.BinaryStats != nil:
		s := d.BinaryStats
		out.Binary = &BinaryColumnStats{MaxColLen: s.MaxColLen, AvgColLen: s.AvgColLen, NumNulls: s.NumNulls}
	case d.DecimalStats != nil:
		s := d.DecimalStats
		out.Decimal = &DecimalColumnStats{LowValue: decimalFromThrift(s.LowValue), HighValue: decimalFromThrift(s.HighValue), NumNulls: s.NumNulls, NumDistinct: s.NumDVs}
	case d.DateStats != nil:
		s := d.DateStats
		out.Date = &DateColumnStats{LowValue: dateFromThrift(s.LowValue), HighValue: dateFromThrift(s.HighValue), NumNulls: s.NumNulls, NumDistinct: s.NumDVs}
	case d.TimestampStats != nil:
		s := d.TimestampStats
		out.Timestamp = &TimestampColumnStats{LowValue: timestampFromThrift(s.LowValue), HighValue: timestampFromThrift(s.HighValue), NumNulls: s.NumNulls, NumDistinct: s.NumDVs}
	}
	return out
}

// columnStatisticsListFromThrift converts a slice of generated
// ColumnStatisticsObj values, preserving the server's own order. It
// returns nil for a nil or empty input (see copyStringMap).
func columnStatisticsListFromThrift(objs []*hive_metastore.ColumnStatisticsObj) []ColumnStatistics {
	if len(objs) == 0 {
		return nil
	}
	out := make([]ColumnStatistics, len(objs))
	for i, o := range objs {
		out[i] = columnStatisticsFromThrift(o)
	}
	return out
}
