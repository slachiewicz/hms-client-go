package hms

import "github.com/slachiewicz/hms-client-go/gen/hive_metastore"

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
		Parameters:  d.Parameters,
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
		Parameters:  db.Parameters,
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
