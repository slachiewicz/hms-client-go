package hms

import (
	"context"
	"iter"

	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
)

// GetAllTables lists the names of every table in the database named dbName,
// in the effective catalog (WithCatalog, overridden per call by InCatalog;
// default "hive").
func (c *Client) GetAllTables(ctx context.Context, dbName string, opts ...CatalogOption) ([]string, error) {
	var names []string
	err := c.read(ctx, "get_all_tables", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
		list, err := cn.getAllTables(ctx, qualifyDBName(cat, dbName))
		if err != nil {
			return err
		}
		names = list
		return nil
	})
	return names, err
}

// newGetTableRequest builds a GetTableRequest from hive_metastore.
// NewGetTableRequest() rather than a bare struct literal, so the
// non-pointer "optional with default" fields Engine and ID (no equivalent
// on this package's exported API) keep NewGetTableRequest's defaults
// ("hive" and -1) instead of falling back to the Go zero value, which the
// server would read as a real (and wrong) engine name or numeric table id
// rather than "unset" (see tableToThrift's identical treatment of
// Table.OwnerType/WriteId).
func newGetTableRequest(dbName, tableName string, cat *string) *hive_metastore.GetTableRequest {
	req := hive_metastore.NewGetTableRequest()
	req.DbName = dbName
	req.TblName = tableName
	req.CatName = cat
	return req
}

// GetTable returns the table named tableName in database dbName.
func (c *Client) GetTable(ctx context.Context, dbName, tableName string, opts ...CatalogOption) (*Table, error) {
	var out *Table
	err := c.read(ctx, "get_table_req", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
		res, err := cn.getTableReq(ctx, newGetTableRequest(dbName, tableName, cat))
		if err != nil {
			return err
		}
		out = tableFromThrift(res.Table)
		return nil
	})
	return out, err
}

// GetTables returns the tables named in tableNames that exist in database
// dbName, in request order, silently skipping any name the server does not
// know. Requests are chunked to at most the client's chunk size (see
// WithChunkSize; default 1000) names each (SPEC §5.4).
func (c *Client) GetTables(ctx context.Context, dbName string, tableNames []string, opts ...CatalogOption) ([]*Table, error) {
	var out []*Table
	err := c.read(ctx, "get_table_objects_by_name_req", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
		var tables []*Table
		in := make(columnIntern)
		for i := 0; i < len(tableNames); i += c.cfg.chunkSize {
			end := i + c.cfg.chunkSize
			if end > len(tableNames) {
				end = len(tableNames)
			}
			res, err := cn.getTableObjectsByNameReq(ctx, &hive_metastore.GetTablesRequest{
				DbName:   dbName,
				TblNames: tableNames[i:end],
				CatName:  cat,
			})
			if err != nil {
				return err
			}
			for _, t := range res.Tables {
				tables = append(tables, tableFromThriftIntern(t, in))
			}
		}
		out = tables
		return nil
	})
	return out, err
}

// GetTablesSeq is GetTables' streaming, memory-bounded form (1.0 addition,
// G11), the same shape GetPartitionsSeq gives GetPartitions: it calls
// GetAllTables once (get_all_tables returns names only, cheap enough to
// hold in full) and then get_table_objects_by_name_req in chunks of the
// client's chunk size (WithChunkSize; default 1000, the same knob and
// default GetTables itself chunks by, SPEC §5.4), yielding each converted
// Table as its chunk arrives instead of appending it to a growing slice,
// so at most one chunk's worth of Tables is ever alive at once however
// many tables the database has. get_table_objects_by_name_req carries no
// legacy-RPC fallback (SPEC §2.1): it exists on every supported version,
// so unlike GetPartitionsSeq's by-names chunks this call's own chunks
// never need cn.tryReq.
//
// On failure, exactly one (nil, err) pair is yielded and the sequence
// ends; breaking out of the range loop early stops the sequence
// immediately and issues no further RPCs, and ctx's cancellation/deadline
// is checked between chunks, exactly as GetPartitionsSeq describes. Like
// GetPartitionsSeq, every RPC runs inside a single c.call invocation (not
// c.read), so a retry on another endpoint -- which would re-run this
// call's own fn from get_all_tables and re-yield every table already
// handed to the caller -- never happens once fn has started; see
// GetPartitionsSeq's doc comment for the full rationale.
func (c *Client) GetTablesSeq(ctx context.Context, dbName string, opts ...CatalogOption) iter.Seq2[*Table, error] {
	return func(yield func(*Table, error) bool) {
		err := c.call(ctx, "get_all_tables", func(ctx context.Context, cn *conn) error {
			cat, err := c.resolveCat(ctx, cn, opts)
			if err != nil {
				return err
			}
			names, err := cn.getAllTables(ctx, qualifyDBName(cat, dbName))
			if err != nil {
				return err
			}
			in := make(columnIntern)
			for i := 0; i < len(names); i += c.cfg.chunkSize {
				if err := ctx.Err(); err != nil {
					return err
				}
				end := i + c.cfg.chunkSize
				if end > len(names) {
					end = len(names)
				}
				res, err := cn.getTableObjectsByNameReq(ctx, &hive_metastore.GetTablesRequest{
					DbName:   dbName,
					TblNames: names[i:end],
					CatName:  cat,
				})
				if err != nil {
					return err
				}
				for _, t := range res.Tables {
					if !yield(tableFromThriftIntern(t, in), nil) {
						return nil
					}
				}
			}
			return nil
		})
		if err != nil {
			yield(nil, err)
		}
	}
}

// CreateTable creates table. A non-empty table.CatalogName overrides the
// client's default catalog for this call (SPEC §5.0).
func (c *Client) CreateTable(ctx context.Context, table *Table) error {
	return c.call(ctx, "create_table", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCatFor(ctx, cn, table.CatalogName, nil)
		if err != nil {
			return err
		}
		return cn.createTable(ctx, tableToThrift(table, cat))
	})
}

// AlterTable replaces the table named tableName in database dbName with
// newTable, which may rename it when newTable.TableName differs from
// tableName. A non-empty newTable.CatalogName overrides the client's
// default catalog for this call, the same way CreateTable's table.
// CatalogName does; opts' InCatalog, if passed, takes precedence over both
// (SPEC §5.0).
func (c *Client) AlterTable(ctx context.Context, dbName, tableName string, newTable *Table, opts ...CatalogOption) error {
	return c.call(ctx, "alter_table", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCatFor(ctx, cn, newTable.CatalogName, opts)
		if err != nil {
			return err
		}
		return cn.alterTable(ctx, qualifyDBName(cat, dbName), tableName, tableToThriftFrom(newTable, cat))
	})
}

// DropTable removes the table named tableName in database dbName. deleteData
// is forwarded to the server. With ifExists true, a missing table is not an
// error.
func (c *Client) DropTable(ctx context.Context, dbName, tableName string, deleteData, ifExists bool, opts ...CatalogOption) error {
	return c.call(ctx, "drop_table", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
		err = cn.dropTable(ctx, qualifyDBName(cat, dbName), tableName, deleteData)
		if err != nil && ifExists && classify(err) == ErrNotFound {
			return nil
		}
		return err
	})
}
