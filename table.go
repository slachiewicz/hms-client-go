package hms

import (
	"context"

	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
)

// defaultChunkSize is the maximum number of table names GetTables requests
// per get_table_objects_by_name_req call (SPEC §5.4). It is a package
// variable rather than a constant so tests can shrink it (see
// export_test.go's SetChunkSizeForTest) without needing thousands of
// fixture tables to exercise chunking.
var defaultChunkSize = 1000

// GetAllTables lists the names of every table in the database named dbName,
// in the effective catalog (WithCatalog, overridden per call by InCatalog;
// default "hive").
func (c *Client) GetAllTables(ctx context.Context, dbName string, opts ...CatalogOption) ([]string, error) {
	var names []string
	err := c.call(ctx, "GetAllTables", func(ctx context.Context, cn *conn) error {
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

// GetTable returns the table named tableName in database dbName.
func (c *Client) GetTable(ctx context.Context, dbName, tableName string, opts ...CatalogOption) (*Table, error) {
	var out *Table
	err := c.call(ctx, "GetTable", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
		res, err := cn.getTableReq(ctx, &hive_metastore.GetTableRequest{DbName: dbName, TblName: tableName, CatName: cat})
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
// know. Requests are chunked to at most defaultChunkSize names each (SPEC
// §5.4).
func (c *Client) GetTables(ctx context.Context, dbName string, tableNames []string, opts ...CatalogOption) ([]*Table, error) {
	var out []*Table
	err := c.call(ctx, "GetTables", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
		var tables []*Table
		for i := 0; i < len(tableNames); i += defaultChunkSize {
			end := i + defaultChunkSize
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
				tables = append(tables, tableFromThrift(t))
			}
		}
		out = tables
		return nil
	})
	return out, err
}

// CreateTable creates table. A non-empty table.CatalogName overrides the
// client's default catalog for this call.
func (c *Client) CreateTable(ctx context.Context, table *Table) error {
	return c.call(ctx, "CreateTable", func(ctx context.Context, cn *conn) error {
		var opts []CatalogOption
		if table.CatalogName != "" {
			opts = append(opts, InCatalog(table.CatalogName))
		}
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
		return cn.createTable(ctx, tableToThrift(table, cat))
	})
}

// AlterTable replaces the table named tableName in database dbName with
// newTable, which may rename it when newTable.TableName differs from
// tableName.
func (c *Client) AlterTable(ctx context.Context, dbName, tableName string, newTable *Table, opts ...CatalogOption) error {
	return c.call(ctx, "AlterTable", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
		return cn.alterTable(ctx, qualifyDBName(cat, dbName), tableName, tableToThrift(newTable, cat))
	})
}

// DropTable removes the table named tableName in database dbName. deleteData
// is forwarded to the server. With ifExists true, a missing table is not an
// error.
func (c *Client) DropTable(ctx context.Context, dbName, tableName string, deleteData, ifExists bool, opts ...CatalogOption) error {
	return c.call(ctx, "DropTable", func(ctx context.Context, cn *conn) error {
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
