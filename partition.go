package hms

import (
	"context"
	"math"

	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
)

// clampParts converts n to the Thrift i16 maxParts wire type: any negative
// value becomes -1 ("all partitions"), and a value above math.MaxInt16 is
// clamped to math.MaxInt16 rather than silently wrapping (gosec G115).
func clampParts(n int) int16 {
	switch {
	case n < 0:
		return -1
	case n > math.MaxInt16:
		return math.MaxInt16
	default:
		return int16(n)
	}
}

// newPartitionsRequest builds a PartitionsRequest from hive_metastore.
// NewPartitionsRequest() rather than a bare struct literal, so the
// non-pointer "optional with default" field ID (no equivalent on this
// package's exported API) keeps NewPartitionsRequest's default of -1
// instead of falling back to the Go zero value 0, a real numeric table id
// on the wire rather than "unset" (see tableToThrift's identical treatment
// of Table.WriteId).
func newPartitionsRequest(dbName, tableName string, cat *string, maxParts int16) *hive_metastore.PartitionsRequest {
	req := hive_metastore.NewPartitionsRequest()
	req.CatName = cat
	req.DbName = dbName
	req.TblName = tableName
	req.MaxParts = maxParts
	return req
}

// GetPartitions returns up to maxParts partitions of the table named
// tableName in database dbName; a negative maxParts means "all partitions".
// Against a server lacking get_partitions_req (Hive 2.3 and 3.x), it
// degrades to the legacy get_partitions RPC (SPEC §2.3 Rule 4).
func (c *Client) GetPartitions(ctx context.Context, dbName, tableName string, maxParts int, opts ...CatalogOption) ([]*Partition, error) {
	var out []*Partition
	err := c.read(ctx, "get_partitions_req", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
		mp := clampParts(maxParts)
		return cn.tryReq(ctx, "get_partitions_req",
			func(ctx context.Context) error {
				resp, err := cn.getPartitionsReq(ctx, newPartitionsRequest(dbName, tableName, cat, mp))
				if err != nil {
					return err
				}
				out = partitionsFromThrift(resp.Partitions)
				return nil
			},
			func(ctx context.Context) error {
				parts, err := cn.getPartitions(ctx, qualifyDBName(cat, dbName), tableName, mp)
				if err != nil {
					return err
				}
				out = partitionsFromThrift(parts)
				return nil
			},
		)
	})
	return out, err
}

// GetPartitionNames returns the names of up to maxParts partitions of the
// table named tableName in database dbName, formatted as
// "key1=value1/key2=value2" from the table's partition keys; a negative
// maxParts means "all partitions".
func (c *Client) GetPartitionNames(ctx context.Context, dbName, tableName string, maxParts int, opts ...CatalogOption) ([]string, error) {
	var out []string
	err := c.read(ctx, "get_partition_names", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
		names, err := cn.getPartitionNames(ctx, qualifyDBName(cat, dbName), tableName, clampParts(maxParts))
		if err != nil {
			return err
		}
		out = names
		return nil
	})
	return out, err
}

// AddPartitions adds partitions to the table named tableName in database
// dbName. Requests are chunked to at most the client's chunk size (see
// WithChunkSize; default 1000) partitions each (SPEC §2.3 Rule 5); chunks
// are sent sequentially, so a failure on a later chunk leaves the earlier
// chunks already committed on the server. With ifNotExists true, a
// partition whose values already exist is silently skipped; otherwise it
// is reported as ErrAlreadyExists.
func (c *Client) AddPartitions(ctx context.Context, dbName, tableName string, partitions []*Partition, ifNotExists bool, opts ...CatalogOption) error {
	return c.call(ctx, "add_partitions_req", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
		for i := 0; i < len(partitions); i += c.cfg.chunkSize {
			end := i + c.cfg.chunkSize
			if end > len(partitions) {
				end = len(partitions)
			}
			_, err := cn.addPartitionsReq(ctx, &hive_metastore.AddPartitionsRequest{
				DbName:      dbName,
				TblName:     tableName,
				Parts:       partitionsToThrift(partitions[i:end], cat, dbName, tableName),
				IfNotExists: ifNotExists,
				NeedResult_: false,
				CatName:     cat,
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// newAlterPartitionsRequest builds an AlterPartitionsRequest from
// hive_metastore.NewAlterPartitionsRequest() rather than a bare struct
// literal, so the non-pointer "optional with default" field WriteId (no
// equivalent on this package's exported API) keeps
// NewAlterPartitionsRequest's default of -1 instead of falling back to the
// Go zero value 0, a real write id on the wire rather than "unset" (see
// tableToThrift's identical treatment of Table.WriteId).
func newAlterPartitionsRequest(dbName, tableName string, cat *string, partitions []*hive_metastore.Partition) *hive_metastore.AlterPartitionsRequest {
	req := hive_metastore.NewAlterPartitionsRequest()
	req.CatName = cat
	req.DbName = dbName
	req.TableName = tableName
	req.Partitions = partitions
	return req
}

// AlterPartitions replaces existing partitions of the table named tableName
// in database dbName, matched by their Values. Against a server lacking
// alter_partitions_req (Hive 2.3 and 3.x), it degrades to the legacy
// alter_partitions RPC (SPEC §2.3 Rule 3).
func (c *Client) AlterPartitions(ctx context.Context, dbName, tableName string, partitions []*Partition, opts ...CatalogOption) error {
	return c.call(ctx, "alter_partitions_req", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
		return cn.tryReq(ctx, "alter_partitions_req",
			func(ctx context.Context) error {
				req := newAlterPartitionsRequest(dbName, tableName, cat, partitionsToThrift(partitions, cat, dbName, tableName))
				_, err := cn.alterPartitionsReq(ctx, req)
				return err
			},
			func(ctx context.Context) error {
				return cn.alterPartitions(ctx, qualifyDBName(cat, dbName), tableName, partitionsToThrift(partitions, cat, dbName, tableName))
			},
		)
	})
}

// DropPartition removes the partition identified by partVals (in
// partition-key order) from the table named tableName in database dbName.
// deleteData is forwarded to the server. With ifExists true, a missing
// partition is not an error.
func (c *Client) DropPartition(ctx context.Context, dbName, tableName string, partVals []string, deleteData, ifExists bool, opts ...CatalogOption) error {
	return c.call(ctx, "drop_partition", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
		_, err = cn.dropPartition(ctx, qualifyDBName(cat, dbName), tableName, partVals, deleteData)
		if err != nil && ifExists && classify(err) == ErrNotFound {
			return nil
		}
		return err
	})
}
