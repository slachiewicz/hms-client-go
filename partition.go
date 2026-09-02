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
// dbName. Requests are batched to at most the client's partition batch size
// (see WithPartitionBatchSize; default 1000) partitions each (SPEC §2.3 Rule
// 5) -- independent of WithChunkSize, which governs GetTables and
// GetPartitionsByNames only; batches are sent sequentially, so a failure on
// a later batch leaves the earlier batches already committed on the server.
// With ifNotExists true, a
// partition whose values already exist is silently skipped; otherwise it
// is reported as ErrAlreadyExists.
//
// Every partition lands in dbName.tableName: a Partition's own
// DatabaseName and TableName are ignored, so one read from another table
// (or another database) is added where this call says rather than where it
// came from. Such a partition also arrives fresh -- the round-trip
// fidelity snapshot is scoped to AlterPartitions (SPEC §5.4), so the
// source partition's server-assigned write id, privileges, and statistics
// are not offered as the definition of the new one.
func (c *Client) AddPartitions(ctx context.Context, dbName, tableName string, partitions []*Partition, ifNotExists bool, opts ...CatalogOption) error {
	return c.call(ctx, "add_partitions_req", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
		for i := 0; i < len(partitions); i += c.cfg.partitionBatchSize {
			end := i + c.cfg.partitionBatchSize
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
// in database dbName, matched by their Values. As in AddPartitions, a
// Partition's own DatabaseName and TableName are ignored: dbName and
// tableName name the table being altered. A partition GetPartitions itself
// returned keeps every field this package does not model -- its write id,
// privileges, and statistics -- instead of having them reset (SPEC §5.4
// "Round-trip fidelity").
//
// Against a server lacking alter_partitions_req (Hive 2.3 and 3.x), it
// degrades to the legacy alter_partitions RPC (SPEC §2.3 Rule 3).
func (c *Client) AlterPartitions(ctx context.Context, dbName, tableName string, partitions []*Partition, opts ...CatalogOption) error {
	return c.call(ctx, "alter_partitions_req", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
		return cn.tryReq(ctx, "alter_partitions_req",
			func(ctx context.Context) error {
				req := newAlterPartitionsRequest(dbName, tableName, cat, partitionsToThriftFrom(partitions, cat, dbName, tableName))
				_, err := cn.alterPartitionsReq(ctx, req)
				return err
			},
			func(ctx context.Context) error {
				return cn.alterPartitions(ctx, qualifyDBName(cat, dbName), tableName, partitionsToThriftFrom(partitions, cat, dbName, tableName))
			},
		)
	})
}

// newGetPartitionsByNamesRequest builds a GetPartitionsByNamesRequest from
// hive_metastore.NewGetPartitionsByNamesRequest() rather than a bare struct
// literal, so the non-pointer "optional with default" field ID (no
// equivalent on this package's exported API) keeps
// NewGetPartitionsByNamesRequest's default of -1 instead of falling back
// to the Go zero value 0 (see newPartitionsRequest's identical treatment).
//
// The generated GetPartitionsByNamesRequest carries no CatName field (a
// gap in the 4.2.1 IDL, unlike PartitionsRequest and
// GetPartitionNamesPsRequest), so a non-default catalog is instead
// expressed the way every other RPC without a dedicated catalog field
// expresses it: a "@<cat>#<db>" qualifier on DbName (qualifyDBName).
func newGetPartitionsByNamesRequest(dbName, tableName string, cat *string, names []string) *hive_metastore.GetPartitionsByNamesRequest {
	req := hive_metastore.NewGetPartitionsByNamesRequest()
	req.DbName = qualifyDBName(cat, dbName)
	req.TblName = tableName
	req.Names = names
	return req
}

// GetPartitionsByNames returns the partitions of the table named
// tableName in database dbName whose partition name (as returned by
// GetPartitionNames, e.g. "dt=2024-01-01") is in names, in the order the
// server returns them per chunk. Requests are chunked to at most the
// client's chunk size (see WithChunkSize; default 1000) names each,
// mirroring GetTables (SPEC §2.3 Rule 5) -- not AddPartitions, whose batch
// size is WithPartitionBatchSize instead; chunks are sent sequentially.
// Against a server lacking get_partitions_by_names_req (Hive 2.3 and 3.x),
// it degrades to the legacy get_partitions_by_names RPC (SPEC §2.3).
func (c *Client) GetPartitionsByNames(ctx context.Context, dbName, tableName string, names []string, opts ...CatalogOption) ([]*Partition, error) {
	var out []*Partition
	err := c.read(ctx, "get_partitions_by_names_req", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
		var parts []*Partition
		for i := 0; i < len(names); i += c.cfg.chunkSize {
			end := i + c.cfg.chunkSize
			if end > len(names) {
				end = len(names)
			}
			chunk := names[i:end]
			err := cn.tryReq(ctx, "get_partitions_by_names_req",
				func(ctx context.Context) error {
					resp, err := cn.getPartitionsByNamesReq(ctx, newGetPartitionsByNamesRequest(dbName, tableName, cat, chunk))
					if err != nil {
						return err
					}
					parts = append(parts, partitionsFromThrift(resp.Partitions)...)
					return nil
				},
				func(ctx context.Context) error {
					ps, err := cn.getPartitionsByNames(ctx, qualifyDBName(cat, dbName), tableName, chunk)
					if err != nil {
						return err
					}
					parts = append(parts, partitionsFromThrift(ps)...)
					return nil
				},
			)
			if err != nil {
				return err
			}
		}
		out = parts
		return nil
	})
	return out, err
}

// GetPartitionsByFilter returns up to maxParts partitions of the table
// named tableName in database dbName matching filter, Hive's partition
// filter expression grammar (e.g. "year = 2024 AND month > 6"); a
// negative maxParts means "all partitions". filter is passed through to
// the server verbatim -- this package neither parses nor validates it.
// GetPartitionsByFilter has no request-variant RPC (SPEC §2.3): it always
// calls the legacy get_partitions_by_filter, on every supported version.
func (c *Client) GetPartitionsByFilter(ctx context.Context, dbName, tableName, filter string, maxParts int, opts ...CatalogOption) ([]*Partition, error) {
	var out []*Partition
	err := c.read(ctx, "get_partitions_by_filter", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
		parts, err := cn.getPartitionsByFilter(ctx, qualifyDBName(cat, dbName), tableName, filter, clampParts(maxParts))
		if err != nil {
			return err
		}
		out = partitionsFromThrift(parts)
		return nil
	})
	return out, err
}

// newGetPartitionNamesPsRequest builds a GetPartitionNamesPsRequest from
// hive_metastore.NewGetPartitionNamesPsRequest() rather than a bare struct
// literal, so the non-pointer "optional with default" field ID (no
// equivalent on this package's exported API) keeps
// NewGetPartitionNamesPsRequest's default of -1 instead of falling back to
// the Go zero value 0 (see newPartitionsRequest's identical treatment).
// Unlike GetPartitionsByNamesRequest, this request struct does carry a
// CatName field, so it is set directly rather than folded into DbName.
func newGetPartitionNamesPsRequest(dbName, tableName string, cat *string, partialValues []string, maxParts int16) *hive_metastore.GetPartitionNamesPsRequest {
	req := hive_metastore.NewGetPartitionNamesPsRequest()
	req.CatName = cat
	req.DbName = dbName
	req.TblName = tableName
	req.PartValues = partialValues
	req.MaxParts = maxParts
	return req
}

// GetPartitionNamesByValues returns the names of up to maxParts partitions
// of the table named tableName in database dbName whose leading
// partition-key values equal partialValues (a prefix; trailing keys are
// wildcarded); a negative maxParts means "all partitions". Against a
// server lacking get_partition_names_ps_req (Hive 2.3 and 3.x), it
// degrades to the legacy get_partition_names_ps RPC (SPEC §2.3).
func (c *Client) GetPartitionNamesByValues(ctx context.Context, dbName, tableName string, partialValues []string, maxParts int, opts ...CatalogOption) ([]string, error) {
	var out []string
	err := c.read(ctx, "get_partition_names_ps_req", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
		mp := clampParts(maxParts)
		return cn.tryReq(ctx, "get_partition_names_ps_req",
			func(ctx context.Context) error {
				resp, err := cn.getPartitionNamesPsReq(ctx, newGetPartitionNamesPsRequest(dbName, tableName, cat, partialValues, mp))
				if err != nil {
					return err
				}
				out = resp.Names
				return nil
			},
			func(ctx context.Context) error {
				names, err := cn.getPartitionNamesPs(ctx, qualifyDBName(cat, dbName), tableName, partialValues, mp)
				if err != nil {
					return err
				}
				out = names
				return nil
			},
		)
	})
	return out, err
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
