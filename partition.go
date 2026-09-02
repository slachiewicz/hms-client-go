package hms

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"math"

	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
)

// checkMaxParts converts maxParts to the Thrift i16 wire type. Any negative
// value becomes -1 ("all partitions"). A value exceeding math.MaxInt16
// returns ErrInvalidOperation rather than clamping or silently wrapping
// (gosec G115), preventing silent truncation of partition listings.
func checkMaxParts(op string, maxParts int) (int16, error) {
	if maxParts > math.MaxInt16 {
		msg := fmt.Sprintf("hms: maxParts %d exceeds Hive metastore limit of %d (math.MaxInt16); use -1 for all partitions", maxParts, math.MaxInt16)
		if op == "get_partitions_req" {
			msg += " or GetPartitionsSeq to stream"
		}
		return 0, wrapAs(op, ErrInvalidOperation, errors.New(msg))
	}
	if maxParts < 0 {
		return -1, nil
	}
	return int16(maxParts), nil
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
// maxParts must not exceed math.MaxInt16 (32767); values above that return
// ErrInvalidOperation to prevent silent listing truncation on the Thrift
// i16 wire field. To list more than 32767 partitions, pass -1 or stream via
// GetPartitionsSeq.
// Against a server lacking get_partitions_req (Hive 2.3 and 3.x), it
// degrades to the legacy get_partitions RPC (SPEC §2.3 Rule 4).
func (c *Client) GetPartitions(ctx context.Context, dbName, tableName string, maxParts int, opts ...CatalogOption) ([]*Partition, error) {
	mp, err := checkMaxParts("get_partitions_req", maxParts)
	if err != nil {
		return nil, err
	}
	var out []*Partition
	err = c.read(ctx, "get_partitions_req", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
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
// maxParts means "all partitions". maxParts must not exceed math.MaxInt16
// (32767); values above that return ErrInvalidOperation to prevent silent
// listing truncation on the Thrift i16 wire field. Pass -1 for all partitions.
func (c *Client) GetPartitionNames(ctx context.Context, dbName, tableName string, maxParts int, opts ...CatalogOption) ([]string, error) {
	mp, err := checkMaxParts("get_partition_names", maxParts)
	if err != nil {
		return nil, err
	}
	var out []string
	err = c.read(ctx, "get_partition_names", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
		names, err := cn.getPartitionNames(ctx, qualifyDBName(cat, dbName), tableName, mp)
		if err != nil {
			return err
		}
		out = names
		return nil
	})
	return out, err
}

// GetPartitionsSeq is GetPartitions' streaming, memory-bounded form (1.0
// addition, G11): GetPartitions(maxParts=-1) always returns every
// partition in one Thrift response, so an iter.Seq2 wrapped around it
// would still hold the whole table's partitions (and every Storage.Columns
// slice) in memory at once before yielding the first one -- it would not
// actually bound memory. GetPartitionsSeq instead calls GetPartitionNames
// once (get_partition_names, unbounded: every name, never chunked -- a
// name is a string, not a full Partition, so the whole list is cheap
// enough to hold at once) and then GetPartitionsByNames in chunks of the
// client's chunk size (WithChunkSize; default 1000, the same knob and
// default GetPartitionsByNames itself chunks by, SPEC §5.4/§5.5), yielding
// each converted Partition as its chunk arrives instead of appending it to
// a growing slice: at most one chunk's worth of Partitions (and their
// Storage.Columns) is ever alive at once, however many partitions the
// table has. Against a server lacking get_partitions_by_names_req (Hive
// 2.3 and 3.x), each chunk independently degrades to the legacy
// get_partitions_by_names RPC (SPEC §2.3), with the fallback decision
// cached per conn exactly as GetPartitionsByNames' own does.
//
// Every chunk's Storage.Columns lists are interned against one another
// (columnIntern): partitions across the whole sequence -- not just one
// chunk -- whose columns are equal (Name/Type/Comment) share a single
// []*FieldSchema slice rather than each getting its own copy, extending
// GetPartitionsByNames' own within-call interning across every chunk this
// call issues. The interner lives in this function's own closure, not
// inside any one RPC's own fn, so it survives across the separate c.read
// calls below. See Partition.Storage's doc comment (types.go): the shared
// slice must be copied before mutation.
//
// The names lookup and each chunk fetch are separate c.read calls -- one
// pooled connection acquired and released per RPC, not one connection held
// for the whole sequence. A chunk's connection is released as soon as
// that chunk's RPC returns, before any of its partitions are yielded to
// the caller, so a slow consumer (one that blocks between items, or that
// itself calls this Client from inside the range loop body) never starves
// the connection pool, and WithRPCObserver sees one RPCInfo per RPC --
// get_partition_names once, then one per chunk with its own real wire
// name -- each Duration measuring only that RPC, never any part of the
// consumer's own loop body (SPEC §5.10). Consequently each chunk's fetch
// retries across endpoints on ErrUnavailable exactly as GetPartitionsByNames
// itself does (SPEC §4.2 point 3): a retry re-runs only that one chunk's
// own fn, which has not yielded anything from that chunk yet when it
// fails, so nothing already handed to the caller is ever re-yielded.
//
// On failure, exactly one (nil, err) pair is yielded and the sequence
// ends; breaking out of the range loop early (or returning from within
// it) stops the sequence immediately and issues no further RPCs -- the
// chunk loop checks the yield function's own return value between every
// partition, and ctx's cancellation/deadline before every chunk fetch, so
// an early stop can never leave a chunk's RPC in flight or a later one
// started needlessly.
func (c *Client) GetPartitionsSeq(ctx context.Context, dbName, tableName string, opts ...CatalogOption) iter.Seq2[*Partition, error] {
	return func(yield func(*Partition, error) bool) {
		var names []string
		err := c.read(ctx, "get_partition_names", func(ctx context.Context, cn *conn) error {
			cat, err := c.resolveCat(ctx, cn, opts)
			if err != nil {
				return err
			}
			names, err = cn.getPartitionNames(ctx, qualifyDBName(cat, dbName), tableName, -1)
			return err
		})
		if err != nil {
			yield(nil, err)
			return
		}

		in := make(columnIntern)
		for i := 0; i < len(names); i += c.cfg.chunkSize {
			if err := ctx.Err(); err != nil {
				yield(nil, wrapError("get_partitions_by_names_req", err))
				return
			}
			end := i + c.cfg.chunkSize
			if end > len(names) {
				end = len(names)
			}
			chunk := names[i:end]

			var raw []*hive_metastore.Partition
			err := c.read(ctx, "get_partitions_by_names_req", func(ctx context.Context, cn *conn) error {
				cat, err := c.resolveCat(ctx, cn, opts)
				if err != nil {
					return err
				}
				return cn.tryReq(ctx, "get_partitions_by_names_req",
					func(ctx context.Context) error {
						resp, err := cn.getPartitionsByNamesReq(ctx, newGetPartitionsByNamesRequest(dbName, tableName, cat, chunk))
						if err != nil {
							return err
						}
						raw = resp.Partitions
						return nil
					},
					func(ctx context.Context) error {
						ps, err := cn.getPartitionsByNames(ctx, qualifyDBName(cat, dbName), tableName, chunk)
						if err != nil {
							return err
						}
						raw = ps
						return nil
					},
				)
			})
			if err != nil {
				yield(nil, err)
				return
			}
			for _, p := range raw {
				if !yield(partitionFromThriftIntern(p, in), nil) {
					return
				}
			}
		}
	}
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
// Requests are batched to at most the client's partition batch size (see
// WithPartitionBatchSize; default 1000) partitions each, the same knob
// AddPartitions uses (SPEC §2.3 Rule 5) -- independent of WithChunkSize;
// batches are sent sequentially, so a failure on a later batch leaves the
// earlier batches already committed on the server.
//
// Against a server lacking alter_partitions_req (Hive 2.3 and 3.x), each
// batch degrades to the legacy alter_partitions RPC (SPEC §2.3 Rule 3).
// The fallback decision itself is made at most once per call: conn.tryReq
// caches it per conn, keyed by method, so only the very first batch that
// reaches a given conn ever attempts alter_partitions_req and observes
// UNKNOWN_METHOD -- every later batch on that conn (this call's own
// remaining batches, and every later AlterPartitions call that lands on
// the same pooled conn) goes straight to legacy.
func (c *Client) AlterPartitions(ctx context.Context, dbName, tableName string, partitions []*Partition, opts ...CatalogOption) error {
	return c.call(ctx, "alter_partitions_req", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
		for i := 0; i < len(partitions); i += c.cfg.partitionBatchSize {
			end := i + c.cfg.partitionBatchSize
			if end > len(partitions) {
				end = len(partitions)
			}
			chunk := partitions[i:end]
			err := cn.tryReq(ctx, "alter_partitions_req",
				func(ctx context.Context) error {
					req := newAlterPartitionsRequest(dbName, tableName, cat, partitionsToThriftFrom(chunk, cat, dbName, tableName))
					_, err := cn.alterPartitionsReq(ctx, req)
					return err
				},
				func(ctx context.Context) error {
					return cn.alterPartitions(ctx, qualifyDBName(cat, dbName), tableName, partitionsToThriftFrom(chunk, cat, dbName, tableName))
				},
			)
			if err != nil {
				return err
			}
		}
		return nil
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
		in := make(columnIntern)
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
					parts = append(parts, partitionsFromThriftIntern(resp.Partitions, in)...)
					return nil
				},
				func(ctx context.Context) error {
					ps, err := cn.getPartitionsByNames(ctx, qualifyDBName(cat, dbName), tableName, chunk)
					if err != nil {
						return err
					}
					parts = append(parts, partitionsFromThriftIntern(ps, in)...)
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
// negative maxParts means "all partitions". maxParts must not exceed
// math.MaxInt16 (32767); values above that return ErrInvalidOperation to
// prevent silent listing truncation on the Thrift i16 wire field. Pass -1
// for all partitions. filter is passed through to the server verbatim --
// this package neither parses nor validates it.
// GetPartitionsByFilter has no request-variant RPC (SPEC §2.3): it always
// calls the legacy get_partitions_by_filter, on every supported version.
func (c *Client) GetPartitionsByFilter(ctx context.Context, dbName, tableName, filter string, maxParts int, opts ...CatalogOption) ([]*Partition, error) {
	mp, err := checkMaxParts("get_partitions_by_filter", maxParts)
	if err != nil {
		return nil, err
	}
	var out []*Partition
	err = c.read(ctx, "get_partitions_by_filter", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
		parts, err := cn.getPartitionsByFilter(ctx, qualifyDBName(cat, dbName), tableName, filter, mp)
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
// wildcarded); a negative maxParts means "all partitions". maxParts must not
// exceed math.MaxInt16 (32767); values above that return ErrInvalidOperation
// to prevent silent listing truncation on the Thrift i16 wire field. Pass -1
// for all partitions. Against a server lacking get_partition_names_ps_req
// (Hive 2.3 and 3.x), it degrades to the legacy get_partition_names_ps RPC
// (SPEC §2.3).
func (c *Client) GetPartitionNamesByValues(ctx context.Context, dbName, tableName string, partialValues []string, maxParts int, opts ...CatalogOption) ([]string, error) {
	mp, err := checkMaxParts("get_partition_names_ps_req", maxParts)
	if err != nil {
		return nil, err
	}
	var out []string
	err = c.read(ctx, "get_partition_names_ps_req", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
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

// newDropPartitionsRequest builds a DropPartitionsRequest from
// hive_metastore.NewDropPartitionsRequest() rather than a bare struct
// literal, so this call's own explicit fields (Parts, DeleteData, IfExists,
// NeedResult_, CatName) are set on top of the constructor's IDL defaults --
// there is no field here without an exported equivalent, unlike
// newPartitionsRequest's ID, so nothing is merely "kept" from the
// constructor rather than overwritten. NeedResult_ is turned off (the
// constructor's own default is true): DropPartitionsByNames never reports
// which partitions were dropped, so there is nothing for the server to
// serialize and return. req.Parts always takes RequestPartsSpec's Names
// arm, never Exprs: Exprs carries Hive's own partition-pruning expression
// bytes (DropPartitionsExpr.Expr), produced by Hive's SQL planner, and this
// package has no such expression builder or use for one.
func newDropPartitionsRequest(dbName, tableName string, cat *string, names []string, deleteData, ifExists bool) *hive_metastore.DropPartitionsRequest {
	req := hive_metastore.NewDropPartitionsRequest()
	req.DbName = dbName
	req.TblName = tableName
	req.Parts = &hive_metastore.RequestPartsSpec{Names: names}
	req.DeleteData = &deleteData
	req.IfExists = ifExists
	req.NeedResult_ = false
	req.CatName = cat
	return req
}

// DropPartitionsByNames removes the partitions of the table named
// tableName in database dbName whose partition name (as returned by
// GetPartitionNames or GetPartitionsByNames, e.g. "dt=2024-01-01", or built
// client-side by PartitionName) is in names. deleteData is forwarded to
// the server. With ifExists true, a name that does not match an existing
// partition is not an error; with ifExists false, the first such name
// aborts the request it was sent in (SPEC §2.3), leaving any partition
// already dropped by an earlier batch dropped (see the batching paragraph
// below) -- drop_partitions_req itself decides where within one request it
// stops, this package does not re-order or retry around a partial failure.
//
// Requests are batched to at most the client's partition batch size (see
// WithPartitionBatchSize; default 1000) names each, the same knob
// AddPartitions and AlterPartitions use (SPEC §2.3 Rule 5); batches are
// sent sequentially, so a failure on a later batch leaves the earlier
// batches already committed on the server.
//
// drop_partitions_req is declared in the Hive 2.3.9 and 3.1.3 IDL as well
// as the 4.2.1 IDL this client is generated from (SPEC §2.1) -- unlike
// alter_partitions_req/get_partitions_req and their siblings, it is not a
// Hive-4-only addition -- so it carries no legacy-RPC fallback (SPEC §2.3
// Rule 2).
func (c *Client) DropPartitionsByNames(ctx context.Context, dbName, tableName string, names []string, deleteData, ifExists bool, opts ...CatalogOption) error {
	return c.call(ctx, "drop_partitions_req", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
		for i := 0; i < len(names); i += c.cfg.partitionBatchSize {
			end := i + c.cfg.partitionBatchSize
			if end > len(names) {
				end = len(names)
			}
			chunk := names[i:end]
			req := newDropPartitionsRequest(dbName, tableName, cat, chunk, deleteData, ifExists)
			if _, err := cn.dropPartitionsReq(ctx, req); err != nil {
				return err
			}
		}
		return nil
	})
}

// DropPartitions removes the partitions of the table named tableName in
// database dbName identified by partVals, each in partition-key order (the
// same shape DropPartition's own partVals takes, one entry per partition
// instead of one partition per call). deleteData and ifExists behave
// exactly as DropPartitionsByNames describes.
//
// DropPartitions first calls GetTable to learn the table's partition keys,
// then builds each partition's name with PartitionName (Hive's own
// Warehouse.makePartName rules, including its escaping and lowercased
// keys) before delegating to DropPartitionsByNames -- names, not values,
// because DropPartitionsRequest's parts field is a RequestPartsSpec, a
// union of a name list and Hive's own partition-pruning expression bytes
// (see newDropPartitionsRequest); there is no arm that takes ordered
// values the way DropPartition's own legacy RPC (drop_partition) does. A
// caller that already has partition names (from GetPartitionNames or
// GetPartitionsByNames) should call DropPartitionsByNames directly rather
// than pay for this extra GetTable round trip.
func (c *Client) DropPartitions(ctx context.Context, dbName, tableName string, partVals [][]string, deleteData, ifExists bool, opts ...CatalogOption) error {
	tbl, err := c.GetTable(ctx, dbName, tableName, opts...)
	if err != nil {
		return err
	}
	keys := make([]string, len(tbl.PartitionKeys))
	for i, k := range tbl.PartitionKeys {
		keys[i] = k.Name
	}
	names := make([]string, len(partVals))
	for i, vals := range partVals {
		name, err := PartitionName(keys, vals)
		if err != nil {
			return wrapAs("drop_partitions_req", ErrInvalidOperation, err)
		}
		names[i] = name
	}
	return c.DropPartitionsByNames(ctx, dbName, tableName, names, deleteData, ifExists, opts...)
}
