// Package hms is a pure-Go client library for Apache Hive Metastore (HMS),
// supporting Hive 2.x, 3.x, and 4.x over both the binary Thrift-over-TCP and
// Thrift-over-HTTP transports. See SPEC.md for the full specification.
package hms

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
	"github.com/slachiewicz/hms-client-go/internal/transport"
)

// errClosed is the cause reported by acquire once the client has been
// closed; call surfaces it wrapped in ErrUnavailable.
var errClosed = errors.New("hms: client closed")

// Client is a connection-pooled Hive Metastore client. Construct one with
// New.
type Client struct {
	cfg      config
	endpoint transport.Endpoint

	// idle holds pooled, currently unused conns. Its buffer size is
	// cfg.poolSize.
	idle chan *conn
	// live counts conns currently dialed (idle or on loan), so acquire
	// knows when it may dial a new one instead of blocking.
	live atomic.Int32

	closed atomic.Bool
}

// New connects to the Hive Metastore endpoint(s) named by uris (a single
// URI, or a comma-separated list for HA; see SPEC §4.1) and returns a ready
// Client. It dials one connection eagerly, so a refused or unreachable
// endpoint fails New immediately with an error wrapping ErrUnavailable.
// Task 11 adds failover across the remaining endpoints; this task uses
// only the first.
func New(ctx context.Context, uris string, opts ...Option) (*Client, error) {
	cfg := newConfig()
	for _, o := range opts {
		o(cfg)
	}

	eps, err := transport.ParseEndpoints(uris)
	if err != nil {
		return nil, err
	}
	ep := eps[0]

	c := &Client{
		cfg:      *cfg,
		endpoint: ep,
		idle:     make(chan *conn, cfg.poolSize),
	}

	cn, err := newConn(ctx, ep, cfg)
	if err != nil {
		return nil, wrapError("New", err)
	}
	c.live.Add(1)
	c.idle <- cn
	return c, nil
}

// Close releases every pooled connection and marks the client closed. Any
// call still in flight completes normally; its connection is closed rather
// than returned to the pool once the call finishes. Close is idempotent.
func (c *Client) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	var errs []error
	for {
		select {
		case cn := <-c.idle:
			if err := cn.close(); err != nil {
				errs = append(errs, err)
			}
			c.live.Add(-1)
		default:
			return errors.Join(errs...)
		}
	}
}

// acquire takes an idle conn from the pool, dials a new one if the pool is
// under capacity, or blocks until one is released or ctx is done. It
// returns errClosed once the client has been closed.
func (c *Client) acquire(ctx context.Context) (*conn, error) {
	if c.closed.Load() {
		return nil, errClosed
	}

	select {
	case cn := <-c.idle:
		return cn, nil
	default:
	}

	for {
		cur := c.live.Load()
		if int(cur) >= c.cfg.poolSize {
			break
		}
		if c.live.CompareAndSwap(cur, cur+1) {
			cn, err := newConn(ctx, c.endpoint, &c.cfg)
			if err != nil {
				c.live.Add(-1)
				return nil, err
			}
			return cn, nil
		}
	}

	select {
	case cn := <-c.idle:
		return cn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// release returns cn to the idle pool, or closes it when the client has
// been closed or the pool is already full.
func (c *Client) release(cn *conn) {
	if c.closed.Load() {
		_ = cn.close()
		c.live.Add(-1)
		return
	}
	select {
	case c.idle <- cn:
	default:
		_ = cn.close()
		c.live.Add(-1)
	}
}

// discard closes cn without returning it to the pool, for use after an
// error that leaves the connection's state unknown (ErrUnavailable).
func (c *Client) discard(cn *conn) {
	_ = cn.close()
	c.live.Add(-1)
}

// call acquires a conn from the pool, runs fn against it, and wraps any
// error fn returns with wrapError(op, err). A conn whose wrapped error
// satisfies errors.Is(err, ErrUnavailable) is discarded instead of
// returned to the pool, since its state after an I/O failure is unknown.
// Task 11 adds the retry/failover loop here.
func (c *Client) call(ctx context.Context, op string, fn func(ctx context.Context, cn *conn) error) error {
	cn, err := c.acquire(ctx)
	if err != nil {
		if errors.Is(err, errClosed) {
			return &hmsError{op: op, sentinel: ErrUnavailable, cause: errClosed}
		}
		return wrapError(op, err)
	}

	if err := fn(ctx, cn); err != nil {
		wrapped := wrapError(op, err)
		if errors.Is(wrapped, ErrUnavailable) {
			c.discard(cn)
		} else {
			c.release(cn)
		}
		return wrapped
	}
	c.release(cn)
	return nil
}

// resolveCat resolves the effective catalog for one call: a per-call
// InCatalog option overrides WithCatalog, which defaults to "hive" (SPEC
// §5.1). It returns nil when the effective catalog is "hive" and cn's
// server does not support catalogs, so the caller never writes a catName
// field on the wire to such a server (SPEC §2.3 Rule 1); it returns
// ErrNotSupported when a non-default catalog is requested against such a
// server; otherwise it returns a pointer to the effective catalog name.
// Any error from the underlying catalog-support probe other than
// UNKNOWN_METHOD propagates unclassified.
func (c *Client) resolveCat(ctx context.Context, cn *conn, opts []CatalogOption) (*string, error) {
	var co catalogOpts
	for _, o := range opts {
		o(&co)
	}
	effective := c.cfg.catalog
	if co.catalog != "" {
		effective = co.catalog
	}
	if effective == "" {
		effective = defaultCatalog
	}

	support, err := cn.supportsCatalogs(ctx)
	if err != nil {
		return nil, err
	}
	if effective == defaultCatalog {
		if !support {
			return nil, nil
		}
		return ptr(defaultCatalog), nil
	}
	if !support {
		return nil, ErrNotSupported
	}
	return ptr(effective), nil
}

// qualifyDBName returns the wire form of a database name for RPCs that
// carry no dedicated catalog field (get_database, drop_database,
// get_all_tables): the Hive convention "@<cat>#<name>" when cat names a
// catalog other than the default, or the bare name otherwise (SPEC §2.3,
// MetaStoreUtils.prependCatalogToDbName).
func qualifyDBName(cat *string, name string) string {
	if cat != nil && *cat != defaultCatalog {
		return "@" + *cat + "#" + name
	}
	return name
}

// GetConfigValue returns the metastore configuration value named name, or
// defaultValue when it is unset.
func (c *Client) GetConfigValue(ctx context.Context, name, defaultValue string) (string, error) {
	var out string
	err := c.call(ctx, "GetConfigValue", func(ctx context.Context, cn *conn) error {
		v, err := cn.getConfigValue(ctx, name, defaultValue)
		if err != nil {
			return err
		}
		out = v
		return nil
	})
	return out, err
}

// ServerVersion reports the connected metastore's version, read from the
// "hive.metastore.version" configuration value, falling back to
// "metastore.version" (as Hive itself does) when that is empty. It returns
// an error wrapping ErrNotSupported if the server reports neither.
func (c *Client) ServerVersion(ctx context.Context) (HiveVersion, error) {
	v, err := c.GetConfigValue(ctx, "hive.metastore.version", "")
	if err != nil {
		return HiveVersion{}, err
	}
	if v == "" {
		v, err = c.GetConfigValue(ctx, "metastore.version", "")
		if err != nil {
			return HiveVersion{}, err
		}
	}
	if v == "" {
		return HiveVersion{}, fmt.Errorf("hms: server reported no metastore version: %w", ErrNotSupported)
	}
	return ParseHiveVersion(v)
}

// GetCatalogs lists the names of every catalog on the connected metastore.
// It returns an error wrapping ErrNotSupported against a server that
// predates catalogs (Hive 2.3).
func (c *Client) GetCatalogs(ctx context.Context) ([]string, error) {
	var names []string
	err := c.call(ctx, "GetCatalogs", func(ctx context.Context, cn *conn) error {
		resp, err := cn.getCatalogs(ctx)
		if err != nil {
			if isUnknownMethod(err) {
				cn.catalogs.Store(int32(catalogNo))
			}
			return err
		}
		cn.catalogs.Store(int32(catalogYes))
		names = resp.Names
		return nil
	})
	return names, err
}

// GetCatalog returns the catalog named name.
func (c *Client) GetCatalog(ctx context.Context, name string) (*Catalog, error) {
	var out *Catalog
	err := c.call(ctx, "GetCatalog", func(ctx context.Context, cn *conn) error {
		resp, err := cn.getCatalog(ctx, &hive_metastore.GetCatalogRequest{Name: name})
		if err != nil {
			return err
		}
		out = catalogFromThrift(resp.Catalog)
		return nil
	})
	return out, err
}

// CreateCatalog creates cat.
func (c *Client) CreateCatalog(ctx context.Context, cat *Catalog) error {
	return c.call(ctx, "CreateCatalog", func(ctx context.Context, cn *conn) error {
		return cn.createCatalog(ctx, &hive_metastore.CreateCatalogRequest{Catalog: catalogToThrift(cat)})
	})
}

// DropCatalog removes the catalog named name. With ifExists true, a missing
// catalog is not an error.
func (c *Client) DropCatalog(ctx context.Context, name string, ifExists bool) error {
	return c.call(ctx, "DropCatalog", func(ctx context.Context, cn *conn) error {
		err := cn.dropCatalog(ctx, &hive_metastore.DropCatalogRequest{Name: name})
		if err != nil && ifExists && classify(err) == ErrNotFound {
			return nil
		}
		return err
	})
}

// GetAllDatabases lists the names of every database in the default "hive"
// catalog. The generated get_all_databases RPC has no catalog parameter, so
// a non-default CatalogOption only affects whether ErrNotSupported is
// returned (when the server predates catalogs); it does not filter the
// result.
func (c *Client) GetAllDatabases(ctx context.Context, opts ...CatalogOption) ([]string, error) {
	var names []string
	err := c.call(ctx, "GetAllDatabases", func(ctx context.Context, cn *conn) error {
		if _, err := c.resolveCat(ctx, cn, opts); err != nil {
			return err
		}
		list, err := cn.getAllDatabases(ctx)
		if err != nil {
			return err
		}
		names = list
		return nil
	})
	return names, err
}

// GetDatabase returns the database named name.
func (c *Client) GetDatabase(ctx context.Context, name string, opts ...CatalogOption) (*Database, error) {
	var out *Database
	err := c.call(ctx, "GetDatabase", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
		d, err := cn.getDatabase(ctx, qualifyDBName(cat, name))
		if err != nil {
			return err
		}
		out = databaseFromThrift(d, cat)
		return nil
	})
	return out, err
}

// CreateDatabase creates db. A non-empty db.CatalogName overrides the
// client's default catalog for this call.
func (c *Client) CreateDatabase(ctx context.Context, db *Database) error {
	return c.call(ctx, "CreateDatabase", func(ctx context.Context, cn *conn) error {
		var opts []CatalogOption
		if db.CatalogName != "" {
			opts = append(opts, InCatalog(db.CatalogName))
		}
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
		return cn.createDatabase(ctx, databaseToThrift(db, cat))
	})
}

// DropDatabase removes the database named name. deleteData and cascade are
// forwarded to the server; cascade must be true to drop a non-empty
// database, or the server returns ErrInvalidOperation. With ifExists true,
// a missing database is not an error.
func (c *Client) DropDatabase(ctx context.Context, name string, deleteData, cascade, ifExists bool, opts ...CatalogOption) error {
	return c.call(ctx, "DropDatabase", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
		err = cn.dropDatabase(ctx, qualifyDBName(cat, name), deleteData, cascade)
		if err != nil && ifExists && classify(err) == ErrNotFound {
			return nil
		}
		return err
	})
}
