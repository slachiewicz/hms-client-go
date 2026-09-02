// Package hms is a pure-Go client library for Apache Hive Metastore (HMS),
// supporting Hive 2.x, 3.x, and 4.x over both the binary Thrift-over-TCP and
// Thrift-over-HTTP transports. See SPEC.md for the full specification.
package hms

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
	"github.com/slachiewicz/hms-client-go/internal/ha"
	"github.com/slachiewicz/hms-client-go/internal/transport"
)

// errClosed is the cause reported by acquire once the client has been
// closed. It wraps ErrUnavailable directly so the normal wrapError/classify
// path (errors.go) reports it correctly without any special-casing here.
var errClosed = fmt.Errorf("hms: client closed: %w", ErrUnavailable)

// endpointPool holds the pooled connections and live count for one
// endpoint. A Client has one endpointPool per endpoint, in endpoint order
// (matching endpoints and cluster's index space); all of them share the
// Client's mu and closeCh.
type endpointPool struct {
	// idle holds pooled, currently unused conns for this endpoint. Its
	// buffer size is cfg.poolSize. Only touched while Client.mu is held.
	idle chan *conn
	// live counts conns currently dialed for this endpoint (idle or on
	// loan), so acquire knows when it may dial a new one instead of
	// blocking.
	live atomic.Int32
}

// Client is a connection-pooled Hive Metastore client, failing over across
// endpoints per SPEC §4.2. Construct one with New.
type Client struct {
	cfg       config
	endpoints []transport.Endpoint
	cluster   *ha.Cluster

	// mu guards closed together with every send into, or drain of, any
	// pool's idle channel: Close must never observe every pool as fully
	// drained while a concurrent release is still deciding whether to
	// send into one (and vice versa), or the conn that "won" that race
	// would never be closed. See release and Close.
	mu sync.Mutex
	// pools holds one endpointPool per endpoint, indexed the same way as
	// endpoints and cluster. Only touched while mu is held.
	pools []*endpointPool
	// closed is true once Close has run. Only touched while mu is held.
	closed bool
	// closeCh is closed exactly once, by Close, so a goroutine blocked in
	// acquire's final select wakes immediately instead of waiting for a
	// release or its context that may never come.
	closeCh chan struct{}

	// probeCancel stops the recovery-probe goroutine; probeDone is
	// closed once that goroutine has actually exited, so Close can wait
	// for it.
	probeCancel context.CancelFunc
	probeDone   chan struct{}
}

// New connects to the Hive Metastore endpoint(s) named by uris (a single
// URI, or a comma-separated list for HA; see SPEC §4.1) and returns a ready
// Client. It dials the first endpoint that answers, in cluster order (list
// order, or random order under WithRandomEndpointOrder), so New fails only
// when every endpoint refuses or is unreachable, with an error wrapping
// ErrUnavailable. A background goroutine periodically probes cooled-down
// endpoints for recovery (SPEC §4.2 point 4); Close stops it.
func New(ctx context.Context, uris string, opts ...Option) (*Client, error) {
	cfg := newConfig()
	for _, o := range opts {
		o(cfg)
	}
	cfg.clamp()

	eps, err := transport.ParseEndpoints(uris)
	if err != nil {
		// transport.ParseEndpoints' errors (bad scheme, malformed URI,
		// mixed schemes, empty list) are all caller mistakes, not
		// metastore or transport failures, and classify has nothing in
		// their shape to recognize; force ErrInvalidOperation rather than
		// letting classify's default of ErrMeta hide that distinction.
		return nil, wrapAs("New", ErrInvalidOperation, err)
	}

	cluster := ha.New(len(eps), cfg.randomOrder, time.Now)
	pools := make([]*endpointPool, len(eps))
	for i := range pools {
		pools[i] = &endpointPool{idle: make(chan *conn, cfg.poolSize)}
	}

	c := &Client{
		cfg:       *cfg,
		endpoints: eps,
		cluster:   cluster,
		pools:     pools,
		closeCh:   make(chan struct{}),
	}

	var lastErr error
	dialed := false
	for attempt := 0; attempt < len(eps); attempt++ {
		idx, ok := cluster.Pick()
		if !ok {
			break
		}
		cn, err := newConn(ctx, eps[idx], cfg)
		if err != nil {
			cluster.MarkFailed(idx)
			lastErr = err
			continue
		}
		pools[idx].live.Add(1)
		pools[idx].idle <- cn
		dialed = true
		break
	}
	if !dialed {
		return nil, wrapError("New", errors.Join(ErrUnavailable, lastErr))
	}

	probeCtx, cancel := context.WithCancel(context.Background())
	c.probeCancel = cancel
	c.probeDone = make(chan struct{})
	go c.recoveryProbe(probeCtx)

	return c, nil
}

// Close releases every pooled connection, stops the recovery-probe
// goroutine (waiting for it to exit), and marks the client closed. Any
// call still in flight completes normally; its connection is closed rather
// than returned to the pool once the call finishes (see release). A
// goroutine blocked in acquire wakes immediately via closeCh. Close is
// idempotent.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	close(c.closeCh)

	var errs []error
	for _, p := range c.pools {
		drainPool(p, &errs)
	}
	c.mu.Unlock()

	if c.probeCancel != nil {
		c.probeCancel()
	}
	if c.probeDone != nil {
		<-c.probeDone
	}
	return errors.Join(errs...)
}

// drainPool closes every conn currently idle in p, appending any close
// error to errs. Client.mu must be held.
func drainPool(p *endpointPool, errs *[]error) {
	for {
		select {
		case cn := <-p.idle:
			if err := cn.close(); err != nil {
				*errs = append(*errs, err)
			}
			p.live.Add(-1)
		default:
			return
		}
	}
}

// acquire takes an idle conn from endpoint idx's pool, dials a new one if
// that pool is under capacity, or blocks until one is released, the client
// is closed, or ctx is done. It returns errClosed once the client has been
// closed.
func (c *Client) acquire(ctx context.Context, idx int) (*conn, error) {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return nil, errClosed
	}
	p := c.pools[idx]

	select {
	case cn := <-p.idle:
		return cn, nil
	default:
	}

	for {
		cur := p.live.Load()
		if int(cur) >= c.cfg.poolSize {
			break
		}
		if p.live.CompareAndSwap(cur, cur+1) {
			cn, err := newConn(ctx, c.endpoints[idx], &c.cfg)
			if err != nil {
				p.live.Add(-1)
				return nil, err
			}
			return cn, nil
		}
	}

	select {
	case cn := <-p.idle:
		return cn, nil
	case <-c.closeCh:
		return nil, errClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// release returns cn to endpoint idx's idle pool, or closes it when the
// client has been closed or that pool is already full. The closed check
// and the idle send happen under the same lock Close holds while draining
// every pool, so a release racing a concurrent Close either lands in idle
// before Close's drain sees it, or observes closed and closes cn itself;
// either way cn is never abandoned in an idle pool nobody will ever drain
// again.
func (c *Client) release(idx int, cn *conn) {
	p := c.pools[idx]
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = cn.close()
		p.live.Add(-1)
		return
	}
	select {
	case p.idle <- cn:
		c.mu.Unlock()
		return
	default:
	}
	c.mu.Unlock()
	_ = cn.close()
	p.live.Add(-1)
}

// discard closes cn without returning it to endpoint idx's pool, for use
// after an error that leaves the connection's state unknown
// (ErrUnavailable).
func (c *Client) discard(idx int, cn *conn) {
	_ = cn.close()
	c.pools[idx].live.Add(-1)
}

// call picks an endpoint via cluster, acquires a conn from its pool, and
// runs fn against it, retrying on another endpoint per SPEC §4.2 point 3:
// a dial failure is always retryable; once fn has started, a failure is
// retried only when it classifies as ErrUnavailable, ctx is not yet done,
// and op is idempotent (its wire name has the "get_" prefix used by every
// read-only RPC). A non-idempotent op (create_*, add_partitions*, drop_*,
// alter_*) that fails after fn started is returned immediately, without
// trying another endpoint, since the request may already have reached the
// server. Attempts are bounded by cfg.maxRetries (clamped to at least 1 by
// config.clamp). When every endpoint is cooling, call returns
// ErrUnavailable joined with the last error observed.
func (c *Client) call(ctx context.Context, op string, fn func(ctx context.Context, cn *conn) error) error {
	idempotent := strings.HasPrefix(op, "get_")
	var last error
	for attempt := 0; attempt < c.cfg.maxRetries; attempt++ {
		idx, ok := c.cluster.Pick()
		if !ok {
			return wrapError(op, errors.Join(ErrUnavailable, last))
		}

		cn, err := c.acquire(ctx, idx)
		if err != nil {
			c.cluster.MarkFailed(idx)
			last = err
			continue // dial failures are always retryable
		}

		err = fn(ctx, cn)
		if err == nil {
			c.release(idx, cn)
			c.cluster.MarkHealthy(idx)
			return nil
		}
		if errors.Is(classify(err), ErrUnavailable) && ctx.Err() == nil {
			c.discard(idx, cn)
			c.cluster.MarkFailed(idx)
			last = err
			if idempotent {
				continue
			}
		} else {
			c.release(idx, cn)
		}
		return wrapError(op, err)
	}
	return wrapError(op, last)
}

// recoveryProbe periodically re-tests every endpoint the cluster currently
// reports as cooling (SPEC §4.2 point 4), until ctx is cancelled by Close.
// probeDone is closed on return so Close can wait for this goroutine to
// actually exit before returning itself.
func (c *Client) recoveryProbe(ctx context.Context) {
	defer close(c.probeDone)
	ticker := time.NewTicker(c.cfg.probeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.probeCooling(ctx)
		}
	}
}

// probeCooling dials a fresh conn to each of the cluster's currently
// cooling endpoints and calls fb303's getStatus on it. A successful probe
// marks that endpoint healthy and hands the freshly dialed conn to its
// pool (or closes it if the pool is already full or the client has been
// closed in the meantime); a failed probe leaves the endpoint cooling and
// closes the conn, if one was even dialed.
func (c *Client) probeCooling(ctx context.Context) {
	for _, idx := range c.cluster.Cooling() {
		cn, err := newConn(ctx, c.endpoints[idx], &c.cfg)
		if err != nil {
			continue
		}
		if _, err := cn.getStatus(ctx); err != nil {
			_ = cn.close()
			continue
		}
		c.cluster.MarkHealthy(idx)

		p := c.pools[idx]
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			_ = cn.close()
			continue
		}
		select {
		case p.idle <- cn:
			p.live.Add(1)
			c.mu.Unlock()
		default:
			c.mu.Unlock()
			_ = cn.close()
		}
	}
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
	err := c.call(ctx, "get_config_value", func(ctx context.Context, cn *conn) error {
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
	err := c.call(ctx, "get_catalogs", func(ctx context.Context, cn *conn) error {
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
	err := c.call(ctx, "get_catalog", func(ctx context.Context, cn *conn) error {
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
	return c.call(ctx, "create_catalog", func(ctx context.Context, cn *conn) error {
		return cn.createCatalog(ctx, &hive_metastore.CreateCatalogRequest{Catalog: catalogToThrift(cat)})
	})
}

// DropCatalog removes the catalog named name. With ifExists true, a missing
// catalog is not an error.
func (c *Client) DropCatalog(ctx context.Context, name string, ifExists bool) error {
	return c.call(ctx, "drop_catalog", func(ctx context.Context, cn *conn) error {
		err := cn.dropCatalog(ctx, &hive_metastore.DropCatalogRequest{Name: name})
		if err != nil && ifExists && classify(err) == ErrNotFound {
			return nil
		}
		return err
	})
}

// GetAllDatabases lists the names of every database in the effective
// catalog (WithCatalog, overridden per call by InCatalog; default "hive").
// The generated get_all_databases RPC has no catalog parameter, so for the
// default catalog this calls it directly; for a non-default catalog it
// calls get_databases with a "@<cat>#*" pattern instead (the Hive
// convention, MetaStoreUtils.prependCatalogToDbName, mirrored by
// qualifyDBName). Against a server that predates catalogs (Hive 2.3), a
// non-default CatalogOption returns ErrNotSupported without issuing either
// RPC.
func (c *Client) GetAllDatabases(ctx context.Context, opts ...CatalogOption) ([]string, error) {
	var names []string
	err := c.call(ctx, "get_all_databases", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
		var list []string
		if cat != nil && *cat != defaultCatalog {
			list, err = cn.getDatabases(ctx, qualifyDBName(cat, "*"))
		} else {
			list, err = cn.getAllDatabases(ctx)
		}
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
	err := c.call(ctx, "get_database", func(ctx context.Context, cn *conn) error {
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
	return c.call(ctx, "create_database", func(ctx context.Context, cn *conn) error {
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
	return c.call(ctx, "drop_database", func(ctx context.Context, cn *conn) error {
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
