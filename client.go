// Package hms is a pure-Go client library for Apache Hive Metastore (HMS),
// supporting Hive 2.x, 3.x, and 4.x over both the binary Thrift-over-TCP and
// Thrift-over-HTTP transports. See SPEC.md for the full specification.
package hms

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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

	// A misconfigured SASL mechanism is a caller mistake too, and must not
	// reach the dial loop below, whose every failure classifies as
	// ErrUnavailable. See validateAuth.
	if err := validateAuth(cfg, eps); err != nil {
		return nil, wrapAs("New", ErrInvalidOperation, err)
	}
	// So is a WithTLS the HTTP transport would silently ignore. See
	// validateTransport.
	if err := validateTransport(cfg, eps); err != nil {
		return nil, wrapAs("New", ErrInvalidOperation, err)
	}

	// The caller's Kerberos credentials are loaded once here, not once per
	// dial: the gokrb5 client behind them runs a session-renewal goroutine
	// that only Close stops (SPEC §3.1). Every conn this Client dials reads
	// the session off cfg (see krbConfig), and Close releases it.
	if cfg.krbSession, err = newKerberosSession(cfg, eps); err != nil {
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
			c.markFailed(idx, "dial")
			lastErr = err
			continue
		}
		pools[idx].live.Add(1)
		pools[idx].idle <- cn
		dialed = true
		break
	}
	if !dialed {
		// No Client is returned, so nothing will ever call Close to
		// release the session this New just built.
		cfg.krbSession.Close()
		return nil, wrapError("New", errors.Join(ErrUnavailable, lastErr))
	}

	probeCtx, cancel := context.WithCancel(context.Background())
	c.probeCancel = cancel
	c.probeDone = make(chan struct{})
	go c.recoveryProbe(probeCtx)

	return c, nil
}

// markFailed marks endpoint idx failed on the cluster, from every call site
// that observes a failure worth cooling an endpoint down for: New's initial
// dial loop and do's own retry loop. It logs at slog.LevelInfo only when
// this is a real transition -- idx was not already cooling (Cluster.
// MarkFailed's bool) -- so a burst of failed attempts against an endpoint
// that is already known down produces one Info line, not one per attempt; a
// repeat failure while already cooling logs at slog.LevelDebug instead
// (SPEC §5.10).
func (c *Client) markFailed(idx int, reason string) {
	ep := endpointURI(c.endpoints[idx])
	if c.cluster.MarkFailed(idx) {
		c.cfg.logger.Info("endpoint marked failed", "endpoint", ep, "reason", reason)
	} else {
		c.cfg.logger.Debug("endpoint still failed", "endpoint", ep, "reason", reason)
	}
}

// markHealthy marks endpoint idx healthy on the cluster, from every call
// site that observes an endpoint working again: do's own success path and
// probeCooling's successful getStatus probe. It logs at slog.LevelInfo only
// when this is a real transition -- idx was actually cooling beforehand
// (Cluster.MarkHealthy's bool) -- so every successful call against an
// endpoint that was already healthy produces no Info line at all (SPEC
// §5.10).
func (c *Client) markHealthy(idx int, reason string) {
	ep := endpointURI(c.endpoints[idx])
	if c.cluster.MarkHealthy(idx) {
		c.cfg.logger.Info("endpoint marked healthy", "endpoint", ep, "reason", reason)
	}
}

// observe invokes the configured WithRPCObserver function, if any, with the
// completed attempt's RPCInfo (SPEC §5.10). It runs synchronously on do's
// own goroutine, as the observer contract requires; a panic escaping f is
// recovered and logged at slog.LevelError rather than propagated to do's
// caller, so a misbehaving observer cannot fail an otherwise successful
// RPC.
func (c *Client) observe(op string, idx, attempt int, dur time.Duration, err error) {
	if c.cfg.observer == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			c.cfg.logger.Error("observer panicked", "method", op, "recovered", r)
		}
	}()
	c.cfg.observer(RPCInfo{
		Method:   op,
		Endpoint: endpointURI(c.endpoints[idx]),
		Attempt:  attempt,
		Duration: dur,
		Err:      err,
	})
}

// Close releases every pooled connection, stops the recovery-probe
// goroutine, releases the Kerberos credentials New loaded when
// WithKerberos was configured (which stops their renewal goroutine), and
// marks the client closed. Every caller -- concurrent or
// sequential, whichever call actually does the closing or not -- waits
// for the probe goroutine to have exited before returning. Any call still
// in flight completes normally; its connection is closed rather than
// returned to the pool once the call finishes (see release). A goroutine
// blocked in acquire wakes immediately via closeCh. Close is idempotent.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		// A concurrent (or later) call also waits for the probe
		// goroutine to have exited, not just the first one: probeDone
		// is closed exactly once by recoveryProbe itself, so reading
		// from it here is safe (and immediate) no matter how many
		// callers do it, or in what order relative to the first
		// caller's own wait below.
		if c.probeDone != nil {
			<-c.probeDone
		}
		return nil
	}
	c.closed = true
	close(c.closeCh)

	var errs []error
	for idx, p := range c.pools {
		drainPool(p, &errs, c.cfg.logger, endpointURI(c.endpoints[idx]))
	}
	c.mu.Unlock()

	if c.probeCancel != nil {
		c.probeCancel()
	}
	if c.probeDone != nil {
		<-c.probeDone
	}
	// Last, once no pooled conn and no probe can still dial with these
	// credentials: closing the Kerberos session stops the gokrb5 client's
	// session-renewal goroutine (SPEC §3.1). It is a no-op when
	// WithKerberos was never configured, and idempotent, so the
	// already-closed path above needs no counterpart.
	c.cfg.krbSession.Close()
	return errors.Join(errs...)
}

// drainPool closes every conn currently idle in p, appending any close
// error to errs and logging each close at slog.LevelDebug against ep.
// Client.mu must be held.
func drainPool(p *endpointPool, errs *[]error, logger *slog.Logger, ep string) {
	for {
		select {
		case cn := <-p.idle:
			if err := cn.close(); err != nil {
				*errs = append(*errs, err)
			}
			p.live.Add(-1)
			logger.Debug("conn closed", "endpoint", ep)
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
		c.cfg.logger.Debug("conn discarded", "endpoint", endpointURI(c.endpoints[idx]), "reason", "client closed")
		return
	}
	select {
	case p.idle <- cn:
		c.mu.Unlock()
		c.cfg.logger.Debug("conn released", "endpoint", endpointURI(c.endpoints[idx]))
		return
	default:
	}
	c.mu.Unlock()
	_ = cn.close()
	p.live.Add(-1)
	c.cfg.logger.Debug("conn discarded", "endpoint", endpointURI(c.endpoints[idx]), "reason", "pool full")
}

// discard closes cn without returning it to endpoint idx's pool, for use
// after an error that leaves the connection's state unknown
// (ErrUnavailable).
func (c *Client) discard(idx int, cn *conn) {
	_ = cn.close()
	c.pools[idx].live.Add(-1)
	c.cfg.logger.Debug("conn discarded", "endpoint", endpointURI(c.endpoints[idx]), "reason", "unavailable")
}

// call runs fn as a non-idempotent RPC (create_*, add_partitions*, drop_*,
// alter_*): an acquire/dial failure retries on another endpoint, but once
// fn has started, any failure -- including one that classifies as
// ErrUnavailable -- is returned immediately without trying another
// endpoint, since the request may already have reached the server. Use
// read instead for a get_* (idempotent, read-only) RPC.
func (c *Client) call(ctx context.Context, op string, fn func(ctx context.Context, cn *conn) error) error {
	return c.do(ctx, op, false, fn)
}

// read runs fn as an idempotent, read-only RPC (get_*): like call, an
// acquire/dial failure retries on another endpoint; additionally, once fn
// has started, a failure that classifies as ErrUnavailable is also retried
// on another endpoint, as long as ctx is not yet done, since re-issuing a
// read cannot duplicate a server-side effect. Use call instead for any RPC
// with a side effect the server might already have applied.
func (c *Client) read(ctx context.Context, op string, fn func(ctx context.Context, cn *conn) error) error {
	return c.do(ctx, op, true, fn)
}

// do is the retry loop shared by call and read: it picks an endpoint via
// cluster, acquires a conn from its pool, and runs fn against it, retrying
// on another endpoint per SPEC §4.2 point 3. A dial/acquire failure is
// retryable unless ctx is already done or the client has been closed
// (errClosed): either means every remaining endpoint would fail the same
// way for a reason that has nothing to do with that endpoint's health, so
// do returns immediately rather than calling MarkFailed on endpoints that
// may be perfectly healthy.
//
// Once fn has started, two decisions are made separately. Whether the conn
// is discarded rather than released back to its pool depends only on
// whether classify(err) is ErrUnavailable: the connection's state on the
// wire is unknown regardless of why fn failed, including when fn failed
// because ctx was cancelled or its deadline passed mid-RPC -- releasing it
// in that case would hand the next caller a conn with a half-read response
// still on the wire. Whether the endpoint is additionally marked failed
// and the call retried on another endpoint requires, on top of that, that
// idempotent is true and ctx is not yet done: a caller's own
// cancellation/deadline is not evidence the endpoint is unhealthy, so it
// must not cool down an otherwise-fine endpoint, and re-issuing the call
// on the caller's behalf after they've already given up would be wrong.
// Attempts are bounded by cfg.maxRetries (clamped to at least 1 by
// config.clamp). When every endpoint is cooling, do returns ErrUnavailable
// joined with the last error observed.
//
// Every time fn actually runs, the WithRPCObserver function (if any) is
// invoked once, synchronously, with that attempt's RPCInfo -- Attempt is
// 1-based and counts only attempts that reached fn, not acquire/dial
// failures that never called it (SPEC §5.10); see observe. A dial/acquire
// failure and a retry decision are logged at slog.LevelDebug through
// WithLogger's logger; the endpoint transitions this loop drives (marked
// failed, marked healthy) are logged at slog.LevelInfo by markFailed and
// markHealthy themselves.
func (c *Client) do(ctx context.Context, op string, idempotent bool, fn func(ctx context.Context, cn *conn) error) error {
	var last error
	rpcAttempt := 0
	for attempt := 0; attempt < c.cfg.maxRetries; attempt++ {
		idx, ok := c.cluster.Pick()
		if !ok {
			return wrapError(op, errors.Join(ErrUnavailable, last))
		}

		cn, err := c.acquire(ctx, idx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, errClosed) {
				// ctx's own cancellation/deadline, or the client
				// being closed, is not evidence idx is unhealthy;
				// marking it failed here would needlessly cool down
				// (and, over repeated calls like this, exhaust) an
				// otherwise-fine endpoint for a reason unrelated to
				// it.
				return wrapError(op, err)
			}
			c.markFailed(idx, "dial")
			last = err
			c.cfg.logger.Debug("retrying on next endpoint", "method", op, "endpoint", endpointURI(c.endpoints[idx]), "err", err)
			continue // dial failures are always retryable
		}

		rpcAttempt++
		start := time.Now()
		err = fn(ctx, cn)
		c.observe(op, idx, rpcAttempt, time.Since(start), err)

		if err == nil {
			c.release(idx, cn)
			c.markHealthy(idx, op)
			return nil
		}
		if errors.Is(classify(err), ErrUnavailable) {
			c.discard(idx, cn)
			if ctx.Err() == nil {
				c.markFailed(idx, op)
				last = err
				if idempotent {
					c.cfg.logger.Debug("retrying on next endpoint", "method", op, "endpoint", endpointURI(c.endpoints[idx]), "err", err)
					continue
				}
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
// closes the conn, if one was even dialed. Every probe's outcome is logged
// at slog.LevelDebug (SPEC §5.10); a successful one additionally gets
// markHealthy's slog.LevelInfo "endpoint marked healthy" log.
func (c *Client) probeCooling(ctx context.Context) {
	for _, idx := range c.cluster.Cooling() {
		ep := endpointURI(c.endpoints[idx])
		cn, err := newConn(ctx, c.endpoints[idx], &c.cfg)
		if err != nil {
			c.cfg.logger.Debug("probe failed", "endpoint", ep, "err", err)
			continue
		}
		if _, err := cn.getStatus(ctx); err != nil {
			_ = cn.close()
			c.cfg.logger.Debug("probe failed", "endpoint", ep, "err", err)
			continue
		}
		c.cfg.logger.Debug("probe succeeded", "endpoint", ep)
		c.markHealthy(idx, "probe")

		p := c.pools[idx]
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			_ = cn.close()
			continue
		}
		// live is incremented before the send, not after: acquire's
		// capacity check (p.live.Load() vs cfg.poolSize) must never
		// observe this conn as neither counted nor yet in idle, or it
		// could dial one more than poolSize permits in the window
		// between the two.
		p.live.Add(1)
		select {
		case p.idle <- cn:
			c.mu.Unlock()
		default:
			c.mu.Unlock()
			_ = cn.close()
			p.live.Add(-1)
		}
	}
}

// resolveCat resolves the effective catalog for one call from opts alone:
// a per-call InCatalog option overrides WithCatalog, which defaults to
// "hive" (SPEC §5.1). It is resolveCatFor with an empty structCat, for the
// majority of calls that take no struct carrying its own CatalogName field.
// See resolveCatFor for the full precedence order and every other aspect of
// this behavior.
func (c *Client) resolveCat(ctx context.Context, cn *conn, opts []CatalogOption) (*string, error) {
	return c.resolveCatFor(ctx, cn, "", opts)
}

// resolveCatFor resolves the effective catalog for one call, in precedence
// order highest first: a per-call InCatalog option (opts), then structCat
// (a create/alter call's own struct.CatalogName field, e.g.
// CreateDatabase's db.CatalogName or AlterTable's newTable.CatalogName;
// empty means the struct did not set one), then WithCatalog, then "hive"
// (SPEC §5.0). It returns nil when the effective catalog is "hive" and
// cn's server does not support catalogs, so the caller never writes a
// catName field on the wire to such a server (SPEC §2.3 Rule 1); it
// returns ErrNotSupported when a non-default catalog is requested against
// such a server; otherwise it returns a pointer to the effective catalog
// name. Any error from the underlying catalog-support probe other than
// UNKNOWN_METHOD propagates unclassified.
func (c *Client) resolveCatFor(ctx context.Context, cn *conn, structCat string, opts []CatalogOption) (*string, error) {
	var co catalogOpts
	for _, o := range opts {
		o(&co)
	}
	effective := c.cfg.catalog
	if structCat != "" {
		effective = structCat
	}
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
	err := c.read(ctx, "get_config_value", func(ctx context.Context, cn *conn) error {
		v, err := cn.getConfigValue(ctx, name, defaultValue)
		if err != nil {
			return err
		}
		out = v
		return nil
	})
	return out, err
}

// ServerVersion reports the connected metastore's release line. It tries
// the fb303 getVersion RPC first: a Hive 4.x server answers with its real
// release (e.g. "4.0.1") and that is returned as-is. Pre-4 metastores do
// not report their release there -- they answer with the metastore schema
// version line instead (every Hive 3.x release answers "3.0", and so does
// Hive 2.3.x) -- so ServerVersion infers the line from capabilities probed
// on the same connection: a server that supports catalogs (SPEC §2.3 Rule
// 1) is Hive 3.x and is reported as HiveVersion{Major: 3, Minor: 0}; one
// that does not is Hive 2.3.x and is reported as HiveVersion{Major: 2,
// Minor: 3}. Raw always carries the server's literal getVersion answer, so
// callers that need the true 3.x patch release cannot get it from this
// RPC. As a last resort, when getVersion itself errors with
// UNKNOWN_METHOD, ServerVersion falls back to the "hive.metastore.version"
// configuration value, then to "metastore.version" (as Hive itself does);
// no capability inference applies to that fallback value. It returns an
// error wrapping ErrNotSupported if the server reports a version from none
// of these.
func (c *Client) ServerVersion(ctx context.Context) (HiveVersion, error) {
	var v string
	var catalogs, haveCatalogs bool
	err := c.read(ctx, "getVersion", func(ctx context.Context, cn *conn) error {
		s, err := cn.getVersion(ctx)
		if err != nil {
			return err
		}
		v = s
		if hv, perr := ParseHiveVersion(s); perr == nil && hv.Major < 4 {
			ok, err := cn.supportsCatalogs(ctx)
			if err != nil {
				return err
			}
			catalogs, haveCatalogs = ok, true
		}
		return nil
	})
	if err != nil && !isUnknownMethod(err) {
		return HiveVersion{}, err
	}
	if v == "" {
		v, err = c.GetConfigValue(ctx, "hive.metastore.version", "")
		if err != nil {
			return HiveVersion{}, err
		}
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
	hv, err := ParseHiveVersion(v)
	if err != nil {
		return HiveVersion{}, err
	}
	if haveCatalogs && hv.Major < 4 {
		if catalogs {
			return HiveVersion{Major: 3, Minor: 0, Raw: hv.Raw}, nil
		}
		return HiveVersion{Major: 2, Minor: 3, Raw: hv.Raw}, nil
	}
	return hv, nil
}

// GetCatalogs lists the names of every catalog on the connected metastore.
// It returns an error wrapping ErrNotSupported against a server that
// predates catalogs (Hive 2.3).
func (c *Client) GetCatalogs(ctx context.Context) ([]string, error) {
	var names []string
	err := c.read(ctx, "get_catalogs", func(ctx context.Context, cn *conn) error {
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
	err := c.read(ctx, "get_catalog", func(ctx context.Context, cn *conn) error {
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
	err := c.read(ctx, "get_all_databases", func(ctx context.Context, cn *conn) error {
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
	err := c.read(ctx, "get_database", func(ctx context.Context, cn *conn) error {
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
//
// When db.LocationURI is empty, CreateDatabase fills it in client-side
// before issuing the RPC, the way Hive's own DDL path does: the generated
// Database.locationUri field has default (not optional) Thrift
// requiredness, so an unset Go LocationURI would otherwise be written to
// the wire as "" rather than left absent as the Java client leaves it, and
// the server rejects that with MetaException(IllegalArgumentException: Can
// not create a Path from an empty string) instead of computing the
// warehouse default itself. The filled-in location is
// "<warehouse>/<db>.db" (lowercased db name).
//
// The warehouse root comes from the resolved catalog's own LocationUri
// (get_catalog) on any server that supports catalogs (Hive 3.1+): the
// default catalog's location IS the warehouse dir there, and a non-default
// catalog's location is its own warehouse root by definition. Only a
// server that predates catalogs (Hive 2.3, which has no catalog to ask)
// falls back to the "hive.metastore.warehouse.dir" configuration value.
// This split exists because Hive 3.1's get_config_value does not resolve
// that key to its "metastore.warehouse.dir" alias the way Hive 4's does --
// it answers empty instead -- so asking the catalog sidesteps the quirk
// entirely rather than working around it.
func (c *Client) CreateDatabase(ctx context.Context, db *Database) error {
	return c.call(ctx, "create_database", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCatFor(ctx, cn, db.CatalogName, nil)
		if err != nil {
			return err
		}

		toCreate := db
		if db.LocationURI == "" {
			var base string
			if cat != nil {
				resp, err := cn.getCatalog(ctx, &hive_metastore.GetCatalogRequest{Name: *cat})
				if err != nil {
					return err
				}
				base = resp.Catalog.LocationUri
			} else {
				base, err = cn.getConfigValue(ctx, "hive.metastore.warehouse.dir", "")
				if err != nil {
					return err
				}
			}
			base = strings.TrimRight(base, "/")
			if base == "" {
				return wrapAs("create_database", ErrInvalidOperation, errors.New("hms: LocationURI is empty and the metastore warehouse dir is unknown"))
			}
			cp := *db
			cp.LocationURI = base + "/" + strings.ToLower(db.Name) + ".db"
			toCreate = &cp
		}

		return cn.createDatabase(ctx, databaseToThrift(toCreate, cat))
	})
}

// AlterDatabase replaces the mutable properties (Description, LocationURI,
// Parameters, OwnerName, OwnerType) of the database named name with db's
// (SPEC §5.3, 1.0 addition); AlterDatabase itself never writes a
// db.CreateTime of its own (see databaseToThrift), though a db that carries
// a round-trip fidelity snapshot -- i.e. one GetDatabase itself returned --
// echoes the original, server-assigned CreateTime back rather than clearing
// it, which is harmless since the field is immutable; a field neither this
// package's Database nor this doc comment mentions (Privileges, Type,
// ConnectorName, RemoteDbname, ManagedLocationUri) survives the same way
// (SPEC §5.4 "Round-trip fidelity"). A non-empty db.CatalogName overrides
// the client's default catalog for this call, the same way CreateDatabase's
// db.CatalogName does; opts' InCatalog, if passed, takes precedence over
// both (SPEC §5.0).
func (c *Client) AlterDatabase(ctx context.Context, name string, db *Database, opts ...CatalogOption) error {
	return c.call(ctx, "alter_database", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCatFor(ctx, cn, db.CatalogName, opts)
		if err != nil {
			return err
		}
		return cn.alterDatabase(ctx, qualifyDBName(cat, name), databaseToThriftFrom(db, cat))
	})
}

// DropDatabase removes the database named name. deleteData and cascade are
// forwarded to the server; cascade must be true to drop a non-empty
// database, or the server returns ErrInvalidOperation. With ifExists true,
// a missing database is not an error.
//
// Hive 3.1's metastore has been observed to raise a bare
// MetaException(java.lang.NullPointerException) from drop_database when
// the named database does not exist, instead of the NoSuchObjectException
// every other supported version raises (and that classify maps to
// ErrNotFound). When drop_database's error does not already classify as
// ErrNotFound, DropDatabase follows up with get_database on the same
// connection; if that reports the database missing, the original error is
// replaced with that NoSuchObjectException so the ErrNotFound contract
// above holds on Hive 3.1 too. This extra RPC is paid only on the error
// path, never on a successful drop.
func (c *Client) DropDatabase(ctx context.Context, name string, deleteData, cascade, ifExists bool, opts ...CatalogOption) error {
	return c.call(ctx, "drop_database", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
		qname := qualifyDBName(cat, name)
		err = cn.dropDatabase(ctx, qname, deleteData, cascade)
		if err != nil && classify(err) != ErrNotFound {
			if _, gerr := cn.getDatabase(ctx, qname); classify(gerr) == ErrNotFound {
				err = gerr
			}
		}
		if err != nil && ifExists && classify(err) == ErrNotFound {
			return nil
		}
		return err
	})
}
