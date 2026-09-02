package hms

import (
	"net/http"
	"time"
)

// Default configuration values applied by New before any Option runs.
const (
	defaultCatalog       = "hive"
	defaultTimeout       = 30 * time.Second
	defaultMaxRetries    = 3
	defaultPoolSize      = 4
	defaultProbeInterval = 30 * time.Second
	defaultChunkSize     = 1000
	// minProbeInterval is the floor clamp enforces on probeInterval: a
	// zero or negative value would panic inside time.NewTicker
	// (recoveryProbe) instead of merely misbehaving, unlike poolSize or
	// maxRetries's clamps below.
	minProbeInterval = time.Millisecond
)

// config accumulates the effect of every Option passed to New.
type config struct {
	catalog       string
	timeout       time.Duration
	maxRetries    int
	randomOrder   bool
	poolSize      int
	probeInterval time.Duration
	chunkSize     int

	httpClient  *http.Client
	httpHeaders map[string]string
	bearerToken string
	user        string

	plainUser     string
	plainPassword string
}

// newConfig returns a config seeded with the library defaults.
func newConfig() *config {
	return &config{
		catalog:       defaultCatalog,
		timeout:       defaultTimeout,
		maxRetries:    defaultMaxRetries,
		poolSize:      defaultPoolSize,
		probeInterval: defaultProbeInterval,
		chunkSize:     defaultChunkSize,
	}
}

// clamp enforces every config field's usable minimum after every Option
// has run, so a caller-supplied value that would otherwise misbehave (a
// zero or negative WithPoolSize hangs New/acquire forever; a zero or
// negative WithMaxRetries would let the retry loop skip the RPC entirely;
// a zero or negative probeInterval would panic inside time.NewTicker)
// instead degrades to the smallest value that still works. See
// WithPoolSize, WithMaxRetries, and withProbeInterval.
func (cfg *config) clamp() {
	if cfg.poolSize < 1 {
		cfg.poolSize = 1
	}
	if cfg.maxRetries < 1 {
		cfg.maxRetries = 1
	}
	if cfg.probeInterval < minProbeInterval {
		cfg.probeInterval = minProbeInterval
	}
	if cfg.chunkSize < 1 {
		cfg.chunkSize = 1
	}
}

// Option configures a Client constructed by New.
type Option func(*config)

// WithCatalog sets the default catalog used by every call that does not
// override it with a CatalogOption. The default is "hive".
func WithCatalog(name string) Option {
	return func(c *config) { c.catalog = name }
}

// WithTimeout sets the socket / per-request timeout applied when a call's
// context carries no deadline of its own. The default is 30 seconds.
func WithTimeout(d time.Duration) Option {
	return func(c *config) { c.timeout = d }
}

// WithMaxRetries sets the maximum number of attempts per RPC across
// endpoints. The default is 3. A value below 1 is clamped to 1, so an RPC
// is always attempted at least once.
func WithMaxRetries(n int) Option {
	return func(c *config) { c.maxRetries = n }
}

// WithRandomEndpointOrder randomizes the order in which endpoints are tried,
// instead of the default list order.
func WithRandomEndpointOrder() Option {
	return func(c *config) { c.randomOrder = true }
}

// WithPoolSize sets the maximum number of pooled connections per endpoint.
// The default is 4. A value below 1 is clamped to 1, since a pool with no
// room for a connection would block every call forever.
func WithPoolSize(n int) Option {
	return func(c *config) { c.poolSize = n }
}

// withProbeInterval sets the interval between recovery-probe sweeps of the
// cluster's cooling endpoints (see Client's recoveryProbe). It is
// unexported: the default of 30s (SPEC §4.2 point 4) is not meant to be
// caller-tunable, but export_test.go exposes it as WithProbeIntervalForTest
// so ha_test.go can bound its waits.
func withProbeInterval(d time.Duration) Option {
	return func(c *config) { c.probeInterval = d }
}

// withChunkSize sets the per-request chunk size used by GetTables and
// AddPartitions (SPEC §5.4, §2.3 Rule 5). The default is 1000. A value
// below 1 is clamped to 1. test hook; not part of the public API:
// export_test.go exposes it as WithChunkSize so tests can exercise
// chunking without needing thousands of fixture rows.
func withChunkSize(n int) Option {
	return func(c *config) { c.chunkSize = n }
}

// WithHTTPClient sets the *http.Client used for the "http://" and "https://"
// transports. If unset, a default client is constructed.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *config) { c.httpClient = hc }
}

// WithHTTPHeaders sets additional headers sent on every HTTP request, for
// example to satisfy a Knox or other reverse proxy. These take precedence
// over the library's own default headers.
func WithHTTPHeaders(h map[string]string) Option {
	return func(c *config) { c.httpHeaders = h }
}

// WithBearerToken selects HTTP JWT authentication, sending the token as an
// "Authorization: Bearer <token>" header.
func WithBearerToken(token string) Option {
	return func(c *config) { c.bearerToken = token }
}

// WithUser sets the principal name sent as "x-actor-username" over HTTP, or
// as the SASL PLAIN user over binary TCP when WithPlainAuth is not used.
func WithUser(name string) Option {
	return func(c *config) { c.user = name }
}

// WithPlainAuth selects SASL PLAIN authentication over the binary TCP
// transport.
func WithPlainAuth(user, password string) Option {
	return func(c *config) {
		c.plainUser = user
		c.plainPassword = password
	}
}

// catalogOpts accumulates the effect of every CatalogOption passed to a
// per-call method.
type catalogOpts struct {
	catalog string
}

// CatalogOption overrides the catalog used by a single call.
type CatalogOption func(*catalogOpts)

// InCatalog overrides the client's default catalog for a single call.
func InCatalog(name string) CatalogOption {
	return func(o *catalogOpts) { o.catalog = name }
}
