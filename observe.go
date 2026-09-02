package hms

import (
	"log/slog"
	"time"

	"github.com/slachiewicz/hms-client-go/internal/transport"
)

// RPCInfo describes one attempt of one RPC, passed to the function
// registered with WithRPCObserver (SPEC §5.10).
type RPCInfo struct {
	// Method is the RPC's Thrift wire name, the op string passed to
	// Client.call / Client.read (e.g. "get_database", "create_table"),
	// not the exported Go method name.
	Method string
	// Endpoint is the endpoint this attempt was made against: host:port
	// for the binary Thrift transport, the full URL for Thrift-over-HTTP.
	Endpoint string
	// Attempt is the 1-based number of this attempt within the RPC's
	// retry loop (do): a call retried across endpoints invokes the
	// observer once per attempt, so Attempt climbs across those calls
	// rather than resetting.
	Attempt int
	// Duration is how long this attempt took, from just before the RPC
	// was issued on the wire to just after it returned.
	Duration time.Duration
	// Err is the error classify would map for this attempt -- the raw
	// error the RPC returned, before wrapError adds "<op>: " context --
	// or nil on success.
	Err error
}

// WithLogger sets the *slog.Logger used to log connection lifecycle (dial,
// release, discard), failover (an endpoint marked failed or healthy), and
// recovery-probe events, at slog.LevelDebug or slog.LevelInfo (SPEC §5.10);
// it never logs RPC payloads or credentials. A nil l -- or never calling
// WithLogger at all -- discards all output via
// slog.New(slog.DiscardHandler).
func WithLogger(l *slog.Logger) Option {
	return func(c *config) {
		if l == nil {
			l = slog.New(slog.DiscardHandler)
		}
		c.logger = l
	}
}

// WithRPCObserver sets f, called once per attempt of every RPC (so a
// retried call invokes it more than once), synchronously, immediately after
// that attempt completes (SPEC §5.10). f must not block or call back into
// the Client: it runs inline on the goroutine that issued the RPC, so doing
// either would stall that call (and, transitively, anything sharing the
// same connection pool). A panic escaping f is recovered and logged at
// slog.LevelError through the configured logger (see WithLogger) rather
// than propagated to the RPC's caller.
func WithRPCObserver(f func(RPCInfo)) Option {
	return func(c *config) { c.observer = f }
}

// endpointURI returns ep's URI as reported in RPCInfo.Endpoint and in log
// records: the full URL for the Thrift-over-HTTP transport, ep.Host
// (host:port) for the binary Thrift transport.
func endpointURI(ep transport.Endpoint) string {
	if ep.URL != "" {
		return ep.URL
	}
	return ep.Host
}
