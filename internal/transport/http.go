package transport

import (
	"context"
	"crypto/tls"
	"net/http"
	"os/user"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

// HTTPConfig configures a Thrift-over-HTTP connection.
type HTTPConfig struct {
	// Client is the underlying HTTP client. If nil, a fresh *http.Client
	// with Timeout and TLS applied is used in its place. When Client is
	// set, TLS is ignored: the caller's Transport wins, so configure TLS
	// on that client directly.
	Client *http.Client
	// Timeout bounds each request when Client is nil. Zero means no
	// client-side timeout; the call's context deadline still governs.
	Timeout time.Duration
	// TLS configures the TLS client used for "https://" endpoints when
	// Client is nil: it becomes the cloned default Transport's
	// TLSClientConfig. Ignored when Client is set (see Client's doc).
	TLS *tls.Config
	// BearerToken, when non-empty, selects JWT auth: it is sent as
	// "Authorization: Bearer <token>" and User is ignored.
	BearerToken string
	// User is sent as "x-actor-username" when BearerToken is empty. If
	// empty, it defaults to the current OS user, falling back to
	// "hms-client-go" when that cannot be determined.
	User string
	// Headers carries extra headers to send on every request, e.g. for a
	// Knox gateway. Entries here take precedence over the library's
	// defaults (Content-Type, Accept, User-Agent, Authorization,
	// x-actor-username): a colliding key replaces the default rather than
	// being sent alongside it.
	Headers map[string]string
	// UserAgent sets the "User-Agent" header.
	UserAgent string
}

// NewHTTP opens a Thrift-over-HTTP connection to rawURL. The returned
// Conn's Client sends cfg's headers on every call; THttpClient.Flush binds
// each call's context to the underlying HTTP request, so no ContextClient
// wrapper is needed here. When cfg.Client is nil, cfg.TLS configures the
// constructed client's transport for "https://" endpoints (SPEC §3.2); a
// caller-supplied cfg.Client is used as-is, so its own Transport's TLS
// configuration governs instead.
func NewHTTP(_ context.Context, rawURL string, cfg HTTPConfig) (*Conn, error) {
	hc := cfg.Client
	if hc == nil {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.TLSClientConfig = cfg.TLS
		hc = &http.Client{Timeout: cfg.Timeout, Transport: t}
	}
	t, err := thrift.NewTHttpClientWithOptions(rawURL, thrift.THttpClientOptions{Client: hc})
	if err != nil {
		return nil, err
	}
	h, ok := t.(*thrift.THttpClient)
	if !ok {
		return nil, thrift.NewTTransportException(thrift.NOT_IMPLEMENTED, "thrift: unexpected TTransport implementation for HTTP client")
	}
	setHeader(h, "Content-Type", "application/x-thrift")
	setHeader(h, "Accept", "application/x-thrift")
	if cfg.UserAgent != "" {
		setHeader(h, "User-Agent", cfg.UserAgent)
	}
	if cfg.BearerToken != "" {
		setHeader(h, "Authorization", "Bearer "+cfg.BearerToken)
	} else {
		setHeader(h, "x-actor-username", userOrDefault(cfg.User))
	}
	// Caller-supplied headers are applied last so they override the
	// defaults above instead of being appended alongside them.
	for k, v := range cfg.Headers {
		setHeader(h, k, v)
	}
	tcfg := &thrift.TConfiguration{}
	proto := thrift.NewTBinaryProtocolConf(t, tcfg)
	return &Conn{Client: thrift.NewTStandardClient(proto, proto), Close: t.Close}, nil
}

// setHeader replaces any existing values of k on h with the single value v.
// THttpClient.SetHeader appends (http.Header.Add semantics), so without
// this a repeated or colliding key would be sent multiple times on the
// wire; DelHeader first ensures exactly one value is sent.
func setHeader(h *thrift.THttpClient, k, v string) {
	h.DelHeader(k)
	h.SetHeader(k, v)
}

// userOrDefault returns u if non-empty, else the current OS username, else
// the literal fallback "hms-client-go".
func userOrDefault(u string) string {
	if u != "" {
		return u
	}
	if cur, err := user.Current(); err == nil && cur.Username != "" {
		return cur.Username
	}
	return "hms-client-go"
}
