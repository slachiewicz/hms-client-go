package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math"
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
	// on that client directly. Either way NewHTTP sends through a copy
	// that refuses redirects and caps response bodies (see hardenClient).
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
	// MaxResponseBytes caps the bytes one reply may occupy, as
	// BinaryConfig.MaxResponseBytes does.
	MaxResponseBytes int64
}

// NewHTTP opens a Thrift-over-HTTP connection to rawURL. The returned
// Conn's Client sends cfg's headers on every call; THttpClient.Flush binds
// each call's context to the underlying HTTP request, so no ContextClient
// wrapper is needed here. When cfg.Client is nil, cfg.TLS configures the
// constructed client's transport for "https://" endpoints (SPEC §3.2); a
// caller-supplied cfg.Client keeps its own Transport's TLS configuration.
func NewHTTP(_ context.Context, rawURL string, cfg HTTPConfig) (*Conn, error) {
	hc := hardenClient(cfg)
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
	tcfg := &thrift.TConfiguration{MaxMessageSize: maxMessageSize(cfg.MaxResponseBytes)}
	proto := thrift.NewTBinaryProtocolConf(newBudgetTransport(t, cfg.MaxResponseBytes), tcfg)
	return &Conn{Client: thrift.NewTStandardClient(proto, proto), Close: t.Close}, nil
}

// hardenClient returns the *http.Client NewHTTP sends through: cfg.Client
// when set, else one built from cfg.Timeout and cfg.TLS. Either way it is a
// shallow copy whose CheckRedirect refuses redirects (unless the caller's
// client sets its own policy) and whose response bodies are capped, so
// neither the caller's client nor http.DefaultClient is modified.
//
// A metastore never redirects a Thrift call. Following one would replay
// the request, with Authorization, x-actor-username and every WithHTTPHeaders
// header, to wherever the redirect points; Go keeps Authorization on a
// same-host https-to-http redirect and forwards custom headers to any host.
//
// THttpClient drains each response body to completion before reusing the
// connection, with no limit of its own, so a reply that never ends would
// hold the next call forever when no deadline applies. The cap stops the
// drain at cfg.MaxResponseBytes; the connection is then closed rather than
// reused.
func hardenClient(cfg HTTPConfig) *http.Client {
	var hc http.Client
	if cfg.Client != nil {
		hc = *cfg.Client
	} else {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.TLSClientConfig = cfg.TLS
		hc = http.Client{Timeout: cfg.Timeout, Transport: t}
	}
	if hc.CheckRedirect == nil {
		hc.CheckRedirect = refuseRedirect
	}
	rt := hc.Transport
	if rt == nil {
		rt = http.DefaultTransport
	}
	hc.Transport = capBody{rt: rt, limit: responseCap(cfg.MaxResponseBytes)}
	return &hc
}

// errRedirect is returned, wrapped in a *url.Error, when a metastore
// answers with a redirect.
var errRedirect = errors.New("hms: the metastore answered with a redirect, which is refused")

func refuseRedirect(req *http.Request, _ []*http.Request) error {
	u := *req.URL
	return fmt.Errorf("%w (to %s)", errRedirect, u.Redacted())
}

// responseCap is the byte cap capBody applies: the reply budget, plus one so
// the budget transport, not a silent EOF, reports a reply that exceeds it.
func responseCap(limit int64) int64 {
	if limit <= 0 {
		limit = DefaultMaxResponseBytes
	}
	if limit == math.MaxInt64 {
		return limit
	}
	return limit + 1
}

// capBody wraps a RoundTripper so every response body yields at most limit
// bytes.
type capBody struct {
	rt    http.RoundTripper
	limit int64
}

func (c capBody) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := c.rt.RoundTrip(req)
	if err != nil || resp.Body == nil {
		return resp, err
	}
	resp.Body = cappedBody{Reader: io.LimitReader(resp.Body, c.limit), Closer: resp.Body}
	return resp, nil
}

type cappedBody struct {
	io.Reader
	io.Closer
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
