package transport

import (
	"context"
	"net/http"
	"os/user"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

// HTTPConfig configures a Thrift-over-HTTP connection.
type HTTPConfig struct {
	// Client is the underlying HTTP client. If nil, a client with Timeout
	// applied is used in its place.
	Client *http.Client
	// Timeout bounds each request when Client is nil.
	Timeout time.Duration
	// BearerToken, when non-empty, selects JWT auth: it is sent as
	// "Authorization: Bearer <token>" and User is ignored.
	BearerToken string
	// User is sent as "x-actor-username" when BearerToken is empty. If
	// empty, it defaults to the current OS user, falling back to
	// "hms-client-go" when that cannot be determined.
	User string
	// Headers carries extra headers to send on every request, e.g. for a
	// Knox gateway.
	Headers map[string]string
	// UserAgent sets the "User-Agent" header.
	UserAgent string
}

// NewHTTP opens a Thrift-over-HTTP connection to rawURL. The returned
// Conn's Client sends cfg's headers on every call; THttpClient.Flush binds
// each call's context to the underlying HTTP request, so no ContextClient
// wrapper is needed here.
func NewHTTP(_ context.Context, rawURL string, cfg HTTPConfig) (*Conn, error) {
	hc := cfg.Client
	if hc == nil {
		hc = &http.Client{Timeout: cfg.Timeout}
	}
	t, err := thrift.NewTHttpClientWithOptions(rawURL, thrift.THttpClientOptions{Client: hc})
	if err != nil {
		return nil, err
	}
	h, ok := t.(*thrift.THttpClient)
	if !ok {
		return nil, thrift.NewTTransportException(thrift.NOT_IMPLEMENTED, "thrift: unexpected TTransport implementation for HTTP client")
	}
	h.SetHeader("Content-Type", "application/x-thrift")
	h.SetHeader("Accept", "application/x-thrift")
	if cfg.UserAgent != "" {
		h.SetHeader("User-Agent", cfg.UserAgent)
	}
	if cfg.BearerToken != "" {
		h.SetHeader("Authorization", "Bearer "+cfg.BearerToken)
	} else {
		h.SetHeader("x-actor-username", userOrDefault(cfg.User))
	}
	for k, v := range cfg.Headers {
		h.SetHeader(k, v)
	}
	tcfg := &thrift.TConfiguration{}
	proto := thrift.NewTBinaryProtocolConf(t, tcfg)
	return &Conn{Client: thrift.NewTStandardClient(proto, proto), Close: t.Close}, nil
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
