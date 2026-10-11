// Package transport implements the low-level Thrift transports (binary TCP
// and Thrift-over-HTTP) and connection plumbing used by package hms. It has
// no exported dependency on any generated Thrift type.
package transport

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

// Scheme identifies the wire protocol of a metastore endpoint.
type Scheme string

// Supported endpoint schemes.
const (
	SchemeThrift Scheme = "thrift"
	SchemeHTTP   Scheme = "http"
	SchemeHTTPS  Scheme = "https"
)

// Default values applied when an endpoint omits them.
const (
	// DefaultBinaryPort is used for thrift:// endpoints that omit a port.
	DefaultBinaryPort = "9083"
	// DefaultHTTPPath is used for http(s):// endpoints that omit a path.
	DefaultHTTPPath = "/metastore"
)

// Endpoint is a single parsed metastore URI.
type Endpoint struct {
	// Scheme is the endpoint's wire protocol.
	Scheme Scheme
	// Host is host:port for thrift; host[:port] for http(s).
	Host string
	// URL is the full URL; populated for http(s) endpoints only. It may
	// carry userinfo, which net/http sends as Basic auth, so log or report
	// Redacted instead.
	URL string
}

// Redacted returns e's URL with any userinfo password masked, as
// url.URL.Redacted does, or e.Host for an endpoint without a URL. It is
// the form to log or put in an error.
func (e Endpoint) Redacted() string {
	if e.URL == "" {
		return e.Host
	}
	u, err := url.Parse(e.URL)
	if err != nil {
		return e.Host
	}
	return u.Redacted()
}

// ParseEndpoints parses a comma-separated list of metastore URIs. All
// endpoints must share a single scheme. An empty list is an error. Errors
// never echo a URI's userinfo password: a URI that fails to parse is
// identified by its position in the list, a parsed one by its redacted
// form.
func ParseEndpoints(uris string) ([]Endpoint, error) {
	var out []Endpoint
	for i, raw := range strings.Split(uris, ",") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		u, err := url.Parse(raw)
		if err != nil {
			// *url.Error repeats the raw URI; keep only its cause.
			var uerr *url.Error
			if errors.As(err, &uerr) {
				err = uerr.Err
			}
			return nil, fmt.Errorf("hms: parse endpoint #%d: %w", i+1, err)
		}
		ep := Endpoint{Scheme: Scheme(u.Scheme), Host: u.Host}
		switch ep.Scheme {
		case SchemeThrift:
			if _, _, err := net.SplitHostPort(u.Host); err != nil {
				ep.Host = net.JoinHostPort(u.Host, DefaultBinaryPort)
			}
		case SchemeHTTP, SchemeHTTPS:
			if u.Path == "" || u.Path == "/" {
				u.Path = DefaultHTTPPath
			}
			ep.URL = u.String()
		default:
			return nil, fmt.Errorf("hms: unsupported scheme %q in %q", u.Scheme, u.Redacted())
		}
		if len(out) > 0 && out[0].Scheme != ep.Scheme {
			return nil, fmt.Errorf("hms: mixed schemes in endpoint list (%s and %s)", out[0].Scheme, ep.Scheme)
		}
		out = append(out, ep)
	}
	if len(out) == 0 {
		return nil, errors.New("hms: empty endpoint list")
	}
	return out, nil
}
