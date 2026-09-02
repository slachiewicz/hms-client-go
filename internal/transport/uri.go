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
	// URL is the full URL; populated for http(s) endpoints only.
	URL string
}

// ParseEndpoints parses a comma-separated list of metastore URIs. All
// endpoints must share a single scheme. An empty list is an error.
func ParseEndpoints(uris string) ([]Endpoint, error) {
	var out []Endpoint
	for _, raw := range strings.Split(uris, ",") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		u, err := url.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("hms: parse endpoint %q: %w", raw, err)
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
			return nil, fmt.Errorf("hms: unsupported scheme %q in %q", u.Scheme, raw)
		}
		if len(out) > 0 && out[0].Scheme != ep.Scheme {
			return nil, fmt.Errorf("hms: mixed schemes in endpoint list %q", uris)
		}
		out = append(out, ep)
	}
	if len(out) == 0 {
		return nil, errors.New("hms: empty endpoint list")
	}
	return out, nil
}
