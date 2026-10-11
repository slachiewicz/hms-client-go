package transport_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/slachiewicz/hms-client-go/gen/fb303"
	"github.com/slachiewicz/hms-client-go/internal/transport"
)

// fbStatusHandler implements fb303.FacebookService by embedding the
// interface and overriding only the method under test.
type fbStatusHandler struct{ fb303.FacebookService }

func (fbStatusHandler) GetStatus(_ context.Context) (fb303.FbStatus, error) {
	return fb303.FbStatus_ALIVE, nil
}

// recordedRequest captures the header and path of a single incoming request
// under a mutex, since the HTTP server handles the request on a different
// goroutine than the test that later inspects it.
type recordedRequest struct {
	mu     sync.Mutex
	header http.Header
	path   string
}

func (r *recordedRequest) record(h http.Header, path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.header = h
	r.path = path
}

func (r *recordedRequest) get() (http.Header, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.header, r.path
}

// fb303HTTPHandler records the incoming request and serves a canned fb303
// getStatus reply over the request body / response writer.
func fb303HTTPHandler(rec *recordedRequest) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rec.record(r.Header.Clone(), r.URL.Path)
		w.Header().Set("Content-Type", "application/x-thrift")
		trans := thrift.NewStreamTransport(r.Body, w)
		proto := thrift.NewTBinaryProtocolConf(trans, nil)
		proc := fb303.NewFacebookServiceProcessor(fbStatusHandler{})
		_, _ = proc.Process(r.Context(), proto, proto)
	}
}

// startFB303HTTPServer starts an httptest server that records the request
// it receives in rec and replies with a canned fb303 getStatus response.
func startFB303HTTPServer(t *testing.T, rec *recordedRequest) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(fb303HTTPHandler(rec))
	t.Cleanup(srv.Close)
	return srv
}

// callGetStatus dials srv via transport.NewHTTP with cfg and issues a
// getStatus RPC, returning the RPC error (if any).
func callGetStatus(ctx context.Context, t *testing.T, rawURL string, cfg transport.HTTPConfig) error {
	t.Helper()
	conn, err := transport.NewHTTP(ctx, rawURL, cfg)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	_, err = fb303.NewFacebookServiceClient(conn.Client).GetStatus(ctx)
	return err
}

func TestNewHTTP_DefaultPath(t *testing.T) {
	t.Parallel()
	rec := &recordedRequest{}
	srv := startFB303HTTPServer(t, rec)

	eps, err := transport.ParseEndpoints(srv.URL)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = callGetStatus(ctx, t, eps[0].URL, transport.HTTPConfig{UserAgent: "test-agent"})
	require.NoError(t, err)

	_, path := rec.get()
	assert.Equal(t, transport.DefaultHTTPPath, path)
}

func TestNewHTTP_Headers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		cfg   transport.HTTPConfig
		check func(t *testing.T, h http.Header)
	}{
		{
			name: "content type and accept",
			cfg:  transport.HTTPConfig{UserAgent: "test-agent"},
			check: func(t *testing.T, h http.Header) {
				t.Helper()
				assert.Equal(t, "application/x-thrift", h.Get("Content-Type"))
				assert.Equal(t, "application/x-thrift", h.Get("Accept"))
			},
		},
		{
			name: "Content-Type is sent exactly once",
			cfg:  transport.HTTPConfig{UserAgent: "test-agent"},
			check: func(t *testing.T, h http.Header) {
				t.Helper()
				assert.Len(t, h.Values("Content-Type"), 1)
			},
		},
		{
			name: "caller header overrides a default",
			cfg:  transport.HTTPConfig{UserAgent: "test-agent", Headers: map[string]string{"Accept": "text/plain"}},
			check: func(t *testing.T, h http.Header) {
				t.Helper()
				assert.Equal(t, []string{"text/plain"}, h.Values("Accept"))
			},
		},
		{
			name: "caller header overrides x-actor-username derived from User",
			cfg:  transport.HTTPConfig{UserAgent: "test-agent", User: "alice", Headers: map[string]string{"x-actor-username": "svc"}},
			check: func(t *testing.T, h http.Header) {
				t.Helper()
				assert.Equal(t, []string{"svc"}, h.Values("x-actor-username"))
			},
		},
		{
			name: "user agent set from config",
			cfg:  transport.HTTPConfig{UserAgent: "hms-client-go/test"},
			check: func(t *testing.T, h http.Header) {
				t.Helper()
				assert.Equal(t, "hms-client-go/test", h.Get("User-Agent"))
			},
		},
		{
			name: "bearer token sets Authorization and omits actor username",
			cfg:  transport.HTTPConfig{UserAgent: "test-agent", BearerToken: "tok"},
			check: func(t *testing.T, h http.Header) {
				t.Helper()
				assert.Equal(t, "Bearer tok", h.Get("Authorization"))
				assert.Empty(t, h.Get("x-actor-username"))
			},
		},
		{
			name: "user sets actor username and omits Authorization",
			cfg:  transport.HTTPConfig{UserAgent: "test-agent", User: "alice"},
			check: func(t *testing.T, h http.Header) {
				t.Helper()
				assert.Equal(t, "alice", h.Get("x-actor-username"))
				assert.Empty(t, h.Get("Authorization"))
			},
		},
		{
			name: "extra headers are forwarded",
			cfg:  transport.HTTPConfig{UserAgent: "test-agent", Headers: map[string]string{"X-Knox": "1"}},
			check: func(t *testing.T, h http.Header) {
				t.Helper()
				assert.Equal(t, "1", h.Get("X-Knox"))
			},
		},
		{
			name: "defaults to OS user when User is unset",
			cfg:  transport.HTTPConfig{UserAgent: "test-agent"},
			check: func(t *testing.T, h http.Header) {
				t.Helper()
				assert.NotEmpty(t, h.Get("x-actor-username"))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := &recordedRequest{}
			srv := startFB303HTTPServer(t, rec)

			eps, err := transport.ParseEndpoints(srv.URL)
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err = callGetStatus(ctx, t, eps[0].URL, tc.cfg)
			require.NoError(t, err)

			h, _ := rec.get()
			tc.check(t, h)
		})
	}
}

func TestNewHTTP_ContextTimeout(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	eps, err := transport.ParseEndpoints(srv.URL)
	require.NoError(t, err)

	conn, err := transport.NewHTTP(context.Background(), eps[0].URL, transport.HTTPConfig{UserAgent: "test-agent"})
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, callErr := fb303.NewFacebookServiceClient(conn.Client).GetStatus(ctx)
		errCh <- callErr
	}()

	select {
	case callErr := <-errCh:
		assert.Error(t, callErr)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for GetStatus to return after context deadline")
	}
}

// TestNewHTTP_TLSRoundTrip proves HTTPConfig.TLS's RootCAs is applied to
// the default client's cloned Transport, so a request to an
// httptest.NewTLSServer (whose certificate is not in the system trust
// store) succeeds when it is trusted and only then.
func TestNewHTTP_TLSRoundTrip(t *testing.T) {
	t.Parallel()
	rec := &recordedRequest{}
	srv := httptest.NewTLSServer(fb303HTTPHandler(rec))
	t.Cleanup(srv.Close)

	eps, err := transport.ParseEndpoints(srv.URL)
	require.NoError(t, err)

	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = callGetStatus(ctx, t, eps[0].URL, transport.HTTPConfig{UserAgent: "test-agent", TLS: &tls.Config{RootCAs: pool}})
	require.NoError(t, err)
}

// TestNewHTTP_TLSUntrustedCertFails proves that without the server's
// certificate in RootCAs, the request fails instead of silently trusting
// an unverified server.
func TestNewHTTP_TLSUntrustedCertFails(t *testing.T) {
	t.Parallel()
	rec := &recordedRequest{}
	srv := httptest.NewTLSServer(fb303HTTPHandler(rec))
	t.Cleanup(srv.Close)

	eps, err := transport.ParseEndpoints(srv.URL)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = callGetStatus(ctx, t, eps[0].URL, transport.HTTPConfig{UserAgent: "test-agent"})
	require.Error(t, err)
}

// TestNewHTTP_TLSIgnoredWhenClientSupplied proves a caller-supplied Client
// wins over HTTPConfig.TLS: TLS is ignored, so the connection uses
// whatever Transport the caller's Client already carries (here, the
// httptest server's own, which already trusts its certificate).
func TestNewHTTP_TLSIgnoredWhenClientSupplied(t *testing.T) {
	t.Parallel()
	rec := &recordedRequest{}
	srv := httptest.NewTLSServer(fb303HTTPHandler(rec))
	t.Cleanup(srv.Close)

	eps, err := transport.ParseEndpoints(srv.URL)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = callGetStatus(ctx, t, eps[0].URL, transport.HTTPConfig{
		UserAgent: "test-agent",
		Client:    srv.Client(),
		TLS:       &tls.Config{RootCAs: x509.NewCertPool()}, // would reject the server's cert if it were honored
	})
	require.NoError(t, err)
}

// TestNewHTTP_Redirects proves a metastore redirect is refused before
// anything reaches its target, whether NewHTTP builds the client or the
// caller supplies one without a policy of its own, and that a caller's own
// CheckRedirect is respected.
func TestNewHTTP_Redirects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		client     func(target *httptest.Server) *http.Client
		wantErr    bool
		wantTarget int
	}{
		{name: "built client", client: func(*httptest.Server) *http.Client { return nil }, wantErr: true},
		{name: "caller client without a policy", client: func(*httptest.Server) *http.Client { return &http.Client{} }, wantErr: true},
		{
			name: "caller client with its own policy",
			client: func(*httptest.Server) *http.Client {
				return &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return nil }}
			},
			wantTarget: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			hits := 0
			rec := &recordedRequest{}
			fb := fb303HTTPHandler(rec)
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				hits++
				mu.Unlock()
				fb(w, r)
			}))
			t.Cleanup(target.Close)
			redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, target.URL+transport.DefaultHTTPPath, http.StatusTemporaryRedirect)
			}))
			t.Cleanup(redirector.Close)

			client := tt.client(target)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := callGetStatus(ctx, t, redirector.URL+transport.DefaultHTTPPath, transport.HTTPConfig{
				Client:      client,
				BearerToken: "secret",
			})
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "redirect")
			} else {
				require.NoError(t, err)
			}
			mu.Lock()
			assert.Equal(t, tt.wantTarget, hits)
			mu.Unlock()
			if client != nil && tt.wantErr {
				assert.Nil(t, client.CheckRedirect, "the caller's client must not be modified")
			}
		})
	}
}

// TestNewHTTP_EndlessErrorBody proves a non-200 reply whose body never ends
// does not hang the call when no deadline applies: THttpClient drains the
// body before returning, and the drain stops at MaxResponseBytes.
func TestNewHTTP_EndlessErrorBody(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		chunk := make([]byte, 4096)
		for r.Context().Err() == nil {
			if _, err := w.Write(chunk); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)

	done := make(chan error, 1)
	go func() {
		done <- callGetStatus(context.Background(), t, srv.URL+transport.DefaultHTTPPath,
			transport.HTTPConfig{MaxResponseBytes: 64 << 10})
	}()
	select {
	case err := <-done:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "500")
	case <-time.After(10 * time.Second):
		t.Fatal("the call did not return: the error body was drained without a limit")
	}
}
