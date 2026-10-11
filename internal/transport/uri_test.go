package transport_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/hms-client-go/internal/transport"
)

// TestParseEndpoints_ErrorsOmitPassword covers that no ParseEndpoints error
// echoes a URI's userinfo password, since callers commonly log New's error.
func TestParseEndpoints_ErrorsOmitPassword(t *testing.T) {
	t.Parallel()
	const secret = "s3cret-dummy"
	tests := []struct {
		name    string
		in      string
		wantErr string
	}{
		{"unparseable", "http://svc:" + secret + "@host:port/x", "parse endpoint #1"},
		{"bad scheme", "ftp://svc:" + secret + "@host", "unsupported scheme"},
		{"mixed schemes", "http://svc:" + secret + "@a,thrift://b", "mixed schemes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := transport.ParseEndpoints(tt.in)
			require.ErrorContains(t, err, tt.wantErr)
			assert.NotContains(t, err.Error(), secret)
		})
	}
}

func TestEndpointRedacted(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"thrift host", "thrift://hms1", "hms1:9083"},
		{"http without userinfo", "http://hms1:8080", "http://hms1:8080/metastore"},
		{"http with password", "https://svc:pw@hms1/m", "https://svc:xxxxx@hms1/m"},
		{"http user only", "https://svc@hms1/m", "https://svc@hms1/m"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			eps, err := transport.ParseEndpoints(tt.in)
			require.NoError(t, err)
			assert.Equal(t, tt.want, eps[0].Redacted())
		})
	}
}

func TestParseEndpoints(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		in      string
		want    []transport.Endpoint
		wantErr string
	}{
		{"single thrift default port", "thrift://hms1", []transport.Endpoint{{Scheme: "thrift", Host: "hms1:9083"}}, ""},
		{"two thrift", "thrift://hms1:9083, thrift://hms2:9084", []transport.Endpoint{{Scheme: "thrift", Host: "hms1:9083"}, {Scheme: "thrift", Host: "hms2:9084"}}, ""},
		{"http default path", "http://hms1:8080", []transport.Endpoint{{Scheme: "http", Host: "hms1:8080", URL: "http://hms1:8080/metastore"}}, ""},
		{"https custom path", "https://hms1/custom", []transport.Endpoint{{Scheme: "https", Host: "hms1", URL: "https://hms1/custom"}}, ""},
		{"mixed schemes", "thrift://a:9083,http://b", nil, "mixed"},
		{"bad scheme", "ftp://a", nil, "scheme"},
		{"empty", "", nil, "empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := transport.ParseEndpoints(tt.in)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
