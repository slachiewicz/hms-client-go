package transport_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/hms-client-go/internal/transport"
)

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
