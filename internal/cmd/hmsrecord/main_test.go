package main

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hms "github.com/slachiewicz/hms-client-go"
	"github.com/slachiewicz/hms-client-go/hmstest"
)

// TestRecordsCallsThroughProxy runs the real client through the proxy to a
// fake metastore: every call must reach the server unchanged and be
// recorded by name.
func TestRecordsCallsThroughProxy(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive31)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var out bytes.Buffer
	rec := newRecorder(&out)
	go func() { _ = serve(ln, srv.Addr(), rec) }()
	t.Cleanup(func() { _ = ln.Close() })

	ctx := context.Background()
	c, err := hms.New(ctx, "thrift://"+ln.Addr().String(), hms.WithPoolSize(1))
	require.NoError(t, err)
	require.NoError(t, c.CreateDatabase(ctx, &hms.Database{Name: "census", LocationURI: "file:///tmp/census.db"}))
	db, err := c.GetDatabase(ctx, "census")
	require.NoError(t, err)
	assert.Equal(t, "census", db.Name)
	require.NoError(t, c.Close())

	summary := strings.Join(rec.summary(), "\n")
	for _, m := range []string{"set_ugi", "create_database", "get_database"} {
		assert.Contains(t, summary, m)
	}
	assert.Contains(t, out.String(), `"method":"get_database","type":"call"`)
	assert.NotContains(t, out.String(), "not TBinaryProtocol")
}

// TestUnparseableStreamIsStillProxied covers a client that does not speak
// unframed TBinaryProtocol: its bytes still reach the server, and a note
// replaces the calls that could not be recorded.
func TestUnparseableStreamIsStillProxied(t *testing.T) {
	t.Parallel()
	var server bytes.Buffer
	var out bytes.Buffer
	rec := newRecorder(&out)
	// A SASL START frame: status byte 0x01, then a length and mechanism name.
	in := []byte{0x01, 0x00, 0x00, 0x00, 0x05, 'P', 'L', 'A', 'I', 'N', 0xff, 0xfe}
	record(bytes.NewReader(in), &server, "test", rec)
	assert.Equal(t, in, server.Bytes())
	assert.Contains(t, out.String(), "not TBinaryProtocol")
}
