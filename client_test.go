package hms_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hms "github.com/slachiewicz/hms-client-go"
	"github.com/slachiewicz/hms-client-go/internal/hmstest"
)

func TestNew_ConnectsAndReadsVersion(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c, err := hms.New(context.Background(), srv.URI())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	v, err := c.ServerVersion(context.Background())
	require.NoError(t, err)
	assert.Equal(t, hms.HiveVersion{Major: 4, Minor: 0, Patch: 1, Raw: "4.0.1"}, v)
}

func TestNew_RefusedEndpoint(t *testing.T) {
	t.Parallel()
	_, err := hms.New(context.Background(), "thrift://127.0.0.1:1")
	require.ErrorIs(t, err, hms.ErrUnavailable)
}

func TestNew_BadURI(t *testing.T) {
	t.Parallel()
	_, err := hms.New(context.Background(), "ftp://x")
	require.Error(t, err)
}

func TestClient_Close(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c, err := hms.New(context.Background(), srv.URI())
	require.NoError(t, err)

	require.NoError(t, c.Close())
	// Close is idempotent.
	require.NoError(t, c.Close())

	_, err = c.GetConfigValue(context.Background(), "x", "y")
	require.ErrorIs(t, err, hms.ErrUnavailable)
}

func TestClient_GetConfigValue(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI())

	v, err := c.GetConfigValue(context.Background(), "does.not.exist", "fallback")
	require.NoError(t, err)
	assert.Equal(t, "fallback", v)
}

func TestServerVersion_Hive23FallsBackToHiveMetastoreVersion(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive23)
	c := mustNew(t, srv.URI())

	v, err := c.ServerVersion(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, v.Major)
	assert.Equal(t, 3, v.Minor)
}

func TestServerVersion_NeitherConfigValueSet(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI())

	// Clear the config values Start seeded, simulating a server that
	// reports neither.
	srv.Store().Config["hive.metastore.version"] = ""
	srv.Store().Config["metastore.version"] = ""

	_, err := c.ServerVersion(context.Background())
	require.ErrorIs(t, err, hms.ErrNotSupported)
}

func TestParseHiveVersion(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		in      string
		want    hms.HiveVersion
		wantErr bool
	}{
		{"simple", "4.0.1", hms.HiveVersion{Major: 4, Minor: 0, Patch: 1, Raw: "4.0.1"}, false},
		{"vendor build", "3.1.3000.7.1.7.0-551", hms.HiveVersion{Major: 3, Minor: 1, Patch: 3000, Raw: "3.1.3000.7.1.7.0-551"}, false},
		{"garbage", "not-a-version", hms.HiveVersion{}, true},
		{"too short", "4.0", hms.HiveVersion{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := hms.ParseHiveVersion(tt.in)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHiveVersion_String(t *testing.T) {
	t.Parallel()
	v, err := hms.ParseHiveVersion("4.0.1")
	require.NoError(t, err)
	assert.Equal(t, "4.0.1", v.String())
}
