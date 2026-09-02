package hms_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	hms "github.com/slachiewicz/hms-client-go"
)

// mustNew connects to uri, failing the test on error, and registers
// t.Cleanup to close the client.
func mustNew(t *testing.T, uri string, opts ...hms.Option) *hms.Client {
	t.Helper()
	c, err := hms.New(context.Background(), uri, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}
