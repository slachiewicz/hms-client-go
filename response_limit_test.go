package hms_test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hms "github.com/slachiewicz/hms-client-go"
	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
	"github.com/slachiewicz/hms-client-go/hmstest"
)

// TestMaxResponseBytes covers WithMaxResponseBytes (SPEC §3.1, §5.1): a
// reply over the cap fails with ErrMeta, and its connection, with the rest
// of that reply still unread, is discarded rather than pooled, so the next
// call on a pool of one gets a fresh connection and a correct answer.
func TestMaxResponseBytes(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	for i := range 100 {
		srv.SeedDatabase(&hive_metastore.Database{Name: fmt.Sprintf("database_%03d", i)})
	}
	var logs syncBuffer
	c := mustNew(t, srv.URI(),
		hms.WithMaxResponseBytes(512),
		hms.WithPoolSize(1),
		hms.WithLogger(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))))
	ctx := context.Background()

	_, err := c.GetAllDatabases(ctx)
	require.ErrorIs(t, err, hms.ErrMeta)
	assert.ErrorContains(t, err, "response exceeds 512 bytes")
	assert.Contains(t, logs.String(), `reason="protocol error"`)

	db, err := c.GetDatabase(ctx, "database_007")
	require.NoError(t, err)
	assert.Equal(t, "database_007", db.Name)
}
