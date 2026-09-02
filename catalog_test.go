package hms_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hms "github.com/slachiewicz/hms-client-go"
	"github.com/slachiewicz/hms-client-go/internal/hmstest"
)

func TestCatalogs(t *testing.T) {
	t.Parallel()

	t.Run("hive40 round trip", func(t *testing.T) {
		t.Parallel()
		srv := hmstest.Start(t, hmstest.Hive40)
		c := mustNew(t, srv.URI())
		ctx := context.Background()

		require.NoError(t, c.CreateCatalog(ctx, &hms.Catalog{Name: "spark", Description: "d", LocationURI: "s3://b/"}))
		names, err := c.GetCatalogs(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"hive", "spark"}, names)

		got, err := c.GetCatalog(ctx, "spark")
		require.NoError(t, err)
		assert.Equal(t, &hms.Catalog{Name: "spark", Description: "d", LocationURI: "s3://b/"}, got)

		require.ErrorIs(t, c.CreateCatalog(ctx, &hms.Catalog{Name: "spark"}), hms.ErrAlreadyExists)

		require.NoError(t, c.DropCatalog(ctx, "spark", false))
		_, err = c.GetCatalog(ctx, "spark")
		require.ErrorIs(t, err, hms.ErrNotFound)
		require.ErrorIs(t, c.DropCatalog(ctx, "spark", false), hms.ErrNotFound)
		require.NoError(t, c.DropCatalog(ctx, "spark", true))
	})

	t.Run("hive23 not supported", func(t *testing.T) {
		t.Parallel()
		srv := hmstest.Start(t, hmstest.Hive23)
		c := mustNew(t, srv.URI())
		_, err := c.GetCatalogs(context.Background())
		require.ErrorIs(t, err, hms.ErrNotSupported)
	})
}

func TestGetCatalog_NotFound(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI())
	_, err := c.GetCatalog(context.Background(), "nope")
	require.ErrorIs(t, err, hms.ErrNotFound)
}
