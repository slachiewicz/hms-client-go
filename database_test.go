package hms_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hms "github.com/slachiewicz/hms-client-go"
	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
	"github.com/slachiewicz/hms-client-go/internal/hmstest"
)

func TestDatabases_CreateGetRoundTrip(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		v    hmstest.Version
	}{
		{"hive23", hmstest.Hive23},
		{"hive31", hmstest.Hive31},
		{"hive40", hmstest.Hive40},
	}

	for _, tt := range tests {
		v, name := tt.v, tt.name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := hmstest.Start(t, v)
			c := mustNew(t, srv.URI())
			ctx := context.Background()

			db := &hms.Database{
				Name:        "mydb",
				Description: "d",
				LocationURI: "hdfs:///mydb",
				Parameters:  map[string]string{"k": "v"},
			}
			require.NoError(t, c.CreateDatabase(ctx, db))

			names, err := c.GetAllDatabases(ctx)
			require.NoError(t, err)
			assert.Contains(t, names, "mydb")

			got, err := c.GetDatabase(ctx, "mydb")
			require.NoError(t, err)
			assert.Equal(t, &hms.Database{
				CatalogName: "hive",
				Name:        "mydb",
				Description: "d",
				LocationURI: "hdfs:///mydb",
				Parameters:  map[string]string{"k": "v"},
			}, got)

			args, ok := srv.LastArgs("create_database").(*hive_metastore.Database)
			require.True(t, ok)
			if v == hmstest.Hive23 {
				assert.Nil(t, args.CatalogName)
			} else {
				require.NotNil(t, args.CatalogName)
				assert.Equal(t, "hive", *args.CatalogName)
			}
		})
	}
}

func TestGetDatabase_Hive23NonDefaultCatalogNotSupported(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive23)
	c := mustNew(t, srv.URI())

	_, err := c.GetDatabase(context.Background(), "x", hms.InCatalog("spark"))
	require.ErrorIs(t, err, hms.ErrNotSupported)
	assert.NotContains(t, srv.Calls(), "get_database")
}

func TestGetDatabase_Hive40QualifiesNonDefaultCatalog(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI())
	ctx := context.Background()

	// get_database identifies its catalog purely from a "@<cat>#<name>"
	// prefix on the wire name (see internal/hmstest/handler.go splitCatDB),
	// so seed the fake server's store directly under that key rather than
	// going through CreateDatabase (a Task 8 table-adjacent concern; this
	// test is only about get_database's own qualifier convention).
	srv.Store().Databases["spark.db"] = &hive_metastore.Database{Name: "db"}

	_, err := c.GetDatabase(ctx, "db", hms.InCatalog("spark"))
	require.NoError(t, err)

	args, ok := srv.LastArgs("get_database").(string)
	require.True(t, ok)
	assert.Equal(t, "@spark#db", args)
}

// TestGetAllDatabases_NonDefaultCatalog covers the fix for
// GetAllDatabases silently ignoring a non-default CatalogOption: since
// get_all_databases has no catalog parameter, a non-default catalog must
// instead go through get_databases with a "@<cat>#*" pattern (see
// (*Client).GetAllDatabases), or every catalog's databases would appear to
// live in "hive".
func TestGetAllDatabases_NonDefaultCatalog(t *testing.T) {
	t.Parallel()

	t.Run("hive40 lists the requested catalog only", func(t *testing.T) {
		t.Parallel()
		srv := hmstest.Start(t, hmstest.Hive40)
		c := mustNew(t, srv.URI())
		ctx := context.Background()

		require.NoError(t, c.CreateCatalog(ctx, &hms.Catalog{Name: "spark"}))
		require.NoError(t, c.CreateDatabase(ctx, &hms.Database{Name: "a", CatalogName: "spark"}))

		names, err := c.GetAllDatabases(ctx, hms.InCatalog("spark"))
		require.NoError(t, err)
		assert.Equal(t, []string{"a"}, names)

		args, ok := srv.LastArgs("get_databases").(string)
		require.True(t, ok)
		assert.Equal(t, "@spark#*", args)

		// The default catalog's own databases are unaffected and not
		// mixed into "spark"'s result.
		hiveNames, err := c.GetAllDatabases(ctx)
		require.NoError(t, err)
		assert.NotContains(t, hiveNames, "a")
	})

	t.Run("hive23 non-default catalog is not supported", func(t *testing.T) {
		t.Parallel()
		srv := hmstest.Start(t, hmstest.Hive23)
		c := mustNew(t, srv.URI())

		_, err := c.GetAllDatabases(context.Background(), hms.InCatalog("spark"))
		require.ErrorIs(t, err, hms.ErrNotSupported)
		assert.NotContains(t, srv.Calls(), "get_databases")
		assert.NotContains(t, srv.Calls(), "get_all_databases")
	})
}

func TestDropDatabase(t *testing.T) {
	t.Parallel()

	t.Run("non-empty without cascade is invalid operation", func(t *testing.T) {
		t.Parallel()
		srv := hmstest.Start(t, hmstest.Hive40)
		c := mustNew(t, srv.URI())
		ctx := context.Background()

		require.NoError(t, c.CreateDatabase(ctx, &hms.Database{Name: "db"}))
		// Table operations land in Task 8; seed the fake server's store
		// directly ("<cat>.<db>.<table>", see internal/hmstest/handler.go
		// dbKey/tblKey) to make the database non-empty.
		srv.Store().Tables["hive.db.t"] = &hive_metastore.Table{DbName: "db", TableName: "t"}

		err := c.DropDatabase(ctx, "db", false, false, false)
		require.ErrorIs(t, err, hms.ErrInvalidOperation)
	})

	t.Run("ifExists true on missing database is nil", func(t *testing.T) {
		t.Parallel()
		srv := hmstest.Start(t, hmstest.Hive40)
		c := mustNew(t, srv.URI())

		err := c.DropDatabase(context.Background(), "nope", false, false, true)
		require.NoError(t, err)
	})

	t.Run("ifExists false on missing database is not found", func(t *testing.T) {
		t.Parallel()
		srv := hmstest.Start(t, hmstest.Hive40)
		c := mustNew(t, srv.URI())

		err := c.DropDatabase(context.Background(), "nope", false, false, false)
		require.ErrorIs(t, err, hms.ErrNotFound)
	})
}
