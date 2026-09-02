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
			// CreateTime is read-only, assigned by the server itself (1.0
			// addition; see Database's doc comment); only its presence is
			// asserted here, not a specific value.
			require.False(t, got.CreateTime.IsZero())
			assert.Equal(t, &hms.Database{
				CatalogName: "hive",
				Name:        "mydb",
				Description: "d",
				LocationURI: "hdfs:///mydb",
				Parameters:  map[string]string{"k": "v"},
				CreateTime:  got.CreateTime,
			}, got)

			args, ok := srv.LastArgs("create_database").(*hive_metastore.Database)
			require.True(t, ok)
			if v == hmstest.Hive23 {
				assert.Nil(t, args.CatalogName)
			} else {
				require.NotNil(t, args.CatalogName)
				assert.Equal(t, "hive", *args.CatalogName)
			}

			// An empty LocationURI is filled in client-side from the
			// warehouse root -- the default catalog's own LocationUri on a
			// server that supports catalogs (hive31, hive40), or the
			// "hive.metastore.warehouse.dir" config value on one that
			// predates them (hive23) -- the way Hive's own DDL path does
			// (see (*Client).CreateDatabase), rather than reaching the
			// server as "".
			require.NoError(t, c.CreateDatabase(ctx, &hms.Database{Name: "nolocdb"}))
			got2, err := c.GetDatabase(ctx, "nolocdb")
			require.NoError(t, err)
			assert.Equal(t, "file:///tmp/hms-warehouse/nolocdb.db", got2.LocationURI)
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

		require.NoError(t, c.CreateCatalog(ctx, &hms.Catalog{Name: "spark", LocationURI: "hdfs:///spark-warehouse"}))
		require.NoError(t, c.CreateDatabase(ctx, &hms.Database{Name: "a", CatalogName: "spark"}))

		names, err := c.GetAllDatabases(ctx, hms.InCatalog("spark"))
		require.NoError(t, err)
		assert.Equal(t, []string{"a"}, names)

		// db "a" was created without a LocationURI, so it derives from
		// its catalog's LocationURI ("hive.metastore.warehouse.dir"
		// only applies to the default catalog).
		gotA, err := c.GetDatabase(ctx, "a", hms.InCatalog("spark"))
		require.NoError(t, err)
		assert.Equal(t, "hdfs:///spark-warehouse/a.db", gotA.LocationURI)

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

	// Hive 3.1's metastore raises a bare
	// MetaException(java.lang.NullPointerException) from drop_database on
	// a missing database instead of NoSuchObjectException (see
	// internal/hmstest/handler.go's DropDatabase and (*Client).DropDatabase's
	// doc comment); these two cases prove the client still maps that to
	// ErrNotFound by following up with get_database.
	t.Run("hive31 missing database maps drop_database's NPE to not found", func(t *testing.T) {
		t.Parallel()
		srv := hmstest.Start(t, hmstest.Hive31)
		c := mustNew(t, srv.URI())

		err := c.DropDatabase(context.Background(), "nope", false, false, false)
		require.ErrorIs(t, err, hms.ErrNotFound)
		assert.Contains(t, srv.Calls(), "get_database")
	})

	t.Run("hive31 missing database with ifExists true is nil", func(t *testing.T) {
		t.Parallel()
		srv := hmstest.Start(t, hmstest.Hive31)
		c := mustNew(t, srv.URI())

		err := c.DropDatabase(context.Background(), "nope", false, false, true)
		require.NoError(t, err)
		assert.Contains(t, srv.Calls(), "get_database")
	})

	// The extra get_database RPC is paid only on the error path: a
	// successful drop never triggers it.
	t.Run("hive31 successful drop does not call get_database", func(t *testing.T) {
		t.Parallel()
		srv := hmstest.Start(t, hmstest.Hive31)
		c := mustNew(t, srv.URI())
		ctx := context.Background()

		require.NoError(t, c.CreateDatabase(ctx, &hms.Database{Name: "db"}))
		require.NoError(t, c.DropDatabase(ctx, "db", false, false, false))
		assert.NotContains(t, srv.Calls(), "get_database")
	})
}
