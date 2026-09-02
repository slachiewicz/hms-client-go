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
			}, hms.StripDatabaseRaw(got))

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

// TestCreateDatabase_CachesWarehouseRoot covers the fix caching the
// warehouse root CreateDatabase resolves for an empty LocationURI (SPEC
// §5.3): a second CreateDatabase call on the same Client, also with an
// empty LocationURI, must reuse the first call's resolved root instead of
// issuing get_catalog again.
func TestCreateDatabase_CachesWarehouseRoot(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI())
	ctx := context.Background()

	require.NoError(t, c.CreateDatabase(ctx, &hms.Database{Name: "db1"}))
	require.NoError(t, c.CreateDatabase(ctx, &hms.Database{Name: "db2"}))

	got1, err := c.GetDatabase(ctx, "db1")
	require.NoError(t, err)
	assert.Equal(t, "file:///tmp/hms-warehouse/db1.db", got1.LocationURI)
	got2, err := c.GetDatabase(ctx, "db2")
	require.NoError(t, err)
	assert.Equal(t, "file:///tmp/hms-warehouse/db2.db", got2.LocationURI)

	n := 0
	for _, call := range srv.Calls() {
		if call == "get_catalog" {
			n++
		}
	}
	assert.Equal(t, 1, n, "the warehouse root must be resolved once and cached, not once per CreateDatabase call")
}

// TestAlterDatabase_PreservesUnmodelledFields covers G3/SPEC §5.4's
// round-trip fidelity guarantee for Database (finding 3 of the Task 3
// review): before Database.raw existed, databaseToThrift always built a
// bare hive_metastore.Database{}, so a GetDatabase -> AlterDatabase round
// trip silently reset every field hms.Database has no field for --
// Privileges, Type, ConnectorName, RemoteDbname, ManagedLocationUri --
// exactly what a database Spark or Trino registered with those set would
// suffer on any AlterDatabase call. The fixture's AlterDatabase replaces
// the stored database wholesale (internal/hmstest/handler.go's
// AlterDatabase), so a surviving field can only come from the snapshot.
func TestAlterDatabase_PreservesUnmodelledFields(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI())
	ctx := context.Background()

	catName := "hive"
	connectorName := "mysql_connector"
	remoteDbname := "remote_db"
	managedLocationURI := "hdfs:///managed/db"
	srv.SeedDatabase(&hive_metastore.Database{
		Name:        "db",
		Description: "d",
		LocationUri: "hdfs:///db",
		Parameters:  map[string]string{"k": "v"},
		CatalogName: &catName,
		Privileges: &hive_metastore.PrincipalPrivilegeSet{
			UserPrivileges: map[string][]*hive_metastore.PrivilegeGrantInfo{
				"alice": {{Privilege: "SELECT", Grantor: "bob", GrantorType: hive_metastore.PrincipalType_USER}},
			},
		},
		Type:               hive_metastore.DatabaseTypePtr(hive_metastore.DatabaseType_REMOTE),
		ConnectorName:      &connectorName,
		RemoteDbname:       &remoteDbname,
		ManagedLocationUri: &managedLocationURI,
	})

	got, err := c.GetDatabase(ctx, "db")
	require.NoError(t, err)

	got.Parameters = map[string]string{"k": "v2"}
	require.NoError(t, c.AlterDatabase(ctx, "db", got))

	after, err := c.GetDatabase(ctx, "db")
	require.NoError(t, err)
	stored := hms.DatabaseRaw(after)
	require.NotNil(t, stored)
	assert.Equal(t, "v2", stored.Parameters["k"], "the modelled field this test actually changed must still take effect")
	require.NotNil(t, stored.Privileges, "Privileges must survive: hms.Database has no field for it")
	assert.Equal(t, "bob", stored.Privileges.UserPrivileges["alice"][0].Grantor)
	require.NotNil(t, stored.Type, "Type must survive: hms.Database has no field for it")
	assert.Equal(t, hive_metastore.DatabaseType_REMOTE, *stored.Type)
	require.NotNil(t, stored.ConnectorName, "ConnectorName must survive: hms.Database has no field for it")
	assert.Equal(t, "mysql_connector", *stored.ConnectorName)
	require.NotNil(t, stored.RemoteDbname, "RemoteDbname must survive: hms.Database has no field for it")
	assert.Equal(t, "remote_db", *stored.RemoteDbname)
	require.NotNil(t, stored.ManagedLocationUri, "ManagedLocationUri must survive: hms.Database has no field for it")
	assert.Equal(t, "hdfs:///managed/db", *stored.ManagedLocationUri)
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
