package hms

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
	"github.com/slachiewicz/hms-client-go/internal/hmstest"
)

// TestTableToThrift_DefaultsOwnerTypeAndWriteId covers the fix for
// tableToThrift building a bare hive_metastore.Table{} literal, which left
// the non-pointer "optional with default" fields OwnerType and WriteId at
// their Go zero value (0) instead of the generated NewTable()'s defaults
// (PrincipalType_USER, -1): ownerType=0 is not a valid PrincipalType on the
// wire, and writeId=0 is a real write id rather than "unassigned". The
// exported Table type carries neither field, so every table this client
// builds is, from tableToThrift's point of view, always "unset" for both
// and must always get NewTable's defaults.
func TestTableToThrift_DefaultsOwnerTypeAndWriteId(t *testing.T) {
	t.Parallel()
	got := tableToThrift(&Table{DatabaseName: "d", TableName: "t"}, nil)
	require.NotNil(t, got)
	assert.Equal(t, hive_metastore.PrincipalType_USER, got.OwnerType)
	assert.Equal(t, int64(-1), got.WriteId)
}

// TestGetTable_PreservesNonDefaultOwnerType proves that a real wire round
// trip through the Hive40 fixture does not corrupt a non-default OwnerType
// already on the wire. The exported Table type has no field for OwnerType
// (tableToThrift always sends PrincipalType_USER; see
// TestTableToThrift_DefaultsOwnerTypeAndWriteId above), so this seeds the
// fixture's store directly with a table whose OwnerType is
// PrincipalType_ROLE (bypassing CreateTable/tableToThrift entirely,
// mirroring the direct-seed technique already used by
// TestGetDatabase_Hive40QualifiesNonDefaultCatalog in database_test.go),
// then fetches it back over an actual connection using the unexported
// get_table_req binding directly (this file is a white-box test, in
// package hms, precisely so it can reach that binding and tableToThrift
// without an exported shim) and asserts the value the client actually
// receives off the wire is unchanged.
func TestGetTable_PreservesNonDefaultOwnerType(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	ctx := context.Background()
	c, err := New(ctx, srv.URI())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	srv.Store().Tables["hive.db.t"] = &hive_metastore.Table{
		DbName:    "db",
		TableName: "t",
		OwnerType: hive_metastore.PrincipalType_ROLE,
		WriteId:   -1,
		CatName:   ptr("hive"),
	}

	cn, err := c.acquire(ctx, 0)
	require.NoError(t, err)
	defer c.release(0, cn)

	res, err := cn.getTableReq(ctx, newGetTableRequest("db", "t", ptr("hive")))
	require.NoError(t, err)
	assert.Equal(t, hive_metastore.PrincipalType_ROLE, res.Table.OwnerType)
}

// TestPartitionToThrift_DefaultsWriteId is partitionToThrift's counterpart
// to TestTableToThrift_DefaultsOwnerTypeAndWriteId above: it must also
// build from hive_metastore.NewPartition() rather than a bare struct
// literal, or the non-pointer "optional with default" WriteId field (no
// equivalent on the exported Partition type) would be left at the Go zero
// value 0 instead of NewPartition's default of -1 — writeId=0 is a real
// write id on the wire, not "unassigned".
func TestPartitionToThrift_DefaultsWriteId(t *testing.T) {
	t.Parallel()
	got := partitionToThrift(&Partition{Values: []string{"2024-01-01"}}, nil, "d", "t")
	require.NotNil(t, got)
	assert.Equal(t, int64(-1), got.WriteId)
}
