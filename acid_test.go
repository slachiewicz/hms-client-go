package hms_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hms "github.com/slachiewicz/hms-client-go"
	"github.com/slachiewicz/hms-client-go/internal/hmstest"
)

// acidVersions is the version table every ACID test runs against,
// matching notification_test.go's notificationVersions: SPEC §5.9 promises
// open_txns/commit_txn/abort_txn/heartbeat/lock/check_lock/unlock on every
// supported version (Hive 2.3+), so nothing here is version-gated.
var acidVersions = []struct {
	name string
	v    hmstest.Version
}{
	{"hive23", hmstest.Hive23},
	{"hive31", hmstest.Hive31},
	{"hive40", hmstest.Hive40},
}

// TestACID_Lifecycle covers the full open -> lock -> check -> unlock ->
// commit path (SPEC §5.9) on every supported version.
func TestACID_Lifecycle(t *testing.T) {
	t.Parallel()
	for _, tt := range acidVersions {
		v, name := tt.v, tt.name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := hmstest.Start(t, v)
			c := mustNew(t, srv.URI())
			ctx := context.Background()

			txnID, err := c.OpenTransaction(ctx, "alice", "host1")
			require.NoError(t, err)
			assert.Greater(t, txnID, int64(0))

			resp, err := c.Lock(ctx, hms.LockRequest{
				Components: []hms.LockComponent{
					{Type: hms.LockTypeSharedRead, Level: hms.LockLevelTable, Database: "db", Table: "t"},
				},
				TxnID: txnID,
				User:  "alice",
				Host:  "host1",
			})
			require.NoError(t, err)
			assert.Equal(t, hms.LockStateAcquired, resp.State)
			require.Greater(t, resp.LockID, int64(0))

			checked, err := c.CheckLock(ctx, resp.LockID)
			require.NoError(t, err)
			assert.Equal(t, hms.LockStateAcquired, checked.State)
			assert.Equal(t, resp.LockID, checked.LockID)

			require.NoError(t, c.Unlock(ctx, resp.LockID))

			_, err = c.CheckLock(ctx, resp.LockID)
			require.ErrorIs(t, err, hms.ErrNotFound, "a released lock must no longer be found")

			require.NoError(t, c.CommitTransaction(ctx, txnID))

			// A committed transaction is fully finalized: neither a
			// second commit nor an abort finds it anymore.
			require.ErrorIs(t, c.CommitTransaction(ctx, txnID), hms.ErrNotFound)
			require.ErrorIs(t, c.AbortTransaction(ctx, txnID), hms.ErrNotFound)
		})
	}
}

// TestACID_LockConflict_Waiting covers SPEC §5.9's WAITING contract: a
// second lock request against a resource an EXCLUSIVE lock already holds
// comes back LockStateWaiting rather than ACQUIRED or an error.
func TestACID_LockConflict_Waiting(t *testing.T) {
	t.Parallel()
	for _, tt := range acidVersions {
		v, name := tt.v, tt.name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := hmstest.Start(t, v)
			c := mustNew(t, srv.URI())
			ctx := context.Background()

			first, err := c.Lock(ctx, hms.LockRequest{
				Components: []hms.LockComponent{
					{Type: hms.LockTypeExclusive, Level: hms.LockLevelTable, Database: "db", Table: "t"},
				},
				User: "alice",
				Host: "host1",
			})
			require.NoError(t, err)
			require.Equal(t, hms.LockStateAcquired, first.State)

			second, err := c.Lock(ctx, hms.LockRequest{
				Components: []hms.LockComponent{
					{Type: hms.LockTypeSharedRead, Level: hms.LockLevelTable, Database: "db", Table: "t"},
				},
				User: "bob",
				Host: "host2",
			})
			require.NoError(t, err)
			assert.Equal(t, hms.LockStateWaiting, second.State)
			assert.NotEqual(t, first.LockID, second.LockID)

			// A lock on an unrelated table is unaffected by the
			// EXCLUSIVE lock held on db.t.
			third, err := c.Lock(ctx, hms.LockRequest{
				Components: []hms.LockComponent{
					{Type: hms.LockTypeSharedRead, Level: hms.LockLevelTable, Database: "db", Table: "other"},
				},
				User: "carol",
				Host: "host3",
			})
			require.NoError(t, err)
			assert.Equal(t, hms.LockStateAcquired, third.State)
		})
	}
}

// TestACID_UnknownTransactionAndLock_ErrNotFound covers SPEC §7's
// NoSuchTxnException/NoSuchLockException -> hms.ErrNotFound mapping for
// every RPC that takes a txn or lock id.
func TestACID_UnknownTransactionAndLock_ErrNotFound(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI())
	ctx := context.Background()

	require.ErrorIs(t, c.CommitTransaction(ctx, 999), hms.ErrNotFound)
	require.ErrorIs(t, c.AbortTransaction(ctx, 999), hms.ErrNotFound)
	require.ErrorIs(t, c.Heartbeat(ctx, 999, 0), hms.ErrNotFound)

	_, err := c.CheckLock(ctx, 999)
	require.ErrorIs(t, err, hms.ErrNotFound)
	require.ErrorIs(t, c.Unlock(ctx, 999), hms.ErrNotFound)
	require.ErrorIs(t, c.Heartbeat(ctx, 0, 999), hms.ErrNotFound)
}

// TestACID_AbortThenCommit_ErrInvalidOperation covers SPEC §7's
// TxnAbortedException -> hms.ErrInvalidOperation mapping: committing a
// transaction already aborted fails distinctly from committing one that
// was never opened (ErrNotFound, covered above). A second abort is
// idempotent, per AbortTxn's own doc comment in internal/hmstest/acid.go.
func TestACID_AbortThenCommit_ErrInvalidOperation(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI())
	ctx := context.Background()

	txnID, err := c.OpenTransaction(ctx, "alice", "host1")
	require.NoError(t, err)
	require.NoError(t, c.AbortTransaction(ctx, txnID))

	err = c.CommitTransaction(ctx, txnID)
	require.ErrorIs(t, err, hms.ErrInvalidOperation)

	require.NoError(t, c.AbortTransaction(ctx, txnID), "aborting an already-aborted transaction is idempotent")
}

// TestACID_Heartbeat_TxnOnlyAndLockOnly covers Heartbeat's "either id may
// be 0 to omit it" contract (SPEC §5.9) in both directions.
func TestACID_Heartbeat_TxnOnlyAndLockOnly(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI())
	ctx := context.Background()

	txnID, err := c.OpenTransaction(ctx, "alice", "host1")
	require.NoError(t, err)
	require.NoError(t, c.Heartbeat(ctx, txnID, 0))

	resp, err := c.Lock(ctx, hms.LockRequest{
		Components: []hms.LockComponent{
			{Type: hms.LockTypeSharedRead, Level: hms.LockLevelTable, Database: "db", Table: "t"},
		},
		User: "alice",
		Host: "host1",
	})
	require.NoError(t, err)
	require.NoError(t, c.Heartbeat(ctx, 0, resp.LockID))
}

// TestLockLevel_LockType_LockState_String covers the enum String() methods
// used in test failure output and any caller-side logging.
func TestLockLevel_LockType_LockState_String(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "DB", hms.LockLevelDB.String())
	assert.Equal(t, "TABLE", hms.LockLevelTable.String())
	assert.Equal(t, "PARTITION", hms.LockLevelPartition.String())
	assert.Equal(t, "LockLevel(9)", hms.LockLevel(9).String())

	assert.Equal(t, "SHARED_READ", hms.LockTypeSharedRead.String())
	assert.Equal(t, "SHARED_WRITE", hms.LockTypeSharedWrite.String())
	assert.Equal(t, "EXCLUSIVE", hms.LockTypeExclusive.String())
	assert.Equal(t, "EXCL_WRITE", hms.LockTypeExclWrite.String())
	assert.Equal(t, "LockType(9)", hms.LockType(9).String())

	assert.Equal(t, "ACQUIRED", hms.LockStateAcquired.String())
	assert.Equal(t, "WAITING", hms.LockStateWaiting.String())
	assert.Equal(t, "ABORT", hms.LockStateAbort.String())
	assert.Equal(t, "NOT_ACQUIRED", hms.LockStateNotAcquired.String())
	assert.Equal(t, "LockState(9)", hms.LockState(9).String())
}
