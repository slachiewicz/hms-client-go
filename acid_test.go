package hms_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hms "github.com/slachiewicz/hms-client-go"
	"github.com/slachiewicz/hms-client-go/hmstest"
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
// idempotent, per AbortTxn's own doc comment in hmstest/acid.go.
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

// TestACID_NegativeIDsRejected covers SPEC §5.9's "0 means none": a
// negative transaction or lock id is a caller mistake -- an uninitialised
// or miscomputed id -- so it is ErrInvalidOperation, refused before any
// RPC reaches the server, rather than an ErrNotFound for an id no server
// could ever have issued.
func TestACID_NegativeIDsRejected(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI())
	ctx := context.Background()

	// New's own eager dial already issued set_ugi (SPEC §3.1's now
	// default-on binary NOSASL identity) before any of the rejected-id
	// calls below run; the cleanup assertion must only see no *new* RPC
	// beyond that baseline, not an empty log altogether.
	baseline := len(srv.Calls())

	tests := []struct {
		name string
		call func() error
	}{
		{"commit", func() error { return c.CommitTransaction(ctx, -1) }},
		{"abort", func() error { return c.AbortTransaction(ctx, -1) }},
		{"heartbeat txn", func() error { return c.Heartbeat(ctx, -1, 0) }},
		{"heartbeat lock", func() error { return c.Heartbeat(ctx, 0, -1) }},
		// Both 0 names nothing to keep alive (SPEC §5.9).
		{"heartbeat nothing", func() error { return c.Heartbeat(ctx, 0, 0) }},
		{"lock", func() error {
			_, err := c.Lock(ctx, hms.LockRequest{TxnID: -1})
			return err
		}},
		{"check lock", func() error {
			_, err := c.CheckLock(ctx, -1)
			return err
		}},
		{"unlock", func() error { return c.Unlock(ctx, -1) }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.call()
			require.Error(t, err)
			assert.ErrorIs(t, err, hms.ErrInvalidOperation)
		})
	}

	// Registered as cleanup, not asserted inline: the parallel subtests
	// above only run once this function has returned.
	t.Cleanup(func() {
		assert.Len(t, srv.Calls(), baseline, "a rejected id must never reach the server")
	})
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

// TestACID_LockConflict_Levels covers the fixture's hierarchical conflict
// rule: an EXCLUSIVE lock at a broader level blocks a narrower lock inside
// its scope (database over table, table over partition), a narrower one
// blocks a broader one that would contain it, and siblings outside the
// scope are unaffected.
func TestACID_LockConflict_Levels(t *testing.T) {
	t.Parallel()
	lock := func(t *testing.T, c *hms.Client, comp hms.LockComponent) hms.LockState {
		t.Helper()
		resp, err := c.Lock(context.Background(), hms.LockRequest{Components: []hms.LockComponent{comp}, User: "u", Host: "h"})
		require.NoError(t, err)
		return resp.State
	}
	tests := []struct {
		name  string
		held  hms.LockComponent
		next  hms.LockComponent
		state hms.LockState
	}{
		{
			name:  "db exclusive blocks table in that db",
			held:  hms.LockComponent{Type: hms.LockTypeExclusive, Level: hms.LockLevelDB, Database: "db"},
			next:  hms.LockComponent{Type: hms.LockTypeSharedRead, Level: hms.LockLevelTable, Database: "db", Table: "t"},
			state: hms.LockStateWaiting,
		},
		{
			name:  "db exclusive leaves another db alone",
			held:  hms.LockComponent{Type: hms.LockTypeExclusive, Level: hms.LockLevelDB, Database: "db"},
			next:  hms.LockComponent{Type: hms.LockTypeSharedRead, Level: hms.LockLevelTable, Database: "other", Table: "t"},
			state: hms.LockStateAcquired,
		},
		{
			name:  "table exclusive blocks a partition of it",
			held:  hms.LockComponent{Type: hms.LockTypeExclusive, Level: hms.LockLevelTable, Database: "db", Table: "t"},
			next:  hms.LockComponent{Type: hms.LockTypeSharedRead, Level: hms.LockLevelPartition, Database: "db", Table: "t", Partition: "dt=2024-01-01"},
			state: hms.LockStateWaiting,
		},
		{
			name:  "partition exclusive blocks the whole table",
			held:  hms.LockComponent{Type: hms.LockTypeExclusive, Level: hms.LockLevelPartition, Database: "db", Table: "t", Partition: "dt=2024-01-01"},
			next:  hms.LockComponent{Type: hms.LockTypeSharedRead, Level: hms.LockLevelTable, Database: "db", Table: "t"},
			state: hms.LockStateWaiting,
		},
		{
			name:  "partition exclusive leaves a sibling partition alone",
			held:  hms.LockComponent{Type: hms.LockTypeExclusive, Level: hms.LockLevelPartition, Database: "db", Table: "t", Partition: "dt=2024-01-01"},
			next:  hms.LockComponent{Type: hms.LockTypeSharedRead, Level: hms.LockLevelPartition, Database: "db", Table: "t", Partition: "dt=2024-01-02"},
			state: hms.LockStateAcquired,
		},
		{
			name:  "shared table lock does not block a shared partition lock",
			held:  hms.LockComponent{Type: hms.LockTypeSharedRead, Level: hms.LockLevelTable, Database: "db", Table: "t"},
			next:  hms.LockComponent{Type: hms.LockTypeSharedRead, Level: hms.LockLevelPartition, Database: "db", Table: "t", Partition: "dt=2024-01-01"},
			state: hms.LockStateAcquired,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := hmstest.Start(t, hmstest.Hive40)
			c := mustNew(t, srv.URI())
			require.Equal(t, hms.LockStateAcquired, lock(t, c, tc.held))
			assert.Equal(t, tc.state, lock(t, c, tc.next))
		})
	}
}
