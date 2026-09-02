package hms

import (
	"context"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
)

// roundTrip serialises msg with a binary-protocol thrift.TSerializer and
// deserialises the resulting bytes into into. Callers must pass an into
// already built from the same generated NewXxx() constructor as msg (not a
// bare &Xxx{}), matching what a real Thrift server actually reads into:
// e.g. ThriftHiveMetastoreGetTableReqArgs.ReadField1 pre-populates its
// GetTableRequest with Engine:"hive", ID:-1 before calling Read, precisely
// because Write omits a field once it equals that default (see
// GetTableRequest.IsSetEngine/IsSetID) — so an omitted field must decode
// back to the constructor default, not the Go zero value a bare literal
// destination would leave it at.
func roundTrip(t *testing.T, msg, into thrift.TStruct) {
	t.Helper()
	ser := thrift.NewTSerializer()
	b, err := ser.Write(context.Background(), msg)
	require.NoError(t, err)
	deser := thrift.NewTDeserializer()
	require.NoError(t, deser.Read(context.Background(), into, b))
}

// TestNewGetTableRequest_KeepsIDLDefaultsOverWire covers the fix for
// table.go's GetTable building a bare hive_metastore.GetTableRequest{}
// literal, which sent Engine="" and ID=0 on the wire instead of
// NewGetTableRequest()'s defaults ("hive" and -1): neither field has an
// equivalent on the exported Table/Client API, so this package must always
// send the defaults, not the Go zero values.
func TestNewGetTableRequest_KeepsIDLDefaultsOverWire(t *testing.T) {
	t.Parallel()
	req := newGetTableRequest("db", "tbl", nil)

	got := hive_metastore.NewGetTableRequest()
	roundTrip(t, req, got)

	assert.Equal(t, "hive", got.Engine)
	assert.Equal(t, int64(-1), got.ID)
}

// TestNewPartitionsRequest_KeepsIDLDefaultsOverWire covers the fix for
// partition.go's GetPartitions building a bare
// hive_metastore.PartitionsRequest{} literal, which sent ID=0 on the wire
// instead of NewPartitionsRequest()'s default of -1 ("unset"); ID has no
// equivalent on the exported API.
func TestNewPartitionsRequest_KeepsIDLDefaultsOverWire(t *testing.T) {
	t.Parallel()
	req := newPartitionsRequest("db", "tbl", nil, -1)

	got := hive_metastore.NewPartitionsRequest()
	roundTrip(t, req, got)

	assert.Equal(t, int64(-1), got.ID)
}

// TestNewAlterPartitionsRequest_KeepsIDLDefaultsOverWire covers the fix for
// partition.go's AlterPartitions building a bare
// hive_metastore.AlterPartitionsRequest{} literal, which sent WriteId=0 on
// the wire instead of NewAlterPartitionsRequest()'s default of -1 (a real
// write id on the wire rather than "unassigned"); WriteId has no
// equivalent on the exported Partition type.
func TestNewAlterPartitionsRequest_KeepsIDLDefaultsOverWire(t *testing.T) {
	t.Parallel()
	req := newAlterPartitionsRequest("db", "tbl", nil, nil)

	got := hive_metastore.NewAlterPartitionsRequest()
	roundTrip(t, req, got)

	assert.Equal(t, int64(-1), got.WriteId)
}

// TestNewDropPartitionsRequest_SetsFieldsAndTurnsOffNeedResult covers the
// fix for partition.go's DropPartitionsByNames: every field it sets has an
// exported equivalent, so unlike newPartitionsRequest/
// newAlterPartitionsRequest there is no IDL default merely "kept" here --
// NeedResult_ is instead deliberately overwritten away from
// NewDropPartitionsRequest()'s own default (true), since
// DropPartitionsByNames never needs the dropped partitions echoed back.
func TestNewDropPartitionsRequest_SetsFieldsAndTurnsOffNeedResult(t *testing.T) {
	t.Parallel()
	req := newDropPartitionsRequest("db", "tbl", nil, []string{"dt=1"}, true, false)

	got := hive_metastore.NewDropPartitionsRequest()
	roundTrip(t, req, got)

	assert.Equal(t, "db", got.DbName)
	assert.Equal(t, "tbl", got.TblName)
	require.NotNil(t, got.Parts)
	assert.Equal(t, []string{"dt=1"}, got.Parts.Names)
	assert.Empty(t, got.Parts.Exprs)
	require.NotNil(t, got.DeleteData)
	assert.True(t, *got.DeleteData)
	assert.False(t, got.IfExists)
	assert.False(t, got.NeedResult_, "DropPartitionsByNames never needs the dropped partitions echoed back")
}

// TestTableToThrift_KeepsIDLDefaultsOverWire is the wire-level counterpart
// to TestTableToThrift_DefaultsOwnerTypeAndWriteId in
// convert_internal_test.go: it proves the defaults tableToThrift sets
// survive an actual binary-protocol serialise/deserialise round trip, not
// just direct field inspection of the built struct.
func TestTableToThrift_KeepsIDLDefaultsOverWire(t *testing.T) {
	t.Parallel()
	msg := tableToThrift(&Table{DatabaseName: "d", TableName: "t"}, nil)

	got := hive_metastore.NewTable()
	roundTrip(t, msg, got)

	assert.Equal(t, hive_metastore.PrincipalType_USER, got.OwnerType)
	assert.Equal(t, int64(-1), got.WriteId)
}

// TestPartitionToThrift_KeepsIDLDefaultsOverWire is the wire-level
// counterpart to TestPartitionToThrift_DefaultsWriteId in
// convert_internal_test.go.
func TestPartitionToThrift_KeepsIDLDefaultsOverWire(t *testing.T) {
	t.Parallel()
	msg := partitionToThrift(&Partition{Values: []string{"v"}}, nil, "d", "t")

	got := hive_metastore.NewPartition()
	roundTrip(t, msg, got)

	assert.Equal(t, int64(-1), got.WriteId)
}

// TestNewGetPartitionsByNamesRequest_KeepsIDLDefaultsOverWire covers
// partition.go's GetPartitionsByNames building a
// hive_metastore.GetPartitionsByNamesRequest via
// NewGetPartitionsByNamesRequest() rather than a bare struct literal,
// which would send Engine="" and ID=0 on the wire instead of the IDL
// defaults ("hive" and -1); neither field has an equivalent on the
// exported API.
func TestNewGetPartitionsByNamesRequest_KeepsIDLDefaultsOverWire(t *testing.T) {
	t.Parallel()
	req := newGetPartitionsByNamesRequest("db", "tbl", nil, []string{"dt=1"})

	got := hive_metastore.NewGetPartitionsByNamesRequest()
	roundTrip(t, req, got)

	assert.Equal(t, "hive", got.Engine)
	assert.Equal(t, int64(-1), got.ID)
}

// TestNewGetPartitionNamesPsRequest_KeepsIDLDefaultsOverWire covers
// partition.go's GetPartitionNamesByValues building a
// hive_metastore.GetPartitionNamesPsRequest via
// NewGetPartitionNamesPsRequest() rather than a bare struct literal, which
// would send ID=0 on the wire instead of the IDL default -1; ID has no
// equivalent on the exported API.
func TestNewGetPartitionNamesPsRequest_KeepsIDLDefaultsOverWire(t *testing.T) {
	t.Parallel()
	req := newGetPartitionNamesPsRequest("db", "tbl", nil, []string{"v"}, -1)

	got := hive_metastore.NewGetPartitionNamesPsRequest()
	roundTrip(t, req, got)

	assert.Equal(t, int64(-1), got.ID)
}

// TestDatabaseToThrift_CopiesParameters covers the fix for
// databaseToThrift assigning db.Parameters directly instead of copying it
// through copyStringMap like every other converter in convert.go: the wire
// struct must never alias the caller's map.
func TestDatabaseToThrift_CopiesParameters(t *testing.T) {
	t.Parallel()
	params := map[string]string{"k": "v"}
	db := &Database{Name: "d", Parameters: params}

	out := databaseToThrift(db, nil)
	require.NotNil(t, out)
	require.Equal(t, params, out.Parameters)

	out.Parameters["k"] = "changed"
	assert.Equal(t, "v", params["k"], "databaseToThrift must not alias the caller's Parameters map")
}

// TestDatabaseFromThrift_CopiesParameters is databaseToThrift's
// counterpart: databaseFromThrift must not hand back a Database whose
// Parameters map aliases the generated struct's map either.
func TestDatabaseFromThrift_CopiesParameters(t *testing.T) {
	t.Parallel()
	params := map[string]string{"k": "v"}
	wire := &hive_metastore.Database{Name: "d", Parameters: params}

	out := databaseFromThrift(wire, nil)
	require.NotNil(t, out)
	require.Equal(t, params, out.Parameters)

	out.Parameters["k"] = "changed"
	assert.Equal(t, "v", params["k"], "databaseFromThrift must not alias the wire struct's Parameters map")
}

// TestNewNotificationEventRequest_KeepsIDLDefaultsOverWire covers
// notification.go's newNotificationEventRequest building a
// NotificationEventRequest via hive_metastore.NewNotificationEventRequest()
// rather than a bare struct literal, and leaving MaxEvents/EventTypeList
// absent (nil) over the wire when max <= 0 / eventTypes is empty, rather
// than sending a Go zero value a real server could misread as "return
// nothing" (MaxEvents: 0) instead of "no limit".
func TestNewNotificationEventRequest_KeepsIDLDefaultsOverWire(t *testing.T) {
	t.Parallel()
	req := newNotificationEventRequest(5, 0, nil)

	got := hive_metastore.NewNotificationEventRequest()
	roundTrip(t, req, got)

	assert.Equal(t, int64(5), got.LastEvent)
	assert.Nil(t, got.MaxEvents)
	assert.Nil(t, got.EventTypeList)
}

// TestNewNotificationEventRequest_SetsMaxEventsAndEventTypeList covers the
// max > 0 / eventTypes non-empty path of newNotificationEventRequest.
func TestNewNotificationEventRequest_SetsMaxEventsAndEventTypeList(t *testing.T) {
	t.Parallel()
	req := newNotificationEventRequest(5, 10, []string{"CREATE_TABLE"})

	got := hive_metastore.NewNotificationEventRequest()
	roundTrip(t, req, got)

	require.NotNil(t, got.MaxEvents)
	assert.Equal(t, int32(10), *got.MaxEvents)
	assert.Equal(t, []string{"CREATE_TABLE"}, got.EventTypeList)
}

// TestNewTableStatsRequest_KeepsIDLDefaultsOverWire covers stats.go's
// GetTableColumnStatistics building a TableStatsRequest via
// NewTableStatsRequest() rather than a bare struct literal, which would
// send Engine="" and ID=0 on the wire instead of the IDL defaults ("hive"
// and -1); neither field has an equivalent on the exported API.
func TestNewTableStatsRequest_KeepsIDLDefaultsOverWire(t *testing.T) {
	t.Parallel()
	req := newTableStatsRequest("db", "tbl", nil, []string{"c1"})

	got := hive_metastore.NewTableStatsRequest()
	roundTrip(t, req, got)

	assert.Equal(t, "hive", got.Engine)
	assert.Equal(t, int64(-1), got.ID)
}

// TestNewOpenTxnRequest_SetsFields covers acid.go's OpenTransaction
// building an OpenTxnRequest via NewOpenTxnRequest(), overriding its
// AgentInfo default ("Unknown") with this package's own identifier.
func TestNewOpenTxnRequest_SetsFields(t *testing.T) {
	t.Parallel()
	req := newOpenTxnRequest("alice", "host1")

	got := hive_metastore.NewOpenTxnRequest()
	roundTrip(t, req, got)

	assert.Equal(t, int32(1), got.NumTxns)
	assert.Equal(t, "alice", got.User)
	assert.Equal(t, "host1", got.Hostname)
	assert.Equal(t, "hms-client-go", got.AgentInfo)
}

// TestNewCommitTxnRequest_KeepsIDLDefaultsOverWire covers acid.go's
// CommitTransaction building a CommitTxnRequest via
// NewCommitTxnRequest() rather than a bare struct literal, which would
// send ExclWriteEnabled=false on the wire instead of the IDL default
// (true); it has no equivalent on the exported API.
func TestNewCommitTxnRequest_KeepsIDLDefaultsOverWire(t *testing.T) {
	t.Parallel()
	req := newCommitTxnRequest(5)

	got := hive_metastore.NewCommitTxnRequest()
	roundTrip(t, req, got)

	assert.Equal(t, int64(5), got.Txnid)
	assert.True(t, got.ExclWriteEnabled)
}

// TestNewAbortTxnRequest_SetsFields covers acid.go's AbortTransaction
// building an AbortTxnRequest via NewAbortTxnRequest().
func TestNewAbortTxnRequest_SetsFields(t *testing.T) {
	t.Parallel()
	req := newAbortTxnRequest(5)

	got := hive_metastore.NewAbortTxnRequest()
	roundTrip(t, req, got)

	assert.Equal(t, int64(5), got.Txnid)
}

// TestNewHeartbeatRequest_OmitsZeroID covers acid.go's Heartbeat building
// a HeartbeatRequest via NewHeartbeatRequest() and setting only the
// non-zero id: a 0 id must stay nil ("omitted") over the wire rather than
// becoming a real 0 the server would look up as an actual id.
func TestNewHeartbeatRequest_OmitsZeroID(t *testing.T) {
	t.Parallel()
	t.Run("txn only", func(t *testing.T) {
		t.Parallel()
		req := newHeartbeatRequest(5, 0)

		got := hive_metastore.NewHeartbeatRequest()
		roundTrip(t, req, got)

		require.NotNil(t, got.Txnid)
		assert.Equal(t, int64(5), *got.Txnid)
		assert.Nil(t, got.Lockid)
	})
	t.Run("lock only", func(t *testing.T) {
		t.Parallel()
		req := newHeartbeatRequest(0, 7)

		got := hive_metastore.NewHeartbeatRequest()
		roundTrip(t, req, got)

		require.NotNil(t, got.Lockid)
		assert.Equal(t, int64(7), *got.Lockid)
		assert.Nil(t, got.Txnid)
	})
}

// TestNewLockComponent_KeepsIDLDefaultsOverWire covers acid.go's
// newLockComponent building a hive_metastore.LockComponent via
// NewLockComponent() rather than a bare struct literal, matching every
// other request builder in this package, while OperationType is deliberately
// overwritten away from that constructor's own default
// (DataOperationType_UNSET, wire value 5): a real metastore's
// TxnHandler.lock rejects UNSET on a transactional lock component outright
// (see lockOperationType's doc comment in acid.go), so LockTypeExclusive
// maps to DataOperationType_NO_TXN (wire value 6) instead.
func TestNewLockComponent_KeepsIDLDefaultsOverWire(t *testing.T) {
	t.Parallel()
	c := newLockComponent(LockComponent{Type: LockTypeExclusive, Level: LockLevelTable, Database: "db", Table: "tbl"})

	got := hive_metastore.NewLockComponent()
	roundTrip(t, c, got)

	assert.Equal(t, hive_metastore.LockType_EXCLUSIVE, got.Type)
	assert.Equal(t, hive_metastore.LockLevel_TABLE, got.Level)
	assert.Equal(t, "db", got.Dbname)
	require.NotNil(t, got.Tablename)
	assert.Equal(t, "tbl", *got.Tablename)
	assert.Nil(t, got.Partitionname)
	assert.Equal(t, hive_metastore.DataOperationType_NO_TXN, got.OperationType)
}

// TestNewLockComponent_OperationTypeByLockType covers lockOperationType's
// full mapping (acid.go) for every exported LockType, asserting the wire
// DataOperationType newLockComponent actually sends -- the field a real
// metastore's TxnHandler.lock rejects the request over when it is left at
// NewLockComponent()'s own default (DataOperationType_UNSET); see
// lockOperationType's doc comment for why each LockType maps where it does.
func TestNewLockComponent_OperationTypeByLockType(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   LockType
		want hive_metastore.DataOperationType
	}{
		{"SharedRead", LockTypeSharedRead, hive_metastore.DataOperationType_SELECT},
		{"SharedWrite", LockTypeSharedWrite, hive_metastore.DataOperationType_INSERT},
		{"ExclWrite", LockTypeExclWrite, hive_metastore.DataOperationType_UPDATE},
		{"Exclusive", LockTypeExclusive, hive_metastore.DataOperationType_NO_TXN},
		{"unknown falls back to NO_TXN, not UNSET", LockType(99), hive_metastore.DataOperationType_NO_TXN},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := newLockComponent(LockComponent{Type: tt.in, Level: LockLevelTable, Database: "db", Table: "tbl"})

			got := hive_metastore.NewLockComponent()
			roundTrip(t, c, got)

			assert.Equal(t, tt.want, got.OperationType)
			assert.NotEqual(t, hive_metastore.DataOperationType_UNSET, got.OperationType,
				"UNSET is rejected by a real metastore's TxnHandler.lock on a transactional lock component")
		})
	}
}

// TestNewLockRequest_KeepsIDLDefaultsOverWire covers acid.go's
// newLockRequest building a hive_metastore.LockRequest via
// NewLockRequest() rather than a bare struct literal, which would send
// AgentInfo="" instead of the IDL default "Unknown"; this package's
// exported LockRequest has no field for AgentInfo. TxnID 0 must stay
// omitted (nil) over the wire.
func TestNewLockRequest_KeepsIDLDefaultsOverWire(t *testing.T) {
	t.Parallel()
	req := newLockRequest(LockRequest{
		Components: []LockComponent{{Type: LockTypeSharedRead, Level: LockLevelDB, Database: "db"}},
		User:       "alice",
		Host:       "host1",
	})

	got := hive_metastore.NewLockRequest()
	roundTrip(t, req, got)

	require.Len(t, got.Component, 1)
	assert.Equal(t, "alice", got.User)
	assert.Equal(t, "host1", got.Hostname)
	assert.Equal(t, "Unknown", got.AgentInfo)
	assert.Nil(t, got.Txnid)
}

// TestNewLockRequest_SetsTxnIDWhenPositive covers newLockRequest's other
// branch: a positive TxnID is sent on the wire.
func TestNewLockRequest_SetsTxnIDWhenPositive(t *testing.T) {
	t.Parallel()
	req := newLockRequest(LockRequest{
		Components: []LockComponent{{Type: LockTypeSharedRead, Level: LockLevelDB, Database: "db"}},
		TxnID:      9,
		User:       "alice",
		Host:       "host1",
	})

	got := hive_metastore.NewLockRequest()
	roundTrip(t, req, got)

	require.NotNil(t, got.Txnid)
	assert.Equal(t, int64(9), *got.Txnid)
}

// TestNewCheckLockRequest_SetsLockid covers acid.go's CheckLock building a
// CheckLockRequest via NewCheckLockRequest().
func TestNewCheckLockRequest_SetsLockid(t *testing.T) {
	t.Parallel()
	req := newCheckLockRequest(7)

	got := hive_metastore.NewCheckLockRequest()
	roundTrip(t, req, got)

	assert.Equal(t, int64(7), got.Lockid)
	assert.Nil(t, got.Txnid)
}

// TestNewUnlockRequest_SetsLockid covers acid.go's Unlock building an
// UnlockRequest via NewUnlockRequest().
func TestNewUnlockRequest_SetsLockid(t *testing.T) {
	t.Parallel()
	req := newUnlockRequest(9)

	got := hive_metastore.NewUnlockRequest()
	roundTrip(t, req, got)

	assert.Equal(t, int64(9), got.Lockid)
}
