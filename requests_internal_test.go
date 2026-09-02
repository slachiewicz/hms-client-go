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
