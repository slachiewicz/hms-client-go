package hms_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hms "github.com/slachiewicz/hms-client-go"
	"github.com/slachiewicz/hms-client-go/internal/hmstest"
)

// notificationVersions is the version table every notification test runs
// against, matching database_test.go's TestDatabases_CreateGetRoundTrip:
// SPEC §5.7 promises get_next_notification/get_current_notificationEventId
// on every supported version (Hive 2.3+), so nothing here is version-gated.
var notificationVersions = []struct {
	name string
	v    hmstest.Version
}{
	{"hive23", hmstest.Hive23},
	{"hive31", hmstest.Hive31},
	{"hive40", hmstest.Hive40},
}

// TestNotifications_IDsIncreaseAndMatchCurrentID covers SPEC §5.7's
// contract that event IDs are a monotonically increasing sequence, and that
// CurrentNotificationID reports the most recently recorded one: three
// create_database/create_table/drop_table-shaped mutations against the fake
// server (internal/hmstest/handler.go's recordEvent) must produce three
// strictly increasing IDs whose last equals CurrentNotificationID.
func TestNotifications_IDsIncreaseAndMatchCurrentID(t *testing.T) {
	t.Parallel()
	for _, tt := range notificationVersions {
		v, name := tt.v, tt.name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := hmstest.Start(t, v)
			c := mustNew(t, srv.URI())
			ctx := context.Background()

			start, err := c.CurrentNotificationID(ctx)
			require.NoError(t, err)
			assert.Equal(t, int64(0), start, "a fresh server has no events yet")

			require.NoError(t, c.CreateDatabase(ctx, &hms.Database{Name: "db", LocationURI: "file:///tmp/db"}))
			require.NoError(t, c.CreateTable(ctx, &hms.Table{DatabaseName: "db", TableName: "t"}))
			require.NoError(t, c.DropTable(ctx, "db", "t", false, false))

			events, err := c.GetNextNotifications(ctx, start, 0, nil)
			require.NoError(t, err)
			require.Len(t, events, 3)

			assert.Equal(t, "CREATE_DATABASE", events[0].Type)
			assert.Equal(t, "CREATE_TABLE", events[1].Type)
			assert.Equal(t, "DROP_TABLE", events[2].Type)

			var last int64
			for i, ev := range events {
				assert.Greater(t, ev.ID, last, "event IDs must strictly increase")
				last = ev.ID
				if i > 0 {
					assert.Greater(t, ev.ID, events[i-1].ID)
				}
			}

			current, err := c.CurrentNotificationID(ctx)
			require.NoError(t, err)
			assert.Equal(t, events[len(events)-1].ID, current, "CurrentNotificationID must match the last recorded event's ID")
		})
	}
}

// TestNotifications_Paging covers lastEventID paging: calling
// GetNextNotifications again with the previous result's own last event ID
// returns only events recorded after it.
func TestNotifications_Paging(t *testing.T) {
	t.Parallel()
	for _, tt := range notificationVersions {
		v, name := tt.v, tt.name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := hmstest.Start(t, v)
			c := mustNew(t, srv.URI())
			ctx := context.Background()

			require.NoError(t, c.CreateDatabase(ctx, &hms.Database{Name: "db", LocationURI: "file:///tmp/db"}))
			require.NoError(t, c.CreateTable(ctx, &hms.Table{DatabaseName: "db", TableName: "t1"}))
			require.NoError(t, c.CreateTable(ctx, &hms.Table{DatabaseName: "db", TableName: "t2"}))

			first, err := c.GetNextNotifications(ctx, 0, 0, nil)
			require.NoError(t, err)
			require.Len(t, first, 3)

			// Paging from the first event only returns what came after it.
			rest, err := c.GetNextNotifications(ctx, first[0].ID, 0, nil)
			require.NoError(t, err)
			require.Len(t, rest, 2)
			assert.Equal(t, first[1].ID, rest[0].ID)
			assert.Equal(t, first[2].ID, rest[1].ID)

			// Paging from the last event returns nothing further.
			none, err := c.GetNextNotifications(ctx, first[len(first)-1].ID, 0, nil)
			require.NoError(t, err)
			assert.Nil(t, none, "GetNextNotifications returns (nil, nil) when nothing matches")
		})
	}
}

// TestNotifications_MaxHonoured covers the max parameter: it bounds how
// many events GetNextNotifications returns, taking the oldest first.
func TestNotifications_MaxHonoured(t *testing.T) {
	t.Parallel()
	for _, tt := range notificationVersions {
		v, name := tt.v, tt.name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := hmstest.Start(t, v)
			c := mustNew(t, srv.URI())
			ctx := context.Background()

			require.NoError(t, c.CreateDatabase(ctx, &hms.Database{Name: "db", LocationURI: "file:///tmp/db"}))
			require.NoError(t, c.CreateTable(ctx, &hms.Table{DatabaseName: "db", TableName: "t1"}))
			require.NoError(t, c.CreateTable(ctx, &hms.Table{DatabaseName: "db", TableName: "t2"}))
			require.NoError(t, c.CreateTable(ctx, &hms.Table{DatabaseName: "db", TableName: "t3"}))

			limited, err := c.GetNextNotifications(ctx, 0, 2, nil)
			require.NoError(t, err)
			require.Len(t, limited, 2)
			assert.Equal(t, "CREATE_DATABASE", limited[0].Type)
			assert.Equal(t, "CREATE_TABLE", limited[1].Type)

			unlimited, err := c.GetNextNotifications(ctx, 0, 0, nil)
			require.NoError(t, err)
			assert.Len(t, unlimited, 4)
		})
	}
}

// TestNotifications_EventTypeFilter covers eventTypes filtering, which must
// work identically on every version (client-side on Hive23/31, since
// NotificationEventRequest.EventTypeList does not exist on the wire there;
// server-side too on Hive40): only the requested types come back, in
// original order.
func TestNotifications_EventTypeFilter(t *testing.T) {
	t.Parallel()
	for _, tt := range notificationVersions {
		v, name := tt.v, tt.name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := hmstest.Start(t, v)
			c := mustNew(t, srv.URI())
			ctx := context.Background()

			require.NoError(t, c.CreateDatabase(ctx, &hms.Database{Name: "db", LocationURI: "file:///tmp/db"}))
			require.NoError(t, c.CreateTable(ctx, &hms.Table{DatabaseName: "db", TableName: "t"}))
			require.NoError(t, c.DropTable(ctx, "db", "t", false, false))
			require.NoError(t, c.CreateTable(ctx, &hms.Table{DatabaseName: "db", TableName: "t2"}))

			events, err := c.GetNextNotifications(ctx, 0, 0, []string{"CREATE_TABLE"})
			require.NoError(t, err)
			require.Len(t, events, 2)
			for _, ev := range events {
				assert.Equal(t, "CREATE_TABLE", ev.Type)
			}
		})
	}
}

// TestNotifications_EmptyLogReturnsNil covers the case where nothing
// matches: GetNextNotifications must return (nil, nil), never an empty
// non-nil slice.
func TestNotifications_EmptyLogReturnsNil(t *testing.T) {
	t.Parallel()
	for _, tt := range notificationVersions {
		v, name := tt.v, tt.name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := hmstest.Start(t, v)
			c := mustNew(t, srv.URI())

			events, err := c.GetNextNotifications(context.Background(), 0, 0, nil)
			require.NoError(t, err)
			assert.Nil(t, events)
		})
	}
}

// TestNotifications_ConvertsMessageAndCatalogDefault covers
// notificationFromThrift's conversion via a black-box observation: Message
// carries the fixture's compact JSON body, MessageFormat is "json-0.2", and
// CatalogName defaults to "hive" (SPEC §5.7; internal/hmstest/handler.go's
// recordEvent never sets CatName, matching a Hive 2.3 server, whose
// NotificationEvent has no such field at all -- 3.x onward does).
func TestNotifications_ConvertsMessageAndCatalogDefault(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c := mustNew(t, srv.URI())
	ctx := context.Background()

	require.NoError(t, c.CreateDatabase(ctx, &hms.Database{Name: "db", LocationURI: "file:///tmp/db"}))
	require.NoError(t, c.CreateTable(ctx, &hms.Table{DatabaseName: "db", TableName: "t"}))

	events, err := c.GetNextNotifications(ctx, 0, 0, nil)
	require.NoError(t, err)
	require.Len(t, events, 2)

	dbEvent := events[0]
	assert.Equal(t, "CREATE_DATABASE", dbEvent.Type)
	assert.Equal(t, `{"db":"db"}`, dbEvent.Message)
	assert.Equal(t, "json-0.2", dbEvent.MessageFormat)
	assert.Equal(t, "hive", dbEvent.CatalogName)
	assert.Equal(t, "db", dbEvent.DatabaseName)
	assert.Empty(t, dbEvent.TableName)
	assert.False(t, dbEvent.Time.IsZero())
	assert.Equal(t, "UTC", dbEvent.Time.Location().String())

	tblEvent := events[1]
	assert.Equal(t, "CREATE_TABLE", tblEvent.Type)
	assert.Equal(t, `{"db":"db","table":"t"}`, tblEvent.Message)
	assert.Equal(t, "db", tblEvent.DatabaseName)
	assert.Equal(t, "t", tblEvent.TableName)
}
