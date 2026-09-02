package hms

import (
	"context"
	"math"
	"slices"
	"time"

	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
)

// NotificationEvent is one entry in the metastore's DbNotificationListener
// event log (SPEC §5.7), used by a consumer that needs to follow metadata
// changes (e.g. a downstream cache invalidator) without polling every
// object in turn.
type NotificationEvent struct {
	// ID is the event's monotonically increasing sequence number, as
	// reported by CurrentNotificationID and consumed as
	// GetNextNotifications' lastEventID.
	ID int64
	// Time is when the server recorded the event, always in UTC.
	Time time.Time
	// Type is the event's wire type, e.g. "CREATE_TABLE",
	// "DROP_PARTITION" (Hive's own NotificationEvent.eventType values);
	// this package does not enumerate them as constants.
	Type string
	// CatalogName is the event's catalog; "hive" when the wire event
	// carries none, matching the default catalog convention used
	// throughout this package (see e.g. tableFromThrift).
	CatalogName string
	// DatabaseName and TableName are empty when the event has no
	// associated database or table (e.g. a catalog-level event).
	DatabaseName string
	TableName    string
	// Message is the event body, typically JSON (see MessageFormat)
	// describing what changed; this package does not parse it.
	Message string
	// MessageFormat names Message's encoding, typically "json-0.2".
	MessageFormat string
}

// newNotificationEventRequest builds a NotificationEventRequest from
// hive_metastore.NewNotificationEventRequest() rather than a bare struct
// literal (matching every other request builder in this package), leaving
// MaxEvents and EventTypeList absent from the wire unless the caller
// actually wants them:
//
//   - MaxEvents stays nil (server default: no limit) unless max > 0; a
//     value above math.MaxInt32 is clamped rather than silently wrapping
//     (gosec G115).
//   - EventTypeList stays nil (every event type) unless eventTypes is
//     non-empty.
//
// EventTypeList -- along with EventTypeSkipList, CatName, DbName, and
// TableNames -- is a Hive 4.x-only addition to NotificationEventRequest:
// present in the 4.2.1 IDL this client is generated from, but absent from
// both the 2.3.9 and 3.1.3 IDLs (verified against
// rel/release-2.3.9/rel/release-3.1.3 hive_metastore.thrift, which declare
// NotificationEventRequest with only lastEvent and maxEvents). A 2.3/3.x
// server's Thrift decoder silently skips a field it does not recognize
// rather than rejecting the request, so sending EventTypeList there is
// harmless but has no server-side effect -- which is exactly why
// GetNextNotifications also filters its result locally (see below), so
// eventTypes behaves the same way on every supported version instead of
// silently degrading to "every type" only on 2.3/3.x.
func newNotificationEventRequest(lastEventID int64, max int, eventTypes []string) *hive_metastore.NotificationEventRequest {
	req := hive_metastore.NewNotificationEventRequest()
	req.LastEvent = lastEventID
	if max > 0 {
		m := max
		if m > math.MaxInt32 {
			m = math.MaxInt32
		}
		v := int32(m)
		req.MaxEvents = &v
	}
	if len(eventTypes) > 0 {
		req.EventTypeList = eventTypes
	}
	return req
}

// CurrentNotificationID returns the metastore's current notification event
// ID, wrapping get_current_notificationEventId (SPEC §5.7). Passing the
// returned value as GetNextNotifications' lastEventID later returns only
// events recorded after this call.
func (c *Client) CurrentNotificationID(ctx context.Context) (int64, error) {
	var out int64
	err := c.read(ctx, "get_current_notificationEventId", func(ctx context.Context, cn *conn) error {
		resp, err := cn.getCurrentNotificationEventId(ctx)
		if err != nil {
			return err
		}
		out = resp.EventId
		return nil
	})
	return out, err
}

// GetNextNotifications returns the events recorded after lastEventID (e.g.
// a value CurrentNotificationID previously returned), oldest first,
// wrapping get_next_notification (SPEC §5.7). max bounds how many events
// the server returns; max <= 0 means no limit. eventTypes, when non-empty,
// restricts the result to those event types (e.g. "CREATE_TABLE",
// "DROP_PARTITION"); nil or empty means every event type.
//
// eventTypes is applied both on the request (see newNotificationEventRequest)
// and again locally against the response, so its filtering is uniform on
// every supported version: NotificationEventRequest.EventTypeList only
// exists on Hive 4.x, so a 2.3/3.x server always returns every event type
// regardless of what this method sends, and the local filter is what makes
// eventTypes actually take effect there too. Both RPCs this method and
// CurrentNotificationID wrap exist on every supported version (Hive 2.3+,
// SPEC §5.7).
//
// GetNextNotifications returns (nil, nil) when no event matches, never an
// empty non-nil slice.
func (c *Client) GetNextNotifications(ctx context.Context, lastEventID int64, max int, eventTypes []string) ([]NotificationEvent, error) {
	var out []NotificationEvent
	err := c.read(ctx, "get_next_notification", func(ctx context.Context, cn *conn) error {
		resp, err := cn.getNextNotification(ctx, newNotificationEventRequest(lastEventID, max, eventTypes))
		if err != nil {
			return err
		}
		var events []NotificationEvent
		for _, e := range resp.Events {
			ev := notificationFromThrift(e)
			if len(eventTypes) > 0 && !slices.Contains(eventTypes, ev.Type) {
				continue
			}
			events = append(events, ev)
		}
		out = events
		return nil
	})
	return out, err
}
