package hms

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
)

// LockLevel is the granularity a LockComponent locks: a whole database, a
// table, or a single partition (SPEC §5.9). Values match the generated
// hive_metastore.LockLevel wire enum (idl/hive_metastore.thrift) 1:1; the
// enum itself, and every value below, exists on every supported version
// (Hive 2.3+).
type LockLevel int32

// LockLevel values, matching hive_metastore.LockLevel.
const (
	LockLevelDB        LockLevel = 1
	LockLevelTable     LockLevel = 2
	LockLevelPartition LockLevel = 3
)

// String returns the wire enum's name (e.g. "TABLE"), or a numeric
// fallback for a value outside the known range.
func (l LockLevel) String() string {
	switch l {
	case LockLevelDB:
		return "DB"
	case LockLevelTable:
		return "TABLE"
	case LockLevelPartition:
		return "PARTITION"
	default:
		return fmt.Sprintf("LockLevel(%d)", int32(l))
	}
}

// LockType is the kind of access a LockComponent requests (SPEC §5.9).
// Values match the generated hive_metastore.LockType wire enum 1:1;
// LockTypeExclWrite exists in the IDL on every supported version
// (verified against hive_metastore.thrift at rel/release-2.3.9,
// rel/release-3.1.3, and 4.2.1) even though only newer query engines
// (e.g. Hive's ACID merge/update path) actually request it.
type LockType int32

// LockType values, matching hive_metastore.LockType.
const (
	LockTypeSharedRead  LockType = 1
	LockTypeSharedWrite LockType = 2
	LockTypeExclusive   LockType = 3
	LockTypeExclWrite   LockType = 4
)

// String returns the wire enum's name (e.g. "SHARED_READ"), or a numeric
// fallback for a value outside the known range.
func (t LockType) String() string {
	switch t {
	case LockTypeSharedRead:
		return "SHARED_READ"
	case LockTypeSharedWrite:
		return "SHARED_WRITE"
	case LockTypeExclusive:
		return "EXCLUSIVE"
	case LockTypeExclWrite:
		return "EXCL_WRITE"
	default:
		return fmt.Sprintf("LockType(%d)", int32(t))
	}
}

// LockState is the outcome of a Lock or CheckLock call (SPEC §5.9): a
// LockStateWaiting response means the caller must poll CheckLock until it
// reports LockStateAcquired (or gives up). Values match the generated
// hive_metastore.LockState wire enum 1:1.
type LockState int32

// LockState values, matching hive_metastore.LockState.
const (
	LockStateAcquired    LockState = 1
	LockStateWaiting     LockState = 2
	LockStateAbort       LockState = 3
	LockStateNotAcquired LockState = 4
)

// String returns the wire enum's name (e.g. "ACQUIRED"), or a numeric
// fallback for a value outside the known range.
func (s LockState) String() string {
	switch s {
	case LockStateAcquired:
		return "ACQUIRED"
	case LockStateWaiting:
		return "WAITING"
	case LockStateAbort:
		return "ABORT"
	case LockStateNotAcquired:
		return "NOT_ACQUIRED"
	default:
		return fmt.Sprintf("LockState(%d)", int32(s))
	}
}

// LockComponent is one resource a LockRequest asks the metastore's lock
// manager to lock, at Level's granularity (SPEC §5.9): Table and Partition
// are left empty for a database-level lock (LockLevelDB), and Partition is
// left empty for a table-level lock (LockLevelTable).
type LockComponent struct {
	Type      LockType
	Level     LockLevel
	Database  string
	Table     string // optional
	Partition string // optional
}

// LockRequest asks the metastore's lock manager to lock every component of
// Components as a single unit (SPEC §5.9). TxnID is 0 for a lock requested
// outside any transaction (e.g. a plain read lock), matching the wire
// field's optional txnid.
type LockRequest struct {
	Components []LockComponent
	TxnID      int64 // 0 means none
	User       string
	Host       string
}

// LockResponse is the metastore's answer to Lock or CheckLock (SPEC §5.9).
// ErrorMessage is set only when State explains a failure the server wants
// to surface as text (a Hive 4.x-only addition to the wire struct,
// verified against hive_metastore.thrift at rel/release-2.3.9 and
// rel/release-3.1.3, which declare LockResponse with only lockid/state);
// it is simply empty on an older server.
type LockResponse struct {
	LockID       int64
	State        LockState
	ErrorMessage string
}

// newOpenTxnRequest builds an OpenTxnRequest from
// hive_metastore.NewOpenTxnRequest() rather than a bare struct literal
// (matching every other request builder in this package), opening a
// single transaction as user on host. AgentInfo identifies this client to
// "show transactions" and similar diagnostics, mirroring the Java
// HiveMetaStoreClient's own agentInfo convention, so it always overrides
// NewOpenTxnRequest's default ("Unknown").
//
// TxnType (the transaction kind: DEFAULT, READ_ONLY, etc.) is a Hive
// 4.x-only field, absent from both the 2.3.9 and 3.1.3 IDLs (verified
// against hive_metastore.thrift at rel/release-2.3.9 and rel/release-3.1.3,
// which declare OpenTxnRequest with only
// num_txns/user/hostname/agentInfo); this package has no exported
// equivalent and leaves it at the generated zero value (TxnType_DEFAULT),
// which a pre-4.x server's decoder never sees regardless.
func newOpenTxnRequest(user, host string) *hive_metastore.OpenTxnRequest {
	req := hive_metastore.NewOpenTxnRequest()
	req.NumTxns = 1
	req.User = user
	req.Hostname = host
	req.AgentInfo = "hms-client-go"
	return req
}

// OpenTransaction opens a single transaction as user on host, wrapping
// open_txns with OpenTxnRequest{NumTxns: 1, ...} (SPEC §5.9) and returning
// the single allocated transaction id (OpenTxnsResponse.TxnIds[0]).
// open_txns exists on every supported version (Hive 2.3+).
func (c *Client) OpenTransaction(ctx context.Context, user, host string) (int64, error) {
	var out int64
	err := c.call(ctx, "open_txns", func(ctx context.Context, cn *conn) error {
		resp, err := cn.openTxns(ctx, newOpenTxnRequest(user, host))
		if err != nil {
			return err
		}
		if len(resp.TxnIds) == 0 {
			return errors.New("hms: open_txns returned no transaction ids")
		}
		out = resp.TxnIds[0]
		return nil
	})
	return out, err
}

// newCommitTxnRequest builds a CommitTxnRequest from
// hive_metastore.NewCommitTxnRequest() rather than a bare struct literal,
// so ExclWriteEnabled keeps NewCommitTxnRequest's default (true) instead
// of falling back to the Go zero value (false); this package has no
// exported equivalent for it.
func newCommitTxnRequest(txnID int64) *hive_metastore.CommitTxnRequest {
	req := hive_metastore.NewCommitTxnRequest()
	req.Txnid = txnID
	return req
}

// CommitTransaction commits the transaction opened as txnID (SPEC §5.9),
// wrapping commit_txn. commit_txn exists on every supported version (Hive
// 2.3+). It returns hms.ErrNotFound for an unknown or already-committed
// transaction id, and hms.ErrInvalidOperation if the transaction was
// already aborted or txnID is negative.
func (c *Client) CommitTransaction(ctx context.Context, txnID int64) error {
	if err := checkID("commit_txn", "txnID", txnID); err != nil {
		return err
	}
	return c.call(ctx, "commit_txn", func(ctx context.Context, cn *conn) error {
		return cn.commitTxn(ctx, newCommitTxnRequest(txnID))
	})
}

// newAbortTxnRequest builds an AbortTxnRequest from
// hive_metastore.NewAbortTxnRequest() rather than a bare struct literal,
// matching every other request builder in this package (AbortTxnRequest
// has no non-zero IDL default of its own; NewAbortTxnRequest() and a bare
// literal are equivalent today, but this keeps the construction path
// uniform and future-proof against a default added later).
func newAbortTxnRequest(txnID int64) *hive_metastore.AbortTxnRequest {
	req := hive_metastore.NewAbortTxnRequest()
	req.Txnid = txnID
	return req
}

// AbortTransaction aborts the transaction opened as txnID (SPEC §5.9),
// wrapping abort_txn. abort_txn exists on every supported version (Hive
// 2.3+). It returns hms.ErrNotFound for an unknown transaction id, and
// hms.ErrInvalidOperation for a negative one.
func (c *Client) AbortTransaction(ctx context.Context, txnID int64) error {
	if err := checkID("abort_txn", "txnID", txnID); err != nil {
		return err
	}
	return c.call(ctx, "abort_txn", func(ctx context.Context, cn *conn) error {
		return cn.abortTxn(ctx, newAbortTxnRequest(txnID))
	})
}

// newHeartbeatRequest builds a HeartbeatRequest from
// hive_metastore.NewHeartbeatRequest() rather than a bare struct literal,
// setting only the non-zero id (SPEC §5.9): both Txnid and Lockid are
// optional pointer fields with no IDL default, so a 0 id stays nil on the
// wire ("omitted") instead of sending a real 0 the server would look up
// as an actual, presumably nonexistent, id.
func newHeartbeatRequest(txnID, lockID int64) *hive_metastore.HeartbeatRequest {
	req := hive_metastore.NewHeartbeatRequest()
	if txnID != 0 {
		req.Txnid = &txnID
	}
	if lockID != 0 {
		req.Lockid = &lockID
	}
	return req
}

// Heartbeat keeps a transaction and/or lock alive past the metastore's
// timeout, wrapping heartbeat (SPEC §5.9). Either id may be 0 to omit it
// from the request (see newHeartbeatRequest): a caller heartbeating a bare
// lock outside any transaction passes txnID 0, and one heartbeating a
// transaction with no separate lock passes lockID 0; since 0 already means
// "none", a negative id is a caller mistake and returns
// hms.ErrInvalidOperation without issuing the RPC. heartbeat exists on
// every supported version (Hive 2.3+).
func (c *Client) Heartbeat(ctx context.Context, txnID, lockID int64) error {
	if err := checkID("heartbeat", "txnID", txnID); err != nil {
		return err
	}
	if err := checkID("heartbeat", "lockID", lockID); err != nil {
		return err
	}
	return c.call(ctx, "heartbeat", func(ctx context.Context, cn *conn) error {
		return cn.heartbeat(ctx, newHeartbeatRequest(txnID, lockID))
	})
}

// newLockComponent builds a hive_metastore.LockComponent from
// hive_metastore.NewLockComponent() rather than a bare struct literal, so
// OperationType keeps NewLockComponent's default (DataOperationType_UNSET)
// instead of falling back to the Go zero value (DataOperationType_SELECT,
// wire value 1) -- this package's exported LockComponent has no field for
// it, matching SPEC §5.9's minimal surface, and UNSET is what a real Hive
// client sends for a lock that is not itself a DML operation.
func newLockComponent(lc LockComponent) *hive_metastore.LockComponent {
	out := hive_metastore.NewLockComponent()
	out.Type = hive_metastore.LockType(lc.Type)
	out.Level = hive_metastore.LockLevel(lc.Level)
	out.Dbname = lc.Database
	if lc.Table != "" {
		out.Tablename = &lc.Table
	}
	if lc.Partition != "" {
		out.Partitionname = &lc.Partition
	}
	return out
}

// newLockRequest builds a hive_metastore.LockRequest from
// hive_metastore.NewLockRequest() rather than a bare struct literal, so
// AgentInfo keeps NewLockRequest's default ("Unknown") the same way every
// other request builder in this package preserves IDL defaults. Txnid is
// set only when req.TxnID is positive (0 means "no transaction", the same
// omit-when-zero convention as newHeartbeatRequest).
//
// ZeroWaitReadEnabled/ExclusiveCTAS/LocklessReadsEnabled are Hive 4.x-only
// additions to LockRequest (verified against hive_metastore.thrift at
// rel/release-2.3.9 and rel/release-3.1.3, which declare LockRequest with
// only component/txnid/user/hostname/agentInfo); this package has no
// exported equivalent for any of them and leaves them at their generated
// zero values (false), which a pre-4.x server's decoder never sees
// regardless.
func newLockRequest(req LockRequest) *hive_metastore.LockRequest {
	out := hive_metastore.NewLockRequest()
	out.Component = make([]*hive_metastore.LockComponent, len(req.Components))
	for i, lc := range req.Components {
		out.Component[i] = newLockComponent(lc)
	}
	if req.TxnID > 0 {
		txnID := req.TxnID
		out.Txnid = &txnID
	}
	out.User = req.User
	out.Hostname = req.Host
	return out
}

// lockStateFromThrift converts a wire LockState -- a Thrift i32 enum the
// generated code represents as Go int64 -- to this package's int32
// LockState, clamping a value outside the int32 range rather than
// wrapping it silently (gosec G115); every value a real server actually
// sends is one of the four enumerated constants (1-4), so the clamp is
// purely defensive.
func lockStateFromThrift(s hive_metastore.LockState) LockState {
	v := int64(s)
	switch {
	case v > math.MaxInt32:
		v = math.MaxInt32
	case v < math.MinInt32:
		v = math.MinInt32
	}
	return LockState(v)
}

// checkID rejects a negative transaction or lock id before it reaches the
// wire (SPEC §5.9): 0 means "none" on every id this package takes, so a
// negative value is a caller mistake -- an uninitialised or arithmetic-gone
// -wrong id -- not a request the server should be asked to look up. name is
// the argument's own name, as the caller wrote it.
func checkID(op, name string, id int64) error {
	if id < 0 {
		return wrapAs(op, ErrInvalidOperation, fmt.Errorf("hms: %s must not be negative, got %d", name, id))
	}
	return nil
}

// lockResponseFromThrift converts a wire LockResponse (as returned by lock
// and check_lock) to this package's LockResponse. A nil response (a server
// that answered with neither a result nor an exception) converts to the
// zero LockResponse, whose State is no LockState value at all, rather than
// panicking on the caller's behalf.
func lockResponseFromThrift(r *hive_metastore.LockResponse) LockResponse {
	if r == nil {
		return LockResponse{}
	}
	out := LockResponse{
		LockID: r.Lockid,
		State:  lockStateFromThrift(r.State),
	}
	if r.ErrorMessage != nil {
		out.ErrorMessage = *r.ErrorMessage
	}
	return out
}

// Lock asks the metastore's lock manager to lock every component of req as
// a unit, wrapping lock (SPEC §5.9). The returned LockResponse.State is
// LockStateAcquired when every component was granted immediately, or
// LockStateWaiting when a conflicting lock already held on the same
// resource (e.g. an EXCLUSIVE lock) means the caller must poll
// CheckLock(ctx, resp.LockID) until it reports LockStateAcquired. A
// negative req.TxnID returns hms.ErrInvalidOperation (0 means "no
// transaction"). lock exists on every supported version (Hive 2.3+).
func (c *Client) Lock(ctx context.Context, req LockRequest) (LockResponse, error) {
	if err := checkID("lock", "TxnID", req.TxnID); err != nil {
		return LockResponse{}, err
	}
	var out LockResponse
	err := c.call(ctx, "lock", func(ctx context.Context, cn *conn) error {
		resp, err := cn.lock(ctx, newLockRequest(req))
		if err != nil {
			return err
		}
		out = lockResponseFromThrift(resp)
		return nil
	})
	return out, err
}

// newCheckLockRequest builds a CheckLockRequest from
// hive_metastore.NewCheckLockRequest() rather than a bare struct literal,
// matching every other request builder in this package; Txnid and
// ElapsedMs stay nil (unset), since CheckLock's exported signature has no
// equivalent for either.
func newCheckLockRequest(lockID int64) *hive_metastore.CheckLockRequest {
	req := hive_metastore.NewCheckLockRequest()
	req.Lockid = lockID
	return req
}

// CheckLock polls the current state of a lock previously requested via
// Lock, wrapping check_lock (SPEC §5.9); see LockResponse.State's doc
// comment on Lock for the LockStateWaiting polling contract. check_lock
// exists on every supported version (Hive 2.3+). It returns
// hms.ErrNotFound for an unknown lockID, and hms.ErrInvalidOperation for a
// negative one.
func (c *Client) CheckLock(ctx context.Context, lockID int64) (LockResponse, error) {
	if err := checkID("check_lock", "lockID", lockID); err != nil {
		return LockResponse{}, err
	}
	var out LockResponse
	err := c.call(ctx, "check_lock", func(ctx context.Context, cn *conn) error {
		resp, err := cn.checkLock(ctx, newCheckLockRequest(lockID))
		if err != nil {
			return err
		}
		out = lockResponseFromThrift(resp)
		return nil
	})
	return out, err
}

// newUnlockRequest builds an UnlockRequest from
// hive_metastore.NewUnlockRequest() rather than a bare struct literal,
// matching every other request builder in this package.
func newUnlockRequest(lockID int64) *hive_metastore.UnlockRequest {
	req := hive_metastore.NewUnlockRequest()
	req.Lockid = lockID
	return req
}

// Unlock releases a lock previously requested via Lock, wrapping unlock
// (SPEC §5.9). unlock exists on every supported version (Hive 2.3+). It
// returns hms.ErrNotFound for an unknown lockID, and
// hms.ErrInvalidOperation for a negative one.
func (c *Client) Unlock(ctx context.Context, lockID int64) error {
	if err := checkID("unlock", "lockID", lockID); err != nil {
		return err
	}
	return c.call(ctx, "unlock", func(ctx context.Context, cn *conn) error {
		return cn.unlock(ctx, newUnlockRequest(lockID))
	})
}
