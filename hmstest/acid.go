package hmstest

import (
	"context"
	"fmt"
	"sync"

	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
)

// acidState is the fake server's in-memory transaction and lock table,
// held by Store.Acid (see NewStore). Txn ids and lock ids each increment
// from 1, independently of each other, matching a real metastore's
// separate TXNS and HIVE_LOCKS sequences. It has its own mutex rather than
// sharing Store.mu: no handler method below also needs a
// Databases/Tables/Partitions lookup within the same call, so sharing
// Store's own lock would only add unnecessary contention between
// transaction/lock traffic and every other RPC.
type acidState struct {
	mu       sync.Mutex
	nextTxn  int64
	nextLock int64
	// txns holds every transaction this fake server has opened that has
	// not yet been committed: an open transaction maps to txnStatusOpen,
	// an aborted one to txnStatusAborted. An aborted transaction's entry
	// is kept (not deleted) so a later commit_txn on it reports
	// TxnAbortedException rather than NoSuchTxnException -- distinguishing
	// "existed but is dead" from "never existed", the same distinction a
	// real metastore's TXNS table makes. commit_txn deletes the entry
	// entirely, exactly as a real metastore's TXNS row for it disappears
	// once committed; a commit_txn or heartbeat against that now-deleted
	// id, or one never opened at all, reports NoSuchTxnException either
	// way -- this fixture does not distinguish "committed" from "never
	// existed" for those RPCs, since nothing in this package's tests
	// depends on it.
	txns map[int64]txnStatus
	// locks holds every lock this fake server has granted or queued that
	// has not yet been released by unlock.
	locks map[int64]*lockEntry
}

// txnStatus is the fixture's own tracking of a still-registered
// transaction's state (see acidState.txns's doc comment).
type txnStatus int

const (
	txnStatusOpen txnStatus = iota
	txnStatusAborted
)

// lockEntry is one outstanding lock request's state, keyed by lock id in
// acidState.locks.
type lockEntry struct {
	components []*hive_metastore.LockComponent
	state      hive_metastore.LockState
}

// newAcidState returns an empty acidState, matching a freshly installed
// metastore with no open transactions or locks.
func newAcidState() *acidState {
	return &acidState{
		txns:  map[int64]txnStatus{},
		locks: map[int64]*lockEntry{},
	}
}

// OpenTxns opens req.NumTxns transactions (this client's own
// OpenTransaction only ever requests 1) and returns their newly allocated
// ids, oldest first.
func (h *handler) OpenTxns(_ context.Context, req *hive_metastore.OpenTxnRequest) (*hive_metastore.OpenTxnsResponse, error) {
	h.rec.record("open_txns", req)
	a := h.store.Acid
	a.mu.Lock()
	defer a.mu.Unlock()

	n := req.NumTxns
	if n < 1 {
		n = 1
	}
	ids := make([]int64, n)
	for i := range ids {
		a.nextTxn++
		a.txns[a.nextTxn] = txnStatusOpen
		ids[i] = a.nextTxn
	}
	return &hive_metastore.OpenTxnsResponse{TxnIds: ids}, nil
}

// CommitTxn commits req.Txnid: NoSuchTxnException for an id never opened
// or already finalized (committed or unknown), TxnAbortedException for one
// previously aborted (see acidState.txns's doc comment).
func (h *handler) CommitTxn(_ context.Context, req *hive_metastore.CommitTxnRequest) error {
	h.rec.record("commit_txn", req)
	a := h.store.Acid
	a.mu.Lock()
	defer a.mu.Unlock()

	status, ok := a.txns[req.Txnid]
	if !ok {
		return &hive_metastore.NoSuchTxnException{Message: fmt.Sprintf("No such transaction %d", req.Txnid)}
	}
	if status == txnStatusAborted {
		return &hive_metastore.TxnAbortedException{Message: fmt.Sprintf("Transaction %d already aborted", req.Txnid)}
	}
	delete(a.txns, req.Txnid)
	return nil
}

// AbortTxn aborts req.Txnid: NoSuchTxnException for an id never opened or
// already committed. Aborting an already-aborted transaction is
// idempotent (no error), mirroring a real metastore's abortTxn.
func (h *handler) AbortTxn(_ context.Context, req *hive_metastore.AbortTxnRequest) error {
	h.rec.record("abort_txn", req)
	a := h.store.Acid
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, ok := a.txns[req.Txnid]; !ok {
		return &hive_metastore.NoSuchTxnException{Message: fmt.Sprintf("No such transaction %d", req.Txnid)}
	}
	a.txns[req.Txnid] = txnStatusAborted
	return nil
}

// Heartbeat validates that req's non-zero Txnid and Lockid still exist,
// returning NoSuchTxnException/TxnAbortedException/NoSuchLockException as
// appropriate. It otherwise records nothing further: a real metastore
// merely bumps a last-heartbeat timestamp this fixture does not model,
// since nothing in this package's tests observes it.
func (h *handler) Heartbeat(_ context.Context, req *hive_metastore.HeartbeatRequest) error {
	h.rec.record("heartbeat", req)
	a := h.store.Acid
	a.mu.Lock()
	defer a.mu.Unlock()

	if req.Txnid != nil {
		status, ok := a.txns[*req.Txnid]
		if !ok {
			return &hive_metastore.NoSuchTxnException{Message: fmt.Sprintf("No such transaction %d", *req.Txnid)}
		}
		if status == txnStatusAborted {
			return &hive_metastore.TxnAbortedException{Message: fmt.Sprintf("Transaction %d already aborted", *req.Txnid)}
		}
	}
	if req.Lockid != nil {
		if _, ok := a.locks[*req.Lockid]; !ok {
			return &hive_metastore.NoSuchLockException{Message: fmt.Sprintf("No such lock %d", *req.Lockid)}
		}
	}
	return nil
}

// Lock grants every component of req as a single unit unless an
// already-acquired EXCLUSIVE lock covers the same database and table, in
// which case the whole request is queued (LockState_WAITING) rather than
// denied outright -- matching a real metastore's lock manager, whose
// caller is expected to poll CheckLock until the lock clears.
//
// A component with DataOperationType_UNSET on a transactional request
// (Txnid set) is rejected the same way a real metastore's
// TxnHandler.lock rejects it -- an unchecked
// "java.lang.IllegalStateException: Unexpected DataOperationType: UNSET",
// undeclared in lock's IDL throws clause, so the generated processor
// turns it into a TApplicationException ("Internal error processing
// lock: ..."), exactly like the plain error returned here does. Verified
// against real Hive 2.3.9 and 4.2.1 servers (decompiling
// InsertTxnComponentsCommand.shouldUpdateTxnComponent from
// hive-standalone-metastore-server-4.2.1.jar for the latter); this
// package's own acid.go.newLockComponent never sends UNSET, so this only
// guards against a regression there.
func (h *handler) Lock(_ context.Context, req *hive_metastore.LockRequest) (*hive_metastore.LockResponse, error) {
	h.rec.record("lock", req)
	a := h.store.Acid
	a.mu.Lock()
	defer a.mu.Unlock()

	if req.Txnid != nil {
		for _, comp := range req.Component {
			if comp.OperationType == hive_metastore.DataOperationType_UNSET {
				return nil, fmt.Errorf("unexpected DataOperationType: UNSET agentInfo=%s txnid:%d", req.AgentInfo, *req.Txnid)
			}
		}
	}

	state := hive_metastore.LockState_ACQUIRED
	for _, comp := range req.Component {
		if a.hasExclusiveConflict(comp) {
			state = hive_metastore.LockState_WAITING
			break
		}
	}
	a.nextLock++
	id := a.nextLock
	a.locks[id] = &lockEntry{components: req.Component, state: state}
	return &hive_metastore.LockResponse{Lockid: id, State: state}, nil
}

// hasExclusiveConflict reports whether an already-acquired EXCLUSIVE lock
// covers the same database and table as comp. a.mu must already be held by
// the caller.
func (a *acidState) hasExclusiveConflict(comp *hive_metastore.LockComponent) bool {
	for _, entry := range a.locks {
		if entry.state != hive_metastore.LockState_ACQUIRED {
			continue
		}
		for _, existing := range entry.components {
			if existing.Type == hive_metastore.LockType_EXCLUSIVE &&
				existing.Dbname == comp.Dbname &&
				lockTablename(existing) == lockTablename(comp) {
				return true
			}
		}
	}
	return false
}

// lockTablename returns c.Tablename's value, or "" for a database-level
// component that carries none.
func lockTablename(c *hive_metastore.LockComponent) string {
	if c.Tablename == nil {
		return ""
	}
	return *c.Tablename
}

// CheckLock reports req.Lockid's current state, NoSuchLockException if
// unknown.
func (h *handler) CheckLock(_ context.Context, req *hive_metastore.CheckLockRequest) (*hive_metastore.LockResponse, error) {
	h.rec.record("check_lock", req)
	a := h.store.Acid
	a.mu.Lock()
	defer a.mu.Unlock()

	entry, ok := a.locks[req.Lockid]
	if !ok {
		return nil, &hive_metastore.NoSuchLockException{Message: fmt.Sprintf("No such lock %d", req.Lockid)}
	}
	return &hive_metastore.LockResponse{Lockid: req.Lockid, State: entry.state}, nil
}

// Unlock releases req.Lockid, NoSuchLockException if unknown.
func (h *handler) Unlock(_ context.Context, req *hive_metastore.UnlockRequest) error {
	h.rec.record("unlock", req)
	a := h.store.Acid
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, ok := a.locks[req.Lockid]; !ok {
		return &hive_metastore.NoSuchLockException{Message: fmt.Sprintf("No such lock %d", req.Lockid)}
	}
	delete(a.locks, req.Lockid)
	return nil
}
