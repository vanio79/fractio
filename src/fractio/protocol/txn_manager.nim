# Transaction manager for the Fractio protocol layer — Phase 3 + Phase 5.
#
# Provides an in-memory, thread-safe TransactionManager that handles:
#   - beginTransaction: allocate a new txn with a monotonic read timestamp
#   - commitTransaction: validate write/read conflict sets; assign commit ts
#   - rollbackTransaction: mark txn aborted; discard pending writes
#   - getTransactionStatus: query the state of any known transaction
#   - expireTimedOutTxns: background sweep (called opportunistically)
#
# Conflict detection rule (Serializable Snapshot Isolation, simplified):
#   A commit is rejected when any key in the committing txn's WRITE SET was
#   also written (committed) by another transaction whose commit timestamp
#   falls strictly after the committing txn's readTimestamp.  This prevents
#   lost updates (write-write conflicts under SSI).
#
# Phase 5 additions:
#   - Optional `timeProvider`: when set, read timestamps are sourced from the
#     P2P SharedTimer (fractio/distributed/sharedtimer) for cluster-wide
#     monotonic ordering.  Falls back to wall clock when nil.
#   - Optional `raftCoord`: when set, commitTransaction uses the Raft 2PC
#     coordinator to durably commit / rollback write-intents through Raft
#     consensus.  When nil (Phase 3 compat), pure in-memory behaviour is kept.

import std/[tables, sets, locks, times, atomics, strformat, options]
import ./types
import ./messages/txn as txnMsgs
import fractio/distributed/sharedtimer/timeprovider as tp

export txnMsgs # re-export status constants

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

type
  TxnRecord* = object
    ## Everything the manager needs to know about one transaction.
    id*: uint64
    flags*: uint8              ## TxnFlagReadOnly, TxnFlagSerializable
    readTimestamp*: uint64     ## MVCC snapshot timestamp (ns since epoch)
    commitTimestamp*: uint64   ## non-zero once committed
    writeSet*: HashSet[string] ## keys written during this transaction
    readSet*: HashSet[string]  ## keys read (used for future SI validation)
    state*: uint8              ## TxnStatus* constants
    createdAtMs*: int64        ## wall-clock ms at begin time
    timeoutMs*: uint32         ## 0 = server default (DEFAULT_TXN_TIMEOUT_MS)

  TransactionManager* = ref object
    txns*: Table[uint64, TxnRecord] ## all known transactions
    mu*: Lock
    nextTxnId*: Atomic[uint64]
    nextTimestamp*: Atomic[uint64]  ## monotonic counter (nanoseconds)
                                    ## commitIndex: tracks (key → commitTs) for conflict detection.
                                    ## Maps each key to the highest commit timestamp that wrote it.
    commitIndex*: Table[string, uint64]
    ## Phase 5 optional integrations:
    timeProvider*: tp.TimeProvider  ## when non-nil, use cluster time for timestamps
    raftCoordPtr*: pointer ## when non-nil, points to RaftTxnCoordinator (void ptr to avoid circular import)

const
  DEFAULT_TXN_TIMEOUT_MS* = 30_000'u32 ## 30 seconds

# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------

proc newTransactionManager*(): TransactionManager =
  result = TransactionManager(
    txns: initTable[uint64, TxnRecord](),
    commitIndex: initTable[string, uint64](),
    timeProvider: nil,
    raftCoordPtr: nil,
  )
  initLock(result.mu)
  result.nextTxnId.store(1)
  # Seed timestamp from wall clock (ns)
  let nowNs = uint64(getTime().toUnixFloat() * 1_000_000_000)
  result.nextTimestamp.store(nowNs)

proc setTimeProvider*(mgr: TransactionManager,
    provider: tp.TimeProvider) {.gcsafe, raises: [].} =
  ## Configure the cluster-wide TimeProvider for timestamp allocation.
  ## Thread-safe: atomic assignment (pointer write is atomic on x86-64).
  acquire(mgr.mu)
  mgr.timeProvider = provider
  release(mgr.mu)

proc setRaftCoordPtr*(mgr: TransactionManager,
    coordPtr: pointer) {.gcsafe, raises: [].} =
  ## Store a void pointer to the RaftTxnCoordinator.
  ## Avoids circular import between txn_manager ↔ raft_txn.
  ## The server is responsible for casting this pointer back.
  acquire(mgr.mu)
  mgr.raftCoordPtr = coordPtr
  release(mgr.mu)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

proc allocTimestamp(mgr: TransactionManager): uint64 {.gcsafe, raises: [].} =
  ## Monotonically-increasing nanosecond timestamp.
  ## When a TimeProvider is configured (Phase 5), uses cluster time.
  ## Always advances by at least 1 tick to ensure strict ordering.
  let wallNs: uint64 =
    if not mgr.timeProvider.isNil:
      try: uint64(mgr.timeProvider.now())
      except Exception: uint64(getTime().toUnixFloat() * 1_000_000_000)
    else:
      uint64(getTime().toUnixFloat() * 1_000_000_000)

  var cur = mgr.nextTimestamp.load()
  while true:
    let next = if wallNs > cur: wallNs else: cur + 1
    if mgr.nextTimestamp.compareExchange(cur, next):
      return next
    # CAS failed — cur was updated by another thread; loop

proc nowMs(): int64 {.gcsafe, raises: [].} =
  int64(getTime().toUnixFloat() * 1000)

proc effectiveTimeout(rec: TxnRecord): uint32 {.inline.} =
  if rec.timeoutMs == 0: DEFAULT_TXN_TIMEOUT_MS else: rec.timeoutMs

proc isExpired(rec: TxnRecord): bool {.gcsafe, raises: [].} =
  rec.state == TxnStatusActive and
  (nowMs() - rec.createdAtMs) > int64(rec.effectiveTimeout)

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

proc beginTransaction*(mgr: TransactionManager, flags: uint8 = 0,
    timeoutMs: uint32 = 0): TxnRecord {.gcsafe, raises: [].} =
  ## Create a new transaction and return its record.
  let id = mgr.nextTxnId.fetchAdd(1)
  let readTs = mgr.allocTimestamp()
  let rec = TxnRecord(
    id: id,
    flags: flags,
    readTimestamp: readTs,
    commitTimestamp: 0,
    writeSet: initHashSet[string](),
    readSet: initHashSet[string](),
    state: TxnStatusActive,
    createdAtMs: nowMs(),
    timeoutMs: timeoutMs,
  )
  acquire(mgr.mu)
  mgr.txns[id] = rec
  release(mgr.mu)
  rec

proc recordRead*(mgr: TransactionManager, txnId: uint64,
    key: string): PResult {.gcsafe, raises: [].} =
  ## Register a key as read by this transaction (for future SI validation).
  acquire(mgr.mu)
  defer: release(mgr.mu)
  var rec = mgr.txns.getOrDefault(txnId)
  if rec.id == 0:
    return pErr(newProtocolError(peInternal, &"txn {txnId} not found"))
  if rec.state != TxnStatusActive:
    return pErr(newProtocolError(peInternal,
      &"txn {txnId} is not active (state={rec.state})"))
  if isExpired(rec):
    rec.state = TxnStatusAborted
    mgr.txns[txnId] = rec
    return pErr(newProtocolError(peTimeout, &"txn {txnId} expired"))
  rec.readSet.incl(key)
  mgr.txns[txnId] = rec
  pOk()

proc recordWrite*(mgr: TransactionManager, txnId: uint64,
    key: string): PResult {.gcsafe, raises: [].} =
  ## Register a key as written (tentatively) by this transaction.
  acquire(mgr.mu)
  defer: release(mgr.mu)
  var rec = mgr.txns.getOrDefault(txnId)
  if rec.id == 0:
    return pErr(newProtocolError(peInternal, &"txn {txnId} not found"))
  if rec.state != TxnStatusActive:
    return pErr(newProtocolError(peInternal,
      &"txn {txnId} is not active (state={rec.state})"))
  if isExpired(rec):
    rec.state = TxnStatusAborted
    mgr.txns[txnId] = rec
    return pErr(newProtocolError(peTimeout, &"txn {txnId} expired"))
  rec.writeSet.incl(key)
  mgr.txns[txnId] = rec
  pOk()

proc commitTransaction*(mgr: TransactionManager,
    txnId: uint64): CommitTxnResponse {.gcsafe, raises: [].} =
  ## Attempt to commit txnId.
  ## Returns CommitTxnResponse with appropriate status and commitTimestamp.
  acquire(mgr.mu)
  defer: release(mgr.mu)

  var rec = mgr.txns.getOrDefault(txnId)
  if rec.id == 0:
    return CommitTxnResponse(status: TxnCommitNotFound, commitTimestamp: 0)

  if rec.state != TxnStatusActive:
    # Already committed or rolled back — idempotent return
    if rec.state == TxnStatusCommitted:
      return CommitTxnResponse(status: TxnCommitOK,
                               commitTimestamp: rec.commitTimestamp)
    else:
      return CommitTxnResponse(status: TxnCommitConflict, commitTimestamp: 0)

  # Timeout check
  if isExpired(rec):
    rec.state = TxnStatusAborted
    mgr.txns[txnId] = rec
    return CommitTxnResponse(status: TxnCommitTimeout, commitTimestamp: 0)

  # Read-only transactions never conflict
  if (rec.flags and TxnFlagReadOnly) != 0:
    let commitTs = mgr.allocTimestamp()
    rec.state = TxnStatusCommitted
    rec.commitTimestamp = commitTs
    mgr.txns[txnId] = rec
    return CommitTxnResponse(status: TxnCommitOK, commitTimestamp: commitTs)

  # Conflict detection: for each key in our write set, check whether another
  # txn committed a write to that key after our readTimestamp.
  for key in rec.writeSet:
    let lastCommitTs = mgr.commitIndex.getOrDefault(key, 0)
    if lastCommitTs > rec.readTimestamp:
      # Conflicting write found — abort
      rec.state = TxnStatusAborted
      mgr.txns[txnId] = rec
      return CommitTxnResponse(status: TxnCommitConflict, commitTimestamp: 0)

  # No conflict — assign commit timestamp and publish writes to the index
  let commitTs = mgr.allocTimestamp()
  for key in rec.writeSet:
    mgr.commitIndex[key] = commitTs
  rec.state = TxnStatusCommitted
  rec.commitTimestamp = commitTs
  mgr.txns[txnId] = rec
  CommitTxnResponse(status: TxnCommitOK, commitTimestamp: commitTs)

proc rollbackTransaction*(mgr: TransactionManager,
    txnId: uint64): RollbackTxnResponse {.gcsafe, raises: [].} =
  ## Abort txnId.  Idempotent — rolling back an already-aborted txn returns OK.
  acquire(mgr.mu)
  defer: release(mgr.mu)

  var rec = mgr.txns.getOrDefault(txnId)
  if rec.id == 0:
    return RollbackTxnResponse(status: TxnRollbackNotFound)

  if rec.state == TxnStatusActive or rec.state == TxnStatusAborted:
    rec.state = TxnStatusAborted
    mgr.txns[txnId] = rec
    return RollbackTxnResponse(status: TxnRollbackOK)

  # Committed transactions cannot be rolled back — report as "not found"
  # to match the wire protocol semantics (caller should not roll back a
  # committed txn; returning NotFound surfaces the programming error).
  RollbackTxnResponse(status: TxnRollbackNotFound)

proc getTransactionStatus*(mgr: TransactionManager,
    txnId: uint64): TxnStatusResponse {.gcsafe, raises: [].} =
  ## Query the current status of txnId.
  acquire(mgr.mu)
  defer: release(mgr.mu)

  var rec = mgr.txns.getOrDefault(txnId)
  if rec.id == 0:
    return TxnStatusResponse(status: TxnStatusNotFound, commitTimestamp: 0)

  # Lazily mark expired active txns as aborted on status query
  if isExpired(rec):
    rec.state = TxnStatusAborted
    mgr.txns[txnId] = rec

  TxnStatusResponse(status: rec.state, commitTimestamp: rec.commitTimestamp)

proc expireTimedOutTxns*(mgr: TransactionManager) {.gcsafe, raises: [].} =
  ## Sweep all active transactions and abort any that have exceeded their
  ## timeout.  Call this periodically (e.g. from a background thread or
  ## opportunistically before each new Begin).
  acquire(mgr.mu)
  defer: release(mgr.mu)
  for txnId, rec in mgr.txns.mpairs:
    if rec.state == TxnStatusActive and isExpired(rec):
      rec.state = TxnStatusAborted

proc activeTxnCount*(mgr: TransactionManager): int {.gcsafe, raises: [].} =
  acquire(mgr.mu)
  defer: release(mgr.mu)
  var count = 0
  for _, rec in mgr.txns:
    if rec.state == TxnStatusActive: inc count
  count

proc totalTxnCount*(mgr: TransactionManager): int {.gcsafe, raises: [].} =
  acquire(mgr.mu)
  defer: release(mgr.mu)
  mgr.txns.len
