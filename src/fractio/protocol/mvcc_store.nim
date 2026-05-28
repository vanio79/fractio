# MVCC Transaction Store for Fractio
#
# Provides MVCC semantics for system table operations through the Raft layer.
# Wraps RaftKVStoreExt and adds transaction support with:
#   - Intent-based writes (provisional until commit)
#   - Snapshot reads (consistent view at transaction start time)
#   - Conflict detection on commit
#   - Automatic rollback on abort

import std/[tables, locks, options, atomics, algorithm, sets]
import ../core/types as coreTypes
import ../utils/query_timer
import ../core/transaction as coreTxn
import ../core/timestamp_provider
import ../storage/mvcc/types as mvccTypes
import ./raft_store
import ./txn_manager
import ./active_txn_registry
import ./messages/kv
import ../distributed/sharedtimer/timeprovider as tp
import ../utils/logging

# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

type
  MvccStoreErrorKind* = enum
    mseNotInTransaction
    mseTransactionNotFound
    mseTransactionNotActive
    mseConflictDetected
    mseIntentNotFound
    mseStorageError
    mseTimeout

  MvccStoreError* = object
    kind*: MvccStoreErrorKind
    msg*: string
    conflictingKey*: string

  MvccResult*[T] = object
    case isOk*: bool
    of true:
      value*: T
    of false:
      error*: MvccStoreError

proc mvccOk*[T](v: T): MvccResult[T] = MvccResult[T](isOk: true, value: v)
proc mvccErr*[T](e: MvccStoreError): MvccResult[T] = MvccResult[T](isOk: false, error: e)

type MvccVoidResult* = object
  case isOk*: bool
  of true: discard
  of false:
    error*: MvccStoreError

proc mvccVOk*(): MvccVoidResult = MvccVoidResult(isOk: true)
proc mvccVErr*(e: MvccStoreError): MvccVoidResult = MvccVoidResult(isOk: false, error: e)

# ---------------------------------------------------------------------------
# Value with metadata (for KV operations)
# ---------------------------------------------------------------------------

type
  MvccValueWithMeta* = object
    ## Value with MVCC metadata for KV operations
    value*: string
    timestamp*: uint64
    version*: uint64 ## Version counter for CAS operations

  MvccPutResult* = object
    ## Result of a put operation with metadata
    status*: uint8 ## PutStatusOK, PutStatusCASFailed, etc.
    timestamp*: uint64
    version*: uint64
    previousValue*: Option[string]

  MvccDeleteResult* = object
    ## Result of a delete operation with metadata
    found*: bool
    previousValue*: Option[string]

# ---------------------------------------------------------------------------
# Session transaction state
# ---------------------------------------------------------------------------

type
  SessionTxnState* = ref object
    txn*: coreTxn.MVCCTransaction
    intents*: tables.Table[string, coreTxn.WriteEntry]
    createdAtNs*: int64

# ---------------------------------------------------------------------------
# MVCC Transaction Store
# ---------------------------------------------------------------------------

type
  MvccTransactionStore* {.acyclic.} = ref object
    raftStore*: RaftKVStoreExt
    txnManager*: TransactionManager
    tsProvider*: TimestampProvider
    sessions*: tables.Table[uint64, SessionTxnState]
    sessionsMu*: Lock
    logger*: Logger
    nextSessionId*: Atomic[uint64]
    # Version tracking for CAS operations
    keyVersions*: tables.Table[string, uint64]
    keyVersionsMu*: Lock
    # Reverse mapping: TransactionID -> sessionId for wire protocol lookups
    txnToSession*: tables.Table[coreTypes.TransactionID, uint64]
    # Active transaction registry pointer (void ptr to avoid circular import)
    activeTxnRegistryPtr*: pointer

# ---------------------------------------------------------------------------
# Key encoding helpers
# ---------------------------------------------------------------------------

proc encodeVersionKey(userKey: string, timestamp: Timestamp): string =
  result = userKey & mvccTypes.VERSION_SEPARATOR
  var tsBytes = mvccTypes.toBigEndian64(timestamp)
  for i in 0..7:
    result.add(chr(int(tsBytes[i])))

proc encodeIntentKey(userKey: string, txnId: coreTypes.TransactionID): string =
  # Intent key format: <userKey>\x00\x01<16 bytes ULID txnId>
  # Total length = userKey.len + 18
  result = userKey & mvccTypes.INTENT_SUFFIX
  let txnBytes = coreTypes.transactionIDToBytes(txnId)
  result.add(txnBytes)

proc isVersionKey*(key: string): bool =
  # Version key format: <userKey>\x00\x00<8 bytes timestamp>
  # Total length = userKey.len + 10
  # VERSION_SEPARATOR is at positions [key.len - 10, key.len - 9]
  if key.len < 10: return false
  let sepPos = key.len - 10
  result = key[sepPos .. sepPos+1] == mvccTypes.VERSION_SEPARATOR

proc isIntentKeyMvcc*(key: string): bool =
  # Intent key format: <userKey>\x00\x01<16 bytes ULID txnId>
  # Total length = userKey.len + 18
  # INTENT_SUFFIX is at positions [key.len - 18, key.len - 17]
  if key.len < 18: return false
  let sepPos = key.len - 18
  result = key[sepPos .. sepPos+1] == mvccTypes.INTENT_SUFFIX

proc extractTxnIdFromIntentKey*(key: string): Option[coreTypes.TransactionID] =
  ## Extract the TransactionID from an MVCC intent key.
  ## Intent key format: <userKey>\x00\x01<16-byte ULID txnId>
  ## Returns none if the key is not a valid intent key.
  if key.len < 18: return none(coreTypes.TransactionID)
  let sepPos = key.len - 18
  if key[sepPos .. sepPos+1] != mvccTypes.INTENT_SUFFIX:
    return none(coreTypes.TransactionID)
  # The last 16 bytes are the ULID transaction ID
  let txnBytes = key[key.len - 16 .. key.len - 1]
  if txnBytes.len != 16: return none(coreTypes.TransactionID)
  try:
    some(coreTypes.transactionIDFromBytes(txnBytes))
  except CatchableError:
    none(coreTypes.TransactionID)

proc extractUserKeyFromIntentKey*(key: string): Option[string] =
  ## Extract the user key from an MVCC intent key.
  ## Intent key format: <userKey>\x00\x01<16-byte ULID txnId>
  ## Returns none if the key is not a valid intent key.
  if key.len < 18: return none(string)
  let sepPos = key.len - 18
  if key[sepPos .. sepPos+1] != mvccTypes.INTENT_SUFFIX:
    return none(string)
  some(key[0 ..< sepPos])

proc decodeVersionKey(encoded: string): tuple[userKey: string,
    timestamp: Timestamp] =
  # Version key format: <userKey>\x00\x00<8 bytes timestamp>
  # userKey ends at position encoded.len - 10
  if encoded.len < 10:
    raise newException(mvccTypes.MVCCError, "Invalid version key: too short")
  let userKeyEnd = encoded.len - 10
  result.userKey = encoded[0 ..< userKeyEnd]
  var tsArr: array[8, uint8]
  for i in 0..7:
    tsArr[i] = uint8(encoded[encoded.len - 8 + i])
  result.timestamp = mvccTypes.fromBigEndian64(Timestamp, tsArr)

# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------

proc newMvccTransactionStore*(raftStore: RaftKVStoreExt,
    txnManager: TransactionManager,
    tsProvider: TimestampProvider): MvccTransactionStore =
  result = MvccTransactionStore(
    raftStore: raftStore,
    txnManager: txnManager,
    tsProvider: tsProvider,
    sessions: initTable[uint64, SessionTxnState](),
    keyVersions: initTable[string, uint64](),
    txnToSession: initTable[coreTypes.TransactionID, uint64](),
    activeTxnRegistryPtr: nil,
    logger: newLogger("protocol.mvcc_store"),
  )
  initLock(result.sessionsMu)
  initLock(result.keyVersionsMu)
  result.nextSessionId.store(1)

# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------

proc createSession*(store: MvccTransactionStore): uint64 {.gcsafe, raises: [].} =
  result = store.nextSessionId.fetchAdd(1)
  let createdAt = if store.tsProvider != nil:
                    try: store.tsProvider.now() except Exception: coreTypes.localTimeNs()
                  else: coreTypes.localTimeNs()
  let state = SessionTxnState(
    txn: nil,
    intents: initTable[string, coreTxn.WriteEntry](),
    createdAtNs: createdAt,
  )
  withLock store.sessionsMu:
    store.sessions[result] = state

proc closeSession*(store: MvccTransactionStore, sessionId: uint64) {.gcsafe,
    raises: [].} =
  withLock store.sessionsMu:
    let state = store.sessions.getOrDefault(sessionId)
    if not state.isNil:
      if state.txn != nil and state.txn.status == mvccTypes.TXN_PENDING:
        discard store.txnManager.rollbackTransaction(state.txn.id)
      store.sessions.del(sessionId)

proc getSessionState*(store: MvccTransactionStore,
    sessionId: uint64): Option[SessionTxnState] {.gcsafe, raises: [].} =
  withLock store.sessionsMu:
    let state = store.sessions.getOrDefault(sessionId)
    if state.isNil:
      return none(SessionTxnState)
    return some(state)

# ---------------------------------------------------------------------------
# Transaction lifecycle
# ---------------------------------------------------------------------------

proc beginTransaction*(store: MvccTransactionStore,
    sessionId: uint64): MvccResult[coreTypes.TransactionID] {.gcsafe, raises: [].} =
  withLock store.sessionsMu:
    let state = store.sessions.getOrDefault(sessionId)
    if state.isNil:
      return mvccErr[coreTypes.TransactionID](MvccStoreError(
        kind: mseTransactionNotFound, msg: "Session not found"))

    if state.txn != nil and state.txn.status == mvccTypes.TXN_PENDING:
      return mvccOk(state.txn.id)

    let txnRec = store.txnManager.beginTransaction()
    state.txn = coreTxn.MVCCTransaction(
      id: txnRec.id,
      status: mvccTypes.TXN_PENDING,
      startTimestamp: coreTypes.Timestamp(txnRec.readTimestamp),
      commitTimestamp: coreTypes.Timestamp(0),
      priority: coreTxn.DEFAULT_PRIORITY,
      writeSet: coreTxn.WriteSet(entries: @[]),
      readSet: coreTxn.ReadSet(keys: @[], timestamps: @[]),
    )
    state.intents = initTable[string, coreTxn.WriteEntry]()
    # Store the reverse mapping
    store.txnToSession[txnRec.id] = sessionId
    return mvccOk(state.txn.id)

proc getSessionIdByTxnId*(store: MvccTransactionStore,
    txnId: coreTypes.TransactionID): Option[uint64] {.gcsafe, raises: [].} =
  ## Look up sessionId from TransactionID
  withLock store.sessionsMu:
    let sessionId = store.txnToSession.getOrDefault(txnId)
    if sessionId != 0:
      return some(sessionId)
    return none(uint64)

proc commitTransaction*(store: MvccTransactionStore,
    sessionId: uint64): MvccResult[coreTypes.Timestamp] {.gcsafe, raises: [].} =
  ## Commit a transaction.  All committed writes (version keys, primary keys)
  ## and intent deletions are batched into a single Raft WriteBatch per group,
  ## reducing N individual Raft consensus rounds to G rounds (one per group).
  withLock store.sessionsMu:
    let state = store.sessions.getOrDefault(sessionId)
    if state.isNil:
      return mvccErr[coreTypes.Timestamp](MvccStoreError(
        kind: mseTransactionNotFound, msg: "Session not found"))

    if state.txn == nil:
      return mvccErr[coreTypes.Timestamp](MvccStoreError(
        kind: mseNotInTransaction, msg: "No active transaction"))

    if state.txn.status != mvccTypes.TXN_PENDING:
      return mvccErr[coreTypes.Timestamp](MvccStoreError(
        kind: mseTransactionNotActive, msg: "Transaction is not active"))

    let commitResp = store.txnManager.commitTransaction(state.txn.id)

    case commitResp.status:
    of TxnCommitOK:
      let commitTs = coreTypes.Timestamp(commitResp.commitTimestamp)
      state.txn.status = mvccTypes.TXN_COMMITTED
      state.txn.commitTimestamp = commitTs

      # Batch all committed writes and intent deletions into one Raft proposal
      # per group, instead of one Raft round per key.
      var puts: seq[tuple[key, value: string]] = @[]
      var deletes: seq[string] = @[]

      for key, entry in state.intents.pairs:
        let versionKey = encodeVersionKey(key, commitTs)
        let intentKey = encodeIntentKey(key, state.txn.id)
        if entry.isDelete:
          let tombstone = mvccTypes.encodeMVCCValue("", commitTs, true, state.txn.id)
          puts.add((versionKey, tombstone))
          puts.add((key, tombstone))
        else:
          let committedValue = mvccTypes.encodeMVCCValue(entry.value, commitTs,
              false, state.txn.id)
          puts.add((versionKey, committedValue))
          puts.add((key, committedValue))
        deletes.add(intentKey)

      let batchRes = store.raftStore.raftWriteBatch(puts, deletes)
      if not batchRes.isOk:
        return mvccErr[coreTypes.Timestamp](MvccStoreError(
          kind: mseStorageError, msg: "Failed to commit batch: " &
          batchRes.error.msg))

      state.intents = initTable[string, coreTxn.WriteEntry]()
      state.txn = nil
      return mvccOk(commitTs)

    of TxnCommitConflict:
      state.txn.status = mvccTypes.TXN_ABORTED
      # Batch intent deletions for rollback too
      var rollbackDeletes: seq[string] = @[]
      for key, entry in state.intents.pairs:
        rollbackDeletes.add(encodeIntentKey(key, state.txn.id))
      discard store.raftStore.raftWriteBatch(@[], rollbackDeletes)
      state.intents = initTable[string, coreTxn.WriteEntry]()
      state.txn = nil
      return mvccErr[coreTypes.Timestamp](MvccStoreError(
        kind: mseConflictDetected, msg: "Transaction conflict detected"))

    of TxnCommitTimeout:
      state.txn.status = mvccTypes.TXN_ABORTED
      # Batch intent deletions for rollback too
      var rollbackDeletes: seq[string] = @[]
      for key, entry in state.intents.pairs:
        rollbackDeletes.add(encodeIntentKey(key, state.txn.id))
      discard store.raftStore.raftWriteBatch(@[], rollbackDeletes)
      state.intents = initTable[string, coreTxn.WriteEntry]()
      state.txn = nil
      return mvccErr[coreTypes.Timestamp](MvccStoreError(
        kind: mseTimeout, msg: "Transaction timed out"))

    else:
      return mvccErr[coreTypes.Timestamp](MvccStoreError(
        kind: mseStorageError, msg: "Unknown commit error"))

proc rollbackTransaction*(store: MvccTransactionStore,
    sessionId: uint64): MvccVoidResult {.gcsafe, raises: [].} =
  withLock store.sessionsMu:
    let state = store.sessions.getOrDefault(sessionId)
    if state.isNil:
      return mvccVErr(MvccStoreError(
        kind: mseTransactionNotFound, msg: "Session not found"))

    if state.txn == nil:
      return mvccVErr(MvccStoreError(
        kind: mseNotInTransaction, msg: "No active transaction"))

    discard store.txnManager.rollbackTransaction(state.txn.id)

    # Batch all intent deletions into a single Raft proposal per group
    var rollbackDeletes: seq[string] = @[]
    for key, entry in state.intents.pairs:
      rollbackDeletes.add(encodeIntentKey(key, state.txn.id))
    discard store.raftStore.raftWriteBatch(@[], rollbackDeletes)

    state.txn.status = mvccTypes.TXN_ABORTED
    state.intents = initTable[string, coreTxn.WriteEntry]()
    state.txn = nil
    return mvccVOk()

proc getTransactionStatus*(store: MvccTransactionStore,
    sessionId: uint64): MvccResult[mvccTypes.MVCCTransactionStatus] {.gcsafe,
        raises: [].} =
  withLock store.sessionsMu:
    let state = store.sessions.getOrDefault(sessionId)
    if state.isNil:
      return mvccErr[mvccTypes.MVCCTransactionStatus](MvccStoreError(
        kind: mseTransactionNotFound, msg: "Session not found"))

    if state.txn == nil:
      return mvccErr[mvccTypes.MVCCTransactionStatus](MvccStoreError(
        kind: mseNotInTransaction, msg: "No active transaction"))

    return mvccOk(state.txn.status)

# ---------------------------------------------------------------------------
# Callback types for wrapper functions
# ---------------------------------------------------------------------------

type
  SessionBody* = proc(sessionId: uint64): MvccVoidResult {.gcsafe, raises: [].}
  SessionBodyWithResult*[T] = proc(sessionId: uint64): MvccResult[T] {.gcsafe,
      raises: [].}
  TransactionBody* = proc(sessionId: uint64): MvccVoidResult {.gcsafe, raises: [].}
  TransactionBodyWithResult*[T] = proc(sessionId: uint64): MvccResult[
      T] {.gcsafe, raises: [].}

proc setActiveTxnRegistryPtr*(store: MvccTransactionStore,
    ptrVal: pointer) {.gcsafe, raises: [].} =
  ## Set the ActiveTxnRegistry pointer. Uses void pointer to avoid circular
  ## import. The server is responsible for casting this pointer back.
  acquire(store.sessionsMu)
  store.activeTxnRegistryPtr = ptrVal
  release(store.sessionsMu)

proc recordIntentKey(store: MvccTransactionStore,
    txnId: coreTypes.TransactionID, intentKey: string) {.gcsafe, raises: [].} =
  ## Notify the ActiveTxnRegistry that a new intent key was written.
  if store.activeTxnRegistryPtr != nil:
    {.cast(raises: []).}:
      try:
        let registry = cast[ActiveTxnRegistry](store.activeTxnRegistryPtr)
        registry.addIntentKey(txnId, intentKey)
      except:
        discard

proc resolveStaleIntentsForUserKey*(store: MvccTransactionStore,
    userKey: string): int {.gcsafe, raises: [].} =
  ## Scan for intent keys belonging to stale (dead) transactions on a user key.
  ## For each stale intent found, force-rollback the owning transaction via the
  ## ActiveTxnRegistry (which also queues intent key deletion in the background
  ## cleaner thread). Returns the number of stale transactions force-rolled back.
  ##
  ## This is called inline by txnPut/txnDelete to proactively clean up dead
  ## transactions' intents before writing a new intent, preventing stale
  ## intents from blocking or polluting reads.
  ##
  ## Thread safety: each forceRollback is atomic; the scan itself is lock-free.
  ## The background cleaner handles actual intent key deletion asynchronously.
  var staleCount = 0
  if store.activeTxnRegistryPtr == nil:
    return 0
  let registry = cast[ActiveTxnRegistry](store.activeTxnRegistryPtr)

  # Scan for intent keys for this user key.
  # Intent key format: <userKey>\x00\x01<16-byte ULID>
  # We scan from userKey + INTENT_SUFFIX to userKey + next prefix byte.
  let scanStart = userKey & mvccTypes.INTENT_SUFFIX
  # Use a scan range that covers all intents for this user key.
  # The intent suffix starts with \x00\x01, and we want all keys where
  # the user key matches exactly. We scan a range starting at the intent
  # prefix and ending just before the version prefix (\x00\x00 sorts before
  # \x00\x01 in byte order, but intent keys have \x00\x01 so they sort
  # after version keys for the same user key).
  # A simple approach: scan from userKey to userKey\x00\x02 (exclusive).
  let scanEnd = userKey & "\x00\x02"

  let scanRes = store.raftStore.raftScan(scanStart, scanEnd, 100'u32,
      includeSystemKeys = true, includeMvccKeys = true)
  if not scanRes.isOk:
    return 0

  for (k, _) in scanRes.value:
    if not isIntentKeyMvcc(k):
      continue
    let blockingTxnIdOpt = extractTxnIdFromIntentKey(k)
    if blockingTxnIdOpt.isNone:
      continue
    let blockingTxnId = blockingTxnIdOpt.get()

    # Check if the blocking transaction is stale
    if registry.isStale(blockingTxnId):
      # Force-rollback the stale transaction. This marks it as aborted
      # in the registry and queues intent key deletion in the background
      # cleaner. The actual deletion happens asynchronously.
      discard registry.forceRollback(blockingTxnId)
      inc staleCount

  return staleCount

proc forceRollbackStaleTransaction*(store: MvccTransactionStore,
    txnId: coreTypes.TransactionID): bool {.gcsafe, raises: [].} =
  ## Check if a transaction is stale (>5s without activity) and force-rollback
  ## it if so. Returns true if the transaction was force-rolled back.
  ##
  ## This is the server-level API for explicit conflict resolution.
  ## When a client operation encounters a conflict with transaction `txnId`,
  ## it calls this to check liveness and force-rollback if stale.
  ##
  ## Thread safety: delegates to ActiveTxnRegistry.forceRollback which is
  ## atomic and lock-free for the isStale check.
  if store.activeTxnRegistryPtr == nil:
    return false
  let registry = cast[ActiveTxnRegistry](store.activeTxnRegistryPtr)
  if registry.isStale(txnId):
    return registry.forceRollback(txnId)
  return false

# ---------------------------------------------------------------------------
# Session wrapper
# ---------------------------------------------------------------------------

proc withSession*(store: MvccTransactionStore,
                  body: SessionBody): MvccVoidResult {.gcsafe, raises: [].} =
  ## Create a session, execute the body, and close the session.
  ## Use this when you need multiple transactions in one session.
  ##
  ## Example:
  ##   discard mvccStore.withSession(proc(sessionId: uint64): MvccVoidResult =
  ##     discard mvccStore.withTransaction(sessionId, proc(sid: uint64): MvccVoidResult =
  ##       discard mvccStore.txnPut(sid, key1, value1)
  ##       return mvccVOk()
  ##     )
  ##     return mvccVOk()
  ##   )

  let sessionId = store.createSession()
  defer: store.closeSession(sessionId)
  return body(sessionId)

proc withSessionResult*[T](store: MvccTransactionStore,
                           body: SessionBodyWithResult[T]): MvccResult[
                               T] {.gcsafe, raises: [].} =
  ## Same as withSession but returns a value.
  let sessionId = store.createSession()
  defer: store.closeSession(sessionId)
  return body(sessionId)

# ---------------------------------------------------------------------------
# Transaction wrapper (session must exist)
# ---------------------------------------------------------------------------

proc withTransaction*(store: MvccTransactionStore,
                      sessionId: uint64,
                      body: TransactionBody): MvccVoidResult {.gcsafe, raises: [].} =
  ## Execute a block within a transaction. Session must already exist.
  ## Automatically commits on success, rolls back on failure.
  ##
  ## Example:
  ##   discard mvccStore.withSession(proc(sid: uint64): MvccVoidResult =
  ##     discard mvccStore.withTransaction(sid, proc(sessionId: uint64): MvccVoidResult =
  ##       discard mvccStore.txnPut(sessionId, key, value)
  ##       return mvccVOk()
  ##     )
  ##     return mvccVOk()
  ##   )

  let beginRes = store.beginTransaction(sessionId)
  if not beginRes.isOk:
    return mvccVErr(beginRes.error)

  let bodyRes = body(sessionId)
  if not bodyRes.isOk:
    discard store.rollbackTransaction(sessionId)
    return bodyRes

  let commitRes = store.commitTransaction(sessionId)
  if not commitRes.isOk:
    return mvccVErr(commitRes.error)

  return mvccVOk()

proc withTransactionResult*[T](store: MvccTransactionStore,
                               sessionId: uint64,
                               body: TransactionBodyWithResult[T]): MvccResult[
                                   T] {.gcsafe, raises: [].} =
  ## Same as withTransaction but returns a value.
  let beginRes = store.beginTransaction(sessionId)
  if not beginRes.isOk:
    return mvccErr[T](beginRes.error)

  let bodyRes = body(sessionId)
  if not bodyRes.isOk:
    discard store.rollbackTransaction(sessionId)
    return mvccErr[T](bodyRes.error)

  let commitRes = store.commitTransaction(sessionId)
  if not commitRes.isOk:
    return mvccErr[T](commitRes.error)

  return bodyRes

# ---------------------------------------------------------------------------
# Convenience: session + transaction in one
# ---------------------------------------------------------------------------

proc withAutoTransaction*(store: MvccTransactionStore,
                          body: TransactionBody): MvccVoidResult {.gcsafe,
                              raises: [].} =
  ## Create session, start transaction, execute body, commit, close session.
  ## Convenience for simple single-transaction cases.
  ##
  ## Example:
  ##   discard mvccStore.withAutoTransaction(proc(sessionId: uint64): MvccVoidResult =
  ##     return mvccStore.txnPut(sessionId, key, value)
  ##   )

  let sessionId = store.createSession()
  defer: store.closeSession(sessionId)
  return store.withTransaction(sessionId, body)

proc withAutoTransactionResult*[T](store: MvccTransactionStore,
                                   body: TransactionBodyWithResult[
                                       T]): MvccResult[T] {.gcsafe, raises: [].} =
  ## Same as withAutoTransaction but returns a value.
  let sessionId = store.createSession()
  defer: store.closeSession(sessionId)
  return store.withTransactionResult(sessionId, body)

# ---------------------------------------------------------------------------
# Transactional KV operations
# ---------------------------------------------------------------------------

proc recordRead*(store: MvccTransactionStore, sessionId: uint64,
    key: string): MvccVoidResult {.gcsafe, raises: [].} =
  withLock store.sessionsMu:
    let state = store.sessions.getOrDefault(sessionId)
    if state.isNil:
      return mvccVErr(MvccStoreError(
        kind: mseTransactionNotFound, msg: "Session not found"))

    if state.txn == nil:
      return mvccVErr(MvccStoreError(
        kind: mseNotInTransaction, msg: "No active transaction"))

    let res = store.txnManager.recordRead(state.txn.id, key)
    if not res.isOk:
      return mvccVErr(MvccStoreError(
        kind: mseStorageError, msg: "Failed to record read"))

    return mvccVOk()

proc txnGet*(store: MvccTransactionStore, sessionId: uint64,
    key: string): MvccResult[Option[string]] {.gcsafe, raises: [].} =
  var readTs: coreTypes.Timestamp = coreTypes.Timestamp(0)
  var localIntents: tables.Table[string, coreTxn.WriteEntry] = initTable[string,
      coreTxn.WriteEntry]()
  var hasTxn = false

  withLock store.sessionsMu:
    let state = store.sessions.getOrDefault(sessionId)
    if state.isNil:
      return mvccErr[Option[string]](MvccStoreError(
        kind: mseTransactionNotFound, msg: "Session not found"))

    if state.txn != nil and state.intents.hasKey(key):
      let entry = state.intents.getOrDefault(key)
      if entry.isDelete:
        return mvccOk(none(string))
      return mvccOk(some(entry.value))

    if state.txn == nil:
      return mvccErr[Option[string]](MvccStoreError(
        kind: mseNotInTransaction, msg: "No active transaction for read"))

    readTs = state.txn.startTimestamp
    localIntents = state.intents
    hasTxn = true

  let versionPrefix = key & VERSION_SEPARATOR
  let scanStart = versionPrefix
  let scanEnd = key & "\x00\x01"

  let scanRes = store.raftStore.raftScan(scanStart, scanEnd, 1000'u32,
      includeSystemKeys = true, includeMvccKeys = true)
  if not scanRes.isOk:
    return mvccErr[Option[string]](MvccStoreError(
      kind: mseStorageError, msg: "Scan failed"))

  var latestVersion: tuple[ts: coreTypes.Timestamp, value: string,
      isDeleted: bool] = (coreTypes.Timestamp(0), "", false)
  var foundVersion = false

  for (k, entry) in scanRes.value:
    try:
      if isVersionKey(k):
        let decoded = decodeVersionKey(k)
        if decoded.timestamp <= readTs and decoded.timestamp > latestVersion.ts:
          let mvccVal = mvccTypes.decodeMVCCValue(entry.value)
          latestVersion = (decoded.timestamp, mvccVal.data, mvccVal.isDeleted)
          foundVersion = true
    except:
      discard

  if not foundVersion or latestVersion.isDeleted:
    return mvccOk(none(string))

  return mvccOk(some(latestVersion.value))

proc txnPut*(store: MvccTransactionStore, sessionId: uint64,
    key: string, value: string): MvccVoidResult {.gcsafe, raises: [].} =
  withLock store.sessionsMu:
    let state = store.sessions.getOrDefault(sessionId)
    if state.isNil:
      return mvccVErr(MvccStoreError(
        kind: mseTransactionNotFound, msg: "Session not found"))

    if state.txn == nil:
      return mvccVErr(MvccStoreError(
        kind: mseNotInTransaction, msg: "No active transaction"))

    if state.txn.status != mvccTypes.TXN_PENDING:
      return mvccVErr(MvccStoreError(
        kind: mseTransactionNotActive, msg: "Transaction is not active"))

    let recordRes = store.txnManager.recordWrite(state.txn.id, key)
    if not recordRes.isOk:
      return mvccVErr(MvccStoreError(
        kind: mseTransactionNotActive, msg: "Failed to record write"))

    # Note: resolveStaleIntentsForUserKey is NOT called here on every write.
    # The background intent scavenger handles stale cleanup asynchronously.
    # Calling resolveStaleIntentsForUserKey on every txnPut would trigger
    # a full Raft scan per write, which is extremely expensive (~7 rows/sec).
    # If a write conflicts with a stale intent, the commit will detect it.

    let intentKey = encodeIntentKey(key, state.txn.id)
    let intentValue = mvccTypes.encodeMVCCValue(value, state.txn.startTimestamp,
        false, state.txn.id)

    let putRes = store.raftStore.raftPut(intentKey, intentValue)
    if not putRes.isOk:
      return mvccVErr(MvccStoreError(
        kind: mseStorageError, msg: "Failed to write intent: " &
        putRes.error.msg))

    state.intents[key] = coreTxn.WriteEntry(key: key, value: value,
        isDelete: false)

    # Record the intent key in the active txn registry for targeted cleanup
    store.recordIntentKey(state.txn.id, intentKey)

    return mvccVOk()

proc txnDelete*(store: MvccTransactionStore, sessionId: uint64,
    key: string): MvccVoidResult {.gcsafe, raises: [].} =
  withLock store.sessionsMu:
    let state = store.sessions.getOrDefault(sessionId)
    if state.isNil:
      return mvccVErr(MvccStoreError(
        kind: mseTransactionNotFound, msg: "Session not found"))

    if state.txn == nil:
      return mvccVErr(MvccStoreError(
        kind: mseNotInTransaction, msg: "No active transaction"))

    if state.txn.status != mvccTypes.TXN_PENDING:
      return mvccVErr(MvccStoreError(
        kind: mseTransactionNotActive, msg: "Transaction is not active"))

    let recordRes = store.txnManager.recordWrite(state.txn.id, key)
    if not recordRes.isOk:
      return mvccVErr(MvccStoreError(
        kind: mseTransactionNotActive, msg: "Failed to record write"))

    # Note: resolveStaleIntentsForUserKey is NOT called here on every write.
    # The background intent scavenger handles stale cleanup asynchronously.
    # Calling resolveStaleIntentsForUserKey on every txnDelete would trigger
    # a full Raft scan per write, which is extremely expensive.

    let intentKey = encodeIntentKey(key, state.txn.id)
    let intentValue = mvccTypes.encodeMVCCValue("", state.txn.startTimestamp,
        true, state.txn.id)

    let putRes = store.raftStore.raftPut(intentKey, intentValue)
    if not putRes.isOk:
      return mvccVErr(MvccStoreError(
        kind: mseStorageError, msg: "Failed to write delete intent: " &
        putRes.error.msg))

    state.intents[key] = coreTxn.WriteEntry(key: key, value: "", isDelete: true)

    # Record the intent key in the active txn registry for targeted cleanup
    store.recordIntentKey(state.txn.id, intentKey)

    return mvccVOk()

proc txnScan*(store: MvccTransactionStore, sessionId: uint64,
    startKey: string, endKey: string,
    limit: uint32 = 0): MvccResult[seq[tuple[key: string,
        value: string]]] {.gcsafe, raises: [].} =
  var readTs: coreTypes.Timestamp = coreTypes.Timestamp(0)
  var localIntents: tables.Table[string, coreTxn.WriteEntry] = initTable[string,
      coreTxn.WriteEntry]()

  withLock store.sessionsMu:
    let state = store.sessions.getOrDefault(sessionId)
    if state.isNil:
      return mvccErr[seq[tuple[key: string, value: string]]](MvccStoreError(
        kind: mseTransactionNotFound, msg: "Session not found"))

    if state.txn == nil:
      return mvccErr[seq[tuple[key: string, value: string]]](MvccStoreError(
        kind: mseNotInTransaction, msg: "No active transaction for scan"))

    readTs = state.txn.startTimestamp
    localIntents = state.intents

  let scanRes = store.raftStore.raftScan(startKey, endKey, limit,
      includeSystemKeys = true, includeMvccKeys = true)
  if not scanRes.isOk:
    return mvccErr[seq[tuple[key: string, value: string]]](MvccStoreError(
      kind: mseStorageError, msg: "Scan failed"))

  var keyVersions: tables.Table[string, tuple[value: string,
      isDeleted: bool, timestamp: coreTypes.Timestamp]] = initTable[string,
      tuple[value: string, isDeleted: bool, timestamp: coreTypes.Timestamp]]()

  for (k, entry) in scanRes.value:
    if isIntentKeyMvcc(k):
      continue

    if isVersionKey(k):
      try:
        let decoded = decodeVersionKey(k)
        if decoded.timestamp <= readTs:
          let mvccVal = mvccTypes.decodeMVCCValue(entry.value)
          # Only update if this version is newer than what we have
          if not keyVersions.hasKey(decoded.userKey):
            keyVersions[decoded.userKey] = (mvccVal.data, mvccVal.isDeleted,
                decoded.timestamp)
          else:
            let existing = keyVersions[decoded.userKey]
            if decoded.timestamp > existing.timestamp:
              keyVersions[decoded.userKey] = (mvccVal.data, mvccVal.isDeleted,
                  decoded.timestamp)
      except:
        discard
    else:
      # Non-MVCC key (regular key) - include as-is or strip MVCC if sysTablePut wrote it directly
      if not keyVersions.hasKey(k):
        if mvccTypes.isLikelyMVCCValue(entry.value):
          try:
            let mvccVal = mvccTypes.decodeMVCCValue(entry.value)
            if mvccVal.timestamp <= readTs:
              keyVersions[k] = (mvccVal.data, mvccVal.isDeleted,
                  mvccVal.timestamp)
          except:
            keyVersions[k] = (entry.value, false, coreTypes.Timestamp(0))
        else:
          keyVersions[k] = (entry.value, false, coreTypes.Timestamp(0))

  for key, entry in localIntents.pairs:
    if key >= startKey and (endKey.len == 0 or key < endKey):
      if entry.isDelete:
        keyVersions.del(key)
      else:
        keyVersions[key] = (entry.value, false, coreTypes.Timestamp(0))

  var results: seq[tuple[key: string, value: string]] = @[]
  var count = 0
  for key, val in keyVersions.pairs:
    if not val.isDeleted:
      results.add((key, val.value))
      inc count
      if limit > 0 and uint32(count) >= limit:
        break

  results.sort(proc(a, b: tuple[key: string, value: string]): int = cmp(a.key, b.key))
  return mvccOk(results)

# ---------------------------------------------------------------------------
# Lightweight snapshot reads (no transaction needed)
# ---------------------------------------------------------------------------

proc snapshotGet*(store: MvccTransactionStore,
    key: string, readTs: coreTypes.Timestamp): MvccResult[Option[string]] {.
    gcsafe, raises: [].} =
  ## Lightweight read at a specific timestamp without transaction overhead.
  # First try to get the key directly
  let directRes = store.raftStore.raftGet(key)
  var nonMvccValue: Option[string] = none(string)
  if directRes.isOk and directRes.value.isSome:
    let val = directRes.value.get().value
    if mvccTypes.isLikelyMVCCValue(val):
      try:
        let decoded = mvccTypes.decodeMVCCValue(val)
        if decoded.timestamp <= readTs:
          if not decoded.isDeleted:
            nonMvccValue = some(decoded.data)
          else:
            nonMvccValue = none(string)
      except:
        nonMvccValue = some(val)
    else:
      nonMvccValue = some(val)

  # Also scan for versioned keys
  let versionPrefix = key & mvccTypes.VERSION_SEPARATOR
  let scanRes = store.raftStore.raftScan(versionPrefix, key & "\x00\x01",
      100'u32, includeSystemKeys = true, includeMvccKeys = true)

  if not scanRes.isOk:
    if nonMvccValue.isSome: return mvccOk(nonMvccValue)
    return mvccErr[Option[string]](MvccStoreError(
      kind: mseStorageError, msg: "Scan failed"))

  var latestVersion: tuple[ts: Timestamp, value: string, isDeleted: bool] = (
    Timestamp(0), "", false)
  var foundVersion = false

  for (k, entry) in scanRes.value:
    if isIntentKeyMvcc(k): continue
    if isVersionKey(k):
      try:
        let decodedKey = decodeVersionKey(k)
        if decodedKey.timestamp <= readTs and decodedKey.timestamp >=
            latestVersion.ts:
          let mvccVal = mvccTypes.decodeMVCCValue(entry.value)
          latestVersion = (decodedKey.timestamp, mvccVal.data,
              mvccVal.isDeleted)
          foundVersion = true
      except: discard

  if foundVersion:
    if latestVersion.isDeleted: return mvccOk(none(string))
    return mvccOk(some(latestVersion.value))

  return mvccOk(nonMvccValue)

proc snapshotScan*(store: MvccTransactionStore,
    startKey: string, endKey: string, readTs: coreTypes.Timestamp,
    limit: uint32 = 0): MvccResult[seq[tuple[key: string, value: string]]] {.
    gcsafe, raises: [].} =
  ## Lightweight scan at a specific timestamp without transaction overhead.
  ## Optimized for single-statement SELECT/SCAN queries.
  ## Note: We scan without limit at the storage layer because MVCC keys can
  ## have multiple versions per user key. We apply the limit after deduplication.

  let scanRes = store.raftStore.raftScan(startKey, endKey, 0, # no limit at storage level
    includeSystemKeys = true, includeMvccKeys = true)
  if not scanRes.isOk:
    return mvccErr[seq[tuple[key: string, value: string]]](MvccStoreError(
      kind: mseStorageError, msg: "Scan failed"))

  var keyVersions: tables.Table[string, tuple[value: string,
      isDeleted: bool, timestamp: coreTypes.Timestamp]] = initTable[string,
      tuple[value: string, isDeleted: bool, timestamp: coreTypes.Timestamp]]()

  # Pass 1: Collect latest versioned keys (keys with \x00\x00 + timestamp suffix)
  for (k, entry) in scanRes.value:
    if isIntentKeyMvcc(k): continue
    if isVersionKey(k):
      try:
        let decoded = decodeVersionKey(k)
        if decoded.timestamp <= readTs:
          let mvccVal = mvccTypes.decodeMVCCValue(entry.value)
          if not keyVersions.hasKey(decoded.userKey) or decoded.timestamp >
              keyVersions[decoded.userKey].timestamp:
            keyVersions[decoded.userKey] = (mvccVal.data, mvccVal.isDeleted,
                decoded.timestamp)
      except:
        discard

  # Pass 2: For keys not an intent or version key, check if likely MVCC.
  # IMPORTANT: We must compare timestamps even if key already exists from Pass 1!
  # This handles the case where newer data is stored without version key suffix
  # but with MVCC-encoded timestamp in the value header.
  for (k, entry) in scanRes.value:
    if isIntentKeyMvcc(k) or isVersionKey(k): continue
    if mvccTypes.isLikelyMVCCValue(entry.value):
      try:
        let mvccVal = mvccTypes.decodeMVCCValue(entry.value)
        if mvccVal.timestamp <= readTs:
          # Compare with existing entry if present - take newer version
          if not keyVersions.hasKey(k) or mvccVal.timestamp > keyVersions[k].timestamp:
            keyVersions[k] = (mvccVal.data, mvccVal.isDeleted,
                mvccVal.timestamp)
      except:
        keyVersions[k] = (entry.value, false, coreTypes.Timestamp(0))
    else:
      # Not MVCC-encoded, add as-is (only if not already present)
      if not keyVersions.hasKey(k):
        keyVersions[k] = (entry.value, false, coreTypes.Timestamp(0))

  var results: seq[tuple[key: string, value: string]] = @[]
  for key, val in keyVersions.pairs:
    if not val.isDeleted:
      results.add((key, val.value))

  results.sort(proc(a, b: tuple[key: string, value: string]): int = cmp(a.key, b.key))

  if limit > 0 and uint32(results.len) > limit:
    results.setLen(int(limit))

  return mvccOk(results)

proc getCurrentTimestamp*(store: MvccTransactionStore): coreTypes.Timestamp {.
    gcsafe, raises: [].} =
  ## Get the current timestamp for snapshot reads.
  ## Uses the transaction manager's time provider.
  let wallNs: uint64 =
    if not store.txnManager.timeProvider.isNil:
      try: uint64(store.txnManager.timeProvider.now())
      except Exception: uint64(coreTypes.localTimeNs())
    else:
      uint64(coreTypes.localTimeNs())
  coreTypes.Timestamp(wallNs)

# ---------------------------------------------------------------------------
# Streaming scan (chunk-based, avoids buffering entire result set)
# ---------------------------------------------------------------------------

type
  ScanChunk* = object
    ## A chunk of deduplicated MVCC scan results, ready for wire encoding.
    pairs*: seq[tuple[key: string, value: string]]
    hasMore*: bool ## True if more results remain

proc snapshotStreamScan*(store: MvccTransactionStore,
    startKey: string, endKey: string, readTs: coreTypes.Timestamp,
    limit: uint32 = 0, chunkSize: int = 1000,
    callback: proc(chunk: ScanChunk) {.gcsafe, raises: [].},
    groupFilter: proc(key: string): bool {.gcsafe, raises: [].} = nil,
    serverFilter: proc(value: string): bool {.gcsafe, raises: [].} = nil,
    raftStore: RaftKVStoreExt = nil): bool {.gcsafe, raises: [].} =
  ## Stream MVCC scan results in chunks, calling `callback` for each chunk.
  ##
  ## Single-pass optimization: LevelDB returns keys in sorted order, and all
  ## MVCC versions of the same user key are contiguous. We exploit this by
  ## doing dedup + filter in a single pass over the sorted scan results,
  ## maintaining natural key ordering without a separate sort step.
  ##
  ## Returns true if all chunks were sent successfully, false on error.
  ##
  ## The `groupFilter` and `serverFilter` callbacks are applied per-row before
  ## adding to a chunk, allowing the server to push down filters during streaming.
  ## If provided, `raftStore` is used for group routing filter resolution.
  ##
  ## `chunkSize` controls how many filtered pairs are accumulated before
  ## invoking the callback. The final chunk may be smaller and has hasMore=false.

  let timer = newQueryTimer()

  # Step 1: Read all raw KV pairs from Raft (this is necessary because MVCC
  # dedup requires seeing all versions of a key to pick the latest).
  let scanRes = store.raftStore.raftScan(startKey, endKey, 0,
      includeSystemKeys = true, includeMvccKeys = true)
  timer.stamp("raft_scan")
  if not scanRes.isOk:
    return false

  let rawCount = scanRes.value.len

  # Step 2: Single-pass dedup + filter
  # LevelDB returns keys in sorted order. All versions/intents for the same
  # userKey are contiguous. We track the "current" userKey and keep the
  # best (highest-timestamp <= readTs) version. When the userKey changes,
  # we finalize it: apply filters, emit to result if it passes.
  # This eliminates the hash table (pass 1+2), the separate filter pass,
  # and the sort — all in one pass with O(1) per-entry overhead.

  # Accumulator for the current user key being deduped.
  # When we encounter a new userKey (or end of scan), we finalize the
  # previous one by applying filters and appending to the result.
  var curUserKey: string = ""
  var curValue: string = ""
  var curIsDeleted: bool = false
  var curTimestamp: coreTypes.Timestamp = coreTypes.Timestamp(0)
  var curHasEntry: bool = false

  # Result buffer: naturally sorted because we iterate in LevelDB order.
  var filteredPairs: seq[tuple[key: string, value: string]] = @[]

  # If limit is set, we can stop early once we have enough results.
  let hasLimit = limit > 0
  let limitInt = int(limit)

  # Finalize the current best version: apply filters, emit if it passes.
  # Inlined as a template to avoid closure capture issues.
  template finalizeCurrentKey(): untyped =
    if curHasEntry and not curIsDeleted:
      var passesFilter = true
      if groupFilter != nil:
        if not groupFilter(curUserKey):
          passesFilter = false
      if passesFilter and serverFilter != nil:
        if not serverFilter(curValue):
          passesFilter = false
      if passesFilter:
        filteredPairs.add((curUserKey, curValue))

  for (k, entry) in scanRes.value:
    # Skip intent keys — they belong to uncommitted transactions
    if isIntentKeyMvcc(k): continue

    var userKey: string
    var isVersion: bool

    if isVersionKey(k):
      try:
        let decoded = decodeVersionKey(k)
        if decoded.timestamp > readTs:
          continue # Future version — invisible at this snapshot
        userKey = decoded.userKey
        isVersion = true
      except:
        continue
    else:
      # Plain key (not version, not intent)
      userKey = k
      isVersion = false

    # Check if this key belongs to a new userKey group
    if curHasEntry and userKey != curUserKey:
      # New userKey — finalize the previous one
      finalizeCurrentKey()
      # Early exit if we've collected enough
      if hasLimit and filteredPairs.len >= limitInt:
        break
      # Reset accumulator for the new userKey
      curHasEntry = false

    # Update the accumulator with this version
    if isVersion:
      # Version key: always MVCC-encoded. Keep if newer than current.
      try:
        let mvccVal = mvccTypes.decodeMVCCValue(entry.value)
        if not curHasEntry or mvccVal.timestamp > curTimestamp:
          curUserKey = userKey
          curValue = mvccVal.data
          curIsDeleted = mvccVal.isDeleted
          curTimestamp = mvccVal.timestamp
          curHasEntry = true
      except:
        discard
    else:
      # Plain key: may or may not be MVCC-encoded
      if mvccTypes.isLikelyMVCCValue(entry.value):
        try:
          let mvccVal = mvccTypes.decodeMVCCValue(entry.value)
          if mvccVal.timestamp <= readTs:
            if not curHasEntry or mvccVal.timestamp > curTimestamp:
              curUserKey = userKey
              curValue = mvccVal.data
              curIsDeleted = mvccVal.isDeleted
              curTimestamp = mvccVal.timestamp
              curHasEntry = true
        except:
          # Malformed MVCC value — treat as plain data
          if not curHasEntry:
            curUserKey = userKey
            curValue = entry.value
            curIsDeleted = false
            curTimestamp = coreTypes.Timestamp(0)
            curHasEntry = true
      else:
        # Plain value (no MVCC encoding) — oldest possible, only if no
        # MVCC version was seen for this key.
        if not curHasEntry:
          curUserKey = userKey
          curValue = entry.value
          curIsDeleted = false
          curTimestamp = coreTypes.Timestamp(0)
          curHasEntry = true

  # Finalize the last userKey
  if not hasLimit or filteredPairs.len < limitInt:
    finalizeCurrentKey()

  timer.stamp("dedup_filter")

  let dedupCount = filteredPairs.len

  # No separate sort needed — results are naturally in LevelDB key order.
  # No separate filter pass needed — filters were applied during dedup.

  # Apply limit (may already be satisfied from early exit above)
  if hasLimit and filteredPairs.len > limitInt:
    filteredPairs.setLen(limitInt)

  let resultCount = filteredPairs.len

  # Step 3: Send chunks via callback
  if filteredPairs.len == 0:
    # Send empty result
    callback(ScanChunk(pairs: @[], hasMore: false))
    timer.stamp("send_chunks")
    let tb = timer.formatBreakdown()
    {.cast(gcsafe).}: {.cast(raises: []).}:
      debug "[scan_timer] raw=" & $rawCount & " dedup_filtered=" & $dedupCount &
          " result=" & $resultCount & " " & tb
    return true

  var sent = 0
  while sent < filteredPairs.len:
    let remaining = filteredPairs.len - sent
    let thisChunk = min(remaining, chunkSize)
    let chunkPairs = filteredPairs[sent ..< sent + thisChunk]
    sent += thisChunk
    let hasMore = sent < filteredPairs.len
    callback(ScanChunk(pairs: chunkPairs, hasMore: hasMore))

  timer.stamp("send_chunks")
  let tb2 = timer.formatBreakdown()
  {.cast(gcsafe).}: {.cast(raises: []).}:
    debug "[scan_timer] raw=" & $rawCount & " dedup_filtered=" & $dedupCount &
        " result=" & $resultCount & " " & tb2
  return true

# ---------------------------------------------------------------------------
# Convenience: latest reads (no timestamp required)
# ---------------------------------------------------------------------------

const LATEST_READ_TIMESTAMP* = coreTypes.Timestamp(high(int64))

proc latestGet*(store: MvccTransactionStore,
    key: string): MvccResult[Option[string]] {.gcsafe, raises: [].} =
  ## Get the latest value for a key without transaction overhead.
  ## Uses max timestamp so committed writes are never invisible due to
  ## clock skew between getCurrentTimestamp and allocTimestamp.
  store.snapshotGet(key, LATEST_READ_TIMESTAMP)

proc latestScan*(store: MvccTransactionStore,
    startKey: string, endKey: string,
    limit: uint32 = 0): MvccResult[seq[tuple[key: string,
        value: string]]] {.gcsafe, raises: [].} =
  ## Scan the latest values without transaction overhead.
  ## Uses max timestamp so committed writes are never invisible due to
  ## clock skew between getCurrentTimestamp and allocTimestamp.
  store.snapshotScan(startKey, endKey, LATEST_READ_TIMESTAMP, limit)

# ---------------------------------------------------------------------------
# KV operations with metadata (for protocol server)
# ---------------------------------------------------------------------------

proc latestGetWithMeta*(store: MvccTransactionStore,
    key: string): MvccResult[Option[MvccValueWithMeta]] {.gcsafe, raises: [].} =
  ## Get the latest value for a key with MVCC metadata.
  ## Uses max timestamp so committed writes are never invisible due to
  ## clock skew between getCurrentTimestamp and allocTimestamp.
  let ts = LATEST_READ_TIMESTAMP

  # First check for existing version
  let directRes = store.raftStore.raftGet(key)
  var latestTs: coreTypes.Timestamp = coreTypes.Timestamp(0)
  var latestValue: string = ""
  var found = false
  var isDeleted = false

  if directRes.isOk and directRes.value.isSome:
    let val = directRes.value.get().value
    if mvccTypes.isLikelyMVCCValue(val):
      try:
        let decoded = mvccTypes.decodeMVCCValue(val)
        if decoded.timestamp <= ts:
          latestTs = decoded.timestamp
          isDeleted = decoded.isDeleted
          if not decoded.isDeleted:
            latestValue = decoded.data
            found = true
      except CatchableError:
        discard
    else:
      # Non-MVCC value (backward compat)
      latestValue = val
      found = true

  # Also scan for versioned keys
  let versionPrefix = key & mvccTypes.VERSION_SEPARATOR
  let scanRes = store.raftStore.raftScan(versionPrefix, key & "\x00\x01",
      100'u32, includeSystemKeys = true, includeMvccKeys = true)

  if scanRes.isOk:
    for (k, entry) in scanRes.value:
      if isIntentKeyMvcc(k): continue
      if isVersionKey(k):
        try:
          let decodedKey = decodeVersionKey(k)
          if decodedKey.timestamp <= ts and decodedKey.timestamp >= latestTs:
            let mvccVal = mvccTypes.decodeMVCCValue(entry.value)
            latestTs = decodedKey.timestamp
            isDeleted = mvccVal.isDeleted
            if not mvccVal.isDeleted:
              latestValue = mvccVal.data
              found = true
            else:
              # Tombstone - clear found flag since this version is deleted
              found = false
        except CatchableError:
          discard

  if not found or isDeleted:
    return mvccOk(none(MvccValueWithMeta))

  # Get version counter
  var version: uint64 = 1
  withLock store.keyVersionsMu:
    version = store.keyVersions.getOrDefault(key, 1'u64)

  return mvccOk(some(MvccValueWithMeta(
    value: latestValue,
    timestamp: uint64(latestTs),
    version: version
  )))

proc txnGetWithMeta*(store: MvccTransactionStore, sessionId: uint64,
    key: string): MvccResult[Option[MvccValueWithMeta]] {.gcsafe, raises: [].} =
  ## Get a value within a transaction, checking intents first.
  ## Returns metadata including timestamp and version.

  # First check if there's an intent for this key in the transaction
  withLock store.sessionsMu:
    let state = store.sessions.getOrDefault(sessionId)
    if state.isNil:
      return mvccErr[Option[MvccValueWithMeta]](MvccStoreError(
        kind: mseTransactionNotFound, msg: "Session not found"))

    # Check intents first (uncommitted writes from this transaction)
    if state.txn != nil and state.intents.hasKey(key):
      let entry = state.intents.getOrDefault(key)
      if entry.isDelete:
        return mvccOk(none(MvccValueWithMeta))
      # Get version for this key
      var ver: uint64 = 1
      withLock store.keyVersionsMu:
        ver = store.keyVersions.getOrDefault(key, 1'u64)
      return mvccOk(some(MvccValueWithMeta(
        value: entry.value,
        timestamp: uint64(state.txn.startTimestamp),
        version: ver
      )))

  # Fall back to committed data
  store.latestGetWithMeta(key)

proc txnPutWithResult*(store: MvccTransactionStore, sessionId: uint64,
    key: string, value: string,
    flags: uint8 = 0, expectedVersion: uint64 = 0): MvccResult[MvccPutResult] {.
    gcsafe, raises: [].} =
  ## Put with full result including previous value and CAS support.
  ## Optimization: skips the expensive latestGetWithMeta when neither
  ## return-previous nor CAS is requested.
  const PutFlagReturnPrev = 0x01'u8
  const PutFlagCAS = 0x04'u8

  let needRead = (flags and PutFlagReturnPrev) != 0 or
      (flags and PutFlagCAS) != 0

  var previousValue: Option[string] = none(string)
  var currentVersion: uint64 = 0

  # Only perform the expensive Raft scan if we actually need the data
  if needRead:
    let getRes = store.latestGetWithMeta(key)
    if getRes.isOk and getRes.value.isSome:
      let meta = getRes.value.get()
      previousValue = some(meta.value)
      currentVersion = meta.version

  # CAS check
  if (flags and PutFlagCAS) != 0:
    if currentVersion != expectedVersion:
      return mvccOk(MvccPutResult(
        status: PutStatusCASFailed,
        timestamp: 0,
        version: currentVersion,
        previousValue: if (flags and PutFlagReturnPrev) !=
            0: previousValue else: none(string)
      ))

  # Perform the put
  let putRes = store.txnPut(sessionId, key, value)
  if not putRes.isOk:
    return mvccErr[MvccPutResult](putRes.error)

  # Increment version
  var newVersion: uint64
  withLock store.keyVersionsMu:
    if needRead:
      newVersion = currentVersion + 1
    else:
      newVersion = store.keyVersions.getOrDefault(key, 0'u64) + 1
    store.keyVersions[key] = newVersion

  let ts = store.getCurrentTimestamp()
  return mvccOk(MvccPutResult(
    status: PutStatusOK,
    timestamp: uint64(ts),
    version: newVersion,
    previousValue: if (flags and PutFlagReturnPrev) !=
        0: previousValue else: none(string)
  ))

proc txnDeleteWithResult*(store: MvccTransactionStore, sessionId: uint64,
    key: string, flags: uint8 = 0): MvccResult[MvccDeleteResult] {.
    gcsafe, raises: [].} =
  ## Delete with full result including previous value and found status.
  ## Optimization: skips the expensive latestGetWithMeta when return-previous
  ## is not requested.
  const DelFlagReturnPrev = 0x01'u8

  let needPrevious = (flags and DelFlagReturnPrev) != 0
  var found = false
  var previousValue: Option[string] = none(string)

  if needPrevious:
    # Only perform the expensive Raft scan if caller wants previous value
    let getRes = store.latestGetWithMeta(key)
    if getRes.isOk and getRes.value.isSome:
      found = true
      previousValue = some(getRes.value.get().value)

    if not found:
      return mvccOk(MvccDeleteResult(
        found: false,
        previousValue: none(string)
      ))

  # Perform the delete
  let delRes = store.txnDelete(sessionId, key)
  if not delRes.isOk:
    return mvccErr[MvccDeleteResult](delRes.error)

  return mvccOk(MvccDeleteResult(
    found: true,
    previousValue: if needPrevious: previousValue else: none(string)
  ))

proc autoPutDirect*(store: MvccTransactionStore, key: string,
    value: string): MvccResult[MvccPutResult] {.gcsafe, raises: [].} =
  ## Single-round auto-commit put: writes version key + primary key in a
  ## single raftWriteBatch, completely bypassing the intent/transaction
  ## lifecycle.  This reduces 2 Raft rounds (intent + commit) to 1.
  ##
  ## Only valid for simple puts (no CAS, no return-previous).
  ## For CAS or return-previous, use autoPutWithResult instead.

  # Allocate commit timestamp (in-memory, no Raft)
  let commitTs = coreTypes.Timestamp(store.txnManager.allocTimestamp())

  # Generate a synthetic TransactionID for MVCC value encoding
  let txnId = coreTypes.genTransactionID(int64(commitTs))

  # Encode version key and committed value
  let versionKey = encodeVersionKey(key, commitTs)
  let committedValue = mvccTypes.encodeMVCCValue(value, commitTs, false, txnId)

  # Build single batch: version key + primary key (no intent)
  let puts = @[
    (key: versionKey, value: committedValue),
    (key: key, value: committedValue),
  ]

  let batchRes = store.raftStore.raftWriteBatch(puts, @[])
  if not batchRes.isOk:
    return mvccErr[MvccPutResult](MvccStoreError(
      kind: mseStorageError, msg: "Failed to commit direct batch: " &
      batchRes.error.msg))

  # Publish the commit to the conflict detection index
  store.txnManager.publishCommit(key, uint64(commitTs))

  # Increment version counter
  var newVersion: uint64
  withLock store.keyVersionsMu:
    newVersion = store.keyVersions.getOrDefault(key, 0'u64) + 1
    store.keyVersions[key] = newVersion

  return mvccOk(MvccPutResult(
    status: PutStatusOK,
    timestamp: uint64(commitTs),
    version: newVersion,
    previousValue: none(string)
  ))

proc autoPutWithResult*(store: MvccTransactionStore, key: string, value: string,
    flags: uint8 = 0, expectedVersion: uint64 = 0): MvccResult[MvccPutResult] {.
    gcsafe, raises: [].} =
  ## Put with auto-transaction and full result.
  ## Optimization: for simple puts (no CAS, no return-previous), uses the
  ## single-round autoPutDirect path which writes all MVCC keys in one
  ## raftWriteBatch — reducing 2 Raft rounds to 1.
  ## For CAS or return-previous, falls back to the full transaction path.
  const PutFlagReturnPrev = 0x01'u8
  const PutFlagCAS = 0x04'u8

  let needRead = (flags and PutFlagReturnPrev) != 0 or
      (flags and PutFlagCAS) != 0

  # Fast path: simple put — single Raft round via autoPutDirect
  if not needRead:
    return store.autoPutDirect(key, value)

  var previousValue: Option[string] = none(string)
  var currentVersion: uint64 = 0

  # Only perform the expensive Raft scan if we actually need the previous
  # value (for return-previous) or version (for CAS check).
  let getRes = store.latestGetWithMeta(key)
  if getRes.isOk and getRes.value.isSome:
    let meta = getRes.value.get()
    previousValue = some(meta.value)
    currentVersion = meta.version

  # CAS check
  if (flags and PutFlagCAS) != 0:
    if currentVersion != expectedVersion:
      return mvccOk(MvccPutResult(
        status: PutStatusCASFailed,
        timestamp: 0,
        version: currentVersion,
        previousValue: if (flags and PutFlagReturnPrev) !=
            0: previousValue else: none(string)
      ))

  # Perform the put with auto-transaction (slow path: 2 Raft rounds)
  let res = store.withAutoTransaction(proc(sid: uint64): MvccVoidResult =
    store.txnPut(sid, key, value)
  )

  if not res.isOk:
    return mvccErr[MvccPutResult](res.error)

  # Increment version
  var newVersion: uint64
  withLock store.keyVersionsMu:
    newVersion = currentVersion + 1
    store.keyVersions[key] = newVersion

  let ts = store.getCurrentTimestamp()
  return mvccOk(MvccPutResult(
    status: PutStatusOK,
    timestamp: uint64(ts),
    version: newVersion,
    previousValue: if (flags and PutFlagReturnPrev) !=
        0: previousValue else: none(string)
  ))

proc autoDeleteDirect*(store: MvccTransactionStore, key: string):
    MvccResult[MvccDeleteResult] {.gcsafe, raises: [].} =
  ## Single-round auto-commit delete: writes tombstone version key + primary
  ## key in a single raftWriteBatch, bypassing the intent/transaction lifecycle.
  ## Only valid for simple deletes (no return-previous).

  # Allocate commit timestamp (in-memory, no Raft)
  let commitTs = coreTypes.Timestamp(store.txnManager.allocTimestamp())

  # Generate a synthetic TransactionID for MVCC value encoding
  let txnId = coreTypes.genTransactionID(int64(commitTs))

  # Encode version key and tombstone
  let versionKey = encodeVersionKey(key, commitTs)
  let tombstone = mvccTypes.encodeMVCCValue("", commitTs, true, txnId)

  # Build single batch: version key + primary key (tombstone, no intent)
  let puts = @[
    (key: versionKey, value: tombstone),
    (key: key, value: tombstone),
  ]

  let batchRes = store.raftStore.raftWriteBatch(puts, @[])
  if not batchRes.isOk:
    return mvccErr[MvccDeleteResult](MvccStoreError(
      kind: mseStorageError, msg: "Failed to commit direct delete batch: " &
      batchRes.error.msg))

  # Publish the commit to the conflict detection index
  store.txnManager.publishCommit(key, uint64(commitTs))

  return mvccOk(MvccDeleteResult(
    found: true,
    previousValue: none(string)
  ))

proc autoDeleteWithResult*(store: MvccTransactionStore, key: string,
    flags: uint8 = 0): MvccResult[MvccDeleteResult] {.gcsafe, raises: [].} =
  ## Delete with auto-transaction and full result.
  ## Optimization: for simple deletes (no return-previous), uses the
  ## single-round autoDeleteDirect path — reducing 2 Raft rounds to 1.
  const DelFlagReturnPrev = 0x01'u8

  let needPrevious = (flags and DelFlagReturnPrev) != 0

  # Fast path: simple delete — single Raft round via autoDeleteDirect
  if not needPrevious:
    return store.autoDeleteDirect(key)

  var found = false
  var previousValue: Option[string] = none(string)

  # Only perform the expensive Raft scan if caller wants previous value
  let getRes = store.latestGetWithMeta(key)
  if getRes.isOk and getRes.value.isSome:
    found = true
    previousValue = some(getRes.value.get().value)

  if not found:
    return mvccOk(MvccDeleteResult(
      found: false,
      previousValue: none(string)
    ))

  # Perform the delete with auto-transaction (slow path: 2 Raft rounds)
  let res = store.withAutoTransaction(proc(sid: uint64): MvccVoidResult =
    store.txnDelete(sid, key)
  )

  if not res.isOk:
    return mvccErr[MvccDeleteResult](res.error)

  return mvccOk(MvccDeleteResult(
    found: true,
    previousValue: previousValue
  ))

# ---------------------------------------------------------------------------
# Utility procs
# ---------------------------------------------------------------------------

proc getActiveTransactionCount*(store: MvccTransactionStore): int {.gcsafe,
    raises: [].} =
  withLock store.sessionsMu:
    for sessionId, state in store.sessions.pairs:
      if state.txn != nil and state.txn.status == mvccTypes.TXN_PENDING:
        inc result

proc getSessionCount*(store: MvccTransactionStore): int {.gcsafe, raises: [].} =
  withLock store.sessionsMu:
    result = store.sessions.len

# ---------------------------------------------------------------------------
# Wire protocol convenience procs (work with TransactionID directly)
# ---------------------------------------------------------------------------

proc commitTransactionByTxnId*(store: MvccTransactionStore,
    txnId: coreTypes.TransactionID): MvccResult[coreTypes.Timestamp] {.gcsafe,
        raises: [].} =
  ## Commit a transaction by its TransactionID (for wire protocol)
  let sessionIdOpt = store.getSessionIdByTxnId(txnId)
  if sessionIdOpt.isNone:
    return mvccErr[coreTypes.Timestamp](MvccStoreError(
      kind: mseTransactionNotFound, msg: "Transaction not found"))
  store.commitTransaction(sessionIdOpt.get())

proc rollbackTransactionByTxnId*(store: MvccTransactionStore,
    txnId: coreTypes.TransactionID): MvccVoidResult {.gcsafe, raises: [].} =
  ## Rollback a transaction by its TransactionID (for wire protocol)
  let sessionIdOpt = store.getSessionIdByTxnId(txnId)
  if sessionIdOpt.isNone:
    return mvccVErr(MvccStoreError(
      kind: mseTransactionNotFound, msg: "Transaction not found"))
  store.rollbackTransaction(sessionIdOpt.get())

proc closeSessionByTxnId*(store: MvccTransactionStore,
    txnId: coreTypes.TransactionID) {.gcsafe, raises: [].} =
  ## Close a session by its TransactionID (for wire protocol)
  let sessionIdOpt = store.getSessionIdByTxnId(txnId)
  if sessionIdOpt.isSome:
    store.closeSession(sessionIdOpt.get())

proc recordReadByTxnId*(store: MvccTransactionStore,
    txnId: coreTypes.TransactionID, key: string): MvccVoidResult {.gcsafe,
        raises: [].} =
  ## Record a read within a transaction by TransactionID (for wire protocol)
  let sessionIdOpt = store.getSessionIdByTxnId(txnId)
  if sessionIdOpt.isNone:
    return mvccVErr(MvccStoreError(
      kind: mseTransactionNotFound, msg: "Transaction not found"))
  store.recordRead(sessionIdOpt.get(), key)

proc txnGetByTxnId*(store: MvccTransactionStore, txnId: coreTypes.TransactionID,
    key: string): MvccResult[Option[string]] {.gcsafe, raises: [].} =
  ## Get a value within a transaction by TransactionID (for wire protocol)
  let sessionIdOpt = store.getSessionIdByTxnId(txnId)
  if sessionIdOpt.isNone:
    return mvccErr[Option[string]](MvccStoreError(
      kind: mseTransactionNotFound, msg: "Transaction not found"))
  store.txnGet(sessionIdOpt.get(), key)

proc txnPutByTxnId*(store: MvccTransactionStore, txnId: coreTypes.TransactionID,
    key: string, value: string): MvccVoidResult {.gcsafe, raises: [].} =
  ## Put a value within a transaction by TransactionID (for wire protocol)
  let sessionIdOpt = store.getSessionIdByTxnId(txnId)
  if sessionIdOpt.isNone:
    return mvccVErr(MvccStoreError(
      kind: mseTransactionNotFound, msg: "Transaction not found"))
  store.txnPut(sessionIdOpt.get(), key, value)

proc txnDeleteByTxnId*(store: MvccTransactionStore,
    txnId: coreTypes.TransactionID, key: string): MvccVoidResult {.gcsafe,
        raises: [].} =
  ## Delete a value within a transaction by TransactionID (for wire protocol)
  let sessionIdOpt = store.getSessionIdByTxnId(txnId)
  if sessionIdOpt.isNone:
    return mvccVErr(MvccStoreError(
      kind: mseTransactionNotFound, msg: "Transaction not found"))
  store.txnDelete(sessionIdOpt.get(), key)

proc txnGetWithMetaByTxnId*(store: MvccTransactionStore,
    txnId: coreTypes.TransactionID,

key: string): MvccResult[Option[MvccValueWithMeta]] {.gcsafe, raises: [].} =
  ## Get a value with metadata within a transaction by TransactionID (for wire protocol)
  let sessionIdOpt = store.getSessionIdByTxnId(txnId)
  if sessionIdOpt.isNone:
    return mvccErr[Option[MvccValueWithMeta]](MvccStoreError(
      kind: mseTransactionNotFound, msg: "Transaction not found"))
  store.txnGetWithMeta(sessionIdOpt.get(), key)

proc txnPutWithResultByTxnId*(store: MvccTransactionStore,
    txnId: coreTypes.TransactionID, key: string, value: string,
    flags: uint8 = 0, expectedVersion: uint64 = 0): MvccResult[MvccPutResult] {.
    gcsafe, raises: [].} =
  ## Put with result within a transaction by TransactionID (for wire protocol)
  let sessionIdOpt = store.getSessionIdByTxnId(txnId)
  if sessionIdOpt.isNone:
    return mvccErr[MvccPutResult](MvccStoreError(
      kind: mseTransactionNotFound, msg: "Transaction not found"))
  store.txnPutWithResult(sessionIdOpt.get(), key, value, flags, expectedVersion)

proc txnDeleteWithResultByTxnId*(store: MvccTransactionStore,
    txnId: coreTypes.TransactionID, key: string, flags: uint8 = 0): MvccResult[
        MvccDeleteResult] {.
    gcsafe, raises: [].} =
  ## Delete with result within a transaction by TransactionID (for wire protocol)
  let sessionIdOpt = store.getSessionIdByTxnId(txnId)
  if sessionIdOpt.isNone:
    return mvccErr[MvccDeleteResult](MvccStoreError(
      kind: mseTransactionNotFound, msg: "Transaction not found"))
  store.txnDeleteWithResult(sessionIdOpt.get(), key, flags)

proc txnScanByTxnId*(store: MvccTransactionStore,
    txnId: coreTypes.TransactionID, startKey: string, endKey: string,
    limit: uint32 = 0): MvccResult[seq[tuple[key: string, value: string]]] {.
    gcsafe, raises: [].} =
  ## Scan within a transaction by TransactionID (for wire protocol)
  let sessionIdOpt = store.getSessionIdByTxnId(txnId)
  if sessionIdOpt.isNone:
    return mvccErr[seq[tuple[key: string, value: string]]](MvccStoreError(
      kind: mseTransactionNotFound, msg: "Transaction not found"))
  store.txnScan(sessionIdOpt.get(), startKey, endKey, limit)
