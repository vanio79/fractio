# MVCC Transaction Store for Fractio
#
# Provides MVCC semantics for system table operations through the Raft layer.
# Wraps RaftKVStoreExt and adds transaction support with:
#   - Intent-based writes (provisional until commit)
#   - Snapshot reads (consistent view at transaction start time)
#   - Conflict detection on commit
#   - Automatic rollback on abort

import std/[tables, locks, options, atomics, strutils, algorithm, times]
import ../core/types as coreTypes
import ../core/transaction as coreTxn
import ../core/timestamp_provider
import ../storage/mvcc/types as mvccTypes
import ./raft_store
import ./txn_manager
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
  let state = SessionTxnState(
    txn: nil,
    intents: initTable[string, coreTxn.WriteEntry](),
    createdAtNs: getTime().toUnixFloat().int64 * 1_000_000_000,
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

      for key, entry in state.intents.pairs:
        let versionKey = encodeVersionKey(key, commitTs)
        let intentKey = encodeIntentKey(key, state.txn.id)
        if entry.isDelete:
          let tombstone = mvccTypes.encodeMVCCValue("", commitTs, true, state.txn.id)
          discard store.raftStore.raftPut(versionKey, tombstone)
          # Requirement 6: Also put to primary key
          discard store.raftStore.raftPut(key, tombstone)
        else:
          let committedValue = mvccTypes.encodeMVCCValue(entry.value, commitTs,
              false, state.txn.id)
          discard store.raftStore.raftPut(versionKey, committedValue)
          # Requirement 6: Also put to primary key
          discard store.raftStore.raftPut(key, committedValue)
        discard store.raftStore.raftDelete(intentKey)

      state.intents = initTable[string, coreTxn.WriteEntry]()
      state.txn = nil
      return mvccOk(commitTs)

    of TxnCommitConflict:
      state.txn.status = mvccTypes.TXN_ABORTED
      for key, entry in state.intents.pairs:
        discard store.raftStore.raftDelete(encodeIntentKey(key, state.txn.id))
      state.intents = initTable[string, coreTxn.WriteEntry]()
      state.txn = nil
      return mvccErr[coreTypes.Timestamp](MvccStoreError(
        kind: mseConflictDetected, msg: "Transaction conflict detected"))

    of TxnCommitTimeout:
      state.txn.status = mvccTypes.TXN_ABORTED
      for key, entry in state.intents.pairs:
        discard store.raftStore.raftDelete(encodeIntentKey(key, state.txn.id))
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

    for key, entry in state.intents.pairs:
      discard store.raftStore.raftDelete(encodeIntentKey(key, state.txn.id))

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
      includeSystemKeys = true)
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

    let intentKey = encodeIntentKey(key, state.txn.id)
    let intentValue = mvccTypes.encodeMVCCValue("", state.txn.startTimestamp,
        true, state.txn.id)

    let putRes = store.raftStore.raftPut(intentKey, intentValue)
    if not putRes.isOk:
      return mvccVErr(MvccStoreError(
        kind: mseStorageError, msg: "Failed to write delete intent: " &
        putRes.error.msg))

    state.intents[key] = coreTxn.WriteEntry(key: key, value: "", isDelete: true)
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
      includeSystemKeys = true)
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
      100'u32, includeSystemKeys = true)

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
    includeSystemKeys = true)
  if not scanRes.isOk:
    return mvccErr[seq[tuple[key: string, value: string]]](MvccStoreError(
      kind: mseStorageError, msg: "Scan failed"))

  var keyVersions: tables.Table[string, tuple[value: string,
      isDeleted: bool, timestamp: coreTypes.Timestamp]] = initTable[string,
      tuple[value: string, isDeleted: bool, timestamp: coreTypes.Timestamp]]()

  # Pass 1: Collect latest versioned keys
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

  # Pass 2: For keys not in Pass 1, if not an intent or version key, check if likely MVCC.
  # If MVCC and ts <= readTs, add to keyVersions. If not MVCC, add as-is.
  for (k, entry) in scanRes.value:
    if isIntentKeyMvcc(k) or isVersionKey(k): continue
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
      except Exception: uint64(getTime().toUnixFloat() * 1_000_000_000)
    else:
      uint64(getTime().toUnixFloat() * 1_000_000_000)
  coreTypes.Timestamp(wallNs)

# ---------------------------------------------------------------------------
# Convenience: latest reads (no timestamp required)
# ---------------------------------------------------------------------------

proc latestGet*(store: MvccTransactionStore,
    key: string): MvccResult[Option[string]] {.gcsafe, raises: [].} =
  ## Get the latest value for a key without transaction overhead.
  ## Equivalent to snapshotGet with the current timestamp.
  let ts = store.getCurrentTimestamp()
  store.snapshotGet(key, ts)

proc latestScan*(store: MvccTransactionStore,
    startKey: string, endKey: string,
    limit: uint32 = 0): MvccResult[seq[tuple[key: string,
        value: string]]] {.gcsafe, raises: [].} =
  ## Scan the latest values without transaction overhead.
  ## Equivalent to snapshotScan with the current timestamp.
  let ts = store.getCurrentTimestamp()
  store.snapshotScan(startKey, endKey, ts, limit)

# ---------------------------------------------------------------------------
# KV operations with metadata (for protocol server)
# ---------------------------------------------------------------------------

proc latestGetWithMeta*(store: MvccTransactionStore,
    key: string): MvccResult[Option[MvccValueWithMeta]] {.gcsafe, raises: [].} =
  ## Get the latest value for a key with MVCC metadata.
  let ts = store.getCurrentTimestamp()

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
      except:
        discard
    else:
      # Non-MVCC value (backward compat)
      latestValue = val
      found = true

  # Also scan for versioned keys
  let versionPrefix = key & mvccTypes.VERSION_SEPARATOR
  let scanRes = store.raftStore.raftScan(versionPrefix, key & "\x00\x01",
      100'u32, includeSystemKeys = true)

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
        except: discard

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
  ## flags: PutFlagReturnPrev, PutFlagCAS
  const PutFlagReturnPrev = 0x01'u8
  const PutFlagCAS = 0x04'u8

  var previousValue: Option[string] = none(string)
  var currentVersion: uint64 = 0

  # Check current value and version
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
  let newVersion = currentVersion + 1
  withLock store.keyVersionsMu:
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
  ## flags: DelFlagReturnPrev
  const DelFlagReturnPrev = 0x01'u8

  # Check current value
  let getRes = store.latestGetWithMeta(key)
  var found = false
  var previousValue: Option[string] = none(string)

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
    previousValue: if (flags and DelFlagReturnPrev) !=
        0: previousValue else: none(string)
  ))

proc autoPutWithResult*(store: MvccTransactionStore, key: string, value: string,
    flags: uint8 = 0, expectedVersion: uint64 = 0): MvccResult[MvccPutResult] {.
    gcsafe, raises: [].} =
  ## Put with auto-transaction and full result.
  const PutFlagReturnPrev = 0x01'u8
  const PutFlagCAS = 0x04'u8

  var previousValue: Option[string] = none(string)
  var currentVersion: uint64 = 0

  # Check current value and version
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

  # Perform the put with auto-transaction
  let res = store.withAutoTransaction(proc(sid: uint64): MvccVoidResult =
    store.txnPut(sid, key, value)
  )

  if not res.isOk:
    return mvccErr[MvccPutResult](res.error)

  # Increment version
  let newVersion = currentVersion + 1
  withLock store.keyVersionsMu:
    store.keyVersions[key] = newVersion

  let ts = store.getCurrentTimestamp()
  return mvccOk(MvccPutResult(
    status: PutStatusOK,
    timestamp: uint64(ts),
    version: newVersion,
    previousValue: if (flags and PutFlagReturnPrev) !=
        0: previousValue else: none(string)
  ))

proc autoDeleteWithResult*(store: MvccTransactionStore, key: string,
    flags: uint8 = 0): MvccResult[MvccDeleteResult] {.gcsafe, raises: [].} =
  ## Delete with auto-transaction and full result.
  const DelFlagReturnPrev = 0x01'u8

  # Check current value
  let getRes = store.latestGetWithMeta(key)
  var found = false
  var previousValue: Option[string] = none(string)

  if getRes.isOk and getRes.value.isSome:
    found = true
    previousValue = some(getRes.value.get().value)

  if not found:
    return mvccOk(MvccDeleteResult(
      found: false,
      previousValue: none(string)
    ))

  # Perform the delete with auto-transaction
  let res = store.withAutoTransaction(proc(sid: uint64): MvccVoidResult =
    store.txnDelete(sid, key)
  )

  if not res.isOk:
    return mvccErr[MvccDeleteResult](res.error)

  return mvccOk(MvccDeleteResult(
    found: true,
    previousValue: if (flags and DelFlagReturnPrev) !=
        0: previousValue else: none(string)
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
