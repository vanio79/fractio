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
import ../storage/mvcc/types
import ./raft_store
import ./txn_manager
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

# ---------------------------------------------------------------------------
# Key encoding helpers
# ---------------------------------------------------------------------------

proc encodeVersionKey(userKey: string, timestamp: Timestamp): string =
  result = userKey & VERSION_SEPARATOR
  var tsBytes = toBigEndian64(timestamp)
  for i in 0..7:
    result.add(chr(int(tsBytes[i])))

proc encodeIntentKey(userKey: string, txnId: coreTypes.TransactionID): string =
  result = userKey & INTENT_SUFFIX
  var txnBytes = toBigEndian64(int64(txnId))
  for i in 0..7:
    result.add(chr(int(txnBytes[i])))

proc isVersionKey*(key: string): bool =
  # Version key format: <userKey>\x00\x00<8 bytes timestamp>
  # Total length = userKey.len + 10
  # VERSION_SEPARATOR is at positions [key.len - 10, key.len - 9]
  if key.len < 10: return false
  let sepPos = key.len - 10
  result = key[sepPos] == '\x00' and key[sepPos + 1] == '\x00'

proc isIntentKeyMvcc*(key: string): bool =
  # Intent key format: <userKey>\x00\x01<8 bytes txnId>
  # Total length = userKey.len + 10
  # INTENT_SUFFIX is at positions [key.len - 10, key.len - 9]
  if key.len < 10: return false
  let sepPos = key.len - 10
  result = key[sepPos] == '\x00' and key[sepPos + 1] == '\x01'

proc decodeVersionKey(encoded: string): tuple[userKey: string,
    timestamp: Timestamp] =
  # Version key format: <userKey>\x00\x00<8 bytes timestamp>
  # userKey ends at position encoded.len - 10
  if encoded.len < 10:
    raise newException(MVCCError, "Invalid version key: too short")
  let userKeyEnd = encoded.len - 10
  result.userKey = encoded[0 ..< userKeyEnd]
  var tsArr: array[8, uint8]
  for i in 0..7:
    tsArr[i] = uint8(encoded[encoded.len - 8 + i])
  result.timestamp = fromBigEndian64(Timestamp, tsArr)

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
    logger: newLogger("protocol.mvcc_store"),
  )
  initLock(result.sessionsMu)
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
      if state.txn != nil and state.txn.status == TXN_PENDING:
        discard store.txnManager.rollbackTransaction(uint64(state.txn.id))
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

    if state.txn != nil and state.txn.status == TXN_PENDING:
      return mvccOk(state.txn.id)

    let txnRec = store.txnManager.beginTransaction()
    state.txn = coreTxn.MVCCTransaction(
      id: coreTypes.TransactionID(txnRec.id),
      status: TXN_PENDING,
      startTimestamp: coreTypes.Timestamp(txnRec.readTimestamp),
      commitTimestamp: coreTypes.Timestamp(0),
      priority: coreTxn.DEFAULT_PRIORITY,
      writeSet: coreTxn.WriteSet(entries: @[]),
      readSet: coreTxn.ReadSet(keys: @[], timestamps: @[]),
    )
    state.intents = initTable[string, coreTxn.WriteEntry]()
    return mvccOk(state.txn.id)

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

    if state.txn.status != TXN_PENDING:
      return mvccErr[coreTypes.Timestamp](MvccStoreError(
        kind: mseTransactionNotActive, msg: "Transaction is not active"))

    let commitResp = store.txnManager.commitTransaction(uint64(state.txn.id))

    case commitResp.status:
    of TxnCommitOK:
      let commitTs = coreTypes.Timestamp(commitResp.commitTimestamp)
      state.txn.status = TXN_COMMITTED
      state.txn.commitTimestamp = commitTs

      for key, entry in state.intents.pairs:
        let versionKey = encodeVersionKey(key, commitTs)
        let intentKey = encodeIntentKey(key, state.txn.id)
        if entry.isDelete:
          let tombstone = encodeMVCCValue("", commitTs, true, state.txn.id)
          discard store.raftStore.raftPut(versionKey, tombstone)
        else:
          let committedValue = encodeMVCCValue(entry.value, commitTs, false, state.txn.id)
          discard store.raftStore.raftPut(versionKey, committedValue)
        discard store.raftStore.raftDelete(intentKey)

      state.intents = initTable[string, coreTxn.WriteEntry]()
      state.txn = nil
      return mvccOk(commitTs)

    of TxnCommitConflict:
      state.txn.status = TXN_ABORTED
      for key, entry in state.intents.pairs:
        discard store.raftStore.raftDelete(encodeIntentKey(key, state.txn.id))
      state.intents = initTable[string, coreTxn.WriteEntry]()
      state.txn = nil
      return mvccErr[coreTypes.Timestamp](MvccStoreError(
        kind: mseConflictDetected, msg: "Transaction conflict detected"))

    of TxnCommitTimeout:
      state.txn.status = TXN_ABORTED
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

    discard store.txnManager.rollbackTransaction(uint64(state.txn.id))

    for key, entry in state.intents.pairs:
      discard store.raftStore.raftDelete(encodeIntentKey(key, state.txn.id))

    state.txn.status = TXN_ABORTED
    state.intents = initTable[string, coreTxn.WriteEntry]()
    state.txn = nil
    return mvccVOk()

proc getTransactionStatus*(store: MvccTransactionStore,
    sessionId: uint64): MvccResult[MVCCTransactionStatus] {.gcsafe, raises: [].} =
  withLock store.sessionsMu:
    let state = store.sessions.getOrDefault(sessionId)
    if state.isNil:
      return mvccErr[TXN_PENDING](MvccStoreError(
        kind: mseTransactionNotFound, msg: "Session not found"))

    if state.txn == nil:
      return mvccErr[TXN_PENDING](MvccStoreError(
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
          let mvccVal = decodeMVCCValue(entry.value)
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

    if state.txn.status != TXN_PENDING:
      return mvccVErr(MvccStoreError(
        kind: mseTransactionNotActive, msg: "Transaction is not active"))

    let recordRes = store.txnManager.recordWrite(uint64(state.txn.id), key)
    if not recordRes.isOk:
      return mvccVErr(MvccStoreError(
        kind: mseTransactionNotActive, msg: "Failed to record write"))

    let intentKey = encodeIntentKey(key, state.txn.id)
    let intentValue = encodeMVCCValue(value, state.txn.startTimestamp, false, state.txn.id)

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

    if state.txn.status != TXN_PENDING:
      return mvccVErr(MvccStoreError(
        kind: mseTransactionNotActive, msg: "Transaction is not active"))

    let recordRes = store.txnManager.recordWrite(uint64(state.txn.id), key)
    if not recordRes.isOk:
      return mvccVErr(MvccStoreError(
        kind: mseTransactionNotActive, msg: "Failed to record write"))

    let intentKey = encodeIntentKey(key, state.txn.id)
    let intentValue = encodeMVCCValue("", state.txn.startTimestamp, true, state.txn.id)

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
          let mvccVal = decodeMVCCValue(entry.value)
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
      # Non-MVCC key (regular key) - include as-is
      # Only include if no MVCC version exists for this key
      if not keyVersions.hasKey(k):
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
  ## This is optimized for single-statement SELECT queries that don't need
  ## write intent tracking or commit/rollback.
  # First try to get the key directly (for non-MVCC keys)
  let directRes = store.raftStore.raftGet(key)
  var nonMvccValue: Option[string] = none(string)
  if directRes.isOk and directRes.value.isSome:
    nonMvccValue = some(directRes.value.get().value)

  # Also scan for versioned keys
  let versionPrefix = key & VERSION_SEPARATOR
  let scanStart = versionPrefix
  let scanEnd = key & "\x00\x01"

  let scanRes = store.raftStore.raftScan(scanStart, scanEnd, 100'u32,
      includeSystemKeys = true)
  if not scanRes.isOk:
    # If scan fails but we have a non-MVCC value, return it
    if nonMvccValue.isSome:
      return mvccOk(nonMvccValue)
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
          let mvccVal = decodeMVCCValue(entry.value)
          latestVersion = (decoded.timestamp, mvccVal.data, mvccVal.isDeleted)
          foundVersion = true
    except:
      discard

  # If we found an MVCC version, return it (takes precedence over non-MVCC)
  if foundVersion and not latestVersion.isDeleted:
    return mvccOk(some(latestVersion.value))

  # If we found an MVCC version but it's deleted, key doesn't exist
  if foundVersion and latestVersion.isDeleted:
    return mvccOk(none(string))

  # Fall back to non-MVCC value if no MVCC version found
  return mvccOk(nonMvccValue)

proc snapshotScan*(store: MvccTransactionStore,
    startKey: string, endKey: string, readTs: coreTypes.Timestamp,
    limit: uint32 = 0): MvccResult[seq[tuple[key: string, value: string]]] {.
    gcsafe, raises: [].} =
  ## Lightweight scan at a specific timestamp without transaction overhead.
  ## Optimized for single-statement SELECT/SCAN queries.
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
          let mvccVal = decodeMVCCValue(entry.value)
          if not keyVersions.hasKey(decoded.userKey):
            keyVersions[decoded.userKey] = (mvccVal.data, mvccVal.isDeleted,
                decoded.timestamp)
          elif decoded.timestamp > keyVersions[decoded.userKey].timestamp:
            keyVersions[decoded.userKey] = (mvccVal.data, mvccVal.isDeleted,
                decoded.timestamp)
      except:
        discard
    else:
      # Non-MVCC key (regular key) - may have MVCC-encoded value
      # Strip MVCC encoding if present
      var value = entry.value
      var isDeleted = false
      try:
        # Check if value is MVCC-encoded (17+ bytes, not starting with '{')
        if entry.value.len >= 17 and entry.value[0] != '{':
          let mvccVal = decodeMVCCValue(entry.value)
          value = mvccVal.data
          isDeleted = mvccVal.isDeleted
      except:
        discard # Not MVCC-encoded, use as-is
      
      # Only include if no MVCC version exists for this key
      if not isDeleted and not keyVersions.hasKey(k):
        keyVersions[k] = (value, false, coreTypes.Timestamp(0))

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
# Utility procs
# ---------------------------------------------------------------------------

proc getActiveTransactionCount*(store: MvccTransactionStore): int {.gcsafe,
    raises: [].} =
  withLock store.sessionsMu:
    for sessionId, state in store.sessions.pairs:
      if state.txn != nil and state.txn.status == TXN_PENDING:
        inc result

proc getSessionCount*(store: MvccTransactionStore): int {.gcsafe, raises: [].} =
  withLock store.sessionsMu:
    result = store.sessions.len
