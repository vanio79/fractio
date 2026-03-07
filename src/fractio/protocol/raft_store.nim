# Raft-backed KV store for the Fractio protocol layer — Phase 5.
#
# RaftKVStore wraps MultiRaftCoordinator and provides the same interface as
# the in-memory KVStore so that server.nim can switch between them with zero
# handler changes.
#
# Design:
#   - Reads:  served from the Raft state machine's KVStateMachine (local read
#             if leader; will add follower-read / leaseholder reads later).
#   - Writes: proposed as WriteBatch commands via proposeAndWait so they go
#             through Raft consensus before returning to the client.
#   - Scan:   iterates the KVStateMachine's in-memory table (sorted).
#   - Transactions: intents are stored as special prefixed keys in the same
#             WriteBatch; resolveIntent commits or aborts them.
#
# Thread safety:
#   - All public procs are {.gcsafe, raises:[].} and safe to call from
#     any clientLoop thread.
#   - The MultiRaftCoordinator itself protects its state via groupsLock.
#   - Each RaftKVStore has its own versionMu Lock for the version counter.
#
# NOT_LEADER handling:
#   - proposeAndWait returns RaftResult(success=false) when the local node is
#     not the leader.  RaftKVStore surfaces this as a RaftStoreError with
#     kind rseNotLeader so that server.nim can send ErrNotLeader to the client.
#
# Key-space layout in the state machine:
#   - User keys:    stored as-is (string)
#   - Intent keys:  "\x00INTENT\x00<txnId8be><userKey>"  (for mvcc intents)
#   - 2PC records:  "\x00COORD\x00<txnId8be>"            (coordinator records)

import std/[tables, locks, options, algorithm, atomics, times, strformat, strutils]
import fractio/distributed/raft/multigroup_coordinator
import fractio/distributed/raft/multigroup_types
import fractio/distributed/range/types as rangeTypes
import fractio/distributed/raft/state_machine
import fractio/storage/wisckey_backend
import fractio/storage/backend
import ../utils/logging

# ---------------------------------------------------------------------------
# Error type
# ---------------------------------------------------------------------------

type
  RaftStoreErrorKind* = enum
    rseNotLeader     ## This node is not the Raft leader for the shard
    rseRangeNotFound ## No Raft group for the requested shard/range
    rseTimeout       ## proposeAndWait timed out
    rseInternal      ## Unexpected error

  RaftStoreError* = object
    kind*: RaftStoreErrorKind
    msg*: string
    leaderHint*: uint32 ## nodeId of the known leader (for rseNotLeader)

proc newRSE(kind: RaftStoreErrorKind, msg: string,
    hint: uint32 = 0): RaftStoreError =
  RaftStoreError(kind: kind, msg: msg, leaderHint: hint)

# ---------------------------------------------------------------------------
# Result helpers (mirrors protocol/types.nim pattern)
# ---------------------------------------------------------------------------

type
  RSResult*[T] = object
    case isOk*: bool
    of true:
      value*: T
    of false:
      error*: RaftStoreError

proc rsOk*[T](v: T): RSResult[T] = RSResult[T](isOk: true, value: v)
proc rsErr*[T](e: RaftStoreError): RSResult[T] = RSResult[T](isOk: false, error: e)

# Void variant
type RSVoidResult* = object
  case isOk*: bool
  of true: discard
  of false:
    error*: RaftStoreError

proc rsVOk*(): RSVoidResult = RSVoidResult(isOk: true)
proc rsVErr*(e: RaftStoreError): RSVoidResult = RSVoidResult(isOk: false, error: e)

# ---------------------------------------------------------------------------
# KV entry
# ---------------------------------------------------------------------------

type
  RaftKVEntry* = object
    value*: string
    version*: uint64
    timestamp*: uint64 ## nanoseconds since epoch

# ---------------------------------------------------------------------------
# Intent key encoding (for transactional writes stored in the Raft state machine)
# ---------------------------------------------------------------------------
# Format: "\x00INTENT\x00" + txnId (8 bytes big-endian) + userKey
const INTENT_PREFIX* = "\x00INTENT\x00"
const COORD_PREFIX* = "\x00COORD\x00"

proc encodeIntentKey*(txnId: uint64, userKey: string): string {.inline.} =
  var buf = INTENT_PREFIX
  for i in countdown(7, 0):
    buf.add(chr(int((txnId shr (i * 8)) and 0xFF)))
  buf.add(userKey)
  buf

proc encodeCoordKey*(txnId: uint64): string {.inline.} =
  var buf = COORD_PREFIX
  for i in countdown(7, 0):
    buf.add(chr(int((txnId shr (i * 8)) and 0xFF)))
  buf

proc isIntentKey*(k: string): bool {.inline.} =
  k.len > INTENT_PREFIX.len and k[0 ..< INTENT_PREFIX.len] == INTENT_PREFIX

proc isCoordKey*(k: string): bool {.inline.} =
  k.len > COORD_PREFIX.len and k[0 ..< COORD_PREFIX.len] == COORD_PREFIX

proc decodeIntentTxnId*(k: string): uint64 {.inline.} =
  ## Extract the txnId from an intent key.
  let off = INTENT_PREFIX.len
  result = 0
  for i in 0 ..< 8:
    result = (result shl 8) or uint64(uint8(k[off + i]))

proc decodeIntentUserKey*(k: string): string {.inline.} =
  k[INTENT_PREFIX.len + 8 ..< k.len]

# ---------------------------------------------------------------------------
# Shard mapping helper
# ---------------------------------------------------------------------------
# Maps a string key to a RangeID by looking up the RouterTable shard list.
# RaftKVStore holds a flat seq of (startKey, endKey, rangeId) tuples.

type
  ShardEntry* = object
    startKey*: string
    endKey*: string
    rangeId*: RangeID

# ---------------------------------------------------------------------------
# RaftKVStore
# ---------------------------------------------------------------------------

type
  RaftKVStore* = ref object of RootObj
    coordinator*: MultiRaftCoordinator
    shards*: seq[ShardEntry] ## sorted by startKey ascending
    shardsMu*: Lock
    nextVersion*: Atomic[uint64]
    proposeTimeout*: int     ## ms; default 5000
    logger*: Logger

proc newRaftKVStore*(coord: MultiRaftCoordinator,
    proposeTimeoutMs: int = 5000): RaftKVStore =
  result = RaftKVStore(
    coordinator: coord,
    shards: @[],
    proposeTimeout: proposeTimeoutMs,
    logger: newLogger("protocol.raft_store"),
  )
  initLock(result.shardsMu)
  result.nextVersion.store(1)

# ---------------------------------------------------------------------------
# Shard management
# ---------------------------------------------------------------------------

proc addShard*(store: RaftKVStore, startKey, endKey: string,
    rangeId: RangeID) {.gcsafe, raises: [].} =
  ## Register a shard range mapping. Must be called before using the store.
  ## Ranges must be non-overlapping; kept sorted by startKey.
  acquire(store.shardsMu)
  defer: release(store.shardsMu)
  store.shards.add(ShardEntry(startKey: startKey, endKey: endKey,
      rangeId: rangeId))
  store.shards.sort(proc(a, b: ShardEntry): int = cmp(a.startKey, b.startKey))

proc bootstrapSingleShard*(store: RaftKVStore, rangeId: RangeID) {.gcsafe,
    raises: [].} =
  ## Set up a single shard covering the entire keyspace. Suitable for
  ## single-node / test deployments.
  store.addShard("", "", rangeId)

proc findRangeId*(store: RaftKVStore, key: string): Option[RangeID] {.gcsafe,
    raises: [].} =
  ## Returns the RangeID whose shard covers `key`, or none.
  acquire(store.shardsMu)
  defer: release(store.shardsMu)
  for entry in store.shards:
    let afterStart = entry.startKey.len == 0 or key >= entry.startKey
    let beforeEnd = entry.endKey.len == 0 or key < entry.endKey
    if afterStart and beforeEnd:
      return some(entry.rangeId)
  none(RangeID)

# ---------------------------------------------------------------------------
# Internal: get KVStateMachine for a RangeID
# ---------------------------------------------------------------------------

proc getKVSM(store: RaftKVStore,
    rangeId: RangeID): Option[KVStateMachine] {.gcsafe, raises: [].} =
  ## Retrieve the KVStateMachine from the coordinator's state machine registry.
  ## In the current MultiRaftCoordinator implementation the state machine is
  ## tracked inside the coordinator.  We expose it via a helper stored in the
  ## coordinator's logs table (KVStateMachine is attached there).
  ##
  ## Note: The upstream MultiRaftCoordinator does not yet maintain a separate
  ## state machine registry.  We keep our own stateMachines table (one per
  ## RangeID) inside RaftKVStore and apply committed entries there.
  none(KVStateMachine) # See stateMachines below

# ---------------------------------------------------------------------------
# State machine registry (owned by RaftKVStore)
# ---------------------------------------------------------------------------
# The MultiRaftCoordinator worker thread commits log entries via the group's
# commitIndex.  To keep Phase 5 self-contained we maintain a parallel
# KVStateMachine per shard here, and apply WriteBatch proposals to it after
# proposeAndWait succeeds.
#
# In a full production implementation the Raft worker would drive the state
# machine directly; for Phase 5 the single-node path is:
#   1. proposeAndWait → coordinator writes to log, commits (quorum = 1)
#   2. RaftKVStore applies the same batch to the KVStateMachine locally
# This is safe for single-node because there is only one writer.

type
  RaftKVStoreExt* = ref object of RaftKVStore
    stateMachines*: Table[RangeID, KVStateMachine]
    smMu*: Lock

proc newRaftKVStoreExt*(coord: MultiRaftCoordinator,
    proposeTimeoutMs: int = 5000): RaftKVStoreExt =
  result = RaftKVStoreExt(
    coordinator: coord,
    shards: @[],
    proposeTimeout: proposeTimeoutMs,
    logger: newLogger("protocol.raft_store"),
    stateMachines: initTable[RangeID, KVStateMachine](),
  )
  initLock(result.shardsMu)
  initLock(result.smMu)
  result.nextVersion.store(1)

proc getOrCreateSM*(store: RaftKVStoreExt,
    rangeId: RangeID): KVStateMachine {.gcsafe, raises: [].} =
  acquire(store.smMu)
  defer: release(store.smMu)
  if store.stateMachines.hasKey(rangeId):
    return store.stateMachines.getOrDefault(rangeId)
  let sm = newKVStateMachine()
  store.stateMachines[rangeId] = sm
  sm

proc addShardExt*(store: RaftKVStoreExt, startKey, endKey: string,
    rangeId: RangeID) {.gcsafe, raises: [].} =
  store.addShard(startKey, endKey, rangeId)
  discard store.getOrCreateSM(rangeId) # pre-create

proc bootstrapSingleShardExt*(store: RaftKVStoreExt,
    rangeId: RangeID) {.gcsafe, raises: [].} =
  store.bootstrapSingleShard(rangeId)
  discard store.getOrCreateSM(rangeId)

# ---------------------------------------------------------------------------
# Internal: propose a WriteBatch and apply to local state machine
# ---------------------------------------------------------------------------

# Helper: string → seq[byte]
proc toBytes(s: string): seq[byte] {.inline.} =
  result = newSeq[byte](s.len)
  for i in 0 ..< s.len:
    result[i] = byte(s[i])

# Helper: seq[byte] → string (safe copy)
proc fromBytes(b: seq[byte]): string {.inline.} =
  result = newString(b.len)
  for i in 0 ..< b.len:
    result[i] = char(b[i])

# ---------------------------------------------------------------------------
# Follower apply callback (called by coordinator on committed entries)
# ---------------------------------------------------------------------------

proc applyBatchToSM*(storePtr: pointer, rid: RangeID,
    batch: WriteBatch) {.gcsafe, raises: [].} =
  ## Callback registered with the coordinator so that follower nodes can apply
  ## committed WriteBatch entries to their local KVStateMachine.
  ## `storePtr` is a raw `pointer` cast from `RaftKVStoreExt` to break
  ## the raft_store → coordinator → raft_store circular import.
  ##
  ## Write-through: every committed entry is also persisted to the WiscKey
  ## backend (opened with syncWrites=true → fdatasync per batch) so that
  ## committed data survives a crash.  This makes the hot path comparable to
  ## PostgreSQL/MySQL/SQLite which all fsync on every commit.
  if storePtr == nil: return
  let store = cast[RaftKVStoreExt](storePtr)
  let sm = store.getOrCreateSM(rid)

  # --- Persist to WiscKey (single fdatasync for entire batch) ---
  # Uses LevelDB's native WriteBatch API so all puts+deletes are flushed
  # in one atomic fdatasync instead of one fdatasync per key.
  let backend = store.coordinator.store
  if backend != nil and backend.isOpen:
    {.cast(raises: []).}:
      var pairs: seq[KeyValuePair] = @[]
      var delKeys: seq[string] = @[]
      for (k, v) in batch.puts:
        pairs.add((key: fromBytes(k), value: fromBytes(v)))
      for k in batch.deletes:
        delKeys.add(fromBytes(k))
      discard backend.writeBatch(pairs, delKeys)

  # --- Update in-memory state machine (for fast reads) ---
  acquire(store.smMu)
  defer: release(store.smMu)
  for (k, v) in batch.puts:
    sm.kvStore[fromBytes(k)] = fromBytes(v)
  for k in batch.deletes:
    sm.kvStore.del(fromBytes(k))

proc wireApplyCallback*(store: RaftKVStoreExt) {.gcsafe, raises: [].} =
  ## Wire the applyBatchToSM callback into the coordinator so that committed
  ## log entries are applied to the local KV state machine on followers.
  ## Call this once after newRaftKVStoreExt() and before coordinator.start().
  {.cast(gcsafe).}: {.cast(raises: []).}:
    multigroup_coordinator.applyBatchCallback = applyBatchToSM
  store.coordinator.kvStorePtr = cast[pointer](store)

proc proposeWrite(store: RaftKVStoreExt, rangeId: RangeID,
    batch: WriteBatch): RSVoidResult {.gcsafe, raises: [].} =
  ## Propose a write batch to Raft and, on success, apply it locally.
  let cmd = RaftCommand(kind: ckWrite, writeBatch: batch)
  let res = store.coordinator.proposeAndWait(rangeId, cmd,
      store.proposeTimeout)
  if not res.success:
    if res.error == "Not the leader":
      return rsVErr(newRSE(rseNotLeader, res.error))
    if res.error.len > 0 and res.error.contains("Range not found"):
      return rsVErr(newRSE(rseRangeNotFound, res.error))
    if res.error.contains("Timeout"):
      return rsVErr(newRSE(rseTimeout, res.error))
    return rsVErr(newRSE(rseInternal, res.error))

  # Apply to local state machine under smMu to serialise with readers
  let sm = store.getOrCreateSM(rangeId)
  acquire(store.smMu)
  defer: release(store.smMu)
  for (k, v) in batch.puts:
    let ks = fromBytes(k)
    let vs = fromBytes(v)
    sm.kvStore[ks] = vs
  for k in batch.deletes:
    let ks = fromBytes(k)
    sm.kvStore.del(ks)

  rsVOk()

# ---------------------------------------------------------------------------
# Public KV interface (drop-in for KVStore in server.nim)
# ---------------------------------------------------------------------------

proc raftGet*(store: RaftKVStoreExt,
    key: string): RSResult[Option[RaftKVEntry]] {.gcsafe, raises: [].} =
  ## Read the current value for `key` from the local state machine.
  ## Reads are served locally (leader / leaseholder read semantics).
  let ridOpt = store.findRangeId(key)
  if ridOpt.isNone:
    return rsErr[Option[RaftKVEntry]](newRSE(rseRangeNotFound,
        &"no shard for key '{key}'"))

  let sm = store.getOrCreateSM(ridOpt.get())
  acquire(store.smMu)
  defer: release(store.smMu)

  # Skip intent keys on plain reads
  if isIntentKey(key) or isCoordKey(key):
    return rsOk[Option[RaftKVEntry]](none(RaftKVEntry))

  if sm.kvStore.hasKey(key):
    let v = sm.kvStore.getOrDefault(key)
    let entry = RaftKVEntry(
      value: v,
      version: 1'u64, # TODO: track versions per key when needed
      timestamp: uint64(getTime().toUnixFloat() * 1_000_000_000),
    )
    return rsOk[Option[RaftKVEntry]](some(entry))

  rsOk[Option[RaftKVEntry]](none(RaftKVEntry))

proc raftPut*(store: RaftKVStoreExt, key, value: string): RSResult[
    RaftKVEntry] {.gcsafe, raises: [].} =
  ## Write `value` under `key` through Raft consensus.
  let ridOpt = store.findRangeId(key)
  if ridOpt.isNone:
    return rsErr[RaftKVEntry](newRSE(rseRangeNotFound,
        &"no shard for key '{key}'"))

  let batch = newWriteBatch()
  batch.put(toBytes(key), toBytes(value))

  let vr = proposeWrite(store, ridOpt.get(), batch)
  if not vr.isOk:
    return rsErr[RaftKVEntry](vr.error)

  let ver = store.nextVersion.fetchAdd(1)
  let ts = uint64(getTime().toUnixFloat() * 1_000_000_000)
  rsOk[RaftKVEntry](RaftKVEntry(value: value, version: ver, timestamp: ts))

proc raftDelete*(store: RaftKVStoreExt,
    key: string): RSResult[Option[RaftKVEntry]] {.gcsafe, raises: [].} =
  ## Delete `key` through Raft consensus.
  let ridOpt = store.findRangeId(key)
  if ridOpt.isNone:
    return rsErr[Option[RaftKVEntry]](newRSE(rseRangeNotFound,
        &"no shard for key '{key}'"))

  # Capture previous value first (under smMu)
  var prevEntry: Option[RaftKVEntry]
  let sm = store.getOrCreateSM(ridOpt.get())
  acquire(store.smMu)
  if sm.kvStore.hasKey(key):
    prevEntry = some(RaftKVEntry(
      value: sm.kvStore.getOrDefault(key),
      version: 1'u64,
      timestamp: uint64(getTime().toUnixFloat() * 1_000_000_000),
    ))
  release(store.smMu)

  let batch = newWriteBatch()
  batch.delete(toBytes(key))

  let vr = proposeWrite(store, ridOpt.get(), batch)
  if not vr.isOk:
    return rsErr[Option[RaftKVEntry]](vr.error)

  rsOk[Option[RaftKVEntry]](prevEntry)

proc raftScan*(store: RaftKVStoreExt, startKey, endKey: string,
    limit: uint32): RSResult[seq[(string, RaftKVEntry)]] {.gcsafe, raises: [].} =
  ## Scan keys in [startKey, endKey) up to `limit` results.
  ## Aggregates across all shards whose ranges overlap the query span.
  var pairs: seq[(string, RaftKVEntry)] = @[]

  acquire(store.shardsMu)
  let shardsCopy = store.shards
  release(store.shardsMu)

  for entry in shardsCopy:
    let sm = store.getOrCreateSM(entry.rangeId)
    acquire(store.smMu)
    for k, v in sm.kvStore:
      # Skip internal intent / coord keys
      if isIntentKey(k) or isCoordKey(k): continue
      let afterStart = startKey.len == 0 or k >= startKey
      let beforeEnd = endKey.len == 0 or k < endKey
      if afterStart and beforeEnd:
        pairs.add((k, RaftKVEntry(value: v, version: 1'u64,
            timestamp: uint64(getTime().toUnixFloat() * 1_000_000_000))))
    release(store.smMu)

  pairs.sort(proc(a, b: (string, RaftKVEntry)): int = cmp(a[0], b[0]))
  if limit > 0 and pairs.len > int(limit):
    pairs.setLen(int(limit))
  rsOk[seq[(string, RaftKVEntry)]](pairs)

proc raftLen*(store: RaftKVStoreExt): int {.gcsafe, raises: [].} =
  var total = 0
  acquire(store.shardsMu)
  let shardsCopy = store.shards
  release(store.shardsMu)
  for entry in shardsCopy:
    let sm = store.getOrCreateSM(entry.rangeId)
    acquire(store.smMu)
    for k in sm.kvStore.keys:
      if not isIntentKey(k) and not isCoordKey(k): inc total
    release(store.smMu)
  total

# ---------------------------------------------------------------------------
# Transactional intent API
# ---------------------------------------------------------------------------

proc raftBufferIntent*(store: RaftKVStoreExt, txnId: uint64, key,
    value: string): RSVoidResult {.gcsafe, raises: [].} =
  ## Stage a transactional write intent directly into WiscKey WITHOUT fsync
  ## and WITHOUT going through the Raft log.
  ##
  ## Rationale: an intent is not a committed value.  If the server crashes
  ## before the transaction commits the intent is lost — which is exactly
  ## correct MVCC behaviour (the txn is treated as aborted).  We therefore
  ## only need the intent to be visible in LevelDB's memtable (for reads
  ## within the same transaction) and do not need durability until commit.
  ##
  ## The commit path (raftResolveIntent) writes the real key through Raft
  ## with fdatasync, which is the only fsync this transaction requires.
  let intentKey = encodeIntentKey(txnId, key)
  let ridOpt = store.findRangeId(key)
  if ridOpt.isNone:
    return rsVErr(newRSE(rseRangeNotFound, &"no shard for key '{key}'"))

  let backend = store.coordinator.store
  if backend != nil and backend.isOpen:
    {.cast(raises: []).}:
      let pairs: seq[KeyValuePair] = @[(key: intentKey, value: value)]
      discard backend.writeBatchNoSync(pairs, @[])

  # Also update the in-memory state machine so reads-your-own-writes work
  let sm = store.getOrCreateSM(ridOpt.get())
  acquire(store.smMu)
  sm.kvStore[intentKey] = value
  release(store.smMu)

  rsVOk()

proc raftDeleteIntent*(store: RaftKVStoreExt, txnId: uint64,
    key: string): RSVoidResult {.gcsafe, raises: [].} =
  ## Remove the intent (used during rollback or abort) — also no fsync needed.
  let intentKey = encodeIntentKey(txnId, key)
  let ridOpt = store.findRangeId(key)
  if ridOpt.isNone:
    return rsVErr(newRSE(rseRangeNotFound, &"no shard for key '{key}'"))

  let backend = store.coordinator.store
  if backend != nil and backend.isOpen:
    {.cast(raises: []).}:
      discard backend.writeBatchNoSync(@[], @[intentKey])

  let sm = store.getOrCreateSM(ridOpt.get())
  acquire(store.smMu)
  sm.kvStore.del(intentKey)
  release(store.smMu)

  rsVOk()

proc raftPutIntent*(store: RaftKVStoreExt, txnId: uint64, key,
    value: string): RSVoidResult {.gcsafe, raises: [].} =
  ## Alias kept for raft_txn.nim compatibility; routes to raftBufferIntent.
  raftBufferIntent(store, txnId, key, value)

proc raftCommitTxn*(store: RaftKVStoreExt, txnId: uint64,
    writeSet: seq[string]): RSVoidResult {.gcsafe, raises: [].} =
  ## Commit a transaction by resolving all intents in a single Raft WriteBatch.
  ## For each key in writeSet:
  ##   - reads intent value from the in-memory state machine
  ##   - adds (realKey → value) to the batch
  ##   - adds (intentKey) to the delete list
  ## Then proposes the whole batch through Raft → ONE fdatasync total.
  ## If no intents are found (e.g. read-only txn) succeeds immediately.
  if writeSet.len == 0:
    return rsVOk()

  # All keys must belong to the same shard (single-shard assumption for now).
  # Use the first key to determine the rangeId.
  let ridOpt = store.findRangeId(writeSet[0])
  if ridOpt.isNone:
    return rsVErr(newRSE(rseRangeNotFound,
        &"no shard for key '{writeSet[0]}'"))
  let rid = ridOpt.get()
  let sm = store.getOrCreateSM(rid)

  let batch = newWriteBatch()

  acquire(store.smMu)
  for key in writeSet:
    let intentKey = encodeIntentKey(txnId, key)
    if sm.kvStore.hasKey(intentKey):
      let val = sm.kvStore.getOrDefault(intentKey)
      batch.put(toBytes(key), toBytes(val))
      batch.delete(toBytes(intentKey))
    # If intent not found (key was never actually written), skip silently.
  release(store.smMu)

  if batch.isEmpty:
    # Nothing to persist — transaction had no actual writes that left intents
    return rsVOk()

  proposeWrite(store, rid, batch)

proc raftResolveIntent*(store: RaftKVStoreExt, txnId: uint64,
    key: string, commit: bool,
    commitValue: string = ""): RSVoidResult {.gcsafe, raises: [].} =
  ## On commit: move the intent to the committed slot.
  ## On abort:  delete the intent.
  let intentKey = encodeIntentKey(txnId, key)
  let ridOpt = store.findRangeId(key)
  if ridOpt.isNone:
    return rsVErr(newRSE(rseRangeNotFound, &"no shard for key '{key}'"))

  let batch = newWriteBatch()
  if commit:
    # Write the committed value under the real key; delete the intent
    batch.put(toBytes(key), toBytes(commitValue))
    batch.delete(toBytes(intentKey))
  else:
    batch.delete(toBytes(intentKey))
  proposeWrite(store, ridOpt.get(), batch)

# ---------------------------------------------------------------------------
# 2PC coordinator record API
# ---------------------------------------------------------------------------

proc raftWriteCoordRecord*(store: RaftKVStoreExt, txnId: uint64,
    payload: string): RSVoidResult {.gcsafe, raises: [].} =
  ## Write a durable 2PC coordinator record to the local Raft log.
  ## Used to ensure coordinator recovery after crash (Phase 5).
  let coordKey = encodeCoordKey(txnId)
  # Coordinator records go into the first (local) shard.
  acquire(store.shardsMu)
  let firstRid = if store.shards.len > 0: store.shards[0].rangeId
                 else: RangeID(0)
  release(store.shardsMu)
  if firstRid.uint64 == 0:
    return rsVErr(newRSE(rseRangeNotFound, "no shards registered"))
  let batch = newWriteBatch()
  batch.put(toBytes(coordKey), toBytes(payload))
  proposeWrite(store, firstRid, batch)

proc raftDeleteCoordRecord*(store: RaftKVStoreExt,
    txnId: uint64): RSVoidResult {.gcsafe, raises: [].} =
  ## Remove the coordinator record after a 2PC round completes.
  let coordKey = encodeCoordKey(txnId)
  acquire(store.shardsMu)
  let firstRid = if store.shards.len > 0: store.shards[0].rangeId
                 else: RangeID(0)
  release(store.shardsMu)
  if firstRid.uint64 == 0:
    return rsVErr(newRSE(rseRangeNotFound, "no shards registered"))
  let batch = newWriteBatch()
  batch.delete(toBytes(coordKey))
  proposeWrite(store, firstRid, batch)

proc raftReadCoordRecord*(store: RaftKVStoreExt,
    txnId: uint64): Option[string] {.gcsafe, raises: [].} =
  ## Read back a coordinator record (for recovery).
  let coordKey = encodeCoordKey(txnId)
  acquire(store.shardsMu)
  let firstRid = if store.shards.len > 0: store.shards[0].rangeId
                 else: RangeID(0)
  release(store.shardsMu)
  if firstRid.uint64 == 0: return none(string)
  let sm = store.getOrCreateSM(firstRid)
  acquire(store.smMu)
  defer: release(store.smMu)
  if sm.kvStore.hasKey(coordKey):
    return some(sm.kvStore.getOrDefault(coordKey))
  none(string)
