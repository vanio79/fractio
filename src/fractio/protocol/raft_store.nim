# Raft-backed KV store for the Fractio protocol layer — Phase 5.
#
# RaftKVStore wraps MultiRaftCoordinator and provides the same interface as
# the in-memory KVStore so that server.nim can switch between them with zero
# handler changes.
#
# Design:
#   - Reads:  served from the WiscKey/LevelDB backend directly (no in-memory
#             mirror). LevelDB's memtable + block cache handle read performance.
#   - Writes: proposed as WriteBatch commands via proposeAndWait so they go
#             through Raft consensus before returning to the client.
#   - Scan:   uses WiscKey backend.scan() for range queries.
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

import std/[tables, locks, options, algorithm, atomics, times, strformat, strutils, json, hashes]
import fractio/distributed/raft/multigroup_coordinator
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/raft/state_machine
import fractio/distributed/meta/system_tables
import fractio/storage/wisckey_backend
import fractio/storage/backend
import ../utils/logging

# ---------------------------------------------------------------------------
# Error type
# ---------------------------------------------------------------------------

type
  RaftStoreErrorKind* = enum
    rseNotLeader     ## This node is not the Raft leader for the shard
    rseGroupNotFound ## No Raft group for the requested shard/group
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
# Unified key → GroupID routing
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# RaftKVStore
# ---------------------------------------------------------------------------

type
  RaftKVStore* {.acyclic.} = ref object of RootObj
    coordinator*: MultiRaftCoordinator
    nextVersion*: Atomic[uint64]
    proposeTimeout*: int     ## ms; default 5000
    logger*: Logger

proc newRaftKVStore*(coord: MultiRaftCoordinator,
    proposeTimeoutMs: int = 5000): RaftKVStore =
  result = RaftKVStore(
    coordinator: coord,
    proposeTimeout: proposeTimeoutMs,
    logger: newLogger("protocol.raft_store"),
  )
  result.nextVersion.store(1)

# ---------------------------------------------------------------------------
# Internal: get KVStateMachine for a GroupID
# ---------------------------------------------------------------------------

proc getKVSM(store: RaftKVStore,
    groupId: GroupID): Option[KVStateMachine] {.gcsafe, raises: [].} =
  ## Legacy stub — kept for compatibility. State machines are lightweight
  ## index trackers only; all data reads go through the WiscKey backend.
  none(KVStateMachine)

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
  SpaceInfo* = object
    spaceId*: int
    name*: string
    replicas*: int        ## 0 = ALL nodes
    groupIds*: seq[uint64]
    oldGroupIds*: seq[uint64]      ## previous groups during rebalance
    rebalancing*: bool             ## true while migration is in progress
    rebalanceWorker*: int          ## nodeId of the migrating worker
    rebalanceHeartbeat*: int64     ## unix epoch seconds of last worker heartbeat
    rebalanceCursor*: string       ## last key migrated (resume point)

  RaftKVStoreExt* = ref object of RaftKVStore
    stateMachines*: Table[GroupID, KVStateMachine]  ## lightweight index tracking only
    smMu*: Lock  ## guards stateMachines table
    spaces*: Table[int, SpaceInfo]  ## spaceId → SpaceInfo
    tableSpaces*: Table[uint32, int] ## tableId → spaceId
    spacesMu*: Lock
    peerStores*: Table[uint32, RaftKVStoreExt]  ## nodeId → peer store for forwarding
    groupMembers*: Table[GroupID, seq[uint32]]   ## groupId → member nodeIds
    preferredLeaders*: Table[GroupID, uint32]     ## groupId → preferred leader nodeId



proc newRaftKVStoreExt*(coord: MultiRaftCoordinator,
    proposeTimeoutMs: int = 5000): RaftKVStoreExt =
  result = RaftKVStoreExt(
    coordinator: coord,
    proposeTimeout: proposeTimeoutMs,
    logger: newLogger("protocol.raft_store"),
    stateMachines: initTable[GroupID, KVStateMachine](),
    spaces: initTable[int, SpaceInfo](),
    tableSpaces: initTable[uint32, int](),
    peerStores: initTable[uint32, RaftKVStoreExt](),
    groupMembers: initTable[GroupID, seq[uint32]](),
    preferredLeaders: initTable[GroupID, uint32](),
  )
  initLock(result.smMu)
  initLock(result.spacesMu)
  result.nextVersion.store(1)

proc resolveGroupId*(store: RaftKVStoreExt, key: string): Option[GroupID] {.gcsafe,
    raises: [].} =
  ## Unified key → GroupID routing. Meta/system keys go to META_GROUP_ID,
  ## everything else goes to DATA_GROUP_START_ID (the default data group).
  ## Space-routed keys bypass this via raftPutInSpace/raftGetInSpace/etc.
  if isMetaGroupKey(key):
    return some(META_GROUP_ID)
  some(DATA_GROUP_START_ID)

proc getOrCreateSM*(store: RaftKVStoreExt,
    groupId: GroupID): KVStateMachine {.gcsafe, raises: [].} =
  acquire(store.smMu)
  defer: release(store.smMu)
  if store.stateMachines.hasKey(groupId):
    return store.stateMachines.getOrDefault(groupId)
  let sm = newKVStateMachine()
  store.stateMachines[groupId] = sm
  sm

proc registerGroup*(store: RaftKVStoreExt,
    groupId: GroupID) {.gcsafe, raises: [].} =
  ## Pre-create a state machine for the given group.
  discard store.getOrCreateSM(groupId)

proc addPeerStore*(store: RaftKVStoreExt, nodeId: uint32,
    peer: RaftKVStoreExt) {.gcsafe, raises: [].} =
  ## Register a peer node's store for forwarding writes/reads to groups
  ## that this node doesn't own.
  store.peerStores[nodeId] = peer

proc getBackend*(store: RaftKVStoreExt): WiscKeyBackend {.inline.} =
  ## Return the WiscKey backend from the coordinator's store.
  store.coordinator.store

proc loadGroupMembers*(store: RaftKVStoreExt) {.gcsafe, raises: [].} =
  ## Scan sys.groups and populate the groupMembers table (groupId → member nodeIds).
  let startKey = "/t/" & align($SYS_GROUPS_TABLE_ID, 10, '0') & "/"
  let endKey = "/t/" & align($(SYS_GROUPS_TABLE_ID + 1), 10, '0') & "/"
  let backend = store.getBackend()
  var entries: seq[KeyValuePair] = @[]
  if backend != nil and backend.isOpen:
    {.cast(raises: []).}:
      entries = backend.scan(startKey, endKey)

  store.groupMembers.clear()
  store.preferredLeaders.clear()
  for (k, v) in entries:
    {.cast(raises: []).}:
      try:
        let j = parseJson(v)
        let gid = GroupID(uint64(j["groupId"].getInt()))
        var members: seq[uint32] = @[]
        if j.hasKey("replicas"):
          for r in j["replicas"]:
            members.add(uint32(r["nodeId"].getInt()))
        store.groupMembers[gid] = members
        if j.hasKey("preferredLeader"):
          let pl = uint32(j["preferredLeader"].getInt())
          if pl > 0:
            store.preferredLeaders[gid] = pl
      except:
        discard

proc findPeerForGroup(store: RaftKVStoreExt,
    groupId: GroupID): Option[RaftKVStoreExt] {.gcsafe, raises: [].} =
  ## Find a peer store that owns the given group.
  ## Prefers a peer whose coordinator has the group and is leader.
  if not store.groupMembers.hasKey(groupId):
    return none(RaftKVStoreExt)
  let members = store.groupMembers.getOrDefault(groupId)
  {.cast(raises: []).}:
    # First pass: find a peer that is the leader for this group
    for nid in members:
      if store.peerStores.hasKey(nid):
        let peer = store.peerStores.getOrDefault(nid)
        if peer.coordinator.hasGroup(groupId):
          let g = peer.coordinator.getGroup(groupId)
          if g.isSome and g.get.isLeader():
            return some(peer)
    # Second pass: find any peer that has the group
    for nid in members:
      if store.peerStores.hasKey(nid):
        let peer = store.peerStores.getOrDefault(nid)
        if peer.coordinator.hasGroup(groupId):
          return some(peer)
  none(RaftKVStoreExt)

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

proc applyBatchToSM*(storePtr: pointer, rid: GroupID,
    batch: WriteBatch) {.gcsafe, raises: [].} =
  ## Callback registered with the coordinator so that committed WriteBatch
  ## entries are applied to the local KVStateMachine and persisted to the
  ## WiscKey backend.  `storePtr` is a raw `pointer` cast from `RaftKVStoreExt`
  ## to break the raft_store → coordinator → raft_store circular import.
  ##
  ## Durability model: the Raft log entry was already written with fdatasync
  ## in putEntryAndState, so the commit is durable before this callback fires.
  ## The WiscKey write here uses writeBatchNoSync (no second fdatasync) — it
  ## keeps committed data readable after a clean restart.  On a crash the Raft
  ## log is replayed from lastApplied to reconstruct any missing SM state.
  ##
  ## ORC safety: We must never create a local `RaftKVStoreExt` (managed ref)
  ## from the raw pointer — ORC would try to destroy it on scope exit, racing
  ## with other threads.  Instead we GC_ref/GC_unref around the cast so the
  ## refcount is balanced and ORC never frees the object.
  if storePtr == nil: return
  let store = cast[RaftKVStoreExt](storePtr)
  GC_ref(store) # prevent ORC from freeing on scope exit (balances the decrement)

  # --- Persist to WiscKey (no fdatasync — Raft log is the durability guarantee) ---
  let backend = store.coordinator.store
  if backend != nil and backend.isOpen:
    {.cast(raises: []).}:
      var pairs: seq[KeyValuePair] = @[]
      var delKeys: seq[string] = @[]
      for (k, v) in batch.puts:
        pairs.add((key: fromBytes(k), value: fromBytes(v)))
      for k in batch.deletes:
        delKeys.add(fromBytes(k))
      discard backend.writeBatchNoSync(pairs, delKeys)

  # No in-memory state machine to update — reads go through WiscKey directly.

  # --- Notify on sys.groups metadata changes (peer group creation) ---
  let groupsPrefix = "/t/" & align($SYS_GROUPS_TABLE_ID, 10, '0') & "/"
  for (k, v) in batch.puts:
    let key = fromBytes(k)
    if key.startsWith(groupsPrefix):
      {.cast(gcsafe).}:
        if onGroupMetadataApplied != nil:
          onGroupMetadataApplied(storePtr, key, fromBytes(v))

proc wireApplyCallback*(store: RaftKVStoreExt) {.gcsafe, raises: [].} =
  ## Wire the applyBatchToSM callback into the coordinator so that committed
  ## log entries are applied to the local KV state machine on followers.
  ## Call this once after newRaftKVStoreExt() and before coordinator.start().
  ##
  ## GC safety: The store is kept alive by the caller (ProtocolServer.raftStore).
  ## The coordinator is always stopped before the server (and thus before the
  ## store ref is released), so kvStorePtr is never a dangling pointer.
  ## In the callback, wasMoved() prevents ORC from decrementing the refcount
  ## on the cast-from-pointer local, since the cast does not transfer ownership.
  {.cast(gcsafe).}: {.cast(raises: []).}:
    multigroup_coordinator.applyBatchCallback = applyBatchToSM
    multigroup_coordinator.getPreferredLeaderCallback = proc(
        storePtr: pointer,
        groupId: GroupID): Option[NodeID] {.gcsafe, raises: [].} =
      let s = cast[RaftKVStoreExt](storePtr)
      let pl = s.preferredLeaders.getOrDefault(groupId, 0'u32)
      if pl > 0:
        result = some(NodeID(pl))
      else:
        result = none(NodeID)
    multigroup_coordinator.onGroupMetadataApplied = proc(
        storePtr: pointer,
        groupKey: string, groupValue: string) {.gcsafe, raises: [].} =
      ## When sys.groups metadata replicates via Raft, create the local Raft
      ## group if this node is a member but hasn't instantiated it yet.
      if storePtr == nil: return
      let s = cast[RaftKVStoreExt](storePtr)
      GC_ref(s)
      defer: GC_unref(s)
      let coord = s.coordinator
      {.cast(gcsafe).}:
        try:
          let j = parseJson(groupValue)
          let gid = GroupID(uint64(j["groupId"].getInt()))
          if gid == META_GROUP_ID or gid == DATA_GROUP_START_ID: return
          if coord.hasGroup(gid): return

          var desc = rangeTypes.newGroupDescriptor(gid)
          if j.hasKey("replicas"):
            for r in j["replicas"]:
              discard desc.addReplica(
                rangeTypes.NodeID(uint32(r["nodeId"].getInt())),
                rangeTypes.rtVoter)

          var myReplicaId = rangeTypes.ReplicaID(0)
          for r in desc.replicas:
            if r.nodeId == coord.nodeId:
              myReplicaId = r.replicaId
              break

          if myReplicaId != rangeTypes.ReplicaID(0):
            discard coord.createAndStartGroup(desc, myReplicaId)
            s.registerGroup(gid)
            # Update group membership cache
            var members: seq[uint32] = @[]
            for r in desc.replicas:
              members.add(uint32(r.nodeId))
            s.groupMembers[gid] = members
            if j.hasKey("preferredLeader"):
              let pl = uint32(j["preferredLeader"].getInt())
              if pl > 0:
                s.preferredLeaders[gid] = pl
        except:
          discard
  store.coordinator.kvStorePtr = cast[pointer](store)

proc bootstrapStore*(store: RaftKVStoreExt,
    groupIds: seq[GroupID]) {.gcsafe, raises: [].} =
  ## Pre-create state machines for the given groups and wire the apply callback.
  ## Call after coord.start() (or at least after the store is ready).
  for rid in groupIds:
    discard store.getOrCreateSM(rid)
  store.wireApplyCallback()

proc proposeWrite(store: RaftKVStoreExt, groupId: GroupID,
    batch: WriteBatch): RSVoidResult {.gcsafe, raises: [].} =
  ## Propose a write batch to Raft and, on success, apply it locally.
  let cmd = RaftCommand(kind: ckWrite, writeBatch: batch)
  let res = store.coordinator.proposeAndWait(groupId, cmd,
      store.proposeTimeout)
  if not res.success:
    if res.error == "Not the leader":
      return rsVErr(newRSE(rseNotLeader, res.error))
    if res.error.len > 0 and res.error.contains("Group not found"):
      return rsVErr(newRSE(rseGroupNotFound, res.error))
    if res.error.contains("Timeout"):
      return rsVErr(newRSE(rseTimeout, res.error))
    return rsVErr(newRSE(rseInternal, res.error))

  # The SM was already updated by applyBatchToSM (called inside applyUpTo,
  # which fires before proposeAndWait returns on the single-node path).
  # A second apply here would be a duplicate write — removed to avoid
  # double-apply data corruption and unnecessary smMu contention.
  rsVOk()

# ---------------------------------------------------------------------------
# Public KV interface (drop-in for KVStore in server.nim)
# ---------------------------------------------------------------------------

proc raftGet*(store: RaftKVStoreExt,
    key: string): RSResult[Option[RaftKVEntry]] {.gcsafe, raises: [].} =
  ## Read the current value for `key` from the WiscKey backend.
  ## Reads are served locally (leader / leaseholder read semantics).
  let ridOpt = store.resolveGroupId(key)
  if ridOpt.isNone:
    return rsErr[Option[RaftKVEntry]](newRSE(rseGroupNotFound,
        &"no shard for key '{key}'"))

  # Skip intent keys on plain reads
  if isIntentKey(key) or isCoordKey(key):
    return rsOk[Option[RaftKVEntry]](none(RaftKVEntry))

  let backend = store.getBackend()
  if backend != nil and backend.isOpen:
    {.cast(raises: []).}:
      let valOpt = backend.get(key)
      if valOpt.isSome:
        let entry = RaftKVEntry(
          value: valOpt.get(),
          version: 1'u64,
          timestamp: uint64(getTime().toUnixFloat() * 1_000_000_000),
        )
        return rsOk[Option[RaftKVEntry]](some(entry))

  rsOk[Option[RaftKVEntry]](none(RaftKVEntry))

proc raftPut*(store: RaftKVStoreExt, key, value: string): RSResult[
    RaftKVEntry] {.gcsafe, raises: [].} =
  ## Write `value` under `key` through Raft consensus.
  let ridOpt = store.resolveGroupId(key)
  if ridOpt.isNone:
    return rsErr[RaftKVEntry](newRSE(rseGroupNotFound,
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
  let ridOpt = store.resolveGroupId(key)
  if ridOpt.isNone:
    return rsErr[Option[RaftKVEntry]](newRSE(rseGroupNotFound,
        &"no shard for key '{key}'"))

  # Capture previous value from backend
  var prevEntry: Option[RaftKVEntry]
  let backend = store.getBackend()
  if backend != nil and backend.isOpen:
    {.cast(raises: []).}:
      let valOpt = backend.get(key)
      if valOpt.isSome:
        prevEntry = some(RaftKVEntry(
          value: valOpt.get(),
          version: 1'u64,
          timestamp: uint64(getTime().toUnixFloat() * 1_000_000_000),
        ))

  let batch = newWriteBatch()
  batch.delete(toBytes(key))

  let vr = proposeWrite(store, ridOpt.get(), batch)
  if not vr.isOk:
    return rsErr[Option[RaftKVEntry]](vr.error)

  rsOk[Option[RaftKVEntry]](prevEntry)

proc raftScan*(store: RaftKVStoreExt, startKey, endKey: string,
    limit: uint32,
    includeSystemKeys: bool = false): RSResult[seq[(string, RaftKVEntry)]] {.gcsafe, raises: [].} =
  ## Scan keys in [startKey, endKey) up to `limit` results.
  ## Uses WiscKey backend.scan() directly — results are already sorted by key.
  ## By default, system table keys (/t/0000000001/... through /t/0000000099/...)
  ## are excluded from results. Set includeSystemKeys=true to include them.
  var pairs: seq[(string, RaftKVEntry)] = @[]
  let backend = store.getBackend()
  if backend == nil or not backend.isOpen:
    return rsOk[seq[(string, RaftKVEntry)]](@[])

  {.cast(raises: []).}:
    # Scan with no limit; we filter below. LevelDB iterates in sorted order.
    let raw = backend.scan(startKey, endKey)
    let ts = uint64(getTime().toUnixFloat() * 1_000_000_000)
    for (k, v) in raw:
      if isIntentKey(k) or isCoordKey(k): continue
      if not includeSystemKeys and isSystemKey(k): continue
      pairs.add((k, RaftKVEntry(value: v, version: 1'u64, timestamp: ts)))
      if limit > 0 and pairs.len >= int(limit):
        break

  rsOk[seq[(string, RaftKVEntry)]](pairs)

proc shardCount*(store: RaftKVStoreExt): int {.gcsafe, raises: [].} =
  ## Returns the number of registered state machines (groups).
  acquire(store.smMu)
  defer: release(store.smMu)
  store.stateMachines.len

proc raftLen*(store: RaftKVStoreExt): int {.gcsafe, raises: [].} =
  ## Count all user keys (excluding intents and coord records) via backend scan.
  var total = 0
  let backend = store.getBackend()
  if backend == nil or not backend.isOpen:
    return 0
  {.cast(raises: []).}:
    let raw = backend.scan("", "")
    for (k, _) in raw:
      if not isIntentKey(k) and not isCoordKey(k): inc total
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
  let ridOpt = store.resolveGroupId(key)
  if ridOpt.isNone:
    return rsVErr(newRSE(rseGroupNotFound, &"no shard for key '{key}'"))

  let backend = store.getBackend()
  if backend != nil and backend.isOpen:
    {.cast(raises: []).}:
      let pairs: seq[KeyValuePair] = @[(key: intentKey, value: value)]
      discard backend.writeBatchNoSync(pairs, @[])

  rsVOk()

proc raftDeleteIntent*(store: RaftKVStoreExt, txnId: uint64,
    key: string): RSVoidResult {.gcsafe, raises: [].} =
  ## Remove the intent (used during rollback or abort) — also no fsync needed.
  let intentKey = encodeIntentKey(txnId, key)
  let ridOpt = store.resolveGroupId(key)
  if ridOpt.isNone:
    return rsVErr(newRSE(rseGroupNotFound, &"no shard for key '{key}'"))

  let backend = store.getBackend()
  if backend != nil and backend.isOpen:
    {.cast(raises: []).}:
      discard backend.writeBatchNoSync(@[], @[intentKey])

  rsVOk()

proc raftPutIntent*(store: RaftKVStoreExt, txnId: uint64, key,
    value: string): RSVoidResult {.gcsafe, raises: [].} =
  ## Alias kept for raft_txn.nim compatibility; routes to raftBufferIntent.
  raftBufferIntent(store, txnId, key, value)

proc raftCommitTxn*(store: RaftKVStoreExt, txnId: uint64,
    writeSet: seq[string]): RSVoidResult {.gcsafe, raises: [].} =
  ## Commit a transaction by resolving all intents across one or more shards.
  ##
  ## Keys are grouped by their GroupID so that each shard receives exactly one
  ## Raft WriteBatch proposal (one fdatasync per shard).  For each key:
  ##   - read the intent value from the in-memory state machine
  ##   - add (realKey → value) and delete (intentKey) to that shard's batch
  ## Shards whose keys have no outstanding intents are skipped entirely.
  if writeSet.len == 0:
    return rsVOk()

  # --- Group keys by GroupID ---
  var batches: Table[GroupID, WriteBatch]
  for key in writeSet:
    let ridOpt = store.resolveGroupId(key)
    if ridOpt.isNone:
      return rsVErr(newRSE(rseGroupNotFound, &"no shard for key '{key}'"))
    let rid = ridOpt.get()
    if not batches.hasKey(rid):
      batches[rid] = newWriteBatch()

  # --- Fill each per-shard batch from the in-memory state machine ---
  # Iterate over a copy of the GroupIDs so we can index batches safely via
  # mgetOrPut (avoids KeyError in raises:[] context).
  var rids: seq[GroupID] = @[]
  for rid in batches.keys: rids.add(rid)

  let backend = store.getBackend()
  for rid in rids:
    for key in writeSet:
      let keyRidOpt = store.resolveGroupId(key)
      if keyRidOpt.isNone or keyRidOpt.get() != rid: continue
      let intentKey = encodeIntentKey(txnId, key)
      if backend != nil and backend.isOpen:
        {.cast(raises: []).}:
          let valOpt = backend.get(intentKey)
          if valOpt.isSome:
            batches[rid].put(toBytes(key), toBytes(valOpt.get()))
            batches[rid].delete(toBytes(intentKey))
      # Intent not found → key was never written; skip silently.

  # --- Propose each non-empty batch to its Raft group ---
  for rid in rids:
    {.cast(raises: []).}:
      let batch = batches[rid]
      if not batch.isEmpty:
        let vr = proposeWrite(store, rid, batch)
        if not vr.isOk:
          return vr

  rsVOk()

proc raftCommitTxnPipelined*(store: RaftKVStoreExt, txnId: uint64,
    writeSet: seq[string]): RSVoidResult {.gcsafe, raises: [].} =
  ## Pipelined variant of raftCommitTxn: dispatches one Raft proposal per
  ## shard simultaneously and waits for all of them in parallel.
  ##
  ## For a single-shard transaction this is identical to raftCommitTxn.
  ## For a multi-shard transaction the wall-clock commit time drops from
  ## Σ(fsync_i) to max(fsync_i) because all shard proposals are in-flight
  ## concurrently — the same technique as pipelining AppendEntries in Raft.
  ##
  ## The implementation:
  ##   1. Group write-set keys by GroupID (same as raftCommitTxn).
  ##   2. Fill per-shard WriteBatches from the in-memory state machine.
  ##   3. Call coordinator.proposeParallel() with all non-empty batches.
  ##   4. Return the first error (if any); all shards that succeeded have
  ##      already committed their batch (no rollback at this layer — the
  ##      caller's 2PC protocol handles partial failures via COORD record).
  if writeSet.len == 0:
    return rsVOk()

  # --- Step 1: group keys by GroupID ---
  var batches: Table[GroupID, WriteBatch]
  for key in writeSet:
    let ridOpt = store.resolveGroupId(key)
    if ridOpt.isNone:
      return rsVErr(newRSE(rseGroupNotFound, "no shard for key '" & key & "'"))
    let rid = ridOpt.get()
    if not batches.hasKey(rid):
      batches[rid] = newWriteBatch()

  # Collect the GroupIDs so we can iterate without Table invalidation.
  var rids: seq[GroupID] = @[]
  for rid in batches.keys: rids.add(rid)

  # --- Step 2: fill per-shard WriteBatches ---
  let backend = store.getBackend()
  for rid in rids:
    for key in writeSet:
      let keyRidOpt = store.resolveGroupId(key)
      if keyRidOpt.isNone or keyRidOpt.get() != rid: continue
      let intentKey = encodeIntentKey(txnId, key)
      if backend != nil and backend.isOpen:
        {.cast(raises: []).}:
          let valOpt = backend.get(intentKey)
          if valOpt.isSome:
            batches[rid].put(toBytes(key), toBytes(valOpt.get()))
            batches[rid].delete(toBytes(intentKey))

  # --- Step 3: build the parallel proposal list ---
  var proposals: seq[tuple[groupId: GroupID, command: RaftCommand]] = @[]
  for rid in rids:
    {.cast(raises: []).}:
      let batch = batches[rid]
      if not batch.isEmpty:
        proposals.add((groupId: rid,
                        command: RaftCommand(kind: ckWrite, writeBatch: batch)))

  if proposals.len == 0:
    return rsVOk()

  # --- Step 4: dispatch all proposals simultaneously ---
  let results = store.coordinator.proposeParallel(proposals,
      store.proposeTimeout)

  for r in results:
    if not r.success:
      if r.error == "Not the leader":
        return rsVErr(newRSE(rseNotLeader, r.error))
      if r.error.contains("Group not found"):
        return rsVErr(newRSE(rseGroupNotFound, r.error))
      if r.error.contains("Timeout"):
        return rsVErr(newRSE(rseTimeout, r.error))
      return rsVErr(newRSE(rseInternal, r.error))

  rsVOk()

proc raftGetForTxn*(store: RaftKVStoreExt, txnId: uint64,
    key: string): RSResult[Option[RaftKVEntry]] {.gcsafe, raises: [].} =
  ## Transactional read: returns the intent value if this transaction has a
  ## buffered write for `key` (reads-your-own-writes), otherwise falls back to
  ## the committed value visible via raftGet.
  ##
  ## The lookup order is:
  ##   1. Intent key  ("\x00INTENT\x00<txnId8be><key>") in the local SM
  ##   2. Committed key (same as raftGet)
  let ridOpt = store.resolveGroupId(key)
  if ridOpt.isNone:
    return rsErr[Option[RaftKVEntry]](newRSE(rseGroupNotFound,
        &"no shard for key '{key}'"))

  let intentKey = encodeIntentKey(txnId, key)
  let backend = store.getBackend()
  let ts = uint64(getTime().toUnixFloat() * 1_000_000_000)

  if backend != nil and backend.isOpen:
    {.cast(raises: []).}:
      # Check intent first (reads-your-own-writes)
      let intentOpt = backend.get(intentKey)
      if intentOpt.isSome:
        return rsOk[Option[RaftKVEntry]](some(RaftKVEntry(
          value: intentOpt.get(),
          version: 1'u64,
          timestamp: ts,
        )))
      # Fall back to committed value
      let committedOpt = backend.get(key)
      if committedOpt.isSome:
        return rsOk[Option[RaftKVEntry]](some(RaftKVEntry(
          value: committedOpt.get(),
          version: 1'u64,
          timestamp: ts,
        )))

  rsOk[Option[RaftKVEntry]](none(RaftKVEntry))

proc raftResolveIntent*(store: RaftKVStoreExt, txnId: uint64,
    key: string, commit: bool,
    commitValue: string = ""): RSVoidResult {.gcsafe, raises: [].} =
  ## On commit: move the intent to the committed slot.
  ## On abort:  delete the intent.
  let intentKey = encodeIntentKey(txnId, key)
  let ridOpt = store.resolveGroupId(key)
  if ridOpt.isNone:
    return rsVErr(newRSE(rseGroupNotFound, &"no shard for key '{key}'"))

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
  ## Write a durable 2PC coordinator record to the meta group.
  let coordKey = encodeCoordKey(txnId)
  let batch = newWriteBatch()
  batch.put(toBytes(coordKey), toBytes(payload))
  proposeWrite(store, META_GROUP_ID, batch)

proc raftDeleteCoordRecord*(store: RaftKVStoreExt,
    txnId: uint64): RSVoidResult {.gcsafe, raises: [].} =
  ## Remove the coordinator record after a 2PC round completes.
  let coordKey = encodeCoordKey(txnId)
  let batch = newWriteBatch()
  batch.delete(toBytes(coordKey))
  proposeWrite(store, META_GROUP_ID, batch)

proc raftReadCoordRecord*(store: RaftKVStoreExt,
    txnId: uint64): Option[string] {.gcsafe, raises: [].} =
  ## Read back a coordinator record (for recovery).
  let coordKey = encodeCoordKey(txnId)
  let backend = store.getBackend()
  if backend != nil and backend.isOpen:
    {.cast(raises: []).}:
      return backend.get(coordKey)
  none(string)

# ---------------------------------------------------------------------------
# Space-aware routing
# ---------------------------------------------------------------------------

proc loadSpaces*(store: RaftKVStoreExt) {.gcsafe, raises: [].} =
  ## Scan sys.spaces and populate the in-memory spaces table.
  ## Call after bootstrap/recovery when the state machine is populated.
  let startKey = "/t/" & align($SYS_SPACES_TABLE_ID, 10, '0') & "/"
  let endKey = "/t/" & align($(SYS_SPACES_TABLE_ID + 1), 10, '0') & "/"
  let backend = store.getBackend()
  var entries: seq[KeyValuePair] = @[]
  if backend != nil and backend.isOpen:
    {.cast(raises: []).}:
      entries = backend.scan(startKey, endKey)

  acquire(store.spacesMu)
  store.spaces.clear()
  for (k, v) in entries:
    {.cast(raises: []).}:
      try:
        let j = parseJson(v)
        var info = SpaceInfo(
          spaceId: j["spaceId"].getInt(),
          name: j["name"].getStr(),
          replicas: j["replicas"].getInt(),
        )
        if j.hasKey("groupIds"):
          for r in j["groupIds"]:
            info.groupIds.add(uint64(r.getInt()))
        if j.hasKey("oldGroupIds"):
          for r in j["oldGroupIds"]:
            info.oldGroupIds.add(uint64(r.getInt()))
        info.rebalancing = j.getOrDefault("rebalancing").getBool(false)
        info.rebalanceWorker = j.getOrDefault("rebalanceWorker").getInt(0)
        info.rebalanceHeartbeat = j.getOrDefault("rebalanceHeartbeat").getBiggestInt(0)
        info.rebalanceCursor = j.getOrDefault("rebalanceCursor").getStr("")
        store.spaces[info.spaceId] = info
      except:
        discard
  release(store.spacesMu)

proc loadTableSpaces*(store: RaftKVStoreExt) {.gcsafe, raises: [].} =
  ## Scan sys.tables and populate the tableId → spaceId mapping.
  let startKey = "/t/" & align($SYS_TABLES_TABLE_ID, 10, '0') & "/"
  let endKey = "/t/" & align($(SYS_TABLES_TABLE_ID + 1), 10, '0') & "/"
  let backend = store.getBackend()
  var entries: seq[KeyValuePair] = @[]
  if backend != nil and backend.isOpen:
    {.cast(raises: []).}:
      entries = backend.scan(startKey, endKey)

  acquire(store.spacesMu)
  store.tableSpaces.clear()
  for (k, v) in entries:
    {.cast(raises: []).}:
      try:
        let j = parseJson(v)
        let tid = uint32(j["tableId"].getInt())
        let sid = j.getOrDefault("spaceId").getInt(1) # default = 1 (default space)
        store.tableSpaces[tid] = sid
      except:
        discard
  release(store.spacesMu)

proc routeToGroup*(primaryKey: string, groupIds: seq[uint64]): GroupID {.inline.} =
  ## Hash-route a primary key to one of the space's groups.
  if groupIds.len == 0:
    return META_GROUP_ID
  if groupIds.len == 1:
    return GroupID(groupIds[0])
  let h = hash(primaryKey)
  let idx = abs(h) mod groupIds.len
  GroupID(groupIds[idx])

proc getSpaceForTable*(store: RaftKVStoreExt,
    tableId: uint32): Option[SpaceInfo] {.gcsafe, raises: [].} =
  acquire(store.spacesMu)
  defer: release(store.spacesMu)
  let sid = store.tableSpaces.getOrDefault(tableId, 1)
  if store.spaces.hasKey(sid):
    return some(store.spaces.getOrDefault(sid))
  none(SpaceInfo)

# ---------------------------------------------------------------------------
# Space-aware KV operations
# ---------------------------------------------------------------------------
# These bypass resolveGroupId() and route directly to a space's Raft group
# using hash(primaryKey) mod numGroups.

proc findAllPeersForGroup(store: RaftKVStoreExt,
    groupId: GroupID): seq[RaftKVStoreExt] {.gcsafe, raises: [].} =
  ## Return all peer stores that own the given group, leader first.
  if not store.groupMembers.hasKey(groupId):
    return @[]
  let members = store.groupMembers.getOrDefault(groupId)
  var leader: seq[RaftKVStoreExt] = @[]
  var others: seq[RaftKVStoreExt] = @[]
  {.cast(raises: []).}:
    for nid in members:
      if store.peerStores.hasKey(nid):
        let peer = store.peerStores.getOrDefault(nid)
        if peer.coordinator.hasGroup(groupId):
          let g = peer.coordinator.getGroup(groupId)
          if g.isSome and g.get.isLeader():
            leader.add(peer)
          else:
            others.add(peer)
  leader & others

proc forwardPutToLeader(store: RaftKVStoreExt, rid: GroupID,
    key, value: string): RSResult[RaftKVEntry] {.gcsafe, raises: [].} =
  ## Try all peers that own `rid` until one succeeds (leader found).
  ## Calls proposeWrite directly on the peer (no recursion).
  let peers = store.findAllPeersForGroup(rid)
  let batch = newWriteBatch()
  batch.put(toBytes(key), toBytes(value))
  for peer in peers:
    let vr = proposeWrite(peer, rid, batch)
    if vr.isOk:
      let ver = peer.nextVersion.fetchAdd(1)
      let ts = uint64(getTime().toUnixFloat() * 1_000_000_000)
      return rsOk[RaftKVEntry](RaftKVEntry(value: value, version: ver, timestamp: ts))
    if vr.error.kind != rseNotLeader:
      return rsErr[RaftKVEntry](vr.error)
  if peers.len > 0:
    return rsErr[RaftKVEntry](newRSE(rseNotLeader,
        "no leader found for group " & $rid.uint64))
  rsErr[RaftKVEntry](newRSE(rseGroupNotFound,
      "no local or peer store for group " & $rid.uint64))

proc raftPutInSpace*(store: RaftKVStoreExt, key, value: string,
    space: SpaceInfo, primaryKey: string): RSResult[RaftKVEntry] {.gcsafe, raises: [].} =
  ## Write `value` under `key` through Raft consensus, routing to the
  ## correct group in the space via hash(primaryKey).
  ## If the local coordinator doesn't own the target group, or if the local
  ## node is not the leader, forward to a peer that is.
  let rid = routeToGroup(primaryKey, space.groupIds)
  if not store.coordinator.hasGroup(rid):
    return store.forwardPutToLeader(rid, key, value)
  let batch = newWriteBatch()
  batch.put(toBytes(key), toBytes(value))
  let vr = proposeWrite(store, rid, batch)
  if not vr.isOk:
    if vr.error.kind == rseNotLeader and store.peerStores.len > 0:
      return store.forwardPutToLeader(rid, key, value)
    return rsErr[RaftKVEntry](vr.error)
  let ver = store.nextVersion.fetchAdd(1)
  let ts = uint64(getTime().toUnixFloat() * 1_000_000_000)
  rsOk[RaftKVEntry](RaftKVEntry(value: value, version: ver, timestamp: ts))

proc raftGetInSpaceFromGroup(store: RaftKVStoreExt, key: string,
    rid: GroupID): RSResult[Option[RaftKVEntry]] {.gcsafe, raises: [].} =
  ## Internal helper: read `key` from a specific group.
  if not store.coordinator.hasGroup(rid):
    let peerOpt = store.findPeerForGroup(rid)
    if peerOpt.isSome:
      return peerOpt.get().raftGetInSpaceFromGroup(key, rid)
    return rsErr[Option[RaftKVEntry]](newRSE(rseGroupNotFound,
        "no local or peer store for group " & $rid.uint64))
  var isLocalLeader = false
  {.cast(raises: []).}:
    let localGroup = store.coordinator.getGroup(rid)
    isLocalLeader = localGroup.isSome and localGroup.get.isLeader()

  if isLocalLeader or store.peerStores.len == 0:
    let backend = store.getBackend()
    if backend != nil and backend.isOpen:
      {.cast(raises: []).}:
        let valOpt = backend.get(key)
        if valOpt.isSome:
          let entry = RaftKVEntry(
            value: valOpt.get(),
            version: 1'u64,
            timestamp: uint64(getTime().toUnixFloat() * 1_000_000_000),
          )
          return rsOk[Option[RaftKVEntry]](some(entry))
    return rsOk[Option[RaftKVEntry]](none(RaftKVEntry))
  else:
    let peerOpt = store.findPeerForGroup(rid)
    if peerOpt.isSome:
      return peerOpt.get().raftGetInSpaceFromGroup(key, rid)
    let backend = store.getBackend()
    if backend != nil and backend.isOpen:
      {.cast(raises: []).}:
        let valOpt = backend.get(key)
        if valOpt.isSome:
          let entry = RaftKVEntry(
            value: valOpt.get(),
            version: 1'u64,
            timestamp: uint64(getTime().toUnixFloat() * 1_000_000_000),
          )
          return rsOk[Option[RaftKVEntry]](some(entry))
    return rsOk[Option[RaftKVEntry]](none(RaftKVEntry))

proc raftGetInSpace*(store: RaftKVStoreExt, key: string,
    space: SpaceInfo, primaryKey: string): RSResult[Option[RaftKVEntry]] {.gcsafe, raises: [].} =
  ## Read the current value for `key` from the space group owning `primaryKey`.
  ## During rebalancing, if the key is not found in the new group, falls back
  ## to the old group.
  let rid = routeToGroup(primaryKey, space.groupIds)
  let res = store.raftGetInSpaceFromGroup(key, rid)
  if not res.isOk:
    return res
  # If found, return it
  if res.value.isSome:
    return res
  # Not found in new group — fall back to old group during rebalance
  if space.rebalancing and space.oldGroupIds.len > 0:
    let oldRid = routeToGroup(primaryKey, space.oldGroupIds)
    if oldRid != rid:
      return store.raftGetInSpaceFromGroup(key, oldRid)
  res

proc forwardDeleteToLeader(store: RaftKVStoreExt, rid: GroupID,
    key: string): RSResult[Option[RaftKVEntry]] {.gcsafe, raises: [].} =
  ## Try all peers that own `rid` until one succeeds (leader found).
  let peers = store.findAllPeersForGroup(rid)
  let batch = newWriteBatch()
  batch.delete(toBytes(key))
  for peer in peers:
    # Capture previous value from peer's backend
    var prevEntry: Option[RaftKVEntry]
    let peerBackend = peer.getBackend()
    if peerBackend != nil and peerBackend.isOpen:
      {.cast(raises: []).}:
        let valOpt = peerBackend.get(key)
        if valOpt.isSome:
          prevEntry = some(RaftKVEntry(
            value: valOpt.get(),
            version: 1'u64,
            timestamp: uint64(getTime().toUnixFloat() * 1_000_000_000),
          ))
    let vr = proposeWrite(peer, rid, batch)
    if vr.isOk:
      return rsOk[Option[RaftKVEntry]](prevEntry)
    if vr.error.kind != rseNotLeader:
      return rsErr[Option[RaftKVEntry]](vr.error)
  if peers.len > 0:
    return rsErr[Option[RaftKVEntry]](newRSE(rseNotLeader,
        "no leader found for group " & $rid.uint64))
  rsErr[Option[RaftKVEntry]](newRSE(rseGroupNotFound,
      "no local or peer store for group " & $rid.uint64))

proc raftDeleteInSpace*(store: RaftKVStoreExt, key: string,
    space: SpaceInfo, primaryKey: string): RSResult[Option[RaftKVEntry]] {.gcsafe, raises: [].} =
  ## Delete `key` through Raft consensus, routing to the correct space group.
  ## If the local coordinator doesn't own the target group, forward to a peer.
  let rid = routeToGroup(primaryKey, space.groupIds)
  if not store.coordinator.hasGroup(rid):
    return store.forwardDeleteToLeader(rid, key)
  # Capture previous value from backend
  var prevEntry: Option[RaftKVEntry]
  let backend = store.getBackend()
  if backend != nil and backend.isOpen:
    {.cast(raises: []).}:
      let valOpt = backend.get(key)
      if valOpt.isSome:
        prevEntry = some(RaftKVEntry(
          value: valOpt.get(),
          version: 1'u64,
          timestamp: uint64(getTime().toUnixFloat() * 1_000_000_000),
        ))
  let batch = newWriteBatch()
  batch.delete(toBytes(key))
  let vr = proposeWrite(store, rid, batch)
  if not vr.isOk:
    if vr.error.kind == rseNotLeader and store.peerStores.len > 0:
      return store.forwardDeleteToLeader(rid, key)
    return rsErr[Option[RaftKVEntry]](vr.error)
  rsOk[Option[RaftKVEntry]](prevEntry)

proc raftDeleteInGroup*(store: RaftKVStoreExt, key: string,
    groupId: GroupID): RSResult[Option[RaftKVEntry]] {.gcsafe, raises: [].} =
  ## Delete `key` from a specific group (used during rebalance migration
  ## to remove a key from its old group after copying to the new one).
  if not store.coordinator.hasGroup(groupId):
    return store.forwardDeleteToLeader(groupId, key)
  let batch = newWriteBatch()
  batch.delete(toBytes(key))
  let vr = proposeWrite(store, groupId, batch)
  if not vr.isOk:
    if vr.error.kind == rseNotLeader and store.peerStores.len > 0:
      return store.forwardDeleteToLeader(groupId, key)
    return rsErr[Option[RaftKVEntry]](vr.error)
  rsOk[Option[RaftKVEntry]](none(RaftKVEntry))

proc scanLocalGroup(store: RaftKVStoreExt, rid: GroupID,
    startKey, endKey: string,
    includeSystemKeys: bool): seq[(string, RaftKVEntry)] {.gcsafe, raises: [].} =
  ## Scan the WiscKey backend for matching keys in the given range.
  ## Note: with a single shared backend, `rid` is informational only; the
  ## backend stores all groups' data in one LevelDB instance.
  var pairs: seq[(string, RaftKVEntry)] = @[]
  let backend = store.getBackend()
  if backend == nil or not backend.isOpen:
    return pairs
  {.cast(raises: []).}:
    let raw = backend.scan(startKey, endKey)
    let ts = uint64(getTime().toUnixFloat() * 1_000_000_000)
    for (k, v) in raw:
      if isIntentKey(k) or isCoordKey(k): continue
      if not includeSystemKeys and isSystemKey(k): continue
      pairs.add((k, RaftKVEntry(value: v, version: 1'u64, timestamp: ts)))
  pairs

proc raftScanSpace*(store: RaftKVStoreExt, startKey, endKey: string,
    space: SpaceInfo, limit: uint32 = 0,
    includeSystemKeys: bool = false): RSResult[seq[(string, RaftKVEntry)]] {.gcsafe, raises: [].} =
  ## Scan keys in the space's key range.
  ## With a shared WiscKey backend, all groups share the same store, so we
  ## do a single backend scan (no per-group fan-out needed). For multi-node
  ## clusters where peers may own different groups, we prefer scanning from
  ## a leader node that has up-to-date data.
  ##
  ## For multi-node: if any of the space's groups are not locally owned,
  ## forward to a peer that is the leader for that group and merge results.
  ## For single-node: just scan the local backend directly.

  if store.peerStores.len == 0:
    # Single-node fast path: scan directly from the local backend
    return store.raftScan(startKey, endKey, limit,
        includeSystemKeys = includeSystemKeys)

  # Multi-node: fan-out to group leaders.
  # Since all groups on the same node share one backend, we track which
  # stores we've already scanned to avoid duplicate results.
  # During rebalancing, scan both new and old group sets.
  var scannedStores: seq[pointer] = @[]
  var allPairs: seq[(string, RaftKVEntry)] = @[]

  var allGroupIds = space.groupIds
  if space.rebalancing:
    for oid in space.oldGroupIds:
      var found = false
      for nid in allGroupIds:
        if nid == oid:
          found = true
          break
      if not found:
        allGroupIds.add(oid)

  for rid64 in allGroupIds:
    let rid = GroupID(rid64)
    var scanStore = store
    if not store.coordinator.hasGroup(rid):
      let peerOpt = store.findPeerForGroup(rid)
      if peerOpt.isSome:
        scanStore = peerOpt.get()
      else:
        continue
    else:
      {.cast(raises: []).}:
        let localGroup = store.coordinator.getGroup(rid)
        let isLocalLeader = localGroup.isSome and localGroup.get.isLeader()
        if not isLocalLeader and store.peerStores.len > 0:
          let peerOpt = store.findPeerForGroup(rid)
          if peerOpt.isSome:
            scanStore = peerOpt.get()

    # Skip if we've already scanned this store's backend
    let storePtr = cast[pointer](scanStore)
    var alreadyScanned = false
    for s in scannedStores:
      if s == storePtr:
        alreadyScanned = true
        break
    if alreadyScanned: continue
    scannedStores.add(storePtr)

    let pairs = scanStore.scanLocalGroup(rid, startKey, endKey, includeSystemKeys)
    for p in pairs:
      allPairs.add(p)

  # Sort and deduplicate
  allPairs.sort(proc(a, b: (string, RaftKVEntry)): int = cmp(a[0], b[0]))
  # Deduplicate by key (same key from different node scans)
  var deduped: seq[(string, RaftKVEntry)] = @[]
  var lastKey = ""
  for p in allPairs:
    if p[0] != lastKey:
      deduped.add(p)
      lastKey = p[0]

  if limit > 0 and deduped.len > int(limit):
    deduped.setLen(int(limit))

  rsOk[seq[(string, RaftKVEntry)]](deduped)

# ---------------------------------------------------------------------------
# Space rebalancing
# ---------------------------------------------------------------------------

proc updateSpaceRecord*(store: RaftKVStoreExt, space: SpaceInfo) {.gcsafe, raises: [].} =
  ## Write the space record back to sys.spaces via Raft.
  let spaceKey = encodeSpaceKey(space.spaceId)
  var groupIdsJ = newJArray()
  for g in space.groupIds:
    groupIdsJ.add(newJInt(int(g)))
  var oldGroupIdsJ = newJArray()
  for g in space.oldGroupIds:
    oldGroupIdsJ.add(newJInt(int(g)))
  let spaceVal = $ %*{
    "spaceId": space.spaceId,
    "name": space.name,
    "replicas": space.replicas,
    "groupCount": space.groupIds.len,
    "groupIds": groupIdsJ,
    "oldGroupIds": oldGroupIdsJ,
    "rebalancing": space.rebalancing,
    "rebalanceWorker": space.rebalanceWorker,
    "rebalanceHeartbeat": space.rebalanceHeartbeat,
    "rebalanceCursor": space.rebalanceCursor,
  }
  discard store.raftPut(spaceKey, spaceVal)

proc rebalanceSpaces*(store: RaftKVStoreExt) {.raises: [].} =
  ## Check all spaces and initiate rebalancing for any space whose group count
  ## doesn't match the current node count. Creates new groups and sets up
  ## dual-read mode.
  {.cast(raises: []).}:
    # Count nodes
    let nodesStart = encodeTableKey(SYS_NODES_TABLE_ID, "")
    let nodesEnd = encodeTableKey(SYS_NODES_TABLE_ID + 1, "")
    let nodesRes = store.raftScan(nodesStart, nodesEnd, 0, includeSystemKeys = true)
    var nodeIds: seq[int] = @[]
    if nodesRes.isOk:
      for (key, entry) in nodesRes.value:
        try:
          let j = parseJson(entry.value)
          nodeIds.add(j["nodeId"].getInt())
        except: discard
    if nodeIds.len == 0: return
    nodeIds.sort()
    let nodeCount = nodeIds.len

    # Scan spaces
    let spacesStart = encodeTableKey(SYS_SPACES_TABLE_ID, "")
    let spacesEnd = encodeTableKey(SYS_SPACES_TABLE_ID + 1, "")
    let spacesRes = store.raftScan(spacesStart, spacesEnd, 0, includeSystemKeys = true)
    if not spacesRes.isOk: return

    for (key, entry) in spacesRes.value:
      try:
        let j = parseJson(entry.value)
        let spaceId = j["spaceId"].getInt()
        let currentGroupCount = j.getOrDefault("groupIds").len
        let isRebalancing = j.getOrDefault("rebalancing").getBool(false)

        # Skip if already rebalancing or group count matches
        if isRebalancing or currentGroupCount == nodeCount:
          continue

        let replicas = j["replicas"].getInt()
        # Skip the default space (replicas=0 means "all nodes", uses meta group)
        if replicas == 0:
          continue
        let effectiveReplicas = replicas
        if effectiveReplicas > nodeCount:
          continue

        # Find max existing groupId
        let grpStart = encodeTableKey(SYS_GROUPS_TABLE_ID, "")
        let grpEnd = encodeTableKey(SYS_GROUPS_TABLE_ID + 1, "")
        let grpRes = store.raftScan(grpStart, grpEnd, 0, includeSystemKeys = true)
        var maxGroupId: uint64 = 1
        if grpRes.isOk:
          for (gk, ge) in grpRes.value:
            try:
              let gj = parseJson(ge.value)
              let gid = uint64(gj["groupId"].getInt())
              if gid > maxGroupId: maxGroupId = gid
            except: discard

        # Create new groups with ring placement
        var newGroupIds: seq[uint64] = @[]
        let coord = store.coordinator
        for g in 0 ..< nodeCount:
          let groupId = maxGroupId + 1 + uint64(g)
          newGroupIds.add(groupId)

          var members: seq[int] = @[]
          for r in 0 ..< effectiveReplicas:
            members.add(nodeIds[(g + r) mod nodeCount])

          # Write group descriptor
          var replicasJson = newJArray()
          for m in members:
            replicasJson.add(%*{"nodeId": m, "type": "voter"})
          let groupKey = encodeTableKey(SYS_GROUPS_TABLE_ID, $groupId)
          let groupVal = $ %*{
            "groupId": int(groupId),
            "spaceId": spaceId,
            "replicas": replicasJson,
            "preferredLeader": members[0],
          }
          discard store.raftPut(groupKey, groupVal)

          # Create Raft group in coordinator
          let gid = GroupID(groupId)
          if not coord.hasGroup(gid):
            var desc = rangeTypes.newGroupDescriptor(gid)
            for m in members:
              discard desc.addReplica(rangeTypes.NodeID(uint32(m)), rangeTypes.rtVoter)
            var myReplicaId = rangeTypes.ReplicaID(0)
            for rep in desc.replicas:
              if rep.nodeId == coord.nodeId:
                myReplicaId = rep.replicaId
                break
            if myReplicaId != rangeTypes.ReplicaID(0):
              let newGroup = coord.createAndStartGroup(desc, myReplicaId)
              store.registerGroup(gid)
              # Single-node: become leader immediately
              if store.peerStores.len == 0:
                newGroup.becomeLeader()

        # Read current groupIds
        var oldGroupIds: seq[uint64] = @[]
        if j.hasKey("groupIds"):
          for r in j["groupIds"]:
            oldGroupIds.add(uint64(r.getInt()))

        # Update space record with rebalance state
        var space = SpaceInfo(
          spaceId: spaceId,
          name: j["name"].getStr(),
          replicas: j["replicas"].getInt(),
          groupIds: newGroupIds,
          oldGroupIds: oldGroupIds,
          rebalancing: true,
          rebalanceWorker: 0,
          rebalanceHeartbeat: 0,
          rebalanceCursor: "",
        )
        store.updateSpaceRecord(space)
      except:
        discard

    # Reload caches
    store.loadSpaces()
    store.loadGroupMembers()

proc runRebalanceMigration*(store: RaftKVStoreExt, spaceId: int) {.raises: [].} =
  ## Migrate data from old groups to new groups for a rebalancing space.
  ## Claims worker role, scans tables, moves keys, and completes cutover.
  {.cast(raises: []).}:
    # Read current space state
    acquire(store.spacesMu)
    var space: SpaceInfo
    var found = false
    if store.spaces.hasKey(spaceId):
      space = store.spaces[spaceId]
      found = true
    release(store.spacesMu)
    if not found or not space.rebalancing: return

    let myNodeId = int(store.coordinator.nodeId)

    # Claim worker role
    let nowSecs = getTime().toUnix()
    space.rebalanceWorker = myNodeId
    space.rebalanceHeartbeat = nowSecs
    store.updateSpaceRecord(space)
    store.loadSpaces()

    # Find all tables in this space
    let tablesStart = encodeTableKey(SYS_TABLES_TABLE_ID, "")
    let tablesEnd = encodeTableKey(SYS_TABLES_TABLE_ID + 1, "")
    let tablesRes = store.raftScan(tablesStart, tablesEnd, 0, includeSystemKeys = true)
    var tableIds: seq[uint32] = @[]
    if tablesRes.isOk:
      for (key, entry) in tablesRes.value:
        try:
          let j = parseJson(entry.value)
          let sid = j.getOrDefault("spaceId").getInt(1)
          if sid == spaceId:
            tableIds.add(uint32(j["tableId"].getInt()))
        except: discard

    let newGroupIds = space.groupIds
    let oldGroupIds = space.oldGroupIds
    let newCount = newGroupIds.len
    let oldCount = oldGroupIds.len
    if newCount == 0 or oldCount == 0: return

    var keysMigrated = 0
    var lastHeartbeat = nowSecs

    # Migrate each table
    for tableId in tableIds:
      let startKey = encodeTableKey(tableId, "d/")
      let endKey = encodeTableKey(tableId, "e")  # just past "d/" range

      # Scan from old groups: use backend scan (all groups share one backend)
      let backend = store.getBackend()
      if backend == nil or not backend.isOpen: continue

      var entries: seq[KeyValuePair] = @[]
      entries = backend.scan(
        if space.rebalanceCursor != "": space.rebalanceCursor
        else: startKey,
        endKey)

      for (k, v) in entries:
        # Extract primary key from the LevelDB key: /t/<tableId>/d/<pk>
        try:
          let decoded = decodeTableKey(k)
          if decoded.tableId != tableId: continue
          let afterD = decoded.primaryKey
          if not afterD.startsWith("d/"): continue
          let pk = afterD[2..^1]

          # Check if key needs to move
          let oldGroup = routeToGroup(pk, oldGroupIds)
          let newGroup = routeToGroup(pk, newGroupIds)
          if oldGroup != newGroup:
            # Write to new group via Raft consensus
            discard store.raftPutInSpace(k, v, space, pk)
            # Note: we do NOT delete from the old group here because all
            # groups on a node share one LevelDB backend — the LevelDB key
            # is the same regardless of routing group. Deleting from the
            # old group would remove the data we just wrote. Cleanup
            # happens at cutover when old Raft groups are removed.

          inc keysMigrated

          # Update cursor and heartbeat periodically
          if keysMigrated mod 100 == 0:
            let curNow = getTime().toUnix()
            # Re-read space to check if we're still the worker
            acquire(store.spacesMu)
            if store.spaces.hasKey(spaceId):
              let curSpace = store.spaces[spaceId]
              if curSpace.rebalanceWorker != myNodeId:
                release(store.spacesMu)
                return  # Another node took over
            release(store.spacesMu)

            space.rebalanceCursor = k
            space.rebalanceHeartbeat = curNow
            store.updateSpaceRecord(space)
            store.loadSpaces()
            lastHeartbeat = curNow
        except:
          continue

    # Phase 3: Cutover — migration complete
    # Remove old groups
    let coord = store.coordinator
    for oldGid64 in oldGroupIds:
      let oldGid = GroupID(oldGid64)
      # Delete from sys.groups
      let groupKey = encodeTableKey(SYS_GROUPS_TABLE_ID, $oldGid64)
      discard store.raftDelete(groupKey)
      # Remove from coordinator
      if coord.hasGroup(oldGid):
        coord.removeGroup(oldGid)

    # Clear rebalance state
    space.oldGroupIds = @[]
    space.rebalancing = false
    space.rebalanceWorker = 0
    space.rebalanceHeartbeat = 0
    space.rebalanceCursor = ""
    store.updateSpaceRecord(space)
    store.loadSpaces()
    store.loadGroupMembers()
