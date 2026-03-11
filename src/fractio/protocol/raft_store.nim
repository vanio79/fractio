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
import fractio/distributed/raft/multigroup_transport
import fractio/storage/wisckey_backend
import fractio/storage/backend
import fractio/protocol/client
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

  NodeInfo* = tuple[host: string, clientPort: int]

  RaftKVStoreExt* = ref object of RaftKVStore
    stateMachines*: Table[GroupID, KVStateMachine]  ## lightweight index tracking only
    smMu*: Lock  ## guards stateMachines table
    spaces*: Table[int, SpaceInfo]  ## spaceId → SpaceInfo
    tableSpaces*: Table[uint32, int] ## tableId → spaceId
    spacesMu*: Lock
    groupMembers*: Table[GroupID, seq[uint32]]   ## groupId → member nodeIds
    preferredLeaders*: Table[GroupID, uint32]     ## groupId → preferred leader nodeId
    nodeInfoCache*: Table[uint32, NodeInfo]       ## nodeId → (host, clientPort) for forwarding
    dataGroupLeaderNodeId*: Atomic[uint32]         ## tracked from AE heartbeats for forwarding
    groupLeaders*: Table[GroupID, uint32]           ## groupId → leader nodeId from sys.groups



proc newRaftKVStoreExt*(coord: MultiRaftCoordinator,
    proposeTimeoutMs: int = 5000): RaftKVStoreExt =
  result = RaftKVStoreExt(
    coordinator: coord,
    proposeTimeout: proposeTimeoutMs,
    logger: newLogger("protocol.raft_store"),
    stateMachines: initTable[GroupID, KVStateMachine](),
    spaces: initTable[int, SpaceInfo](),
    tableSpaces: initTable[uint32, int](),
    groupMembers: initTable[GroupID, seq[uint32]](),
    preferredLeaders: initTable[GroupID, uint32](),
    nodeInfoCache: initTable[uint32, NodeInfo](),
    groupLeaders: initTable[GroupID, uint32](),
  )
  initLock(result.smMu)
  initLock(result.spacesMu)
  result.nextVersion.store(1)

proc routeToGroup*(primaryKey: string, groupIds: seq[uint64]): GroupID {.inline.} =
  ## Hash-route a primary key to one of the space's groups.
  if groupIds.len == 0:
    return META_GROUP_ID
  if groupIds.len == 1:
    return GroupID(groupIds[0])
  let h = hash(primaryKey)
  let idx = abs(h) mod groupIds.len
  GroupID(groupIds[idx])

proc resolveGroupId*(store: RaftKVStoreExt, key: string): Option[GroupID] {.gcsafe,
    raises: [].} =
  ## Unified key → GroupID routing. Meta/system keys go to META_GROUP_ID,
  ## user-table data keys in a space route to the space's Raft group via
  ## hash(primaryKey), and everything else goes to DATA_GROUP_START_ID.
  if isMetaGroupKey(key):
    return some(META_GROUP_ID)
  # Check if this is a user-table data key that belongs to a space
  if isTableKey(key):
    {.cast(raises: []).}:
      try:
        let (tableId, primaryKey) = decodeTableKey(key)
        if tableId >= FIRST_USER_TABLE_ID:
          acquire(store.spacesMu)
          let sid = store.tableSpaces.getOrDefault(tableId, 0)
          if sid > 1 and store.spaces.hasKey(sid):
            let space = store.spaces.getOrDefault(sid)
            release(store.spacesMu)
            # decodeTableKey returns "d/<pk>" for data rows; strip the
            # "d/" prefix so we hash the same bare PK that raftPutInSpace
            # and the SQL executor use.
            let pk = if primaryKey.startsWith("d/"):
                       primaryKey[2 .. ^1]
                     else:
                       primaryKey
            return some(routeToGroup(pk, space.groupIds))
          release(store.spacesMu)
      except:
        discard
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
  store.groupLeaders.clear()
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
        if j.hasKey("leader"):
          let ldr = uint32(j["leader"].getInt())
          if ldr > 0:
            store.groupLeaders[gid] = ldr
      except:
        discard

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

# Forward declarations
proc raftPut*(store: RaftKVStoreExt, key, value: string): RSResult[
    RaftKVEntry] {.gcsafe, raises: [].}
proc lookupNodeInfo*(store: RaftKVStoreExt,
    nodeId: uint32): Option[NodeInfo] {.gcsafe, raises: [].}

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

    # --- Track data group leader from AE heartbeats ---
    multigroup_transport.onDataGroupLeaderSeen = proc(
        storePtr: pointer, leaderNodeId: uint32) {.gcsafe, raises: [].} =
      if storePtr == nil: return
      let s = cast[RaftKVStoreExt](storePtr)
      s.dataGroupLeaderNodeId.store(leaderNodeId)

    # --- Persist leader in sys.groups when a node wins election ---
    multigroup_coordinator.onLeaderChanged = proc(
        storePtr: pointer, groupId: GroupID,
        leaderNodeId: NodeID) {.gcsafe, raises: [].} =
      if storePtr == nil: return
      # Skip meta and data groups to avoid write loops
      if groupId == META_GROUP_ID or groupId == DATA_GROUP_START_ID: return
      let s = cast[RaftKVStoreExt](storePtr)
      GC_ref(s)
      defer: GC_unref(s)
      {.cast(raises: []).}:
        try:
          let groupKey = encodeTableKey(SYS_GROUPS_TABLE_ID,
              $uint64(groupId))
          let backend = s.getBackend()
          if backend == nil or not backend.isOpen: return
          let valOpt = backend.get(groupKey)
          if valOpt.isNone: return
          var j = parseJson(valOpt.get())
          j["leader"] = newJInt(int(uint32(leaderNodeId)))
          let updated = $j

          # Try local raftPut (routes to DATA_GROUP_START_ID)
          let res = s.raftPut(groupKey, updated)
          if res.isOk: return
          if not res.isOk and res.error.kind == rseNotLeader:
            # Forward to data group leader via ProtocolClient
            let dgLeader = s.dataGroupLeaderNodeId.load()
            if dgLeader == 0: return
            let infoOpt = s.lookupNodeInfo(dgLeader)
            if infoOpt.isNone: return
            let info = infoOpt.get()
            let cfg = ClientConfig(
              host: info.host,
              port: info.clientPort,
              timeoutMs: 3000,
            )
            let pc = newProtocolClient(cfg)
            let cr = pc.connect()
            if cr.isOk:
              discard pc.kvPut(groupKey, updated)
              pc.disconnect()
        except CatchableError:
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

proc raftPutInGroup*(store: RaftKVStoreExt, key, value: string,
    groupId: GroupID): RSResult[RaftKVEntry] {.gcsafe, raises: [].} =
  ## Write `value` under `key` through Raft consensus, routed to a specific group.
  ## Used by the protocol server for group-routed forwarded requests.
  let batch = newWriteBatch()
  batch.put(toBytes(key), toBytes(value))
  let vr = proposeWrite(store, groupId, batch)
  if not vr.isOk:
    return rsErr[RaftKVEntry](vr.error)
  let ver = store.nextVersion.fetchAdd(1)
  let ts = uint64(getTime().toUnixFloat() * 1_000_000_000)
  rsOk[RaftKVEntry](RaftKVEntry(value: value, version: ver, timestamp: ts))

proc raftDeleteInGroupExplicit*(store: RaftKVStoreExt, key: string,
    groupId: GroupID): RSResult[Option[RaftKVEntry]] {.gcsafe, raises: [].} =
  ## Delete `key` from a specific Raft group. Used by the protocol server for
  ## group-routed forwarded requests.
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
  let vr = proposeWrite(store, groupId, batch)
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
      if k.startsWith("/raft/"): continue
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
  ## Count all user keys (excluding intents, coord records, and Raft
  ## internal keys like /raft/*/log/* and /raft/*/state) via backend scan.
  var total = 0
  let backend = store.getBackend()
  if backend == nil or not backend.isOpen:
    return 0
  {.cast(raises: []).}:
    let raw = backend.scan("", "")
    for (k, _) in raw:
      if isIntentKey(k) or isCoordKey(k): continue
      if k.startsWith("/raft/"): continue
      inc total
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

proc lookupNodeInfo*(store: RaftKVStoreExt,
    nodeId: uint32): Option[NodeInfo] {.gcsafe, raises: [].} =
  ## Look up a node's host and clientPort, using a cache to avoid repeated
  ## backend reads. Falls back to scanning sys.nodes in the local backend.
  if store.nodeInfoCache.hasKey(nodeId):
    return some(store.nodeInfoCache.getOrDefault(nodeId))
  let backend = store.getBackend()
  if backend == nil or not backend.isOpen:
    return none(NodeInfo)
  {.cast(raises: []).}:
    try:
      let nodeKey = encodeTableKey(SYS_NODES_TABLE_ID, $nodeId)
      let valOpt = backend.get(nodeKey)
      if valOpt.isSome:
        let j = parseJson(valOpt.get())
        let host = j.getOrDefault("host").getStr("")
        let port = j.getOrDefault("clientPort").getInt(0)
        if host != "" and port > 0:
          let info: NodeInfo = (host: host, clientPort: port)
          store.nodeInfoCache[nodeId] = info
          return some(info)
    except:
      discard
  none(NodeInfo)


proc raftPutInSpace*(store: RaftKVStoreExt, key, value: string,
    space: SpaceInfo, primaryKey: string): RSResult[RaftKVEntry] {.gcsafe, raises: [].} =
  ## Write `value` under `key` through Raft consensus, routing to the
  ## correct group in the space via hash(primaryKey).
  ## Returns rseNotLeader if this node is not the leader for the target group.
  let rid = routeToGroup(primaryKey, space.groupIds)
  if not store.coordinator.hasGroup(rid):
    return rsErr[RaftKVEntry](newRSE(rseNotLeader,
        "not leader for group " & $rid.uint64))
  let batch = newWriteBatch()
  batch.put(toBytes(key), toBytes(value))
  let vr = proposeWrite(store, rid, batch)
  if not vr.isOk:
    return rsErr[RaftKVEntry](vr.error)
  let ver = store.nextVersion.fetchAdd(1)
  let ts = uint64(getTime().toUnixFloat() * 1_000_000_000)
  rsOk[RaftKVEntry](RaftKVEntry(value: value, version: ver, timestamp: ts))

proc raftGetInSpaceFromGroup(store: RaftKVStoreExt, key: string,
    rid: GroupID): RSResult[Option[RaftKVEntry]] {.gcsafe, raises: [].} =
  ## Internal helper: read `key` from a specific group.
  ## Returns the value if this node is the leader for the group.
  ## Returns rseNotLeader if this node is not the leader.
  {.cast(raises: []).}:
    let gOpt = store.coordinator.getGroup(rid)
    if gOpt.isNone or not gOpt.get.isLeader():
      return rsErr[Option[RaftKVEntry]](newRSE(rseNotLeader,
          "not leader for group " & $rid.uint64))
    let backend = store.getBackend()
    if backend != nil and backend.isOpen:
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
  ## During rebalancing, if the key is not found in the new group (or this node
  ## is not the leader for the new group), falls back to the old group.
  let rid = routeToGroup(primaryKey, space.groupIds)
  let res = store.raftGetInSpaceFromGroup(key, rid)
  if res.isOk and res.value.isSome:
    return res
  # Not found or not leader for new group — fall back to old group during rebalance
  if space.rebalancing and space.oldGroupIds.len > 0:
    let oldRid = routeToGroup(primaryKey, space.oldGroupIds)
    if oldRid != rid:
      let oldRes = store.raftGetInSpaceFromGroup(key, oldRid)
      if oldRes.isOk:
        return oldRes
  # Return the original result (success with none, or error)
  res


proc raftDeleteInSpace*(store: RaftKVStoreExt, key: string,
    space: SpaceInfo, primaryKey: string): RSResult[Option[RaftKVEntry]] {.gcsafe, raises: [].} =
  ## Delete `key` through Raft consensus, routing to the correct space group.
  ## Returns rseNotLeader if this node is not the leader for the target group.
  let rid = routeToGroup(primaryKey, space.groupIds)
  if not store.coordinator.hasGroup(rid):
    return rsErr[Option[RaftKVEntry]](newRSE(rseNotLeader,
        "not leader for group " & $rid.uint64))
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
    return rsErr[Option[RaftKVEntry]](vr.error)
  rsOk[Option[RaftKVEntry]](prevEntry)

proc raftDeleteInGroup*(store: RaftKVStoreExt, key: string,
    groupId: GroupID): RSResult[Option[RaftKVEntry]] {.gcsafe, raises: [].} =
  ## Delete `key` from a specific group (used during rebalance migration
  ## to remove a key from its old group after copying to the new one).
  ## Returns rseNotLeader if this node is not the leader for the group.
  if not store.coordinator.hasGroup(groupId):
    return rsErr[Option[RaftKVEntry]](newRSE(rseNotLeader,
        "not leader for group " & $groupId.uint64))
  let batch = newWriteBatch()
  batch.delete(toBytes(key))
  let vr = proposeWrite(store, groupId, batch)
  if not vr.isOk:
    return rsErr[Option[RaftKVEntry]](vr.error)
  rsOk[Option[RaftKVEntry]](none(RaftKVEntry))

proc raftScanSpace*(store: RaftKVStoreExt, startKey, endKey: string,
    space: SpaceInfo, limit: uint32 = 0,
    includeSystemKeys: bool = false): RSResult[seq[(string, RaftKVEntry)]] {.gcsafe, raises: [].} =
  ## Scan keys across all groups in the space.
  ##
  ## For each group, we need to read from the **leader** to guarantee
  ## we see committed data (followers may lag). Strategy:
  ## - If this node is the leader of a group, data is in the local backend.
  ## - Otherwise, scan the group's leader (or any member as fallback) over
  ##   the network.
  ##
  ## Since all groups on a node share the same WiscKey backend, we coalesce:
  ## one scan per distinct remote node is sufficient.

  # First, determine which groups this node leads vs which need remote scan.
  # A group is "locally covered" if this node is leader for it.
  var localNodeId = store.coordinator.nodeId.uint32
  var remoteNodes: seq[uint32]  # nodes we need to scan
  var allGroupsCoveredLocally = true
  {.cast(raises: []).}:
    for gid in space.groupIds:
      let groupId = GroupID(gid)
      # Check if this node is leader for this group
      if store.coordinator.hasGroup(groupId):
        let gOpt = store.coordinator.getGroup(groupId)
        if gOpt.isSome and gOpt.get.isLeader():
          continue  # leader — data is definitely in local backend
      allGroupsCoveredLocally = false
      # Need to scan a remote node for this group.
      # Prefer the known leader, fall back to any member.
      var targetNode: uint32 = 0
      if store.groupLeaders.hasKey(groupId):
        targetNode = store.groupLeaders[groupId]
      if targetNode == 0 or targetNode == localNodeId:
        let members = store.groupMembers.getOrDefault(groupId, @[])
        for nid in members:
          if nid != localNodeId:
            targetNode = nid
            break
      if targetNode != 0 and targetNode != localNodeId and
          targetNode notin remoteNodes:
        remoteNodes.add(targetNode)

  # Always start with local scan (covers groups we lead + follower data)
  let localResult = store.raftScan(startKey, endKey, limit,
      includeSystemKeys = includeSystemKeys)
  if not localResult.isOk:
    return localResult

  if remoteNodes.len == 0:
    return localResult

  # Fan out scan to remote nodes and merge results
  var allKeys: seq[string]
  var resultMap: Table[string, RaftKVEntry]
  for (k, entry) in localResult.value:
    if k notin allKeys:
      allKeys.add(k)
      resultMap[k] = entry

  {.cast(raises: []).}:
    try:
      for nid in remoteNodes:
        let infoOpt = store.lookupNodeInfo(nid)
        if infoOpt.isNone: continue
        let info = infoOpt.get()
        let cfg = ClientConfig(host: info.host, port: info.clientPort,
                               timeoutMs: 5000)
        let pc = newProtocolClient(cfg)
        let cr = pc.connect()
        if not cr.isOk: continue
        let sr = pc.kvScan(startKey, endKey, limit)
        pc.disconnect()
        if sr.isOk:
          for pair in sr.val.pairs:
            if pair.key notin resultMap:
              allKeys.add(pair.key)
              resultMap[pair.key] = RaftKVEntry(
                value: pair.value, version: 1'u64,
                timestamp: uint64(getTime().toUnixFloat() * 1_000_000_000),
              )
    except CatchableError:
      discard

  # Sort by key and build result
  allKeys.sort()
  var finalResult: seq[(string, RaftKVEntry)]
  for k in allKeys:
    finalResult.add((k, resultMap.getOrDefault(k)))
  if limit > 0 and uint32(finalResult.len) > limit:
    finalResult = finalResult[0 ..< int(limit)]
  rsOk[seq[(string, RaftKVEntry)]](finalResult)

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
              # Single-node: become leader immediately if group has only 1 member
              if members.len == 1:
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
            # Write to new group via Raft consensus.
            # Use proposeWrite directly (not raftPutInSpace) since this node
            # may not be the leader for the new group — proposeWrite will
            # return rseNotLeader which we skip (the leader for that group
            # must run its own migration).
            let newRid = GroupID(newGroup)
            if store.coordinator.hasGroup(newRid):
              let batch = newWriteBatch()
              batch.put(toBytes(k), toBytes(v))
              discard proposeWrite(store, newRid, batch)
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
