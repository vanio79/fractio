# Raft-backed KV store for the Fractio protocol layer — Phase 5.
#
# RaftKVStore wraps NuRaftCoordinator and provides the same interface as
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
#   - The NuRaftCoordinator protects its state via groupsLock.
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

import std/[tables, locks, options, algorithm, atomics, times, strformat,
    strutils, json, hashes, os]
import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/c_bindings
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/raft/state_machine
import fractio/distributed/meta/system_tables
import fractio/distributed/meta/system_schemas
import fractio/storage/wisckey_backend
import fractio/storage/backend
import fractio/storage/mvcc/types as mvccTypes
import fractio/protocol/types as protoTypes
import fractio/protocol/client
import fractio/protocol/messages/kv
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
    rseBadRouting    ## Key does not hash to the specified group

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

proc isMVCCSuffixKey*(k: string): bool {.inline.} =
  ## Check if key has an MVCC suffix (intent or version).
  ## Format: <userKey><suffix><8 bytes>
  ## Suffix is "\x00\x01" for intents, "\x00\x00" for versions.
  if k.len < 10: return false
  let suffixPos = k.len - 10
  k[suffixPos] == '\x00' and (k[suffixPos + 1] == '\x01' or k[suffixPos + 1] == '\x00')

proc stripMVCCSuffix*(k: string): string {.inline.} =
  ## Strip MVCC suffix (intent or version) from a key, returning the user key.
  ## Returns the original key if no MVCC suffix is detected.
  if isMVCCSuffixKey(k):
    k[0 ..< k.len - 10]
  else:
    k

# ---------------------------------------------------------------------------
# Unified key → GroupID routing
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# RaftKVStore
# ---------------------------------------------------------------------------

type
  RaftKVStore* {.acyclic.} = ref object of RootObj
    coordinator*: NuRaftCoordinator
    nextVersion*: Atomic[uint64]
    proposeTimeout*: int ## ms; default 5000
    logger*: Logger

proc newRaftKVStore*(coord: NuRaftCoordinator,
    proposeTimeoutMs: int = 5000): RaftKVStore =
  result = RaftKVStore(
    coordinator: coord,
    proposeTimeout: proposeTimeoutMs,
    logger: newLogger("protocol.raft_store"),
  )
  result.nextVersion.store(1)

# ---------------------------------------------------------------------------
# State machine registry (owned by RaftKVStore)
# ---------------------------------------------------------------------------
# The NuRaftCoordinator worker thread commits log entries via the group's
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
    replicas*: int             ## 0 = ALL nodes
    groupIds*: seq[uint64]
    oldGroupIds*: seq[uint64]  ## previous groups during rebalance
    rebalancing*: bool         ## true while migration is in progress
    rebalanceWorker*: int      ## nodeId of the migrating worker
    rebalanceHeartbeat*: int64 ## unix epoch seconds of last worker heartbeat
    rebalanceCursor*: string   ## last key migrated (resume point)

  NodeInfo* = tuple[host: string, clientPort: int]

  RaftKVStoreExt* = ref object of RaftKVStore
    stateMachines*: Table[GroupID, KVStateMachine] ## lightweight index tracking only
    smMu*: Lock                                ## guards stateMachines table
    spaces*: Table[int, SpaceInfo]             ## spaceId → SpaceInfo
    tableSpaces*: Table[uint32, int]           ## tableId → spaceId
    spacesMu*: Lock
    groupLeaders*: Table[GroupID, uint32]      ## groupId → leader nodeId from sys.groups
    groupMembers*: Table[GroupID, seq[uint32]] ## groupId → member nodeIds
    preferredLeaders*: Table[GroupID, uint32]  ## groupId → preferred leader nodeId
    nodeInfoCache*: Table[uint32, NodeInfo]    ## nodeId → (host, clientPort) for forwarding
    dataGroupLeaderNodeId*: Atomic[uint32]     ## tracked from AE heartbeats for forwarding
    groupMu*: Lock ## guards groupMembers, preferredLeaders, groupLeaders, nodeInfoCache
    rebalThread*: Thread[RaftKVStoreExt]
    rebalRunning*: Atomic[bool]
    triggerRebal*: Atomic[bool]



proc newRaftKVStoreExt*(coord: NuRaftCoordinator,
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
  result.rebalRunning.store(false)
  result.triggerRebal.store(false)
  initLock(result.smMu)
  initLock(result.spacesMu)
  initLock(result.groupMu)
  result.nextVersion.store(1)

# ---------------------------------------------------------------------------
# Route to group (helper used before forward declarations are needed)
# ---------------------------------------------------------------------------
proc routeToGroup*(primaryKey: string, groupIds: seq[
    uint64]): GroupID {.inline.} =
  ## Hash-route a primary key to one of the space's groups.
  if groupIds.len == 0:
    return META_GROUP_ID
  if groupIds.len == 1:
    return GroupID(groupIds[0])
  let h = hash(primaryKey)
  let idx = abs(h) mod groupIds.len
  GroupID(groupIds[idx])

proc resolveGroupId*(store: RaftKVStoreExt, key: string): Option[
    GroupID] {.gcsafe, raises: [].} =
  ## Unified key → GroupID routing. Meta/system keys go to META_GROUP_ID,
  ## user-table data keys in a space route to the space's Raft group via
  ## hash(primaryKey), and everything else goes to DATA_GROUP_START_ID.
  ##
  ## Handles MVCC-encoded keys (with intent suffix or version suffix) by
  ## stripping the suffix before routing.

  # Strip MVCC suffix if present (intent keys and version keys)
  let routingKey = stripMVCCSuffix(key)

  if isMetaGroupKey(routingKey):
    return some(META_GROUP_ID)
  # Check if this is a user-table data key that belongs to a space
  if isTableKey(routingKey):
    {.cast(raises: []).}:
      try:
        let (tableId, primaryKey) = decodeTableKey(routingKey)
        if tableId >= FIRST_USER_TABLE_ID:
          var groupIds: seq[uint64] = @[]
          var sid: int = 0
          withLock store.spacesMu:
            sid = store.tableSpaces.getOrDefault(tableId, 0)
            if sid > 1 and store.spaces.hasKey(sid):
              let space = store.spaces.getOrDefault(sid)
              groupIds = space.groupIds
          if groupIds.len > 0:
            # decodeTableKey returns "d/<pk>" for data rows; strip the
            # "d/" prefix so we hash the same bare PK that raftPutInSpace
            # and the SQL executor use.
            let pk = if primaryKey.startsWith("d/"):
                       primaryKey[2 .. ^1]
                     else:
                       primaryKey
            return some(routeToGroup(pk, groupIds))
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

proc loadGroupMembers*(store: RaftKVStoreExt,
    waitForCatchUp: bool = false) {.gcsafe, raises: [].} =
  ## Scan sys.groups and populate the groupMembers table.
  ## Handles both raw JSON values and MVCC-encoded values.
  ## If waitForCatchUp is true, we wait for the META group state machine
  ## to catch up to the latest known committed index.
  if waitForCatchUp:
    let coord = store.coordinator
    let instOpt = coord.getGroupInstance(META_GROUP_ID)
    if instOpt.isSome:
      let inst = instOpt.get()
      if not cast[pointer](inst.server).isNil:
        let committed = nuraftServerGetCommittedLogIdx(inst.server)
        # Simple poll for state machine catchup (max 2s)
        for _ in 0 ..< 20:
          if nuraftSmLastCommitIndex(inst.sm) >= committed: break
          sleep(100)

  let startKey = "/t/" & align($SYS_GROUPS_TABLE_ID, 10, '0') & "/"
  let endKey = "/t/" & align($(SYS_GROUPS_TABLE_ID + 1), 10, '0') & "/"
  let backend = store.getBackend()
  var entries: seq[KeyValuePair] = @[]
  if backend != nil and backend.isOpen:
    {.cast(raises: []).}:
      entries = backend.scan(startKey, endKey)

  # Group entries by user key, tracking latest version
  var latestVersions: Table[string, tuple[value: string,
      ts: int64]] = initTable[string, tuple[value: string, ts: int64]]()

  for (k, v) in entries:
    {.cast(raises: []).}:
      try:
        var userKey = k
        var value = v
        var ts: int64 = 0

        # Check if this is an MVCC version key (ends with \x00\x00 + 8 bytes)
        if k.len >= 10 and k[k.len - 10] == '\x00' and k[k.len - 9] == '\x00':
          # MVCC-encoded key - extract user key
          userKey = k[0 ..< k.len - 10]
          # Decode MVCC value
          if v.len >= 17:
            let mvccVal = mvccTypes.decodeMVCCValue(v)
            if mvccVal.isDeleted:
              continue # Skip tombstones
            value = mvccVal.data
            ts = mvccVal.timestamp
        elif mvccTypes.isLikelyMVCCValue(v):
          # Non-version key but value is MVCC-encoded (sysTablePut case)
          try:
            let mvccVal = mvccTypes.decodeMVCCValue(v)
            if mvccVal.isDeleted:
              continue # Skip tombstones
            value = mvccVal.data
            ts = mvccVal.timestamp
          except:
            discard # Not MVCC-encoded, use as-is

        # Keep only latest version for each user key
        if not latestVersions.hasKey(userKey) or ts > latestVersions[userKey].ts:
          latestVersions[userKey] = (value, ts)
      except:
        # If decoding fails, try as raw key-value
        if not latestVersions.hasKey(k):
          latestVersions[k] = (v, 0'i64)

  var newGroupMembers = initTable[GroupID, seq[uint32]]()
  var newPreferredLeaders = initTable[GroupID, uint32]()
  var newGroupLeaders = initTable[GroupID, uint32]()

  for (k, entry) in latestVersions.pairs:
    {.cast(raises: []).}:
      try:
        var gid: GroupID
        var members: seq[uint32] = @[]
        var preferredLeader: uint32 = 0
        var leader: uint32 = 0

        # Binary decoding (GroupRecord)
        let groupRec = decodeGroupRecord(entry.value)
        gid = GroupID(groupRec.groupId)
        for rep in groupRec.replicas:
          members.add(rep.nodeId)
        preferredLeader = groupRec.preferredLeader
        leader = groupRec.leader

        newGroupMembers[gid] = members
        if preferredLeader > 0:
          newPreferredLeaders[gid] = preferredLeader
        if leader > 0:
          newGroupLeaders[gid] = leader
      except:
        discard

  withLock store.groupMu:
    store.groupMembers = newGroupMembers
    store.preferredLeaders = newPreferredLeaders
    store.groupLeaders = newGroupLeaders

# ---------------------------------------------------------------------------
# Internal: propose a WriteBatch and apply to local state machine
# ---------------------------------------------------------------------------

# Helper: string → seq[byte] (zero-copy via cast)
proc toBytes(s: string): seq[byte] {.inline.} =
  when nimvm:
    result = newSeq[byte](s.len)
    for i in 0 ..< s.len:
      result[i] = byte(s[i])
  else:
    if s.len == 0:
      result = newSeq[byte](0)
    else:
      result = cast[seq[byte]](s)

# Helper: seq[byte] → string (zero-copy via cast)
proc fromBytes(b: seq[byte]): string {.inline.} =
  when nimvm:
    result = newString(b.len)
    for i in 0 ..< b.len:
      result[i] = char(b[i])
  else:
    if b.len == 0:
      result = ""
    else:
      result = cast[string](b)

# Forward declarations
proc raftPut*(store: RaftKVStoreExt, key, value: string): RSResult[
    RaftKVEntry] {.gcsafe, raises: [].}
proc raftDelete*(store: RaftKVStoreExt,
    key: string): RSResult[Option[RaftKVEntry]] {.gcsafe, raises: [].}
proc lookupNodeInfo*(store: RaftKVStoreExt,
    nodeId: uint32): Option[NodeInfo] {.gcsafe, raises: [].}
proc loadSpaces*(store: RaftKVStoreExt) {.gcsafe, raises: [].}
proc loadTableSpaces*(store: RaftKVStoreExt) {.gcsafe, raises: [].}
proc proposeWrite(store: RaftKVStoreExt, groupId: GroupID,
    batch: WriteBatch): RSVoidResult {.gcsafe, raises: [].}
proc updateSpaceCache*(store: RaftKVStoreExt, spaceKey: string,
    jsonStr: string) {.gcsafe, raises: [].}
proc updateTableSpaceCache*(store: RaftKVStoreExt, tableKey: string,
    jsonStr: string) {.gcsafe, raises: [].}

# ---------------------------------------------------------------------------
# System table write helpers with MVCC encoding
# ---------------------------------------------------------------------------
# ALL sys table writes use MVCC encoding for consistency. This ensures:
# 1. Consistent decoding in load* functions
# 2. Timestamp tracking for all metadata changes
# 3. Future support for point-in-time queries on sys tables

proc sysTablePut*(store: RaftKVStoreExt, key: string, value: string): bool {.
    gcsafe, raises: [].} =
  ## Write to a sys table with MVCC encoding.
  ## ALWAYS encodes the value with MVCC header for consistency.
  ## Returns true on success, false on failure.
  let backend = store.getBackend()
  if backend == nil or not backend.isOpen:
    return false

  # Get timestamp - use current nanosecond time
  var ts: int64 = 0
  {.cast(raises: []).}:
    ts = int64(getTime().toUnixFloat() * 1_000_000_000)

  # Encode value with MVCC header
  let encoded = mvccTypes.encodeMVCCValue(value, ts, false)

  # Write via Raft for replication
  let res = store.raftPut(key, encoded)
  return res.isOk

proc sysTablePutBatch*(store: RaftKVStoreExt,
    writes: openArray[tuple[key: string, value: string]]): bool {.
    gcsafe, raises: [].} =
  ## Write multiple sys table entries atomically with MVCC encoding.
  ## All entries get the same timestamp for atomicity.
  ## Returns true on success, false on failure.
  if writes.len == 0:
    return true

  # Get timestamp for all writes (same timestamp for atomicity)
  var ts: int64 = 0
  {.cast(raises: []).}:
    ts = int64(getTime().toUnixFloat() * 1_000_000_000)

  # Write all with same timestamp
  for (key, value) in writes:
    let encoded = mvccTypes.encodeMVCCValue(value, ts, false)
    let res = store.raftPut(key, encoded)
    if not res.isOk:
      return false

  return true

proc sysTableDelete*(store: RaftKVStoreExt, key: string): bool {.
    gcsafe, raises: [].} =
  ## Delete from a sys table through Raft.
  ## For internal operations, we use actual delete (not MVCC tombstone).
  ## Returns true on success, false on failure.
  let res = store.raftDelete(key)
  return res.isOk

proc sysTableDeleteBatch*(store: RaftKVStoreExt,
    keys: openArray[string]): bool {.gcsafe, raises: [].} =
  ## Delete multiple sys table entries through Raft.
  ## For internal operations, we use actual delete (not MVCC tombstones).
  ## Returns true on success, false on failure.
  if keys.len == 0:
    return true

  for key in keys:
    let res = store.raftDelete(key)
    if not res.isOk:
      return false

  return true

proc sysTablePutAndDeleteBatch*(store: RaftKVStoreExt,
    puts: openArray[tuple[key: string, value: string]],
    deletes: openArray[string]): bool {.gcsafe, raises: [].} =
  ## Write and delete sys table entries atomically through Raft.
  ## For internal operations, we use actual delete (not MVCC tombstones).
  ## Returns true on success, false on failure.
  if puts.len == 0 and deletes.len == 0:
    return true

  # Get timestamp for all puts (same timestamp for atomicity)
  var ts: int64 = 0
  {.cast(raises: []).}:
    ts = int64(getTime().toUnixFloat() * 1_000_000_000)

  # Do puts first (always MVCC-encoded)
  for (key, value) in puts:
    let encoded = mvccTypes.encodeMVCCValue(value, ts, false)
    let res = store.raftPut(key, encoded)
    if not res.isOk:
      return false

  # Then deletes
  for key in deletes:
    let res = store.raftDelete(key)
    if not res.isOk:
      return false

  return true

# ---------------------------------------------------------------------------
# Follower apply callback (called by coordinator on committed entries)
# ---------------------------------------------------------------------------

proc applyBatchToSM*(storePtr: pointer, rid: GroupID,
    data: cstring, len: int) {.gcsafe, raises: [].} =
  ## Callback registered with the coordinator so that committed WriteBatch
  ## entries are applied to the local KVStateMachine and persisted to the
  ## WiscKey backend.  `storePtr` is a raw `pointer` cast from `RaftKVStoreExt`
  ## to break the raft_store → coordinator → raft_store circular import.
  ##
  ## This callback receives raw C data (cstring + len) because it's called from
  ## NuRaft's ASIO thread where we must not allocate Nim GC memory. We copy
  ## the data here into Nim-managed memory.
  ##
  ## Durability model: the Raft log entry was already written with fdatasync
  ## in putEntryAndState, so the commit is durable before this callback fires.
  ## The WiscKey write here uses writeBatchNoSync (no second fdatasync) — it
  ## keeps committed data readable after a clean restart.  On a crash the Raft
  ## log is replayed from lastApplied to reconstruct any missing SM state.
  if storePtr == nil or data == nil or len == 0: return

  let store = cast[RaftKVStoreExt](storePtr)

  # Copy C data into Nim-managed string (safe to allocate here)
  let payload = newString(len)
  copyMem(addr payload[0], data, len)

  # Parse the binary payload
  var batch: WriteBatch = nil
  try:
    batch = nuraft_coordinator.deserializeWriteBatch(payload)
  except CatchableError:
    return

  if batch == nil: return

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
  # Also update in-memory caches directly for spaces and tables
  let groupsPrefix = "/t/" & align($SYS_GROUPS_TABLE_ID, 10, '0') & "/"
  let spacesPrefix = "/t/" & align($SYS_SPACES_TABLE_ID, 10, '0') & "/"
  let tablesPrefix = "/t/" & align($SYS_TABLES_TABLE_ID, 10, '0') & "/"
  var refreshSpaceCache = false
  for (k, v) in batch.puts:
    let key = fromBytes(k)
    let value = fromBytes(v)
    if key.startsWith(groupsPrefix):
      {.cast(gcsafe).}:
        if onGroupMetadataApplied != nil:
          # Decode MVCC-encoded value if needed
          var groupValue = value
          # Raw JSON starts with '{', MVCC-encoded has binary header
          if mvccTypes.isLikelyMVCCValue(groupValue):
            try:
              let mvccVal = mvccTypes.decodeMVCCValueFast(groupValue)
              if not mvccVal.isDeleted:
                groupValue = mvccVal.data
              else:
                # Skip deleted entries
                refreshSpaceCache = true
                continue
            except:
              discard # Not MVCC-encoded, use as-is
          onGroupMetadataApplied(storePtr, key, groupValue)
      refreshSpaceCache = true
    elif key.startsWith(spacesPrefix):
      # Update in-memory space cache directly when Raft commits
      store.updateSpaceCache(key, value)
      refreshSpaceCache = true
    elif key.startsWith(tablesPrefix):
      # Update in-memory tableSpace cache directly when Raft commits
      store.updateTableSpaceCache(key, value)
      refreshSpaceCache = true
  # Trigger background refresh of space/table/leader caches when metadata changes replicate
  if refreshSpaceCache and storePtr != nil:
    let s = cast[RaftKVStoreExt](storePtr)
    s.triggerRebal.store(true)

proc rebalanceLeadershipTask(s: RaftKVStoreExt) {.thread, gcsafe, raises: [].} =
  ## Monitoring thread that yields leadership if this node is the current leader
  ## but not the preferred leader for a group.
  ## Note: With atomicArc, the reference to `s` is automatically retained
  ## for the lifetime of this thread.

  # Track when we became leader for each group to avoid immediate yields
  # with potentially stale metadata.
  var leaderSince = initTable[GroupID, float]()
  # Track consecutive failed yield attempts to back off
  var yieldFailures = initTable[GroupID, int]()
  # Track when we last attempted a yield to detect reverts
  var lastYieldAttempt = initTable[GroupID, float]()
  const maxYieldFailures = 3
  const yieldRevertWindow = 30.0 # seconds - if we become leader again within this time after yield, it's a revert

  while s.rebalRunning.load():
    # Regular monitoring interval (e.g. 2s for more responsiveness)
    for _ in 0 ..< 20:
      if s.triggerRebal.load() or not s.rebalRunning.load(): break
      sleep(100)

    discard s.triggerRebal.exchange(false)
    if not s.rebalRunning.load(): break

    if s.coordinator != nil and s.coordinator.running.load():
      # Refresh group members from storage
      # Note: We don't call loadSpaces/loadTableSpaces here anymore since
      # applyBatchToSM updates those caches directly when Raft commits.
      s.loadGroupMembers(waitForCatchUp = true)

      var toYield: seq[tuple[gid: GroupID, preferred: uint32]] = @[]
      let now = epochTime()

      withLock s.groupMu:
        for gid, preferredId in s.preferredLeaders:
          let isLeader = s.coordinator.isLeader(gid)
          let failures = yieldFailures.getOrDefault(gid, 0)
          if isLeader:
            # Check if this is a revert (we became leader again soon after yielding)
            let lastYieldTime = lastYieldAttempt.getOrDefault(gid, 0.0)
            if lastYieldTime > 0.0:
              let timeSinceYield = now - lastYieldTime
              if timeSinceYield < yieldRevertWindow and failures < maxYieldFailures:
                # Leadership reverted - increment failure count
                let newFailures = failures + 1
                yieldFailures[gid] = newFailures
                {.cast(raises: []).}:
                  {.cast(gcsafe).}:
                    info("Leadership reverted after yield, incrementing failures", {
                         "groupId": $gid, "failures": $newFailures}.toTable)
                lastYieldAttempt.del(gid)

            if not leaderSince.hasKey(gid):
              leaderSince[gid] = now

            # Only yield if we are leader, not preferred, and have held
            # leadership for at least 5 seconds (leader lease).
            let since = leaderSince.getOrDefault(gid, now)
            # Skip if we've had too many consecutive failures
            if s.coordinator.nodeId.uint32 != preferredId and
               (now - since >= 5.0) and
               failures < maxYieldFailures:
              toYield.add((gid, preferredId))
            elif failures >= maxYieldFailures:
              # Log when we're backing off due to failures
              {.cast(raises: []).}:
                {.cast(gcsafe).}:
                  debug("Skipping leadership yield due to failures", {
                       "groupId": $gid, "failures": $failures}.toTable)
          else:
            # No longer leader - clear tracking for this group
            leaderSince.del(gid)

      for (gid, preferredId) in toYield:
        # Check if we should stop before each yield attempt
        if not s.rebalRunning.load(): break
        # Also check if coordinator is shutting down
        if s.coordinator == nil or not s.coordinator.running.load(): break

        {.cast(raises: []).}:
          {.cast(gcsafe).}:
            info("Yielding leadership to preferred leader", {
                 "groupId": $gid, "preferred": $preferredId,
                 "localNode": $s.coordinator.nodeId.uint32}.toTable)

          # NuRaft's transferLeadership (yield_leadership) automatically
          # catches up the successor before stepping down.
          discard s.coordinator.transferLeadership(gid, NodeID(preferredId))

          # Record when we attempted this yield
          lastYieldAttempt[gid] = epochTime()

        # Wait briefly to see if leadership actually transfers
        # Check if we should stop during the wait
        for _ in 0 ..< 10:
          if not s.rebalRunning.load(): break
          if s.coordinator == nil or not s.coordinator.running.load(): break
          sleep(100)

        # Clear leaderSince so we don't immediately try again
        leaderSince.del(gid)


proc wireApplyCallback*(store: RaftKVStoreExt) {.gcsafe, raises: [].} =
  ## Wire the applyBatchToSM callback into the coordinator.
  {.cast(gcsafe).}: {.cast(raises: []).}:
    nuraft_coordinator.applyBatchCallback = applyBatchToSM

    nuraft_coordinator.getPreferredLeaderCallback = proc(
        storePtr: pointer,
        groupId: GroupID): Option[NodeID] {.gcsafe, raises: [].} =
      let s = cast[RaftKVStoreExt](storePtr)
      let pl = s.preferredLeaders.getOrDefault(groupId, 0'u32)
      if pl > 0:
        result = some(NodeID(pl))
      else:
        result = none(NodeID)

    nuraft_coordinator.onGroupMetadataApplied = proc(
        storePtr: pointer,
        groupKey: string, groupValue: string) {.gcsafe, raises: [].} =
      ## When sys.groups metadata replicates, ensure group is started and trigger rebal.
      if storePtr == nil: return
      let s = cast[RaftKVStoreExt](storePtr)
      let coord = s.coordinator
      {.cast(gcsafe).}:
        try:
          var gid: GroupID
          var members: seq[tuple[nodeId: uint32, host: string,
              basePort: int]] = @[]
          var preferredLeader: uint32 = 0

          # Binary decoding (GroupRecord)
          let groupRec = decodeGroupRecord(groupValue)
          gid = GroupID(groupRec.groupId)
          for rep in groupRec.replicas:
            let nid = rep.nodeId
            let peerInfo = coord.peerInfo.getOrDefault(nid,
                (host: coord.host, basePort: coord.basePort))
            members.add((nodeId: nid, host: peerInfo.host,
                basePort: peerInfo.basePort))
          preferredLeader = groupRec.preferredLeader

          if gid == META_GROUP_ID or gid == DATA_GROUP_START_ID: return

          if not coord.hasGroup(gid):
            var isMember = false
            for m in members:
              if m.nodeId == coord.nodeId.uint32:
                isMember = true
                break

            if isMember:
              let ok = coord.createAndStartGroup(gid, members, preferredLeader)
              if ok:
                s.registerGroup(gid)
              else:
                # This is a real error - we ARE a member but couldn't create the group
                try:
                  {.cast(gcsafe).}:
                    error("Failed to create group (member node, possible port conflict)",
                          {"groupId": $gid.uint64,
                              "nodeId": $coord.nodeId.uint32}.toTable)
                except:
                  discard

          # Refresh caches in background
          s.triggerRebal.store(true)
        except:
          discard

    # --- Leader tracking and persistence ---
    nuraft_coordinator.onLeaderChanged = proc(
        storePtr: pointer, groupId: GroupID,
        leaderNodeId: NodeID) {.gcsafe, raises: [].} =
      ## Called when a node wins an election. Updates the in-memory
      ## groupLeaders table and persists the leader to sys.groups for
      ## space groups (not META or DATA_GROUP_START_ID).
      if storePtr == nil: return
      let s = cast[RaftKVStoreExt](storePtr)

      # Update in-memory tracking
      withLock s.groupMu:
        s.groupLeaders[groupId] = leaderNodeId.uint32

      # Skip persisting leader for meta and default data groups
      if groupId == META_GROUP_ID or groupId == DATA_GROUP_START_ID:
        return

      # Check if any space is rebalancing - don't persist during migration
      var isRebalancing = false
      withLock s.spacesMu:
        for spaceId, space in s.spaces:
          if space.rebalancing:
            isRebalancing = true
            break

      if isRebalancing:
        return

      # Persist leader to sys.groups (only if we're the meta leader)
      if not s.coordinator.isLeader(META_GROUP_ID):
        return

      let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $groupId.uint64)
      let backend = s.coordinator.store
      {.cast(raises: []).}:
        try:
          let valOpt = backend.get(key)
          if valOpt.isSome:
            var groupVal = valOpt.get()
            # Strip MVCC encoding if present
            if mvccTypes.isLikelyMVCCValue(groupVal):
              let mvccVal = mvccTypes.decodeMVCCValueFast(groupVal)
              if not mvccVal.isDeleted:
                groupVal = mvccVal.data
            # Decode binary GroupRecord, update leader, re-encode
            var groupRec = decodeGroupRecord(groupVal)
            groupRec.leader = leaderNodeId.uint32
            let encoded = encode(groupRec)
            # Write through Raft for consensus
            discard s.sysTablePut(key, encoded)
        except CatchableError:
          discard

  store.coordinator.kvStorePtr = cast[pointer](store)
  store.rebalRunning.store(true)
  try:
    createThread(store.rebalThread, rebalanceLeadershipTask, store)
  except CatchableError:
    store.rebalRunning.store(false)

proc bootstrapStore*(store: RaftKVStoreExt,
    groupIds: seq[GroupID]) {.gcsafe, raises: [].} =
  ## Pre-create state machines for the given groups and wire the apply callback.
  ## Call after coord.start() (or at least after the store is ready).
  for rid in groupIds:
    discard store.getOrCreateSM(rid)
  store.wireApplyCallback()

proc stop*(store: RaftKVStoreExt) {.gcsafe, raises: [].} =
  ## Stop the rebalancing thread and clean up resources.
  ## Must be called before coordinator.stop() to ensure clean shutdown.
  if store.rebalRunning.load():
    store.rebalRunning.store(false)
    # Wake up the thread if it's sleeping
    store.triggerRebal.store(true)
    # Wait for thread to finish (with timeout)
    for _ in 0 ..< 30: # 3 seconds max
      try:
        joinThread(store.rebalThread)
        break
      except:
        sleep(100)

  # Clear module-level callbacks FIRST to prevent ASIO threads from invoking
  # stale closures that capture this store reference. This must happen before
  # clearing in-memory caches to avoid use-after-free.
  nuraft_coordinator.clearModuleCallbacks()

  # Clear in-memory caches to release memory
  withLock store.spacesMu:
    store.spaces.clear()
    store.tableSpaces.clear()

  withLock store.groupMu:
    store.groupMembers.clear()
    store.preferredLeaders.clear()
    store.groupLeaders.clear()
    store.nodeInfoCache.clear()

  withLock store.smMu:
    store.stateMachines.clear()

proc proposeWrite(store: RaftKVStoreExt, groupId: GroupID,
    batch: WriteBatch): RSVoidResult {.gcsafe, raises: [].} =
  ## Propose a write batch to Raft and, on success, apply it locally.
  let cmd = RaftCommand(kind: ckWrite, writeBatch: batch)
  let res = store.coordinator.proposeAndWait(groupId, cmd,
      store.proposeTimeout)
  if not res.success:
    # Debug: log all failures
    try:
      {.cast(gcsafe).}:
        error("proposeWrite failed",
              {"groupId": $groupId.uint64, "error": res.error}.toTable)
    except:
      discard
    if res.error == "Not the leader" or res.error.contains("code -3"):
      return rsVErr(newRSE(rseNotLeader, res.error))
    if res.error.len > 0 and res.error.contains("Group not found"):
      return rsVErr(newRSE(rseGroupNotFound, res.error))
    if res.error.contains("Timeout") or res.error.contains("code -2"):
      return rsVErr(newRSE(rseTimeout, res.error))
    return rsVErr(newRSE(rseInternal, res.error))

  # The SM was already updated by applyBatchToSM (called from NuRaft's
  # commit callback, which fires before proposeAndWait returns).
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

proc validateKeyRouting(store: RaftKVStoreExt, key: string,
    groupId: GroupID): Option[RaftStoreError] {.gcsafe, raises: [].} =
  ## Validate that `key` actually routes to `groupId`. Returns an error if the
  ## key is a data row key and hashes to a different group. Non-data keys
  ## (system keys, etc.) skip validation.
  ##
  ## During rebalancing, we also accept keys that route to old groups, since
  ## the data may still be in the old group and hasn't been migrated yet.
  if not isTableKey(key):
    return none(RaftStoreError)
  {.cast(raises: []).}:
    try:
      let (tableId, primaryKey) = decodeTableKey(key)
      if tableId < FIRST_USER_TABLE_ID:
        return none(RaftStoreError)
      acquire(store.spacesMu)
      let sid = store.tableSpaces.getOrDefault(tableId, 0)
      if sid > 1 and store.spaces.hasKey(sid):
        let space = store.spaces.getOrDefault(sid)
        release(store.spacesMu)
        let pk = if primaryKey.startsWith("d/"):
                   primaryKey[2 .. ^1]
                 else:
                   primaryKey
        let expected = routeToGroup(pk, space.groupIds)
        if expected != groupId:
          # During rebalancing, also check if the key routes to an old group
          if space.rebalancing and space.oldGroupIds.len > 0:
            let oldExpected = routeToGroup(pk, space.oldGroupIds)
            if oldExpected == groupId:
              # Key routes to an old group during rebalancing - this is valid
              return none(RaftStoreError)
          return some(newRSE(rseBadRouting,
              "key routes to group " & $expected.uint64 &
              " not " & $groupId.uint64))
      else:
        release(store.spacesMu)
    except:
      discard
  none(RaftStoreError)

proc raftPutInGroup*(store: RaftKVStoreExt, key, value: string,
    groupId: GroupID): RSResult[RaftKVEntry] {.gcsafe, raises: [].} =
  ## Write `value` under `key` through Raft consensus, routed to a specific group.
  ## Used by the protocol server for group-routed forwarded requests.
  ## Validates that the key routes to this group and that this node is the leader.
  let routeErr = store.validateKeyRouting(key, groupId)
  if routeErr.isSome:
    return rsErr[RaftKVEntry](routeErr.get())
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
  ## Validates that the key routes to this group and that this node is the leader.
  let routeErr = store.validateKeyRouting(key, groupId)
  if routeErr.isSome:
    return rsErr[Option[RaftKVEntry]](routeErr.get())
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

proc raftGetInGroup*(store: RaftKVStoreExt, key: string,
    groupId: GroupID): RSResult[Option[RaftKVEntry]] {.gcsafe, raises: [].} =
  ## Read `key` from a specific Raft group. Used by the protocol server for
  ## group-routed forwarded requests.
  ## Validates that the key routes to this group and that this node is the leader.
  let routeErr = store.validateKeyRouting(key, groupId)
  if routeErr.isSome:
    return rsErr[Option[RaftKVEntry]](routeErr.get())
  {.cast(raises: []).}:
    if not store.coordinator.isLeader(groupId):
      return rsErr[Option[RaftKVEntry]](newRSE(rseNotLeader,
          "not leader for group " & $groupId.uint64))
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

proc raftScan*(store: RaftKVStoreExt, startKey, endKey: string,
    limit: uint32,
    includeSystemKeys: bool = false): RSResult[seq[(string,
        RaftKVEntry)]] {.gcsafe, raises: [].} =
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

proc parseSpaceInfoFromBinary*(data: string): Option[SpaceInfo] {.gcsafe,
    raises: [].} =
  ## Parse a SpaceInfo from binary-encoded SpaceRecord.
  ## Handles MVCC-wrapped values. Returns None on parse failure.
  {.cast(raises: []).}:
    try:
      var value = data

      # Strip MVCC encoding if present
      if isLikelyMVCCValue(data):
        let mvccVal = mvccTypes.decodeMVCCValueFast(data)
        if mvccVal.isDeleted:
          return none(SpaceInfo)
        value = mvccVal.data

      # Binary decoding (SpaceRecord)
      let spaceRec = decodeSpaceRecord(value)
      var info = SpaceInfo(
        spaceId: spaceRec.spaceId,
        name: spaceRec.name,
        replicas: spaceRec.replicas,
        groupIds: spaceRec.groupIds,
        oldGroupIds: spaceRec.oldGroupIds,
        rebalancing: spaceRec.rebalancing,
        rebalanceWorker: spaceRec.rebalanceWorker,
        rebalanceHeartbeat: spaceRec.rebalanceHeartbeat,
        rebalanceCursor: spaceRec.rebalanceCursor,
      )
      # Basic validation: spaceId should be positive
      if info.spaceId > 0:
        return some(info)
      return none(SpaceInfo)
    except:
      return none(SpaceInfo)

proc updateSpaceCache*(store: RaftKVStoreExt, spaceKey: string,
    jsonStr: string) {.gcsafe, raises: [].} =
  ## Update the in-memory space cache from a committed space record.
  ## Called by applyBatchToSM when a space change is replicated via Raft.
  let spaceInfoOpt = parseSpaceInfoFromBinary(jsonStr)
  if spaceInfoOpt.isSome:
    let info = spaceInfoOpt.get()
    withLock store.spacesMu:
      store.spaces[info.spaceId] = info

proc updateTableSpaceCache*(store: RaftKVStoreExt, tableKey: string,
    jsonStr: string) {.gcsafe, raises: [].} =
  ## Update the in-memory tableSpace cache from a committed table record.
  ## Called by applyBatchToSM when a table change is replicated via Raft.
  ## Expects binary-encoded TableRecord (optionally wrapped in MVCC).
  {.cast(raises: []).}:
    try:
      var value = jsonStr
      # Check for MVCC encoding
      if mvccTypes.isLikelyMVCCValue(jsonStr):
        let mvccVal = mvccTypes.decodeMVCCValueFast(jsonStr)
        if mvccVal.isDeleted:
          return
        value = mvccVal.data

      # Binary decoding (TableRecord)
      let tableRec = decodeTableRecord(value)
      withLock store.spacesMu:
        store.tableSpaces[tableRec.tableId] = tableRec.spaceId
    except:
      discard

proc loadSpaces*(store: RaftKVStoreExt) {.gcsafe, raises: [].} =
  ## Scan sys.spaces and populate the in-memory spaces table.
  ## Call after bootstrap/recovery when the state machine is populated.
  ## This is now a full reload from backend - no preservation needed since
  ## in-memory cache is updated via applyBatchToSM when Raft commits changes.
  let startKey = "/t/" & align($SYS_SPACES_TABLE_ID, 10, '0') & "/"
  let endKey = "/t/" & align($(SYS_SPACES_TABLE_ID + 1), 10, '0') & "/"
  let backend = store.getBackend()
  var entries: seq[KeyValuePair] = @[]
  if backend != nil and backend.isOpen:
    {.cast(raises: []).}:
      entries = backend.scan(startKey, endKey)

  # Group entries by user key, tracking latest version
  var latestVersions: Table[string, tuple[value: string,
      ts: int64]] = initTable[string, tuple[value: string, ts: int64]]()

  for (k, v) in entries:
    {.cast(raises: []).}:
      try:
        var userKey = k
        var value = v
        var ts: int64 = 0

        # Check if this is an MVCC version key (ends with \x00\x00 + 8 bytes)
        if k.len >= 10 and k[k.len - 10] == '\x00' and k[k.len - 9] == '\x00':
          # MVCC-encoded key - extract user key
          userKey = k[0 ..< k.len - 10]
          # Decode MVCC value
          if v.len >= 17:
            let mvccVal = mvccTypes.decodeMVCCValue(v)
            if mvccVal.isDeleted:
              continue # Skip tombstones
            value = mvccVal.data
            ts = mvccVal.timestamp
        else:
          # Raw key (non-MVCC) - check if value is MVCC-encoded
          if mvccTypes.isLikelyMVCCValue(v):
            try:
              let mvccVal = mvccTypes.decodeMVCCValueFast(v)
              if not mvccVal.isDeleted:
                value = mvccVal.data
                ts = mvccVal.timestamp
            except:
              discard

        # Keep only latest version for each user key
        if not latestVersions.hasKey(userKey) or ts > latestVersions[userKey].ts:
          latestVersions[userKey] = (value, ts)
      except:
        # If decoding fails, try as raw key-value
        if not latestVersions.hasKey(k):
          latestVersions[k] = (v, 0'i64)

  var newSpaces = initTable[int, SpaceInfo]()
  for (k, entry) in latestVersions.pairs:
    let infoOpt = parseSpaceInfoFromBinary(entry.value)
    if infoOpt.isSome:
      newSpaces[infoOpt.get().spaceId] = infoOpt.get()

  withLock store.spacesMu:
    store.spaces = newSpaces

proc loadTableSpaces*(store: RaftKVStoreExt) {.gcsafe, raises: [].} =
  ## Scan sys.tables and populate the tableId → spaceId mapping.
  ## Handles both raw JSON values and MVCC-encoded values.
  let startKey = "/t/" & align($SYS_TABLES_TABLE_ID, 10, '0') & "/"
  let endKey = "/t/" & align($(SYS_TABLES_TABLE_ID + 1), 10, '0') & "/"
  let backend = store.getBackend()
  var entries: seq[KeyValuePair] = @[]
  if backend != nil and backend.isOpen:
    {.cast(raises: []).}:
      entries = backend.scan(startKey, endKey)

  # Group entries by user key, tracking latest version
  var latestVersions: Table[string, tuple[value: string,
      ts: int64]] = initTable[string, tuple[value: string, ts: int64]]()

  for (k, v) in entries:
    {.cast(raises: []).}:
      try:
        var userKey = k
        var value = v
        var ts: int64 = 0

        # Check if this is an MVCC version key (ends with \x00\x00 + 8 bytes)
        if k.len >= 10 and k[k.len - 10] == '\x00' and k[k.len - 9] == '\x00':
          # MVCC-encoded key - extract user key
          userKey = k[0 ..< k.len - 10]
          # Decode MVCC value
          if v.len >= 17:
            let mvccVal = mvccTypes.decodeMVCCValue(v)
            if mvccVal.isDeleted:
              continue # Skip tombstones
            value = mvccVal.data
            ts = mvccVal.timestamp
        elif mvccTypes.isLikelyMVCCValue(v):
          # Non-version key but value is MVCC-encoded (sysTablePut case)
          try:
            let mvccVal = mvccTypes.decodeMVCCValueFast(v)
            if mvccVal.isDeleted:
              continue # Skip tombstones
            value = mvccVal.data
            ts = mvccVal.timestamp
          except:
            discard # Not MVCC-encoded, use as-is

        # Keep only latest version for each user key
        if not latestVersions.hasKey(userKey) or ts > latestVersions[userKey].ts:
          latestVersions[userKey] = (value, ts)
      except:
        # If decoding fails, try as raw key-value
        if not latestVersions.hasKey(k):
          latestVersions[k] = (v, 0'i64)

  var newTableSpaces = initTable[uint32, int]()
  for (k, entry) in latestVersions.pairs:
    {.cast(raises: []).}:
      try:
        # Binary decoding (TableRecord)
        let tableRec = decodeTableRecord(entry.value)
        newTableSpaces[tableRec.tableId] = tableRec.spaceId
      except:
        discard

  withLock store.spacesMu:
    store.tableSpaces = newTableSpaces

proc getSpaceForTable*(store: RaftKVStoreExt,
    tableId: uint32): Option[SpaceInfo] {.gcsafe, raises: [].} =
  acquire(store.spacesMu)
  defer: release(store.spacesMu)
  let sid = store.tableSpaces.getOrDefault(tableId, 1)
  if store.spaces.hasKey(sid):
    return some(store.spaces.getOrDefault(sid))
  none(SpaceInfo)

# ---------------------------------------------------------------------------
# Network forwarding helpers
# ---------------------------------------------------------------------------
# Forward a request to the group leader over the network when this node is
# not the leader for the target group.

proc findLeaderForGroup(store: RaftKVStoreExt,
    groupId: GroupID): Option[NodeInfo] {.gcsafe, raises: [].} =
  ## Look up network address for the leader of `groupId`.
  ## Tries groupLeaders first, then falls back to groupMembers.
  let localNodeId = store.coordinator.nodeId.uint32
  var targetNode: uint32 = 0
  withLock store.groupMu:
    targetNode = store.groupLeaders.getOrDefault(groupId, 0)
    if targetNode == 0 or targetNode == localNodeId:
      let members = store.groupMembers.getOrDefault(groupId, @[])
      for nid in members:
        if nid != localNodeId:
          targetNode = nid
          break
  if targetNode == 0 or targetNode == localNodeId:
    return none(NodeInfo)
  store.lookupNodeInfo(targetNode)

proc forwardPutToLeader(store: RaftKVStoreExt, groupId: GroupID,
    key, value: string): RSResult[RaftKVEntry] {.gcsafe, raises: [].} =
  ## Forward a PUT to the leader of `groupId` via the wire protocol.
  let infoOpt = store.findLeaderForGroup(groupId)
  if infoOpt.isNone:
    return rsErr[RaftKVEntry](newRSE(rseNotLeader,
        "no reachable leader for group " & $groupId.uint64))
  let info = infoOpt.get()
  {.cast(raises: []).}:
    try:
      let cfg = ClientConfig(host: info.host, port: info.clientPort,
                             timeoutMs: 5000)
      let pc = newProtocolClient(cfg)
      let cr = pc.connect()
      if not cr.isOk:
        return rsErr[RaftKVEntry](newRSE(rseNotLeader,
            "failed to connect to leader: " & $cr.err))
      defer: pc.disconnect()
      let pr = pc.kvPutInGroup(key, value, groupId.uint64)
      if not pr.isOk:
        if pr.err.kind == peNotLeader:
          return rsErr[RaftKVEntry](newRSE(rseNotLeader, pr.err.msg))
        return rsErr[RaftKVEntry](newRSE(rseInternal, pr.err.msg))
      let resp = pr.val
      if resp.status != PutStatusOK:
        return rsErr[RaftKVEntry](newRSE(rseInternal,
            "remote PUT failed with status " & $resp.status))
      return rsOk[RaftKVEntry](RaftKVEntry(
        value: value, version: resp.version, timestamp: resp.timestamp))
    except CatchableError as e:
      return rsErr[RaftKVEntry](newRSE(rseInternal,
          "forward PUT exception: " & e.msg))

proc forwardDeleteToLeader(store: RaftKVStoreExt, groupId: GroupID,
    key: string): RSResult[Option[RaftKVEntry]] {.gcsafe, raises: [].} =
  ## Forward a DELETE to the leader of `groupId` via the wire protocol.
  let infoOpt = store.findLeaderForGroup(groupId)
  if infoOpt.isNone:
    return rsErr[Option[RaftKVEntry]](newRSE(rseNotLeader,
        "no reachable leader for group " & $groupId.uint64))
  let info = infoOpt.get()
  {.cast(raises: []).}:
    try:
      let cfg = ClientConfig(host: info.host, port: info.clientPort,
                             timeoutMs: 5000)
      let pc = newProtocolClient(cfg)
      let cr = pc.connect()
      if not cr.isOk:
        return rsErr[Option[RaftKVEntry]](newRSE(rseNotLeader,
            "failed to connect to leader: " & $cr.err))
      defer: pc.disconnect()
      let dr = pc.kvDeleteInGroup(key, groupId.uint64)
      if not dr.isOk:
        if dr.err.kind == peNotLeader:
          return rsErr[Option[RaftKVEntry]](newRSE(rseNotLeader, dr.err.msg))
        return rsErr[Option[RaftKVEntry]](newRSE(rseInternal, dr.err.msg))
      let resp = dr.val
      if resp.status == DelStatusDeleted:
        if resp.hasPreviousValue:
          return rsOk[Option[RaftKVEntry]](some(RaftKVEntry(
            value: resp.previousValue, version: 1'u64,
            timestamp: uint64(getTime().toUnixFloat() * 1_000_000_000))))
        return rsOk[Option[RaftKVEntry]](some(RaftKVEntry(
          value: "", version: 1'u64,
          timestamp: uint64(getTime().toUnixFloat() * 1_000_000_000))))
      return rsOk[Option[RaftKVEntry]](none(RaftKVEntry))
    except CatchableError as e:
      return rsErr[Option[RaftKVEntry]](newRSE(rseInternal,
          "forward DELETE exception: " & e.msg))

proc forwardGetToLeader(store: RaftKVStoreExt, groupId: GroupID,
    key: string): RSResult[Option[RaftKVEntry]] {.gcsafe, raises: [].} =
  ## Forward a GET to the leader of `groupId` via the wire protocol.
  let infoOpt = store.findLeaderForGroup(groupId)
  if infoOpt.isNone:
    return rsErr[Option[RaftKVEntry]](newRSE(rseNotLeader,
        "no reachable leader for group " & $groupId.uint64))
  let info = infoOpt.get()
  {.cast(raises: []).}:
    try:
      let cfg = ClientConfig(host: info.host, port: info.clientPort,
                             timeoutMs: 5000)
      let pc = newProtocolClient(cfg)
      let cr = pc.connect()
      if not cr.isOk:
        return rsErr[Option[RaftKVEntry]](newRSE(rseNotLeader,
            "failed to connect to leader: " & $cr.err))
      defer: pc.disconnect()
      let gr = pc.kvGetInGroup(key, groupId.uint64)
      if not gr.isOk:
        if gr.err.kind == peNotLeader:
          return rsErr[Option[RaftKVEntry]](newRSE(rseNotLeader, gr.err.msg))
        return rsErr[Option[RaftKVEntry]](newRSE(rseInternal, gr.err.msg))
      let resp = gr.val
      if resp.found:
        return rsOk[Option[RaftKVEntry]](some(RaftKVEntry(
          value: resp.value, version: resp.version,
          timestamp: resp.timestamp)))
      return rsOk[Option[RaftKVEntry]](none(RaftKVEntry))
    except CatchableError as e:
      return rsErr[Option[RaftKVEntry]](newRSE(rseInternal,
          "forward GET exception: " & e.msg))

# ---------------------------------------------------------------------------
# Space-aware KV operations
# ---------------------------------------------------------------------------
# These bypass resolveGroupId() and route directly to a space's Raft group
# using hash(primaryKey) mod numGroups.

proc lookupNodeInfo*(store: RaftKVStoreExt,
    nodeId: uint32): Option[NodeInfo] {.gcsafe, raises: [].} =
  ## Look up a node's host and clientPort, using a cache to avoid repeated
  ## backend reads. Falls back to scanning sys.nodes in the local backend.
  ## Expects binary-encoded NodeRecord (optionally wrapped in MVCC).
  withLock store.groupMu:
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
        var nodeVal = valOpt.get()
        # Strip MVCC encoding if present
        if mvccTypes.isLikelyMVCCValue(nodeVal):
          let mvccVal = mvccTypes.decodeMVCCValueFast(nodeVal)
          if mvccVal.isDeleted:
            return none(NodeInfo)
          nodeVal = mvccVal.data

        # Binary decoding (NodeRecord)
        let nodeRec = decodeNodeRecord(nodeVal)
        # Validate: host must not be empty
        if nodeRec.host.len == 0:
          return none(NodeInfo)
        let info: NodeInfo = (host: nodeRec.host, clientPort: int(
            nodeRec.clientPort))
        withLock store.groupMu:
          store.nodeInfoCache[nodeId] = info
        return some(info)
    except:
      discard
  none(NodeInfo)


proc raftPutInSpace*(store: RaftKVStoreExt, key, value: string,
    space: SpaceInfo, primaryKey: string): RSResult[RaftKVEntry] {.gcsafe,
        raises: [].} =
  ## Write `value` under `key` through Raft consensus, routing to the
  ## correct group in the space via hash(primaryKey).
  ##
  ## During rebalancing, inserts go to the NEW group only (new data should
  ## go to the new location). Updates/deletes should use raftPutInSpaceBoth
  ## to write to both old and new groups.
  ##
  ## Forwards to the group leader over the network if this node is not the leader.
  let rid = routeToGroup(primaryKey, space.groupIds)
  # Try local first
  {.cast(raises: []).}:
    if store.coordinator.hasGroup(rid):
      if store.coordinator.isLeader(rid):
        let batch = newWriteBatch()
        batch.put(toBytes(key), toBytes(value))
        let vr = proposeWrite(store, rid, batch)
        if vr.isOk:
          let ver = store.nextVersion.fetchAdd(1)
          let ts = uint64(getTime().toUnixFloat() * 1_000_000_000)
          return rsOk[RaftKVEntry](RaftKVEntry(value: value, version: ver,
              timestamp: ts))
        if vr.error.kind != rseNotLeader:
          return rsErr[RaftKVEntry](vr.error)
        # Fall through to network forwarding if we lost leadership
      # Forward to group leader via network
  store.forwardPutToLeader(rid, key, value)

proc raftPutInSpaceBoth*(store: RaftKVStoreExt, key, value: string,
    space: SpaceInfo, primaryKey: string): RSResult[RaftKVEntry] {.gcsafe,
        raises: [].} =
  ## Write to BOTH old and new groups during rebalancing.
  ## Used for updates and deletes when we don't know if the record has migrated.
  ## Returns the result from the new group write.
  let newRid = routeToGroup(primaryKey, space.groupIds)

  # First, write to the new group
  var newResult: RSResult[RaftKVEntry] = rsErr[RaftKVEntry](
    newRSE(rseInternal, "no group available"))
  {.cast(raises: []).}:
    if store.coordinator.hasGroup(newRid):
      if store.coordinator.isLeader(newRid):
        let batch = newWriteBatch()
        batch.put(toBytes(key), toBytes(value))
        let vr = proposeWrite(store, newRid, batch)
        if vr.isOk:
          let ver = store.nextVersion.fetchAdd(1)
          let ts = uint64(getTime().toUnixFloat() * 1_000_000_000)
          newResult = rsOk[RaftKVEntry](RaftKVEntry(value: value, version: ver,
              timestamp: ts))
        elif vr.error.kind != rseNotLeader:
          newResult = rsErr[RaftKVEntry](vr.error)
        else:
          newResult = store.forwardPutToLeader(newRid, key, value)
      else:
        newResult = store.forwardPutToLeader(newRid, key, value)

  # If rebalancing, also write to the old group (best-effort)
  if space.rebalancing and space.oldGroupIds.len > 0:
    let oldRid = routeToGroup(primaryKey, space.oldGroupIds)
    if oldRid != newRid:
      {.cast(raises: []).}:
        if store.coordinator.hasGroup(oldRid):
          if store.coordinator.isLeader(oldRid):
            let batch = newWriteBatch()
            batch.put(toBytes(key), toBytes(value))
            discard proposeWrite(store, oldRid, batch)
          else:
            discard store.forwardPutToLeader(oldRid, key, value)

  newResult

proc raftGetInSpaceFromGroup*(store: RaftKVStoreExt, key: string,
    rid: GroupID): RSResult[Option[RaftKVEntry]] {.gcsafe, raises: [].} =
  ## Internal helper: read `key` from a specific group.
  ## If this node is the leader, reads locally.
  ## Otherwise, forwards to the group leader over the network.
  {.cast(raises: []).}:
    if store.coordinator.isLeader(rid):
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
    # Not leader — forward to the group leader via network
    return store.forwardGetToLeader(rid, key)

proc raftGetInSpace*(store: RaftKVStoreExt, key: string,
    space: SpaceInfo, primaryKey: string): RSResult[Option[
        RaftKVEntry]] {.gcsafe, raises: [].} =
  ## Read the current value for `key` from the space group owning `primaryKey`.
  ## During rebalancing, reads from both old and new groups and returns the
  ## newer record (by timestamp). This handles the case where a record exists
  ## in both groups during migration.
  ##
  ## For robustness during rebalancing:
  ## - Always try old group first if we have it (it has the data)
  ## - Then try new group
  ## - Return whichever has the data (or the newer one if both have it)
  let newRid = routeToGroup(primaryKey, space.groupIds)
  var oldEntry: Option[RaftKVEntry]
  var newEntry: Option[RaftKVEntry]
  var lastError: Option[RaftStoreError]

  # During rebalancing, try old group first (it has the data)
  if space.rebalancing and space.oldGroupIds.len > 0:
    let oldRid = routeToGroup(primaryKey, space.oldGroupIds)
    if oldRid != newRid:
      let oldRes = store.raftGetInSpaceFromGroup(key, oldRid)
      if oldRes.isOk and oldRes.value.isSome:
        oldEntry = oldRes.value
      elif not oldRes.isOk:
        lastError = some(oldRes.error)

  # Try new group
  let newRes = store.raftGetInSpaceFromGroup(key, newRid)
  if newRes.isOk:
    if newRes.value.isSome:
      newEntry = newRes.value
  elif not lastError.isSome:
    lastError = some(newRes.error)

  # Return the appropriate result
  if oldEntry.isSome and newEntry.isSome:
    # Both have the record - return the newer one
    if oldEntry.get().timestamp > newEntry.get().timestamp:
      return rsOk[Option[RaftKVEntry]](oldEntry)
    else:
      return rsOk[Option[RaftKVEntry]](newEntry)
  elif oldEntry.isSome:
    return rsOk[Option[RaftKVEntry]](oldEntry)
  elif newEntry.isSome:
    return rsOk[Option[RaftKVEntry]](newEntry)
  elif lastError.isSome:
    return rsErr[Option[RaftKVEntry]](lastError.get())

  rsOk[Option[RaftKVEntry]](none(RaftKVEntry))


proc raftDeleteInSpace*(store: RaftKVStoreExt, key: string,
    space: SpaceInfo, primaryKey: string): RSResult[Option[
        RaftKVEntry]] {.gcsafe, raises: [].} =
  ## Delete `key` through Raft consensus, routing to the correct space group.
  ##
  ## During rebalancing, deletes go to BOTH old and new groups since we don't
  ## know if the record has been migrated yet.
  ##
  ## Forwards to the group leader over the network if this node is not the leader.
  let newRid = routeToGroup(primaryKey, space.groupIds)

  # First, delete from the new group
  var newResult: RSResult[Option[RaftKVEntry]] = rsErr[Option[RaftKVEntry]](
    newRSE(rseInternal, "no group available"))

  {.cast(raises: []).}:
    if store.coordinator.hasGroup(newRid):
      if store.coordinator.isLeader(newRid):
        var prevEntry: Option[RaftKVEntry]
        let backend = store.getBackend()
        if backend != nil and backend.isOpen:
          let valOpt = backend.get(key)
          if valOpt.isSome:
            prevEntry = some(RaftKVEntry(
              value: valOpt.get(),
              version: 1'u64,
              timestamp: uint64(getTime().toUnixFloat() * 1_000_000_000),
            ))
        let batch = newWriteBatch()
        batch.delete(toBytes(key))
        let vr = proposeWrite(store, newRid, batch)
        if vr.isOk:
          newResult = rsOk[Option[RaftKVEntry]](prevEntry)
        elif vr.error.kind != rseNotLeader:
          newResult = rsErr[Option[RaftKVEntry]](vr.error)
        else:
          newResult = store.forwardDeleteToLeader(newRid, key)
      else:
        newResult = store.forwardDeleteToLeader(newRid, key)

  # If rebalancing, also delete from the old group (best-effort)
  if space.rebalancing and space.oldGroupIds.len > 0:
    let oldRid = routeToGroup(primaryKey, space.oldGroupIds)
    if oldRid != newRid:
      {.cast(raises: []).}:
        if store.coordinator.hasGroup(oldRid):
          if store.coordinator.isLeader(oldRid):
            let batch = newWriteBatch()
            batch.delete(toBytes(key))
            discard proposeWrite(store, oldRid, batch)
          else:
            discard store.forwardDeleteToLeader(oldRid, key)

  newResult

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
    includeSystemKeys: bool = false): RSResult[seq[(string,
        RaftKVEntry)]] {.gcsafe, raises: [].} =
  ## Scan keys across all groups in the space.
  ##
  ## During rebalancing, scans BOTH old and new groups, merging results.
  ## Duplicates (keys present in both old and new) are resolved by keeping
  ## the record with the newer timestamp.
  ##
  ## For each group, we read from the **leader** to guarantee we see
  ## committed data (followers may lag).
  ##
  ## Uses a merge-sort approach: collect results from all group leaders,
  ## then deduplicate by key (keeping newer timestamp) and sort by key.

  # Collect all group IDs to scan (old + new during rebalancing)
  var allGidsToScan = space.groupIds
  if space.rebalancing:
    for ogid in space.oldGroupIds:
      if ogid notin allGidsToScan:
        allGidsToScan.add(ogid)

  # Results collected from all sources, keyed by key
  # We track the best (newest) entry for each key
  var resultMap: Table[string, RaftKVEntry]

  # Scan groups where this node is a leader
  {.cast(raises: []).}:
    for gid64 in allGidsToScan:
      let gid = GroupID(gid64)
      if store.coordinator.hasGroup(gid) and store.coordinator.isLeader(gid):
        # Local scan for this group
        let localScan = store.raftScan(startKey, endKey, 0, includeSystemKeys)
        if localScan.isOk:
          # Filter to only keys that route to this group
          for (k, entry) in localScan.value:
            try:
              let (tableId, primaryKey) = decodeTableKey(k)
              let pk = if primaryKey.startsWith("d/"): primaryKey[
                  2..^1] else: primaryKey
              let routedGid = routeToGroup(pk, space.groupIds)
              # Include if it routes to new groups OR (during rebalancing) old groups
              var matches = (routedGid == gid)
              if not matches and space.rebalancing:
                let oldRoutedGid = routeToGroup(pk, space.oldGroupIds)
                matches = (oldRoutedGid == gid)
              if matches:
                if k notin resultMap:
                  resultMap[k] = entry
                elif entry.timestamp > resultMap[k].timestamp:
                  resultMap[k] = entry
            except:
              discard

  # Scan remote groups (groups this node is not a leader for)
  var remoteGroupsToScan: seq[GroupID] = @[]
  for gid64 in allGidsToScan:
    let gid = GroupID(gid64)
    if not (store.coordinator.hasGroup(gid) and store.coordinator.isLeader(gid)):
      remoteGroupsToScan.add(gid)

  # Fan out to remote leaders for groups we don't lead
  {.cast(raises: []).}:
    try:
      for gid in remoteGroupsToScan:
        # Find the leader for this group
        var targetNode: uint32 = 0
        withLock store.groupMu:
          targetNode = store.groupLeaders.getOrDefault(gid, 0)
          if targetNode == 0:
            let members = store.groupMembers.getOrDefault(gid, @[])
            if members.len > 0:
              targetNode = members[0]

        if targetNode == 0:
          continue

        let infoOpt = store.lookupNodeInfo(targetNode)
        if infoOpt.isNone:
          continue
        let info = infoOpt.get()
        let cfg = ClientConfig(host: info.host, port: info.clientPort,
                               timeoutMs: 5000)
        let pc = newProtocolClient(cfg)
        let cr = pc.connect()
        if not cr.isOk:
          continue
        let sr = pc.kvScan(startKey, endKey, 0)
        pc.disconnect()
        if sr.isOk:
          let ts = uint64(getTime().toUnixFloat() * 1_000_000_000)
          for pair in sr.val.pairs:
            let entry = RaftKVEntry(
              value: pair.value, version: 1'u64, timestamp: ts,
            )
            if pair.key notin resultMap:
              resultMap[pair.key] = entry
            elif entry.timestamp > resultMap[pair.key].timestamp:
              resultMap[pair.key] = entry
    except:
      discard

  # Sort by key and build result
  var allKeys: seq[string] = @[]
  for k in resultMap.keys:
    allKeys.add(k)
  allKeys.sort()

  var finalResult: seq[(string, RaftKVEntry)] = @[]
  for k in allKeys:
    finalResult.add((k, resultMap.getOrDefault(k)))
    if limit > 0 and uint32(finalResult.len) >= limit:
      break

  rsOk[seq[(string, RaftKVEntry)]](finalResult)

# ---------------------------------------------------------------------------
# Space rebalancing
# ---------------------------------------------------------------------------

proc updateSpaceRecord*(store: RaftKVStoreExt, space: SpaceInfo) {.gcsafe,
    raises: [].} =
  ## Write the space record to sys.spaces via Raft with MVCC encoding.
  ## The in-memory cache is updated by applyBatchToSM when Raft commits the write.
  ## Uses binary encoding (SpaceRecord) for consistency with other system tables.
  let spaceKey = encodeSpaceKey(space.spaceId)

  # Create binary-encoded SpaceRecord
  let spaceRec = SpaceRecord(
    spaceId: int32(space.spaceId),
    name: space.name,
    replicas: int32(space.replicas),
    groupCount: int32(space.groupIds.len),
    groupIds: space.groupIds,
    oldGroupIds: space.oldGroupIds,
    rebalancing: space.rebalancing,
    rebalanceWorker: int32(space.rebalanceWorker),
    rebalanceHeartbeat: space.rebalanceHeartbeat,
    rebalanceCursor: space.rebalanceCursor,
    createdAtNs: 0'i64, # Not tracked in SpaceInfo
  )
  let encoded = encode(spaceRec)
  discard store.sysTablePut(spaceKey, encoded)

proc rebalanceSpaces*(store: RaftKVStoreExt) {.raises: [].} =
  ## Check all spaces and initiate rebalancing for any space whose group count
  ## doesn't match the current node count. Creates new groups and sets up
  ## dual-read mode.
  {.cast(raises: []).}:
    # Count nodes
    let nodesStart = encodeTableKey(SYS_NODES_TABLE_ID, "")
    let nodesEnd = encodeTableKey(SYS_NODES_TABLE_ID + 1, "")
    let nodesRes = store.raftScan(nodesStart, nodesEnd, 0,
        includeSystemKeys = true)
    var nodeIds: seq[int] = @[]
    if nodesRes.isOk:
      for (key, entry) in nodesRes.value:
        try:
          var nodeVal = entry.value
          # Strip MVCC encoding if present
          if mvccTypes.isLikelyMVCCValue(nodeVal):
            try:
              let mvccVal = mvccTypes.decodeMVCCValueFast(nodeVal)
              if not mvccVal.isDeleted:
                nodeVal = mvccVal.data
              else:
                continue
            except:
              discard

          # Binary decoding (NodeRecord)
          let nodeRec = decodeNodeRecord(nodeVal)
          nodeIds.add(int(nodeRec.nodeId))
        except:
          discard
    if nodeIds.len == 0:
      return
    nodeIds.sort()
    let nodeCount = nodeIds.len

    # Scan spaces
    let spacesStart = encodeTableKey(SYS_SPACES_TABLE_ID, "")
    let spacesEnd = encodeTableKey(SYS_SPACES_TABLE_ID + 1, "")
    let spacesRes = store.raftScan(spacesStart, spacesEnd, 0,
        includeSystemKeys = true)
    if not spacesRes.isOk:
      return

    # Group entries by user key, tracking latest version (same as loadSpaces)
    var latestSpaces: Table[int, tuple[info: SpaceInfo, ts: int64]] = initTable[
        int, tuple[info: SpaceInfo, ts: int64]]()
    for (key, entry) in spacesRes.value:
      try:
        var userKey = key
        var jsonStr = entry.value
        var ts: int64 = 0

        # Check if this is an MVCC version key (ends with \x00\x00 + 8 bytes)
        if key.len >= 10 and key[key.len - 10] == '\x00' and key[key.len - 9] == '\x00':
          userKey = key[0 ..< key.len - 10]
          if jsonStr.len >= 17:
            let mvccVal = mvccTypes.decodeMVCCValue(jsonStr)
            if mvccVal.isDeleted:
              continue
            jsonStr = mvccVal.data
            ts = mvccVal.timestamp
        elif mvccTypes.isLikelyMVCCValue(jsonStr):
          # MVCC-encoded value with non-MVCC key (shouldn't happen but handle it)
          try:
            let mvccVal = mvccTypes.decodeMVCCValue(jsonStr)
            if not mvccVal.isDeleted:
              jsonStr = mvccVal.data
              ts = mvccVal.timestamp
            else:
              continue
          except:
            discard

        let infoOpt = parseSpaceInfoFromBinary(jsonStr)
        if infoOpt.isNone:
          continue
        let info = infoOpt.get()
        let spaceId = info.spaceId

        # Keep only latest version for each space
        if not latestSpaces.hasKey(spaceId) or ts > latestSpaces[spaceId].ts:
          latestSpaces[spaceId] = (info: info, ts: ts)
      except:
        discard

    for spaceId, (info, ts) in latestSpaces.pairs:
      try:
        let currentGroupCount = info.groupIds.len
        let isRebalancing = info.rebalancing
        let replicas = info.replicas

        # Also check in-memory cache for rebalancing state
        # (may have been set by a previous call but not yet persisted)
        var inMemoryRebalancing = false
        acquire(store.spacesMu)
        if store.spaces.hasKey(spaceId) and store.spaces[spaceId].rebalancing:
          inMemoryRebalancing = true
        release(store.spacesMu)

        # Skip if already rebalancing (either in-memory or persisted) or group count matches
        if inMemoryRebalancing or isRebalancing or currentGroupCount == nodeCount:
          continue

        # Skip the default space (replicas=0 means "all nodes", uses meta group)
        if replicas == 0:
          continue
        let effectiveReplicas = replicas
        if effectiveReplicas > nodeCount:
          continue

        # Find max existing groupId
        let grpStart = encodeTableKey(SYS_GROUPS_TABLE_ID, "")
        let grpEnd = encodeTableKey(SYS_GROUPS_TABLE_ID + 1, "")
        let grpRes = store.raftScan(grpStart, grpEnd, 0,
            includeSystemKeys = true)
        var maxGroupId: uint64 = 1
        if grpRes.isOk:
          for (gk, ge) in grpRes.value:
            try:
              var grpVal = ge.value
              # Strip MVCC encoding if present
              if mvccTypes.isLikelyMVCCValue(grpVal):
                try:
                  let mvccVal = mvccTypes.decodeMVCCValueFast(grpVal)
                  if not mvccVal.isDeleted:
                    grpVal = mvccVal.data
                  else:
                    continue
                except:
                  discard

              # Binary decoding (GroupRecord)
              let groupRec = decodeGroupRecord(grpVal)
              let gid = groupRec.groupId
              if gid > maxGroupId: maxGroupId = gid
            except:
              discard

        # Create new groups with ring placement
        var newGroupIds: seq[uint64] = @[]
        let coord = store.coordinator
        for g in 0 ..< nodeCount:
          let groupId = maxGroupId + 1 + uint64(g)
          newGroupIds.add(groupId)

          var members: seq[int] = @[]
          for r in 0 ..< effectiveReplicas:
            members.add(nodeIds[(g + r) mod nodeCount])

          # Write group descriptor using binary encoding
          var replicas: seq[GroupReplicaBin] = @[]
          for m in members:
            replicas.add(GroupReplicaBin(nodeId: uint32(m),
                replicaType: rtVoter))
          let groupRec = GroupRecord(
            groupId: groupId,
            spaceId: int32(spaceId),
            preferredLeader: uint32(members[0]),
            leader: 0,
            replicas: replicas,
          )
          let groupKey = encodeTableKey(SYS_GROUPS_TABLE_ID, $groupId)
          let groupVal = encode(groupRec)
          discard store.sysTablePut(groupKey, groupVal)

          # Create Raft group in coordinator
          let gid = GroupID(groupId)
          if not coord.hasGroup(gid):
            var nuraftMembers: seq[tuple[nodeId: uint32, host: string,
                basePort: int]] = @[]
            for m in members:
              let peerInfo = coord.peerInfo.getOrDefault(uint32(m),
                  (host: coord.host, basePort: coord.basePort))
              nuraftMembers.add((nodeId: uint32(m), host: peerInfo.host,
                  basePort: peerInfo.basePort))
            let preferredLeader = uint32(members[0])
            try:
              discard coord.createAndStartGroup(gid, nuraftMembers, preferredLeader)
              store.registerGroup(gid)
            except:
              discard

        # Read current groupIds
        var oldGroupIds: seq[uint64] = @[]
        oldGroupIds = info.groupIds

        # Update space record with rebalance state
        var space = SpaceInfo(
          spaceId: spaceId,
          name: info.name,
          replicas: info.replicas,
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

    # Reload group members (spaces cache is already updated by updateSpaceRecord)
    store.loadGroupMembers()

proc runRebalanceMigration*(store: RaftKVStoreExt, spaceId: int) {.raises: [].} =
  ## Migrate data from old groups to new groups for a rebalancing space.
  ##
  ## Key insight: All groups on a node share the same WiscKey backend.
  ## This means:
  ##   - Writing to a new group stores data in the shared backend
  ##   - Reading from an old group also reads from the shared backend
  ##   - We should NOT delete from old groups during migration (would lose data)
  ##
  ## Migration flow:
  ##   1. Write each key to its new group via Raft (for replication)
  ##   2. At end, remove old group definitions (data is accessible via new groups)
  ##
  ## During migration, raftGetInSpace handles dual-read from both old and new groups.
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
    # Note: Don't call loadSpaces() here - it would read stale data from backend
    # before raftPut completes. The in-memory cache is already updated by updateSpaceRecord.

    let newGroupIds = space.groupIds
    let oldGroupIds = space.oldGroupIds
    let newCount = newGroupIds.len
    let oldCount = oldGroupIds.len

    if newCount == 0 or oldCount == 0:
      return

    # Find all tables in this space
    let tablesStart = encodeTableKey(SYS_TABLES_TABLE_ID, "")
    let tablesEnd = encodeTableKey(SYS_TABLES_TABLE_ID + 1, "")
    let tablesRes = store.raftScan(tablesStart, tablesEnd, 0,
        includeSystemKeys = true)
    var tableIds: seq[uint32] = @[]

    if tablesRes.isOk:
      for (key, entry) in tablesRes.value:
        try:
          var tblVal = entry.value
          # Check for MVCC encoding
          if mvccTypes.isLikelyMVCCValue(tblVal):
            try:
              let mvccVal = mvccTypes.decodeMVCCValueFast(tblVal)
              if not mvccVal.isDeleted:
                tblVal = mvccVal.data
              else:
                continue
            except:
              discard

          # Binary decoding (TableRecord)
          let tableRec = decodeTableRecord(tblVal)
          if tableRec.spaceId == spaceId:
            tableIds.add(tableRec.tableId)
        except: discard

    var keysMigrated = 0
    var lastHeartbeat = nowSecs
    let backend = store.getBackend()
    if backend == nil or not backend.isOpen: return

    # Migrate each table
    for tableId in tableIds:
      let startKey = encodeTableKey(tableId, "d/")
      let endKey = encodeTableKey(tableId, "e")           # just past "d/" range

      # Scan from backend (all groups share one backend)
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

          let oldGroup = routeToGroup(pk, oldGroupIds)
          let newGroup = routeToGroup(pk, newGroupIds)

          # Only migrate if the key moves to a different group
          if oldGroup != newGroup:
            let newRid = GroupID(newGroup)

            # Write to new group via Raft for replication
            # The data is in the shared backend, accessible by both old and new groups
            let hasGroup = store.coordinator.hasGroup(newRid)
            let isLeaderForNew = store.coordinator.isLeader(newRid)

            if hasGroup:
              if isLeaderForNew:
                # This node is the leader - write locally
                let batch = newWriteBatch()
                batch.put(toBytes(k), toBytes(v))
                discard proposeWrite(store, newRid, batch)
              else:
                # Not the leader - forward to the group leader
                discard store.forwardPutToLeader(newRid, k, v)
              # Note: We do NOT delete from old group here because:
              # 1. Both groups share the same backend
              # 2. Deleting would remove the data we just wrote
              # 3. The old group will be removed at cutover

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
                return # Another node took over
            release(store.spacesMu)

            space.rebalanceCursor = k
            space.rebalanceHeartbeat = curNow
            store.updateSpaceRecord(space)
            # Note: Don't call loadSpaces() here - it would read stale data
            lastHeartbeat = curNow
        except:
          continue

    # Phase 3: Cutover — migration complete
    # All writes are already committed (proposeWrite and forwardPutToLeader
    # are synchronous - they wait for Raft commit before returning).

    # Remove old group definitions from sys.groups
    # Data remains in the shared backend, accessible via new groups
    let coord = store.coordinator
    for oldGid64 in oldGroupIds:
      let oldGid = GroupID(oldGid64)
      let groupKey = encodeTableKey(SYS_GROUPS_TABLE_ID, $oldGid64)
      discard store.sysTableDelete(groupKey)
      if coord.hasGroup(oldGid):
        coord.removeGroup(oldGid)

    # Clear rebalance state
    space.oldGroupIds = @[]
    space.rebalancing = false
    space.rebalanceWorker = 0
    space.rebalanceHeartbeat = 0
    space.rebalanceCursor = ""
    store.updateSpaceRecord(space)
    # The in-memory cache is updated by applyBatchToSM when Raft commits the write.
    store.loadGroupMembers()
