# NuRaft-based Multi-Group Coordinator
#
# Manages multiple NuRaft instances (one per Raft group), each listening
# on its own ASIO port. Replaces the hand-rolled multigroup_coordinator.nim.
#
# Port scheme: each group uses basePort + groupId.
# Example: node with basePort=7000, group 6 → port 7006.

import std/atomics
import std/json
import std/locks
import std/options
import std/strutils
import std/tables
import std/logging

import fractio/distributed/raft/c_bindings
import fractio/distributed/raft/group_types
import fractio/distributed/raft/multigroup_types
import fractio/storage/backend
import fractio/storage/wisckey_backend

# ============================================================================
# Types
# ============================================================================

type
  NuRaftGroupInstance* = ref object
    groupId*: GroupID
    launcher*: NuRaftLauncher
    server*: NuRaftServer
    sm*: NuRaftSM
    smgr*: NuRaftSMgr
    port*: int
    ## Back-reference to coordinator (raw pointer to break cycles)
    coordPtr*: pointer

  NuRaftCoordinator* = ref object
    nodeId*: NodeID
    basePort*: int
    host*: string
    dataDir*: string
    groups*: Table[GroupID, NuRaftGroupInstance]
    groupsLock*: Lock
    running*: Atomic[bool]

    ## WiscKey storage backend (shared across all groups)
    store*: WiscKeyBackend

    ## Back-reference to RaftKVStoreExt (raw pointer to break circular imports)
    kvStorePtr*: pointer

    ## Timing parameters (milliseconds)
    electionTimeoutLowerMs*: int32
    electionTimeoutUpperMs*: int32
    heartbeatIntervalMs*: int32

    ## Peer info cache: nodeId → (host, basePort)
    ## Updated when nodes join/leave.
    peerInfo*: Table[uint32, tuple[host: string, basePort: int]]

# ============================================================================
# Module-level callbacks (same pattern as old coordinator)
# ============================================================================

## Called when a committed WriteBatch should be applied to the KV state machine.
var applyBatchCallback*: proc(storePtr: pointer, rid: GroupID,
    batch: WriteBatch) {.gcsafe, raises: [].} = nil

## Called when a sys.groups key is applied via Raft.
var onGroupMetadataApplied*: proc(storePtr: pointer,
    groupKey: string, groupValue: string) {.gcsafe, raises: [].} = nil

## Called when a node wins an election (becomes leader).
var onLeaderChanged*: proc(storePtr: pointer, groupId: GroupID,
    leaderNodeId: NodeID) {.gcsafe, raises: [].} = nil

## Called to look up preferred leaders.
var getPreferredLeaderCallback*: proc(storePtr: pointer,
    groupId: GroupID): Option[NodeID] {.gcsafe, raises: [].} = nil

## Called when space metadata changes replicate.
var onSpaceMetadataChanged*: proc(storePtr: pointer) {.gcsafe, raises: [].} = nil

# ============================================================================
# WriteBatch Serialization (JSON — same format as multigroup_log.nim)
# ============================================================================

proc serializeWriteBatch*(batch: WriteBatch): string =
  ## Serialize a WriteBatch to JSON string for NuRaft log entries.
  let j = %*{
    "commandKind": ord(ckWrite),
    "puts": newJArray(),
    "deletes": newJArray()
  }
  for (k, v) in batch.puts:
    j["puts"].add(%*{"key": %k, "value": %v})
  for k in batch.deletes:
    j["deletes"].add(%*{"key": %k})
  result = $j

proc deserializeWriteBatch*(data: string): WriteBatch =
  ## Deserialize a WriteBatch from JSON string.
  let j = parseJson(data)
  result = newWriteBatch()
  if j.hasKey("puts"):
    for p in j["puts"]:
      var key: seq[byte]
      for b in p["key"]:
        key.add(byte(b.getInt()))
      var value: seq[byte]
      for b in p["value"]:
        value.add(byte(b.getInt()))
      result.put(key, value)
  if j.hasKey("deletes"):
    for d in j["deletes"]:
      var key: seq[byte]
      for b in d["key"]:
        key.add(byte(b.getInt()))
      result.delete(key)

# ============================================================================
# NuRaft Commit Callback (C → Nim bridge)
# ============================================================================

proc nuraftCommitCb(ctx: pointer, logIdx: uint64,
    data: cstring, len: csize_t) {.cdecl, gcsafe.} =
  ## Called from NuRaft C++ when a log entry is committed.
  ## ctx is a raw pointer to NuRaftGroupInstance.
  if ctx == nil or data == nil or len == 0: return

  {.cast(gcsafe).}:
    let inst = cast[NuRaftGroupInstance](ctx)
    let coord = cast[NuRaftCoordinator](inst.coordPtr)
    if coord == nil or coord.kvStorePtr == nil: return

    let payload = newString(len.int)
    copyMem(addr payload[0], data, len.int)

    try:
      let j = parseJson(payload)
      let cmdKind = CommandKind(j["commandKind"].getInt())

      if cmdKind == ckWrite:
        let batch = deserializeWriteBatch(payload)
        if applyBatchCallback != nil:
          applyBatchCallback(coord.kvStorePtr, inst.groupId, batch)
    except CatchableError:
      discard

# ============================================================================
# NuRaft Event Callback (leader/follower changes)
# ============================================================================

proc nuraftEventCb(ctx: pointer, eventType: int32,
    leaderId: int32, term: uint64) {.cdecl, gcsafe.} =
  ## Called from NuRaft C++ on BecomeLeader/BecomeFollower events.
  if ctx == nil: return

  {.cast(gcsafe).}:
    let inst = cast[NuRaftGroupInstance](ctx)
    let coord = cast[NuRaftCoordinator](inst.coordPtr)
    if coord == nil: return

    if eventType == NuRaftBecomeLeader:
      if onLeaderChanged != nil and coord.kvStorePtr != nil:
        onLeaderChanged(coord.kvStorePtr, inst.groupId, coord.nodeId)

# ============================================================================
# Coordinator Lifecycle
# ============================================================================

type
  CoordinatorConfig* = object
    nodeId*: NodeID
    basePort*: int
    host*: string
    dataDir*: string
    electionTimeoutLowerMs*: int32
    electionTimeoutUpperMs*: int32
    heartbeatIntervalMs*: int32
    ## WiscKey storage settings
    writeBufferSize*: int
    blockCacheSize*: int
    vlogMaxSize*: int64
    vlogCleanThreshold*: int64
    vlogMinCleanThreshold*: int64
    vlogCleanBufferSize*: int64

proc newNuRaftCoordinator*(config: CoordinatorConfig): NuRaftCoordinator =
  new(result)
  result.nodeId = config.nodeId
  result.basePort = config.basePort
  result.host = config.host
  result.dataDir = config.dataDir
  result.electionTimeoutLowerMs = config.electionTimeoutLowerMs
  result.electionTimeoutUpperMs = config.electionTimeoutUpperMs
  result.heartbeatIntervalMs = config.heartbeatIntervalMs
  result.kvStorePtr = nil
  result.running.store(false)
  initLock(result.groupsLock)

  # Open WiscKey backend
  let wbs = if config.writeBufferSize > 0: config.writeBufferSize
            else: 4 * 1024 * 1024
  let storeCfg = StorageConfig(
    path: config.dataDir, createIfMissing: true, syncWrites: true,
    writeBufferSize: wbs, blockCacheSize: config.blockCacheSize,
    vlogMaxSize: config.vlogMaxSize,
    vlogCleanThreshold: config.vlogCleanThreshold,
    vlogMinCleanThreshold: config.vlogMinCleanThreshold,
    vlogCleanBufferSize: config.vlogCleanBufferSize)
  result.store = newWiscKeyBackend(storeCfg)
  if not result.store.open(storeCfg):
    raise newException(CatchableError, "Failed to open storage backend")

proc start*(c: NuRaftCoordinator) =
  ## Start the coordinator. Groups are started individually via createAndStartGroup.
  c.running.store(true)

proc stop*(c: NuRaftCoordinator) =
  ## Stop all NuRaft instances.
  if not c.running.load: return
  c.running.store(false)

  withLock c.groupsLock:
    for gid, inst in c.groups:
      discard nuraftLauncherShutdown(inst.launcher, 5)
      nuraftLauncherDestroy(inst.launcher)
    c.groups.clear()

# ============================================================================
# Group Management
# ============================================================================

proc computePort*(basePort: int, groupId: GroupID): int {.inline.} =
  ## Compute the ASIO port for a group on a given node.
  basePort + groupId.int

proc createAndStartGroup*(c: NuRaftCoordinator, groupId: GroupID,
    members: seq[tuple[nodeId: uint32, host: string, basePort: int]],
    preferredLeader: uint32 = 0): bool =
  ## Create and start a NuRaft instance for one Raft group.
  ## members: list of (nodeId, host, basePort) for all replicas.
  ## Returns true on success.

  withLock c.groupsLock:
    if c.groups.hasKey(groupId):
      return true # Already exists

  # Build server list for state manager
  var serverIds = newSeq[int32](members.len)
  var endpoints = newSeq[string](members.len)
  var myEndpoint = ""
  var myPort = 0

  for i, m in members:
    serverIds[i] = int32(m.nodeId)
    let port = computePort(m.basePort, groupId)
    endpoints[i] = m.host & ":" & $port
    if m.nodeId == c.nodeId.uint32:
      myEndpoint = endpoints[i]
      myPort = port

  if myEndpoint == "":
    return false # This node is not a member

  # Create the group instance
  let inst = NuRaftGroupInstance(
    groupId: groupId,
    port: myPort,
  )
  inst.coordPtr = cast[pointer](c)

  # Create state machine with commit callback
  inst.sm = nuraftSmCreate(nuraftCommitCb, cast[pointer](inst))

  # Create state manager
  var cServerIds = newSeq[int32](members.len)
  var cEndpoints = newSeq[cstring](members.len)
  # Keep string refs alive
  for i in 0 ..< members.len:
    cServerIds[i] = serverIds[i]
    cEndpoints[i] = cstring(endpoints[i])

  inst.smgr = nuraftSmgrCreate(
    int32(c.nodeId.uint32),
    cstring(myEndpoint),
    int32(members.len),
    addr cServerIds[0],
    addr cEndpoints[0])

  # Create raft params
  let params = nuraftParamsCreate()
  nuraftParamsSetElectionTimeout(params, c.electionTimeoutLowerMs,
      c.electionTimeoutUpperMs)
  nuraftParamsSetHeartbeatInterval(params, c.heartbeatIntervalMs)
  nuraftParamsSetReturnMethod(params, 0) # blocking
  nuraftParamsSetSnapshotDistance(params, 0) # disabled
  nuraftParamsSetClientReqTimeout(params, 5000)
  nuraftParamsSetMaxAppendSize(params, 100)

  # Create and init launcher
  inst.launcher = nuraftLauncherCreate()
  let ok = nuraftLauncherInit(inst.launcher, inst.sm, inst.smgr,
      int32(myPort), params, nuraftEventCb, cast[pointer](inst))

  nuraftParamsDestroy(params)

  if not ok:
    nuraftLauncherDestroy(inst.launcher)
    nuraftSmDestroy(inst.sm)
    nuraftSmgrDestroy(inst.smgr)
    return false

  # Wait for initialization
  let initialized = nuraftLauncherWaitInit(inst.launcher, 5000)
  if not initialized:
    discard nuraftLauncherShutdown(inst.launcher, 3)
    nuraftLauncherDestroy(inst.launcher)
    nuraftSmDestroy(inst.sm)
    nuraftSmgrDestroy(inst.smgr)
    return false

  inst.server = nuraftLauncherGetServer(inst.launcher)

  # Set priority for preferred leader
  if preferredLeader > 0 and inst.server != nil:
    for m in members:
      if m.nodeId == preferredLeader:
        discard nuraftServerSetPriority(inst.server, int32(m.nodeId), 100)
      else:
        discard nuraftServerSetPriority(inst.server, int32(m.nodeId), 50)

  withLock c.groupsLock:
    c.groups[groupId] = inst

  {.cast(gcsafe).}:
    var fields = initTable[string, string]()
    fields["groupId"] = $groupId
    fields["port"] = $myPort
    fields["members"] = $members.len
    info("Started NuRaft group", fields)

  return true

proc removeGroup*(c: NuRaftCoordinator, groupId: GroupID) =
  ## Stop and remove a NuRaft group instance.
  var inst: NuRaftGroupInstance
  withLock c.groupsLock:
    if not c.groups.hasKey(groupId): return
    inst = c.groups[groupId]
    c.groups.del(groupId)

  discard nuraftLauncherShutdown(inst.launcher, 5)
  nuraftLauncherDestroy(inst.launcher)

proc hasGroup*(c: NuRaftCoordinator, groupId: GroupID): bool =
  withLock c.groupsLock:
    result = c.groups.hasKey(groupId)

proc getGroupInstance*(c: NuRaftCoordinator,
    groupId: GroupID): Option[NuRaftGroupInstance] =
  withLock c.groupsLock:
    if c.groups.hasKey(groupId):
      result = some(c.groups[groupId])

proc isLeader*(c: NuRaftCoordinator, groupId: GroupID): bool =
  withLock c.groupsLock:
    if c.groups.hasKey(groupId):
      let inst = c.groups[groupId]
      if inst.server != nil:
        result = nuraftServerIsLeader(inst.server)

proc getLeader*(c: NuRaftCoordinator, groupId: GroupID): int32 =
  ## Returns the leader's server ID, or -1 if unknown.
  withLock c.groupsLock:
    if c.groups.hasKey(groupId):
      let inst = c.groups[groupId]
      if inst.server != nil:
        return nuraftServerGetLeader(inst.server)
  return -1

proc getGroupCount*(c: NuRaftCoordinator): int =
  withLock c.groupsLock:
    result = c.groups.len

proc getLeaderCount*(c: NuRaftCoordinator): int =
  withLock c.groupsLock:
    for inst in c.groups.values:
      if inst.server != nil and nuraftServerIsLeader(inst.server):
        inc result

# ============================================================================
# Proposal (Write Path)
# ============================================================================

proc proposeAndWait*(c: NuRaftCoordinator, groupId: GroupID,
    command: RaftCommand, timeoutMs: int = 5000): RaftResult {.raises: [].} =
  ## Propose a write command and block until committed.
  ## Currently only ckWrite commands are supported.
  if command.kind != ckWrite:
    return RaftResult(success: false, error: "Only write commands supported")

  var inst: NuRaftGroupInstance
  withLock c.groupsLock:
    if not c.groups.hasKey(groupId):
      return RaftResult(success: false,
          error: "Group not found: " & $groupId.uint64)
    {.cast(raises: []).}:
      inst = c.groups[groupId]

  if inst.server == nil:
    return RaftResult(success: false, error: "Server not initialized")

  # Serialize the WriteBatch to JSON
  {.cast(raises: []).}:
    let payload = serializeWriteBatch(command.writeBatch)

    var logIdx: uint64 = 0
    let rc = nuraftServerAppendEntry(inst.server, cstring(payload),
        csize_t(payload.len), addr logIdx)

    if rc == 0:
      result = RaftResult(success: true, index: logIdx)
    else:
      result = RaftResult(success: false,
          error: "Raft append failed (code " & $rc & ")")

proc proposeParallel*(c: NuRaftCoordinator,
    proposals: seq[tuple[groupId: GroupID, command: RaftCommand]],
    timeoutMs: int = 5000): seq[RaftResult] {.raises: [].} =
  ## Submit N proposals to N different groups simultaneously.
  ## Uses threads for true parallelism since NuRaft append_entries is blocking.
  let n = proposals.len
  if n == 0: return @[]

  # For single proposal, just call directly
  if n == 1:
    return @[c.proposeAndWait(proposals[0].groupId, proposals[0].command,
        timeoutMs)]

  # For multiple, use threads
  type ThreadArg = object
    coord: pointer
    groupId: GroupID
    command: RaftCommand
    resultPtr: ptr RaftResult

  result = newSeq[RaftResult](n)
  var threads = newSeq[Thread[ThreadArg]](n)

  {.cast(raises: []).}:
    for i in 0 ..< n:
      let arg = ThreadArg(
        coord: cast[pointer](c),
        groupId: proposals[i].groupId,
        command: proposals[i].command,
        resultPtr: addr result[i],
      )
      createThread(threads[i], proc(a: ThreadArg) {.thread, gcsafe.} =
        let coord = cast[NuRaftCoordinator](a.coord)
        a.resultPtr[] = coord.proposeAndWait(a.groupId, a.command)
      , arg)

    for i in 0 ..< n:
      joinThread(threads[i])

# ============================================================================
# Leadership Transfer
# ============================================================================

proc transferLeadership*(c: NuRaftCoordinator, groupId: GroupID,
    targetNodeId: NodeID): bool =
  ## Transfer leadership to the target node.
  ## Sets the target's priority high and yields.
  var inst: NuRaftGroupInstance
  withLock c.groupsLock:
    if not c.groups.hasKey(groupId): return false
    inst = c.groups[groupId]

  if inst.server == nil: return false

  # Set target priority high
  discard nuraftServerSetPriority(inst.server, int32(targetNodeId.uint32), 200)
  # Yield leadership
  nuraftServerYieldLeadership(inst.server)
  return true

proc addServerToGroup*(c: NuRaftCoordinator, groupId: GroupID,
    nodeId: uint32, host: string, basePort: int): int32 =
  ## Add a new server to an existing Raft group (membership change).
  ## Only the leader can do this.
  var inst: NuRaftGroupInstance
  withLock c.groupsLock:
    if not c.groups.hasKey(groupId): return -1
    inst = c.groups[groupId]

  if inst.server == nil: return -1

  let port = computePort(basePort, groupId)
  let endpoint = host & ":" & $port
  return nuraftServerAddSrv(inst.server, int32(nodeId), cstring(endpoint))

proc removeServerFromGroup*(c: NuRaftCoordinator, groupId: GroupID,
    nodeId: uint32): int32 =
  ## Remove a server from an existing Raft group.
  var inst: NuRaftGroupInstance
  withLock c.groupsLock:
    if not c.groups.hasKey(groupId): return -1
    inst = c.groups[groupId]

  if inst.server == nil: return -1
  return nuraftServerRemoveSrv(inst.server, int32(nodeId))

# ============================================================================
# Convenience: register group (for raft_store compatibility)
# ============================================================================

proc registerGroup*(c: NuRaftCoordinator, groupId: GroupID) =
  ## No-op — group registration is handled by createAndStartGroup.
  discard
