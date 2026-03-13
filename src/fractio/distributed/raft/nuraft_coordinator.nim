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
import std/os
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
  NuRaftGroupInstance* = object
    groupId*: GroupID
    launcher*: NuRaftLauncher
    server*: NuRaftServer
    sm*: NuRaftSM
    smgr*: NuRaftSMgr
    port*: int
    ## Back-reference to coordinator (raw pointer to break cycles)
    coordPtr*: pointer
    ## Set to true during shutdown to prevent callbacks from accessing freed memory
    stopped*: bool

  NuRaftGroupInstancePtr* = ptr NuRaftGroupInstance

  NuRaftCoordinator* = ref object
    nodeId*: NodeID
    basePort*: int
    host*: string
    dataDir*: string
    groups*: Table[GroupID, NuRaftGroupInstancePtr]
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

# Use C malloc/free to avoid atomicArc cross-thread dealloc crashes.
# NuRaftGroupInstance may be allocated on NuRaft ASIO threads (via
# onGroupMetadataApplied callback), and Nim's allocator crashes in
# addToSharedFreeList when freeing cross-thread allocations.
proc c_malloc(size: csize_t): pointer {.importc: "malloc", header: "<stdlib.h>".}
proc c_free(p: pointer) {.importc: "free", header: "<stdlib.h>".}

proc allocInstance(): NuRaftGroupInstancePtr =
  result = cast[NuRaftGroupInstancePtr](c_malloc(csize_t(sizeof(NuRaftGroupInstance))))
  zeroMem(result, sizeof(NuRaftGroupInstance))

proc freeInstance(p: NuRaftGroupInstancePtr) =
  if p != nil:
    c_free(p)

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
    let inst = cast[NuRaftGroupInstancePtr](ctx)
    if inst.stopped: return
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
    let inst = cast[NuRaftGroupInstancePtr](ctx)
    if inst.stopped: return
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
  if result.electionTimeoutLowerMs == 0: result.electionTimeoutLowerMs = 1000
  if result.electionTimeoutUpperMs == 0: result.electionTimeoutUpperMs = 2000
  if result.heartbeatIntervalMs == 0: result.heartbeatIntervalMs = 500
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

proc shutdownGroupInstance(inst: NuRaftGroupInstancePtr) {.thread.} =
  discard nuraftLauncherShutdown(inst.launcher, 3)

proc stop*(c: NuRaftCoordinator) =
  ## Stop all NuRaft instances and close the storage backend.
  if not c.running.load: return
  c.running.store(false)

  # Mark all instances as stopped to prevent callbacks from accessing
  # freed coordinator memory.
  withLock c.groupsLock:
    for gid, inst in c.groups:
      inst.stopped = true
  sleep(100) # Give C++ threads a moment to observe the stopped flag

  # Shutdown all launchers in parallel to avoid sequential blocking.
  # Each group shutdown can block for up to 3 seconds if the ASIO service
  # has active workers. Running them in parallel bounds total time.
  var threads: seq[Thread[NuRaftGroupInstancePtr]]
  withLock c.groupsLock:
    threads.setLen(c.groups.len)
    var i = 0
    for gid, inst in c.groups:
      createThread(threads[i], shutdownGroupInstance, inst)
      inc i

  for t in threads.mitems:
    joinThread(t)

  # Destroy C++ resources and free the raw ptrs
  withLock c.groupsLock:
    for gid, inst in c.groups:
      nuraftLauncherDestroy(inst.launcher)
      nuraftSmDestroy(inst.sm)
      nuraftSmgrDestroy(inst.smgr)
      freeInstance(inst)
    c.groups.clear()

  if c.store != nil:
    c.store.close()

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

  # Create the group instance using raw alloc (not ref) to avoid
  # atomicArc cross-thread deallocation issues.
  let inst = allocInstance()
  inst.groupId = groupId
  inst.port = myPort
  inst.coordPtr = cast[pointer](c)

  # Create state machine with commit callback
  inst.sm = nuraftSmCreate(nuraftCommitCb, cast[pointer](inst))
  if inst.sm.isNil:
    error("Failed to create NuRaft SM", "groupId", $groupId)
    freeInstance(inst)
    return false

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
  if inst.smgr.isNil:
    error("Failed to create NuRaft SMgr", "groupId", $groupId)
    nuraftSmDestroy(inst.sm)
    freeInstance(inst)
    return false

  # Create raft params
  let params = nuraftParamsCreate()
  nuraftParamsSetElectionTimeout(params, c.electionTimeoutLowerMs,
      c.electionTimeoutUpperMs)
  nuraftParamsSetHeartbeatInterval(params, c.heartbeatIntervalMs)
  nuraftParamsSetReturnMethod(params, 0) # blocking
  nuraftParamsSetSnapshotDistance(params, 0) # disabled
  nuraftParamsSetClientReqTimeout(params, 5000)
  nuraftParamsSetMaxAppendSize(params, 100)
  # Enable NuRaft's internal automatic leadership rebalancing to prefer the highest priority node
  nuraftParamsSetLeadershipTransferMinWaitTime(params, 1000)

  # Create and init launcher. Retry a few times in case of transient port
  # bind failures (common in tests with high churn).
  inst.launcher = nuraftLauncherCreate()
  var ok = false
  for attempt in 1 .. 5:
    ok = nuraftLauncherInit(inst.launcher, inst.sm, inst.smgr,
        int32(myPort), params, nuraftEventCb, cast[pointer](inst))
    if ok: break
    if attempt < 5:
      warn("NuRaft launcher init failed, retrying...", "groupId", $groupId,
           "port", $myPort, "attempt", $attempt)
      sleep(200 * attempt) # Linear backoff

  nuraftParamsDestroy(params)

  if not ok:
    error("Failed to initialize NuRaft launcher", "groupId", $groupId, "port", $myPort)
    nuraftLauncherDestroy(inst.launcher)
    nuraftSmDestroy(inst.sm)
    nuraftSmgrDestroy(inst.smgr)
    freeInstance(inst)
    return false

  # Wait for initialization so that launcher.get_server() is valid.
  let waitMs = if members.len == 1: 5000'i32 else: 500'i32
  discard nuraftLauncherWaitInit(inst.launcher, waitMs)

  inst.server = nuraftLauncherGetServer(inst.launcher)
  if inst.server == nil:
    error("NuRaft launcher initialized but server is nil", "groupId", $groupId, "port", $myPort)
    discard nuraftLauncherShutdown(inst.launcher, 3)
    nuraftLauncherDestroy(inst.launcher)
    nuraftSmDestroy(inst.sm)
    nuraftSmgrDestroy(inst.smgr)
    freeInstance(inst)
    return false

  # Set priority for preferred leader
  if preferredLeader > 0:
    for m in members:
      if m.nodeId == preferredLeader:
        discard nuraftServerSetPriority(inst.server, int32(m.nodeId), 100)
      else:
        discard nuraftServerSetPriority(inst.server, int32(m.nodeId), 50)

  withLock c.groupsLock:
    c.groups[groupId] = inst

  info("Started NuRaft group",
       "groupId", $groupId, "port", $myPort, "members", $members.len)

  return true

proc removeGroup*(c: NuRaftCoordinator, groupId: GroupID) =
  ## Stop and remove a NuRaft group instance.
  var inst: NuRaftGroupInstancePtr
  withLock c.groupsLock:
    if not c.groups.hasKey(groupId): return
    inst = c.groups[groupId]
    c.groups.del(groupId)

  inst.stopped = true
  discard nuraftLauncherShutdown(inst.launcher, 2)
  sleep(50)
  nuraftLauncherDestroy(inst.launcher)
  nuraftSmDestroy(inst.sm)
  nuraftSmgrDestroy(inst.smgr)
  freeInstance(inst)

proc hasGroup*(c: NuRaftCoordinator, groupId: GroupID): bool =
  withLock c.groupsLock:
    result = c.groups.hasKey(groupId)

proc getGroupInstance*(c: NuRaftCoordinator,
    groupId: GroupID): Option[NuRaftGroupInstancePtr] =
  withLock c.groupsLock:
    let inst = c.groups.getOrDefault(groupId, nil)
    if inst != nil:
      result = some(inst)

proc isLeader*(c: NuRaftCoordinator, groupId: GroupID): bool {.raises: [].} =
  withLock c.groupsLock:
    let inst = c.groups.getOrDefault(groupId, nil)
    if inst != nil and inst.server != nil:
      result = nuraftServerIsLeader(inst.server)
      if result:
        {.cast(gcsafe).}: {.cast(raises: []).}:
          debug("isLeader: true", {"groupId": $groupId, "nodeId": $c.nodeId.uint32}.toTable)

proc getLeader*(c: NuRaftCoordinator, groupId: GroupID): int32 =
  ## Returns the leader's server ID, or -1 if unknown.
  withLock c.groupsLock:
    let inst = c.groups.getOrDefault(groupId, nil)
    if inst != nil and inst.server != nil:
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

  var inst: NuRaftGroupInstancePtr
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

proc setPriority*(c: NuRaftCoordinator, groupId: GroupID,
    targetNodeId: NodeID, priority: int32): bool =
  ## Set priority for a server in the group.
  var inst: NuRaftGroupInstancePtr
  withLock c.groupsLock:
    inst = c.groups.getOrDefault(groupId, nil)

  if inst == nil or inst.server == nil: return false

  let rc = nuraftServerSetPriority(inst.server, int32(targetNodeId.uint32), priority)
  if rc != 0:
    warn("Failed to set priority", "groupId", $groupId, "target", $targetNodeId.uint32, "rc", $rc)
    return false
  return true

proc transferLeadership*(c: NuRaftCoordinator, groupId: GroupID,
    targetNodeId: NodeID): bool =
  ## Transfer leadership to the target node by yielding.
  var inst: NuRaftGroupInstancePtr
  withLock c.groupsLock:
    inst = c.groups.getOrDefault(groupId, nil)

  if inst == nil or inst.server == nil: return false

  # Yield leadership gracefully to the target
  nuraftServerYieldLeadership(inst.server, false, int32(targetNodeId.uint32))
  return true
proc addServerToGroup*(c: NuRaftCoordinator, groupId: GroupID,
    nodeId: uint32, host: string, basePort: int): int32 =
  ## Add a new server to an existing Raft group (membership change).
  ## Only the leader can do this.
  var inst: NuRaftGroupInstancePtr
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
  var inst: NuRaftGroupInstancePtr
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
