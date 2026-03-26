# NuRaft-based Multi-Group Coordinator
#
# Manages multiple NuRaft instances sharing a single TCP port.
# All Raft groups are multiplexed over one port using GroupID-based routing.
#
# Architecture:
# - Each Raft group has its own raft_server with its own MultiplexedContext
# - All contexts share the same MultiplexedRaftTransport (single TCP port)
# - Outbound messages: send callback includes GroupID in frame
# - Inbound messages: transport demuxes by GroupID, delivers to correct group

import std/atomics
import std/locks
import std/net
import std/options
import std/os
import std/strutils
import std/tables as nimtables
import std/times
import std/typedthreads
import std/logging

import fractio/core/types as core_types
import fractio/distributed/raft/c_bindings
import fractio/distributed/raft/group_types
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/multiplexed_bindings
import fractio/distributed/raft/multiplexed_transport
import fractio/utils/binary
import fractio/storage/wisckey_backend
import fractio/storage/backend

# ============================================================================
# Types
# ============================================================================

type
  NuRaftGroupInstance* = object
    groupId*: GroupID
    server*: NuRaftServer
    sm*: NuRaftSM
    smgr*: NuRaftSMgr
    ## Per-group RPC context
    rpcContext*: MultiplexedContext
    ## Back-reference to coordinator (raw pointer to break cycles)
    coordPtr*: pointer
    ## Set to true during shutdown to prevent callbacks from accessing freed memory
    stopped*: bool

  NuRaftGroupInstancePtr* = ptr NuRaftGroupInstance

  GroupCreationRequest = object
    ## Request to create a Raft group asynchronously.
    groupId*: GroupID
    members*: seq[tuple[nodeId: uint32, host: string, port: int]]
    preferredLeader*: uint32
    storePtr*: pointer ## RaftKVStoreExt for registerGroup call

  NuRaftCoordinator* = ref object
    nodeId*: group_types.NodeID
    port*: int                           ## Single port for all groups
    host*: string
    dataDir*: string
    groups*: nimtables.Table[GroupID, NuRaftGroupInstancePtr]
    groupsLock*: Lock
    running*: Atomic[bool]

    ## WiscKey storage backend (shared across all groups)
    store*: WiscKeyBackend

    ## Back-reference to RaftKVStoreExt (raw pointer to break circular imports)
    kvStorePtr*: pointer

    ## Multiplexed transport (single TCP port for all groups)
    transport*: MultiplexedRaftTransport

    ## Timing parameters (milliseconds)
    electionTimeoutLowerMs*: int32
    electionTimeoutUpperMs*: int32
    heartbeatIntervalMs*: int32

    ## Peer info cache: nodeId → (host, port)
    ## Updated when nodes join/leave.
    peerInfo*: nimtables.Table[uint32, tuple[host: string, port: int]]

    ## Async group creation queue (to avoid blocking NuRaft threads)
    groupCreationQueue*: seq[GroupCreationRequest]
    groupCreationLock*: Lock
    groupCreationThread*: Thread[pointer]
    groupCreationRunning*: Atomic[bool]
    groupCreationPending*: Atomic[int32] ## Number of groups being created
    groupsCreating*: nimtables.Table[GroupID,
        bool] ## Groups currently being created (prevents duplicate queueing)
    groupsCreatingLock*: Lock

    ## Timer ID counter
    nextTimerId*: int32
    timerLock*: Lock

# Use C malloc/free to avoid atomicArc cross-thread dealloc crashes.
proc c_malloc(size: csize_t): pointer {.importc: "malloc",
    header: "<stdlib.h>".}
proc c_free(p: pointer) {.importc: "free", header: "<stdlib.h>".}

proc allocInstance(): NuRaftGroupInstancePtr =
  result = cast[NuRaftGroupInstancePtr](c_malloc(csize_t(sizeof(
      NuRaftGroupInstance))))
  zeroMem(result, sizeof(NuRaftGroupInstance))

proc freeInstance(p: NuRaftGroupInstancePtr) =
  if p != nil:
    c_free(p)

# ============================================================================
# Module-level callbacks (same pattern as before)
# ============================================================================

## Called when a committed WriteBatch should be applied to the KV state machine.
var applyBatchCallback*: proc(storePtr: pointer, rid: GroupID,
    data: cstring, len: int) {.gcsafe, raises: [].} = nil

## Called when a sys.groups key is applied via Raft.
var onGroupMetadataApplied*: proc(storePtr: pointer,
    groupKey: string, groupValue: string) {.gcsafe, raises: [].} = nil

## Called when a node wins an election (becomes leader).
var onLeaderChanged*: proc(storePtr: pointer, groupId: GroupID,
    leaderNodeId: group_types.NodeID) {.gcsafe, raises: [].} = nil

## Called to look up preferred leaders.
var getPreferredLeaderCallback*: proc(storePtr: pointer,
    groupId: GroupID): Option[group_types.NodeID] {.gcsafe, raises: [].} = nil

## Called when a group is successfully created asynchronously.
var onGroupCreatedCallback*: proc(storePtr: pointer, groupId: GroupID) {.gcsafe,
    raises: [].} = nil

proc clearModuleCallbacks*() {.gcsafe, raises: [].} =
  ## Clear all module-level callbacks.
  {.cast(gcsafe).}:
    applyBatchCallback = nil
    onGroupMetadataApplied = nil
    onLeaderChanged = nil
    getPreferredLeaderCallback = nil
    onGroupCreatedCallback = nil

# ============================================================================
# WriteBatch Serialization (Binary)
# ============================================================================

proc serializeWriteBatch*(batch: WriteBatch): string =
  ## Serialize a WriteBatch to binary format for NuRaft log entries.
  var w = initBinaryWriter()
  w.writeU8(uint8(ckWrite))
  w.writeU32(uint32(batch.puts.len))
  for (k, v) in batch.puts:
    w.writeU32(uint32(k.len))
    w.writeBytes(k)
    w.writeU32(uint32(v.len))
    w.writeBytes(v)
  w.writeU32(uint32(batch.deletes.len))
  for k in batch.deletes:
    w.writeU32(uint32(k.len))
    w.writeBytes(k)
  result = w.finish()

proc deserializeWriteBatch*(data: string): WriteBatch =
  ## Deserialize a WriteBatch from binary format.
  var r = initBinaryReader(data)
  let cmdKind = CommandKind(r.readU8())
  if cmdKind != ckWrite:
    return nil
  result = newWriteBatch()
  let putsCount = int(r.readU32())
  for _ in 0 ..< putsCount:
    let keyLen = int(r.readU32())
    let key = r.readBytes(keyLen)
    let valLen = int(r.readU32())
    let value = r.readBytes(valLen)
    result.put(key, value)
  let delCount = int(r.readU32())
  for _ in 0 ..< delCount:
    let keyLen = int(r.readU32())
    let key = r.readBytes(keyLen)
    result.delete(key)

# ============================================================================
# NuRaft Commit Callback (C → Nim bridge)
# ============================================================================

proc nuraftCommitCb(ctx: pointer, logIdx: uint64,
    data: cstring, len: csize_t) {.cdecl.} =
  echo "DEBUG nuraftCommitCb: ctx=", ctx.repr, " logIdx=", logIdx, " len=", len
  discard logIdx
  if ctx == nil or data == nil or len == 0:
    echo "DEBUG nuraftCommitCb: early return nil params"
    return

  let inst = cast[NuRaftGroupInstancePtr](ctx)
  if inst.stopped:
    echo "DEBUG nuraftCommitCb: inst stopped"
    return
  let coord = cast[NuRaftCoordinator](inst.coordPtr)
  if coord == nil or coord.kvStorePtr == nil:
    echo "DEBUG nuraftCommitCb: coord or kvStorePtr nil"
    return

  echo "DEBUG nuraftCommitCb: calling applyBatchCallback for groupId=", inst.groupId
  {.cast(gcsafe).}:
    if applyBatchCallback != nil:
      applyBatchCallback(coord.kvStorePtr, inst.groupId, data, len.int)
    else:
      echo "DEBUG nuraftCommitCb: applyBatchCallback is nil!"

# ============================================================================
# NuRaft Event Callback (leader/follower changes)
# ============================================================================

proc nuraftEventCb(ctx: pointer, eventType: int32,
    leaderId: int32, term: uint64) {.cdecl, gcsafe.} =
  discard leaderId
  discard term
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
# Send Callback (called from C++ when NuRaft wants to send a message)
# ============================================================================

proc multiplexedSendCb(ctx: pointer, groupIdBytes: cstring, srcNodeId: int32,
    dstNodeId: int32, msgData: cstring, msgLen: csize_t): int32 {.cdecl, gcsafe.} =
  if ctx == nil or msgData == nil or msgLen == 0:
    return -1

  let inst = cast[NuRaftGroupInstancePtr](ctx)
  if inst.stopped:
    return -1

  let coord = cast[NuRaftCoordinator](inst.coordPtr)
  if coord == nil or coord.transport == nil:
    return -1

  echo "DEBUG multiplexedSendCb: src=", srcNodeId, " dst=", dstNodeId, " len=", msgLen

  # Use the instance's groupId (C++ passes a placeholder of zeros)
  let groupId = inst.groupId
  let ulid = groupIDToULID(groupId)

  # Look up peer info
  var peerHost = ""
  var peerPort = 0

  withLock coord.groupsLock:
    if coord.peerInfo.hasKey(uint32(dstNodeId)):
      (peerHost, peerPort) = coord.peerInfo[uint32(dstNodeId)]

  echo "DEBUG multiplexedSendCb: dstNodeId=", dstNodeId, " peerHost=", peerHost,
      " peerPort=", peerPort

  if peerHost == "":
    echo "DEBUG multiplexedSendCb: no peer info for dstNodeId=", dstNodeId
    return -1

  # Build frame: magic(4) + groupId(16) + length(4) + payload
  const RaftMagic = 0x52414654'u32
  var frame = newString(4 + 16 + 4 + int(msgLen))
  var pos = 0

  # Magic
  frame[pos] = char((RaftMagic shr 24) and 0xFF)
  frame[pos + 1] = char((RaftMagic shr 16) and 0xFF)
  frame[pos + 2] = char((RaftMagic shr 8) and 0xFF)
  frame[pos + 3] = char(RaftMagic and 0xFF)
  pos += 4

  # GroupID (16 bytes)
  for i in 0..<16:
    frame[pos + i] = char(ulid.data[i])
  pos += 16

  # Length prefix (big-endian)
  let payloadLen = uint32(msgLen)
  frame[pos] = char((payloadLen shr 24) and 0xFF)
  frame[pos + 1] = char((payloadLen shr 16) and 0xFF)
  frame[pos + 2] = char((payloadLen shr 8) and 0xFF)
  frame[pos + 3] = char(payloadLen and 0xFF)
  pos += 4

  # Payload
  if msgLen > 0:
    copyMem(addr frame[pos], msgData, msgLen)

  let corePeerId = core_types.NodeID("n" & $dstNodeId)

  # Use synchronous send (for callback context) - pass host/port to create connection if needed
  if coord.transport.sendSync(corePeerId, frame, peerHost, peerPort):
    return 0
  return -1

# Timer callbacks (simplified - handled internally by C++ timer thread)
proc multiplexedScheduleTimerCb(ctx: pointer, groupIdHash: int32,
    timerType: int32, delayMs: int32) {.cdecl, gcsafe.} =
  discard

proc multiplexedCancelTimerCb(ctx: pointer, groupIdHash: int32,
    timerId: int32) {.cdecl, gcsafe.} =
  discard

# ============================================================================
# Message Delivery (called by transport when messages arrive)
# ============================================================================

proc deliverMessageToGroup(c: NuRaftCoordinator, groupId: GroupID,
    msgData: cstring, msgLen: csize_t) =
  echo "DEBUG deliverMessageToGroup: groupId=", groupId, " len=", msgLen
  var listener: MultiplexedListener
  withLock c.groupsLock:
    let inst = c.groups.getOrDefault(groupId, nil)
    echo "DEBUG deliverMessageToGroup: inst is nil=", inst.isNil
    if inst != nil and not inst.rpcContext.isNil:
      listener = nuraftMultiplexedGetListener(inst.rpcContext)
      echo "DEBUG deliverMessageToGroup: listener is nil=", listener.isNil
  if not listener.isNil:
    nuraftMultiplexedDeliverMessage(listener, msgData, msgLen)
    echo "DEBUG deliverMessageToGroup: delivered to listener"
  else:
    echo "DEBUG deliverMessageToGroup: no listener, message dropped!"

proc deliverMessageWrapper(coordPtr: pointer, groupId: GroupID,
    msgData: cstring, msgLen: csize_t) {.gcsafe, cdecl.} =
  let c = cast[NuRaftCoordinator](coordPtr)
  if c != nil:
    {.cast(gcsafe).}:
      c.deliverMessageToGroup(groupId, msgData, msgLen)

# ============================================================================
# Coordinator Lifecycle
# ============================================================================

type
  CoordinatorConfig* = object
    nodeId*: group_types.NodeID
    port*: int ## Single port for all groups
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
  result.port = config.port
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
  result.groupCreationRunning.store(false)
  result.groupCreationPending.store(0)
  result.nextTimerId = 1
  result.groups = initTable[GroupID, NuRaftGroupInstancePtr]()
  result.groupsCreating = initTable[GroupID, bool]()
  result.peerInfo = initTable[uint32, tuple[host: string, port: int]]()
  initLock(result.groupsLock)
  initLock(result.groupCreationLock)
  initLock(result.groupsCreatingLock)
  initLock(result.timerLock)

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

# Forward declarations
proc createAndStartGroup*(c: NuRaftCoordinator, groupId: GroupID,
    members: seq[tuple[nodeId: uint32, host: string, port: int]],
    preferredLeader: uint32 = 0): bool

proc hasGroup*(c: NuRaftCoordinator, groupId: GroupID): bool

proc start*(c: NuRaftCoordinator) =
  ## Start the coordinator.
  c.running.store(true)
  c.groupCreationRunning.store(true)

  # Create the multiplexed transport
  let coreNodeId = core_types.NodeID("n" & $c.nodeId.uint32)
  c.transport = newMultiplexedRaftTransport(coreNodeId, c.host, c.port)

  # Set up the coordinator callback for message delivery
  let coordPtr = cast[pointer](c)
  c.transport.setCoordinatorCallback(proc(groupId: GroupID, msgData: cstring,
      msgLen: csize_t) {.gcsafe, closure.} =
    deliverMessageWrapper(coordPtr, groupId, msgData, msgLen)
  )

  if not c.transport.startServer():
    error("Failed to start multiplexed transport", "port", c.port)
    return

  # Start the async group creation worker thread
  createThread(c.groupCreationThread, proc(p: pointer) {.thread, gcsafe.} =
    let coord = cast[NuRaftCoordinator](p)
    while coord.groupCreationRunning.load():
      var requests: seq[GroupCreationRequest] = @[]
      withLock coord.groupCreationLock:
        if coord.groupCreationQueue.len > 0:
          requests = coord.groupCreationQueue
          coord.groupCreationQueue = @[]
          discard coord.groupCreationPending.fetchAdd(int32(requests.len))

      for req in requests:
        if not coord.groupCreationRunning.load(): break
        {.cast(gcsafe).}:
          let ok = coord.createAndStartGroup(req.groupId, req.members,
              req.preferredLeader)
          if ok and req.storePtr != nil:
            if onGroupCreatedCallback != nil:
              onGroupCreatedCallback(req.storePtr, req.groupId)
        withLock coord.groupsCreatingLock:
          coord.groupsCreating.del(req.groupId)
        discard coord.groupCreationPending.fetchAdd(-1)

      sleep(50)
  , cast[pointer](c))

proc stop*(c: NuRaftCoordinator) =
  ## Stop all NuRaft instances and close the storage backend.
  if not c.running.load: return
  c.running.store(false)

  # Stop the async group creation worker
  c.groupCreationRunning.store(false)
  joinThread(c.groupCreationThread)

  # Clear in-progress tracking
  withLock c.groupsCreatingLock:
    c.groupsCreating.clear()

  # Mark all instances as stopped (to prevent callbacks from accessing freed memory)
  withLock c.groupsLock:
    for gid, inst in c.groups:
      inst.stopped = true

  # Wait for any in-flight callbacks to complete
  # This is critical: callbacks may still be executing in NuRaft threads
  sleep(500)

  # Collect instances to destroy while holding lock, then release lock before destroying
  # This prevents deadlock where destruction waits for threads that need the lock
  var instancesToDestroy: seq[NuRaftGroupInstancePtr] = @[]
  withLock c.groupsLock:
    for gid, inst in c.groups:
      instancesToDestroy.add(inst)
    c.groups.clear()

  # Now destroy without holding the lock
  for inst in instancesToDestroy:
    if not inst.server.isNil:
      nuraftServerShutdown(inst.server)
      nuraftServerDestroy(inst.server)
    # Note: SM and SMgr are owned by the context when using nuraftServerCreateWithContext
    # so we don't destroy them separately here
    if not inst.rpcContext.isNil:
      nuraftMultiplexedDestroy(inst.rpcContext)
    freeInstance(inst)

  # Stop transport
  if c.transport != nil:
    c.transport.stopServer()
    c.transport.destroy()

  if c.store != nil:
    c.store.close()

  deinitLock(c.groupsLock)
  deinitLock(c.groupCreationLock)
  deinitLock(c.groupsCreatingLock)
  deinitLock(c.timerLock)

proc queueGroupCreation*(c: NuRaftCoordinator, groupId: GroupID,
    members: seq[tuple[nodeId: uint32, host: string, port: int]],
    preferredLeader: uint32 = 0, storePtr: pointer = nil): bool =
  ## Queue a group creation request to be processed asynchronously.
  if c.hasGroup(groupId):
    return true

  withLock c.groupsCreatingLock:
    if c.groupsCreating.hasKey(groupId):
      return true
    c.groupsCreating[groupId] = true

  withLock c.groupCreationLock:
    c.groupCreationQueue.add(GroupCreationRequest(
      groupId: groupId,
      members: members,
      preferredLeader: preferredLeader,
      storePtr: storePtr
    ))
  return true

proc waitForGroupCreationQueue*(c: NuRaftCoordinator,
    timeoutMs: int = 5000): bool =
  let startMs = getTime().toUnixFloat() * 1000
  while true:
    var queueLen = 0
    var creatingLen = 0
    withLock c.groupCreationLock:
      queueLen = c.groupCreationQueue.len
    withLock c.groupsCreatingLock:
      creatingLen = c.groupsCreating.len
    let pending = c.groupCreationPending.load()
    if queueLen == 0 and pending == 0 and creatingLen == 0:
      return true
    let nowMs = getTime().toUnixFloat() * 1000
    if nowMs - startMs > timeoutMs.float:
      return false
    sleep(10)

# ============================================================================
# Group Management
# ============================================================================

proc createAndStartGroup*(c: NuRaftCoordinator, groupId: GroupID,
    members: seq[tuple[nodeId: uint32, host: string, port: int]],
    preferredLeader: uint32 = 0): bool =
  ## Create and start a NuRaft instance for one Raft group.
  ## All members use the same port (multiplexed).

  withLock c.groupsLock:
    if c.groups.hasKey(groupId):
      return true

  # Build server list
  var serverIds = newSeq[int32](members.len)
  var endpoints = newSeq[string](members.len)
  var myEndpoint = ""

  # Cache peer info
  # IMPORTANT: Use "serverId@host:port" format so NuRaft can extract the server ID
  # from the endpoint when creating RPC clients
  for i, m in members:
    serverIds[i] = int32(m.nodeId)
    # Format: serverId@host:port - NuRaft expects this format for create_client
    endpoints[i] = $m.nodeId & "@" & m.host & ":" & $m.port
    if m.nodeId == c.nodeId.uint32:
      myEndpoint = endpoints[i]
    withLock c.groupsLock:
      c.peerInfo[m.nodeId] = (m.host, m.port)
    echo "DEBUG createAndStartGroup: nodeId=", c.nodeId.uint32,
        " caching peer nodeId=", m.nodeId, " endpoint=", endpoints[i]

  if myEndpoint == "":
    return false

  # Create the group instance
  let inst = allocInstance()
  inst.groupId = groupId
  inst.coordPtr = cast[pointer](c)

  # Create state machine
  inst.sm = nuraftSmCreate(nuraftCommitCb, cast[pointer](inst))
  if inst.sm.isNil:
    error("Failed to create NuRaft SM", "groupId", $groupId)
    freeInstance(inst)
    return false

  # Create state manager
  var cServerIds = newSeq[int32](members.len)
  var cEndpoints = newSeq[cstring](members.len)
  for i in 0 ..< members.len:
    cServerIds[i] = serverIds[i]
    cEndpoints[i] = cstring(endpoints[i])

  inst.smgr = nuraftSmgrCreate(
    int32(c.nodeId.uint32),
    cstring(myEndpoint),
    int32(members.len),
    addr cServerIds[0],
    addr cEndpoints[0]
  )

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
  nuraftParamsSetReturnMethod(params, 0)
  nuraftParamsSetSnapshotDistance(params, 0)
  nuraftParamsSetClientReqTimeout(params, 5000)
  nuraftParamsSetMaxAppendSize(params, 100)
  nuraftParamsSetLeadershipTransferMinWaitTime(params, 1000)

  # Create per-group RPC context
  inst.rpcContext = nuraftMultiplexedCreate(
    int32(c.nodeId.uint32),
    cast[pointer](inst),
    multiplexedSendCb,
    cast[pointer](c),
    multiplexedScheduleTimerCb,
    multiplexedCancelTimerCb
  )

  if inst.rpcContext.isNil:
    error("Failed to create multiplexed RPC context", "groupId", $groupId)
    nuraftParamsDestroy(params)
    nuraftSmgrDestroy(inst.smgr)
    nuraftSmDestroy(inst.sm)
    freeInstance(inst)
    return false

  # Get the listener and set up response callback
  let listener = nuraftMultiplexedGetListener(inst.rpcContext)
  if not listener.isNil:
    # Set the GroupID bytes for this listener
    let ulid = groupIDToULID(groupId)
    nuraftMultiplexedSetGroupId(listener, cast[cstring](addr ulid.data[0]))
    # Set the source node ID for responses
    nuraftMultiplexedSetSrcNodeId(listener, int32(c.nodeId.uint32))
    # Set the response callback - uses the same send logic
    nuraftMultiplexedSetResponseCallback(listener, cast[pointer](inst), multiplexedSendCb)

  # Create raft server with multiplexed context
  inst.server = nuraftServerCreateWithContext(
    inst.rpcContext,
    inst.sm,
    inst.smgr,
    params,
    nuraftEventCb,
    cast[pointer](inst)
  )

  nuraftParamsDestroy(params)

  if inst.server.isNil:
    error("Failed to create NuRaft server", "groupId", $groupId)
    nuraftMultiplexedDestroy(inst.rpcContext)
    nuraftSmgrDestroy(inst.smgr)
    nuraftSmDestroy(inst.sm)
    freeInstance(inst)
    return false

  # Wire up the message handler: the raft_server is the handler for incoming messages
  # This is critical - without this, incoming messages are dropped because handler is null
  let listenerForListen = nuraftMultiplexedGetListener(inst.rpcContext)
  if not listenerForListen.isNil:
    nuraftMultiplexedListen(listenerForListen, inst.server)

  # Set priority for preferred leader
  if preferredLeader > 0:
    for m in members:
      if m.nodeId == preferredLeader:
        discard nuraftServerSetPriority(inst.server, int32(m.nodeId), 100)
      else:
        discard nuraftServerSetPriority(inst.server, int32(m.nodeId), 50)

  # Add to groups table
  withLock c.groupsLock:
    c.groups[groupId] = inst

  info("Started NuRaft group", "groupId", $groupId, "port", c.port,
       "members", $members.len)

  return true

# ============================================================================
# Parallel Group Creation
# ============================================================================

type
  GroupCreationArg = object
    coord: pointer
    groupId: GroupID
    members: seq[tuple[nodeId: uint32, host: string, port: int]]
    preferredLeader: uint32
    success: bool

proc groupCreationWorker(arg: ptr GroupCreationArg) {.thread.} =
  let coord = cast[NuRaftCoordinator](arg.coord)
  arg.success = coord.createAndStartGroup(
    arg.groupId, arg.members, arg.preferredLeader)

proc createAndStartGroupsParallel*(c: NuRaftCoordinator,
    groupIds: openArray[GroupID],
    members: seq[tuple[nodeId: uint32, host: string, port: int]],
    preferredLeader: uint32 = 0): bool =
  ## Create and start multiple NuRaft instances in parallel.
  if groupIds.len == 0:
    return true

  for gid in groupIds:
    withLock c.groupsLock:
      if c.groups.hasKey(gid):
        continue

  if groupIds.len == 1:
    return c.createAndStartGroup(groupIds[0], members, preferredLeader)

  var args = newSeq[GroupCreationArg](groupIds.len)
  var threads = newSeq[Thread[ptr GroupCreationArg]](groupIds.len)

  {.cast(raises: []).}:
    for i, gid in groupIds:
      args[i] = GroupCreationArg(
        coord: cast[pointer](c),
        groupId: gid,
        members: members,
        preferredLeader: preferredLeader,
        success: false
      )
      createThread(threads[i], groupCreationWorker, addr args[i])

    for t in threads.mitems:
      joinThread(t)

  var allSuccess = true
  for i, arg in args:
    if not arg.success:
      error("Failed to create group in parallel", "groupId", $groupIds[i])
      allSuccess = false

  return allSuccess

proc removeGroup*(c: NuRaftCoordinator, groupId: GroupID) =
  var inst: NuRaftGroupInstancePtr
  withLock c.groupsLock:
    if not c.groups.hasKey(groupId): return
    inst = c.groups[groupId]
    c.groups.del(groupId)

  inst.stopped = true
  if not inst.server.isNil:
    nuraftServerShutdown(inst.server)
    nuraftServerDestroy(inst.server)
  if not inst.rpcContext.isNil:
    nuraftMultiplexedDestroy(inst.rpcContext)
  if not inst.sm.isNil:
    nuraftSmDestroy(inst.sm)
  if not inst.smgr.isNil:
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
          debug("isLeader: true", {"groupId": $groupId,
              "nodeId": $c.nodeId.uint32}.toTable)

proc getLeader*(c: NuRaftCoordinator, groupId: GroupID): int32 =
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
  if command.kind != ckWrite:
    return RaftResult(success: false, error: "Only write commands supported")

  var inst: NuRaftGroupInstancePtr
  withLock c.groupsLock:
    if not c.groups.hasKey(groupId):
      return RaftResult(success: false,
          error: "Group not found: " & $groupId)
    {.cast(raises: []).}:
      inst = c.groups[groupId]

  if inst.server == nil:
    return RaftResult(success: false, error: "Server not initialized")

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
  let n = proposals.len
  if n == 0: return @[]

  if n == 1:
    return @[c.proposeAndWait(proposals[0].groupId, proposals[0].command,
        timeoutMs)]

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
    targetNodeId: group_types.NodeID, priority: int32): bool =
  var inst: NuRaftGroupInstancePtr
  withLock c.groupsLock:
    inst = c.groups.getOrDefault(groupId, nil)

  if inst == nil or inst.server == nil: return false

  let rc = nuraftServerSetPriority(inst.server, int32(targetNodeId.uint32), priority)
  if rc != 0:
    warn("Failed to set priority", "groupId", $groupId, "target",
        $targetNodeId.uint32, "rc", $rc)
    return false
  return true

proc transferLeadership*(c: NuRaftCoordinator, groupId: GroupID,
    targetNodeId: group_types.NodeID): bool =
  var inst: NuRaftGroupInstancePtr
  withLock c.groupsLock:
    inst = c.groups.getOrDefault(groupId, nil)

  if inst == nil or inst.server == nil: return false

  nuraftServerYieldLeadership(inst.server, false, int32(targetNodeId.uint32))
  return true

proc addServerToGroup*(c: NuRaftCoordinator, groupId: GroupID,
    nodeId: uint32, host: string, port: int): int32 =
  var inst: NuRaftGroupInstancePtr
  withLock c.groupsLock:
    if not c.groups.hasKey(groupId): return -1
    inst = c.groups[groupId]

  if inst.server == nil: return -1

  let endpoint = host & ":" & $port
  return nuraftServerAddSrv(inst.server, int32(nodeId), cstring(endpoint))

proc removeServerFromGroup*(c: NuRaftCoordinator, groupId: GroupID,
    nodeId: uint32): int32 =
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
  discard
