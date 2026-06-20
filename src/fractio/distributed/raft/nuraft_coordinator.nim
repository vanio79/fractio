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
import std/algorithm
import std/logging
import std/sequtils
import posix

import fractio/core/types as core_types except NodeID
import fractio/distributed/sharedtimer/timeprovider
import fractio/distributed/raft/c_bindings
import fractio/distributed/raft/group_types
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/multiplexed_bindings
import fractio/distributed/raft/multiplexed_transport
import fractio/distributed/raft/raft_persistent_store
import fractio/distributed/raft/raft_store_callbacks
import fractio/distributed/meta/system_tables
import fractio/utils/binary
import fractio/storage/wisckey_backend
import fractio/storage/backend
from fractio/distributed/raft/multiplexed_bindings import deliverMessage

# ============================================================================
# Types

type
  CommitWaitData = object
    ## Condition variable for proposeAndWait to block on instead of polling.
    ## Signaled from nuraftCommitCb when the state machine advances.
    lock: Lock
    cond: Cond

proc timedWait(cond: var Cond, lock: var Lock, timeoutMs: int): bool =
  ## Wait on condition variable with timeout in milliseconds.
  ## Returns true if signaled (or spurious wakeup), false on timeout.
  ## On Linux, Nim's Cond = pthread_cond_t and Lock = pthread_mutex_t,
  ## so we can safely cast to posix types for pthread_cond_timedwait.
  when defined(posix):
    var ts: Timespec
    discard clock_gettime(CLOCK_REALTIME, ts)
    let extraNs = clong(timeoutMs) * 1_000_000
    ts.tv_nsec = ts.tv_nsec + extraNs
    while ts.tv_nsec >= 1_000_000_000:
      ts.tv_nsec = ts.tv_nsec - 1_000_000_000
      inc ts.tv_sec
    # Cast Nim Lock/Cond to posix Pthread_mutex/Pthread_cond — same C type
    let rc = pthread_cond_timedwait(
      cast[ptr Pthread_cond](addr cond),
      cast[ptr Pthread_mutex](addr lock),
      addr ts)
    result = rc == 0
  else:
    sleep(timeoutMs)
    result = false

type
  NuRaftGroupInstance* = object
    groupId*: GroupID
    server*: NuRaftServer
    sm*: NuRaftSM
    smgr*: NuRaftSMgr
    ## WiscKey-backed persistent store for Raft log and state
    persistentStore*: RaftPersistentStore
    ## Per-group RPC context
    rpcContext*: MultiplexedContext
    ## Listener handle (for cleanup)
    listener*: MultiplexedListener
    ## Back-reference to coordinator (raw pointer to break cycles)
    coordPtr*: pointer
    ## Set to true during shutdown to prevent callbacks from accessing freed memory
    stopped*: bool
    ## Set to true after listener is set up - before that, buffer messages
    ## Uses atomic for thread-safe access from transport accept thread
    ready*: Atomic[bool]
    ## Reference count: incremented by acquireGroupInstance, decremented by
    ## releaseGroupInstance. removeGroup decrements and only frees when 0.
    ## Prevents use-after-free when a group is removed while another thread
    ## holds a pointer to the instance.
    refCount*: Atomic[int32]
    ## Condition variable for proposeAndWait — signaled on commit callback
    commitWaitPtr*: pointer

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

    ## Clock source for coordinated timestamps (nil = use local clock)
    timeProvider*: TimeProvider

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

    ## Pending messages for groups that aren't ready yet
    pendingMessages*: nimtables.Table[GroupID, seq[tuple[data: string, len: int]]]
    pendingMessagesLock*: Lock

# Use C malloc/free to avoid atomicArc cross-thread dealloc crashes.
proc c_malloc(size: csize_t): pointer {.importc: "malloc",
    header: "<stdlib.h>".}
proc c_free(p: pointer) {.importc: "free", header: "<stdlib.h>".}

var coordinatorTimeProvider*: TimeProvider = nil

proc coordNowNs*(): int64 {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    if coordinatorTimeProvider != nil:
      try:
        return coordinatorTimeProvider.now()
      except Exception:
        discard
  return localTimeNs()

proc allocInstance(): NuRaftGroupInstancePtr =
  result = cast[NuRaftGroupInstancePtr](c_malloc(csize_t(sizeof(
      NuRaftGroupInstance))))
  zeroMem(result, sizeof(NuRaftGroupInstance))
  result.refCount.store(1'i32, moRelaxed) # Group table holds one reference
  # Allocate and initialize condition variable for proposeAndWait
  let cw = cast[ptr CommitWaitData](c_malloc(csize_t(sizeof(CommitWaitData))))
  zeroMem(cw, sizeof(CommitWaitData))
  initLock(cw.lock)
  initCond(cw.cond)
  result.commitWaitPtr = cast[pointer](cw)

proc freeInstance(p: NuRaftGroupInstancePtr) =
  if p != nil:
    # Deinit and free the CommitWaitData
    if p.commitWaitPtr != nil:
      let cw = cast[ptr CommitWaitData](p.commitWaitPtr)
      deinitCond(cw.cond)
      deinitLock(cw.lock)
      c_free(p.commitWaitPtr)
      p.commitWaitPtr = nil
    c_free(p)

# Forward declarations needed for releaseGroupInstance
proc unregisterValidContext(ctx: pointer)
proc cancelAllTimersForContext(ctx: pointer)

proc acquireGroupInstance*(inst: NuRaftGroupInstancePtr) =
  ## Increment the reference count on a group instance.
  ## Must be called while the instance is still in the groups table
  ## (i.e., while groupsLock is held).
  if inst != nil:
    discard inst.refCount.fetchAdd(1'i32, moAcquireRelease)

proc releaseGroupInstance(inst: NuRaftGroupInstancePtr) {.raises: [], gcsafe.} =
  ## Decrement the reference count on a group instance.
  ## If the count reaches 0, destroy and free the instance.
  ## NOTE: nuraftServerShutdown must have been called already (by removeGroup)
  ## before the last reference is released. This proc only destroys the
  ## C++ objects and frees the C memory.
  if inst == nil:
    return
  let prev = inst.refCount.fetchSub(1'i32, moAcquireRelease)
  if prev == 1:
    # Last reference dropped — safe to destroy C++ objects and free memory
    {.cast(raises: []).}:
      {.cast(gcsafe).}:
        if not inst.server.isNil:
          nuraftServerDestroy(inst.server)
        if not inst.listener.isNil:
          nuraftMpListenerDestroy(inst.listener)
        if not inst.rpcContext.isNil:
          unregisterValidContext(cast[pointer](inst.rpcContext))
          cancelAllTimersForContext(cast[pointer](inst.rpcContext))
          nuraftMpContextDestroy(inst.rpcContext)
        if not inst.sm.isNil:
          nuraftSmDestroy(inst.sm)
        if not inst.smgr.isNil:
          nuraftSmgrDestroy(inst.smgr)
        if not inst.persistentStore.isNil:
          inst.persistentStore.close()
    freeInstance(inst)

proc setTimeProvider*(c: NuRaftCoordinator, tp: TimeProvider) =
  c.timeProvider = tp
  coordinatorTimeProvider = tp

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

# Forward declarations
proc isLeader*(c: NuRaftCoordinator, groupId: GroupID): bool {.raises: [].}
proc getLeader*(c: NuRaftCoordinator, groupId: GroupID): int32

proc clearModuleCallbacks*() {.gcsafe, raises: [].} =
  ## Clear all module-level callbacks.
  {.cast(gcsafe).}:
    applyBatchCallback = nil
    onGroupMetadataApplied = nil
    onLeaderChanged = nil
    getPreferredLeaderCallback = nil
    onGroupCreatedCallback = nil

proc cleanupGlobalState*() {.gcsafe, raises: [].} =
  ## Clear all global state (call at program exit to avoid GC issues).
  ## This must be called after all coordinators have been stopped.
  clearModuleCallbacks()

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
  ## Called by NuRaft when a log entry is committed.
  if ctx == nil or data == nil or len == 0:
    return

  let inst = cast[NuRaftGroupInstancePtr](ctx)
  if inst.stopped:
    return
  let coord = cast[NuRaftCoordinator](inst.coordPtr)
  if coord == nil or coord.kvStorePtr == nil:
    return

  # Signal the condition variable so proposeAndWait stops polling
  if inst.commitWaitPtr != nil:
    let cw = cast[ptr CommitWaitData](inst.commitWaitPtr)
    signal(cw.cond)

  {.cast(gcsafe).}:
    if applyBatchCallback != nil:
      applyBatchCallback(coord.kvStorePtr, inst.groupId, data, len.int)

# ============================================================================
# NuRaft Event Callback (leader/follower changes)
# ============================================================================

proc nuraftEventCb(ctx: pointer, eventType: int32,
    leaderId: int32, term: uint64) {.cdecl, gcsafe.} =
  ## Called when a node's Raft state changes (becomes leader or follower).
  ## When a node becomes a follower, `leaderId` contains the new leader's ID.
  ## This is critical for propagating leadership changes to all nodes, not just
  ## the new leader itself.
  if ctx == nil: return

  {.cast(gcsafe).}:
    let inst = cast[NuRaftGroupInstancePtr](ctx)
    if inst.stopped: return
    let coord = cast[NuRaftCoordinator](inst.coordPtr)
    if coord == nil: return

    let gidUlid = groupIDToULID(inst.groupId)
    var gidHex = ""
    for i in 12..<16:
      gidHex.add(toHex(gidUlid.data[i], 2))

    if eventType == NuRaftBecomeLeader:
      echo "[Nim] EVENT: node=", $coord.nodeId.uint32,
          " BECAME LEADER for group=", gidHex, " term=", $term
      if onLeaderChanged != nil and coord.kvStorePtr != nil:
        onLeaderChanged(coord.kvStorePtr, inst.groupId, coord.nodeId)
    elif eventType == NuRaftBecomeFollower:
      echo "[Nim] EVENT: node=", $coord.nodeId.uint32,
          " BECAME FOLLOWER for group=", gidHex, " leader=", $leaderId,
          " term=", $term
      # This is critical: followers learn about new leaders here
      if leaderId > 0 and onLeaderChanged != nil and coord.kvStorePtr != nil:
        onLeaderChanged(coord.kvStorePtr, inst.groupId, group_types.NodeID(leaderId))

# ============================================================================
# Config Change Callback (called when NuRaft cluster config changes)
# ============================================================================

proc nuraftConfigChangeCb(ctx: pointer, serverId: int32,
    endpoint: cstring): int32 {.cdecl, gcsafe.} =
  ## Called when NuRaft configuration changes (e.g., add_srv is committed).
  ## The endpoint format is "serverId@host:port" - we parse it and update peerInfo.
  if ctx == nil or endpoint == nil:
    return -1

  let inst = cast[NuRaftGroupInstancePtr](ctx)
  if inst.stopped:
    return -1

  let coord = cast[NuRaftCoordinator](inst.coordPtr)
  if coord == nil:
    return -1

  # Parse endpoint format: "serverId@host:port"
  let endpointStr = $endpoint

  let atPos = endpointStr.find('@')
  if atPos < 0:
    return -1

  let hostPort = endpointStr[atPos + 1 ..< endpointStr.len]
  let colonPos = hostPort.find(':')
  if colonPos < 0:
    return -1

  let host = hostPort[0 ..< colonPos]
  let portStr = hostPort[colonPos + 1 ..< hostPort.len]
  let port = parseInt(portStr)

  # Update peerInfo table
  var needConnect = false
  let corePeerId = core_types.NodeID("n" & $serverId)
  withLock coord.groupsLock:
    coord.peerInfo[uint32(serverId)] = (host, port)
    # Check if we need to connect to this peer (while holding groupsLock)
    if coord.transport != nil and uint32(serverId) != uint32(coord.nodeId):
      withLock coord.transport.connectionsLock:
        if not coord.transport.connections.hasKey(corePeerId):
          needConnect = true

  # Proactively establish TCP connection to this peer (outside locks to avoid deadlock).
  # This ensures connections exist BEFORE an election is needed.
  if needConnect:
    discard coord.transport.connectToPeer(corePeerId, host, port)

  return 0

# ============================================================================
# Quorum Update Callback (called when cluster config changes server count)
# ============================================================================

proc nuraftQuorumUpdateCb(ctx: pointer, serverId: int32,
    quorumSize: int32) {.cdecl, gcsafe.} =
  ## Called when NuRaft config changes and quorum needs to be updated.
  if ctx == nil:
    return

  let inst = cast[NuRaftGroupInstancePtr](ctx)
  if inst.stopped:
    return

  if inst.server.isNil:
    return

  # Update the quorum on the running server
  nuraftServerUpdateQuorum(inst.server, quorumSize)

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

  # Use the instance's groupId (C++ passes a placeholder of zeros)
  let groupId = inst.groupId
  let ulid = groupIDToULID(groupId)

  # Look up peer info
  var peerHost = ""
  var peerPort = 0

  withLock coord.groupsLock:
    if coord.peerInfo.hasKey(uint32(dstNodeId)):
      (peerHost, peerPort) = coord.peerInfo[uint32(dstNodeId)]

  if peerHost == "":
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
  let sendResult = coord.transport.sendSync(corePeerId, frame, peerHost, peerPort)
  if sendResult:
    return 0
  return -1

# ============================================================================
# Global Timer Management
# =============================================================================

# Global timer management (simpler than per-coordinator)
# Key is (timerId, rpcCtx) to avoid collisions between groups
var gActiveTimers: nimtables.Table[tuple[timerId: int32, rpcCtx: pointer],
    tuple[expireNs: int64]]
var gTimerLock: Lock
var gTimerCond: Cond
var gTimerThread: Thread[void]
var gTimerThreadRunning: Atomic[bool]
var gTimerThreadRefCount: int32 # Reference count for timer thread

# Track valid contexts to prevent timer invocation on destroyed contexts
var gValidContexts: nimtables.Table[pointer, bool]
var gValidContextsLock: Lock

initLock(gTimerLock)
initCond(gTimerCond)
initLock(gValidContextsLock)
gTimerThreadRunning.store(false)
gTimerThreadRefCount = 0

# Timer callbacks - actually schedule timers
# The ctx is the rpcContext (MultiplexedContext) pointer
proc multiplexedScheduleTimerCb(ctx: pointer, timerId: int32,
    delayMs: int32) {.cdecl, gcsafe.} =
  # ctx is the rpcContext (MultiplexedContext) pointer
  let rpcCtx = cast[MultiplexedContext](ctx)
  if rpcCtx.isNil:
    return

  # Schedule actual timer in Nim - when it fires, call nuraftMpInvokeTimer
  {.cast(gcsafe).}:
    withLock gTimerLock:
      let nowNs = coordNowNs()
      let expireNs = nowNs + delayMs.int64 * 1_000_000
      # Use (timerId, ctx) as key to avoid collisions between groups
      gActiveTimers[(timerId: timerId, rpcCtx: ctx)] = (expireNs: expireNs)
      # Wake the timer thread so it can recalculate the minimum wait
      signal(gTimerCond)

proc multiplexedCancelTimerCb(ctx: pointer, timerId: int32) {.cdecl, gcsafe.} =
  {.cast(gcsafe).}:
    withLock gTimerLock:
      let key = (timerId: timerId, rpcCtx: ctx)
      gActiveTimers.del(key)

proc registerValidContext(ctx: pointer) =
  ## Register a context as valid for timer invocation.
  {.cast(gcsafe).}:
    withLock gValidContextsLock:
      gValidContexts[ctx] = true

proc unregisterValidContext(ctx: pointer) =
  ## Mark a context as invalid (about to be destroyed).
  ## Prevents timer callbacks from accessing freed memory.
  {.cast(gcsafe).}:
    withLock gValidContextsLock:
      gValidContexts.del(ctx)

proc isValidContext(ctx: pointer): bool =
  ## Check if a context is still valid for timer invocation.
  {.cast(gcsafe).}:
    withLock gValidContextsLock:
      result = gValidContexts.hasKey(ctx)

proc cancelAllTimersForContext(ctx: pointer) =
  ## Cancel all timers associated with a specific rpcCtx.
  ## Called when a context is destroyed to prevent stale timer callbacks.
  {.cast(gcsafe).}:
    withLock gTimerLock:
      var keysToDelete: seq[tuple[timerId: int32, rpcCtx: pointer]] = @[]
      for key, entry in gActiveTimers:
        if key.rpcCtx == ctx:
          keysToDelete.add(key)
      for key in keysToDelete:
        gActiveTimers.del(key)

# Timer thread that waits for expired timers and invokes them.
# Uses a condition variable with a calculated timeout (minimum timer expiry)
# instead of sleep(5) polling, reducing CPU waste and improving timer accuracy.
proc timerThreadProc() {.thread, gcsafe.} =
  var purgeCounter = 0
  while gTimerThreadRunning.load(moRelaxed):
    # Collect expired timers under lock
    var expiredTimers: seq[tuple[timerId: int32, rpcCtx: pointer]] = @[]
    var waitMs = 100 # Default wait: 100ms if no timers are active
    {.cast(gcsafe).}:
      acquire(gTimerLock)
      let nowNs = coordNowNs()
      # Find the earliest timer expiry and collect all expired timers
      var earliestNs: int64 = 0
      for key, entry in gActiveTimers:
        if entry.expireNs <= nowNs:
          expiredTimers.add(key)
        elif earliestNs == 0 or entry.expireNs < earliestNs:
          earliestNs = entry.expireNs
      # Delete expired timers
      for key in expiredTimers:
        gActiveTimers.del(key)
      # Calculate wait time: sleep until earliest timer expires
      if earliestNs > 0:
        let delayNs = earliestNs - nowNs
        waitMs = max(1, int(delayNs div 1_000_000)) # Convert ns→ms, min 1ms
      elif gActiveTimers.len == 0:
        waitMs = 100 # No timers active — wait longer
      # Wait on condition variable with calculated timeout.
      # The cond is signaled when: a new timer is scheduled, or the thread
      # should stop. This replaces the old sleep(5) busy-poll.
      if not gTimerThreadRunning.load(moRelaxed):
        release(gTimerLock)
        break
      discard timedWait(gTimerCond, gTimerLock, waitMs)
      release(gTimerLock)

    # Invoke timers WITHOUT holding the lock to avoid deadlock
    for item in expiredTimers:
      # Check if context is still valid before invoking
      if not isValidContext(item.rpcCtx):
        continue
      try:
        let rpcCtx = cast[MultiplexedContext](item.rpcCtx)
        discard nuraftMpInvokeTimer(rpcCtx, item.timerId)
      except CatchableError:
        discard
      except:
        discard

    # Purge expired RPC handlers every ~10 seconds (100 iterations × 100ms)
    inc purgeCounter
    if purgeCounter >= 100:
      purgeCounter = 0
      nuraftPurgeExpiredHandlers()

proc startTimerThread() =
  {.cast(gcsafe).}:
    withLock gTimerLock:
      inc gTimerThreadRefCount
      if not gTimerThreadRunning.load(moRelaxed):
        gTimerThreadRunning.store(true)
        createThread(gTimerThread, timerThreadProc)

proc stopTimerThread() =
  {.cast(gcsafe).}:
    withLock gTimerLock:
      if gTimerThreadRefCount > 0:
        dec gTimerThreadRefCount
        if gTimerThreadRefCount == 0:
          # Last reference - actually stop the thread
          gTimerThreadRunning.store(false)
          # Signal the cond so the timer thread wakes up immediately
          signal(gTimerCond)
  # Join outside lock to avoid deadlock
  if gTimerThreadRefCount == 0:
    joinThread(gTimerThread)
    {.cast(gcsafe).}:
      withLock gTimerLock:
        gActiveTimers.clear()
      deinitCond(gTimerCond)

# ============================================================================
# Message Delivery (called by transport when messages arrive)
# ============================================================================

proc bufferMessage(c: NuRaftCoordinator, groupId: GroupID, msgData: pointer,
    msgLen: csize_t) {.gcsafe.} =
  var data = newString(msgLen.int)
  if msgLen > 0:
    copyMem(addr data[0], msgData, msgLen)
  {.cast(gcsafe).}:
    withLock c.pendingMessagesLock:
      if not c.pendingMessages.hasKey(groupId):
        c.pendingMessages[groupId] = @[]
      c.pendingMessages[groupId].add((data: data, len: msgLen.int))

proc deliverBufferedMessages(c: NuRaftCoordinator,
    groupId: GroupID) {.gcsafe.} =
  {.cast(gcsafe).}:
    withLock c.pendingMessagesLock:
      if c.pendingMessages.hasKey(groupId):
        for (data, len) in c.pendingMessages[groupId]:
          # Use the new deliverMessage wrapper
          var inst: NuRaftGroupInstancePtr
          withLock c.groupsLock:
            inst = c.groups.getOrDefault(groupId, nil)
            if inst != nil:
              acquireGroupInstance(inst)
          if inst != nil:
            if not inst.stopped and not inst.server.isNil and
                not inst.rpcContext.isNil:
              deliverMessage(inst.rpcContext, inst.server, data)
            releaseGroupInstance(inst)
        c.pendingMessages.del(groupId)

proc clearPendingMessages(c: NuRaftCoordinator) {.gcsafe, raises: [].} =
  ## Clear all pending messages (called during shutdown).
  ## IMPORTANT: Under AtomicArc, we must NOT destroy the pendingMessages table
  ## synchronously because NuRaft's ASIO threads may still be executing
  ## bufferMessage callbacks that reference the table's internal data structures.
  ## Even though setCoordinatorCallback(nil) was called earlier, in-flight
  ## callbacks can still complete and access the table under pendingMessagesLock.
  ## Destroying the table (via clear, swap, or =sink) while a callback is
  ## concurrently holding the lock leads to a use-after-free in Nim's allocator
  ## (addToSharedFreeList crash).
  ## Instead, we just reset the running flag (already done) and leave the table
  ## to be cleaned up by GC when the coordinator object is collected. The
  ## transport has been destroyed, so no new messages will arrive. Any
  ## in-flight callbacks will find running=false and return early.
  discard

proc deliverMessageToGroup(c: NuRaftCoordinator, groupId: GroupID,
    msgData: pointer, msgLen: csize_t) =
  # Check if coordinator is still running
  if not c.running.load(moRelaxed):
    return

  var shouldBuffer = false
  var inst: NuRaftGroupInstancePtr

  withLock c.groupsLock:
    inst = c.groups.getOrDefault(groupId, nil)
    if inst != nil:
      acquireGroupInstance(inst)
      if inst.ready.load(moRelaxed) and not inst.rpcContext.isNil and
          not inst.server.isNil and not inst.stopped:
        # Instance is fully ready - deliver message directly
        discard
      else:
        # Instance exists but not fully ready yet (or shutting down)
        shouldBuffer = true
    else:
      # Instance doesn't exist yet - buffer the message
      shouldBuffer = true

  if shouldBuffer:
    if inst != nil:
      releaseGroupInstance(inst)
    bufferMessage(c, groupId, msgData, msgLen)
  elif inst != nil:
    # Deliver message using the new API
    # IMPORTANT: msgData is binary data, must copy with explicit length (not $cstring)
    var binaryMsg = newString(msgLen.int)
    if msgLen > 0:
      copyMem(addr binaryMsg[0], msgData, msgLen.int)
    deliverMessage(inst.rpcContext, inst.server, binaryMsg)
    releaseGroupInstance(inst)

proc deliverMessageWrapper(coordPtr: pointer, groupId: GroupID,
    msgData: pointer, msgLen: csize_t) {.gcsafe, cdecl.} =
  let c = cast[NuRaftCoordinator](coordPtr)
  if c != nil and c.running.load(moRelaxed):
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
    maxFileSize*: int
      ## LevelDB max SST file size in bytes; 0 = LevelDB default (2 MB).
      ## Larger files mean fewer L0 SSTs, which reduces L0 compaction backlog.
    l0CompactionTrigger*: int
      ## L0 file count threshold for manual c_leveldb_compact_range();
      ## 0 = disabled. Fork's C API does not expose the standard
      ## level0_slowdown_writes_trigger knob.

proc newNuRaftCoordinator*(config: CoordinatorConfig): NuRaftCoordinator =
  # The global NuRaft manager causes OOM during 3-replica group creation.
  # Root cause: global_mgr worker threads trigger Nim code paths that conflict.
  # Instead, use per-group threads but with reduced stack size (2MB vs default 8MB).
  # With 5 groups per node (3 data + 2 meta), that's 10 threads × 2MB = 20MB.
  # This is much better than the original 10 × 8MB = 80MB.
  # NOTE: nuraftGlobalMgrInit is left disabled until the reentrancy issue is fixed.
  # let initResult = nuraftGlobalMgrInit(1, 1)
  # if initResult < 0:
  #   raise newException(CatchableError, "Failed to initialize NuRaft global manager")
  # if initResult == 1:
  #   info "NuRaft global manager initialized (shared thread pool)"

  new(result)
  result.nodeId = config.nodeId
  result.port = config.port
  result.host = config.host
  result.dataDir = config.dataDir
  result.electionTimeoutLowerMs = config.electionTimeoutLowerMs
  result.electionTimeoutUpperMs = config.electionTimeoutUpperMs
  result.heartbeatIntervalMs = config.heartbeatIntervalMs
  # Bumped from 300/600/100 (May 2026) to 1000/2000/150 to prevent
  # spurious elections under sustained INSERT load on 3-replica clusters.
  # With aggressive defaults, the META group experienced term bouncing
  # (term 1 -> 2 -> 3 -> 4 in <8 min) under 1M-row load, because followers
  # would start elections before the busy leader could send a heartbeat.
  # Standard Raft guidance: heartbeat = election_timeout / 10.
  #
  # Bumped again (Jun 2026) to 3000/5000/300 to handle heavy sustained
  # load (1M-row bulk inserts). Under 1M load we observed 4+ META
  # leadership changes in 1 hour (term 1->3->4->5), each triggering
  # 30+ minute stalls for in-flight INSERT batches. Larger election
  # windows tolerate longer leader-busy periods without spurious
  # elections. Heartbeat at 300ms / election at 3000ms = 1:10 ratio.
  if result.electionTimeoutLowerMs == 0: result.electionTimeoutLowerMs = 3000
  if result.electionTimeoutUpperMs == 0: result.electionTimeoutUpperMs = 5000
  if result.heartbeatIntervalMs == 0: result.heartbeatIntervalMs = 300
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
  initLock(result.pendingMessagesLock)

  # Open WiscKey backend
  let wbs = if config.writeBufferSize > 0: config.writeBufferSize
            else: 4 * 1024 * 1024
  let storeCfg = StorageConfig(
    path: config.dataDir, createIfMissing: true, syncWrites: true,
    writeBufferSize: wbs, blockCacheSize: config.blockCacheSize,
    maxFileSize: config.maxFileSize,
    l0CompactionTrigger: config.l0CompactionTrigger,
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
    preferredLeader: uint32 = 0): bool {.gcsafe.}

proc hasGroup*(c: NuRaftCoordinator, groupId: GroupID): bool

proc start*(c: NuRaftCoordinator) =
  ## Start the coordinator.
  c.running.store(true)
  c.groupCreationRunning.store(true)

  # Start the global timer thread (for invoking NuRaft timers)
  startTimerThread()

  # Create the multiplexed transport
  let coreNodeId = core_types.NodeID("n" & $c.nodeId.uint32)
  c.transport = newMultiplexedRaftTransport(coreNodeId, c.host, c.port)

  # Set up the coordinator callback for message delivery
  let coordPtr = cast[pointer](c)
  c.transport.setCoordinatorCallback(proc(groupId: GroupID, msgData: pointer,
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

        # CRITICAL: If we're NOT the preferred leader, delay before creating.
        # This gives the preferred leader time to create its instance first
        # and start sending heartbeats. Without this delay, we create with
        # skipInitialElection=true and wait for heartbeats that haven't started.
        let isPreferredLeader = uint32(coord.nodeId) == req.preferredLeader
        if not isPreferredLeader and req.preferredLeader > 0:
          # Wait for preferred leader to create its instance.
          # The preferred leader's election timeout is 300-500ms,
          # so we wait slightly longer to ensure it has started.
          when defined(debugGroupCreation):
            echo "[asyncWorker] waiting for preferred leader to create groupId=",
                req.groupId, " preferredLeader=", req.preferredLeader
          sleep(100) # Give preferred leader 100ms head start

        {.cast(gcsafe).}:
          # Log the creation attempt
          when defined(debugGroupCreation):
            echo "[asyncWorker] createAndStartGroup groupId=", req.groupId,
                " preferredLeader=", req.preferredLeader, " myNodeId=", coord.nodeId
          let ok = coord.createAndStartGroup(req.groupId, req.members,
              req.preferredLeader)
          when defined(debugGroupCreation):
            echo "[asyncWorker] createAndStartGroup result groupId=",
                req.groupId, " ok=", ok
          if ok:
            if req.storePtr != nil:
              if onGroupCreatedCallback != nil:
                onGroupCreatedCallback(req.storePtr, req.groupId)

            # If we're the preferred leader, wait for leader election
            # The other member will create its instance via onGroupMetadataApplied
            # and we need the election to complete before proceeding
            if isPreferredLeader:
              var leaderElected = false
              for i in 0 ..< 50: # 50 * 20ms = 1 second max
                if coord.isLeader(req.groupId):
                  leaderElected = true
                  when defined(debugGroupCreation):
                    echo "[asyncWorker] leader elected (I am leader) groupId=", req.groupId
                  break
                let leaderId = coord.getLeader(req.groupId)
                if leaderId > 0:
                  leaderElected = true
                  when defined(debugGroupCreation):
                    echo "[asyncWorker] leader elected (other) groupId=",
                        req.groupId, " leaderId=", leaderId
                  break
                sleep(20)
              when defined(debugGroupCreation):
                if not leaderElected:
                  echo "[asyncWorker] leader election timed out groupId=", req.groupId
        withLock coord.groupsCreatingLock:
          coord.groupsCreating.del(req.groupId)
        # Stagger group creation to avoid simultaneous elections
        # Each group's Raft server uses a random election timeout, but
        # if servers are created at the same instant, they may get the
        # same random seed. Adding a small delay ensures different seeds.
        sleep(50)
        discard coord.groupCreationPending.fetchAdd(-1)

      sleep(50)
  , cast[pointer](c))

proc stop*(c: NuRaftCoordinator) =
  ## Stop all NuRaft instances and close the storage backend.
  if not c.running.load: return
  c.running.store(false)

  # Stop the global timer thread FIRST before destroying any contexts
  # The timer thread may be trying to invoke timers on contexts we're about to destroy
  stopTimerThread()

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

  # CRITICAL: Stop transport FIRST before destroying instances
  # The transport's coordinatorCb may still try to deliver messages
  if c.transport != nil:
    # Clear the callback first to prevent new message deliveries
    c.transport.setCoordinatorCallback(nil)
    # Now stop the server - this closes all connections and stops the accept loop
    c.transport.stopServer()

  # Collect instances to release while holding lock, then release lock before releasing
  # This prevents deadlock where destruction waits for threads that need the lock
  var instancesToRelease: seq[NuRaftGroupInstancePtr] = @[]
  withLock c.groupsLock:
    for gid, inst in c.groups:
      instancesToRelease.add(inst)
    c.groups.clear()

  # Now shutdown and release without holding the lock
  # Since we cleared c.groups, each instance lost the table's reference.
  # We call nuraftServerShutdown first, then release (which may destroy if
  # no other thread holds a ref).
  for inst in instancesToRelease:
    if not inst.server.isNil:
      nuraftServerShutdown(inst.server)
    releaseGroupInstance(inst)

  # Fully destroy transport
  if c.transport != nil:
    c.transport.destroy()

  if c.store != nil:
    c.store.close()

  # NOTE: nuraftGlobalMgrShutdown left disabled (matching init above).
  # nuraftGlobalMgrShutdown()
  # info "NuRaft global manager shut down"

  # Clear pending messages (prevent GC access during final cleanup)
  clearPendingMessages(c)

  deinitLock(c.groupsLock)
  deinitLock(c.groupCreationLock)
  deinitLock(c.groupsCreatingLock)
  deinitLock(c.timerLock)
  deinitLock(c.pendingMessagesLock)

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
  let startMs = coordNowNs().float / 1_000_000.0
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
    let nowMs = coordNowNs().float / 1_000_000.0
    if nowMs - startMs > timeoutMs.float:
      return false
    sleep(10)

# ============================================================================
# Group Management
# ============================================================================

proc createAndStartGroup*(c: NuRaftCoordinator, groupId: GroupID,
    members: seq[tuple[nodeId: uint32, host: string, port: int]],
    preferredLeader: uint32 = 0): bool {.gcsafe.} =
  ## Create and start a NuRaft instance for one Raft group.
  ## All members use the same port (multiplexed).

  withLock c.groupsLock:
    if c.groups.hasKey(groupId):
      return true

  # Build server list
  var sortedMembers = members
  sortedMembers.sort(proc (x, y: tuple[nodeId: uint32, host: string,
      port: int]): int =
    cmp(x.nodeId, y.nodeId)
  )

  var serverIds = newSeq[int32](sortedMembers.len)
  var endpoints = newSeq[string](sortedMembers.len)
  var myEndpoint = ""

  # Cache peer info
  # IMPORTANT: Use "serverId@host:port" format so NuRaft can extract the server ID
  # from the endpoint when creating RPC clients
  # NOTE: NuRaft uses int32 for server IDs. Our nodeIds are uint32 in range 1..65535
  # (validated in config), so they always fit in int32. Use cast[int32] instead of
  # int32() to avoid RangeDefect when recovering from corrupted group records where
  # nodeId may exceed int32.max (e.g. from MVCC-encoded data being misread).
  for i, m in sortedMembers:
    serverIds[i] = cast[int32](m.nodeId)
    # Format: serverId@host:port - NuRaft expects this format for create_client
    endpoints[i] = $m.nodeId & "@" & m.host & ":" & $m.port
    if m.nodeId == c.nodeId.uint32:
      myEndpoint = endpoints[i]
    withLock c.groupsLock:
      c.peerInfo[m.nodeId] = (m.host, m.port)

  if myEndpoint == "":
    return false

  # Create the group instance
  let inst = allocInstance()
  inst.groupId = groupId
  inst.coordPtr = cast[pointer](c)

  # IMPORTANT: Add to groups table BEFORE creating the server.
  # This allows incoming messages to be buffered if they arrive before
  # the RPC context is ready.
  withLock c.groupsLock:
    c.groups[groupId] = inst

  # Helper to clean up on failure: removes from table (ref count not affected)
  # The caller is responsible for destroying C++ objects and calling
  # releaseGroupInstance(inst) or freeInstance(inst) after this.
  template cleanupOnFailure() =
    withLock c.groupsLock:
      c.groups.del(groupId)

  # Create state machine
  inst.sm = nuraftSmCreate(nuraftCommitCb, cast[pointer](inst))
  if inst.sm.isNil:
    error("Failed to create NuRaft SM", "groupId", $groupId)
    cleanupOnFailure()
    freeInstance(inst)
    return false

  # Determine if we're a joining node (not the preferred leader)
  # For SINGLE-MEMBER clusters only, use catching_up=true to prevent auto-leader
  # promotion. For multi-member clusters, we need election timers enabled so
  # nodes can participate in failover when the leader dies.
  # IMPORTANT: catching_up=true disables election_timer_allowed, preventing
  # the node from starting elections even when heartbeats stop!
  let isJoiningNode = (preferredLeader > 0'u32 and
                       preferredLeader != uint32(c.nodeId))
  let useCatchingUp = isJoiningNode and members.len == 1

  # Create WiscKey-backed persistent store for this group's Raft log and state.
  # This replaces the old per-group binary state file approach with durable
  # WiscKey storage using /raft/<groupId>/log/<index> and /raft/<groupId>/state.
  let persistentStore = newRaftPersistentStore(c.store, groupId)
  inst.persistentStore = persistentStore

  # Create state manager with callback-based persistence (WiscKey-backed)
  # The callbacks bridge C++ → Nim → RaftPersistentStore → WiscKey
  inst.smgr = createCallbackSmgr(
    persistentStore,
    cast[int32](c.nodeId.uint32),
    myEndpoint,
    serverIds,
    endpoints,
    useCatchingUp
  )

  if inst.smgr.isNil:
    error("Failed to create NuRaft SMgr", "groupId", $groupId)
    nuraftSmDestroy(inst.sm)
    if not inst.persistentStore.isNil:
      inst.persistentStore.close()
    cleanupOnFailure()
    freeInstance(inst)
    return false

  # Set config change callback so followers learn about new peers
  # when add_srv is committed
  nuraftSmgrSetConfigCb(inst.smgr, cast[pointer](inst), nuraftConfigChangeCb)

  # Create raft params
  # Use standard NuRaft election timeouts with random jitter.
  # The stagger during group creation ensures different random seeds.
  # Non-overlapping windows (like we had) cause leader to lose leadership
  # because election timing becomes deterministic.
  let params = nuraftParamsCreate()
  nuraftParamsSetElectionTimeout(params, c.electionTimeoutLowerMs,
      c.electionTimeoutUpperMs)
  nuraftParamsSetHeartbeatInterval(params, c.heartbeatIntervalMs)
  nuraftParamsSetReturnMethod(params, 0)
  # Snapshot distance: set to 0 to disable automatic snapshots.
  # NuRaft's snapshot mechanism requires a properly functioning state machine
  # snapshot implementation. Our callback_state_machine's create_snapshot()
  # stores metadata only (no KV state), and the log compaction that follows
  # snapshot creation causes large allocations in callback_log_store::pack().
  # TODO: Re-enable after implementing proper snapshot transfer and fixing
  # the pack() memory allocation issue.
  nuraftParamsSetSnapshotDistance(params, 0)
  nuraftParamsSetClientReqTimeout(params, 5000)
  nuraftParamsSetMaxAppendSize(params, 100)
  nuraftParamsSetLeadershipTransferMinWaitTime(params, 1000)

  # Set proper election quorum: majority = floor(N/2) + 1
  # NuRaft's default get_quorum_for_election() uses num_voting_members / 2,
  # which gives 1 for a 3-node cluster (wrong!). We need majority quorum.
  #
  # NuRaft's pre-vote uses election_quorum_size = get_quorum_for_election() + 1
  # For pre-vote to succeed, we need:
  #   election_quorum_size = majorityQuorum (so dead >= majorityQuorum triggers vote)
  # This means get_quorum_for_election() = majorityQuorum - 1
  # And custom_election_quorum_size = get_quorum_for_election() + 1 = majorityQuorum
  #
  # For 3 nodes: majorityQuorum = 2, custom = 2
  # Then get_quorum_for_election() = 2 - 1 = 1
  # And election_quorum_size = 1 + 1 = 2 ✓
  #
# For pre-vote: dead = 2 (self + surviving node) >= election_quorum_size = 2 ✓
  let majorityQuorum = int32(members.len div 2 + 1)
  let quorumSize = majorityQuorum # Fixed: was majorityQuorum + 1 (too high)
  nuraftParamsSetCustomElectionQuorumSize(params, quorumSize)
  # NOTE: auto_adjust_quorum is DISABLED. When enabled, NuRaft overrides the
  # manually-set custom_election_quorum_size after add_srv(), which causes
  # quorum to stay at 1 even after peers join. This allows ANY single node to
  # declare itself leader, producing split-brain on identical GroupIDs and
  # infinite redirect loops (the root cause of the 3-replica OOM).
  #
  # With auto_adjust disabled:
  # - 1-member group: quorum=1, leader elected immediately (1 alive >= 1)
  # - After add_srv: quorum callback updates to 2 (majority of 2 nodes)
  # - After 2nd add_srv: quorum=2 (majority of 3 nodes)
  #
  # This is correct and safe.
  nuraftParamsSetAutoAdjustQuorum(params, 0)

  # Create per-group RPC context
  inst.rpcContext = nuraftMpContextCreate(
    cast[int32](c.nodeId.uint32),
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
    if not inst.persistentStore.isNil:
      inst.persistentStore.close()
    cleanupOnFailure()
    freeInstance(inst)
    return false

  # Register context as valid for timer callbacks
  registerValidContext(cast[pointer](inst.rpcContext))

  # Set the GroupID bytes for the context
  # This is critical for response correlation across multiple groups
  let ulid = groupIDToULID(groupId)
  nuraftMpContextSetGroupId(inst.rpcContext, cast[cstring](addr ulid.data[0]))

  # Get and setup the listener for response handling
  inst.listener = setupListener(
    inst.rpcContext,
    cast[int32](c.nodeId.uint32),
    cast[pointer](inst),
    multiplexedSendCb
  )

  # Determine if we should skip initial election based on preferredLeader
  # If we're NOT the preferred leader, we skip the initial election and wait
  # for heartbeats from the preferred leader. This prevents split votes when
  # multiple members start at the same time.
  #
  # The preferred leader will:
  # 1. Start with election timer (skipInitialElection = false)
  # 2. Timeout and become candidate
  # 3. Send RequestVote to other members
  # 4. Win election with majority votes
  # 5. Send heartbeats to other members
  #
  # Non-preferred leaders will:
  # 1. Skip initial election timer (skipInitialElection = true)
  # 2. Wait for heartbeats from preferred leader
  # 3. Become follower when they receive heartbeats
  let skipInitialElection = (preferredLeader > 0'u32 and
                             preferredLeader != uint32(c.nodeId))

  # Create raft server with multiplexed context
  inst.server = nuraftServerCreate(
    inst.rpcContext,
    inst.sm,
    inst.smgr,
    params,
    nuraftEventCb,
    cast[pointer](inst),
    skipInitialElection
  )

  nuraftParamsDestroy(params)

  if inst.server.isNil:
    error("Failed to create NuRaft server", "groupId", $groupId)
    if not inst.listener.isNil:
      nuraftMpListenerDestroy(inst.listener)
    if not inst.rpcContext.isNil:
      unregisterValidContext(cast[pointer](inst.rpcContext))
      cancelAllTimersForContext(cast[pointer](inst.rpcContext))
      nuraftMpContextDestroy(inst.rpcContext)
    nuraftSmgrDestroy(inst.smgr)
    nuraftSmDestroy(inst.sm)
    if not inst.persistentStore.isNil:
      inst.persistentStore.close()
    cleanupOnFailure()
    freeInstance(inst)
    return false

  # Wire the raft server pointer into the state manager so it can update
  # the custom_election_quorum_size and custom_commit_quorum_size dynamically
  # when the cluster config changes (add_srv / remove_srv). Without this, the
  # state manager's commit_config() has no raft server reference and cannot
  # propagate quorum changes — leaving the group stuck at the initial quorum=1
  # even after peers join, which produces split-brain in 3-replica clusters.
  nuraftSmgrSetRaftServer(inst.smgr, inst.server)

  # Set the quorum update callback so quorum is dynamically updated
  # when servers are added/removed via add_srv
  nuraftSmgrSetQuorumCb(inst.smgr, cast[pointer](inst), nuraftQuorumUpdateCb)

  # Mark this instance as ready BEFORE delivering buffered messages
  # This ensures new messages arriving during delivery won't be double-buffered
  inst.ready.store(true, moRelease)
  # Deliver any messages that were buffered before this group was ready
  deliverBufferedMessages(c, groupId)

  # Proactively establish TCP connections to all other members.
  # This is CRITICAL for the timer thread: when the election timer fires,
  # sendSync must find existing connections or it will fail immediately
  # (timer thread must not block on TCP connect). By connecting now, from
  # a normal (non-timer) thread, we ensure connections exist before they're
  # needed for election messages.
  for m in sortedMembers:
    if m.nodeId != uint32(c.nodeId):
      let peerCoreId = core_types.NodeID("n" & $m.nodeId)
      var alreadyConnected = false
      if c.transport != nil:
        withLock c.transport.connectionsLock:
          alreadyConnected = c.transport.connections.hasKey(peerCoreId)
        if not alreadyConnected:
          discard c.transport.connectToPeer(peerCoreId, m.host, m.port)

  # NOTE: preferredLeader is disabled for now. The NuRaft priority system
  # is designed for leadership transfer in running clusters, not for initial
  # election control. Setting priorities after server start sends
  # priority_change_request messages which don't trigger elections.
  # Instead, we rely on the normal election process with randomized timeouts.
  # TODO: If preferredLeader is needed, set priority through state manager's
  # initial server config BEFORE creating the raft_server.
  discard preferredLeader # Suppress unused warning

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
  {.cast(gcsafe).}:
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
    # Mark stopped so callbacks and readers know this group is shutting down
    inst.stopped = true

  # Phase 1: Immediately shut down the Raft server.
  # This stops the server from accepting new requests and initiates
  # graceful shutdown of the Raft protocol for this group.
  # The C++ objects remain alive (not destroyed) so any thread holding
  # a reference via acquireGroupInstance can still safely check inst.stopped
  # and return early without accessing freed memory.
  if not inst.server.isNil:
    nuraftServerShutdown(inst.server)

  # Phase 2: Release the table's reference.
  # If no other thread holds a ref, the instance will be destroyed
  # immediately. Otherwise, destruction is deferred until the last
  # holder calls releaseGroupInstance.
  releaseGroupInstance(inst)

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
  # CRITICAL: Release groupsLock BEFORE calling NuRaft C functions.
  # NuRaft C++ code has internal mutexes; holding Nim's groupsLock while
  # calling into NuRaft creates a cross-language deadlock when NuRaft's
  # ASIO thread holds a C++ mutex and tries to acquire groupsLock via
  # a Nim callback simultaneously.
  var inst: NuRaftGroupInstancePtr = nil
  withLock c.groupsLock:
    inst = c.groups.getOrDefault(groupId, nil)
    if inst != nil:
      acquireGroupInstance(inst)
  if inst != nil:
    defer: releaseGroupInstance(inst)
    if inst.stopped: return false
    if inst.server != nil:
      result = nuraftServerIsLeader(inst.server)

proc isWriteReady*(c: NuRaftCoordinator, groupId: GroupID): bool {.raises: [].} =
  ## Check if a group is ready to accept writes.
  ## A group is write-ready if it's the leader AND the server is initialized.
  # CRITICAL: Release groupsLock BEFORE calling NuRaft C functions (see isLeader).
  var inst: NuRaftGroupInstancePtr = nil
  withLock c.groupsLock:
    inst = c.groups.getOrDefault(groupId, nil)
    if inst != nil:
      acquireGroupInstance(inst)
  if inst != nil:
    defer: releaseGroupInstance(inst)
    if inst.stopped: return false
    if inst.server != nil:
      result = nuraftServerIsLeader(inst.server) and nuraftServerIsInitialized(inst.server)

proc getLeader*(c: NuRaftCoordinator, groupId: GroupID): int32 =
  # CRITICAL: Release groupsLock BEFORE calling NuRaft C functions (see isLeader).
  var inst: NuRaftGroupInstancePtr = nil
  withLock c.groupsLock:
    inst = c.groups.getOrDefault(groupId, nil)
    if inst != nil:
      acquireGroupInstance(inst)
  if inst != nil:
    defer: releaseGroupInstance(inst)
    if inst.stopped: return -1
    if inst.server != nil:
      result = nuraftServerGetLeader(inst.server)
  if result == 0:
    result = -1

proc getGroupCount*(c: NuRaftCoordinator): int =
  withLock c.groupsLock:
    result = c.groups.len

proc getLeaderCount*(c: NuRaftCoordinator): int =
  # CRITICAL: Release groupsLock BEFORE calling NuRaft C functions (see isLeader).
  # We collect all instance pointers under the lock, then check leadership outside.
  var instances: seq[NuRaftGroupInstancePtr] = @[]
  withLock c.groupsLock:
    for inst in c.groups.values:
      acquireGroupInstance(inst)
      instances.add(inst)
  # Check leadership outside the lock
  for inst in instances:
    if not inst.stopped and inst.server != nil:
      if nuraftServerIsLeader(inst.server):
        inc result
    releaseGroupInstance(inst)

proc getGroupServerCount*(c: NuRaftCoordinator, groupId: GroupID): int32 =
  ## Get the number of servers in the cluster config for a specific group.
  ## Returns -1 if the group doesn't exist.
  # CRITICAL: Release groupsLock BEFORE calling NuRaft C functions (see isLeader).
  var inst: NuRaftGroupInstancePtr = nil
  withLock c.groupsLock:
    inst = c.groups.getOrDefault(groupId, nil)
    if inst != nil:
      acquireGroupInstance(inst)
  if inst != nil:
    defer: releaseGroupInstance(inst)
    if inst.stopped: return -1
    if inst.server != nil:
      return nuraftServerGetServerCount(inst.server)
  return -1

proc isPeerAlive*(c: NuRaftCoordinator, groupId: GroupID,
    peerId: uint32): bool {.gcsafe.} =
  ## Check if a peer node is alive by querying its last successful response time.
  ## Uses the META group's server as the reference. Returns true if the peer
  ## has responded within the last 10 seconds, or if peer info is unavailable
  ## (assume alive — safer default).
  const ALIVE_THRESHOLD_US = 10_000_000'u64 # 10 seconds in microseconds
                                            # CRITICAL: Release groupsLock BEFORE calling NuRaft C functions (see isLeader).
  var inst: NuRaftGroupInstancePtr = nil
  withLock c.groupsLock:
    inst = c.groups.getOrDefault(META_GROUP_ID, nil)
    if inst != nil:
      acquireGroupInstance(inst)
  if inst == nil:
    return true # Can't check, assume alive
  defer: releaseGroupInstance(inst)
  if inst.stopped or inst.server == nil:
    return true # Can't check, assume alive
  var info: NuRaftPeerInfo
  let rc = nuraftServerGetPeerInfo(inst.server, cast[int32](peerId), addr info)
  if rc != 0 or info.exists == 0:
    return true # Peer not in config, assume alive (may not have been added yet)
  # lastSuccRespUs is the elapsed time since the last successful response.
  # If < 10 seconds, the peer is considered alive.
  result = info.lastSuccRespUs <= ALIVE_THRESHOLD_US

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
    acquireGroupInstance(inst)

  # Ensure release on all exit paths
  defer: releaseGroupInstance(inst)

  if inst.stopped:
    return RaftResult(success: false, error: "Group is shutting down")
  if inst.server.isNil or inst.sm.isNil:
    return RaftResult(success: false, error: "Server not initialized")

  # Check if we're the leader
  let isLdr = nuraftServerIsLeader(inst.server)
  let actualLeader = nuraftServerGetLeader(inst.server)

  # Debug: log leadership state before attempting write
  {.cast(gcsafe).}: {.cast(raises: []).}:
    debug("proposeAndWait: leadership check", {
      "groupId": $groupId,
      "nodeId": $c.nodeId.uint32,
      "isLeader": $isLdr,
      "actualLeader": $actualLeader
    }.toTable)

  {.cast(raises: []).}:
    let payload = serializeWriteBatch(command.writeBatch)

    # Capture current SM commit index BEFORE the write
    let smIdxBefore = nuraftSmLastCommitIndex(inst.sm)

    var logIdx: uint64 = 0
    let rc = nuraftServerAppendEntry(inst.server, payload.strPtr,
        csize_t(payload.len), addr logIdx)

    if rc != 0:
      # Debug: log failure details
      {.cast(gcsafe).}: {.cast(raises: []).}:
        debug("proposeAndWait: append failed", {
          "groupId": $groupId,
          "nodeId": $c.nodeId.uint32,
          "rc": $rc,
          "isLeader": $isLdr,
          "actualLeader": $actualLeader
        }.toTable)

    if rc == 0:
      # Wait for the state machine to advance past the current index.
      # Since logIdx may be 0 (NuRaft API quirk), we wait for SM index to increment.
      # Uses condition variable signaled by nuraftCommitCb instead of sleep(1)
      # polling, reducing latency and CPU waste.
      let cw = cast[ptr CommitWaitData](inst.commitWaitPtr)
      let startTime = coordNowNs().float / 1_000_000.0
      acquire(cw.lock)
      while true:
        if inst.stopped:
          release(cw.lock)
          return RaftResult(success: false,
              error: "Group shutting down during commit wait")
        let smLastIdx = nuraftSmLastCommitIndex(inst.sm)
        # Wait for SM to advance by at least 1 (the write we just proposed)
        if smLastIdx > smIdxBefore:
          break
        let elapsed = (coordNowNs().float / 1_000_000.0) - startTime
        if elapsed > float(timeoutMs):
          release(cw.lock)
          return RaftResult(success: false, error: "Timeout waiting for commit")
        # Wait for commit callback to signal, with 10ms timeout as safety net
        # (in case the signal was missed between the check and the wait)
        discard timedWait(cw.cond, cw.lock, 10)
      release(cw.lock)
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
      createThread(threads[i], proc(a: ThreadArg) {.thread.} =
        {.cast(gcsafe).}:
          let coord = cast[NuRaftCoordinator](a.coord)
          a.resultPtr[] = coord.proposeAndWait(a.groupId, a.command)
      , arg)

    for i in 0 ..< n:
      joinThread(threads[i])

# ============================================================================
# Write Readiness
# ============================================================================

proc waitForWriteReady*(c: NuRaftCoordinator, groupId: GroupID,
    timeoutMs: int = 5000): bool {.raises: [].} =
  ## Wait until the group is ready to accept writes.
  ## This waits for:
  ## 1. Leadership to be acquired
  ## 2. NuRaft server to be initialized
  ## 3. A probe write to succeed (confirms readiness)
  ##
  ## Returns true if write-ready, false on timeout.
  let startTime = coordNowNs().float / 1_000_000.0

  # Phase 1: Wait for leadership and initialization
  while true:
    if c.isWriteReady(groupId):
      break
    let elapsed = (coordNowNs().float / 1_000_000.0) - startTime
    if elapsed > float(timeoutMs * 2 div 3): # Give 2/3 of time for election
      return false
    sleep(5)

  # Phase 2: Probe write to confirm NuRaft can accept writes
  # Use an empty write batch - NuRaft must commit this to be ready
  let probeBatch = newWriteBatch()
  let probeCmd = RaftCommand(kind: ckWrite, writeBatch: probeBatch)
  let remainingMs = max(100, timeoutMs - int((coordNowNs().float /
      1_000_000.0) - startTime))

  # Retry probe writes on NOT_LEADER - this handles the race between
  # isWriteReady returning true and NuRaft actually being ready
  for attempt in 0 ..< 10:
    let result = c.proposeAndWait(groupId, probeCmd, remainingMs)
    if result.success:
      return true
    # NOT_LEADER (-3) means NuRaft isn't ready yet despite isWriteReady check
    if "code -3" in result.error or "NOT_LEADER" in result.error:
      sleep(10)
      continue
    # Other error - give up
    break

  return false

# ============================================================================
# Leadership Transfer
# ============================================================================

proc setPriority*(c: NuRaftCoordinator, groupId: GroupID,
    targetNodeId: group_types.NodeID, priority: int32): bool =
  var inst: NuRaftGroupInstancePtr
  withLock c.groupsLock:
    inst = c.groups.getOrDefault(groupId, nil)
    if inst != nil:
      acquireGroupInstance(inst)

  if inst == nil: return false
  defer: releaseGroupInstance(inst)
  if inst.stopped or inst.server == nil: return false

  let rc = nuraftServerSetPriority(inst.server, cast[int32](
      targetNodeId.uint32), priority)
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
    if inst != nil:
      acquireGroupInstance(inst)

  if inst == nil: return false
  defer: releaseGroupInstance(inst)
  if inst.stopped or inst.server == nil: return false

  # NuRaft leadership transfer strategy:
  # 1. Use priority system to bias election toward target
  # 2. Use graceful handoff (immediate=false) which nominates successor
  #
  # Priority values: 0 = never leader, higher = more likely to win
  # Graceful handoff waits for election timeout before stepping down,
  # giving the successor time to prepare and win.

  let myNodeId = cast[int32](c.nodeId.uint32)
  let targetId = cast[int32](targetNodeId.uint32)

  # Set target node priority to highest (100) - ensures it wins election
  let rc1 = nuraftServerSetPriority(inst.server, targetId, 100)
  if rc1 != 0:
    warn("Failed to set target priority", "groupId", $groupId, "target",
        targetId, "rc", rc1)

  # Set current leader (self) priority to low (1) to prevent re-election
  let rc2 = nuraftServerSetPriority(inst.server, myNodeId, 1)
  if rc2 != 0:
    warn("Failed to set self priority", "groupId", $groupId, "self", myNodeId,
        "rc", rc2)

  # Use graceful handoff (immediate=false):
  # - Leader pauses writes and waits for election timeout
  # - Nominates successor (targetId) which combined with priority boost
  #   should win the election
  nuraftServerYieldLeadership(inst.server, false, targetId)
  return true

proc addServerToGroup*(c: NuRaftCoordinator, groupId: GroupID,
    nodeId: uint32, host: string, port: int): int32 =
  var inst: NuRaftGroupInstancePtr
  withLock c.groupsLock:
    if not c.groups.hasKey(groupId): return -1
    inst = c.groups[groupId]
    acquireGroupInstance(inst)

  defer: releaseGroupInstance(inst)
  if inst.stopped or inst.server == nil: return -1

  # Proactively establish TCP connection to the new peer.
  # This ensures the connection exists before the leader needs to send
  # heartbeats or append_entries to the new server.
  let peerCoreId = core_types.NodeID("n" & $nodeId)
  if c.transport != nil:
    var alreadyConnected = false
    withLock c.transport.connectionsLock:
      alreadyConnected = c.transport.connections.hasKey(peerCoreId)
    if not alreadyConnected:
      discard c.transport.connectToPeer(peerCoreId, host, port)

  # IMPORTANT: Add the peer to peerInfo BEFORE calling add_srv.
  # nuraftConfigChangeCb (called after config commit) also adds to peerInfo,
  # but add_srv needs to replicate the config change to the new server via
  # heartbeats/append_entries BEFORE it's committed. Without peerInfo, the
  # send callback can't look up the peer's address if the TCP connection
  # drops and needs to be re-established.
  withLock c.groupsLock:
    c.peerInfo[nodeId] = (host, port)

  # IMPORTANT: Use "serverId@host:port" format so NuRaft can extract the server ID
  # from the endpoint when creating RPC clients (same format as createAndStartGroup)
  let endpoint = $nodeId & "@" & host & ":" & $port
  let rc = nuraftServerAddSrv(inst.server, cast[int32](nodeId), cstring(endpoint))
  return rc

proc removeServerFromGroup*(c: NuRaftCoordinator, groupId: GroupID,
    nodeId: uint32): int32 =
  var inst: NuRaftGroupInstancePtr
  withLock c.groupsLock:
    if not c.groups.hasKey(groupId): return -1
    inst = c.groups[groupId]
    acquireGroupInstance(inst)

  defer: releaseGroupInstance(inst)
  if inst.stopped or inst.server == nil: return -1
  return nuraftServerRemoveSrv(inst.server, cast[int32](nodeId))

# ============================================================================
# Convenience: register group (for raft_store compatibility)
# ============================================================================

proc registerGroup*(c: NuRaftCoordinator, groupId: GroupID) =
  discard
