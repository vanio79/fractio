# Store Liveness for Leader Leases
#
# This module implements store-level failure detection for leader leases.
# Based on CockroachDB v25.2+ "Leader Leases" design.
#
# Key concept: A Raft leader can only hold a lease if its store liveness
# is "supported" by a quorum of stores in the Raft group. This eliminates
# the single point of failure of the node liveness group.

import std/atomics
import std/locks
import std/tables
import std/options
import std/sets

import fractio/distributed/raft/group_types
import fractio/distributed/sharedtimer/timeprovider
from fractio/core/types import localTimeNs
import fractio/utils/logging

# ============================================================================
# Constants
# ============================================================================

const
  DEFAULT_HEARTBEAT_INTERVAL_NS* = 1_000_000_000'i64  # 1 second
  DEFAULT_SUPPORT_EXPIRATION_NS* = 10_000_000_000'i64 # 10 seconds
  LIVENESS_KEY_PREFIX* = "/sys/liveness/"

# ============================================================================
# Liveness State
# ============================================================================

type
  LivenessState* = object
    ## State of a single store's liveness
    nodeId*: NodeID
    lastHeartbeat*: int64  # Nanoseconds since epoch
    supportedUntil*: int64 # Nanoseconds since epoch
    epoch*: uint64         # Incremented on restart

  StoreLivenessMode* = enum
    ## Mode of store liveness operation
    slmFollower  # Not participating in liveness (passive)
    slmCandidate # Trying to become liveness leader
    slmLeader    # Liveness leader for a range

  SupportState* = enum
    ## Support state for a store
    ssUnsupported # Not supported
    ssSupported   # Supported until expiration
    ssExpired     # Support has expired

            # ============================================================================
                  # Store Liveness
            # ============================================================================

type
  StoreLiveness* = ref object
    ## Store-level failure detection for leader leases.
    ## Each node tracks liveness of other stores and provides
    ## "support" to stores that are alive.

    nodeId*: NodeID
    epoch*: Atomic[uint64]

    # Liveness state of all known stores
    stores*: Table[NodeID, LivenessState]
    storesLock*: Lock

    # Support state - which stores we are supporting
    supporting*: HashSet[NodeID]
    supportingLock*: Lock

    # Configuration
    heartbeatIntervalNs*: int64
    supportExpirationNs*: int64

    # Running state
    running*: Atomic[bool]
    lastHeartbeatSent*: Atomic[int64]

    # Cluster time source
    timeProvider*: TimeProvider

  LivenessMessage* = object
    ## Message exchanged between stores for liveness
    nodeId*: NodeID
    epoch*: uint64
    timestamp*: int64
    messageType*: LivenessMessageType

  LivenessMessageType* = enum
    lmtHeartbeat         # Regular heartbeat
    lmtHeartbeatResponse # Response to heartbeat
    lmtSupport           # Grant support
    lmtWithdraw          # Withdraw support
    lmtProbe             # Probe for liveness

           # ============================================================================
                         # Store Liveness Operations
           # ============================================================================

proc newStoreLiveness*(nodeId: NodeID,
                        heartbeatIntervalNs = DEFAULT_HEARTBEAT_INTERVAL_NS,
                        supportExpirationNs = DEFAULT_SUPPORT_EXPIRATION_NS,
                        timeProvider: TimeProvider = nil): StoreLiveness =
  ## Create a new store liveness manager
  new(result)
  result.nodeId = nodeId
  result.epoch.store(1) # Start with epoch 1
  result.heartbeatIntervalNs = heartbeatIntervalNs
  result.supportExpirationNs = supportExpirationNs
  result.stores = initTable[NodeID, LivenessState]()
  result.supporting = initHashSet[NodeID]()
  initLock(result.storesLock)
  initLock(result.supportingLock)
  result.running.store(false)
  result.lastHeartbeatSent.store(0)
  result.timeProvider = timeProvider

proc nowNs(sl: StoreLiveness): int64 {.inline.} =
  ## Get current nanoseconds using timeProvider when available, falls back to localTimeNs.
  if not sl.timeProvider.isNil:
    try: sl.timeProvider.now()
    except Exception: localTimeNs()
  else:
    localTimeNs()

proc close*(sl: StoreLiveness) =
  ## Clean up resources
  deinitLock(sl.storesLock)
  deinitLock(sl.supportingLock)

# ============================================================================
# Heartbeat Processing
# ============================================================================

proc processHeartbeat*(sl: StoreLiveness,
    msg: LivenessMessage): LivenessMessage =
  ## Process an incoming heartbeat message
  ## Returns a heartbeat response

  withLock sl.storesLock:
    var state = sl.stores.getOrDefault(msg.nodeId)
    state.nodeId = msg.nodeId
    state.lastHeartbeat = msg.timestamp
    state.epoch = msg.epoch

    # Update supported until based on our configuration
    let now = sl.nowNs()
    state.supportedUntil = now + sl.supportExpirationNs

    sl.stores[msg.nodeId] = state

  var fields = initTable[string, string]()
  fields["from"] = $msg.nodeId
  fields["epoch"] = $msg.epoch
  debug("Processed heartbeat", fields)

  # Send response
  result = LivenessMessage(
    nodeId: sl.nodeId,
    epoch: sl.epoch.load(),
    timestamp: sl.nowNs(),
    messageType: lmtHeartbeatResponse
  )

proc recordHeartbeat*(sl: StoreLiveness, nodeId: NodeID, epoch: uint64) =
  ## Record a heartbeat from another store
  let now = sl.nowNs()

  withLock sl.storesLock:
    var state = sl.stores.getOrDefault(nodeId)
    state.nodeId = nodeId
    state.lastHeartbeat = now
    state.epoch = epoch
    state.supportedUntil = now + sl.supportExpirationNs
    sl.stores[nodeId] = state

# ============================================================================
# Support Management
# ============================================================================

proc grantSupport*(sl: StoreLiveness, nodeId: NodeID) =
  ## Grant support to a store
  withLock sl.supportingLock:
    sl.supporting.incl(nodeId)

  var fields = initTable[string, string]()
  fields["nodeId"] = $nodeId
  debug("Granted support", fields)

proc withdrawSupport*(sl: StoreLiveness, nodeId: NodeID) =
  ## Withdraw support from a store
  withLock sl.supportingLock:
    sl.supporting.excl(nodeId)

  var fields = initTable[string, string]()
  fields["nodeId"] = $nodeId
  debug("Withdrawn support", fields)

proc isSupporting*(sl: StoreLiveness, nodeId: NodeID): bool =
  ## Check if we are supporting a store
  withLock sl.supportingLock:
    result = nodeId in sl.supporting

proc getSupportedStores*(sl: StoreLiveness): HashSet[NodeID] =
  ## Get all stores we are supporting
  withLock sl.supportingLock:
    result = sl.supporting

# ============================================================================
# Liveness Queries
# ============================================================================

proc isAlive*(sl: StoreLiveness, nodeId: NodeID): bool =
  ## Check if a store is considered alive
  let now = sl.nowNs()

  withLock sl.storesLock:
    if sl.stores.hasKey(nodeId):
      let state = sl.stores[nodeId]
      return now < state.supportedUntil
  return false

proc getLivenessState*(sl: StoreLiveness, nodeId: NodeID): Option[
    LivenessState] =
  ## Get the liveness state for a store
  withLock sl.storesLock:
    if sl.stores.hasKey(nodeId):
      result = some(sl.stores[nodeId])

proc getSupportState*(sl: StoreLiveness, nodeId: NodeID): SupportState =
  ## Get the support state for a store
  let now = sl.nowNs()

  withLock sl.storesLock:
    if not sl.stores.hasKey(nodeId):
      return ssUnsupported

    let state = sl.stores[nodeId]
    if now >= state.supportedUntil:
      return ssExpired
    return ssSupported

proc timeUntilExpiration*(sl: StoreLiveness, nodeId: NodeID): int64 =
  ## Get time until liveness expires for a store (nanoseconds)
  let now = sl.nowNs()

  withLock sl.storesLock:
    if sl.stores.hasKey(nodeId):
      let state = sl.stores[nodeId]
      result = state.supportedUntil - now
      if result < 0:
        result = 0
    else:
      result = 0

# ============================================================================
# Quorum Support
# ============================================================================

proc countSupported*(sl: StoreLiveness, nodes: seq[NodeID]): int =
  ## Count how many nodes in the list are supported (alive)
  for nodeId in nodes:
    if sl.isAlive(nodeId):
      inc result

proc hasQuorumSupport*(sl: StoreLiveness, nodes: seq[NodeID]): bool =
  ## Check if a quorum of nodes are supported (alive)
  let quorumSize = (nodes.len div 2) + 1
  let supported = sl.countSupported(nodes)
  result = supported >= quorumSize

proc canAcquireLease*(sl: StoreLiveness, voters: seq[ReplicaDescriptor]): bool =
  ## Check if we can acquire a lease for a range.
  ## We need support from a quorum of voters.
  var voterNodes: seq[NodeID]
  for rep in voters:
    if rep.replicaType == rtVoter:
      voterNodes.add(rep.nodeId)

  result = sl.hasQuorumSupport(voterNodes)

# ============================================================================
# Heartbeat Generation
# ============================================================================

proc createHeartbeat*(sl: StoreLiveness): LivenessMessage =
  ## Create a heartbeat message to send
  result = LivenessMessage(
    nodeId: sl.nodeId,
    epoch: sl.epoch.load(),
    timestamp: sl.nowNs(),
    messageType: lmtHeartbeat
  )
  sl.lastHeartbeatSent.store(result.timestamp)

proc shouldSendHeartbeat*(sl: StoreLiveness): bool =
  ## Check if we should send a heartbeat
  let now = sl.nowNs()
  let lastSent = sl.lastHeartbeatSent.load()
  result = (now - lastSent) >= sl.heartbeatIntervalNs

# ============================================================================
# Epoch Management
# ============================================================================

proc incrementEpoch*(sl: StoreLiveness) =
  ## Increment epoch (called on restart)
  discard sl.epoch.fetchAdd(1)

  var fields = initTable[string, string]()
  fields["nodeId"] = $sl.nodeId
  fields["newEpoch"] = $sl.epoch.load()
  info("Incremented liveness epoch", fields)

proc getEpoch*(sl: StoreLiveness): uint64 =
  ## Get current epoch
  sl.epoch.load()

# ============================================================================
# Store Registration
# ============================================================================

proc registerStore*(sl: StoreLiveness, nodeId: NodeID, epoch: uint64 = 0) =
  ## Register a new store
  let now = sl.nowNs()

  withLock sl.storesLock:
    var state = LivenessState(
      nodeId: nodeId,
      lastHeartbeat: now,
      supportedUntil: now + sl.supportExpirationNs,
      epoch: epoch
    )
    sl.stores[nodeId] = state

  var fields = initTable[string, string]()
  fields["nodeId"] = $nodeId
  debug("Registered store", fields)

proc unregisterStore*(sl: StoreLiveness, nodeId: NodeID) =
  ## Unregister a store
  withLock sl.storesLock:
    sl.stores.del(nodeId)

  withLock sl.supportingLock:
    sl.supporting.excl(nodeId)

  var fields = initTable[string, string]()
  fields["nodeId"] = $nodeId
  debug("Unregistered store", fields)

# ============================================================================
# Diagnostics
# ============================================================================

proc getAliveStores*(sl: StoreLiveness): seq[NodeID] =
  ## Get all stores that are currently alive
  let now = sl.nowNs()
  withLock sl.storesLock:
    for nodeId, state in sl.stores:
      if now < state.supportedUntil:
        result.add(nodeId)

proc getStats*(sl: StoreLiveness): tuple[total: int, alive: int,
    supporting: int] =
  ## Get liveness statistics
  let now = sl.nowNs()
  acquire(sl.storesLock)
  result.total = sl.stores.len
  for state in sl.stores.values:
    if now < state.supportedUntil:
      inc result.alive
  release(sl.storesLock)

  acquire(sl.supportingLock)
  result.supporting = sl.supporting.len
  release(sl.supportingLock)
