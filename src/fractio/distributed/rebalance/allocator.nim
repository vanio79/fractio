# Allocator for Replica Placement
#
# This module implements the allocator decision logic for replica placement.
# It decides where to place replicas based on:
# - Store capacity (disk, CPU, memory)
# - Replica count balance across stores
# - Locality constraints (zone, region)
# - Load distribution

import std/options
import std/tables
import std/locks
import std/sequtils
import std/algorithm

import fractio/distributed/raft/group_types

# ============================================================================
# Constants
# ============================================================================

const
  DEFAULT_REPLICATION_FACTOR* = 3
    ## Default number of replicas per range

  DEFAULT_MAX_REPLICAS_PER_STORE* = 1000
    ## Maximum replicas per store before rebalancing

  DEFAULT_TARGET_SIZE_BYTES* = 64 * 1024 * 1024'i64
    ## Target range size: 64MB

  MIN_RANGE_SIZE_BYTES* = 1 * 1024 * 1024'i64
    ## Minimum range size before merge: 1MB

  MAX_RANGE_SIZE_BYTES* = 256 * 1024 * 1024'i64
    ## Maximum range size before split: 256MB

  OVERLOAD_THRESHOLD* = 1.2
    ## Store is overloaded if > 120% of average load

  UNDERLOAD_THRESHOLD* = 0.8
    ## Store is underloaded if < 80% of average load

# ============================================================================
# Store Statistics
# ============================================================================

type
  StoreStats* = object
    ## Statistics for a single store
    nodeId*: NodeID
    replicaCount*: int
      ## Number of replicas on this store
    leaderCount*: int
      ## Number of ranges where this store is leader
    totalBytes*: int64
      ## Total data size on this store
    capacityBytes*: int64
      ## Disk capacity
    cpuUsage*: float64
      ## CPU utilization (0.0 - 1.0)
    memoryUsage*: float64
      ## Memory utilization (0.0 - 1.0)
    writeKeysPerSecond*: float64
      ## Write throughput
    readKeysPerSecond*: float64
      ## Read throughput
    locality*: seq[tuple[key, value: string]]
      ## Locality tags

proc newStoreStats*(nodeId: NodeID): StoreStats =
  ## Create new store statistics
  result = StoreStats(
    nodeId: nodeId,
    replicaCount: 0,
    leaderCount: 0,
    totalBytes: 0,
    capacityBytes: 0,
    cpuUsage: 0.0,
    memoryUsage: 0.0,
    writeKeysPerSecond: 0.0,
    readKeysPerSecond: 0.0,
    locality: @[]
  )

proc utilization*(stats: StoreStats): float64 =
  ## Calculate overall utilization score (0.0 - 1.0)
  ## Higher score = more utilized
  if stats.capacityBytes == 0:
    return 0.0

  let diskUtil = float64(stats.totalBytes) / float64(stats.capacityBytes)
  let cpuUtil = stats.cpuUsage
  let memUtil = stats.memoryUsage

  # Weighted average: disk 40%, CPU 40%, memory 20%
  result = diskUtil * 0.4 + cpuUtil * 0.4 + memUtil * 0.2

proc loadScore*(stats: StoreStats): float64 =
  ## Calculate load score for rebalancing decisions
  ## Higher score = more loaded
  let utilScore = stats.utilization()
  let leaderScore = float64(stats.leaderCount) / 100.0       # Normalize
  let ioScore = (stats.writeKeysPerSecond + stats.readKeysPerSecond) / 10000.0

  result = utilScore * 0.5 + leaderScore * 0.3 + min(ioScore, 1.0) * 0.2

proc hasLocality*(stats: StoreStats, key, value: string): bool =
  ## Check if store has a specific locality tag
  for loc in stats.locality:
    if loc.key == key and loc.value == value:
      return true
  return false

proc localityMatch*(stats: StoreStats, other: StoreStats): int =
  ## Count matching locality tags between two stores
  result = 0
  for loc in stats.locality:
    if other.hasLocality(loc.key, loc.value):
      inc result

# ============================================================================
# Store Pool
# ============================================================================

type
  StorePool* = ref object
    ## Pool of stores for allocation decisions
    stores*: Table[NodeID, StoreStats]
    lock*: Lock

proc newStorePool*(): StorePool =
  ## Create a new store pool
  new(result)
  result.stores = initTable[NodeID, StoreStats]()
  initLock(result.lock)

proc destroy*(pool: StorePool) =
  ## Clean up resources
  deinitLock(pool.lock)

proc addStore*(pool: StorePool, stats: StoreStats) =
  ## Add or update a store in the pool
  withLock pool.lock:
    pool.stores[stats.nodeId] = stats

proc removeStore*(pool: StorePool, nodeId: NodeID) =
  ## Remove a store from the pool
  withLock pool.lock:
    pool.stores.del(nodeId)

proc getStore*(pool: StorePool, nodeId: NodeID): Option[StoreStats] =
  ## Get store statistics
  withLock pool.lock:
    if pool.stores.contains(nodeId):
      return some(pool.stores[nodeId])
    return none(StoreStats)

proc getAliveStores*(pool: StorePool): seq[StoreStats] =
  ## Get all stores in the pool
  withLock pool.lock:
    for stats in pool.stores.values:
      result.add(stats)

proc averageLoad*(pool: StorePool): float64 =
  ## Calculate average load across all stores
  withLock pool.lock:
    if pool.stores.len == 0:
      return 0.0

    var total = 0.0
    for stats in pool.stores.values:
      total += stats.loadScore()

    result = total / float64(pool.stores.len)

proc totalReplicaCount*(pool: StorePool): int =
  ## Get total replica count across all stores
  withLock pool.lock:
    for stats in pool.stores.values:
      result += stats.replicaCount

proc averageReplicaCount*(pool: StorePool): float64 =
  ## Calculate average replica count per store
  withLock pool.lock:
    if pool.stores.len == 0:
      return 0.0

    var total = 0
    for stats in pool.stores.values:
      total += stats.replicaCount

    result = float64(total) / float64(pool.stores.len)

# ============================================================================
# Allocation Constraints
# ============================================================================

type
  AllocationConstraint* = object
    ## Constraints for replica allocation
    minReplicas*: int
      ## Minimum number of replicas
    maxReplicas*: int
      ## Maximum number of replicas
    requiredLocalities*: seq[tuple[key, value: string]]
      ## Required locality constraints
    forbiddenNodes*: seq[NodeID]
      ## Nodes that cannot be used
    preferredNodes*: seq[NodeID]
      ## Nodes that are preferred (e.g., for lease transfer)

proc defaultConstraints*(): AllocationConstraint =
  ## Create default allocation constraints
  result = AllocationConstraint(
    minReplicas: DEFAULT_REPLICATION_FACTOR,
    maxReplicas: DEFAULT_REPLICATION_FACTOR,
    requiredLocalities: @[],
    forbiddenNodes: @[],
    preferredNodes: @[]
  )

proc withMinReplicas*(c: AllocationConstraint, n: int): AllocationConstraint =
  ## Set minimum replicas
  result = c
  result.minReplicas = n

proc withMaxReplicas*(c: AllocationConstraint, n: int): AllocationConstraint =
  ## Set maximum replicas
  result = c
  result.maxReplicas = n

proc withForbiddenNodes*(c: AllocationConstraint, nodes: seq[
    NodeID]): AllocationConstraint =
  ## Set forbidden nodes
  result = c
  result.forbiddenNodes = nodes

proc withPreferredNodes*(c: AllocationConstraint, nodes: seq[
    NodeID]): AllocationConstraint =
  ## Set preferred nodes
  result = c
  result.preferredNodes = nodes

# ============================================================================
# Allocation Decision
# ============================================================================

type
  AllocationDecisionKind* = enum
    ## Kind of allocation decision
    adkAddReplica
    adkRemoveReplica
    adkTransferLease
    adkNoAction

  AllocationDecision* = object
    ## A single allocation decision
    case kind*: AllocationDecisionKind
    of adkAddReplica:
      addTarget*: NodeID
      addGroupId*: GroupID
    of adkRemoveReplica:
      removeTarget*: NodeID
      removeGroupId*: GroupID
    of adkTransferLease:
      transferFrom*: NodeID
      transferTo*: NodeID
      transferGroupId*: GroupID
    of adkNoAction:
      discard

    priority*: int
      ## Priority of this decision (higher = more urgent)
    reason*: string
      ## Human-readable reason for this decision

proc newAddReplicaDecision*(groupId: GroupID, target: NodeID,
                            priority: int, reason: string): AllocationDecision =
  ## Create an add replica decision
  result = AllocationDecision(
    kind: adkAddReplica,
    addTarget: target,
    addGroupId: groupId,
    priority: priority,
    reason: reason
  )

proc newRemoveReplicaDecision*(groupId: GroupID, target: NodeID,
                               priority: int,
                                   reason: string): AllocationDecision =
  ## Create a remove replica decision
  result = AllocationDecision(
    kind: adkRemoveReplica,
    removeTarget: target,
    removeGroupId: groupId,
    priority: priority,
    reason: reason
  )

proc newTransferLeaseDecision*(groupId: GroupID, source, target: NodeID,
                               priority: int,
                                   reason: string): AllocationDecision =
  ## Create a transfer lease decision
  result = AllocationDecision(
    kind: adkTransferLease,
    transferFrom: source,
    transferTo: target,
    transferGroupId: groupId,
    priority: priority,
    reason: reason
  )

proc newNoActionDecision*(): AllocationDecision =
  ## Create a no-action decision
  result = AllocationDecision(
    kind: adkNoAction,
    priority: 0,
    reason: "No action needed"
  )

# ============================================================================
# Allocator
# ============================================================================

type
  Allocator* = ref object
    ## Makes allocation decisions for replicas
    pool*: StorePool
    replicationFactor*: int
    maxReplicasPerStore*: int
    lock*: Lock

proc newAllocator*(pool: StorePool,
                   replicationFactor: int = DEFAULT_REPLICATION_FACTOR): Allocator =
  ## Create a new allocator
  new(result)
  result.pool = pool
  result.replicationFactor = replicationFactor
  result.maxReplicasPerStore = DEFAULT_MAX_REPLICAS_PER_STORE
  initLock(result.lock)

proc destroy*(alloc: Allocator) =
  ## Clean up resources
  deinitLock(alloc.lock)

proc selectStoreForReplica*(alloc: Allocator,
                            existingReplicas: seq[NodeID],
                            constraints: AllocationConstraint): Option[NodeID] =
  ## Select the best store for a new replica
  ## Considers load balance, locality, and constraints

  let stores = alloc.pool.getAliveStores()
  if stores.len == 0:
    return none(NodeID)

  # Filter out forbidden nodes and existing replicas
  var candidates: seq[StoreStats] = @[]
  for stats in stores:
    if stats.nodeId in constraints.forbiddenNodes:
      continue
    if stats.nodeId in existingReplicas:
      continue
    candidates.add(stats)

  if candidates.len == 0:
    return none(NodeID)

  # Check preferred nodes first
  for prefNode in constraints.preferredNodes:
    for stats in candidates:
      if stats.nodeId == prefNode:
        return some(stats.nodeId)

  # Sort by load score (ascending - prefer less loaded)
  candidates.sort(proc(a, b: StoreStats): int =
    cmp(a.loadScore(), b.loadScore()))

  # Return the least loaded store
  return some(candidates[0].nodeId)

proc selectStoreForRemoval*(alloc: Allocator,
                            existingReplicas: seq[NodeID]): Option[NodeID] =
  ## Select the best replica to remove
  ## Prefers to remove from overloaded stores

  let stores = alloc.pool.getAliveStores()
  if existingReplicas.len == 0:
    return none(NodeID)

  # Get stats for existing replicas
  var replicaStats: seq[StoreStats] = @[]
  for nodeId in existingReplicas:
    let statsOpt = alloc.pool.getStore(nodeId)
    if statsOpt.isSome:
      replicaStats.add(statsOpt.get)

  if replicaStats.len == 0:
    return some(existingReplicas[0])

  # Sort by load score (descending - prefer removing from most loaded)
  replicaStats.sort(proc(a, b: StoreStats): int =
    cmp(b.loadScore(), a.loadScore()))

  return some(replicaStats[0].nodeId)

proc selectLeaseholder*(alloc: Allocator,
                        replicas: seq[ReplicaDescriptor],
                        currentLeaseholder: NodeID): Option[NodeID] =
  ## Select the best replica to hold the lease
  ## Prefers underloaded stores

  if replicas.len == 0:
    return none(NodeID)

  # Get stats for all replicas
  var replicaStats: seq[tuple[rep: ReplicaDescriptor, stats: StoreStats]] = @[]
  for rep in replicas:
    let statsOpt = alloc.pool.getStore(rep.nodeId)
    if statsOpt.isSome:
      replicaStats.add((rep, statsOpt.get))

  if replicaStats.len == 0:
    return some(replicas[0].nodeId)

  # Sort by load score (ascending - prefer less loaded)
  replicaStats.sort(proc(a, b: tuple[rep: ReplicaDescriptor,
      stats: StoreStats]): int =
    cmp(a.stats.loadScore(), b.stats.loadScore()))

  # Prefer current leaseholder if it's not overloaded
  let avgLoad = alloc.pool.averageLoad()
  for (rep, stats) in replicaStats:
    if rep.nodeId == currentLeaseholder:
      if stats.loadScore() < avgLoad * OVERLOAD_THRESHOLD:
        return some(currentLeaseholder)
      break

  # Return the least loaded store
  return some(replicaStats[0].rep.nodeId)

proc shouldRebalance*(alloc: Allocator, groupId: GroupID,
                      replicas: seq[ReplicaDescriptor],
                      leaseholder: NodeID): seq[AllocationDecision] =
  ## Check if a range needs rebalancing
  ## Returns a list of decisions to make

  result = @[]

  # Check replica count
  if replicas.len < alloc.replicationFactor:
    # Need more replicas
    let existing = replicas.mapIt(it.nodeId)
    let constraints = defaultConstraints()
    let target = alloc.selectStoreForReplica(existing, constraints)

    if target.isSome:
      result.add(newAddReplicaDecision(
        groupId, target.get, 10,
        "Range needs more replicas (" & $replicas.len & "/" &
        $alloc.replicationFactor & ")"
      ))

  elif replicas.len > alloc.replicationFactor:
    # Too many replicas
    let existing = replicas.mapIt(it.nodeId)
    let target = alloc.selectStoreForRemoval(existing)

    if target.isSome:
      result.add(newRemoveReplicaDecision(
        groupId, target.get, 5,
        "Range has too many replicas (" & $replicas.len & "/" &
        $alloc.replicationFactor & ")"
      ))

  # Check leaseholder balance
  let stores = alloc.pool.getAliveStores()
  if stores.len > 0:
    let avgLoad = alloc.pool.averageLoad()
    let leaseholderStats = alloc.pool.getStore(leaseholder)

    if leaseholderStats.isSome:
      let stats = leaseholderStats.get
      if stats.loadScore() > avgLoad * OVERLOAD_THRESHOLD:
        # Leaseholder is overloaded, consider transfer
        let newLeaseholder = alloc.selectLeaseholder(replicas, leaseholder)

        if newLeaseholder.isSome and newLeaseholder.get != leaseholder:
          result.add(newTransferLeaseDecision(
            groupId, leaseholder, newLeaseholder.get, 3,
            "Leaseholder overloaded (load: " & $stats.loadScore() & ", avg: " &
                $avgLoad & ")"
          ))

proc allocateNewGroup*(alloc: Allocator,
                       constraints: AllocationConstraint): seq[NodeID] =
  ## Allocate replicas for a new range
  ## Returns the list of nodes to place replicas on

  result = @[]
  var existing: seq[NodeID] = @[]

  for i in 0..<constraints.minReplicas:
    let target = alloc.selectStoreForReplica(existing, constraints)
    if target.isSome:
      result.add(target.get)
      existing.add(target.get)
    else:
      break

proc isStoreOverloaded*(alloc: Allocator, nodeId: NodeID): bool =
  ## Check if a store is overloaded
  let statsOpt = alloc.pool.getStore(nodeId)
  if statsOpt.isNone:
    return false

  let stats = statsOpt.get
  let avgLoad = alloc.pool.averageLoad()

  return stats.loadScore() > avgLoad * OVERLOAD_THRESHOLD

proc isStoreUnderloaded*(alloc: Allocator, nodeId: NodeID): bool =
  ## Check if a store is underloaded
  let statsOpt = alloc.pool.getStore(nodeId)
  if statsOpt.isNone:
    return false

  let stats = statsOpt.get
  let avgLoad = alloc.pool.averageLoad()

  return stats.loadScore() < avgLoad * UNDERLOAD_THRESHOLD

proc getOverloadedStores*(alloc: Allocator): seq[NodeID] =
  ## Get all overloaded stores
  let stores = alloc.pool.getAliveStores()
  let avgLoad = alloc.pool.averageLoad()

  for stats in stores:
    if stats.loadScore() > avgLoad * OVERLOAD_THRESHOLD:
      result.add(stats.nodeId)

proc getUnderloadedStores*(alloc: Allocator): seq[NodeID] =
  ## Get all underloaded stores
  let stores = alloc.pool.getAliveStores()
  let avgLoad = alloc.pool.averageLoad()

  for stats in stores:
    if stats.loadScore() < avgLoad * UNDERLOAD_THRESHOLD:
      result.add(stats.nodeId)
