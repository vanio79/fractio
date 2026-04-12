# Unit tests for fractio/distributed/rebalance/allocator.nim
# Tests StoreStats, StorePool, AllocationConstraint, AllocationDecision, and Allocator

import std/[unittest, tables, options, sequtils]
import fractio/core/types except NodeID
import fractio/distributed/raft/group_types
import fractio/distributed/rebalance/allocator

suite "StoreStats":

  test "newStoreStats creates empty stats":
    let stats = newStoreStats(NodeID(1))
    check stats.nodeId == NodeID(1)
    check stats.replicaCount == 0
    check stats.leaderCount == 0
    check stats.totalBytes == 0
    check stats.capacityBytes == 0
    check stats.cpuUsage == 0.0
    check stats.memoryUsage == 0.0
    check stats.writeKeysPerSecond == 0.0
    check stats.readKeysPerSecond == 0.0
    check stats.locality.len == 0

  test "utilization with zero capacity":
    let stats = newStoreStats(NodeID(1))
    check stats.utilization() == 0.0

  test "utilization with capacity":
    var stats = newStoreStats(NodeID(1))
    stats.capacityBytes = 1000'i64
    stats.totalBytes = 500'i64
    stats.cpuUsage = 0.5
    stats.memoryUsage = 0.5
    # diskUtil = 0.5, cpuUtil = 0.5, memUtil = 0.5
    # result = 0.5 * 0.4 + 0.5 * 0.4 + 0.5 * 0.2 = 0.5
    check stats.utilization() == 0.5

  test "utilization calculation weights":
    var stats = newStoreStats(NodeID(1))
    stats.capacityBytes = 1000'i64
    stats.totalBytes = 1000'i64 # diskUtil = 1.0
    stats.cpuUsage = 0.0
    stats.memoryUsage = 0.0
    # result = 1.0 * 0.4 + 0.0 * 0.4 + 0.0 * 0.2 = 0.4
    check stats.utilization() == 0.4

  test "loadScore calculation":
    var stats = newStoreStats(NodeID(1))
    stats.capacityBytes = 1000'i64
    stats.totalBytes = 500'i64
    stats.leaderCount = 50
    stats.writeKeysPerSecond = 5000.0
    stats.readKeysPerSecond = 5000.0
    let score = stats.loadScore()
    check score > 0.0
    check score < 2.0 # Should be reasonable

  test "hasLocality true":
    var stats = newStoreStats(NodeID(1))
    stats.locality = @[("zone", "a"), ("region", "east")]
    check stats.hasLocality("zone", "a")
    check stats.hasLocality("region", "east")

  test "hasLocality false":
    var stats = newStoreStats(NodeID(1))
    stats.locality = @[("zone", "a")]
    check not stats.hasLocality("zone", "b")
    check not stats.hasLocality("region", "east")

  test "hasLocality empty":
    let stats = newStoreStats(NodeID(1))
    check not stats.hasLocality("zone", "a")

  test "localityMatch count":
    var stats1 = newStoreStats(NodeID(1))
    stats1.locality = @[("zone", "a"), ("region", "east"), ("rack", "r1")]

    var stats2 = newStoreStats(NodeID(2))
    stats2.locality = @[("zone", "a"), ("region", "west")]

    check stats1.localityMatch(stats2) == 1 # Only zone matches

  test "localityMatch all match":
    var stats1 = newStoreStats(NodeID(1))
    stats1.locality = @[("zone", "a"), ("region", "east")]

    var stats2 = newStoreStats(NodeID(2))
    stats2.locality = @[("zone", "a"), ("region", "east")]

    check stats1.localityMatch(stats2) == 2

  test "localityMatch no match":
    var stats1 = newStoreStats(NodeID(1))
    stats1.locality = @[("zone", "a")]

    var stats2 = newStoreStats(NodeID(2))
    stats2.locality = @[("zone", "b")]

    check stats1.localityMatch(stats2) == 0

suite "StorePool":

  test "newStorePool creates empty pool":
    let pool = newStorePool()
    check pool.stores.len == 0
    pool.destroy()

  test "addStore":
    let pool = newStorePool()
    let stats = newStoreStats(NodeID(1))
    pool.addStore(stats)
    check pool.stores.len == 1
    pool.destroy()

  test "addStore multiple":
    let pool = newStorePool()
    pool.addStore(newStoreStats(NodeID(1)))
    pool.addStore(newStoreStats(NodeID(2)))
    pool.addStore(newStoreStats(NodeID(3)))
    check pool.stores.len == 3
    pool.destroy()

  test "addStore update existing":
    let pool = newStorePool()
    var stats1 = newStoreStats(NodeID(1))
    stats1.replicaCount = 10
    pool.addStore(stats1)

    var stats2 = newStoreStats(NodeID(1))
    stats2.replicaCount = 20
    pool.addStore(stats2)

    check pool.stores.len == 1
    let retrieved = pool.getStore(NodeID(1))
    check retrieved.isSome
    check retrieved.get.replicaCount == 20
    pool.destroy()

  test "removeStore":
    let pool = newStorePool()
    pool.addStore(newStoreStats(NodeID(1)))
    pool.addStore(newStoreStats(NodeID(2)))
    pool.removeStore(NodeID(1))
    check pool.stores.len == 1
    check pool.getStore(NodeID(1)).isNone
    check pool.getStore(NodeID(2)).isSome
    pool.destroy()

  test "getStore existing":
    let pool = newStorePool()
    let stats = newStoreStats(NodeID(1))
    pool.addStore(stats)
    let result = pool.getStore(NodeID(1))
    check result.isSome
    check result.get.nodeId == NodeID(1)
    pool.destroy()

  test "getStore non-existing":
    let pool = newStorePool()
    let result = pool.getStore(NodeID(99))
    check result.isNone
    pool.destroy()

  test "getAliveStores":
    let pool = newStorePool()
    pool.addStore(newStoreStats(NodeID(1)))
    pool.addStore(newStoreStats(NodeID(2)))
    let stores = pool.getAliveStores()
    check stores.len == 2
    pool.destroy()

  test "getAliveStores empty":
    let pool = newStorePool()
    let stores = pool.getAliveStores()
    check stores.len == 0
    pool.destroy()

  test "averageLoad empty pool":
    let pool = newStorePool()
    check pool.averageLoad() == 0.0
    pool.destroy()

  test "averageLoad with stores":
    let pool = newStorePool()
    var stats1 = newStoreStats(NodeID(1))
    stats1.capacityBytes = 1000'i64
    stats1.totalBytes = 500'i64
    pool.addStore(stats1)

    var stats2 = newStoreStats(NodeID(2))
    stats2.capacityBytes = 1000'i64
    stats2.totalBytes = 500'i64
    pool.addStore(stats2)

    check pool.averageLoad() > 0.0
    pool.destroy()

  test "totalReplicaCount":
    let pool = newStorePool()
    var stats1 = newStoreStats(NodeID(1))
    stats1.replicaCount = 10
    pool.addStore(stats1)

    var stats2 = newStoreStats(NodeID(2))
    stats2.replicaCount = 15
    pool.addStore(stats2)

    check pool.totalReplicaCount() == 25
    pool.destroy()

  test "averageReplicaCount empty":
    let pool = newStorePool()
    check pool.averageReplicaCount() == 0.0
    pool.destroy()

  test "averageReplicaCount with stores":
    let pool = newStorePool()
    var stats1 = newStoreStats(NodeID(1))
    stats1.replicaCount = 10
    pool.addStore(stats1)

    var stats2 = newStoreStats(NodeID(2))
    stats2.replicaCount = 20
    pool.addStore(stats2)

    check pool.averageReplicaCount() == 15.0
    pool.destroy()

suite "AllocationConstraint":

  test "defaultConstraints":
    let c = defaultConstraints()
    check c.minReplicas == DEFAULT_REPLICATION_FACTOR
    check c.maxReplicas == DEFAULT_REPLICATION_FACTOR
    check c.requiredLocalities.len == 0
    check c.forbiddenNodes.len == 0
    check c.preferredNodes.len == 0

  test "withMinReplicas":
    let c = defaultConstraints().withMinReplicas(5)
    check c.minReplicas == 5

  test "withMaxReplicas":
    let c = defaultConstraints().withMaxReplicas(7)
    check c.maxReplicas == 7

  test "withForbiddenNodes":
    let c = defaultConstraints().withForbiddenNodes(@[NodeID(1), NodeID(2)])
    check c.forbiddenNodes.len == 2
    check NodeID(1) in c.forbiddenNodes
    check NodeID(2) in c.forbiddenNodes

  test "withPreferredNodes":
    let c = defaultConstraints().withPreferredNodes(@[NodeID(3), NodeID(4)])
    check c.preferredNodes.len == 2
    check NodeID(3) in c.preferredNodes
    check NodeID(4) in c.preferredNodes

suite "AllocationDecision":

  test "newAddReplicaDecision":
    let groupId = genGroupID()
    let decision = newAddReplicaDecision(groupId, NodeID(5), 10, "test reason")
    check decision.kind == adkAddReplica
    check decision.addTarget == NodeID(5)
    check decision.addGroupId == groupId
    check decision.priority == 10
    check decision.reason == "test reason"

  test "newRemoveReplicaDecision":
    let groupId = genGroupID()
    let decision = newRemoveReplicaDecision(groupId, NodeID(3), 5, "remove reason")
    check decision.kind == adkRemoveReplica
    check decision.removeTarget == NodeID(3)
    check decision.removeGroupId == groupId
    check decision.priority == 5
    check decision.reason == "remove reason"

  test "newTransferLeaseDecision":
    let groupId = genGroupID()
    let decision = newTransferLeaseDecision(groupId, NodeID(1), NodeID(2), 3, "transfer reason")
    check decision.kind == adkTransferLease
    check decision.transferFrom == NodeID(1)
    check decision.transferTo == NodeID(2)
    check decision.transferGroupId == groupId
    check decision.priority == 3
    check decision.reason == "transfer reason"

  test "newNoActionDecision":
    let decision = newNoActionDecision()
    check decision.kind == adkNoAction
    check decision.priority == 0
    check decision.reason == "No action needed"

suite "Allocator":

  test "newAllocator":
    let pool = newStorePool()
    let alloc = newAllocator(pool)
    check alloc.pool == pool
    check alloc.replicationFactor == DEFAULT_REPLICATION_FACTOR
    check alloc.maxReplicasPerStore == DEFAULT_MAX_REPLICAS_PER_STORE
    alloc.destroy()
    pool.destroy()

  test "newAllocator custom replication factor":
    let pool = newStorePool()
    let alloc = newAllocator(pool, 5)
    check alloc.replicationFactor == 5
    alloc.destroy()
    pool.destroy()

  test "selectStoreForReplica empty pool":
    let pool = newStorePool()
    let alloc = newAllocator(pool)
    let result = alloc.selectStoreForReplica(@[], defaultConstraints())
    check result.isNone
    alloc.destroy()
    pool.destroy()

  test "selectStoreForReplica single store":
    let pool = newStorePool()
    pool.addStore(newStoreStats(NodeID(1)))
    let alloc = newAllocator(pool)
    let result = alloc.selectStoreForReplica(@[], defaultConstraints())
    check result.isSome
    check result.get == NodeID(1)
    alloc.destroy()
    pool.destroy()

  test "selectStoreForReplica prefers least loaded":
    let pool = newStorePool()
    var stats1 = newStoreStats(NodeID(1))
    stats1.capacityBytes = 1000'i64
    stats1.totalBytes = 900'i64 # 90% utilized
    pool.addStore(stats1)

    var stats2 = newStoreStats(NodeID(2))
    stats2.capacityBytes = 1000'i64
    stats2.totalBytes = 100'i64 # 10% utilized
    pool.addStore(stats2)

    let alloc = newAllocator(pool)
    let result = alloc.selectStoreForReplica(@[], defaultConstraints())
    check result.isSome
    check result.get == NodeID(2) # Less loaded
    alloc.destroy()
    pool.destroy()

  test "selectStoreForReplica excludes forbidden nodes":
    let pool = newStorePool()
    pool.addStore(newStoreStats(NodeID(1)))
    pool.addStore(newStoreStats(NodeID(2)))

    let alloc = newAllocator(pool)
    let constraints = defaultConstraints().withForbiddenNodes(@[NodeID(1)])
    let result = alloc.selectStoreForReplica(@[], constraints)
    check result.isSome
    check result.get == NodeID(2)
    alloc.destroy()
    pool.destroy()

  test "selectStoreForReplica excludes existing replicas":
    let pool = newStorePool()
    pool.addStore(newStoreStats(NodeID(1)))
    pool.addStore(newStoreStats(NodeID(2)))

    let alloc = newAllocator(pool)
    let result = alloc.selectStoreForReplica(@[NodeID(1)], defaultConstraints())
    check result.isSome
    check result.get == NodeID(2)
    alloc.destroy()
    pool.destroy()

  test "selectStoreForReplica prefers preferred nodes":
    let pool = newStorePool()
    var stats1 = newStoreStats(NodeID(1))
    stats1.capacityBytes = 1000'i64
    stats1.totalBytes = 100'i64 # Less loaded
    pool.addStore(stats1)

    var stats2 = newStoreStats(NodeID(2))
    stats2.capacityBytes = 1000'i64
    stats2.totalBytes = 900'i64 # More loaded
    pool.addStore(stats2)

    let alloc = newAllocator(pool)
    let constraints = defaultConstraints().withPreferredNodes(@[NodeID(2)])
    let result = alloc.selectStoreForReplica(@[], constraints)
    check result.isSome
    check result.get == NodeID(2) # Preferred over less loaded
    alloc.destroy()
    pool.destroy()

  test "selectStoreForReplica no candidates":
    let pool = newStorePool()
    pool.addStore(newStoreStats(NodeID(1)))

    let alloc = newAllocator(pool)
    let result = alloc.selectStoreForReplica(@[NodeID(1)], defaultConstraints())
    check result.isNone # Only store is already a replica
    alloc.destroy()
    pool.destroy()

  test "selectStoreForRemoval empty":
    let pool = newStorePool()
    let alloc = newAllocator(pool)
    let result = alloc.selectStoreForRemoval(@[])
    check result.isNone
    alloc.destroy()
    pool.destroy()

  test "selectStoreForRemoval single replica":
    let pool = newStorePool()
    pool.addStore(newStoreStats(NodeID(1)))

    let alloc = newAllocator(pool)
    let result = alloc.selectStoreForRemoval(@[NodeID(1)])
    check result.isSome
    check result.get == NodeID(1)
    alloc.destroy()
    pool.destroy()

  test "selectStoreForRemoval prefers most loaded":
    let pool = newStorePool()
    var stats1 = newStoreStats(NodeID(1))
    stats1.capacityBytes = 1000'i64
    stats1.totalBytes = 100'i64 # Less loaded
    pool.addStore(stats1)

    var stats2 = newStoreStats(NodeID(2))
    stats2.capacityBytes = 1000'i64
    stats2.totalBytes = 900'i64 # More loaded
    pool.addStore(stats2)

    let alloc = newAllocator(pool)
    let result = alloc.selectStoreForRemoval(@[NodeID(1), NodeID(2)])
    check result.isSome
    check result.get == NodeID(2) # Most loaded
    alloc.destroy()
    pool.destroy()

  test "selectStoreForRemoval no stats available":
    let pool = newStorePool()
    let alloc = newAllocator(pool)
    let result = alloc.selectStoreForRemoval(@[NodeID(99)])
    check result.isSome
    check result.get == NodeID(99) # Returns first if no stats
    alloc.destroy()
    pool.destroy()

  test "selectLeaseholder empty replicas":
    let pool = newStorePool()
    let alloc = newAllocator(pool)
    let result = alloc.selectLeaseholder(@[], NodeID(0))
    check result.isNone
    alloc.destroy()
    pool.destroy()

  test "selectLeaseholder single replica":
    let pool = newStorePool()
    pool.addStore(newStoreStats(NodeID(1)))

    let alloc = newAllocator(pool)
    let replicas = @[newReplicaDescriptor(NodeID(1), ReplicaID(1), rtVoter)]
    let result = alloc.selectLeaseholder(replicas, NodeID(0))
    check result.isSome
    check result.get == NodeID(1)
    alloc.destroy()
    pool.destroy()

  test "selectLeaseholder prefers least loaded":
    let pool = newStorePool()
    var stats1 = newStoreStats(NodeID(1))
    stats1.capacityBytes = 1000'i64
    stats1.totalBytes = 900'i64 # More loaded
    pool.addStore(stats1)

    var stats2 = newStoreStats(NodeID(2))
    stats2.capacityBytes = 1000'i64
    stats2.totalBytes = 100'i64 # Less loaded
    pool.addStore(stats2)

    let alloc = newAllocator(pool)
    let replicas = @[
      newReplicaDescriptor(NodeID(1), ReplicaID(1), rtVoter),
      newReplicaDescriptor(NodeID(2), ReplicaID(2), rtVoter)
    ]
    let result = alloc.selectLeaseholder(replicas, NodeID(0))
    check result.isSome
    check result.get == NodeID(2) # Least loaded
    alloc.destroy()
    pool.destroy()

  test "selectLeaseholder keeps current if not overloaded":
    let pool = newStorePool()
    # Both stores have similar low load - neither is overloaded
    var stats1 = newStoreStats(NodeID(1))
    stats1.capacityBytes = 1000'i64
    stats1.totalBytes = 100'i64 # 10% utilization
    pool.addStore(stats1)

    var stats2 = newStoreStats(NodeID(2))
    stats2.capacityBytes = 1000'i64
    stats2.totalBytes = 110'i64 # Similar load, slightly more
    pool.addStore(stats2)

    let alloc = newAllocator(pool)
    let replicas = @[
      newReplicaDescriptor(NodeID(1), ReplicaID(1), rtVoter),
      newReplicaDescriptor(NodeID(2), ReplicaID(2), rtVoter)
    ]
    let result = alloc.selectLeaseholder(replicas, NodeID(1))
    check result.isSome
    check result.get == NodeID(1) # Current leaseholder not overloaded, kept even though NodeID(2) is slightly less loaded
    alloc.destroy()
    pool.destroy()

  test "selectLeaseholder no stats available":
    let pool = newStorePool()
    let alloc = newAllocator(pool)
    let replicas = @[newReplicaDescriptor(NodeID(1), ReplicaID(1), rtVoter)]
    let result = alloc.selectLeaseholder(replicas, NodeID(0))
    check result.isSome
    check result.get == NodeID(1) # Returns first if no stats
    alloc.destroy()
    pool.destroy()

  test "shouldRebalance needs more replicas":
    let pool = newStorePool()
    pool.addStore(newStoreStats(NodeID(1)))
    pool.addStore(newStoreStats(NodeID(2)))
    pool.addStore(newStoreStats(NodeID(3)))
    pool.addStore(newStoreStats(NodeID(4)))

    let alloc = newAllocator(pool, 3)
    let groupId = genGroupID()
    let replicas = @[
      newReplicaDescriptor(NodeID(1), ReplicaID(1), rtVoter)
    ]
    let decisions = alloc.shouldRebalance(groupId, replicas, NodeID(1))
    check decisions.len == 1
    check decisions[0].kind == adkAddReplica
    alloc.destroy()
    pool.destroy()

  test "shouldRebalance too many replicas":
    let pool = newStorePool()
    pool.addStore(newStoreStats(NodeID(1)))
    pool.addStore(newStoreStats(NodeID(2)))
    pool.addStore(newStoreStats(NodeID(3)))
    pool.addStore(newStoreStats(NodeID(4)))

    let alloc = newAllocator(pool, 3)
    let groupId = genGroupID()
    let replicas = @[
      newReplicaDescriptor(NodeID(1), ReplicaID(1), rtVoter),
      newReplicaDescriptor(NodeID(2), ReplicaID(2), rtVoter),
      newReplicaDescriptor(NodeID(3), ReplicaID(3), rtVoter),
      newReplicaDescriptor(NodeID(4), ReplicaID(4), rtVoter)
    ]
    let decisions = alloc.shouldRebalance(groupId, replicas, NodeID(1))
    check decisions.len >= 1
    check decisions[0].kind == adkRemoveReplica
    alloc.destroy()
    pool.destroy()

  test "shouldRebalance correct replica count":
    let pool = newStorePool()
    pool.addStore(newStoreStats(NodeID(1)))
    pool.addStore(newStoreStats(NodeID(2)))
    pool.addStore(newStoreStats(NodeID(3)))

    let alloc = newAllocator(pool, 3)
    let groupId = genGroupID()
    let replicas = @[
      newReplicaDescriptor(NodeID(1), ReplicaID(1), rtVoter),
      newReplicaDescriptor(NodeID(2), ReplicaID(2), rtVoter),
      newReplicaDescriptor(NodeID(3), ReplicaID(3), rtVoter)
    ]
    let decisions = alloc.shouldRebalance(groupId, replicas, NodeID(1))
    # No replica count changes needed (might have lease transfer decision)
    let addOrRemove = decisions.filterIt(it.kind == adkAddReplica or it.kind == adkRemoveReplica)
    check addOrRemove.len == 0
    alloc.destroy()
    pool.destroy()

  test "allocateNewGroup":
    let pool = newStorePool()
    pool.addStore(newStoreStats(NodeID(1)))
    pool.addStore(newStoreStats(NodeID(2)))
    pool.addStore(newStoreStats(NodeID(3)))

    let alloc = newAllocator(pool, 3)
    let nodes = alloc.allocateNewGroup(defaultConstraints())
    check nodes.len == 3
    alloc.destroy()
    pool.destroy()

  test "allocateNewGroup limited stores":
    let pool = newStorePool()
    pool.addStore(newStoreStats(NodeID(1)))
    pool.addStore(newStoreStats(NodeID(2)))

    let alloc = newAllocator(pool, 3)
    let nodes = alloc.allocateNewGroup(defaultConstraints())
    check nodes.len == 2 # Only 2 stores available
    alloc.destroy()
    pool.destroy()

  test "isStoreOverloaded true":
    let pool = newStorePool()
    var stats1 = newStoreStats(NodeID(1))
    stats1.capacityBytes = 1000'i64
    stats1.totalBytes = 900'i64 # High utilization
    pool.addStore(stats1)

    var stats2 = newStoreStats(NodeID(2))
    stats2.capacityBytes = 1000'i64
    stats2.totalBytes = 100'i64 # Low utilization
    pool.addStore(stats2)

    let alloc = newAllocator(pool)
    check alloc.isStoreOverloaded(NodeID(1))
    alloc.destroy()
    pool.destroy()

  test "isStoreOverloaded false":
    let pool = newStorePool()
    var stats1 = newStoreStats(NodeID(1))
    stats1.capacityBytes = 1000'i64
    stats1.totalBytes = 100'i64 # Low utilization
    pool.addStore(stats1)

    var stats2 = newStoreStats(NodeID(2))
    stats2.capacityBytes = 1000'i64
    stats2.totalBytes = 500'i64 # Average utilization
    pool.addStore(stats2)

    let alloc = newAllocator(pool)
    check not alloc.isStoreOverloaded(NodeID(1))
    alloc.destroy()
    pool.destroy()

  test "isStoreOverloaded unknown node":
    let pool = newStorePool()
    let alloc = newAllocator(pool)
    check not alloc.isStoreOverloaded(NodeID(99))
    alloc.destroy()
    pool.destroy()

  test "isStoreUnderloaded true":
    let pool = newStorePool()
    var stats1 = newStoreStats(NodeID(1))
    stats1.capacityBytes = 1000'i64
    stats1.totalBytes = 100'i64 # Low utilization
    pool.addStore(stats1)

    var stats2 = newStoreStats(NodeID(2))
    stats2.capacityBytes = 1000'i64
    stats2.totalBytes = 900'i64 # High utilization
    pool.addStore(stats2)

    let alloc = newAllocator(pool)
    check alloc.isStoreUnderloaded(NodeID(1))
    alloc.destroy()
    pool.destroy()

  test "isStoreUnderloaded false":
    let pool = newStorePool()
    var stats1 = newStoreStats(NodeID(1))
    stats1.capacityBytes = 1000'i64
    stats1.totalBytes = 900'i64 # High utilization
    pool.addStore(stats1)

    var stats2 = newStoreStats(NodeID(2))
    stats2.capacityBytes = 1000'i64
    stats2.totalBytes = 500'i64 # Average utilization
    pool.addStore(stats2)

    let alloc = newAllocator(pool)
    check not alloc.isStoreUnderloaded(NodeID(1))
    alloc.destroy()
    pool.destroy()

  test "isStoreUnderloaded unknown node":
    let pool = newStorePool()
    let alloc = newAllocator(pool)
    check not alloc.isStoreUnderloaded(NodeID(99))
    alloc.destroy()
    pool.destroy()

  test "getOverloadedStores":
    let pool = newStorePool()
    var stats1 = newStoreStats(NodeID(1))
    stats1.capacityBytes = 1000'i64
    stats1.totalBytes = 900'i64 # High
    pool.addStore(stats1)

    var stats2 = newStoreStats(NodeID(2))
    stats2.capacityBytes = 1000'i64
    stats2.totalBytes = 100'i64 # Low
    pool.addStore(stats2)

    let alloc = newAllocator(pool)
    let overloaded = alloc.getOverloadedStores()
    check NodeID(1) in overloaded
    check NodeID(2) notin overloaded
    alloc.destroy()
    pool.destroy()

  test "getUnderloadedStores":
    let pool = newStorePool()
    var stats1 = newStoreStats(NodeID(1))
    stats1.capacityBytes = 1000'i64
    stats1.totalBytes = 900'i64 # High
    pool.addStore(stats1)

    var stats2 = newStoreStats(NodeID(2))
    stats2.capacityBytes = 1000'i64
    stats2.totalBytes = 100'i64 # Low
    pool.addStore(stats2)

    let alloc = newAllocator(pool)
    let underloaded = alloc.getUnderloadedStores()
    check NodeID(1) notin underloaded
    check NodeID(2) in underloaded
    alloc.destroy()
    pool.destroy()
