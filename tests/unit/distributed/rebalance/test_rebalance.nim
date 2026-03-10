# Unit tests for rebalance modules
#
# Tests for:
# - Allocator decision logic
# - Store pool management
# - Rebalance scheduler
# - Rebalance constraints

import std/unittest
import std/options

import fractio/distributed/raft/group_types
import fractio/distributed/rebalance/allocator
import fractio/distributed/rebalance/scheduler

suite "Store Stats":
  test "create store stats":
    let stats = newStoreStats(NodeID(1))
    check stats.nodeId == NodeID(1)
    check stats.replicaCount == 0
    check stats.leaderCount == 0
    check stats.totalBytes == 0

  test "calculate utilization":
    var stats = newStoreStats(NodeID(1))
    stats.totalBytes = 50_000_000_000'i64
    stats.capacityBytes = 100_000_000_000'i64
    stats.cpuUsage = 0.5
    stats.memoryUsage = 0.4

    let util = stats.utilization()
    check util > 0.0
    check util < 1.0

  test "calculate load score":
    var stats = newStoreStats(NodeID(1))
    stats.totalBytes = 50_000_000_000'i64
    stats.capacityBytes = 100_000_000_000'i64
    stats.leaderCount = 10

    let score = stats.loadScore()
    check score > 0.0

  test "locality matching":
    var stats1 = newStoreStats(NodeID(1))
    stats1.locality = @[("region", "us-west"), ("zone", "a")]

    var stats2 = newStoreStats(NodeID(2))
    stats2.locality = @[("region", "us-west"), ("zone", "b")]

    check stats1.hasLocality("region", "us-west")
    check not stats1.hasLocality("region", "us-east")
    check stats1.localityMatch(stats2) == 1 # region matches

suite "Store Pool":
  test "create store pool":
    let pool = newStorePool()
    pool.destroy()

  test "add and get store":
    let pool = newStorePool()
    var stats = newStoreStats(NodeID(1))
    stats.replicaCount = 5

    pool.addStore(stats)

    let result = pool.getStore(NodeID(1))
    check result.isSome
    check result.get.replicaCount == 5
    pool.destroy()

  test "remove store":
    let pool = newStorePool()
    var stats = newStoreStats(NodeID(1))
    pool.addStore(stats)

    pool.removeStore(NodeID(1))

    let result = pool.getStore(NodeID(1))
    check result.isNone
    pool.destroy()

  test "get alive stores":
    let pool = newStorePool()
    pool.addStore(newStoreStats(NodeID(1)))
    pool.addStore(newStoreStats(NodeID(2)))
    pool.addStore(newStoreStats(NodeID(3)))

    let stores = pool.getAliveStores()
    check stores.len == 3
    pool.destroy()

  test "average load":
    let pool = newStorePool()
    var stats1 = newStoreStats(NodeID(1))
    stats1.totalBytes = 50_000_000_000'i64
    stats1.capacityBytes = 100_000_000_000'i64

    var stats2 = newStoreStats(NodeID(2))
    stats2.totalBytes = 30_000_000_000'i64
    stats2.capacityBytes = 100_000_000_000'i64

    pool.addStore(stats1)
    pool.addStore(stats2)

    let avg = pool.averageLoad()
    check avg >= 0.0
    pool.destroy()

suite "Allocator":
  test "create allocator":
    let pool = newStorePool()
    let alloc = newAllocator(pool)
    alloc.destroy()
    pool.destroy()

  test "select store for replica":
    let pool = newStorePool()

    # Add stores with different loads
    var stats1 = newStoreStats(NodeID(1))
    stats1.totalBytes = 80_000_000_000'i64
    stats1.capacityBytes = 100_000_000_000'i64

    var stats2 = newStoreStats(NodeID(2))
    stats2.totalBytes = 20_000_000_000'i64
    stats2.capacityBytes = 100_000_000_000'i64

    pool.addStore(stats1)
    pool.addStore(stats2)

    let alloc = newAllocator(pool)
    let constraints = defaultConstraints()

    let selected = alloc.selectStoreForReplica(@[], constraints)
    check selected.isSome
    # Should prefer the less loaded store
    check selected.get == NodeID(2)

    alloc.destroy()
    pool.destroy()

  test "select store with forbidden nodes":
    let pool = newStorePool()
    pool.addStore(newStoreStats(NodeID(1)))
    pool.addStore(newStoreStats(NodeID(2)))

    let alloc = newAllocator(pool)
    var constraints = defaultConstraints()
    constraints.forbiddenNodes = @[NodeID(2)]

    let selected = alloc.selectStoreForReplica(@[], constraints)
    check selected.isSome
    check selected.get == NodeID(1)

    alloc.destroy()
    pool.destroy()

  test "should rebalance under-replicated":
    let pool = newStorePool()
    pool.addStore(newStoreStats(NodeID(1)))
    pool.addStore(newStoreStats(NodeID(2)))

    let alloc = newAllocator(pool)

    let replicas = @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    let decisions = alloc.shouldRebalance(GroupID(1), replicas, NodeID(1))

    # Should add replica (only 1 of 3)
    check decisions.len == 1
    check decisions[0].kind == adkAddReplica

    alloc.destroy()
    pool.destroy()

  test "allocate new group":
    let pool = newStorePool()
    pool.addStore(newStoreStats(NodeID(1)))
    pool.addStore(newStoreStats(NodeID(2)))
    pool.addStore(newStoreStats(NodeID(3)))

    let alloc = newAllocator(pool)
    let constraints = defaultConstraints()

    let nodes = alloc.allocateNewGroup(constraints)
    check nodes.len == 3 # Default replication factor

    alloc.destroy()
    pool.destroy()

suite "Allocation Decision":
  test "create add replica decision":
    let decision = newAddReplicaDecision(GroupID(1), NodeID(2), 10, "test")
    check decision.kind == adkAddReplica
    check decision.addGroupId == GroupID(1)
    check decision.addTarget == NodeID(2)
    check decision.priority == 10

  test "create remove replica decision":
    let decision = newRemoveReplicaDecision(GroupID(1), NodeID(2), 5, "test")
    check decision.kind == adkRemoveReplica
    check decision.removeGroupId == GroupID(1)
    check decision.removeTarget == NodeID(2)

  test "create transfer lease decision":
    let decision = newTransferLeaseDecision(GroupID(1), NodeID(1), NodeID(2), 3, "test")
    check decision.kind == adkTransferLease
    check decision.transferGroupId == GroupID(1)
    check decision.transferFrom == NodeID(1)
    check decision.transferTo == NodeID(2)

suite "Rebalance Queue":
  test "create queue":
    let queue = newRebalanceQueue()
    queue.destroy()

  test "enqueue and dequeue":
    let queue = newRebalanceQueue()
    let decision = newAddReplicaDecision(GroupID(1), NodeID(2), 10, "test")

    let op = queue.enqueue(decision, 1000)
    check op.decision.addGroupId == GroupID(1)

    let dequeued = queue.dequeue()
    check dequeued.isSome
    check dequeued.get.decision.addGroupId == GroupID(1)

    queue.destroy()

  test "priority ordering":
    let queue = newRebalanceQueue()

    # Add low priority first
    let low = newAddReplicaDecision(GroupID(2), NodeID(1), 1, "low")
    let high = newAddReplicaDecision(GroupID(1), NodeID(2), 10, "high")

    discard queue.enqueue(low, 1000)
    discard queue.enqueue(high, 1000)

    # Should get high priority first
    let first = queue.dequeue()
    check first.get.decision.priority == 10

    queue.destroy()

  test "operation state transitions":
    let queue = newRebalanceQueue()
    let decision = newAddReplicaDecision(GroupID(1), NodeID(2), 10, "test")

    let op = queue.enqueue(decision, 1000)
    check op.state == rosPending

    queue.startOp(op, 2000)
    check op.state == rosInProgress
    check op.startedAtNs == 2000

    queue.completeOp(op, 3000)
    check op.state == rosCompleted
    check op.completedAtNs == 3000

    queue.destroy()

suite "Rebalance Scheduler":
  test "create scheduler":
    let pool = newStorePool()
    let alloc = newAllocator(pool)

    proc mockExecute(op: RebalanceOp): bool = true

    let scheduler = newRebalanceScheduler(alloc, mockExecute)
    scheduler.destroy()
    alloc.destroy()
    pool.destroy()

  test "add decision":
    let pool = newStorePool()
    let alloc = newAllocator(pool)

    proc mockExecute(op: RebalanceOp): bool = true

    let scheduler = newRebalanceScheduler(alloc, mockExecute)
    let decision = newAddReplicaDecision(GroupID(1), NodeID(2), 10, "test")

    let op = scheduler.addDecision(decision, 1000)
    check op.isSome

    scheduler.destroy()
    alloc.destroy()
    pool.destroy()

  test "process batch":
    let pool = newStorePool()
    let alloc = newAllocator(pool)

    var executed = 0
    proc mockExecute(op: RebalanceOp): bool =
      inc executed
      return true

    let scheduler = newRebalanceScheduler(alloc, mockExecute)

    # Add multiple decisions
    for i in 1..5:
      let decision = newAddReplicaDecision(GroupID(i), NodeID(2), 10, "test")
      discard scheduler.addDecision(decision, 1000)

    # Process batch
    let processed = scheduler.processBatch(2000)
    check processed == 5
    check executed == 5

    scheduler.destroy()
    alloc.destroy()
    pool.destroy()

  test "get stats":
    let pool = newStorePool()
    let alloc = newAllocator(pool)

    proc mockExecute(op: RebalanceOp): bool = true

    let scheduler = newRebalanceScheduler(alloc, mockExecute)
    let stats = scheduler.getStats()

    check stats.executed == 0
    check stats.succeeded == 0
    check stats.failed == 0

    scheduler.destroy()
    alloc.destroy()
    pool.destroy()

suite "Constants":
  test "rebalance constants":
    check DEFAULT_REPLICATION_FACTOR == 3
    check DEFAULT_MAX_REPLICAS_PER_STORE == 1000
    check OVERLOAD_THRESHOLD == 1.2
    check UNDERLOAD_THRESHOLD == 0.8
