# Unit tests for rebalance modules
#
# Tests for:
# - Allocator decision logic
# - Store pool management
# - Rebalance scheduler
# - Split and merge triggers

import std/unittest
import std/options

import fractio/distributed/range/types
import fractio/distributed/rebalance/allocator
import fractio/distributed/rebalance/scheduler
import fractio/distributed/range/split
import fractio/distributed/range/merge

suite "Store Stats":
  test "create store stats":
    let stats = newStoreStats(RangeNodeID(1))
    check stats.nodeId == RangeNodeID(1)
    check stats.replicaCount == 0
    check stats.leaderCount == 0
    check stats.totalBytes == 0

  test "calculate utilization":
    var stats = newStoreStats(RangeNodeID(1))
    stats.totalBytes = 50_000_000_000'i64
    stats.capacityBytes = 100_000_000_000'i64
    stats.cpuUsage = 0.5
    stats.memoryUsage = 0.4

    let util = stats.utilization()
    check util > 0.0
    check util < 1.0

  test "calculate load score":
    var stats = newStoreStats(RangeNodeID(1))
    stats.totalBytes = 50_000_000_000'i64
    stats.capacityBytes = 100_000_000_000'i64
    stats.leaderCount = 10

    let score = stats.loadScore()
    check score > 0.0

  test "locality matching":
    var stats1 = newStoreStats(RangeNodeID(1))
    stats1.locality = @[("region", "us-west"), ("zone", "a")]

    var stats2 = newStoreStats(RangeNodeID(2))
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
    var stats = newStoreStats(RangeNodeID(1))
    stats.replicaCount = 5

    pool.addStore(stats)

    let result = pool.getStore(RangeNodeID(1))
    check result.isSome
    check result.get.replicaCount == 5
    pool.destroy()

  test "remove store":
    let pool = newStorePool()
    var stats = newStoreStats(RangeNodeID(1))
    pool.addStore(stats)

    pool.removeStore(RangeNodeID(1))

    let result = pool.getStore(RangeNodeID(1))
    check result.isNone
    pool.destroy()

  test "get alive stores":
    let pool = newStorePool()
    pool.addStore(newStoreStats(RangeNodeID(1)))
    pool.addStore(newStoreStats(RangeNodeID(2)))
    pool.addStore(newStoreStats(RangeNodeID(3)))

    let stores = pool.getAliveStores()
    check stores.len == 3
    pool.destroy()

  test "average load":
    let pool = newStorePool()
    var stats1 = newStoreStats(RangeNodeID(1))
    stats1.totalBytes = 50_000_000_000'i64
    stats1.capacityBytes = 100_000_000_000'i64

    var stats2 = newStoreStats(RangeNodeID(2))
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
    var stats1 = newStoreStats(RangeNodeID(1))
    stats1.totalBytes = 80_000_000_000'i64
    stats1.capacityBytes = 100_000_000_000'i64

    var stats2 = newStoreStats(RangeNodeID(2))
    stats2.totalBytes = 20_000_000_000'i64
    stats2.capacityBytes = 100_000_000_000'i64

    pool.addStore(stats1)
    pool.addStore(stats2)

    let alloc = newAllocator(pool)
    let constraints = defaultConstraints()

    let selected = alloc.selectStoreForReplica(@[], constraints)
    check selected.isSome
    # Should prefer the less loaded store
    check selected.get == RangeNodeID(2)

    alloc.destroy()
    pool.destroy()

  test "select store with forbidden nodes":
    let pool = newStorePool()
    pool.addStore(newStoreStats(RangeNodeID(1)))
    pool.addStore(newStoreStats(RangeNodeID(2)))

    let alloc = newAllocator(pool)
    var constraints = defaultConstraints()
    constraints.forbiddenNodes = @[RangeNodeID(2)]

    let selected = alloc.selectStoreForReplica(@[], constraints)
    check selected.isSome
    check selected.get == RangeNodeID(1)

    alloc.destroy()
    pool.destroy()

  test "should rebalance under-replicated":
    let pool = newStorePool()
    pool.addStore(newStoreStats(RangeNodeID(1)))
    pool.addStore(newStoreStats(RangeNodeID(2)))

    let alloc = newAllocator(pool)

    let replicas = @[newReplicaDescriptor(RangeNodeID(1), ReplicaID(1))]
    let decisions = alloc.shouldRebalance(RangeID(1), replicas, RangeNodeID(1))

    # Should add replica (only 1 of 3)
    check decisions.len == 1
    check decisions[0].kind == adkAddReplica

    alloc.destroy()
    pool.destroy()

  test "allocate new range":
    let pool = newStorePool()
    pool.addStore(newStoreStats(RangeNodeID(1)))
    pool.addStore(newStoreStats(RangeNodeID(2)))
    pool.addStore(newStoreStats(RangeNodeID(3)))

    let alloc = newAllocator(pool)
    let constraints = defaultConstraints()

    let nodes = alloc.allocateNewRange(constraints)
    check nodes.len == 3 # Default replication factor

    alloc.destroy()
    pool.destroy()

suite "Allocation Decision":
  test "create add replica decision":
    let decision = newAddReplicaDecision(RangeID(1), RangeNodeID(2), 10, "test")
    check decision.kind == adkAddReplica
    check decision.addRangeId == RangeID(1)
    check decision.addTarget == RangeNodeID(2)
    check decision.priority == 10

  test "create remove replica decision":
    let decision = newRemoveReplicaDecision(RangeID(1), RangeNodeID(2), 5, "test")
    check decision.kind == adkRemoveReplica
    check decision.removeRangeId == RangeID(1)
    check decision.removeTarget == RangeNodeID(2)

  test "create transfer lease decision":
    let decision = newTransferLeaseDecision(RangeID(1), RangeNodeID(1), RangeNodeID(2), 3, "test")
    check decision.kind == adkTransferLease
    check decision.transferRangeId == RangeID(1)
    check decision.transferFrom == RangeNodeID(1)
    check decision.transferTo == RangeNodeID(2)

suite "Rebalance Queue":
  test "create queue":
    let queue = newRebalanceQueue()
    queue.destroy()

  test "enqueue and dequeue":
    let queue = newRebalanceQueue()
    let decision = newAddReplicaDecision(RangeID(1), RangeNodeID(2), 10, "test")

    let op = queue.enqueue(decision, 1000)
    check op.decision.addRangeId == RangeID(1)

    let dequeued = queue.dequeue()
    check dequeued.isSome
    check dequeued.get.decision.addRangeId == RangeID(1)

    queue.destroy()

  test "priority ordering":
    let queue = newRebalanceQueue()

    # Add low priority first
    let low = newAddReplicaDecision(RangeID(2), RangeNodeID(1), 1, "low")
    let high = newAddReplicaDecision(RangeID(1), RangeNodeID(2), 10, "high")

    discard queue.enqueue(low, 1000)
    discard queue.enqueue(high, 1000)

    # Should get high priority first
    let first = queue.dequeue()
    check first.get.decision.priority == 10

    queue.destroy()

  test "operation state transitions":
    let queue = newRebalanceQueue()
    let decision = newAddReplicaDecision(RangeID(1), RangeNodeID(2), 10, "test")

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
    let decision = newAddReplicaDecision(RangeID(1), RangeNodeID(2), 10, "test")

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
      let decision = newAddReplicaDecision(RangeID(i), RangeNodeID(2), 10, "test")
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

suite "Split Statistics":
  test "create range stats":
    let stats = newRangeStats(RangeID(1))
    check stats.rangeId == RangeID(1)
    check stats.totalBytes == 0
    check stats.keyCount == 0

  test "should split by size":
    var stats = newRangeStats(RangeID(1))
    stats.totalBytes = SPLIT_THRESHOLD_BYTES + 1

    check stats.shouldSplit(0)

  test "should split by keys":
    var stats = newRangeStats(RangeID(1))
    stats.keyCount = SPLIT_THRESHOLD_KEYS + 1

    check stats.shouldSplit(0)

  test "split cooldown":
    var stats = newRangeStats(RangeID(1))
    stats.lastSplitNs = 1000

    # Should not split during cooldown
    check not stats.canSplit(10000)
    check stats.canSplit(SPLIT_COOLDOWN_NS + 2000)

suite "Split Decision":
  test "create split decision":
    let decision = newSplitDecision(
      RangeID(1),
      @[byte(50)],
      RangeID(2),
      RangeID(3),
      10,
      "test"
    )

    check decision.rangeId == RangeID(1)
    check decision.splitKey == @[byte(50)]
    check decision.leftRangeId == RangeID(2)
    check decision.rightRangeId == RangeID(3)
    check decision.priority == 10

suite "Split Executor":
  test "create executor":
    let executor = newSplitExecutor()
    executor.destroy()

  test "allocate range ID":
    let executor = newSplitExecutor()

    let id1 = executor.allocateRangeId()
    let id2 = executor.allocateRangeId()

    check id1 != id2
    executor.destroy()

  test "create split descriptors":
    let executor = newSplitExecutor()

    let original = newRangeDescriptor(
      RangeID(1),
      @[byte(0)],
      @[byte(100)],
      @[newReplicaDescriptor(RangeNodeID(1), ReplicaID(1))]
    )

    let (left, right) = executor.createSplitDescriptor(
      original,
      @[byte(50)],
      RangeID(2),
      RangeID(3)
    )

    check left.startKey == @[byte(0)]
    check left.endKey == @[byte(50)]
    check right.startKey == @[byte(50)]
    check right.endKey == @[byte(100)]

    executor.destroy()

suite "Merge Statistics":
  test "create merge stats":
    let stats = newMergeStats(RangeID(1))
    check stats.rangeId == RangeID(1)
    check stats.totalBytes == 0

  test "should merge by size":
    var stats = newMergeStats(RangeID(1))
    stats.totalBytes = MERGE_THRESHOLD_BYTES - 1

    check stats.shouldMerge(0)

  test "merge cooldown":
    var stats = newMergeStats(RangeID(1))
    stats.lastMergeNs = 1000

    check not stats.canMerge(10000)
    check stats.canMerge(MERGE_COOLDOWN_NS + 2000)

suite "Merge Decision":
  test "create merge decision":
    let decision = newMergeDecision(
      RangeID(1),
      RangeID(2),
      RangeID(1),
      5,
      "test"
    )

    check decision.leftRangeId == RangeID(1)
    check decision.rightRangeId == RangeID(2)
    check decision.mergedRangeId == RangeID(1)
    check decision.priority == 5

suite "Merge Executor":
  test "create executor":
    let executor = newMergeExecutor()
    executor.destroy()

  test "create merged descriptor":
    let executor = newMergeExecutor()

    let left = newRangeDescriptor(
      RangeID(1),
      @[byte(0)],
      @[byte(50)],
      @[newReplicaDescriptor(RangeNodeID(1), ReplicaID(1))]
    )

    let right = newRangeDescriptor(
      RangeID(2),
      @[byte(50)],
      @[byte(100)],
      @[newReplicaDescriptor(RangeNodeID(1), ReplicaID(2))]
    )

    let merged = executor.createMergedDescriptor(left, right)

    check merged.startKey == @[byte(0)]
    check merged.endKey == @[byte(100)]

    executor.destroy()

suite "Constants":
  test "rebalance constants":
    check DEFAULT_REPLICATION_FACTOR == 3
    check DEFAULT_MAX_REPLICAS_PER_STORE == 1000
    check OVERLOAD_THRESHOLD == 1.2
    check UNDERLOAD_THRESHOLD == 0.8

  test "split constants":
    check SPLIT_THRESHOLD_BYTES == MAX_RANGE_SIZE_BYTES
    check MIN_SPLIT_SIZE_BYTES == MIN_RANGE_SIZE_BYTES * 2

  test "merge constants":
    check MERGE_THRESHOLD_BYTES == MIN_RANGE_SIZE_BYTES
