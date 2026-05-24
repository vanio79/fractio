# Unit tests for fractio/distributed/rebalance/scheduler.nim
# Tests RebalanceOp, RebalanceQueue, RebalanceScheduler, and RebalanceConstraints

import std/[unittest, options, atomics]
import fractio/core/types except NodeID
import fractio/distributed/raft/group_types
import fractio/distributed/rebalance/allocator
import fractio/distributed/rebalance/scheduler

suite "RebalanceOpState":

  test "all states defined":
    check rosPending.ord == 0
    check rosInProgress.ord == 1
    check rosCompleted.ord == 2
    check rosFailed.ord == 3
    check rosCancelled.ord == 4

suite "RebalanceOp":

  test "newRebalanceOp":
    let decision = newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 10, "test")
    let op = newRebalanceOp(decision, 1000'i64)
    check op.id != zeroTransactionID()
    check op.decision.kind == adkAddReplica
    check op.decision.priority == 10
    check op.state == rosPending
    check op.createdAtNs == 1000'i64
    check op.startedAtNs == 0
    check op.completedAtNs == 0
    check op.attempts == 0
    check op.lastError == ""

  test "markInProgress":
    let decision = newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 10, "test")
    let op = newRebalanceOp(decision, 1000'i64)
    op.markInProgress(2000'i64)
    check op.state == rosInProgress
    check op.startedAtNs == 2000'i64
    check op.attempts == 1

  test "markCompleted":
    let decision = newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 10, "test")
    let op = newRebalanceOp(decision, 1000'i64)
    op.markInProgress(2000'i64)
    op.markCompleted(3000'i64)
    check op.state == rosCompleted
    check op.completedAtNs == 3000'i64

  test "markFailed":
    let decision = newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 10, "test")
    let op = newRebalanceOp(decision, 1000'i64)
    op.markInProgress(2000'i64)
    op.markFailed(3000'i64, "test error")
    check op.state == rosFailed
    check op.completedAtNs == 3000'i64
    check op.lastError == "test error"

  test "markCancelled":
    let decision = newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 10, "test")
    let op = newRebalanceOp(decision, 1000'i64)
    op.markCancelled(2000'i64)
    check op.state == rosCancelled
    check op.completedAtNs == 2000'i64

  test "ageNs":
    let decision = newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 10, "test")
    let op = newRebalanceOp(decision, 1000'i64)
    check op.ageNs(5000'i64) == 4000'i64

  test "durationNs completed":
    let decision = newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 10, "test")
    let op = newRebalanceOp(decision, 1000'i64)
    op.markInProgress(2000'i64)
    op.markCompleted(5000'i64)
    check op.durationNs(10000'i64) == 3000'i64

  test "durationNs in progress":
    let decision = newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 10, "test")
    let op = newRebalanceOp(decision, 1000'i64)
    op.markInProgress(2000'i64)
    check op.durationNs(5000'i64) == 3000'i64

  test "durationNs not started":
    let decision = newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 10, "test")
    let op = newRebalanceOp(decision, 1000'i64)
    check op.durationNs(5000'i64) == 0

suite "RebalanceQueue":

  test "newRebalanceQueue":
    let queue = newRebalanceQueue()
    check queue.pending.len == 0
    check queue.inProgress.len == 0
    check queue.completed.len == 0
    queue.destroy()

  test "enqueue":
    let queue = newRebalanceQueue()
    let decision = newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 10, "test")
    let op = queue.enqueue(decision, 1000'i64)
    check queue.pending.len == 1
    check op.decision.kind == adkAddReplica
    check op.decision.priority == 10
    queue.destroy()

  test "enqueue sorts by priority":
    let queue = newRebalanceQueue()
    let lowPriority = newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 5, "low")
    let highPriority = newAddReplicaDecision(genGroupIDLocal(), NodeID(2), 10, "high")

    discard queue.enqueue(lowPriority, 1000'i64)
    discard queue.enqueue(highPriority, 1000'i64)

    # High priority should be first
    let first = queue.dequeue()
    check first.isSome
    check first.get.decision.priority == 10
    queue.destroy()

  test "dequeue empty":
    let queue = newRebalanceQueue()
    let result = queue.dequeue()
    check result.isNone
    queue.destroy()

  test "dequeue removes from pending":
    let queue = newRebalanceQueue()
    let decision = newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 10, "test")
    discard queue.enqueue(decision, 1000'i64)

    let op = queue.dequeue()
    check op.isSome
    check queue.pending.len == 0
    queue.destroy()

  test "startOp":
    let queue = newRebalanceQueue()
    let decision = newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 10, "test")
    discard queue.enqueue(decision, 1000'i64)

    # First dequeue to remove from pending
    let dequeued = queue.dequeue()
    check dequeued.isSome
    queue.startOp(dequeued.get, 2000'i64)
    check dequeued.get.state == rosInProgress
    check queue.inProgress.len == 1
    queue.destroy()

  test "completeOp":
    let queue = newRebalanceQueue()
    let decision = newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 10, "test")
    discard queue.enqueue(decision, 1000'i64)

    let op = queue.dequeue()
    check op.isSome
    queue.startOp(op.get, 2000'i64)
    queue.completeOp(op.get, 3000'i64)

    check op.get.state == rosCompleted
    check queue.inProgress.len == 0
    check queue.completed.len == 1
    queue.destroy()

  test "failOp":
    let queue = newRebalanceQueue()
    let decision = newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 10, "test")
    discard queue.enqueue(decision, 1000'i64)

    let op = queue.dequeue()
    check op.isSome
    queue.startOp(op.get, 2000'i64)
    queue.failOp(op.get, 3000'i64, "error")

    check op.get.state == rosFailed
    check op.get.lastError == "error"
    check queue.inProgress.len == 0
    check queue.completed.len == 1
    queue.destroy()

  test "cancelOp from pending":
    let queue = newRebalanceQueue()
    let decision = newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 10, "test")
    let op = queue.enqueue(decision, 1000'i64)

    queue.cancelOp(op, 2000'i64)

    check op.state == rosCancelled
    check queue.pending.len == 0
    # cancelOp returns early when found in pending, doesn't add to completed
    queue.destroy()

  test "cancelOp from inProgress":
    let queue = newRebalanceQueue()
    let decision = newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 10, "test")
    discard queue.enqueue(decision, 1000'i64)

    # Proper flow: dequeue removes from pending, then startOp adds to inProgress
    let op = queue.dequeue()
    check op.isSome
    queue.startOp(op.get, 2000'i64)
    queue.cancelOp(op.get, 3000'i64)

    check op.get.state == rosCancelled
    check queue.inProgress.len == 0
    check queue.completed.len == 1
    queue.destroy()

  test "cancelOp not found":
    let queue = newRebalanceQueue()
    let decision = newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 10, "test")
    # Create op but don't enqueue it
    let op = newRebalanceOp(decision, 1000'i64)

    queue.cancelOp(op, 2000'i64)

    check op.state == rosCancelled
    check queue.completed.len == 1 # Added to completed since not found elsewhere
    queue.destroy()

  test "pendingCount":
    let queue = newRebalanceQueue()
    check queue.pendingCount() == 0

    discard queue.enqueue(newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 10,
        "test"), 1000'i64)
    check queue.pendingCount() == 1
    queue.destroy()

  test "inProgressCount":
    let queue = newRebalanceQueue()
    check queue.inProgressCount() == 0

    discard queue.enqueue(newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 10,
        "test"), 1000'i64)
    let op = queue.dequeue()
    check op.isSome
    queue.startOp(op.get, 2000'i64)
    check queue.inProgressCount() == 1
    queue.destroy()

  test "totalCount":
    let queue = newRebalanceQueue()
    check queue.totalCount() == 0

    discard queue.enqueue(newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 10,
        "test"), 1000'i64)
    check queue.totalCount() == 1

    # Proper flow: dequeue removes from pending, then startOp adds to inProgress
    let dequeued = queue.dequeue()
    check dequeued.isSome
    queue.startOp(dequeued.get, 2000'i64)
    check queue.totalCount() == 1 # Still total (pending + inProgress) = 0 + 1
    queue.destroy()

  test "getStats":
    let queue = newRebalanceQueue()
    let stats = queue.getStats()
    check stats.pending == 0
    check stats.inProgress == 0
    check stats.completed == 0

    discard queue.enqueue(newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 10,
        "test"), 1000'i64)
    let dequeued = queue.dequeue()
    check dequeued.isSome
    queue.startOp(dequeued.get, 2000'i64)
    queue.completeOp(dequeued.get, 3000'i64)

    let stats2 = queue.getStats()
    check stats2.pending == 0
    check stats2.inProgress == 0
    check stats2.completed == 1
    queue.destroy()

suite "RebalanceScheduler":

  proc dummyCallback(op: RebalanceOp): bool {.gcsafe.} = true

  test "newRebalanceScheduler":
    let pool = newStorePool()
    let alloc = newAllocator(pool)
    let scheduler = newRebalanceScheduler(alloc, dummyCallback)

    check scheduler.allocator == alloc
    check scheduler.queue != nil
    check scheduler.rebalanceIntervalNs == DEFAULT_REBALANCE_INTERVAL_NS
    check scheduler.batchSize == DEFAULT_BATCH_SIZE
    check scheduler.maxPending == DEFAULT_MAX_PENDING
    check scheduler.opsExecuted.load() == 0
    check scheduler.opsSucceeded.load() == 0
    check scheduler.opsFailed.load() == 0
    check not scheduler.running.load()

    scheduler.destroy()
    alloc.destroy()
    pool.destroy()

  test "addDecision":
    let pool = newStorePool()
    let alloc = newAllocator(pool)
    let scheduler = newRebalanceScheduler(alloc, dummyCallback)

    let decision = newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 10, "test")
    let result = scheduler.addDecision(decision, 1000'i64)

    check result.isSome
    check scheduler.queue.pendingCount() == 1

    scheduler.destroy()
    alloc.destroy()
    pool.destroy()

  test "addDecision when max pending reached":
    let pool = newStorePool()
    let alloc = newAllocator(pool)
    var scheduler = newRebalanceScheduler(alloc, dummyCallback)
    scheduler.maxPending = 1

    # Add first decision
    let decision1 = newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 10, "test")
    let result1 = scheduler.addDecision(decision1, 1000'i64)
    check result1.isSome

    # Try to add second - should fail
    let decision2 = newAddReplicaDecision(genGroupIDLocal(), NodeID(2), 10, "test2")
    let result2 = scheduler.addDecision(decision2, 1000'i64)
    check result2.isNone

    scheduler.destroy()
    alloc.destroy()
    pool.destroy()

  test "addDecisions":
    let pool = newStorePool()
    let alloc = newAllocator(pool)
    let scheduler = newRebalanceScheduler(alloc, dummyCallback)

    let decisions = @[
      newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 10, "test1"),
      newAddReplicaDecision(genGroupIDLocal(), NodeID(2), 5, "test2")
    ]
    let ops = scheduler.addDecisions(decisions, 1000'i64)

    check ops.len == 2
    check scheduler.queue.pendingCount() == 2

    scheduler.destroy()
    alloc.destroy()
    pool.destroy()

  test "addDecisions respects max pending":
    let pool = newStorePool()
    let alloc = newAllocator(pool)
    var scheduler = newRebalanceScheduler(alloc, dummyCallback)
    scheduler.maxPending = 2

    let decisions = @[
      newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 10, "test1"),
      newAddReplicaDecision(genGroupIDLocal(), NodeID(2), 5, "test2"),
      newAddReplicaDecision(genGroupIDLocal(), NodeID(3), 3, "test3")
    ]
    let ops = scheduler.addDecisions(decisions, 1000'i64)

    check ops.len == 2 # Only first 2 added
    check scheduler.queue.totalCount() == 2

    scheduler.destroy()
    alloc.destroy()
    pool.destroy()

  proc successCallback(op: RebalanceOp): bool {.gcsafe.} = true
  proc failCallback(op: RebalanceOp): bool {.gcsafe.} = false

  test "processBatch success":
    let pool = newStorePool()
    let alloc = newAllocator(pool)
    let scheduler = newRebalanceScheduler(alloc, successCallback)

    discard scheduler.addDecision(newAddReplicaDecision(genGroupIDLocal(), NodeID(1),
        10, "test"), 1000'i64)
    discard scheduler.addDecision(newAddReplicaDecision(genGroupIDLocal(), NodeID(2),
        5, "test2"), 1000'i64)

    let processed = scheduler.processBatch(2000'i64)
    check processed == 2
    check scheduler.opsExecuted.load() == 2
    check scheduler.opsSucceeded.load() == 2
    check scheduler.opsFailed.load() == 0
    check scheduler.queue.completed.len == 2

    scheduler.destroy()
    alloc.destroy()
    pool.destroy()

  test "processBatch failure":
    let pool = newStorePool()
    let alloc = newAllocator(pool)
    let scheduler = newRebalanceScheduler(alloc, failCallback)

    discard scheduler.addDecision(newAddReplicaDecision(genGroupIDLocal(), NodeID(1),
        10, "test"), 1000'i64)

    let processed = scheduler.processBatch(2000'i64)
    check processed == 1
    check scheduler.opsExecuted.load() == 1
    check scheduler.opsSucceeded.load() == 0
    check scheduler.opsFailed.load() == 1
    check scheduler.queue.completed.len == 1

    scheduler.destroy()
    alloc.destroy()
    pool.destroy()

  test "processBatch empty queue":
    let pool = newStorePool()
    let alloc = newAllocator(pool)
    let scheduler = newRebalanceScheduler(alloc, successCallback)

    let processed = scheduler.processBatch(2000'i64)
    check processed == 0

    scheduler.destroy()
    alloc.destroy()
    pool.destroy()

  test "processBatch respects batch size":
    let pool = newStorePool()
    let alloc = newAllocator(pool)
    var scheduler = newRebalanceScheduler(alloc, successCallback)
    scheduler.batchSize = 2

    for i in 1..5:
      discard scheduler.addDecision(newAddReplicaDecision(genGroupIDLocal(), NodeID(
          i.uint32), 10, "test"), 1000'i64)

    let processed = scheduler.processBatch(2000'i64)
    check processed == 2 # Only 2 processed
    check scheduler.queue.pendingCount() == 3 # 3 still pending

    scheduler.destroy()
    alloc.destroy()
    pool.destroy()

  test "checkGroupForRebalance":
    let pool = newStorePool()
    pool.addStore(newStoreStats(NodeID(1)))
    pool.addStore(newStoreStats(NodeID(2)))
    pool.addStore(newStoreStats(NodeID(3)))
    pool.addStore(newStoreStats(NodeID(4)))

    let alloc = newAllocator(pool, 3)
    let scheduler = newRebalanceScheduler(alloc, successCallback)

    let groupId = genGroupIDLocal()
    let replicas = @[newReplicaDescriptor(NodeID(1), ReplicaID(1), rtVoter)]

    let ops = scheduler.checkGroupForRebalance(groupId, replicas, NodeID(1), 1000'i64)
    check ops.len == 1 # Should add replica
    check scheduler.queue.pendingCount() == 1

    scheduler.destroy()
    alloc.destroy()
    pool.destroy()

  test "getStats":
    let pool = newStorePool()
    let alloc = newAllocator(pool)
    let scheduler = newRebalanceScheduler(alloc, successCallback)

    discard scheduler.addDecision(newAddReplicaDecision(genGroupIDLocal(), NodeID(1),
        10, "test"), 1000'i64)
    discard scheduler.processBatch(2000'i64)

    let stats = scheduler.getStats()
    check stats.pending == 0
    check stats.inProgress == 0
    check stats.executed == 1
    check stats.succeeded == 1
    check stats.failed == 0

    scheduler.destroy()
    alloc.destroy()
    pool.destroy()

suite "Rebalance Prioritization":

  proc dummyCallback(op: RebalanceOp): bool {.gcsafe.} = true

  test "prioritizeByLoad":
    let pool = newStorePool()
    let alloc = newAllocator(pool)
    let scheduler = newRebalanceScheduler(alloc, dummyCallback)

    var decisions = @[
      newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 5, "low"),
      newAddReplicaDecision(genGroupIDLocal(), NodeID(2), 10, "high"),
      newAddReplicaDecision(genGroupIDLocal(), NodeID(3), 7, "medium")
    ]

    scheduler.prioritizeByLoad(decisions)

    check decisions[0].priority == 10
    check decisions[1].priority == 7
    check decisions[2].priority == 5

    scheduler.destroy()
    alloc.destroy()
    pool.destroy()

  test "prioritizeByAge":
    let pool = newStorePool()
    let alloc = newAllocator(pool)
    let scheduler = newRebalanceScheduler(alloc, dummyCallback)

    var decisions = @[
      newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 3, "low"),
      newAddReplicaDecision(genGroupIDLocal(), NodeID(2), 10, "high")
    ]

    scheduler.prioritizeByAge(decisions)

    # Currently just sorts by priority
    check decisions[0].priority == 10

    scheduler.destroy()
    alloc.destroy()
    pool.destroy()

  test "filterHighPriority":
    let pool = newStorePool()
    let alloc = newAllocator(pool)
    let scheduler = newRebalanceScheduler(alloc, dummyCallback)

    let decisions = @[
      newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 10, "high"),
      newAddReplicaDecision(genGroupIDLocal(), NodeID(2), 5, "medium"),
      newAddReplicaDecision(genGroupIDLocal(), NodeID(3), 2, "low")
    ]

    let filtered = scheduler.filterHighPriority(decisions)
    check filtered.len == 1
    check filtered[0].priority == 10

    scheduler.destroy()
    alloc.destroy()
    pool.destroy()

  test "filterLowPriority":
    let pool = newStorePool()
    let alloc = newAllocator(pool)
    let scheduler = newRebalanceScheduler(alloc, dummyCallback)

    let decisions = @[
      newAddReplicaDecision(genGroupIDLocal(), NodeID(1), 10, "high"),
      newAddReplicaDecision(genGroupIDLocal(), NodeID(2), 5, "medium"),
      newAddReplicaDecision(genGroupIDLocal(), NodeID(3), 2, "low")
    ]

    let filtered = scheduler.filterLowPriority(decisions)
    check filtered.len == 1
    check filtered[0].priority == 2

    scheduler.destroy()
    alloc.destroy()
    pool.destroy()

suite "RebalanceConstraints":

  test "defaultRebalanceConstraints":
    let c = defaultRebalanceConstraints()
    check c.maxConcurrentMoves == 5
    check c.maxConcurrentTransfers == 10
    check c.minIntervalNs == 10_000_000_000'i64
    check c.forbiddenGroups.len == 0

  test "canRebalance true":
    let c = defaultRebalanceConstraints()
    check c.canRebalance(genGroupIDLocal())

  test "canRebalance false for forbidden":
    let groupId = genGroupIDLocal()
    let c = defaultRebalanceConstraints().withForbiddenGroups(@[groupId])
    check not c.canRebalance(groupId)

  test "canRebalance true for non-forbidden":
    let groupId1 = genGroupIDLocal()
    let groupId2 = genGroupIDLocal()
    let c = defaultRebalanceConstraints().withForbiddenGroups(@[groupId1])
    check c.canRebalance(groupId2)

  test "withForbiddenGroups":
    let group1 = genGroupIDLocal()
    let group2 = genGroupIDLocal()
    let c = defaultRebalanceConstraints().withForbiddenGroups(@[group1, group2])
    check c.forbiddenGroups.len == 2
    check group1 in c.forbiddenGroups
    check group2 in c.forbiddenGroups

suite "Constants":

  test "DEFAULT_REBALANCE_INTERVAL_NS":
    check DEFAULT_REBALANCE_INTERVAL_NS == 60_000_000_000'i64

  test "DEFAULT_BATCH_SIZE":
    check DEFAULT_BATCH_SIZE == 10

  test "DEFAULT_MAX_PENDING":
    check DEFAULT_MAX_PENDING == 50

  test "HIGH_PRIORITY_THRESHOLD":
    check HIGH_PRIORITY_THRESHOLD == 8

  test "LOW_PRIORITY_THRESHOLD":
    check LOW_PRIORITY_THRESHOLD == 3
