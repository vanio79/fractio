# Rebalance Scheduler
#
# This module implements the rebalance scheduler that:
# - Periodically checks for rebalancing opportunities
# - Prioritizes and executes allocation decisions
# - Coordinates with Raft for configuration changes

import std/options
import std/tables
import std/locks
import std/atomics
import std/sequtils
import std/algorithm
import std/times

import fractio/distributed/range/types
import fractio/distributed/rebalance/allocator

# ============================================================================
# Constants
# ============================================================================

const
  DEFAULT_REBALANCE_INTERVAL_NS* = 60_000_000_000'i64
    ## Default rebalance check interval: 60 seconds

  DEFAULT_BATCH_SIZE* = 10
    ## Maximum number of rebalance operations per batch

  DEFAULT_MAX_PENDING* = 50
    ## Maximum pending rebalance operations

  HIGH_PRIORITY_THRESHOLD* = 8
    ## Priority threshold for high-priority operations

  LOW_PRIORITY_THRESHOLD* = 3
    ## Priority threshold for low-priority operations

# ============================================================================
# Rebalance Operation
# ============================================================================

type
  RebalanceOpState* = enum
    ## State of a rebalance operation
    rosPending
    rosInProgress
    rosCompleted
    rosFailed
    rosCancelled

  RebalanceOp* = ref object
    ## A single rebalance operation
    id*: int64
      ## Unique operation ID
    decision*: AllocationDecision
      ## The allocation decision
    state*: RebalanceOpState
      ## Current state
    createdAtNs*: int64
      ## When this operation was created
    startedAtNs*: int64
      ## When this operation was started
    completedAtNs*: int64
      ## When this operation completed
    attempts*: int
      ## Number of attempts
    lastError*: string
      ## Last error message

proc newRebalanceOp*(id: int64, decision: AllocationDecision,
                     nowNs: int64): RebalanceOp =
  ## Create a new rebalance operation
  new(result)
  result.id = id
  result.decision = decision
  result.state = rosPending
  result.createdAtNs = nowNs
  result.startedAtNs = 0
  result.completedAtNs = 0
  result.attempts = 0
  result.lastError = ""

proc markInProgress*(op: RebalanceOp, nowNs: int64) =
  ## Mark operation as in progress
  op.state = rosInProgress
  op.startedAtNs = nowNs
  inc op.attempts

proc markCompleted*(op: RebalanceOp, nowNs: int64) =
  ## Mark operation as completed
  op.state = rosCompleted
  op.completedAtNs = nowNs

proc markFailed*(op: RebalanceOp, nowNs: int64, error: string) =
  ## Mark operation as failed
  op.state = rosFailed
  op.completedAtNs = nowNs
  op.lastError = error

proc markCancelled*(op: RebalanceOp, nowNs: int64) =
  ## Mark operation as cancelled
  op.state = rosCancelled
  op.completedAtNs = nowNs

proc ageNs*(op: RebalanceOp, nowNs: int64): int64 =
  ## Get the age of this operation
  nowNs - op.createdAtNs

proc durationNs*(op: RebalanceOp, nowNs: int64): int64 =
  ## Get the duration of this operation
  if op.completedAtNs > 0:
    op.completedAtNs - op.startedAtNs
  elif op.startedAtNs > 0:
    nowNs - op.startedAtNs
  else:
    0

# ============================================================================
# Rebalance Queue
# ============================================================================

type
  RebalanceQueue* = ref object
    ## Priority queue for rebalance operations
    pending*: seq[RebalanceOp]
    inProgress*: seq[RebalanceOp]
    completed*: seq[RebalanceOp]
    nextId*: int64
    lock*: Lock

proc newRebalanceQueue*(): RebalanceQueue =
  ## Create a new rebalance queue
  new(result)
  result.pending = @[]
  result.inProgress = @[]
  result.completed = @[]
  result.nextId = 1
  initLock(result.lock)

proc destroy*(queue: RebalanceQueue) =
  ## Clean up resources
  deinitLock(queue.lock)

proc enqueue*(queue: RebalanceQueue, decision: AllocationDecision,
              nowNs: int64): RebalanceOp =
  ## Add a new operation to the queue
  withLock queue.lock:
    let op = newRebalanceOp(queue.nextId, decision, nowNs)
    inc queue.nextId
    queue.pending.add(op)

    # Sort by priority (descending)
    queue.pending.sort(proc(a, b: RebalanceOp): int =
      cmp(b.decision.priority, a.decision.priority))

    result = op

proc dequeue*(queue: RebalanceQueue): Option[RebalanceOp] =
  ## Get the next pending operation
  withLock queue.lock:
    if queue.pending.len > 0:
      result = some(queue.pending[0])
      queue.pending.delete(0)
    else:
      result = none(RebalanceOp)

proc startOp*(queue: RebalanceQueue, op: RebalanceOp, nowNs: int64) =
  ## Mark an operation as started
  withLock queue.lock:
    op.markInProgress(nowNs)
    queue.inProgress.add(op)

proc completeOp*(queue: RebalanceQueue, op: RebalanceOp, nowNs: int64) =
  ## Mark an operation as completed
  withLock queue.lock:
    op.markCompleted(nowNs)
    let idx = queue.inProgress.find(op)
    if idx >= 0:
      queue.inProgress.delete(idx)
    queue.completed.add(op)

proc failOp*(queue: RebalanceQueue, op: RebalanceOp, nowNs: int64,
             error: string) =
  ## Mark an operation as failed
  withLock queue.lock:
    op.markFailed(nowNs, error)
    let idx = queue.inProgress.find(op)
    if idx >= 0:
      queue.inProgress.delete(idx)
    queue.completed.add(op)

proc cancelOp*(queue: RebalanceQueue, op: RebalanceOp, nowNs: int64) =
  ## Cancel an operation
  withLock queue.lock:
    op.markCancelled(nowNs)

    # Remove from pending if there
    let pendingIdx = queue.pending.find(op)
    if pendingIdx >= 0:
      queue.pending.delete(pendingIdx)
      return

    # Remove from inProgress if there
    let inProgressIdx = queue.inProgress.find(op)
    if inProgressIdx >= 0:
      queue.inProgress.delete(inProgressIdx)

    queue.completed.add(op)

proc pendingCount*(queue: RebalanceQueue): int =
  ## Get number of pending operations
  withLock queue.lock:
    result = queue.pending.len

proc inProgressCount*(queue: RebalanceQueue): int =
  ## Get number of in-progress operations
  withLock queue.lock:
    result = queue.inProgress.len

proc totalCount*(queue: RebalanceQueue): int =
  ## Get total number of operations
  withLock queue.lock:
    result = queue.pending.len + queue.inProgress.len

proc getStats*(queue: RebalanceQueue): tuple[pending, inProgress,
    completed: int] =
  ## Get queue statistics
  withLock queue.lock:
    result.pending = queue.pending.len
    result.inProgress = queue.inProgress.len
    result.completed = queue.completed.len

# ============================================================================
# Rebalance Scheduler
# ============================================================================

type
  RebalanceCallback* = proc(op: RebalanceOp): bool {.closure, gcsafe.}
    ## Callback to execute a rebalance operation

  RebalanceScheduler* = ref object
    ## Scheduler for rebalance operations
    allocator*: Allocator
    queue*: RebalanceQueue

    # Configuration
    rebalanceIntervalNs*: int64
    batchSize*: int
    maxPending*: int

    # Callbacks
    executeCallback*: RebalanceCallback

    # Statistics
    opsExecuted*: Atomic[int64]
    opsSucceeded*: Atomic[int64]
    opsFailed*: Atomic[int64]

    # State
    running*: Atomic[bool]
    lock*: Lock

proc newRebalanceScheduler*(alloc: Allocator,
                            callback: RebalanceCallback): RebalanceScheduler =
  ## Create a new rebalance scheduler
  new(result)
  result.allocator = alloc
  result.queue = newRebalanceQueue()
  result.rebalanceIntervalNs = DEFAULT_REBALANCE_INTERVAL_NS
  result.batchSize = DEFAULT_BATCH_SIZE
  result.maxPending = DEFAULT_MAX_PENDING
  result.executeCallback = callback
  result.opsExecuted.store(0)
  result.opsSucceeded.store(0)
  result.opsFailed.store(0)
  result.running.store(false)
  initLock(result.lock)

proc destroy*(scheduler: RebalanceScheduler) =
  ## Clean up resources
  scheduler.queue.destroy()
  deinitLock(scheduler.lock)

proc addDecision*(scheduler: RebalanceScheduler,
                  decision: AllocationDecision, nowNs: int64): Option[RebalanceOp] =
  ## Add a rebalance decision to the queue
  if scheduler.queue.totalCount() >= scheduler.maxPending:
    return none(RebalanceOp)

  let op = scheduler.queue.enqueue(decision, nowNs)
  return some(op)

proc addDecisions*(scheduler: RebalanceScheduler,
                   decisions: seq[AllocationDecision],
                   nowNs: int64): seq[RebalanceOp] =
  ## Add multiple decisions to the queue
  for decision in decisions:
    if scheduler.queue.totalCount() >= scheduler.maxPending:
      break
    result.add(scheduler.queue.enqueue(decision, nowNs))

proc processBatch*(scheduler: RebalanceScheduler, nowNs: int64): int =
  ## Process a batch of rebalance operations
  ## Returns the number of operations processed

  var processed = 0

  while processed < scheduler.batchSize:
    let opOpt = scheduler.queue.dequeue()
    if opOpt.isNone:
      break

    let op = opOpt.get
    scheduler.queue.startOp(op, nowNs)
    discard scheduler.opsExecuted.fetchAdd(1)

    # Execute the operation
    var success = false
    try:
      success = scheduler.executeCallback(op)
    except CatchableError:
      success = false

    if success:
      scheduler.queue.completeOp(op, nowNs)
      discard scheduler.opsSucceeded.fetchAdd(1)
    else:
      scheduler.queue.failOp(op, nowNs, "Execution failed")
      discard scheduler.opsFailed.fetchAdd(1)

    inc processed

  result = processed

proc checkRangeForRebalance*(scheduler: RebalanceScheduler,
                              rangeId: RangeID,
                              replicas: seq[ReplicaDescriptor],
                              leaseholder: NodeID,
                              nowNs: int64): seq[RebalanceOp] =
  ## Check a range for rebalancing and add decisions to queue

  let decisions = scheduler.allocator.shouldRebalance(rangeId, replicas, leaseholder)
  result = scheduler.addDecisions(decisions, nowNs)

proc getStats*(scheduler: RebalanceScheduler): tuple[
  pending: int, inProgress: int, executed: int64,
  succeeded: int64, failed: int64] =
  ## Get scheduler statistics
  let queueStats = scheduler.queue.getStats()
  result.pending = queueStats.pending
  result.inProgress = queueStats.inProgress
  result.executed = scheduler.opsExecuted.load()
  result.succeeded = scheduler.opsSucceeded.load()
  result.failed = scheduler.opsFailed.load()

# ============================================================================
# Rebalance Prioritization
# ============================================================================

proc prioritizeByLoad*(scheduler: RebalanceScheduler,
                       decisions: var seq[AllocationDecision]) =
  ## Sort decisions by load imbalance (most urgent first)
  decisions.sort(proc(a, b: AllocationDecision): int =
    cmp(b.priority, a.priority))

proc prioritizeByAge*(scheduler: RebalanceScheduler,
                      decisions: var seq[AllocationDecision]) =
  ## Sort decisions by age (oldest first)
  # This would require tracking creation time in decisions
  # For now, just sort by priority
  decisions.sort(proc(a, b: AllocationDecision): int =
    cmp(b.priority, a.priority))

proc filterHighPriority*(scheduler: RebalanceScheduler,
                         decisions: seq[AllocationDecision]): seq[
                             AllocationDecision] =
  ## Filter to only high-priority decisions
  for d in decisions:
    if d.priority >= HIGH_PRIORITY_THRESHOLD:
      result.add(d)

proc filterLowPriority*(scheduler: RebalanceScheduler,
                        decisions: seq[AllocationDecision]): seq[
                            AllocationDecision] =
  ## Filter to only low-priority decisions
  for d in decisions:
    if d.priority < LOW_PRIORITY_THRESHOLD:
      result.add(d)

# ============================================================================
# Rebalance Constraints
# ============================================================================

type
  RebalanceConstraints* = object
    ## Constraints for rebalancing
    maxConcurrentMoves*: int
      ## Maximum concurrent replica moves
    maxConcurrentTransfers*: int
      ## Maximum concurrent lease transfers
    minIntervalNs*: int64
      ## Minimum interval between operations on same range
    forbiddenRanges*: seq[RangeID]
      ## Ranges that cannot be rebalanced

proc defaultRebalanceConstraints*(): RebalanceConstraints =
  ## Create default rebalance constraints
  result = RebalanceConstraints(
    maxConcurrentMoves: 5,
    maxConcurrentTransfers: 10,
    minIntervalNs: 10_000_000_000'i64, # 10 seconds
    forbiddenRanges: @[]
  )

proc canRebalance*(constraints: RebalanceConstraints,
                   rangeId: RangeID): bool =
  ## Check if a range can be rebalanced
  result = rangeId notin constraints.forbiddenRanges

proc withForbiddenRanges*(c: RebalanceConstraints,
                          ranges: seq[RangeID]): RebalanceConstraints =
  ## Set forbidden ranges
  result = c
  result.forbiddenRanges = ranges
