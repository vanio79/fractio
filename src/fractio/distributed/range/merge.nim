# Range Merge Implementation
#
# This module implements range merging:
# - Merge trigger detection
# - Adjacent range finding
# - Merge execution via Raft
# - Meta range updates

import std/options
import std/tables
import std/locks
import std/atomics

import fractio/distributed/range/types
import fractio/distributed/rebalance/allocator

# ============================================================================
# Constants
# ============================================================================

const
  MERGE_THRESHOLD_BYTES* = MIN_RANGE_SIZE_BYTES
    ## Size threshold to trigger merge

  MERGE_THRESHOLD_KEYS* = 100_000
    ## Key count threshold to trigger merge

  MERGE_COOLDOWN_NS* = 120_000_000_000'i64
    ## Cooldown period between merges: 120 seconds

  MAX_MERGE_SIZE_BYTES* = DEFAULT_TARGET_SIZE_BYTES div 2
    ## Maximum size for merged range

# ============================================================================
# Merge Statistics
# ============================================================================

type
  MergeStats* = object
    ## Statistics for merge evaluation
    rangeId*: RangeID
    totalBytes*: int64
    keyCount*: int64
    lastMergeNs*: int64

proc newMergeStats*(rangeId: RangeID): MergeStats =
  ## Create new merge statistics
  result = MergeStats(
    rangeId: rangeId,
    totalBytes: 0,
    keyCount: 0,
    lastMergeNs: 0
  )

proc shouldMerge*(stats: MergeStats, nowNs: int64): bool =
  ## Check if a range should be merged
  if stats.totalBytes <= MERGE_THRESHOLD_BYTES:
    return true
  if stats.keyCount <= MERGE_THRESHOLD_KEYS:
    return true
  return false

proc canMerge*(stats: MergeStats, nowNs: int64): bool =
  ## Check if a range can be merged (cooldown check)
  if stats.lastMergeNs > 0:
    let elapsed = nowNs - stats.lastMergeNs
    if elapsed < MERGE_COOLDOWN_NS:
      return false
  return true

# ============================================================================
# Merge Decision
# ============================================================================

type
  MergeDecision* = object
    ## Decision to merge two ranges
    leftRangeId*: RangeID
      ## Left range (will absorb right)
    rightRangeId*: RangeID
      ## Right range (will be absorbed)
    mergedRangeId*: RangeID
      ## Resulting range ID (usually left)
    priority*: int
    reason*: string

proc newMergeDecision*(left, right, merged: RangeID,
                       priority: int, reason: string): MergeDecision =
  ## Create a new merge decision
  result = MergeDecision(
    leftRangeId: left,
    rightRangeId: right,
    mergedRangeId: merged,
    priority: priority,
    reason: reason
  )

# ============================================================================
# Merge Executor
# ============================================================================

type
  MergeExecutor* = ref object
    ## Executes range merges
    lock*: Lock

proc newMergeExecutor*(): MergeExecutor =
  ## Create a new merge executor
  new(result)
  initLock(result.lock)

proc destroy*(executor: MergeExecutor) =
  ## Clean up resources
  deinitLock(executor.lock)

proc createMergedDescriptor*(executor: MergeExecutor,
                             left, right: RangeDescriptor): RangeDescriptor =
  ## Create a descriptor for the merged range

  # Validate that ranges are adjacent
  if left.endKey != right.startKey:
    raise newException(ValueError, "Ranges are not adjacent")

  # Create merged descriptor
  result = newRangeDescriptor(
    left.rangeId,
    left.startKey,
    right.endKey,
    left.replicas # Use left's replicas
  )
  result.generation = max(left.generation, right.generation) + 1

proc proposeMerge*(executor: MergeExecutor,
                   left, right: RangeDescriptor): MergeDecision =
  ## Propose a merge operation

  # Validate adjacent ranges
  if left.endKey != right.startKey:
    raise newException(ValueError, "Ranges are not adjacent")

  # Check combined size
  # In production, would check actual sizes
  # For now, assume it's valid

  result = newMergeDecision(
    left.rangeId,
    right.rangeId,
    left.rangeId, # Keep left's ID
    5, # Lower priority than splits
    "Range below size threshold"
  )

# ============================================================================
# Merge Trigger
# ============================================================================

type
  MergeTrigger* = ref object
    ## Detects when ranges need to be merged
    executor*: MergeExecutor
    stats*: Table[RangeID, MergeStats]
    descriptors*: Table[RangeID, RangeDescriptor]
    lock*: Lock

proc newMergeTrigger*(executor: MergeExecutor): MergeTrigger =
  ## Create a new merge trigger
  new(result)
  result.executor = executor
  result.stats = initTable[RangeID, MergeStats]()
  result.descriptors = initTable[RangeID, RangeDescriptor]()
  initLock(result.lock)

proc destroy*(trigger: MergeTrigger) =
  ## Clean up resources
  deinitLock(trigger.lock)

proc updateStats*(trigger: MergeTrigger, stats: MergeStats) =
  ## Update statistics for a range
  withLock trigger.lock:
    trigger.stats[stats.rangeId] = stats

proc updateDescriptor*(trigger: MergeTrigger, desc: RangeDescriptor) =
  ## Update descriptor for a range
  withLock trigger.lock:
    trigger.descriptors[desc.rangeId] = desc

proc removeRange*(trigger: MergeTrigger, rangeId: RangeID) =
  ## Remove a range from tracking
  withLock trigger.lock:
    trigger.stats.del(rangeId)
    trigger.descriptors.del(rangeId)

proc findAdjacentRange*(trigger: MergeTrigger,
                        rangeId: RangeID): Option[RangeDescriptor] =
  ## Find an adjacent range that could be merged
  withLock trigger.lock:
    if not trigger.descriptors.contains(rangeId):
      return none(RangeDescriptor)

    let desc = trigger.descriptors[rangeId]

    # Look for range that starts where this one ends
    for otherId, other in trigger.descriptors:
      if otherId == rangeId:
        continue

      # Check if other range starts where this one ends
      if other.startKey == desc.endKey:
        return some(other)

      # Check if this range starts where other ends
      if desc.startKey == other.endKey:
        return some(other)

  return none(RangeDescriptor)

proc checkForMerges*(trigger: MergeTrigger, nowNs: int64): seq[MergeDecision] =
  ## Check all ranges for merge opportunities
  withLock trigger.lock:
    for rangeId, stats in trigger.stats:
      if not stats.shouldMerge(nowNs):
        continue
      if not stats.canMerge(nowNs):
        continue

      # Find adjacent range
      let adjacentOpt = trigger.findAdjacentRange(rangeId)
      if adjacentOpt.isNone:
        continue

      let adjacent = adjacentOpt.get

      # Check if we have stats for adjacent range
      if not trigger.stats.contains(adjacent.rangeId):
        continue

      let adjacentStats = trigger.stats[adjacent.rangeId]
      if not adjacentStats.shouldMerge(nowNs):
        continue

      # Propose merge
      let left = trigger.descriptors.getOrDefault(rangeId)
      let right = adjacent

      # Determine which is left
      var mergeDecision: MergeDecision
      if left.startKey < right.startKey:
        mergeDecision = trigger.executor.proposeMerge(left, right)
      else:
        mergeDecision = trigger.executor.proposeMerge(right, left)

      result.add(mergeDecision)

# ============================================================================
# Merge Validation
# ============================================================================

proc validateMerge*(left, right, merged: RangeDescriptor): bool =
  ## Validate that a merge is correct

  # Check that merged range covers both
  if merged.startKey != left.startKey:
    return false
  if merged.endKey != right.endKey:
    return false

  # Check that ranges are adjacent
  if left.endKey != right.startKey:
    return false

  # Check that replicas are preserved
  if merged.replicas.len != left.replicas.len:
    return false

  return true

proc canMergeRanges*(left, right: RangeDescriptor): bool =
  ## Check if two ranges can be merged

  # Must be adjacent
  if left.endKey != right.startKey:
    return false

  # Must have same replicas (for now)
  if left.replicas.len != right.replicas.len:
    return false

  return true

# ============================================================================
# Merge Priority
# ============================================================================

proc calculateMergePriority*(leftStats, rightStats: MergeStats): int =
  ## Calculate priority for a merge operation
  ## Higher priority = more urgent

  # Lower size = higher priority
  let totalBytes = leftStats.totalBytes + rightStats.totalBytes
  if totalBytes < MERGE_THRESHOLD_BYTES:
    result = 8 # High priority
  elif totalBytes < MERGE_THRESHOLD_BYTES * 2:
    result = 5 # Medium priority
  else:
    result = 2 # Low priority
