# Range Split Implementation
#
# This module implements range splitting:
# - Split trigger detection
# - Split key calculation
# - Split execution via Raft
# - Meta range updates

import std/options
import std/tables
import std/locks
import std/atomics
import std/algorithm

import fractio/distributed/range/types
import fractio/distributed/rebalance/allocator
import fractio/distributed/meta/lookup

# ============================================================================
# Constants
# ============================================================================

const
  SPLIT_THRESHOLD_BYTES* = MAX_RANGE_SIZE_BYTES
    ## Size threshold to trigger split

  SPLIT_THRESHOLD_KEYS* = 1_000_000
    ## Key count threshold to trigger split

  MIN_SPLIT_SIZE_BYTES* = MIN_RANGE_SIZE_BYTES * 2
    ## Minimum size for a valid split

  SPLIT_COOLDOWN_NS* = 60_000_000_000'i64
    ## Cooldown period between splits: 60 seconds

# ============================================================================
# Split Statistics
# ============================================================================

type
  RangeStats* = object
    ## Statistics for a range
    rangeId*: RangeID
    totalBytes*: int64
      ## Total data size
    keyCount*: int64
      ## Number of keys
    writeKeysPerSecond*: float64
      ## Write rate
    readKeysPerSecond*: float64
      ## Read rate
    lastSplitNs*: int64
      ## Last split timestamp

proc newRangeStats*(rangeId: RangeID): RangeStats =
  ## Create new range statistics
  result = RangeStats(
    rangeId: rangeId,
    totalBytes: 0,
    keyCount: 0,
    writeKeysPerSecond: 0.0,
    readKeysPerSecond: 0.0,
    lastSplitNs: 0
  )

proc shouldSplit*(stats: RangeStats, nowNs: int64): bool =
  ## Check if a range should be split
  if stats.totalBytes >= SPLIT_THRESHOLD_BYTES:
    return true
  if stats.keyCount >= SPLIT_THRESHOLD_KEYS:
    return true
  return false

proc canSplit*(stats: RangeStats, nowNs: int64): bool =
  ## Check if a range can be split (cooldown check)
  if stats.lastSplitNs > 0:
    let elapsed = nowNs - stats.lastSplitNs
    if elapsed < SPLIT_COOLDOWN_NS:
      return false
  return true

# ============================================================================
# Split Decision
# ============================================================================

type
  SplitDecision* = object
    ## Decision to split a range
    rangeId*: RangeID
    splitKey*: seq[byte]
      ## The key to split at
    leftRangeId*: RangeID
      ## New range ID for left half
    rightRangeId*: RangeID
      ## New range ID for right half
    priority*: int
    reason*: string

proc newSplitDecision*(rangeId: RangeID, splitKey: seq[byte],
                       leftRangeId, rightRangeId: RangeID,
                       priority: int, reason: string): SplitDecision =
  ## Create a new split decision
  result = SplitDecision(
    rangeId: rangeId,
    splitKey: splitKey,
    leftRangeId: leftRangeId,
    rightRangeId: rightRangeId,
    priority: priority,
    reason: reason
  )

# ============================================================================
# Split Key Calculation
# ============================================================================

proc calculateSplitKey*(startKey, endKey: seq[byte],
                        sampleKeys: seq[seq[byte]]): seq[byte] =
  ## Calculate the best split key
  ## Uses sample keys to find a balanced split point

  if sampleKeys.len == 0:
    # No samples, use midpoint
    return splitKey(startKey, endKey)

  # Sort samples
  var sorted = sampleKeys
  sorted.sort(proc(a, b: seq[byte]): int =
    if a < b: -1 elif a > b: 1 else: 0)

  # Find median
  let mid = sorted.len div 2
  result = sorted[mid]

  # Ensure it's within the range
  if result <= startKey:
    result = splitKey(startKey, endKey)
  elif endKey.len > 0 and result >= endKey:
    result = splitKey(startKey, endKey)

proc calculateSplitKeyBySize*(startKey, endKey: seq[byte],
                               leftSize, rightSize: int64): seq[byte] =
  ## Calculate split key given size distribution
  ## Adjusts split point to balance sizes

  # For now, use simple midpoint
  # In production, would use more sophisticated algorithms
  result = splitKey(startKey, endKey)

# ============================================================================
# Split Executor
# ============================================================================

type
  SplitExecutor* = ref object
    ## Executes range splits
    nextRangeId*: Atomic[uint64]
    lock*: Lock

proc newSplitExecutor*(startRangeId: uint64 = 1): SplitExecutor =
  ## Create a new split executor
  new(result)
  result.nextRangeId.store(startRangeId)
  initLock(result.lock)

proc destroy*(executor: SplitExecutor) =
  ## Clean up resources
  deinitLock(executor.lock)

proc allocateRangeId*(executor: SplitExecutor): RangeID =
  ## Allocate a new range ID
  let id = executor.nextRangeId.fetchAdd(1)
  result = RangeID(id)

proc createSplitDescriptor*(executor: SplitExecutor,
                            original: RangeDescriptor,
                            splitKey: seq[byte],
                            leftRangeId, rightRangeId: RangeID): tuple[
                            left, right: RangeDescriptor] =
  ## Create descriptors for the two new ranges after a split

  # Left range: startKey -> splitKey
  result.left = newRangeDescriptor(
    leftRangeId,
    original.startKey,
    splitKey,
    original.replicas
  )
  result.left.generation = original.generation + 1

  # Right range: splitKey -> endKey
  result.right = newRangeDescriptor(
    rightRangeId,
    splitKey,
    original.endKey,
    original.replicas
  )
  result.right.generation = original.generation + 1

proc proposeSplit*(executor: SplitExecutor,
                   rangeId: RangeID,
                   splitKey: seq[byte],
                   nowNs: int64): SplitDecision =
  ## Propose a split operation
  let leftRangeId = executor.allocateRangeId()
  let rightRangeId = executor.allocateRangeId()

  result = newSplitDecision(
    rangeId,
    splitKey,
    leftRangeId,
    rightRangeId,
    10, # High priority
    "Range size exceeds threshold"
  )

# ============================================================================
# Split Trigger
# ============================================================================

type
  SplitTrigger* = ref object
    ## Detects when ranges need to be split
    executor*: SplitExecutor
    stats*: Table[RangeID, RangeStats]
    lock*: Lock

proc newSplitTrigger*(executor: SplitExecutor): SplitTrigger =
  ## Create a new split trigger
  new(result)
  result.executor = executor
  result.stats = initTable[RangeID, RangeStats]()
  initLock(result.lock)

proc destroy*(trigger: SplitTrigger) =
  ## Clean up resources
  deinitLock(trigger.lock)

proc updateStats*(trigger: SplitTrigger, stats: RangeStats) =
  ## Update statistics for a range
  withLock trigger.lock:
    trigger.stats[stats.rangeId] = stats

proc removeStats*(trigger: SplitTrigger, rangeId: RangeID) =
  ## Remove statistics for a range
  withLock trigger.lock:
    trigger.stats.del(rangeId)

proc checkForSplits*(trigger: SplitTrigger, nowNs: int64): seq[SplitDecision] =
  ## Check all ranges for split opportunities
  withLock trigger.lock:
    for rangeId, stats in trigger.stats:
      if stats.shouldSplit(nowNs) and stats.canSplit(nowNs):
        # Calculate split key (would use actual data in production)
        let splitKey = splitKey(@[byte(0)], @[byte(100)]) # Placeholder
        result.add(trigger.executor.proposeSplit(rangeId, splitKey, nowNs))

proc checkRangeForSplit*(trigger: SplitTrigger,
                         rangeId: RangeID,
                         startKey, endKey: seq[byte],
                         nowNs: int64): Option[SplitDecision] =
  ## Check a specific range for split
  withLock trigger.lock:
    if trigger.stats.contains(rangeId):
      let stats = trigger.stats[rangeId]
      if stats.shouldSplit(nowNs) and stats.canSplit(nowNs):
        let splitKey = splitKey(startKey, endKey)
        return some(trigger.executor.proposeSplit(rangeId, splitKey, nowNs))

  return none(SplitDecision)

# ============================================================================
# Split Validation
# ============================================================================

proc validateSplit*(original: RangeDescriptor,
                    left, right: RangeDescriptor): bool =
  ## Validate that a split is correct

  # Check that ranges are contiguous
  if left.endKey != right.startKey:
    return false

  # Check that ranges cover original
  if left.startKey != original.startKey:
    return false
  if right.endKey != original.endKey:
    return false

  # Check that replicas are preserved
  if left.replicas.len != original.replicas.len:
    return false
  if right.replicas.len != original.replicas.len:
    return false

  return true

proc isSplitKeyValid*(splitKey, startKey, endKey: seq[byte]): bool =
  ## Check if a split key is valid for a range
  if splitKey <= startKey:
    return false
  if endKey.len > 0 and splitKey >= endKey:
    return false
  return true
