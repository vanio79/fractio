# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import fractio/storage/[types, snapshot_nonce]
import std/[atomics, tables, locks]

# Forward declarations
type
  SequenceNumberCounter* = object
    value: Atomic[SeqNo]

# Keeps track of open snapshots
type
  SnapshotTrackerInner* = ref object
    seqno*: SequenceNumberCounter
    gcLock*: RWLock
    data*: Table[SeqNo, int] # Maps sequence numbers to reference counts
    freedCount*: Atomic[uint64]
    lowestFreedInstant*: Atomic[SeqNo]

  SnapshotTracker* = ref object
    inner*: SnapshotTrackerInner

# Constructor for SequenceNumberCounter
proc newSequenceNumberCounter*(): SequenceNumberCounter =
  SequenceNumberCounter(value: Atomic[SeqNo](0))

# Get current sequence number
proc get*(counter: SequenceNumberCounter): SeqNo =
  counter.value.load(moAcquire)

# Get next sequence number
proc next*(counter: var SequenceNumberCounter): SeqNo =
  counter.value.fetchAdd(1, moAcqRel) + 1

# Fetch max
proc fetchMax*(counter: var SequenceNumberCounter, value: SeqNo) =
  var current = counter.value.load(moAcquire)
  while value > current:
    if counter.value.compareExchange(current, value, moAcqRel, moAcquire):
      break
    current = counter.value.load(moAcquire)

# Constructor for SnapshotTracker
proc newSnapshotTracker*(seqno: SequenceNumberCounter): SnapshotTracker =
  let inner = SnapshotTrackerInner(
    seqno: seqno,
    data: initTable[SeqNo, int](),
    freedCount: Atomic[uint64](0),
    lowestFreedInstant: Atomic[SeqNo](0)
  )
  initRWLock(inner.gcLock)
  SnapshotTracker(inner: inner)

# Get reference to sequence number counter
proc getRef*(tracker: SnapshotTracker): SequenceNumberCounter =
  tracker.inner.seqno

# Get current sequence number
proc get*(tracker: SnapshotTracker): SeqNo =
  tracker.inner.seqno.get()

# Set sequence number (used in recovery)
proc set*(tracker: SnapshotTracker, value: SeqNo) =
  tracker.inner.seqno.fetchMax(value)

# Get number of unique sequence numbers being tracked
proc len*(tracker: SnapshotTracker): int =
  tracker.inner.data.len

# Get total number of open snapshots
proc openSnapshots*(tracker: SnapshotTracker): int =
  var total = 0
  for count in tracker.inner.data.values:
    total += count
  return total

# Open a new snapshot
proc open*(tracker: SnapshotTracker): SnapshotNonce =
  # Acquire read lock
  tracker.inner.gcLock.readLock()
  defer: tracker.inner.gcLock.readUnlock()

  let seqno = tracker.inner.seqno.get()

  if seqno in tracker.inner.data:
    tracker.inner.data[seqno] += 1
  else:
    tracker.inner.data[seqno] = 1

  newSnapshotNonce(seqno, tracker[])

# Clone a snapshot
proc cloneSnapshot*(tracker: SnapshotTracker,
    nonce: SnapshotNonce): SnapshotNonce =
  # Acquire read lock
  tracker.inner.gcLock.readLock()
  defer: tracker.inner.gcLock.readUnlock()

  if nonce.instant in tracker.inner.data:
    tracker.inner.data[nonce.instant] += 1
  else:
    tracker.inner.data[nonce.instant] = 1

  newSnapshotNonce(nonce.instant, tracker[])

# Close a snapshot
proc close*(tracker: SnapshotTracker, nonce: SnapshotNonce) =
  tracker.closeRaw(nonce.instant)

# Close a snapshot by sequence number
proc closeRaw*(tracker: SnapshotTracker, instant: SeqNo) =
  # Acquire read lock
  tracker.inner.gcLock.readLock()
  defer: tracker.inner.gcLock.readUnlock()

  if instant in tracker.inner.data:
    tracker.inner.data[instant] = max(0, tracker.inner.data[instant] - 1)

  let freed = tracker.inner.freedCount.fetchAdd(1, moAcqRel) + 1

  # Every 10,000 freed snapshots, run garbage collection
  if freed mod 10000 == 0:
    tracker.gc()

# Publish write completion
proc publish*(tracker: SnapshotTracker, batchSeqno: SeqNo) =
  tracker.inner.seqno.fetchMax(batchSeqno + 1)

# Get sequence number safe for garbage collection
proc getSeqnoSafeToGc*(tracker: SnapshotTracker): SeqNo =
  tracker.inner.lowestFreedInstant.load(moAcquire)

# Pull up the watermark
proc pullup*(tracker: SnapshotTracker) =
  # Acquire write lock
  tracker.inner.gcLock.writeLock()
  defer: tracker.inner.gcLock.writeUnlock()

  if tracker.inner.data.len == 0:
    let current = tracker.inner.seqno.get()
    tracker.inner.lowestFreedInstant.store(max(0, current - 1), moRelease)

# Garbage collection
proc gc*(tracker: SnapshotTracker) =
  # Acquire write lock
  tracker.inner.gcLock.writeLock()
  defer: tracker.inner.gcLock.writeUnlock()

  let seqnoThreshold = tracker.inner.seqno.get()

  var lowestRetained: SeqNo = 0
  var noneRetained = true

  # Remove entries with zero count and below threshold
  var keysToRemove: seq[SeqNo] = @[]
  for key, value in tracker.inner.data.pairs:
    let shouldBeRetained = value > 0 or key >= seqnoThreshold

    if shouldBeRetained:
      lowestRetained = if lowestRetained == 0: key else: min(lowestRetained, key)
      noneRetained = false
    else:
      keysToRemove.add(key)

  # Actually remove the keys
  for key in keysToRemove:
    tracker.inner.data.del(key)

  if noneRetained:
    lowestRetained = seqnoThreshold

  discard tracker.inner.lowestFreedInstant.fetchMax(max(0, lowestRetained - 1), moAcqRel)
