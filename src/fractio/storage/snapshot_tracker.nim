# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Snapshot Tracker
##
## Tracks open snapshots for MVCC (Multi-Version Concurrency Control).

import fractio/storage/types
import std/[atomics, tables, locks]

# Forward declarations
type
  SequenceNumberCounter* = ref object
    value*: Atomic[SeqNo]

  SnapshotTracker* = ref object
    inner*: SnapshotTrackerInner

  SnapshotTrackerInner* = ref object
    seqno*: SequenceNumberCounter
    gcLock*: Lock            # Using simple lock instead of RWLock for now
    data*: Table[SeqNo, int] # Maps sequence numbers to reference counts
    freedCount*: Atomic[uint64]
    lowestFreedInstant*: Atomic[SeqNo]

  SnapshotNonce* = ref object
    instant*: SeqNo
    tracker*: SnapshotTracker

# Forward declarations for recursive functions
proc gc*(tracker: SnapshotTracker)

# Constructor for SequenceNumberCounter
proc newSequenceNumberCounter*(): SequenceNumberCounter =
  result = SequenceNumberCounter()
  result.value.store(0.SeqNo, moRelaxed)

# Get current sequence number
proc get*(counter: SequenceNumberCounter): SeqNo =
  counter.value.load(moAcquire)

# Get next sequence number
proc next*(counter: SequenceNumberCounter): SeqNo =
  counter.value.fetchAdd(1, moRelaxed) + 1

# Fetch max
proc fetchMax*(counter: SequenceNumberCounter, value: SeqNo) =
  var current = counter.value.load(moAcquire)
  while value > current:
    if counter.value.compareExchange(current, value, moAcquire, moAcquire):
      break
    current = counter.value.load(moAcquire)

# Constructor for SnapshotNonce
proc newSnapshotNonce*(seqno: SeqNo, tracker: SnapshotTracker): SnapshotNonce =
  SnapshotNonce(instant: seqno, tracker: tracker)

# Constructor for SnapshotTracker
proc newSnapshotTracker*(seqno: SequenceNumberCounter): SnapshotTracker =
  var freedCount: Atomic[uint64]
  freedCount.store(0, moRelaxed)
  var lowestFreedInstant: Atomic[SeqNo]
  lowestFreedInstant.store(0.SeqNo, moRelaxed)

  let inner = SnapshotTrackerInner(
    seqno: seqno,
    data: initTable[SeqNo, int](),
    freedCount: freedCount,
    lowestFreedInstant: lowestFreedInstant
  )
  initLock(inner.gcLock)
  SnapshotTracker(inner: inner)

# Constructor for SnapshotTracker with its own counter
proc newSnapshotTracker*(): SnapshotTracker =
  newSnapshotTracker(newSequenceNumberCounter())

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
  # Acquire lock
  tracker.inner.gcLock.acquire()
  defer: tracker.inner.gcLock.release()

  let seqno = tracker.inner.seqno.get()

  if seqno in tracker.inner.data:
    tracker.inner.data[seqno] += 1
  else:
    tracker.inner.data[seqno] = 1

  newSnapshotNonce(seqno, tracker)

# Clone a snapshot
proc cloneSnapshot*(tracker: SnapshotTracker,
    nonce: SnapshotNonce): SnapshotNonce =
  # Acquire lock
  tracker.inner.gcLock.acquire()
  defer: tracker.inner.gcLock.release()

  if nonce.instant in tracker.inner.data:
    tracker.inner.data[nonce.instant] += 1
  else:
    tracker.inner.data[nonce.instant] = 1

  newSnapshotNonce(nonce.instant, tracker)

# Close a snapshot by sequence number (defined before close)
proc closeRaw*(tracker: SnapshotTracker, instant: SeqNo) =
  # Acquire lock
  tracker.inner.gcLock.acquire()
  defer: tracker.inner.gcLock.release()

  if instant in tracker.inner.data:
    tracker.inner.data[instant] = max(0, tracker.inner.data[instant] - 1)

  let freed = tracker.inner.freedCount.fetchAdd(1, moRelaxed) + 1

  # Every 10,000 freed snapshots, run garbage collection
  if freed mod 10000 == 0:
    tracker.gc()

# Close a snapshot
proc close*(tracker: SnapshotTracker, nonce: SnapshotNonce) =
  closeRaw(tracker, nonce.instant)

# Publish write completion
proc publish*(tracker: SnapshotTracker, batchSeqno: SeqNo) =
  tracker.inner.seqno.fetchMax(batchSeqno + 1)

# Get sequence number safe for garbage collection
proc getSeqnoSafeToGc*(tracker: SnapshotTracker): SeqNo =
  tracker.inner.lowestFreedInstant.load(moAcquire)

# Pull up the watermark
proc pullup*(tracker: SnapshotTracker) =
  # Acquire lock
  tracker.inner.gcLock.acquire()
  defer: tracker.inner.gcLock.release()

  if tracker.inner.data.len == 0:
    let current = tracker.inner.seqno.get()
    if current > 0:
      tracker.inner.lowestFreedInstant.store(current - 1, moRelease)

# Garbage collection
proc gc*(tracker: SnapshotTracker) =
  # Acquire lock
  tracker.inner.gcLock.acquire()
  defer: tracker.inner.gcLock.release()

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

  # Update lowestFreedInstant if we have a valid value
  if lowestRetained > 0:
    let newValue = lowestRetained - 1
    var current = tracker.inner.lowestFreedInstant.load(moAcquire)
    while newValue > current:
      if tracker.inner.lowestFreedInstant.compareExchange(current, newValue,
          moAcquire, moAcquire):
        break
      current = tracker.inner.lowestFreedInstant.load(moAcquire)
