# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Compaction Worker
##
## Performs background compaction of SSTables.

import fractio/storage/[error, snapshot_tracker, stats, logging]
import fractio/storage/keyspace as ks
import fractio/storage/lsm_tree/lsm_tree
import std/[times, atomics, os]

# Runs a single run of compaction
proc run*(keyspace: ks.Keyspace,
          snapshotTracker: SnapshotTracker,
          stats: var Stats): StorageResult[void] =
  ## Runs compaction on a keyspace.

  if keyspace == nil:
    logInfo("Compaction: keyspace is nil, skipping")
    return okVoid

  if keyspace.inner == nil:
    logInfo("Compaction: keyspace.inner is nil, skipping")
    return okVoid

  # Check if keyspace is deleted
  if keyspace.inner.isDeleted.load(moRelaxed):
    logInfo("Compaction: keyspace is deleted, skipping")
    return okVoid

  # Increment active compaction count
  discard fetchAdd(stats.activeCompactionCount, 1, moRelaxed)

  let start = getTime()

  # Get GC watermark
  let gcWatermark = snapshotTracker.getSeqnoSafeToGc()

  logInfo("Compaction: starting on keyspace " & keyspace.inner.name &
      " with gcWatermark=" & $gcWatermark)

  # Run compaction on the tree
  let compactResult = keyspace.inner.tree.majorCompact(0'u64, gcWatermark)

  # Calculate elapsed time
  let elapsed = getTime() - start
  let elapsedMicros = elapsed.inMicroseconds

  # Update stats
  discard fetchAdd(stats.timeCompacting, uint64(elapsedMicros), moRelaxed)
  discard fetchSub(stats.activeCompactionCount, 1, moRelaxed)

  if compactResult.isErr:
    logError("Compaction failed: " & $compactResult.error)
    discard fetchAdd(stats.compactionsCompleted, 1, moRelaxed)
    return compactResult

  discard fetchAdd(stats.compactionsCompleted, 1, moRelaxed)
  logInfo("Compaction: completed in " & $elapsedMicros & "us")

  # Small delay to avoid compaction storm
  sleep(1)

  return okVoid
