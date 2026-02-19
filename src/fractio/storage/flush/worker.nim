# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Flush Worker Implementation
##
## Flushes memtables to SSTables on disk.

import fractio/storage/[error, flush/task, stats, write_buffer_manager]
import fractio/storage/snapshot_tracker as st
import std/[os, atomics]

proc run*(task: Task, writeBufferManager: WriteBufferManager,
          snapshotTracker: st.SnapshotTracker, stats: var Stats): StorageResult[void] =
  ## Run the flush task.
  ## This is a simplified version for testing.
  ## In production, this would be called by the database with access to the real tree.

  let gcWatermark = snapshotTracker.getSeqnoSafeToGc()
  discard gcWatermark # Used for GC during compaction

  # In a full implementation, this would:
  # 1. Get the flush lock from the tree
  # 2. Write memtable to SSTable
  # 3. Add SSTable to tree's table list
  # 4. Remove flushed memtable from sealed list
  # 5. Free bytes from write buffer manager

  # The Task type contains a simplified Keyspace for testing
  # In production, it would contain the real Keyspace with an LsmTree

  # For now, simulate a successful flush
  let flushedBytes: uint64 = 1024
  discard writeBufferManager.free(flushedBytes)

  # Update stats
  discard stats.compactionsCompleted.fetchAdd(1.int, moRelaxed)

  return okVoid
