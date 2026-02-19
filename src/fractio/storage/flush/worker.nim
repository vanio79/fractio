# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Flush Worker Implementation
##
## Flushes memtables to SSTables on disk.

import fractio/storage/[error, flush/task, snapshot_tracker, stats, write_buffer_manager]
import std/[os, atomics]

# Run flush logic
proc run*(task: Task, writeBufferManager: WriteBufferManager,
          snapshotTracker: SnapshotTracker, stats: Stats): StorageResult[void] =
  discard snapshotTracker.getSeqnoSafeToGc()

  # In a full implementation, this would:
  # 1. Get the flush lock from the tree
  # 2. Write memtable to SSTable
  # 3. Add SSTable to tree's table list
  # 4. Remove flushed memtable from sealed list
  # 5. Free bytes from write buffer manager

  # For now, simulate a successful flush
  let flushedBytes: uint64 = 1024
  discard writeBufferManager.free(flushedBytes)

  return okVoid
