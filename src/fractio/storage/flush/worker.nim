# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Flush Worker
##
## Performs background flushing of memtables to SSTables.

import fractio/storage/[error, snapshot_tracker, stats, write_buffer_manager]
import fractio/storage/lsm_tree/[lsm_tree]
import fractio/storage/keyspace as ks

# Run flush logic for a task
proc run*(keyspace: ks.Keyspace,
          writeBufferManager: WriteBufferManager,
          snapshotTracker: SnapshotTracker,
          stats: var Stats): StorageResult[void] =
  ## Flushes the oldest sealed memtable to disk.

  if keyspace == nil or keyspace.inner == nil or keyspace.inner.tree == nil:
    return okVoid

  # Try to flush the oldest sealed memtable
  let flushResult = keyspace.flushOldestSealed()

  if flushResult.isErr:
    return err[void, StorageError](flushResult.error)

  let flushedBytes = flushResult.value

  if flushedBytes > 0 and writeBufferManager != nil:
    # Free the write buffer space
    discard writeBufferManager.free(flushedBytes)

  return okVoid
