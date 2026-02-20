# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Supervisor
##
## Central coordination point for database operations.
## Holds references to all shared state.
##
## Design note: JournalManager is included with its own lock for thread safety.
## The JournalManager uses keyspace IDs (not references) to avoid GC cycles.

import fractio/storage/[types, snapshot_tracker, journal, write_buffer_manager,
                        flush/manager]
import std/[locks, atomics]

# Database configuration
type
  DbConfig* = object
    maxJournalingSizeInBytes*: uint64
    maxWriteBufferSizeInBytes*: uint64

type
  SupervisorInner* = ref object
    ## Inner supervisor state
    dbConfig*: DbConfig

    ## Write buffer size tracker
    writeBufferSize*: WriteBufferManager

    ## Flush task queue
    flushManager*: FlushManager

    ## Global sequence number counter
    seqno*: SequenceNumberCounter

    ## Snapshot tracking for GC watermark
    snapshotTracker*: SnapshotTracker

    ## Active journal
    journal*: Journal

    ## Journal manager for tracking sealed journals
    ## Uses keyspace IDs to avoid reference cycles
    journalManager*: JournalManager
    journalManagerLock*: Lock

    ## Backpressure lock for write throttling
    backpressureLock*: Lock

  Supervisor* = ref object
    ## Handle to supervisor
    inner*: SupervisorInner

# Constructor
proc newSupervisor*(inner: SupervisorInner): Supervisor =
  Supervisor(inner: inner)

# Create a new supervisor with defaults
proc newSupervisorWithDefaults*(): Supervisor =
  var inner = SupervisorInner(
    flushManager: newFlushManager(),
    writeBufferSize: newWriteBufferManager(),
    journalManager: newJournalManager(),
    dbConfig: DbConfig(
      maxJournalingSizeInBytes: 256 * 1024 * 1024,                     # 256 MB
    maxWriteBufferSizeInBytes: 64 * 1024 * 1024                        # 64 MB
  )
  )
  initLock(inner.journalManagerLock)
  initLock(inner.backpressureLock)
  result = Supervisor(inner: inner)

# Dereference operator equivalent
proc get*(supervisor: Supervisor): SupervisorInner =
  supervisor.inner

# Acquire journal manager lock
proc lockJournalManager*(supervisor: Supervisor) =
  acquire(supervisor.inner.journalManagerLock)

# Release journal manager lock
proc unlockJournalManager*(supervisor: Supervisor) =
  release(supervisor.inner.journalManagerLock)

# Helper template to run code with journal manager locked
template withJournalManager*(supervisor: Supervisor, body: untyped): untyped =
  supervisor.lockJournalManager()
  try:
    body
  finally:
    supervisor.unlockJournalManager()
