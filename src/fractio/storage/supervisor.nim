# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import fractio/storage/[types, snapshot_tracker, journal, write_buffer_manager]
import std/[locks, atomics]

# Forward declarations for types that will be defined later
type
  Config* = object
  Keyspaces* = object
  FlushManager* = object
  JournalManager* = object

type
  SupervisorInner* = ref object
    dbConfig*: Config
    keyspaces*: ptr Lock
    writeBufferSize*: WriteBufferManager
    flushManager*: FlushManager
    seqno*: SequenceNumberCounter
    snapshotTracker*: SnapshotTracker
    journal*: Journal # Use the actual Journal type from journal.nim
    journalManager*: ptr Lock
    backpressureLock*: Lock

  Supervisor* = ref object
    inner*: SupervisorInner

# Constructor
proc newSupervisor*(inner: SupervisorInner): Supervisor =
  Supervisor(inner: inner)

# Dereference operator equivalent
proc get*(supervisor: Supervisor): SupervisorInner =
  supervisor.inner
