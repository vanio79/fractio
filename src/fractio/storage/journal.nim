# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import fractio/storage/[error, types, file]
import fractio/storage/journal/writer
import fractio/storage/journal/reader
import fractio/storage/journal/batch_reader
import fractio/storage/journal/recovery
import fractio/storage/journal/manager
import std/[os, locks]

# Journal type with proper writer management
type
  Journal* = ref object
    writer*: Writer
    lock*: Lock
    path*: string

# Guard type that holds the journal lock and provides access to writer
type
  JournalWriterGuard* = object
    journal*: Journal
    lockHeld*: bool

# Acquire the journal lock and return a guard with writer access
proc getWriter*(journal: Journal): JournalWriterGuard =
  journal.lock.acquire()
  result = JournalWriterGuard(journal: journal, lockHeld: true)

# Get the actual writer from the guard
proc writer*(guard: JournalWriterGuard): Writer =
  guard.journal.writer

# Release the lock when guard goes out of scope
proc release*(guard: var JournalWriterGuard) =
  if guard.lockHeld:
    guard.journal.lock.release()
    guard.lockHeld = false

# Debug representation
proc `$`*(journal: Journal): string =
  "Journal(" & journal.path & ")"

# Constructor from file - opens existing journal
proc openJournal*(path: string): StorageResult[Journal] =
  let writerResult = fromFile(path)
  if not writerResult.isOk:
    return err[Journal, StorageError](writerResult.err[])

  let writer = writerResult.value
  var lock: Lock
  initLock(lock)

  return ok[Journal, StorageError](Journal(
    writer: writer,
    lock: lock,
    path: path
  ))

# Create new journal
proc createJournal*(path: string): StorageResult[Journal] =
  # Create directory if it doesn't exist
  let folder = path.splitFile().dir
  try:
    createDir(folder)
  except OSError:
    return err[Journal, StorageError](StorageError(kind: seIo,
        ioError: "Failed to create journal folder"))

  # Create writer
  let writerResult = createNew(path)
  if not writerResult.isOk:
    return err[Journal, StorageError](writerResult.err[])

  let writer = writerResult.value

  # Sync directory
  let syncResult = fsyncDirectory(folder)
  if not syncResult.isOk:
    return err[Journal, StorageError](syncResult.err[])

  var lock: Lock
  initLock(lock)

  return ok[Journal, StorageError](Journal(
    writer: writer,
    lock: lock,
    path: path
  ))

# Get path
proc getPath*(journal: Journal): string =
  journal.path

# Get reader
proc getReader*(journal: Journal): StorageResult[JournalBatchReader] =
  let rawReaderResult = newJournalReader(journal.path)
  if not rawReaderResult.isOk:
    return err[JournalBatchReader, StorageError](rawReaderResult.err[])
  let rawReader = rawReaderResult.value
  return ok[JournalBatchReader, StorageError](newJournalBatchReader(rawReader))

# Persist the journal (must be called while holding the lock via guard)
proc persist*(guard: JournalWriterGuard, mode: PersistMode): StorageResult[void] =
  if guard.journal.writer == nil:
    return err[void, StorageError](StorageError(kind: seIo,
        ioError: "Journal writer is nil"))
  return guard.journal.writer.persist(mode)

# Write raw entry (must be called while holding the lock via guard)
proc writeRaw*(guard: JournalWriterGuard, keyspaceId: uint64, key: string,
               value: string, valueType: ValueType,
                   seqno: uint64): StorageResult[int] =
  if guard.journal.writer == nil:
    return err[int, StorageError](StorageError(kind: seIo,
        ioError: "Journal writer is nil"))
  return guard.journal.writer.writeRaw(keyspaceId, key, value, valueType, seqno)

# Write clear entry (must be called while holding the lock via guard)
proc writeClear*(guard: JournalWriterGuard, keyspaceId: uint64,
    seqno: SeqNo): StorageResult[int] =
  if guard.journal.writer == nil:
    return err[int, StorageError](StorageError(kind: seIo,
        ioError: "Journal writer is nil"))
  return guard.journal.writer.writeClear(keyspaceId, seqno)

# Re-export recoverJournals from recovery module
export recovery.recoverJournals
