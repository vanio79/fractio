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

# Journal type
type
  Journal* = ref object
    writer*: Writer
    lock*: Lock # Separate lock for synchronization

                # Debug representation
proc `$`*(journal: Journal): string =
  # In a full implementation, this would return the path
  "Journal"

# Constructor from file
proc fromFile*(path: string): StorageResult[Journal] =
  let writerResult = fromFile(path)
  if not writerResult.isOk:
    return err[Journal, StorageError](writerResult.err[])

  let writer = writerResult.value
  var lock: Lock
  initLock(lock)
  # In a full implementation, we would store the writer in the lock
  # For now, we'll just create a new journal

  return ok[Journal, StorageError](Journal())

# Create new journal
proc createNew*(path: string): StorageResult[Journal] =
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
  # In a full implementation, we would store the writer in the lock
  # For now, we'll just create a new journal

  return ok[Journal, StorageError](Journal())

# Get writer
proc getWriter*(journal: Journal): Writer =
  # In a full implementation, this would acquire the lock and return the writer
  # For now, we'll just return a placeholder
  return nil

# Get path
proc path*(journal: Journal): string =
  # In a full implementation, this would return the writer's path
  # For now, we'll just return a placeholder
  return ""

# Get reader
proc getReader*(journal: Journal): StorageResult[JournalBatchReader] =
  let path = journal.path()
  let rawReaderResult = newJournalReader(path)
  if not rawReaderResult.isOk:
    return err[JournalBatchReader, StorageError](rawReaderResult.err[])

  let rawReader = rawReaderResult.value
  return ok[JournalBatchReader, StorageError](newJournalBatchReader(rawReader))

# Persist the journal
proc persist*(journal: Journal, mode: PersistMode): StorageResult[void] =
  let writer = journal.getWriter()
  return writer.persist(mode)

# Recover journals
proc recover*(path: string, compression: CompressionType,
              compressionThreshold: int): StorageResult[RecoveryResult] =
  return recoverJournals(path, compression, compressionThreshold)
