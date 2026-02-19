# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import fractio/storage/[error, types, file, journal/[writer, reader,
    batch_reader, recovery, manager]]
import std/[os, locks]

# Journal type
type
  Journal* = ref object
    writer*: Lock[Writer] # Using Lock as equivalent to Mutex

# Debug representation
proc `$`*(journal: Journal): string =
  # In a full implementation, this would return the path
  "Journal"

# Constructor from file
proc fromFile*(path: string): StorageResult[Journal] =
  let writerResult = fromFile(path)
  if writerResult.isErr():
    return err(writerResult.error())

  let writer = writerResult.get()
  var lock: Lock[Writer]
  initLock(lock)
  # In a full implementation, we would store the writer in the lock
  # For now, we'll just create a new journal

  return ok(Journal())

# Create new journal
proc createNew*(path: string): StorageResult[Journal] =
  logDebug("Creating new journal at " & path)

  # Create directory if it doesn't exist
  let folder = path.splitFile().dir
  try:
    createDir(folder)
  except OSError:
    logError("Failed to create journal folder at: " & path)
    return err(StorageError(kind: seIo, ioError: "Failed to create journal folder"))

  # Create writer
  let writerResult = createNew(path)
  if writerResult.isErr():
    return err(writerResult.error())

  let writer = writerResult.get()

  # Sync directory
  let syncResult = fsyncDirectory(folder)
  if syncResult.isErr():
    return err(syncResult.error())

  var lock: Lock[Writer]
  initLock(lock)
  # In a full implementation, we would store the writer in the lock
  # For now, we'll just create a new journal

  return ok(Journal())

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
  if rawReaderResult.isErr():
    return err(rawReaderResult.error())

  let rawReader = rawReaderResult.get()
  return ok(newJournalBatchReader(rawReader))

# Persist the journal
proc persist*(journal: Journal, mode: PersistMode): StorageResult[void] =
  let writer = journal.getWriter()
  return writer.persist(mode)

# Recover journals
proc recover*(path: string, compression: CompressionType,
              compressionThreshold: int): StorageResult[RecoveryResult] =
  return recoverJournals(path, compression, compressionThreshold)
