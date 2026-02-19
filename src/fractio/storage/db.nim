# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import fractio/storage/[error, types, db_config, file, keyspace, batch, journal,
                       snapshot, snapshot_tracker, stats, supervisor,
                       worker_pool,
                       write_buffer_manager, flush, poison_dart, version, path]
import std/[os, atomics, locks, tables, times]

# Forward declarations
type
  MetaKeyspace* = object
  KeyspaceCreateOptions* = object
  Keyspaces* = Table[string, Keyspace]
  StopSignal* = object
  Openable* = concept T
    proc open*(config: Config): StorageResult[T]

# Database inner structure
type
  DatabaseInner* = ref object
    metaKeyspace*: MetaKeyspace

    # Database configuration
    config*: Config

    supervisor*: Supervisor

    # Stop signal when database is dropped to stop background threads
    stopSignal*: StopSignal

    # Counter of background threads
    activeThreadCounter*: Atomic[int]

    # True if fsync failed
    isPoisoned*: Atomic[bool]

    stats*: Stats
    keyspaceIdCounter*: uint64 # Simplified from SequenceNumberCounter

    workerPool*: WorkerPool
    lockFile*: object          # Placeholder for LockedFileGuard

# Database handle
type
  Database* = ref object
    inner*: DatabaseInner

# Implementation of Openable for Database
proc open*(config: Config): StorageResult[Database] =
  Database.open(config)

# Database methods
proc snapshot*(db: Database): Snapshot =
  Snapshot.new(db.supervisor.snapshotTracker.open())

proc builder*(path: string): Builder[Database] =
  newBuilder[Database](path)

proc batch*(db: Database): WriteBatch =
  var batch = newWriteBatch(db[])

  if not db.config.manualJournalPersist:
    batch = batch.durability(some(pmBuffer))

  return batch

# Stats methods
proc writeBufferSize*(db: Database): uint64 =
  db.supervisor.writeBufferSize.get()

proc outstandingFlushes*(db: Database): int =
  db.supervisor.flushManager.len()

proc timeCompacting*(db: Database): Duration =
  let us = db.stats.timeCompacting.load(moRelaxed)
  initDuration(microseconds = int64(us))

proc activeCompactions*(db: Database): int =
  db.stats.activeCompactionCount.load(moRelaxed)

proc compactionsCompleted*(db: Database): int =
  db.stats.compactionsCompleted.load(moRelaxed)

# Journal methods
proc journalCount*(db: Database): int =
  # In a full implementation, this would read from the journal manager
  # For now, we'll return 1 (active journal)
  return 1

proc journalDiskSpace*(db: Database): StorageResult[uint64] =
  # In a full implementation, this would calculate journal disk space
  # For now, we'll return 0
  return ok(0)

proc diskSpace*(db: Database): StorageResult[uint64] =
  let journalSizeResult = db.journalDiskSpace()
  if journalSizeResult.isErr():
    return err(journalSizeResult.error())

  let journalSize = journalSizeResult.get()

  # In a full implementation, this would sum keyspace disk space
  # For now, we'll return just the journal size
  return ok(journalSize)

# Persistence
proc persist*(db: Database, mode: PersistMode): StorageResult[void] =
  if db.isPoisoned.load(moRelaxed):
    return asErr(StorageError(kind: sePoisoned))

  # In a full implementation, this would persist the journal
  # For now, we'll return success
  return okVoid()

proc cacheCapacity*(db: Database): uint64 =
  # In a full implementation, this would return cache capacity
  # For now, we'll return 0
  return 0

# Main open method
proc open*(dbType: typeDesc[Database], config: Config): StorageResult[Database] =
  logDebug("cache capacity=" & $(config.cache.capacity div (1024 * 1024)) & "MiB")

  let dbResult = Database.createOrRecover(config)
  if dbResult.isErr():
    return err(dbResult.error())

  let db = dbResult.get()

  return ok(db)

# Create or recover
proc createOrRecover*(dbType: typeDesc[Database],
    config: Config): StorageResult[Database] =
  let versionMarkerPath = config.path / VERSION_MARKER
  if existsFile(versionMarkerPath):
    return Database.recover(config)
  else:
    return Database.createNew(config)

# Keyspace operations
proc deleteKeyspace*(db: Database, handle: Keyspace): StorageResult[void] =
  # In a full implementation, this would delete the keyspace
  # For now, we'll return success
  return okVoid()

proc keyspace*(db: Database, name: string,
               createOptions: proc(): KeyspaceCreateOptions): StorageResult[Keyspace] =
  # Validate keyspace name
  if not isValidKeyspaceName(name):
    raise newException(ValueError, "Invalid keyspace name")

  # In a full implementation, this would create or open a keyspace
  # For now, we'll return an error
  return asErr(StorageError(kind: seStorage, storageError: "Not implemented"))

proc keyspaceCount*(db: Database): int =
  # In a full implementation, this would return the number of keyspaces
  # For now, we'll return 0
  return 0

proc listKeyspaceNames*(db: Database): seq[string] =
  # In a full implementation, this would list keyspace names
  # For now, we'll return an empty sequence
  return @[]

proc keyspaceExists*(db: Database, name: string): bool =
  # In a full implementation, this would check if keyspace exists
  # For now, we'll return false
  return false

# Sequence number methods
proc seqno*(db: Database): SeqNo =
  # In a full implementation, this would return the sequence number
  # For now, we'll return 0
  return 0

proc visibleSeqno*(db: Database): SeqNo =
  # In a full implementation, this would return the visible sequence number
  # For now, we'll return 0
  return 0

# Version checking
proc checkVersion*(path: string): StorageResult[void] =
  let versionMarkerPath = path / VERSION_MARKER
  if not existsFile(versionMarkerPath):
    return asErr(StorageError(kind: seInvalidVersion, invalidVersion: none(uint32)))

  # In a full implementation, this would read and parse the version
  # For now, we'll assume version 3
  return okVoid()

# Recovery
proc recover*(dbType: typeDesc[Database], config: Config): StorageResult[Database] =
  logInfo("Recovering database at " & config.path)

  # Check version
  let versionCheckResult = Database.checkVersion(config.path)
  if versionCheckResult.isErr():
    return err(versionCheckResult.error())

  # In a full implementation, this would perform database recovery
  # For now, we'll return an error
  return asErr(StorageError(kind: seStorage, storageError: "Not implemented"))

# Create new database
proc createNew*(dbType: typeDesc[Database], config: Config): StorageResult[Database] =
  logInfo("Creating database at " & config.path)

  # Create directories
  try:
    createDir(config.path)
  except OSError:
    return asErr(StorageError(kind: seIo, ioError: "Failed to create database directory"))

  let keyspacesFolderPath = config.path / KEYSPACES_FOLDER
  try:
    createDir(keyspacesFolderPath)
  except OSError:
    return asErr(StorageError(kind: seIo, ioError: "Failed to create keyspaces directory"))

  # Create version marker
  let versionMarkerPath = config.path / VERSION_MARKER
  var versionFile: File
  if open(versionFile, versionMarkerPath, fmWrite):
    var buffer: seq[byte] = @[]
    writeFileHeader(fvV3, buffer)
    versionFile.write(buffer)
    versionFile.close()
  else:
    return asErr(StorageError(kind: seIo, ioError: "Failed to create version marker"))

  # Sync directories
  discard fsyncDirectory(keyspacesFolderPath)
  discard fsyncDirectory(config.path)

  # In a full implementation, this would create a new database
  # For now, we'll return an error
  return asErr(StorageError(kind: seStorage, storageError: "Not implemented"))
