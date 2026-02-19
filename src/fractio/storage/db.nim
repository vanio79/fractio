# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Database Implementation
##
## Main entry point for the storage engine.

import fractio/storage/[error, types, file, snapshot, snapshot_tracker, stats, supervisor,
                        write_buffer_manager, flush, poison_dart, version, path,
                        logging, journal]
import fractio/storage/recovery as db_recovery
import fractio/storage/db_config as dbcfg
import fractio/storage/keyspace as ks
import fractio/storage/keyspace/name
import fractio/storage/keyspace/options
import fractio/storage/lsm_tree/[types as lsm_types]
import fractio/storage/lsm_tree/lsm_tree
import fractio/storage/journal/writer # For PersistMode
import std/[os, atomics, locks, tables, times, options]

type
  PersistMode* = writer.PersistMode # Use the journal writer's PersistMode

type
  Config* = dbcfg.Config

  MetaKeyspace* = object
    ## Stores keyspace metadata
    keyspaces*: Table[string, uint64]
    reverseMap*: Table[uint64, string]

  KeyspaceCreateOptions* = object

  Keyspaces* = Table[string, ks.Keyspace]

  StopSignal* = object
    stop*: Atomic[bool]

type
  DatabaseInner* = ref object
    metaKeyspace*: MetaKeyspace

    config*: Config

    supervisor*: Supervisor

    stopSignal*: StopSignal
    activeThreadCounter*: Atomic[int]
    isPoisoned*: Atomic[bool]

    stats*: Stats
    keyspaceIdCounter*: uint64

    keyspaces*: Keyspaces
    keyspacesLock*: Lock

  Database* = ref object
    inner*: DatabaseInner

# Forward declarations
proc createNew*(dbType: typeDesc[Database], config: Config): StorageResult[Database]
proc recover*(dbType: typeDesc[Database], config: Config): StorageResult[Database]

proc open*(config: Config): StorageResult[Database] =
  ## Open a database, creating if necessary
  let versionMarkerPath = config.path / VERSION_MARKER

  if fileExists(versionMarkerPath):
    return Database.recover(config)
  else:
    return Database.createNew(config)

proc snapshot*(db: Database): Snapshot =
  newSnapshot(db.inner.supervisor.inner.snapshotTracker.open())

proc persist*(db: Database, mode: PersistMode): StorageResult[void] =
  if db.inner.isPoisoned.load(moRelaxed):
    return asErr(StorageError(kind: sePoisoned))

  if db.inner.supervisor.inner.journal != nil:
    var guard = db.inner.supervisor.inner.journal.getWriter()
    let persistResult = guard.persist(mode)
    guard.release()
    if persistResult.isErr:
      db.inner.isPoisoned.store(true, moRelease)
      return asErr(StorageError(kind: sePoisoned))

  return okVoid

proc writeBufferSize*(db: Database): uint64 =
  db.inner.supervisor.inner.writeBufferSize.get()

proc outstandingFlushes*(db: Database): int =
  # Placeholder
  0

proc timeCompacting*(db: Database): Duration =
  let us = db.inner.stats.timeCompacting.load(moRelaxed)
  initDuration(microseconds = int64(us))

proc activeCompactions*(db: Database): int =
  db.inner.stats.activeCompactionCount.load(moRelaxed)

proc compactionsCompleted*(db: Database): int =
  db.inner.stats.compactionsCompleted.load(moRelaxed)

proc seqno*(db: Database): SeqNo =
  db.inner.supervisor.inner.seqno.get()

proc visibleSeqno*(db: Database): SeqNo =
  db.inner.supervisor.inner.snapshotTracker.get()

proc checkVersion*(path: string): StorageResult[void] =
  let versionMarkerPath = path / VERSION_MARKER
  if not fileExists(versionMarkerPath):
    return asErr(StorageError(kind: seInvalidVersion, invalidVersion: none(
        FormatVersion)))
  return okVoid

proc createNew*(dbType: typeDesc[Database], config: Config): StorageResult[Database] =
  logInfo("Creating database at " & config.path)

  # Create directories
  try:
    createDir(config.path)
  except OSError:
    return err[Database, StorageError](StorageError(kind: seIo,
        ioError: "Failed to create database directory"))

  let keyspacesFolderPath = config.path / KEYSPACES_FOLDER
  try:
    createDir(keyspacesFolderPath)
  except OSError:
    return err[Database, StorageError](StorageError(kind: seIo,
        ioError: "Failed to create keyspaces directory"))

  # Create journals directory
  let journalsFolderPath = config.path / "journals"
  try:
    createDir(journalsFolderPath)
  except OSError:
    return err[Database, StorageError](StorageError(kind: seIo,
        ioError: "Failed to create journals directory"))

  # Create version marker
  let versionMarkerPath = config.path / VERSION_MARKER
  var versionFile: File
  if open(versionFile, versionMarkerPath, fmWrite):
    var buffer: seq[byte] = @[]
    writeFileHeader(fvV3, buffer)
    versionFile.write(buffer)
    versionFile.close()
  else:
    return err[Database, StorageError](StorageError(kind: seIo,
        ioError: "Failed to create version marker"))

  # Sync directories
  discard fsyncDirectory(keyspacesFolderPath)
  discard fsyncDirectory(config.path)

  # Create database structure
  var db = Database()
  new(db.inner)

  db.inner.config = config
  db.inner.isPoisoned.store(false, moRelaxed)
  db.inner.stopSignal.stop.store(false, moRelaxed)
  db.inner.keyspaceIdCounter = 1'u64
  initLock(db.inner.keyspacesLock)

  # Create stats
  db.inner.stats = newStats()

  # Create supervisor
  let seqnoCounter = snapshot_tracker.newSequenceNumberCounter()
  var supervisorInner = SupervisorInner(
    seqno: seqnoCounter,
    snapshotTracker: snapshot_tracker.newSnapshotTracker(seqnoCounter),
    writeBufferSize: newWriteBufferManager()
  )

  db.inner.supervisor = Supervisor(inner: supervisorInner)

  # Create journal
  let journalPath = journalsFolderPath / "0.jnl"
  let journalResult = createJournal(journalPath)
  if journalResult.isErr:
    return err[Database, StorageError](journalResult.error)

  db.inner.supervisor.inner.journal = journalResult.value

  logInfo("Database created successfully")

  return ok[Database, StorageError](db)

proc recover*(dbType: typeDesc[Database], config: Config): StorageResult[Database] =
  logInfo("Recovering database at " & config.path)

  # Check version
  let versionCheckResult = checkVersion(config.path)
  if versionCheckResult.isErr:
    return err[Database, StorageError](versionCheckResult.error)

  # Create database structure
  var db = Database()
  new(db.inner)

  db.inner.config = config
  db.inner.isPoisoned.store(false, moRelaxed)
  db.inner.stopSignal.stop.store(false, moRelaxed)
  db.inner.keyspaceIdCounter = 1'u64
  initLock(db.inner.keyspacesLock)

  # Create stats
  db.inner.stats = newStats()

  # Create supervisor
  let seqnoCounter2 = snapshot_tracker.newSequenceNumberCounter()
  var supervisorInner = SupervisorInner(
    seqno: seqnoCounter2,
    snapshotTracker: snapshot_tracker.newSnapshotTracker(seqnoCounter2),
    writeBufferSize: newWriteBufferManager()
  )

  db.inner.supervisor = Supervisor(inner: supervisorInner)

  # Recover journals
  let journalsPath = config.path / "journals"
  let recoveryResult = db_recovery.recoverJournals(journalsPath, ctNone, 0)
  if recoveryResult.isErr:
    return err[Database, StorageError](recoveryResult.error)

  let recovery = recoveryResult.value

  # Create Journal from recovered Writer
  var lock: Lock
  initLock(lock)
  var journal: Journal
  journal.writer = recovery.active
  journal.lock = lock
  journal.path = journalsPath / "0.jnl"
  db.inner.supervisor.inner.journal = journal

  # Update sequence number counter based on recovery
  let posResult = recovery.active.pos()
  if posResult.isOk and posResult.value > 0:
    db.inner.supervisor.inner.seqno.fetchMax(posResult.value)

  logInfo("Database recovered successfully. Seqno=" &
      $db.inner.supervisor.inner.seqno.get())

  return ok[Database, StorageError](db)

proc keyspace*(db: Database, name: string): StorageResult[ks.Keyspace] =
  ## Get or create a keyspace
  ## NOTE: This is a simplified implementation for testing
  return err[ks.Keyspace, StorageError](StorageError(kind: seStorage,
      storageError: "Keyspace creation not yet implemented"))

proc keyspaceExists*(db: Database, name: string): bool =
  false

proc listKeyspaceNames*(db: Database): seq[string] =
  @[]

proc close*(db: Database) =
  ## Close the database
  db.inner.stopSignal.stop.store(true, moRelease)

  # Journal will be cleaned up by GC
  logInfo("Database closed")
