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
import fractio/storage/keyspace/options as ksopts
import fractio/storage/lsm_tree/[types as lsm_types]
import fractio/storage/lsm_tree/lsm_tree
import fractio/storage/journal/writer # For PersistMode
import std/[os, atomics, locks, tables, times, options, sequtils, streams, strutils]

const KEYSPACE_META_FILE* = "keyspaces.meta"

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

# Save keyspace metadata to file (must be called with keyspacesLock held)
proc saveKeyspaceMeta*(db: Database): StorageResult[void] =
  let metaPath = db.inner.config.path / KEYSPACE_META_FILE
  var strm = newFileStream(metaPath, fmWrite)
  if strm == nil:
    return err[void, StorageError](StorageError(kind: seIo,
        ioError: "Failed to create keyspace metadata file"))

  defer: strm.close()

  # Write number of keyspaces
  strm.write(uint64(db.inner.metaKeyspace.keyspaces.len))

  # Write each name -> id mapping
  for name, id in db.inner.metaKeyspace.keyspaces.pairs:
    strm.write(uint32(name.len))
    strm.write(name)
    strm.write(id)

  return okVoid

# Load keyspace metadata from file
proc loadKeyspaceMeta*(db: Database): StorageResult[void] =
  let metaPath = db.inner.config.path / KEYSPACE_META_FILE

  if not fileExists(metaPath):
    return okVoid

  var strm = newFileStream(metaPath, fmRead)
  if strm == nil:
    return err[void, StorageError](StorageError(kind: seIo,
        ioError: "Failed to open keyspace metadata file"))

  defer: strm.close()

  let count = strm.readUInt64()

  for i in 0 ..< int(count):
    let nameLen = strm.readUInt32()
    let name = strm.readStr(int(nameLen))
    let id = strm.readUInt64()

    db.inner.metaKeyspace.keyspaces[name] = id
    db.inner.metaKeyspace.reverseMap[id] = name

    # Update keyspace ID counter
    if id >= db.inner.keyspaceIdCounter:
      db.inner.keyspaceIdCounter = id + 1

  return okVoid

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

  # Initialize metaKeyspace
  db.inner.metaKeyspace = MetaKeyspace(
    keyspaces: initTable[string, uint64](),
    reverseMap: initTable[uint64, string]()
  )

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

  # Initialize metaKeyspace
  db.inner.metaKeyspace = MetaKeyspace(
    keyspaces: initTable[string, uint64](),
    reverseMap: initTable[uint64, string]()
  )

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
  new(journal)
  journal.writer = recovery.active
  journal.lock = lock
  journal.path = journalsPath / "0.jnl"
  db.inner.supervisor.inner.journal = journal

  # Update sequence number counter based on recovery
  let posResult = recovery.active.pos()
  if posResult.isOk and posResult.value > 0:
    db.inner.supervisor.inner.seqno.fetchMax(posResult.value)

  # Load keyspace metadata
  let metaResult = db.loadKeyspaceMeta()
  if metaResult.isErr:
    logInfo("Warning: Failed to load keyspace metadata")

  # Recover keyspaces from metadata
  logInfo("Recovering " & $db.inner.metaKeyspace.keyspaces.len & " keyspaces")

  for name, keyspaceId in db.inner.metaKeyspace.keyspaces.pairs:
    let keyspaceFolder = config.path / KEYSPACES_FOLDER / $keyspaceId

    if not dirExists(keyspaceFolder):
      logInfo("Warning: Keyspace folder missing for " & name & ", skipping")
      continue

    logInfo("Recovering keyspace " & name & " with id " & $keyspaceId)

    # Create LSM tree config
    let treeConfig = lsm_tree.newConfig(keyspaceFolder)

    # Create LSM tree
    let tree = lsm_tree.newLsmTree(
      treeConfig,
      db.inner.supervisor.inner.seqno,
      db.inner.supervisor.inner.snapshotTracker
    )

    # Load existing SSTables from disk
    tree.loadSsTables()

    # Create default options
    let keyConfig = ksopts.defaultCreateOptions()

    # Create keyspace
    let keyspace = ks.newKeyspace(
      keyspaceId,
      tree,
      name,
      keyConfig,
      db.inner.supervisor,
      db.inner.stats,
      addr db.inner.isPoisoned
    )

    # Store in cache
    db.inner.keyspaces[name] = keyspace

    logInfo("Keyspace " & name & " recovered successfully")

  logInfo("Database recovered successfully. Seqno=" &
      $db.inner.supervisor.inner.seqno.get())

  return ok[Database, StorageError](db)

proc keyspace*(db: Database, name: string): StorageResult[ks.Keyspace] =
  ## Get or create a keyspace

  # Validate keyspace name
  if not isValidKeyspaceName(name):
    return err[ks.Keyspace, StorageError](StorageError(kind: seStorage,
        storageError: "Invalid keyspace name: " & name))

  # Check if keyspace already exists
  db.inner.keyspacesLock.acquire()
  defer: db.inner.keyspacesLock.release()

  if name in db.inner.keyspaces:
    return ok[ks.Keyspace, StorageError](db.inner.keyspaces[name])

  # Create new keyspace
  let keyspaceId = db.inner.keyspaceIdCounter
  db.inner.keyspaceIdCounter += 1

  logInfo("Creating keyspace " & name & " with id " & $keyspaceId)

  # Create keyspace folder
  let keyspaceFolder = db.inner.config.path / KEYSPACES_FOLDER / $keyspaceId
  try:
    createDir(keyspaceFolder)
  except OSError:
    return err[ks.Keyspace, StorageError](StorageError(kind: seIo,
        ioError: "Failed to create keyspace directory: " & keyspaceFolder))

  # Create LSM tree config
  let treeConfig = lsm_tree.newConfig(keyspaceFolder)

  # Create LSM tree with shared seqno counter and snapshot tracker
  let tree = lsm_tree.newLsmTree(
    treeConfig,
    db.inner.supervisor.inner.seqno,
    db.inner.supervisor.inner.snapshotTracker
  )

  # Create default options
  let keyConfig = ksopts.defaultCreateOptions()

  # Create keyspace with all required components
  let keyspace = ks.newKeyspace(
    keyspaceId,
    tree,
    name,
    keyConfig,
    db.inner.supervisor,
    db.inner.stats,
    addr db.inner.isPoisoned
  )

  # Store in cache
  db.inner.keyspaces[name] = keyspace

  # Store in metadata
  db.inner.metaKeyspace.keyspaces[name] = keyspaceId
  db.inner.metaKeyspace.reverseMap[keyspaceId] = name

  # Persist metadata
  let saveResult = db.saveKeyspaceMeta()
  if saveResult.isErr:
    logInfo("Warning: Failed to save keyspace metadata")

  logInfo("Keyspace " & name & " created successfully")

  return ok[ks.Keyspace, StorageError](keyspace)

proc keyspaceExists*(db: Database, name: string): bool =
  db.inner.keyspacesLock.acquire()
  defer: db.inner.keyspacesLock.release()
  name in db.inner.keyspaces

proc listKeyspaceNames*(db: Database): seq[string] =
  db.inner.keyspacesLock.acquire()
  defer: db.inner.keyspacesLock.release()
  toSeq(db.inner.keyspaces.keys())

proc close*(db: Database) =
  ## Close the database
  db.inner.stopSignal.stop.store(true, moRelease)

  # Journal will be cleaned up by GC
  logInfo("Database closed")
