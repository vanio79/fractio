# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Database Implementation
##
## Main entry point for the storage engine.

import fractio/storage/[error, types, db_config, file, keyspace, batch, journal,
                        snapshot, snapshot_tracker, stats, supervisor,
                        write_buffer_manager, flush, poison_dart, version, path,
                        recovery]
import fractio/storage/lsm_tree/[types as lsm_types]
import std/[os, atomics, locks, tables, times, options]

type
  MetaKeyspace* = object
    ## Stores keyspace metadata
    keyspaces*: Table[string, uint64]
    reverseMap*: Table[uint64, string]

  KeyspaceCreateOptions* = object

  Keyspaces* = Table[string, Keyspace]

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

proc open*(config: Config): StorageResult[Database] =
  ## Open a database, creating if necessary
  let versionMarkerPath = config.path / VERSION_MARKER

  if fileExists(versionMarkerPath):
    return Database.recover(config)
  else:
    return Database.createNew(config)

proc snapshot*(db: Database): Snapshot =
  Snapshot.new(db.inner.supervisor.inner.snapshotTracker.open())

proc persist*(db: Database, mode: PersistMode): StorageResult[void] =
  if db.inner.isPoisoned.load(moRelaxed):
    return asErr(StorageError(kind: sePoisoned))

  if db.inner.supervisor.inner.journal != nil:
    let persistResult = db.inner.supervisor.inner.journal.persist(mode)
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
    return asErr(StorageError(kind: seInvalidVersion, invalidVersion: none(uint32)))
  return okVoid

proc createNew*(dbType: typeDesc[Database], config: Config): StorageResult[Database] =
  logInfo("Creating database at " & config.path)

  # Create directories
  try:
    createDir(config.path)
  except OSError:
    return asErr(StorageError(kind: seIo,
        ioError: "Failed to create database directory"))

  let keyspacesFolderPath = config.path / KEYSPACES_FOLDER
  try:
    createDir(keyspacesFolderPath)
  except OSError:
    return asErr(StorageError(kind: seIo,
        ioError: "Failed to create keyspaces directory"))

  # Create journals directory
  let journalsFolderPath = config.path / "journals"
  try:
    createDir(journalsFolderPath)
  except OSError:
    return asErr(StorageError(kind: seIo,
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
    return asErr(StorageError(kind: seIo,
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
  var supervisorInner = SupervisorInner(
    seqno: newSequenceNumberCounter(),
    snapshotTracker: newSnapshotTracker(),
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
  let versionCheckResult = Database.checkVersion(config.path)
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
  var supervisorInner = SupervisorInner(
    seqno: newSequenceNumberCounter(),
    snapshotTracker: newSnapshotTracker(),
    writeBufferSize: newWriteBufferManager()
  )

  db.inner.supervisor = Supervisor(inner: supervisorInner)

  # Recover journals
  let journalsPath = config.path / "journals"
  let recoveryResult = recoverJournals(journalsPath, ctNone, 0)
  if recoveryResult.isErr:
    return err[Database, StorageError](recoveryResult.error)

  let recovery = recoveryResult.value
  db.inner.supervisor.inner.journal = recovery.active

  # Update sequence number counter based on recovery
  let highestSeqno = recovery.active.pos()
  if highestSeqno > 0:
    db.inner.supervisor.inner.seqno.fetchMax(highestSeqno)

  logInfo("Database recovered successfully. Seqno=" &
      $db.inner.supervisor.inner.seqno.get())

  return ok[Database, StorageError](db)

proc keyspace*(db: Database, name: string): StorageResult[Keyspace] =
  ## Get or create a keyspace
  if not isValidKeyspaceName(name):
    return asErr(StorageError(kind: seStorage,
        storageError: "Invalid keyspace name"))

  # Check if keyspace already exists
  db.inner.keyspacesLock.acquire()
  defer: db.inner.keyspacesLock.release()

  if name in db.inner.keyspaces:
    return ok[Keyspace, StorageError](db.inner.keyspaces[name])

  # Create new keyspace
  let keyspaceId = db.inner.keyspaceIdCounter
  db.inner.keyspaceIdCounter += 1

  let keyspacePath = db.inner.config.path / KEYSPACES_FOLDER / $keyspaceId
  try:
    createDir(keyspacePath)
  except OSError:
    return asErr(StorageError(kind: seIo,
        ioError: "Failed to create keyspace directory"))

  # Create LSM tree for keyspace
  let treeConfig = LsmTreeConfig(
    path: keyspacePath,
    levelCount: 7,
    maxMemtableSize: 64 * 1024 * 1024,
    blockSize: 4096
  )

  let tree = newLsmTree(treeConfig,
                        db.inner.supervisor.inner.seqno,
                        db.inner.supervisor.inner.snapshotTracker)

  var anyTree = AnyTree(kind: true, tree: tree)

  let keyspaceOptions = defaultCreateOptions()

  let ks = fromDatabase(keyspaceId, db.inner, anyTree, name, keyspaceOptions)

  # Register keyspace
  db.inner.keyspaces[name] = ks
  db.inner.metaKeyspace.keyspaces[name] = keyspaceId
  db.inner.metaKeyspace.reverseMap[keyspaceId] = name

  logInfo("Created keyspace: " & name & " (id=" & $keyspaceId & ")")

  return ok[Keyspace, StorageError](ks)

proc keyspaceExists*(db: Database, name: string): bool =
  db.inner.keyspacesLock.acquire()
  defer: db.inner.keyspacesLock.release()
  name in db.inner.keyspaces

proc listKeyspaceNames*(db: Database): seq[string] =
  db.inner.keyspacesLock.acquire()
  defer: db.inner.keyspacesLock.release()

  result = @[]
  for name in db.inner.keyspaces.keys:
    result.add(name)

proc close*(db: Database) =
  ## Close the database
  db.inner.stopSignal.stop.store(true, moRelease)

  # Close journal
  if db.inner.supervisor.inner.journal != nil:
    db.inner.supervisor.inner.journal.close()

  logInfo("Database closed")
