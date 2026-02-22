# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Database Implementation
##
## Main entry point for the storage engine.

import fractio/storage/[error, types, file, snapshot, snapshot_tracker, stats, supervisor,
                        write_buffer_manager, flush, flush/manager, poison_dart,
                        version, path,
                        logging, journal, worker_pool, descriptor_table]
import fractio/storage/recovery as db_recovery
import fractio/storage/db_config as dbcfg
import fractio/storage/keyspace as ks
import fractio/storage/keyspace/name
import fractio/storage/keyspace/options as ksopts
import fractio/storage/lsm_tree/[types as lsm_types]
import fractio/storage/lsm_tree/lsm_tree
import fractio/storage/journal/writer # For PersistMode, BatchItem
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

    workerPool*: WorkerPool

    stopSignal*: StopSignal
    activeThreadCounter*: Atomic[int]
    isPoisoned*: Atomic[bool]

    stats*: Stats
    keyspaceIdCounter*: uint64

    keyspaces*: Keyspaces
    keyspacesLock*: Lock

    # Shared descriptor table for caching file handles
    descriptorTable*: DescriptorTable

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

  # Create descriptor table for file handle caching
  let maxDescriptorFiles = if config.descriptorTableMaxFiles > 0:
                             config.descriptorTableMaxFiles
                           else:
                             64 # Default
  db.inner.descriptorTable = newDescriptorTable(maxDescriptorFiles)

  # Create supervisor with JournalManager
  let seqnoCounter = snapshot_tracker.newSequenceNumberCounter()
  var supervisorInner = SupervisorInner(
    seqno: seqnoCounter,
    snapshotTracker: snapshot_tracker.newSnapshotTracker(seqnoCounter),
    writeBufferSize: newWriteBufferManager(),
    flushManager: newFlushManager(),
    journalManager: newJournalManager(),
    dbConfig: DbConfig(
      maxJournalingSizeInBytes: 256'u64 * 1024'u64 * 1024'u64,
      maxWriteBufferSizeInBytes: 64'u64 * 1024'u64 * 1024'u64
    )
  )
  initLock(supervisorInner.journalManagerLock)
  initLock(supervisorInner.backpressureLock)

  db.inner.supervisor = Supervisor(inner: supervisorInner)

  # Create journal
  let journalPath = journalsFolderPath / "0.jnl"
  let journalResult = createJournal(journalPath)
  if journalResult.isErr:
    return err[Database, StorageError](journalResult.error)

  db.inner.supervisor.inner.journal = journalResult.value

  # Create and start worker pool
  db.inner.workerPool = newWorkerPool(4) # 4 worker threads
  db.inner.workerPool.start(
    db.inner.supervisor.inner.flushManager,
    db.inner.supervisor.inner.writeBufferSize,
    db.inner.supervisor.inner.snapshotTracker,
    addr db.inner.stats,
    addr db.inner.keyspaces,
    addr db.inner.keyspacesLock,
    db.inner.supervisor.inner.journalManager,
    addr db.inner.supervisor.inner.journalManagerLock
  )

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

  # Create descriptor table for file handle caching
  let maxDescriptorFiles = if config.descriptorTableMaxFiles > 0:
                             config.descriptorTableMaxFiles
                           else:
                             64 # Default
  db.inner.descriptorTable = newDescriptorTable(maxDescriptorFiles)

  # Create supervisor with JournalManager
  let seqnoCounter2 = snapshot_tracker.newSequenceNumberCounter()
  var supervisorInner = SupervisorInner(
    seqno: seqnoCounter2,
    snapshotTracker: snapshot_tracker.newSnapshotTracker(seqnoCounter2),
    writeBufferSize: newWriteBufferManager(),
    flushManager: newFlushManager(),
    journalManager: newJournalManager(),
    dbConfig: DbConfig(
      maxJournalingSizeInBytes: 256'u64 * 1024'u64 * 1024'u64,
      maxWriteBufferSizeInBytes: 64'u64 * 1024'u64 * 1024'u64
    )
  )
  initLock(supervisorInner.journalManagerLock)
  initLock(supervisorInner.backpressureLock)

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

  # Create worker pool (before recovering keyspaces so they can reference it)
  db.inner.workerPool = newWorkerPool(4) # 4 worker threads

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

    # Create rotation callback
    let onRotateCb = proc(ksId: uint64) =
      if db.inner.workerPool != nil:
        db.inner.keyspacesLock.acquire()
        defer: db.inner.keyspacesLock.release()
        for name, ks in db.inner.keyspaces.pairs:
          if ks.inner.id == ksId:
            db.inner.workerPool.requestMemtableRotation(ks)
            break

    # Create keyspace with worker pool reference
    let keyspace = ks.newKeyspace(
      keyspaceId,
      tree,
      name,
      keyConfig,
      db.inner.supervisor,
      db.inner.stats,
      addr db.inner.isPoisoned,
      db.inner.supervisor.inner.flushManager,
      onRotateCb
    )

    # Store in cache
    db.inner.keyspaces[name] = keyspace

    logInfo("Keyspace " & name & " recovered successfully")

  # Now start the worker pool
  db.inner.workerPool.start(
    db.inner.supervisor.inner.flushManager,
    db.inner.supervisor.inner.writeBufferSize,
    db.inner.supervisor.inner.snapshotTracker,
    addr db.inner.stats,
    addr db.inner.keyspaces,
    addr db.inner.keyspacesLock,
    db.inner.supervisor.inner.journalManager,
    addr db.inner.supervisor.inner.journalManagerLock
  )

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

  # Create rotation callback that will be called when memtable needs rotation
  let onRotateCb = proc(ksId: uint64) =
    # This callback is called when memtable size exceeds threshold
    # It enqueues a rotation task to the worker pool
    if db.inner.workerPool != nil:
      # Find the keyspace and request rotation
      db.inner.keyspacesLock.acquire()
      defer: db.inner.keyspacesLock.release()
      for name, ks in db.inner.keyspaces.pairs:
        if ks.inner.id == ksId:
          db.inner.workerPool.requestMemtableRotation(ks)
          break

  # Create keyspace with all required components
  let keyspace = ks.newKeyspace(
    keyspaceId,
    tree,
    name,
    keyConfig,
    db.inner.supervisor,
    db.inner.stats,
    addr db.inner.isPoisoned,
    db.inner.supervisor.inner.flushManager,
    onRotateCb
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

proc keyspaceCount*(db: Database): int =
  ## Returns the number of keyspaces in the database.
  db.inner.keyspacesLock.acquire()
  defer: db.inner.keyspacesLock.release()
  db.inner.keyspaces.len

proc deleteKeyspace*(db: Database, name: string): StorageResult[void] =
  ## Delete a keyspace and all its data.
  ##
  ## This operation:
  ## 1. Marks the keyspace as deleted (prevents new operations)
  ## 2. Closes the keyspace files
  ## 3. Deletes all keyspace files from disk
  ## 4. Removes the keyspace from memory
  ##
  ## Warning: This is a destructive operation and cannot be undone.

  db.inner.keyspacesLock.acquire()
  defer: db.inner.keyspacesLock.release()

  # Check if keyspace exists
  if name notin db.inner.keyspaces:
    return err[void, StorageError](StorageError(
      kind: seStorage,
      storageError: "Keyspace not found: " & name
    ))

  let keyspace = db.inner.keyspaces[name]
  let keyspaceId = keyspace.inner.id
  let keyspacePath = db.inner.config.path / KEYSPACES_FOLDER / $keyspaceId

  logInfo("Deleting keyspace: " & name & " (id=" & $keyspaceId & ")")

  # Mark keyspace as deleted to prevent new operations
  keyspace.inner.isDeleted.store(true, moRelease)

  # Close the keyspace LSM tree (flush any pending data)
  # Note: In a full implementation, we would wait for pending operations

  # Remove from memory
  db.inner.keyspaces.del(name)

  # Remove from metadata
  db.inner.metaKeyspace.keyspaces.del(name)
  db.inner.metaKeyspace.reverseMap.del(keyspaceId)

  # Persist metadata
  let saveResult = db.saveKeyspaceMeta()
  if saveResult.isErr:
    logInfo("Warning: Failed to save metadata after keyspace deletion")

  # Delete keyspace files from disk
  try:
    if dirExists(keyspacePath):
      removeDir(keyspacePath)
      logInfo("Deleted keyspace directory: " & keyspacePath)
  except OSError as e:
    logInfo("Warning: Failed to delete keyspace directory: " & e.msg)
    # Don't fail the operation - the keyspace is already removed from memory

  logInfo("Keyspace " & name & " deleted successfully")
  return okVoid

proc listKeyspaceNames*(db: Database): seq[string] =
  db.inner.keyspacesLock.acquire()
  defer: db.inner.keyspacesLock.release()
  toSeq(db.inner.keyspaces.keys())

proc diskSpace*(db: Database): uint64 =
  ## Returns the total disk space used by the database in bytes.
  ## This includes all SSTables across all keyspaces.
  db.inner.keyspacesLock.acquire()
  defer: db.inner.keyspacesLock.release()

  result = 0
  for name, keyspace in db.inner.keyspaces.pairs:
    result += keyspace.diskSpace()

proc journalDiskSpace*(db: Database): uint64 =
  ## Returns the disk space used by journals.
  let journalsPath = db.inner.config.path / "journals"
  if not dirExists(journalsPath):
    return 0

  result = 0
  for kind, path in walkDir(journalsPath):
    if kind == pcFile:
      result += uint64(getFileSize(path))

proc journalCount*(db: Database): int =
  ## Returns the number of journal files (sealed + active).
  ## This is useful for monitoring and debugging.
  db.inner.supervisor.inner.journalManagerLock.acquire()
  defer: db.inner.supervisor.inner.journalManagerLock.release()
  return db.inner.supervisor.inner.journalManager.journalCount()

proc cacheCapacity*(db: Database): uint64 =
  ## Returns the block cache capacity in bytes.
  ## This is the configured capacity for each keyspace's block cache.
  ## Returns 0 if no keyspaces exist yet.
  db.inner.keyspacesLock.acquire()
  defer: db.inner.keyspacesLock.release()

  # Return the cache capacity from the first keyspace's config
  # (all keyspaces use the same default cache capacity)
  for name, keyspace in db.inner.keyspaces.pairs:
    return keyspace.inner.tree.config.cacheCapacity

  # No keyspaces yet, return default
  return 256 * 1024 * 1024'u64 # 256 MiB default

proc getMetrics*(db: Database): DatabaseMetrics =
  ## Returns aggregate metrics for the entire database.
  ##
  ## Includes:
  ## - Total keyspaces count
  ## - Aggregate disk space
  ## - Write buffer size
  ## - Total reads/writes across all keyspaces
  ## - Compaction statistics

  db.inner.keyspacesLock.acquire()
  defer: db.inner.keyspacesLock.release()

  result = DatabaseMetrics()
  result.keyspaceCount = db.inner.keyspaces.len

  # Aggregate metrics from all keyspaces
  for name, keyspace in db.inner.keyspaces.pairs:
    let ksMetrics = keyspace.metrics()
    result.totalDiskSpace += ksMetrics.diskSpaceBytes
    result.totalReads += ksMetrics.reads
    result.totalWrites += ksMetrics.writes
    result.totalCacheHits += ksMetrics.cacheHits
    result.totalCacheMisses += ksMetrics.cacheMisses

  # Add journal disk space
  result.totalJournalSpace = db.journalDiskSpace()

  # Write buffer size
  result.writeBufferSize = db.inner.supervisor.inner.writeBufferSize.get()

  # Compaction stats
  result.activeCompactions = db.inner.stats.activeCompactionCount.load(moRelaxed)
  result.compactionsCompleted = db.inner.stats.compactionsCompleted.load(moRelaxed)
  result.timeCompactingUs = db.inner.stats.timeCompacting.load(moRelaxed)

proc descriptorTable*(db: Database): DescriptorTable =
  ## Returns the shared descriptor table for file handle caching.
  ##
  ## This can be used by keyspaces and LSM trees to cache open file handles.
  db.inner.descriptorTable

proc close*(db: Database) =
  ## Close the database
  db.inner.stopSignal.stop.store(true, moRelease)

  # Stop worker pool
  if db.inner.workerPool != nil:
    db.inner.workerPool.stop()

  # Close descriptor table
  if db.inner.descriptorTable != nil:
    db.inner.descriptorTable.close()

  # Journal will be cleaned up by GC
  logInfo("Database closed")

# ============================================================================
# Write Batch Support
# ============================================================================

# Import batch after Database type is defined
import fractio/storage/batch as batch_module

proc batch*(db: Database): batch_module.WriteBatch =
  ## Creates a new write batch for this database.
  ##
  ## Example:
  ##   var wb = db.batch()
  ##   wb.insert(ks1, "key1", "value1")
  ##   wb.insert(ks2, "key2", "value2")
  ##   let result = db.commit(wb)
  ##
  ## Note: Currently commit() applies entries individually. For true atomicity,
  ## the journal would need to be used. This is a simplified implementation.
  # Cast to the batch's Database type (they're both ref objects)
  result = batch_module.newWriteBatch(cast[batch_module.Database](db))

proc commit*(db: Database, wb: batch_module.WriteBatch): StorageResult[void] =
  ## Commits a write batch to the database atomically.
  ##
  ## The batch is first written to the journal (for durability), then applied
  ## to memtables. If a crash occurs after journal write but before memtable
  ## apply, recovery will replay the batch from the journal.
  ##
  ## Returns an error if any operation fails.
  if wb.isEmpty:
    return okVoid

  # Check poisoned flag first
  if db.inner.isPoisoned.load(moRelaxed):
    return err[void, StorageError](StorageError(kind: sePoisoned))

  # Acquire journal lock
  var guard = db.inner.supervisor.inner.journal.getWriter()

  # Check poisoned flag again after acquiring lock (TOCTOU)
  if db.inner.isPoisoned.load(moRelaxed):
    guard.release()
    return err[void, StorageError](StorageError(kind: sePoisoned))

  # Get a single sequence number for the entire batch
  let batchSeqno = db.inner.supervisor.inner.seqno.next()

  # Convert batch items to journal format
  var journalItems: seq[writer.BatchItem] = @[]
  for item in wb.data:
    if item.keyspace == nil:
      continue

    # Check if keyspace is deleted
    if item.keyspace.inner.isDeleted.load(moRelaxed):
      guard.release()
      return err[void, StorageError](StorageError(kind: seKeyspaceDeleted))

    journalItems.add(writer.BatchItem(
      keyspace: writer.BatchItemKeyspace(id: item.keyspace.inner.id),
      key: item.key,
      value: item.value,
      valueType: item.valueType
    ))

  # Write entire batch to journal (atomicity guarantee)
  let writeResult = guard.journal.writer.writeBatch(journalItems, batchSeqno)
  if writeResult.isErr:
    guard.release()
    return err[void, StorageError](writeResult.error)

  # Persist if durability mode is set
  if wb.durability.isSome:
    let persistResult = guard.persist(wb.durability.get())
    if persistResult.isErr:
      db.inner.isPoisoned.store(true, moRelease)
      guard.release()
      return err[void, StorageError](StorageError(kind: sePoisoned))

  # Track batch size for write buffer management
  var batchSize: uint64 = 0

  # Apply to memtables (still holding journal lock for consistency)
  for item in wb.data:
    if item.keyspace == nil:
      continue

    let keyspace = item.keyspace

    # Apply the operation with the batch seqno
    var itemSize: uint64 = 0
    case item.valueType
    of vtValue:
      let res = keyspace.inner.tree.insert(item.key, item.value, batchSeqno)
      itemSize = res.itemSize
    of vtTombstone:
      let res = keyspace.inner.tree.remove(item.key, batchSeqno)
      itemSize = res.itemSize
    of vtWeakTombstone:
      let res = keyspace.inner.tree.removeWeak(item.key, batchSeqno)
      itemSize = res.itemSize
    of vtIndirection:
      guard.release()
      return err[void, StorageError](StorageError(
        kind: seStorage,
        storageError: "Cannot apply indirection value to batch"
      ))

    batchSize += itemSize

  # Publish the batch seqno to snapshot tracker
  db.inner.supervisor.inner.snapshotTracker.publish(batchSeqno)

  # Release journal lock
  guard.release()

  # Update write buffer size
  discard db.inner.supervisor.inner.writeBufferSize.allocate(batchSize)

  # Check for memtable rotation on affected keyspaces
  # (This could trigger write stall in a future implementation)
  for item in wb.data:
    if item.keyspace != nil:
      let memtableSize = item.keyspace.inner.tree.activeMemtableSize()
      if memtableSize > item.keyspace.inner.config.maxMemtableSize:
        # Request rotation through callback
        if item.keyspace.inner.requestRotationCb != nil:
          item.keyspace.inner.requestRotationCb(item.keyspace.inner.id)

  return okVoid

# ============================================================================
# Transaction Support
# ============================================================================

import fractio/storage/tx as tx_module

type
  WriteTransaction* = tx_module.WriteTransaction

proc beginTx*(db: Database): tx_module.WriteTransaction =
  ## Begins a new write transaction.
  ##
  ## Transactions provide:
  ## - Atomicity: All writes succeed or none do
  ## - Isolation: Read-your-own-writes (RYOW) semantics
  ## - Consistency: Snapshot-based reads
  ##
  ## Only one write transaction can be active at a time (single-writer).
  ## This call will block if another transaction is active.
  ##
  ## Example:
  ##   var tx = db.beginTx()
  ##   txInsert(tx, ks, "key1", "value1")
  ##   txInsert(tx, ks, "key2", "value2")
  ##   if db.commitTx(tx).isOk:
  ##     echo "Transaction committed"
  ##   else:
  ##     db.rollbackTx(tx)
  let snapshotSeqno = db.visibleSeqno()
  return tx_module.newWriteTransaction(snapshotSeqno)

proc beginTx*(db: Database, durability: Option[
    PersistMode]): tx_module.WriteTransaction =
  ## Begins a new write transaction with the specified durability mode.
  let snapshotSeqno = db.visibleSeqno()
  var tx = tx_module.newWriteTransaction(snapshotSeqno)
  tx.durability = durability
  return tx

proc commitTx*(db: Database, tx: var tx_module.WriteTransaction): StorageResult[void] =
  ## Commits a write transaction.
  ##
  ## All changes made in the transaction are applied atomically.
  ## After commit, the transaction is no longer active.
  ##
  ## Returns an error if:
  ## - The transaction is not active
  ## - Any keyspace was deleted
  ## - The database is poisoned
  if not tx.isActive:
    return err[void, StorageError](StorageError(
      kind: seStorage,
      storageError: "Transaction is not active"
    ))

  # Check if database is poisoned
  if db.inner.isPoisoned.load(moRelaxed):
    return err[void, StorageError](StorageError(kind: sePoisoned))

  # Build keyspaces table for commit
  var keyspaces: Table[string, ks.Keyspace]
  db.inner.keyspacesLock.acquire()
  for name, keyspace in db.inner.keyspaces:
    keyspaces[name] = keyspace
  db.inner.keyspacesLock.release()

  # Commit the transaction
  let result = tx.commit(keyspaces)

  return result

proc rollbackTx*(db: Database, tx: var tx_module.WriteTransaction) =
  ## Rolls back a write transaction.
  ##
  ## All changes made in the transaction are discarded.
  ## After rollback, the transaction is no longer active.
  tx.rollback()

proc txInsert*(tx: var WriteTransaction, keyspace: ks.Keyspace,
               key: string, value: string): StorageResult[void] =
  ## Inserts a key-value pair into the transaction.
  tx_module.txInsert(tx, keyspace, key, value)

proc txRemove*(tx: var WriteTransaction, keyspace: ks.Keyspace,
               key: string): StorageResult[void] =
  ## Removes a key from the transaction (inserts a tombstone).
  tx_module.txRemove(tx, keyspace, key)

proc txRemoveWeak*(tx: var WriteTransaction, keyspace: ks.Keyspace,
                   key: string): StorageResult[void] =
  ## Removes a key from the transaction (inserts a weak tombstone).
  tx_module.txRemoveWeak(tx, keyspace, key)

proc txGet*(tx: var WriteTransaction, keyspace: ks.Keyspace,
            key: string): StorageResult[Option[string]] =
  ## Gets a value, checking uncommitted writes first (RYOW).
  tx_module.txGet(tx, keyspace, key)

proc txContainsKey*(tx: var WriteTransaction, keyspace: ks.Keyspace,
                    key: string): StorageResult[bool] =
  ## Checks if a key exists, checking uncommitted writes first.
  tx_module.txContainsKey(tx, keyspace, key)

proc txSizeOf*(tx: var WriteTransaction, keyspace: ks.Keyspace,
               key: string): StorageResult[Option[uint32]] =
  ## Gets the size of a value, checking uncommitted writes first.
  tx_module.txSizeOf(tx, keyspace, key)

proc txTake*(tx: var WriteTransaction, keyspace: ks.Keyspace,
             key: string): StorageResult[Option[string]] =
  ## Removes a key and returns its previous value atomically.
  tx_module.txTake(tx, keyspace, key)

proc txFetchUpdate*(tx: var WriteTransaction, keyspace: ks.Keyspace,
                    key: string,
                    f: proc(v: Option[string]): Option[string]): StorageResult[
                        Option[string]] =
  ## Atomically updates a key and returns the previous value.
  tx_module.txFetchUpdate(tx, keyspace, key, f)

proc txUpdateFetch*(tx: var WriteTransaction, keyspace: ks.Keyspace,
                    key: string,
                    f: proc(v: Option[string]): Option[string]): StorageResult[
                        Option[string]] =
  ## Atomically updates a key and returns the new value.
  tx_module.txUpdateFetch(tx, keyspace, key, f)
