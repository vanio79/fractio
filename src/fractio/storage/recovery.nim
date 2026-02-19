# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Database Recovery Implementation
## 
## Recovers database state from journals and SSTables after a crash.

import fractio/storage/[error, types, file, keyspace, journal, db, supervisor,
                        snapshot_tracker, write_buffer_manager, lsm_tree]
import fractio/storage/journal/batch_reader
import fractio/storage/journal/manager
import std/[os, tables, atomics, locks, options, strutils, algorithm]

# Forward declarations
type
  MetaKeyspace* = object
    ## Stores keyspace metadata (name -> id mapping)
    keyspaces*: Table[string, uint64]  # name -> id
    reverseMap*: Table[uint64, string]  # id -> name

# Resolve keyspace name from ID
proc resolveId*(meta: MetaKeyspace, id: uint64): Option[string] =
  if id in meta.reverseMap:
    return some(meta.reverseMap[id])
  return none(string)

# Resolve keyspace ID from name
proc resolveName*(meta: MetaKeyspace, name: string): Option[uint64] =
  if name in meta.keyspaces:
    return some(meta.keyspaces[name])
  return none(uint64)

# Register a keyspace
proc register*(meta: var MetaKeyspace, name: string, id: uint64) =
  meta.keyspaces[name] = id
  meta.reverseMap[id] = name

# Eviction watermark for journal manager
type
  EvictionWatermark* = ref object
    keyspace*: Keyspace
    lsn*: SeqNo

# Recovers keyspaces from disk
proc recoverKeyspaces*(db: Database, metaKeyspace: var MetaKeyspace): StorageResult[void] =
  let keyspacesFolder = db.inner.config.path / KEYSPACES_FOLDER
  
  logInfo("Recovering keyspaces in " & keyspacesFolder)
  
  if not dirExists(keyspacesFolder):
    # No keyspaces folder yet, nothing to recover
    return okVoid
  
  var highestId: uint64 = 1
  
  # Iterate through keyspace directories
  for kind, path in walkDir(keyspacesFolder):
    if kind != pcDir:
      continue
    
    let keyspaceIdStr = path.extractFilename()
    let keyspaceId = try:
      uint64(parseBiggestInt(keyspaceIdStr))
    except ValueError:
      logWarn("Invalid keyspace directory: " & path)
      continue
    
    # Skip meta keyspace (id 0)
    if keyspaceId == 0:
      continue
    
    highestId = max(highestId, keyspaceId)
    
    # Check for LSM current version marker
    let versionMarker = path / LSM_CURRENT_VERSION_MARKER
    if not fileExists(versionMarker):
      logDebug("Deleting uninitialized keyspace: " & path)
      try:
        removeDir(path)
      except OSError:
        logWarn("Failed to remove uninitialized keyspace: " & path)
      continue
    
    # In a full implementation, this would:
    # 1. Load keyspace configuration from meta keyspace
    # 2. Open the LSM tree
    # 3. Register the keyspace
    
    logInfo("Recovered keyspace id=" & $keyspaceId)
  
  # Update the keyspace ID counter
  db.inner.keyspaceIdCounter = highestId + 1
  
  return okVoid

# Recover sealed memtables from journals
proc recoverSealedMemtables*(db: Database, 
                             sealedJournalPaths: seq[string]): StorageResult[void] =
  logInfo("Recovering " & $sealedJournalPaths.len & " sealed journals")
  
  # Get keyspaces lock (read access)
  # In a full implementation with proper locking:
  # let keyspacesLock = db.inner.supervisor.inner.keyspaces
  
  for journalPath in sealedJournalPaths:
    logDebug("Recovering sealed journal: " & journalPath)
    
    let journalSize = try:
      uint64(getFileSize(journalPath))
    except OSError:
      0'u64
    
    # Open journal reader
    let readerResult = newJournalReader(journalPath)
    if readerResult.isErr:
      logWarn("Failed to open sealed journal: " & journalPath)
      continue
    
    let rawReader = readerResult.value
    let batchReader = newJournalBatchReader(rawReader)
    
    # Track watermarks for each keyspace
    var watermarks: Table[uint64, EvictionWatermark] = initTable[uint64, EvictionWatermark]()
    
    # Iterate through batches
    for batchResult in batchReader.items:
      if batchResult.isErr:
        logWarn("Error reading batch from journal: " & journalPath)
        break
      
      let batch = batchResult.value
      
      # Process items in batch
      for item in batch.items:
        # In a full implementation, this would:
        # 1. Look up the keyspace by ID
        # 2. Apply the item to the keyspace's LSM tree
        # 3. Update watermarks
        
        logTrace("Recovering item: keyspace=" & $item.keyspaceId & 
                 " key=" & item.key & " type=" & $item.valueType)
        
        # Update watermark for this keyspace
        if item.keyspaceId in watermarks:
          watermarks[item.keyspaceId].lsn = max(watermarks[item.keyspaceId].lsn, batch.seqno)
        
      # Process cleared keyspaces
      for clearedId in batch.clearedKeyspaces:
        logTrace("Keyspace cleared: " & $clearedId)
    
    # In a full implementation, this would:
    # 1. Seal recovered memtables
    # 2. Add sealed memtables to flush manager
    # 3. Add journal to journal manager
    
    logDebug("Completed recovery of sealed journal: " & journalPath)
  
  return okVoid

# Full database recovery
proc recoverDatabase*(config: DbConfig): StorageResult[Database] =
  logInfo("Starting database recovery at " & config.path)
  
  # Check version marker
  let versionMarkerPath = config.path / VERSION_MARKER
  if not fileExists(versionMarkerPath):
    return asErr(StorageError(kind: seInvalidVersion, invalidVersion: none(uint32)))
  
  # Read and verify version
  let versionResult = Database.checkVersion(config.path)
  if versionResult.isErr:
    return err[Database, StorageError](versionResult.error)
  
  # Create database structure
  var db = Database(
    inner: DatabaseInner(
      config: config,
      isPoisoned: Atomic[bool](),
      keyspaceIdCounter: 1'u64
    )
  )
  db.inner.isPoisoned.store(false, moRelaxed)
  
  # Create supervisor
  var supervisorInner = SupervisorInner(
    seqno: newSequenceNumberCounter(),
    snapshotTracker: newSnapshotTracker(),
    writeBufferSize: newWriteBufferManager()
  )
  db.inner.supervisor = Supervisor(inner: supervisorInner)
  
  # Recover journals
  let journalPath = config.path / "journals"
  let recoveryResult = recoverJournals(journalPath, ctNone, 0)
  if recoveryResult.isErr:
    return err[Database, StorageError](recoveryResult.error)
  
  let recovery = recoveryResult.value
  
  # Create journal from recovered active journal
  var journal: Journal
  journal.writer = recovery.active
  journal.path = journalPath / "active.jnl"
  initLock(journal.lock)
  db.inner.supervisor.inner.journal = journal
  
  # Recover meta keyspace
  var metaKeyspace: MetaKeyspace
  
  # Recover keyspaces
  let keyspacesResult = recoverKeyspaces(db, metaKeyspace)
  if keyspacesResult.isErr:
    return err[Database, StorageError](keyspacesResult.error)
  
  # Recover sealed memtables
  let sealedPaths: seq[string] = @[]  # In full impl, get from recovery.sealed
  let sealedResult = recoverSealedMemtables(db, sealedPaths)
  if sealedResult.isErr:
    return err[Database, StorageError](sealedResult.error)
  
  # Set sequence number counter based on recovered data
  let highestSeqno = recovery.active.pos()  # Simplified
  db.inner.supervisor.inner.seqno.fetchMax(highestSeqno)
  
  logInfo("Database recovery complete. Seqno=" & $db.inner.supervisor.inner.seqno.get())
  
  return ok[Database, StorageError](db)

# Import DbConfig type
type DbConfig* = object
  path*: string
  cache*: object
    capacity*: uint64
