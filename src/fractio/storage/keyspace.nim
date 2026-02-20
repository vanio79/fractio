# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Keyspace Implementation
##
## A keyspace (a.k.a. column family) provides an isolated key-value namespace
## within a database.

import fractio/storage/[error, types, journal, snapshot_tracker, stats, supervisor,
                        write_buffer_manager, iter, snapshot]
import fractio/storage/flush/manager
import fractio/storage/lsm_tree/[types as lsm_types, lsm_tree, memtable]
import fractio/storage/lsm_tree/sstable/reader
import fractio/storage/lsm_tree/sstable/types as sst_types
import fractio/storage/journal/writer # For PersistMode
import fractio/storage/keyspace/options
import fractio/storage/keyspace/name
import std/[atomics, options, algorithm, strutils, os, locks]

# Keyspace key (a.k.a. column family, locality group)
type
  KeyspaceKey* = string
  InternalKeyspaceId* = uint64

# Forward declarations for types we don't fully define here
type
  Database* = object
  Ingestion* = object
  LockedFileGuard* = object
  WorkerMessage* = object
  WorkerMessager* = object # Placeholder for message channel
  Iter* = object # Placeholder for iterator type
  Guard* = object # Placeholder for guard type

# Keyspace inner structure
type
  KeyspaceInner* = ref object
    # Internal ID
    id*: InternalKeyspaceId

    # Keyspace name
    name*: KeyspaceKey

    # Keyspace configuration
    config*: CreateOptions

    # If true, the keyspace is marked as deleted
    isDeleted*: Atomic[bool]

    # If true, fsync failed during persisting (shared with database)
    isPoisoned*: ptr Atomic[bool]

    # LSM-tree
    tree*: lsm_types.LsmTree

    supervisor*: Supervisor
    stats*: Stats
    workerMessager*: WorkerMessager # Placeholder for message channel
    flushManager*: FlushManager     # Reference to flush manager
                                    # Callback to request rotation - takes keyspace ID
    requestRotationCb*: proc(ksId: uint64) {.closure.}

    lockFile*: LockedFileGuard

# Handle to a keyspace
type
  Keyspace* = ref object
    inner*: KeyspaceInner

# Constructor from components
proc newKeyspace*(keyspaceId: InternalKeyspaceId,
                  tree: lsm_types.LsmTree,
                  name: KeyspaceKey,
                  config: CreateOptions,
                  supervisor: Supervisor,
                  stats: Stats,
                  isPoisoned: ptr Atomic[bool],
                  flushManager: FlushManager = nil,
                  requestRotationCb: proc(
                      ksId: uint64) {.closure.} = nil): Keyspace =
  var inner = KeyspaceInner(
    id: keyspaceId,
    name: name,
    config: config,
    tree: tree,
    supervisor: supervisor,
    stats: stats,
    isPoisoned: isPoisoned,
    flushManager: flushManager,
    requestRotationCb: requestRotationCb
  )
  inner.isDeleted.store(false, moRelaxed)
  Keyspace(inner: inner)

# Constructor from database (deprecated - use newKeyspace)
proc fromDatabase*(keyspaceId: InternalKeyspaceId, db: Database,
                   tree: lsm_types.LsmTree, name: KeyspaceKey,
                   config: CreateOptions): Keyspace =
  var inner = KeyspaceInner(
    id: keyspaceId,
    name: name,
    config: config,
    tree: tree
  )
  inner.isDeleted.store(false, moRelaxed)
  Keyspace(inner: inner)

# Get keyspace ID
proc id*(keyspace: Keyspace): InternalKeyspaceId =
  keyspace.inner.id

# Get keyspace name
proc name*(keyspace: Keyspace): KeyspaceKey =
  keyspace.inner.name

# Clear the entire keyspace
proc clear*(keyspace: Keyspace): StorageResult[void] =
  if keyspace.inner.isDeleted.load(moRelaxed):
    return asErr(StorageError(kind: seKeyspaceDeleted))

  if keyspace.inner.supervisor == nil or
      keyspace.inner.supervisor.inner.journal == nil:
    return asErr(StorageError(kind: seIo, ioError: "Journal not available"))

  var guard = keyspace.inner.supervisor.inner.journal.getWriter()

  if keyspace.inner.isPoisoned != nil and keyspace.inner.isPoisoned[].load(moRelaxed):
    guard.release()
    return asErr(StorageError(kind: sePoisoned))

  let seqno = keyspace.inner.supervisor.inner.seqno.next()

  let writeResult = guard.writeClear(keyspace.inner.id, seqno)
  if writeResult.isErr:
    guard.release()
    return err[void, StorageError](writeResult.error)

  if not keyspace.inner.config.manualJournalPersist:
    let persistResult = guard.persist(pmBuffer)
    if persistResult.isErr:
      if keyspace.inner.isPoisoned != nil:
        keyspace.inner.isPoisoned[].store(true, moRelease)
      guard.release()
      return asErr(StorageError(kind: sePoisoned))

  # Clear the tree
  let clearResult = keyspace.inner.tree.clear()
  if clearResult.isErr:
    guard.release()
    return err[void, StorageError](clearResult.error)

  keyspace.inner.supervisor.inner.snapshotTracker.publish(seqno)
  guard.release()

  return okVoid

# Disk space usage
proc diskSpace*(keyspace: Keyspace): uint64 =
  keyspace.inner.tree.diskSpace()

# Approximate length
proc approximateLen*(keyspace: Keyspace): int =
  keyspace.inner.tree.approximateLen()

# Check if empty
proc isEmpty*(keyspace: Keyspace): StorageResult[bool] =
  let seqno = keyspace.inner.supervisor.inner.seqno.get()
  return ok[bool, StorageError](keyspace.inner.tree.isEmpty(seqno, none(uint64)))

# Check if contains key
proc containsKey*(keyspace: Keyspace, key: string): StorageResult[bool] =
  let seqno = keyspace.inner.supervisor.inner.seqno.get()
  return ok[bool, StorageError](keyspace.inner.tree.containsKey(key, seqno))

# Get value by key
proc get*(keyspace: Keyspace, key: string): StorageResult[Option[UserValue]] =
  let seqno = keyspace.inner.supervisor.inner.seqno.get()
  let value = keyspace.inner.tree.get(key, seqno)
  return ok[Option[UserValue], StorageError](value)

# Check write halt
proc checkWriteHalt*(keyspace: Keyspace) =
  discard

# Local backpressure
proc localBackpressure*(keyspace: Keyspace): bool =
  false

# Request rotation - calls the registered callback to request background memtable rotation
proc requestRotation*(keyspace: Keyspace) =
  if keyspace.inner.requestRotationCb != nil:
    keyspace.inner.requestRotationCb(keyspace.inner.id)

# Request flush - enqueues a flush task for this keyspace
proc requestFlush*(keyspace: Keyspace) =
  if keyspace.inner.flushManager != nil:
    let task = Task(keyspacePtr: cast[pointer](keyspace))
    keyspace.inner.flushManager.enqueue(task)

# Check memtable rotate
proc checkMemtableRotate*(keyspace: Keyspace, size: uint64) =
  if size > keyspace.inner.config.maxMemtableSize:
    keyspace.requestRotation()

# Maintenance
proc maintenance*(keyspace: Keyspace, memtableSize: uint64) =
  keyspace.checkMemtableRotate(memtableSize)
  discard keyspace.localBackpressure()

# Insert key-value pair
proc insert*(keyspace: Keyspace, key: UserKey, value: UserValue): StorageResult[void] =
  if keyspace.inner.isDeleted.load(moRelaxed):
    return asErr(StorageError(kind: seKeyspaceDeleted))

  if keyspace.inner.supervisor == nil or
      keyspace.inner.supervisor.inner.journal == nil:
    return asErr(StorageError(kind: seIo, ioError: "Journal not available"))

  var guard = keyspace.inner.supervisor.inner.journal.getWriter()

  if keyspace.inner.isPoisoned != nil and keyspace.inner.isPoisoned[].load(moRelaxed):
    guard.release()
    return asErr(StorageError(kind: sePoisoned))

  let seqno = keyspace.inner.supervisor.inner.seqno.next()

  # Write to journal (WAL)
  let writeResult = guard.writeRaw(keyspace.inner.id, key, value, vtValue, seqno)
  if writeResult.isErr:
    guard.release()
    return err[void, StorageError](writeResult.error)

  # Persist if not manual journal persist mode
  if not keyspace.inner.config.manualJournalPersist:
    let persistResult = guard.persist(pmBuffer)
    if persistResult.isErr:
      if keyspace.inner.isPoisoned != nil:
        keyspace.inner.isPoisoned[].store(true, moRelease)
      guard.release()
      return asErr(StorageError(kind: sePoisoned))

  # Insert into LSM tree
  let (itemSize, memtableSize) = keyspace.inner.tree.insert(key, value, seqno)

  # Publish sequence number
  keyspace.inner.supervisor.inner.snapshotTracker.publish(seqno)

  guard.release()

  # Allocate write buffer size
  discard keyspace.inner.supervisor.inner.writeBufferSize.allocate(itemSize)

  # Run maintenance
  keyspace.maintenance(memtableSize)

  return okVoid

# Remove key
proc remove*(keyspace: Keyspace, key: UserKey): StorageResult[void] =
  if keyspace.inner.isDeleted.load(moRelaxed):
    return asErr(StorageError(kind: seKeyspaceDeleted))

  if keyspace.inner.supervisor == nil or
      keyspace.inner.supervisor.inner.journal == nil:
    return asErr(StorageError(kind: seIo, ioError: "Journal not available"))

  var guard = keyspace.inner.supervisor.inner.journal.getWriter()

  if keyspace.inner.isPoisoned != nil and keyspace.inner.isPoisoned[].load(moRelaxed):
    guard.release()
    return asErr(StorageError(kind: sePoisoned))

  let seqno = keyspace.inner.supervisor.inner.seqno.next()

  # Write tombstone to journal
  let writeResult = guard.writeRaw(keyspace.inner.id, key, "", vtTombstone, seqno)
  if writeResult.isErr:
    guard.release()
    return err[void, StorageError](writeResult.error)

  if not keyspace.inner.config.manualJournalPersist:
    let persistResult = guard.persist(pmBuffer)
    if persistResult.isErr:
      if keyspace.inner.isPoisoned != nil:
        keyspace.inner.isPoisoned[].store(true, moRelease)
      guard.release()
      return asErr(StorageError(kind: sePoisoned))

  # Remove from tree (writes a tombstone)
  let (itemSize, memtableSize) = keyspace.inner.tree.remove(key, seqno)

  keyspace.inner.supervisor.inner.snapshotTracker.publish(seqno)

  guard.release()

  discard keyspace.inner.supervisor.inner.writeBufferSize.allocate(itemSize)

  keyspace.maintenance(memtableSize)

  return okVoid

# Remove key weakly - for merge operations
proc removeWeak*(keyspace: Keyspace, key: UserKey): StorageResult[void] =
  if keyspace.inner.isDeleted.load(moRelaxed):
    return asErr(StorageError(kind: seKeyspaceDeleted))

  if keyspace.inner.supervisor == nil or
      keyspace.inner.supervisor.inner.journal == nil:
    return asErr(StorageError(kind: seIo, ioError: "Journal not available"))

  var guard = keyspace.inner.supervisor.inner.journal.getWriter()

  if keyspace.inner.isPoisoned != nil and keyspace.inner.isPoisoned[].load(moRelaxed):
    guard.release()
    return asErr(StorageError(kind: sePoisoned))

  let seqno = keyspace.inner.supervisor.inner.seqno.next()

  # Write weak tombstone to journal
  let writeResult = guard.writeRaw(keyspace.inner.id, key, "", vtWeakTombstone, seqno)
  if writeResult.isErr:
    guard.release()
    return err[void, StorageError](writeResult.error)

  if not keyspace.inner.config.manualJournalPersist:
    let persistResult = guard.persist(pmBuffer)
    if persistResult.isErr:
      if keyspace.inner.isPoisoned != nil:
        keyspace.inner.isPoisoned[].store(true, moRelease)
      guard.release()
      return asErr(StorageError(kind: sePoisoned))

  # Remove from tree (writes a weak tombstone)
  let (itemSize, memtableSize) = keyspace.inner.tree.removeWeak(key, seqno)

  keyspace.inner.supervisor.inner.snapshotTracker.publish(seqno)

  guard.release()

  discard keyspace.inner.supervisor.inner.writeBufferSize.allocate(itemSize)

  keyspace.maintenance(memtableSize)

  return okVoid

# Rotate memtable
proc rotateMemtable*(keyspace: Keyspace): StorageResult[bool] =
  let rotated = keyspace.inner.tree.rotateMemtable()
  return ok[bool, StorageError](rotated.isSome)

# L0 table count
proc l0TableCount*(keyspace: Keyspace): int =
  keyspace.inner.tree.l0RunCount()

# Table count
proc tableCount*(keyspace: Keyspace): int =
  keyspace.inner.tree.tableCount()

# Sealed memtable count
proc sealedMemtableCount*(keyspace: Keyspace): int =
  keyspace.inner.tree.sealedMemtableCount()

# Has sealed memtables to flush
proc hasSealedMemtables*(keyspace: Keyspace): bool =
  keyspace.inner.tree.hasSealedMemtables()

# Flush oldest sealed memtable
proc flushOldestSealed*(keyspace: Keyspace): StorageResult[uint64] =
  keyspace.inner.tree.flushOldestSealed()

# Major compaction
proc majorCompaction*(keyspace: Keyspace): StorageResult[void] =
  let gcWatermark = keyspace.inner.supervisor.inner.snapshotTracker.getSeqnoSafeToGc()
  return keyspace.inner.tree.majorCompact(0'u64, gcWatermark)

# Create an iterator over all keys in the keyspace
proc iter*(keyspace: Keyspace): KeyspaceIter =
  ## Creates an iterator over all key-value pairs in the keyspace.
  ## The iterator holds a snapshot to ensure consistent reads.

  # Open a snapshot to ensure consistent reads
  let nonce = keyspace.inner.supervisor.inner.snapshotTracker.open()
  let seqno = nonce.instant

  result = newKeyspaceIter(nonce)

  # Collect all entries from memtables
  # Active memtable
  let activeEntries = keyspace.inner.tree.activeMemtable.getSortedEntries()
  for e in activeEntries:
    if e.seqno <= seqno:
      result.add(e.key, e.value, e.seqno, e.valueType)

  # Sealed memtables
  for memtable in keyspace.inner.tree.sealedMemtables:
    let entries = memtable.getSortedEntries()
    for e in entries:
      if e.seqno <= seqno:
        result.add(e.key, e.value, e.seqno, e.valueType)

  # Collect entries from SSTables - need to acquire version lock
  keyspace.inner.tree.versionLock.acquire()
  defer: keyspace.inner.tree.versionLock.release()

  for level in keyspace.inner.tree.tables:
    for sstable in level:
      if sstable.path.len > 0:
        if not fileExists(sstable.path):
          continue
        let readerResult = openSsTable(sstable.path)
        if readerResult.isErr:
          continue
        let reader = readerResult.value
        # Read all entries from the SSTable
        for idxEntry in reader.indexBlock.entries:
          let blockResult = readDataBlock(reader.stream, idxEntry.handle)
          if blockResult.isOk:
            let dataBlk = blockResult.value
            for entry in dataBlk.entries:
              if entry.seqno <= seqno:
                # Map uint8 valueType to lsm_types.ValueType enum
                var vt: lsm_types.ValueType
                case entry.valueType
                of 0'u8: vt = lsm_types.vtValue
                of 1'u8: vt = lsm_types.vtTombstone
                of 2'u8: vt = lsm_types.vtWeakTombstone
                else: vt = lsm_types.vtIndirection
                result.add(entry.key, entry.value, entry.seqno, vt)
        reader.close()

  # Sort all entries by key
  result.sortEntries()

# Create an iterator over a range of keys
proc rangeIter*(keyspace: Keyspace, startKey: string,
    endKey: string): KeyspaceIter =
  ## Creates an iterator over a range of keys [startKey, endKey].

  let fullIter = keyspace.iter()

  let nonce = keyspace.inner.supervisor.inner.snapshotTracker.open()
  result = newKeyspaceIter(nonce)

  # Filter entries within the range
  for entry in fullIter.entries:
    if entry.key >= startKey and entry.key <= endKey:
      result.add(entry.key, entry.value, entry.seqno, entry.valueType)

  result.sortEntries()

# Create an iterator over keys with a prefix
proc prefixIter*(keyspace: Keyspace, prefixStr: string): KeyspaceIter =
  ## Creates an iterator over all keys starting with the given prefix.

  let fullIter = keyspace.iter()

  let nonce = keyspace.inner.supervisor.inner.snapshotTracker.open()
  result = newKeyspaceIter(nonce)

  # Filter entries with the prefix
  for entry in fullIter.entries:
    if entry.key.startsWith(prefixStr):
      result.add(entry.key, entry.value, entry.seqno, entry.valueType)
    elif entry.key > prefixStr:
      # Entries are sorted, so we can stop once we pass the prefix
      break

  result.sortEntries()
