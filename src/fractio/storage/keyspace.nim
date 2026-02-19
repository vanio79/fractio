# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import fractio/storage/[error, types, file, flush, journal, snapshot,
    snapshot_tracker, stats, supervisor, lsm_tree, write_buffer_manager]
import fractio/storage/journal/writer # For PersistMode
import fractio/storage/keyspace/options
import fractio/storage/keyspace/name
import fractio/storage/keyspace/write_delay
import fractio/storage/keyspace/config
import std/[os, atomics, locks, times, options]

# Keyspace key (a.k.a. column family, locality group)
type
  KeyspaceKey* = string
  InternalKeyspaceId* = uint64

# Forward declarations
type
  Database* = object
  Ingestion* = object
  LockedFileGuard* = object
  AnyTree* = object
  WorkerMessage* = object
  WorkerMessager* = object # Placeholder for message channel
  Iter* = object # Placeholder for iterator type
  Guard* = object # Placeholder for guard type

# Apply configuration to base config
proc applyToBaseConfig*(config: LsmTreeConfig,
    ourConfig: CreateOptions): LsmTreeConfig =
  # In a full implementation, this would apply the configuration
  # For now, we'll just return the config
  return config

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

    # LSM-tree wrapper
    tree*: AnyTree

    supervisor*: Supervisor
    stats*: Stats
    workerMessager*: WorkerMessager # Placeholder for message channel

    lockFile*: LockedFileGuard

# Handle to a keyspace
type
  Keyspace* = ref object
    inner*: KeyspaceInner

# Constructor from database
proc fromDatabase*(keyspaceId: InternalKeyspaceId, db: Database, tree: AnyTree,
                   name: KeyspaceKey, config: CreateOptions): Keyspace =
  var inner = KeyspaceInner(
    id: keyspaceId,
    name: name,
    config: config,
    tree: tree
    # Other fields would be set from db in full implementation
  )
  inner.isDeleted.store(false, moRelaxed)
  Keyspace(inner: inner)

# Create new keyspace
proc createNew*(keyspaceId: InternalKeyspaceId, db: Database, name: KeyspaceKey,
                config: CreateOptions): StorageResult[Keyspace] =
  # In a full implementation, this would create the LSM tree
  # For now, we'll create a placeholder
  let tree = AnyTree()
  return ok[Keyspace, StorageError](fromDatabase(keyspaceId, db, tree, name, config))

# Get keyspace ID
proc id*(keyspace: Keyspace): InternalKeyspaceId =
  keyspace.inner.id

# Get keyspace name
proc name*(keyspace: Keyspace): KeyspaceKey =
  keyspace.inner.name

# Clear the entire keyspace
proc clear*(keyspace: Keyspace): StorageResult[void] =
  # Check if keyspace is deleted
  if keyspace.inner.isDeleted.load(moRelaxed):
    return asErr(StorageError(kind: seKeyspaceDeleted))

  # Get journal writer guard (acquires lock)
  if keyspace.inner.supervisor == nil or
      keyspace.inner.supervisor.inner.journal == nil:
    return asErr(StorageError(kind: seIo, ioError: "Journal not available"))

  var guard = keyspace.inner.supervisor.inner.journal.getWriter()

  # IMPORTANT: Check poisoned flag AFTER acquiring lock (TOCTOU prevention)
  if keyspace.inner.isPoisoned != nil and keyspace.inner.isPoisoned[].load(moRelaxed):
    guard.release()
    return asErr(StorageError(kind: sePoisoned))

  # Get next sequence number
  var seqnoCounter = keyspace.inner.supervisor.inner.seqno
  let seqno = seqnoCounter.next()

  # Write clear marker to journal
  let writeResult = guard.writeClear(keyspace.inner.id, seqno)
  if writeResult.isErr:
    guard.release()
    return err[void, StorageError](writeResult.error)

  # Persist if not manual
  if not keyspace.inner.config.manualJournalPersist:
    let persistResult = guard.persist(pmBuffer)
    if persistResult.isErr:
      # Mark as poisoned on persist failure
      if keyspace.inner.isPoisoned != nil:
        keyspace.inner.isPoisoned[].store(true, moRelease)
      guard.release()
      return asErr(StorageError(kind: sePoisoned))

  # Clear the tree (in a full implementation, this would clear the LSM tree)
  # self.tree.clear() ?

  # Publish sequence number to snapshot tracker
  keyspace.inner.supervisor.inner.snapshotTracker.publish(seqno)

  # Release the lock
  guard.release()

  return okVoid

# Fragmented blob bytes
proc fragmentedBlobBytes*(keyspace: Keyspace): uint64 =
  # In a full implementation, this would return fragmented bytes
  # For now, we'll return 0
  return 0

# Start ingestion
proc startIngestion*(keyspace: Keyspace): StorageResult[Ingestion] =
  # In a full implementation, this would start ingestion
  # For now, we'll return an error
  return err[Ingestion, StorageError](StorageError(kind: seStorage,
      storageError: "Not implemented"))

# Disk space usage
proc diskSpace*(keyspace: Keyspace): uint64 =
  # In a full implementation, this would return disk space usage
  # For now, we'll return 0
  return 0

# Iterator over the entire keyspace
proc iter*(keyspace: Keyspace): Iter =
  # In a full implementation, this would return an iterator
  # For now, we'll return a placeholder
  return Iter()

# Range iterator
proc range*(keyspace: Keyspace, startKey: string, endKey: string): Iter =
  # In a full implementation, this would return a range iterator
  # For now, we'll return a placeholder
  return Iter()

# Prefix iterator
proc prefix*(keyspace: Keyspace, prefixStr: string): Iter =
  # In a full implementation, this would return a prefix iterator
  # For now, we'll return a placeholder
  return Iter()

# Approximate length
proc approximateLen*(keyspace: Keyspace): int =
  # In a full implementation, this would return approximate length
  # For now, we'll return 0
  return 0

# Exact length
proc len*(keyspace: Keyspace): StorageResult[int] =
  # In a full implementation, this would count all items
  # For now, we'll return 0
  return ok[int, StorageError](0)

# Check if empty
proc isEmpty*(keyspace: Keyspace): StorageResult[bool] =
  # In a full implementation, this would check if empty
  # For now, we'll return true
  return ok[bool, StorageError](true)

# Check if contains key
proc containsKey*(keyspace: Keyspace, key: string): StorageResult[bool] =
  # In a full implementation, this would check if key exists
  # For now, we'll return false
  return ok[bool, StorageError](false)

# Get value by key
proc get*(keyspace: Keyspace, key: string): StorageResult[Option[UserValue]] =
  # In a full implementation, this would get the value
  # For now, we'll return none
  return ok[Option[UserValue], StorageError](none(UserValue))

# Get size of value by key
proc sizeOf*(keyspace: Keyspace, key: string): StorageResult[Option[uint32]] =
  # In a full implementation, this would get the size
  # For now, we'll return none
  return ok[Option[uint32], StorageError](none(uint32))

# Get first key-value pair
proc firstKeyValue*(keyspace: Keyspace): Option[Guard] =
  # In a full implementation, this would get the first key-value
  # For now, we'll return none
  return none(Guard)

# Get last key-value pair
proc lastKeyValue*(keyspace: Keyspace): Option[Guard] =
  # In a full implementation, this would get the last key-value
  # For now, we'll return none
  return none(Guard)

# Check if key-value separated
proc isKvSeparated*(keyspace: Keyspace): bool =
  # In a full implementation, this would check if KV separated
  # For now, we'll return false
  return false

# Rotate memtable and wait
proc rotateMemtableAndWait*(keyspace: Keyspace): StorageResult[void] =
  # In a full implementation, this would rotate memtable and wait
  # For now, we'll return success
  return okVoid

# Rotate memtable
proc rotateMemtable*(keyspace: Keyspace): StorageResult[bool] =
  # In a full implementation, this would rotate memtable
  # For now, we'll return false
  return ok[bool, StorageError](false)

# Inner rotate memtable
proc innerRotateMemtable*(keyspace: Keyspace,
    memtableId: uint64): StorageResult[bool] =
  # In a full implementation, this would perform inner rotation
  # For now, we'll return false
  return ok[bool, StorageError](false)

# Check write halt
proc checkWriteHalt*(keyspace: Keyspace) =
  # In a full implementation, this would check write halt
  # For now, we'll do nothing
  discard

# Local backpressure
proc localBackpressure*(keyspace: Keyspace): bool =
  # In a full implementation, this would check backpressure
  # For now, we'll return false
  return false

# Request rotation
proc requestRotation*(keyspace: Keyspace) =
  # In a full implementation, this would request rotation
  # For now, we'll do nothing
  discard

# Check memtable rotate
proc checkMemtableRotate*(keyspace: Keyspace, size: uint64) =
  # In a full implementation, this would check if memtable needs rotation
  if size > keyspace.inner.config.maxMemtableSize:
    keyspace.requestRotation()

# Maintenance
proc maintenance*(keyspace: Keyspace, memtableSize: uint64) =
  keyspace.checkMemtableRotate(memtableSize)
  discard keyspace.localBackpressure()

# L0 table count
proc l0TableCount*(keyspace: Keyspace): int =
  # In a full implementation, this would return L0 table count
  # For now, we'll return 0
  return 0

# Table count
proc tableCount*(keyspace: Keyspace): int =
  # In a full implementation, this would return table count
  # For now, we'll return 0
  return 0

# Blob file count
proc blobFileCount*(keyspace: Keyspace): int =
  # In a full implementation, this would return blob file count
  # For now, we'll return 0
  return 0

# Major compaction
proc majorCompaction*(keyspace: Keyspace): StorageResult[void] =
  # In a full implementation, this would perform major compaction
  # For now, we'll return success
  return okVoid

# Insert key-value pair - implements the full write path from Rust
proc insert*(keyspace: Keyspace, key: UserKey, value: UserValue): StorageResult[void] =
  # Check if keyspace is deleted
  if keyspace.inner.isDeleted.load(moRelaxed):
    return asErr(StorageError(kind: seKeyspaceDeleted))

  # Get journal writer guard (acquires lock)
  if keyspace.inner.supervisor == nil or
      keyspace.inner.supervisor.inner.journal == nil:
    return asErr(StorageError(kind: seIo, ioError: "Journal not available"))

  var guard = keyspace.inner.supervisor.inner.journal.getWriter()

  # IMPORTANT: Check poisoned flag AFTER acquiring lock (TOCTOU prevention)
  # This prevents a race condition where the database becomes poisoned
  # between the initial check and acquiring the lock
  if keyspace.inner.isPoisoned != nil and keyspace.inner.isPoisoned[].load(moRelaxed):
    guard.release()
    return asErr(StorageError(kind: sePoisoned))

  # Get next sequence number
  var seqnoCounter = keyspace.inner.supervisor.inner.seqno
  let seqno = seqnoCounter.next()

  # Write to journal (WAL - Write Ahead Log)
  # This ensures durability before the data is written to the tree
  let writeResult = guard.writeRaw(keyspace.inner.id, key, value, vtValue, seqno)
  if writeResult.isErr:
    guard.release()
    return err[void, StorageError](writeResult.error)

  # Persist if not manual journal persist mode
  if not keyspace.inner.config.manualJournalPersist:
    let persistResult = guard.persist(pmBuffer)
    if persistResult.isErr:
      # Mark database as poisoned on persist failure
      # This is a FATAL error - future writes will be rejected
      # as consistency cannot be guaranteed
      if keyspace.inner.isPoisoned != nil:
        keyspace.inner.isPoisoned[].store(true, moRelease)
      guard.release()
      return asErr(StorageError(kind: sePoisoned))

  # Insert into tree
  # In a full implementation, this would insert into the LSM tree
  # let (itemSize, memtableSize) = keyspace.inner.tree.insert(key, value, seqno)
  # For now, use placeholder values
  let itemSize = uint64(key.len + value.len)
  let memtableSize = itemSize # Simplified

  # Publish sequence number to snapshot tracker
  # This allows readers to see the new write
  keyspace.inner.supervisor.inner.snapshotTracker.publish(seqno)

  # Release the lock
  guard.release()

  # Allocate write buffer size
  # This tracks memory usage for backpressure
  discard keyspace.inner.supervisor.inner.writeBufferSize.allocate(itemSize)

  # Run maintenance (check rotation, backpressure)
  keyspace.maintenance(memtableSize)

  return okVoid

# Remove key - implements the full delete path from Rust
proc remove*(keyspace: Keyspace, key: UserKey): StorageResult[void] =
  # Check if keyspace is deleted
  if keyspace.inner.isDeleted.load(moRelaxed):
    return asErr(StorageError(kind: seKeyspaceDeleted))

  # Get journal writer guard (acquires lock)
  if keyspace.inner.supervisor == nil or
      keyspace.inner.supervisor.inner.journal == nil:
    return asErr(StorageError(kind: seIo, ioError: "Journal not available"))

  var guard = keyspace.inner.supervisor.inner.journal.getWriter()

  # IMPORTANT: Check poisoned flag AFTER acquiring lock (TOCTOU prevention)
  if keyspace.inner.isPoisoned != nil and keyspace.inner.isPoisoned[].load(moRelaxed):
    guard.release()
    return asErr(StorageError(kind: sePoisoned))

  # Get next sequence number
  var seqnoCounter = keyspace.inner.supervisor.inner.seqno
  let seqno = seqnoCounter.next()

  # Write tombstone to journal
  # Tombstones are special markers that indicate a key has been deleted
  # They are necessary for LSM-tree compaction and MVCC
  let writeResult = guard.writeRaw(keyspace.inner.id, key, "", vtTombstone, seqno)
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

  # Remove from tree (writes a tombstone)
  # In a full implementation, this would remove from the LSM tree
  # let (itemSize, memtableSize) = keyspace.inner.tree.remove(key, seqno)
  # For now, use placeholder values
  let itemSize = uint64(key.len)
  let memtableSize = itemSize

  # Publish sequence number to snapshot tracker
  keyspace.inner.supervisor.inner.snapshotTracker.publish(seqno)

  # Release the lock
  guard.release()

  # Allocate write buffer size
  discard keyspace.inner.supervisor.inner.writeBufferSize.allocate(itemSize)

  # Run maintenance
  keyspace.maintenance(memtableSize)

  return okVoid

# Remove key weakly - for merge operations
# Weak tombstones are removed during compaction when matched with a single write
proc removeWeak*(keyspace: Keyspace, key: UserKey): StorageResult[void] =
  # Check if keyspace is deleted
  if keyspace.inner.isDeleted.load(moRelaxed):
    return asErr(StorageError(kind: seKeyspaceDeleted))

  # Get journal writer guard (acquires lock)
  if keyspace.inner.supervisor == nil or
      keyspace.inner.supervisor.inner.journal == nil:
    return asErr(StorageError(kind: seIo, ioError: "Journal not available"))

  var guard = keyspace.inner.supervisor.inner.journal.getWriter()

  # IMPORTANT: Check poisoned flag AFTER acquiring lock (TOCTOU prevention)
  if keyspace.inner.isPoisoned != nil and keyspace.inner.isPoisoned[].load(moRelaxed):
    guard.release()
    return asErr(StorageError(kind: sePoisoned))

  # Get next sequence number
  var seqnoCounter = keyspace.inner.supervisor.inner.seqno
  let seqno = seqnoCounter.next()

  # Write weak tombstone to journal
  let writeResult = guard.writeRaw(keyspace.inner.id, key, "", vtWeakTombstone, seqno)
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

  # Remove from tree (writes a weak tombstone)
  # In a full implementation, this would remove from the LSM tree
  let itemSize = uint64(key.len)
  let memtableSize = itemSize

  # Publish sequence number to snapshot tracker
  keyspace.inner.supervisor.inner.snapshotTracker.publish(seqno)

  # Release the lock
  guard.release()

  # Allocate write buffer size
  discard keyspace.inner.supervisor.inner.writeBufferSize.allocate(itemSize)

  # Run maintenance
  keyspace.maintenance(memtableSize)

  return okVoid
