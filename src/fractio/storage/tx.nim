# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Transaction Support
##
## This module provides two types of transaction support:
##
## 1. Single-writer transactions (WriteTransaction, TxDatabase):
##    - Only one write transaction can be active at a time
##    - Simpler implementation, good for low-contention workloads
##    - Uses a mutex lock to enforce single-writer
##
## 2. Optimistic transactions (OptimisticTransaction, OptimisticTxDatabase):
##    - Multiple transactions can run concurrently
##    - Uses MVCC conflict detection at commit time
##    - Better for read-heavy workloads with occasional writes
##    - Transactions that conflict are aborted and must be retried
##
## Transactions provide:
## - Atomicity: All writes succeed or none do
## - Isolation: Read-your-own-writes (RYOW) semantics
## - Consistency: Snapshot-based reads

import fractio/storage/[error, types, snapshot_tracker, journal/writer, iter]
import fractio/storage/keyspace as ks
import fractio/storage/lsm_tree/[memtable, types as lsm_types, lsm_tree]
import std/[options, tables, locks, atomics, sets, hashes, strutils]

# Transaction sequence numbers start with high bit set
const TX_SEQNO_START* = 0x8000_0000_0000_0000'u64

type
  # Forward declarations - will be properly typed when integrated with Database
  DatabaseRef = ref object

  # Snapshot for transaction reads
  TxSnapshot* = object
    seqno*: uint64

  # ============================================================================
  # SINGLE-WRITER TRANSACTIONS
  # ============================================================================

  # Write transaction - holds uncommitted changes
  WriteTransaction* = object
    ## A write transaction with RYOW support.
    ##
    ## Changes are stored in per-keyspace memtables until commit.
    ## On commit, changes are applied through the batch mechanism.

    # Per-keyspace uncommitted changes (keyspace id -> entries)
    memtables*: Table[uint64, Memtable]

    # Snapshot sequence number for consistent reads
    snapshotSeqno*: uint64

    # Current sequence number for new writes (starts at high bit)
    currentSeqno*: uint64

    # Whether transaction is still active
    isActive*: bool

    # Durability mode for commit
    durability*: Option[PersistMode]

  # Transactional database wrapper (single-writer)
  TxDatabase* = ref object
    ## A database wrapper that enforces single-writer transaction semantics.

    # The underlying database
    db*: DatabaseRef

    # Lock for single-writer enforcement
    txLock*: Lock

    # Flag to check if a transaction is active
    txActive*: bool

    # Keyspace references for commit
    keyspaces*: Table[string, ks.Keyspace]

  # Transactional keyspace
  TxKeyspace* = object
    ## A keyspace reference for use in transactions.
    inner*: ks.Keyspace
    name*: string

  # ============================================================================
  # OPTIMISTIC TRANSACTIONS (MVCC with conflict detection)
  # ============================================================================

  # Read set entry - tracks a key that was read and its seqno at read time
  ReadSetEntry* = object
    ## Tracks a key that was read during an optimistic transaction.
    ## The seqno is the sequence number of the value that was read.
    ## If the key's seqno has changed by commit time, we have a conflict.
    keyspaceId*: uint64
    key*: string
    seqno*: uint64 # The seqno of the value when read (0 means key didn't exist)

  # Read range entry - tracks a range that was scanned
  ReadRangeEntry* = object
    ## Tracks a range scan during an optimistic transaction.
    ## At commit time, we check if any key in this range was modified.
    keyspaceId*: uint64
    startKey*: string
    endKey*: string
    # We track the highest seqno seen in this range for conflict detection
    highestSeqno*: uint64

  # Read all entry - tracks a full table scan
  ReadAllEntry* = object
    ## Tracks a full table scan during an optimistic transaction.
    ## Any write to this keyspace is a conflict.
    keyspaceId*: uint64
    highestSeqno*: uint64

  # Optimistic transaction - tracks read/write sets for conflict detection
  OptimisticTransaction* = object
    ## An optimistic transaction with MVCC conflict detection.
    ##
    ## Reads track the seqno of values, and at commit time we check if
    ## any read key has been modified by another committed transaction.
    ## If a conflict is detected, the commit fails and must be retried.
    ##
    ## Supports three types of read tracking:
    ## - Point reads: Track individual keys
    ## - Range reads: Track key ranges [start, end]
    ## - Full scans: Track entire keyspace

    # Per-keyspace uncommitted writes (keyspace id -> entries)
    writeSet*: Table[uint64, Memtable]

    # Per-keyspace read set (composite key: keyspaceId + "|" + key -> seqno)
    readSet*: Table[string, ReadSetEntry]

    # Range reads (keyspace id -> list of ranges)
    readRanges*: Table[uint64, seq[ReadRangeEntry]]

    # Full keyspace scans (keyspace id -> entry)
    readAll*: Table[uint64, ReadAllEntry]

    # Snapshot sequence number for consistent reads
    snapshotSeqno*: uint64

    # Current sequence number for new writes (starts at high bit)
    currentSeqno*: uint64

    # Whether transaction is still active
    isActive*: bool

    # Durability mode for commit
    durability*: Option[PersistMode]

  # Transaction conflict error
  TransactionConflict* = object
    ## Error returned when an optimistic transaction has a conflict.
    conflictingKeys*: seq[string]

  # Optimistic transactional database wrapper
  OptimisticTxDatabase* = ref object
    ## A database wrapper that allows concurrent optimistic transactions.
    ##
    ## Multiple transactions can run concurrently, but conflicts are
    ## detected at commit time. Transactions that conflict are aborted.

    # The underlying database
    db*: DatabaseRef

    # Lock for commit serialization (only one commit at a time)
    commitLock*: Lock

    # Keyspace references for commit
    keyspaces*: Table[string, ks.Keyspace]

    # Counter for transaction IDs (for debugging/logging)
    nextTxId*: Atomic[uint64]

# Create a new write transaction
proc newWriteTransaction*(snapshotSeqno: uint64): WriteTransaction =
  ## Creates a new write transaction with the given snapshot sequence number.
  result = WriteTransaction(
    memtables: initTable[uint64, Memtable](),
    snapshotSeqno: snapshotSeqno,
    currentSeqno: TX_SEQNO_START,
    isActive: true,
    durability: none(PersistMode)
  )

# Set durability mode for the transaction
proc durability*(tx: var WriteTransaction, mode: Option[
    PersistMode]): var WriteTransaction =
  ## Sets the durability mode for the transaction.
  tx.durability = mode
  return tx

# Create a new transactional database wrapper
proc newTxDatabase*(): TxDatabase =
  ## Creates a new empty transactional database wrapper.
  new(result)
  initLock(result.txLock)
  result.txActive = false
  result.keyspaces = initTable[string, ks.Keyspace]()

# Get or create memtable for a keyspace
proc getMemtable*(tx: var WriteTransaction, keyspaceId: uint64): Memtable =
  ## Gets or creates a memtable for the given keyspace ID.
  if keyspaceId notin tx.memtables:
    tx.memtables[keyspaceId] = newMemtable()
  return tx.memtables[keyspaceId]

# Check if transaction is active
proc checkActive*(tx: WriteTransaction): StorageResult[void] =
  ## Returns an error if the transaction is not active.
  if not tx.isActive:
    return err[void, StorageError](StorageError(
      kind: seStorage,
      storageError: "Transaction is not active"
    ))
  return okVoid

# Insert into transaction
proc txInsert*(tx: var WriteTransaction, keyspace: ks.Keyspace, key: string,
    value: string): StorageResult[void] =
  ## Inserts a key-value pair into the transaction.
  ## The change is not visible to other readers until commit().
  let check = tx.checkActive()
  if check.isErr:
    return check

  let keyspaceId = keyspace.inner.id
  let memtable = tx.getMemtable(keyspaceId)
  discard memtable.insert(key, value, tx.currentSeqno, lsm_types.vtValue)
  tx.currentSeqno += 1
  return okVoid

# Remove from transaction (inserts tombstone)
proc txRemove*(tx: var WriteTransaction, keyspace: ks.Keyspace,
    key: string): StorageResult[void] =
  ## Removes a key from the transaction by inserting a tombstone.
  ## The change is not visible to other readers until commit().
  let check = tx.checkActive()
  if check.isErr:
    return check

  let keyspaceId = keyspace.inner.id
  let memtable = tx.getMemtable(keyspaceId)
  discard memtable.remove(key, tx.currentSeqno, weak = false)
  tx.currentSeqno += 1
  return okVoid

# Remove weak from transaction (inserts weak tombstone)
proc txRemoveWeak*(tx: var WriteTransaction, keyspace: ks.Keyspace,
    key: string): StorageResult[void] =
  ## Removes a key from the transaction by inserting a weak tombstone.
  ## Weak tombstones vanish when they collide with their corresponding insertion.
  let check = tx.checkActive()
  if check.isErr:
    return check

  let keyspaceId = keyspace.inner.id
  let memtable = tx.getMemtable(keyspaceId)
  discard memtable.remove(key, tx.currentSeqno, weak = true)
  tx.currentSeqno += 1
  return okVoid

# Get value with RYOW support
proc txGet*(tx: var WriteTransaction, keyspace: ks.Keyspace,
    key: string): StorageResult[Option[string]] =
  ## Gets a value, checking uncommitted writes first (RYOW).
  ## If the key was modified in this transaction, returns the modified value.
  ## Otherwise, reads from the keyspace at the transaction's snapshot seqno.
  let check = tx.checkActive()
  if check.isErr:
    return err[Option[string], StorageError](check.err[])

  let keyspaceId = keyspace.inner.id

  # Check transaction memtable first (RYOW)
  if keyspaceId in tx.memtables:
    let memtable = tx.memtables[keyspaceId]
    let entry = memtable.get(key)
    if entry.isSome:
      let e = entry.get
      if e.valueType == lsm_types.vtTombstone or e.valueType ==
          lsm_types.vtWeakTombstone:
        return ok[Option[string], StorageError](none(string))
      return ok[Option[string], StorageError](some(e.value))

  # Fall back to keyspace read at snapshot seqno
  let value = keyspace.inner.tree.get(key, tx.snapshotSeqno)
  return ok[Option[string], StorageError](value)

# Check if key exists with RYOW support
proc txContainsKey*(tx: var WriteTransaction, keyspace: ks.Keyspace,
    key: string): StorageResult[bool] =
  ## Checks if a key exists, checking uncommitted writes first (RYOW).
  let check = tx.checkActive()
  if check.isErr:
    return err[bool, StorageError](check.err[])

  let keyspaceId = keyspace.inner.id

  if keyspaceId in tx.memtables:
    let memtable = tx.memtables[keyspaceId]
    let entry = memtable.get(key)
    if entry.isSome:
      let e = entry.get
      if e.valueType == lsm_types.vtTombstone or e.valueType ==
          lsm_types.vtWeakTombstone:
        return ok[bool, StorageError](false)
      return ok[bool, StorageError](true)

  let exists = keyspace.inner.tree.containsKey(key, tx.snapshotSeqno)
  return ok[bool, StorageError](exists)

# Get size of value with RYOW support
proc txSizeOf*(tx: var WriteTransaction, keyspace: ks.Keyspace,
    key: string): StorageResult[Option[uint32]] =
  ## Gets the size of a value, checking uncommitted writes first (RYOW).
  let check = tx.checkActive()
  if check.isErr:
    return err[Option[uint32], StorageError](check.err[])

  let keyspaceId = keyspace.inner.id

  if keyspaceId in tx.memtables:
    let memtable = tx.memtables[keyspaceId]
    let entry = memtable.get(key)
    if entry.isSome:
      let e = entry.get
      if e.valueType == lsm_types.vtTombstone or e.valueType ==
          lsm_types.vtWeakTombstone:
        return ok[Option[uint32], StorageError](none(uint32))
      return ok[Option[uint32], StorageError](some(uint32(e.value.len)))

  # Fall back to keyspace - would need size_of method in LsmTree
  return ok[Option[uint32], StorageError](none(uint32))

# Rollback the transaction
proc rollback*(tx: var WriteTransaction) =
  ## Rolls back the transaction, discarding all uncommitted changes.
  tx.isActive = false
  tx.memtables.clear()

# Commit the transaction using batch mechanism
proc commit*(tx: var WriteTransaction, keyspaces: Table[string,
    ks.Keyspace]): StorageResult[void] =
  ## Commits all changes in the transaction.
  ##
  ## This applies all uncommitted changes to the keyspaces atomically.
  ## Changes are applied using the keyspace's insert/remove methods,
  ## which handle journaling and persistence.
  ##
  ## For true atomicity across keyspaces, this should be integrated with
  ## the Database's batch commit mechanism.
  let check = tx.checkActive()
  if check.isErr:
    return check

  # Mark as inactive immediately to prevent re-entrance
  tx.isActive = false

  # If no changes, nothing to do
  if tx.memtables.len == 0:
    return okVoid

  # Build a lookup from keyspace id to keyspace
  var idToKeyspace: Table[uint64, ks.Keyspace]
  for name, keyspace in keyspaces:
    idToKeyspace[keyspace.inner.id] = keyspace

  # Apply changes from each memtable
  # We use last-write-wins semantics: for each key, only apply the latest version
  for keyspaceId, memtable in tx.memtables:
    if keyspaceId notin idToKeyspace:
      continue

    let keyspace = idToKeyspace[keyspaceId]
    let entries = memtable.getSortedEntries()

    # Track which keys we've already applied (keep last version)
    var appliedKeys: Table[string, bool]

    # Apply in reverse order to get the latest version
    for i in countdown(entries.len - 1, 0):
      let entry = entries[i]

      # Skip if we've already applied this key
      if entry.key in appliedKeys:
        continue
      appliedKeys[entry.key] = true

      # Apply the operation
      case entry.valueType
      of lsm_types.vtValue:
        let applyResult = keyspace.insert(entry.key, entry.value)
        if applyResult.isErr:
          return err[void, StorageError](applyResult.error)
      of lsm_types.vtTombstone:
        let applyResult = keyspace.remove(entry.key)
        if applyResult.isErr:
          return err[void, StorageError](applyResult.error)
      of lsm_types.vtWeakTombstone:
        let applyResult = keyspace.removeWeak(entry.key)
        if applyResult.isErr:
          return err[void, StorageError](applyResult.error)
      of lsm_types.vtIndirection:
        # Indirection values (blob handles) need special handling
        let applyResult = keyspace.insert(entry.key, entry.value)
        if applyResult.isErr:
          return err[void, StorageError](applyResult.error)

  tx.memtables.clear()
  return okVoid

# Take - remove and return value
proc txTake*(tx: var WriteTransaction, keyspace: ks.Keyspace,
    key: string): StorageResult[Option[string]] =
  ## Removes a key and returns its previous value atomically.
  let prev = tx.txGet(keyspace, key)
  if prev.isErr:
    return prev

  if prev.value.isSome:
    let removeResult = tx.txRemove(keyspace, key)
    if removeResult.isErr:
      return err[Option[string], StorageError](removeResult.err[])

  return prev

# Fetch update - atomically update and return previous value
proc txFetchUpdate*(tx: var WriteTransaction, keyspace: ks.Keyspace,
    key: string, f: proc(v: Option[string]): Option[string]): StorageResult[
        Option[string]] =
  ## Atomically updates a key and returns the previous value.
  ## The update function receives the current value (or none) and returns
  ## the new value (or none to delete).
  let prev = tx.txGet(keyspace, key)
  if prev.isErr:
    return prev

  let updated = f(prev.value)

  if updated.isSome:
    let insertResult = tx.txInsert(keyspace, key, updated.get)
    if insertResult.isErr:
      return err[Option[string], StorageError](insertResult.err[])
  elif prev.value.isSome:
    # Only remove if there was a previous value
    let removeResult = tx.txRemove(keyspace, key)
    if removeResult.isErr:
      return err[Option[string], StorageError](removeResult.err[])

  return prev

# Update fetch - atomically update and return new value
proc txUpdateFetch*(tx: var WriteTransaction, keyspace: ks.Keyspace,
    key: string, f: proc(v: Option[string]): Option[string]): StorageResult[
        Option[string]] =
  ## Atomically updates a key and returns the new value.
  ## The update function receives the current value (or none) and returns
  ## the new value (or none to delete).
  let prev = tx.txGet(keyspace, key)
  if prev.isErr:
    return prev

  let updated = f(prev.value)

  if updated.isSome:
    let insertResult = tx.txInsert(keyspace, key, updated.get)
    if insertResult.isErr:
      return err[Option[string], StorageError](insertResult.err[])
  elif prev.value.isSome:
    # Only remove if there was a previous value
    let removeResult = tx.txRemove(keyspace, key)
    if removeResult.isErr:
      return err[Option[string], StorageError](removeResult.err[])

  return ok[Option[string], StorageError](updated)

# Begin a write transaction on the database wrapper
proc beginTx*(txDb: var TxDatabase, snapshotSeqno: uint64): WriteTransaction =
  ## Begins a new write transaction.
  ##
  ## This blocks if another transaction is active (single-writer enforcement).
  acquire(txDb.txLock)
  txDb.txActive = true
  return newWriteTransaction(snapshotSeqno)

# Begin a write transaction with durability mode
proc beginTx*(txDb: var TxDatabase, snapshotSeqno: uint64,
    durability: Option[PersistMode]): WriteTransaction =
  ## Begins a new write transaction with the specified durability mode.
  acquire(txDb.txLock)
  txDb.txActive = true
  var tx = newWriteTransaction(snapshotSeqno)
  tx.durability = durability
  return tx

# End a transaction (release lock)
proc endTx*(txDb: var TxDatabase) =
  ## Ends the transaction and releases the write lock.
  ## This should be called after commit() or rollback().
  txDb.txActive = false
  release(txDb.txLock)

# Register a keyspace with the transaction database
proc registerKeyspace*(txDb: var TxDatabase, name: string,
    keyspace: ks.Keyspace) =
  ## Registers a keyspace for use in transactions.
  txDb.keyspaces[name] = keyspace

# Unregister a keyspace
proc unregisterKeyspace*(txDb: var TxDatabase, name: string) =
  ## Unregisters a keyspace from the transaction database.
  txDb.keyspaces.del(name)

# Check if keyspace is registered
proc hasKeyspace*(txDb: TxDatabase, name: string): bool =
  ## Returns true if the keyspace is registered.
  name in txDb.keyspaces

# Get registered keyspace names
proc keyspaceNames*(txDb: TxDatabase): seq[string] =
  ## Returns the names of all registered keyspaces.
  result = @[]
  for name in txDb.keyspaces.keys:
    result.add(name)

# Note: TxDatabase cleanup is handled by Nim's GC
# The Lock will be cleaned up when the TxDatabase object is collected

# ============================================================================
# OPTIMISTIC TRANSACTION IMPLEMENTATION
# ============================================================================

# Helper to create a composite key for the read set
proc makeReadSetKey(keyspaceId: uint64, key: string): string =
  ## Creates a composite key for storing in the read set.
  result = $keyspaceId & ":" & key

# Create a new optimistic transaction
proc newOptimisticTransaction*(snapshotSeqno: uint64): OptimisticTransaction =
  ## Creates a new optimistic transaction with the given snapshot sequence number.
  result = OptimisticTransaction(
    writeSet: initTable[uint64, Memtable](),
    readSet: initTable[string, ReadSetEntry](),
    readRanges: initTable[uint64, seq[ReadRangeEntry]](),
    readAll: initTable[uint64, ReadAllEntry](),
    snapshotSeqno: snapshotSeqno,
    currentSeqno: TX_SEQNO_START,
    isActive: true,
    durability: none(PersistMode)
  )

# Set durability mode for optimistic transaction
proc durability*(tx: var OptimisticTransaction, mode: Option[
    PersistMode]): var OptimisticTransaction =
  ## Sets the durability mode for the transaction.
  tx.durability = mode
  return tx

# Create a new optimistic transactional database wrapper
proc newOptimisticTxDatabase*(): OptimisticTxDatabase =
  ## Creates a new empty optimistic transactional database wrapper.
  new(result)
  initLock(result.commitLock)
  result.keyspaces = initTable[string, ks.Keyspace]()
  result.nextTxId.store(0, moRelaxed)

# Get or create write memtable for a keyspace
proc getWriteMemtable*(tx: var OptimisticTransaction,
    keyspaceId: uint64): Memtable =
  ## Gets or creates a write memtable for the given keyspace ID.
  if keyspaceId notin tx.writeSet:
    tx.writeSet[keyspaceId] = newMemtable()
  return tx.writeSet[keyspaceId]

# Check if optimistic transaction is active
proc checkActive*(tx: OptimisticTransaction): StorageResult[void] =
  ## Returns an error if the transaction is not active.
  if not tx.isActive:
    return err[void, StorageError](StorageError(
      kind: seStorage,
      storageError: "Transaction is not active"
    ))
  return okVoid

# Record a read in the read set (for conflict detection)
proc recordRead*(tx: var OptimisticTransaction, keyspaceId: uint64, key: string,
    seqno: uint64) =
  ## Records a read operation in the read set for conflict detection.
  ## If the key is already in the write set (RYOW), we don't need to track it.
  let readSetKey = makeReadSetKey(keyspaceId, key)
  if readSetKey notin tx.readSet:
    tx.readSet[readSetKey] = ReadSetEntry(
      keyspaceId: keyspaceId,
      key: key,
      seqno: seqno
    )

# Record a range read (for SSI conflict detection)
proc recordRangeRead*(tx: var OptimisticTransaction, keyspaceId: uint64,
                      startKey: string, endKey: string, highestSeqno: uint64) =
  ## Records a range scan in the read ranges for conflict detection.
  ## At commit time, we check if any key in this range was modified.
  if keyspaceId notin tx.readRanges:
    tx.readRanges[keyspaceId] = @[]

  # Check if this range overlaps with an existing range - merge if so
  var merged = false
  for i in 0 ..< tx.readRanges[keyspaceId].len:
    var entry = tx.readRanges[keyspaceId][i]
    # Check for overlap or adjacency
    if startKey <= entry.endKey and endKey >= entry.startKey:
      # Merge ranges
      entry.startKey = min(entry.startKey, startKey)
      entry.endKey = max(entry.endKey, endKey)
      entry.highestSeqno = max(entry.highestSeqno, highestSeqno)
      tx.readRanges[keyspaceId][i] = entry
      merged = true
      break

  if not merged:
    tx.readRanges[keyspaceId].add(ReadRangeEntry(
      keyspaceId: keyspaceId,
      startKey: startKey,
      endKey: endKey,
      highestSeqno: highestSeqno
    ))

# Record a full keyspace scan (for SSI conflict detection)
proc recordFullScan*(tx: var OptimisticTransaction, keyspaceId: uint64,
                     highestSeqno: uint64) =
  ## Records a full keyspace scan for conflict detection.
  ## Any write to this keyspace is a conflict.
  if keyspaceId notin tx.readAll:
    tx.readAll[keyspaceId] = ReadAllEntry(
      keyspaceId: keyspaceId,
      highestSeqno: highestSeqno
    )
  else:
    # Update highest seqno if needed
    if highestSeqno > tx.readAll[keyspaceId].highestSeqno:
      tx.readAll[keyspaceId].highestSeqno = highestSeqno

# Insert into optimistic transaction
proc otxInsert*(tx: var OptimisticTransaction, keyspace: ks.Keyspace,
    key: string, value: string): StorageResult[void] =
  ## Inserts a key-value pair into the optimistic transaction.
  let check = tx.checkActive()
  if check.isErr:
    return check

  let keyspaceId = keyspace.inner.id

  # Remove from read set if present (write overrides read tracking)
  let readSetKey = makeReadSetKey(keyspaceId, key)
  tx.readSet.del(readSetKey)

  let memtable = tx.getWriteMemtable(keyspaceId)
  discard memtable.insert(key, value, tx.currentSeqno, lsm_types.vtValue)
  tx.currentSeqno += 1
  return okVoid

# Remove from optimistic transaction (inserts tombstone)
proc otxRemove*(tx: var OptimisticTransaction, keyspace: ks.Keyspace,
    key: string): StorageResult[void] =
  ## Removes a key from the optimistic transaction by inserting a tombstone.
  let check = tx.checkActive()
  if check.isErr:
    return check

  let keyspaceId = keyspace.inner.id

  # Remove from read set if present (write overrides read tracking)
  let readSetKey = makeReadSetKey(keyspaceId, key)
  tx.readSet.del(readSetKey)

  let memtable = tx.getWriteMemtable(keyspaceId)
  discard memtable.remove(key, tx.currentSeqno, weak = false)
  tx.currentSeqno += 1
  return okVoid

# Remove weak from optimistic transaction
proc otxRemoveWeak*(tx: var OptimisticTransaction, keyspace: ks.Keyspace,
    key: string): StorageResult[void] =
  ## Removes a key from the optimistic transaction by inserting a weak tombstone.
  let check = tx.checkActive()
  if check.isErr:
    return check

  let keyspaceId = keyspace.inner.id
  let readSetKey = makeReadSetKey(keyspaceId, key)
  tx.readSet.del(readSetKey)

  let memtable = tx.getWriteMemtable(keyspaceId)
  discard memtable.remove(key, tx.currentSeqno, weak = true)
  tx.currentSeqno += 1
  return okVoid

# Get value with RYOW support (optimistic)
proc otxGet*(tx: var OptimisticTransaction, keyspace: ks.Keyspace,
    key: string): StorageResult[Option[string]] =
  ## Gets a value, checking uncommitted writes first (RYOW).
  ## Records the read in the read set for conflict detection.
  let check = tx.checkActive()
  if check.isErr:
    return err[Option[string], StorageError](check.err[])

  let keyspaceId = keyspace.inner.id

  # Check write set first (RYOW)
  if keyspaceId in tx.writeSet:
    let memtable = tx.writeSet[keyspaceId]
    let entry = memtable.get(key)
    if entry.isSome:
      let e = entry.get
      if e.valueType == lsm_types.vtTombstone or e.valueType ==
          lsm_types.vtWeakTombstone:
        # Record as read (key didn't exist) but don't include in conflict set
        # Actually, we should still track that we read "not exists"
        return ok[Option[string], StorageError](none(string))
      return ok[Option[string], StorageError](some(e.value))

  # Read from keyspace at snapshot seqno
  let (value, seqno) = keyspace.inner.tree.getWithSeqno(key, tx.snapshotSeqno)

  # Record the read with its seqno (0 means key didn't exist)
  let readSeqno = if value.isSome: seqno else: 0'u64
  tx.recordRead(keyspaceId, key, readSeqno)

  return ok[Option[string], StorageError](value)

# Check if key exists with RYOW support (optimistic)
proc otxContainsKey*(tx: var OptimisticTransaction, keyspace: ks.Keyspace,
    key: string): StorageResult[bool] =
  ## Checks if a key exists, checking uncommitted writes first (RYOW).
  let check = tx.checkActive()
  if check.isErr:
    return err[bool, StorageError](check.err[])

  let keyspaceId = keyspace.inner.id

  if keyspaceId in tx.writeSet:
    let memtable = tx.writeSet[keyspaceId]
    let entry = memtable.get(key)
    if entry.isSome:
      let e = entry.get
      if e.valueType == lsm_types.vtTombstone or e.valueType ==
          lsm_types.vtWeakTombstone:
        return ok[bool, StorageError](false)
      return ok[bool, StorageError](true)

  let exists = keyspace.inner.tree.containsKey(key, tx.snapshotSeqno)

  # Record the read (we need to track that this key was checked)
  # For containsKey, we track the current seqno of the key
  if exists:
    let (_, seqno) = keyspace.inner.tree.getWithSeqno(key, tx.snapshotSeqno)
    tx.recordRead(keyspaceId, key, seqno)
  else:
    # Key doesn't exist - record with seqno 0
    tx.recordRead(keyspaceId, key, 0)

  return ok[bool, StorageError](exists)

# Get size of value with RYOW support (optimistic)
proc otxSizeOf*(tx: var OptimisticTransaction, keyspace: ks.Keyspace,
    key: string): StorageResult[Option[uint32]] =
  ## Gets the size of a value, checking uncommitted writes first (RYOW).
  let check = tx.checkActive()
  if check.isErr:
    return err[Option[uint32], StorageError](check.err[])

  let keyspaceId = keyspace.inner.id

  if keyspaceId in tx.writeSet:
    let memtable = tx.writeSet[keyspaceId]
    let entry = memtable.get(key)
    if entry.isSome:
      let e = entry.get
      if e.valueType == lsm_types.vtTombstone or e.valueType ==
          lsm_types.vtWeakTombstone:
        return ok[Option[uint32], StorageError](none(uint32))
      return ok[Option[uint32], StorageError](some(uint32(e.value.len)))

  # Would need size_of in LsmTree - for now return none
  return ok[Option[uint32], StorageError](none(uint32))

# Rollback the optimistic transaction
proc rollback*(tx: var OptimisticTransaction) =
  ## Rolls back the optimistic transaction, discarding all uncommitted changes.
  tx.isActive = false
  tx.writeSet.clear()
  tx.readSet.clear()
  tx.readRanges.clear()
  tx.readAll.clear()

# Detect conflicts between read set and current database state
proc detectConflicts*(tx: OptimisticTransaction, keyspaces: Table[string,
    ks.Keyspace]): seq[string] =
  ## Checks if any key in the read set has been modified since the transaction started.
  ## Returns a list of conflicting keys (empty if no conflicts).
  ##
  ## Checks three types of conflicts:
  ## 1. Point read conflicts: A key was read and then modified
  ## 2. Range read conflicts: A range was scanned and a key in that range was modified
  ## 3. Full scan conflicts: A keyspace was fully scanned and any key was modified
  result = @[]

  # Build a lookup from keyspace id to keyspace
  var idToKeyspace: Table[uint64, ks.Keyspace]
  for name, keyspace in keyspaces:
    idToKeyspace[keyspace.inner.id] = keyspace

  # Check 1: Point read conflicts
  for readSetKey, entry in tx.readSet:
    if entry.keyspaceId notin idToKeyspace:
      continue

    let keyspace = idToKeyspace[entry.keyspaceId]

    # Get the current seqno of this key
    let (currentValue, currentSeqno) = keyspace.inner.tree.getWithSeqno(
        entry.key, high(uint64))

    if entry.seqno == 0:
      # Key didn't exist when we read it
      # Conflict if key now exists
      if currentValue.isSome:
        result.add(entry.key)
    else:
      # Key existed with seqno when we read it
      # Conflict if seqno has changed (including if key was deleted)
      if currentValue.isNone or currentSeqno != entry.seqno:
        result.add(entry.key)

  # Check 2: Range read conflicts
  # For each range we read, check if any key in that range was modified
  for keyspaceId, ranges in tx.readRanges:
    if keyspaceId notin idToKeyspace:
      continue

    let keyspace = idToKeyspace[keyspaceId]

    for rangeEntry in ranges:
      # Check if any write in this transaction overlaps with the range
      # If we wrote to this range, skip conflict detection (RYOW)
      var wroteToRange = false
      if keyspaceId in tx.writeSet:
        let memtable = tx.writeSet[keyspaceId]
        for key, _ in memtable.entries:
          if key >= rangeEntry.startKey and key <= rangeEntry.endKey:
            wroteToRange = true
            break

      if wroteToRange:
        continue

      # Check if any key in the range has been modified since our scan
      # We compare against the highest seqno we saw during the scan
      var iter = keyspace.rangeIter(rangeEntry.startKey, rangeEntry.endKey)
      defer: iter.close()
      for entry in iter.entries:
        # Get the current seqno of this key
        let (_, currentSeqno) = keyspace.inner.tree.getWithSeqno(entry.key,
            high(uint64))
        # If the current seqno is higher than what we recorded, there's a conflict
        if currentSeqno > rangeEntry.highestSeqno:
          result.add("range:" & rangeEntry.startKey & "-" & rangeEntry.endKey &
              ":" & entry.key)
          break

  # Check 3: Full scan conflicts
  # If we did a full scan, any write to the keyspace by another transaction is a conflict
  # Note: RYOW does NOT apply here - even if we wrote to the keyspace, we still need to
  # check if OTHER transactions wrote to it after our scan
  for keyspaceId, fullScan in tx.readAll:
    if keyspaceId notin idToKeyspace:
      continue

    let keyspace = idToKeyspace[keyspaceId]

    # For full scan conflicts, we check if the keyspace has any writes
    # since our scan. We do this by checking if the highest seqno in the
    # memtable is higher than what we recorded.
    let highestMemtableSeqno = keyspace.inner.tree.getHighestMemtableSeqno()
    if highestMemtableSeqno.isSome and highestMemtableSeqno.get() >
        fullScan.highestSeqno:
      result.add("fullscan:" & $keyspaceId & ":new_writes_detected")
      continue

    # Also check if any existing key has been modified
    # by comparing current seqno with recorded highest
    var iter = keyspace.iter()
    defer: iter.close()
    for entry in iter.entries:
      let (_, currentSeqno) = keyspace.inner.tree.getWithSeqno(entry.key, high(uint64))
      if currentSeqno > fullScan.highestSeqno:
        result.add("fullscan:" & $keyspaceId & ":" & entry.key)
        break

# Commit the optimistic transaction with conflict detection
proc otxCommit*(tx: var OptimisticTransaction, keyspaces: Table[string,
    ks.Keyspace]): StorageResult[void] =
  ## Commits the optimistic transaction with conflict detection.
  ##
  ## Returns an error if there are conflicts. The transaction should be
  ## rolled back and retried in that case.
  let check = tx.checkActive()
  if check.isErr:
    return check

  # Mark as inactive immediately to prevent re-entrance
  tx.isActive = false

  # Check for conflicts
  let conflicts = tx.detectConflicts(keyspaces)
  if conflicts.len > 0:
    tx.writeSet.clear()
    tx.readSet.clear()
    tx.readRanges.clear()
    tx.readAll.clear()
    return err[void, StorageError](StorageError(
      kind: seStorage,
      storageError: "Transaction conflict detected for keys: " & conflicts.join(", ")
    ))

  # If no changes, nothing to do
  if tx.writeSet.len == 0:
    tx.readSet.clear()
    tx.readRanges.clear()
    tx.readAll.clear()
    return okVoid

  # Build a lookup from keyspace id to keyspace
  var idToKeyspace: Table[uint64, ks.Keyspace]
  for name, keyspace in keyspaces:
    idToKeyspace[keyspace.inner.id] = keyspace

  # Apply changes from each memtable
  for keyspaceId, memtable in tx.writeSet:
    if keyspaceId notin idToKeyspace:
      continue

    let keyspace = idToKeyspace[keyspaceId]
    let entries = memtable.getSortedEntries()

    # Track which keys we've already applied (keep last version)
    var appliedKeys: Table[string, bool]

    # Apply in reverse order to get the latest version
    for i in countdown(entries.len - 1, 0):
      let entry = entries[i]

      # Skip if we've already applied this key
      if entry.key in appliedKeys:
        continue
      appliedKeys[entry.key] = true

      # Apply the operation
      case entry.valueType
      of lsm_types.vtValue:
        let applyResult = keyspace.insert(entry.key, entry.value)
        if applyResult.isErr:
          return err[void, StorageError](applyResult.error)
      of lsm_types.vtTombstone:
        let applyResult = keyspace.remove(entry.key)
        if applyResult.isErr:
          return err[void, StorageError](applyResult.error)
      of lsm_types.vtWeakTombstone:
        let applyResult = keyspace.removeWeak(entry.key)
        if applyResult.isErr:
          return err[void, StorageError](applyResult.error)
      of lsm_types.vtIndirection:
        let applyResult = keyspace.insert(entry.key, entry.value)
        if applyResult.isErr:
          return err[void, StorageError](applyResult.error)

  tx.writeSet.clear()
  tx.readSet.clear()
  tx.readRanges.clear()
  tx.readAll.clear()
  return okVoid

# Take - remove and return value (optimistic)
proc otxTake*(tx: var OptimisticTransaction, keyspace: ks.Keyspace,
    key: string): StorageResult[Option[string]] =
  ## Removes a key and returns its previous value atomically.
  let prev = tx.otxGet(keyspace, key)
  if prev.isErr:
    return prev

  if prev.value.isSome:
    let removeResult = tx.otxRemove(keyspace, key)
    if removeResult.isErr:
      return err[Option[string], StorageError](removeResult.err[])

  return prev

# Fetch update - atomically update and return previous value (optimistic)
proc otxFetchUpdate*(tx: var OptimisticTransaction, keyspace: ks.Keyspace,
    key: string, f: proc(v: Option[string]): Option[string]): StorageResult[
        Option[string]] =
  ## Atomically updates a key and returns the previous value.
  let prev = tx.otxGet(keyspace, key)
  if prev.isErr:
    return prev

  let updated = f(prev.value)

  if updated.isSome:
    let insertResult = tx.otxInsert(keyspace, key, updated.get)
    if insertResult.isErr:
      return err[Option[string], StorageError](insertResult.err[])
  elif prev.value.isSome:
    let removeResult = tx.otxRemove(keyspace, key)
    if removeResult.isErr:
      return err[Option[string], StorageError](removeResult.err[])

  return prev

# Update fetch - atomically update and return new value (optimistic)
proc otxUpdateFetch*(tx: var OptimisticTransaction, keyspace: ks.Keyspace,
    key: string, f: proc(v: Option[string]): Option[string]): StorageResult[
        Option[string]] =
  ## Atomically updates a key and returns the new value.
  let prev = tx.otxGet(keyspace, key)
  if prev.isErr:
    return prev

  let updated = f(prev.value)

  if updated.isSome:
    let insertResult = tx.otxInsert(keyspace, key, updated.get)
    if insertResult.isErr:
      return err[Option[string], StorageError](insertResult.err[])
  elif prev.value.isSome:
    let removeResult = tx.otxRemove(keyspace, key)
    if removeResult.isErr:
      return err[Option[string], StorageError](removeResult.err[])

  return ok[Option[string], StorageError](updated)

# Begin an optimistic transaction on the database wrapper
proc beginOptimisticTx*(txDb: var OptimisticTxDatabase,
    snapshotSeqno: uint64): OptimisticTransaction =
  ## Begins a new optimistic transaction.
  ## Multiple optimistic transactions can run concurrently.
  let txId = txDb.nextTxId.fetchAdd(1, moRelaxed) + 1
  result = newOptimisticTransaction(snapshotSeqno)
  # txId can be used for logging/debugging

# Begin an optimistic transaction with durability mode
proc beginOptimisticTx*(txDb: var OptimisticTxDatabase, snapshotSeqno: uint64,
    durability: Option[PersistMode]): OptimisticTransaction =
  ## Begins a new optimistic transaction with the specified durability mode.
  discard txDb.nextTxId.fetchAdd(1, moRelaxed)
  result = newOptimisticTransaction(snapshotSeqno)
  result.durability = durability

# Commit an optimistic transaction with commit serialization
proc commitOptimisticTx*(txDb: var OptimisticTxDatabase,
    tx: var OptimisticTransaction): StorageResult[void] =
  ## Commits an optimistic transaction.
  ## This acquires a commit lock to serialize commits.
  acquire(txDb.commitLock)
  defer: release(txDb.commitLock)

  return tx.otxCommit(txDb.keyspaces)

# Register a keyspace with the optimistic transaction database
proc registerKeyspace*(txDb: var OptimisticTxDatabase, name: string,
    keyspace: ks.Keyspace) =
  ## Registers a keyspace for use in optimistic transactions.
  txDb.keyspaces[name] = keyspace

# Unregister a keyspace from optimistic transaction database
proc unregisterKeyspace*(txDb: var OptimisticTxDatabase, name: string) =
  ## Unregisters a keyspace from the optimistic transaction database.
  txDb.keyspaces.del(name)

# Check if keyspace is registered (optimistic)
proc hasKeyspace*(txDb: OptimisticTxDatabase, name: string): bool =
  ## Returns true if the keyspace is registered.
  name in txDb.keyspaces

# Get registered keyspace names (optimistic)
proc keyspaceNames*(txDb: OptimisticTxDatabase): seq[string] =
  ## Returns the names of all registered keyspaces.
  result = @[]
  for name in txDb.keyspaces.keys:
    result.add(name)

# ============================================================================
# RANGE AND PREFIX ITERATION WITH CONFLICT TRACKING
# ============================================================================

# Range iteration with conflict tracking
proc otxRangeIter*(tx: var OptimisticTransaction, keyspace: ks.Keyspace,
                   startKey: string, endKey: string): StorageResult[KeyspaceIter] =
  ## Creates a range iterator with conflict tracking.
  ##
  ## The range [startKey, endKey] is recorded in the read set for SSI.
  ## At commit time, if any key in this range was modified by another
  ## transaction, a conflict will be detected.
  let check = tx.checkActive()
  if check.isErr:
    return err[KeyspaceIter, StorageError](check.err[])

  let keyspaceId = keyspace.inner.id

  # Create the iterator
  let iter = keyspace.rangeIter(startKey, endKey)

  # Track the highest seqno in the range for conflict detection
  var highestSeqno: uint64 = 0
  for entry in iter.entries:
    if entry.seqno > highestSeqno:
      highestSeqno = entry.seqno

  # Record the range read
  tx.recordRangeRead(keyspaceId, startKey, endKey, highestSeqno)

  return ok[KeyspaceIter, StorageError](iter)

# Prefix iteration with conflict tracking
proc otxPrefixIter*(tx: var OptimisticTransaction, keyspace: ks.Keyspace,
                    prefix: string): StorageResult[KeyspaceIter] =
  ## Creates a prefix iterator with conflict tracking.
  ##
  ## The prefix range is recorded in the read set for SSI.
  ## At commit time, if any key with this prefix was modified by another
  ## transaction, a conflict will be detected.
  let check = tx.checkActive()
  if check.isErr:
    return err[KeyspaceIter, StorageError](check.err[])

  let keyspaceId = keyspace.inner.id

  # Create the iterator
  let iter = keyspace.prefixIter(prefix)

  # For prefix iteration, we track it as a range
  # The end key is the prefix with all 0xFF bytes appended (lexicographic upper bound)
  var endKey = prefix
  for i in 0 ..< 8: # Add enough 0xFF to cover most key lengths
    endKey.add('\xFF')

  # Track the highest seqno in the prefix range
  var highestSeqno: uint64 = 0
  for entry in iter.entries:
    if entry.seqno > highestSeqno:
      highestSeqno = entry.seqno

  # Record the range read
  tx.recordRangeRead(keyspaceId, prefix, endKey, highestSeqno)

  return ok[KeyspaceIter, StorageError](iter)

# Full keyspace scan with conflict tracking
proc otxIter*(tx: var OptimisticTransaction,
    keyspace: ks.Keyspace): StorageResult[KeyspaceIter] =
  ## Creates a full keyspace iterator with conflict tracking.
  ##
  ## The entire keyspace scan is recorded for SSI.
  ## At commit time, if any key in this keyspace was modified by another
  ## transaction, a conflict will be detected.
  let check = tx.checkActive()
  if check.isErr:
    return err[KeyspaceIter, StorageError](check.err[])

  let keyspaceId = keyspace.inner.id

  # Create the iterator
  let iter = keyspace.iter()

  # Track the highest seqno in the keyspace
  var highestSeqno: uint64 = 0
  for entry in iter.entries:
    if entry.seqno > highestSeqno:
      highestSeqno = entry.seqno

  # Record the full scan
  tx.recordFullScan(keyspaceId, highestSeqno)

  return ok[KeyspaceIter, StorageError](iter)

# Check if transaction has any range reads
proc hasRangeReads*(tx: OptimisticTransaction): bool =
  ## Returns true if this transaction performed any range or prefix scans.
  tx.readRanges.len > 0 or tx.readAll.len > 0

# Get the number of tracked reads (for debugging/metrics)
proc readSetSize*(tx: OptimisticTransaction): int =
  ## Returns the total number of tracked reads (point + range + full).
  result = tx.readSet.len
  for _, ranges in tx.readRanges:
    result += ranges.len
  result += tx.readAll.len
