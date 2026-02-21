# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Transaction Support
##
## This module provides single-writer transaction support.
##
## Transactions provide:
## - Atomicity: All writes succeed or none do
## - Isolation: Read-your-own-writes (RYOW) semantics
## - Consistency: Snapshot-based reads
##
## Single-writer means only one write transaction can be active at a time.
## This is enforced by a mutex lock.

import fractio/storage/[error, types, snapshot_tracker, journal/writer]
import fractio/storage/keyspace as ks
import fractio/storage/lsm_tree/[memtable, types as lsm_types, lsm_tree]
import std/[options, tables, locks, atomics]

# Transaction sequence numbers start with high bit set
const TX_SEQNO_START* = 0x8000_0000_0000_0000'u64

type
  # Forward declarations - will be properly typed when integrated with Database
  DatabaseRef = ref object

  # Snapshot for transaction reads
  TxSnapshot* = object
    seqno*: uint64

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

  # Transactional database wrapper
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
