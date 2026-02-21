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

import fractio/storage/[error, types, snapshot_tracker, journal/writer, batch]
import fractio/storage/keyspace as ks
import fractio/storage/lsm_tree/[memtable, types as lsm_types, lsm_tree]
import std/[options, tables, locks, atomics]

# Transaction sequence numbers start with high bit set
const TX_SEQNO_START* = 0x8000_0000_0000_0000'u64

type
  # Forward declarations
  Database = ref object # Placeholder

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

    # Current sequence number for new writes
    currentSeqno*: uint64

    # Whether transaction is still active
    isActive*: bool

  # Transactional database wrapper
  TxDatabase* = object
    ## A database wrapper that enforces single-writer transaction semantics.

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
  result = WriteTransaction(
    memtables: initTable[uint64, Memtable](),
    snapshotSeqno: snapshotSeqno,
    currentSeqno: TX_SEQNO_START,
    isActive: true
  )

# Create a new transactional database wrapper
proc newTxDatabase*(): TxDatabase =
  initLock(result.txLock)
  result.txActive = false
  result.keyspaces = initTable[string, ks.Keyspace]()

# Get or create memtable for a keyspace
proc getMemtable*(tx: var WriteTransaction, keyspaceId: uint64): Memtable =
  if keyspaceId notin tx.memtables:
    tx.memtables[keyspaceId] = newMemtable()
  return tx.memtables[keyspaceId]

# Check if transaction is active
proc checkActive*(tx: WriteTransaction): StorageResult[void] =
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
  ## Removes a key from the transaction.
  let check = tx.checkActive()
  if check.isErr:
    return check

  let keyspaceId = keyspace.inner.id
  let memtable = tx.getMemtable(keyspaceId)
  discard memtable.remove(key, tx.currentSeqno, weak = false)
  tx.currentSeqno += 1
  return okVoid

# Get value with RYOW support
proc txGet*(tx: var WriteTransaction, keyspace: ks.Keyspace,
    key: string): StorageResult[Option[string]] =
  ## Gets a value, checking uncommitted writes first (RYOW).
  let check = tx.checkActive()
  if check.isErr:
    return err[Option[string], StorageError](check.err[])

  let keyspaceId = keyspace.inner.id

  # Check transaction memtable first
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
  ## Checks if a key exists, checking uncommitted writes first.
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

# Rollback the transaction
proc rollback*(tx: var WriteTransaction) =
  ## Rolls back the transaction, discarding all changes.
  tx.isActive = false
  tx.memtables.clear()

# Commit the transaction
proc commit*(tx: var WriteTransaction, keyspaces: Table[string,
    ks.Keyspace]): StorageResult[void] =
  ## Commits all changes in the transaction.
  ##
  ## This applies all uncommitted changes to the keyspaces.
  ## Changes are applied in order, with the last write to each key winning.
  let check = tx.checkActive()
  if check.isErr:
    return check

  # Mark as inactive immediately to prevent re-entrance
  tx.isActive = false

  # If no changes, nothing to do
  if tx.memtables.len == 0:
    return okVoid

  # Apply all changes to keyspaces
  # Build a lookup from keyspace id to keyspace
  var idToKeyspace: Table[uint64, ks.Keyspace]
  for name, keyspace in keyspaces:
    idToKeyspace[keyspace.inner.id] = keyspace

  # Apply changes from each memtable
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
      of lsm_types.vtTombstone, lsm_types.vtWeakTombstone:
        let applyResult = keyspace.remove(entry.key)
        if applyResult.isErr:
          return err[void, StorageError](applyResult.error)
      of lsm_types.vtIndirection:
        # Indirection values need special handling
        let applyResult = keyspace.insert(entry.key, entry.value)
        if applyResult.isErr:
          return err[void, StorageError](applyResult.error)

  tx.memtables.clear()
  return okVoid

# Begin a write transaction on the database wrapper
proc beginTx*(txDb: var TxDatabase, snapshotSeqno: uint64): WriteTransaction =
  ## Begins a new write transaction.
  ##
  ## This blocks if another transaction is active.
  acquire(txDb.txLock)
  txDb.txActive = true
  return newWriteTransaction(snapshotSeqno)

# End a transaction (release lock)
proc endTx*(txDb: var TxDatabase) =
  ## Ends the transaction and releases the write lock.
  txDb.txActive = false
  release(txDb.txLock)

# Register a keyspace with the transaction database
proc registerKeyspace*(txDb: var TxDatabase, name: string,
    keyspace: ks.Keyspace) =
  ## Registers a keyspace for use in transactions.
  txDb.keyspaces[name] = keyspace
