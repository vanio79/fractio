# Mock KV Store for Unit Testing
#
# In-memory implementation of KVStore interface for testing.
# Supports MVCC transactions with staging and commit/rollback.

import std/[tables as stdtables, options, locks, atomics, algorithm]
import ./types
import ./kv_interface

type
  MockKVStore* = ref object of KVStore
    ## Mock implementation of KVStore for unit testing.
    ## Stores data in memory with support for MVCC transactions.
    ##
    ## Features:
    ## - In-memory storage (no I/O)
    ## - Transaction staging (writes are buffered until commit)
    ## - Read snapshot support (reads see committed state at timestamp)
    ## - Thread-safe via lock

    data*: stdtables.Table[string, string]
      ## Main committed data store

    txnStaging*: stdtables.Table[TransactionID, stdtables.Table[string, string]]
      ## Staged writes per transaction

    txnDeletes*: stdtables.Table[TransactionID, seq[string]]
      ## Staged deletes per transaction

    nextTimestamp*: Atomic[uint64]
      ## Monotonically increasing timestamp

    nextTxnId*: Atomic[uint32]
      ## Counter for generating transaction IDs

    lock*: Lock
      ## Lock for thread-safe access

proc newMockKVStore*(): MockKVStore =
  ## Create a new mock KV store.
  new(result)
  result.data = stdtables.initTable[string, string]()
  result.txnStaging = stdtables.initTable[TransactionID, stdtables.Table[string,
      string]]()
  result.txnDeletes = stdtables.initTable[TransactionID, seq[string]]()
  result.nextTimestamp.store(1)
  result.nextTxnId.store(1)
  initLock(result.lock)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

proc genMockTxnId(store: MockKVStore): TransactionID =
  ## Generate a mock transaction ID (not a real ULID, but valid for testing).
  let counter = store.nextTxnId.fetchAdd(1)
  var ulidData: array[16, uint8]
  # Encode counter in last 4 bytes for uniqueness
  ulidData[12] = uint8((counter shr 24) and 0xFF)
  ulidData[13] = uint8((counter shr 16) and 0xFF)
  ulidData[14] = uint8((counter shr 8) and 0xFF)
  ulidData[15] = uint8(counter and 0xFF)
  TransactionID(ULID(data: ulidData))

proc genTimestamp(store: MockKVStore): uint64 =
  ## Generate a new timestamp.
  store.nextTimestamp.fetchAdd(1)

# ---------------------------------------------------------------------------
# KVStore method implementations
# ---------------------------------------------------------------------------

method get*(store: MockKVStore, key: string,
            txnId: TransactionID = zeroTransactionID(),
            readTimestamp: uint64 = 0): KVOpResult[Option[string]] =
  ## Get a value by key.
  ## Transaction-aware: if in a transaction, check staging first.
  store.lock.acquire()
  try:
    # If in transaction, check staging for writes
    if txnId != zeroTransactionID() and txnId in store.txnStaging:
      let staging = store.txnStaging[txnId]
      if key in staging:
        return kvOpOk(some(staging[key]))
      # Check if staged for delete
      if txnId in store.txnDeletes:
        if key in store.txnDeletes[txnId]:
          return kvOpOk(none(string))

    # Check committed data
    if key in store.data:
      return kvOpOk(some(store.data[key]))
    else:
      return kvOpOk(none(string))
  finally:
    store.lock.release()

method put*(store: MockKVStore, key: string, value: string,
            txnId: TransactionID = zeroTransactionID()): KVOpVoidResult =
  ## Put a key-value pair.
  ## Transaction-aware: if txnId provided, stage the write.
  store.lock.acquire()
  try:
    if txnId != zeroTransactionID():
      # Stage the write
      if txnId notin store.txnStaging:
        store.txnStaging[txnId] = stdtables.initTable[string, string]()
      store.txnStaging[txnId][key] = value

      # Remove from deletes if previously staged for delete
      if txnId in store.txnDeletes:
        var newDeletes: seq[string] = @[]
        for k in store.txnDeletes[txnId]:
          if k != key:
            newDeletes.add(k)
        store.txnDeletes[txnId] = newDeletes

      return kvVoidOk()
    else:
      # Direct write (auto-commit)
      store.data[key] = value
      return kvVoidOk()
  finally:
    store.lock.release()

method delete*(store: MockKVStore, key: string,
               txnId: TransactionID = zeroTransactionID()): KVOpVoidResult =
  ## Delete a key.
  ## Transaction-aware: if txnId provided, stage the delete.
  store.lock.acquire()
  try:
    if txnId != zeroTransactionID():
      # Stage the delete
      if txnId notin store.txnDeletes:
        store.txnDeletes[txnId] = @[]
      if key notin store.txnDeletes[txnId]:
        store.txnDeletes[txnId].add(key)

      # Remove from staging if previously staged for write
      if txnId in store.txnStaging:
        store.txnStaging[txnId].del(key)

      return kvVoidOk()
    else:
      # Direct delete (auto-commit)
      if key in store.data:
        store.data.del(key)
      return kvVoidOk()
  finally:
    store.lock.release()

method scan*(store: MockKVStore, startKey: string, endKey: string,
             limit: uint32 = 0,
             txnId: TransactionID = zeroTransactionID(),
             readTimestamp: uint64 = 0): KVOpResult[seq[tuple[key,
                 value: string]]] =
  ## Scan a key range.
  ## Transaction-aware: includes staged writes, excludes staged deletes.
  var entries: seq[tuple[key, value: string]] = @[]

  store.lock.acquire()
  try:
    # Collect from committed data
    for key, value in store.data.pairs:
      if key >= startKey and (endKey.len == 0 or key < endKey):
        # Skip if staged for delete
        if txnId != zeroTransactionID() and txnId in store.txnDeletes:
          if key in store.txnDeletes[txnId]:
            continue
        entries.add((key: key, value: value))

    # Add staged writes (merge)
    if txnId != zeroTransactionID() and txnId in store.txnStaging:
      for key, value in store.txnStaging[txnId].pairs:
        if key >= startKey and (endKey.len == 0 or key < endKey):
          # Check if already in entries (override with staged value)
          var found = false
          for i, e in entries:
            if e.key == key:
              entries[i] = (key: key, value: value)
              found = true
              break
          if not found:
            entries.add((key: key, value: value))

    # Sort by key for consistent ordering
    entries.sort do (a, b: tuple[key, value: string]) -> int:
      cmp(a.key, b.key)

    # Apply limit
    if limit > 0 and entries.len > int(limit):
      entries.setLen(int(limit))

    kvOpOk(entries)
  finally:
    store.lock.release()

method beginTxn*(store: MockKVStore): KVOpResult[TxnBeginResult] =
  ## Begin a new transaction.
  store.lock.acquire()
  try:
    let txnId = store.genMockTxnId()
    let readTimestamp = store.genTimestamp()

    # Initialize empty staging
    store.txnStaging[txnId] = stdtables.initTable[string, string]()
    store.txnDeletes[txnId] = @[]

    kvOpOk((txnId: txnId, readTimestamp: readTimestamp))
  finally:
    store.lock.release()

method commitTxn*(store: MockKVStore, txnId: TransactionID): KVOpVoidResult =
  ## Commit a transaction.
  ## Applies all staged writes and deletes to committed data.
  store.lock.acquire()
  try:
    if txnId notin store.txnStaging and txnId notin store.txnDeletes:
      return kvVoidErr("transaction not found")

    # Apply staged writes
    if txnId in store.txnStaging:
      for key, value in store.txnStaging[txnId].pairs:
        store.data[key] = value

    # Apply staged deletes
    if txnId in store.txnDeletes:
      for key in store.txnDeletes[txnId]:
        if key in store.data:
          store.data.del(key)

    # Cleanup staging
    store.txnStaging.del(txnId)
    store.txnDeletes.del(txnId)

    kvVoidOk()
  finally:
    store.lock.release()

method rollbackTxn*(store: MockKVStore, txnId: TransactionID): KVOpVoidResult =
  ## Rollback a transaction.
  ## Discards all staged writes and deletes.
  store.lock.acquire()
  try:
    if txnId notin store.txnStaging and txnId notin store.txnDeletes:
      # Transaction not found, but that's OK for rollback
      return kvVoidOk()

    # Cleanup staging
    store.txnStaging.del(txnId)
    store.txnDeletes.del(txnId)

    kvVoidOk()
  finally:
    store.lock.release()

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

proc clear*(store: MockKVStore) =
  ## Clear all data and transactions.
  store.lock.acquire()
  try:
    store.data.clear()
    store.txnStaging.clear()
    store.txnDeletes.clear()
    store.nextTimestamp.store(1)
    store.nextTxnId.store(1)
  finally:
    store.lock.release()

proc setData*(store: MockKVStore, key: string, value: string) =
  ## Directly set data (bypasses transactions).
  store.lock.acquire()
  try:
    store.data[key] = value
  finally:
    store.lock.release()

proc getData*(store: MockKVStore, key: string): Option[string] =
  ## Directly get data (bypasses transactions).
  store.lock.acquire()
  try:
    if key in store.data:
      some(store.data[key])
    else:
      none(string)
  finally:
    store.lock.release()

proc hasKey*(store: MockKVStore, key: string): bool =
  ## Check if key exists in committed data.
  store.lock.acquire()
  try:
    key in store.data
  finally:
    store.lock.release()

proc keyCount*(store: MockKVStore): int =
  ## Count of keys in committed data.
  store.lock.acquire()
  try:
    store.data.len
  finally:
    store.lock.release()

proc allKeys*(store: MockKVStore): seq[string] =
  ## Get all keys in committed data.
  store.lock.acquire()
  try:
    var keys: seq[string] = @[]
    for key in store.data.keys:
      keys.add(key)
    keys.sort()
    keys
  finally:
    store.lock.release()
