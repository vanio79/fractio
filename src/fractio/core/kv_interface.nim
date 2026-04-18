# KV Store Interface
#
# Abstract interface for KV operations that can be mocked for testing.
# Enables unit testing of executor and client without real network I/O.

import std/options
import ./types
import ../distributed/raft/group_types # for GroupID
import ../storage/backend # for StreamResultSet, StreamConfig, KeyValuePair

# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

type
  KVOpResult*[T] = object
    ## Result type for KV operations that return a value
    isOk*: bool
    val*: T
    err*: string

  KVOpVoidResult* = object
    ## Result type for KV operations that don't return a value (put, delete)
    isOk*: bool
    err*: string

  KVStreamResult* = object
    ## Result type for streaming scan operations
    isOk*: bool
    stream*: StreamResultSet
    err*: string

proc kvOpOk*[T](v: T): KVOpResult[T] =
  KVOpResult[T](isOk: true, val: v)

proc kvOpErr*[T](msg: string): KVOpResult[T] =
  KVOpResult[T](isOk: false, err: msg)

proc isErr*[T](r: KVOpResult[T]): bool =
  not r.isOk

proc isOk*[T](r: KVOpResult[T]): bool =
  r.isOk

proc value*[T](r: KVOpResult[T]): T =
  doAssert r.isOk, "called .value on Err result: " & r.err
  r.val

proc error*[T](r: KVOpResult[T]): string =
  doAssert not r.isOk, "called .error on Ok result"
  r.err

# Void result constructors
proc kvVoidOk*(): KVOpVoidResult =
  KVOpVoidResult(isOk: true)

proc kvVoidErr*(msg: string): KVOpVoidResult =
  KVOpVoidResult(isOk: false, err: msg)

proc isErr*(r: KVOpVoidResult): bool =
  not r.isOk

proc isOk*(r: KVOpVoidResult): bool =
  r.isOk

# Stream result constructors
proc kvStreamOk*(stream: StreamResultSet): KVStreamResult =
  KVStreamResult(isOk: true, stream: stream)

proc kvStreamErr*(msg: string): KVStreamResult =
  KVStreamResult(isOk: false, err: msg)

proc isErr*(r: KVStreamResult): bool =
  not r.isOk

proc isOk*(r: KVStreamResult): bool =
  r.isOk

proc stream*(r: KVStreamResult): StreamResultSet =
  doAssert r.isOk, "called .stream on Err result: " & r.err
  r.stream

proc error*(r: KVStreamResult): string =
  doAssert not r.isOk, "called .error on Ok result"
  r.err

# ---------------------------------------------------------------------------
# Transaction result type
# ---------------------------------------------------------------------------

type
  TxnBeginResult* = tuple[txnId: TransactionID, readTimestamp: uint64]

# ---------------------------------------------------------------------------
# KV Store Interface (abstract base class)
# ---------------------------------------------------------------------------

type
  KVStore* = ref object of RootObj
    ## Abstract interface for KV operations.
    ## Can be implemented by FractioClient (real) or MockKVStore (testing).
    ##
    ## All operations support MVCC transactions via txnId and readTimestamp.
    ## When txnId is zero, operations use the latest committed version.

# Get operation
method get*(store: KVStore, key: string,
            txnId: TransactionID = zeroTransactionID(),
            readTimestamp: uint64 = 0): KVOpResult[Option[string]] {.base, gcsafe.} =
  ## Get a value by key.
  ## Returns some(value) if found, none(string) if not found.
  ## Transaction-aware: reads from snapshot at readTimestamp if provided.
  kvOpErr[Option[string]]("not implemented")

# Put operation
method put*(store: KVStore, key: string, value: string,
            txnId: TransactionID = zeroTransactionID()): KVOpVoidResult {.base, gcsafe.} =
  ## Put a key-value pair.
  ## If txnId is provided, writes are staged until commit.
  ## Otherwise, writes immediately.
  kvVoidErr("not implemented")

# Delete operation
method delete*(store: KVStore, key: string,
               txnId: TransactionID = zeroTransactionID()): KVOpVoidResult {.base, gcsafe.} =
  ## Delete a key.
  ## If txnId is provided, deletion is staged until commit.
  kvVoidErr("not implemented")

# Scan operation (returns all results as array - deprecated for large scans)
method scan*(store: KVStore, startKey: string, endKey: string,
             limit: uint32 = 0,
             txnId: TransactionID = zeroTransactionID(),
             readTimestamp: uint64 = 0): KVOpResult[seq[tuple[key,
                 value: string]]] {.base, gcsafe.} =
  ## Scan a key range.
  ## Returns all key-value pairs in the range [startKey, endKey).
  ## If limit > 0, returns at most limit entries.
  ## Transaction-aware: reads from snapshot at readTimestamp if provided.
  ## DEPRECATED for large scans - use streamScan instead.
  kvOpErr[seq[tuple[key, value: string]]]("not implemented")

# Streaming scan operation (preferred for large result sets)
method streamScan*(store: KVStore, startKey: string, endKey: string,
                  limit: uint32 = 0,
                  txnId: TransactionID = zeroTransactionID(),
                  readTimestamp: uint64 = 0,
                  config: StreamConfig = defaultStreamConfig()): KVStreamResult {.base, gcsafe.} =
  ## Streaming scan for large key ranges.
  ## Returns a StreamResultSet that lazily fetches data using a background thread.
  ## Use this for queries that may return large result sets to avoid memory pressure.
  ## Consumer can iterate while prefetch thread continues reading ahead.
  kvStreamErr("not implemented")

# Transaction operations
method beginTxn*(store: KVStore): KVOpResult[TxnBeginResult] {.base.} =
  ## Begin a new transaction.
  ## Returns (txnId, readTimestamp) for subsequent operations.
  kvOpErr[TxnBeginResult]("not implemented")

method commitTxn*(store: KVStore, txnId: TransactionID): KVOpVoidResult {.base.} =
  ## Commit a transaction.
  ## Returns error if conflict detected.
  kvVoidErr("not implemented")

method rollbackTxn*(store: KVStore, txnId: TransactionID): KVOpVoidResult {.base.} =
  ## Rollback a transaction.
  ## Discards all staged writes/deletes.
  kvVoidErr("not implemented")

# ---------------------------------------------------------------------------
# KV Store with routing support (optional extension)
# ---------------------------------------------------------------------------

type
  KVStoreWithRouting* = ref object of KVStore
    ## Extended interface for KV stores that support group routing.
    ## Used by FractioClient which routes to specific Raft groups.

# Group-aware operations (for distributed KV stores)
method getInGroup*(store: KVStoreWithRouting, key: string, groupId: GroupID,
                   txnId: TransactionID = zeroTransactionID(),
                   readTimestamp: uint64 = 0): KVOpResult[Option[
                       string]] {.base, gcsafe.} =
  ## Get from a specific group.
  kvOpErr[Option[string]]("not implemented")

method putInGroup*(store: KVStoreWithRouting, key: string, value: string, groupId: GroupID,
                   txnId: TransactionID = zeroTransactionID()): KVOpVoidResult {.base, gcsafe.} =
  ## Put to a specific group.
  kvVoidErr("not implemented")

method deleteInGroup*(store: KVStoreWithRouting, key: string, groupId: GroupID,
                      txnId: TransactionID = zeroTransactionID()): KVOpVoidResult {.base, gcsafe.} =
  ## Delete from a specific group.
  kvVoidErr("not implemented")

# ---------------------------------------------------------------------------
# Convenience procs for working with KVStore
# ---------------------------------------------------------------------------

proc txnGet*(store: KVStore, key: string, txnId: TransactionID,
             readTimestamp: uint64): KVOpResult[Option[string]] =
  ## Get within a transaction context.
  store.get(key, txnId = txnId, readTimestamp = readTimestamp)

proc txnPut*(store: KVStore, key: string, value: string,
    txnId: TransactionID): KVOpVoidResult =
  ## Put within a transaction context.
  store.put(key, value, txnId = txnId)

proc txnDelete*(store: KVStore, key: string,
    txnId: TransactionID): KVOpVoidResult =
  ## Delete within a transaction context.
  store.delete(key, txnId = txnId)

proc txnScan*(store: KVStore, startKey: string, endKey: string,
              txnId: TransactionID, readTimestamp: uint64,
              limit: uint32 = 0): KVOpResult[seq[tuple[key, value: string]]] =
  ## Scan within a transaction context.
  store.scan(startKey, endKey, limit, txnId = txnId,
      readTimestamp = readTimestamp)

proc txnStreamScan*(store: KVStore, startKey: string, endKey: string,
                   txnId: TransactionID, readTimestamp: uint64,
                   limit: uint32 = 0,
                   config: StreamConfig = defaultStreamConfig()): KVStreamResult =
  ## Streaming scan within a transaction context.
  ## Preferred for large result sets.
  store.streamScan(startKey, endKey, limit, txnId = txnId,
      readTimestamp = readTimestamp, config = config)

# Helper to consume entire stream into a sequence (for backward compatibility)
proc consumeStream*(rs: StreamResultSet): seq[KeyValuePair] =
  ## Consume entire stream and return all results as a sequence.
  ## WARNING: This defeats the streaming purpose - use only for small results.
  ## Caller must close the stream after calling this.
  result = @[]
  while rs.hasNext():
    let kv = rs.next()
    if kv.isSome:
      result.add(kv.get)
