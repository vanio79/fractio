## KV Provider Interface
## ====================
##
## Abstract interface for KV operations used by the SQL executor.
## Allows the SQL executor to work with both:
## - Server-side: RaftKVStoreExt + MvccTransactionStore (in-process)
## - Client-side: ProtocolClient (network)
##
## This enables client-side SQL parsing, planning, and execution
## with only KV operations sent to the server.

import std/[options]
import ../protocol/types # For Result type

type
  KVError* = object
    ## Error type for KV operations
    msg*: string

  KVResult*[T] = Result[T, KVError]

  KVEntry* = object
    ## A key-value entry
    key*: string
    value*: string

  KVProvider* = ref object
    ## Abstract KV provider interface using callbacks.
    ## This allows both in-process and remote implementations.

    # Session/Transaction management
    createSessionImpl*: proc(): uint64 {.gcsafe, raises: [].}
    closeSessionImpl*: proc(sessionId: uint64) {.gcsafe, raises: [].}
    beginTransactionImpl*: proc(sessionId: uint64): KVResult[uint64] {.gcsafe,
        raises: [].}
    commitTransactionImpl*: proc(sessionId: uint64): KVResult[void] {.gcsafe,
        raises: [].}
    rollbackTransactionImpl*: proc(sessionId: uint64): KVResult[void] {.gcsafe,
        raises: [].}

    # Read operations
    getImpl*: proc(sessionId: uint64, key: string,
                   readTimestamp: uint64 = 0): KVResult[Option[
                       string]] {.gcsafe, raises: [].}
    scanImpl*: proc(sessionId: uint64, startKey, endKey: string,
                    limit: uint32 = 0, readTimestamp: uint64 = 0): KVResult[seq[
                        KVEntry]] {.gcsafe, raises: [].}

    # Latest committed read (no transaction context)
    latestGetImpl*: proc(key: string): KVResult[Option[string]] {.gcsafe,
        raises: [].}
    latestScanImpl*: proc(startKey, endKey: string,
                          limit: uint32 = 0): KVResult[seq[KVEntry]] {.gcsafe,
                              raises: [].}

    # Write operations
    putImpl*: proc(sessionId: uint64, key, value: string): KVResult[
        void] {.gcsafe, raises: [].}
    deleteImpl*: proc(sessionId: uint64, key: string): KVResult[void] {.gcsafe,
        raises: [].}

    # Batch operations
    batchPutImpl*: proc(ops: seq[(string, string, bool)]): KVResult[
        void] {.gcsafe, raises: [].}
    ## ops is seq of (key, value, isDelete)

# ---------------------------------------------------------------------------
# Error helpers
# ---------------------------------------------------------------------------

proc kvErr*[T](msg: string): KVResult[T] =
  KVResult[T](isOk: false, err: KVError(msg: msg))

proc kvOk*[T](v: T): KVResult[T] =
  KVResult[T](isOk: true, val: v)

# ---------------------------------------------------------------------------
# Convenience procs
# ---------------------------------------------------------------------------

proc createSession*(p: KVProvider): uint64 =
  ## Create a new session for transaction context
  if p.createSessionImpl != nil:
    p.createSessionImpl()
  else:
    0'u64

proc closeSession*(p: KVProvider, sessionId: uint64) =
  ## Close a session
  if p.closeSessionImpl != nil:
    p.closeSessionImpl(sessionId)

proc beginTransaction*(p: KVProvider, sessionId: uint64): KVResult[uint64] =
  ## Begin a transaction in the session. Returns the transaction ID.
  if p.beginTransactionImpl != nil:
    p.beginTransactionImpl(sessionId)
  else:
    kvErr[uint64]("beginTransaction not implemented")

proc commitTransaction*(p: KVProvider, sessionId: uint64): KVResult[void] =
  ## Commit the current transaction
  if p.commitTransactionImpl != nil:
    p.commitTransactionImpl(sessionId)
  else:
    kvErr[void]("commitTransaction not implemented")

proc rollbackTransaction*(p: KVProvider, sessionId: uint64): KVResult[void] =
  ## Rollback the current transaction
  if p.rollbackTransactionImpl != nil:
    p.rollbackTransactionImpl(sessionId)
  else:
    kvErr[void]("rollbackTransaction not implemented")

proc get*(p: KVProvider, sessionId: uint64, key: string,
          readTimestamp: uint64 = 0): KVResult[Option[string]] =
  ## Get a value within a transaction context
  if p.getImpl != nil:
    p.getImpl(sessionId, key, readTimestamp)
  else:
    kvErr[Option[string]]("get not implemented")

proc scan*(p: KVProvider, sessionId: uint64, startKey, endKey: string,
           limit: uint32 = 0, readTimestamp: uint64 = 0): KVResult[seq[KVEntry]] =
  ## Scan a range within a transaction context
  if p.scanImpl != nil:
    p.scanImpl(sessionId, startKey, endKey, limit, readTimestamp)
  else:
    kvErr[seq[KVEntry]]("scan not implemented")

proc latestGet*(p: KVProvider, key: string): KVResult[Option[string]] =
  ## Get the latest committed value (no transaction context)
  if p.latestGetImpl != nil:
    p.latestGetImpl(key)
  else:
    kvErr[Option[string]]("latestGet not implemented")

proc latestScan*(p: KVProvider, startKey, endKey: string,
                 limit: uint32 = 0): KVResult[seq[KVEntry]] =
  ## Scan latest committed values (no transaction context)
  if p.latestScanImpl != nil:
    p.latestScanImpl(startKey, endKey, limit)
  else:
    kvErr[seq[KVEntry]]("latestScan not implemented")

proc put*(p: KVProvider, sessionId: uint64, key, value: string): KVResult[void] =
  ## Put a value within a transaction context
  if p.putImpl != nil:
    p.putImpl(sessionId, key, value)
  else:
    kvErr[void]("put not implemented")

proc delete*(p: KVProvider, sessionId: uint64, key: string): KVResult[void] =
  ## Delete a key within a transaction context
  if p.deleteImpl != nil:
    p.deleteImpl(sessionId, key)
  else:
    kvErr[void]("delete not implemented")

proc batchPut*(p: KVProvider, ops: seq[(string, string, bool)]): KVResult[void] =
  ## Execute a batch of put/delete operations
  if p.batchPutImpl != nil:
    p.batchPutImpl(ops)
  else:
    kvErr[void]("batchPut not implemented")
