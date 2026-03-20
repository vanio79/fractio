## Client-side KV Provider Implementation
## ========================================
##
## Implements KVProvider interface using ProtocolClient.
## Used for client-side SQL execution with remote server.
##
## This enables:
## - SQL parsing on client
## - SQL planning on client
## - SQL execution on client (sending KV ops to server)
##
## Benefits:
## - Reduced server CPU load
## - Better testability (tests can use real network)
## - Clearer separation of concerns

import std/[options]
import ./kv_provider
import ../protocol/client
import ../protocol/messages/kv as kvMsgs
import ../protocol/messages/txn as txnMsgs

type
  ClientKVProvider* = ref object of KVProvider
    ## Client-side KV provider using ProtocolClient
    client*: ProtocolClient
    currentTxnId*: uint64 # Current transaction ID (if any)

proc newClientKVProvider*(client: ProtocolClient): ClientKVProvider =
  ## Create a new client-side KV provider
  result = ClientKVProvider(
    client: client,
    currentTxnId: 0
  )

  # Session management - on client side, sessions are implicit
  # The server manages sessions; we just track transaction IDs
  result.createSessionImpl = proc(): uint64 {.gcsafe, raises: [].} =
    # On client side, session ID is just a counter
    # Real session management happens on server
    1'u64 # Placeholder

  result.closeSessionImpl = proc(sessionId: uint64) {.gcsafe, raises: [].} =
    # No-op on client side
    discard

  result.beginTransactionImpl = proc(sessionId: uint64): KVResult[
      uint64] {.gcsafe, raises: [].} =
    let res = client.beginTxn()
    if res.isOk:
      result.currentTxnId = res.value.txnId
      KVResult[uint64].ok(res.value.txnId)
    else:
      KVResult[uint64].err(res.error.msg)

  result.commitTransactionImpl = proc(sessionId: uint64): KVResult[
      void] {.gcsafe, raises: [].} =
    if result.currentTxnId == 0:
      return KVResult[void].err("no active transaction")
    let res = client.commitTxn(result.currentTxnId)
    result.currentTxnId = 0
    if res.isOk and res.value.status == txnMsgs.TxnCommitOK:
      KVResult[void].ok()
    else:
      let errMsg = if res.isOk: "commit failed" else: res.error.msg
      KVResult[void].err(errMsg)

  result.rollbackTransactionImpl = proc(sessionId: uint64): KVResult[
      void] {.gcsafe, raises: [].} =
    if result.currentTxnId == 0:
      return KVResult[void].ok() # No transaction to rollback
    let res = client.rollbackTxn(result.currentTxnId)
    result.currentTxnId = 0
    if res.isOk:
      KVResult[void].ok()
    else:
      KVResult[void].err(res.error.msg)

  # Read operations - use txnId for transactional reads
  result.getImpl = proc(sessionId: uint64, key: string,
                        readTimestamp: uint64 = 0): KVResult[Option[
                            string]] {.gcsafe, raises: [].} =
    let res = client.kvGet(key, txnId = result.currentTxnId,
                           readTimestamp = readTimestamp)
    if res.isOk:
      if res.value.found:
        KVResult[Option[string]].ok(some(res.value.value))
      else:
        KVResult[Option[string]].ok(none(string))
    else:
      KVResult[Option[string]].err(res.error.msg)

  result.scanImpl = proc(sessionId: uint64, startKey, endKey: string,
                         limit: uint32 = 0,
                             readTimestamp: uint64 = 0): KVResult[seq[
                             KVEntry]] {.gcsafe, raises: [].} =
    let res = client.kvScan(startKey, endKey, limit, txnId = result.currentTxnId,
                            readTimestamp = readTimestamp)
    if res.isOk:
      var entries: seq[KVEntry] = @[]
      # ScanResponseFrame contains entries
      for i in 0 ..< res.value.entries.len:
        entries.add(KVEntry(key: res.value.entries[i].key,
                           value: res.value.entries[i].value))
      KVResult[seq[KVEntry]].ok(entries)
    else:
      KVResult[seq[KVEntry]].err(res.error.msg)

  # Latest committed reads (no transaction context)
  result.latestGetImpl = proc(key: string): KVResult[Option[string]] {.gcsafe,
      raises: [].} =
    let res = client.kvGet(key)
    if res.isOk:
      if res.value.found:
        KVResult[Option[string]].ok(some(res.value.value))
      else:
        KVResult[Option[string]].ok(none(string))
    else:
      KVResult[Option[string]].err(res.error.msg)

  result.latestScanImpl = proc(startKey, endKey: string,
                               limit: uint32 = 0): KVResult[seq[
                                   KVEntry]] {.gcsafe, raises: [].} =
    let res = client.kvScan(startKey, endKey, limit)
    if res.isOk:
      var entries: seq[KVEntry] = @[]
      for i in 0 ..< res.value.entries.len:
        entries.add(KVEntry(key: res.value.entries[i].key,
                           value: res.value.entries[i].value))
      KVResult[seq[KVEntry]].ok(entries)
    else:
      KVResult[seq[KVEntry]].err(res.error.msg)

  # Write operations
  result.putImpl = proc(sessionId: uint64, key, value: string): KVResult[
      void] {.gcsafe, raises: [].} =
    # For transactional writes, we need to use the transaction-aware put
    # The ProtocolClient.kvPut has a txnId parameter
    let res = client.kvPut(key, value, txnId = result.currentTxnId)
    if res.isOk and res.value.status == kvMsgs.PutStatusOK:
      KVResult[void].ok()
    else:
      let errMsg = if res.isOk: "put failed" else: res.error.msg
      KVResult[void].err(errMsg)

  result.deleteImpl = proc(sessionId: uint64, key: string): KVResult[
      void] {.gcsafe, raises: [].} =
    let res = client.kvDelete(key, txnId = result.currentTxnId)
    if res.isOk and res.value.status in {kvMsgs.DelStatusDeleted,
        kvMsgs.DelStatusNotFound}:
      KVResult[void].ok()
    else:
      let errMsg = if res.isOk: "delete failed" else: res.error.msg
      KVResult[void].err(errMsg)

  # Batch operations
  result.batchPutImpl = proc(ops: seq[(string, string, bool)]): KVResult[
      void] {.gcsafe, raises: [].} =
    # Build batch request
    var batchOps: seq[kvMsgs.BatchOp] = @[]
    for (key, value, isDelete) in ops:
      batchOps.add(kvMsgs.BatchOp(
        op: if isDelete: kvMsgs.boDelete else: kvMsgs.boPut,
        key: key,
        value: value
      ))

    let res = client.kvBatch(batchOps)
    if res.isOk and res.value.status == kvMsgs.BatchStatusOK:
      KVResult[void].ok()
    else:
      let errMsg = if res.isOk: "batch failed" else: res.error.msg
      KVResult[void].err(errMsg)
