## Server-side KV Provider Implementation
## ========================================
##
## Implements KVProvider interface using RaftKVStoreExt + MvccTransactionStore.
## Used for in-process SQL execution on the server.

import std/[options, tables]
import ./kv_provider
import ../protocol/raft_store
import ../protocol/mvcc_store

proc newServerKVProvider*(raftStore: RaftKVStoreExt,
                          mvccStore: MvccTransactionStore): KVProvider =
  ## Create a new server-side KV provider using in-process stores.
  ## The returned KVProvider can be used with the SQL executor.

  result = KVProvider()

  # Session management
  result.createSessionImpl = proc(): uint64 {.gcsafe, raises: [].} =
    mvccStore.createSession()

  result.closeSessionImpl = proc(sessionId: uint64) {.gcsafe, raises: [].} =
    mvccStore.closeSession(sessionId)

  result.beginTransactionImpl = proc(sessionId: uint64): KVResult[
      uint64] {.gcsafe, raises: [].} =
    let res = mvccStore.beginTransaction(sessionId)
    if res.isOk:
      kvOk(res.value)
    else:
      kvErr[uint64]($res.error.kind)

  result.commitTransactionImpl = proc(sessionId: uint64): KVResult[
      void] {.gcsafe, raises: [].} =
    let res = mvccStore.commitTransaction(sessionId)
    if res.isOk:
      kvOk[void]()
    else:
      kvErr[void]($res.error.kind)

  result.rollbackTransactionImpl = proc(sessionId: uint64): KVResult[
      void] {.gcsafe, raises: [].} =
    let res = mvccStore.rollbackTransaction(sessionId)
    if res.isOk:
      kvOk[void]()
    else:
      kvErr[void]($res.error.kind)

  # Read operations - transactional
  result.getImpl = proc(sessionId: uint64, key: string,
                        readTimestamp: uint64 = 0): KVResult[Option[
                            string]] {.gcsafe, raises: [].} =
    let res = mvccStore.txnGet(sessionId, key)
    if res.isOk:
      kvOk(res.value)
    else:
      kvErr[Option[string]]($res.error.kind)

  result.scanImpl = proc(sessionId: uint64, startKey, endKey: string,
                         limit: uint32 = 0,
                             readTimestamp: uint64 = 0): KVResult[seq[
                             KVEntry]] {.gcsafe, raises: [].} =
    let res = mvccStore.txnScan(sessionId, startKey, endKey, limit)
    if res.isOk:
      var entries: seq[KVEntry] = @[]
      for (k, v) in res.value:
        entries.add(KVEntry(key: k, value: v))
      kvOk(entries)
    else:
      kvErr[seq[KVEntry]]($res.error.kind)

  # Latest committed reads
  result.latestGetImpl = proc(key: string): KVResult[Option[string]] {.gcsafe,
      raises: [].} =
    let res = mvccStore.latestGet(key)
    if res.isOk:
      kvOk(res.value)
    else:
      kvErr[Option[string]]($res.error.kind)

  result.latestScanImpl = proc(startKey, endKey: string,
                               limit: uint32 = 0): KVResult[seq[
                                   KVEntry]] {.gcsafe, raises: [].} =
    # Combine raftScan and MVCC scan for latest values
    var keyValues: Table[string, string] = initTable[string, string]()

    # First, scan raft store for non-MVCC keys
    let regularRes = raftStore.raftScan(startKey, endKey, limit,
        includeSystemKeys = true)
    if regularRes.isOk:
      for (k, entry) in regularRes.value:
        # Skip MVCC-encoded keys
        if not isVersionKey(k) and not isIntentKeyMvcc(k):
          keyValues[k] = entry.value

    # Then, scan MVCC for latest versions
    let mvccRes = mvccStore.latestScan(startKey, endKey, limit)
    if mvccRes.isOk:
      for (k, v) in mvccRes.value:
        keyValues[k] = v

    var entries: seq[KVEntry] = @[]
    for k, v in keyValues.pairs:
      entries.add(KVEntry(key: k, value: v))

    kvOk(entries)

  # Write operations
  result.putImpl = proc(sessionId: uint64, key, value: string): KVResult[
      void] {.gcsafe, raises: [].} =
    let res = mvccStore.txnPut(sessionId, key, value)
    if res.isOk:
      kvOk[void]()
    else:
      kvErr[void]($res.error.kind)

  result.deleteImpl = proc(sessionId: uint64, key: string): KVResult[
      void] {.gcsafe, raises: [].} =
    let res = mvccStore.txnDelete(sessionId, key)
    if res.isOk:
      kvOk[void]()
    else:
      kvErr[void]($res.error.kind)

  # Batch operations
  result.batchPutImpl = proc(ops: seq[(string, string, bool)]): KVResult[
      void] {.gcsafe, raises: [].} =
    # Use raftPut for batch operations (auto-commit)
    for (key, value, isDelete) in ops:
      if isDelete:
        let res = raftStore.raftDelete(key)
        if res.isErr:
          return kvErr[void]($res.error.kind)
      else:
        let res = raftStore.raftPut(key, value)
        if res.isErr:
          return kvErr[void]($res.error.kind)
    kvOk[void]()
