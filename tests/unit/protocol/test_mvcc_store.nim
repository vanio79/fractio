# Unit tests for fractio/protocol/mvcc_store.nim
# Tests key encoding helpers, result types, and error handling

import std/[unittest, options, tables]
import fractio/protocol/mvcc_store
import fractio/core/types
import fractio/core/transaction
import fractio/storage/mvcc/types as mvccTypes
import fractio/protocol/messages/kv

suite "MvccResult Helpers":

  test "mvccOk with value":
    let result = mvccOk(42)
    check result.isOk == true
    check result.value == 42

  test "mvccOk with string":
    let result = mvccOk("test_value")
    check result.isOk == true
    check result.value == "test_value"

  test "mvccErr with error":
    let err = MvccStoreError(kind: mseTransactionNotFound, msg: "test error")
    let result = mvccErr[int](err)
    check result.isOk == false
    check result.error.kind == mseTransactionNotFound
    check result.error.msg == "test error"

  test "mvccVOk":
    let result = mvccVOk()
    check result.isOk == true

  test "mvccVErr":
    let err = MvccStoreError(kind: mseConflictDetected, msg: "conflict")
    let result = mvccVErr(err)
    check result.isOk == false
    check result.error.kind == mseConflictDetected

suite "MvccStoreError Types":

  test "mseNotInTransaction":
    let err = MvccStoreError(kind: mseNotInTransaction, msg: "no txn")
    check err.kind == mseNotInTransaction
    check err.msg == "no txn"

  test "mseTransactionNotFound":
    let err = MvccStoreError(kind: mseTransactionNotFound, msg: "missing")
    check err.kind == mseTransactionNotFound

  test "mseTransactionNotActive":
    let err = MvccStoreError(kind: mseTransactionNotActive, msg: "inactive")
    check err.kind == mseTransactionNotActive

  test "mseConflictDetected":
    let err = MvccStoreError(kind: mseConflictDetected, msg: "conflict",
        conflictingKey: "key1")
    check err.kind == mseConflictDetected
    check err.conflictingKey == "key1"

  test "mseIntentNotFound":
    let err = MvccStoreError(kind: mseIntentNotFound, msg: "intent")
    check err.kind == mseIntentNotFound

  test "mseStorageError":
    let err = MvccStoreError(kind: mseStorageError, msg: "storage")
    check err.kind == mseStorageError

  test "mseTimeout":
    let err = MvccStoreError(kind: mseTimeout, msg: "timeout")
    check err.kind == mseTimeout

suite "MvccValueWithMeta":

  test "basic value with meta":
    let v = MvccValueWithMeta(value: "data", timestamp: 1000'u64,
        version: 5'u64)
    check v.value == "data"
    check v.timestamp == 1000'u64
    check v.version == 5'u64

  test "empty value":
    let v = MvccValueWithMeta(value: "", timestamp: 0'u64, version: 1'u64)
    check v.value == ""
    check v.timestamp == 0'u64

suite "MvccPutResult":

  test "OK result":
    let r = MvccPutResult(
      status: PutStatusOK,
      timestamp: 1000'u64,
      version: 2'u64,
      previousValue: none(string)
    )
    check r.status == PutStatusOK
    check r.timestamp == 1000'u64
    check r.previousValue.isNone

  test "result with previous value":
    let r = MvccPutResult(
      status: PutStatusOK,
      timestamp: 2000'u64,
      version: 3'u64,
      previousValue: some("old_value")
    )
    check r.previousValue.isSome
    check r.previousValue.get() == "old_value"

  test "CASFailed result":
    let r = MvccPutResult(
      status: PutStatusCASFailed,
      timestamp: 0'u64,
      version: 1'u64,
      previousValue: none(string)
    )
    check r.status == PutStatusCASFailed

suite "MvccDeleteResult":

  test "found and deleted":
    let r = MvccDeleteResult(found: true, previousValue: some("deleted"))
    check r.found == true
    check r.previousValue.isSome
    check r.previousValue.get() == "deleted"

  test "not found":
    let r = MvccDeleteResult(found: false, previousValue: none(string))
    check r.found == false
    check r.previousValue.isNone

suite "Key Encoding Helpers":

  test "isVersionKey true":
    var key = "user_key"
    key.add("\x00\x00")
    key.add("\x00\x00\x00\x00\x00\x00\x00\x01")
    check key.len >= 10
    check isVersionKey(key) == true

  test "isVersionKey false - too short":
    let key = "short"
    check isVersionKey(key) == false

  test "isVersionKey false - wrong suffix":
    var key = "user_key"
    key.add("\x00\x01")
    key.add("\x00\x00\x00\x00\x00\x00\x00\x01")
    check isVersionKey(key) == false

  test "isIntentKeyMvcc true":
    let txnId = genTransactionIDLocal()
    let txnBytes = transactionIDToBytes(txnId)
    var key = "user_key\x00\x01"
    key.add(txnBytes)
    check isIntentKeyMvcc(key) == true

  test "isIntentKeyMvcc false - too short":
    let key = "short_key"
    check isIntentKeyMvcc(key) == false

  test "isIntentKeyMvcc false - wrong suffix":
    let key = "user_key\x00\x00\x00\x00\x00\x00\x00\x00\x00"
    check isIntentKeyMvcc(key) == false

suite "SessionTxnState":

  test "SessionTxnState default":
    let state = SessionTxnState(
      txn: nil,
      intents: initTable[string, WriteEntry]()
    )
    check state.txn == nil
    check state.intents.len == 0

suite "Error Kind Enumeration":

  test "all error kinds are distinct":
    check mseNotInTransaction != mseTransactionNotFound
    check mseTransactionNotFound != mseTransactionNotActive
    check mseTransactionNotActive != mseConflictDetected
    check mseConflictDetected != mseIntentNotFound
    check mseIntentNotFound != mseStorageError
    check mseStorageError != mseTimeout
