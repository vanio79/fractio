import std/[unittest, options, strutils]
import fractio/core/types
import fractio/core/mock_kv
import fractio/core/kv_interface

suite "MockKVStore - Basic Operations":
  test "create empty store":
    let store = newMockKVStore()
    check store.keyCount() == 0

  test "put and get":
    let store = newMockKVStore()
    let putRes = store.put("key1", "value1")
    check putRes.isOk

    let getRes = store.get("key1")
    check getRes.isOk
    check getRes.val.isSome
    check getRes.val.get() == "value1"

  test "get non-existent key":
    let store = newMockKVStore()
    let getRes = store.get("nonexistent")
    check getRes.isOk
    check getRes.val.isNone

  test "delete existing key":
    let store = newMockKVStore()
    store.setData("key1", "value1")
    check store.hasKey("key1")

    let delRes = store.delete("key1")
    check delRes.isOk
    check not store.hasKey("key1")

  test "delete non-existent key":
    let store = newMockKVStore()
    let delRes = store.delete("nonexistent")
    check delRes.isOk
    check store.keyCount() == 0

  test "put overwrites existing":
    let store = newMockKVStore()
    store.setData("key1", "value1")

    let putRes = store.put("key1", "value2")
    check putRes.isOk

    let getRes = store.get("key1")
    check getRes.isOk
    check getRes.val.get() == "value2"

suite "MockKVStore - Scan Operations":
  test "scan empty store":
    let store = newMockKVStore()
    let scanRes = store.scan("", "")
    check scanRes.isOk
    check scanRes.val.len == 0

  test "scan all keys":
    let store = newMockKVStore()
    store.setData("a", "val_a")
    store.setData("b", "val_b")
    store.setData("c", "val_c")

    let scanRes = store.scan("", "")
    check scanRes.isOk
    check scanRes.val.len == 3
    # Should be sorted by key
    check scanRes.val[0].key == "a"
    check scanRes.val[1].key == "b"
    check scanRes.val[2].key == "c"

  test "scan with start/end range":
    let store = newMockKVStore()
    store.setData("a1", "val1")
    store.setData("a2", "val2")
    store.setData("b1", "val3")
    store.setData("b2", "val4")

    let scanRes = store.scan("a", "b")
    check scanRes.isOk
    check scanRes.val.len == 2 # a1 and a2, not b1
    check scanRes.val[0].key == "a1"
    check scanRes.val[1].key == "a2"

  test "scan with limit":
    let store = newMockKVStore()
    store.setData("key1", "val1")
    store.setData("key2", "val2")
    store.setData("key3", "val3")

    let scanRes = store.scan("", "", limit = 2)
    check scanRes.isOk
    check scanRes.val.len == 2

suite "MockKVStore - Transaction Operations":
  test "begin transaction":
    let store = newMockKVStore()
    let txnRes = store.beginTxn()
    check txnRes.isOk
    check not isZero(txnRes.val.txnId)
    check txnRes.val.readTimestamp > 0

  test "commit empty transaction":
    let store = newMockKVStore()
    let txnRes = store.beginTxn()
    let commitRes = store.commitTxn(txnRes.val.txnId)
    check commitRes.isOk

  test "commit non-existent transaction":
    let store = newMockKVStore()
    let commitRes = store.commitTxn(zeroTransactionID())
    check commitRes.isErr
    check "transaction not found" in commitRes.err

  test "rollback transaction":
    let store = newMockKVStore()
    let txnRes = store.beginTxn()
    let rollbackRes = store.rollbackTxn(txnRes.val.txnId)
    check rollbackRes.isOk

  test "rollback non-existent transaction":
    let store = newMockKVStore()
    let rollbackRes = store.rollbackTxn(zeroTransactionID())
    check rollbackRes.isOk # Rollback is OK even if txn doesn't exist

suite "MockKVStore - Transaction Staging":
  test "staged write not visible before commit":
    let store = newMockKVStore()
    let txnRes = store.beginTxn()

    let putRes = store.put("key1", "value1", txnId = txnRes.val.txnId)
    check putRes.isOk

    # Uncommitted write should NOT be visible in committed data
    check not store.hasKey("key1")

    # But SHOULD be visible when reading with the same txnId
    let getRes = store.get("key1", txnId = txnRes.val.txnId,
                           readTimestamp = txnRes.val.readTimestamp)
    check getRes.isOk
    check getRes.val.isSome
    check getRes.val.get() == "value1"

  test "staged write visible after commit":
    let store = newMockKVStore()
    let txnRes = store.beginTxn()
    discard store.put("key1", "value1", txnId = txnRes.val.txnId)

    let commitRes = store.commitTxn(txnRes.val.txnId)
    check commitRes.isOk

    # Now visible in committed data
    check store.hasKey("key1")
    check store.getData("key1").get() == "value1"

  test "staged delete":
    let store = newMockKVStore()
    store.setData("key1", "value1")

    let txnRes = store.beginTxn()
    let delRes = store.delete("key1", txnId = txnRes.val.txnId)
    check delRes.isOk

    # Still in committed data before commit
    check store.hasKey("key1")

    # Not visible when reading with txnId
    let getRes = store.get("key1", txnId = txnRes.val.txnId,
                           readTimestamp = txnRes.val.readTimestamp)
    check getRes.isOk
    check getRes.val.isNone

  test "staged delete after commit":
    let store = newMockKVStore()
    store.setData("key1", "value1")

    let txnRes = store.beginTxn()
    discard store.delete("key1", txnId = txnRes.val.txnId)
    discard store.commitTxn(txnRes.val.txnId)

    check not store.hasKey("key1")

  test "rollback discards staged writes":
    let store = newMockKVStore()
    let txnRes = store.beginTxn()
    discard store.put("key1", "value1", txnId = txnRes.val.txnId)

    discard store.rollbackTxn(txnRes.val.txnId)

    # Not visible in committed data
    check not store.hasKey("key1")

suite "MockKVStore - Test Helpers":
  test "setData bypasses transactions":
    let store = newMockKVStore()
    # Without starting a transaction
    store.setData("key1", "value1")
    check store.hasKey("key1")

  test "getData bypasses transactions":
    let store = newMockKVStore()
    store.setData("key1", "value1")

    # Start a transaction with a staged write
    let txnRes = store.beginTxn()
    discard store.put("key1", "value2", txnId = txnRes.val.txnId)

    # getData should return committed value, not staged
    check store.getData("key1").get() == "value1"

  test "clear removes all data":
    let store = newMockKVStore()
    store.setData("key1", "value1")
    store.setData("key2", "value2")

    let txnRes = store.beginTxn()
    discard store.put("key3", "value3", txnId = txnRes.val.txnId)

    store.clear()

    check store.keyCount() == 0
    check store.allKeys().len == 0

  test "allKeys returns sorted keys":
    let store = newMockKVStore()
    store.setData("c", "val")
    store.setData("a", "val")
    store.setData("b", "val")

    let keys = store.allKeys()
    check keys == @["a", "b", "c"]

suite "MockKVStore - Thread Safety":
  test "multiple transactions are isolated":
    let store = newMockKVStore()

    let txn1 = store.beginTxn()
    let txn2 = store.beginTxn()

    discard store.put("key1", "value_txn1", txnId = txn1.val.txnId)
    discard store.put("key1", "value_txn2", txnId = txn2.val.txnId)

    # Each txn sees its own write
    let get1 = store.get("key1", txnId = txn1.val.txnId,
                         readTimestamp = txn1.val.readTimestamp)
    let get2 = store.get("key1", txnId = txn2.val.txnId,
                         readTimestamp = txn2.val.readTimestamp)

    check get1.val.get() == "value_txn1"
    check get2.val.get() == "value_txn2"

suite "KVInterface - Result Types":
  test "kvOpOk creates success result":
    let res = kvOpOk(some("value"))
    check res.isOk
    check not res.isErr
    check res.val.isSome
    check res.val.get() == "value"

  test "kvOpErr creates error result":
    let res = kvOpErr[Option[string]]("error message")
    check res.isErr
    check not res.isOk
    check res.err == "error message"

  test "kvVoidOk creates success void result":
    let res = kvVoidOk()
    check res.isOk
    check not res.isErr

  test "kvVoidErr creates error void result":
    let res = kvVoidErr("error message")
    check res.isErr
    check not res.isOk
    check res.err == "error message"
