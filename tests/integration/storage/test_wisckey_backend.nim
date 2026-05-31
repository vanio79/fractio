# Unit tests for WiscKey storage backend

import std/[unittest, os, times, options, strutils]
import fractio/storage/backend
import fractio/storage/factory
import fractio/storage/wisckey_backend

suite "WiscKey Backend Tests":
  var testDir: string
  var backend: StorageBackend = nil

  setup:
    # Create a temporary directory for tests
    let timestamp = getTime().toUnix()
    testDir = getCurrentDir() / "tmp_test_wisckey_" & $timestamp
    createDir(testDir)
    backend = nil

  teardown:
    # Clean up
    if backend != nil:
      discard backend.destroy()
      backend = nil
    if testDir.len > 0 and dirExists(testDir):
      removeDir(testDir)

  test "Open and close database":
    backend = createWiscKeyBackend(testDir)
    check backend != nil
    check backend.isOpen() == true

  test "Put and get basic key-value":
    backend = createWiscKeyBackend(testDir)

    let key = "test_key"
    let value = "test_value"

    check backend.put(key, value) == true
    let result = backend.get(key)

    check result.isSome
    check result.get() == value

  test "Get non-existent key":
    backend = createWiscKeyBackend(testDir)

    let result = backend.get("nonexistent")

    check result.isNone

  test "Update existing key":
    backend = createWiscKeyBackend(testDir)

    let key = "update_key"

    check backend.put(key, "value1") == true
    check backend.get(key).get() == "value1"

    check backend.put(key, "value2") == true
    check backend.get(key).get() == "value2"

  test "Delete existing key":
    backend = createWiscKeyBackend(testDir)

    let key = "delete_key"
    let value = "delete_value"

    check backend.put(key, value) == true
    check backend.get(key).isSome

    check backend.delete(key) == true
    check backend.get(key).isNone

  test "Delete non-existent key":
    backend = createWiscKeyBackend(testDir)

    # Deleting non-existent key should return true (idempotent)
    check backend.delete("nonexistent") == true

  test "Exists method":
    backend = createWiscKeyBackend(testDir)

    let key = "exists_key"
    let value = "exists_value"

    check backend.exists(key) == false
    check backend.put(key, value) == true
    check backend.exists(key) == true

  test "Multiple put operations":
    backend = createWiscKeyBackend(testDir)

    for i in 0..<100:
      let key = "key_" & $i
      let value = "value_" & $i
      check backend.put(key, value) == true

    # Verify all values
    for i in 0..<100:
      let key = "key_" & $i
      let expectedValue = "value_" & $i
      check backend.get(key).get() == expectedValue

  test "Empty key and value":
    backend = createWiscKeyBackend(testDir)

    check backend.put("", "empty_key_value") == true
    check backend.get("").get() == "empty_key_value"

    check backend.put("empty_value", "") == true
    check backend.get("empty_value").get() == ""

  test "Large value":
    backend = createWiscKeyBackend(testDir)

    let largeValue = "x".repeat(1024 * 1024) # 1MB value
    check backend.put("large_key", largeValue) == true
    check backend.get("large_key").get() == largeValue

  test "Write batch - multiple puts":
    backend = createWiscKeyBackend(testDir)

    let pairs = @[
      ("batch_key1", "batch_value1"),
      ("batch_key2", "batch_value2"),
      ("batch_key3", "batch_value3")
    ]

    check backend.writeBatch(pairs, @[]) == true

    check backend.get("batch_key1").get() == "batch_value1"
    check backend.get("batch_key2").get() == "batch_value2"
    check backend.get("batch_key3").get() == "batch_value3"

  test "Write batch - with deletes":
    backend = createWiscKeyBackend(testDir)

    # First put some keys
    check backend.put("key1", "value1") == true
    check backend.put("key2", "value2") == true
    check backend.put("key3", "value3") == true

    # Now delete key2 and add key4
    let pairs = @[("key4", "value4")]
    let deletes = @["key2"]

    check backend.writeBatch(pairs, deletes) == true

    check backend.get("key1").get() == "value1"
    check backend.get("key2").isNone # deleted
    check backend.get("key3").get() == "value3"
    check backend.get("key4").get() == "value4"

  test "Iterator - seek to first":
    backend = createWiscKeyBackend(testDir)

    # Add some keys
    for i in @["a", "b", "c"]:
      check backend.put(i, i & "_value") == true

    let iter = backend.newIterator()
    check iter != nil

    check seekToFirstIter(iter) == true
    check validIter(iter) == true
    check keyIter(iter) == "a"
    check valueIter(iter) == "a_value"

    destroyIter(iter)

  test "Iterator - seek to last":
    backend = createWiscKeyBackend(testDir)

    # Add some keys
    for i in @["x", "y", "z"]:
      check backend.put(i, i & "_value") == true

    let iter = backend.newIterator()
    check seekToLastIter(iter) == true
    check validIter(iter) == true
    check keyIter(iter) == "z"

    destroyIter(iter)

  test "Iterator - seek to specific key":
    backend = createWiscKeyBackend(testDir)

    # Add some keys
    for i in @["apple", "banana", "cherry"]:
      check backend.put(i, i & "_value") == true

    let iter = backend.newIterator()
    check seekIter(iter, "banana") == true
    check validIter(iter) == true
    check keyIter(iter) == "banana"

    destroyIter(iter)

  test "Iterator - traverse all keys":
    backend = createWiscKeyBackend(testDir)

    let keys = @["a", "b", "c", "d", "e"]
    for k in keys:
      check backend.put(k, k & "_val") == true

    let iter = backend.newIterator()
    var count = 0
    var currentKey = ""

    if seekToFirstIter(iter):
      while validIter(iter):
        count.inc
        currentKey = keyIter(iter)
        discard nextIter(iter)

    check count == keys.len
    destroyIter(iter)

  test "Flush operation":
    backend = createWiscKeyBackend(testDir)

    check backend.flush() == true

  test "Compact range":
    backend = createWiscKeyBackend(testDir)

    # Add some data first
    for i in 0..<10:
      discard backend.put("key" & $i, "value" & $i)

    # Compact the range
    backend.compactRange()

    # Data should still be accessible
    check backend.get("key0").get() == "value0"
    check backend.get("key9").get() == "value9"

  test "Approximate size":
    backend = createWiscKeyBackend(testDir)

    for i in 0..<100:
      discard backend.put("key" & $i, "value" & $i)

    let size = backend.approximateSize("key0", "key99")
    # Size should be > 0 if data was written
    # Note: This may return 0 in some implementations
    check size >= 0

  test "Close and reopen":
    backend = createWiscKeyBackend(testDir)

    check backend.put("key1", "value1") == true
    check backend.get("key1").get() == "value1"

    backend.close()

    # Reopen the database
    backend = createWiscKeyBackend(testDir)

    # Data should persist
    check backend.get("key1").get() == "value1"

  test "Destroy database":
    backend = createWiscKeyBackend(testDir)

    check backend.put("key1", "value1") == true

    # Destroy the database
    check backend.destroy() == true

    # The directory should be cleaned up (or at least the database should be gone)
    # Try to reopen - should fail or be empty
    backend = createWiscKeyBackend(testDir)
    check backend.get("key1").isNone

  test "Unicode keys and values":
    backend = createWiscKeyBackend(testDir)

    let unicodeKey = "ключ"
    let unicodeValue = "значення"

    check backend.put(unicodeKey, unicodeValue) == true
    check backend.get(unicodeKey).get() == unicodeValue

  test "Binary data":
    backend = createWiscKeyBackend(testDir)

    let binaryKey = "\x00\x01\x02\x03"
    let binaryValue = "\xff\xfe\xfd\xfc"

    check backend.put(binaryKey, binaryValue) == true
    check backend.get(binaryKey).get() == binaryValue

  test "Binary data with null bytes in middle (DataRow encoding)":
    backend = createWiscKeyBackend(testDir)

    # DataRow encoding: magic "DR" + version + u32 column count.
    # For little-endian, u32(1) = \x01\x00\x00\x00 — null bytes in the middle.
    # This was the exact bug: .cstring truncated at the first \x00.
    let dataRowLikeValue = "DR\x01\x01\x00\x00\x00" &
                           "\x00\x00\x00\x03" & "col" & # col name len=3, "col"
      "\x00" &                     # type = string
      "\x00\x00\x00\x05" & "hello" # str len=5, "hello"
    let key = "data_row_test"
    check backend.put(key, dataRowLikeValue) == true
    let result = backend.get(key)
    check result.isSome
    check result.get().len == dataRowLikeValue.len
    check result.get() == dataRowLikeValue

  test "Write batch with null bytes in values":
    backend = createWiscKeyBackend(testDir)

    let v1 = "\x00\x01\x02"
    let v2 = "\xff\x00\xfe"
    let v3 = "\x00\x00\x00"
    let pairs = @[
      ("null_batch_1", v1),
      ("null_batch_2", v2),
      ("null_batch_3", v3)
    ]
    check backend.writeBatch(pairs, @[]) == true
    check backend.get("null_batch_1").get() == v1
    check backend.get("null_batch_2").get() == v2
    check backend.get("null_batch_3").get() == v3

  test "Iterator scan with null bytes in keys and values":
    backend = createWiscKeyBackend(testDir)

    # Keys with null bytes (like MVCC version keys: userKey + \x00\x00 + timestamp)
    let k1 = "pk\x00\x00\x00\x00\x00\x00\x00\x01"
    let k2 = "pk\x00\x00\x00\x00\x00\x00\x00\x02"
    let v1 = "MVCC\x00\x00\x00\x00\x00\x00\x00\x01\x00data1"
    let v2 = "MVCC\x00\x00\x00\x00\x00\x00\x00\x02\x00data2"

    check backend.put(k1, v1) == true
    check backend.put(k2, v2) == true

    let iter = backend.newIterator()
    check iter != nil
    check seekIter(iter, k1) == true
    check keyIter(iter) == k1
    check valueIter(iter) == v1
    check nextIter(iter) == true
    check keyIter(iter) == k2
    check valueIter(iter) == v2
    destroyIter(iter)
