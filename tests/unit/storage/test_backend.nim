# Unit tests for Storage Backend Interface
# Comprehensive tests for storage backend operations, iterators, error handling,
# concurrent operations, and edge cases

import unittest
import std/[options, hashes, tables, strutils, locks, atomics, sequtils,
    algorithm]
import std/typedthreads
import fractio/storage/backend
import fractio/di/mocks as diMocks

# =============================================================================
# Enhanced Mock Backend with Full Iterator Support
# =============================================================================

type
  EnhancedMockBackend = ref object of StorageBackend
    data: tables.Table[string, string]
    sortedKeys: seq[string]
    putCount: int
    getCount: int
    deleteCount: int
    existsCount: int
    scanCount: int
    flushCount: int
    compactCount: int
    compactRangeCount: int
    writeBatchCount: int
    writeBatchNoSyncCount: int
    newIteratorCount: int
    approximateSizeCount: int
    destroyCount: int
    openCount: int
    closeCount: int
    isOpenFlag: bool
    forceErrorFlag: Atomic[bool]
    errorMessage: string
    statsData: StorageStats
    lock: Lock

  EnhancedMockIterator = ref object of StorageIterator
    currentIndex: int
    validFlag: bool
    seekKey: string

proc getBackend(iter: EnhancedMockIterator): EnhancedMockBackend =
  cast[EnhancedMockBackend](iter.backend)

proc newEnhancedMockBackend(): EnhancedMockBackend =
  new(result)
  result.data = initTable[string, string]()
  result.sortedKeys = @[]
  result.isOpenFlag = true
  result.forceErrorFlag.store(false)
  initLock(result.lock)

method open(backend: EnhancedMockBackend, config: StorageConfig): bool =
  withLock(backend.lock):
    backend.openCount += 1
    if backend.forceErrorFlag.load():
      return false
    backend.isOpenFlag = true
    result = true

method close(backend: EnhancedMockBackend) =
  withLock(backend.lock):
    backend.closeCount += 1
    backend.isOpenFlag = false

method isOpen(backend: EnhancedMockBackend): bool =
  withLock(backend.lock):
    result = backend.isOpenFlag

method put(backend: EnhancedMockBackend, key: string, value: string): bool =
  withLock(backend.lock):
    backend.putCount += 1
    if backend.forceErrorFlag.load():
      return false
    backend.data[key] = value
    backend.statsData.writes += 1
    backend.statsData.bytesWritten += value.len.int64
    if key notin backend.sortedKeys:
      backend.sortedKeys.add(key)
      backend.sortedKeys.sort()
    result = true

method get(backend: EnhancedMockBackend, key: string): Option[string] =
  withLock(backend.lock):
    backend.getCount += 1
    if backend.forceErrorFlag.load():
      return none(string)
    backend.statsData.reads += 1
    if key in backend.data:
      backend.statsData.bytesRead += backend.data[key].len.int64
      result = some(backend.data[key])
    else:
      backend.statsData.cacheMisses += 1
      result = none(string)

method delete(backend: EnhancedMockBackend, key: string): bool =
  withLock(backend.lock):
    backend.deleteCount += 1
    if backend.forceErrorFlag.load():
      return false
    if key in backend.data:
      backend.data.del(key)
      backend.sortedKeys = backend.sortedKeys.filterIt(it != key)
      result = true
    else:
      result = false

method exists(backend: EnhancedMockBackend, key: string): bool =
  withLock(backend.lock):
    backend.existsCount += 1
    result = key in backend.data

method writeBatch(backend: EnhancedMockBackend, pairs: seq[KeyValuePair],
    deletes: seq[string]): bool =
  withLock(backend.lock):
    backend.writeBatchCount += 1
    if backend.forceErrorFlag.load():
      return false
    for pair in pairs:
      backend.data[pair.key] = pair.value
      backend.statsData.writes += 1
      backend.statsData.bytesWritten += pair.value.len.int64
      if pair.key notin backend.sortedKeys:
        backend.sortedKeys.add(pair.key)
    for delKey in deletes:
      if delKey in backend.data:
        backend.data.del(delKey)
        backend.sortedKeys = backend.sortedKeys.filterIt(it != delKey)
    backend.sortedKeys.sort()
    result = true

method writeBatchNoSync(backend: EnhancedMockBackend, pairs: seq[KeyValuePair],
    deletes: seq[string]): bool =
  withLock(backend.lock):
    backend.writeBatchNoSyncCount += 1
    if backend.forceErrorFlag.load():
      return false
    for pair in pairs:
      backend.data[pair.key] = pair.value
      if pair.key notin backend.sortedKeys:
        backend.sortedKeys.add(pair.key)
    for delKey in deletes:
      if delKey in backend.data:
        backend.data.del(delKey)
        backend.sortedKeys = backend.sortedKeys.filterIt(it != delKey)
    backend.sortedKeys.sort()
    result = true

method flush(backend: EnhancedMockBackend): bool =
  withLock(backend.lock):
    backend.flushCount += 1
    result = not backend.forceErrorFlag.load()

method compactRange(backend: EnhancedMockBackend,
    startKey: Option[string] = none(string), endKey: Option[string] = none(string)) =
  withLock(backend.lock):
    backend.compactRangeCount += 1
    backend.statsData.compactions += 1

method getStats(backend: EnhancedMockBackend): StorageStats =
  withLock(backend.lock):
    result = backend.statsData

method approximateSize(backend: EnhancedMockBackend, startKey: string,
    endKey: string): int64 =
  withLock(backend.lock):
    backend.approximateSizeCount += 1
    var size: int64 = 0
    for key, value in backend.data.pairs:
      if key >= startKey and key <= endKey:
        size += value.len.int64
    result = size

method destroy(backend: EnhancedMockBackend): bool =
  withLock(backend.lock):
    backend.destroyCount += 1
    backend.data.clear()
    backend.sortedKeys = @[]
    backend.isOpenFlag = false
    result = true

method newIterator(backend: EnhancedMockBackend): StorageIterator =
  withLock(backend.lock):
    backend.newIteratorCount += 1
    var iter: EnhancedMockIterator
    new(iter)
    iter.backend = backend
    iter.currentIndex = -1
    iter.validFlag = false
    result = iter

method seekToFirst(iter: EnhancedMockIterator): bool =
  let mb = iter.getBackend()
  withLock(mb.lock):
    if mb.sortedKeys.len > 0:
      iter.currentIndex = 0
      iter.validFlag = true
      return true
    iter.validFlag = false
    return false

method seekToLast(iter: EnhancedMockIterator): bool =
  let mb = iter.getBackend()
  withLock(mb.lock):
    if mb.sortedKeys.len > 0:
      iter.currentIndex = mb.sortedKeys.len - 1
      iter.validFlag = true
      return true
    iter.validFlag = false
    return false

method seek(iter: EnhancedMockIterator, key: string): bool =
  let mb = iter.getBackend()
  withLock(mb.lock):
    for i, k in mb.sortedKeys:
      if k >= key:
        iter.currentIndex = i
        iter.validFlag = true
        iter.seekKey = key
        return true
    iter.validFlag = false
    return false

method next(iter: EnhancedMockIterator): bool =
  let mb = iter.getBackend()
  withLock(mb.lock):
    if iter.currentIndex >= 0 and iter.currentIndex < mb.sortedKeys.len - 1:
      iter.currentIndex += 1
      iter.validFlag = true
      return true
    iter.validFlag = false
    return false

method prev(iter: EnhancedMockIterator): bool =
  let mb = iter.getBackend()
  withLock(mb.lock):
    if iter.currentIndex > 0:
      iter.currentIndex -= 1
      iter.validFlag = true
      return true
    iter.validFlag = false
    return false

method valid(iter: EnhancedMockIterator): bool =
  let mb = iter.getBackend()
  withLock(mb.lock):
    result = iter.validFlag and iter.currentIndex >= 0 and
             iter.currentIndex < mb.sortedKeys.len

method key(iter: EnhancedMockIterator): string =
  let mb = iter.getBackend()
  withLock(mb.lock):
    # Check valid without calling valid() to avoid nested lock
    if iter.validFlag and iter.currentIndex >= 0 and
       iter.currentIndex < mb.sortedKeys.len:
      return mb.sortedKeys[iter.currentIndex]
    return ""

method value(iter: EnhancedMockIterator): string =
  let mb = iter.getBackend()
  withLock(mb.lock):
    # Check valid without calling valid() to avoid nested lock
    if iter.validFlag and iter.currentIndex >= 0 and
       iter.currentIndex < mb.sortedKeys.len:
      let k = mb.sortedKeys[iter.currentIndex]
      if k in mb.data:
        return mb.data[k]
    return ""

method destroy(iter: EnhancedMockIterator) =
  withLock(iter.getBackend().lock):
    iter.validFlag = false
    iter.currentIndex = -1

proc setForceError(backend: EnhancedMockBackend, enable: bool,
    msg: string = "mock error") =
  backend.forceErrorFlag.store(enable)
  backend.errorMessage = msg

proc reset(backend: EnhancedMockBackend) =
  withLock(backend.lock):
    backend.data.clear()
    backend.sortedKeys = @[]
    backend.putCount = 0
    backend.getCount = 0
    backend.deleteCount = 0
    backend.existsCount = 0
    backend.scanCount = 0
    backend.flushCount = 0
    backend.compactCount = 0
    backend.compactRangeCount = 0
    backend.writeBatchCount = 0
    backend.writeBatchNoSyncCount = 0
    backend.newIteratorCount = 0
    backend.approximateSizeCount = 0
    backend.destroyCount = 0
    backend.openCount = 0
    backend.closeCount = 0
    backend.isOpenFlag = true
    backend.forceErrorFlag.store(false)
    backend.errorMessage = ""
    backend.statsData = StorageStats()

# =============================================================================
# Test Suites - Configuration
# =============================================================================

suite "Storage Backend - Configuration":
  test "create default storage config":
    let config = defaultStorageConfig("/tmp/test")
    check config.path == "/tmp/test"
    check config.maxOpenFiles == 1000
    check config.writeBufferSize == 4 * 1024 * 1024
    check config.blockSize == 4 * 1024
    check config.compression == ctSnappy
    check config.createIfMissing == true
    check config.errorIfExists == false
    check config.syncWrites == false

  test "create custom storage config":
    let config = StorageConfig(
      path: "/data/db",
      maxOpenFiles: 500,
      writeBufferSize: 8 * 1024 * 1024,
      blockSize: 8 * 1024,
      compression: ctLz4,
      createIfMissing: false,
      errorIfExists: true,
      syncWrites: true,
      blockCacheSize: 16 * 1024 * 1024
    )
    check config.path == "/data/db"
    check config.maxOpenFiles == 500
    check config.writeBufferSize == 8 * 1024 * 1024
    check config.blockSize == 8 * 1024
    check config.compression == ctLz4
    check config.createIfMissing == false
    check config.errorIfExists == true
    check config.syncWrites == true
    check config.blockCacheSize == 16 * 1024 * 1024

  test "wiskey-specific config":
    let config = StorageConfig(
      path: "/data/wiskey",
      vlogMaxSize: 2_000_000_000,
      vlogCleanThreshold: 200_000,
      vlogMinCleanThreshold: 5_000,
      vlogCleanBufferSize: 128 * 1024 * 1024
    )
    check config.vlogMaxSize == 2_000_000_000
    check config.vlogCleanThreshold == 200_000
    check config.vlogMinCleanThreshold == 5_000
    check config.vlogCleanBufferSize == 128 * 1024 * 1024

  test "compression types string representation":
    check $ctNone == "none"
    check $ctSnappy == "snappy"
    check $ctLz4 == "lz4"

  test "compression type ordinals":
    check ctNone.ord == 0
    check ctSnappy.ord == 1
    check ctLz4.ord == 2

# =============================================================================
# Test Suites - Statistics
# =============================================================================

suite "Storage Backend - Statistics":
  test "create storage stats with values":
    let stats = StorageStats(
      reads: 100,
      writes: 50,
      bytesRead: 10000,
      bytesWritten: 5000,
      compactions: 5,
      cacheHits: 80,
      cacheMisses: 20
    )
    check stats.reads == 100
    check stats.writes == 50
    check stats.bytesRead == 10000
    check stats.bytesWritten == 5000
    check stats.compactions == 5
    check stats.cacheHits == 80
    check stats.cacheMisses == 20

  test "empty storage stats defaults to zero":
    let stats = StorageStats()
    check stats.reads == 0
    check stats.writes == 0
    check stats.bytesRead == 0
    check stats.bytesWritten == 0
    check stats.compactions == 0
    check stats.cacheHits == 0
    check stats.cacheMisses == 0

  test "stats accumulate through operations":
    let backend = newEnhancedMockBackend()
    discard backend.put("key1", "value12345")
    discard backend.put("key2", "value67890")
    discard backend.get("key1")
    discard backend.get("nonexistent")
    backend.compactRange()

    let stats = backend.getStats()
    check stats.writes == 2
    check stats.bytesWritten == 20
    check stats.reads == 2
    check stats.bytesRead == 10
    check stats.compactions == 1
    check stats.cacheMisses == 1

    backend.close()

# =============================================================================
# Test Suites - Error Handling
# =============================================================================

suite "Storage Backend - Error Handling":
  test "create storage error with code":
    let err = newStorageError(secNotFound, "Key not found")
    check err.code == secNotFound
    check err.msg == "Key not found"

  test "storage error codes ordinals":
    check secNotFound.ord == 0
    check secCorruption.ord == 1
    check secIOError.ord == 2
    check secNotSupported.ord == 3
    check secAlreadyExists.ord == 4
    check secInvalidArgument.ord == 5
    check secOutOfMemory.ord == 6

  test "different error types messages":
    let notFound = newStorageError(secNotFound, "Not found")
    let corruption = newStorageError(secCorruption, "Data corrupted")
    let ioError = newStorageError(secIOError, "IO failed")
    let notSupported = newStorageError(secNotSupported, "Not supported")
    let alreadyExists = newStorageError(secAlreadyExists, "Already exists")
    let invalidArg = newStorageError(secInvalidArgument, "Invalid argument")
    let outOfMem = newStorageError(secOutOfMemory, "Out of memory")

    check notFound.code == secNotFound
    check corruption.code == secCorruption
    check ioError.code == secIOError
    check notSupported.code == secNotSupported
    check alreadyExists.code == secAlreadyExists
    check invalidArg.code == secInvalidArgument
    check outOfMem.code == secOutOfMemory

  test "error can be caught":
    let err = newStorageError(secCorruption, "test error")
    check err.code == secCorruption
    check "test error" in err.msg

# =============================================================================
# Test Suites - Key-Value Pair
# =============================================================================

suite "Storage Backend - Key-Value Pair":
  test "create key-value pair":
    let pair: KeyValuePair = (key: "test_key", value: "test_value")
    check pair.key == "test_key"
    check pair.value == "test_value"

  test "key-value pair sequence":
    let pairs: seq[KeyValuePair] = @[
      (key: "key1", value: "value1"),
      (key: "key2", value: "value2"),
      (key: "key3", value: "value3")
    ]
    check pairs.len == 3
    check pairs[0].key == "key1"
    check pairs[1].key == "key2"
    check pairs[2].key == "key3"

  test "key-value pair with empty value":
    let pair: KeyValuePair = (key: "key", value: "")
    check pair.key == "key"
    check pair.value == ""

  test "key-value pair with empty key":
    let pair: KeyValuePair = (key: "", value: "value")
    check pair.key == ""
    check pair.value == "value"

# =============================================================================
# Test Suites - Hash Operations
# =============================================================================

suite "Storage Backend - Hash Operations":
  test "hash string returns non-zero":
    let hashVal = toHash("test_key")
    check hashVal != 0

  test "hash different strings produces different values":
    let hash1 = toHash("key1")
    let hash2 = toHash("key2")
    check hash1 != hash2

  test "hash same strings produces same value":
    let hash1 = toHash("key")
    let hash2 = toHash("key")
    check hash1 == hash2

  test "hash empty string":
    let hashVal = toHash("")
    check hashVal != 0

# =============================================================================
# Test Suites - Basic Operations (Enhanced Mock)
# =============================================================================

suite "Storage Backend - Basic Operations":
  var backend: EnhancedMockBackend

  setup:
    backend = newEnhancedMockBackend()

  teardown:
    backend.close()

  test "put and get single key":
    check backend.put("key1", "value1") == true
    let result = backend.get("key1")
    check result.isSome
    check result.get == "value1"

  test "put overwrites existing key":
    discard backend.put("key1", "value1")
    check backend.put("key1", "value2") == true
    let result = backend.get("key1")
    check result.isSome
    check result.get == "value2"
    check backend.putCount == 2

  test "get non-existent key returns none":
    let result = backend.get("nonexistent")
    check result.isNone

  test "delete existing key":
    discard backend.put("key1", "value1")
    check backend.delete("key1") == true
    check backend.get("key1").isNone

  test "delete non-existent key returns false":
    check backend.delete("nonexistent") == false

  test "exists check for existing key":
    discard backend.put("key1", "value1")
    check backend.exists("key1") == true

  test "exists check for non-existent key":
    check backend.exists("nonexistent") == false

  test "multiple put operations":
    for i in 0..<100:
      discard backend.put("key" & $i, "value" & $i)
    check backend.data.len == 100
    check backend.putCount == 100

  test "mixed operations sequence":
    discard backend.put("key1", "value1")
    discard backend.put("key2", "value2")
    discard backend.put("key3", "value3")
    discard backend.get("key1")
    discard backend.delete("key2")
    discard backend.get("key3")

    check backend.putCount == 3
    check backend.getCount == 2
    check backend.deleteCount == 1
    check backend.data.len == 2

# =============================================================================
# Test Suites - WriteBatch Operations
# =============================================================================

suite "Storage Backend - WriteBatch Operations":
  var backend: EnhancedMockBackend

  setup:
    backend = newEnhancedMockBackend()

  teardown:
    backend.close()

  test "writeBatch with puts only":
    let pairs: seq[KeyValuePair] = @[
      (key: "key1", value: "value1"),
      (key: "key2", value: "value2"),
      (key: "key3", value: "value3")
    ]
    check backend.writeBatch(pairs, @[]) == true
    check backend.data.len == 3
    check backend.writeBatchCount == 1

  test "writeBatch with deletes only":
    discard backend.put("key1", "value1")
    discard backend.put("key2", "value2")

    check backend.writeBatch(@[], @["key1", "key2"]) == true
    check backend.data.len == 0

  test "writeBatch with mixed puts and deletes":
    discard backend.put("old1", "oldvalue1")
    discard backend.put("old2", "oldvalue2")

    let pairs: seq[KeyValuePair] = @[
      (key: "new1", value: "newvalue1"),
      (key: "new2", value: "newvalue2")
    ]
    check backend.writeBatch(pairs, @["old1"]) == true
    check backend.data.len == 3
    check backend.get("old1").isNone
    check backend.get("new1").isSome

  test "writeBatch empty operations":
    check backend.writeBatch(@[], @[]) == true
    check backend.data.len == 0

  test "writeBatchNoSync with puts":
    let pairs: seq[KeyValuePair] = @[
      (key: "key1", value: "value1"),
      (key: "key2", value: "value2")
    ]
    check backend.writeBatchNoSync(pairs, @[]) == true
    check backend.data.len == 2
    check backend.writeBatchNoSyncCount == 1
    check backend.statsData.writes == 0

  test "writeBatchNoSync does not update stats":
    discard backend.put("key1", "value1")
    let initialWrites = backend.statsData.writes

    discard backend.writeBatchNoSync(@[(key: "key2", value: "value2")], @[])
    check backend.statsData.writes == initialWrites

# =============================================================================
# Test Suites - Flush and Compact Operations
# =============================================================================

suite "Storage Backend - Flush and Compact":
  var backend: EnhancedMockBackend

  setup:
    backend = newEnhancedMockBackend()

  teardown:
    backend.close()

  test "flush operation":
    check backend.flush() == true
    check backend.flushCount == 1

  test "multiple flush operations":
    for i in 0..<5:
      discard backend.flush()
    check backend.flushCount == 5

  test "compactRange without parameters":
    backend.compactRange()
    check backend.compactRangeCount == 1

  test "compactRange with start key":
    backend.compactRange(some("startKey"))
    check backend.compactRangeCount == 1

  test "compactRange with start and end keys":
    backend.compactRange(some("startKey"), some("endKey"))
    check backend.compactRangeCount == 1
    check backend.statsData.compactions == 1

  test "compactRange updates stats":
    backend.compactRange()
    backend.compactRange()
    let stats = backend.getStats()
    check stats.compactions == 2

# =============================================================================
# Test Suites - Iterator Operations
# =============================================================================

suite "Storage Backend - Iterator Operations":
  var backend: EnhancedMockBackend

  setup:
    backend = newEnhancedMockBackend()
    discard backend.put("key1", "value1")
    discard backend.put("key2", "value2")
    discard backend.put("key3", "value3")
    discard backend.put("key4", "value4")
    discard backend.put("key5", "value5")

  teardown:
    backend.close()

  test "newIterator creates iterator":
    let iter = backend.newIterator()
    check iter != nil
    check backend.newIteratorCount == 1

  test "seekToFirst on populated backend":
    let iter = backend.newIterator()
    check iter.seekToFirst() == true
    check iter.valid() == true
    check iter.key() == "key1"

  test "seekToLast on populated backend":
    let iter = backend.newIterator()
    check iter.seekToLast() == true
    check iter.valid() == true
    check iter.key() == "key5"

  test "next advances iterator":
    let iter = backend.newIterator()
    discard iter.seekToFirst()
    check iter.next() == true
    check iter.key() == "key2"
    check iter.next() == true
    check iter.key() == "key3"

  test "prev moves iterator backward":
    let iter = backend.newIterator()
    discard iter.seekToLast()
    check iter.prev() == true
    check iter.key() == "key4"
    check iter.prev() == true
    check iter.key() == "key3"

  test "seek finds key":
    let iter = backend.newIterator()
    check iter.seek("key3") == true
    check iter.valid() == true
    check iter.key() == "key3"

  test "seek finds first key >= target":
    let iter = backend.newIterator()
    check iter.seek("key2.5") == true
    check iter.valid() == true
    check iter.key() == "key3"

  test "iterator value retrieval":
    let iter = backend.newIterator()
    discard iter.seekToFirst()
    check iter.value() == "value1"
    discard iter.next()
    check iter.value() == "value2"

  test "iterate all keys forward":
    let iter = backend.newIterator()
    discard iter.seekToFirst()
    var keys: seq[string] = @[]
    while iter.valid():
      keys.add(iter.key())
      discard iter.next()
    check keys == @["key1", "key2", "key3", "key4", "key5"]

  test "iterate all keys backward":
    let iter = backend.newIterator()
    discard iter.seekToLast()
    var keys: seq[string] = @[]
    while iter.valid():
      keys.add(iter.key())
      discard iter.prev()
    check keys == @["key5", "key4", "key3", "key2", "key1"]

  test "iterator on empty backend":
    backend.reset()
    let iter = backend.newIterator()
    check iter.seekToFirst() == false
    check iter.valid() == false
    check iter.seekToLast() == false

  test "iterator next returns false at end":
    let iter = backend.newIterator()
    discard iter.seekToLast()
    check iter.next() == false
    check iter.valid() == false

  test "iterator prev returns false at start":
    let iter = backend.newIterator()
    discard iter.seekToFirst()
    check iter.prev() == false
    check iter.valid() == false

  test "destroy iterator":
    let iter = backend.newIterator()
    discard iter.seekToFirst()
    iter.destroy()
    check iter.valid() == false

# =============================================================================
# Test Suites - Approximate Size
# =============================================================================

suite "Storage Backend - Approximate Size":
  var backend: EnhancedMockBackend

  setup:
    backend = newEnhancedMockBackend()

  teardown:
    backend.close()

  test "approximate size empty backend":
    let size = backend.approximateSize("a", "z")
    check size == 0
    check backend.approximateSizeCount == 1

  test "approximate size with data":
    discard backend.put("key1", "value10") # 7 chars
    discard backend.put("key2", "value10")
    discard backend.put("key3", "value10")

    let size = backend.approximateSize("key1", "key3")
    check size == 21 # 3 values * 7 chars = 21 bytes

  test "approximate size range filter":
    discard backend.put("a", "value1")
    discard backend.put("b", "value2")
    discard backend.put("c", "value3")
    discard backend.put("d", "value4")

    let size = backend.approximateSize("b", "c")
    check size == 12

  test "approximate size no matching keys":
    discard backend.put("key1", "value1")
    let size = backend.approximateSize("x", "z")
    check size == 0

# =============================================================================
# Test Suites - Open/Close/Destroy Operations
# =============================================================================

suite "Storage Backend - Open/Close/Destroy":
  var backend: EnhancedMockBackend

  setup:
    backend = newEnhancedMockBackend()

  test "backend starts open":
    check backend.isOpen() == true

  test "close marks backend as closed":
    backend.close()
    check backend.isOpen() == false
    check backend.closeCount == 1

  test "open after close":
    backend.close()
    check backend.isOpen() == false
    discard backend.open(defaultStorageConfig("/tmp/test"))
    check backend.isOpen() == true
    check backend.openCount == 1

  test "destroy clears all data":
    discard backend.put("key1", "value1")
    discard backend.put("key2", "value2")
    check backend.data.len == 2

    check backend.destroy() == true
    check backend.data.len == 0
    check backend.isOpenFlag == false
    check backend.destroyCount == 1

  test "multiple close calls":
    backend.close()
    backend.close()
    backend.close()
    check backend.closeCount == 3

# =============================================================================
# Test Suites - Error Injection
# =============================================================================

suite "Storage Backend - Error Injection":
  var backend: EnhancedMockBackend

  setup:
    backend = newEnhancedMockBackend()

  teardown:
    backend.close()

  test "force error on put":
    backend.setForceError(true, "put error")
    check backend.put("key", "value") == false

  test "force error on get":
    discard backend.put("key", "value")
    backend.setForceError(true, "get error")
    check backend.get("key").isNone

  test "force error on delete":
    discard backend.put("key", "value")
    backend.setForceError(true, "delete error")
    check backend.delete("key") == false

  test "force error on writeBatch":
    backend.setForceError(true, "batch error")
    check backend.writeBatch(@[(key: "key", value: "value")], @[]) == false

  test "force error on writeBatchNoSync":
    backend.setForceError(true, "batch error")
    check backend.writeBatchNoSync(@[(key: "key", value: "value")], @[]) == false

  test "force error on flush":
    backend.setForceError(true, "flush error")
    check backend.flush() == false

  test "force error on open":
    backend.close()
    backend.setForceError(true, "open error")
    check backend.open(defaultStorageConfig("/tmp/test")) == false

  test "clear error allows operations":
    backend.setForceError(true, "error")
    check backend.put("key", "value") == false

    backend.setForceError(false)
    check backend.put("key", "value") == true

  test "error does not affect stats accumulation":
    discard backend.put("key1", "value1")
    let initialWrites = backend.statsData.writes

    backend.setForceError(true)
    discard backend.put("key2", "value2")

    backend.setForceError(false)
    check backend.statsData.writes == initialWrites

# =============================================================================
# Test Suites - Operation Tracking
# =============================================================================

suite "Storage Backend - Operation Tracking":
  var backend: EnhancedMockBackend

  setup:
    backend = newEnhancedMockBackend()

  teardown:
    backend.close()

  test "put call count":
    for i in 0..<10:
      discard backend.put("key" & $i, "value")
    check backend.putCount == 10

  test "get call count":
    for i in 0..<10:
      discard backend.get("key" & $i)
    check backend.getCount == 10

  test "delete call count":
    for i in 0..<10:
      discard backend.delete("key" & $i)
    check backend.deleteCount == 10

  test "exists call count":
    for i in 0..<10:
      discard backend.exists("key" & $i)
    check backend.existsCount == 10

  test "writeBatch call count":
    for i in 0..<5:
      discard backend.writeBatch(@[(key: "key", value: "value")], @[])
    check backend.writeBatchCount == 5

  test "flush call count":
    for i in 0..<5:
      discard backend.flush()
    check backend.flushCount == 5

  test "compactRange call count":
    for i in 0..<5:
      backend.compactRange()
    check backend.compactRangeCount == 5

  test "newIterator call count":
    for i in 0..<5:
      discard backend.newIterator()
    check backend.newIteratorCount == 5

  test "reset clears all counts":
    discard backend.put("key", "value")
    discard backend.get("key")
    discard backend.delete("key")

    backend.reset()
    check backend.putCount == 0
    check backend.getCount == 0
    check backend.deleteCount == 0
    check backend.data.len == 0

# =============================================================================
# Test Suites - Edge Cases
# =============================================================================

suite "Storage Backend - Edge Cases":
  var backend: EnhancedMockBackend

  setup:
    backend = newEnhancedMockBackend()

  teardown:
    backend.close()

  test "empty key":
    check backend.put("", "empty_key_value") == true
    let result = backend.get("")
    check result.isSome
    check result.get == "empty_key_value"

  test "empty value":
    check backend.put("empty_value_key", "") == true
    let result = backend.get("empty_value_key")
    check result.isSome
    check result.get == ""

  test "empty key and empty value":
    check backend.put("", "") == true
    let result = backend.get("")
    check result.isSome
    check result.get == ""

  test "large key":
    let largeKey = "k".repeat(10000)
    check backend.put(largeKey, "value") == true
    let result = backend.get(largeKey)
    check result.isSome
    check result.get == "value"

  test "large value":
    let largeValue = "v".repeat(100000)
    check backend.put("key", largeValue) == true
    let result = backend.get("key")
    check result.isSome
    check result.get.len == 100000

  test "very large key and value":
    let largeKey = "k".repeat(50000)
    let largeValue = "v".repeat(50000)
    check backend.put(largeKey, largeValue) == true
    let result = backend.get(largeKey)
    check result.isSome
    check result.get.len == 50000

  test "binary data in key with null bytes":
    let binaryKey = "\x00\x01\x02\x03\x04"
    check backend.put(binaryKey, "value") == true
    check backend.get(binaryKey).isSome

  test "binary data in value with null bytes":
    let binaryValue = "\x00\x01\x02\x03\x04"
    check backend.put("key", binaryValue) == true
    let result = backend.get("key")
    check result.isSome
    check result.get == binaryValue

  test "unicode key":
    let unicodeKey = "日本語キー"
    check backend.put(unicodeKey, "value") == true
    check backend.get(unicodeKey).isSome

  test "unicode value":
    let unicodeValue = "日本語値値値"
    check backend.put("key", unicodeValue) == true
    check backend.get("key").get == unicodeValue

  test "special characters in key":
    let specialKey = "!@#$%^&*()_+-=[]{}|;':\",./<>?"
    check backend.put(specialKey, "value") == true
    check backend.get(specialKey).isSome

  test "key with newlines":
    let newlineKey = "key\nwith\nnewlines"
    check backend.put(newlineKey, "value") == true
    check backend.get(newlineKey).isSome

  test "value with newlines":
    let newlineValue = "value\nwith\nnewlines\n"
    check backend.put("key", newlineValue) == true
    check backend.get("key").get == newlineValue

  test "many keys sorted correctly":
    for i in 0..<1000:
      discard backend.put("key" & $i, "value" & $i)
    check backend.sortedKeys.len == 1000

    let iter = backend.newIterator()
    discard iter.seekToFirst()
    var prevKey = ""
    while iter.valid():
      let currKey = iter.key()
      check currKey > prevKey
      prevKey = currKey
      discard iter.next()

  test "delete and reinsert same key":
    discard backend.put("key", "value1")
    discard backend.delete("key")
    check backend.get("key").isNone

    discard backend.put("key", "value2")
    check backend.get("key").isSome
    check backend.get("key").get == "value2"

  test "writeBatch with duplicate keys in puts":
    let pairs: seq[KeyValuePair] = @[
      (key: "key", value: "value1"),
      (key: "key", value: "value2"),
      (key: "key", value: "value3")
    ]
    discard backend.writeBatch(pairs, @[])
    check backend.data.len == 1
    check backend.get("key").get == "value3"

# =============================================================================
# Test Suites - DI Mock Backend (from di/mocks.nim)
# =============================================================================

suite "Storage Backend - DI Mock Backend":
  var backend: diMocks.MockBackend

  setup:
    backend = diMocks.newMockBackend()

  test "put and get":
    check backend.put("key1", "value1") == true
    let result = backend.get("key1")
    check result.isSome
    check result.get() == "value1"

  test "get non-existent key":
    let result = backend.get("nonexistent")
    check result.isNone

  test "delete existing key":
    discard backend.put("key1", "value1")
    check backend.delete("key1") == true
    check backend.get("key1").isNone

  test "delete non-existent key":
    check backend.delete("nonexistent") == true

  test "scan with prefix":
    discard backend.put("key_a", "value_a")
    discard backend.put("key_b", "value_b")
    discard backend.put("key_c", "value_c")
    discard backend.put("other_key", "other_value")

    let results = backend.scan("key_", 10)
    check results.len == 3

  test "scan with limit":
    discard backend.put("key_1", "value1")
    discard backend.put("key_2", "value2")
    discard backend.put("key_3", "value3")
    discard backend.put("key_4", "value4")
    discard backend.put("key_5", "value5")

    let results = backend.scan("key_", 3)
    check results.len == 3

  test "flush":
    check backend.flush() == true
    check backend.flushCallCount == 1

  test "compact":
    check backend.compact() == true
    check backend.compactCallCount == 1

  test "close":
    backend.close()
    check backend.closed == true

  test "stats":
    discard backend.put("key1", "value1")
    discard backend.get("key1")
    discard backend.get("key2")

    let statsTable = backend.stats()
    check statsTable.hasKey("put_count") == true
    check statsTable["put_count"] == 1
    check statsTable["get_count"] == 2

  test "reset":
    discard backend.put("key1", "value1")
    discard backend.put("key2", "value2")
    backend.reset()
    check backend.data.len == 0
    check backend.getCallCount == 0
    check backend.putCallCount == 0

# =============================================================================
# Test Suites - Storage Backend Base Class
# =============================================================================

suite "Storage Backend - Base Class Methods":
  test "storage backend base default implementations":
    let baseBackend = StorageBackend()
    check baseBackend.isOpen() == false
    check baseBackend.get("key").isNone
    check baseBackend.put("key", "value") == false
    check baseBackend.delete("key") == false
    check baseBackend.exists("key") == false

  test "storage iterator base default implementations":
    let baseIter = StorageIterator(backend: nil)
    check baseIter.valid() == false
    check baseIter.key() == ""
    check baseIter.value() == ""

  test "base iterator methods return false":
    let baseIter = StorageIterator(backend: nil)
    check baseIter.seekToFirst() == false
    check baseIter.seekToLast() == false
    check baseIter.seek("key") == false
    check baseIter.next() == false
    check baseIter.prev() == false

  test "base writeBatch returns false":
    let baseBackend = StorageBackend()
    check baseBackend.writeBatch(@[(key: "k", value: "v")], @[]) == false

  test "base writeBatchNoSync returns false":
    let baseBackend = StorageBackend()
    check baseBackend.writeBatchNoSync(@[(key: "k", value: "v")], @[]) == false

  test "base flush returns false":
    let baseBackend = StorageBackend()
    check baseBackend.flush() == false

  test "base destroy returns false":
    let baseBackend = StorageBackend()
    check baseBackend.destroy() == false

  test "base approximateSize returns zero":
    let baseBackend = StorageBackend()
    check baseBackend.approximateSize("a", "z") == 0

  test "base getStats returns empty stats":
    let baseBackend = StorageBackend()
    let stats = baseBackend.getStats()
    check stats.reads == 0
    check stats.writes == 0

# =============================================================================
# Test Suites - Concurrent Operations
# =============================================================================

type
  ConcurrentTestData = object
    backend: EnhancedMockBackend
    writeCount: Atomic[int]
    readCount: Atomic[int]
    deleteCount: Atomic[int]
    errors: Atomic[int]

proc concurrentWriter(data: ptr ConcurrentTestData) {.thread.} =
  for i in 0..<100:
    let key = "thread_key_" & $i
    if data.backend.put(key, "value_" & $i):
      atomicInc data.writeCount
    else:
      atomicInc data.errors

proc concurrentReader(data: ptr ConcurrentTestData) {.thread.} =
  for i in 0..<100:
    let key = "key_" & $i # Match the key prefix used in test setup
    if data.backend.get(key).isSome:
      atomicInc data.readCount

proc concurrentDeleter(data: ptr ConcurrentTestData) {.thread.} =
  for i in 0..<50:
    let key = "thread_key_" & $i
    if data.backend.delete(key):
      atomicInc data.deleteCount

suite "Storage Backend - Concurrent Operations":
  test "concurrent writes are thread-safe":
    var backend = newEnhancedMockBackend()
    var data: ConcurrentTestData
    data.backend = backend
    data.writeCount.store(0)
    data.errors.store(0)

    var threads: array[4, Thread[ptr ConcurrentTestData]]
    for i in 0..<4:
      createThread(threads[i], concurrentWriter, addr data)

    joinThreads(threads)

    check data.writeCount.load() > 0
    check data.errors.load() == 0
    backend.close()

  test "concurrent reads are thread-safe":
    var backend = newEnhancedMockBackend()

    for i in 0..<100:
      discard backend.put("key_" & $i, "value_" & $i)

    var data: ConcurrentTestData
    data.backend = backend
    data.readCount.store(0)

    var threads: array[4, Thread[ptr ConcurrentTestData]]
    for i in 0..<4:
      createThread(threads[i], concurrentReader, addr data)

    joinThreads(threads)

    check data.readCount.load() > 0
    backend.close()

  test "concurrent mixed operations are thread-safe":
    var backend = newEnhancedMockBackend()

    for i in 0..<100:
      discard backend.put("thread_key_" & $i, "initial_" & $i)

    var data: ConcurrentTestData
    data.backend = backend
    data.writeCount.store(0)
    data.readCount.store(0)
    data.deleteCount.store(0)
    data.errors.store(0)

    var threads: array[6, Thread[ptr ConcurrentTestData]]
    createThread(threads[0], concurrentWriter, addr data)
    createThread(threads[1], concurrentWriter, addr data)
    createThread(threads[2], concurrentReader, addr data)
    createThread(threads[3], concurrentReader, addr data)
    createThread(threads[4], concurrentDeleter, addr data)
    createThread(threads[5], concurrentDeleter, addr data)

    joinThreads(threads)

    check data.writeCount.load() + data.readCount.load() +
        data.deleteCount.load() > 0
    backend.close()

  test "concurrent stats access":
    var backend = newEnhancedMockBackend()

    for i in 0..<50:
      discard backend.put("key_" & $i, "value_" & $i)

    var statsCount: Atomic[int]
    statsCount.store(0)

    proc statsWorker(b: EnhancedMockBackend) {.thread.} =
      for i in 0..<10:
        let stats = b.getStats()
        if stats.writes >= 0:
          discard

    var threads: array[4, Thread[EnhancedMockBackend]]
    for i in 0..<4:
      createThread(threads[i], statsWorker, backend)

    joinThreads(threads)

    backend.close()

# =============================================================================
# Test Suites - Stress Tests
# =============================================================================

suite "Storage Backend - Stress Tests":
  var backend: EnhancedMockBackend

  setup:
    backend = newEnhancedMockBackend()

  teardown:
    backend.close()

  test "many sequential puts":
    for i in 0..<10000:
      discard backend.put("stress_key_" & $i, "stress_value_" & $i)
    check backend.data.len == 10000

  test "many sequential gets":
    for i in 0..<1000:
      discard backend.put("key_" & $i, "value_" & $i)

    for i in 0..<10000:
      discard backend.get("key_" & $(i mod 1000))
    check backend.getCount == 10000

  test "rapid put-delete cycle":
    for cycle in 0..<100:
      for i in 0..<100:
        discard backend.put("cycle_" & $cycle & "_key_" & $i, "value")
      for i in 0..<100:
        discard backend.delete("cycle_" & $cycle & "_key_" & $i)

    check backend.data.len == 0
    check backend.putCount == 10000
    check backend.deleteCount == 10000

  test "many writeBatch operations":
    for batch in 0..<100:
      var pairs: seq[KeyValuePair] = @[]
      for i in 0..<10:
        pairs.add((key: "batch_" & $batch & "_" & $i, value: "value"))
      discard backend.writeBatch(pairs, @[])

    check backend.data.len == 1000

  test "iterator traversal of large dataset":
    for i in 0..<5000:
      discard backend.put("large_key_" & $i, "value_" & $i)

    let iter = backend.newIterator()
    discard iter.seekToFirst()
    var count = 0
    while iter.valid():
      count += 1
      discard iter.next()

    check count == 5000
    iter.destroy()

# =============================================================================
# Test Suites - Stats Collection
# =============================================================================

suite "Storage Backend - Stats Collection":
  var backend: EnhancedMockBackend

  setup:
    backend = newEnhancedMockBackend()

  teardown:
    backend.close()

  test "stats track reads":
    discard backend.put("key1", "value10")
    discard backend.get("key1")
    discard backend.get("key1")
    discard backend.get("nonexistent")

    let stats = backend.getStats()
    check stats.reads == 3

  test "stats track writes":
    discard backend.put("key1", "value")
    discard backend.put("key2", "value")
    discard backend.put("key1", "newvalue")

    let stats = backend.getStats()
    check stats.writes == 3

  test "stats track bytes read":
    discard backend.put("key1", "12345")
    discard backend.put("key2", "67890")

    discard backend.get("key1")
    discard backend.get("key2")

    let stats = backend.getStats()
    check stats.bytesRead == 10

  test "stats track bytes written":
    discard backend.put("key1", "value10") # 7 chars = 7 bytes
    discard backend.put("key2", "value10") # 7 chars = 7 bytes

    let stats = backend.getStats()
    check stats.bytesWritten == 14 # 7 + 7 = 14 bytes

  test "stats track cache misses":
    discard backend.get("nonexistent")
    discard backend.get("another_nonexistent")

    let stats = backend.getStats()
    check stats.cacheMisses == 2

  test "stats track compactions":
    backend.compactRange()
    backend.compactRange()
    backend.compactRange()

    let stats = backend.getStats()
    check stats.compactions == 3

  test "stats persist across reset":
    discard backend.put("key", "value")
    let statsBefore = backend.getStats()

    backend.reset()
    let statsAfter = backend.getStats()
    check statsAfter.writes == 0

  test "writeBatch updates stats":
    discard backend.writeBatch(@[
      (key: "k1", value: "v1"),
      (key: "k2", value: "v2")
    ], @[])

    let stats = backend.getStats()
    check stats.writes == 2
    check stats.bytesWritten == 4

  test "writeBatchNoSync does not update stats":
    discard backend.writeBatchNoSync(@[
      (key: "k1", value: "v1"),
      (key: "k2", value: "v2")
    ], @[])

    let stats = backend.getStats()
    check stats.writes == 0
