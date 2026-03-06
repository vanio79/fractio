# Integration tests for MVCC Transactions
# Tests the full transaction lifecycle with MVCC storage

import unittest
import std/[times, options, strutils, os]
import fractio/core/types
import fractio/core/transaction
import fractio/core/timestamp_provider
import fractio/core/transaction_manager
import fractio/core/conflict_detection
import fractio/storage/backend
import fractio/storage/mvcc/types
import fractio/storage/mvcc/engine
import fractio/storage/mvcc/garbage_collector
import fractio/storage/wisckey_backend

# Constants
const
  INVALID_TIMESTAMP* = Timestamp(0)
  MAX_TIMESTAMP* = high(Timestamp)
  DEFAULT_PRIORITY* = 500
  DEFAULT_MAX_OFFSET_NS* = 100_000_000
  DEFAULT_MAX_RETRIES* = 15

suite "MVCC Transactions - Single Node":
  test "transaction begin and commit":
    # Create a temporary directory for the test
    let testPath = "/tmp/test_mvcc_single_node"
    removeDir(testPath)
    createDir(testPath)

    try:
      # Create storage backend
      let backend = newWiscKeyBackend(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      check backend.open(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      # Create timestamp provider (mock)
      let tsProvider = TimestampProvider(
        timer: nil,
        lastTimestamp: 1000,
        lastCounter: 0,
        maxOffset: DEFAULT_MAX_OFFSET_NS,
        nodeId: 0
      )

      # Create MVCC engine
      let engine = newMVCCEngine(backend, tsProvider)

      # Create transaction manager
      let tm = newTransactionManager(tsProvider, engine)

      # Begin transaction
      let txn = tm.beginTransaction()

      check txn != nil
      check txn.isPending()
      check txn.startTimestamp > INVALID_TIMESTAMP

      # Write some data
      let writeResult = engine.mvccPut("key1", "value1", txn)
      check writeResult.success

      # Commit transaction
      let commitResult = tm.commitTransaction(txn)

      check commitResult.success
      check txn.isCommitted()
      check commitResult.commitTimestamp > INVALID_TIMESTAMP

      # Verify data was written
      let readResult = engine.mvccGet("key1", commitResult.commitTimestamp)
      check readResult.success
      check readResult.value.isSome
      check readResult.value.get().data == "value1"

      # Cleanup
      discard backend.close()
      discard backend.destroy()

    finally:
      # Cleanup test directory
      removeDir(testPath)

  test "transaction begin and abort":
    let testPath = "/tmp/test_mvcc_abort"
    removeDir(testPath)
    createDir(testPath)

    try:
      let backend = newWiscKeyBackend(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      check backend.open(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      let tsProvider = TimestampProvider(
        timer: nil,
        lastTimestamp: 1000,
        lastCounter: 0,
        maxOffset: DEFAULT_MAX_OFFSET_NS,
        nodeId: 0
      )

      let engine = newMVCCEngine(backend, tsProvider)
      let tm = newTransactionManager(tsProvider, engine)

      # Begin transaction
      let txn = tm.beginTransaction()

      # Write some data
      let writeResult = engine.mvccPut("key1", "value1", txn)
      check writeResult.success

      # Abort transaction
      tm.abortTransaction(txn)

      check txn.isAborted()

      # Verify data was NOT written
      let readResult = engine.mvccGet("key1", Timestamp(10000))
      check not readResult.success

      # Cleanup
      discard backend.close()
      discard backend.destroy()

    finally:
      removeDir(testPath)

  test "multiple transactions read committed data":
    let testPath = "/tmp/test_mvcc_multiple_txns"
    removeDir(testPath)
    createDir(testPath)

    try:
      let backend = newWiscKeyBackend(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      check backend.open(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      let tsProvider = TimestampProvider(
        timer: nil,
        lastTimestamp: 1000,
        lastCounter: 0,
        maxOffset: DEFAULT_MAX_OFFSET_NS,
        nodeId: 0
      )

      let engine = newMVCCEngine(backend, tsProvider)
      let tm = newTransactionManager(tsProvider, engine)

      # First transaction writes data
      let txn1 = tm.beginTransaction()
      let writeResult = engine.mvccPut(txn1, "key1", "value1")
      check writeResult.success
      let commitResult1 = tm.commitTransaction(txn1)
      check commitResult1.success

      # Second transaction reads committed data
      let txn2 = tm.beginTransaction()
      let readResult = engine.mvccGet("key1", txn2.startTimestamp)
      check readResult.success
      check readResult.value.isSome
      check readResult.value.get().data == "value1"

      let commitResult2 = tm.commitTransaction(txn2)
      check commitResult2.success

      # Cleanup
      discard backend.close()
      discard backend.destroy()

    finally:
      removeDir(testPath)

  test "transaction sees its own writes":
    let testPath = "/tmp/test_mvcc_own_writes"
    removeDir(testPath)
    createDir(testPath)

    try:
      let backend = newWiscKeyBackend(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      check backend.open(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      let tsProvider = TimestampProvider(
        timer: nil,
        lastTimestamp: 1000,
        lastCounter: 0,
        maxOffset: DEFAULT_MAX_OFFSET_NS,
        nodeId: 0
      )

      let engine = newMVCCEngine(backend, tsProvider)
      let tm = newTransactionManager(tsProvider, engine)

      # Begin transaction
      let txn = tm.beginTransaction()

      # Write data
      let writeResult = engine.mvccPut("key1", "value1", txn)
      check writeResult.success

      # Read own write
      let readResult = engine.mvccGet("key1", txn.startTimestamp)
      check readResult.success
      check readResult.value.isSome
      check readResult.value.get().data == "value1"

      # Commit
      let commitResult = tm.commitTransaction(txn)
      check commitResult.success

      # Cleanup
      discard backend.close()
      discard backend.destroy()

    finally:
      removeDir(testPath)

  test "transaction with multiple writes":
    let testPath = "/tmp/test_mvcc_multiple_writes"
    removeDir(testPath)
    createDir(testPath)

    try:
      let backend = newWiscKeyBackend(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      check backend.open(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      let tsProvider = TimestampProvider(
        timer: nil,
        lastTimestamp: 1000,
        lastCounter: 0,
        maxOffset: DEFAULT_MAX_OFFSET_NS,
        nodeId: 0
      )

      let engine = newMVCCEngine(backend, tsProvider)
      let tm = newTransactionManager(tsProvider, engine)

      # Begin transaction
      let txn = tm.beginTransaction()

      # Write multiple keys
      check engine.mvccPut("key1", "value1", txn).success
      check engine.mvccPut("key2", "value2", txn).success
      check engine.mvccPut("key3", "value3", txn).success

      # Commit
      let commitResult = tm.commitTransaction(txn)
      check commitResult.success

      # Verify all writes
      check engine.mvccGet("key1", commitResult.commitTimestamp).value.get().data == "value1"
      check engine.mvccGet("key2", commitResult.commitTimestamp).value.get().data == "value2"
      check engine.mvccGet("key3", commitResult.commitTimestamp).value.get().data == "value3"

      # Cleanup
      discard backend.close()
      discard backend.destroy()

    finally:
      removeDir(testPath)

  test "transaction with delete":
    let testPath = "/tmp/test_mvcc_delete"
    removeDir(testPath)
    createDir(testPath)

    try:
      let backend = newWiscKeyBackend(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      check backend.open(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      let tsProvider = TimestampProvider(
        timer: nil,
        lastTimestamp: 1000,
        lastCounter: 0,
        maxOffset: DEFAULT_MAX_OFFSET_NS,
        nodeId: 0
      )

      let engine = newMVCCEngine(backend, tsProvider)
      let tm = newTransactionManager(tsProvider, engine)

      # First transaction writes data
      let txn1 = tm.beginTransaction()
      check engine.mvccPut(txn1, "key1", "value1").success
      let commitResult1 = tm.commitTransaction(txn1)
      check commitResult1.success

      # Verify data exists
      check engine.mvccGet("key1", commitResult1.commitTimestamp).success

      # Second transaction deletes data
      let txn2 = tm.beginTransaction()
      check engine.mvccDelete(txn2, "key1").success
      let commitResult2 = tm.commitTransaction(txn2)
      check commitResult2.success

      # Verify data is deleted
      let readResult = engine.mvccGet("key1", commitResult2.commitTimestamp)
      check not readResult.success

      # Cleanup
      discard backend.close()
      discard backend.destroy()

    finally:
      removeDir(testPath)

suite "MVCC Transactions - Write-Write Conflicts":
  test "concurrent transactions writing same key":
    let testPath = "/tmp/test_mvcc_ww_conflict"
    removeDir(testPath)
    createDir(testPath)

    try:
      let backend = newWiscKeyBackend(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      check backend.open(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      let tsProvider = TimestampProvider(
        timer: nil,
        lastTimestamp: 1000,
        lastCounter: 0,
        maxOffset: DEFAULT_MAX_OFFSET_NS,
        nodeId: 0
      )

      let engine = newMVCCEngine(backend, tsProvider)
      let tm = newTransactionManager(tsProvider, engine)

      # First transaction writes key
      let txn1 = tm.beginTransaction()
      check engine.mvccPut(txn1, "key1", "value1").success

      # Second transaction tries to write same key
      let txn2 = tm.beginTransaction()
      check engine.mvccPut(txn2, "key1", "value2").success

      # First transaction commits
      let commitResult1 = tm.commitTransaction(txn1)
      check commitResult1.success

      # Second transaction should detect conflict and abort
      let commitResult2 = tm.commitTransaction(txn2)
      check not commitResult2.success
      check commitResult2.error.retryable

      # Verify first transaction's value is committed
      let readResult = engine.mvccGet("key1", commitResult1.commitTimestamp)
      check readResult.success
      check readResult.value.get().data == "value1"

      # Cleanup
      discard backend.close()
      discard backend.destroy()

    finally:
      removeDir(testPath)

suite "MVCC Transactions - Serializable Isolation":
  test "serializable isolation prevents write skew":
    let testPath = "/tmp/test_mvcc_serializable"
    removeDir(testPath)
    createDir(testPath)

    try:
      let backend = newWiscKeyBackend(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      check backend.open(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      let tsProvider = TimestampProvider(
        timer: nil,
        lastTimestamp: 1000,
        lastCounter: 0,
        maxOffset: DEFAULT_MAX_OFFSET_NS,
        nodeId: 0
      )

      let engine = newMVCCEngine(backend, tsProvider)
      let tm = newTransactionManager(tsProvider, engine)

      # Initial state: key1=10, key2=20
      let txnInit = tm.beginTransaction()
      check engine.mvccPut(txnInit, "key1", "10").success
      check engine.mvccPut(txnInit, "key2", "20").success
      let commitResultInit = tm.commitTransaction(txnInit)
      check commitResultInit.success

      # Transaction 1: reads key1, wants to write key2
      let txn1 = tm.beginTransaction()
      let readResult1 = engine.mvccGet("key1", txn1.startTimestamp)
      check readResult1.success
      check readResult1.value.get().data == "10"

      # Transaction 2: reads key2, wants to write key1
      let txn2 = tm.beginTransaction()
      let readResult2 = engine.mvccGet("key2", txn2.startTimestamp)
      check readResult2.success
      check readResult2.value.get().data == "20"

      # Transaction 1 writes key2
      check engine.mvccPut(txn1, "key2", "30").success

      # Transaction 2 writes key1
      check engine.mvccPut(txn2, "key1", "15").success

      # Commit transaction 1
      let commitResult1 = tm.commitTransaction(txn1)
      check commitResult1.success

      # Transaction 2 should detect conflict (serializable isolation)
      let commitResult2 = tm.commitTransaction(txn2)
      check not commitResult2.success
      check commitResult2.error.retryable

      # Verify serializable result: key1=10, key2=30
      let finalRead1 = engine.mvccGet("key1", commitResult1.commitTimestamp)
      let finalRead2 = engine.mvccGet("key2", commitResult1.commitTimestamp)
      check finalRead1.value.get().data == "10"
      check finalRead2.value.get().data == "30"

      # Cleanup
      discard backend.close()
      discard backend.destroy()

    finally:
      removeDir(testPath)

suite "MVCC Transactions - Rollback Behavior":
  test "rollback removes intents":
    let testPath = "/tmp/test_mvcc_rollback_intents"
    removeDir(testPath)
    createDir(testPath)

    try:
      let backend = newWiscKeyBackend(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      check backend.open(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      let tsProvider = TimestampProvider(
        timer: nil,
        lastTimestamp: 1000,
        lastCounter: 0,
        maxOffset: DEFAULT_MAX_OFFSET_NS,
        nodeId: 0
      )

      let engine = newMVCCEngine(backend, tsProvider)
      let tm = newTransactionManager(tsProvider, engine)

      # Begin transaction
      let txn = tm.beginTransaction()

      # Write data (creates intent)
      check engine.mvccPut("key1", "value1", txn).success

      # Check intent exists
      check engine.hasIntent("key1")

      # Abort transaction
      tm.abortTransaction(txn)

      # Verify intent was removed
      check not engine.hasIntent("key1")

      # Verify data was not committed
      let readResult = engine.mvccGet("key1", Timestamp(10000))
      check not readResult.success

      # Cleanup
      discard backend.close()
      discard backend.destroy()

    finally:
      removeDir(testPath)

  test "rollback after partial writes":
    let testPath = "/tmp/test_mvcc_rollback_partial"
    removeDir(testPath)
    createDir(testPath)

    try:
      let backend = newWiscKeyBackend(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      check backend.open(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      let tsProvider = TimestampProvider(
        timer: nil,
        lastTimestamp: 1000,
        lastCounter: 0,
        maxOffset: DEFAULT_MAX_OFFSET_NS,
        nodeId: 0
      )

      let engine = newMVCCEngine(backend, tsProvider)
      let tm = newTransactionManager(tsProvider, engine)

      # Begin transaction
      let txn = tm.beginTransaction()

      # Write multiple keys
      check engine.mvccPut("key1", "value1", txn).success
      check engine.mvccPut("key2", "value2", txn).success
      check engine.mvccPut("key3", "value3", txn).success

      # Abort transaction
      tm.abortTransaction(txn)

      # Verify none of the data was committed
      check not engine.mvccGet("key1", Timestamp(10000)).success
      check not engine.mvccGet("key2", Timestamp(10000)).success
      check not engine.mvccGet("key3", Timestamp(10000)).success

      # Cleanup
      discard backend.close()
      discard backend.destroy()

    finally:
      removeDir(testPath)

suite "MVCC Transactions - Garbage Collection":
  test "GC collects old versions":
    let testPath = "/tmp/test_mvcc_gc"
    removeDir(testPath)
    createDir(testPath)

    try:
      let backend = newWiscKeyBackend(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      check backend.open(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      let tsProvider = TimestampProvider(
        timer: nil,
        lastTimestamp: 1000,
        lastCounter: 0,
        maxOffset: DEFAULT_MAX_OFFSET_NS,
        nodeId: 0
      )

      let engine = newMVCCEngine(backend, tsProvider)
      let tm = newTransactionManager(tsProvider, engine)

      # Write multiple versions of the same key
      for i in 0 ..< 15:
        let txn = tm.beginTransaction()
        check engine.mvccPut("key1", "value" & $i, txn).success
        check tm.commitTransaction(txn).success

      # Get all versions
      let versions = engine.getAllVersions("key1")
      check versions.len > 10 # Should have more than max versions

      # Create GC with maxVersionsPerKey = 10
      let gc = newGarbageCollector(engine, GCPolicy(
        minTimestamp: Timestamp(0),
        maxVersionsPerKey: 10,
        maxAgeMs: 60000,
        collectIntervalMs: 60000
      ))

      # Run GC
      let gcResult = gc.collectVersionsForKey("key1")
      check gcResult.success
      check gcResult.versionsCollected > 0

      # Verify only 10 versions remain
      let versionsAfterGC = engine.getAllVersions("key1")
      check versionsAfterGC.len == 10

      # Cleanup
      discard backend.close()
      discard backend.destroy()

    finally:
      removeDir(testPath)

  test "GC collects transaction intents after abort":
    let testPath = "/tmp/test_mvcc_gc_intents"
    removeDir(testPath)
    createDir(testPath)

    try:
      let backend = newWiscKeyBackend(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      check backend.open(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      let tsProvider = TimestampProvider(
        timer: nil,
        lastTimestamp: 1000,
        lastCounter: 0,
        maxOffset: DEFAULT_MAX_OFFSET_NS,
        nodeId: 0
      )

      let engine = newMVCCEngine(backend, tsProvider)
      let tm = newTransactionManager(tsProvider, engine)

      # Begin transaction and write data
      let txn = tm.beginTransaction()
      check engine.mvccPut("key1", "value1", txn).success

      # Verify intent exists
      check engine.hasIntent("key1")

      # Abort transaction (should remove intent)
      tm.abortTransaction(txn)

      # Verify intent was removed
      check not engine.hasIntent("key1")

      # Cleanup
      discard backend.close()
      discard backend.destroy()

    finally:
      removeDir(testPath)

suite "MVCC Transactions - Performance":
  test "simple transaction throughput":
    let testPath = "/tmp/test_mvcc_performance"
    removeDir(testPath)
    createDir(testPath)

    try:
      let backend = newWiscKeyBackend(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      check backend.open(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      let tsProvider = TimestampProvider(
        timer: nil,
        lastTimestamp: 1000,
        lastCounter: 0,
        maxOffset: DEFAULT_MAX_OFFSET_NS,
        nodeId: 0
      )

      let engine = newMVCCEngine(backend, tsProvider)
      let tm = newTransactionManager(tsProvider, engine)

      # Run 1000 transactions
      let numTxns = 1000
      let startTime = epochTime()

      for i in 0 ..< numTxns:
        let txn = tm.beginTransaction()
        discard engine.mvccPut("key" & $i, "value" & $i, txn)
        check tm.commitTransaction(txn).success

      let endTime = epochTime()
      let duration = endTime - startTime
      let throughput = numTxns.float / duration

      echo "Throughput: " & $throughput & " txns/sec"

      # Should be at least 1000 txns/sec on modern hardware
      check throughput > 1000.0

      # Cleanup
      discard backend.close()
      discard backend.destroy()

    finally:
      removeDir(testPath)
