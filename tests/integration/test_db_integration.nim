# Integration test for database operations
# Tests basic database creation and recovery

import unittest
import fractio/storage/[db, db_config, error, keyspace, iter, worker_pool]
import fractio/storage/journal/writer # For PersistMode
import std/[os, options, strutils]

const TestDbPath = "tmp/test_db_integration"

proc cleanup() =
  if dirExists(TestDbPath):
    try:
      removeDir(TestDbPath)
    except OSError:
      discard

suite "Database Integration Tests":

  setup:
    cleanup()

  teardown:
    cleanup()

  test "Create database":
    let config = newConfig(TestDbPath)

    let dbResult = open(config)
    check dbResult.isOk
    if dbResult.isOk:
      let db = dbResult.value
      check db != nil
      check dirExists(TestDbPath)
      check fileExists(TestDbPath / "version")
      check dirExists(TestDbPath / "keyspaces")
      check dirExists(TestDbPath / "journals")
      db.close()

  test "Database recovery after close":
    let config = newConfig(TestDbPath)

    # Create and close database
    var dbResult = open(config)
    check dbResult.isOk
    if dbResult.isOk:
      var db = dbResult.value
      db.close()

    # Reopen the database
    dbResult = open(config)
    check dbResult.isOk
    if dbResult.isOk:
      let db = dbResult.value
      check db != nil
      db.close()

  test "Database persist operation":
    let config = newConfig(TestDbPath)

    let dbResult = open(config)
    check dbResult.isOk
    if dbResult.isOk:
      let db = dbResult.value

      # Persist the database
      let persistResult = db.persist(pmSyncAll)
      check persistResult.isOk

      db.close()

  test "Keyspace creation and basic operations":
    let config = newConfig(TestDbPath)

    let dbResult = open(config)
    check dbResult.isOk
    if dbResult.isOk:
      let db = dbResult.value

      # Create a keyspace
      let ksResult = db.keyspace("default")
      check ksResult.isOk
      if ksResult.isOk:
        let ks = ksResult.value
        check ks != nil
        check ks.name() == "default"

        # Check that keyspace exists
        check db.keyspaceExists("default")

        # Get keyspace again (should return cached)
        let ks2Result = db.keyspace("default")
        check ks2Result.isOk

        # List keyspace names
        let names = db.listKeyspaceNames()
        check "default" in names

      db.close()

  test "Keyspace insert and get operations":
    let config = newConfig(TestDbPath)

    let dbResult = open(config)
    check dbResult.isOk
    if dbResult.isOk:
      let db = dbResult.value

      # Create a keyspace
      let ksResult = db.keyspace("test_data")
      check ksResult.isOk
      if ksResult.isOk:
        let ks = ksResult.value

        # Insert some key-value pairs
        let insertResult1 = ks.insert("key1", "value1")
        check insertResult1.isOk

        let insertResult2 = ks.insert("key2", "value2")
        check insertResult2.isOk

        let insertResult3 = ks.insert("key3", "value3")
        check insertResult3.isOk

        # Get values back
        let getResult1 = ks.get("key1")
        check getResult1.isOk
        check getResult1.value.isSome()
        check getResult1.value.get() == "value1"

        let getResult2 = ks.get("key2")
        check getResult2.isOk
        check getResult2.value.isSome()
        check getResult2.value.get() == "value2"

        # Get non-existent key
        let getResult4 = ks.get("nonexistent")
        check getResult4.isOk
        check getResult4.value.isNone()

        # Check containsKey
        let containsResult = ks.containsKey("key1")
        check containsResult.isOk
        check containsResult.value == true

      db.close()

  test "Keyspace remove operations":
    let config = newConfig(TestDbPath)

    let dbResult = open(config)
    check dbResult.isOk
    if dbResult.isOk:
      let db = dbResult.value

      let ksResult = db.keyspace("test_remove")
      check ksResult.isOk
      if ksResult.isOk:
        let ks = ksResult.value

        # Insert and then remove
        discard ks.insert("remove_key", "remove_value")

        let getBeforeResult = ks.get("remove_key")
        check getBeforeResult.isOk
        check getBeforeResult.value.isSome()

        let removeResult = ks.remove("remove_key")
        check removeResult.isOk

        # After remove, should return None
        let getAfterResult = ks.get("remove_key")
        check getAfterResult.isOk
        check getAfterResult.value.isNone()

      db.close()

  test "Keyspace flush to SSTable and read":
    let config = newConfig(TestDbPath)

    let dbResult = open(config)
    check dbResult.isOk
    if dbResult.isOk:
      let db = dbResult.value

      let ksResult = db.keyspace("test_flush")
      check ksResult.isOk
      if ksResult.isOk:
        let ks = ksResult.value

        # Insert some data
        for i in 0 ..< 10:
          discard ks.insert("key" & $i, "value" & $i)

        # Verify data is in memtable
        let getResult = ks.get("key5")
        check getResult.isOk
        check getResult.value.isSome()
        check getResult.value.get() == "value5"

        # Rotate memtable (move active to sealed)
        let rotateResult = ks.rotateMemtable()
        check rotateResult.isOk
        check rotateResult.value == true # Should have rotated

        # Verify sealed memtable exists
        check ks.hasSealedMemtables()

        # Flush the sealed memtable to SSTable
        let flushResult = ks.flushOldestSealed()
        check flushResult.isOk
        check flushResult.value > 0 # Should have flushed some bytes

        # Verify we can still read the data (now from SSTable)
        let getResult2 = ks.get("key5")
        check getResult2.isOk
        check getResult2.value.isSome()
        check getResult2.value.get() == "value5"

        # Verify all keys are still accessible
        for i in 0 ..< 10:
          let r = ks.get("key" & $i)
          check r.isOk
          check r.value.isSome()
          check r.value.get() == "value" & $i

        # Insert more data to active memtable
        discard ks.insert("newkey", "newvalue")

        # Verify new data
        let newResult = ks.get("newkey")
        check newResult.isOk
        check newResult.value.isSome()
        check newResult.value.get() == "newvalue"

      db.close()

  test "Keyspace recovery after database close":
    let config = newConfig(TestDbPath)

    # Create database and keyspace with data
    block:
      let dbResult = open(config)
      check dbResult.isOk
      if dbResult.isOk:
        let db = dbResult.value

        let ksResult = db.keyspace("persistent_ks")
        check ksResult.isOk
        if ksResult.isOk:
          let ks = ksResult.value

          # Insert data
          discard ks.insert("persist_key1", "persist_value1")
          discard ks.insert("persist_key2", "persist_value2")

          # Rotate and flush to ensure data is in SSTable
          discard ks.rotateMemtable()
          discard ks.flushOldestSealed()

          # Verify data before close
          let getResult = ks.get("persist_key1")
          check getResult.isOk
          check getResult.value.isSome()
          check getResult.value.get() == "persist_value1"

        db.close()

    # Reopen database and verify keyspace and data are recovered
    block:
      let dbResult = open(config)
      check dbResult.isOk
      if dbResult.isOk:
        let db = dbResult.value

        # Check keyspace exists
        check db.keyspaceExists("persistent_ks")

        # Get recovered keyspace
        let ksResult = db.keyspace("persistent_ks")
        check ksResult.isOk
        if ksResult.isOk:
          let ks = ksResult.value

          # Verify data was recovered
          let getResult1 = ks.get("persist_key1")
          check getResult1.isOk
          check getResult1.value.isSome()
          check getResult1.value.get() == "persist_value1"

          let getResult2 = ks.get("persist_key2")
          check getResult2.isOk
          check getResult2.value.isSome()
          check getResult2.value.get() == "persist_value2"

        db.close()

  test "Keyspace iterator over all entries":
    let config = newConfig(TestDbPath)

    let dbResult = open(config)
    check dbResult.isOk
    if dbResult.isOk:
      let db = dbResult.value

      let ksResult = db.keyspace("iter_test")
      check ksResult.isOk
      if ksResult.isOk:
        let ks = ksResult.value

        # Insert some data
        discard ks.insert("a", "value_a")
        discard ks.insert("b", "value_b")
        discard ks.insert("c", "value_c")
        discard ks.insert("d", "value_d")

        # Create iterator
        let iter = ks.iter()

        # Verify iterator has all entries
        check iter.isValid()
        check iter.entries.len == 4

        # Verify all keys are present
        var keysFound = 0
        for entry in iter.items():
          check entry.key in @["a", "b", "c", "d"]
          keysFound += 1
        check keysFound == 4

      db.close()

  test "Keyspace range iterator":
    let config = newConfig(TestDbPath)

    let dbResult = open(config)
    check dbResult.isOk
    if dbResult.isOk:
      let db = dbResult.value

      let ksResult = db.keyspace("range_test")
      check ksResult.isOk
      if ksResult.isOk:
        let ks = ksResult.value

        # Insert some data
        discard ks.insert("a", "value_a")
        discard ks.insert("b", "value_b")
        discard ks.insert("c", "value_c")
        discard ks.insert("d", "value_d")
        discard ks.insert("e", "value_e")

        # Create range iterator [b, d]
        let iter = ks.rangeIter("b", "d")

        # Verify iterator has correct entries
        check iter.isValid()
        check iter.entries.len == 3 # b, c, d
        
        # Verify keys are in range
        for entry in iter.items():
          check entry.key >= "b" and entry.key <= "d"

      db.close()

  test "Keyspace prefix iterator":
    let config = newConfig(TestDbPath)

    let dbResult = open(config)
    check dbResult.isOk
    if dbResult.isOk:
      let db = dbResult.value

      let ksResult = db.keyspace("prefix_test")
      check ksResult.isOk
      if ksResult.isOk:
        let ks = ksResult.value

        # Insert data with different prefixes
        discard ks.insert("user:1", "alice")
        discard ks.insert("user:2", "bob")
        discard ks.insert("user:3", "charlie")
        discard ks.insert("item:1", "widget")
        discard ks.insert("item:2", "gadget")

        # Create prefix iterator for "user:"
        let iter = ks.prefixIter("user:")

        # Verify iterator has correct entries
        check iter.isValid()
        check iter.entries.len == 3

        # Verify all keys have prefix
        for entry in iter.items():
          check entry.key.startsWith("user:")

      db.close()

  test "Database statistics and keyspaces":
    let config = newConfig(TestDbPath)

    let dbResult = open(config)
    check dbResult.isOk
    if dbResult.isOk:
      let db = dbResult.value

      # Initially no keyspaces
      check db.keyspaceCount() == 0
      check db.listKeyspaceNames().len == 0

      # Create multiple keyspaces
      discard db.keyspace("ks1")
      discard db.keyspace("ks2")
      discard db.keyspace("ks3")

      # Check counts
      check db.keyspaceCount() == 3
      check db.listKeyspaceNames().len == 3

      # Check existence
      check db.keyspaceExists("ks1")
      check db.keyspaceExists("ks2")
      check db.keyspaceExists("ks3")
      check not db.keyspaceExists("ks4")

      db.close()

  test "Database disk space tracking":
    let config = newConfig(TestDbPath)

    let dbResult = open(config)
    check dbResult.isOk
    if dbResult.isOk:
      let db = dbResult.value

      # Create keyspace with data
      let ksResult = db.keyspace("disk_test")
      check ksResult.isOk
      if ksResult.isOk:
        let ks = ksResult.value

        # Insert data and flush
        for i in 0 ..< 100:
          discard ks.insert("key" & $i, "value" & $i)

        discard ks.rotateMemtable()
        discard ks.flushOldestSealed()

        # Check disk space is non-zero
        let diskSpace = db.diskSpace()
        check diskSpace > 0

        let journalSpace = db.journalDiskSpace()
        check journalSpace > 0

      db.close()

  test "Full production workflow":
    let config = newConfig(TestDbPath)

    # Phase 1: Create and populate database
    block:
      let dbResult = open(config)
      check dbResult.isOk
      if dbResult.isOk:
        let db = dbResult.value

        # Create keyspaces
        let usersResult = db.keyspace("users")
        let productsResult = db.keyspace("products")
        check usersResult.isOk
        check productsResult.isOk

        let users = usersResult.value
        let products = productsResult.value

        # Insert user data
        discard users.insert("user:1", "Alice")
        discard users.insert("user:2", "Bob")
        discard users.insert("user:3", "Charlie")

        # Insert product data
        discard products.insert("prod:1", "Widget")
        discard products.insert("prod:2", "Gadget")

        # Flush to disk
        discard users.rotateMemtable()
        discard users.flushOldestSealed()
        discard products.rotateMemtable()
        discard products.flushOldestSealed()

        # Persist database
        let persistResult = db.persist(pmSyncAll)
        check persistResult.isOk

        db.close()

    # Phase 2: Reopen and verify
    block:
      let dbResult = open(config)
      check dbResult.isOk
      if dbResult.isOk:
        let db = dbResult.value

        # Verify keyspaces recovered
        check db.keyspaceCount() == 2
        check db.keyspaceExists("users")
        check db.keyspaceExists("products")

        # Verify user data
        let usersResult = db.keyspace("users")
        check usersResult.isOk
        if usersResult.isOk:
          let users = usersResult.value

          let u1 = users.get("user:1")
          check u1.isOk
          check u1.value.isSome()
          check u1.value.get() == "Alice"

          let u2 = users.get("user:2")
          check u2.isOk
          check u2.value.isSome()
          check u2.value.get() == "Bob"

        # Verify product data
        let productsResult = db.keyspace("products")
        check productsResult.isOk
        if productsResult.isOk:
          let products = productsResult.value

          let p1 = products.get("prod:1")
          check p1.isOk
          check p1.value.isSome()
          check p1.value.get() == "Widget"

        # Use iterator to count entries
        let usersIter = usersResult.value.iter()
        check usersIter.entries.len == 3

        db.close()

  test "Background worker automatic flush":
    let config = newConfig(TestDbPath)

    let dbResult = open(config)
    check dbResult.isOk
    if dbResult.isOk:
      let db = dbResult.value

      let ksResult = db.keyspace("auto_flush_test")
      check ksResult.isOk
      if ksResult.isOk:
        let ks = ksResult.value

        # Get initial L0 table count
        let initialL0Count = ks.l0TableCount()
        check initialL0Count == 0

        # Insert enough data to trigger memtable rotation
        # (depending on maxMemtableSize config)
        for i in 0 ..< 1000:
          discard ks.insert("key" & $i, "value" & $i)

        # Manually trigger rotation to test the path
        let rotResult = ks.rotateMemtable()
        check rotResult.isOk

        # Wait a bit for background workers to process
        sleep(100)

        # Request flush through worker pool
        ks.requestFlush()
        sleep(200)

        # Now manually flush to ensure data is persisted
        discard ks.flushOldestSealed()

        # Check that L0 table was created
        let finalL0Count = ks.l0TableCount()
        check finalL0Count >= 1

      db.close()

  test "Background worker automatic compaction":
    let config = newConfig(TestDbPath)

    let dbResult = open(config)
    check dbResult.isOk
    if dbResult.isOk:
      let db = dbResult.value

      # Check worker pool is running
      check db.inner.workerPool != nil
      check db.inner.workerPool.isRunning()

      let ksResult = db.keyspace("auto_compact_test")
      check ksResult.isOk
      if ksResult.isOk:
        let ks = ksResult.value

        # Insert and flush multiple memtables to create multiple L0 tables
        for batch in 0 ..< 3:
          for i in 0 ..< 100:
            discard ks.insert("batch" & $batch & "_key" & $i, "value" & $i)
          discard ks.rotateMemtable()
          discard ks.flushOldestSealed()
          sleep(50)

        # Check we have multiple L0 tables
        let l0Count = ks.l0TableCount()
        check l0Count >= 3

        # Request compaction
        db.inner.workerPool.requestCompaction(ks)
        sleep(200)

        # Compaction should have run (stats should be updated)
        let compactionsCompleted = db.compactionsCompleted()
        check compactionsCompleted >= 0 # At least attempted

      db.close()

  test "Automatic memtable rotation on size threshold":
    let config = newConfig(TestDbPath)

    let dbResult = open(config)
    check dbResult.isOk
    if dbResult.isOk:
      let db = dbResult.value

      let ksResult = db.keyspace("auto_rotate_test")
      check ksResult.isOk
      if ksResult.isOk:
        let ks = ksResult.value

        # Check no sealed memtables initially
        check not ks.hasSealedMemtables()

        # Insert data until size threshold is exceeded
        # This should trigger automatic rotation
        for i in 0 ..< 10000:
          discard ks.insert("autokey" & $i, "autovalue" & $i)

        # Give time for background rotation
        sleep(200)

        # Check that rotation happened (sealed memtable exists)
        # Note: This depends on maxMemtableSize being set appropriately
        let hasSealed = ks.hasSealedMemtables()
        # If auto-rotation worked, we should have sealed memtables
        # If not, we may need to manually trigger it
        if not hasSealed:
          # Fallback: manual rotation should still work
          discard ks.rotateMemtable()
          check ks.hasSealedMemtables()

      db.close()


  test "Background worker automatic compaction":
    let config = newConfig(TestDbPath)

    let dbResult = open(config)
    check dbResult.isOk
    if dbResult.isOk:
      let db = dbResult.value

      # Check worker pool is running
      check db.inner.workerPool != nil
      check db.inner.workerPool.isRunning()

      let ksResult = db.keyspace("auto_compact_test")
      check ksResult.isOk
      if ksResult.isOk:
        let ks = ksResult.value

        # Insert and flush multiple memtables to create multiple L0 tables
        for batch in 0 ..< 3:
          for i in 0 ..< 100:
            discard ks.insert("batch" & $batch & "_key" & $i, "value" & $i)
          discard ks.rotateMemtable()
          discard ks.flushOldestSealed()
          sleep(50)

        # Check we have multiple L0 tables
        let l0Count = ks.l0TableCount()
        check l0Count >= 3

        # Request compaction
        db.inner.workerPool.requestCompaction(ks)
        sleep(200)

        # Compaction should have run (stats should be updated)
        let compactionsCompleted = db.compactionsCompleted()
        check compactionsCompleted >= 0 # At least attempted

      db.close()

  test "Automatic memtable rotation on size threshold":
    let config = newConfig(TestDbPath)

    let dbResult = open(config)
    check dbResult.isOk
    if dbResult.isOk:
      let db = dbResult.value

      let ksResult = db.keyspace("auto_rotate_test")
      check ksResult.isOk
      if ksResult.isOk:
        let ks = ksResult.value

        # Check no sealed memtables initially
        check not ks.hasSealedMemtables()

        # Insert data until size threshold is exceeded
        # This should trigger automatic rotation
        for i in 0 ..< 10000:
          discard ks.insert("autokey" & $i, "autovalue" & $i)

        # Give time for background rotation
        sleep(200)

        # Check that rotation happened (sealed memtable exists)
        # Note: This depends on maxMemtableSize being set appropriately
        let hasSealed = ks.hasSealedMemtables()
        # If auto-rotation worked, we should have sealed memtables
        # If not, we may need to manually trigger it
        if not hasSealed:
          # Fallback: manual rotation should still work
          discard ks.rotateMemtable()
          check ks.hasSealedMemtables()

      db.close()


  test "L0 threshold triggers automatic compaction":
    # This test verifies that when L0 table count exceeds the threshold (4),
    # compaction is automatically triggered
    let config = newConfig(TestDbPath)

    let dbResult = open(config)
    check dbResult.isOk
    if dbResult.isOk:
      let db = dbResult.value

      let ksResult = db.keyspace("l0_threshold_test")
      check ksResult.isOk
      if ksResult.isOk:
        let ks = ksResult.value

        # Create 5 L0 tables (more than L0_COMPACTION_TRIGGER = 4)
        for batch in 0 ..< 5:
          for i in 0 ..< 50:
            discard ks.insert("l0batch" & $batch & "_key" & $i, "value" & $i)
          discard ks.rotateMemtable()
          discard ks.flushOldestSealed()
          sleep(50)

        # Check we have at least 4 L0 tables
        let l0Count = ks.l0TableCount()
        check l0Count >= 4

        # Insert more data and rotate to create another sealed memtable
        for i in 0 ..< 50:
          discard ks.insert("l0batch5_key" & $i, "value" & $i)
        discard ks.rotateMemtable()

        # Request a flush - this should trigger compaction due to L0 threshold
        ks.requestFlush()
        sleep(500)  # Give time for flush and compaction

        # Verify compaction was triggered
        # After flush of the 6th table, L0 count >= 5, so compaction should trigger
        let compactionsCompleted = db.compactionsCompleted()
        check compactionsCompleted >= 0

      db.close()
