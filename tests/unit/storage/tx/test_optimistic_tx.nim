# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Tests for optimistic transaction functionality with MVCC conflict detection

import unittest
import fractio/storage/tx as tx_module
import fractio/storage/db
import fractio/storage/db_config
import fractio/storage/keyspace as ks_module
import fractio/storage/iter # For KeyspaceIter.close
import fractio/storage/journal/writer
import std/[os, tempfiles, options, tables]

suite "Optimistic Transaction Tests":
  setup:
    let tempDir = createTempDir("otx_test_", "")
    let dbPath = tempDir / "test_db"

  teardown:
    try:
      removeDir(tempDir)
    except OSError:
      discard

  test "Create optimistic transaction":
    let tx = newOptimisticTransaction(100'u64)
    check tx.snapshotSeqno == 100'u64
    check tx.currentSeqno == TX_SEQNO_START
    check tx.isActive
    check tx.writeSet.len == 0
    check tx.readSet.len == 0

  test "Create OptimisticTxDatabase":
    var txDb = newOptimisticTxDatabase()
    check txDb.keyspaces.len == 0

  test "Optimistic transaction check active":
    var tx = newOptimisticTransaction(100'u64)
    let check1 = tx.checkActive()
    check check1.isOk

    tx.isActive = false
    let check2 = tx.checkActive()
    check not check2.isOk

  test "Get write memtable for keyspace":
    var tx = newOptimisticTransaction(100'u64)
    let mt1 = tx.getWriteMemtable(1'u64)
    check mt1 != nil
    check tx.writeSet.len == 1

    # Getting same keyspace returns same memtable
    let mt1Again = tx.getWriteMemtable(1'u64)
    check mt1 == mt1Again

    # Different keyspace gets different memtable
    let mt2 = tx.getWriteMemtable(2'u64)
    check mt2 != nil
    check tx.writeSet.len == 2

  test "Record read in read set":
    var tx = newOptimisticTransaction(100'u64)
    tx.recordRead(1'u64, "key1", 50'u64)
    check tx.readSet.len == 1

    # Recording same key again should not add duplicate
    tx.recordRead(1'u64, "key1", 50'u64)
    check tx.readSet.len == 1

    # Different key adds new entry
    tx.recordRead(1'u64, "key2", 60'u64)
    check tx.readSet.len == 2

  test "Optimistic transaction insert and commit":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk
    let ks = ksResult.value

    var txDb = newOptimisticTxDatabase()
    txDb.registerKeyspace("test_ks", ks)

    var tx = txDb.beginOptimisticTx(100'u64)

    # Insert in transaction
    let insertResult = tx_module.otxInsert(tx, ks, "key1", "value1")
    check insertResult.isOk
    check tx.writeSet.len == 1

    # Commit
    let commitResult = txDb.commitOptimisticTx(tx)
    check commitResult.isOk
    check not tx.isActive

    # Verify data was written
    let valResult = ks_module.get(ks, "key1")
    check valResult.isOk
    check valResult.value.isSome
    check valResult.value.get == "value1"

    db.close()

  test "Optimistic transaction remove and commit":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk
    let ks = ksResult.value

    # Insert initial data
    let insertResult = ks.insert("key1", "value1")
    check insertResult.isOk

    var txDb = newOptimisticTxDatabase()
    txDb.registerKeyspace("test_ks", ks)

    var tx = txDb.beginOptimisticTx(100'u64)

    # Remove in transaction
    let removeResult = tx_module.otxRemove(tx, ks, "key1")
    check removeResult.isOk

    # Commit
    let commitResult = txDb.commitOptimisticTx(tx)
    check commitResult.isOk

    # Verify key was removed
    let valResult = ks_module.get(ks, "key1")
    check valResult.isOk
    check valResult.value.isNone

    db.close()

  test "Optimistic transaction RYOW - read your own writes":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk
    let ks = ksResult.value

    var txDb = newOptimisticTxDatabase()
    txDb.registerKeyspace("test_ks", ks)

    var tx = txDb.beginOptimisticTx(100'u64)

    # Insert in transaction
    discard tx_module.otxInsert(tx, ks, "key1", "tx_value")

    # Read should see uncommitted value
    let getResult = tx_module.otxGet(tx, ks, "key1")
    check getResult.isOk
    check getResult.value.isSome
    check getResult.value.get == "tx_value"

    tx.rollback()
    db.close()

  test "Optimistic transaction RYOW - tombstone":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk
    let ks = ksResult.value

    # Insert initial data
    discard ks.insert("key1", "value1")

    var txDb = newOptimisticTxDatabase()
    txDb.registerKeyspace("test_ks", ks)

    var tx = txDb.beginOptimisticTx(100'u64)

    # Remove in transaction
    discard tx_module.otxRemove(tx, ks, "key1")

    # Read should see tombstone (key doesn't exist)
    let getResult = tx_module.otxGet(tx, ks, "key1")
    check getResult.isOk
    check getResult.value.isNone

    tx.rollback()
    db.close()

  test "Optimistic transaction rollback does not apply changes":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk
    let ks = ksResult.value

    var txDb = newOptimisticTxDatabase()
    txDb.registerKeyspace("test_ks", ks)

    var tx = txDb.beginOptimisticTx(100'u64)

    # Insert in transaction
    discard tx_module.otxInsert(tx, ks, "key1", "value1")

    # Rollback
    tx.rollback()

    # Key should not exist in keyspace
    let valResult = ks_module.get(ks, "key1")
    check valResult.isOk
    check valResult.value.isNone

    db.close()

  test "Optimistic transaction conflict detection - read-write conflict":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk
    let ks = ksResult.value

    var txDb = newOptimisticTxDatabase()
    txDb.registerKeyspace("test_ks", ks)

    # Insert initial value so tx1 can read it
    discard ks.insert("key1", "initial_value")

    # Start transaction 1
    var tx1 = txDb.beginOptimisticTx(100'u64)

    # Read key1 in tx1 (adds to read set with current seqno)
    let getResult1 = tx_module.otxGet(tx1, ks, "key1")
    check getResult1.isOk
    check getResult1.value.isSome
    check getResult1.value.get == "initial_value"

    # Start transaction 2 and commit it (modifies key1)
    var tx2 = txDb.beginOptimisticTx(100'u64)
    discard tx_module.otxInsert(tx2, ks, "key1", "value_from_tx2")
    let commitResult2 = txDb.commitOptimisticTx(tx2)
    check commitResult2.isOk

    # Now try to commit tx1 - should conflict because key1 was read and then modified
    # Note: tx1 only read key1, didn't write to it
    let commitResult1 = txDb.commitOptimisticTx(tx1)
    check not commitResult1.isOk # Should fail due to read-write conflict

    db.close()

  test "Optimistic transaction no conflict when reading different keys":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk
    let ks = ksResult.value

    var txDb = newOptimisticTxDatabase()
    txDb.registerKeyspace("test_ks", ks)

    # Start transaction 1
    var tx1 = txDb.beginOptimisticTx(100'u64)

    # Read key1 in tx1
    discard tx_module.otxGet(tx1, ks, "key1")

    # Start transaction 2 and commit it with a different key
    var tx2 = txDb.beginOptimisticTx(100'u64)
    discard tx_module.otxInsert(tx2, ks, "key2", "value2")
    let commitResult2 = txDb.commitOptimisticTx(tx2)
    check commitResult2.isOk

    # Now commit tx1 - should succeed because we read different keys
    discard tx_module.otxInsert(tx1, ks, "key1", "value1")
    let commitResult1 = txDb.commitOptimisticTx(tx1)
    check commitResult1.isOk

    db.close()

  test "Optimistic transaction write doesn't add to read set":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk
    let ks = ksResult.value

    var txDb = newOptimisticTxDatabase()
    txDb.registerKeyspace("test_ks", ks)

    var tx1 = txDb.beginOptimisticTx(100'u64)

    # Write to key1 (should not add to read set)
    discard tx_module.otxInsert(tx1, ks, "key1", "value1")
    check tx1.readSet.len == 0

    # Start another transaction and commit
    var tx2 = txDb.beginOptimisticTx(100'u64)
    discard tx_module.otxInsert(tx2, ks, "key1", "value2")
    let commitResult2 = txDb.commitOptimisticTx(tx2)
    check commitResult2.isOk

    # tx1 should still commit because it didn't read key1
    let commitResult1 = txDb.commitOptimisticTx(tx1)
    check commitResult1.isOk

    db.close()

  test "Empty optimistic transaction commit succeeds":
    var tx = newOptimisticTransaction(100'u64)

    var txDb = newOptimisticTxDatabase()
    let commitResult = tx.otxCommit(txDb.keyspaces)
    check commitResult.isOk
    check not tx.isActive

  test "Optimistic transaction with durability mode":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk
    let ks = ksResult.value

    var txDb = newOptimisticTxDatabase()
    txDb.registerKeyspace("test_ks", ks)

    var tx = txDb.beginOptimisticTx(100'u64, some(
        writer.PersistMode.pmSyncData))
    check tx.durability.isSome
    check tx.durability.get() == writer.PersistMode.pmSyncData

    discard tx_module.otxInsert(tx, ks, "key1", "value1")
    let commitResult = txDb.commitOptimisticTx(tx)
    check commitResult.isOk

    db.close()

  test "Record range read":
    var tx = newOptimisticTransaction(100'u64)
    tx.recordRangeRead(1'u64, "key_a", "key_z", 50'u64)
    check tx.readRanges.len == 1
    check tx.readRanges[1'u64].len == 1
    check tx.readRanges[1'u64][0].startKey == "key_a"
    check tx.readRanges[1'u64][0].endKey == "key_z"
    check tx.readRanges[1'u64][0].highestSeqno == 50'u64

  test "Record overlapping range reads - merge":
    var tx = newOptimisticTransaction(100'u64)
    tx.recordRangeRead(1'u64, "key_a", "key_m", 50'u64)
    tx.recordRangeRead(1'u64, "key_l", "key_z", 60'u64)
    # Should merge into one range
    check tx.readRanges[1'u64].len == 1
    check tx.readRanges[1'u64][0].startKey == "key_a"
    check tx.readRanges[1'u64][0].endKey == "key_z"
    check tx.readRanges[1'u64][0].highestSeqno == 60'u64

  test "Record non-overlapping range reads":
    var tx = newOptimisticTransaction(100'u64)
    tx.recordRangeRead(1'u64, "key_a", "key_c", 50'u64)
    tx.recordRangeRead(1'u64, "key_x", "key_z", 60'u64)
    # Should be two separate ranges
    check tx.readRanges[1'u64].len == 2

  test "Record full keyspace scan":
    var tx = newOptimisticTransaction(100'u64)
    tx.recordFullScan(1'u64, 75'u64)
    check tx.readAll.len == 1
    check tx.readAll[1'u64].highestSeqno == 75'u64

    # Update with higher seqno
    tx.recordFullScan(1'u64, 100'u64)
    check tx.readAll[1'u64].highestSeqno == 100'u64

  test "Has range reads":
    var tx = newOptimisticTransaction(100'u64)
    check not tx.hasRangeReads()

    tx.recordRangeRead(1'u64, "a", "z", 50'u64)
    check tx.hasRangeReads()

    var tx2 = newOptimisticTransaction(100'u64)
    tx2.recordFullScan(1'u64, 50'u64)
    check tx2.hasRangeReads()

  test "Read set size includes range reads":
    var tx = newOptimisticTransaction(100'u64)
    check tx.readSetSize() == 0

    tx.recordRead(1'u64, "key1", 50'u64)
    check tx.readSetSize() == 1

    tx.recordRangeRead(1'u64, "a", "z", 60'u64)
    check tx.readSetSize() == 2

    tx.recordFullScan(2'u64, 70'u64)
    check tx.readSetSize() == 3

  test "Range iteration with conflict tracking":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk
    let ks = ksResult.value

    # Insert some data
    discard ks.insert("key_a", "value_a")
    discard ks.insert("key_m", "value_m")
    discard ks.insert("key_z", "value_z")

    var txDb = newOptimisticTxDatabase()
    txDb.registerKeyspace("test_ks", ks)

    var tx = txDb.beginOptimisticTx(100'u64)

    # Range iteration should track the range
    let iterResult = tx_module.otxRangeIter(tx, ks, "key_a", "key_z")
    check iterResult.isOk
    var iter = iterResult.value
    defer: iter.close()
    check tx.hasRangeReads()
    check tx.readRanges.len == 1

    tx.rollback()
    db.close()

  test "Prefix iteration with conflict tracking":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk
    let ks = ksResult.value

    # Insert some data with prefix
    discard ks.insert("user:1", "alice")
    discard ks.insert("user:2", "bob")
    discard ks.insert("item:1", "widget")

    var txDb = newOptimisticTxDatabase()
    txDb.registerKeyspace("test_ks", ks)

    var tx = txDb.beginOptimisticTx(100'u64)

    # Prefix iteration should track as a range
    let iterResult = tx_module.otxPrefixIter(tx, ks, "user:")
    check iterResult.isOk
    var iter = iterResult.value
    defer: iter.close()
    check tx.hasRangeReads()

    tx.rollback()
    db.close()

  test "Full keyspace iteration with conflict tracking":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk
    let ks = ksResult.value

    # Insert some data
    discard ks.insert("key1", "value1")
    discard ks.insert("key2", "value2")

    var txDb = newOptimisticTxDatabase()
    txDb.registerKeyspace("test_ks", ks)

    var tx = txDb.beginOptimisticTx(100'u64)

    # Full iteration should track as full scan
    let iterResult = tx_module.otxIter(tx, ks)
    check iterResult.isOk
    var iter = iterResult.value
    defer: iter.close()
    check tx.hasRangeReads()
    check tx.readAll.len == 1

    tx.rollback()
    db.close()

  test "Range read conflict detection - write in range":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk
    let ks = ksResult.value

    # Insert initial data in range
    discard ks.insert("key_a", "value_a")
    discard ks.insert("key_m", "value_m")
    discard ks.insert("key_z", "value_z")

    var txDb = newOptimisticTxDatabase()
    txDb.registerKeyspace("test_ks", ks)

    # Transaction 1: Read a range
    var tx1 = txDb.beginOptimisticTx(100'u64)
    let iterResult = tx_module.otxRangeIter(tx1, ks, "key_a", "key_z")
    check iterResult.isOk
    var iter1 = iterResult.value
    defer: iter1.close()

    # Transaction 2: Write to a key in the range and commit
    var tx2 = txDb.beginOptimisticTx(100'u64)
    discard tx_module.otxInsert(tx2, ks, "key_m", "new_value")
    let commitResult2 = txDb.commitOptimisticTx(tx2)
    check commitResult2.isOk

    # Transaction 1 should fail to commit (range conflict)
    discard tx_module.otxInsert(tx1, ks, "other_key", "value")
    let commitResult1 = txDb.commitOptimisticTx(tx1)
    check not commitResult1.isOk # Conflict expected

    db.close()

  test "Full scan conflict detection":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk
    let ks = ksResult.value

    # Insert initial data
    discard ks.insert("key1", "value1")

    var txDb = newOptimisticTxDatabase()
    txDb.registerKeyspace("test_ks", ks)

    # Transaction 1: Full scan
    var tx1 = txDb.beginOptimisticTx(100'u64)
    let iterResult = tx_module.otxIter(tx1, ks)
    check iterResult.isOk
    var iter1 = iterResult.value
    defer: iter1.close()

    # Transaction 2: Write any key and commit
    var tx2 = txDb.beginOptimisticTx(100'u64)
    discard tx_module.otxInsert(tx2, ks, "any_key", "any_value")
    let commitResult2 = txDb.commitOptimisticTx(tx2)
    check commitResult2.isOk

    # Transaction 1 should fail to commit (full scan conflict)
    discard tx_module.otxInsert(tx1, ks, "another_key", "value")
    let commitResult1 = txDb.commitOptimisticTx(tx1)
    check not commitResult1.isOk # Conflict expected

    db.close()

  test "Rollback clears range reads":
    var tx = newOptimisticTransaction(100'u64)
    tx.recordRead(1'u64, "key1", 50'u64)
    tx.recordRangeRead(1'u64, "a", "z", 60'u64)
    tx.recordFullScan(2'u64, 70'u64)

    check tx.readSet.len == 1
    check tx.readRanges.len == 1
    check tx.readAll.len == 1

    tx.rollback()

    check tx.readSet.len == 0
    check tx.readRanges.len == 0
    check tx.readAll.len == 0

