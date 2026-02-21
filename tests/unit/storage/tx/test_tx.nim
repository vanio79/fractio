# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Tests for transaction functionality

import unittest
import fractio/storage/tx as tx_module
import fractio/storage/db
import fractio/storage/db_config
import fractio/storage/keyspace as ks_module
import std/[os, tempfiles, options, tables]

suite "Transaction Tests":
  setup:
    let tempDir = createTempDir("tx_test_", "")
    let dbPath = tempDir / "test_db"

  teardown:
    try:
      removeDir(tempDir)
    except OSError:
      discard

  test "Create write transaction":
    let tx = newWriteTransaction(100'u64)
    check tx.snapshotSeqno == 100'u64
    check tx.currentSeqno == TX_SEQNO_START
    check tx.isActive
    check tx.memtables.len == 0

  test "Transaction check active":
    var tx = newWriteTransaction(100'u64)
    let check1 = tx.checkActive()
    check check1.isOk

    tx.isActive = false
    let check2 = tx.checkActive()
    check not check2.isOk

  test "Get memtable for keyspace":
    var tx = newWriteTransaction(100'u64)
    let mt1 = tx.getMemtable(1'u64)
    check mt1 != nil
    check tx.memtables.len() == 1

    # Getting same keyspace returns same memtable
    let mt1Again = tx.getMemtable(1'u64)
    check mt1 == mt1Again

    # Different keyspace gets different memtable
    let mt2 = tx.getMemtable(2'u64)
    check mt2 != nil
    check tx.memtables.len() == 2

  test "Rollback clears memtables":
    var tx = newWriteTransaction(100'u64)
    discard tx.getMemtable(1'u64)
    check tx.memtables.len() == 1

    tx.rollback()
    check not tx.isActive
    check tx.memtables.len() == 0

  test "Create TxDatabase":
    var txDb = newTxDatabase()
    check not txDb.txActive
    check txDb.keyspaces.len() == 0

  test "Register keyspace":
    var txDb = newTxDatabase()

    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk
    let ks = ksResult.value

    txDb.registerKeyspace("test_ks", ks)
    check txDb.keyspaces.len() == 1
    check "test_ks" in txDb.keyspaces

    db.close()

  test "Begin and end transaction":
    var txDb = newTxDatabase()

    let tx = txDb.beginTx(100'u64)
    check tx.isActive
    check tx.snapshotSeqno == 100'u64
    check txDb.txActive

    txDb.endTx()
    check not txDb.txActive

  test "Transaction insert and commit":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk
    let ks = ksResult.value

    var txDb = newTxDatabase()
    txDb.registerKeyspace("test_ks", ks)

    var tx = txDb.beginTx(100'u64)

    # Insert in transaction
    let insertResult = tx_module.txInsert(tx, ks, "key1", "value1")
    check insertResult.isOk
    check tx.memtables.len() == 1

    # Commit
    let commitResult = tx.commit(txDb.keyspaces)
    check commitResult.isOk
    check not tx.isActive

    # Verify data was written
    let valResult = ks_module.get(ks, "key1")
    check valResult.isOk
    check valResult.value.isSome
    check valResult.value.get == "value1"

    txDb.endTx()
    db.close()

  test "Transaction remove and commit":
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

    var txDb = newTxDatabase()
    txDb.registerKeyspace("test_ks", ks)

    var tx = txDb.beginTx(100'u64)

    # Remove in transaction
    let removeResult = tx_module.txRemove(tx, ks, "key1")
    check removeResult.isOk

    # Commit
    let commitResult = tx.commit(txDb.keyspaces)
    check commitResult.isOk

    # Verify key was removed
    let valResult = ks_module.get(ks, "key1")
    check valResult.isOk
    check valResult.value.isNone

    txDb.endTx()
    db.close()

  test "Transaction RYOW - read your own writes":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk
    let ks = ksResult.value

    var txDb = newTxDatabase()
    txDb.registerKeyspace("test_ks", ks)

    var tx = txDb.beginTx(100'u64)

    # Insert in transaction
    discard tx_module.txInsert(tx, ks, "key1", "tx_value")

    # Read should see uncommitted value
    let getResult = tx_module.txGet(tx, ks, "key1")
    check getResult.isOk
    check getResult.value.isSome
    check getResult.value.get == "tx_value"

    tx.rollback()
    txDb.endTx()
    db.close()

  test "Transaction RYOW - tombstone":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk
    let ks = ksResult.value

    # Insert initial data
    discard ks.insert("key1", "value1")

    var txDb = newTxDatabase()
    txDb.registerKeyspace("test_ks", ks)

    var tx = txDb.beginTx(100'u64)

    # Remove in transaction
    discard tx_module.txRemove(tx, ks, "key1")

    # Read should see tombstone (key doesn't exist)
    let getResult = tx_module.txGet(tx, ks, "key1")
    check getResult.isOk
    check getResult.value.isNone

    tx.rollback()
    txDb.endTx()
    db.close()

  test "Transaction containsKey with RYOW":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk
    let ks = ksResult.value

    var txDb = newTxDatabase()
    txDb.registerKeyspace("test_ks", ks)

    var tx = txDb.beginTx(100'u64)

    # Insert in transaction
    discard tx_module.txInsert(tx, ks, "key1", "value1")

    # containsKey should see uncommitted value
    let containsResult = tx_module.txContainsKey(tx, ks, "key1")
    check containsResult.isOk
    check containsResult.value

    # Key not in transaction should check keyspace
    let containsResult2 = tx_module.txContainsKey(tx, ks, "key_notexist")
    check containsResult2.isOk
    check not containsResult2.value

    tx.rollback()
    txDb.endTx()
    db.close()

  test "Transaction rollback does not apply changes":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk
    let ks = ksResult.value

    var txDb = newTxDatabase()
    txDb.registerKeyspace("test_ks", ks)

    var tx = txDb.beginTx(100'u64)

    # Insert in transaction
    discard tx_module.txInsert(tx, ks, "key1", "value1")

    # Rollback
    tx.rollback()

    # Key should not exist in keyspace
    let valResult = ks_module.get(ks, "key1")
    check valResult.isOk
    check valResult.value.isNone

    txDb.endTx()
    db.close()

  test "Multiple inserts in transaction":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk
    let ks = ksResult.value

    var txDb = newTxDatabase()
    txDb.registerKeyspace("test_ks", ks)

    var tx = txDb.beginTx(100'u64)

    # Insert multiple items
    discard tx_module.txInsert(tx, ks, "key1", "value1")
    discard tx_module.txInsert(tx, ks, "key2", "value2")
    discard tx_module.txInsert(tx, ks, "key3", "value3")

    # Commit
    let commitResult = tx.commit(txDb.keyspaces)
    check commitResult.isOk

    # Verify all items
    for i in 1..3:
      let key = "key" & $i
      let val = "value" & $i
      let valResult = ks_module.get(ks, key)
      check valResult.isOk
      check valResult.value.isSome
      check valResult.value.get == val

    txDb.endTx()
    db.close()

  test "Empty transaction commit succeeds":
    var tx = newWriteTransaction(100'u64)

    let commitResult = tx.commit(initTable[string, ks_module.Keyspace]())
    check commitResult.isOk
    check not tx.isActive

  test "Transaction after commit is inactive":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk
    let ks = ksResult.value

    var txDb = newTxDatabase()
    txDb.registerKeyspace("test_ks", ks)

    var tx = txDb.beginTx(100'u64)
    discard tx_module.txInsert(tx, ks, "key1", "value1")

    let commitResult = tx.commit(txDb.keyspaces)
    check commitResult.isOk

    # Second commit should fail
    let commitResult2 = tx.commit(txDb.keyspaces)
    check not commitResult2.isOk

    txDb.endTx()
    db.close()
