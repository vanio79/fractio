# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Integration tests for Database transaction support

import unittest
import fractio/storage/db as storage_db
import fractio/storage/db_config
import fractio/storage/keyspace
import fractio/storage/keyspace/options
import fractio/storage/journal/writer
import std/[os, tempfiles, options]

proc runTransactionTests() =
  suite "Database Transaction Integration Tests":
    var db: storage_db.Database = nil
    var tempDir = ""

    # Create database once for the suite
    tempDir = createTempDir("db_tx_test_", "")
    let dbPath = tempDir / "db"
    let config = db_config.newConfig(dbPath)
    let dbResult = storage_db.open(config)
    if dbResult.isOk:
      db = dbResult.value

    test "Begin and commit transaction":
      check db != nil
      if db == nil:
        return

      # Create a keyspace
      let ksResult = db.keyspace("test_ks1")
      check ksResult.isOk
      let ks = ksResult.value

      # Begin transaction
      var tx = db.beginTx()
      check tx.isActive

      # Insert in transaction
      let insertResult = storage_db.txInsert(tx, ks, "key1", "value1")
      check insertResult.isOk

      # Commit
      let commitResult = db.commitTx(tx)
      check commitResult.isOk
      check not tx.isActive

      # Verify data is visible
      let valueResult = ks.get("key1")
      check valueResult.isOk
      check valueResult.value.isSome
      check valueResult.value.get() == "value1"

    test "Begin and rollback transaction":
      check db != nil
      if db == nil:
        return

      let ksResult = db.keyspace("test_ks2")
      check ksResult.isOk
      let ks = ksResult.value

      # Insert initial data
      check ks.insert("existing", "data").isOk

      # Begin transaction
      var tx = db.beginTx()
      check tx.isActive

      # Insert in transaction
      check storage_db.txInsert(tx, ks, "key1", "value1").isOk
      check storage_db.txRemove(tx, ks, "existing").isOk

      # Rollback
      db.rollbackTx(tx)
      check not tx.isActive

      # Verify changes were not applied
      let valueResult = ks.get("key1")
      check valueResult.isOk
      check valueResult.value.isNone

      let existingResult = ks.get("existing")
      check existingResult.isOk
      check existingResult.value.isSome
      check existingResult.value.get() == "data"

    test "RYOW - Read your own writes":
      check db != nil
      if db == nil:
        return

      let ksResult = db.keyspace("test_ks3")
      check ksResult.isOk
      let ks = ksResult.value

      var tx = db.beginTx()

      # Insert
      check storage_db.txInsert(tx, ks, "key1", "original").isOk

      # Read should see the inserted value
      let read1 = storage_db.txGet(tx, ks, "key1")
      check read1.isOk
      check read1.value.isSome
      check read1.value.get() == "original"

      # Update
      check storage_db.txInsert(tx, ks, "key1", "updated").isOk

      # Read should see the updated value (only latest)
      let read2 = storage_db.txGet(tx, ks, "key1")
      check read2.isOk
      check read2.value.isSome
      check read2.value.get() == "updated"

      discard db.commitTx(tx)

    test "Empty transaction commit":
      check db != nil
      if db == nil:
        return

      var tx = db.beginTx()

      # Commit with no changes
      let commitResult = db.commitTx(tx)
      check commitResult.isOk

    test "Cannot commit inactive transaction":
      check db != nil
      if db == nil:
        return

      var tx = db.beginTx()
      check db.commitTx(tx).isOk

      # Try to commit again
      let commitResult = db.commitTx(tx)
      check not commitResult.isOk

    test "Transaction with durability mode":
      check db != nil
      if db == nil:
        return

      let ksResult = db.keyspace("test_ks4")
      check ksResult.isOk
      let ks = ksResult.value

      var tx = db.beginTx(some(writer.PersistMode.pmSyncData))
      check tx.durability.isSome
      check tx.durability.get() == writer.PersistMode.pmSyncData

      check storage_db.txInsert(tx, ks, "key1", "value1").isOk
      check db.commitTx(tx).isOk

    # Cleanup
    if db != nil:
      db.close()
    if tempDir.len > 0:
      try:
        removeDir(tempDir)
      except OSError:
        discard

runTransactionTests()
