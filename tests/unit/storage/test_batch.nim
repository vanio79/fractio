# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Tests for write batch functionality

import unittest
import fractio/storage/db
import fractio/storage/db_config
import fractio/storage/batch as batch_module
import fractio/storage/keyspace as ks_module
import std/[os, tempfiles, options]

suite "Write Batch Tests":
  setup:
    let tempDir = createTempDir("batch_test_", "")
    let dbPath = tempDir / "test_db"

  teardown:
    try:
      removeDir(tempDir)
    except OSError:
      discard

  test "Create empty batch":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let wb = db.batch()
    check wb.len == 0
    check wb.isEmpty

    db.close()

  test "Insert single item into batch":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk
    let ks = ksResult.value

    var wb = db.batch()
    wb.insert(ks, "key1", "value1")

    check wb.len == 1
    check not wb.isEmpty

    db.close()

  test "Insert multiple items into batch":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk
    let ks = ksResult.value

    var wb = db.batch()
    wb.insert(ks, "key1", "value1")
    wb.insert(ks, "key2", "value2")
    wb.insert(ks, "key3", "value3")

    check wb.len == 3

    db.close()

  test "Commit empty batch succeeds":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let wb = db.batch()
    let commitResult = db.commit(wb)
    check commitResult.isOk

    db.close()

  test "Commit batch with inserts":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk
    let ks = ksResult.value

    var wb = db.batch()
    wb.insert(ks, "key1", "value1")
    wb.insert(ks, "key2", "value2")

    let commitResult = db.commit(wb)
    check commitResult.isOk

    # Verify data was written
    let val1Result = ks_module.get(ks, "key1")
    check val1Result.isOk
    check val1Result.value.isSome
    check val1Result.value.get == "value1"

    let val2Result = ks_module.get(ks, "key2")
    check val2Result.isOk
    check val2Result.value.isSome
    check val2Result.value.get == "value2"

    db.close()

  test "Commit batch with removes":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk
    let ks = ksResult.value

    # Insert initial data
    let insertResult1 = ks.insert("key1", "value1")
    check insertResult1.isOk

    # Create batch with remove
    var wb = db.batch()
    wb.remove(ks, "key1")

    let commitResult = db.commit(wb)
    check commitResult.isOk

    # Verify key was removed
    let valResult = ks_module.get(ks, "key1")
    check valResult.isOk
    check valResult.value.isNone

    db.close()

  test "Batch across multiple keyspaces":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ks1Result = db.keyspace("ks1")
    check ks1Result.isOk
    let ks1 = ks1Result.value

    let ks2Result = db.keyspace("ks2")
    check ks2Result.isOk
    let ks2 = ks2Result.value

    var wb = db.batch()
    wb.insert(ks1, "key1", "value1")
    wb.insert(ks2, "key2", "value2")

    let commitResult = db.commit(wb)
    check commitResult.isOk

    # Verify data in both keyspaces
    let val1Result = ks_module.get(ks1, "key1")
    check val1Result.isOk
    check val1Result.value.isSome
    check val1Result.value.get == "value1"

    let val2Result = ks_module.get(ks2, "key2")
    check val2Result.isOk
    check val2Result.value.isSome
    check val2Result.value.get == "value2"

    db.close()

  test "Batch size calculation":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk
    let ks = ksResult.value

    var wb = db.batch()
    wb.insert(ks, "key1", "val1") # 4 + 4 = 8 bytes
    wb.insert(ks, "key2", "val2") # 4 + 4 = 8 bytes

    let size = wb.size()
    check size == 16'u64

    db.close()

  test "Batch clear":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk
    let ks = ksResult.value

    var wb = db.batch()
    wb.insert(ks, "key1", "value1")
    check wb.len == 1

    wb.clear()
    check wb.len == 0
    check wb.isEmpty

    db.close()

