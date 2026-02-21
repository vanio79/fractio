# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Tests for the Ingestion API (bulk loading)

import unittest
import std/[os, tempfiles, locks]
import fractio/storage/[ingestion, keyspace, types, error]
import fractio/storage/lsm_tree/[types as lsm_types, lsm_tree]
import fractio/storage/keyspace/options
import fractio/storage/supervisor
import fractio/storage/stats

suite "Ingestion API":

  test "newIngestion creates valid ingestion handle":
    let tempDir = createTempDir("ingest_test_", "")
    defer: removeDir(tempDir)

    let config = lsm_tree.newConfig(tempDir)
    let tree = lsm_tree.open(config)

    let keyspace = newKeyspace(
      keyspaceId = 1'u64,
      tree = tree,
      name = "test_ks",
      config = CreateOptions(),
      supervisor = nil,
      stats = newStats(),
      isPoisoned = nil
    )

    let ingestResult = newIngestion(keyspace)
    check ingestResult.isOk
    let ingestion = ingestResult.value
    check ingestion.len == 0
    check ingestion.size == 0'u64
    check not ingestion.isFinished

  test "write adds entries to ingestion buffer":
    let tempDir = createTempDir("ingest_test_", "")
    defer: removeDir(tempDir)

    let config = lsm_tree.newConfig(tempDir)
    let tree = lsm_tree.open(config)

    let keyspace = newKeyspace(
      keyspaceId = 1'u64,
      tree = tree,
      name = "test_ks",
      config = CreateOptions(),
      supervisor = nil,
      stats = newStats(),
      isPoisoned = nil
    )

    var ingestResult = newIngestion(keyspace)
    check ingestResult.isOk
    var ingestion = ingestResult.value

    # Write some entries
    check ingestion.write("key1", "value1").isOk
    check ingestion.write("key2", "value2").isOk
    check ingestion.write("key3", "value3").isOk

    check ingestion.len == 3
    check ingestion.size == 30'u64 # 3 * (4+6) = 30

  test "writeTombstone adds tombstone entries":
    let tempDir = createTempDir("ingest_test_", "")
    defer: removeDir(tempDir)

    let config = lsm_tree.newConfig(tempDir)
    let tree = lsm_tree.open(config)

    let keyspace = newKeyspace(
      keyspaceId = 1'u64,
      tree = tree,
      name = "test_ks",
      config = CreateOptions(),
      supervisor = nil,
      stats = newStats(),
      isPoisoned = nil
    )

    var ingestResult = newIngestion(keyspace)
    check ingestResult.isOk
    var ingestion = ingestResult.value

    check ingestion.writeTombstone("deleted_key").isOk
    check ingestion.len == 1

  test "writeWeakTombstone adds weak tombstone entries":
    let tempDir = createTempDir("ingest_test_", "")
    defer: removeDir(tempDir)

    let config = lsm_tree.newConfig(tempDir)
    let tree = lsm_tree.open(config)

    let keyspace = newKeyspace(
      keyspaceId = 1'u64,
      tree = tree,
      name = "test_ks",
      config = CreateOptions(),
      supervisor = nil,
      stats = newStats(),
      isPoisoned = nil
    )

    var ingestResult = newIngestion(keyspace)
    check ingestResult.isOk
    var ingestion = ingestResult.value

    check ingestion.writeWeakTombstone("weak_deleted").isOk
    check ingestion.len == 1

  test "strictOrder rejects out-of-order keys":
    let tempDir = createTempDir("ingest_test_", "")
    defer: removeDir(tempDir)

    let config = lsm_tree.newConfig(tempDir)
    let tree = lsm_tree.open(config)

    let keyspace = newKeyspace(
      keyspaceId = 1'u64,
      tree = tree,
      name = "test_ks",
      config = CreateOptions(),
      supervisor = nil,
      stats = newStats(),
      isPoisoned = nil
    )

    var ingestResult = newIngestion(keyspace, strictOrder = true)
    check ingestResult.isOk
    var ingestion = ingestResult.value

    # In-order writes should succeed
    check ingestion.write("a", "value_a").isOk
    check ingestion.write("b", "value_b").isOk
    check ingestion.write("c", "value_c").isOk

    # Out-of-order write should fail
    let result = ingestion.write("a", "value_a2")
    check result.isErr
    check result.error.kind == seInvalidArgument

  test "finish with empty ingestion returns 0":
    let tempDir = createTempDir("ingest_test_", "")
    defer: removeDir(tempDir)

    let config = lsm_tree.newConfig(tempDir)
    let tree = lsm_tree.open(config)

    let keyspace = newKeyspace(
      keyspaceId = 1'u64,
      tree = tree,
      name = "test_ks",
      config = CreateOptions(),
      supervisor = nil,
      stats = newStats(),
      isPoisoned = nil
    )

    var ingestResult = newIngestion(keyspace)
    check ingestResult.isOk
    var ingestion = ingestResult.value

    let finishResult = ingestion.finish()
    check finishResult.isOk
    check finishResult.value == 0'u64
    check ingestion.isFinished

  test "finish creates SSTables and registers in tree":
    let tempDir = createTempDir("ingest_test_", "")
    defer: removeDir(tempDir)

    let config = lsm_tree.newConfig(tempDir)
    let tree = lsm_tree.open(config)

    let keyspace = newKeyspace(
      keyspaceId = 1'u64,
      tree = tree,
      name = "test_ks",
      config = CreateOptions(),
      supervisor = nil,
      stats = newStats(),
      isPoisoned = nil
    )

    var ingestResult = newIngestion(keyspace)
    check ingestResult.isOk
    var ingestion = ingestResult.value

    # Write some entries
    for i in 0..<10:
      check ingestion.write("key" & $i, "value" & $i).isOk

    check ingestion.len == 10

    # Finish ingestion
    let finishResult = ingestion.finish()
    check finishResult.isOk
    check finishResult.value == 10'u64

    # Verify entries are now in the tree
    tree.versionLock.acquire()
    defer: tree.versionLock.release()

    check tree.tables[0].len > 0
    check ingestion.isFinished

  test "finish sorts unsorted entries":
    let tempDir = createTempDir("ingest_test_", "")
    defer: removeDir(tempDir)

    let config = lsm_tree.newConfig(tempDir)
    let tree = lsm_tree.open(config)

    let keyspace = newKeyspace(
      keyspaceId = 1'u64,
      tree = tree,
      name = "test_ks",
      config = CreateOptions(),
      supervisor = nil,
      stats = newStats(),
      isPoisoned = nil
    )

    var ingestResult = newIngestion(keyspace)
    check ingestResult.isOk
    var ingestion = ingestResult.value

    # Write entries out of order
    check ingestion.write("z", "value_z").isOk
    check ingestion.write("a", "value_a").isOk
    check ingestion.write("m", "value_m").isOk

    # Finish should sort and succeed
    let finishResult = ingestion.finish()
    check finishResult.isOk
    check finishResult.value == 3'u64

  test "write after finish fails":
    let tempDir = createTempDir("ingest_test_", "")
    defer: removeDir(tempDir)

    let config = lsm_tree.newConfig(tempDir)
    let tree = lsm_tree.open(config)

    let keyspace = newKeyspace(
      keyspaceId = 1'u64,
      tree = tree,
      name = "test_ks",
      config = CreateOptions(),
      supervisor = nil,
      stats = newStats(),
      isPoisoned = nil
    )

    var ingestResult = newIngestion(keyspace)
    check ingestResult.isOk
    var ingestion = ingestResult.value

    # Finish immediately
    let finishResult = ingestion.finish()
    check finishResult.isOk

    # Writing after finish should fail
    let writeResult = ingestion.write("key", "value")
    check writeResult.isErr
    check writeResult.error.kind == seInvalidState

  test "key too large is rejected":
    let tempDir = createTempDir("ingest_test_", "")
    defer: removeDir(tempDir)

    let config = lsm_tree.newConfig(tempDir)
    let tree = lsm_tree.open(config)

    let keyspace = newKeyspace(
      keyspaceId = 1'u64,
      tree = tree,
      name = "test_ks",
      config = CreateOptions(),
      supervisor = nil,
      stats = newStats(),
      isPoisoned = nil
    )

    var ingestResult = newIngestion(keyspace)
    check ingestResult.isOk
    var ingestion = ingestResult.value

    # Key > 65536 bytes
    let hugeKey = newString(70000)
    let result = ingestion.write(hugeKey, "value")
    check result.isErr
    check result.error.kind == seInvalidArgument

  test "finish splits into multiple SSTables when large":
    let tempDir = createTempDir("ingest_test_", "")
    defer: removeDir(tempDir)

    let config = lsm_tree.newConfig(tempDir)
    let tree = lsm_tree.open(config)

    let keyspace = newKeyspace(
      keyspaceId = 1'u64,
      tree = tree,
      name = "test_ks",
      config = CreateOptions(),
      supervisor = nil,
      stats = newStats(),
      isPoisoned = nil
    )

    # Use small table size to force splitting
    var ingestResult = newIngestion(keyspace, maxTableSize = 100)
    check ingestResult.isOk
    var ingestion = ingestResult.value

    # Write enough entries to span multiple tables
    for i in 0..<50:
      check ingestion.write("key" & $i, "value" & $i).isOk

    # Finish should create multiple SSTables
    let finishResult = ingestion.finish()
    check finishResult.isOk
    check finishResult.value == 50'u64

    # Verify multiple tables created
    tree.versionLock.acquire()
    defer: tree.versionLock.release()

    check tree.tables[0].len > 1 # More than one table

suite "Ingestion with mixed entries":

  test "mix of values and tombstones":
    let tempDir = createTempDir("ingest_test_", "")
    defer: removeDir(tempDir)

    let config = lsm_tree.newConfig(tempDir)
    let tree = lsm_tree.open(config)

    let keyspace = newKeyspace(
      keyspaceId = 1'u64,
      tree = tree,
      name = "test_ks",
      config = CreateOptions(),
      supervisor = nil,
      stats = newStats(),
      isPoisoned = nil
    )

    var ingestResult = newIngestion(keyspace)
    check ingestResult.isOk
    var ingestion = ingestResult.value

    check ingestion.write("key1", "value1").isOk
    check ingestion.writeTombstone("key2").isOk
    check ingestion.write("key3", "value3").isOk
    check ingestion.writeWeakTombstone("key4").isOk

    check ingestion.len == 4

    let finishResult = ingestion.finish()
    check finishResult.isOk
    check finishResult.value == 4'u64
