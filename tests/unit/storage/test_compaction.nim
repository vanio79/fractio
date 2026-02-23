# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Compaction Unit Test
##
## Tests that verify compaction works correctly with small datasets.

import unittest
import fractio/storage/db as db_module
import fractio/storage.db_config as db_config_module
import fractio/storage.keyspace as ks_module
import fractio.storage.error
import std/[os, options]

const TestDbPath = "tmp/test_compaction_small"

proc cleanupTestDir(path: string) =
  if dirExists(path):
    try:
      removeDir(path)
    except OSError:
      discard

suite "Compaction Unit Tests":

  setup:
    cleanupTestDir(TestDbPath)

  teardown:
    try:
      GC_fullCollect()
      sleep(50)
      cleanupTestDir(TestDbPath)
    except:
      discard

  test "Manual compaction with tiny dataset":
    echo "\n  [START] Tiny manual compaction test"

    let config = db_config_module.newConfig(TestDbPath)
    let dbResult = db_module.open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test")
    check ksResult.isOk
    let ks = ksResult.value

    # Write TINY amount of data - 10 records
    for i in 0..<10:
      discard ks.insert("key_" & $i, "value_" & $i)

    # Flush to create SSTable
    discard ks.rotateMemtable()
    discard ks.flushOldestSealed()
    sleep(50)

    let l0Before = ks.l0TableCount()
    echo "    L0 before compaction: ", l0Before

    if l0Before > 0:
      # Trigger manual compaction
      let compactResult = ks.majorCompaction()
      if compactResult.isErr:
        echo "    Compaction error: ", $compactResult.error
      check compactResult.isOk

      let l0After = ks.l0TableCount()
      echo "    L0 after compaction: ", l0After

    db.close()
    echo "  [DONE] Tiny manual compaction test"

  test "Data integrity after compaction":
    echo "\n  [START] Data integrity test"

    let config = db_config_module.newConfig(TestDbPath)
    let dbResult = db_module.open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("test")
    check ksResult.isOk
    let ks = ksResult.value

    # Write known data
    for i in 0..<50:
      discard ks.insert("key_" & $i, "value_" & $i)

    # Flush
    discard ks.rotateMemtable()
    discard ks.flushOldestSealed()
    sleep(50)

    # Verify before compaction
    var beforeCount = 0
    for i in 0..<50:
      let r = ks.get("key_" & $i)
      if r.isOk and r.value.isSome:
        beforeCount += 1
    echo "    Verified before compaction: ", beforeCount, "/50"

    # Compact
    let l0Before = ks.l0TableCount()
    if l0Before > 0:
      let cr = ks.majorCompaction()
      if cr.isErr:
        echo "    Compaction error: ", $cr.error
      check cr.isOk

    # Verify after
    var afterCount = 0
    for i in 0..<50:
      let r = ks.get("key_" & $i)
      if r.isOk and r.value.isSome:
        if r.value.get == "value_" & $i:
          afterCount += 1
    echo "    Verified after compaction: ", afterCount, "/50"

    check afterCount == 50

    db.close()
    echo "  [DONE] Data integrity test"
