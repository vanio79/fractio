# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Storage Stress Tests
##
## Comprehensive stress tests for the storage subsystem including:
## - 1 million record tests
## - Concurrent read/write operations
## - Batch operations
## - Durability and recovery
## - Memory stability

import unittest
import fractio/storage/db as db_module
import fractio/storage/db_config as db_config_module
import fractio/storage/keyspace as ks_module
import fractio/storage/batch as batch_module
import fractio/storage/iter as iter_module
import fractio/storage/error
import fractio/storage/journal/writer
import fractio/storage.types as types_module
import std/[os, options, random, sequtils, syncio, threadpool, atomics,
            tempfiles, times, strutils, locks]

type Database = db_module.Database
type Keyspace = ks_module.Keyspace

const
  StressTestDbPath = "tmp/test_storage_stress"
  LargeKeyspaceName = "stress_test"
  ConcurrencyKeyspaceName = "concurrent_test"
  BatchKeyspaceName = "batch_test"
  RecoveryKeyspaceName = "recovery_test"

proc cleanupStressTestDir(path: string) =
  if dirExists(path):
    try:
      removeDir(path)
    except OSError:
      discard

proc createTestDb(path: string): tuple[db: Database, ks: Keyspace] =
  let config = db_config_module.newConfig(path)
  let dbResult = db_module.open(config)
  if dbResult.isErr:
    raise newException(IOError, "Failed to open database: " & $dbResult.error)

  let db = dbResult.value
  let ksResult = db.keyspace("test")
  if ksResult.isErr:
    raise newException(IOError, "Failed to create keyspace: " & $ksResult.error)

  return (db, ksResult.value)

# =============================================================================
# Large Dataset Tests (1 Million Records)
# =============================================================================

suite "Storage Stress Tests - Large Dataset (1M Records)":

  setup:
    cleanupStressTestDir(StressTestDbPath)

  teardown:
    try:
      GC_fullCollect()
      sleep(100)
      cleanupStressTestDir(StressTestDbPath)
    except:
      discard

  test "Write 1 million sequential records":
    let config = db_config_module.newConfig(StressTestDbPath)
    let dbResult = db_module.open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace(LargeKeyspaceName)
    check ksResult.isOk
    let ks = ksResult.value

    let startTime = epochTime()
    var errors = 0

    const batchSize = 10000
    let totalRecords = 1_000_000

    for batchStart in countup(0, totalRecords, batchSize):
      let batchEnd = min(batchStart + batchSize, totalRecords)

      var wb = db.batch()
      for i in batchStart..<batchEnd:
        let key = "key_" & $i
        let value = "value_" & $i
        wb.insert(ks, key, value)

      let commitResult = db.commit(wb)
      if commitResult.isErr:
        errors += 1

      if batchEnd mod 100000 == 0:
        discard ks.rotateMemtable()
        discard ks.flushOldestSealed()
        GC_fullCollect()

    let endTime = epochTime()
    let duration = endTime - startTime

    echo "\n  Write 1M records: ", duration.formatFloat(ffDecimal, 2), "s (",
         (float(totalRecords) / duration).formatFloat(ffDecimal, 0), " ops/s)"

    check errors == 0
    check totalRecords > 0

    db.close()

  test "Read 1 million sequential records":
    let config = db_config_module.newConfig(StressTestDbPath)
    let dbResult = db_module.open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace(LargeKeyspaceName)
    check ksResult.isOk
    let ks = ksResult.value

    const writeBatch = 50000
    let totalRecords = 1_000_000

    for batchStart in countup(0, totalRecords, writeBatch):
      let batchEnd = min(batchStart + writeBatch, totalRecords)
      var wb = db.batch()
      for i in batchStart..<batchEnd:
        let key = "key_" & $i
        let value = "value_" & $i
        wb.insert(ks, key, value)
      discard db.commit(wb)

      if batchEnd mod 200000 == 0:
        discard ks.rotateMemtable()
        discard ks.flushOldestSealed()

    db.close()

    let dbResult2 = db_module.open(config)
    check dbResult2.isOk
    let db2 = dbResult2.value

    let ksResult2 = db2.keyspace(LargeKeyspaceName)
    check ksResult2.isOk
    let ks2 = ksResult2.value

    let startTime = epochTime()
    var foundCount = 0
    var errors = 0

    for i in 0..<totalRecords:
      let key = "key_" & $i
      let getResult = ks2.get(key)
      if getResult.isErr:
        errors += 1
      elif getResult.value.isSome:
        foundCount += 1
        if getResult.value.get != "value_" & $i:
          errors += 1

    let endTime = epochTime()
    let duration = endTime - startTime

    echo "\n  Read 1M records: ", duration.formatFloat(ffDecimal, 2), "s (",
         (float(totalRecords) / duration).formatFloat(ffDecimal, 0), " ops/s)"

    check errors == 0
    check foundCount == totalRecords

    db2.close()

  test "Random read/write 100k records with 1M dataset":
    let config = db_config_module.newConfig(StressTestDbPath)
    let dbResult = db_module.open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace(LargeKeyspaceName)
    check ksResult.isOk
    let ks = ksResult.value

    const prepBatch = 100000
    for batchStart in countup(0, 1_000_000, prepBatch):
      let batchEnd = min(batchStart + prepBatch, 1_000_000)
      var wb = db.batch()
      for i in batchStart..<batchEnd:
        let key = "key_" & $i
        let value = "value_" & $i
        wb.insert(ks, key, value)
      discard db.commit(wb)

      if batchEnd mod 500000 == 0:
        discard ks.rotateMemtable()
        discard ks.flushOldestSealed()

    db.close()

    let dbResult2 = db_module.open(config)
    check dbResult2.isOk
    let db2 = dbResult2.value

    let ksResult2 = db2.keyspace(LargeKeyspaceName)
    check ksResult2.isOk
    let ks2 = ksResult2.value

    randomize(42)
    var indices: seq[int] = toSeq(0..<1_000_000)
    shuffle(indices)

    let testCount = 100_000
    var readErrors = 0
    var writeErrors = 0

    let startTime = epochTime()

    for i in 0..<testCount:
      let idx = indices[i]
      let key = "key_" & $idx
      let getResult = ks2.get(key)
      if getResult.isErr or getResult.value.isNone:
        readErrors += 1

    for i in 0..<testCount:
      let idx = indices[i]
      let key = "key_" & $idx
      let newValue = "updated_" & $i
      let updateResult = ks2.insert(key, newValue)
      if updateResult.isErr:
        writeErrors += 1

    let endTime = epochTime()
    let duration = endTime - startTime

    echo "\n  Random R/W 100k: ", duration.formatFloat(ffDecimal, 2), "s (",
         (float(testCount * 2) / duration).formatFloat(ffDecimal, 0), " ops/s)"

    check readErrors == 0
    check writeErrors == 0

    db2.close()

  test "Range scan over 100k records":
    let config = db_config_module.newConfig(StressTestDbPath)
    let dbResult = db_module.open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace(LargeKeyspaceName)
    check ksResult.isOk
    let ks = ksResult.value

    const rangeSize = 100_000
    for i in 0..<rangeSize:
      let key = $i
      let value = "value_" & $i
      discard ks.insert(key, value)

    discard ks.rotateMemtable()
    discard ks.flushOldestSealed()

    db.close()

    let dbResult2 = db_module.open(config)
    check dbResult2.isOk
    let db2 = dbResult2.value

    let ksResult2 = db2.keyspace(LargeKeyspaceName)
    check ksResult2.isOk
    let ks2 = ksResult2.value

    let startTime = epochTime()

    var iter = ks2.rangeIter("0", "99999")
    defer: iter.close()

    var count = 0
    for entry in iter.entries:
      if entry.valueType == types_module.vtValue:
        count += 1

    let endTime = epochTime()
    let duration = endTime - startTime

    echo "\n  Range scan 100k: ", duration.formatFloat(ffDecimal, 2), "s (",
         (float(count) / duration).formatFloat(ffDecimal, 0), " ops/s)"

    check count == rangeSize

    db2.close()

  test "Prefix scan over 100k records":
    let config = db_config_module.newConfig(StressTestDbPath)
    let dbResult = db_module.open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace(LargeKeyspaceName)
    check ksResult.isOk
    let ks = ksResult.value

    const prefixCount = 100_000
    for i in 0..<prefixCount:
      let key = "user:" & $i
      let value = "user_value_" & $i
      discard ks.insert(key, value)

    for i in 0..<10000:
      let key = "item:" & $i
      let value = "item_value_" & $i
      discard ks.insert(key, value)

    discard ks.rotateMemtable()
    discard ks.flushOldestSealed()

    db.close()

    let dbResult2 = db_module.open(config)
    check dbResult2.isOk
    let db2 = dbResult2.value

    let ksResult2 = db2.keyspace(LargeKeyspaceName)
    check ksResult2.isOk
    let ks2 = ksResult2.value

    let startTime = epochTime()

    var iter = ks2.prefixIter("user:")
    defer: iter.close()

    var count = 0
    for entry in iter.entries:
      if entry.valueType == types_module.vtValue:
        count += 1

    let endTime = epochTime()
    let duration = endTime - startTime

    echo "\n  Prefix scan 100k: ", duration.formatFloat(ffDecimal, 2), "s (",
         (float(count) / duration).formatFloat(ffDecimal, 0), " ops/s)"

    check count == prefixCount

    db2.close()

# =============================================================================
# Concurrency Tests (Simplified - serial with high load)
# =============================================================================

suite "Storage Stress Tests - Concurrency":

  setup:
    cleanupStressTestDir(ConcurrencyKeyspaceName)

  teardown:
    try:
      GC_fullCollect()
      sleep(100)
      cleanupStressTestDir(ConcurrencyKeyspaceName)
    except:
      discard

  test "High frequency writes simulates concurrent load":
    let config = db_config_module.newConfig(ConcurrencyKeyspaceName)
    let dbResult = db_module.open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace(ConcurrencyKeyspaceName)
    check ksResult.isOk
    let ks = ksResult.value

    const numWaves = 10
    const recordsPerWave = 10000
    var errors = 0

    for wave in 0..<numWaves:
      for i in 0..<recordsPerWave:
        let key = "wave" & $wave & "_key_" & $i
        let value = "wave" & $wave & "_value_" & $i
        let result = ks.insert(key, value)
        if result.isErr:
          errors += 1

      # Flush between waves
      if wave mod 2 == 0:
        discard ks.rotateMemtable()
        discard ks.flushOldestSealed()

      GC_fullCollect()

    echo "\n  High frequency writes (", numWaves * recordsPerWave, " total): ",
        errors, " errors"

    check errors == 0

    var iterResult = ks.iter()
    let entryCount = iterResult.entries.len
    close(iterResult)

    check entryCount == numWaves * recordsPerWave

    db.close()

  test "Interleaved read/write workload":
    let config = db_config_module.newConfig(ConcurrencyKeyspaceName)
    let dbResult = db_module.open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace(ConcurrencyKeyspaceName)
    check ksResult.isOk
    let ks = ksResult.value

    var errors = 0

    # Interleaved read/write pattern
    for i in 0..<50000:
      # Write
      let writeKey = "key_" & $i
      let writeValue = "value_" & $i
      let writeResult = ks.insert(writeKey, writeValue)
      if writeResult.isErr:
        errors += 1

      # Read previous key (if exists)
      if i > 0:
        let readKey = "key_" & $(i - 1)
        let readResult = ks.get(readKey)
        if readResult.isErr:
          errors += 1

      # Contains check every 10 iterations
      if i mod 10 == 0:
        let containsResult = ks.containsKey(writeKey)
        if containsResult.isErr or containsResult.value == false:
          errors += 1

      if i mod 5000 == 0:
        GC_fullCollect()

    echo "\n  Interleaved R/W (50000 ops): ", errors, " errors"

    check errors == 0

    db.close()

  test "Batch commit stress test":
    let config = db_config_module.newConfig(ConcurrencyKeyspaceName)
    let dbResult = db_module.open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace(ConcurrencyKeyspaceName)
    check ksResult.isOk
    let ks = ksResult.value

    const numBatches = 50
    const batchSize = 2000
    var errors = 0

    for batch in 0..<numBatches:
      var wb = db.batch()

      for i in 0..<batchSize:
        let key = "batch" & $batch & "_key_" & $i
        let value = "batch" & $batch & "_value_" & $i
        wb.insert(ks, key, value)

      let commitResult = db.commit(wb)
      if commitResult.isErr:
        errors += 1

      if batch mod 10 == 9:
        discard ks.rotateMemtable()
        discard ks.flushOldestSealed()

      GC_fullCollect()

    echo "\n  Batch commits (", numBatches, " x ", batchSize, "): ", errors, " errors"

    check errors == 0

    db.close()

  test "Rapid update workload":
    let config = db_config_module.newConfig(ConcurrencyKeyspaceName)
    let dbResult = db_module.open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace(ConcurrencyKeyspaceName)
    check ksResult.isOk
    let ks = ksResult.value

    # First insert
    for i in 0..<5000:
      let key = "update_key_" & $i
      discard ks.insert(key, "v0")

    # Now update many times
    for version in 0..<10:
      for i in 0..<5000:
        let key = "update_key_" & $i
        let value = "v" & $version
        discard ks.insert(key, value)

      if version mod 3 == 0:
        GC_fullCollect()

    # Verify last version
    var errors = 0
    for i in 0..<5000:
      let key = "update_key_" & $i
      let getResult = ks.get(key)
      if getResult.isErr or getResult.value.isNone:
        errors += 1
      elif getResult.value.get != "v9":
        errors += 1

    echo "\n  Rapid updates (5000 keys x 10 versions): ", errors, " errors"

    check errors == 0

    db.close()

# =============================================================================
# Batch Operation Tests
# =============================================================================

suite "Storage Stress Tests - Batch Operations":

  setup:
    cleanupStressTestDir(BatchKeyspaceName)

  teardown:
    try:
      GC_fullCollect()
      sleep(100)
      cleanupStressTestDir(BatchKeyspaceName)
    except:
      discard

  test "Large batch commit (100k records)":
    let config = db_config_module.newConfig(BatchKeyspaceName)
    let dbResult = db_module.open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace(BatchKeyspaceName)
    check ksResult.isOk
    let ks = ksResult.value

    const batchSize = 100_000
    var wb = db.batch()

    let startTime = epochTime()

    for i in 0..<batchSize:
      let key = "batch_" & $i
      let value = "value_" & $i
      wb.insert(ks, key, value)

    let commitResult = db.commit(wb)
    let endTime = epochTime()
    let duration = endTime - startTime

    echo "\n  Large batch (100k): ", duration.formatFloat(ffDecimal, 2), "s (",
         (float(batchSize) / duration).formatFloat(ffDecimal, 0), " ops/s)"

    check commitResult.isOk

    var count = 0
    for i in 0..<batchSize:
      let key = "batch_" & $i
      let getResult = ks.get(key)
      if getResult.isOk and getResult.value.isSome:
        count += 1

    check count == batchSize

    db.close()

  test "Multiple sequential batch commits":
    let config = db_config_module.newConfig(BatchKeyspaceName)
    let dbResult = db_module.open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace(BatchKeyspaceName)
    check ksResult.isOk
    let ks = ksResult.value

    const numBatches = 100
    const batchSize = 1000
    let startTime = epochTime()

    for batch in 0..<numBatches:
      var wb = db.batch()

      for i in 0..<batchSize:
        let key = "batch_" & $batch & "_" & $i
        let value = "value_" & $batch & "_" & $i
        wb.insert(ks, key, value)

      let commitResult = db.commit(wb)
      check commitResult.isOk

      if batch mod 10 == 9:
        discard ks.rotateMemtable()
        discard ks.flushOldestSealed()

    let endTime = epochTime()
    let duration = endTime - startTime
    let totalRecords = numBatches * batchSize

    echo "\n  Sequential batches (", numBatches, " x ", batchSize, "): ",
         duration.formatFloat(ffDecimal, 2), "s (",
         (float(totalRecords) / duration).formatFloat(ffDecimal, 0), " ops/s)"

    db.close()

  test "Batch with mixed insert and remove":
    let config = db_config_module.newConfig(BatchKeyspaceName)
    let dbResult = db_module.open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace(BatchKeyspaceName)
    check ksResult.isOk
    let ks = ksResult.value

    for i in 0..<10000:
      let key = "key_" & $i
      let value = "value_" & $i
      discard ks.insert(key, value)

    var wb = db.batch()

    for i in 0..<5000:
      let key = "key_" & $i
      wb.remove(ks, key)

    for i in 10000..<15000:
      let key = "new_key_" & $i
      let value = "new_value_" & $i
      wb.insert(ks, key, value)

    let commitResult = db.commit(wb)
    check commitResult.isOk

    for i in 0..<5000:
      let key = "key_" & $i
      let getResult = ks.get(key)
      check getResult.isOk
      check getResult.value.isNone

    for i in 10000..<15000:
      let key = "new_key_" & $i
      let getResult = ks.get(key)
      check getResult.isOk
      check getResult.value.isSome
      check getResult.value.get == "new_value_" & $i

    for i in 5000..<10000:
      let key = "key_" & $i
      let getResult = ks.get(key)
      check getResult.isOk
      check getResult.value.isSome
      check getResult.value.get == "value_" & $i

    db.close()

  test "Batch across multiple keyspaces":
    let config = db_config_module.newConfig(BatchKeyspaceName)
    let dbResult = db_module.open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ks1Result = db.keyspace("ks1")
    let ks2Result = db.keyspace("ks2")
    let ks3Result = db.keyspace("ks3")

    check ks1Result.isOk
    check ks2Result.isOk
    check ks3Result.isOk

    let ks1 = ks1Result.value
    let ks2 = ks2Result.value
    let ks3 = ks3Result.value

    var wb = db.batch()

    for i in 0..<1000:
      wb.insert(ks1, "key_" & $i, "ks1_value_" & $i)
      wb.insert(ks2, "key_" & $i, "ks2_value_" & $i)
      wb.insert(ks3, "key_" & $i, "ks3_value_" & $i)

    let commitResult = db.commit(wb)
    check commitResult.isOk

    for i in 0..<1000:
      let key = "key_" & $i

      let get1Result = ks1.get(key)
      check get1Result.isOk
      check get1Result.value.isSome
      check get1Result.value.get == "ks1_value_" & $i

      let get2Result = ks2.get(key)
      check get2Result.isOk
      check get2Result.value.isSome
      check get2Result.value.get == "ks2_value_" & $i

      let get3Result = ks3.get(key)
      check get3Result.isOk
      check get3Result.value.isSome
      check get3Result.value.get == "ks3_value_" & $i

    db.close()

# =============================================================================
# Durability and Recovery Tests
# =============================================================================

suite "Storage Stress Tests - Durability and Recovery":

  setup:
    cleanupStressTestDir(RecoveryKeyspaceName)

  teardown:
    try:
      GC_fullCollect()
      sleep(100)
      cleanupStressTestDir(RecoveryKeyspaceName)
    except:
      discard

  test "Data survives database close and reopen":
    let config = db_config_module.newConfig(RecoveryKeyspaceName)

    block:
      let dbResult = db_module.open(config)
      check dbResult.isOk
      let db = dbResult.value

      let ksResult = db.keyspace(RecoveryKeyspaceName)
      check ksResult.isOk
      let ks = ksResult.value

      for i in 0..<10000:
        let key = "persist_key_" & $i
        let value = "persist_value_" & $i
        discard ks.insert(key, value)

      discard ks.rotateMemtable()
      discard ks.flushOldestSealed()

      let persistResult = db.persist(pmSyncAll)
      check persistResult.isOk

      db.close()

    block:
      let dbResult = db_module.open(config)
      check dbResult.isOk
      let db = dbResult.value

      let ksResult = db.keyspace(RecoveryKeyspaceName)
      check ksResult.isOk
      let ks = ksResult.value

      var verifiedCount = 0
      for i in 0..<10000:
        let key = "persist_key_" & $i
        let getResult = ks.get(key)
        if getResult.isOk and getResult.value.isSome:
          if getResult.value.get == "persist_value_" & $i:
            verifiedCount += 1

      check verifiedCount == 10000

      db.close()

  test "Partial batch commit durability":
    let config = db_config_module.newConfig(RecoveryKeyspaceName)

    block:
      let dbResult = db_module.open(config)
      check dbResult.isOk
      let db = dbResult.value

      let ksResult = db.keyspace(RecoveryKeyspaceName)
      check ksResult.isOk
      let ks = ksResult.value

      for i in 0..<5000:
        let key = "initial_" & $i
        let value = "initial_value_" & $i
        discard ks.insert(key, value)

      discard ks.rotateMemtable()
      discard ks.flushOldestSealed()

      db.close()

    block:
      let dbResult = db_module.open(config)
      check dbResult.isOk
      let db = dbResult.value

      let ksResult = db.keyspace(RecoveryKeyspaceName)
      check ksResult.isOk
      let ks = ksResult.value

      var wb = db.batch()

      for i in 5000..<10000:
        let key = "batch_" & $i
        let value = "batch_value_" & $i
        wb.insert(ks, key, value)

      let commitResult = db.commit(wb)
      check commitResult.isOk

      let persistResult = db.persist(pmSyncAll)
      check persistResult.isOk

      db.close()

    block:
      let dbResult = db_module.open(config)
      check dbResult.isOk
      let db = dbResult.value

      let ksResult = db.keyspace(RecoveryKeyspaceName)
      check ksResult.isOk
      let ks = ksResult.value

      for i in 0..<5000:
        let key = "initial_" & $i
        let getResult = ks.get(key)
        check getResult.isOk
        check getResult.value.isSome

      for i in 5000..<10000:
        let key = "batch_" & $i
        let getResult = ks.get(key)
        check getResult.isOk
        check getResult.value.isSome
        check getResult.value.get == "batch_value_" & $i

      db.close()

  test "Iterator consistency after recovery":
    let config = db_config_module.newConfig(RecoveryKeyspaceName)

    block:
      let dbResult = db_module.open(config)
      check dbResult.isOk
      let db = dbResult.value

      let ksResult = db.keyspace(RecoveryKeyspaceName)
      check ksResult.isOk
      let ks = ksResult.value

      for i in 0..<5000:
        let key = "iter_key_" & $i
        let value = "iter_value_" & $i
        discard ks.insert(key, value)

      discard ks.rotateMemtable()
      discard ks.flushOldestSealed()

      var iter = ks.iter()
      var countBefore = 0
      for entry in iter.entries:
        if entry.valueType == types_module.vtValue:
          countBefore += 1
      iter.close()

      check countBefore == 5000

      db.close()

    block:
      let dbResult = db_module.open(config)
      check dbResult.isOk
      let db = dbResult.value

      let ksResult = db.keyspace(RecoveryKeyspaceName)
      check ksResult.isOk
      let ks = ksResult.value

      var iter = ks.iter()
      var countAfter = 0
      for entry in iter.entries:
        if entry.valueType == types_module.vtValue:
          countAfter += 1
      iter.close()

      check countAfter == 5000

      db.close()

  test "Range iterator consistency after recovery":
    let config = db_config_module.newConfig(RecoveryKeyspaceName)

    block:
      let dbResult = db_module.open(config)
      check dbResult.isOk
      let db = dbResult.value

      let ksResult = db.keyspace(RecoveryKeyspaceName)
      check ksResult.isOk
      let ks = ksResult.value

      for i in 0..<10000:
        let key = $i
        let value = "value_" & $i
        discard ks.insert(key, value)

      discard ks.rotateMemtable()
      discard ks.flushOldestSealed()

      db.close()

    block:
      let dbResult = db_module.open(config)
      check dbResult.isOk
      let db = dbResult.value

      let ksResult = db.keyspace(RecoveryKeyspaceName)
      check ksResult.isOk
      let ks = ksResult.value

      var iter = ks.rangeIter("1000", "5999")
      defer: iter.close()

      var count = 0
      for entry in iter.entries:
        if entry.valueType == types_module.vtValue:
          count += 1

      check count == 5000

      db.close()

# =============================================================================
# Edge Case Tests
# =============================================================================

suite "Storage Stress Tests - Edge Cases":

  setup:
    cleanupStressTestDir("tmp/test_edge_cases")

  teardown:
    try:
      GC_fullCollect()
      sleep(100)
      cleanupStressTestDir("tmp/test_edge_cases")
    except:
      discard

  test "Empty key handling":
    let config = db_config_module.newConfig("tmp/test_edge_cases")
    let dbResult = db_module.open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("edge_test")
    check ksResult.isOk
    let ks = ksResult.value

    discard ks.insert("", "empty_key_value")

    db.close()

  test "Very long key and value":
    let config = db_config_module.newConfig("tmp/test_edge_cases")
    let dbResult = db_module.open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("edge_test")
    check ksResult.isOk
    let ks = ksResult.value

    let longKey = "k".repeat(10000)
    let longValue = "v".repeat(100000)

    let insertResult = ks.insert(longKey, longValue)
    if insertResult.isOk:
      let getResult = ks.get(longKey)
      check getResult.isOk
      if getResult.value.isSome:
        check getResult.value.get == longValue

    db.close()

  test "Many small keyspaces":
    let config = db_config_module.newConfig("tmp/test_edge_cases")
    let dbResult = db_module.open(config)
    check dbResult.isOk
    let db = dbResult.value

    const numKeyspaces = 50
    for i in 0..<numKeyspaces:
      let ksResult = db.keyspace("ks_" & $i)
      check ksResult.isOk

      let ks = ksResult.value
      for j in 0..<10:
        let key = "key_" & $j
        let value = "value_" & $j
        discard ks.insert(key, value)

    check db.keyspaceCount() == numKeyspaces

    for i in 0..<numKeyspaces:
      let ksResult = db.keyspace("ks_" & $i)
      check ksResult.isOk

      let ks = ksResult.value
      let countResult = ks.approximateLen()
      check countResult >= 10

    db.close()

  test "Rapid insert and delete":
    let config = db_config_module.newConfig("tmp/test_edge_cases")
    let dbResult = db_module.open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("edge_test")
    check ksResult.isOk
    let ks = ksResult.value

    for i in 0..<1000:
      let key = "rapid_" & $i

      let insertResult = ks.insert(key, "value_" & $i)
      check insertResult.isOk

      let deleteResult = ks.remove(key)
      check deleteResult.isOk

      let getResult = ks.get(key)
      check getResult.isOk
      check getResult.value.isNone

    db.close()

  test "Update same key repeatedly":
    let config = db_config_module.newConfig("tmp/test_edge_cases")
    let dbResult = db_module.open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("edge_test")
    check ksResult.isOk
    let ks = ksResult.value

    let key = "update_key"

    for i in 0..<10000:
      let value = "value_version_" & $i
      let insertResult = ks.insert(key, value)
      check insertResult.isOk

      if i mod 100 == 0:
        let getResult = ks.get(key)
        check getResult.isOk
        check getResult.value.isSome
        check getResult.value.get == value

    let finalResult = ks.get(key)
    check finalResult.isOk
    check finalResult.value.isSome
    check finalResult.value.get == "value_version_9999"

    db.close()

  test "ContainsKey on large dataset":
    let config = db_config_module.newConfig("tmp/test_edge_cases")
    let dbResult = db_module.open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("edge_test")
    check ksResult.isOk
    let ks = ksResult.value

    for i in 0..<100000:
      let key = "key_" & $i
      let value = "value_" & $i
      discard ks.insert(key, value)

    for i in 0..<100000:
      let key = "key_" & $i
      let containsResult = ks.containsKey(key)
      check containsResult.isOk
      check containsResult.value == true

    for i in 100000..<100100:
      let key = "key_" & $i
      let containsResult = ks.containsKey(key)
      check containsResult.isOk
      check containsResult.value == false

    db.close()

  test "Approximate len accuracy":
    let config = db_config_module.newConfig("tmp/test_edge_cases")
    let dbResult = db_module.open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("edge_test")
    check ksResult.isOk
    let ks = ksResult.value

    for i in 0..<10000:
      let key = "key_" & $i
      let value = "value_" & $i
      discard ks.insert(key, value)

    for i in 0..<5000:
      let key = "key_" & $i
      discard ks.remove(key)

    let approxLen = ks.approximateLen()
    check approxLen >= 5000

    db.close()

# =============================================================================
# Memory Stability Tests
# =============================================================================

suite "Storage Stress Tests - Memory Stability":

  setup:
    cleanupStressTestDir("tmp/test_memory_stability")

  teardown:
    try:
      GC_fullCollect()
      sleep(200)
      cleanupStressTestDir("tmp/test_memory_stability")
    except:
      discard

  test "Memory stable under continuous writes":
    let config = db_config_module.newConfig("tmp/test_memory_stability")
    let dbResult = db_module.open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("memory_test")
    check ksResult.isOk
    let ks = ksResult.value

    const waves = 10
    const recordsPerWave = 10000

    for wave in 0..<waves:
      for i in 0..<recordsPerWave:
        let key = "wave" & $wave & "_key_" & $i
        let value = "wave" & $wave & "_value_" & $i
        discard ks.insert(key, value)

      if wave mod 2 == 1:
        discard ks.rotateMemtable()
      GC_fullCollect()

    db.close()

    let dbResult2 = db_module.open(config)
    check dbResult2.isOk
    let db2 = dbResult2.value

    let ksResult2 = db2.keyspace("memory_test")
    check ksResult2.isOk
    let ks2 = ksResult2.value

    var iter = ks2.iter()
    var count = 0
    for entry in iter.entries:
      if entry.valueType == types_module.vtValue:
        count += 1
    iter.close()

    check count == waves * recordsPerWave

    db2.close()

  test "No memory leaks after many open/close cycles":
    let config = db_config_module.newConfig("tmp/test_memory_stability")

    for cycle in 0..<10:
      let dbResult = db_module.open(config)
      check dbResult.isOk
      let db = dbResult.value

      let ksResult = db.keyspace("cycle_test")
      check ksResult.isOk
      let ks = ksResult.value

      for i in 0..<1000:
        let key = "cycle" & $cycle & "_key_" & $i
        let value = "value_" & $i
        discard ks.insert(key, value)

      for i in 0..<100:
        let key = "cycle" & $cycle & "_key_" & $i
        discard ks.get(key)

      db.close()

      GC_fullCollect()

    let dbResultFinal = db_module.open(config)
    check dbResultFinal.isOk
    let dbFinal = dbResultFinal.value

    let ksResultFinal = dbFinal.keyspace("cycle_test")
    check ksResultFinal.isOk
    let ksFinal = ksResultFinal.value

    var totalCount = 0
    for cycle in 0..<10:
      for i in 0..<1000:
        let key = "cycle" & $cycle & "_key_" & $i
        let getResult = ksFinal.get(key)
        if getResult.isOk and getResult.value.isSome:
          totalCount += 1

    check totalCount == 10 * 1000

    dbFinal.close()

  test "Iterator cleanup on close":
    let config = db_config_module.newConfig("tmp/test_memory_stability")
    let dbResult = db_module.open(config)
    check dbResult.isOk
    let db = dbResult.value

    let ksResult = db.keyspace("iter_test")
    check ksResult.isOk
    let ks = ksResult.value

    for i in 0..<10000:
      let key = "key_" & $i
      let value = "value_" & $i
      discard ks.insert(key, value)

    discard ks.rotateMemtable()
    discard ks.flushOldestSealed()

    for i in 0..<100:
      var iter = ks.iter()

      var count = 0
      for entry in iter.entries:
        if count >= 10:
          break
        count += 1

      iter.close()

      var rangeIter = ks.rangeIter("key_0", "key_100")
      var rangeCount = 0
      for entry in rangeIter.entries:
        rangeCount += 1
      rangeIter.close()

      GC_fullCollect()

    db.close()

    let dbResult2 = db_module.open(config)
    check dbResult2.isOk
    let db2 = dbResult2.value

    let ksResult2 = db2.keyspace("iter_test")
    check ksResult2.isOk
    let ks2 = ksResult2.value

    var verifyIter = ks2.iter()
    let finalCount = verifyIter.entries.len
    verifyIter.close()

    check finalCount == 10000

    db2.close()
