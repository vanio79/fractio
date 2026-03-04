# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Tests for compaction logic

import unittest
import fractio/storage/lsm_tree/compaction
import fractio/storage/lsm_tree/types
import fractio/storage/lsm_tree/sstable/writer
import fractio/storage/lsm_tree/sstable/reader
import fractio/storage/lsm_tree/memtable
import std/[os, strutils, tempfiles, options]

suite "Compaction Helper Tests":
  setup:
    let tempDir = createTempDir("compaction_test_", "")

  teardown:
    try:
      removeDir(tempDir)
    except OSError:
      discard

  test "mergeEntries with empty input":
    let allEntries: seq[seq[MergeEntry]] = @[]
    let result = mergeEntries(allEntries, 0)
    check result.isOk
    check result.value.len == 0

  test "mergeEntries with single source":
    var entries: seq[MergeEntry] = @[]
    entries.add(MergeEntry(key: "a", value: "1", seqno: 1, valueType: vtValue))
    entries.add(MergeEntry(key: "b", value: "2", seqno: 2, valueType: vtValue))
    entries.add(MergeEntry(key: "c", value: "3", seqno: 3, valueType: vtValue))

    let allEntries = @[entries]
    let result = mergeEntries(allEntries, 0)
    check result.isOk
    check result.value.len == 3
    check result.value[0].key == "a"
    check result.value[1].key == "b"
    check result.value[2].key == "c"

  test "mergeEntries keeps newest version of key":
    var entries1: seq[MergeEntry] = @[]
    entries1.add(MergeEntry(key: "a", value: "old", seqno: 1,
        valueType: vtValue))

    var entries2: seq[MergeEntry] = @[]
    entries2.add(MergeEntry(key: "a", value: "new", seqno: 10,
        valueType: vtValue))

    let allEntries = @[entries1, entries2]
    let result = mergeEntries(allEntries, 0)
    check result.isOk
    check result.value.len == 1
    check result.value[0].value == "new"
    check result.value[0].seqno == 10

  test "mergeEntries removes old tombstones":
    var entries: seq[MergeEntry] = @[]
    entries.add(MergeEntry(key: "a", value: "", seqno: 1,
        valueType: vtTombstone))
    entries.add(MergeEntry(key: "b", value: "keep", seqno: 2,
        valueType: vtValue))

    let allEntries = @[entries]
    # gcWatermark = 5, so seqno 1 tombstone should be removed
    let result = mergeEntries(allEntries, 5)
    check result.isOk
    check result.value.len == 1
    check result.value[0].key == "b"

  test "mergeEntries keeps recent tombstones":
    var entries: seq[MergeEntry] = @[]
    entries.add(MergeEntry(key: "a", value: "", seqno: 10,
        valueType: vtTombstone))
    entries.add(MergeEntry(key: "b", value: "keep", seqno: 2,
        valueType: vtValue))

    let allEntries = @[entries]
    # gcWatermark = 5, so seqno 10 tombstone should be kept
    let result = mergeEntries(allEntries, 5)
    check result.isOk
    check result.value.len == 2

  test "writeCompactedTables creates SSTables":
    var entries: seq[MergeEntry] = @[]
    for i in 0 ..< 100:
      entries.add(MergeEntry(
        key: "key" & intToStr(i, 4),
        value: "value" & $i,
        seqno: uint64(i),
        valueType: vtValue
      ))

    var tableIdCounter: uint64 = 1
    let result = writeCompactedTables(entries, tempDir, 1, 1024, tableIdCounter)
    check result.isOk
    check result.value.len > 0

    # Verify files exist
    let l1Path = tempDir / "L1"
    check dirExists(l1Path)

    for table in result.value:
      check fileExists(table.path)
      check table.level == 1

  test "readSsTableEntries roundtrip":
    # Create a memtable and write to SSTable
    let memtable = newMemtable()
    discard memtable.insert("key1", "value1", 1, vtValue)
    discard memtable.insert("key2", "value2", 2, vtValue)
    discard memtable.insert("key3", "value3", 3, vtValue)

    let sstablePath = tempDir / "test.sst"
    let writeResult = writeMemtable(sstablePath, memtable)
    check writeResult.isOk

    # Read entries back
    let readResult = readSsTableEntries(sstablePath)
    check readResult.isOk
    check readResult.value.len == 3

    # Verify entries
    var found = false
    for entry in readResult.value:
      if entry.key == "key1":
        check entry.value == "value1"
        check entry.seqno == 1
        found = true
    check found

  test "keyRangesOverlap detects overlapping ranges":
    check keyRangesOverlap(("a", "m"), ("k", "z")) == true
    check keyRangesOverlap(("a", "c"), ("d", "f")) == false
    check keyRangesOverlap(("a", "z"), ("b", "c")) == true
    check keyRangesOverlap(("", ""), ("a", "z")) == false

  test "Full compaction cycle":
    # Create multiple memtables with overlapping keys
    let memtable1 = newMemtable()
    discard memtable1.insert("key1", "v1_old", 1, vtValue)
    discard memtable1.insert("key2", "v2", 2, vtValue)
    discard memtable1.insert("deleted_key", "", 3, vtTombstone)

    let memtable2 = newMemtable()
    discard memtable2.insert("key1", "v1_new", 10, vtValue) # Overwrites old
    discard memtable2.insert("key3", "v3", 11, vtValue)

    # Create L0 directory and write SSTables
    let l0Path = tempDir / "L0"
    createDir(l0Path)

    let sstable1Result = writeMemtable(l0Path / "1.sst", memtable1)
    check sstable1Result.isOk

    let sstable2Result = writeMemtable(l0Path / "2.sst", memtable2)
    check sstable2Result.isOk

    # Read entries from both tables
    let entries1Result = readSsTableEntries(l0Path / "1.sst")
    let entries2Result = readSsTableEntries(l0Path / "2.sst")
    check entries1Result.isOk
    check entries2Result.isOk

    # Merge entries (gcWatermark = 5, so seqno 3 tombstone should be GC'd)
    let allEntries = @[entries1Result.value, entries2Result.value]
    let mergeResult = mergeEntries(allEntries, 5)
    check mergeResult.isOk

    let merged = mergeResult.value

    # Should have 3 entries: key1 (new version), key2, key3
    # deleted_key should be GC'd
    check merged.len == 3

    # Verify key1 has new value
    var key1Entry: MergeEntry
    for e in merged:
      if e.key == "key1":
        key1Entry = e
        break
    check key1Entry.value == "v1_new"
    check key1Entry.seqno == 10

    # Write compacted tables to L1
    var tableIdCounter: uint64 = 100
    let writeResult = writeCompactedTables(merged, tempDir, 1, 4096, tableIdCounter)
    check writeResult.isOk
    check writeResult.value.len >= 1

    # Verify data can be read from compacted tables
    for table in writeResult.value:
      let readerResult = openSsTable(table.path)
      if readerResult.isOk:
        let reader = readerResult.value
        check reader.get("key1") == some("v1_new")
        check reader.get("key2") == some("v2")
        check reader.get("key3") == some("v3")
        check reader.get("deleted_key") == none(string)
        reader.close()
