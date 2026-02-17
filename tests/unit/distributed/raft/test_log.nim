# Unit tests for RaftLog
# 100% coverage target

import unittest
import os, random, times, strutils, posix, tables
import fractio/distributed/raft/log
import fractio/distributed/raft/types
import fractio/utils/logging
import fractio/core/errors

proc toSeq(s: string): seq[byte] =
  result = @[]
  for c in s:
    result.add(byte(c))

suite "RaftLog Unit Tests":

  var
    testDir: string
    log: RaftLog
    logger: Logger

  setup:
    testDir = getTempDir() / "raft_log_test_" & $getpid() & "_" & $rand(1000000)
    createDir(testDir)
    logger = Logger(name: "RaftLogTest", minLevel: llWarn, handlers: @[])
    log = newRaftLog(testDir, logger)

  teardown:
    if log != nil:
      log.close()
    try:
      removeDir(testDir)
    except:
      discard

  test "new log is empty":
    check log.lastIndex == 0'u64
    check log.lastTerm == 0'u64
    check log.snapshotIndex == 0'u64
    check log.snapshotTerm == 0'u64
    check log.indexMap.len() == 0

  test "append first entry":
    let entry = RaftEntry(
      term: 1'u64,
      index: 1'u64,
      command: RaftCommand(kind: rckClientCommand, data: "test".toSeq()),
      checksum: 0
    )
    let newIdx = log.append(entry)
    check newIdx == 1'u64
    check log.lastIndex == 1'u64
    check log.lastTerm == 1'u64
    check log.indexMap.len() == 1
    # Read back
    let readEntry = log.getEntry(1'u64)
    check readEntry.term == 1'u64
    check readEntry.index == 1'u64
    check readEntry.command.kind == rckClientCommand
    check readEntry.command.data == "test".toSeq()

  test "append multiple entries":
    for i in 1..5:
      let entry = RaftEntry(
        term: 1'u64,
        index: uint64(i),
        command: RaftCommand(kind: rckClientCommand, data: toSeq($i)),
        checksum: 0
      )
      discard log.append(entry)
    check log.lastIndex == 5'u64
    check log.lastTerm == 1'u64
    check log.indexMap.len() == 5
    for i in 1..5:
      let e = log.getEntry(uint64(i))
      check e.term == 1'u64
      check e.index == uint64(i)
      check e.command.data == toSeq($i)

  test "append must follow consecutive indices":
    # log currently up to 5
    let badEntry = RaftEntry(
      term: 1'u64,
      index: 7'u64,
      command: RaftCommand(kind: rckClientCommand, data: "bad".toSeq()),
      checksum: 0
    )
    expect FractioError:
      discard log.append(badEntry)

  test "getEntry out of range":
    expect FractioError:
      discard log.getEntry(999'u64)

  test "getEntry compacted by snapshot":
    # Create at least 3 entries
    for i in 1..3:
      let entry = RaftEntry(
        term: 1'u64,
        index: uint64(i),
        command: RaftCommand(kind: rckClientCommand, data: toSeq($i)),
        checksum: 0
      )
      discard log.append(entry)
    # Create snapshot at index 2
    let snapData = "snap2".toSeq()
    let snap = log.createSnapshot(2'u64, 1'u64, snapData)
    check log.snapshotIndex == 2'u64
    check log.snapshotTerm == 1'u64
    check log.indexMap.len() == 1 # only entry 3 remains
    # Entries 1 and 2 should raise (compacted)
    expect FractioError:
      discard log.getEntry(1'u64)
    expect FractioError:
      discard log.getEntry(2'u64)
    # Entry 3 still readable
    let e = log.getEntry(3'u64)
    check e.index == 3'u64

  test "append after snapshot":
    # Setup: append entries 1-3 and create snapshot at index 2
    for i in 1..3:
      let entry = RaftEntry(
        term: 1'u64,
        index: uint64(i),
        command: RaftCommand(kind: rckClientCommand, data: toSeq($i)),
        checksum: 0
      )
      discard log.append(entry)
    discard log.createSnapshot(2'u64, 1'u64, "snap".toSeq())
    # Now next append should be index 4
    let entry4 = RaftEntry(
      term: 1'u64,
      index: 4'u64,
      command: RaftCommand(kind: rckClientCommand, data: "4".toSeq()),
      checksum: 0
    )
    discard log.append(entry4)
    check log.lastIndex == 4'u64
    let e4 = log.getEntry(4'u64)
    check e4.command.data == "4".toSeq()

  test "createSnapshot requires valid index":
    # Empty log, can't create snapshot
    expect FractioError:
      discard log.createSnapshot(1'u64, 1'u64, "data".toSeq())
    # Also can't create snapshot beyond lastIndex
    # (lastIndex is 0)
    # Already covered

  test "installSnapshot resets log":
    # Install snapshot with index 10, term 2
    let snap = Snapshot(
      lastIndex: 10'u64,
      lastTerm: 2'u64,
      data: "bigsnap".toSeq()
    )
    log.installSnapshot(snap)
    check log.snapshotIndex == 10'u64
    check log.snapshotTerm == 2'u64
    check log.lastIndex == 0'u64
    check log.indexMap.len() == 0
    # Next append should be index 11
    let entry11 = RaftEntry(
      term: 3'u64,
      index: 11'u64,
      command: RaftCommand(kind: rckClientCommand, data: "11".toSeq()),
      checksum: 0
    )
    discard log.append(entry11)
    check log.lastIndex == 11'u64
    let e = log.getEntry(11'u64)
    check e.index == 11'u64
    # Old indices (1..10) should not exist
    for i in 1..10:
      expect FractioError:
        discard log.getEntry(uint64(i))

  test "recovery after close":
    # Create log, append entries, close, reopen
    for i in 1..3:
      let entry = RaftEntry(
        term: 1'u64,
        index: uint64(i),
        command: RaftCommand(kind: rckClientCommand, data: toSeq($i)),
        checksum: 0
      )
      discard log.append(entry)
    log.close()
    # Reopen
    log = newRaftLog(testDir, logger)
    check log.lastIndex == 3'u64
    check log.lastTerm == 1'u64
    check log.indexMap.len() == 3
    for i in 1..3:
      let e = log.getEntry(uint64(i))
      check e.command.data == toSeq($i)

  test "checksum verification detects corruption":
    # Append an entry with short data (len=7)
    let entry = RaftEntry(
      term: 1'u64,
      index: 1'u64,
      command: RaftCommand(kind: rckClientCommand, data: "corrupt".toSeq()),
      checksum: 0
    )
    discard log.append(entry)
    log.close()
    # Corrupt the stored checksum (last 4 bytes of the entry)
    var f = open(log.logPath, fmReadWriteExisting)
    # Compute offset of checksum: entry size = 8+8+1+4+7+4 = 32. Checksum at offset 28.
    f.setFilePos(28)
    var corruptByte: array[1, byte] = [0xFF'u8]
    discard writeBuffer(f, corruptByte[0].addr, 1)
    discard posix.fsync(getOsFileHandle(f))
    f.close()
    # Reopen and try to read
    log = newRaftLog(testDir, logger)
    expect FractioError:
      discard log.getEntry(1'u64)

  test "simulate incomplete entry tail truncation":
    # Append a valid entry with short data (len=2)
    let entry = RaftEntry(
      term: 1'u64,
      index: 1'u64,
      command: RaftCommand(kind: rckClientCommand, data: "ok".toSeq()),
      checksum: 0
    )
    discard log.append(entry)
    # Append garbage bytes to simulate incomplete write (incomplete header)
    var f = open(log.logPath, fmReadWriteExisting)
    let validSize = getFileSize(f) # should be 27
    f.setFilePos(validSize)
    let garbage = [0xDE'u8, 0xAD'u8, 0xBE'u8, 0xEF'u8]
    discard writeBuffer(f, garbage[0].addr, 4)
    discard posix.fsync(getOsFileHandle(f))
    f.close()
    # Reopen; the log should truncate the incomplete tail
    log = newRaftLog(testDir, logger)
    check log.lastIndex == 1'u64
    check log.indexMap.len() == 1
    let newSize = getFileSize(log.logFile)
    check newSize == validSize # original valid size (no extra)

  test "snapshot does not truncate log file automatically":
    # Append entries 1..3, create snapshot at 2; log file should retain all entries
    for i in 1..3:
      let entry = RaftEntry(
        term: 1'u64,
        index: uint64(i),
        command: RaftCommand(kind: rckClientCommand, data: toSeq($i)),
        checksum: 0
      )
      discard log.append(entry)
    let preSnapSize = getFileSize(log.logFile)
    discard log.createSnapshot(2'u64, 1'u64, "snap".toSeq())
    let postSnapSize = getFileSize(log.logFile)
    # File size unchanged (we didn't physically truncate)
    check postSnapSize == preSnapSize
    # But entries 1-2 compacted
    check log.indexMap.len() == 1
    # Entry 3 still there
    check log.getEntry(3'u64).index == 3'u64

  test "createSnapshot too large index fails":
    # lastIndex = 1, try index 2? That's allowed. Actually need index <= lastIndex.
    # We have entry 1, try snapshot index 2 > lastIndex -> fail
    expect FractioError:
      discard log.createSnapshot(2'u64, 1'u64, "snap".toSeq())

  test "installSnapshot clears log file":
    # Prepopulate some entries
    for i in 1..3:
      let entry = RaftEntry(
        term: 1'u64,
        index: uint64(i),
        command: RaftCommand(kind: rckClientCommand, data: toSeq($i)),
        checksum: 0
      )
      discard log.append(entry)
    let preSize = getFileSize(log.logFile)
    # Install snapshot
    let snap = Snapshot(lastIndex: 10'u64, lastTerm: 5'u64,
        data: "newsnap".toSeq())
    log.installSnapshot(snap)
    # Log file should be empty (size 0)
    let postSize = getFileSize(log.logFile)
    check postSize == 0

  when isMainModule:
    discard
