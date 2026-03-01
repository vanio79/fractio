# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License

## LSM Tree v2 - Write-Ahead Log (WAL)
## Append-only binary log for durability and crash recovery

import std/os
import std/posix
import std/locks
import std/atomics
import std/strutils
import types
import error
import memtable

# ============================================================================
# WAL Record Types
# ============================================================================

const
  WAL_TYPE_INSERT* = 0'u8
  WAL_TYPE_REMOVE* = 1'u8

# Helper: Decode 64-bit int from bytes
proc decodeFixed64*(data: string): int64 =
  result = 0
  for i in 0..<8:
    result = result or (int64(data[i].uint8) shl (i * 8))

# Helper: Encode 64-bit int to bytes
proc encodeFixed64*(value: int64): string =
  result = newString(8)
  for i in 0..<8:
    result[i] = chr((value shr (i * 8)) and 0xFF)

# Helper: Decode 32-bit int from bytes
proc decodeFixed32*(data: string): uint32 =
  result = 0
  for i in 0..<4:
    result = result or (uint32(data[i].uint8) shl (i * 8))

# Helper: Encode 32-bit int to bytes
proc encodeFixed32*(value: uint32): string =
  result = newString(4)
  for i in 0..<4:
    result[i] = chr((value shr (i * 8)) and 0xFF)

# Helper: Decode 64-bit from byte array
proc decodeFixed64*(data: array[8, uint8]): int64 =
  result = 0
  for i in 0..<8:
    result = result or (int64(data[i]) shl (i * 8))

# Helper: Decode 32-bit from byte array
proc decodeFixed32*(data: array[4, uint8]): uint32 =
  result = 0
  for i in 0..<4:
    result = result or (uint32(data[i]) shl (i * 8))

# ============================================================================
# WAL Manager
# ============================================================================

type
  WALManager* = ref object
    path*: string
    file*: cint
    writeLock*: Lock
    nextSeqno*: Atomic[int64]

proc newWALManager*(path: string): LsmResult[WALManager] =
  ## Create a new WAL manager. Creates directory if needed.
  try:
    if not os.dirExists(path):
      os.createDir(path)

    let walPath = path & "/wal"
    let fd = posix.open(cstring(walPath), O_WRONLY or O_CREAT, 0o644)
    if fd == -1:
      return err[WALManager](newIoError("Failed to create/open WAL file: " & walPath))

    var writeLock: Lock
    initLock(writeLock)

    var nextSeqno: Atomic[int64]
    nextSeqno.store(0, moRelaxed)

    let w = WALManager(
      path: path,
      file: fd,
      writeLock: writeLock,
      nextSeqno: nextSeqno
    )
    return ok(w)
  except:
    return err[WALManager](newIoError("Failed to initialize WAL manager: " &
        getCurrentExceptionMsg()))

proc appendWrite*(w: var WALManager, seqno: SeqNo, key: string,
    value: string): LsmResult[void] =
  ## Append an insert record to WAL
  try:
    acquire(w.writeLock)
    defer: release(w.writeLock)

    # Write: [8][1][4][key][4][value]
    var buf = newString(0)
    buf.add(encodeFixed64(seqno.int64))
    buf.add(chr(WAL_TYPE_INSERT))
    buf.add(encodeFixed32(key.len.uint32))
    buf.add(key)
    buf.add(encodeFixed32(value.len.uint32))
    buf.add(value)

    let bytesWritten = posix.write(w.file, cast[pointer](addr(buf[0])), buf.len)
    if bytesWritten != buf.len:
      return errVoid(newIoError("Failed to write full WAL insert record"))
    # NOTE: No fsync to match Rust lsm-tree behavior (in-memory only)

    # Advance next seqno (tracking is best-effort)
    discard fetchAdd(w.nextSeqno, 1, moRelaxed)

    okVoid()
  except:
    errVoid(newIoError("Failed to append WAL insert record: " &
        getCurrentExceptionMsg()))

proc appendRemove*(w: var WALManager, seqno: SeqNo, key: string): LsmResult[void] =
  ## Append a remove record to WAL
  try:
    acquire(w.writeLock)
    defer: release(w.writeLock)

    # Write: [8][1][4][key][0][0]
    var buf = newString(0)
    buf.add(encodeFixed64(seqno.int64))
    buf.add(chr(WAL_TYPE_REMOVE))
    buf.add(encodeFixed32(key.len.uint32))
    buf.add(key)
    buf.add(encodeFixed32(0)) # value len = 0

    let bytesWritten = posix.write(w.file, cast[pointer](addr(buf[0])), buf.len)
    if bytesWritten != buf.len:
      return errVoid(newIoError("Failed to write full WAL remove record"))
    # NOTE: No fsync to match Rust lsm-tree behavior (in-memory only)

    # Advance next seqno (tracking is best-effort)
    discard fetchAdd(w.nextSeqno, 1, moRelaxed)

    okVoid()
  except:
    errVoid(newIoError("Failed to append WAL remove record: " &
        getCurrentExceptionMsg()))

proc replay*(w: WALManager, memtable: var Memtable): LsmResult[void] =
  ## Replay WAL into memtable. Must be called on startup.
  try:
    let walPath = w.path & "/wal"
    if not os.fileExists(walPath):
      return okVoid() # No WAL, no recovery needed

    let fd = posix.open(cstring(walPath), cint(FileFlags.frRead))
    if fd == -1:
      return errVoid(newIoError("Failed to open WAL file for replay: " & walPath))

    var st: posix.Stat
    if posix.stat(cstring(walPath), st) != 0:
      discard posix.close(fd)
      return errVoid(newIoError("Failed to stat WAL file: " & walPath))
    let fileSize = st.st_size.int
    if fileSize == 0:
      discard posix.close(fd)
      return okVoid() # Empty WAL, nothing to replay
    var pos = 0

    while pos < fileSize:
      # Read 8-byte seqno
      if pos + 8 > fileSize:
        break
      var seqnoBytes: array[8, uint8]
      let bytesRead = posix.read(fd, cast[pointer](addr(seqnoBytes[0])), 8)
      if bytesRead != 8:
        break
      let seqno = SeqNo(decodeFixed64(seqnoBytes))
      pos += 8

      # Read 1-byte type
      if pos + 1 > fileSize:
        break
      var typeByte: uint8
      let r1 = posix.read(fd, cast[pointer](addr(typeByte)), 1)
      if r1 != 1:
        break
      pos += 1

      # Read 4-byte key len
      if pos + 4 > fileSize:
        break
      var keyLenBytes: array[4, uint8]
      let r2 = posix.read(fd, cast[pointer](addr(keyLenBytes[0])), 4)
      if r2 != 4:
        break
      let keyLen = decodeFixed32(keyLenBytes).int
      pos += 4

      # Read key
      if pos + keyLen > fileSize:
        break
      var keyBuf = newString(keyLen)
      let r3 = posix.read(fd, cast[pointer](addr(keyBuf[0])), keyLen)
      if r3 != keyLen:
        break
      pos += keyLen

      # Read 4-byte value len
      if pos + 4 > fileSize:
        break
      var valLenBytes: array[4, uint8]
      let r4 = posix.read(fd, cast[pointer](addr(valLenBytes[0])), 4)
      if r4 != 4:
        break
      let valLen = decodeFixed32(valLenBytes).int
      pos += 4

      # Read value
      var value = ""
      if valLen > 0:
        if pos + valLen > fileSize:
          break
        var valBuf = newString(valLen)
        let r5 = posix.read(fd, cast[pointer](addr(valBuf[0])), valLen)
        if r5 != valLen:
          break
        value = valBuf
        pos += valLen

      # Apply to memtable
      if typeByte == WAL_TYPE_INSERT:
        discard memtable.insertFromString(keyBuf, value, seqno)
      else:
        discard memtable.insertFromString(keyBuf, "", seqno)

    discard posix.close(fd)
    okVoid()
  except:
    errVoid(newIoError("Failed to replay WAL: " & getCurrentExceptionMsg()))

# ============================================================================
# Tests
# ============================================================================

when isMainModule:
  echo "Testing WAL..."

  let path = "/tmp/wal_test_" & $getpid()
  if dirExists(path):
    removeDir(path)
  createDir(path)

  let wal = newWALManager(path).get
  let memtable = newMemtable(0)

  # Test append
  discard wal.appendWrite(1.SeqNo, "key1", "value1")
  discard wal.appendRemove(2.SeqNo, "key2")
  discard wal.appendWrite(3.SeqNo, "key3", "value3")

  # Close and reopen
  wal.file.close()

  # Replay
  let memtable2 = newMemtable(0)
  discard wal.replay(memtable2)

  # Verify
  let result1 = memtable2.get("key1", 1.SeqNo)
  assert(result1.isSome and result1.get == "value1")

  let result2 = memtable2.get("key2", 2.SeqNo)
  assert(result2.isNone)

  let result3 = memtable2.get("key3", 3.SeqNo)
  assert(result3.isSome and result3.get == "value3")

  echo "WAL tests passed!"
  removeDir(path)
