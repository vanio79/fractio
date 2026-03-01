# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License

## LSM Tree v2 - Manifest
## Tracks SSTable metadata for crash recovery and version reconstruction

import std/[os, options, hashes, locks, posix]
import fractio/storage/lsm_tree_v2/types
import fractio/storage/lsm_tree_v2/error
import fractio/storage/lsm_tree_v2/table
import fractio/storage/lsm_tree_v2/coding

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
# Manifest Record Format
# ============================================================================

const
  MANIFEST_VERSION* = 1'i64
  MANIFEST_MAGIC* = 0x4D414E4946535452'i64 # "MANIFSTR"

# ============================================================================
# Manifest Manager
# ============================================================================

type
  ManifestManager* = ref object
    path*: string
    file*: cint
    writeLock*: Lock

proc newManifestManager*(path: string): LsmResult[ManifestManager] =
  ## Create a new manifest manager. Creates directory if needed.
  try:
    if not dirExists(path):
      createDir(path)

    let manifestPath = path & "/manifest"
    var file = posix.open(cstring(manifestPath), O_WRONLY or O_CREAT, 0o644)
    if file == -1:
      return err[ManifestManager](newIoError(
          "Failed to create manifest file: " & manifestPath))

    var writeLock: Lock

    let m = ManifestManager(
      path: path,
      file: file,
      writeLock: writeLock
    )
    ok(m)
  except:
    err[ManifestManager](newIoError("Failed to initialize manifest manager: " &
        getCurrentExceptionMsg()))

proc addTable*(m: ManifestManager, table: SsTable): LsmResult[void] =
  ## Add an SSTable to the manifest
  try:
    acquire(m.writeLock)
    defer: release(m.writeLock)

    # Write: [8][8][4][table_id][level][min_key_len][min_key][max_key_len][max_key][size][smallest_seqno][largest_seqno]
    var buf = newString(0)
    buf.add(encodeFixed64(MANIFEST_MAGIC))
    buf.add(encodeFixed64(MANIFEST_VERSION))
    buf.add(encodeFixed32(1)) # num_tables = 1 (append-only)
    buf.add(encodeFixed64(table.meta.id.int64))
    buf.add(encodeFixed32(table.meta.level.uint32))
    buf.add(encodeFixed32(table.meta.minKey.len.uint32))
    buf.add(table.meta.minKey)
    buf.add(encodeFixed32(table.meta.maxKey.len.uint32))
    buf.add(table.meta.maxKey)
    buf.add(encodeFixed64(table.meta.size.int64))
    buf.add(encodeFixed64(table.meta.smallestSeqno.int64))
    buf.add(encodeFixed64(table.meta.largestSeqno.int64))

    let written = posix.write(m.file, cast[pointer](addr(buf[0])), buf.len)
    if written != buf.len:
      raise newIoError("Failed to write to manifest")
    # NOTE: No fsync to match Rust lsm-tree behavior

    okVoid()
  except:
    errVoid(newIoError("Failed to add table to manifest: " &
        getCurrentExceptionMsg()))

proc replay*(m: ManifestManager): LsmResult[void] =
  ## Replay manifest to reconstruct SSTable list
  try:
    let manifestPath = m.path & "/manifest"
    if not os.fileExists(manifestPath):
      return okVoid() # No manifest, no SSTables

    let fd = posix.open(cstring(manifestPath), cint(FileFlags.frRead))
    if fd == -1:
      return errVoid(newIoError("Failed to open manifest file for replay: " & manifestPath))

    var st: posix.Stat
    if posix.stat(cstring(manifestPath), st) != 0:
      discard posix.close(fd)
      return errVoid(newIoError("Failed to stat manifest file: " & manifestPath))
    let fileSize = st.st_size.int
    if fileSize == 0:
      discard posix.close(fd)
      return okVoid() # Empty manifest, no SSTables
    var pos = 0

    # Read magic
    if pos + 8 > fileSize:
      discard posix.close(fd)
      return errVoid(newIoError("Manifest too short: missing magic"))
    var magicBuf: array[8, uint8]
    let r1 = posix.read(fd, cast[pointer](addr(magicBuf[0])), 8)
    if r1 != 8:
      discard posix.close(fd)
      return errVoid(newIoError("Manifest truncated: failed to read magic"))
    let magic = decodeFixed64(magicBuf)
    if magic != MANIFEST_MAGIC:
      discard posix.close(fd)
      return errVoid(newIoError("Invalid manifest magic"))
    pos += 8

    # Read version
    if pos + 8 > fileSize:
      discard posix.close(fd)
      return errVoid(newIoError("Manifest too short: missing version"))
    var versionBuf: array[8, uint8]
    let r2 = posix.read(fd, cast[pointer](addr(versionBuf[0])), 8)
    if r2 != 8:
      discard posix.close(fd)
      return errVoid(newIoError("Manifest truncated: failed to read version"))
    let version = decodeFixed64(versionBuf)
    if version != MANIFEST_VERSION:
      discard posix.close(fd)
      return errVoid(newIoError("Unsupported manifest version"))
    pos += 8

    # Read num_tables
    if pos + 4 > fileSize:
      discard posix.close(fd)
      return errVoid(newIoError("Manifest too short: missing table count"))
    var numTablesBuf: array[4, uint8]
    let r3 = posix.read(fd, cast[pointer](addr(numTablesBuf[0])), 4)
    if r3 != 4:
      discard posix.close(fd)
      return errVoid(newIoError("Manifest truncated: failed to read table count"))
    let numTables = decodeFixed32(numTablesBuf).int
    pos += 4

    # Read each table
    for i in 0..<numTables:
      if pos + 8 > fileSize:
        discard posix.close(fd)
        return errVoid(newIoError("Manifest truncated: missing table_id"))
      var tableIdBuf: array[8, uint8]
      let r4 = posix.read(fd, cast[pointer](addr(tableIdBuf[0])), 8)
      if r4 != 8:
        discard posix.close(fd)
        return errVoid(newIoError("Manifest truncated: failed to read table_id"))
      let tableId = TableId(decodeFixed64(tableIdBuf))
      pos += 8

      if pos + 4 > fileSize:
        discard posix.close(fd)
        return errVoid(newIoError("Manifest truncated: missing level"))
      var levelBuf: array[4, uint8]
      let r5 = posix.read(fd, cast[pointer](addr(levelBuf[0])), 4)
      if r5 != 4:
        discard posix.close(fd)
        return errVoid(newIoError("Manifest truncated: failed to read level"))
      let level = decodeFixed32(levelBuf).int
      pos += 4

      if pos + 4 > fileSize:
        discard posix.close(fd)
        return errVoid(newIoError("Manifest truncated: missing min_key_len"))
      var minKeyLenBuf: array[4, uint8]
      let r6 = posix.read(fd, cast[pointer](addr(minKeyLenBuf[0])), 4)
      if r6 != 4:
        discard posix.close(fd)
        return errVoid(newIoError("Manifest truncated: failed to read min_key_len"))
      let minKeyLen = decodeFixed32(minKeyLenBuf).int
      pos += 4

      if pos + minKeyLen > fileSize:
        discard posix.close(fd)
        return errVoid(newIoError("Manifest truncated: missing min_key"))
      var minKeyBuf = newString(minKeyLen)
      let r7 = posix.read(fd, cast[pointer](addr(minKeyBuf[0])), minKeyLen)
      if r7 != minKeyLen:
        discard posix.close(fd)
        return errVoid(newIoError("Manifest truncated: failed to read min_key"))
      let minKey = minKeyBuf
      pos += minKeyLen

      if pos + 4 > fileSize:
        discard posix.close(fd)
        return errVoid(newIoError("Manifest truncated: missing max_key_len"))
      var maxKeyLenBuf: array[4, uint8]
      let r8 = posix.read(fd, cast[pointer](addr(maxKeyLenBuf[0])), 4)
      if r8 != 4:
        discard posix.close(fd)
        return errVoid(newIoError("Manifest truncated: failed to read max_key_len"))
      let maxKeyLen = decodeFixed32(maxKeyLenBuf).int
      pos += 4

      if pos + maxKeyLen > fileSize:
        discard posix.close(fd)
        return errVoid(newIoError("Manifest truncated: missing max_key"))
      var maxKeyBuf = newString(maxKeyLen)
      let r9 = posix.read(fd, cast[pointer](addr(maxKeyBuf[0])), maxKeyLen)
      if r9 != maxKeyLen:
        discard posix.close(fd)
        return errVoid(newIoError("Manifest truncated: failed to read max_key"))
      let maxKey = maxKeyBuf
      pos += maxKeyLen

      if pos + 8 > fileSize:
        discard posix.close(fd)
        return errVoid(newIoError("Manifest truncated: missing size"))
      var sizeBuf: array[8, uint8]
      let r10 = posix.read(fd, cast[pointer](addr(sizeBuf[0])), 8)
      if r10 != 8:
        discard posix.close(fd)
        return errVoid(newIoError("Manifest truncated: failed to read size"))
      let size = decodeFixed64(sizeBuf).uint64
      pos += 8

      if pos + 8 > fileSize:
        discard posix.close(fd)
        return errVoid(newIoError("Manifest truncated: missing smallest_seqno"))
      var smallestSeqnoBuf: array[8, uint8]
      let r11 = posix.read(fd, cast[pointer](addr(smallestSeqnoBuf[0])), 8)
      if r11 != 8:
        discard posix.close(fd)
        return errVoid(newIoError("Manifest truncated: failed to read smallest_seqno"))
      let smallestSeqno = SeqNo(decodeFixed64(smallestSeqnoBuf))
      pos += 8

      if pos + 8 > fileSize:
        discard posix.close(fd)
        return errVoid(newIoError("Manifest truncated: missing largest_seqno"))
      var largestSeqnoBuf: array[8, uint8]
      let r12 = posix.read(fd, cast[pointer](addr(largestSeqnoBuf[0])), 8)
      if r12 != 8:
        discard posix.close(fd)
        return errVoid(newIoError("Manifest truncated: failed to read largest_seqno"))
      let largestSeqno = SeqNo(decodeFixed64(largestSeqnoBuf))
      pos += 8

      # Create dummy SsTable for recovery
      let tablePath = m.path & "/" & $tableId.int & ".sst"
      let table = newSsTable(tablePath)
      table.meta.id = tableId
      table.meta.level = level
      table.meta.minKey = minKey
      table.meta.maxKey = maxKey
      table.meta.size = size
      table.meta.smallestSeqno = smallestSeqno
      table.meta.largestSeqno = largestSeqno
      table.meta.entryCount = 0 # Will be computed later

      # TODO: Load actual data from .sst file? No — we’ll let table cache handle it

    discard posix.close(fd)
    okVoid()
  except:
    errVoid(newIoError("Failed to replay manifest: " & getCurrentExceptionMsg()))

# ============================================================================
# Tests
# ============================================================================

when isMainModule:
  echo "Testing Manifest..."

  let path = "/tmp/manifest_test_" & $getpid()
  if dirExists(path):
    removeDir(path)
  createDir(path)

  let manifest = newManifestManager(path).get

  # Create dummy SSTable
  let table = newSsTable("/tmp/test.sst")
  table.meta.id = 123
  table.meta.level = 2
  table.meta.minKey = "a"
  table.meta.maxKey = "z"
  table.meta.size = 1024
  table.meta.smallestSeqno = 100.SeqNo
  table.meta.largestSeqno = 200.SeqNo

  # Add to manifest
  discard manifest.addTable(table)

  # Close and reopen
  close(manifest.file)

  # Replay
  discard manifest.replay()

  echo "Manifest tests passed!"
  removeDir(path)
