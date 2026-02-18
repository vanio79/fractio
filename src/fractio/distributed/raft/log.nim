## Persistent log storage for Raft consensus
## Implements append-only log with CRC checks, snapshotting, and recovery.
## Thread-safe with mutex protection.

import system
import os
import tables
import strutils
import locks
import posix
import ../../core/errors

proc c_fileno(f: File): cint {.importc: "fileno", header: "<stdio.h>".}

proc fsyncFile(f: File) {.raises: [FractioError].} =
  let fd = c_fileno(f)
  if posix.fsync(fd) != 0:
    raise storageError("fsync failed: " & $posix.strerror(posix.errno), "")
import ../../utils/logging
import ./types

# Safe file I/O helpers that convert IOError to FractioError
proc seekFile(f: File, pos: int64, origin = fspSet) {.raises: [FractioError].} =
  try:
    f.setFilePos(pos, origin)
  except IOError as e:
    raise storageError("seek failed: " & e.msg, "")

proc tellFile(f: File): int64 {.raises: [FractioError].} =
  try:
    result = f.getFilePos()
  except IOError as e:
    raise storageError("tell failed: " & e.msg, "")

proc sizeFile(f: File): int64 {.raises: [FractioError].} =
  try:
    result = f.getFileSize()
  except IOError as e:
    raise storageError("size failed: " & e.msg, "")

proc readSome(f: File, dest: pointer, n: Natural): int {.raises: [
    FractioError].} =
  try:
    result = readBuffer(f, dest, n)
  except IOError as e:
    raise storageError("read failed: " & e.msg, "")

proc writeSome(f: File, source: pointer, n: Natural): int {.raises: [
    FractioError].} =
  try:
    result = writeBuffer(f, source, n)
  except IOError as e:
    raise storageError("write failed: " & e.msg, "")

type
  RaftMeta* = object
    currentTerm*: uint64
    votedFor*: uint64
    lastLogIndex*: uint64
    lastLogTerm*: uint64
    commitIndex*: uint64
    lastApplied*: uint64

  RaftLog* = ref object
    logPath*: string
    metaPath*: string
    logFile*: File
    indexMap*: Table[uint64, RaftEntry]
    lastIndex*: uint64
    lastTerm*: uint64
    snapshotIndex*: uint64
    snapshotTerm*: uint64
    meta*: RaftMeta
    logger*: Logger
    lock*: Lock

# CRC32 table (IEEE 802.3 polynomial 0xEDB88320)
const crc32Table: array[256, uint32] = block:
  var tbl: array[256, uint32]
  for i in 0..255:
    var crc = uint32(i)
    for j in 0..7:
      if (crc and 1) != 0:
        crc = (crc shr 1) xor 0xEDB88320'u32
      else:
        crc = crc shr 1
    tbl[i] = crc
  tbl

proc crc32*(data: openArray[byte]): uint32 =
  var crc: uint32 = 0xFFFFFFFF'u32
  for b in data:
    crc = (crc shr 8) xor crc32Table[(crc xor uint32(b)) and 0xFF]
  result = crc xor 0xFFFFFFFF'u32

# Helper: truncate a file to given size using posix ftruncate
proc truncateFile(path: string, size: int64) {.raises: [FractioError].} =
  let fd = posix.open(path, O_WRONLY)
  if fd < 0:
    raise storageError("truncateFile: cannot open " & path & ": " &
        $posix.strerror(posix.errno), "")
  try:
    if posix.ftruncate(fd, size) != 0:
      raise storageError("truncateFile: ftruncate failed: " & $posix.strerror(
          posix.errno), "")
  finally:
    discard posix.close(fd)

proc recovery(self: RaftLog) {.raises: [FractioError].}
proc scanLog(self: RaftLog) {.raises: [FractioError].}

proc newRaftLog*(logDir: string, logger: Logger): RaftLog =
  ## Create a new RaftLog in the given directory. If existing log and meta exist, they will be recovered.
  result = RaftLog()
  result.logPath = logDir / "raft.log"
  result.metaPath = logDir / "raft.meta"
  result.logger = logger
  initLock(result.lock)
  result.indexMap = initTable[uint64, RaftEntry]()
  result.lastIndex = 0'u64
  result.lastTerm = 0'u64
  result.snapshotIndex = 0'u64
  result.snapshotTerm = 0'u64
  result.recovery()

proc close*(self: RaftLog) =
  ## Close the log file. After this, the log cannot be used until reopened (not supported). Intended for shutdown.
  acquire(self.lock)
  defer:
    release(self.lock)
    deinitLock(self.lock)
  if self.logFile != nil:
    try:
      self.logFile.close()
    except:
      discard
    self.logFile = nil

proc readU64(f: File): uint64 {.raises: [FractioError].} =
  var v: uint64
  let bytes =
    try:
      readBuffer(f, addr v, 8)
    except IOError as e:
      raise storageError("Failed to read uint64: " & e.msg, "")
  if bytes != 8:
    raise storageError("Failed to read uint64", "")
  result = v

proc writeU64(f: File, v: uint64) {.raises: [FractioError].} =
  let written =
    try:
      writeBuffer(f, addr v, 8)
    except IOError as e:
      raise storageError("Failed to write uint64: " & e.msg, "")
  if written != 8:
    raise storageError("Failed to write uint64", "")

proc readMeta(self: RaftLog) {.raises: [FractioError].} =
  ## Read the meta file if it exists, otherwise initialize with defaults.
  if not fileExists(self.metaPath):
    self.meta = RaftMeta(currentTerm: 0, votedFor: 0, lastLogIndex: 0,
        lastLogTerm: 0, commitIndex: 0, lastApplied: 0)
    return
  var f =
    try:
      open(self.metaPath, fmRead)
    except IOError as e:
      raise storageError("Failed to open meta file: " & e.msg, "")
  try:
    self.meta.currentTerm = readU64(f)
    self.meta.votedFor = readU64(f)
    self.meta.lastLogIndex = readU64(f)
    self.meta.lastLogTerm = readU64(f)
    self.meta.commitIndex = readU64(f)
    self.meta.lastApplied = readU64(f)
  finally:
    f.close()

proc writeMetaLocked(self: RaftLog) {.raises: [FractioError].} =
  ## Write meta to disk. Caller must hold self.lock.
  self.meta.lastLogIndex = self.lastIndex
  self.meta.lastLogTerm = self.lastTerm
  var f =
    try:
      open(self.metaPath, fmWrite) # truncates
    except IOError as e:
      raise storageError("Failed to open meta file for writing: " & e.msg, "")
  try:
    writeU64(f, self.meta.currentTerm)
    writeU64(f, self.meta.votedFor)
    writeU64(f, self.meta.lastLogIndex)
    writeU64(f, self.meta.lastLogTerm)
    writeU64(f, self.meta.commitIndex)
    writeU64(f, self.meta.lastApplied)
    flushFile(f)
    fsyncFile(f)
  finally:
    f.close()

proc persistMeta*(self: RaftLog) {.gcsafe, raises: [FractioError].} =
  ## Public: acquire lock and write meta.
  acquire(self.lock)
  try:
    self.writeMetaLocked()
  finally:
    release(self.lock)

# --- Public getters/setters with locking ---

proc getCurrentTerm*(self: RaftLog): uint64 {.gcsafe, raises: [].} =
  ## Get the current term from meta (thread-safe, no FractioError).
  acquire(self.lock)
  result = self.meta.currentTerm
  release(self.lock)

proc setCurrentTerm*(self: RaftLog, term: uint64) {.gcsafe, raises: [].} =
  ## Set the current term in meta (thread-safe, no FractioError).
  acquire(self.lock)
  self.meta.currentTerm = term
  release(self.lock)

proc getVotedFor*(self: RaftLog): uint64 {.gcsafe, raises: [].} =
  acquire(self.lock)
  result = self.meta.votedFor
  release(self.lock)

proc setVotedFor*(self: RaftLog, nodeId: uint64) {.gcsafe, raises: [].} =
  acquire(self.lock)
  self.meta.votedFor = nodeId
  release(self.lock)

proc getLastLogIndex*(self: RaftLog): uint64 {.gcsafe, raises: [].} =
  ## Get the last log index, which is the maximum of the highest entry index
  ## and the snapshot index (if any). Reflects the logical end of the log.
  acquire(self.lock)
  if self.lastIndex > self.snapshotIndex:
    result = self.lastIndex
  else:
    result = self.snapshotIndex
  release(self.lock)

proc getLastLogTerm*(self: RaftLog): uint64 {.gcsafe, raises: [].} =
  ## Get the term of the last log entry. If the log is compacted to a snapshot,
  ## returns the snapshot's last term.
  acquire(self.lock)
  if self.lastIndex > self.snapshotIndex:
    result = self.lastTerm
  else:
    result = self.snapshotTerm
  release(self.lock)

proc getCommitIndex*(self: RaftLog): uint64 {.gcsafe, raises: [].} =
  acquire(self.lock)
  result = self.meta.commitIndex
  release(self.lock)

proc setCommitIndex*(self: RaftLog, idx: uint64) {.gcsafe, raises: [].} =
  acquire(self.lock)
  self.meta.commitIndex = idx
  release(self.lock)

proc getLastApplied*(self: RaftLog): uint64 {.gcsafe, raises: [].} =
  acquire(self.lock)
  result = self.meta.lastApplied
  release(self.lock)

proc setLastApplied*(self: RaftLog, idx: uint64) {.gcsafe, raises: [].} =
  acquire(self.lock)
  self.meta.lastApplied = idx
  release(self.lock)

proc getSnapshotIndex*(self: RaftLog): uint64 {.gcsafe, raises: [].} =
  acquire(self.lock)
  result = self.snapshotIndex
  release(self.lock)

proc getSnapshotTerm*(self: RaftLog): uint64 {.raises: [].} =
  acquire(self.lock)
  result = self.snapshotTerm
  release(self.lock)

proc truncateFrom*(self: RaftLog, index: uint64) {.gcsafe, raises: [
    FractioError].} =
  ## Delete all entries at or after `index`. This includes removing them from
  ## memory and rewriting the log file to discard those bytes.
  ## Precondition: index > snapshotIndex and index <= lastIndex+1.
  acquire(self.lock)
  try:
    if index <= self.snapshotIndex:
      raise storageError("truncateFrom: index must be > snapshotIndex", "")
    if index > self.lastIndex + 1:
      return # nothing to truncate
    # Collect keys to delete from indexMap
    var keysToDelete: seq[uint64] = @[]
    for idx in self.indexMap.keys:
      if idx >= index:
        keysToDelete.add(idx)
    # Remove from indexMap
    for idx in keysToDelete:
      self.indexMap.del(idx)
    # Recompute lastIndex and lastTerm from remaining entries
    self.lastIndex = 0'u64
    self.lastTerm = 0'u64
    for idx, ent in self.indexMap.pairs:
      if idx > self.lastIndex:
        self.lastIndex = idx
        self.lastTerm = ent.term
    # Rebuild the log file: write only the remaining entries to a temporary file, then replace.
    let tmpPath = self.logPath & ".tmp"
    var tmpFile =
      try:
        open(tmpPath, fmWrite)
      except IOError as e:
        raise storageError("truncateFrom: cannot create temp file: " & e.msg, "")
    var f = self.logFile
    seekFile(f, 0) # rewind original log
    while true:
      # Read term
      var term: uint64
      let termBytes = readSome(f, addr term, sizeof(term))
      if termBytes == 0: break
      if termBytes < sizeof(term): break
      # Read index
      var idx: uint64
      let idxBytes = readSome(f, addr idx, sizeof(idx))
      if idxBytes < sizeof(idx): break
      # Read kind
      var kindByte: uint8
      let kindBytes = readSome(f, addr kindByte, sizeof(kindByte))
      if kindBytes < sizeof(kindByte): break
      let kind = cast[RaftCommandKind](kindByte)
      # Read dataLen
      var dataLen: uint32
      let lenBytes = readSome(f, addr dataLen, sizeof(dataLen))
      if lenBytes < sizeof(dataLen): break
      # Read data
      var data: seq[byte] = @[]
      if dataLen > 0:
        data = newSeq[byte](dataLen)
        let dataBytes = readSome(f, unsafeAddr data[0], dataLen)
        if dataBytes < int(dataLen): break
      # Read checksum
      var storedChecksum: uint32
      let chkBytes = readSome(f, addr storedChecksum, sizeof(storedChecksum))
      if chkBytes < sizeof(storedChecksum): break
      # If this entry's index is kept, write it to tmp
      if idx >= index:
        continue # skip this entry (delete)
      # Write entry to tmpFile
      discard writeSome(tmpFile, addr term, sizeof(term))
      discard writeSome(tmpFile, addr idx, sizeof(idx))
      var kindByteTmp = kindByte
      discard writeSome(tmpFile, addr kindByteTmp, sizeof(kindByteTmp))
      var dlen = dataLen
      discard writeSome(tmpFile, addr dlen, sizeof(dlen))
      if dataLen > 0:
        discard writeSome(tmpFile, unsafeAddr data[0], dataLen)
      discard writeSome(tmpFile, addr storedChecksum, sizeof(storedChecksum))
    # End while
    tmpFile.close()
    f.close()
    # Replace original log with tmp
    try:
      removeFile(self.logPath)
    except Exception as e:
      raise storageError("truncateFrom: removeFile failed: " & e.msg, "")
    try:
      moveFile(tmpPath, self.logPath)
    except Exception as e:
      raise storageError("truncateFrom: moveFile failed: " & e.msg, "")
    # Reopen log file for future use
    self.logFile =
      try:
        open(self.logPath, fmReadWriteExisting)
      except IOError as e:
        raise storageError("truncateFrom: failed to reopen log: " & e.msg, "")
    seekFile(self.logFile, 0, fspEnd) # position at end
    # Meta's lastLogIndex/term already updated above; persist them
    self.meta.lastLogIndex = self.lastIndex
    self.meta.lastLogTerm = self.lastTerm
    self.writeMetaLocked()
  finally:
    release(self.lock)

proc recovery(self: RaftLog) {.raises: [FractioError].} =
  ## Recover state from disk: read meta and scan log entries.
  self.readMeta()
  # Open log file
  try:
    self.logFile = open(self.logPath, fmReadWriteExisting)
  except IOError:
    # File doesn't exist, create empty
    var tf =
      try:
        open(self.logPath, fmWrite)
      except IOError as e:
        raise storageError("Failed to create log file: " & e.msg, "")
    tf.close()
    self.logFile =
      try:
        open(self.logPath, fmReadWriteExisting)
      except IOError as e:
        raise storageError("Failed to open log file: " & e.msg, "")
  # Scan log entries
  self.scanLog()
  # After scan, position at end for appends
  seekFile(self.logFile, 0, fspEnd)
  # Sync meta's lastLogIndex with actual scanned entries (in case meta was stale)
  self.meta.lastLogIndex = self.lastIndex
  self.meta.lastLogTerm = self.lastTerm

proc scanLog(self: RaftLog) {.raises: [FractioError].} =
  ## Scan the log file, reconstruct entries, and truncate any corrupted tail.
  var f = self.logFile
  seekFile(f, 0) # rewind
  var validPos: int64 = 0
  var truncateNeeded = false
  while true:
    let startPos = tellFile(f)
    # Read term (uint64)
    var term: uint64
    let termBytes = readSome(f, addr term, sizeof(term))
    if termBytes == 0:
      break # normal EOF
    if termBytes < sizeof(term):
      truncateNeeded = true
      break
    # Read index (uint64)
    var index: uint64
    let idxBytes = readSome(f, addr index, sizeof(index))
    if idxBytes < sizeof(index):
      truncateNeeded = true
      break
    # Read kind (uint8)
    var kindByte: uint8
    let kindBytes = readSome(f, addr kindByte, sizeof(kindByte))
    if kindBytes < sizeof(kindByte):
      truncateNeeded = true
      break
    let kind = cast[RaftCommandKind](kindByte)
    # Read dataLen (uint32)
    var dataLen: uint32
    let lenBytes = readSome(f, addr dataLen, sizeof(dataLen))
    if lenBytes < sizeof(dataLen):
      truncateNeeded = true
      break
    # Read data
    var data: seq[byte] = @[]
    if dataLen > 0:
      data = newSeq[byte](dataLen)
      let dataBytes = readSome(f, unsafeAddr data[0], dataLen)
      if dataBytes < int(dataLen):
        truncateNeeded = true
        break
    # Read checksum (uint32)
    var storedChecksum: uint32
    let chkBytes = readSome(f, addr storedChecksum, sizeof(storedChecksum))
    if chkBytes < sizeof(storedChecksum):
      truncateNeeded = true
      break
    # Verify checksum
    let computed = crc32(data)
    if computed != storedChecksum:
      truncateNeeded = true
      break
    # Check for duplicate index
    if index in self.indexMap:
      truncateNeeded = true
      break
    # Valid entry, add to map
    let entry = RaftEntry(term: term, index: index, command: RaftCommand(
        kind: kind, data: data), checksum: storedChecksum)
    try:
      self.indexMap[index] = entry
    except KeyError as e:
      raise storageError("Failed to store entry: " & $index, "")
    self.lastIndex = index
    self.lastTerm = term
    validPos = tellFile(f)
  # End while
  let currentSize = sizeFile(f)
  if truncateNeeded and validPos < currentSize:
    # Truncate the file to validPos
    f.close()
    truncateFile(self.logPath, validPos)
    try:
      self.logFile = open(self.logPath, fmReadWriteExisting)
    except IOError as e:
      raise storageError("Failed to reopen log after truncation: " & e.msg, "")
    seekFile(self.logFile, validPos) # position at new end
    if self.logger != nil:
      try:
        self.logger.warn("Log truncated from " & $currentSize & " to " & $validPos)
      except:
        discard

proc append*(self: RaftLog, entry: RaftEntry): uint64 {.gcsafe, raises: [
    FractioError].} =
  ## Append a new entry to the log. The entry's index must be either lastIndex+1 or snapshotIndex+1 (if log empty).
  acquire(self.lock)
  defer: release(self.lock)
  # Determine expected next index
  let expectedIndex =
    if self.lastIndex > 0: self.lastIndex + 1
    else: self.snapshotIndex + 1
  if entry.index != expectedIndex:
    raise storageError("Entry index not consecutive: expected " &
        $expectedIndex & " got " & $entry.index, "")
  # Compute checksum of command data
  let data = entry.command.data
  let checksum = crc32(data)
  var f = self.logFile
  # Ensure we are at end
  seekFile(f, 0, fspEnd)
  # Write term
  discard writeSome(f, addr entry.term, sizeof(entry.term))
  # Write index
  discard writeSome(f, addr entry.index, sizeof(entry.index))
  # Write kind as uint8
  var kindByte = uint8(entry.command.kind)
  discard writeSome(f, addr kindByte, sizeof(kindByte))
  # Write dataLen as uint32
  var dataLen = uint32(data.len)
  discard writeSome(f, addr dataLen, sizeof(dataLen))
  # Write data
  if dataLen > 0:
    discard writeSome(f, unsafeAddr data[0], dataLen)
  # Write checksum
  discard writeSome(f, addr checksum, sizeof(checksum))
  # Flush to disk
  try: flushFile(f)
  except OSError as e:
    raise storageError("flush failed: " & e.msg, "")
  try: fsyncFile(f)
  except OSError as e:
    raise storageError("fsync failed: " & e.msg, "")
  # Update in-memory state
  try:
    self.indexMap[entry.index] = entry
  except KeyError as e:
    raise storageError("Failed to store entry: " & $entry.index, "")
  self.lastIndex = entry.index
  self.lastTerm = entry.term
  self.meta.lastLogIndex = entry.index
  self.meta.lastLogTerm = entry.term
  result = entry.index

proc getEntry*(self: RaftLog, index: uint64): RaftEntry {.gcsafe, raises: [
    FractioError].} =
  ## Retrieve an entry by its index. Raises FractioError if index is compacted or missing.
  acquire(self.lock)
  defer: release(self.lock)
  if index <= self.snapshotIndex:
    raise storageError("Entry at index " & $index &
        " has been compacted by snapshot", "")
  if index notin self.indexMap:
    raise storageError("No entry found at index " & $index, "")
  try:
    result = self.indexMap[index]
  except KeyError as e:
    raise storageError("Entry vanished: " & $index, "")

proc createSnapshot*(self: RaftLog, lastIncludedIndex, lastIncludedTerm: uint64,
    data: seq[byte]): Snapshot {.gcsafe, raises: [FractioError].} =
  ## Create a snapshot of the state machine up to the given index. Removes compacted entries from memory.
  acquire(self.lock)
  defer: release(self.lock)
  # Validate index exists and term matches
  if lastIncludedIndex notin self.indexMap:
    raise storageError("Cannot create snapshot: index " &
        $lastIncludedIndex & " not in log", "")
  let entry =
    try:
      self.indexMap[lastIncludedIndex]
    except KeyError as e:
      raise storageError("Entry missing: " & $lastIncludedIndex, "")
  if entry.term != lastIncludedTerm:
    raise storageError("Snapshot term mismatch: expected " &
        $entry.term & " got " & $lastIncludedTerm, "")
  # Write snapshot file
  let snapPath = self.logPath & ".snap"
  var f =
    try:
      open(snapPath, fmWrite)
    except IOError as e:
      raise storageError("Failed to create snapshot file: " & e.msg, "")
  try:
    var idx = lastIncludedIndex
    var term = lastIncludedTerm
    var dlen = uint64(data.len)
    discard writeSome(f, addr idx, sizeof(idx))
    discard writeSome(f, addr term, sizeof(term))
    discard writeSome(f, addr dlen, sizeof(dlen))
    if dlen > 0:
      discard writeSome(f, unsafeAddr data[0], dlen)
    flushFile(f)
    fsyncFile(f)
  finally:
    f.close()
  # Remove entries <= lastIncludedIndex from memory
  var keysToDelete: seq[uint64] = @[]
  for idx in self.indexMap.keys:
    if idx <= lastIncludedIndex:
      keysToDelete.add(idx)
  for idx in keysToDelete:
    self.indexMap.del(idx)
  # Update snapshot info
  self.snapshotIndex = lastIncludedIndex
  self.snapshotTerm = lastIncludedTerm
  # Recompute lastIndex and lastTerm from remaining entries
  self.lastIndex = 0'u64
  self.lastTerm = 0'u64
  for idx, ent in self.indexMap.pairs:
    if idx > self.lastIndex:
      self.lastIndex = idx
      self.lastTerm = ent.term
  # Note: log file is NOT truncated
  result = Snapshot(lastIndex: lastIncludedIndex, lastTerm: lastIncludedTerm, data: data)

proc installSnapshot*(self: RaftLog, snap: Snapshot) {.gcsafe, raises: [
    FractioError].} =
  ## Install a snapshot received from the leader. This replaces the entire log state.
  acquire(self.lock)
  defer: release(self.lock)
  # Write snapshot file to persist it
  let snapPath = self.logPath & ".snap"
  var f =
    try:
      open(snapPath, fmWrite)
    except IOError as e:
      raise storageError("Failed to install snapshot file: " & e.msg, "")
  try:
    var idx = snap.lastIndex
    var term = snap.lastTerm
    var dlen = uint64(snap.data.len)
    discard writeSome(f, addr idx, sizeof(idx))
    discard writeSome(f, addr term, sizeof(term))
    discard writeSome(f, addr dlen, sizeof(dlen))
    if dlen > 0:
      discard writeSome(f, unsafeAddr snap.data[0], dlen)
    flushFile(f)
    fsyncFile(f)
  finally:
    f.close()
  # Truncate the log file to zero length
  if self.logFile != nil:
    try: self.logFile.close() except: discard
  truncateFile(self.logPath, 0)
  # Reopen log file for future appends
  self.logFile =
    try:
      open(self.logPath, fmReadWriteExisting)
    except IOError as e:
      raise storageError("Failed to reopen log after snapshot: " & e.msg, "")
  # Clear memory
  self.indexMap.clear()
  self.lastIndex = 0'u64
  self.lastTerm = 0'u64
  self.snapshotIndex = snap.lastIndex
  self.snapshotTerm = snap.lastTerm
  # Reset meta's lastLog fields to 0
  self.meta.lastLogIndex = 0'u64
  self.meta.lastLogTerm = 0'u64
