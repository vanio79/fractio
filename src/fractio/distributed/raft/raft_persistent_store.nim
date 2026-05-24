# Raft Persistent Store — WiscKey-backed Raft log and state persistence
#
# Stores NuRaft log entries and state in the shared WiscKey/LevelDB backend
# using /raft/<groupId>/log/<index> and /raft/<groupId>/state key prefixes.
#
# This replaces the in-memory log store (lost on crash) with durable WiscKey storage.
#
#
# Key format:
#   Log entries:  /raft/<groupId>/log/<index>   → serialized log entry
#   Raft state:   /raft/<groupId>/state          → serialized state
#   Cluster config: /raft/<groupId>/config       → serialized config
#
# All writes go through the WiscKey backend with syncWrites=true to ensure
# durability before acknowledging to NuRaft.

import std/[locks, options, strutils]
import fractio/storage/backend
import fractio/storage/wisckey_backend
import fractio/distributed/raft/group_types

# ============================================================================
# Log entry serialization format
# ============================================================================
#
# Each log entry is serialized as:
#   [term:8 bytes BE][val_type:1 byte][data_len:4 bytes BE][data:data_len bytes]
#
# This matches NuRaft's log_entry::serialize() format:
#   term (ulong, 8 bytes) + val_type (byte) + buffer data
#
# We add a data_len prefix so we can delimit entries when scanning.

const
  LOG_ENTRY_MAGIC* = 0x4C45'u16   # "LE" - log entry magic number for validation
  STATE_ENTRY_MAGIC* = 0x5354'u16 # "ST" - state entry magic number

type
  LogValType* = enum
    lvtAppLog = 1 ## Regular application log entry (matches NuRaft log_val_type::app_log)
    lvtClusterConfig = 2 ## Cluster configuration entry (matches NuRaft log_val_type::conf)

  RaftLogEntry* = object
    ## A single Raft log entry.
    term*: uint64        ## The term of this entry
    valType*: LogValType ## Type of entry (app_log or cluster_config)
    data*: string        ## The payload data

  RaftState* = object
    ## Persistent Raft state (term, voted_for, config_log_idx_hwm).
    term*: uint64
    votedFor*: int32
    configLogIdxHwm*: uint64

  RaftPersistentStore* = ref object
    ## WiscKey-backed persistent store for a single Raft group's log and state.
    ##
    ## Thread-safe: all operations acquire a lock to prevent concurrent access
    ## to the in-memory index while WiscKey handles its own concurrency.
    backend*: WiscKeyBackend ## Shared WiscKey backend (thread-safe)
    groupId*: GroupID ## The Raft group this store belongs to
    lock*: Lock ## Protects startIndex, cached entries
    startIndex*: uint64 ## First log index (1 for fresh, >1 after compaction)
    nextIndex*: uint64 ## Next log index to write (1-based)
    lastDurableIndex*: uint64 ## Last index known to be durable on disk

# ============================================================================
# Log entry serialization / deserialization
# ============================================================================

proc serializeLogEntry*(entry: RaftLogEntry): string =
  ## Serialize a RaftLogEntry to binary format:
  ## [magic:2][term:8][val_type:1][data_len:4][data:data_len]
  result = newString(2 + 8 + 1 + 4 + entry.data.len)
  var offset = 0
  # Magic (2 bytes)
  result[offset] = char(LOG_ENTRY_MAGIC shr 8 and 0xFF)
  result[offset + 1] = char(LOG_ENTRY_MAGIC and 0xFF)
  offset += 2
  # Term (8 bytes big-endian)
  var term = entry.term
  for i in countdown(7, 0):
    result[offset + i] = char(term and 0xFF)
    term = term shr 8
  offset += 8
  # Value type (1 byte)
  result[offset] = char(entry.valType.ord)
  offset += 1
  # Data length (4 bytes big-endian)
  var dataLen = uint32(entry.data.len)
  for i in countdown(3, 0):
    result[offset + i] = char(dataLen and 0xFF)
    dataLen = dataLen shr 8
  offset += 4
  # Data payload
  if entry.data.len > 0:
    copyMem(addr result[offset], unsafeAddr entry.data[0], entry.data.len)

proc deserializeLogEntry*(data: string): Option[RaftLogEntry] =
  ## Deserialize a RaftLogEntry from binary format.
  ## Returns none if data is too short or magic is invalid.
  if data.len < 15: # 2 + 8 + 1 + 4 = 15 minimum (no data)
    return none(RaftLogEntry)
  var offset = 0
  # Magic (2 bytes)
  let magic = uint16(data[offset].ord) shl 8 or uint16(data[offset + 1].ord)
  offset += 2
  if magic != LOG_ENTRY_MAGIC:
    return none(RaftLogEntry)
  # Term (8 bytes big-endian)
  var term: uint64 = 0
  for i in 0..<8:
    term = (term shl 8) or uint64(data[offset + i].ord)
  offset += 8
  # Value type (1 byte)
  let valTypeByte = data[offset].ord
  offset += 1
  if valTypeByte < 1 or valTypeByte > 4:
    return none(RaftLogEntry)
  let valType = LogValType(valTypeByte)
  # Data length (4 bytes big-endian)
  var dataLen: uint32 = 0
  for i in 0..<4:
    dataLen = (dataLen shl 8) or uint32(data[offset + i].ord)
  offset += 4
  # Data payload
  let dataLenInt = int(dataLen)
  if offset + dataLenInt > data.len:
    return none(RaftLogEntry)
  var payload = newString(dataLenInt)
  if dataLenInt > 0:
    copyMem(addr payload[0], unsafeAddr data[offset], dataLenInt)
  some(RaftLogEntry(term: term, valType: valType, data: payload))

# ============================================================================
# Raft state serialization / deserialization
# ============================================================================

proc serializeRaftState*(state: RaftState): string =
  ## Serialize RaftState to binary format (compatible with old file format v2):
  ## [magic:2][term:8][voted_for:4][padding:4][config_log_idx_hwm:8]
  ## Total: 26 bytes
  result = newString(26)
  # Magic (2 bytes)
  result[0] = char(STATE_ENTRY_MAGIC shr 8 and 0xFF)
  result[1] = char(STATE_ENTRY_MAGIC and 0xFF)
  # Term (8 bytes big-endian)
  var term = state.term
  for i in countdown(9, 2):
    result[i] = char(term and 0xFF)
    term = term shr 8
  # voted_for (4 bytes big-endian)
  var votedFor = state.votedFor
  for i in countdown(13, 10):
    result[i] = char(votedFor and 0xFF)
    votedFor = votedFor shr 8
  # Padding (4 bytes, 14-17)
  for i in 14..<18:
    result[i] = '\0'
  # config_log_idx_hwm (8 bytes big-endian, 18-25)
  var hwm = state.configLogIdxHwm
  for i in countdown(25, 18):
    result[i] = char(hwm and 0xFF)
    hwm = hwm shr 8

proc deserializeRaftState*(data: string): Option[RaftState] =
  ## Deserialize RaftState from binary format (v3 only).
  ## Format: [magic:2][term:8][voted_for:4][padding:4][config_hwm:8] = 26 bytes
  ## Returns none if data is too short or magic is invalid.
  const stateSize = 26
  if data.len < stateSize:
    return none(RaftState)

  let magic = uint16(data[0].ord) shl 8 or uint16(data[1].ord)
  if magic != STATE_ENTRY_MAGIC:
    return none(RaftState)

  var term: uint64 = 0
  for i in 0..<8:
    term = (term shl 8) or uint64(data[2 + i].ord)
  var votedFor: int32 = 0
  for i in 0..<4:
    votedFor = (votedFor shl 8) or int32(data[10 + i].ord)
  var hwm: uint64 = 0
  for i in 0..<8:
    hwm = (hwm shl 8) or uint64(data[18 + i].ord)
  some(RaftState(term: term, votedFor: votedFor, configLogIdxHwm: hwm))

# ============================================================================
# RaftPersistentStore implementation
# ============================================================================

proc newRaftPersistentStore*(backend: WiscKeyBackend,
    groupId: GroupID): RaftPersistentStore =
  ## Create a new RaftPersistentStore backed by the given WiscKey backend.
  ## Reads existing log entries from WiscKey to reconstruct the in-memory index.
  result = RaftPersistentStore(
    backend: backend,
    groupId: groupId,
    startIndex: 1'u64,
    nextIndex: 1'u64,
    lastDurableIndex: 0'u64
  )
  initLock(result.lock)

  # Reconstruct the in-memory index by scanning log entries in WiscKey
  # Find the prefix: /raft/<groupId>/log/
  let groupStr = $groupId
  let prefix = "/raft/" & groupStr & "/log/"

  let startKey = prefix & "0"
  # Scan keys after the prefix
  var maxIndex: uint64 = 0
  var minIndex: uint64 = high(uint64)

  let iter = newIterator(backend)
  if not iter.isNil:
    if seek(iter, startKey):
      while valid(iter) and key(iter).startsWith(prefix):
        let k = key(iter)
        let idxStr = k[prefix.len ..^ 1]
        try:
          let idx = parseBiggestUInt(idxStr)
          if idx < minIndex:
            minIndex = idx
          if idx > maxIndex:
            maxIndex = idx
        except ValueError:
          discard
        if not next(iter):
          break
    destroy(iter)

  if maxIndex > 0:
    result.startIndex = if minIndex <= maxIndex: minIndex else: 1'u64
    result.nextIndex = maxIndex + 1
    result.lastDurableIndex = maxIndex
  else:
    result.startIndex = 1'u64
    result.nextIndex = 1'u64
    result.lastDurableIndex = 0'u64

proc close*(store: RaftPersistentStore) =
  ## Clean up resources.
  deinitLock(store.lock)

# ============================================================================
# Log store operations
# ============================================================================

proc appendEntry*(store: RaftPersistentStore, entry: RaftLogEntry): uint64 =
  ## Append a log entry to the store. Returns the index where it was stored.
  ## Thread-safe: acquires lock, writes to WiscKey, updates index.
  withLock store.lock:
    let index = store.nextIndex
    let key = encodeLogKey(store.groupId, index)
    let value = serializeLogEntry(entry)
    discard store.backend.put(key, value)
    store.nextIndex = index + 1
    result = index

proc writeAt*(store: RaftPersistentStore, index: uint64, entry: RaftLogEntry) =
  ## Write a log entry at the given index, truncating all entries after it.
  ## This implements NuRaft's write_at semantics.
  withLock store.lock:
    let key = encodeLogKey(store.groupId, index)
    let value = serializeLogEntry(entry)
    discard store.backend.put(key, value)

    # Truncate all entries after this index
    if index + 1 < store.nextIndex:
      var delIndex = index + 1
      while delIndex < store.nextIndex:
        let delKey = encodeLogKey(store.groupId, delIndex)
        discard store.backend.delete(delKey)
        inc delIndex

    store.nextIndex = index + 1

proc getEntry*(store: RaftPersistentStore, index: uint64): Option[RaftLogEntry] =
  ## Get a log entry at the given index.
  let key = encodeLogKey(store.groupId, index)
  let value = store.backend.get(key)
  if value.isSome:
    deserializeLogEntry(value.get())
  else:
    none(RaftLogEntry)

proc termAt*(store: RaftPersistentStore, index: uint64): uint64 =
  ## Get the term for the entry at the given index.
  ## Returns 0 if the entry doesn't exist.
  let entryOpt = store.getEntry(index)
  if entryOpt.isSome:
    entryOpt.get().term
  else:
    0'u64

proc nextSlot*(store: RaftPersistentStore): uint64 =
  ## Return the next available log index (1-based).
  withLock store.lock:
    result = store.nextIndex

proc startIndex*(store: RaftPersistentStore): uint64 =
  ## Return the first available log index.
  withLock store.lock:
    result = store.startIndex

proc lastEntry*(store: RaftPersistentStore): Option[RaftLogEntry] =
  ## Return the last log entry. Returns none if the log is empty.
  var idx: uint64
  withLock store.lock:
    if store.nextIndex <= store.startIndex:
      return none(RaftLogEntry)
    idx = store.nextIndex - 1
  store.getEntry(idx)

proc logEntries*(store: RaftPersistentStore, startIdx: uint64,
    endIdx: uint64): seq[RaftLogEntry] =
  ## Get entries in range [startIdx, endIdx).
  result = @[]
  var idx = startIdx
  while idx < endIdx:
    let entryOpt = store.getEntry(idx)
    if entryOpt.isSome:
      result.add(entryOpt.get())
    else:
      break
    inc idx

proc compact*(store: RaftPersistentStore, lastLogIndex: uint64): bool =
  ## Remove all log entries up to and including lastLogIndex.
  ## Returns true on success.
  withLock store.lock:
    if lastLogIndex < store.startIndex:
      return true # Already compacted

    # Delete entries from startIndex to lastLogIndex
    var idx = store.startIndex
    while idx <= lastLogIndex:
      let key = encodeLogKey(store.groupId, idx)
      discard store.backend.delete(key)
      inc idx

    store.startIndex = lastLogIndex + 1
    # Ensure nextIndex is at least startIndex
    if store.nextIndex < store.startIndex:
      store.nextIndex = store.startIndex
    result = true

proc flush*(store: RaftPersistentStore): bool =
  ## Flush all pending writes to durable storage.
  discard store.backend.flush()
  withLock store.lock:
    store.lastDurableIndex = store.nextIndex - 1
  true

# ============================================================================
# Pack / unpack for snapshot transfer
# ============================================================================

proc packEntries*(store: RaftPersistentStore, startIndex: uint64,
    count: int32): string =
  ## Serialize `count` log entries starting at `startIndex` into a binary buffer.
  ## Format: [count:4][entry1_len:4][entry1_bytes][entry2_len:4][entry2_bytes]...
  var entries: seq[RaftLogEntry] = @[]
  for i in 0..<count:
    let idx = startIndex + uint64(i)
    let entryOpt = store.getEntry(idx)
    if entryOpt.isSome:
      entries.add(entryOpt.get())
    else:
      break

  # Calculate total size
  var totalSize = 4 # count field
  for entry in entries:
    let serialized = serializeLogEntry(entry)
    totalSize += 4 + serialized.len # len prefix + data

  result = newString(totalSize)
  var offset = 0

  # Count (4 bytes big-endian)
  var countVal = uint32(entries.len)
  for i in countdown(3, 0):
    result[offset + i] = char(countVal and 0xFF)
    countVal = countVal shr 8
  offset += 4

  # Each entry: [len:4][data]
  for entry in entries:
    let serialized = serializeLogEntry(entry)
    var entryLen = uint32(serialized.len)
    for i in countdown(3, 0):
      result[offset + i] = char(entryLen and 0xFF)
      entryLen = entryLen shr 8
    offset += 4
    if serialized.len > 0:
      copyMem(addr result[offset], unsafeAddr serialized[0], serialized.len)
    offset += serialized.len

proc applyPack*(store: RaftPersistentStore, startIndex: uint64, data: string) =
  ## Deserialize and apply a packed log from snapshot transfer.
  ## Replaces entries starting at `startIndex`.
  if data.len < 4:
    return

  var offset = 0
  # Count (4 bytes big-endian)
  var count: uint32 = 0
  for i in 0..<4:
    count = (count shl 8) or uint32(data[offset + i].ord)
  offset += 4

  for i in 0..<int(count):
    if offset + 4 > data.len:
      break
    # Entry length (4 bytes big-endian)
    var entryLen: uint32 = 0
    for j in 0..<4:
      entryLen = (entryLen shl 8) or uint32(data[offset + j].ord)
    offset += 4
    if offset + int(entryLen) > data.len:
      break
    let entryData = data[offset .. offset + int(entryLen) - 1]
    offset += int(entryLen)

    let entryOpt = deserializeLogEntry(entryData)
    if entryOpt.isSome:
      let idx = startIndex + uint64(i)
      let key = encodeLogKey(store.groupId, idx)
      discard store.backend.put(key, entryData)

  withLock store.lock:
    store.startIndex = startIndex
    store.nextIndex = startIndex + uint64(count)

# ============================================================================
# State operations
# ============================================================================

proc saveState*(store: RaftPersistentStore, state: RaftState) =
  ## Persist Raft state (term, voted_for, config_hwm) to WiscKey.
  let key = encodeStateKey(store.groupId)
  let value = serializeRaftState(state)
  discard store.backend.put(key, value)

proc loadState*(store: RaftPersistentStore): Option[RaftState] =
  ## Load Raft state from WiscKey. Returns none if no state exists.
  let key = encodeStateKey(store.groupId)
  let value = store.backend.get(key)
  if value.isSome:
    deserializeRaftState(value.get())
  else:
    none(RaftState)

# ============================================================================
# Cluster config operations
# ============================================================================

proc saveClusterConfig*(store: RaftPersistentStore, configData: string) =
  ## Persist cluster configuration to WiscKey.
  ## configData is the serialized cluster_config from NuRaft.
  let key = "/raft/" & $store.groupId & "/config"
  discard store.backend.put(key, configData)

proc loadClusterConfig*(store: RaftPersistentStore): Option[string] =
  ## Load cluster configuration from WiscKey. Returns none if no config exists.
  let key = "/raft/" & $store.groupId & "/config"
  store.backend.get(key)

# ============================================================================
# Utility
# ============================================================================

proc lastDurableIndex*(store: RaftPersistentStore): uint64 =
  ## Return the last durable log index.
  withLock store.lock:
    result = store.lastDurableIndex

proc entryCount*(store: RaftPersistentStore): uint64 =
  ## Return the number of entries in the log.
  withLock store.lock:
    if store.nextIndex > store.startIndex:
      result = store.nextIndex - store.startIndex
    else:
      result = 0
