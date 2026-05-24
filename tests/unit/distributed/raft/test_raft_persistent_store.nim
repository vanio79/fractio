# Unit tests for RaftPersistentStore — WiscKey-backed Raft log and state persistence
#
# Tests:
# - Log entry serialization/deserialization
# - Raft state serialization/deserialization
# - RaftPersistentStore: append, get, write_at, compact, pack/apply_pack
# - State save/load round-trip
# - Recovery from WiscKey after simulated restart

import std/[unittest, options, strutils]
import fractio/distributed/raft/raft_persistent_store
import fractio/distributed/raft/group_types
import fractio/distributed/meta/system_tables
import fractio/storage/wisckey_backend
import fractio/storage/backend
import fractio/core/types

proc newTestBackend(path: string): WiscKeyBackend =
  let backend = newWiscKeyBackend(StorageConfig(
    path: path,
    createIfMissing: true,
    syncWrites: false,
    writeBufferSize: 4 * 1024 * 1024,
    blockCacheSize: 1 * 1024 * 1024,
    vlogMaxSize: 8 * 1024 * 1024,
    vlogCleanThreshold: 1000,
    vlogMinCleanThreshold: 100,
    vlogCleanBufferSize: 1 * 1024 * 1024
  ))
  discard backend.open(StorageConfig(
    path: path,
    createIfMissing: true,
    syncWrites: false
  ))
  result = backend

suite "Log Entry Serialization":
  test "serialize and deserialize app_log entry":
    let entry = RaftLogEntry(term: 5, valType: lvtAppLog, data: "hello world")
    let serialized = serializeLogEntry(entry)
    let deserialized = deserializeLogEntry(serialized)
    check deserialized.isSome
    check deserialized.get().term == 5
    check deserialized.get().valType == lvtAppLog
    check deserialized.get().data == "hello world"

  test "serialize and deserialize cluster_config entry":
    let entry = RaftLogEntry(term: 10, valType: lvtClusterConfig, data: "")
    let serialized = serializeLogEntry(entry)
    let deserialized = deserializeLogEntry(serialized)
    check deserialized.isSome
    check deserialized.get().term == 10
    check deserialized.get().valType == lvtClusterConfig
    check deserialized.get().data == ""

  test "serialize and deserialize entry with binary data":
    var binaryData = newString(256)
    for i in 0..<256:
      binaryData[i] = char(i)
    let entry = RaftLogEntry(term: 42, valType: lvtAppLog, data: binaryData)
    let serialized = serializeLogEntry(entry)
    let deserialized = deserializeLogEntry(serialized)
    check deserialized.isSome
    check deserialized.get().term == 42
    check deserialized.get().data.len == 256
    for i in 0..<256:
      check deserialized.get().data[i] == char(i)

  test "deserialize empty data returns none":
    let empty = ""
    check deserializeLogEntry(empty).isNone

  test "deserialize truncated data returns none":
    let entry = RaftLogEntry(term: 1, valType: lvtAppLog, data: "test")
    let serialized = serializeLogEntry(entry)
    # Truncate to just magic + term (10 bytes), missing val_type and data
    let truncated = serialized[0..<10]
    check deserializeLogEntry(truncated).isNone

  test "deserialize with wrong magic returns none":
    var data = newString(20)
    data[0] = '\xFF'
    data[1] = '\xFF'
    check deserializeLogEntry(data).isNone

  test "large term values":
    let entry = RaftLogEntry(term: high(uint64), valType: lvtAppLog, data: "x")
    let serialized = serializeLogEntry(entry)
    let deserialized = deserializeLogEntry(serialized)
    check deserialized.isSome
    check deserialized.get().term == high(uint64)

suite "Raft State Serialization":
  test "serialize and deserialize v3 format":
    let state = RaftState(term: 42, votedFor: 3, configLogIdxHwm: 100)
    let serialized = serializeRaftState(state)
    check serialized.len == 26
    let deserialized = deserializeRaftState(serialized)
    check deserialized.isSome
    check deserialized.get().term == 42
    check deserialized.get().votedFor == 3
    check deserialized.get().configLogIdxHwm == 100

  test "deserialize v3 format round-trips":
    let state = RaftState(term: 42, votedFor: 3, configLogIdxHwm: 100)
    let serialized = serializeRaftState(state)
    check serialized.len == 26
    let deserialized = deserializeRaftState(serialized)
    check deserialized.isSome
    check deserialized.get().term == 42
    check deserialized.get().votedFor == 3
    check deserialized.get().configLogIdxHwm == 100

  test "reject data without magic header":
    # 24 bytes with no magic — this was the old v2 format, now rejected
    var data = newString(24)
    var term: uint64 = 7
    for i in countdown(7, 0):
      data[i] = char(term and 0xFF)
      term = term shr 8
    var vf: int32 = 1
    for i in countdown(11, 8):
      data[i] = char(vf and 0xFF)
      vf = vf shr 8
    for i in 12..<16:
      data[i] = '\0'
    var hwm: uint64 = 50
    for i in countdown(23, 16):
      data[i] = char(hwm and 0xFF)
      hwm = hwm shr 8
    check deserializeRaftState(data).isNone

  test "reject short data (16 bytes, old v1 format)":
    # 16 bytes with no magic — this was the old v1 format, now rejected
    var data = newString(16)
    var term: uint64 = 15
    for i in countdown(7, 0):
      data[i] = char(term and 0xFF)
      term = term shr 8
    var vf: int32 = 5
    for i in countdown(11, 8):
      data[i] = char(vf and 0xFF)
      vf = vf shr 8
    for i in 12..<16:
      data[i] = '\0'
    check deserializeRaftState(data).isNone

  test "deserialize too-short data returns none":
    let short = newString(10)
    check deserializeRaftState(short).isNone

  test "deserialize data with wrong magic returns none":
    # 26 bytes but wrong magic header
    var data = newString(26)
    data[0] = char(0xFF) # wrong magic
    data[1] = char(0xFF)
    check deserializeRaftState(data).isNone

  test "zero term state":
    let state = RaftState(term: 0, votedFor: -1, configLogIdxHwm: 0)
    let serialized = serializeRaftState(state)
    let deserialized = deserializeRaftState(serialized)
    check deserialized.isSome
    check deserialized.get().term == 0
    check deserialized.get().votedFor == -1
    check deserialized.get().configLogIdxHwm == 0

  test "negative votedFor":
    let state = RaftState(term: 1, votedFor: -1, configLogIdxHwm: 0)
    let serialized = serializeRaftState(state)
    let deserialized = deserializeRaftState(serialized)
    check deserialized.isSome
    check deserialized.get().votedFor == -1

suite "RaftPersistentStore - Log Operations":
  # Single shared backend for the entire suite to avoid LevelDB open/close issues
  var backend: WiscKeyBackend
  let testDir = "/tmp/fractio_test_raft_log_" & $genULIDLocal()

  setup:
    if backend.isNil or not backend.isOpen:
      backend = newTestBackend(testDir)

  test "append and get entries":
    let groupId = GroupID(systemTableULID(10'u8))
    var store = newRaftPersistentStore(backend, groupId)
    let idx1 = store.appendEntry(RaftLogEntry(term: 1, valType: lvtAppLog,
        data: "entry1"))
    check idx1 == 1
    let idx2 = store.appendEntry(RaftLogEntry(term: 1, valType: lvtAppLog,
        data: "entry2"))
    check idx2 == 2
    let idx3 = store.appendEntry(RaftLogEntry(term: 2, valType: lvtAppLog,
        data: "entry3"))
    check idx3 == 3
    check store.getEntry(1).isSome
    check store.getEntry(1).get().term == 1
    check store.getEntry(1).get().data == "entry1"
    check store.getEntry(2).isSome
    check store.getEntry(2).get().data == "entry2"
    check store.getEntry(3).isSome
    check store.getEntry(3).get().term == 2
    store.close()

  test "nextSlot and startIndex":
    let groupId = GroupID(systemTableULID(11'u8))
    var store = newRaftPersistentStore(backend, groupId)
    check store.nextSlot() == 1
    check store.startIndex() == 1
    discard store.appendEntry(RaftLogEntry(term: 1, valType: lvtAppLog, data: "a"))
    check store.nextSlot() == 2
    check store.startIndex() == 1
    store.close()

  test "termAt returns correct term":
    let groupId = GroupID(systemTableULID(12'u8))
    var store = newRaftPersistentStore(backend, groupId)
    discard store.appendEntry(RaftLogEntry(term: 1, valType: lvtAppLog, data: "a"))
    discard store.appendEntry(RaftLogEntry(term: 2, valType: lvtAppLog, data: "b"))
    discard store.appendEntry(RaftLogEntry(term: 2, valType: lvtAppLog, data: "c"))
    check store.termAt(1) == 1
    check store.termAt(2) == 2
    check store.termAt(3) == 2
    check store.termAt(4) == 0
    store.close()

  test "lastEntry returns last appended entry":
    let groupId = GroupID(systemTableULID(13'u8))
    var store = newRaftPersistentStore(backend, groupId)
    check store.lastEntry().isNone
    discard store.appendEntry(RaftLogEntry(term: 1, valType: lvtAppLog, data: "a"))
    discard store.appendEntry(RaftLogEntry(term: 2, valType: lvtAppLog, data: "b"))
    let last = store.lastEntry()
    check last.isSome
    check last.get().term == 2
    check last.get().data == "b"
    store.close()

  test "writeAt truncates entries after index":
    let groupId = GroupID(systemTableULID(14'u8))
    var store = newRaftPersistentStore(backend, groupId)
    discard store.appendEntry(RaftLogEntry(term: 1, valType: lvtAppLog, data: "a"))
    discard store.appendEntry(RaftLogEntry(term: 1, valType: lvtAppLog, data: "b"))
    discard store.appendEntry(RaftLogEntry(term: 2, valType: lvtAppLog, data: "c"))
    store.writeAt(2, RaftLogEntry(term: 3, valType: lvtAppLog, data: "b2"))
    check store.nextSlot() == 3
    check store.getEntry(1).get().data == "a"
    check store.getEntry(2).get().data == "b2"
    check store.getEntry(3).isNone
    store.close()

  test "logEntries returns range of entries":
    let groupId = GroupID(systemTableULID(15'u8))
    var store = newRaftPersistentStore(backend, groupId)
    discard store.appendEntry(RaftLogEntry(term: 1, valType: lvtAppLog, data: "a"))
    discard store.appendEntry(RaftLogEntry(term: 1, valType: lvtAppLog, data: "b"))
    discard store.appendEntry(RaftLogEntry(term: 2, valType: lvtAppLog, data: "c"))
    discard store.appendEntry(RaftLogEntry(term: 2, valType: lvtAppLog, data: "d"))
    let entries = store.logEntries(2, 4)
    check entries.len == 2
    check entries[0].data == "b"
    check entries[1].data == "c"
    store.close()

  test "compact removes entries up to lastLogIndex":
    let groupId = GroupID(systemTableULID(16'u8))
    var store = newRaftPersistentStore(backend, groupId)
    for i in 1..5:
      discard store.appendEntry(RaftLogEntry(term: 1, valType: lvtAppLog,
          data: "e" & $i))
    check store.startIndex() == 1
    check store.compact(3)
    check store.startIndex() == 4
    check store.getEntry(1).isNone
    check store.getEntry(2).isNone
    check store.getEntry(3).isNone
    check store.getEntry(4).isSome
    check store.getEntry(5).isSome
    store.close()

  test "compact with index below startIndex is no-op":
    let groupId = GroupID(systemTableULID(17'u8))
    var store = newRaftPersistentStore(backend, groupId)
    discard store.appendEntry(RaftLogEntry(term: 1, valType: lvtAppLog, data: "a"))
    check store.compact(0)
    check store.startIndex() == 1
    store.close()

  test "flush returns true":
    let groupId = GroupID(systemTableULID(18'u8))
    var store = newRaftPersistentStore(backend, groupId)
    discard store.appendEntry(RaftLogEntry(term: 1, valType: lvtAppLog, data: "a"))
    check store.flush()
    store.close()

  test "packEntries and applyPack round-trip":
    let groupId1 = GroupID(systemTableULID(19'u8))
    var store = newRaftPersistentStore(backend, groupId1)
    for i in 1..4:
      discard store.appendEntry(RaftLogEntry(term: 1 + uint64(i div 2),
          valType: lvtAppLog, data: "entry" & $i))
    let packed = store.packEntries(1, 4)
    check packed.len > 0
    # Apply to a different group
    let groupId2 = GroupID(systemTableULID(20'u8))
    var store2 = newRaftPersistentStore(backend, groupId2)
    store2.applyPack(5, packed)
    check store2.getEntry(5).isSome
    check store2.getEntry(5).get().data == "entry1"
    check store2.getEntry(6).get().data == "entry2"
    check store2.getEntry(8).get().data == "entry4"
    store2.close()
    store.close()

  test "entryCount tracks entries correctly":
    let groupId = GroupID(systemTableULID(21'u8))
    var store = newRaftPersistentStore(backend, groupId)
    check store.entryCount() == 0
    discard store.appendEntry(RaftLogEntry(term: 1, valType: lvtAppLog, data: "a"))
    check store.entryCount() == 1
    discard store.appendEntry(RaftLogEntry(term: 1, valType: lvtAppLog, data: "b"))
    check store.entryCount() == 2
    store.writeAt(2, RaftLogEntry(term: 2, valType: lvtAppLog, data: "b2"))
    check store.entryCount() == 2
    store.close()

suite "RaftPersistentStore - State Operations":
  var backend: WiscKeyBackend
  let testDir = "/tmp/fractio_test_raft_state_" & $genULIDLocal()

  setup:
    if backend.isNil or not backend.isOpen:
      backend = newTestBackend(testDir)

  test "save and load state":
    let groupId = GroupID(systemTableULID(30'u8))
    var store = newRaftPersistentStore(backend, groupId)
    let state = RaftState(term: 42, votedFor: 3, configLogIdxHwm: 100)
    store.saveState(state)
    let loaded = store.loadState()
    check loaded.isSome
    check loaded.get().term == 42
    check loaded.get().votedFor == 3
    check loaded.get().configLogIdxHwm == 100
    store.close()

  test "load state when none exists returns none":
    let groupId = GroupID(systemTableULID(31'u8))
    var store = newRaftPersistentStore(backend, groupId)
    let loaded = store.loadState()
    check loaded.isNone
    store.close()

  test "save overwrites previous state":
    let groupId = GroupID(systemTableULID(32'u8))
    var store = newRaftPersistentStore(backend, groupId)
    store.saveState(RaftState(term: 1, votedFor: 1, configLogIdxHwm: 10))
    store.saveState(RaftState(term: 5, votedFor: 2, configLogIdxHwm: 50))
    let loaded = store.loadState()
    check loaded.isSome
    check loaded.get().term == 5
    check loaded.get().votedFor == 2
    check loaded.get().configLogIdxHwm == 50
    store.close()

  test "save and load cluster config":
    let groupId = GroupID(systemTableULID(33'u8))
    var store = newRaftPersistentStore(backend, groupId)
    let configData = "{\"servers\":[{\"id\":1,\"endpoint\":\"1@localhost:9001\"}]}"
    store.saveClusterConfig(configData)
    let loaded = store.loadClusterConfig()
    check loaded.isSome
    check loaded.get() == configData
    store.close()

  test "load cluster config when none exists returns none":
    let groupId = GroupID(systemTableULID(34'u8))
    var store = newRaftPersistentStore(backend, groupId)
    let loaded = store.loadClusterConfig()
    check loaded.isNone
    store.close()

suite "RaftPersistentStore - Key Encoding":
  test "encodeLogKey produces correct format":
    let groupId = GroupID(systemTableULID(5'u8))
    let key = encodeLogKey(groupId, 42'u64)
    check key.startsWith("/raft/")
    check "/log/" in key
    check key.endsWith("/42")

  test "encodeStateKey produces correct format":
    let groupId = GroupID(systemTableULID(5'u8))
    let key = encodeStateKey(groupId)
    check key.startsWith("/raft/")
    check key.endsWith("/state")

  test "different groups produce different keys":
    let group1 = GroupID(systemTableULID(1'u8))
    let group2 = GroupID(systemTableULID(2'u8))
    let key1 = encodeLogKey(group1, 1'u64)
    let key2 = encodeLogKey(group2, 1'u64)
    check key1 != key2
