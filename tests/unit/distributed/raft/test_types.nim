# Unit tests for fractio/distributed/raft/types.nim
# Tests LogEntry encoding/decoding, RaftNodeState, RaftConfig

import std/[unittest, strutils]
import fractio/distributed/raft/types
import fractio/utils/binary

suite "ServerRole":

  test "all roles defined":
    check SR_LEADER.ord == 0
    check SR_CANDIDATE.ord == 1
    check SR_FOLLOWER.ord == 2

suite "LogEntryType":

  test "all entry types defined":
    check LET_NORMAL.ord == 0
    check LET_CONFIG_CHANGE.ord == 1
    check LET_NO_OP.ord == 2

suite "LogEntry Binary Encoding":

  test "encodeLogEntry minimum size":
    let entry = LogEntry(term: 1, entryType: LET_NORMAL, data: "")
    let encoded = encodeLogEntry(entry)
    # Magic (2) + Version (1) + Term (8) + EntryType (1) + DataLen (4) = 16
    check encoded.len == 16

  test "encodeLogEntry with data":
    let entry = LogEntry(term: 5, entryType: LET_NORMAL, data: "hello")
    let encoded = encodeLogEntry(entry)
    # 16 + data.len = 21
    check encoded.len == 16 + 5

  test "encodeLogEntry magic header":
    let entry = LogEntry(term: 1, entryType: LET_NO_OP, data: "")
    let encoded = encodeLogEntry(entry)
    check encoded[0] == char(LOG_ENTRY_MAGIC[0])
    check encoded[1] == char(LOG_ENTRY_MAGIC[1])
    check encoded[2] == char(LOG_ENTRY_VERSION)

  test "encodeLogEntry term encoding":
    let entry = LogEntry(term: 12345, entryType: LET_NORMAL, data: "")
    let encoded = encodeLogEntry(entry)
    var r = initBinaryReader(encoded)
    # Skip magic and version
    discard r.readBytes(3)
    let term = r.readI64()
    check term == 12345

  test "encodeLogEntry entryType encoding":
    for et in [LET_NORMAL, LET_CONFIG_CHANGE, LET_NO_OP]:
      let entry = LogEntry(term: 1, entryType: et, data: "")
      let encoded = encodeLogEntry(entry)
      var r = initBinaryReader(encoded)
      discard r.readBytes(3) # Skip header
      discard r.readI64() # Skip term
      let decodedType = LogEntryType(int(r.readU8()))
      check decodedType == et

suite "LogEntry Binary Decoding":

  test "decodeLogEntry roundtrip":
    let original = LogEntry(term: 42, entryType: LET_NORMAL,
        data: "test payload")
    let encoded = encodeLogEntry(original)
    let decoded = decodeLogEntry(encoded)
    check decoded.term == original.term
    check decoded.entryType == original.entryType
    check decoded.data == original.data

  test "decodeLogEntry empty data":
    let original = LogEntry(term: 1, entryType: LET_NO_OP, data: "")
    let encoded = encodeLogEntry(original)
    let decoded = decodeLogEntry(encoded)
    check decoded.data == ""

  test "decodeLogEntry binary data":
    let original = LogEntry(term: 10, entryType: LET_NORMAL,
        data: "\x00\x01\x02\x03")
    let encoded = encodeLogEntry(original)
    let decoded = decodeLogEntry(encoded)
    check decoded.data == original.data

  test "decodeLogEntry large data":
    let largeData = "x".repeat(1000)
    let original = LogEntry(term: 1, entryType: LET_NORMAL, data: largeData)
    let encoded = encodeLogEntry(original)
    let decoded = decodeLogEntry(encoded)
    check decoded.data == largeData

  test "decodeLogEntry invalid magic raises":
    let invalidData = "\x00\x00\x01" # Wrong magic
    var raised = false
    try:
      discard decodeLogEntry(invalidData)
    except ValueError:
      raised = true
    check raised

  test "decodeLogEntry invalid version raises":
    let invalidData = LOG_ENTRY_MAGIC[0].char & LOG_ENTRY_MAGIC[1].char &
        "\xFF" # Wrong version
    var raised = false
    try:
      discard decodeLogEntry(invalidData)
    except ValueError:
      raised = true
    check raised

  test "decodeLogEntry too small raises":
    let invalidData = "RE" # Just magic, missing version
    var raised = false
    try:
      discard decodeLogEntry(invalidData)
    except ValueError:
      raised = true
    check raised

suite "RaftNodeState":

  test "default state":
    let state = RaftNodeState()
    check state.role == SR_LEADER
    check state.currentTerm == 0
    check state.votedFor == 0
    check state.leaderId == 0
    check state.commitIndex == 0
    check state.lastApplied == 0

  test "initialized state":
    let state = RaftNodeState(
      role: SR_FOLLOWER,
      currentTerm: 5,
      votedFor: 3,
      leaderId: 2,
      commitIndex: 100,
      lastApplied: 95
    )
    check state.role == SR_FOLLOWER
    check state.currentTerm == 5
    check state.votedFor == 3
    check state.leaderId == 2
    check state.commitIndex == 100
    check state.lastApplied == 95

suite "RaftConfig":

  test "default config":
    let config = RaftConfig()
    check config.serverId == 0
    check config.endpoint == ""
    check config.electionTimeout == 0
    check config.heartbeatInterval == 0
    check config.logStoragePath == ""
    check config.snapshotEnabled == false
    check config.snapshotDistance == 0
    check config.maxAppendSize == 0

  test "full config":
    let config = RaftConfig(
      serverId: 1,
      endpoint: "localhost:8080",
      electionTimeout: 500,
      heartbeatInterval: 100,
      logStoragePath: "/var/fractio/raft",
      snapshotEnabled: true,
      snapshotDistance: 1000,
      maxAppendSize: 50
    )
    check config.serverId == 1
    check config.endpoint == "localhost:8080"
    check config.electionTimeout == 500
    check config.heartbeatInterval == 100
    check config.logStoragePath == "/var/fractio/raft"
    check config.snapshotEnabled == true
    check config.snapshotDistance == 1000
    check config.maxAppendSize == 50

suite "RaftNode":

  test "default RaftNode":
    let node = RaftNode()
    check node.serverId == 0
    check node.endpoint == ""
    check not node.initialized
    check not node.isLeader
    check node.leaderId == 0
    check node.commitIndex == 0
    check node.lastApplied == 0

  test "initialized RaftNode":
    let config = RaftConfig(serverId: 5, endpoint: "node5:9000")
    let node = RaftNode(
      serverId: 5,
      endpoint: "node5:9000",
      config: config,
      initialized: true,
      isLeader: true,
      leaderId: 5,
      commitIndex: 50
    )
    check node.serverId == 5
    check node.endpoint == "node5:9000"
    check node.initialized
    check node.isLeader
    check node.leaderId == 5

suite "LogEntry Types":

  test "LET_NORMAL for data entries":
    let entry = LogEntry(term: 1, entryType: LET_NORMAL, data: "command")
    check entry.entryType == LET_NORMAL

  test "LET_CONFIG_CHANGE for config":
    let entry = LogEntry(term: 1, entryType: LET_CONFIG_CHANGE, data: "config")
    check entry.entryType == LET_CONFIG_CHANGE

  test "LET_NO_OP for heartbeats":
    let entry = LogEntry(term: 1, entryType: LET_NO_OP, data: "")
    check entry.entryType == LET_NO_OP

suite "Multiple Entry Roundtrip":

  test "multiple entries roundtrip":
    var entries: seq[LogEntry] = @[]
    for i in 1..10:
      entries.add(LogEntry(term: i.int64, entryType: LET_NORMAL, data: "entry" & $i))

    for entry in entries:
      let encoded = encodeLogEntry(entry)
      let decoded = decodeLogEntry(encoded)
      check decoded.term == entry.term
      check decoded.entryType == entry.entryType
      check decoded.data == entry.data

suite "Term Encoding Edge Cases":

  test "zero term":
    let entry = LogEntry(term: 0, entryType: LET_NORMAL, data: "")
    let decoded = decodeLogEntry(encodeLogEntry(entry))
    check decoded.term == 0

  test "negative term (invalid but test encoding)":
    let entry = LogEntry(term: -1, entryType: LET_NORMAL, data: "")
    let decoded = decodeLogEntry(encodeLogEntry(entry))
    check decoded.term == -1

  test "large term":
    let entry = LogEntry(term: 0x7FFFFFFFFFFFFFFF, entryType: LET_NORMAL, data: "")
    let decoded = decodeLogEntry(encodeLogEntry(entry))
    check decoded.term == 0x7FFFFFFFFFFFFFFF
