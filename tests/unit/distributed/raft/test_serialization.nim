# Unit Tests for Serialization/Deserialization

import unittest
import std/[json, strutils, options]

import fractio/distributed/raft/types
import fractio/distributed/raft/node
import fractio/distributed/raft/cluster

# Helper procs for serialization testing
proc serializeLogEntry*(entry: LogEntry): string =
  ## Serialize a log entry to JSON string
  let escapedData = entry.data.multiReplace(("\"", "\\\""), ("\\", "\\\\"), (
      "\n", "\\n"), ("\r", "\\r"), ("\t", "\\t"))
  result = """{"term": """ & $entry.term &
    ", \"type\": \"" & $entry.entryType &
    "\", \"data\": \"" & escapedData & "\"}"

proc deserializeLogEntry*(data: string): Option[LogEntry] =
  ## Deserialize a log entry from JSON string
  try:
    let jsonNode = parseJson(data)
    result = some(LogEntry(
      term: jsonNode["term"].getInt(),
      entryType: parseEnum[LogEntryType](jsonNode["type"].getStr()),
      data: jsonNode["data"].getStr()
    ))
  except JsonParsingError, KeyError, ValueError:
    result = none(LogEntry)


# Test Suite
suite "Log Entry Serialization Tests":

  test "Serialize basic log entry":
    let entry = LogEntry(
      term: 1,
      entryType: LET_NORMAL,
      data: "test data"
    )

    let serialized = serializeLogEntry(entry)
    check "\"term\": 1" in serialized
    check "\"type\": \"LET_NORMAL\"" in serialized
    check "\"data\": \"test data\"" in serialized

  test "Deserialize basic log entry":
    let serialized = """{"term": 1, "type": "LET_NORMAL", "data": "test data"}"""
    let entry = deserializeLogEntry(serialized)

    check entry.isSome
    check entry.get.term == 1
    check entry.get.entryType == LET_NORMAL
    check entry.get.data == "test data"

  test "Round-trip serialization":
    let original = LogEntry(
      term: 42,
      entryType: LET_CONFIG_CHANGE,
      data: "round trip test"
    )

    let serialized = serializeLogEntry(original)
    let restored = deserializeLogEntry(serialized)

    check restored.isSome
    check restored.get.term == original.term
    check restored.get.entryType == original.entryType
    check restored.get.data == original.data

  test "Serialize all entry types":
    let types = [LET_NORMAL, LET_CONFIG_CHANGE, LET_NO_OP]

    for entryType in types:
      let entry = LogEntry(term: 1, entryType: entryType, data: "test")
      let serialized = serializeLogEntry(entry)
      let restored = deserializeLogEntry(serialized)

      check restored.isSome
      check restored.get.entryType == entryType

  test "Serialize entry with empty data":
    let entry = LogEntry(term: 1, entryType: LET_NORMAL, data: "")
    let serialized = serializeLogEntry(entry)
    let restored = deserializeLogEntry(serialized)

    check restored.isSome
    check restored.get.data == ""

  test "Serialize entry with special characters":
    let specialData = "data\nwith\nnewlines\tand\"quotes\""
    let entry = LogEntry(term: 1, entryType: LET_NORMAL, data: specialData)
    let serialized = serializeLogEntry(entry)
    let restored = deserializeLogEntry(serialized)

    check restored.isSome
    check restored.get.data == specialData

  test "Serialize entry with large data":
    let largeData = "x".repeat(10000)
    let entry = LogEntry(term: 1, entryType: LET_NORMAL, data: largeData)
    let serialized = serializeLogEntry(entry)
    let restored = deserializeLogEntry(serialized)

    check restored.isSome
    check restored.get.data == largeData

  test "Deserialize invalid JSON":
    let invalid = "not valid json"
    let entry = deserializeLogEntry(invalid)
    check entry.isNone

  test "Deserialize missing fields":
    let incomplete = """{"term": 1}"""
    let entry = deserializeLogEntry(incomplete)
    check entry.isNone

  test "Deserialize invalid entry type":
    let invalidType = """{"term": 1, "type": "INVALID", "data": "test"}"""
    let entry = deserializeLogEntry(invalidType)
    check entry.isNone


suite "Cluster Configuration Binary Serialization Tests":

  test "Encode cluster configuration":
    let cluster = newRaftCluster(RaftConfig(
      serverId: 1,
      endpoint: "127.0.0.1:9000",
      electionTimeout: 1000,
      heartbeatInterval: 100,
      logStoragePath: "/tmp/raft",
      snapshotEnabled: false,
      snapshotDistance: 1000
    ))

    discard cluster.addServer(1, "127.0.0.1:9000")
    discard cluster.addServer(2, "127.0.0.1:9001")
    discard cluster.addServer(3, "127.0.0.1:9002")

    let encoded = cluster.encodeCluster()
    # Check magic header
    check encoded[0] == 'R'
    check encoded[1] == 'C'
    check encoded[2] == 'L'
    check encoded[3].ord == 1 # version

  test "Decode cluster configuration":
    let cluster = newRaftCluster(RaftConfig(
      serverId: 1,
      endpoint: "127.0.0.1:9000",
      electionTimeout: 1000,
      heartbeatInterval: 100,
      logStoragePath: "/tmp/raft",
      snapshotEnabled: false,
      snapshotDistance: 1000
    ))

    discard cluster.addServer(1, "127.0.0.1:9000")
    discard cluster.addServer(2, "127.0.0.1:9001")

    let encoded = cluster.encodeCluster()
    let restored = decodeCluster(encoded)

    check restored.selfId == cluster.selfId
    check restored.getServerCount() == cluster.getServerCount()
    check restored.config.serverId == cluster.config.serverId
    check restored.config.electionTimeout == cluster.config.electionTimeout
    check restored.config.heartbeatInterval == cluster.config.heartbeatInterval

  test "Cluster round-trip binary serialization":
    let original = newRaftCluster(RaftConfig(
      serverId: 1,
      endpoint: "127.0.0.1:9000",
      electionTimeout: 500,
      heartbeatInterval: 50,
      logStoragePath: "/data/raft",
      snapshotEnabled: true,
      snapshotDistance: 500
    ))

    discard original.addServer(1, "127.0.0.1:9000")
    discard original.addServer(2, "127.0.0.1:9001")
    discard original.addServer(3, "127.0.0.1:9002")
    discard original.addServer(4, "127.0.0.1:9003")
    discard original.addServer(5, "127.0.0.1:9004")

    let encoded = original.encodeCluster()
    let restored = decodeCluster(encoded)

    check restored.selfId == original.selfId
    check restored.getServerCount() == original.getServerCount()
    check restored.getServerCount() == 5
    check restored.config.snapshotEnabled == original.config.snapshotEnabled
    check restored.config.logStoragePath == original.config.logStoragePath

    # Verify all servers
    for serverId in original.getServers():
      let origEndpoint = original.getServerEndpoint(serverId)
      let restEndpoint = restored.getServerEndpoint(serverId)
      check origEndpoint.isSome
      check restEndpoint.isSome
      check origEndpoint.get == restEndpoint.get

  test "Cluster binary with invalid magic":
    let invalidData = "INVALID"
    var raised = false
    try:
      discard decodeCluster(invalidData)
    except ValueError:
      raised = true
    check raised

  test "Cluster binary with empty servers":
    let cluster = newRaftCluster(RaftConfig(
      serverId: 1,
      endpoint: "127.0.0.1:9000",
      electionTimeout: 1000,
      heartbeatInterval: 100,
      logStoragePath: "/tmp/raft",
      snapshotEnabled: false,
      snapshotDistance: 1000
    ))

    let encoded = cluster.encodeCluster()
    let restored = decodeCluster(encoded)

    check restored.getServerCount() == 0
    check restored.selfId == cluster.selfId


suite "RaftNodeState Serialization Tests":

  test "RaftNodeState basic fields":
    let state = RaftNodeState(
      role: SR_LEADER,
      currentTerm: 5,
      votedFor: 3,
      leaderId: 1,
      commitIndex: 100,
      lastApplied: 95
    )

    check state.role == SR_LEADER
    check state.currentTerm == 5
    check state.votedFor == 3
    check state.leaderId == 1
    check state.commitIndex == 100
    check state.lastApplied == 95

  test "RaftNodeState role transitions":
    var state = RaftNodeState(
      role: SR_FOLLOWER,
      currentTerm: 0,
      votedFor: -1,
      leaderId: -1,
      commitIndex: 0,
      lastApplied: 0
    )

    # Follower -> Candidate
    state.role = SR_CANDIDATE
    state.currentTerm = 1
    state.votedFor = 1
    check state.role == SR_CANDIDATE

    # Candidate -> Leader
    state.role = SR_LEADER
    check state.role == SR_LEADER


suite "RaftConfig Serialization Tests":

  test "RaftConfig basic fields":
    let config = RaftConfig(
      serverId: 1,
      endpoint: "127.0.0.1:9000",
      electionTimeout: 1000,
      heartbeatInterval: 100,
      logStoragePath: "/tmp/raft",
      snapshotEnabled: true,
      snapshotDistance: 1000
    )

    check config.serverId == 1
    check config.endpoint == "127.0.0.1:9000"
    check config.electionTimeout == 1000
    check config.heartbeatInterval == 100
    check config.logStoragePath == "/tmp/raft"
    check config.snapshotEnabled == true
    check config.snapshotDistance == 1000

  test "RaftConfig with different values":
    let config = RaftConfig(
      serverId: 99,
      endpoint: "192.168.1.100:8080",
      electionTimeout: 5000,
      heartbeatInterval: 500,
      logStoragePath: "/var/lib/raft",
      snapshotEnabled: false,
      snapshotDistance: 500
    )

    check config.serverId == 99
    check config.endpoint == "192.168.1.100:8080"
    check config.electionTimeout == 5000
    check config.heartbeatInterval == 500


suite "RPC Message Tests":

  test "RaftRPC creation":
    let rpc = RaftRPC(
      rpcType: RPC_APPEND_ENTRIES,
      term: 1,
      leaderId: 1,
      prevLogIndex: 0,
      prevLogTerm: 0,
      entries: @[LogEntry(term: 1, entryType: LET_NORMAL, data: "test")],
      leaderCommit: 0,
      success: false
    )

    check rpc.rpcType == RPC_APPEND_ENTRIES
    check rpc.term == 1
    check rpc.leaderId == 1
    check rpc.entries.len == 1

  test "RaftRPC response":
    let response = RaftRPC(
      rpcType: RPC_APPEND_ENTRIES,
      term: 1,
      leaderId: 1,
      success: true
    )

    check response.success == true
    check response.rpcType == RPC_APPEND_ENTRIES

  test "RaftRPC with multiple entries":
    let entries = [
      LogEntry(term: 1, entryType: LET_NORMAL, data: "e1"),
      LogEntry(term: 1, entryType: LET_NORMAL, data: "e2"),
      LogEntry(term: 1, entryType: LET_NORMAL, data: "e3")
    ]

    let rpc = RaftRPC(
      rpcType: RPC_APPEND_ENTRIES,
      term: 1,
      leaderId: 1,
      prevLogIndex: 5,
      prevLogTerm: 1,
      entries: @entries,
      leaderCommit: 5,
      success: false
    )

    check rpc.entries.len == 3
    check rpc.prevLogIndex == 5
    check rpc.leaderCommit == 5


suite "Edge Cases and Error Handling":

  test "Empty log entry data":
    let entry = LogEntry(term: 1, entryType: LET_NORMAL, data: "")
    check entry.data == ""

  test "Log entry with unicode":
    let unicodeData = "unicode: \u0048\u0065\u006C\u006C\u006F \u4E16\u754C"
    let entry = LogEntry(term: 1, entryType: LET_NORMAL, data: unicodeData)
    check entry.data == unicodeData

  test "Log entry with very long term":
    let entry = LogEntry(term: 9223372036854775807, entryType: LET_NORMAL, data: "test")
    check entry.term == 9223372036854775807

  test "Cluster with many servers":
    let cluster = newRaftCluster(RaftConfig(
      serverId: 1,
      endpoint: "127.0.0.1:9000",
      electionTimeout: 1000,
      heartbeatInterval: 100,
      logStoragePath: "/tmp/raft",
      snapshotEnabled: false,
      snapshotDistance: 1000
    ))

    for i in 1..100:
      discard cluster.addServer(int32(i), "127.0.0.1:" & $(9000 + i))

    check cluster.getServerCount() == 100
    check cluster.getMajority() == 51
    check cluster.getQuorum() == 51

  test "Cluster majority calculation":
    # 1 node -> majority 1
    let c1 = newRaftCluster(RaftConfig())
    discard c1.addServer(1, "addr")
    check c1.getMajority() == 1

    # 2 nodes -> majority 2
    let c2 = newRaftCluster(RaftConfig())
    discard c2.addServer(1, "addr1")
    discard c2.addServer(2, "addr2")
    check c2.getMajority() == 2

    # 3 nodes -> majority 2
    let c3 = newRaftCluster(RaftConfig())
    discard c3.addServer(1, "addr1")
    discard c3.addServer(2, "addr2")
    discard c3.addServer(3, "addr3")
    check c3.getMajority() == 2

    # 5 nodes -> majority 3
    let c5 = newRaftCluster(RaftConfig())
    for i in 1..5:
      discard c5.addServer(int32(i), "addr" & $i)
    check c5.getMajority() == 3
