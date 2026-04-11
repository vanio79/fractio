# Unit tests for Raft serialization (binary and JSON)
# Tests LogEntry, RaftNodeState, RaftConfig, RaftRPC serialization

import std/[unittest, json, strutils, options, tables]
import fractio/distributed/raft/types
import fractio/distributed/raft/node
import fractio/distributed/raft/cluster
import fractio/utils/binary

suite "LogEntry Binary Serialization Extended":
  test "encode with minimum term":
    let entry = LogEntry(term: 0'i64, entryType: LET_NORMAL, data: "")
    let encoded = encodeLogEntry(entry)
    let decoded = decodeLogEntry(encoded)
    check decoded.term == 0

  test "encode with maximum term":
    let entry = LogEntry(term: int64.high, entryType: LET_NORMAL, data: "")
    let encoded = encodeLogEntry(entry)
    let decoded = decodeLogEntry(encoded)
    check decoded.term == int64.high

  test "encode with negative term":
    let entry = LogEntry(term: -1'i64, entryType: LET_NORMAL, data: "")
    let encoded = encodeLogEntry(entry)
    let decoded = decodeLogEntry(encoded)
    check decoded.term == -1

  test "encode all entry types":
    for et in [LET_NORMAL, LET_CONFIG_CHANGE, LET_NO_OP]:
      let entry = LogEntry(term: 1, entryType: et, data: "data")
      let encoded = encodeLogEntry(entry)
      let decoded = decodeLogEntry(encoded)
      check decoded.entryType == et

  test "encode with unicode data":
    let unicode = "Hello 世界 🚀"
    let entry = LogEntry(term: 1, entryType: LET_NORMAL, data: unicode)
    let encoded = encodeLogEntry(entry)
    let decoded = decodeLogEntry(encoded)
    check decoded.data == unicode

  test "encode with null bytes in data":
    let nullData = "before\x00after"
    let entry = LogEntry(term: 1, entryType: LET_NORMAL, data: nullData)
    let encoded = encodeLogEntry(entry)
    let decoded = decodeLogEntry(encoded)
    check decoded.data == nullData

  test "encode with very large data":
    let largeData = "x".repeat(100000)
    let entry = LogEntry(term: 1, entryType: LET_NORMAL, data: largeData)
    let encoded = encodeLogEntry(entry)
    check encoded.len > 100000
    let decoded = decodeLogEntry(encoded)
    check decoded.data == largeData

  test "encode preserves data exactly":
    let special = "\x00\x01\x02\x03\xff\xfe\xfd\xfc"
    let entry = LogEntry(term: 1, entryType: LET_NORMAL, data: special)
    let encoded = encodeLogEntry(entry)
    let decoded = decodeLogEntry(encoded)
    check decoded.data.len == special.len
    for i in 0..<decoded.data.len:
      check decoded.data[i] == special[i]

  test "decode rejects truncated after header":
    let entry = LogEntry(term: 1, entryType: LET_NORMAL, data: "test")
    let encoded = encodeLogEntry(entry)
    let truncated = encoded[0..2]
    expect ValueError:
      discard decodeLogEntry(truncated)

  test "decode rejects truncated term":
    var w = initBinaryWriter()
    w.writeBytes(LOG_ENTRY_MAGIC)
    w.writeU8(LOG_ENTRY_VERSION)
    w.writeI64(1'i64)
    let partial = w.finish()[0..10]
    expect ValueError:
      discard decodeLogEntry(partial)

  test "decode rejects truncated data length":
    var w = initBinaryWriter()
    w.writeBytes(LOG_ENTRY_MAGIC)
    w.writeU8(LOG_ENTRY_VERSION)
    w.writeI64(1'i64)
    w.writeU8(0'u8)
    w.writeU32(100'u32)
    let partial = w.finish()[0..15]
    expect ValueError:
      discard decodeLogEntry(partial)

suite "LogEntry JSON Serialization":
  proc serializeLogEntryJson(entry: LogEntry): JsonNode =
    %*{
      "term": entry.term,
      "entryType": $entry.entryType,
      "data": entry.data
    }

  proc deserializeLogEntryJson(j: JsonNode): Option[LogEntry] =
    try:
      some(LogEntry(
        term: j["term"].getInt(),
        entryType: parseEnum[LogEntryType](j["entryType"].getStr()),
        data: j["data"].getStr()
      ))
    except:
      none(LogEntry)

  test "serialize basic entry":
    let entry = LogEntry(term: 42, entryType: LET_NORMAL, data: "test")
    let json = serializeLogEntryJson(entry)
    check json["term"].getInt == 42
    check json["entryType"].getStr == "LET_NORMAL"
    check json["data"].getStr == "test"

  test "deserialize basic entry":
    let json = %*{"term": 42, "entryType": "LET_NORMAL", "data": "test"}
    let entry = deserializeLogEntryJson(json)
    check entry.isSome
    check entry.get.term == 42
    check entry.get.entryType == LET_NORMAL
    check entry.get.data == "test"

  test "roundtrip all entry types":
    for et in [LET_NORMAL, LET_CONFIG_CHANGE, LET_NO_OP]:
      let entry = LogEntry(term: 1, entryType: et, data: "data")
      let json = serializeLogEntryJson(entry)
      let restored = deserializeLogEntryJson(json)
      check restored.isSome
      check restored.get.entryType == et

  test "roundtrip with empty data":
    let entry = LogEntry(term: 1, entryType: LET_NORMAL, data: "")
    let json = serializeLogEntryJson(entry)
    let restored = deserializeLogEntryJson(json)
    check restored.isSome
    check restored.get.data == ""

  test "deserialize invalid entry type":
    let json = %*{"term": 1, "entryType": "INVALID", "data": "test"}
    let entry = deserializeLogEntryJson(json)
    check entry.isNone

  test "deserialize missing term":
    let json = %*{"entryType": "LET_NORMAL", "data": "test"}
    let entry = deserializeLogEntryJson(json)
    check entry.isNone

  test "deserialize missing data":
    let json = %*{"term": 1, "entryType": "LET_NORMAL"}
    let entry = deserializeLogEntryJson(json)
    check entry.isNone

suite "RaftNodeState Serialization":
  test "state fields correct":
    let state = RaftNodeState(
      role: SR_LEADER,
      currentTerm: 10,
      votedFor: 5,
      leaderId: 1,
      commitIndex: 100,
      lastApplied: 95
    )
    check state.role == SR_LEADER
    check state.currentTerm == 10
    check state.votedFor == 5
    check state.leaderId == 1
    check state.commitIndex == 100
    check state.lastApplied == 95

  test "state default values":
    let state = RaftNodeState()
    check state.role == SR_LEADER
    check state.currentTerm == 0
    check state.votedFor == 0

  test "state role transitions":
    var state = RaftNodeState(role: SR_FOLLOWER, currentTerm: 0)
    state.role = SR_CANDIDATE
    state.currentTerm = 1
    state.votedFor = 1
    check state.role == SR_CANDIDATE
    state.role = SR_LEADER
    check state.role == SR_LEADER

suite "RaftConfig Serialization":
  test "config all fields":
    let config = RaftConfig(
      serverId: 1,
      endpoint: "localhost:9000",
      electionTimeout: 500,
      heartbeatInterval: 100,
      logStoragePath: "/var/raft",
      snapshotEnabled: true,
      snapshotDistance: 1000,
      maxAppendSize: 50
    )
    check config.serverId == 1
    check config.endpoint == "localhost:9000"
    check config.electionTimeout == 500
    check config.heartbeatInterval == 100
    check config.logStoragePath == "/var/raft"
    check config.snapshotEnabled == true
    check config.snapshotDistance == 1000
    check config.maxAppendSize == 50

  test "config default values":
    let config = RaftConfig()
    check config.serverId == 0
    check config.endpoint == ""
    check config.snapshotEnabled == false

  test "config with empty paths":
    let config = RaftConfig(serverId: 1, logStoragePath: "")
    check config.logStoragePath == ""

  test "config with large values":
    let config = RaftConfig(
      serverId: int32.high,
      electionTimeout: int.high,
      heartbeatInterval: int.high,
      snapshotDistance: int.high,
      maxAppendSize: int.high
    )
    check config.serverId == int32.high

suite "RaftRPC Serialization":
  test "rpc all fields":
    let entries = @[LogEntry(term: 1, entryType: LET_NORMAL, data: "test")]
    let rpc = RaftRPC(
      rpcType: RPC_APPEND_ENTRIES,
      term: 5,
      leaderId: 1,
      prevLogIndex: 10,
      prevLogTerm: 4,
      entries: entries,
      leaderCommit: 8,
      success: true,
      data: "response"
    )
    check rpc.rpcType == RPC_APPEND_ENTRIES
    check rpc.term == 5
    check rpc.leaderId == 1
    check rpc.prevLogIndex == 10
    check rpc.prevLogTerm == 4
    check rpc.entries.len == 1
    check rpc.leaderCommit == 8
    check rpc.success == true
    check rpc.data == "response"

  test "rpc default values":
    let rpc = RaftRPC()
    check rpc.rpcType == RPC_APPEND_ENTRIES
    check rpc.term == 0
    check rpc.leaderId == 0
    check rpc.prevLogIndex == 0
    check rpc.prevLogTerm == 0
    check rpc.entries.len == 0
    check rpc.leaderCommit == 0
    check rpc.success == false
    check rpc.data == ""

  test "rpc with multiple entries":
    let entries = @[
      LogEntry(term: 1, entryType: LET_NORMAL, data: "e1"),
      LogEntry(term: 1, entryType: LET_NORMAL, data: "e2"),
      LogEntry(term: 1, entryType: LET_CONFIG_CHANGE, data: "config")
    ]
    let rpc = RaftRPC(
      rpcType: RPC_APPEND_ENTRIES,
      entries: entries
    )
    check rpc.entries.len == 3
    check rpc.entries[0].data == "e1"
    check rpc.entries[2].entryType == LET_CONFIG_CHANGE

  test "rpc request vote":
    let rpc = RaftRPC(
      rpcType: RPC_REQUEST_VOTE,
      term: 5,
      leaderId: 3,
      prevLogIndex: 10,
      prevLogTerm: 4
    )
    check rpc.rpcType == RPC_REQUEST_VOTE

  test "rpc client request":
    let rpc = RaftRPC(
      rpcType: RPC_CLIENT_REQUEST,
      term: 5,
      data: "command data"
    )
    check rpc.rpcType == RPC_CLIENT_REQUEST
    check rpc.data == "command data"

suite "Cluster Binary Serialization Extended":
  test "encode empty cluster":
    let config = RaftConfig(serverId: 1, endpoint: "")
    let cluster = newRaftCluster(config)
    let encoded = cluster.encodeCluster()
    check encoded.len > 0
    check encoded[0] == 'R'
    check encoded[1] == 'C'
    check encoded[2] == 'L'

  test "decode empty cluster":
    let config = RaftConfig(serverId: 1, endpoint: "")
    let cluster = newRaftCluster(config)
    let encoded = cluster.encodeCluster()
    let decoded = decodeCluster(encoded)
    check decoded.getServerCount() == 0
    check decoded.selfId == 1

  test "roundtrip with many servers":
    let config = RaftConfig(serverId: 1, endpoint: "localhost:9000")
    let cluster = newRaftCluster(config)
    for i in 1..50:
      discard cluster.addServer(int32(i), "node" & $i & ":9000")
    let encoded = cluster.encodeCluster()
    let decoded = decodeCluster(encoded)
    check decoded.getServerCount() == 50

  test "roundtrip with all config fields":
    let config = RaftConfig(
      serverId: 1,
      endpoint: "192.168.1.100:8080",
      electionTimeout: 5000,
      heartbeatInterval: 500,
      logStoragePath: "/data/raft/logs",
      snapshotEnabled: true,
      snapshotDistance: 10000,
      maxAppendSize: 1000
    )
    let cluster = newRaftCluster(config)
    discard cluster.addServer(1, "192.168.1.100:8080")
    let encoded = cluster.encodeCluster()
    let decoded = decodeCluster(encoded)
    check decoded.config.electionTimeout == 5000
    check decoded.config.snapshotEnabled == true
    check decoded.config.logStoragePath == "/data/raft/logs"

  test "decode rejects invalid magic":
    let invalid = "INVALID_MAGIC_HEADER_DATA"
    expect ValueError:
      discard decodeCluster(invalid)

  test "decode rejects truncated header":
    let truncated = "RCL"
    expect ValueError:
      discard decodeCluster(truncated)

  test "decode rejects wrong version":
    var w = initBinaryWriter()
    w.writeBytes(CLUSTER_MAGIC)
    w.writeU8(99'u8)
    let encoded = w.finish()
    expect ValueError:
      discard decodeCluster(encoded)

  test "encode with long endpoints":
    let config = RaftConfig(
      serverId: 1,
      endpoint: "very-long-endpoint-name-123456789.example.com:9000"
    )
    let cluster = newRaftCluster(config)
    let encoded = cluster.encodeCluster()
    let decoded = decodeCluster(encoded)
    check decoded.config.endpoint == config.endpoint

  test "encode with long logStoragePath":
    let config = RaftConfig(
      serverId: 1,
      endpoint: "localhost:9000",
      logStoragePath: "/very/long/path/to/raft/logs/that/goes/very/deep"
    )
    let cluster = newRaftCluster(config)
    let encoded = cluster.encodeCluster()
    let decoded = decodeCluster(encoded)
    check decoded.config.logStoragePath == config.logStoragePath

suite "Cluster Operations Extended":
  test "addServer duplicate":
    let config = RaftConfig(serverId: 1, endpoint: "l:9000")
    let cluster = newRaftCluster(config)
    check cluster.addServer(2, "l:9001")
    check not cluster.addServer(2, "l:9001")
    check cluster.getServerCount() == 1

  test "removeServer existing":
    let config = RaftConfig(serverId: 1, endpoint: "l:9000")
    let cluster = newRaftCluster(config)
    discard cluster.addServer(2, "l:9001")
    check cluster.removeServer(2)
    check cluster.getServerCount() == 0

  test "removeServer non-existing":
    let config = RaftConfig(serverId: 1, endpoint: "l:9000")
    let cluster = newRaftCluster(config)
    check not cluster.removeServer(99)

  test "getServerEndpoint existing":
    let config = RaftConfig(serverId: 1, endpoint: "l:9000")
    let cluster = newRaftCluster(config)
    discard cluster.addServer(2, "l:9001")
    let ep = cluster.getServerEndpoint(2)
    check ep.isSome
    check ep.get == "l:9001"

  test "getServerEndpoint non-existing":
    let config = RaftConfig(serverId: 1, endpoint: "l:9000")
    let cluster = newRaftCluster(config)
    let ep = cluster.getServerEndpoint(99)
    check ep.isNone

  test "getServers returns all":
    let config = RaftConfig(serverId: 1, endpoint: "l:9000")
    let cluster = newRaftCluster(config)
    discard cluster.addServer(2, "l:9001")
    discard cluster.addServer(3, "l:9002")
    discard cluster.addServer(4, "l:9003")
    let servers = cluster.getServers()
    check servers.len == 3

  test "getMajority calculations":
    let config = RaftConfig()
    for count in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:
      let cluster = newRaftCluster(config)
      for i in 1..count:
        discard cluster.addServer(int32(i), "l:" & $i)
      let expected = (count div 2) + 1
      check cluster.getMajority() == expected

  test "getQuorum equals majority":
    let config = RaftConfig()
    let cluster = newRaftCluster(config)
    for i in 1..5:
      discard cluster.addServer(int32(i), "l:" & $i)
    check cluster.getQuorum() == cluster.getMajority()

  test "isSelfLeader correct":
    let config = RaftConfig(serverId: 5)
    let cluster = newRaftCluster(config)
    check cluster.isSelfLeader(5)
    check not cluster.isSelfLeader(1)

  test "getSelfEndpoint returns self":
    let config = RaftConfig(serverId: 5, endpoint: "self:9000")
    let cluster = newRaftCluster(config)
    discard cluster.addServer(5, "self:9000")
    let ep = cluster.getSelfEndpoint()
    check ep.isSome
    check ep.get == "self:9000"

  test "isValidCluster requires selfId":
    let config = RaftConfig(serverId: 0, endpoint: "l:9000")
    let cluster = newRaftCluster(config)
    discard cluster.addServer(1, "l:9000")
    check not cluster.isValidCluster()

  test "isValidCluster requires servers":
    let config = RaftConfig(serverId: 1, endpoint: "l:9000")
    let cluster = newRaftCluster(config)
    check not cluster.isValidCluster()

  test "isValidCluster requires self endpoint":
    let config = RaftConfig(serverId: 1, endpoint: "l:9000")
    let cluster = newRaftCluster(config)
    discard cluster.addServer(2, "l:9001")
    check not cluster.isValidCluster()

  test "getClusterInfo output":
    let config = RaftConfig(serverId: 1, endpoint: "l:9000")
    let cluster = newRaftCluster(config)
    discard cluster.addServer(1, "l:9000")
    discard cluster.addServer(2, "l:9001")
    let info = cluster.getClusterInfo()
    check info.contains("Raft Cluster")
    check info.contains("Self ID: 1")
    check info.contains("Server Count: 2")

suite "Binary Reader/Writer Edge Cases":
  test "BinaryWriter empty finish":
    var w = initBinaryWriter()
    let result = w.finish()
    check result == ""

  test "BinaryWriter capacity expansion":
    var w = initBinaryWriter(4)
    let largeData = "x".repeat(100)
    w.writeString(largeData)
    let result = w.finish()
    check result.len == 104

  test "BinaryReader read all types":
    var w = initBinaryWriter()
    w.writeU8(255'u8)
    w.writeU16(65535'u16)
    w.writeU32(0xFFFFFFFF'u32)
    w.writeU64(0xFFFFFFFFFFFFFFFF'u64)
    w.writeI32(-1'i32)
    w.writeI64(-1'i64)
    w.writeString("test")
    let data = w.finish()

    var r = initBinaryReader(data)
    check r.readU8() == 255'u8
    check r.readU16() == 65535'u16
    check r.readU32() == 0xFFFFFFFF'u32
    check r.readU64() == 0xFFFFFFFFFFFFFFFF'u64
    check r.readI32() == -1'i32
    check r.readI64() == -1'i64
    check r.readString() == "test"

  test "BinaryReader remaining":
    var w = initBinaryWriter()
    w.writeU32(123'u32)
    w.writeString("data")
    let data = w.finish()

    var r = initBinaryReader(data)
    check r.remaining == data.len
    discard r.readU32()
    check r.remaining == data.len - 4
    discard r.readString()
    check r.remaining == 0

  test "BinaryReader end of data raises":
    var w = initBinaryWriter()
    w.writeU8(1'u8)
    let data = w.finish()

    var r = initBinaryReader(data)
    discard r.readU8()
    expect ValueError:
      discard r.readU8()

suite "Serialization Integration":
  test "multiple entries batch serialize":
    var entries: seq[LogEntry] = @[]
    for i in 1..100:
      entries.add(LogEntry(term: i.int64, entryType: LET_NORMAL,
          data: "entry_" & $i))
    for entry in entries:
      let encoded = encodeLogEntry(entry)
      let decoded = decodeLogEntry(encoded)
      check decoded.term == entry.term
      check decoded.data == entry.data

  test "nested serialization":
    let innerEntry = LogEntry(term: 1, entryType: LET_NORMAL, data: "inner")
    let outerEntry = LogEntry(
      term: 2,
      entryType: LET_CONFIG_CHANGE,
      data: encodeLogEntry(innerEntry)
    )
    let outerEncoded = encodeLogEntry(outerEntry)
    let outerDecoded = decodeLogEntry(outerEncoded)
    let innerDecoded = decodeLogEntry(outerDecoded.data)
    check innerDecoded.term == 1
    check innerDecoded.data == "inner"

  test "cluster with entries in RPC":
    let config = RaftConfig(serverId: 1, endpoint: "l:9000")
    let cluster = newRaftCluster(config)
    discard cluster.addServer(1, "l:9000")
    discard cluster.addServer(2, "l:9001")

    let encoded = cluster.encodeCluster()
    let entries = @[LogEntry(term: 1, entryType: LET_CONFIG_CHANGE,
        data: encoded)]
    let rpc = RaftRPC(rpcType: RPC_APPEND_ENTRIES, entries: entries)
    check rpc.entries[0].data.len == encoded.len
