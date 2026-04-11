# Unit tests for fractio/distributed/raft/node.nim
# Tests RaftNodeImpl, WiscKeyLogStore, state transitions, RPC handling

import std/[unittest, options, os, strutils, tables, times, locks]
import fractio/distributed/raft/types except RaftError
import fractio/distributed/raft/node
import fractio/distributed/raft/state_machine
import fractio/di/mocks except LogEntry

suite "WiscKeyLogStore":
  var tmpDir: string

  setup:
    tmpDir = "/tmp/fractio_test_raft_log_" & $getTime().toUnix
    createDir(tmpDir)

  teardown:
    if dirExists(tmpDir):
      try:
        removeDir(tmpDir)
      except:
        discard

  test "newWiscKeyLogStore creates valid store":
    let store = newWiscKeyLogStore(tmpDir)
    check store != nil
    check store.backend != nil
    check store.startIndex == 1
    check store.nextIndex == 1
    store.close()

  test "loadWiscKeyLogStore recovers existing store":
    let store1 = newWiscKeyLogStore(tmpDir)
    let entry = LogEntry(term: 5, entryType: LET_NORMAL, data: "test data")
    discard store1.appendEntry(entry)
    store1.close()

    let store2 = loadWiscKeyLogStore(tmpDir)
    check store2.nextIndex == 2
    let recovered = store2.getEntry(1)
    check recovered.isSome
    check recovered.get.term == 5
    check recovered.get.data == "test data"
    store2.close()

  test "appendEntry returns sequential indices":
    let store = newWiscKeyLogStore(tmpDir)
    let idx1 = store.appendEntry(LogEntry(term: 1, entryType: LET_NORMAL, data: "a"))
    let idx2 = store.appendEntry(LogEntry(term: 1, entryType: LET_NORMAL, data: "b"))
    let idx3 = store.appendEntry(LogEntry(term: 1, entryType: LET_NORMAL, data: "c"))
    check idx1 == 1
    check idx2 == 2
    check idx3 == 3
    check store.nextIndex == 4
    store.close()

  test "appendEntry thread-safe index assignment":
    let store = newWiscKeyLogStore(tmpDir)
    var indices: seq[int64] = @[]
    for i in 1..100:
      let idx = store.appendEntry(LogEntry(term: 1, entryType: LET_NORMAL, data: $i))
      indices.add(idx)
    check indices.len == 100
    for i in 1..100:
      check indices[i-1] == i.int64
    store.close()

  test "getEntry returns none for missing index":
    let store = newWiscKeyLogStore(tmpDir)
    let missing = store.getEntry(999)
    check missing.isNone
    store.close()

  test "getEntry roundtrip for all entry types":
    let store = newWiscKeyLogStore(tmpDir)
    for et in [LET_NORMAL, LET_CONFIG_CHANGE, LET_NO_OP]:
      let entry = LogEntry(term: 42, entryType: et, data: "type_" & $et)
      let idx = store.appendEntry(entry)
      let recovered = store.getEntry(idx)
      check recovered.isSome
      check recovered.get.term == 42
      check recovered.get.entryType == et
      check recovered.get.data == "type_" & $et
    store.close()

  test "getEntries returns range of entries":
    let store = newWiscKeyLogStore(tmpDir)
    for i in 1..10:
      discard store.appendEntry(LogEntry(term: i.int64, entryType: LET_NORMAL, data: $i))
    let entries = store.getEntries(3, 7)
    check entries.len == 5
    check entries[0].term == 3
    check entries[4].term == 7
    store.close()

  test "getLastEntry returns last appended":
    let store = newWiscKeyLogStore(tmpDir)
    check store.getLastEntry().isNone
    discard store.appendEntry(LogEntry(term: 1, entryType: LET_NORMAL,
        data: "first"))
    discard store.appendEntry(LogEntry(term: 2, entryType: LET_NORMAL,
        data: "second"))
    let last = store.getLastEntry()
    check last.isSome
    check last.get.term == 2
    check last.get.data == "second"
    store.close()

  test "close releases lock file":
    let store = newWiscKeyLogStore(tmpDir)
    store.close()
    let lockFile = tmpDir & "/LOCK"
    check not fileExists(lockFile)

  test "appendEntry raises when backend nil":
    var store = WiscKeyLogStore()
    store.backend = nil
    var raised = false
    try:
      discard store.appendEntry(LogEntry(term: 1, entryType: LET_NORMAL, data: "x"))
    except RaftError:
      raised = true
    check raised

  test "getEntry raises when backend nil":
    var store = WiscKeyLogStore()
    store.backend = nil
    var raised = false
    try:
      discard store.getEntry(1)
    except RaftError:
      raised = true
    check raised

  test "getEntries raises when backend nil":
    var store = WiscKeyLogStore()
    store.backend = nil
    var raised = false
    try:
      discard store.getEntries(1, 10)
    except RaftError:
      raised = true
    check raised

  test "appendEntry with binary data":
    let store = newWiscKeyLogStore(tmpDir)
    let binaryData = "\x00\x01\x02\x03\xff\xfe\xfd"
    let idx = store.appendEntry(LogEntry(term: 1, entryType: LET_NORMAL,
        data: binaryData))
    let recovered = store.getEntry(idx)
    check recovered.isSome
    check recovered.get.data == binaryData
    store.close()

  test "appendEntry with large data":
    let store = newWiscKeyLogStore(tmpDir)
    let largeData = "x".repeat(10000)
    let idx = store.appendEntry(LogEntry(term: 1, entryType: LET_NORMAL,
        data: largeData))
    let recovered = store.getEntry(idx)
    check recovered.isSome
    check recovered.get.data.len == 10000
    store.close()

suite "RaftNodeImpl Initialization":
  var tmpDir: string

  setup:
    tmpDir = "/tmp/fractio_test_raft_node_" & $getTime().toUnix
    createDir(tmpDir)

  teardown:
    if dirExists(tmpDir):
      try:
        removeDir(tmpDir)
      except:
        discard

  test "init creates follower state":
    let node = RaftNodeImpl()
    let config = RaftConfig(
      serverId: 1,
      endpoint: "localhost:9000",
      logStoragePath: tmpDir & "/log",
      electionTimeout: 500,
      heartbeatInterval: 100
    )
    let sm = newKVStateMachine()
    check node.init(config, sm)
    check node.initialized
    check node.nodeState.role == SR_FOLLOWER
    check node.nodeState.currentTerm == 0
    check node.nodeState.votedFor == -1
    check node.nodeState.leaderId == -1
    check not node.isLeader
    node.shutdown()

  test "init idempotent":
    let node = RaftNodeImpl()
    let config = RaftConfig(
      serverId: 1,
      endpoint: "localhost:9000",
      logStoragePath: tmpDir & "/log",
      electionTimeout: 500,
      heartbeatInterval: 100
    )
    let sm = newKVStateMachine()
    check node.init(config, sm)
    check node.init(config, sm)
    check node.initialized
    node.shutdown()

  test "init creates parent directory":
    let node = RaftNodeImpl()
    let config = RaftConfig(
      serverId: 1,
      endpoint: "localhost:9000",
      logStoragePath: tmpDir & "/nested/deep/log",
      electionTimeout: 500,
      heartbeatInterval: 100
    )
    let sm = newKVStateMachine()
    check node.init(config, sm)
    check dirExists(tmpDir & "/nested/deep/log")
    node.shutdown()

  test "init sets serverId and endpoint":
    let node = RaftNodeImpl()
    let config = RaftConfig(
      serverId: 42,
      endpoint: "192.168.1.100:8080",
      logStoragePath: tmpDir & "/log",
      electionTimeout: 500,
      heartbeatInterval: 100
    )
    let sm = newKVStateMachine()
    check node.init(config, sm)
    check node.serverId == 42
    check node.endpoint == "192.168.1.100:8080"
    node.shutdown()

  test "shutdown clears initialized flag":
    let node = RaftNodeImpl()
    let config = RaftConfig(
      serverId: 1,
      endpoint: "localhost:9000",
      logStoragePath: tmpDir & "/log",
      electionTimeout: 500,
      heartbeatInterval: 100
    )
    let sm = newKVStateMachine()
    check node.init(config, sm)
    check node.initialized
    node.shutdown()
    check not node.initialized

suite "RaftNodeImpl State Transitions":
  var node: RaftNodeImpl
  var tmpDir: string

  setup:
    tmpDir = "/tmp/fractio_test_raft_state_" & $getTime().toUnix
    createDir(tmpDir)
    node = RaftNodeImpl()
    let config = RaftConfig(
      serverId: 1,
      endpoint: "localhost:9000",
      logStoragePath: tmpDir & "/log"
    )
    let sm = newKVStateMachine()
    discard node.init(config, sm)

  teardown:
    node.shutdown()
    if dirExists(tmpDir):
      try:
        removeDir(tmpDir)
      except:
        discard

  test "becomeCandidate increments term":
    node.becomeCandidate()
    check node.nodeState.role == SR_CANDIDATE
    check node.nodeState.currentTerm == 1
    check node.nodeState.votedFor == 1

  test "becomeCandidate sets votedFor to self":
    node.becomeCandidate()
    check node.nodeState.votedFor == node.serverId
    check node.lastVoteTerm == 1

  test "becomeLeader requires candidate role":
    node.nodeState.role = SR_FOLLOWER
    node.becomeLeader()
    check node.nodeState.role == SR_FOLLOWER
    check not node.isLeader

  test "becomeLeader transitions from candidate":
    node.becomeCandidate()
    node.becomeLeader()
    check node.nodeState.role == SR_LEADER
    check node.isLeader
    check node.nodeState.leaderId == node.serverId

  test "stepDown on higher term":
    node.nodeState.currentTerm = 5
    node.nodeState.role = SR_LEADER
    node.isLeader = true
    node.stepDown(10)
    check node.nodeState.role == SR_FOLLOWER
    check node.nodeState.currentTerm == 10
    check node.nodeState.votedFor == -1
    check not node.isLeader

  test "stepDown ignores lower term":
    node.nodeState.currentTerm = 10
    node.nodeState.role = SR_LEADER
    node.isLeader = true
    node.stepDown(5)
    check node.nodeState.role == SR_LEADER
    check node.isLeader

  test "startElection transitions to candidate":
    node.nodeState.role = SR_FOLLOWER
    node.startElection()
    check node.nodeState.role == SR_CANDIDATE

  test "startElection ignores leader":
    node.nodeState.role = SR_LEADER
    node.startElection()
    check node.nodeState.role == SR_LEADER

suite "RaftNodeImpl RPC Handling - Follower":
  var node: RaftNodeImpl
  var tmpDir: string

  setup:
    tmpDir = "/tmp/fractio_test_raft_rpc_f_" & $getTime().toUnix
    createDir(tmpDir)
    node = RaftNodeImpl()
    let config = RaftConfig(
      serverId: 1,
      endpoint: "localhost:9000",
      logStoragePath: tmpDir & "/log"
    )
    let sm = newKVStateMachine()
    discard node.init(config, sm)

  teardown:
    node.shutdown()
    if dirExists(tmpDir):
      try:
        removeDir(tmpDir)
      except:
        discard

  test "handleRPCAsFollower accepts AppendEntries with higher term":
    let rpc = RaftRPC(rpcType: RPC_APPEND_ENTRIES, term: 5, leaderId: 2)
    let resp = node.handleRPCAsFollower(rpc)
    check resp.success
    check node.nodeState.currentTerm == 5
    check node.nodeState.leaderId == 2

  test "handleRPCAsFollower accepts AppendEntries with equal term":
    node.nodeState.currentTerm = 5
    let rpc = RaftRPC(rpcType: RPC_APPEND_ENTRIES, term: 5, leaderId: 2)
    let resp = node.handleRPCAsFollower(rpc)
    check resp.success

  test "handleRPCAsFollower updates lastHeartbeat":
    let rpc = RaftRPC(rpcType: RPC_APPEND_ENTRIES, term: 1, leaderId: 2)
    discard node.handleRPCAsFollower(rpc)
    check node.lastHeartbeat > 0

  test "handleRPCAsFollower appends entries":
    let entries = @[
      LogEntry(term: 1, entryType: LET_NORMAL, data: "entry1"),
      LogEntry(term: 1, entryType: LET_NORMAL, data: "entry2")
    ]
    let rpc = RaftRPC(
      rpcType: RPC_APPEND_ENTRIES,
      term: 1,
      leaderId: 2,
      entries: entries,
      leaderCommit: 2
    )
    let resp = node.handleRPCAsFollower(rpc)
    check resp.success
    check node.wsLogStore.nextIndex == 3
    let e1 = node.wsLogStore.getEntry(1)
    check e1.isSome
    check e1.get.data == "entry1"

  test "handleRPCAsFollower updates commitIndex":
    let rpc = RaftRPC(
      rpcType: RPC_APPEND_ENTRIES,
      term: 1,
      leaderId: 2,
      entries: @[LogEntry(term: 1, entryType: LET_NORMAL, data: "x")],
      leaderCommit: 100
    )
    discard node.handleRPCAsFollower(rpc)
    check node.nodeState.commitIndex == 1

  test "handleRPCAsFollower grants vote for higher term":
    let rpc = RaftRPC(rpcType: RPC_REQUEST_VOTE, term: 10, leaderId: 3)
    let resp = node.handleRPCAsFollower(rpc)
    check resp.success
    check node.nodeState.currentTerm == 10
    check node.nodeState.votedFor == 3

  test "handleRPCAsFollower grants vote when not voted":
    node.nodeState.currentTerm = 5
    node.nodeState.votedFor = -1
    let rpc = RaftRPC(rpcType: RPC_REQUEST_VOTE, term: 5, leaderId: 3)
    let resp = node.handleRPCAsFollower(rpc)
    check resp.success

  test "handleRPCAsFollower denies vote when already voted":
    node.nodeState.currentTerm = 5
    node.nodeState.votedFor = 2
    let rpc = RaftRPC(rpcType: RPC_REQUEST_VOTE, term: 5, leaderId: 3)
    let resp = node.handleRPCAsFollower(rpc)
    check not resp.success

  test "handleRPCAsFollower forwards client request":
    node.nodeState.leaderId = 5
    let rpc = RaftRPC(rpcType: RPC_CLIENT_REQUEST, term: 1, data: "cmd")
    let resp = node.handleRPCAsFollower(rpc)
    check not resp.success
    check resp.leaderId == 5

suite "RaftNodeImpl RPC Handling - Leader":
  var node: RaftNodeImpl
  var tmpDir: string

  setup:
    tmpDir = "/tmp/fractio_test_raft_rpc_l_" & $getTime().toUnix
    createDir(tmpDir)
    node = RaftNodeImpl()
    let config = RaftConfig(
      serverId: 1,
      endpoint: "localhost:9000",
      logStoragePath: tmpDir & "/log"
    )
    let sm = newKVStateMachine()
    discard node.init(config, sm)
    node.becomeCandidate()
    node.becomeLeader()

  teardown:
    node.shutdown()
    if dirExists(tmpDir):
      try:
        removeDir(tmpDir)
      except:
        discard

  test "handleRPCAsLeader steps down on higher term":
    check node.isLeader
    let rpc = RaftRPC(rpcType: RPC_APPEND_ENTRIES, term: 100, leaderId: 2)
    let resp = node.handleRPCAsLeader(rpc)
    check node.nodeState.role == SR_FOLLOWER
    check not node.isLeader
    check node.nodeState.currentTerm == 100

  test "handleRPCAsLeader denies vote to other candidates":
    let rpc = RaftRPC(rpcType: RPC_REQUEST_VOTE,
        term: node.nodeState.currentTerm, leaderId: 2)
    let resp = node.handleRPCAsLeader(rpc)
    check not resp.success

  test "handleRPCAsLeader processes client request":
    let rpc = RaftRPC(rpcType: RPC_CLIENT_REQUEST, term: 1,
        data: "test command")
    let resp = node.handleRPCAsLeader(rpc)
    check resp.success
    check node.wsLogStore.nextIndex == 2

  test "handleRPCAsLeader returns index in response":
    let rpc = RaftRPC(rpcType: RPC_CLIENT_REQUEST, term: 1, data: "cmd")
    let resp = node.handleRPCAsLeader(rpc)
    check resp.data.contains("index")

  test "handleRPCAsLeader fails without state machine":
    node.stateMachine = nil
    let rpc = RaftRPC(rpcType: RPC_CLIENT_REQUEST, term: 1, data: "cmd")
    let resp = node.handleRPCAsLeader(rpc)
    check not resp.success
    check resp.data.contains("No state machine")

suite "RaftNodeImpl RPC Handling - Candidate":
  var node: RaftNodeImpl
  var tmpDir: string

  setup:
    tmpDir = "/tmp/fractio_test_raft_rpc_c_" & $getTime().toUnix
    createDir(tmpDir)
    node = RaftNodeImpl()
    let config = RaftConfig(
      serverId: 1,
      endpoint: "localhost:9000",
      logStoragePath: tmpDir & "/log"
    )
    let sm = newKVStateMachine()
    discard node.init(config, sm)
    node.becomeCandidate()

  teardown:
    node.shutdown()
    if dirExists(tmpDir):
      try:
        removeDir(tmpDir)
      except:
        discard

  test "handleRPCAsCandidate steps down on higher term AppendEntries":
    let rpc = RaftRPC(rpcType: RPC_APPEND_ENTRIES, term: 10, leaderId: 2)
    let resp = node.handleRPCAsCandidate(rpc)
    check node.nodeState.role == SR_FOLLOWER
    check node.nodeState.currentTerm == 10

  test "handleRPCAsCandidate steps down on equal term AppendEntries":
    node.nodeState.currentTerm = 5
    let rpc = RaftRPC(rpcType: RPC_APPEND_ENTRIES, term: 5, leaderId: 2)
    let resp = node.handleRPCAsCandidate(rpc)
    check node.nodeState.role == SR_FOLLOWER

  test "handleRPCAsCandidate denies vote to others":
    let rpc = RaftRPC(rpcType: RPC_REQUEST_VOTE,
        term: node.nodeState.currentTerm, leaderId: 2)
    let resp = node.handleRPCAsCandidate(rpc)
    check not resp.success

  test "handleRPCAsCandidate forwards client request if leader known":
    node.nodeState.leaderId = 5
    let rpc = RaftRPC(rpcType: RPC_CLIENT_REQUEST, term: 1, data: "cmd")
    let resp = node.handleRPCAsCandidate(rpc)
    check not resp.success
    check resp.leaderId == 5

suite "RaftNodeImpl appendEntries and commit":
  var node: RaftNodeImpl
  var tmpDir: string

  setup:
    tmpDir = "/tmp/fractio_test_raft_append_" & $getTime().toUnix
    createDir(tmpDir)
    node = RaftNodeImpl()
    let config = RaftConfig(
      serverId: 1,
      endpoint: "localhost:9000",
      logStoragePath: tmpDir & "/log"
    )
    let sm = newKVStateMachine()
    discard node.init(config, sm)
    node.becomeCandidate()
    node.becomeLeader()

  teardown:
    node.shutdown()
    if dirExists(tmpDir):
      try:
        removeDir(tmpDir)
      except:
        discard

  test "appendEntries returns last index":
    let entries = @[
      LogEntry(term: 1, entryType: LET_NORMAL, data: "a"),
      LogEntry(term: 1, entryType: LET_NORMAL, data: "b"),
      LogEntry(term: 1, entryType: LET_NORMAL, data: "c")
    ]
    let lastIdx = node.appendEntries(entries)
    check lastIdx == 3

  test "commit raises when not leader":
    node.nodeState.role = SR_FOLLOWER
    node.isLeader = false
    var raised = false
    try:
      discard node.commit("data")
    except RaftError:
      raised = true
    check raised

  test "commit appends entry and returns index":
    let idx = node.commit("test commit")
    check idx == 1
    check node.wsLogStore.nextIndex == 2

  test "commit creates normal entry type":
    let idx = node.commit("data")
    let entry = node.wsLogStore.getEntry(idx)
    check entry.isSome
    check entry.get.entryType == LET_NORMAL

suite "RaftRPC Type":
  test "RPC_APPEND_ENTRIES ordinal":
    check RPC_APPEND_ENTRIES.ord == 0

  test "RPC_REQUEST_VOTE ordinal":
    check RPC_REQUEST_VOTE.ord == 1

  test "RPC_CLIENT_REQUEST ordinal":
    check RPC_CLIENT_REQUEST.ord == 2

  test "RaftRPC default values":
    let rpc = RaftRPC()
    check rpc.rpcType == RPC_APPEND_ENTRIES
    check rpc.term == 0
    check rpc.leaderId == 0
    check rpc.entries.len == 0
    check not rpc.success

  test "RaftRPC with all fields":
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
      data: "response data"
    )
    check rpc.rpcType == RPC_APPEND_ENTRIES
    check rpc.term == 5
    check rpc.prevLogIndex == 10
    check rpc.prevLogTerm == 4
    check rpc.entries.len == 1
    check rpc.leaderCommit == 8
    check rpc.success
    check rpc.data == "response data"

suite "handleRPC dispatch":
  var node: RaftNodeImpl
  var tmpDir: string

  setup:
    tmpDir = "/tmp/fractio_test_raft_dispatch_" & $getTime().toUnix
    createDir(tmpDir)
    node = RaftNodeImpl()
    let config = RaftConfig(
      serverId: 1,
      endpoint: "localhost:9000",
      logStoragePath: tmpDir & "/log"
    )
    let sm = newKVStateMachine()
    discard node.init(config, sm)

  teardown:
    node.shutdown()
    if dirExists(tmpDir):
      try:
        removeDir(tmpDir)
      except:
        discard

  test "handleRPC dispatches to follower":
    node.nodeState.role = SR_FOLLOWER
    let rpc = RaftRPC(rpcType: RPC_APPEND_ENTRIES, term: 1, leaderId: 2)
    let resp = node.handleRPC(rpc)
    check resp.success

  test "handleRPC dispatches to candidate":
    node.nodeState.role = SR_CANDIDATE
    let rpc = RaftRPC(rpcType: RPC_APPEND_ENTRIES, term: 10, leaderId: 2)
    let resp = node.handleRPC(rpc)
    check node.nodeState.role == SR_FOLLOWER

  test "handleRPC dispatches to leader":
    node.nodeState.role = SR_LEADER
    node.isLeader = true
    let rpc = RaftRPC(rpcType: RPC_CLIENT_REQUEST, term: 1, data: "cmd")
    let resp = node.handleRPC(rpc)
    check resp.rpcType == RPC_CLIENT_REQUEST

suite "Edge Cases":
  var tmpDir: string

  setup:
    tmpDir = "/tmp/fractio_test_raft_edge_" & $getTime().toUnix
    createDir(tmpDir)

  teardown:
    if dirExists(tmpDir):
      try:
        removeDir(tmpDir)
      except:
        discard

  test "empty entries in AppendEntries":
    let store = newWiscKeyLogStore(tmpDir & "/log")
    let rpc = RaftRPC(rpcType: RPC_APPEND_ENTRIES, term: 1, leaderId: 2,
        entries: @[])
    check rpc.entries.len == 0
    store.close()

  test "multiple consecutive state transitions":
    let node = RaftNodeImpl()
    let config = RaftConfig(serverId: 1, endpoint: "l:9000",
        logStoragePath: tmpDir & "/log")
    let sm = newKVStateMachine()
    discard node.init(config, sm)
    node.becomeCandidate()
    check node.nodeState.role == SR_CANDIDATE
    node.becomeLeader()
    check node.nodeState.role == SR_LEADER
    node.stepDown(100)
    check node.nodeState.role == SR_FOLLOWER
    node.shutdown()

  test "election after shutdown and reinit":
    let node = RaftNodeImpl()
    let config = RaftConfig(serverId: 1, endpoint: "l:9000",
        logStoragePath: tmpDir & "/log2")
    let sm = newKVStateMachine()
    discard node.init(config, sm)
    node.shutdown()
    discard node.init(config, sm)
    node.startElection()
    check node.nodeState.role == SR_CANDIDATE
    node.shutdown()
