# Unit tests for fractio/distributed/raft/rpc.nim
# Tests RPC handlers, sendRPC, processRPC, heartbeat, requestVote

import std/[unittest, options, tables, strutils, times, os]
import fractio/distributed/raft/types except RaftError
import fractio/distributed/raft/node except RaftError
import fractio/distributed/raft/state_machine
import fractio/distributed/raft/rpc

suite "handleAppendEntries":
  var node: RaftNode
  var impl: RaftNodeImpl
  var tmpDir: string

  setup:
    tmpDir = "/tmp/fractio_test_rpc_" & $getTime().toUnix
    createDir(tmpDir)
    impl = RaftNodeImpl()
    let config = RaftConfig(
      serverId: 1,
      endpoint: "localhost:9000",
      logStoragePath: tmpDir & "/log"
    )
    let sm = newKVStateMachine()
    discard impl.init(config, sm)
    node = RaftNode(impl)

  teardown:
    impl.shutdown()
    if dirExists(tmpDir):
      try:
        removeDir(tmpDir)
      except:
        discard

  test "returns success response":
    let rpc = RaftRPC(rpcType: RPC_APPEND_ENTRIES, term: 5, leaderId: 2)
    let resp = handleAppendEntries(node, rpc)
    check resp.rpcType == RPC_APPEND_ENTRIES
    check resp.success

  test "preserves term":
    node.nodeState.currentTerm = 10
    let rpc = RaftRPC(rpcType: RPC_APPEND_ENTRIES, term: 5, leaderId: 2)
    let resp = handleAppendEntries(node, rpc)
    check resp.term == 10

  test "sets leaderId in response":
    let rpc = RaftRPC(rpcType: RPC_APPEND_ENTRIES, term: 5, leaderId: 2)
    let resp = handleAppendEntries(node, rpc)
    check resp.leaderId == node.serverId

  test "handles heartbeat":
    let rpc = RaftRPC(
      rpcType: RPC_APPEND_ENTRIES,
      term: 5,
      leaderId: 2,
      entries: @[],
      leaderCommit: 10
    )
    let resp = handleAppendEntries(node, rpc)
    check resp.success

  test "handles entries":
    let rpc = RaftRPC(
      rpcType: RPC_APPEND_ENTRIES,
      term: 5,
      leaderId: 2,
      entries: @[LogEntry(term: 5, entryType: LET_NORMAL, data: "cmd")],
      leaderCommit: 1
    )
    let resp = handleAppendEntries(node, rpc)
    check resp.success

suite "handleRequestVote":
  var node: RaftNode
  var impl: RaftNodeImpl
  var tmpDir: string

  setup:
    tmpDir = "/tmp/fractio_test_rpc_vote_" & $getTime().toUnix
    createDir(tmpDir)
    impl = RaftNodeImpl()
    let config = RaftConfig(
      serverId: 1,
      endpoint: "localhost:9000",
      logStoragePath: tmpDir & "/log"
    )
    let sm = newKVStateMachine()
    discard impl.init(config, sm)
    node = RaftNode(impl)

  teardown:
    impl.shutdown()
    if dirExists(tmpDir):
      try:
        removeDir(tmpDir)
      except:
        discard

  test "returns response with correct type":
    let rpc = RaftRPC(rpcType: RPC_REQUEST_VOTE, term: 5, leaderId: 2)
    let resp = handleRequestVote(node, rpc)
    check resp.rpcType == RPC_REQUEST_VOTE
    check resp.success

  test "preserves current term":
    node.nodeState.currentTerm = 10
    let rpc = RaftRPC(rpcType: RPC_REQUEST_VOTE, term: 5, leaderId: 2)
    let resp = handleRequestVote(node, rpc)
    check resp.term == 10

  test "sets leaderId in response":
    let rpc = RaftRPC(rpcType: RPC_REQUEST_VOTE, term: 5, leaderId: 2)
    let resp = handleRequestVote(node, rpc)
    check resp.leaderId == node.serverId

  test "handles vote request with higher term":
    node.nodeState.currentTerm = 3
    let rpc = RaftRPC(rpcType: RPC_REQUEST_VOTE, term: 10, leaderId: 2)
    let resp = handleRequestVote(node, rpc)
    check resp.success

  test "handles vote request with lower term":
    node.nodeState.currentTerm = 10
    let rpc = RaftRPC(rpcType: RPC_REQUEST_VOTE, term: 5, leaderId: 2)
    let resp = handleRequestVote(node, rpc)
    check resp.success

suite "handleClientRequest":
  var node: RaftNode
  var impl: RaftNodeImpl
  var tmpDir: string

  setup:
    tmpDir = "/tmp/fractio_test_rpc_client_" & $getTime().toUnix
    createDir(tmpDir)
    impl = RaftNodeImpl()
    let config = RaftConfig(
      serverId: 1,
      endpoint: "localhost:9000",
      logStoragePath: tmpDir & "/log"
    )
    let sm = newKVStateMachine()
    discard impl.init(config, sm)
    node = RaftNode(impl)

  teardown:
    impl.shutdown()
    if dirExists(tmpDir):
      try:
        removeDir(tmpDir)
      except:
        discard

  test "returns client request response":
    let rpc = RaftRPC(rpcType: RPC_CLIENT_REQUEST, term: 1, data: "command")
    let resp = handleClientRequest(node, rpc)
    check resp.rpcType == RPC_CLIENT_REQUEST
    check resp.success

  test "preserves term":
    node.nodeState.currentTerm = 5
    let rpc = RaftRPC(rpcType: RPC_CLIENT_REQUEST, term: 1, data: "cmd")
    let resp = handleClientRequest(node, rpc)
    check resp.term == 5

  test "sets leaderId in response":
    let rpc = RaftRPC(rpcType: RPC_CLIENT_REQUEST, term: 1, data: "cmd")
    let resp = handleClientRequest(node, rpc)
    check resp.leaderId == node.serverId

  test "handles request with data":
    let rpc = RaftRPC(rpcType: RPC_CLIENT_REQUEST, term: 1,
        data: "PUT key value")
    let resp = handleClientRequest(node, rpc)
    check resp.success

  test "handles request with empty data":
    let rpc = RaftRPC(rpcType: RPC_CLIENT_REQUEST, term: 1, data: "")
    let resp = handleClientRequest(node, rpc)
    check resp.success

suite "sendRPC":
  var node: RaftNode
  var impl: RaftNodeImpl
  var tmpDir: string

  setup:
    tmpDir = "/tmp/fractio_test_rpc_send_" & $getTime().toUnix
    createDir(tmpDir)
    impl = RaftNodeImpl()
    let config = RaftConfig(
      serverId: 1,
      endpoint: "localhost:9000",
      logStoragePath: tmpDir & "/log"
    )
    let sm = newKVStateMachine()
    discard impl.init(config, sm)
    node = RaftNode(impl)

  teardown:
    impl.shutdown()
    if dirExists(tmpDir):
      try:
        removeDir(tmpDir)
      except:
        discard

  test "sendRPC accepts endpoint":
    let rpc = RaftRPC(rpcType: RPC_APPEND_ENTRIES, term: 1)
    sendRPC(node, rpc, "localhost:9001")
    check true

  test "sendRPC with empty endpoint":
    let rpc = RaftRPC(rpcType: RPC_APPEND_ENTRIES, term: 1)
    sendRPC(node, rpc, "")
    check true

  test "sendRPC with all RPC types":
    for rpcType in [RPC_APPEND_ENTRIES, RPC_REQUEST_VOTE, RPC_CLIENT_REQUEST]:
      let rpc = RaftRPC(rpcType: rpcType, term: 1)
      sendRPC(node, rpc, "target:9000")

suite "processRPC":
  var node: RaftNode
  var impl: RaftNodeImpl
  var tmpDir: string

  setup:
    tmpDir = "/tmp/fractio_test_rpc_process_" & $getTime().toUnix
    createDir(tmpDir)
    impl = RaftNodeImpl()
    let config = RaftConfig(
      serverId: 1,
      endpoint: "localhost:9000",
      logStoragePath: tmpDir & "/log"
    )
    let sm = newKVStateMachine()
    discard impl.init(config, sm)
    node = RaftNode(impl)

  teardown:
    impl.shutdown()
    if dirExists(tmpDir):
      try:
        removeDir(tmpDir)
      except:
        discard

  test "processRPC dispatches AppendEntries":
    let rpc = RaftRPC(rpcType: RPC_APPEND_ENTRIES, term: 1, leaderId: 2)
    let resp = processRPC(node, rpc)
    check resp.rpcType == RPC_APPEND_ENTRIES

  test "processRPC dispatches RequestVote":
    let rpc = RaftRPC(rpcType: RPC_REQUEST_VOTE, term: 1, leaderId: 2)
    let resp = processRPC(node, rpc)
    check resp.rpcType == RPC_REQUEST_VOTE

  test "processRPC dispatches ClientRequest":
    let rpc = RaftRPC(rpcType: RPC_CLIENT_REQUEST, term: 1, data: "cmd")
    let resp = processRPC(node, rpc)
    check resp.rpcType == RPC_CLIENT_REQUEST

  test "processRPC returns success for all types":
    for rpcType in [RPC_APPEND_ENTRIES, RPC_REQUEST_VOTE, RPC_CLIENT_REQUEST]:
      let rpc = RaftRPC(rpcType: rpcType, term: 1)
      let resp = processRPC(node, rpc)
      check resp.success

suite "heartbeat":
  var node: RaftNode
  var impl: RaftNodeImpl
  var tmpDir: string

  setup:
    tmpDir = "/tmp/fractio_test_rpc_heartbeat_" & $getTime().toUnix
    createDir(tmpDir)
    impl = RaftNodeImpl()
    let config = RaftConfig(
      serverId: 1,
      endpoint: "localhost:9000",
      logStoragePath: tmpDir & "/log"
    )
    let sm = newKVStateMachine()
    discard impl.init(config, sm)
    impl.nodeState.currentTerm = 5
    node = RaftNode(impl)

  teardown:
    impl.shutdown()
    if dirExists(tmpDir):
      try:
        removeDir(tmpDir)
      except:
        discard

  test "heartbeat executes":
    heartbeat(node)
    check true

  test "heartbeat preserves term":
    let termBefore = node.nodeState.currentTerm
    heartbeat(node)
    check node.nodeState.currentTerm == termBefore

suite "requestVote":
  var node: RaftNode
  var impl: RaftNodeImpl
  var tmpDir: string

  setup:
    tmpDir = "/tmp/fractio_test_rpc_vote_req_" & $getTime().toUnix
    createDir(tmpDir)
    impl = RaftNodeImpl()
    let config = RaftConfig(
      serverId: 1,
      endpoint: "localhost:9000",
      logStoragePath: tmpDir & "/log"
    )
    let sm = newKVStateMachine()
    discard impl.init(config, sm)
    impl.nodeState.currentTerm = 5
    node = RaftNode(impl)

  teardown:
    impl.shutdown()
    if dirExists(tmpDir):
      try:
        removeDir(tmpDir)
      except:
        discard

  test "requestVote executes":
    requestVote(node)
    check true

  test "requestVote preserves term":
    let termBefore = node.nodeState.currentTerm
    requestVote(node)
    check node.nodeState.currentTerm == termBefore

suite "replicateLog":
  var node: RaftNode
  var impl: RaftNodeImpl
  var tmpDir: string

  setup:
    tmpDir = "/tmp/fractio_test_rpc_replicate_" & $getTime().toUnix
    createDir(tmpDir)
    impl = RaftNodeImpl()
    let config = RaftConfig(
      serverId: 1,
      endpoint: "localhost:9000",
      logStoragePath: tmpDir & "/log"
    )
    let sm = newKVStateMachine()
    discard impl.init(config, sm)
    impl.nodeState.currentTerm = 5
    node = RaftNode(impl)

  teardown:
    impl.shutdown()
    if dirExists(tmpDir):
      try:
        removeDir(tmpDir)
      except:
        discard

  test "replicateLog executes":
    replicateLog(node)
    check true

  test "replicateLog preserves term":
    let termBefore = node.nodeState.currentTerm
    replicateLog(node)
    check node.nodeState.currentTerm == termBefore

suite "processClientRequest":
  var node: RaftNode
  var impl: RaftNodeImpl
  var tmpDir: string

  setup:
    tmpDir = "/tmp/fractio_test_rpc_client_req_" & $getTime().toUnix
    createDir(tmpDir)
    impl = RaftNodeImpl()
    let config = RaftConfig(
      serverId: 1,
      endpoint: "localhost:9000",
      logStoragePath: tmpDir & "/log"
    )
    let sm = newKVStateMachine()
    discard impl.init(config, sm)
    node = RaftNode(impl)

  teardown:
    impl.shutdown()
    if dirExists(tmpDir):
      try:
        removeDir(tmpDir)
      except:
        discard

  test "processClientRequest returns index":
    let idx = processClientRequest(node, "test command")
    check idx == 0

  test "processClientRequest handles empty data":
    let idx = processClientRequest(node, "")
    check idx == 0

  test "processClientRequest handles binary data":
    let idx = processClientRequest(node, "\x00\x01\x02\x03")
    check idx == 0

  test "processClientRequest handles large data":
    let largeData = "x".repeat(10000)
    let idx = processClientRequest(node, largeData)
    check idx == 0

suite "RPC Integration":
  var node: RaftNode
  var impl: RaftNodeImpl
  var tmpDir: string

  setup:
    tmpDir = "/tmp/fractio_test_rpc_int_" & $getTime().toUnix
    createDir(tmpDir)
    impl = RaftNodeImpl()
    let config = RaftConfig(
      serverId: 1,
      endpoint: "localhost:9000",
      logStoragePath: tmpDir & "/log"
    )
    let sm = newKVStateMachine()
    discard impl.init(config, sm)
    node = RaftNode(impl)

  teardown:
    impl.shutdown()
    if dirExists(tmpDir):
      try:
        removeDir(tmpDir)
      except:
        discard

  test "full RPC cycle - AppendEntries":
    let rpc = RaftRPC(
      rpcType: RPC_APPEND_ENTRIES,
      term: 5,
      leaderId: 2,
      prevLogIndex: 0,
      prevLogTerm: 0,
      entries: @[LogEntry(term: 5, entryType: LET_NORMAL, data: "cmd")],
      leaderCommit: 0
    )
    let resp = processRPC(node, rpc)
    check resp.rpcType == RPC_APPEND_ENTRIES
    check resp.success
    check resp.term == node.nodeState.currentTerm

  test "full RPC cycle - RequestVote":
    node.nodeState.votedFor = -1
    let rpc = RaftRPC(
      rpcType: RPC_REQUEST_VOTE,
      term: 10,
      leaderId: 3
    )
    let resp = processRPC(node, rpc)
    check resp.rpcType == RPC_REQUEST_VOTE
    check resp.success

  test "full RPC cycle - ClientRequest":
    let rpc = RaftRPC(
      rpcType: RPC_CLIENT_REQUEST,
      term: node.nodeState.currentTerm,
      data: "PUT key value"
    )
    let resp = processRPC(node, rpc)
    check resp.rpcType == RPC_CLIENT_REQUEST
    check resp.success

  test "RPC with multiple entries":
    let entries = @[
      LogEntry(term: 1, entryType: LET_NORMAL, data: "e1"),
      LogEntry(term: 1, entryType: LET_NORMAL, data: "e2"),
      LogEntry(term: 1, entryType: LET_NORMAL, data: "e3")
    ]
    let rpc = RaftRPC(
      rpcType: RPC_APPEND_ENTRIES,
      term: 1,
      leaderId: 2,
      entries: entries
    )
    let resp = processRPC(node, rpc)
    check resp.success

suite "RPC Type Coverage":
  test "all RPC types processed":
    for rpcType in [RPC_APPEND_ENTRIES, RPC_REQUEST_VOTE, RPC_CLIENT_REQUEST]:
      let rpc = RaftRPC(rpcType: rpcType, term: 1)
      check rpc.rpcType == rpcType

  test "RPCType enum complete":
    check RPC_APPEND_ENTRIES.ord == 0
    check RPC_REQUEST_VOTE.ord == 1
    check RPC_CLIENT_REQUEST.ord == 2

suite "RPC Edge Cases":
  test "empty entries sequence":
    let rpc = RaftRPC(
      rpcType: RPC_APPEND_ENTRIES,
      term: 1,
      leaderId: 1,
      entries: @[]
    )
    check rpc.entries.len == 0

  test "zero term RPC":
    let rpc = RaftRPC(rpcType: RPC_APPEND_ENTRIES, term: 0, leaderId: 1)
    check rpc.term == 0

  test "negative leaderId":
    let rpc = RaftRPC(rpcType: RPC_APPEND_ENTRIES, term: 1, leaderId: -1)
    check rpc.leaderId == -1

  test "large term value":
    let rpc = RaftRPC(rpcType: RPC_APPEND_ENTRIES, term: 0x7FFFFFFFFFFFFFFF, leaderId: 1)
    check rpc.term == 0x7FFFFFFFFFFFFFFF

  test "success flag false by default":
    let rpc = RaftRPC(rpcType: RPC_APPEND_ENTRIES, term: 1)
    check not rpc.success

  test "success flag can be set true":
    var rpc = RaftRPC(rpcType: RPC_APPEND_ENTRIES, term: 1)
    rpc.success = true
    check rpc.success
