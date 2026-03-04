# Unit tests for NuRaft bindings

import std/[unittest, options]
import fractio/distributed/raft

suite "NuRaft Buffer Tests":
  test "Create buffer":
    let buf = newBuffer(1024)
    check buf != nil
    check buf.position == 0
    check buf.data.len == 1024

  test "Buffer put":
    let buf = newBuffer(1024)
    buf.put("hello world")
    check buf.position == 11

  test "Buffer put multiple":
    let buf = newBuffer(1024)
    buf.put("hello")
    buf.put(" ")
    buf.put("world")
    check buf.position == 11
    check buf.data[0..<11] == "hello world"

  test "Buffer size":
    let buf = newBuffer(1024)
    buf.put("test")
    check buf.size() == 4

  test "Buffer reset":
    let buf = newBuffer(1024)
    buf.put("test")
    buf.reset()
    check buf.position == 0

  test "Buffer putInt32":
    let buf = newBuffer(1024)
    buf.putInt32(42'i32)
    check buf.position == 4

  test "Buffer putInt64":
    let buf = newBuffer(1024)
    buf.putInt64(123456789'i64)
    check buf.position == 8

  test "Buffer get":
    let buf = newBuffer(1024)
    buf.put("hello world")
    buf.reset()
    let result = buf.get(5)
    check result == "hello"
    check buf.position == 5

suite "NuRaft RaftParams Tests":
  test "Create default params":
    let params = newRaftParams()
    check params != nil
    check params.electionTimeout == 5000
    check params.heartbeatInterval == 1000
    check params.heartbeatTimeout == 3000
    check params.logMaxSize == 1024 * 1024
    check params.snapshotEnabled == false
    check params.rpcFailureMax == 3

  test "Set election timeout":
    let params = newRaftParams()
    params.setElectionTimeout(10000)
    check params.electionTimeout == 10000

  test "Set heartbeat interval":
    let params = newRaftParams()
    params.setHeartbeatInterval(500)
    check params.heartbeatInterval == 500

  test "Set heartbeat timeout":
    let params = newRaftParams()
    params.setHeartbeatTimeout(1500)
    check params.heartbeatTimeout == 1500

  test "Set log max size":
    let params = newRaftParams()
    params.setLogMaxSize(512 * 1024)
    check params.logMaxSize == 512 * 1024

  test "Set snapshot enabled":
    let params = newRaftParams()
    params.setSnapshotEnabled(true)
    check params.snapshotEnabled == true

  test "Set RPC failure max":
    let params = newRaftParams()
    params.setRpcFailureMax(5)
    check params.rpcFailureMax == 5

  test "defaultRaftParams":
    let params = defaultRaftParams()
    check params != nil
    check params.electionTimeout == 5000

suite "NuRaft ServerConfig Tests":
  test "Create server config":
    let config = newServerConfig(1, "localhost:8001")
    check config != nil
    check config.serverId == 1
    check config.endpoint == "localhost:8001"

suite "NuRaft RaftState Tests":
  test "Create default state":
    let state = newRaftState()
    check state != nil
    check state.term == 0
    check state.voteFor == -1
    check state.role == srFollower
    check state.leaderId == -1
    check state.lastLogIndex == 0
    check state.commitIndex == 0

  test "State role transitions":
    let state = newRaftState()
    state.role = srCandidate
    check state.role == srCandidate
    state.role = srLeader
    check state.role == srLeader

  test "State term update":
    let state = newRaftState()
    state.term = 5
    check state.term == 5

  test "State leader update":
    let state = newRaftState()
    state.leaderId = 1
    check state.leaderId == 1
    state.leaderId = -1 # No leader
    check state.leaderId == -1

suite "NuRaft RaftServer Tests":
  test "Create server":
    let server = newRaftServer(1, "localhost:8001")
    check server != nil
    check server.serverId == 1
    check server.endpoint == "localhost:8001"
    check server.state != nil
    check server.params != nil
    check server.initialized == false

  test "Create server with custom params":
    let params = newRaftParams()
    params.setElectionTimeout(10000)
    let server = newRaftServer(2, "localhost:8002", params)
    check server.params.electionTimeout == 10000

  test "Server init":
    let server = newRaftServer(1, "localhost:8001")
    let result = server.init()
    check result == true
    check server.initialized == true

  test "Server shutdown":
    let server = newRaftServer(1, "localhost:8001")
    discard server.init()
    let result = server.shutdown()
    check result == true
    check server.initialized == false

  test "Server isLeader":
    let server = newRaftServer(1, "localhost:8001")
    check server.isLeader() == false
    server.state.role = srLeader
    check server.isLeader() == true

  test "Server getLeader":
    let server = newRaftServer(1, "localhost:8001")
    check server.getLeader() == -1
    server.state.leaderId = 2
    check server.getLeader() == 2

  test "Server getTerm":
    let server = newRaftServer(1, "localhost:8001")
    check server.getTerm() == 0
    server.state.term = 10
    check server.getTerm() == 10

  test "Server getState":
    let server = newRaftServer(1, "localhost:8001")
    check server.getState() == srFollower
    server.state.role = srLeader
    check server.getState() == srLeader

  test "Server lastLogIndex":
    let server = newRaftServer(1, "localhost:8001")
    check server.lastLogIndex() == 0
    server.state.lastLogIndex = 100
    check server.lastLogIndex() == 100

  test "Server commitIndex":
    let server = newRaftServer(1, "localhost:8001")
    check server.commitIndex() == 0
    server.state.commitIndex = 50
    check server.commitIndex() == 50

suite "NuRaft Logger Tests":
  test "Create logger":
    let logger = newLogger(llInfo)
    check logger != nil
    check logger.level == llInfo

  test "Logger levels":
    let loggerDebug = newLogger(llDebug)
    check loggerDebug.level == llDebug

    let loggerWarn = newLogger(llWarn)
    check loggerWarn.level == llWarn

    let loggerError = newLogger(llError)
    check loggerError.level == llError

suite "NuRaft Integration Tests":
  test "Full server lifecycle":
    # Create params
    let params = newRaftParams()
    params.setElectionTimeout(3000)
    params.setHeartbeatInterval(500)

    # Create server
    let server = newRaftServer(1, "localhost:9001", params)

    # Verify initial state
    check server.getState() == srFollower
    check server.getTerm() == 0
    check server.isLeader() == false
    check server.getLeader() == -1

    # Initialize
    let initResult = server.init()
    check initResult == true
    check server.initialized == true

    # Simulate becoming leader
    server.state.role = srLeader
    server.state.term = 1
    server.state.leaderId = 1

    check server.isLeader() == true
    check server.getTerm() == 1
    check server.getLeader() == 1

    # Simulate log replication
    server.state.lastLogIndex = 1000
    server.state.commitIndex = 500

    check server.lastLogIndex() == 1000
    check server.commitIndex() == 500

    # Shutdown
    let shutdownResult = server.shutdown()
    check shutdownResult == true
    check server.initialized == false

  test "Multiple servers":
    let server1 = newRaftServer(1, "localhost:9001")
    let server2 = newRaftServer(2, "localhost:9002")
    let server3 = newRaftServer(3, "localhost:9003")

    # Initialize all
    discard server1.init()
    discard server2.init()
    discard server3.init()

    # Server 1 becomes leader
    server1.state.role = srLeader
    server1.state.term = 1
    server1.state.leaderId = 1

    # Server 2 and 3 follow
    server2.state.role = srFollower
    server2.state.term = 1
    server2.state.leaderId = 1

    server3.state.role = srFollower
    server3.state.term = 1
    server3.state.leaderId = 1

    check server1.isLeader() == true
    check server2.isLeader() == false
    check server3.isLeader() == false
    check server1.getLeader() == 1
    check server2.getLeader() == 1
    check server3.getLeader() == 1

    # Cleanup
    discard server1.shutdown()
    discard server2.shutdown()
    discard server3.shutdown()

  test "Election timeout progression":
    let params = newRaftParams()
    params.setElectionTimeout(5000)

    let server = newRaftServer(1, "localhost:9101", params)
    discard server.init()

    # Start of term 1
    server.state.term = 1

    # Simulate election
    server.state.role = srCandidate
    server.state.term = 2
    server.state.voteFor = 1

    check server.getState() == srCandidate
    check server.getTerm() == 2

    # Become leader
    server.state.role = srLeader

    check server.isLeader() == true
    check server.getState() == srLeader
