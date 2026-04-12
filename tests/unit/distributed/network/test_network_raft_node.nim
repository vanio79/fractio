# Unit tests for fractio/distributed/network/network_raft_node.nim
# Tests NetworkRaftNode creation, state transitions, and peer management

import std/[unittest, tables, atomics, locks]
import fractio/core/types
import fractio/distributed/network/types
import fractio/distributed/network/config
import fractio/distributed/network/connection_manager
import fractio/distributed/network/raft_transport
import fractio/distributed/network/network_raft_node
import fractio/distributed/raft/types as raft_types

suite "NetworkRaftNode Creation":

  test "newNetworkRaftNode creates node with defaults":
    var raftConfig: raft_types.RaftConfig
    raftConfig.serverId = 1'i32
    raftConfig.electionTimeout = 1000

    let netConfig = newNetworkConfig(NodeID("raft_1"), 9000)
    let node = newNetworkRaftNode(raftConfig, netConfig)

    check node.serverId == 1'i32
    check node.nodeState.role == SR_FOLLOWER
    check node.nodeState.currentTerm == 0
    check node.nodeState.votedFor == -1
    check node.nodeState.leaderId == -1
    check node.nodeState.commitIndex == 0
    check node.nodeState.lastApplied == 0

    node.close()

  test "newNetworkRaftNode initializes tracking tables":
    var raftConfig: raft_types.RaftConfig
    raftConfig.serverId = 1'i32

    let netConfig = newNetworkConfig(NodeID("raft_1"), 9000)
    let node = newNetworkRaftNode(raftConfig, netConfig)

    check node.votesReceived.len == 0
    check node.matchIndex.len == 0
    check node.nextIndex.len == 0

    node.close()

  test "newNetworkRaftNode initializes locks":
    var raftConfig: raft_types.RaftConfig
    raftConfig.serverId = 1'i32

    let netConfig = newNetworkConfig(NodeID("raft_1"), 9000)
    let node = newNetworkRaftNode(raftConfig, netConfig)

    # Locks are initialized - we can use withLock
    withLock node.votesLock:
      node.votesReceived[1'i32] = true

    check node.votesReceived.len == 1

    node.close()

  test "newNetworkRaftNode creates connection manager":
    var raftConfig: raft_types.RaftConfig
    raftConfig.serverId = 1'i32

    let netConfig = newNetworkConfig(NodeID("raft_1"), 9000)
    let node = newNetworkRaftNode(raftConfig, netConfig)

    check node.connManager != nil
    check node.raftTransport != nil

    node.close()

suite "NetworkRaftNode Peer Management":

  test "addPeer adds peer to tracking tables":
    var raftConfig: raft_types.RaftConfig
    raftConfig.serverId = 1'i32

    let netConfig = newNetworkConfig(NodeID("raft_1"), 9000)
    let node = newNetworkRaftNode(raftConfig, netConfig)

    node.addPeer(2'i32, "localhost", 9100, 9101, 9102)

    check node.nextIndex.hasKey(2'i32)
    check node.matchIndex.hasKey(2'i32)
    check node.nextIndex[2'i32] == 1'u64
    check node.matchIndex[2'i32] == 0'u64

    node.close()

  test "addPeer multiple peers":
    var raftConfig: raft_types.RaftConfig
    raftConfig.serverId = 1'i32

    let netConfig = newNetworkConfig(NodeID("raft_1"), 9000)
    let node = newNetworkRaftNode(raftConfig, netConfig)

    node.addPeer(2'i32, "localhost", 9100, 9101, 9102)
    node.addPeer(3'i32, "localhost", 9200, 9201, 9202)
    node.addPeer(4'i32, "localhost", 9300, 9301, 9302)

    check node.nextIndex.len == 3
    check node.matchIndex.len == 3

    node.close()

  test "removePeer removes from tracking tables":
    var raftConfig: raft_types.RaftConfig
    raftConfig.serverId = 1'i32

    let netConfig = newNetworkConfig(NodeID("raft_1"), 9000)
    let node = newNetworkRaftNode(raftConfig, netConfig)

    node.addPeer(2'i32, "localhost", 9100, 9101, 9102)
    node.addPeer(3'i32, "localhost", 9200, 9201, 9202)

    check node.nextIndex.len == 2

    node.removePeer(2'i32)

    check node.nextIndex.len == 1
    check node.nextIndex.hasKey(3'i32)
    check not node.nextIndex.hasKey(2'i32)

    node.close()

  test "removePeer non-existent peer is safe":
    var raftConfig: raft_types.RaftConfig
    raftConfig.serverId = 1'i32

    let netConfig = newNetworkConfig(NodeID("raft_1"), 9000)
    let node = newNetworkRaftNode(raftConfig, netConfig)

    node.addPeer(2'i32, "localhost", 9100, 9101, 9102)

    node.removePeer(99'i32) # Non-existent

    check node.nextIndex.len == 1

    node.close()

suite "NetworkRaftNode State Transitions":

  test "becomeCandidate increments term and sets votedFor":
    var raftConfig: raft_types.RaftConfig
    raftConfig.serverId = 1'i32

    let netConfig = newNetworkConfig(NodeID("raft_1"), 9000)
    let node = newNetworkRaftNode(raftConfig, netConfig)

    check node.nodeState.role == SR_FOLLOWER
    check node.nodeState.currentTerm == 0

    node.becomeCandidate()

    check node.nodeState.role == SR_CANDIDATE
    check node.nodeState.currentTerm == 1
    check node.nodeState.votedFor == 1'i32

    node.close()

  test "becomeCandidate clears votesReceived":
    var raftConfig: raft_types.RaftConfig
    raftConfig.serverId = 1'i32

    let netConfig = newNetworkConfig(NodeID("raft_1"), 9000)
    let node = newNetworkRaftNode(raftConfig, netConfig)

    # Add some existing votes
    withLock node.votesLock:
      node.votesReceived[2'i32] = true

    check node.votesReceived.len == 1

    node.becomeCandidate()

    # Candidate clears and adds self-vote
    check node.votesReceived.len == 1
    check node.votesReceived.hasKey(1'i32)
    check node.votesReceived[1'i32] == true

    node.close()

  test "becomeLeader transitions from candidate":
    var raftConfig: raft_types.RaftConfig
    raftConfig.serverId = 1'i32

    let netConfig = newNetworkConfig(NodeID("raft_1"), 9000)
    let node = newNetworkRaftNode(raftConfig, netConfig)

    node.becomeCandidate()
    node.becomeLeader()

    check node.nodeState.role == SR_LEADER
    check node.nodeState.leaderId == 1'i32

    node.close()

  test "becomeLeader fails if not candidate":
    var raftConfig: raft_types.RaftConfig
    raftConfig.serverId = 1'i32

    let netConfig = newNetworkConfig(NodeID("raft_1"), 9000)
    let node = newNetworkRaftNode(raftConfig, netConfig)

    # Start as follower
    check node.nodeState.role == SR_FOLLOWER

    node.becomeLeader() # Should not transition

    check node.nodeState.role == SR_FOLLOWER
    check node.nodeState.leaderId == -1

    node.close()

  test "becomeFollower sets term and clears votedFor":
    var raftConfig: raft_types.RaftConfig
    raftConfig.serverId = 1'i32

    let netConfig = newNetworkConfig(NodeID("raft_1"), 9000)
    let node = newNetworkRaftNode(raftConfig, netConfig)

    node.becomeCandidate()
    check node.nodeState.currentTerm == 1
    check node.nodeState.votedFor == 1'i32

    node.becomeFollower(5'i64)

    check node.nodeState.role == SR_FOLLOWER
    check node.nodeState.currentTerm == 5'i64
    check node.nodeState.votedFor == -1

    node.close()

suite "NetworkRaftNode Status Queries":

  test "isLeader returns true when leader":
    var raftConfig: raft_types.RaftConfig
    raftConfig.serverId = 1'i32

    let netConfig = newNetworkConfig(NodeID("raft_1"), 9000)
    let node = newNetworkRaftNode(raftConfig, netConfig)

    check node.isLeader() == false

    node.becomeCandidate()
    node.becomeLeader()

    check node.isLeader() == true

    node.close()

  test "isCandidate returns true when candidate":
    var raftConfig: raft_types.RaftConfig
    raftConfig.serverId = 1'i32

    let netConfig = newNetworkConfig(NodeID("raft_1"), 9000)
    let node = newNetworkRaftNode(raftConfig, netConfig)

    check node.isCandidate() == false

    node.becomeCandidate()

    check node.isCandidate() == true

    node.close()

  test "isFollower returns true when follower":
    var raftConfig: raft_types.RaftConfig
    raftConfig.serverId = 1'i32

    let netConfig = newNetworkConfig(NodeID("raft_1"), 9000)
    let node = newNetworkRaftNode(raftConfig, netConfig)

    check node.isFollower() == true

    node.becomeCandidate()

    check node.isFollower() == false

    node.close()

  test "getTerm returns current term":
    var raftConfig: raft_types.RaftConfig
    raftConfig.serverId = 1'i32

    let netConfig = newNetworkConfig(NodeID("raft_1"), 9000)
    let node = newNetworkRaftNode(raftConfig, netConfig)

    check node.getTerm() == 0'i64

    node.becomeCandidate()
    check node.getTerm() == 1'i64

    node.becomeFollower(10'i64)
    check node.getTerm() == 10'i64

    node.close()

  test "getRole returns current role":
    var raftConfig: raft_types.RaftConfig
    raftConfig.serverId = 1'i32

    let netConfig = newNetworkConfig(NodeID("raft_1"), 9000)
    let node = newNetworkRaftNode(raftConfig, netConfig)

    check node.getRole() == SR_FOLLOWER

    node.becomeCandidate()
    check node.getRole() == SR_CANDIDATE

    node.becomeLeader()
    check node.getRole() == SR_LEADER

    node.close()

  test "getCommitIndex returns commit index":
    var raftConfig: raft_types.RaftConfig
    raftConfig.serverId = 1'i32

    let netConfig = newNetworkConfig(NodeID("raft_1"), 9000)
    let node = newNetworkRaftNode(raftConfig, netConfig)

    check node.getCommitIndex() == 0'i64

    node.nodeState.commitIndex = 5'i64
    check node.getCommitIndex() == 5'i64

    node.close()

  test "getLeaderId returns leader ID":
    var raftConfig: raft_types.RaftConfig
    raftConfig.serverId = 1'i32

    let netConfig = newNetworkConfig(NodeID("raft_1"), 9000)
    let node = newNetworkRaftNode(raftConfig, netConfig)

    check node.getLeaderId() == -1'i32

    node.becomeCandidate()
    node.becomeLeader()
    check node.getLeaderId() == 1'i32

    node.close()

suite "NetworkRaftNode Election Timer":

  test "resetElectionTimer updates timestamp":
    var raftConfig: raft_types.RaftConfig
    raftConfig.serverId = 1'i32

    let netConfig = newNetworkConfig(NodeID("raft_1"), 9000)
    let node = newNetworkRaftNode(raftConfig, netConfig)

    node.resetElectionTimer()

    let resetTime = node.getLastResetTime()
    check resetTime > 0'i64

    node.close()

  test "getLastResetTime returns stored value":
    var raftConfig: raft_types.RaftConfig
    raftConfig.serverId = 1'i32

    let netConfig = newNetworkConfig(NodeID("raft_1"), 9000)
    let node = newNetworkRaftNode(raftConfig, netConfig)

    node.resetElectionTimer()
    let time1 = node.getLastResetTime()

    node.resetElectionTimer()
    let time2 = node.getLastResetTime()

    # Second reset should be >= first (time advances)
    check time2 >= time1

    node.close()

suite "NetworkRaftNode Vote Recording":

  test "recordVote adds vote to table":
    var raftConfig: raft_types.RaftConfig
    raftConfig.serverId = 1'i32

    let netConfig = newNetworkConfig(NodeID("raft_1"), 9000)
    let node = newNetworkRaftNode(raftConfig, netConfig)

    # Start as candidate with self-vote
    node.becomeCandidate()

    let hasMajority = node.recordVote(2'i32, true)

    check node.votesReceived.hasKey(2'i32)
    check node.votesReceived[2'i32] == true

    node.close()

  test "recordVote counts yes votes":
    var raftConfig: raft_types.RaftConfig
    raftConfig.serverId = 1'i32

    let netConfig = newNetworkConfig(NodeID("raft_1"), 9000)
    let node = newNetworkRaftNode(raftConfig, netConfig)

    # Add peers so we have a cluster
    node.addPeer(2'i32, "localhost", 9100, 9101, 9102)
    node.addPeer(3'i32, "localhost", 9200, 9201, 9202)

    node.becomeCandidate()

    # With 3 nodes, majority is 2
    # We already have self-vote, so one more vote gives majority
    let hasMajority = node.recordVote(2'i32, true)

    # 2 votes out of 3 nodes = majority (2)
    check hasMajority == true

    node.close()

  test "recordVote false vote counted but not majority":
    var raftConfig: raft_types.RaftConfig
    raftConfig.serverId = 1'i32

    let netConfig = newNetworkConfig(NodeID("raft_1"), 9000)
    let node = newNetworkRaftNode(raftConfig, netConfig)

    # Single node cluster
    node.becomeCandidate()

    # Record a false vote from imaginary peer
    discard node.recordVote(2'i32, false)

    # Still only 1 yes vote (self)
    check node.votesReceived[2'i32] == false

    node.close()

  test "recordVote multiple votes":
    var raftConfig: raft_types.RaftConfig
    raftConfig.serverId = 1'i32

    let netConfig = newNetworkConfig(NodeID("raft_1"), 9000)
    let node = newNetworkRaftNode(raftConfig, netConfig)

    # 5-node cluster
    node.addPeer(2'i32, "localhost", 9100, 9101, 9102)
    node.addPeer(3'i32, "localhost", 9200, 9201, 9202)
    node.addPeer(4'i32, "localhost", 9300, 9301, 9302)
    node.addPeer(5'i32, "localhost", 9400, 9401, 9402)

    node.becomeCandidate()

    # 5 nodes, majority is 3
    discard node.recordVote(2'i32, true)
    let hasMajority1 = node.recordVote(3'i32, true)

    # 3 yes votes (self + 2) = majority
    check hasMajority1 == true

    node.close()

suite "NetworkRaftNode Running State":

  test "running flag is initially false":
    var raftConfig: raft_types.RaftConfig
    raftConfig.serverId = 1'i32

    let netConfig = newNetworkConfig(NodeID("raft_1"), 9000)
    let node = newNetworkRaftNode(raftConfig, netConfig)

    check node.running.load() == false

    node.close()

  test "running flag set to false on close":
    var raftConfig: raft_types.RaftConfig
    raftConfig.serverId = 1'i32

    let netConfig = newNetworkConfig(NodeID("raft_1"), 9000)
    let node = newNetworkRaftNode(raftConfig, netConfig)

    node.running.store(true)
    check node.running.load() == true

    node.close()

    check node.running.load() == false

suite "NetworkRaftNode Multiple Close":

  test "Multiple close calls are safe":
    var raftConfig: raft_types.RaftConfig
    raftConfig.serverId = 1'i32

    let netConfig = newNetworkConfig(NodeID("raft_1"), 9000)
    let node = newNetworkRaftNode(raftConfig, netConfig)

    node.close()
    node.close() # Should not crash
