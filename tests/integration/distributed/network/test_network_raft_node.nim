# Integration Tests for Network Raft Node
# Tests TCP-based network transport for Raft consensus

import unittest
import std/[tables, os, times, atomics, options]

import fractio/distributed/network/types
import fractio/distributed/network/config
import fractio/distributed/network/raft_transport
import fractio/distributed/network/connection_manager
import fractio/distributed/network/network_raft_node
import fractio/distributed/raft/types as raft_types
import fractio/utils/logging

# =============================================================================
# Test Helpers
# =============================================================================

proc createTestNetworkConfig(serverId: int32, basePort: int): NetworkConfig =
  ## Create a test network configuration
  result = newNetworkConfig(
    nodeId = toNodeID(serverId),
    basePort = basePort,
    bindAddress = "127.0.0.1"
  )

proc createTestRaftConfig(serverId: int32): raft_types.RaftConfig =
  ## Create a test Raft configuration
  result = raft_types.RaftConfig(
    serverId: serverId,
    endpoint: "127.0.0.1:9000",
    electionTimeout: 150,
    heartbeatInterval: 50,
    logStoragePath: "tmp/network_raft_test_" & $serverId & "/",
    snapshotEnabled: false,
    snapshotDistance: 1000,
    maxAppendSize: 100
  )

proc cleanupTestDir(path: string) =
  ## Clean up test directory
  if dirExists(path):
    try:
      removeDir(path)
    except:
      discard

# =============================================================================
# Integration Tests
# =============================================================================

suite "Network Raft Node Integration Tests":

  test "Create and start single network raft node":
    # Create configuration
    let netConfig = createTestNetworkConfig(1, 9100)
    let raftConfig = createTestRaftConfig(1)

    # Create node
    var node = newNetworkRaftNode(raftConfig, netConfig)

    check node.serverId == 1
    check node.nodeState.role == SR_FOLLOWER
    check node.nodeState.currentTerm == 0

    # Start the node
    let started = node.start()
    check started == true
    check load(node.running, moRelaxed) == true

    # Stop the node
    node.stop()
    node.close()

    # Cleanup
    cleanupTestDir(raftConfig.logStoragePath)

  test "Add peer to network raft node":
    # Create node 1
    let netConfig1 = createTestNetworkConfig(1, 9101)
    let raftConfig1 = createTestRaftConfig(1)
    var node1 = newNetworkRaftNode(raftConfig1, netConfig1)

    # Add peer (using computed ports based on peer's base port)
    let peerBasePort = 9200
    node1.addPeer(2, "127.0.0.1", peerBasePort, peerBasePort + 1, peerBasePort + 2)

    # Verify peer was added
    let nodes = node1.connManager.getAllNodes()
    check nodes.len == 2 # Self + peer

    node1.close()
    cleanupTestDir(raftConfig1.logStoragePath)

  test "Two nodes can discover each other":
    # Create node 1
    let netConfig1 = createTestNetworkConfig(1, 9110)
    let raftConfig1 = createTestRaftConfig(1)
    var node1 = newNetworkRaftNode(raftConfig1, netConfig1)

    # Create node 2
    let netConfig2 = createTestNetworkConfig(2, 9120)
    let raftConfig2 = createTestRaftConfig(2)
    var node2 = newNetworkRaftNode(raftConfig2, netConfig2)

    # Add each other as peers
    # Node 1 knows about node 2 (which uses base port 9120)
    node1.addPeer(2, "127.0.0.1", 9120, 9121, 9122)
    # Node 2 knows about node 1 (which uses base port 9110)
    node2.addPeer(1, "127.0.0.1", 9110, 9111, 9112)

    # Verify both nodes know about each other
    let nodes1 = node1.connManager.getAllNodes()
    let nodes2 = node2.connManager.getAllNodes()

    check nodes1.len == 2
    check nodes2.len == 2

    node1.close()
    node2.close()

    cleanupTestDir(raftConfig1.logStoragePath)
    cleanupTestDir(raftConfig2.logStoragePath)

  test "Node state transitions":
    # Create node
    let netConfig = createTestNetworkConfig(1, 9130)
    let raftConfig = createTestRaftConfig(1)
    var node = newNetworkRaftNode(raftConfig, netConfig)

    # Start as follower
    check node.isFollower() == true
    check node.isCandidate() == false
    check node.isLeader() == false

    # Become candidate
    node.becomeCandidate()
    check node.isCandidate() == true
    check node.getTerm() == 1

    # Become leader
    node.becomeLeader()
    check node.isLeader() == true
    check node.getLeaderId() == 1

    # Become follower
    node.becomeFollower(2)
    check node.isFollower() == true
    check node.getTerm() == 2

    node.close()
    cleanupTestDir(raftConfig.logStoragePath)

  test "Vote tracking for candidate":
    # Create node
    let netConfig = createTestNetworkConfig(1, 9140)
    let raftConfig = createTestRaftConfig(1)
    var node = newNetworkRaftNode(raftConfig, netConfig)

    # Add peers for majority calculation
    node.addPeer(2, "127.0.0.1", 9141, 9142, 9143)
    node.addPeer(3, "127.0.0.1", 9144, 9145, 9146)

    # Become candidate
    node.becomeCandidate()

    # Record votes
    # 3 nodes, need 2 votes (majority)
    let hasMajority1 = node.recordVote(1, true)         # Self vote
    check hasMajority1 == false # Not majority yet

    let hasMajority2 = node.recordVote(2, true)         # Peer vote
    check hasMajority2 == true # Now has majority

    node.close()
    cleanupTestDir(raftConfig.logStoragePath)

  test "Start and stop two connected nodes":
    # Create node 1
    let netConfig1 = createTestNetworkConfig(1, 9150)
    let raftConfig1 = createTestRaftConfig(1)
    var node1 = newNetworkRaftNode(raftConfig1, netConfig1)

    # Create node 2
    let netConfig2 = createTestNetworkConfig(2, 9160)
    let raftConfig2 = createTestRaftConfig(2)
    var node2 = newNetworkRaftNode(raftConfig2, netConfig2)

    # Add peers
    node1.addPeer(2, "127.0.0.1", 9160, 9161, 9162)
    node2.addPeer(1, "127.0.0.1", 9150, 9151, 9152)

    # Start both nodes
    let started1 = node1.start()
    let started2 = node2.start()

    check started1 == true
    check started2 == true
    check load(node1.running, moRelaxed) == true
    check load(node2.running, moRelaxed) == true

    # Give time for servers to start
    sleep(100)

    # Stop both nodes
    node1.stop()
    node2.stop()

    node1.close()
    node2.close()

    cleanupTestDir(raftConfig1.logStoragePath)
    cleanupTestDir(raftConfig2.logStoragePath)

  test "Three node cluster initialization":
    # Create 3 nodes
    let netConfig1 = createTestNetworkConfig(1, 9170)
    let raftConfig1 = createTestRaftConfig(1)
    var node1 = newNetworkRaftNode(raftConfig1, netConfig1)

    let netConfig2 = createTestNetworkConfig(2, 9180)
    let raftConfig2 = createTestRaftConfig(2)
    var node2 = newNetworkRaftNode(raftConfig2, netConfig2)

    let netConfig3 = createTestNetworkConfig(3, 9190)
    let raftConfig3 = createTestRaftConfig(3)
    var node3 = newNetworkRaftNode(raftConfig3, netConfig3)

    # Each node knows about all others
    node1.addPeer(2, "127.0.0.1", 9180, 9181, 9182)
    node1.addPeer(3, "127.0.0.1", 9190, 9191, 9192)

    node2.addPeer(1, "127.0.0.1", 9170, 9171, 9172)
    node2.addPeer(3, "127.0.0.1", 9190, 9191, 9192)

    node3.addPeer(1, "127.0.0.1", 9170, 9171, 9172)
    node3.addPeer(2, "127.0.0.1", 9180, 9181, 9182)

    # Verify all nodes have correct peer count
    check node1.connManager.getAllNodes().len == 3
    check node2.connManager.getAllNodes().len == 3
    check node3.connManager.getAllNodes().len == 3

    node1.close()
    node2.close()
    node3.close()

    cleanupTestDir(raftConfig1.logStoragePath)
    cleanupTestDir(raftConfig2.logStoragePath)
    cleanupTestDir(raftConfig3.logStoragePath)

  test "Node ID conversion":
    # Test NodeID <-> serverId conversion
    let id1 = toNodeID(1)
    let id2 = toNodeID(2)
    let idInvalid = toNodeID(-1)

    check toServerId(id1) == 1
    check toServerId(id2) == 2
    check toServerId(idInvalid) == -1

    # Test round-trip
    check toServerId(toNodeID(42)) == 42

  test "Heartbeat sending from leader":
    # Create 2 nodes
    let netConfig1 = createTestNetworkConfig(1, 9200)
    let raftConfig1 = createTestRaftConfig(1)
    var node1 = newNetworkRaftNode(raftConfig1, netConfig1)

    let netConfig2 = createTestNetworkConfig(2, 9210)
    let raftConfig2 = createTestRaftConfig(2)
    var node2 = newNetworkRaftNode(raftConfig2, netConfig2)

    # Add peers
    node1.addPeer(2, "127.0.0.1", 9210, 9211, 9212)
    node2.addPeer(1, "127.0.0.1", 9200, 9201, 9202)

    # Start nodes
    check node1.start() == true
    check node2.start() == true

    # Make node1 leader
    node1.becomeCandidate()
    node1.becomeLeader()

    check node1.isLeader() == true

    # Send heartbeat
    node1.sendHeartbeat()

    # Give time for message processing
    sleep(200)

    # Stop nodes
    node1.stop()
    node2.stop()

    node1.close()
    node2.close()

    cleanupTestDir(raftConfig1.logStoragePath)
    cleanupTestDir(raftConfig2.logStoragePath)

  test "RequestVote sending from candidate":
    # Create 2 nodes
    let netConfig1 = createTestNetworkConfig(1, 9220)
    let raftConfig2 = createTestRaftConfig(2)
    let raftConfig1 = createTestRaftConfig(1)
    var node1 = newNetworkRaftNode(raftConfig1, netConfig1)

    let netConfig2 = createTestNetworkConfig(2, 9230)
    var node2 = newNetworkRaftNode(raftConfig2, netConfig2)

    # Add peers
    node1.addPeer(2, "127.0.0.1", 9230, 9231, 9232)
    node2.addPeer(1, "127.0.0.1", 9220, 9221, 9222)

    # Start nodes
    check node1.start() == true
    check node2.start() == true

    # Make node1 candidate
    node1.becomeCandidate()
    check node1.isCandidate() == true

    # Send RequestVote
    node1.sendRequestVote()

    # Give time for message processing
    sleep(200)

    # Stop nodes
    node1.stop()
    node2.stop()

    node1.close()
    node2.close()

    cleanupTestDir(raftConfig1.logStoragePath)
    cleanupTestDir(raftConfig2.logStoragePath)

  test "Network config helper functions":
    # Test NetworkConfig helper functions
    let config = createTestNetworkConfig(1, 9000)

    check config.raftPort() == 9000
    check config.clientPort() == 9001
    check config.adminPort() == 9002
    check config.timerPort() == 9003

    check config.raftAddr() == "127.0.0.1:9000"
    check config.clientAddr() == "127.0.0.1:9001"

  test "Peer config creation":
    # Test PeerConfig
    let peer = newPeerConfig(toNodeID(2), "127.0.0.1", 9100)

    check toServerId(peer.nodeId) == 2
    check peer.host == "127.0.0.1"
    check peer.basePort == 9100
    check peer.raftPort == 9100
    check peer.clientPort == 9101
    check peer.adminPort == 9102
    check peer.timerPort == 9103

  test "Network config with peers":
    var config = createTestNetworkConfig(1, 9000)

    # Add peers using config API
    config.addPeer(newPeerConfig(toNodeID(2), "127.0.0.1", 9100))
    config.addPeer(newPeerConfig(toNodeID(3), "127.0.0.1", 9200))

    check config.peers.len == 2
    check config.hasPeer(toNodeID(2)) == true
    check config.hasPeer(toNodeID(3)) == true
    check config.hasPeer(toNodeID(4)) == false

    # Get peer
    let peer2 = config.getPeer(toNodeID(2))
    check peer2.isSome()
    check peer2.get().basePort == 9100

    # Remove peer
    config.removePeer(toNodeID(2))
    check config.peers.len == 1
    check config.hasPeer(toNodeID(2)) == false
