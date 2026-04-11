# Comprehensive unit tests for network config

import unittest
import tables
import options
import math
import random
import fractio/distributed/network/config
import fractio/core/types

suite "Peer Configuration - Extended":
  test "newPeerConfig creates config with correct ports":
    let peer = newPeerConfig(NodeID("node1"), "localhost", 9000)

    check string(peer.nodeId) == "node1"
    check peer.host == "localhost"
    check peer.basePort == 9000
    check peer.raftPort == 9000
    check peer.clientPort == 9001
    check peer.adminPort == 9002
    check peer.timerPort == 9003

  test "newPeerConfig with different base ports":
    let peer1 = newPeerConfig(NodeID("node1"), "localhost", 9000)
    check peer1.raftPort == 9000
    check peer1.clientPort == 9001

    let peer2 = newPeerConfig(NodeID("node2"), "localhost", 10000)
    check peer2.raftPort == 10000
    check peer2.clientPort == 10001

    let peer3 = newPeerConfig(NodeID("node3"), "localhost", 0)
    check peer3.raftPort == 0
    check peer3.clientPort == 1

  test "PeerConfig port offsets are correct":
    let peer = newPeerConfig(NodeID("node1"), "localhost", 5000)

    check peer.raftPort == peer.basePort + 0
    check peer.clientPort == peer.basePort + 1
    check peer.adminPort == peer.basePort + 2
    check peer.timerPort == peer.basePort + 3

  test "PeerConfig with different hosts":
    let peerLocal = newPeerConfig(NodeID("node1"), "localhost", 9000)
    check peerLocal.host == "localhost"

    let peerIP = newPeerConfig(NodeID("node2"), "192.168.1.1", 9000)
    check peerIP.host == "192.168.1.1"

    let peerDNS = newPeerConfig(NodeID("node3"), "node3.example.com", 9000)
    check peerDNS.host == "node3.example.com"

suite "Network Configuration - Extended":
  test "newNetworkConfig creates config with defaults":
    let config = newNetworkConfig(NodeID("node1"))

    check string(config.nodeId) == "node1"
    check config.basePort == DEFAULT_BASE_PORT
    check config.bindAddress == DEFAULT_BIND_ADDRESS

  test "newNetworkConfig with custom base port":
    let config = newNetworkConfig(NodeID("node1"), 10000)

    check config.basePort == 10000

  test "newNetworkConfig with custom bind address":
    let config = newNetworkConfig(NodeID("node1"), 9000, "192.168.1.1")

    check config.bindAddress == "192.168.1.1"

  test "newNetworkConfig TCP defaults":
    let config = newNetworkConfig(NodeID("node1"))

    check config.tcpNoDelay == DEFAULT_TCP_NO_DELAY
    check config.tcpKeepAlive == DEFAULT_TCP_KEEP_ALIVE
    check config.tcpSendBufferSize == DEFAULT_TCP_SEND_BUFFER_SIZE
    check config.tcpRecvBufferSize == DEFAULT_TCP_RECV_BUFFER_SIZE
    check config.tcpConnectTimeoutMs == DEFAULT_TCP_CONNECT_TIMEOUT_MS
    check config.tcpReadTimeoutMs == DEFAULT_TCP_READ_TIMEOUT_MS
    check config.tcpWriteTimeoutMs == DEFAULT_TCP_WRITE_TIMEOUT_MS
    check config.tcpMaxMessageSize == DEFAULT_TCP_MAX_MESSAGE_SIZE

  test "newNetworkConfig connection pooling defaults":
    let config = newNetworkConfig(NodeID("node1"))

    check config.maxConnectionsPerNode == 4
    check config.idleTimeoutMs == DEFAULT_IDLE_TIMEOUT_MS

  test "newNetworkConfig health checking defaults":
    let config = newNetworkConfig(NodeID("node1"))

    check config.healthCheckIntervalMs == DEFAULT_HEALTH_CHECK_INTERVAL_MS
    check config.failureThreshold == DEFAULT_FAILURE_THRESHOLD
    check config.recoveryThreshold == DEFAULT_RECOVERY_THRESHOLD

  test "newNetworkConfig thread pool defaults":
    let config = newNetworkConfig(NodeID("node1"))

    check config.raftWorkers == DEFAULT_RAFT_WORKERS
    check config.clientWorkers == DEFAULT_CLIENT_WORKERS
    check config.adminWorkers == DEFAULT_ADMIN_WORKERS

  test "newNetworkConfig peers empty initially":
    let config = newNetworkConfig(NodeID("node1"))

    check config.peers.len == 0

  test "newNetworkConfig is ref object":
    let config1 = newNetworkConfig(NodeID("node1"))
    let config2 = config1

    config1.basePort = 9999
    check config2.basePort == 9999

suite "Network Configuration Port Helpers - Extended":
  test "raftPort returns correct port":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    check config.raftPort() == 9000

    let config2 = newNetworkConfig(NodeID("node2"), 10000)
    check config2.raftPort() == 10000

  test "clientPort returns correct port":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    check config.clientPort() == 9001

    let config2 = newNetworkConfig(NodeID("node2"), 10000)
    check config2.clientPort() == 10001

  test "adminPort returns correct port":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    check config.adminPort() == 9002

    let config2 = newNetworkConfig(NodeID("node2"), 10000)
    check config2.adminPort() == 10002

  test "timerPort returns correct port":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    check config.timerPort() == 9003

    let config2 = newNetworkConfig(NodeID("node2"), 10000)
    check config2.timerPort() == 10003

  test "raftAddr returns correct address":
    let config = newNetworkConfig(NodeID("node1"), 9000, "0.0.0.0")
    check config.raftAddr() == "0.0.0.0:9000"

    let config2 = newNetworkConfig(NodeID("node2"), 10000, "192.168.1.1")
    check config2.raftAddr() == "192.168.1.1:10000"

  test "clientAddr returns correct address":
    let config = newNetworkConfig(NodeID("node1"), 9000, "0.0.0.0")
    check config.clientAddr() == "0.0.0.0:9001"

  test "adminAddr returns correct address":
    let config = newNetworkConfig(NodeID("node1"), 9000, "0.0.0.0")
    check config.adminAddr() == "0.0.0.0:9002"

  test "Port helpers work with modified config":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    config.basePort = 5000

    check config.raftPort() == 5000
    check config.clientPort() == 5001
    check config.adminPort() == 5002
    check config.timerPort() == 5003

suite "Network Configuration Peer Management - Extended":
  test "addPeer adds peer to config":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let peer = newPeerConfig(NodeID("node2"), "localhost", 9100)

    config.addPeer(peer)

    check config.peers.len == 1
    check config.peers[0].nodeId == NodeID("node2")

  test "addPeer multiple peers":
    let config = newNetworkConfig(NodeID("node1"), 9000)

    let peer1 = newPeerConfig(NodeID("node2"), "localhost", 9100)
    let peer2 = newPeerConfig(NodeID("node3"), "localhost", 9200)
    let peer3 = newPeerConfig(NodeID("node4"), "localhost", 9300)

    config.addPeer(peer1)
    config.addPeer(peer2)
    config.addPeer(peer3)

    check config.peers.len == 3

  test "removePeer removes peer":
    let config = newNetworkConfig(NodeID("node1"), 9000)

    let peer1 = newPeerConfig(NodeID("node2"), "localhost", 9100)
    let peer2 = newPeerConfig(NodeID("node3"), "localhost", 9200)

    config.addPeer(peer1)
    config.addPeer(peer2)

    check config.peers.len == 2

    config.removePeer(NodeID("node2"))

    check config.peers.len == 1
    check config.peers[0].nodeId == NodeID("node3")

  test "removePeer for non-existent peer":
    let config = newNetworkConfig(NodeID("node1"), 9000)

    let peer = newPeerConfig(NodeID("node2"), "localhost", 9100)
    config.addPeer(peer)

    config.removePeer(NodeID("node_nonexistent"))

    check config.peers.len == 1

  test "getPeer returns peer":
    let config = newNetworkConfig(NodeID("node1"), 9000)

    let peer = newPeerConfig(NodeID("node2"), "192.168.1.1", 9100)
    config.addPeer(peer)

    let found = config.getPeer(NodeID("node2"))
    check found.isSome
    check found.get().host == "192.168.1.1"
    check found.get().basePort == 9100

  test "getPeer returns none for unknown peer":
    let config = newNetworkConfig(NodeID("node1"), 9000)

    let found = config.getPeer(NodeID("node_nonexistent"))
    check found.isNone

  test "hasPeer returns true for existing peer":
    let config = newNetworkConfig(NodeID("node1"), 9000)

    let peer = newPeerConfig(NodeID("node2"), "localhost", 9100)
    config.addPeer(peer)

    check config.hasPeer(NodeID("node2")) == true

  test "hasPeer returns false for non-existent peer":
    let config = newNetworkConfig(NodeID("node1"), 9000)

    check config.hasPeer(NodeID("node_nonexistent")) == false

  test "Peer management operations are consistent":
    let config = newNetworkConfig(NodeID("node1"), 9000)

    let peer1 = newPeerConfig(NodeID("node2"), "localhost", 9100)
    config.addPeer(peer1)
    check config.hasPeer(NodeID("node2")) == true
    check config.getPeer(NodeID("node2")).isSome

    config.removePeer(NodeID("node2"))
    check config.hasPeer(NodeID("node2")) == false
    check config.getPeer(NodeID("node2")).isNone

suite "Backoff Policy - Extended":
  test "newBackoffPolicy creates policy with defaults":
    let policy = newBackoffPolicy()

    check policy.initialDelayMs == DEFAULT_BACKOFF_INITIAL_MS
    check policy.maxDelayMs == DEFAULT_BACKOFF_MAX_MS
    check policy.multiplier == DEFAULT_BACKOFF_MULTIPLIER
    check policy.jitter == true

  test "newBackoffPolicy with custom values":
    let policy = newBackoffPolicy(
      initialMs = 50,
      maxMs = 1000,
      multiplier = 2.0,
      jitter = false
    )

    check policy.initialDelayMs == 50
    check policy.maxDelayMs == 1000
    check policy.multiplier == 2.0
    check policy.jitter == false

  test "calculateBackoff for first attempt":
    let policy = newBackoffPolicy(100, 5000, 1.5, false)

    let delay = policy.calculateBackoff(0)
    check delay == 100

  test "calculateBackoff increases exponentially":
    let policy = newBackoffPolicy(100, 5000, 2.0, false)

    check policy.calculateBackoff(0) == 100
    check policy.calculateBackoff(1) == 200
    check policy.calculateBackoff(2) == 400
    check policy.calculateBackoff(3) == 800
    check policy.calculateBackoff(4) == 1600

  test "calculateBackoff caps at max delay":
    let policy = newBackoffPolicy(100, 500, 2.0, false)

    check policy.calculateBackoff(0) == 100
    check policy.calculateBackoff(1) == 200
    check policy.calculateBackoff(2) == 400
    check policy.calculateBackoff(3) == 500
    check policy.calculateBackoff(4) == 500
    check policy.calculateBackoff(10) == 500

  test "calculateBackoff with jitter adds randomness":
    let policy = newBackoffPolicy(100, 5000, 1.5, true)

    let delays: seq[int] = @[policy.calculateBackoff(1), policy.calculateBackoff(1),
                             policy.calculateBackoff(1),
                                 policy.calculateBackoff(1)]

    let baseDelay = int(100.0 * 1.5)
    for d in delays:
      check d >= baseDelay
      check d <= baseDelay + (baseDelay div 2)

  test "calculateBackoff without jitter is deterministic":
    let policy = newBackoffPolicy(100, 5000, 1.5, false)

    let delay1 = policy.calculateBackoff(1)
    let delay2 = policy.calculateBackoff(1)
    let delay3 = policy.calculateBackoff(1)

    check delay1 == delay2
    check delay2 == delay3

  test "calculateBackoff zero initial delay":
    let policy = newBackoffPolicy(0, 1000, 2.0, false)

    check policy.calculateBackoff(0) == 0

  test "calculateBackoff different multipliers":
    let policy1 = newBackoffPolicy(100, 5000, 1.0, false)
    check policy1.calculateBackoff(5) == 100

    let policy2 = newBackoffPolicy(100, 5000, 3.0, false)
    check policy2.calculateBackoff(1) == 300
    check policy2.calculateBackoff(2) == 900

suite "Default Constants Validation":
  test "TCP constants":
    check DEFAULT_BASE_PORT == 9000
    check DEFAULT_BIND_ADDRESS == "0.0.0.0"
    check DEFAULT_TCP_NO_DELAY == true
    check DEFAULT_TCP_KEEP_ALIVE == true
    check DEFAULT_TCP_SEND_BUFFER_SIZE == 4 * 1024 * 1024
    check DEFAULT_TCP_RECV_BUFFER_SIZE == 4 * 1024 * 1024
    check DEFAULT_TCP_CONNECT_TIMEOUT_MS == 5000
    check DEFAULT_TCP_READ_TIMEOUT_MS == 30000
    check DEFAULT_TCP_WRITE_TIMEOUT_MS == 30000
    check DEFAULT_TCP_MAX_MESSAGE_SIZE == 16 * 1024 * 1024

  test "Connection pooling constants":
    check DEFAULT_MAX_CONNECTIONS_PER_NODE == 4
    check DEFAULT_IDLE_TIMEOUT_MS == 60000

  test "Health checking constants":
    check DEFAULT_HEALTH_CHECK_INTERVAL_MS == 1000
    check DEFAULT_FAILURE_THRESHOLD == 3
    check DEFAULT_RECOVERY_THRESHOLD == 2

  test "Thread pool constants":
    check DEFAULT_RAFT_WORKERS == 4
    check DEFAULT_CLIENT_WORKERS == 8
    check DEFAULT_ADMIN_WORKERS == 2

  test "Backoff constants":
    check DEFAULT_BACKOFF_INITIAL_MS == 100
    check DEFAULT_BACKOFF_MAX_MS == 5000
    check DEFAULT_BACKOFF_MULTIPLIER == 1.5

suite "Network Configuration Modification":
  test "Can modify TCP settings":
    let config = newNetworkConfig(NodeID("node1"))

    config.tcpNoDelay = false
    config.tcpKeepAlive = false
    config.tcpConnectTimeoutMs = 1000

    check config.tcpNoDelay == false
    check config.tcpKeepAlive == false
    check config.tcpConnectTimeoutMs == 1000

  test "Can modify connection pooling settings":
    let config = newNetworkConfig(NodeID("node1"))

    config.maxConnectionsPerNode = 10
    config.idleTimeoutMs = 30000

    check config.maxConnectionsPerNode == 10
    check config.idleTimeoutMs == 30000

  test "Can modify health checking settings":
    let config = newNetworkConfig(NodeID("node1"))

    config.healthCheckIntervalMs = 500
    config.failureThreshold = 5
    config.recoveryThreshold = 3

    check config.healthCheckIntervalMs == 500
    check config.failureThreshold == 5
    check config.recoveryThreshold == 3

  test "Can modify thread pool settings":
    let config = newNetworkConfig(NodeID("node1"))

    config.raftWorkers = 8
    config.clientWorkers = 16
    config.adminWorkers = 4

    check config.raftWorkers == 8
    check config.clientWorkers == 16
    check config.adminWorkers == 4

suite "Network Configuration Edge Cases":
  test "Config with nodeId containing special characters":
    let config = newNetworkConfig(NodeID("node-with-hyphen_underscore"), 9000)
    check string(config.nodeId) == "node-with-hyphen_underscore"

  test "Config with empty nodeId":
    let config = newNetworkConfig(NodeID(""), 9000)
    check string(config.nodeId) == ""

  test "Config with very high base port":
    let config = newNetworkConfig(NodeID("node1"), 65000)
    check config.basePort == 65000
    check config.raftPort() == 65000
    check config.timerPort() == 65003

  test "Config with empty bind address":
    let config = newNetworkConfig(NodeID("node1"), 9000, "")
    check config.bindAddress == ""

  test "Peer with empty host":
    let peer = newPeerConfig(NodeID("node1"), "", 9000)
    check peer.host == ""

suite "Network Configuration Integration":
  test "Config can be shared across components":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    config.tcpNoDelay = true
    config.maxConnectionsPerNode = 8

    check config.tcpNoDelay == true
    check config.maxConnectionsPerNode == 8

  test "Config modifications affect computed values":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    config.bindAddress = "127.0.0.1"

    check config.raftAddr() == "127.0.0.1:9000"
    check config.clientAddr() == "127.0.0.1:9001"

suite "Backoff Policy Edge Cases":
  test "Backoff with very high multiplier":
    let policy = newBackoffPolicy(100, 5000, 10.0, false)

    check policy.calculateBackoff(0) == 100
    check policy.calculateBackoff(1) == 1000
    check policy.calculateBackoff(2) == 5000
    check policy.calculateBackoff(3) == 5000

  test "Backoff with very low multiplier":
    let policy = newBackoffPolicy(100, 5000, 0.5, false)

    check policy.calculateBackoff(0) == 100
    check policy.calculateBackoff(1) == 50
    check policy.calculateBackoff(2) == 25

  test "Backoff with zero max delay":
    let policy = newBackoffPolicy(100, 0, 2.0, false)

    check policy.calculateBackoff(0) == 0
    check policy.calculateBackoff(1) == 0

  test "Backoff with high attempt number":
    let policy = newBackoffPolicy(100, 5000, 2.0, false)

    check policy.calculateBackoff(100) == 5000
    check policy.calculateBackoff(1000) == 5000

suite "Network Configuration Peer Edge Cases":
  test "Multiple peers with same nodeId":
    let config = newNetworkConfig(NodeID("node1"), 9000)

    let peer1 = newPeerConfig(NodeID("node2"), "localhost", 9100)
    let peer2 = newPeerConfig(NodeID("node2"), "remotehost", 9200)

    config.addPeer(peer1)
    config.addPeer(peer2)

    check config.peers.len == 2
    check config.peers[0].host == "localhost"
    check config.peers[1].host == "remotehost"

  test "getPeer returns first matching peer":
    let config = newNetworkConfig(NodeID("node1"), 9000)

    let peer1 = newPeerConfig(NodeID("node2"), "localhost", 9100)
    let peer2 = newPeerConfig(NodeID("node2"), "remotehost", 9200)

    config.addPeer(peer1)
    config.addPeer(peer2)

    let found = config.getPeer(NodeID("node2"))
    check found.isSome
    check found.get().host == "localhost"

  test "removePeer removes all matching peers":
    let config = newNetworkConfig(NodeID("node1"), 9000)

    let peer1 = newPeerConfig(NodeID("node2"), "localhost", 9100)
    let peer2 = newPeerConfig(NodeID("node2"), "remotehost", 9200)
    let peer3 = newPeerConfig(NodeID("node3"), "localhost", 9300)

    config.addPeer(peer1)
    config.addPeer(peer2)
    config.addPeer(peer3)

    config.removePeer(NodeID("node2"))

    check config.peers.len == 1
    check config.peers[0].nodeId == NodeID("node3")
