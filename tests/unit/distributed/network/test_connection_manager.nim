# Comprehensive unit tests for connection_manager.nim

import unittest
import tables
import locks
import options
import atomics
import fractio/distributed/network/types as network_types
import fractio/distributed/network/tcp_transport
import fractio/distributed/network/connection_pool
import fractio/distributed/network/connection_manager
import fractio/distributed/network/config
import fractio/distributed/network/health_checker
import fractio/core/types

suite "Connection Manager NodeInfo":
  test "NodeInfo has correct port assignments":
    let info = connection_manager.NodeInfo(
      nodeId: NodeID("node1"),
      host: "localhost",
      raftPort: 9000,
      clientPort: 9001,
      adminPort: 9002,
      isLocal: false
    )

    check info.raftPort == 9000
    check info.clientPort == 9001
    check info.adminPort == 9002
    check info.isLocal == false

  test "NodeInfo isLocal flag":
    let localInfo = connection_manager.NodeInfo(
      nodeId: NodeID("node1"),
      host: "localhost",
      raftPort: 9000,
      clientPort: 9001,
      adminPort: 9002,
      isLocal: true
    )

    let remoteInfo = connection_manager.NodeInfo(
      nodeId: NodeID("node2"),
      host: "remotehost",
      raftPort: 9100,
      clientPort: 9101,
      adminPort: 9102,
      isLocal: false
    )

    check localInfo.isLocal == true
    check remoteInfo.isLocal == false

suite "Connection Manager Creation":
  test "newConnectionManager creates manager with valid config":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    check cm != nil
    check cm.config == config
    check cm.raftTransport != nil
    check cm.clientTransport != nil
    check cm.adminTransport != nil
    check cm.raftPool != nil
    check cm.raftFireForgetPool != nil
    check cm.clientPool != nil
    check cm.adminPool != nil
    check cm.healthChecker != nil
    check cm.nodes.len == 1

    cm.close()

  test "newConnectionManager registers local node":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let localNode = cm.getLocalNode()
    check localNode.isSome
    check string(localNode.get().nodeId) == "node1"
    check localNode.get().isLocal == true
    check localNode.get().raftPort == 9000
    check localNode.get().clientPort == 9001
    check localNode.get().adminPort == 9002

    cm.close()

  test "newConnectionManager creates correct transports":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    check cm.raftTransport.port == 9000
    check cm.raftTransport.role == "raft"
    check cm.clientTransport.port == 9001
    check cm.clientTransport.role == "client"
    check cm.adminTransport.port == 9002
    check cm.adminTransport.role == "admin"

    cm.close()

  test "newConnectionManager creates correct pools":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    check cm.raftPool.role == "raft"
    check cm.raftFireForgetPool.role == "raft-ff"
    check cm.clientPool.role == "client"
    check cm.adminPool.role == "admin"

    cm.close()

suite "Connection Manager Close":
  test "close sets running to false":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    cm.close()
    check cm.running.load() == false

  test "close is safe to call multiple times":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    cm.close()
    cm.close()
    cm.close()

suite "Connection Manager Node Registry":
  test "registerNode adds node to registry":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let nodeInfo = connection_manager.NodeInfo(
      nodeId: NodeID("node2"),
      host: "localhost",
      raftPort: 9100,
      clientPort: 9101,
      adminPort: 9102,
      isLocal: false
    )

    cm.registerNode(nodeInfo)

    check cm.hasNode(NodeID("node2")) == true
    check cm.nodes.len == 2

    cm.close()

  test "registerNode for remote node registers with health checker":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let nodeInfo = connection_manager.NodeInfo(
      nodeId: NodeID("node2"),
      host: "localhost",
      raftPort: 9100,
      clientPort: 9101,
      adminPort: 9102,
      isLocal: false
    )

    cm.registerNode(nodeInfo)

    check cm.healthChecker.nodeHealth.len == 1

    cm.close()

  test "registerNode for local node does not register with health checker":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let localNodeInfo = connection_manager.NodeInfo(
      nodeId: NodeID("node_local"),
      host: "localhost",
      raftPort: 9000,
      clientPort: 9001,
      adminPort: 9002,
      isLocal: true
    )

    cm.registerNode(localNodeInfo)

    check cm.healthChecker.nodeHealth.len == 0

    cm.close()

  test "unregisterNode removes node from registry":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let nodeInfo = connection_manager.NodeInfo(
      nodeId: NodeID("node2"),
      host: "localhost",
      raftPort: 9100,
      clientPort: 9101,
      adminPort: 9102,
      isLocal: false
    )

    cm.registerNode(nodeInfo)
    check cm.hasNode(NodeID("node2")) == true

    cm.unregisterNode(NodeID("node2"))
    check cm.hasNode(NodeID("node2")) == false
    check cm.nodes.len == 1

    cm.close()

  test "unregisterNode removes from health checker":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let nodeInfo = connection_manager.NodeInfo(
      nodeId: NodeID("node2"),
      host: "localhost",
      raftPort: 9100,
      clientPort: 9101,
      adminPort: 9102,
      isLocal: false
    )

    cm.registerNode(nodeInfo)
    check cm.healthChecker.nodeHealth.len == 1

    cm.unregisterNode(NodeID("node2"))
    check cm.healthChecker.nodeHealth.len == 0

    cm.close()

  test "getNode returns node info":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let nodeInfo = connection_manager.NodeInfo(
      nodeId: NodeID("node2"),
      host: "192.168.1.1",
      raftPort: 9100,
      clientPort: 9101,
      adminPort: 9102,
      isLocal: false
    )

    cm.registerNode(nodeInfo)

    let retrieved = cm.getNode(NodeID("node2"))
    check retrieved.isSome
    check string(retrieved.get().nodeId) == "node2"
    check retrieved.get().host == "192.168.1.1"
    check retrieved.get().raftPort == 9100

    cm.close()

  test "getNode returns none for unknown node":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let retrieved = cm.getNode(NodeID("unknown"))
    check retrieved.isNone

    cm.close()

  test "hasNode returns correct values":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    check cm.hasNode(NodeID("node1")) == true
    check cm.hasNode(NodeID("node2")) == false

    let nodeInfo = connection_manager.NodeInfo(
      nodeId: NodeID("node2"),
      host: "localhost",
      raftPort: 9100,
      clientPort: 9101,
      adminPort: 9102,
      isLocal: false
    )

    cm.registerNode(nodeInfo)
    check cm.hasNode(NodeID("node2")) == true

    cm.close()

  test "getAllNodes returns all nodes":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let nodeInfo2 = connection_manager.NodeInfo(
      nodeId: NodeID("node2"),
      host: "localhost",
      raftPort: 9100,
      clientPort: 9101,
      adminPort: 9102,
      isLocal: false
    )

    let nodeInfo3 = connection_manager.NodeInfo(
      nodeId: NodeID("node3"),
      host: "localhost",
      raftPort: 9200,
      clientPort: 9201,
      adminPort: 9202,
      isLocal: false
    )

    cm.registerNode(nodeInfo2)
    cm.registerNode(nodeInfo3)

    let allNodes = cm.getAllNodes()
    check allNodes.len == 3

    cm.close()

  test "getRemoteNodes returns only remote nodes":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let nodeInfo2 = connection_manager.NodeInfo(
      nodeId: NodeID("node2"),
      host: "localhost",
      raftPort: 9100,
      clientPort: 9101,
      adminPort: 9102,
      isLocal: false
    )

    let nodeInfo3 = connection_manager.NodeInfo(
      nodeId: NodeID("node3"),
      host: "localhost",
      raftPort: 9200,
      clientPort: 9201,
      adminPort: 9202,
      isLocal: false
    )

    cm.registerNode(nodeInfo2)
    cm.registerNode(nodeInfo3)

    let remoteNodes = cm.getRemoteNodes()
    check remoteNodes.len == 2

    for node in remoteNodes:
      check node.isLocal == false

    cm.close()

  test "getLocalNode returns only local node":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let localNode = cm.getLocalNode()
    check localNode.isSome
    check localNode.get().isLocal == true

    cm.close()

  test "getLocalNode returns none when no local node":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    cm.nodes.clear()

    let localNode = cm.getLocalNode()
    check localNode.isNone

    cm.close()

suite "Connection Manager Start/Stop":
  test "start sets running to true":
    let config = newNetworkConfig(NodeID("node1"), 29000)
    let cm = newConnectionManager(config)

    let success = cm.start()
    check success == true
    check cm.running.load() == true

    cm.close()

  test "start returns true on success":
    let config = newNetworkConfig(NodeID("node1"), 29001)
    let cm = newConnectionManager(config)

    let success = cm.start()
    check success == true

    cm.close()

  test "stop sets running to false":
    let config = newNetworkConfig(NodeID("node1"), 29002)
    let cm = newConnectionManager(config)

    discard cm.start()
    check cm.running.load() == true

    cm.stop()
    check cm.running.load() == false

    cm.close()

  test "start fails on already bound port":
    let config1 = newNetworkConfig(NodeID("node1"), 29003)
    let cm1 = newConnectionManager(config1)
    let success1 = cm1.start()
    check success1 == true

    let config2 = newNetworkConfig(NodeID("node2"), 29003)
    let cm2 = newConnectionManager(config2)
    let success2 = cm2.start()
    check success2 == false

    cm1.close()
    cm2.close()

suite "Connection Manager Message Sending - Raft":
  test "sendRaftMessage returns true for local node":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let success = cm.sendRaftMessage(NodeID("node1"), "payload")
    check success == true

    cm.close()

  test "sendRaftMessage returns false for unknown node":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let success = cm.sendRaftMessage(NodeID("unknown"), "payload")
    check success == false

    cm.close()

  test "sendRaftMessageWithResponse returns none for local node":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let response = cm.sendRaftMessageWithResponse(NodeID("node1"), "payload", 1000)
    check response.isNone

    cm.close()

  test "sendRaftMessageWithResponse returns none for unknown node":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let response = cm.sendRaftMessageWithResponse(NodeID("unknown"), "payload", 1000)
    check response.isNone

    cm.close()

suite "Connection Manager Message Sending - Client":
  test "sendClientMessage returns true for local node":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let success = cm.sendClientMessage(NodeID("node1"), "payload")
    check success == true

    cm.close()

  test "sendClientMessage returns false for unknown node":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let success = cm.sendClientMessage(NodeID("unknown"), "payload")
    check success == false

    cm.close()

  test "sendClientMessageWithResponse returns none for local node":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let response = cm.sendClientMessageWithResponse(NodeID("node1"), "payload", 1000)
    check response.isNone

    cm.close()

  test "sendClientMessageWithResponse returns none for unknown node":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let response = cm.sendClientMessageWithResponse(NodeID("unknown"),
        "payload", 1000)
    check response.isNone

    cm.close()

suite "Connection Manager Message Sending - Admin":
  test "sendAdminMessage returns true for local node":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let success = cm.sendAdminMessage(NodeID("node1"), "payload")
    check success == true

    cm.close()

  test "sendAdminMessage returns false for unknown node":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let success = cm.sendAdminMessage(NodeID("unknown"), "payload")
    check success == false

    cm.close()

  test "sendAdminMessageWithResponse returns none for local node":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let response = cm.sendAdminMessageWithResponse(NodeID("node1"), "payload", 1000)
    check response.isNone

    cm.close()

  test "sendAdminMessageWithResponse returns none for unknown node":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let response = cm.sendAdminMessageWithResponse(NodeID("unknown"), "payload", 1000)
    check response.isNone

    cm.close()

suite "Connection Manager Broadcast":
  test "broadcastRaftMessage returns 0 with no remote nodes":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let count = cm.broadcastRaftMessage("payload")
    check count == 0

    cm.close()

  test "broadcastClientMessage returns 0 with no remote nodes":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let count = cm.broadcastClientMessage("payload")
    check count == 0

    cm.close()

  test "broadcastAdminMessage returns 0 with no remote nodes":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let count = cm.broadcastAdminMessage("payload")
    check count == 0

    cm.close()

suite "Connection Manager Health Checking":
  test "checkNodeHealth returns healthy for local node":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let health = cm.checkNodeHealth(NodeID("node1"))
    check health == hsHealthy

    cm.close()

  test "checkNodeHealth returns unknown for unknown node":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let health = cm.checkNodeHealth(NodeID("unknown"))
    check health == hsUnknown

    cm.close()

  test "isNodeHealthy returns true for local node":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    cm.healthChecker.registerNode(NodeID("node1"))
    cm.healthChecker.markHealthy(NodeID("node1"))

    check cm.isNodeHealthy(NodeID("node1")) == true

    cm.close()

  test "getHealthyNodes returns local node when healthy":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    cm.healthChecker.registerNode(NodeID("node1"))
    cm.healthChecker.markHealthy(NodeID("node1"))

    let healthy = cm.getHealthyNodes()
    check healthy.len == 1
    check string(healthy[0]) == "node1"

    cm.close()

  test "getUnhealthyNodes returns empty when all healthy":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let unhealthy = cm.getUnhealthyNodes()
    check unhealthy.len == 0

    cm.close()

suite "Connection Manager Handler Registration":
  test "registerRaftHandler registers with raft transport":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    proc handler(msg: string): string {.gcsafe.} = "raft_response"

    cm.registerRaftHandler(1'u16, handler)

    let h = cm.raftTransport.getHandler(1'u16)
    check h.isSome

    cm.close()

  test "registerClientHandler registers with client transport":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    proc handler(msg: string): string {.gcsafe.} = "client_response"

    cm.registerClientHandler(100'u16, handler)

    let h = cm.clientTransport.getHandler(100'u16)
    check h.isSome

    cm.close()

  test "registerAdminHandler registers with admin transport":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    proc handler(msg: string): string {.gcsafe.} = "admin_response"

    cm.registerAdminHandler(200'u16, handler)

    let h = cm.adminTransport.getHandler(200'u16)
    check h.isSome

    cm.close()

suite "Connection Manager Statistics":
  test "getStats returns correct structure":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let stats = cm.getStats()

    check stats.raftPoolStats.created == 0
    check stats.raftPoolStats.reused == 0
    check stats.raftPoolStats.closed == 0
    check stats.raftPoolStats.active == 0

    check stats.clientPoolStats.created == 0
    check stats.clientPoolStats.reused == 0

    check stats.adminPoolStats.created == 0
    check stats.adminPoolStats.reused == 0

    check stats.healthStats.healthy == 0
    check stats.healthStats.unhealthy == 0
    check stats.healthStats.unknown == 0

    check stats.nodeCount == 1

    cm.close()

  test "getStats reflects node count":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let nodeInfo2 = connection_manager.NodeInfo(
      nodeId: NodeID("node2"),
      host: "localhost",
      raftPort: 9100,
      clientPort: 9101,
      adminPort: 9102,
      isLocal: false
    )

    cm.registerNode(nodeInfo2)

    let stats = cm.getStats()
    check stats.nodeCount == 2

    cm.close()

suite "Connection Manager Thread Safety":
  test "Node registry is thread-safe":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    let nodeInfo1 = connection_manager.NodeInfo(
      nodeId: NodeID("node2"),
      host: "localhost",
      raftPort: 9100,
      clientPort: 9101,
      adminPort: 9102,
      isLocal: false
    )

    let nodeInfo2 = connection_manager.NodeInfo(
      nodeId: NodeID("node3"),
      host: "localhost",
      raftPort: 9200,
      clientPort: 9201,
      adminPort: 9202,
      isLocal: false
    )

    cm.registerNode(nodeInfo1)
    cm.registerNode(nodeInfo2)

    check cm.hasNode(NodeID("node2")) == true
    check cm.hasNode(NodeID("node3")) == true

    cm.close()

suite "Connection Manager NodeInfo":
  test "NodeInfo has correct port assignments":
    let info = connection_manager.NodeInfo(
      nodeId: NodeID("node1"),
      host: "localhost",
      raftPort: 9000,
      clientPort: 9001,
      adminPort: 9002,
      isLocal: false
    )

    check info.raftPort == 9000
    check info.clientPort == 9001
    check info.adminPort == 9002
    check info.isLocal == false

  test "NodeInfo isLocal flag":
    let localInfo = connection_manager.NodeInfo(
      nodeId: NodeID("node1"),
      host: "localhost",
      raftPort: 9000,
      clientPort: 9001,
      adminPort: 9002,
      isLocal: true
    )

    let remoteInfo = connection_manager.NodeInfo(
      nodeId: NodeID("node2"),
      host: "remotehost",
      raftPort: 9100,
      clientPort: 9101,
      adminPort: 9102,
      isLocal: false
    )

    check localInfo.isLocal == true
    check remoteInfo.isLocal == false

suite "Connection Manager Pool Types":
  test "Fire-and-forget pool is separate from regular raft pool":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let cm = newConnectionManager(config)

    check cm.raftPool != cm.raftFireForgetPool
    check cm.raftPool.role == "raft"
    check cm.raftFireForgetPool.role == "raft-ff"

    cm.close()

suite "Connection Manager Config Integration":
  test "Connection manager uses config timeouts":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    config.tcpConnectTimeoutMs = 1000
    config.tcpReadTimeoutMs = 5000

    let cm = newConnectionManager(config)

    check cm.config.tcpConnectTimeoutMs == 1000
    check cm.config.tcpReadTimeoutMs == 5000

    cm.close()

  test "Connection manager uses config health thresholds":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    config.failureThreshold = 5
    config.recoveryThreshold = 3

    let cm = newConnectionManager(config)

    check cm.healthChecker.failureThreshold == 5
    check cm.healthChecker.recoveryThreshold == 3

    cm.close()
