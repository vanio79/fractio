# Integration tests for Phase 8 Cluster Admin Protocol.
#
# Covers:
#   - messages/cluster: codec round-trips for JoinNode, RemoveNode, ListNodes,
#     RebalanceStatus
#   - server/client: end-to-end cluster admin commands over TCP
#   - JoinNode: add nodes, duplicate join replaces entry, zero nodeId rejected
#   - RemoveNode: remove existing node, remove non-existent returns failure
#   - ListNodes: empty list, single node, multiple nodes
#   - RebalanceStatus: returns zero counters by default
#
# Port allocation: 20450–20499

import std/[unittest, os, atomics]
import fractio/protocol/types
import fractio/protocol/server
import fractio/protocol/client
import fractio/protocol/messages/cluster as clusterMsgs

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc startClusterServer(port: int): ProtocolServer =
  var cfg = defaultServerConfig()
  cfg.host = "127.0.0.1"
  cfg.port = port
  cfg.idleTimeoutSecs = 120
  cfg.serverName = "fractio-cluster-test"
  result = newProtocolServer(cfg)
  result.start()
  sleep(60)

proc connectCluster(port: int): ProtocolClient =
  var cfg = defaultClientConfig("127.0.0.1", port)
  cfg.timeoutMs = 10_000
  result = newProtocolClient(cfg)
  let r = result.connect()
  doAssert r.isOk, "connect failed: " & $r.err

proc withClusterServer(port: int, body: proc(srv: ProtocolServer,
    cli: ProtocolClient)) =
  let srv = startClusterServer(port)
  let cli = connectCluster(port)
  try:
    body(srv, cli)
  finally:
    cli.disconnect()
    srv.stop()
    sleep(50)

# ---------------------------------------------------------------------------
# Suite: cluster codec — JoinNode round-trips
# ---------------------------------------------------------------------------

suite "cluster codec - JoinNode":
  test "encode and decode JoinNodeRequest":
    let req = clusterMsgs.JoinNodeRequest(
      nodeId: 3,
      host: "10.0.0.3",
      raftPort: 8300,
      clientPort: 9000,
    )
    let payload = clusterMsgs.encodeJoinNodeRequest(req)
    let r = clusterMsgs.decodeJoinNodeRequest(payload)
    check r.isOk
    check r.value.nodeId == 3
    check r.value.host == "10.0.0.3"
    check r.value.raftPort == 8300
    check r.value.clientPort == 9000

  test "encode and decode JoinNodeResponse - success":
    let resp = clusterMsgs.JoinNodeResponse(success: true,
        message: "node 3 joined")
    let payload = clusterMsgs.encodeJoinNodeResponse(resp)
    let r = clusterMsgs.decodeJoinNodeResponse(payload)
    check r.isOk
    check r.value.success == true
    check r.value.message == "node 3 joined"

  test "encode and decode JoinNodeResponse - failure":
    let resp = clusterMsgs.JoinNodeResponse(success: false,
        message: "nodeId 0 is reserved")
    let payload = clusterMsgs.encodeJoinNodeResponse(resp)
    let r = clusterMsgs.decodeJoinNodeResponse(payload)
    check r.isOk
    check r.value.success == false
    check r.value.message == "nodeId 0 is reserved"

# ---------------------------------------------------------------------------
# Suite: cluster codec — RemoveNode round-trips
# ---------------------------------------------------------------------------

suite "cluster codec - RemoveNode":
  test "encode and decode RemoveNodeRequest":
    let req = clusterMsgs.RemoveNodeRequest(nodeId: 5)
    let payload = clusterMsgs.encodeRemoveNodeRequest(req)
    let r = clusterMsgs.decodeRemoveNodeRequest(payload)
    check r.isOk
    check r.value.nodeId == 5

  test "encode and decode RemoveNodeResponse - success":
    let resp = clusterMsgs.RemoveNodeResponse(success: true,
        message: "node 5 removed")
    let payload = clusterMsgs.encodeRemoveNodeResponse(resp)
    let r = clusterMsgs.decodeRemoveNodeResponse(payload)
    check r.isOk
    check r.value.success == true
    check r.value.message == "node 5 removed"

  test "encode and decode RemoveNodeResponse - not found":
    let resp = clusterMsgs.RemoveNodeResponse(success: false,
        message: "node 99 not found")
    let payload = clusterMsgs.encodeRemoveNodeResponse(resp)
    let r = clusterMsgs.decodeRemoveNodeResponse(payload)
    check r.isOk
    check r.value.success == false
    check r.value.message == "node 99 not found"

# ---------------------------------------------------------------------------
# Suite: cluster codec — ListNodes round-trips
# ---------------------------------------------------------------------------

suite "cluster codec - ListNodes":
  test "encode and decode ListNodesRequest":
    let payload = clusterMsgs.encodeListNodesRequest()
    let r = clusterMsgs.decodeListNodesRequest(payload)
    check r.isOk

  test "encode and decode ListNodesResponse - empty":
    let resp = clusterMsgs.ListNodesResponse(nodes: @[])
    let payload = clusterMsgs.encodeListNodesResponse(resp)
    let r = clusterMsgs.decodeListNodesResponse(payload)
    check r.isOk
    check r.value.nodes.len == 0

  test "encode and decode ListNodesResponse - multiple nodes":
    let resp = clusterMsgs.ListNodesResponse(nodes: @[
      clusterMsgs.NodeInfo(nodeId: 1, host: "10.0.0.1", raftPort: 8300,
                           clientPort: 9000,
                           status: clusterMsgs.NodeStatusActive),
      clusterMsgs.NodeInfo(nodeId: 2, host: "10.0.0.2", raftPort: 8301,
                           clientPort: 9001,
                           status: clusterMsgs.NodeStatusDraining),
    ])
    let payload = clusterMsgs.encodeListNodesResponse(resp)
    let r = clusterMsgs.decodeListNodesResponse(payload)
    check r.isOk
    check r.value.nodes.len == 2
    check r.value.nodes[0].nodeId == 1
    check r.value.nodes[0].host == "10.0.0.1"
    check r.value.nodes[0].raftPort == 8300
    check r.value.nodes[0].clientPort == 9000
    check r.value.nodes[0].status == clusterMsgs.NodeStatusActive
    check r.value.nodes[1].nodeId == 2
    check r.value.nodes[1].host == "10.0.0.2"
    check r.value.nodes[1].status == clusterMsgs.NodeStatusDraining

# ---------------------------------------------------------------------------
# Suite: cluster codec — RebalanceStatus round-trips
# ---------------------------------------------------------------------------

suite "cluster codec - RebalanceStatus":
  test "encode and decode RebalanceStatusRequest":
    let payload = clusterMsgs.encodeRebalanceStatusRequest()
    let r = clusterMsgs.decodeRebalanceStatusRequest(payload)
    check r.isOk

  test "encode and decode RebalanceStatusResponse":
    let resp = clusterMsgs.RebalanceStatusResponse(
      pending: 3, inProgress: 1, completed: 42, failed: 0)
    let payload = clusterMsgs.encodeRebalanceStatusResponse(resp)
    let r = clusterMsgs.decodeRebalanceStatusResponse(payload)
    check r.isOk
    check r.value.pending == 3
    check r.value.inProgress == 1
    check r.value.completed == 42
    check r.value.failed == 0

  test "encode and decode RebalanceStatusResponse - all zeros":
    let resp = clusterMsgs.RebalanceStatusResponse(
      pending: 0, inProgress: 0, completed: 0, failed: 0)
    let payload = clusterMsgs.encodeRebalanceStatusResponse(resp)
    let r = clusterMsgs.decodeRebalanceStatusResponse(payload)
    check r.isOk
    check r.value.pending == 0
    check r.value.inProgress == 0

# ---------------------------------------------------------------------------
# Suite: end-to-end JoinNode
# ---------------------------------------------------------------------------

suite "cluster e2e - JoinNode":
  test "join a new node succeeds":
    withClusterServer(20450) do (srv: ProtocolServer, cli: ProtocolClient):
      let r = cli.joinNode(1, "10.0.0.1", 8300, 9000)
      check r.isOk
      check r.value.success == true
      check r.value.message.len > 0

  test "join with nodeId 0 is rejected":
    withClusterServer(20451) do (srv: ProtocolServer, cli: ProtocolClient):
      let r = cli.joinNode(0, "10.0.0.1", 8300, 9000)
      check r.isOk
      check r.value.success == false

  test "join with empty host is rejected":
    withClusterServer(20452) do (srv: ProtocolServer, cli: ProtocolClient):
      let r = cli.joinNode(1, "", 8300, 9000)
      check r.isOk
      check r.value.success == false

  test "duplicate join replaces existing node entry":
    withClusterServer(20453) do (srv: ProtocolServer, cli: ProtocolClient):
      let r1 = cli.joinNode(1, "10.0.0.1", 8300, 9000)
      check r1.isOk
      check r1.value.success == true
      # Re-join with different host — should succeed (upsert semantics)
      let r2 = cli.joinNode(1, "10.0.0.99", 8300, 9000)
      check r2.isOk
      check r2.value.success == true
      # Verify the new host is visible
      let lr = cli.listNodes()
      check lr.isOk
      check lr.value.nodes.len == 1
      check lr.value.nodes[0].host == "10.0.0.99"

  test "join multiple distinct nodes":
    withClusterServer(20454) do (srv: ProtocolServer, cli: ProtocolClient):
      let r1 = cli.joinNode(1, "10.0.0.1", 8300, 9000)
      let r2 = cli.joinNode(2, "10.0.0.2", 8301, 9001)
      let r3 = cli.joinNode(3, "10.0.0.3", 8302, 9002)
      check r1.isOk and r1.value.success
      check r2.isOk and r2.value.success
      check r3.isOk and r3.value.success
      let lr = cli.listNodes()
      check lr.isOk
      check lr.value.nodes.len == 3

# ---------------------------------------------------------------------------
# Suite: end-to-end RemoveNode
# ---------------------------------------------------------------------------

suite "cluster e2e - RemoveNode":
  test "remove an existing node succeeds":
    withClusterServer(20455) do (srv: ProtocolServer, cli: ProtocolClient):
      discard cli.joinNode(1, "10.0.0.1", 8300, 9000)
      let r = cli.removeNode(1)
      check r.isOk
      check r.value.success == true

  test "remove a non-existent node returns failure":
    withClusterServer(20456) do (srv: ProtocolServer, cli: ProtocolClient):
      let r = cli.removeNode(99)
      check r.isOk
      check r.value.success == false

  test "remove then re-join a node":
    withClusterServer(20457) do (srv: ProtocolServer, cli: ProtocolClient):
      discard cli.joinNode(2, "10.0.0.2", 8301, 9001)
      let rm = cli.removeNode(2)
      check rm.isOk and rm.value.success
      # After removal the node list should be empty
      let lr1 = cli.listNodes()
      check lr1.isOk
      check lr1.value.nodes.len == 0
      # Re-join should succeed
      let rj = cli.joinNode(2, "10.0.0.2", 8301, 9001)
      check rj.isOk and rj.value.success

# ---------------------------------------------------------------------------
# Suite: end-to-end ListNodes
# ---------------------------------------------------------------------------

suite "cluster e2e - ListNodes":
  test "list on empty registry returns empty list":
    withClusterServer(20458) do (srv: ProtocolServer, cli: ProtocolClient):
      let r = cli.listNodes()
      check r.isOk
      check r.value.nodes.len == 0

  test "list after joining two nodes returns two entries":
    withClusterServer(20459) do (srv: ProtocolServer, cli: ProtocolClient):
      discard cli.joinNode(1, "10.0.0.1", 8300, 9000)
      discard cli.joinNode(2, "10.0.0.2", 8301, 9001)
      let r = cli.listNodes()
      check r.isOk
      check r.value.nodes.len == 2

  test "joined nodes have NodeStatusActive":
    withClusterServer(20460) do (srv: ProtocolServer, cli: ProtocolClient):
      discard cli.joinNode(5, "host5", 8304, 9004)
      let r = cli.listNodes()
      check r.isOk
      check r.value.nodes.len == 1
      check r.value.nodes[0].status == clusterMsgs.NodeStatusActive
      check r.value.nodes[0].nodeId == 5
      check r.value.nodes[0].host == "host5"
      check r.value.nodes[0].raftPort == 8304
      check r.value.nodes[0].clientPort == 9004

  test "list after remove shows correct count":
    withClusterServer(20461) do (srv: ProtocolServer, cli: ProtocolClient):
      discard cli.joinNode(1, "10.0.0.1", 8300, 9000)
      discard cli.joinNode(2, "10.0.0.2", 8301, 9001)
      discard cli.joinNode(3, "10.0.0.3", 8302, 9002)
      discard cli.removeNode(2)
      let r = cli.listNodes()
      check r.isOk
      check r.value.nodes.len == 2

# ---------------------------------------------------------------------------
# Suite: end-to-end RebalanceStatus
# ---------------------------------------------------------------------------

suite "cluster e2e - RebalanceStatus":
  test "rebalance status returns all zeros on fresh server":
    withClusterServer(20462) do (srv: ProtocolServer, cli: ProtocolClient):
      let r = cli.rebalanceStatus()
      check r.isOk
      check r.value.pending == 0
      check r.value.inProgress == 0
      check r.value.completed == 0
      check r.value.failed == 0

  test "rebalance counters reflect manual increments":
    withClusterServer(20463) do (srv: ProtocolServer, cli: ProtocolClient):
      # Directly increment the server-side atomics
      discard srv.nodeRegistry.rebalancePending.fetchAdd(2)
      discard srv.nodeRegistry.rebalanceInProgress.fetchAdd(1)
      discard srv.nodeRegistry.rebalanceCompleted.fetchAdd(5)
      discard srv.nodeRegistry.rebalanceFailed.fetchAdd(1)
      let r = cli.rebalanceStatus()
      check r.isOk
      check r.value.pending == 2
      check r.value.inProgress == 1
      check r.value.completed == 5
      check r.value.failed == 1

# ---------------------------------------------------------------------------
# Suite: multiple clients share the same node registry
# ---------------------------------------------------------------------------

suite "cluster e2e - shared registry":
  test "two clients see the same node list":
    withClusterServer(20464) do (srv: ProtocolServer, cli: ProtocolClient):
      discard cli.joinNode(1, "10.0.0.1", 8300, 9000)
      # Second client connects and reads
      let cli2 = connectCluster(20464)
      defer: cli2.disconnect()
      let r = cli2.listNodes()
      check r.isOk
      check r.value.nodes.len == 1
      check r.value.nodes[0].nodeId == 1

  test "node removal by one client is visible to another":
    withClusterServer(20465) do (srv: ProtocolServer, cli: ProtocolClient):
      discard cli.joinNode(7, "10.0.0.7", 8306, 9006)
      let cli2 = connectCluster(20465)
      defer: cli2.disconnect()
      discard cli.removeNode(7)
      let r = cli2.listNodes()
      check r.isOk
      check r.value.nodes.len == 0
