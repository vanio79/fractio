# Unit tests for fractio/protocol/messages/cluster.nim
# Tests JoinNode, RemoveNode, ListNodes, RebalanceStatus, DrainNode,
# CreateGroup, JoinGroup encoding/decoding

import std/unittest
import fractio/protocol/messages/cluster
import fractio/protocol/types
import fractio/protocol/codec

# =============================================================================
# JoinNode Tests
# =============================================================================

suite "JoinNodeRequest/JoinNodeResponse":

  test "encodeJoinNodeRequest basic":
    let req = JoinNodeRequest(
      nodeId: 1'u16,
      host: "localhost",
      raftPort: 9000'u16,
      clientPort: 9001'u16
    )
    let encoded = encodeJoinNodeRequest(req)
    check encoded.len > 2
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.isOk
    check mt.value == uint16(mtJoinNode)

  test "encodeJoinNodeRequest with IP address":
    let req = JoinNodeRequest(
      nodeId: 42'u16,
      host: "192.168.1.100",
      raftPort: 8000'u16,
      clientPort: 8001'u16
    )
    let encoded = encodeJoinNodeRequest(req)
    check encoded.len > 2

  test "encodeJoinNodeRequest max values":
    let req = JoinNodeRequest(
      nodeId: 65535'u16,
      host: "node.example.com",
      raftPort: 65535'u16,
      clientPort: 65535'u16
    )
    let encoded = encodeJoinNodeRequest(req)
    check encoded.len > 2

  test "decodeJoinNodeRequest roundtrip":
    let req = JoinNodeRequest(
      nodeId: 5'u16,
      host: "test-host",
      raftPort: 7000'u16,
      clientPort: 7001'u16
    )
    let encoded = encodeJoinNodeRequest(req)
    let decoded = decodeJoinNodeRequest(encoded)
    check decoded.isOk
    check decoded.value.nodeId == req.nodeId
    check decoded.value.host == req.host
    check decoded.value.raftPort == req.raftPort
    check decoded.value.clientPort == req.clientPort

  test "decodeJoinNodeRequest empty host":
    let req = JoinNodeRequest(
      nodeId: 1'u16,
      host: "",
      raftPort: 9000'u16,
      clientPort: 9001'u16
    )
    let encoded = encodeJoinNodeRequest(req)
    let decoded = decodeJoinNodeRequest(encoded)
    check decoded.isOk
    check decoded.value.host == ""

  test "decodeJoinNodeRequest truncated":
    let truncated = "\x07\x03"
    let decoded = decodeJoinNodeRequest(truncated)
    check decoded.isErr

  test "decodeJoinNodeRequest missing host length":
    let truncated = "\x07\x03\x00\x01" # MT + nodeId only
    let decoded = decodeJoinNodeRequest(truncated)
    check decoded.isErr

  test "encodeJoinNodeResponse success":
    let resp = JoinNodeResponse(success: true,
        message: "Node joined successfully")
    let encoded = encodeJoinNodeResponse(resp)
    check encoded.len > 2
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.isOk
    check mt.value == uint16(mtJoinNode)

  test "encodeJoinNodeResponse failure":
    let resp = JoinNodeResponse(success: false,
        message: "Node ID already exists")
    let encoded = encodeJoinNodeResponse(resp)
    check encoded.len > 2

  test "encodeJoinNodeResponse empty message":
    let resp = JoinNodeResponse(success: true, message: "")
    let encoded = encodeJoinNodeResponse(resp)
    check encoded.len == 4 # MT + success byte + empty message length

  test "decodeJoinNodeResponse roundtrip success":
    let resp = JoinNodeResponse(success: true, message: "Welcome to cluster")
    let encoded = encodeJoinNodeResponse(resp)
    let decoded = decodeJoinNodeResponse(encoded)
    check decoded.isOk
    check decoded.value.success == true
    check decoded.value.message == resp.message

  test "decodeJoinNodeResponse roundtrip failure":
    let resp = JoinNodeResponse(success: false, message: "Connection refused")
    let encoded = encodeJoinNodeResponse(resp)
    let decoded = decodeJoinNodeResponse(encoded)
    check decoded.isOk
    check decoded.value.success == false
    check decoded.value.message == resp.message

  test "decodeJoinNodeResponse truncated":
    let truncated = "\x07\x03"
    let decoded = decodeJoinNodeResponse(truncated)
    check decoded.isErr

# =============================================================================
# RemoveNode Tests
# =============================================================================

suite "RemoveNodeRequest/RemoveNodeResponse":

  test "encodeRemoveNodeRequest":
    let req = RemoveNodeRequest(nodeId: 5'u16)
    let encoded = encodeRemoveNodeRequest(req)
    check encoded.len == 4 # MT + nodeId
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.isOk
    check mt.value == uint16(mtRemoveNode)

  test "encodeRemoveNodeRequest max nodeId":
    let req = RemoveNodeRequest(nodeId: 65535'u16)
    let encoded = encodeRemoveNodeRequest(req)
    check encoded.len == 4

  test "decodeRemoveNodeRequest roundtrip":
    let req = RemoveNodeRequest(nodeId: 10'u16)
    let encoded = encodeRemoveNodeRequest(req)
    let decoded = decodeRemoveNodeRequest(encoded)
    check decoded.isOk
    check decoded.value.nodeId == req.nodeId

  test "decodeRemoveNodeRequest truncated":
    let truncated = "\x07\x04"
    let decoded = decodeRemoveNodeRequest(truncated)
    check decoded.isErr

  test "encodeRemoveNodeResponse success":
    let resp = RemoveNodeResponse(success: true, message: "Node removed")
    let encoded = encodeRemoveNodeResponse(resp)
    check encoded.len > 2

  test "encodeRemoveNodeResponse failure":
    let resp = RemoveNodeResponse(success: false, message: "Node not found")
    let encoded = encodeRemoveNodeResponse(resp)
    check encoded.len > 2

  test "decodeRemoveNodeResponse roundtrip":
    let resp = RemoveNodeResponse(success: true, message: "Goodbye")
    let encoded = encodeRemoveNodeResponse(resp)
    let decoded = decodeRemoveNodeResponse(encoded)
    check decoded.isOk
    check decoded.value.success == true
    check decoded.value.message == resp.message

  test "decodeRemoveNodeResponse failure roundtrip":
    let resp = RemoveNodeResponse(success: false,
        message: "Cannot remove leader")
    let encoded = encodeRemoveNodeResponse(resp)
    let decoded = decodeRemoveNodeResponse(encoded)
    check decoded.isOk
    check decoded.value.success == false
    check decoded.value.message == resp.message

  test "decodeRemoveNodeResponse truncated":
    let truncated = "\x07\x04"
    let decoded = decodeRemoveNodeResponse(truncated)
    check decoded.isErr

# =============================================================================
# ListNodes Tests
# =============================================================================

suite "ListNodesRequest/ListNodesResponse":

  test "encodeListNodesRequest":
    let encoded = encodeListNodesRequest()
    check encoded.len == 2 # Just MT
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.isOk
    check mt.value == uint16(mtListNodes)

  test "decodeListNodesRequest":
    let encoded = encodeListNodesRequest()
    let decoded = decodeListNodesRequest(encoded)
    check decoded.isOk

  test "decodeListNodesRequest truncated":
    let truncated = "\x07"
    let decoded = decodeListNodesRequest(truncated)
    check decoded.isErr

  test "encodeListNodesResponse empty":
    let resp = ListNodesResponse(nodes: @[])
    let encoded = encodeListNodesResponse(resp)
    check encoded.len == 4 # MT + count (0)

  test "encodeListNodesResponse single node":
    let resp = ListNodesResponse(nodes: @[
      NodeInfo(nodeId: 1'u16, host: "node1", raftPort: 9000'u16,
               clientPort: 9001'u16, status: NodeStatusActive)
    ])
    let encoded = encodeListNodesResponse(resp)
    check encoded.len > 4

  test "encodeListNodesResponse multiple nodes":
    let resp = ListNodesResponse(nodes: @[
      NodeInfo(nodeId: 1'u16, host: "leader", raftPort: 9000'u16,
               clientPort: 9001'u16, status: NodeStatusActive),
      NodeInfo(nodeId: 2'u16, host: "follower1", raftPort: 9000'u16,
               clientPort: 9001'u16, status: NodeStatusActive),
      NodeInfo(nodeId: 3'u16, host: "follower2", raftPort: 9000'u16,
               clientPort: 9001'u16, status: NodeStatusDraining)
    ])
    let encoded = encodeListNodesResponse(resp)
    check encoded.len > 4

  test "encodeListNodesResponse with different statuses":
    let resp = ListNodesResponse(nodes: @[
      NodeInfo(nodeId: 1'u16, host: "active", raftPort: 9000'u16,
               clientPort: 9001'u16, status: NodeStatusActive),
      NodeInfo(nodeId: 2'u16, host: "draining", raftPort: 9000'u16,
               clientPort: 9001'u16, status: NodeStatusDraining),
      NodeInfo(nodeId: 3'u16, host: "down", raftPort: 9000'u16,
               clientPort: 9001'u16, status: NodeStatusDown),
      NodeInfo(nodeId: 4'u16, host: "unknown", raftPort: 9000'u16,
               clientPort: 9001'u16, status: NodeStatusUnknown)
    ])
    let encoded = encodeListNodesResponse(resp)
    check encoded.len > 4

  test "decodeListNodesResponse empty":
    let resp = ListNodesResponse(nodes: @[])
    let encoded = encodeListNodesResponse(resp)
    let decoded = decodeListNodesResponse(encoded)
    check decoded.isOk
    check decoded.value.nodes.len == 0

  test "decodeListNodesResponse roundtrip single":
    let resp = ListNodesResponse(nodes: @[
      NodeInfo(nodeId: 42'u16, host: "test-node", raftPort: 8000'u16,
               clientPort: 8001'u16, status: NodeStatusActive)
    ])
    let encoded = encodeListNodesResponse(resp)
    let decoded = decodeListNodesResponse(encoded)
    check decoded.isOk
    check decoded.value.nodes.len == 1
    check decoded.value.nodes[0].nodeId == 42'u16
    check decoded.value.nodes[0].host == "test-node"
    check decoded.value.nodes[0].raftPort == 8000'u16
    check decoded.value.nodes[0].clientPort == 8001'u16
    check decoded.value.nodes[0].status == NodeStatusActive

  test "decodeListNodesResponse roundtrip multiple":
    let resp = ListNodesResponse(nodes: @[
      NodeInfo(nodeId: 1'u16, host: "node1", raftPort: 9000'u16,
               clientPort: 9001'u16, status: NodeStatusActive),
      NodeInfo(nodeId: 2'u16, host: "node2", raftPort: 9002'u16,
               clientPort: 9003'u16, status: NodeStatusDown)
    ])
    let encoded = encodeListNodesResponse(resp)
    let decoded = decodeListNodesResponse(encoded)
    check decoded.isOk
    check decoded.value.nodes.len == 2
    check decoded.value.nodes[0].nodeId == 1'u16
    check decoded.value.nodes[1].nodeId == 2'u16
    check decoded.value.nodes[1].status == NodeStatusDown

  test "decodeListNodesResponse truncated":
    let truncated = "\x07\x05"
    let decoded = decodeListNodesResponse(truncated)
    check decoded.isErr

  test "decodeListNodesResponse truncated node data":
    let truncated = "\x07\x05\x00\x01" # MT + count=1, but no node data
    let decoded = decodeListNodesResponse(truncated)
    check decoded.isErr

# =============================================================================
# RebalanceStatus Tests
# =============================================================================

suite "RebalanceStatusRequest/RebalanceStatusResponse":

  test "encodeRebalanceStatusRequest":
    let encoded = encodeRebalanceStatusRequest()
    check encoded.len == 2 # Just MT
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.isOk
    check mt.value == uint16(mtRebalanceStatus)

  test "decodeRebalanceStatusRequest":
    let encoded = encodeRebalanceStatusRequest()
    let decoded = decodeRebalanceStatusRequest(encoded)
    check decoded.isOk

  test "decodeRebalanceStatusRequest truncated":
    let truncated = "\x07"
    let decoded = decodeRebalanceStatusRequest(truncated)
    check decoded.isErr

  test "encodeRebalanceStatusResponse all zero":
    let resp = RebalanceStatusResponse(
      pending: 0'u32,
      inProgress: 0'u32,
      completed: 0'u32,
      failed: 0'u32
    )
    let encoded = encodeRebalanceStatusResponse(resp)
    check encoded.len == 18 # MT + 4 uint32s

  test "encodeRebalanceStatusResponse with values":
    let resp = RebalanceStatusResponse(
      pending: 5'u32,
      inProgress: 2'u32,
      completed: 100'u32,
      failed: 3'u32
    )
    let encoded = encodeRebalanceStatusResponse(resp)
    check encoded.len == 18

  test "encodeRebalanceStatusResponse large values":
    let resp = RebalanceStatusResponse(
      pending: 4294967295'u32,
      inProgress: 1000000'u32,
      completed: 999999'u32,
      failed: 0'u32
    )
    let encoded = encodeRebalanceStatusResponse(resp)
    check encoded.len == 18

  test "decodeRebalanceStatusResponse roundtrip":
    let resp = RebalanceStatusResponse(
      pending: 10'u32,
      inProgress: 5'u32,
      completed: 200'u32,
      failed: 1'u32
    )
    let encoded = encodeRebalanceStatusResponse(resp)
    let decoded = decodeRebalanceStatusResponse(encoded)
    check decoded.isOk
    check decoded.value.pending == resp.pending
    check decoded.value.inProgress == resp.inProgress
    check decoded.value.completed == resp.completed
    check decoded.value.failed == resp.failed

  test "decodeRebalanceStatusResponse zero values":
    let resp = RebalanceStatusResponse()
    let encoded = encodeRebalanceStatusResponse(resp)
    let decoded = decodeRebalanceStatusResponse(encoded)
    check decoded.isOk
    check decoded.value.pending == 0'u32
    check decoded.value.inProgress == 0'u32
    check decoded.value.completed == 0'u32
    check decoded.value.failed == 0'u32

  test "decodeRebalanceStatusResponse truncated":
    let truncated = "\x07\x06"
    let decoded = decodeRebalanceStatusResponse(truncated)
    check decoded.isErr

  test "decodeRebalanceStatusResponse partial data":
    let truncated = "\x07\x06\x00\x00\x00\x05" # MT + pending only
    let decoded = decodeRebalanceStatusResponse(truncated)
    check decoded.isErr

# =============================================================================
# DrainNode Tests
# =============================================================================

suite "DrainNodeRequest/DrainNodeResponse":

  test "encodeDrainNodeRequest":
    let req = DrainNodeRequest(nodeId: 3'u16)
    let encoded = encodeDrainNodeRequest(req)
    check encoded.len == 4 # MT + nodeId
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.isOk
    check mt.value == uint16(mtDrainNode)

  test "encodeDrainNodeRequest max nodeId":
    let req = DrainNodeRequest(nodeId: 65535'u16)
    let encoded = encodeDrainNodeRequest(req)
    check encoded.len == 4

  test "decodeDrainNodeRequest roundtrip":
    let req = DrainNodeRequest(nodeId: 7'u16)
    let encoded = encodeDrainNodeRequest(req)
    let decoded = decodeDrainNodeRequest(encoded)
    check decoded.isOk
    check decoded.value.nodeId == req.nodeId

  test "decodeDrainNodeRequest truncated":
    let truncated = "\x07\x07"
    let decoded = decodeDrainNodeRequest(truncated)
    check decoded.isErr

  test "encodeDrainNodeResponse success":
    let resp = DrainNodeResponse(success: true,
        message: "Node draining started")
    let encoded = encodeDrainNodeResponse(resp)
    check encoded.len > 2

  test "encodeDrainNodeResponse failure":
    let resp = DrainNodeResponse(success: false, message: "Node not found")
    let encoded = encodeDrainNodeResponse(resp)
    check encoded.len > 2

  test "decodeDrainNodeResponse roundtrip success":
    let resp = DrainNodeResponse(success: true, message: "Draining in progress")
    let encoded = encodeDrainNodeResponse(resp)
    let decoded = decodeDrainNodeResponse(encoded)
    check decoded.isOk
    check decoded.value.success == true
    check decoded.value.message == resp.message

  test "decodeDrainNodeResponse roundtrip failure":
    let resp = DrainNodeResponse(success: false, message: "Already draining")
    let encoded = encodeDrainNodeResponse(resp)
    let decoded = decodeDrainNodeResponse(encoded)
    check decoded.isOk
    check decoded.value.success == false
    check decoded.value.message == resp.message

  test "decodeDrainNodeResponse truncated":
    let truncated = "\x07\x07"
    let decoded = decodeDrainNodeResponse(truncated)
    check decoded.isErr

# =============================================================================
# CreateGroup Tests
# =============================================================================

suite "CreateGroupRequest/CreateGroupResponse":

  test "encodeCreateGroupRequest single member":
    let groupId = "0123456789abcdef" # 16 bytes
    let req = CreateGroupRequest(
      groupId: groupId,
      preferredLeaderId: 1'u16,
      members: @[
        CreateGroupMember(nodeId: 1'u16, host: "node1", raftPort: 9000'u16,
                          clientPort: 9001'u16)
      ]
    )
    let encoded = encodeCreateGroupRequest(req)
    check encoded.len > 2
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.isOk
    check mt.value == uint16(mtCreateGroup)

  test "encodeCreateGroupRequest multiple members":
    let groupId = "abcdefghijklmnop" # 16 bytes
    let req = CreateGroupRequest(
      groupId: groupId,
      preferredLeaderId: 2'u16,
      members: @[
        CreateGroupMember(nodeId: 1'u16, host: "node1", raftPort: 9000'u16,
                          clientPort: 9001'u16),
        CreateGroupMember(nodeId: 2'u16, host: "node2", raftPort: 9002'u16,
                          clientPort: 9003'u16),
        CreateGroupMember(nodeId: 3'u16, host: "node3", raftPort: 9004'u16,
                          clientPort: 9005'u16)
      ]
    )
    let encoded = encodeCreateGroupRequest(req)
    check encoded.len > 2

  test "encodeCreateGroupRequest empty members":
    let groupId = "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
    let req = CreateGroupRequest(
      groupId: groupId,
      preferredLeaderId: 1'u16,
      members: @[]
    )
    let encoded = encodeCreateGroupRequest(req)
    check encoded.len == 22 # MT + groupId(16) + preferredLeaderId(2) + memberCount(2)

  test "decodeCreateGroupRequest roundtrip single member":
    let groupId = "groupid12345678A" # exactly 16 chars
    let req = CreateGroupRequest(
      groupId: groupId,
      preferredLeaderId: 5'u16,
      members: @[
        CreateGroupMember(nodeId: 5'u16, host: "leader", raftPort: 8000'u16,
                          clientPort: 8001'u16)
      ]
    )
    let encoded = encodeCreateGroupRequest(req)
    let decoded = decodeCreateGroupRequest(encoded)
    check decoded.isOk
    check decoded.value.groupId == groupId
    check decoded.value.preferredLeaderId == 5'u16
    check decoded.value.members.len == 1
    check decoded.value.members[0].nodeId == 5'u16
    check decoded.value.members[0].host == "leader"

  test "decodeCreateGroupRequest roundtrip multiple members":
    let groupId = "testgroup123456A" # exactly 16 chars
    let req = CreateGroupRequest(
      groupId: groupId,
      preferredLeaderId: 1'u16,
      members: @[
        CreateGroupMember(nodeId: 1'u16, host: "node1", raftPort: 9000'u16,
                          clientPort: 9001'u16),
        CreateGroupMember(nodeId: 2'u16, host: "node2", raftPort: 9002'u16,
                          clientPort: 9003'u16)
      ]
    )
    let encoded = encodeCreateGroupRequest(req)
    let decoded = decodeCreateGroupRequest(encoded)
    check decoded.isOk
    check decoded.value.members.len == 2
    check decoded.value.members[0].nodeId == 1'u16
    check decoded.value.members[1].nodeId == 2'u16

  test "decodeCreateGroupRequest truncated no groupId":
    let truncated = "\x07\x0a" # MT only
    let decoded = decodeCreateGroupRequest(truncated)
    check decoded.isErr

  test "decodeCreateGroupRequest truncated partial groupId":
    let truncated = "\x07\x0a\x00\x00\x00\x00\x00\x00\x00\x00" # MT + 8 bytes
    let decoded = decodeCreateGroupRequest(truncated)
    check decoded.isErr

  test "encodeCreateGroupResponse success":
    let groupId = "successgroupid01" # exactly 16 chars
    let resp = CreateGroupResponse(success: true, groupId: groupId, error: "")
    let encoded = encodeCreateGroupResponse(resp)
    check encoded.len == 19 # MT + success(1) + groupId(16)

  test "encodeCreateGroupResponse failure":
    let resp = CreateGroupResponse(success: false, groupId: "",
        error: "Group creation failed")
    let encoded = encodeCreateGroupResponse(resp)
    check encoded.len > 3

  test "decodeCreateGroupResponse roundtrip success":
    let groupId = "roundtripgroup01" # exactly 16 chars
    let resp = CreateGroupResponse(success: true, groupId: groupId, error: "")
    let encoded = encodeCreateGroupResponse(resp)
    let decoded = decodeCreateGroupResponse(encoded)
    check decoded.isOk
    check decoded.value.success == true
    check decoded.value.groupId == groupId

  test "decodeCreateGroupResponse roundtrip failure":
    let resp = CreateGroupResponse(success: false, groupId: "",
        error: "No capacity")
    let encoded = encodeCreateGroupResponse(resp)
    let decoded = decodeCreateGroupResponse(encoded)
    check decoded.isOk
    check decoded.value.success == false
    check decoded.value.error == "No capacity"

  test "decodeCreateGroupResponse truncated":
    let truncated = "\x07\x0a\x01" # MT + success only
    let decoded = decodeCreateGroupResponse(truncated)
    check decoded.isErr

# =============================================================================
# JoinGroup Tests
# =============================================================================

suite "JoinGroupRequest/JoinGroupResponse":

  test "encodeJoinGroupRequest basic":
    let groupId = "joingroup123456A" # exactly 16 chars
    let req = JoinGroupRequest(
      groupId: groupId,
      creatorNodeId: 1'u16,
      creatorHost: "creator",
      creatorPort: 9000'u16,
      members: @[]
    )
    let encoded = encodeJoinGroupRequest(req)
    check encoded.len > 2
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.isOk
    check mt.value == uint16(mtJoinGroup)

  test "encodeJoinGroupRequest with members":
    let groupId = "joingroup789abcA" # exactly 16 chars
    let req = JoinGroupRequest(
      groupId: groupId,
      creatorNodeId: 1'u16,
      creatorHost: "creator",
      creatorPort: 9000'u16,
      members: @[
        CreateGroupMember(nodeId: 2'u16, host: "node2", raftPort: 9002'u16,
                          clientPort: 9003'u16),
        CreateGroupMember(nodeId: 3'u16, host: "node3", raftPort: 9004'u16,
                          clientPort: 9005'u16)
      ]
    )
    let encoded = encodeJoinGroupRequest(req)
    check encoded.len > 2

  test "decodeJoinGroupRequest roundtrip basic":
    let groupId = "testjoin123456AA" # exactly 16 chars
    let req = JoinGroupRequest(
      groupId: groupId,
      creatorNodeId: 5'u16,
      creatorHost: "host5",
      creatorPort: 8000'u16,
      members: @[]
    )
    let encoded = encodeJoinGroupRequest(req)
    let decoded = decodeJoinGroupRequest(encoded)
    check decoded.isOk
    check decoded.value.groupId == groupId
    check decoded.value.creatorNodeId == 5'u16
    check decoded.value.creatorHost == "host5"
    check decoded.value.creatorPort == 8000'u16

  test "decodeJoinGroupRequest roundtrip with members":
    let groupId = "withmembers1234A" # exactly 16 chars
    let req = JoinGroupRequest(
      groupId: groupId,
      creatorNodeId: 1'u16,
      creatorHost: "creator",
      creatorPort: 9000'u16,
      members: @[
        CreateGroupMember(nodeId: 2'u16, host: "member2", raftPort: 9002'u16,
                          clientPort: 9003'u16)
      ]
    )
    let encoded = encodeJoinGroupRequest(req)
    let decoded = decodeJoinGroupRequest(encoded)
    check decoded.isOk
    check decoded.value.members.len == 1
    check decoded.value.members[0].nodeId == 2'u16
    check decoded.value.members[0].host == "member2"

  test "decodeJoinGroupRequest truncated no groupId":
    let truncated = "\x07\x0b" # MT only
    let decoded = decodeJoinGroupRequest(truncated)
    check decoded.isErr

  test "decodeJoinGroupRequest truncated partial groupId":
    let truncated = "\x07\x0b\x00\x00\x00\x00\x00\x00" # MT + 6 bytes
    let decoded = decodeJoinGroupRequest(truncated)
    check decoded.isErr

  test "encodeJoinGroupResponse success":
    let groupId = "joinsuccess1234A" # exactly 16 chars
    let resp = JoinGroupResponse(success: true, groupId: groupId, error: "")
    let encoded = encodeJoinGroupResponse(resp)
    check encoded.len == 19 # MT + success(1) + groupId(16)

  test "encodeJoinGroupResponse failure":
    let resp = JoinGroupResponse(success: false, groupId: "",
        error: "Join rejected")
    let encoded = encodeJoinGroupResponse(resp)
    check encoded.len > 3

  test "decodeJoinGroupResponse roundtrip success":
    let groupId = "joinedgroup1234A" # exactly 16 chars
    let resp = JoinGroupResponse(success: true, groupId: groupId, error: "")
    let encoded = encodeJoinGroupResponse(resp)
    let decoded = decodeJoinGroupResponse(encoded)
    check decoded.isOk
    check decoded.value.success == true
    check decoded.value.groupId == groupId

  test "decodeJoinGroupResponse roundtrip failure":
    let resp = JoinGroupResponse(success: false, groupId: "",
        error: "Group not found")
    let encoded = encodeJoinGroupResponse(resp)
    let decoded = decodeJoinGroupResponse(encoded)
    check decoded.isOk
    check decoded.value.success == false
    check decoded.value.error == "Group not found"

  test "decodeJoinGroupResponse truncated":
    let truncated = "\x07\x0b\x01" # MT + success only
    let decoded = decodeJoinGroupResponse(truncated)
    check decoded.isErr

# =============================================================================
# NodeStatus Constants Tests
# =============================================================================

suite "NodeStatus Constants":

  test "NodeStatusUnknown value":
    check NodeStatusUnknown == 0x00'u8

  test "NodeStatusActive value":
    check NodeStatusActive == 0x01'u8

  test "NodeStatusDraining value":
    check NodeStatusDraining == 0x02'u8

  test "NodeStatusDown value":
    check NodeStatusDown == 0x03'u8

# =============================================================================
# Integration Tests
# =============================================================================

suite "Cluster Message Integration":

  test "JoinNode full roundtrip":
    let req = JoinNodeRequest(
      nodeId: 100'u16,
      host: "new-node.cluster.local",
      raftPort: 10000'u16,
      clientPort: 10001'u16
    )
    let reqEncoded = encodeJoinNodeRequest(req)
    let reqDecoded = decodeJoinNodeRequest(reqEncoded)
    check reqDecoded.isOk
    check reqDecoded.value.nodeId == 100'u16

    let resp = JoinNodeResponse(success: true, message: "Node 100 joined")
    let respEncoded = encodeJoinNodeResponse(resp)
    let respDecoded = decodeJoinNodeResponse(respEncoded)
    check respDecoded.isOk
    check respDecoded.value.success == true

  test "RemoveNode full roundtrip":
    let req = RemoveNodeRequest(nodeId: 50'u16)
    let reqEncoded = encodeRemoveNodeRequest(req)
    let reqDecoded = decodeRemoveNodeRequest(reqEncoded)
    check reqDecoded.isOk
    check reqDecoded.value.nodeId == 50'u16

    let resp = RemoveNodeResponse(success: true, message: "Node 50 removed")
    let respEncoded = encodeRemoveNodeResponse(resp)
    let respDecoded = decodeRemoveNodeResponse(respEncoded)
    check respDecoded.isOk

  test "ListNodes full roundtrip":
    let reqEncoded = encodeListNodesRequest()
    let reqDecoded = decodeListNodesRequest(reqEncoded)
    check reqDecoded.isOk

    let resp = ListNodesResponse(nodes: @[
      NodeInfo(nodeId: 1'u16, host: "prod-1", raftPort: 9000'u16,
               clientPort: 9001'u16, status: NodeStatusActive),
      NodeInfo(nodeId: 2'u16, host: "prod-2", raftPort: 9000'u16,
               clientPort: 9001'u16, status: NodeStatusActive),
      NodeInfo(nodeId: 3'u16, host: "prod-3", raftPort: 9000'u16,
               clientPort: 9001'u16, status: NodeStatusDown)
    ])
    let respEncoded = encodeListNodesResponse(resp)
    let respDecoded = decodeListNodesResponse(respEncoded)
    check respDecoded.isOk
    check respDecoded.value.nodes.len == 3

  test "RebalanceStatus full roundtrip":
    let reqEncoded = encodeRebalanceStatusRequest()
    let reqDecoded = decodeRebalanceStatusRequest(reqEncoded)
    check reqDecoded.isOk

    let resp = RebalanceStatusResponse(
      pending: 3'u32,
      inProgress: 1'u32,
      completed: 50'u32,
      failed: 0'u32
    )
    let respEncoded = encodeRebalanceStatusResponse(resp)
    let respDecoded = decodeRebalanceStatusResponse(respEncoded)
    check respDecoded.isOk
    check respDecoded.value.pending == 3'u32

  test "CreateGroup full roundtrip":
    let groupId = "productiongrp01A" # exactly 16 chars
    let req = CreateGroupRequest(
      groupId: groupId,
      preferredLeaderId: 1'u16,
      members: @[
        CreateGroupMember(nodeId: 1'u16, host: "srv1", raftPort: 9000'u16,
                          clientPort: 9001'u16),
        CreateGroupMember(nodeId: 2'u16, host: "srv2", raftPort: 9000'u16,
                          clientPort: 9001'u16)
      ]
    )
    let reqEncoded = encodeCreateGroupRequest(req)
    let reqDecoded = decodeCreateGroupRequest(reqEncoded)
    check reqDecoded.isOk
    check reqDecoded.value.groupId == groupId

    let resp = CreateGroupResponse(success: true, groupId: groupId, error: "")
    let respEncoded = encodeCreateGroupResponse(resp)
    let respDecoded = decodeCreateGroupResponse(respEncoded)
    check respDecoded.isOk
    check respDecoded.value.success == true
