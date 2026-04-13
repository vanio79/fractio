# Unit tests for fractio/protocol/messages/cluster.nim
# Tests JoinNode, RemoveNode, ListNodes, RebalanceStatus, DrainNode, CreateGroup, JoinGroup encoding/decoding

import std/unittest
import fractio/protocol/messages/cluster
import fractio/protocol/types
import fractio/protocol/codec
import fractio/core/types

suite "JoinNode Messages":

  test "encodeJoinNodeRequest":
    let req = JoinNodeRequest(
      nodeId: 1'u16,
      host: "localhost",
      raftPort: 7001'u16,
      clientPort: 8001'u16
    )
    let encoded = encodeJoinNodeRequest(req)
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.value == uint16(mtJoinNode)
    let nodeId = readUint16BE(encoded, pos)
    check nodeId.value == 1'u16

  test "encodeJoinNodeRequest different ports":
    let req = JoinNodeRequest(
      nodeId: 10'u16,
      host: "node10.example.com",
      raftPort: 9999'u16,
      clientPort: 8888'u16
    )
    let encoded = encodeJoinNodeRequest(req)
    let decoded = decodeJoinNodeRequest(encoded)
    check decoded.isOk
    check decoded.value.nodeId == 10'u16
    check decoded.value.host == "node10.example.com"
    check decoded.value.raftPort == 9999'u16
    check decoded.value.clientPort == 8888'u16

  test "encodeJoinNodeRequest empty host":
    let req = JoinNodeRequest(
      nodeId: 1'u16,
      host: "",
      raftPort: 7001'u16,
      clientPort: 8001'u16
    )
    let encoded = encodeJoinNodeRequest(req)
    let decoded = decodeJoinNodeRequest(encoded)
    check decoded.isOk
    check decoded.value.host == ""

  test "decodeJoinNodeRequest valid":
    let req = JoinNodeRequest(
      nodeId: 5'u16,
      host: "192.168.1.5",
      raftPort: 7005'u16,
      clientPort: 8005'u16
    )
    let encoded = encodeJoinNodeRequest(req)
    let decoded = decodeJoinNodeRequest(encoded)
    check decoded.isOk
    check decoded.value.nodeId == 5'u16
    check decoded.value.host == "192.168.1.5"
    check decoded.value.raftPort == 7005'u16
    check decoded.value.clientPort == 8005'u16

  test "decodeJoinNodeRequest truncated nodeId":
    let invalid = "\x07\x03" # Just message type
    let decoded = decodeJoinNodeRequest(invalid)
    check decoded.isErr

  test "decodeJoinNodeRequest truncated host":
    let invalid = "\x07\x03\x00\x01" # MT + nodeId, no host
    let decoded = decodeJoinNodeRequest(invalid)
    check decoded.isErr

  test "encodeJoinNodeResponse success":
    let resp = JoinNodeResponse(success: true,
        message: "Node joined successfully")
    let encoded = encodeJoinNodeResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let success = readUint8(encoded, pos)
    check success.value == 0x01'u8

  test "encodeJoinNodeResponse failure":
    let resp = JoinNodeResponse(success: false,
        message: "Node ID already exists")
    let encoded = encodeJoinNodeResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let success = readUint8(encoded, pos)
    check success.value == 0x00'u8

  test "encodeJoinNodeResponse empty message":
    let resp = JoinNodeResponse(success: true, message: "")
    let encoded = encodeJoinNodeResponse(resp)
    let decoded = decodeJoinNodeResponse(encoded)
    check decoded.isOk
    check decoded.value.message == ""

  test "decodeJoinNodeResponse success":
    let resp = JoinNodeResponse(success: true, message: "Welcome to cluster")
    let encoded = encodeJoinNodeResponse(resp)
    let decoded = decodeJoinNodeResponse(encoded)
    check decoded.isOk
    check decoded.value.success == true
    check decoded.value.message == "Welcome to cluster"

  test "decodeJoinNodeResponse failure":
    let resp = JoinNodeResponse(success: false, message: "Connection failed")
    let encoded = encodeJoinNodeResponse(resp)
    let decoded = decodeJoinNodeResponse(encoded)
    check decoded.isOk
    check decoded.value.success == false
    check decoded.value.message == "Connection failed"

  test "decodeJoinNodeResponse truncated":
    let invalid = "\x07\x03" # Just message type
    let decoded = decodeJoinNodeResponse(invalid)
    check decoded.isErr

  test "JoinNode roundtrip":
    let req = JoinNodeRequest(nodeId: 1'u16, host: "test", raftPort: 7001'u16,
        clientPort: 8001'u16)
    let encoded = encodeJoinNodeRequest(req)
    let decoded = decodeJoinNodeRequest(encoded)
    check decoded.isOk
    check decoded.value.nodeId == req.nodeId

suite "RemoveNode Messages":

  test "encodeRemoveNodeRequest":
    let req = RemoveNodeRequest(nodeId: 5'u16)
    let encoded = encodeRemoveNodeRequest(req)
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.value == uint16(mtRemoveNode)
    let nodeId = readUint16BE(encoded, pos)
    check nodeId.value == 5'u16

  test "decodeRemoveNodeRequest valid":
    let req = RemoveNodeRequest(nodeId: 10'u16)
    let encoded = encodeRemoveNodeRequest(req)
    let decoded = decodeRemoveNodeRequest(encoded)
    check decoded.isOk
    check decoded.value.nodeId == 10'u16

  test "decodeRemoveNodeRequest truncated":
    let invalid = "\x07\x04" # Just message type
    let decoded = decodeRemoveNodeRequest(invalid)
    check decoded.isErr

  test "encodeRemoveNodeResponse success":
    let resp = RemoveNodeResponse(success: true, message: "Node removed")
    let encoded = encodeRemoveNodeResponse(resp)
    let decoded = decodeRemoveNodeResponse(encoded)
    check decoded.isOk
    check decoded.value.success == true

  test "encodeRemoveNodeResponse failure":
    let resp = RemoveNodeResponse(success: false, message: "Node not found")
    let encoded = encodeRemoveNodeResponse(resp)
    let decoded = decodeRemoveNodeResponse(encoded)
    check decoded.isOk
    check decoded.value.success == false
    check decoded.value.message == "Node not found"

  test "decodeRemoveNodeResponse truncated":
    let invalid = "\x07\x04" # Just message type
    let decoded = decodeRemoveNodeResponse(invalid)
    check decoded.isErr

suite "ListNodes Messages":

  test "encodeListNodesRequest":
    let encoded = encodeListNodesRequest()
    check encoded.len == 2 # Just message type
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.value == uint16(mtListNodes)

  test "decodeListNodesRequest valid":
    let encoded = encodeListNodesRequest()
    let decoded = decodeListNodesRequest(encoded)
    check decoded.isOk

  test "decodeListNodesRequest truncated":
    let invalid = "" # Empty
    let decoded = decodeListNodesRequest(invalid)
    check decoded.isErr

  test "encodeListNodesResponse empty":
    let resp = ListNodesResponse(nodes: @[])
    let encoded = encodeListNodesResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let count = readUint16BE(encoded, pos)
    check count.value == 0'u16

  test "encodeListNodesResponse single node":
    let resp = ListNodesResponse(
      nodes: @[cluster.NodeInfo(
        nodeId: 1'u16,
        host: "node1",
        raftPort: 7001'u16,
        clientPort: 8001'u16,
        status: NodeStatusActive
      )]
    )
    let encoded = encodeListNodesResponse(resp)
    let decoded = decodeListNodesResponse(encoded)
    check decoded.isOk
    check decoded.value.nodes.len == 1
    check decoded.value.nodes[0].nodeId == 1'u16
    check decoded.value.nodes[0].status == NodeStatusActive

  test "encodeListNodesResponse multiple nodes":
    let resp = ListNodesResponse(
      nodes: @[
        cluster.NodeInfo(nodeId: 1'u16, host: "n1", raftPort: 7001'u16,
            clientPort: 8001'u16, status: NodeStatusActive),
        cluster.NodeInfo(nodeId: 2'u16, host: "n2", raftPort: 7002'u16,
            clientPort: 8002'u16, status: NodeStatusDraining),
        cluster.NodeInfo(nodeId: 3'u16, host: "n3", raftPort: 7003'u16,
            clientPort: 8003'u16, status: NodeStatusDown)
      ]
    )
    let encoded = encodeListNodesResponse(resp)
    let decoded = decodeListNodesResponse(encoded)
    check decoded.isOk
    check decoded.value.nodes.len == 3
    check decoded.value.nodes[0].status == NodeStatusActive
    check decoded.value.nodes[1].status == NodeStatusDraining
    check decoded.value.nodes[2].status == NodeStatusDown

  test "encodeListNodesResponse various statuses":
    for status in [NodeStatusUnknown, NodeStatusActive, NodeStatusDraining,
        NodeStatusDown]:
      let resp = ListNodesResponse(
        nodes: @[cluster.NodeInfo(nodeId: 1'u16, host: "n", raftPort: 7000'u16,
            clientPort: 8000'u16, status: status)]
      )
      let encoded = encodeListNodesResponse(resp)
      let decoded = decodeListNodesResponse(encoded)
      check decoded.isOk
      check decoded.value.nodes[0].status == status

  test "decodeListNodesResponse truncated count":
    let invalid = "\x07\x05" # Just message type
    let decoded = decodeListNodesResponse(invalid)
    check decoded.isErr

  test "decodeListNodesResponse truncated node":
    let invalid = "\x07\x05\x00\x01" # MT + count=1, no node data
    let decoded = decodeListNodesResponse(invalid)
    check decoded.isErr

suite "RebalanceStatus Messages":

  test "encodeRebalanceStatusRequest":
    let encoded = encodeRebalanceStatusRequest()
    check encoded.len == 2
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.value == uint16(mtRebalanceStatus)

  test "decodeRebalanceStatusRequest valid":
    let encoded = encodeRebalanceStatusRequest()
    let decoded = decodeRebalanceStatusRequest(encoded)
    check decoded.isOk

  test "decodeRebalanceStatusRequest truncated":
    let invalid = "" # Empty
    let decoded = decodeRebalanceStatusRequest(invalid)
    check decoded.isErr

  test "encodeRebalanceStatusResponse":
    let resp = RebalanceStatusResponse(
      pending: 5'u32,
      inProgress: 2'u32,
      completed: 100'u32,
      failed: 3'u32
    )
    let encoded = encodeRebalanceStatusResponse(resp)
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.value == uint16(mtRebalanceStatus)

  test "encodeRebalanceStatusResponse zero values":
    let resp = RebalanceStatusResponse()
    let encoded = encodeRebalanceStatusResponse(resp)
    let decoded = decodeRebalanceStatusResponse(encoded)
    check decoded.isOk
    check decoded.value.pending == 0'u32

  test "encodeRebalanceStatusResponse max values":
    let resp = RebalanceStatusResponse(
      pending: 0xFFFFFFFF'u32,
      inProgress: 0xFFFFFFFF'u32,
      completed: 0xFFFFFFFF'u32,
      failed: 0xFFFFFFFF'u32
    )
    let encoded = encodeRebalanceStatusResponse(resp)
    let decoded = decodeRebalanceStatusResponse(encoded)
    check decoded.isOk
    check decoded.value.pending == 0xFFFFFFFF'u32

  test "decodeRebalanceStatusResponse valid":
    let resp = RebalanceStatusResponse(
      pending: 10'u32,
      inProgress: 5'u32,
      completed: 200'u32,
      failed: 10'u32
    )
    let encoded = encodeRebalanceStatusResponse(resp)
    let decoded = decodeRebalanceStatusResponse(encoded)
    check decoded.isOk
    check decoded.value.pending == 10'u32
    check decoded.value.inProgress == 5'u32
    check decoded.value.completed == 200'u32
    check decoded.value.failed == 10'u32

  test "decodeRebalanceStatusResponse truncated":
    let invalid = "\x07\x06" # Just message type
    let decoded = decodeRebalanceStatusResponse(invalid)
    check decoded.isErr

  test "RebalanceStatus roundtrip":
    let resp = RebalanceStatusResponse(pending: 1'u32, inProgress: 2'u32,
        completed: 3'u32, failed: 4'u32)
    let encoded = encodeRebalanceStatusResponse(resp)
    let decoded = decodeRebalanceStatusResponse(encoded)
    check decoded.isOk
    check decoded.value.pending == resp.pending
    check decoded.value.inProgress == resp.inProgress

suite "DrainNode Messages":

  test "encodeDrainNodeRequest":
    let req = DrainNodeRequest(nodeId: 3'u16)
    let encoded = encodeDrainNodeRequest(req)
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.value == uint16(mtDrainNode)
    let nodeId = readUint16BE(encoded, pos)
    check nodeId.value == 3'u16

  test "decodeDrainNodeRequest valid":
    let req = DrainNodeRequest(nodeId: 7'u16)
    let encoded = encodeDrainNodeRequest(req)
    let decoded = decodeDrainNodeRequest(encoded)
    check decoded.isOk
    check decoded.value.nodeId == 7'u16

  test "decodeDrainNodeRequest truncated":
    let invalid = "\x07\x07" # Just message type
    let decoded = decodeDrainNodeRequest(invalid)
    check decoded.isErr

  test "encodeDrainNodeResponse success":
    let resp = DrainNodeResponse(success: true,
        message: "Node draining started")
    let encoded = encodeDrainNodeResponse(resp)
    let decoded = decodeDrainNodeResponse(encoded)
    check decoded.isOk
    check decoded.value.success == true

  test "encodeDrainNodeResponse failure":
    let resp = DrainNodeResponse(success: false, message: "Cannot drain leader")
    let encoded = encodeDrainNodeResponse(resp)
    let decoded = decodeDrainNodeResponse(encoded)
    check decoded.isOk
    check decoded.value.success == false
    check decoded.value.message == "Cannot drain leader"

  test "decodeDrainNodeResponse truncated":
    let invalid = "\x07\x07" # Just message type
    let decoded = decodeDrainNodeResponse(invalid)
    check decoded.isErr

suite "CreateGroup Messages":

  test "encodeCreateGroupRequest":
    let groupIdBytes = ulidToBytes(genULID())
    let req = CreateGroupRequest(
      groupId: groupIdBytes,
      preferredLeaderId: 1'u16,
      members: @[CreateGroupMember(nodeId: 1'u16, host: "n1",
          raftPort: 7001'u16, clientPort: 8001'u16)]
    )
    let encoded = encodeCreateGroupRequest(req)
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.value == uint16(mtCreateGroup)

  test "encodeCreateGroupRequest multiple members":
    let groupIdBytes = ulidToBytes(genULID())
    let req = CreateGroupRequest(
      groupId: groupIdBytes,
      preferredLeaderId: 1'u16,
      members: @[
        CreateGroupMember(nodeId: 1'u16, host: "n1", raftPort: 7001'u16,
            clientPort: 8001'u16),
        CreateGroupMember(nodeId: 2'u16, host: "n2", raftPort: 7002'u16,
            clientPort: 8002'u16),
        CreateGroupMember(nodeId: 3'u16, host: "n3", raftPort: 7003'u16,
            clientPort: 8003'u16)
      ]
    )
    let encoded = encodeCreateGroupRequest(req)
    let decoded = decodeCreateGroupRequest(encoded)
    check decoded.isOk
    check decoded.value.members.len == 3

  test "encodeCreateGroupRequest empty members":
    let groupIdBytes = ulidToBytes(genULID())
    let req = CreateGroupRequest(
      groupId: groupIdBytes,
      preferredLeaderId: 1'u16,
      members: @[]
    )
    let encoded = encodeCreateGroupRequest(req)
    let decoded = decodeCreateGroupRequest(encoded)
    check decoded.isOk
    check decoded.value.members.len == 0

  test "decodeCreateGroupRequest valid":
    let groupIdBytes = ulidToBytes(genULID())
    let req = CreateGroupRequest(
      groupId: groupIdBytes,
      preferredLeaderId: 2'u16,
      members: @[CreateGroupMember(nodeId: 2'u16, host: "leader",
          raftPort: 7002'u16, clientPort: 8002'u16)]
    )
    let encoded = encodeCreateGroupRequest(req)
    let decoded = decodeCreateGroupRequest(encoded)
    check decoded.isOk
    check decoded.value.groupId == groupIdBytes
    check decoded.value.preferredLeaderId == 2'u16
    check decoded.value.members.len == 1

  test "decodeCreateGroupRequest truncated groupId":
    let invalid = "\x07\x0a" # Just message type
    let decoded = decodeCreateGroupRequest(invalid)
    check decoded.isErr

  test "encodeCreateGroupResponse success":
    let groupIdBytes = ulidToBytes(genULID())
    let resp = CreateGroupResponse(success: true, groupId: groupIdBytes, error: "")
    let encoded = encodeCreateGroupResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let success = readUint8(encoded, pos)
    check success.value == 0x01'u8

  test "encodeCreateGroupResponse failure":
    let resp = CreateGroupResponse(success: false, groupId: "",
        error: "Failed to create group")
    let encoded = encodeCreateGroupResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let success = readUint8(encoded, pos)
    check success.value == 0x00'u8

  test "decodeCreateGroupResponse success":
    let groupIdBytes = ulidToBytes(genULID())
    let resp = CreateGroupResponse(success: true, groupId: groupIdBytes, error: "")
    let encoded = encodeCreateGroupResponse(resp)
    let decoded = decodeCreateGroupResponse(encoded)
    check decoded.isOk
    check decoded.value.success == true
    check decoded.value.groupId == groupIdBytes

  test "decodeCreateGroupResponse failure":
    let resp = CreateGroupResponse(success: false, groupId: "",
        error: "Node unavailable")
    let encoded = encodeCreateGroupResponse(resp)
    let decoded = decodeCreateGroupResponse(encoded)
    check decoded.isOk
    check decoded.value.success == false
    check decoded.value.error == "Node unavailable"

  test "decodeCreateGroupResponse truncated":
    let invalid = "\x07\x0a" # Just message type
    let decoded = decodeCreateGroupResponse(invalid)
    check decoded.isErr

suite "JoinGroup Messages":

  test "encodeJoinGroupRequest":
    let groupIdBytes = ulidToBytes(genULID())
    let req = JoinGroupRequest(
      groupId: groupIdBytes,
      creatorNodeId: 1'u16,
      creatorHost: "creator",
      creatorPort: 7001'u16,
      members: @[CreateGroupMember(nodeId: 2'u16, host: "joiner",
          raftPort: 7002'u16, clientPort: 8002'u16)]
    )
    let encoded = encodeJoinGroupRequest(req)
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.value == uint16(mtJoinGroup)

  test "encodeJoinGroupRequest no members":
    let groupIdBytes = ulidToBytes(genULID())
    let req = JoinGroupRequest(
      groupId: groupIdBytes,
      creatorNodeId: 1'u16,
      creatorHost: "creator",
      creatorPort: 7001'u16,
      members: @[]
    )
    let encoded = encodeJoinGroupRequest(req)
    let decoded = decodeJoinGroupRequest(encoded)
    check decoded.isOk
    check decoded.value.members.len == 0

  test "decodeJoinGroupRequest valid":
    let groupIdBytes = ulidToBytes(genULID())
    let req = JoinGroupRequest(
      groupId: groupIdBytes,
      creatorNodeId: 5'u16,
      creatorHost: "192.168.1.5",
      creatorPort: 7005'u16,
      members: @[]
    )
    let encoded = encodeJoinGroupRequest(req)
    let decoded = decodeJoinGroupRequest(encoded)
    check decoded.isOk
    check decoded.value.groupId == groupIdBytes
    check decoded.value.creatorNodeId == 5'u16
    check decoded.value.creatorHost == "192.168.1.5"
    check decoded.value.creatorPort == 7005'u16

  test "decodeJoinGroupRequest truncated groupId":
    let invalid = "\x07\x0b" # Just message type
    let decoded = decodeJoinGroupRequest(invalid)
    check decoded.isErr

  test "encodeJoinGroupResponse success":
    let groupIdBytes = ulidToBytes(genULID())
    let resp = JoinGroupResponse(success: true, groupId: groupIdBytes, error: "")
    let encoded = encodeJoinGroupResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let success = readUint8(encoded, pos)
    check success.value == 0x01'u8

  test "encodeJoinGroupResponse failure":
    let resp = JoinGroupResponse(success: false, groupId: "",
        error: "Group not found")
    let encoded = encodeJoinGroupResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let success = readUint8(encoded, pos)
    check success.value == 0x00'u8

  test "decodeJoinGroupResponse success":
    let groupIdBytes = ulidToBytes(genULID())
    let resp = JoinGroupResponse(success: true, groupId: groupIdBytes, error: "")
    let encoded = encodeJoinGroupResponse(resp)
    let decoded = decodeJoinGroupResponse(encoded)
    check decoded.isOk
    check decoded.value.success == true
    check decoded.value.groupId == groupIdBytes

  test "decodeJoinGroupResponse failure":
    let resp = JoinGroupResponse(success: false, groupId: "",
        error: "Already member")
    let encoded = encodeJoinGroupResponse(resp)
    let decoded = decodeJoinGroupResponse(encoded)
    check decoded.isOk
    check decoded.value.success == false
    check decoded.value.error == "Already member"

  test "decodeJoinGroupResponse truncated":
    let invalid = "\x07\x0b" # Just message type
    let decoded = decodeJoinGroupResponse(invalid)
    check decoded.isErr

suite "Cluster Constants":

  test "NodeStatusUnknown value":
    check NodeStatusUnknown == 0x00'u8

  test "NodeStatusActive value":
    check NodeStatusActive == 0x01'u8

  test "NodeStatusDraining value":
    check NodeStatusDraining == 0x02'u8

  test "NodeStatusDown value":
    check NodeStatusDown == 0x03'u8
