# Unit tests for GroupDescriptor/ReplicaDescriptor binary serialization

import unittest
import std/strutils
import fractio/core/types except NodeID
import fractio/distributed/raft/group_types
import fractio/utils/binary

suite "ReplicaDescriptor Binary Serialization":
  test "encode ReplicaDescriptor voter":
    let rep = ReplicaDescriptor(
      nodeId: NodeID(1),
      replicaId: ReplicaID(10),
      replicaType: rtVoter
    )
    let encoded = encodeReplicaDescriptor(rep)
    # 3 (magic) + 1 (version) + 4 (nodeId) + 4 (replicaId) + 1 (type) = 13 bytes
    check encoded.len == 13
    check encoded[0] == 'R'
    check encoded[1] == 'P'
    check encoded[2] == 'D'

  test "encode ReplicaDescriptor non-voter":
    let rep = ReplicaDescriptor(
      nodeId: NodeID(42),
      replicaId: ReplicaID(100),
      replicaType: rtNonVoter
    )
    let encoded = encodeReplicaDescriptor(rep)
    check encoded.len == 13

  test "decode ReplicaDescriptor roundtrip voter":
    let rep = ReplicaDescriptor(
      nodeId: NodeID(5),
      replicaId: ReplicaID(25),
      replicaType: rtVoter
    )
    let encoded = encodeReplicaDescriptor(rep)
    let decoded = decodeReplicaDescriptor(encoded)
    check decoded.nodeId == rep.nodeId
    check decoded.replicaId == rep.replicaId
    check decoded.replicaType == rep.replicaType

  test "decode ReplicaDescriptor roundtrip non-voter":
    let rep = ReplicaDescriptor(
      nodeId: NodeID(100),
      replicaId: ReplicaID(200),
      replicaType: rtNonVoter
    )
    let encoded = encodeReplicaDescriptor(rep)
    let decoded = decodeReplicaDescriptor(encoded)
    check decoded.nodeId == rep.nodeId
    check decoded.replicaId == rep.replicaId
    check decoded.replicaType == rep.replicaType

  test "decode rejects invalid magic":
    let badData = "XXX\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
    expect ValueError:
      discard decodeReplicaDescriptor(badData)

  test "decode rejects truncated data":
    let rep = ReplicaDescriptor(nodeId: NodeID(1), replicaId: ReplicaID(1),
        replicaType: rtVoter)
    let encoded = encodeReplicaDescriptor(rep)
    let truncated = encoded[0..5]
    expect ValueError:
      discard decodeReplicaDescriptor(truncated)

  test "decode rejects unsupported version":
    var w = initBinaryWriter()
    w.writeBytes(REPLICA_DESC_MAGIC)
    w.writeU8(99'u8) # Invalid version
    w.writeU32(1'u32)
    w.writeU32(1'u32)
    w.writeU8(0'u8)
    let encoded = w.finish()
    expect ValueError:
      discard decodeReplicaDescriptor(encoded)

suite "GroupDescriptor Binary Serialization":
  test "encode minimal GroupDescriptor":
    let groupId = genGroupIDLocal()
    let desc = newGroupDescriptor(groupId)
    let encoded = encodeGroupDescriptor(desc)
    # 3 (magic) + 1 (version) + 16 (groupId) + 8 (gen) + 4 (nextRepId) +
    # 4 (prefLeader) + 4 (leader) + 4 (repCount) = 44 bytes minimum
    check encoded.len == 44
    check encoded[0] == 'G'
    check encoded[1] == 'P'
    check encoded[2] == 'D'

  test "encode GroupDescriptor with replicas":
    let groupId = genGroupIDLocal()
    let desc = newGroupDescriptor(groupId)
    discard desc.addReplica(NodeID(1))
    discard desc.addReplica(NodeID(2))
    discard desc.addReplica(NodeID(3))
    let encoded = encodeGroupDescriptor(desc)
    # 44 + 3*13 = 83 bytes
    check encoded.len == 83

  test "decode GroupDescriptor roundtrip minimal":
    let groupId = genGroupIDLocal()
    let desc = newGroupDescriptor(groupId)
    let encoded = encodeGroupDescriptor(desc)
    let decoded = decodeGroupDescriptor(encoded)
    check decoded.groupId == desc.groupId
    check decoded.replicas.len == 0
    check decoded.generation == desc.generation
    check decoded.nextReplicaId == desc.nextReplicaId

  test "decode GroupDescriptor roundtrip with replicas":
    let groupId = genGroupIDLocal()
    let desc = newGroupDescriptor(groupId)
    discard desc.addReplica(NodeID(1), rtVoter)
    discard desc.addReplica(NodeID(2), rtVoter)
    discard desc.addReplica(NodeID(3), rtNonVoter)
    desc.preferredLeader = NodeID(1)
    desc.leader = NodeID(2)

    let encoded = encodeGroupDescriptor(desc)
    let decoded = decodeGroupDescriptor(encoded)

    check decoded.groupId == desc.groupId
    check decoded.replicas.len == 3
    check decoded.replicas[0].nodeId == NodeID(1)
    check decoded.replicas[0].replicaType == rtVoter
    check decoded.replicas[1].nodeId == NodeID(2)
    check decoded.replicas[1].replicaType == rtVoter
    check decoded.replicas[2].nodeId == NodeID(3)
    check decoded.replicas[2].replicaType == rtNonVoter
    check decoded.generation == desc.generation
    check decoded.nextReplicaId == desc.nextReplicaId
    check decoded.preferredLeader == NodeID(1)
    check decoded.leader == NodeID(2)

  test "decode GroupDescriptor with invalid leader fields":
    let groupId = genGroupIDLocal()
    let desc = newGroupDescriptor(groupId)
    # InvalidNodeID is 0
    check not desc.preferredLeader.isValid
    check not desc.leader.isValid

    let encoded = encodeGroupDescriptor(desc)
    let decoded = decodeGroupDescriptor(encoded)

    check not decoded.preferredLeader.isValid
    check not decoded.leader.isValid

  test "decode rejects invalid magic":
    let badData = "XXX\x01" & "\x00".repeat(40)
    expect ValueError:
      discard decodeGroupDescriptor(badData)

  test "decode rejects truncated data":
    let groupId = genGroupIDLocal()
    let desc = newGroupDescriptor(groupId)
    let encoded = encodeGroupDescriptor(desc)
    let truncated = encoded[0..20]
    expect ValueError:
      discard decodeGroupDescriptor(truncated)

  test "decode rejects unsupported version":
    var w = initBinaryWriter()
    w.writeBytes(GROUP_DESC_MAGIC)
    w.writeU8(99'u8) # Invalid version
    w.writeBytes("\x00".repeat(16)) # groupId
    w.writeU64(1'u64)
    w.writeU32(1'u32)
    w.writeU32(0'u32)
    w.writeU32(0'u32)
    w.writeU32(0'u32)
    let encoded = w.finish()
    expect ValueError:
      discard decodeGroupDescriptor(encoded)

suite "GroupDescriptor Binary - Edge Cases":
  test "large number of replicas":
    let groupId = genGroupIDLocal()
    let desc = newGroupDescriptor(groupId)
    for i in 1..100:
      discard desc.addReplica(NodeID(i))

    let encoded = encodeGroupDescriptor(desc)
    let decoded = decodeGroupDescriptor(encoded)

    check decoded.replicas.len == 100
    for i in 0..<100:
      check decoded.replicas[i].nodeId == NodeID(i + 1)

  test "generation and nextReplicaId preserved":
    let groupId = genGroupIDLocal()
    let desc = newGroupDescriptor(groupId)
    desc.generation = 12345
    desc.nextReplicaId = ReplicaID(50)

    let encoded = encodeGroupDescriptor(desc)
    let decoded = decodeGroupDescriptor(encoded)

    check decoded.generation == 12345
    check decoded.nextReplicaId == ReplicaID(50)

  test "zero groupId roundtrip":
    let desc = newGroupDescriptor(ZeroGroupID())
    let encoded = encodeGroupDescriptor(desc)
    let decoded = decodeGroupDescriptor(encoded)
    check decoded.groupId == ZeroGroupID()
