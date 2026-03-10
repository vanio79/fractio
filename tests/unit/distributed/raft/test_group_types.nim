# Unit tests for Group Types

import std/unittest
import std/json
import std/strutils
import std/tables
import std/options

import fractio/distributed/raft/group_types

suite "NodeID":
  test "string representation":
    let id = NodeID(42)
    check $id == "n42"

  test "parse from string":
    let id = parseNodeID("n42")
    check id == NodeID(42)

  test "invalid NodeID is zero":
    let invalid = invalidNodeID()
    check invalid.uint32 == 0
    check not isValid(invalid)

  test "valid NodeID is non-zero":
    let valid = NodeID(1)
    check isValid(valid)

  test "equality comparison":
    check NodeID(1) == NodeID(1)
    check NodeID(1) != NodeID(2)

  test "ordering":
    check NodeID(1) < NodeID(2)
    check NodeID(1) <= NodeID(2)
    check NodeID(2) <= NodeID(2)

  test "hash for tables":
    var table = {NodeID(1): "one", NodeID(2): "two"}.toTable
    check table[NodeID(1)] == "one"

suite "GroupID":
  test "string representation":
    let id = GroupID(100)
    check $id == "r100"

  test "parse from string":
    let id = parseGroupID("r100")
    check id == GroupID(100)

  test "first group ID":
    let first = firstGroupID()
    check first.uint64 == 1

  test "equality comparison":
    check GroupID(1) == GroupID(1)
    check GroupID(1) != GroupID(2)

  test "ordering":
    check GroupID(1) < GroupID(2)
    check GroupID(1) <= GroupID(2)

suite "ReplicaID":
  test "string representation":
    let id = ReplicaID(5)
    check $id == "rep5"

  test "parse from string":
    let id = parseReplicaID("rep5")
    check id == ReplicaID(5)

  test "first replica ID":
    let first = firstReplicaID()
    check first.uint32 == 1

  test "next increments":
    var id = ReplicaID(1)
    let current = id.next()
    check current == ReplicaID(1)
    check id == ReplicaID(2)

suite "ReplicaDescriptor":
  test "create voter":
    let rep = newReplicaDescriptor(NodeID(1), ReplicaID(1))
    check rep.nodeId == NodeID(1)
    check rep.replicaId == ReplicaID(1)
    check rep.isVoter()
    check rep.replicaType == rtVoter

  test "create non-voter":
    let rep = newReplicaDescriptor(NodeID(1), ReplicaID(1), rtNonVoter)
    check not rep.isVoter()
    check rep.replicaType == rtNonVoter

  test "equality":
    let rep1 = newReplicaDescriptor(NodeID(1), ReplicaID(1))
    let rep2 = newReplicaDescriptor(NodeID(1), ReplicaID(1))
    let rep3 = newReplicaDescriptor(NodeID(2), ReplicaID(1))
    check rep1 == rep2
    check rep1 != rep3

  test "JSON serialization":
    let rep = newReplicaDescriptor(NodeID(42), ReplicaID(7), rtNonVoter)
    let json = rep.toJson()
    check json["nodeId"].getInt() == 42
    check json["replicaId"].getInt() == 7
    check json["replicaType"].getInt() == ord(rtNonVoter)

  test "JSON deserialization":
    let json = %*{"nodeId": 42, "replicaId": 7, "replicaType": 1}
    let rep = parseReplicaDescriptor(json)
    check rep.nodeId == NodeID(42)
    check rep.replicaId == ReplicaID(7)
    check rep.replicaType == rtNonVoter

suite "GroupDescriptor":
  test "create basic descriptor":
    let desc = newGroupDescriptor(GroupID(1))
    check desc.groupId == GroupID(1)
    check desc.replicas.len == 0
    check desc.generation == 1

  test "add replica":
    let desc = newGroupDescriptor(GroupID(1))
    let rep1 = desc.addReplica(NodeID(1))
    check desc.replicas.len == 1
    check rep1.nodeId == NodeID(1)
    check rep1.replicaId == ReplicaID(1)
    check desc.generation == 2

    let rep2 = desc.addReplica(NodeID(2))
    check desc.replicas.len == 2
    check rep2.replicaId == ReplicaID(2)
    check desc.generation == 3

  test "add non-voter replica":
    let desc = newGroupDescriptor(GroupID(1))
    discard desc.addReplica(NodeID(1))
    let rep2 = desc.addReplica(NodeID(2), rtNonVoter)
    check rep2.replicaType == rtNonVoter

  test "remove replica":
    let desc = newGroupDescriptor(GroupID(1))
    discard desc.addReplica(NodeID(1))
    discard desc.addReplica(NodeID(2))
    check desc.replicas.len == 2
    check desc.generation == 3 # gen starts at 1, +1 for each add

    let removed = desc.removeReplica(ReplicaID(1))
    check removed
    check desc.replicas.len == 1
    check desc.generation == 4 # +1 for remove

  test "get replica by node":
    let desc = newGroupDescriptor(GroupID(1))
    discard desc.addReplica(NodeID(1))
    discard desc.addReplica(NodeID(2))

    let rep = desc.getReplica(NodeID(1))
    check rep.isSome()
    check rep.get().nodeId == NodeID(1)

    let missing = desc.getReplica(NodeID(99))
    check missing.isNone()

  test "get voters and non-voters":
    let desc = newGroupDescriptor(GroupID(1))
    discard desc.addReplica(NodeID(1)) # voter
    discard desc.addReplica(NodeID(2)) # voter
    discard desc.addReplica(NodeID(3), rtNonVoter) # non-voter

    let voters = desc.getVoters()
    check voters.len == 2

    let nonVoters = desc.getNonVoters()
    check nonVoters.len == 1

  test "quorum size":
    let desc = newGroupDescriptor(GroupID(1))
    discard desc.addReplica(NodeID(1))
    discard desc.addReplica(NodeID(2))
    discard desc.addReplica(NodeID(3))
    check desc.quorumSize() == 2 # majority of 3

  test "is initialized":
    var desc = newGroupDescriptor(GroupID(0))
    check not desc.isInitialized()

    desc = newGroupDescriptor(GroupID(1))
    discard desc.addReplica(NodeID(1))
    check desc.isInitialized()

  test "JSON round-trip":
    let desc = newGroupDescriptor(GroupID(42))
    discard desc.addReplica(NodeID(1))
    discard desc.addReplica(NodeID(2))
    discard desc.addReplica(NodeID(3))

    let json = desc.toJson()
    let parsed = parseGroupDescriptor(json)

    check parsed.groupId == desc.groupId
    check parsed.replicas.len == desc.replicas.len
    check parsed.generation == desc.generation

suite "Key Encoding":
  test "group prefix":
    let prefix = encodeGroupPrefix(GroupID(123))
    check prefix == "/range/123/"

  test "data key":
    let key = encodeDataKey(GroupID(123), @[byte 0x01, 0x02])
    check key.startsWith("/range/123/data/")

  test "log key":
    let key = encodeLogKey(GroupID(456), 789'u64)
    check key == "/raft/456/log/789"

  test "state key":
    let key = encodeStateKey(GroupID(456))
    check key == "/raft/456/state"

  test "snapshot key":
    let key = encodeSnapshotKey(GroupID(456))
    check key == "/raft/456/snapshot"

  test "parse log index":
    let index = parseLogIndex("/raft/456/log/789")
    check index == 789'u64

    expect ValueError:
      discard parseLogIndex("/invalid/key")
