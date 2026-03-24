# Unit tests for Group Types

import std/unittest
import std/json
import std/strutils
import std/tables
import std/options

import fractio/core/types except NodeID
import fractio/distributed/raft/group_types
import fractio/distributed/meta/system_tables

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
  test "string representation is 26-char ULID":
    let id = genGroupID()
    let str = $id
    check str.len == 26
    check str.allCharsInSet(Digits + {'A'..'Z'})

  test "parse from ULID string":
    let id = genGroupID()
    let parsed = parseGroupID($id)
    check parsed == id

  test "META_GROUP_ID has expected format":
    # META_GROUP_ID should be a well-known ULID (ends with 01)
    let str = $META_GROUP_ID
    check str.len == 26
    check str == "00000000000000000000000001"

  test "equality comparison":
    check META_GROUP_ID == META_GROUP_ID
    check DATA_GROUP_START_ID == DATA_GROUP_START_ID
    check META_GROUP_ID != DATA_GROUP_START_ID

  test "ordering":
    check META_GROUP_ID < DATA_GROUP_START_ID
    check META_GROUP_ID <= DATA_GROUP_START_ID

  test "generation produces unique IDs":
    let id1 = genGroupID()
    let id2 = genGroupID()
    # ULIDs are unique
    check id1 != id2
    # Both are valid ULIDs
    check ($id1).len == 26
    check ($id2).len == 26

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
    let desc = newGroupDescriptor(META_GROUP_ID)
    check desc.groupId == META_GROUP_ID
    check desc.replicas.len == 0
    check desc.generation == 1

  test "add replica":
    let desc = newGroupDescriptor(META_GROUP_ID)
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
    let desc = newGroupDescriptor(META_GROUP_ID)
    discard desc.addReplica(NodeID(1))
    let rep2 = desc.addReplica(NodeID(2), rtNonVoter)
    check rep2.replicaType == rtNonVoter

  test "remove replica":
    let desc = newGroupDescriptor(META_GROUP_ID)
    discard desc.addReplica(NodeID(1))
    discard desc.addReplica(NodeID(2))
    check desc.replicas.len == 2
    check desc.generation == 3 # gen starts at 1, +1 for each add

    let removed = desc.removeReplica(ReplicaID(1))
    check removed
    check desc.replicas.len == 1
    check desc.generation == 4 # +1 for remove

  test "get replica by node":
    let desc = newGroupDescriptor(META_GROUP_ID)
    discard desc.addReplica(NodeID(1))
    discard desc.addReplica(NodeID(2))

    let rep = desc.getReplica(NodeID(1))
    check rep.isSome()
    check rep.get().nodeId == NodeID(1)

    let missing = desc.getReplica(NodeID(99))
    check missing.isNone()

  test "get voters and non-voters":
    let desc = newGroupDescriptor(META_GROUP_ID)
    discard desc.addReplica(NodeID(1)) # voter
    discard desc.addReplica(NodeID(2)) # voter
    discard desc.addReplica(NodeID(3), rtNonVoter) # non-voter

    let voters = desc.getVoters()
    check voters.len == 2

    let nonVoters = desc.getNonVoters()
    check nonVoters.len == 1

  test "quorum size":
    let desc = newGroupDescriptor(META_GROUP_ID)
    discard desc.addReplica(NodeID(1))
    discard desc.addReplica(NodeID(2))
    discard desc.addReplica(NodeID(3))
    check desc.quorumSize() == 2 # majority of 3

  test "is initialized":
    var desc = newGroupDescriptor(GroupID(ULID()))
    check not desc.isInitialized()

    desc = newGroupDescriptor(META_GROUP_ID)
    discard desc.addReplica(NodeID(1))
    check desc.isInitialized()

  test "JSON round-trip":
    let gid = genGroupID()
    let desc = newGroupDescriptor(gid)
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
    let gid = genGroupID()
    let prefix = encodeGroupPrefix(gid)
    check prefix.startsWith("/range/")
    check prefix.endsWith("/")

  test "data key":
    let gid = genGroupID()
    let key = encodeDataKey(gid, @[byte 0x01, 0x02])
    check key.startsWith("/range/")
    check key.contains("/data/")

  test "log key":
    let gid = genGroupID()
    let key = encodeLogKey(gid, 789'u64)
    check key.startsWith("/raft/")
    check key.endsWith("/log/789")

  test "state key":
    let gid = genGroupID()
    let key = encodeStateKey(gid)
    check key.startsWith("/raft/")
    check key.endsWith("/state")

  test "snapshot key":
    let gid = genGroupID()
    let key = encodeSnapshotKey(gid)
    check key.startsWith("/raft/")
    check key.endsWith("/snapshot")

  test "parse log index":
    let gid = genGroupID()
    let key = encodeLogKey(gid, 789'u64)
    let index = parseLogIndex(key)
    check index == 789'u64

    expect ValueError:
      discard parseLogIndex("/invalid/key")
