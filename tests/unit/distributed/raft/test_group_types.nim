# Unit tests for Group Types

import std/unittest
import std/strutils
import std/tables
import std/sets
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
    let id = genGroupIDLocal()
    let str = $id
    check str.len == 26
    check str.allCharsInSet(Digits + {'A'..'Z'})

  test "parse from ULID string":
    let id = genGroupIDLocal()
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
    let id1 = genGroupIDLocal()
    let id2 = genGroupIDLocal()
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

  test "binary serialization":
    let rep = newReplicaDescriptor(NodeID(42), ReplicaID(7), rtNonVoter)
    let encoded = encodeReplicaDescriptor(rep)
    check encoded.len == 13 # Fixed size

  test "binary deserialization":
    let rep = newReplicaDescriptor(NodeID(42), ReplicaID(7), rtNonVoter)
    let encoded = encodeReplicaDescriptor(rep)
    let decoded = decodeReplicaDescriptor(encoded)
    check decoded.nodeId == NodeID(42)
    check decoded.replicaId == ReplicaID(7)
    check decoded.replicaType == rtNonVoter

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

  test "binary round-trip":
    let gid = genGroupIDLocal()
    let desc = newGroupDescriptor(gid)
    discard desc.addReplica(NodeID(1))
    discard desc.addReplica(NodeID(2))
    discard desc.addReplica(NodeID(3))

    let encoded = encodeGroupDescriptor(desc)
    let decoded = decodeGroupDescriptor(encoded)

    check decoded.groupId == desc.groupId
    check decoded.replicas.len == desc.replicas.len
    check decoded.generation == desc.generation

suite "Key Encoding":
  test "group prefix":
    let gid = genGroupIDLocal()
    let prefix = encodeGroupPrefix(gid)
    check prefix.startsWith("/range/")
    check prefix.endsWith("/")

  test "data key":
    let gid = genGroupIDLocal()
    let key = encodeDataKey(gid, @[byte 0x01, 0x02])
    check key.startsWith("/range/")
    check key.contains("/data/")

  test "log key":
    let gid = genGroupIDLocal()
    let key = encodeLogKey(gid, 789'u64)
    check key.startsWith("/raft/")
    check key.endsWith("/log/789")

  test "state key":
    let gid = genGroupIDLocal()
    let key = encodeStateKey(gid)
    check key.startsWith("/raft/")
    check key.endsWith("/state")

  test "snapshot key":
    let gid = genGroupIDLocal()
    let key = encodeSnapshotKey(gid)
    check key.startsWith("/raft/")
    check key.endsWith("/snapshot")

  test "parse log index":
    let gid = genGroupIDLocal()
    let key = encodeLogKey(gid, 789'u64)
    let index = parseLogIndex(key)
    check index == 789'u64

    expect ValueError:
      discard parseLogIndex("/invalid/key")

suite "Byte Sequence Comparisons":
  test "byte sequence less than":
    let a = @[byte 0x01, 0x02]
    let b = @[byte 0x01, 0x03]
    check a < b
    check not (b < a)

  test "byte sequence less than different lengths":
    let a = @[byte 0x01, 0x02]
    let b = @[byte 0x01, 0x02, 0x03]
    check a < b

  test "byte sequence equal":
    let a = @[byte 0x01, 0x02, 0x03]
    let b = @[byte 0x01, 0x02, 0x03]
    check not (a < b)
    check not (b < a)
    check a <= b
    check b <= a
    check a >= b
    check b >= a
    check not (a > b)

  test "byte sequence greater":
    let a = @[byte 0x02]
    let b = @[byte 0x01]
    check a > b
    check a >= b
    check not (a < b)

  test "empty byte sequences":
    let a: seq[byte] = @[]
    let b: seq[byte] = @[]
    check a <= b
    check not (a < b)
    check a == b

  test "empty vs non-empty":
    let a: seq[byte] = @[]
    let b = @[byte 0x01]
    check a < b

suite "NodeID Extended":
  test "parseNodeID invalid format no prefix":
    expect ValueError:
      discard parseNodeID("42")

  test "parseNodeID invalid format empty":
    expect ValueError:
      discard parseNodeID("")

  test "parseNodeID invalid format wrong prefix":
    expect ValueError:
      discard parseNodeID("x42")

  test "NodeID max value":
    let id = NodeID(uint32.high)
    check $id == "n" & $uint32.high
    check id.uint32 == uint32.high

  test "NodeID distinct type":
    let id1 = NodeID(42)
    let id2: uint32 = 42
    check id1.uint32 == id2

  test "NodeID ordering edge cases":
    check NodeID(0) < NodeID(1)
    check NodeID(0) <= NodeID(0)
    check NodeID(uint32.high) > NodeID(1)

suite "GroupID Extended":
  test "groupIDFromInt":
    let id = groupIDFromInt(12345)
    check id.ULID.data[8] == 0x39'u8

  test "ZeroGroupID":
    let zero = ZeroGroupID()
    for b in zero.ULID.data:
      check b == 0'u8

  test "groupIDFromULID and groupIDToULID":
    let ulid = genULIDLocal()
    let gid = groupIDFromULID(ulid)
    let recovered = groupIDToULID(gid)
    check recovered == ulid

  test "groupIDToBytes and groupIDFromBytes":
    let gid = genGroupIDLocal()
    let bytes = groupIDToBytes(gid)
    check bytes.len == 16
    let recovered = groupIDFromBytes(bytes)
    check recovered == gid

  test "GroupID hash consistency":
    let id1 = genGroupIDLocal()
    let id2 = id1
    check hash(id1) == hash(id2)

  test "GroupID in HashSet":
    var set = initHashSet[GroupID]()
    let id = genGroupIDLocal()
    set.incl(id)
    check id in set
    set.excl(id)
    check id notin set

  test "GroupID ordering time-based":
    let id1 = genGroupIDLocal()
    let id2 = genGroupIDLocal()
    check (id1 < id2) or (id2 < id1) or (id1 == id2)

suite "ReplicaID Extended":
  test "parseReplicaID invalid format no prefix":
    expect ValueError:
      discard parseReplicaID("5")

  test "parseReplicaID invalid format wrong prefix":
    expect ValueError:
      discard parseReplicaID("repX")

  test "parseReplicaID empty":
    expect ValueError:
      discard parseReplicaID("")

  test "ReplicaID max value":
    var id = ReplicaID(uint32.high)
    check id.uint32 == uint32.high

  test "ReplicaID multiple next calls":
    var id = firstReplicaID()
    check id.next() == ReplicaID(1)
    check id == ReplicaID(2)
    check id.next() == ReplicaID(2)
    check id == ReplicaID(3)

  test "ReplicaID hash":
    var table = {ReplicaID(1): "one", ReplicaID(2): "two"}.toTable
    check table[ReplicaID(1)] == "one"

suite "ReplicaDescriptor Extended":
  test "hash combines nodeId and replicaId":
    let rep1 = newReplicaDescriptor(NodeID(1), ReplicaID(1))
    let rep2 = newReplicaDescriptor(NodeID(1), ReplicaID(2))
    let rep3 = newReplicaDescriptor(NodeID(2), ReplicaID(1))
    check hash(rep1) != hash(rep2)
    check hash(rep1) != hash(rep3)

  test "equality different replicaId":
    let rep1 = newReplicaDescriptor(NodeID(1), ReplicaID(1))
    let rep2 = newReplicaDescriptor(NodeID(1), ReplicaID(2))
    check rep1 != rep2

  test "binary encode verify magic":
    let rep = newReplicaDescriptor(NodeID(1), ReplicaID(1))
    let encoded = encodeReplicaDescriptor(rep)
    check encoded[0] == char(REPLICA_DESC_MAGIC[0])
    check encoded[1] == char(REPLICA_DESC_MAGIC[1])
    check encoded[2] == char(REPLICA_DESC_MAGIC[2])
    check encoded[3] == char(REPLICA_DESC_VERSION)

  test "binary decode invalid magic":
    let invalid = "INVALID_DATA_TOO_SMALL"
    expect ValueError:
      discard decodeReplicaDescriptor(invalid)

  test "binary decode too small":
    let small = "RPD\x01"
    expect ValueError:
      discard decodeReplicaDescriptor(small)

  test "binary decode invalid version":
    var encoded = encodeReplicaDescriptor(newReplicaDescriptor(NodeID(1),
        ReplicaID(1)))
    encoded[3] = char(0xFF)
    expect ValueError:
      discard decodeReplicaDescriptor(encoded)

  test "binary roundtrip all types":
    for rt in [rtVoter, rtNonVoter]:
      let rep = newReplicaDescriptor(NodeID(42), ReplicaID(7), rt)
      let encoded = encodeReplicaDescriptor(rep)
      let decoded = decodeReplicaDescriptor(encoded)
      check decoded.replicaType == rt

suite "GroupDescriptor Extended":
  test "addReplica duplicate node returns existing":
    let desc = newGroupDescriptor(genGroupIDLocal())
    let rep1 = desc.addReplica(NodeID(1))
    let rep2 = desc.addReplica(NodeID(1))
    check rep1 == rep2
    check desc.replicas.len == 1

  test "removeReplica non-existent returns false":
    let desc = newGroupDescriptor(genGroupIDLocal())
    discard desc.addReplica(NodeID(1))
    check not desc.removeReplica(ReplicaID(99))

  test "removeReplica removes correct replica":
    let desc = newGroupDescriptor(genGroupIDLocal())
    discard desc.addReplica(NodeID(1))
    discard desc.addReplica(NodeID(2))
    discard desc.addReplica(NodeID(3))
    check desc.removeReplica(ReplicaID(2))
    check desc.replicas.len == 2
    check desc.replicas[0].nodeId == NodeID(1)
    check desc.replicas[1].nodeId == NodeID(3)

  test "quorum size edge cases":
    let desc1 = newGroupDescriptor(genGroupIDLocal())
    discard desc1.addReplica(NodeID(1))
    check desc1.quorumSize() == 1

    let desc2 = newGroupDescriptor(genGroupIDLocal())
    discard desc2.addReplica(NodeID(1))
    discard desc2.addReplica(NodeID(2))
    check desc2.quorumSize() == 2

    let desc5 = newGroupDescriptor(genGroupIDLocal())
    for i in 1..5:
      discard desc5.addReplica(NodeID(int32(i)))
    check desc5.quorumSize() == 3

  test "quorum size only non-voters":
    let desc = newGroupDescriptor(genGroupIDLocal())
    discard desc.addReplica(NodeID(1), rtNonVoter)
    discard desc.addReplica(NodeID(2), rtNonVoter)
    check desc.quorumSize() == 1

  test "isInitialized with replicas":
    let gid = genGroupIDLocal()
    let desc = newGroupDescriptor(gid)
    check not desc.isInitialized()
    discard desc.addReplica(NodeID(1))
    check desc.isInitialized()

  test "isMetaGroup":
    let metaDesc = newGroupDescriptor(GroupID(ULID()))
    metaDesc.groupId.ULID.data[15] = 1'u8
    check metaDesc.isMetaGroup()

    let dataDesc = newGroupDescriptor(genGroupIDLocal())
    check not dataDesc.isMetaGroup()

  test "preferredLeader field":
    let desc = newGroupDescriptor(genGroupIDLocal())
    check desc.preferredLeader == NodeID(0)
    desc.preferredLeader = NodeID(5)
    check desc.preferredLeader == NodeID(5)

  test "leader field":
    let desc = newGroupDescriptor(genGroupIDLocal())
    check desc.leader == NodeID(0)
    desc.leader = NodeID(3)
    check desc.leader == NodeID(3)

  test "binary encode verify magic":
    let desc = newGroupDescriptor(genGroupIDLocal())
    discard desc.addReplica(NodeID(1))
    let encoded = encodeGroupDescriptor(desc)
    check encoded[0] == char(GROUP_DESC_MAGIC[0])
    check encoded[1] == char(GROUP_DESC_MAGIC[1])
    check encoded[2] == char(GROUP_DESC_MAGIC[2])
    check encoded[3] == char(GROUP_DESC_VERSION)

  test "binary decode invalid magic":
    let invalid = "INVALID_DATA_FOR_GROUP_DESCRIPTOR_TOO_SMALL"
    expect ValueError:
      discard decodeGroupDescriptor(invalid)

  test "binary decode too small":
    let small = "GPD\x01"
    expect ValueError:
      discard decodeGroupDescriptor(small)

  test "binary decode invalid version":
    var encoded = encodeGroupDescriptor(newGroupDescriptor(genGroupIDLocal()))
    encoded[3] = char(0xFF)
    expect ValueError:
      discard decodeGroupDescriptor(encoded)

  test "binary roundtrip with all fields":
    let desc = newGroupDescriptor(genGroupIDLocal())
    discard desc.addReplica(NodeID(1))
    discard desc.addReplica(NodeID(2), rtNonVoter)
    desc.preferredLeader = NodeID(5)
    desc.leader = NodeID(3)

    let encoded = encodeGroupDescriptor(desc)
    let decoded = decodeGroupDescriptor(encoded)

    check decoded.groupId == desc.groupId
    check decoded.generation == desc.generation
    check decoded.nextReplicaId == desc.nextReplicaId
    check decoded.preferredLeader == desc.preferredLeader
    check decoded.leader == desc.leader
    check decoded.replicas.len == desc.replicas.len

  test "string representation":
    let desc = newGroupDescriptor(genGroupIDLocal())
    discard desc.addReplica(NodeID(1))
    let str = $desc
    check str.contains("GroupDescriptor")
    check str.contains("replicas=1")

suite "Key Encoding Extended":
  test "encodeDataKey with empty bytes":
    let gid = genGroupIDLocal()
    let key = encodeDataKey(gid, @[])
    check key.contains("/data/")

  test "encodeDataKey with large bytes":
    let gid = genGroupIDLocal()
    var largeBytes = newSeq[byte](1000)
    for i in 0..<largeBytes.len:
      largeBytes[i] = byte(i mod 256)
    let key = encodeDataKey(gid, largeBytes)
    check key.len > 1000

  test "encodeLogKey with zero index":
    let gid = genGroupIDLocal()
    let key = encodeLogKey(gid, 0'u64)
    check key.endsWith("/log/0")

  test "encodeLogKey with max index":
    let gid = genGroupIDLocal()
    let key = encodeLogKey(gid, uint64.high)
    check key.contains("/log/")

  test "parseLogIndex various formats":
    let gid = genGroupIDLocal()
    for idx in [0'u64, 1'u64, 100'u64, uint64.high]:
      let key = encodeLogKey(gid, idx)
      let parsed = parseLogIndex(key)
      check parsed == idx

  test "parseLogIndex missing log segment":
    expect ValueError:
      discard parseLogIndex("/raft/groupid/state")

  test "parseLogIndex wrong format entirely":
    expect ValueError:
      discard parseLogIndex("random_string")

suite "GroupDescriptor Operations":
  test "many replicas":
    let desc = newGroupDescriptor(genGroupIDLocal())
    for i in 1..100:
      discard desc.addReplica(NodeID(int32(i)))
    check desc.replicas.len == 100
    check desc.quorumSize() == 51

  test "alternate voter/non-voter":
    let desc = newGroupDescriptor(genGroupIDLocal())
    for i in 1..10:
      let rt = if i mod 2 == 0: rtNonVoter else: rtVoter
      discard desc.addReplica(NodeID(int32(i)), rt)
    let voters = desc.getVoters()
    let nonVoters = desc.getNonVoters()
    check voters.len == 5
    check nonVoters.len == 5

  test "remove all replicas":
    let desc = newGroupDescriptor(genGroupIDLocal())
    for i in 1..5:
      discard desc.addReplica(NodeID(int32(i)))
    for i in 1..5:
      check desc.removeReplica(ReplicaID(uint32(i)))
    check desc.replicas.len == 0

  test "generation increments correctly":
    let desc = newGroupDescriptor(genGroupIDLocal())
    check desc.generation == 1
    discard desc.addReplica(NodeID(1))
    check desc.generation == 2
    discard desc.addReplica(NodeID(2))
    check desc.generation == 3
    check desc.removeReplica(ReplicaID(1))
    check desc.generation == 4
