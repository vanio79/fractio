# Unit tests for Range Types

import std/unittest
import std/json
import std/strutils
import std/tables
import std/options

import fractio/distributed/range/types

suite "RangeNodeID":
  test "string representation":
    let id = RangeNodeID(42)
    check $id == "n42"

  test "parse from string":
    let id = parseNodeID("n42")
    check id == RangeNodeID(42)

  test "invalid RangeNodeID is zero":
    let invalid = invalidNodeID()
    check invalid.uint32 == 0
    check not isValid(invalid)

  test "valid RangeNodeID is non-zero":
    let valid = RangeNodeID(1)
    check isValid(valid)

  test "equality comparison":
    check RangeNodeID(1) == RangeNodeID(1)
    check RangeNodeID(1) != RangeNodeID(2)

  test "ordering":
    check RangeNodeID(1) < RangeNodeID(2)
    check RangeNodeID(1) <= RangeNodeID(2)
    check RangeNodeID(2) <= RangeNodeID(2)

  test "hash for tables":
    var table = {RangeNodeID(1): "one", RangeNodeID(2): "two"}.toTable
    check table[RangeNodeID(1)] == "one"

suite "RangeID":
  test "string representation":
    let id = RangeID(100)
    check $id == "r100"

  test "parse from string":
    let id = parseRangeID("r100")
    check id == RangeID(100)

  test "first range ID":
    let first = firstRangeID()
    check first.uint64 == 1

  test "equality comparison":
    check RangeID(1) == RangeID(1)
    check RangeID(1) != RangeID(2)

  test "ordering":
    check RangeID(1) < RangeID(2)
    check RangeID(1) <= RangeID(2)

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
    let rep = newReplicaDescriptor(RangeNodeID(1), ReplicaID(1))
    check rep.nodeId == RangeNodeID(1)
    check rep.replicaId == ReplicaID(1)
    check rep.isVoter()
    check rep.replicaType == rtVoter

  test "create non-voter":
    let rep = newReplicaDescriptor(RangeNodeID(1), ReplicaID(1), rtNonVoter)
    check not rep.isVoter()
    check rep.replicaType == rtNonVoter

  test "equality":
    let rep1 = newReplicaDescriptor(RangeNodeID(1), ReplicaID(1))
    let rep2 = newReplicaDescriptor(RangeNodeID(1), ReplicaID(1))
    let rep3 = newReplicaDescriptor(RangeNodeID(2), ReplicaID(1))
    check rep1 == rep2
    check rep1 != rep3

  test "JSON serialization":
    let rep = newReplicaDescriptor(RangeNodeID(42), ReplicaID(7), rtNonVoter)
    let json = rep.toJson()
    check json["nodeId"].getInt() == 42
    check json["replicaId"].getInt() == 7
    check json["replicaType"].getInt() == ord(rtNonVoter)

  test "JSON deserialization":
    let json = %*{"nodeId": 42, "replicaId": 7, "replicaType": 1}
    let rep = parseReplicaDescriptor(json)
    check rep.nodeId == RangeNodeID(42)
    check rep.replicaId == ReplicaID(7)
    check rep.replicaType == rtNonVoter

suite "RangeDescriptor":
  test "create basic descriptor":
    let desc = newRangeDescriptor(RangeID(1), @[byte 0x00], @[byte 0xFF])
    check desc.rangeId == RangeID(1)
    check desc.startKey == @[byte 0x00]
    check desc.endKey == @[byte 0xFF]
    check desc.replicas.len == 0
    check desc.generation == 1

  test "add replica":
    let desc = newRangeDescriptor(RangeID(1), @[byte 0x00], @[byte 0xFF])
    let rep1 = desc.addReplica(RangeNodeID(1))
    check desc.replicas.len == 1
    check rep1.nodeId == RangeNodeID(1)
    check rep1.replicaId == ReplicaID(1)
    check desc.generation == 2

    let rep2 = desc.addReplica(RangeNodeID(2))
    check desc.replicas.len == 2
    check rep2.replicaId == ReplicaID(2)
    check desc.generation == 3

  test "add non-voter replica":
    let desc = newRangeDescriptor(RangeID(1), @[byte 0x00], @[byte 0xFF])
    discard desc.addReplica(RangeNodeID(1))
    let rep2 = desc.addReplica(RangeNodeID(2), rtNonVoter)
    check rep2.replicaType == rtNonVoter

  test "remove replica":
    let desc = newRangeDescriptor(RangeID(1), @[byte 0x00], @[byte 0xFF])
    discard desc.addReplica(RangeNodeID(1))
    discard desc.addReplica(RangeNodeID(2))
    check desc.replicas.len == 2
    check desc.generation == 3 # gen starts at 1, +1 for each add

    let removed = desc.removeReplica(ReplicaID(1))
    check removed
    check desc.replicas.len == 1
    check desc.generation == 4 # +1 for remove

  test "get replica by node":
    let desc = newRangeDescriptor(RangeID(1), @[byte 0x00], @[byte 0xFF])
    discard desc.addReplica(RangeNodeID(1))
    discard desc.addReplica(RangeNodeID(2))

    let rep = desc.getReplica(RangeNodeID(1))
    check rep.isSome()
    check rep.get().nodeId == RangeNodeID(1)

    let missing = desc.getReplica(RangeNodeID(99))
    check missing.isNone()

  test "get voters and non-voters":
    let desc = newRangeDescriptor(RangeID(1), @[byte 0x00], @[byte 0xFF])
    discard desc.addReplica(RangeNodeID(1)) # voter
    discard desc.addReplica(RangeNodeID(2)) # voter
    discard desc.addReplica(RangeNodeID(3), rtNonVoter) # non-voter

    let voters = desc.getVoters()
    check voters.len == 2

    let nonVoters = desc.getNonVoters()
    check nonVoters.len == 1

  test "contains key":
    let desc = newRangeDescriptor(RangeID(1), @[byte 0x00], @[byte 0x10])
    check desc.containsKey(@[byte 0x05])
    check desc.containsKey(@[byte 0x00])
    check not desc.containsKey(@[byte 0x10])
    check not desc.containsKey(@[byte 0xFF])

  test "quorum size":
    let desc = newRangeDescriptor(RangeID(1), @[byte 0x00], @[byte 0xFF])
    discard desc.addReplica(RangeNodeID(1))
    discard desc.addReplica(RangeNodeID(2))
    discard desc.addReplica(RangeNodeID(3))
    check desc.quorumSize() == 2 # majority of 3

  test "is initialized":
    var desc = newRangeDescriptor(RangeID(0), @[byte 0x00], @[byte 0xFF])
    check not desc.isInitialized()

    desc = newRangeDescriptor(RangeID(1), @[byte 0x00], @[byte 0xFF])
    discard desc.addReplica(RangeNodeID(1))
    check desc.isInitialized()

  test "JSON round-trip":
    let desc = newRangeDescriptor(RangeID(42), @[byte 0x00], @[byte 0xFF])
    discard desc.addReplica(RangeNodeID(1))
    discard desc.addReplica(RangeNodeID(2))
    discard desc.addReplica(RangeNodeID(3))

    let json = desc.toJson()
    let parsed = parseRangeDescriptor(json)

    check parsed.rangeId == desc.rangeId
    check parsed.startKey == desc.startKey
    check parsed.endKey == desc.endKey
    check parsed.replicas.len == desc.replicas.len
    check parsed.generation == desc.generation

suite "Key Encoding":
  test "range prefix":
    let prefix = encodeRangePrefix(RangeID(123))
    check prefix == "/range/123/"

  test "data key":
    let key = encodeDataKey(RangeID(123), @[byte 0x01, 0x02])
    check key.startsWith("/range/123/data/")

  test "log key":
    let key = encodeLogKey(RangeID(456), 789'u64)
    check key == "/raft/456/log/789"

  test "state key":
    let key = encodeStateKey(RangeID(456))
    check key == "/raft/456/state"

  test "snapshot key":
    let key = encodeSnapshotKey(RangeID(456))
    check key == "/raft/456/snapshot"

  test "parse log index":
    let index = parseLogIndex("/raft/456/log/789")
    check index == 789'u64

    expect ValueError:
      discard parseLogIndex("/invalid/key")
