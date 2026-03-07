# Unit tests for Multi-Group Raft Types

import std/unittest
import std/atomics

import fractio/distributed/range/types
import fractio/distributed/raft/multigroup_types

suite "RaftState":
  test "state transitions":
    check rsFollower.ord < rsCandidate.ord
    check rsCandidate.ord < rsLeader.ord

suite "CommandKind":
  test "all command kinds":
    let kinds = {ckNoop, ckWrite, ckSplit, ckMerge, ckChangeReplicas,
                 ckTransferLease, ckAcquireLease}
    check kinds.card == 7

suite "WriteBatch":
  test "create empty batch":
    let batch = newWriteBatch()
    check batch.isEmpty()
    check batch.len == 0

  test "add puts":
    let batch = newWriteBatch()
    batch.put(@[byte 0x01], @[byte 0x02])
    batch.put(@[byte 0x03], @[byte 0x04])
    check batch.puts.len == 2
    check batch.deletes.len == 0
    check batch.len == 2

  test "add deletes":
    let batch = newWriteBatch()
    batch.delete(@[byte 0x01])
    batch.delete(@[byte 0x02])
    check batch.puts.len == 0
    check batch.deletes.len == 2
    check batch.len == 2

  test "mixed operations":
    let batch = newWriteBatch()
    batch.put(@[byte 0x01], @[byte 0x02])
    batch.delete(@[byte 0x03])
    check batch.len == 2
    check not batch.isEmpty()

suite "LogEntry":
  test "create noop entry":
    let entry = newNoopEntry(1'u64, 1'u64)
    check entry.term == 1
    check entry.index == 1
    check entry.command.kind == ckNoop

  test "create write entry":
    let batch = newWriteBatch()
    batch.put(@[byte 0x01], @[byte 0x02])
    let entry = newWriteEntry(1'u64, 1'u64, batch)
    check entry.command.kind == ckWrite
    check entry.command.writeBatch.puts.len == 1

  test "create split entry":
    let entry = newLogEntry(1'u64, 1'u64, RaftCommand(
      kind: ckSplit,
      splitKey: @[byte 0x05],
      newRangeId: RangeID(2)
    ))
    check entry.command.kind == ckSplit
    check entry.command.splitKey == @[byte 0x05]
    check entry.command.newRangeId == RangeID(2)

suite "RaftGroup":
  test "create group":
    let desc = newRangeDescriptor(RangeID(1), @[byte 0x00], @[byte 0xFF])
    discard desc.addReplica(RangeNodeID(1))
    discard desc.addReplica(RangeNodeID(2))
    discard desc.addReplica(RangeNodeID(3))

    let group = newRaftGroup(RangeID(1), RangeNodeID(1), ReplicaID(1), desc)
    check group.rangeId == RangeID(1)
    check group.nodeId == RangeNodeID(1)
    check group.replicaId == ReplicaID(1)
    check group.state.load() == rsFollower
    check group.getTerm() == 0
    group.close()

  test "state transitions":
    let desc = newRangeDescriptor(RangeID(1), @[byte 0x00], @[byte 0xFF])
    discard desc.addReplica(RangeNodeID(1))

    let group = newRaftGroup(RangeID(1), RangeNodeID(1), ReplicaID(1), desc)

    # Start as follower
    check group.state.load() == rsFollower
    check not group.isLeader()

    # Become candidate
    group.becomeCandidate()
    check group.state.load() == rsCandidate
    check group.getTerm() == 1

    # Become leader
    group.becomeLeader()
    check group.state.load() == rsLeader
    check group.isLeader()

    # Become follower again
    group.becomeFollower(2'u64)
    check group.state.load() == rsFollower
    check group.getTerm() == 2
    check not group.isLeader()

    group.close()

  test "quorum calculation":
    let desc = newRangeDescriptor(RangeID(1), @[byte 0x00], @[byte 0xFF])
    discard desc.addReplica(RangeNodeID(1))
    discard desc.addReplica(RangeNodeID(2))
    discard desc.addReplica(RangeNodeID(3))

    let group = newRaftGroup(RangeID(1), RangeNodeID(1), ReplicaID(1), desc)
    check group.quorum() == 2 # Majority of 3
    check group.hasQuorum(2)
    check not group.hasQuorum(1)

    group.close()

  test "heartbeat tracking":
    let desc = newRangeDescriptor(RangeID(1), @[byte 0x00], @[byte 0xFF])
    discard desc.addReplica(RangeNodeID(1))

    let group = newRaftGroup(RangeID(1), RangeNodeID(1), ReplicaID(1), desc)

    group.updateHeartbeat()
    let elapsed = group.timeSinceHeartbeat()
    check elapsed >= 0
    check elapsed < 1_000_000_000 # Less than 1 second

    group.close()

suite "Lease":
  test "lease creation":
    let lease = Lease(
      leaseholder: RangeNodeID(1),
      startTs: 0,
      expirationTs: 1_000_000_000
    )
    check lease.leaseholder == RangeNodeID(1)

  test "lease states":
    check lsNone.ord < lsAcquiring.ord
    check lsAcquiring.ord < lsHeld.ord
    check lsHeld.ord < lsTransferring.ord
    check lsTransferring.ord < lsExpired.ord

suite "RaftResult":
  test "success result":
    let result = RaftResult(success: true, index: 42)
    check result.success
    check result.index == 42
    check result.error == ""

  test "error result":
    let result = RaftResult(success: false, error: "Not leader")
    check not result.success
    check result.error == "Not leader"

suite "Errors":
  test "error hierarchy":
    var err = newException(MultiRaftError, "test")
    check err.msg == "test"

    err = newException(NotLeaderError, "not leader")
    check err of MultiRaftError

    err = newException(RangeNotFoundError, "range not found")
    check err of MultiRaftError
