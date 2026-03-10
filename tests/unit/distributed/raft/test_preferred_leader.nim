# Unit tests for Preferred Leader support
#
# Tests:
#   - lastPreferredLeaderStepdownNs field on RaftGroup
#   - transferLeadership proc on coordinator
#   - preferredLeaders table on RaftKVStoreExt (loadGroupMembers parsing)
#   - getPreferredLeaderCallback wiring

import std/[unittest, os, options, json, strutils, tables, atomics]
import fractio/distributed/raft/group_types
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/multigroup_coordinator
import fractio/distributed/meta/system_tables
import fractio/protocol/raft_store

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc cleanDir(path: string) =
  try: removeDir(path) except CatchableError: discard
  try: createDir(path) except CatchableError: discard

proc makeStore(storagePath: string): tuple[
    coord: MultiRaftCoordinator, store: RaftKVStoreExt] =
  cleanDir(storagePath)
  let cfg = CoordinatorConfig(
    nodeId: NodeID(1),
    numWorkers: 1,
    electionTimeoutNs: DEFAULT_ELECTION_TIMEOUT_NS,
    heartbeatIntervalNs: DEFAULT_HEARTBEAT_INTERVAL_NS,
    storagePath: storagePath,
  )
  let coord = newMultiRaftCoordinator(cfg)
  for rid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    let desc = newGroupDescriptor(rid)
    let rep = desc.addReplica(NodeID(1))
    let group = coord.createGroup(desc, rep.replicaId)
    group.becomeLeader()
  coord.start()
  let store = newRaftKVStoreExt(coord, proposeTimeoutMs = 2000)
  store.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])
  (coord, store)

proc teardown(coord: MultiRaftCoordinator, path: string) =
  coord.stop()
  try: removeDir(path) except CatchableError: discard

# ---------------------------------------------------------------------------
# Suite: RaftGroup preferred leader stepdown field
# ---------------------------------------------------------------------------

suite "RaftGroup - preferredLeaderStepdown field":
  test "lastPreferredLeaderStepdownNs initialises to zero":
    let desc = newGroupDescriptor(GroupID(1))
    discard desc.addReplica(NodeID(1))
    let group = newRaftGroup(GroupID(1), NodeID(1), ReplicaID(1), desc)
    check group.lastPreferredLeaderStepdownNs.load() == 0
    group.close()

  test "lastPreferredLeaderStepdownNs can be stored and loaded":
    let desc = newGroupDescriptor(GroupID(1))
    discard desc.addReplica(NodeID(1))
    let group = newRaftGroup(GroupID(1), NodeID(1), ReplicaID(1), desc)
    group.lastPreferredLeaderStepdownNs.store(123_456_789)
    check group.lastPreferredLeaderStepdownNs.load() == 123_456_789
    group.close()

# ---------------------------------------------------------------------------
# Suite: transferLeadership
# ---------------------------------------------------------------------------

suite "MultiRaftCoordinator - transferLeadership":
  test "transferLeadership steps down leader for target node":
    let path = "/tmp/fractio_pref_lead_t01"
    cleanDir(path)
    let cfg = CoordinatorConfig(
      nodeId: NodeID(1),
      numWorkers: 1,
      electionTimeoutNs: DEFAULT_ELECTION_TIMEOUT_NS,
      heartbeatIntervalNs: DEFAULT_HEARTBEAT_INTERVAL_NS,
      storagePath: path,
    )
    let coord = newMultiRaftCoordinator(cfg)
    let desc = newGroupDescriptor(GroupID(10))
    discard desc.addReplica(NodeID(1))
    discard desc.addReplica(NodeID(2))
    discard desc.addReplica(NodeID(3))
    let rep = desc.getReplica(NodeID(1)).get
    let group = coord.createGroup(desc, rep.replicaId)
    group.becomeLeader()
    coord.start()
    defer:
      coord.stop()
      try: removeDir(path) except CatchableError: discard

    check group.isLeader()
    let ok = coord.transferLeadership(GroupID(10), NodeID(2))
    check ok
    check not group.isLeader()
    check group.state.load() == rsFollower
    check group.lastPreferredLeaderStepdownNs.load() > 0

  test "transferLeadership returns false when not leader":
    let path = "/tmp/fractio_pref_lead_t02"
    cleanDir(path)
    let cfg = CoordinatorConfig(
      nodeId: NodeID(1),
      numWorkers: 1,
      electionTimeoutNs: DEFAULT_ELECTION_TIMEOUT_NS,
      heartbeatIntervalNs: DEFAULT_HEARTBEAT_INTERVAL_NS,
      storagePath: path,
    )
    let coord = newMultiRaftCoordinator(cfg)
    let desc = newGroupDescriptor(GroupID(10))
    discard desc.addReplica(NodeID(1))
    discard desc.addReplica(NodeID(2))
    let rep = desc.getReplica(NodeID(1)).get
    discard coord.createGroup(desc, rep.replicaId)
    # group stays follower
    coord.start()
    defer:
      coord.stop()
      try: removeDir(path) except CatchableError: discard

    let ok = coord.transferLeadership(GroupID(10), NodeID(2))
    check not ok

  test "transferLeadership returns false when target is self":
    let path = "/tmp/fractio_pref_lead_t03"
    cleanDir(path)
    let cfg = CoordinatorConfig(
      nodeId: NodeID(1),
      numWorkers: 1,
      electionTimeoutNs: DEFAULT_ELECTION_TIMEOUT_NS,
      heartbeatIntervalNs: DEFAULT_HEARTBEAT_INTERVAL_NS,
      storagePath: path,
    )
    let coord = newMultiRaftCoordinator(cfg)
    let desc = newGroupDescriptor(GroupID(10))
    discard desc.addReplica(NodeID(1))
    discard desc.addReplica(NodeID(2))
    let rep = desc.getReplica(NodeID(1)).get
    let group = coord.createGroup(desc, rep.replicaId)
    group.becomeLeader()
    coord.start()
    defer:
      coord.stop()
      try: removeDir(path) except CatchableError: discard

    # Target is self — should not step down
    let ok = coord.transferLeadership(GroupID(10), NodeID(1))
    check not ok
    check group.isLeader()

  test "transferLeadership returns false when target not a member":
    let path = "/tmp/fractio_pref_lead_t04"
    cleanDir(path)
    let cfg = CoordinatorConfig(
      nodeId: NodeID(1),
      numWorkers: 1,
      electionTimeoutNs: DEFAULT_ELECTION_TIMEOUT_NS,
      heartbeatIntervalNs: DEFAULT_HEARTBEAT_INTERVAL_NS,
      storagePath: path,
    )
    let coord = newMultiRaftCoordinator(cfg)
    let desc = newGroupDescriptor(GroupID(10))
    discard desc.addReplica(NodeID(1))
    discard desc.addReplica(NodeID(2))
    let rep = desc.getReplica(NodeID(1)).get
    let group = coord.createGroup(desc, rep.replicaId)
    group.becomeLeader()
    coord.start()
    defer:
      coord.stop()
      try: removeDir(path) except CatchableError: discard

    # NodeID(99) is not a member
    let ok = coord.transferLeadership(GroupID(10), NodeID(99))
    check not ok
    check group.isLeader()

  test "transferLeadership returns false for unknown group":
    let path = "/tmp/fractio_pref_lead_t05"
    cleanDir(path)
    let cfg = CoordinatorConfig(
      nodeId: NodeID(1),
      numWorkers: 1,
      electionTimeoutNs: DEFAULT_ELECTION_TIMEOUT_NS,
      heartbeatIntervalNs: DEFAULT_HEARTBEAT_INTERVAL_NS,
      storagePath: path,
    )
    let coord = newMultiRaftCoordinator(cfg)
    coord.start()
    defer:
      coord.stop()
      try: removeDir(path) except CatchableError: discard

    let ok = coord.transferLeadership(GroupID(999), NodeID(2))
    check not ok

# ---------------------------------------------------------------------------
# Suite: preferredLeaders table in RaftKVStoreExt
# ---------------------------------------------------------------------------

suite "RaftKVStoreExt - preferredLeaders table":
  test "preferredLeaders starts empty":
    let path = "/tmp/fractio_pref_lead_t10"
    let (coord, store) = makeStore(path)
    defer: teardown(coord, path)
    check store.preferredLeaders.len == 0

  test "loadGroupMembers populates preferredLeaders from sys.groups":
    let path = "/tmp/fractio_pref_lead_t11"
    let (coord, store) = makeStore(path)
    defer: teardown(coord, path)

    # Seed sys.groups with a group that has preferredLeader
    let gid = GroupID(42)
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $gid.uint64)
    let val = $ %*{
      "groupId": gid.uint64.int,
      "replicas": [
        {"nodeId": 1, "type": "voter"},
        {"nodeId": 2, "type": "voter"},
        {"nodeId": 3, "type": "voter"},
      ],
      "preferredLeader": 2,
    }
    let wr = store.raftPut(key, val)
    check wr.isOk

    store.loadGroupMembers()

    check store.preferredLeaders.hasKey(gid)
    check store.preferredLeaders[gid] == 2'u32

  test "loadGroupMembers skips preferredLeader when field is missing":
    let path = "/tmp/fractio_pref_lead_t12"
    let (coord, store) = makeStore(path)
    defer: teardown(coord, path)

    let gid = GroupID(43)
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $gid.uint64)
    let val = $ %*{
      "groupId": gid.uint64.int,
      "replicas": [{"nodeId": 1, "type": "voter"}],
    }
    discard store.raftPut(key, val)
    store.loadGroupMembers()

    check not store.preferredLeaders.hasKey(gid)

  test "loadGroupMembers skips preferredLeader when value is 0":
    let path = "/tmp/fractio_pref_lead_t13"
    let (coord, store) = makeStore(path)
    defer: teardown(coord, path)

    let gid = GroupID(44)
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $gid.uint64)
    let val = $ %*{
      "groupId": gid.uint64.int,
      "replicas": [{"nodeId": 1, "type": "voter"}],
      "preferredLeader": 0,
    }
    discard store.raftPut(key, val)
    store.loadGroupMembers()

    check not store.preferredLeaders.hasKey(gid)

  test "loadGroupMembers clears old preferredLeaders on reload":
    let path = "/tmp/fractio_pref_lead_t14"
    let (coord, store) = makeStore(path)
    defer: teardown(coord, path)

    # Insert a group with preferred leader
    let gid = GroupID(45)
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $gid.uint64)
    let val = $ %*{
      "groupId": gid.uint64.int,
      "replicas": [{"nodeId": 1, "type": "voter"}],
      "preferredLeader": 3,
    }
    discard store.raftPut(key, val)
    store.loadGroupMembers()
    check store.preferredLeaders.hasKey(gid)

    # Delete the group entry and reload
    discard store.raftDelete(key)
    store.loadGroupMembers()
    check not store.preferredLeaders.hasKey(gid)

# ---------------------------------------------------------------------------
# Suite: getPreferredLeaderCallback wiring
# ---------------------------------------------------------------------------

suite "getPreferredLeaderCallback wiring":
  test "callback returns preferred leader when set":
    let path = "/tmp/fractio_pref_lead_t20"
    let (coord, store) = makeStore(path)
    defer: teardown(coord, path)

    # Manually populate preferredLeaders
    store.preferredLeaders[GroupID(50)] = 3'u32

    # The callback should have been wired by bootstrapStore → wireApplyCallback
    check getPreferredLeaderCallback != nil

    let result = getPreferredLeaderCallback(
      cast[pointer](store), GroupID(50))
    check result.isSome
    check result.get == NodeID(3)

  test "callback returns none for unknown group":
    let path = "/tmp/fractio_pref_lead_t21"
    let (coord, store) = makeStore(path)
    defer: teardown(coord, path)

    check getPreferredLeaderCallback != nil
    let result = getPreferredLeaderCallback(
      cast[pointer](store), GroupID(999))
    check result.isNone

# ---------------------------------------------------------------------------
# Suite: PREFERRED_LEADER_STEPDOWN_COOLDOWN_NS constant
# ---------------------------------------------------------------------------

suite "Preferred leader constants":
  test "cooldown is 10 seconds":
    check PREFERRED_LEADER_STEPDOWN_COOLDOWN_NS == 10_000_000_000'i64
