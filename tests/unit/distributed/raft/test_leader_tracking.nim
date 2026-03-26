# Unit tests for group leader tracking in sys.groups
#
# Tests:
#   - onLeaderChanged callback fires and persists leader in sys.groups
#   - loadGroupMembers reads the "leader" field into groupLeaders table
#   - dataGroupLeaderNodeId atomic field
#   - onLeaderChanged skips meta and data groups

import std/[unittest, os, options, strutils, tables, atomics]
import fractio/core/types except NodeID
import fractio/distributed/raft/group_types
import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/meta/system_tables
import fractio/distributed/meta/system_schemas
import fractio/protocol/raft_store
import fractio/storage/mvcc/types as mvccTypes

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc cleanDir(path: string) =
  try: removeDir(path) except CatchableError: discard
  try: createDir(path) except CatchableError: discard

var testBasePort {.global.} = 23500

proc nextBasePort(): int =
  result = testBasePort
  testBasePort += 100

proc makeStore(storagePath: string): tuple[
    coord: NuRaftCoordinator, store: RaftKVStoreExt] =
  cleanDir(storagePath)
  let nodeId = NodeID(1)
  let port = nextBasePort()
  let members = @[(nodeId: 1'u32, host: "127.0.0.1", port: port)]
  let coord = newNuRaftCoordinator(nuraft_coordinator.CoordinatorConfig(
    nodeId: nodeId, port: port, host: "127.0.0.1", dataDir: storagePath,
    electionTimeoutLowerMs: 200, electionTimeoutUpperMs: 400,
    heartbeatIntervalMs: 100,
  ))
  coord.start()
  for rid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    doAssert coord.createAndStartGroup(rid, members)
  for attempt in 0 ..< 50:
    if coord.isLeader(META_GROUP_ID) and coord.isLeader(
        DATA_GROUP_START_ID): break
    os.sleep(100)
  let store = newRaftKVStoreExt(coord, proposeTimeoutMs = 2000)
  store.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])
  (coord, store)

# ---------------------------------------------------------------------------
# Suite: loadGroupMembers reads "leader" field
# Uses replica nodeIds (10, 20) that don't match the coordinator's nodeId (1),
# so onGroupMetadataApplied won't try to auto-create NuRaft groups.
# ---------------------------------------------------------------------------

suite "RaftKVStoreExt - groupLeaders table":
  var coord: NuRaftCoordinator
  var store: RaftKVStoreExt
  let testDir = "/tmp/fractio_leader_grp_" & $getCurrentProcessId()

  setup:
    (coord, store) = makeStore(testDir)

  teardown:
    coord.stop()
    cleanDir(testDir)

  test "groupLeaders starts empty":
    check store.groupLeaders.len == 0

  test "loadGroupMembers populates groupLeaders from sys.groups":
    let gid = genGroupID()
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $groupIDToULID(gid))
    let val = GroupRecord(
      groupId: groupIDToULID(gid),
      spaceId: ZeroULID(),
      preferredLeader: 0,
      leader: 20,
      replicas: @[
        GroupReplicaBin(nodeId: 10, replicaType: rtVoter),
        GroupReplicaBin(nodeId: 20, replicaType: rtVoter),
      ]
    )
    let wr = store.raftPut(key, encode(val))
    check wr.isOk

    store.loadGroupMembers()

    check store.groupLeaders.hasKey(gid)
    check store.groupLeaders[gid] == 20'u32

  test "loadGroupMembers skips leader when field is missing":
    let gid = genGroupID()
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $groupIDToULID(gid))
    let val = GroupRecord(
      groupId: groupIDToULID(gid),
      spaceId: ZeroULID(),
      preferredLeader: 0,
      leader: 0, # 0 = no leader known
      replicas: @[GroupReplicaBin(nodeId: 10, replicaType: rtVoter)]
    )
    discard store.raftPut(key, encode(val))
    store.loadGroupMembers()

    check not store.groupLeaders.hasKey(gid)

  test "loadGroupMembers skips leader when value is 0":
    let gid = genGroupID()
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $groupIDToULID(gid))
    let val = GroupRecord(
      groupId: groupIDToULID(gid),
      spaceId: ZeroULID(),
      preferredLeader: 0,
      leader: 0, # 0 = no leader known
      replicas: @[GroupReplicaBin(nodeId: 10, replicaType: rtVoter)]
    )
    discard store.raftPut(key, encode(val))
    store.loadGroupMembers()

    check not store.groupLeaders.hasKey(gid)

  test "loadGroupMembers clears old groupLeaders on reload":
    let gid = genGroupID()
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $groupIDToULID(gid))
    let val = GroupRecord(
      groupId: groupIDToULID(gid),
      spaceId: ZeroULID(),
      preferredLeader: 0,
      leader: 30,
      replicas: @[GroupReplicaBin(nodeId: 10, replicaType: rtVoter)]
    )
    discard store.raftPut(key, encode(val))
    store.loadGroupMembers()
    check store.groupLeaders.hasKey(gid)
    check store.groupLeaders[gid] == 30'u32

    # Delete the group entry and reload
    discard store.raftDelete(key)
    store.loadGroupMembers()
    check not store.groupLeaders.hasKey(gid)

# ---------------------------------------------------------------------------
# Suite: dataGroupLeaderNodeId atomic field
# ---------------------------------------------------------------------------

suite "RaftKVStoreExt - dataGroupLeaderNodeId":
  var coord: NuRaftCoordinator
  var store: RaftKVStoreExt
  let testDir = "/tmp/fractio_leader_dgln_" & $getCurrentProcessId()

  setup:
    (coord, store) = makeStore(testDir)

  teardown:
    coord.stop()
    cleanDir(testDir)

  test "dataGroupLeaderNodeId starts at zero":
    check store.dataGroupLeaderNodeId.load() == 0'u32

  test "dataGroupLeaderNodeId can be stored and loaded":
    store.dataGroupLeaderNodeId.store(42)
    check store.dataGroupLeaderNodeId.load() == 42'u32

# ---------------------------------------------------------------------------
# Suite: onLeaderChanged callback
# Uses replica nodeIds (10, 20, 30) that don't match the coordinator's
# nodeId (1), so onGroupMetadataApplied won't auto-create NuRaft groups
# that would interfere with leader assertions.
# ---------------------------------------------------------------------------

suite "onLeaderChanged callback":
  var coord: NuRaftCoordinator
  var store: RaftKVStoreExt
  let testDir = "/tmp/fractio_leader_cb_" & $getCurrentProcessId()

  setup:
    (coord, store) = makeStore(testDir)

  teardown:
    coord.stop()
    cleanDir(testDir)

  test "callback is registered after wireApplyCallback":
    check nuraft_coordinator.onLeaderChanged != nil

  test "callback persists leader in sys.groups for space group":
    # Create a sys.groups record for a space group with non-local replicas
    let gid = genGroupID()
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $groupIDToULID(gid))
    let val = GroupRecord(
      groupId: groupIDToULID(gid),
      spaceId: ZeroULID(),
      preferredLeader: 0,
      leader: 0,
      replicas: @[
        GroupReplicaBin(nodeId: 10, replicaType: rtVoter),
        GroupReplicaBin(nodeId: 20, replicaType: rtVoter),
      ]
    )
    let wr = store.raftPut(key, encode(val))
    check wr.isOk

    # Simulate the onLeaderChanged callback (node 20 won election for group 100)
    let storePtr = cast[pointer](store)
    nuraft_coordinator.onLeaderChanged(storePtr, gid, NodeID(20))

    # Read back the sys.groups record and verify the leader field
    let gr = store.raftGet(key)
    check gr.isOk
    check gr.value.isSome
    var rawVal = gr.value.get().value
    # Strip MVCC encoding if present (sysTablePut wraps values in MVCC)
    if mvccTypes.isLikelyMVCCValue(rawVal):
      let mvccVal = mvccTypes.decodeMVCCValue(rawVal)
      rawVal = mvccVal.data
    let rec = decodeGroupRecord(rawVal)
    check rec.leader == 20

  test "callback updates existing leader field":
    let gid = genGroupID()
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $groupIDToULID(gid))
    let val = GroupRecord(
      groupId: groupIDToULID(gid),
      spaceId: ZeroULID(),
      preferredLeader: 0,
      leader: 10,
      replicas: @[
        GroupReplicaBin(nodeId: 10, replicaType: rtVoter),
        GroupReplicaBin(nodeId: 20, replicaType: rtVoter),
        GroupReplicaBin(nodeId: 30, replicaType: rtVoter),
      ]
    )
    discard store.raftPut(key, encode(val))

    # Node 30 wins election
    let storePtr = cast[pointer](store)
    nuraft_coordinator.onLeaderChanged(storePtr, gid, NodeID(30))

    let gr = store.raftGet(key)
    check gr.isOk
    var rawVal = gr.value.get().value
    # Strip MVCC encoding if present (sysTablePut wraps values in MVCC)
    if mvccTypes.isLikelyMVCCValue(rawVal):
      let mvccVal = mvccTypes.decodeMVCCValue(rawVal)
      rawVal = mvccVal.data
    let rec = decodeGroupRecord(rawVal)
    check rec.leader == 30

  test "callback persists leader for META_GROUP_ID":
    # META_GROUP_ID leader IS persisted so clients can route to it
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $groupIDToULID(META_GROUP_ID))
    let val = GroupRecord(
      groupId: groupIDToULID(META_GROUP_ID),
      spaceId: ZeroULID(),
      preferredLeader: 0,
      leader: 0,
      replicas: @[GroupReplicaBin(nodeId: 10, replicaType: rtVoter)]
    )
    discard store.raftPut(key, encode(val))

    # Fire callback for META_GROUP_ID
    let storePtr = cast[pointer](store)
    nuraft_coordinator.onLeaderChanged(storePtr, META_GROUP_ID, NodeID(10))

    # Leader SHOULD be updated (clients need to route to meta leader)
    let gr = store.raftGet(key)
    check gr.isOk
    var rawVal = gr.value.get().value
    # Strip MVCC encoding if present (sysTablePut wraps values in MVCC)
    if mvccTypes.isLikelyMVCCValue(rawVal):
      let mvccVal = mvccTypes.decodeMVCCValue(rawVal)
      rawVal = mvccVal.data
    let rec = decodeGroupRecord(rawVal)
    check rec.leader == 10 # Updated to 10

  test "callback skips DATA_GROUP_START_ID":
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $groupIDToULID(DATA_GROUP_START_ID))
    let val = GroupRecord(
      groupId: groupIDToULID(DATA_GROUP_START_ID),
      spaceId: ZeroULID(),
      preferredLeader: 0,
      leader: 0,
      replicas: @[GroupReplicaBin(nodeId: 10, replicaType: rtVoter)]
    )
    discard store.raftPut(key, encode(val))

    let storePtr = cast[pointer](store)
    nuraft_coordinator.onLeaderChanged(storePtr, DATA_GROUP_START_ID, NodeID(10))

    let gr = store.raftGet(key)
    check gr.isOk
    var rawVal = gr.value.get().value
    # Strip MVCC encoding if present (sysTablePut wraps values in MVCC)
    if mvccTypes.isLikelyMVCCValue(rawVal):
      let mvccVal = mvccTypes.decodeMVCCValue(rawVal)
      rawVal = mvccVal.data
    let rec = decodeGroupRecord(rawVal)
    check rec.leader == 0 # Still 0 (not updated)

  test "callback ignores nil storePtr":
    # Should not crash with nil pointer
    let gid = genGroupID()
    nuraft_coordinator.onLeaderChanged(nil, gid, NodeID(10))

  test "callback is no-op when sys.groups record does not exist":
    # Fire callback for a group that has no sys.groups record
    let gid = genGroupID()
    let storePtr = cast[pointer](store)
    nuraft_coordinator.onLeaderChanged(storePtr, gid, NodeID(10))

    # Should not create a record
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $groupIDToULID(gid))
    let gr = store.raftGet(key)
    check gr.isOk
    check gr.value.isNone

  test "loadGroupMembers reads back persisted leader after callback":
    let gid = genGroupID()
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $groupIDToULID(gid))
    let val = GroupRecord(
      groupId: groupIDToULID(gid),
      spaceId: ZeroULID(),
      preferredLeader: 0,
      leader: 0,
      replicas: @[
        GroupReplicaBin(nodeId: 10, replicaType: rtVoter),
        GroupReplicaBin(nodeId: 20, replicaType: rtVoter),
      ]
    )
    discard store.raftPut(key, encode(val))

    # Simulate leader election
    let storePtr = cast[pointer](store)
    nuraft_coordinator.onLeaderChanged(storePtr, gid, NodeID(20))

    # Reload group members and verify groupLeaders
    store.loadGroupMembers()
    check store.groupLeaders.hasKey(gid)
    check store.groupLeaders[gid] == 20'u32
