# Unit tests for Preferred Leader support
#
# Tests:
#   - preferredLeaders table on RaftKVStoreExt (loadGroupMembers parsing)
#   - getPreferredLeaderCallback wiring

import std/[unittest, os, options, json, strutils, tables, atomics]
import fractio/core/types except NodeID
import fractio/distributed/raft/group_types
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/meta/system_tables
import fractio/distributed/meta/system_schemas
import fractio/protocol/raft_store

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc cleanDir(path: string) =
  try: removeDir(path) except CatchableError: discard
  try: createDir(path) except CatchableError: discard

var testBasePort {.global.} = 23000

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

proc teardown(coord: NuRaftCoordinator, path: string) =
  coord.stop()
  try: removeDir(path) except CatchableError: discard

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

    # Seed sys.groups with a group that has preferredLeader (binary format)
    let gid = genGroupIDLocal()
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $groupIDToULID(gid))
    let val = GroupRecord(
      groupId: groupIDToULID(gid),
      spaceId: zeroSpaceID(),
      preferredLeader: 20,
      leader: 0,
      replicas: @[
        GroupReplicaBin(nodeId: 10, replicaType: rtVoter),
        GroupReplicaBin(nodeId: 20, replicaType: rtVoter),
        GroupReplicaBin(nodeId: 30, replicaType: rtVoter),
      ]
    )
    let wr = store.raftPut(key, encode(val))
    check wr.isOk

    store.loadGroupMembers()

    check store.preferredLeaders.hasKey(gid)
    check store.preferredLeaders[gid] == 20'u32

  test "loadGroupMembers skips preferredLeader when field is missing":
    let path = "/tmp/fractio_pref_lead_t12"
    let (coord, store) = makeStore(path)
    defer: teardown(coord, path)

    let gid = genGroupIDLocal()
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $groupIDToULID(gid))
    let val = GroupRecord(
      groupId: groupIDToULID(gid),
      spaceId: zeroSpaceID(),
      preferredLeader: 0, # 0 = no preferred leader
      leader: 0,
      replicas: @[GroupReplicaBin(nodeId: 10, replicaType: rtVoter)]
    )
    discard store.raftPut(key, encode(val))
    store.loadGroupMembers()

    check not store.preferredLeaders.hasKey(gid)

  test "loadGroupMembers skips preferredLeader when value is 0":
    let path = "/tmp/fractio_pref_lead_t13"
    let (coord, store) = makeStore(path)
    defer: teardown(coord, path)

    let gid = genGroupIDLocal()
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $groupIDToULID(gid))
    let val = GroupRecord(
      groupId: groupIDToULID(gid),
      spaceId: zeroSpaceID(),
      preferredLeader: 0, # 0 = no preferred leader
      leader: 0,
      replicas: @[GroupReplicaBin(nodeId: 10, replicaType: rtVoter)]
    )
    discard store.raftPut(key, encode(val))
    store.loadGroupMembers()

    check not store.preferredLeaders.hasKey(gid)

  test "loadGroupMembers clears old preferredLeaders on reload":
    let path = "/tmp/fractio_pref_lead_t14"
    let (coord, store) = makeStore(path)
    defer: teardown(coord, path)

    # Insert a group with preferred leader (binary format)
    let gid = genGroupIDLocal()
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $groupIDToULID(gid))
    let val = GroupRecord(
      groupId: groupIDToULID(gid),
      spaceId: zeroSpaceID(),
      preferredLeader: 3,
      leader: 0,
      replicas: @[GroupReplicaBin(nodeId: 10, replicaType: rtVoter)]
    )
    discard store.raftPut(key, encode(val))
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
    let testGid = genGroupIDLocal()
    store.preferredLeaders[testGid] = 3'u32

    # The callback should have been wired by bootstrapStore -> wireApplyCallback
    check getPreferredLeaderCallback != nil

    let result = getPreferredLeaderCallback(
      cast[pointer](store), testGid)
    check result.isSome
    check result.get == NodeID(3)

  test "callback returns none for unknown group":
    let path = "/tmp/fractio_pref_lead_t21"
    let (coord, store) = makeStore(path)
    defer: teardown(coord, path)

    check getPreferredLeaderCallback != nil
    let result = getPreferredLeaderCallback(
      cast[pointer](store), genGroupIDLocal())
    check result.isNone
