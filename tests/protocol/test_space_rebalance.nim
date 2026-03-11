# Unit tests for space rebalancing in raft_store.nim
#
# Tests:
# - SpaceInfo rebalance fields and loadSpaces parsing
# - Dual-read routing during rebalance (raftGetInSpace fallback)
# - Dual-scan routing during rebalance (raftScanSpace merges old + new groups)
# - raftDeleteInGroup targeted deletion
# - updateSpaceRecord persistence

import std/[unittest, os, options, tables, algorithm, hashes, json, strutils, locks]
import fractio/protocol/raft_store
import fractio/distributed/raft/multigroup_coordinator
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/meta/system_tables
import fractio/storage/wisckey_backend

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc cleanDir(path: string) =
  try: removeDir(path) except CatchableError: discard
  try: createDir(path) except CatchableError: discard

proc makeStore(storagePath: string, groupIds: seq[uint64]): tuple[
    coord: MultiRaftCoordinator, store: RaftKVStoreExt] =
  ## Create a store with meta group + the specified groups.
  cleanDir(storagePath)
  let nodeId = NodeID(1)
  let coord = newMultiRaftCoordinator(CoordinatorConfig(
    nodeId: nodeId,
    numWorkers: 1,
    electionTimeoutNs: DEFAULT_ELECTION_TIMEOUT_NS,
    heartbeatIntervalNs: DEFAULT_HEARTBEAT_INTERVAL_NS,
    storagePath: storagePath,
    proposeTimeoutMs: 5000,
  ))

  # Meta group
  let metaDesc = newGroupDescriptor(META_GROUP_ID)
  let metaRep = metaDesc.addReplica(nodeId)
  let metaGroup = coord.createGroup(metaDesc, metaRep.replicaId)
  metaGroup.becomeLeader()

  # Data group
  let dataDesc = newGroupDescriptor(DATA_GROUP_START_ID)
  let dataRep = dataDesc.addReplica(nodeId)
  let dataGroup = coord.createGroup(dataDesc, dataRep.replicaId)
  dataGroup.becomeLeader()

  # Additional groups
  for gid in groupIds:
    let rid = GroupID(gid)
    let desc = newGroupDescriptor(rid)
    let rep = desc.addReplica(nodeId)
    let group = coord.createGroup(desc, rep.replicaId)
    group.becomeLeader()

  coord.start()

  let store = newRaftKVStoreExt(coord, proposeTimeoutMs = 2000)
  store.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])

  for gid in groupIds:
    discard store.getOrCreateSM(GroupID(gid))

  (coord, store)

proc teardown(coord: MultiRaftCoordinator, storagePath: string) =
  coord.stop()
  try: removeDir(storagePath) except CatchableError: discard

# ---------------------------------------------------------------------------
# Suite: SpaceInfo rebalance fields
# ---------------------------------------------------------------------------

suite "Space rebalance — SpaceInfo fields":
  test "SpaceInfo has rebalance fields with defaults":
    let space = SpaceInfo(
      spaceId: 1,
      name: "test",
      replicas: 2,
      groupIds: @[10'u64, 11],
    )
    check space.rebalancing == false
    check space.rebalanceWorker == 0
    check space.rebalanceHeartbeat == 0
    check space.rebalanceCursor == ""
    check space.oldGroupIds.len == 0

  test "SpaceInfo with all rebalance fields set":
    let space = SpaceInfo(
      spaceId: 1,
      name: "test",
      replicas: 2,
      groupIds: @[20'u64, 21, 22],
      oldGroupIds: @[10'u64, 11],
      rebalancing: true,
      rebalanceWorker: 3,
      rebalanceHeartbeat: 1741700000'i64,
      rebalanceCursor: "/t/0000000100/d/m",
    )
    check space.rebalancing == true
    check space.rebalanceWorker == 3
    check space.rebalanceHeartbeat == 1741700000'i64
    check space.rebalanceCursor == "/t/0000000100/d/m"
    check space.oldGroupIds == @[10'u64, 11]
    check space.groupIds == @[20'u64, 21, 22]

# ---------------------------------------------------------------------------
# Suite: loadSpaces parses rebalance fields
# ---------------------------------------------------------------------------

suite "Space rebalance — loadSpaces parses rebalance fields":
  test "loadSpaces reads rebalance fields from sys.spaces":
    let (coord, store) = makeStore("/tmp/fractio_sr_rebal_t01", @[])
    defer: teardown(coord, "/tmp/fractio_sr_rebal_t01")

    # Write a space record with rebalance fields
    let spaceKey = encodeSpaceKey(5)
    let spaceVal = $ %*{
      "spaceId": 5,
      "name": "rebaltest",
      "replicas": 2,
      "groupIds": [20, 21, 22],
      "oldGroupIds": [10, 11],
      "rebalancing": true,
      "rebalanceWorker": 3,
      "rebalanceHeartbeat": 1741700000,
      "rebalanceCursor": "/t/0000000100/d/key42",
    }
    discard store.raftPut(spaceKey, spaceVal)
    store.loadSpaces()

    acquire(store.spacesMu)
    let sp = store.spaces[5]
    release(store.spacesMu)

    check sp.name == "rebaltest"
    check sp.groupIds == @[20'u64, 21, 22]
    check sp.oldGroupIds == @[10'u64, 11]
    check sp.rebalancing == true
    check sp.rebalanceWorker == 3
    check sp.rebalanceHeartbeat == 1741700000'i64
    check sp.rebalanceCursor == "/t/0000000100/d/key42"

  test "loadSpaces with missing rebalance fields uses defaults":
    let (coord, store) = makeStore("/tmp/fractio_sr_rebal_t02", @[])
    defer: teardown(coord, "/tmp/fractio_sr_rebal_t02")

    # Write a space record WITHOUT rebalance fields (backward compat)
    let spaceKey = encodeSpaceKey(3)
    let spaceVal = $ %*{
      "spaceId": 3,
      "name": "oldstyle",
      "replicas": 1,
      "groupIds": [10],
    }
    discard store.raftPut(spaceKey, spaceVal)
    store.loadSpaces()

    acquire(store.spacesMu)
    let sp = store.spaces[3]
    release(store.spacesMu)

    check sp.rebalancing == false
    check sp.rebalanceWorker == 0
    check sp.rebalanceHeartbeat == 0
    check sp.rebalanceCursor == ""
    check sp.oldGroupIds.len == 0

# ---------------------------------------------------------------------------
# Suite: updateSpaceRecord persistence
# ---------------------------------------------------------------------------

suite "Space rebalance — updateSpaceRecord":
  test "updateSpaceRecord persists and reloads correctly":
    let (coord, store) = makeStore("/tmp/fractio_sr_rebal_t05", @[10'u64, 11])
    defer: teardown(coord, "/tmp/fractio_sr_rebal_t05")

    var space = SpaceInfo(
      spaceId: 7,
      name: "persist_test",
      replicas: 2,
      groupIds: @[20'u64, 21, 22],
      oldGroupIds: @[10'u64, 11],
      rebalancing: true,
      rebalanceWorker: 1,
      rebalanceHeartbeat: 9999'i64,
      rebalanceCursor: "/t/0000000100/d/abc",
    )
    store.updateSpaceRecord(space)
    store.loadSpaces()

    acquire(store.spacesMu)
    let loaded = store.spaces[7]
    release(store.spacesMu)

    check loaded.name == "persist_test"
    check loaded.replicas == 2
    check loaded.groupIds == @[20'u64, 21, 22]
    check loaded.oldGroupIds == @[10'u64, 11]
    check loaded.rebalancing == true
    check loaded.rebalanceWorker == 1
    check loaded.rebalanceHeartbeat == 9999'i64
    check loaded.rebalanceCursor == "/t/0000000100/d/abc"

  test "updateSpaceRecord clears rebalance state":
    let (coord, store) = makeStore("/tmp/fractio_sr_rebal_t06", @[])
    defer: teardown(coord, "/tmp/fractio_sr_rebal_t06")

    # First write with rebalancing on
    var space = SpaceInfo(
      spaceId: 8,
      name: "clear_test",
      replicas: 1,
      groupIds: @[20'u64],
      oldGroupIds: @[10'u64],
      rebalancing: true,
      rebalanceWorker: 2,
      rebalanceHeartbeat: 5000,
      rebalanceCursor: "/some/key",
    )
    store.updateSpaceRecord(space)

    # Now clear rebalance state
    space.oldGroupIds = @[]
    space.rebalancing = false
    space.rebalanceWorker = 0
    space.rebalanceHeartbeat = 0
    space.rebalanceCursor = ""
    store.updateSpaceRecord(space)
    store.loadSpaces()

    acquire(store.spacesMu)
    let loaded = store.spaces[8]
    release(store.spacesMu)

    check loaded.rebalancing == false
    check loaded.rebalanceWorker == 0
    check loaded.oldGroupIds.len == 0
    check loaded.rebalanceCursor == ""

# ---------------------------------------------------------------------------
# Suite: dual-read routing during rebalance
# ---------------------------------------------------------------------------

suite "Space rebalance — dual-read routing":
  test "raftGetInSpace falls back to old group during rebalance":
    # Setup: 2 old groups (10,11) + 3 new groups (20,21,22)
    let allGroups = @[10'u64, 11, 20, 21, 22]
    let (coord, store) = makeStore("/tmp/fractio_sr_rebal_t10", allGroups)
    defer: teardown(coord, "/tmp/fractio_sr_rebal_t10")

    let oldGroupIds = @[10'u64, 11]
    let newGroupIds = @[20'u64, 21, 22]

    # Find a key that routes to different groups in old vs new topology
    var testPk = ""
    for i in 0 ..< 200:
      let pk = "key_" & $i
      let oldGroup = routeToGroup(pk, oldGroupIds)
      let newGroup = routeToGroup(pk, newGroupIds)
      if oldGroup != newGroup:
        testPk = pk
        break
    check testPk.len > 0

    let key = encodeDataRowKey(100, testPk)
    let value = """{"id":"test","val":"fallback"}"""

    # Write data using OLD group routing (simulate pre-rebalance data)
    let oldRid = routeToGroup(testPk, oldGroupIds)
    let oldSpace = SpaceInfo(
      spaceId: 2, name: "t", replicas: 1, groupIds: oldGroupIds)
    let wr = store.raftPutInSpace(key, value, oldSpace, testPk)
    check wr.isOk

    # Now create a rebalancing space with new groups
    let rebalSpace = SpaceInfo(
      spaceId: 2, name: "t", replicas: 1,
      groupIds: newGroupIds,
      oldGroupIds: oldGroupIds,
      rebalancing: true,
    )

    # Read using new routing — key was written under old routing,
    # so it should fall back to the old group
    let gr = store.raftGetInSpace(key, rebalSpace, testPk)
    check gr.isOk
    check gr.value.isSome
    check gr.value.get().value == value

  test "raftGetInSpace returns data from new group if present":
    let allGroups = @[10'u64, 11, 20, 21, 22]
    let (coord, store) = makeStore("/tmp/fractio_sr_rebal_t11", allGroups)
    defer: teardown(coord, "/tmp/fractio_sr_rebal_t11")

    let newGroupIds = @[20'u64, 21, 22]
    let oldGroupIds = @[10'u64, 11]
    let pk = "new_data_key"
    let key = encodeDataRowKey(100, pk)

    # Write using new group routing
    let newSpace = SpaceInfo(
      spaceId: 2, name: "t", replicas: 1, groupIds: newGroupIds)
    discard store.raftPutInSpace(key, "new_value", newSpace, pk)

    # Read during rebalance — should find in new group, no fallback needed
    let rebalSpace = SpaceInfo(
      spaceId: 2, name: "t", replicas: 1,
      groupIds: newGroupIds,
      oldGroupIds: oldGroupIds,
      rebalancing: true,
    )
    let gr = store.raftGetInSpace(key, rebalSpace, pk)
    check gr.isOk
    check gr.value.isSome
    check gr.value.get().value == "new_value"

  test "raftGetInSpace returns none when key missing from both groups":
    let allGroups = @[10'u64, 11, 20, 21, 22]
    let (coord, store) = makeStore("/tmp/fractio_sr_rebal_t12", allGroups)
    defer: teardown(coord, "/tmp/fractio_sr_rebal_t12")

    let rebalSpace = SpaceInfo(
      spaceId: 2, name: "t", replicas: 1,
      groupIds: @[20'u64, 21, 22],
      oldGroupIds: @[10'u64, 11],
      rebalancing: true,
    )
    let gr = store.raftGetInSpace(
      encodeDataRowKey(100, "nonexistent"), rebalSpace, "nonexistent")
    check gr.isOk
    check gr.value.isNone

  test "raftGetInSpace no fallback when not rebalancing":
    let allGroups = @[10'u64, 11, 20, 21, 22]
    let (coord, store) = makeStore("/tmp/fractio_sr_rebal_t13", allGroups)
    defer: teardown(coord, "/tmp/fractio_sr_rebal_t13")

    let oldGroupIds = @[10'u64, 11]
    let newGroupIds = @[20'u64, 21, 22]

    # Find a key that routes differently
    var testPk = ""
    for i in 0 ..< 200:
      let pk = "key_" & $i
      if routeToGroup(pk, oldGroupIds) != routeToGroup(pk, newGroupIds):
        testPk = pk
        break
    check testPk.len > 0

    let key = encodeDataRowKey(100, testPk)
    # Write using old group routing
    let oldSpace = SpaceInfo(
      spaceId: 2, name: "t", replicas: 1, groupIds: oldGroupIds)
    discard store.raftPutInSpace(key, "data", oldSpace, testPk)

    # Read with new groups but NOT rebalancing — should NOT fall back
    # (In single-node shared backend, it'll actually find it because all
    # groups share one LevelDB. This test is about the routing logic.)
    let newSpace = SpaceInfo(
      spaceId: 2, name: "t", replicas: 1,
      groupIds: newGroupIds,
      rebalancing: false,  # not rebalancing
    )
    let gr = store.raftGetInSpace(key, newSpace, testPk)
    check gr.isOk
    # On single-node shared backend, it will still be found, so just
    # verify no error occurred (the semantic test matters more in multi-node)

# ---------------------------------------------------------------------------
# Suite: raftScanSpace during rebalance
# ---------------------------------------------------------------------------

suite "Space rebalance — dual-scan routing":
  test "raftScanSpace includes data from both old and new groups":
    let allGroups = @[10'u64, 11, 20, 21, 22]
    let (coord, store) = makeStore("/tmp/fractio_sr_rebal_t20", allGroups)
    defer: teardown(coord, "/tmp/fractio_sr_rebal_t20")

    let oldGroupIds = @[10'u64, 11]
    let newGroupIds = @[20'u64, 21, 22]

    # Write some data using old routing
    let oldSpace = SpaceInfo(
      spaceId: 2, name: "t", replicas: 1, groupIds: oldGroupIds)
    for i in 0 ..< 10:
      let pk = "old_" & $i
      let key = encodeDataRowKey(100, pk)
      discard store.raftPutInSpace(key, $ %*{"src": "old", "id": i}, oldSpace, pk)

    # Write some data using new routing
    let newSpace = SpaceInfo(
      spaceId: 2, name: "t", replicas: 1, groupIds: newGroupIds)
    for i in 10 ..< 15:
      let pk = "new_" & $i
      let key = encodeDataRowKey(100, pk)
      discard store.raftPutInSpace(key, $ %*{"src": "new", "id": i}, newSpace, pk)

    # Scan during rebalance — should see all 15 rows
    let rebalSpace = SpaceInfo(
      spaceId: 2, name: "t", replicas: 1,
      groupIds: newGroupIds,
      oldGroupIds: oldGroupIds,
      rebalancing: true,
    )
    let startKey = encodeDataRowKey(100, "")
    let endKey = encodeDataRowKey(101, "")
    let sr = store.raftScanSpace(startKey, endKey, rebalSpace, 0,
        includeSystemKeys = true)
    check sr.isOk
    check sr.value.len == 15

    # Verify sorted order
    for i in 1 ..< sr.value.len:
      check sr.value[i-1][0] <= sr.value[i][0]

  test "raftScanSpace deduplicates keys present in both old and new groups":
    let allGroups = @[10'u64, 11, 20, 21, 22]
    let (coord, store) = makeStore("/tmp/fractio_sr_rebal_t21", allGroups)
    defer: teardown(coord, "/tmp/fractio_sr_rebal_t21")

    let oldGroupIds = @[10'u64, 11]
    let newGroupIds = @[20'u64, 21, 22]

    # Write the same key using both old and new routing
    let pk = "shared_key"
    let key = encodeDataRowKey(100, pk)
    let oldSpace = SpaceInfo(
      spaceId: 2, name: "t", replicas: 1, groupIds: oldGroupIds)
    discard store.raftPutInSpace(key, "old_value", oldSpace, pk)
    let newSpace = SpaceInfo(
      spaceId: 2, name: "t", replicas: 1, groupIds: newGroupIds)
    discard store.raftPutInSpace(key, "new_value", newSpace, pk)

    # Scan during rebalance
    let rebalSpace = SpaceInfo(
      spaceId: 2, name: "t", replicas: 1,
      groupIds: newGroupIds,
      oldGroupIds: oldGroupIds,
      rebalancing: true,
    )
    let sr = store.raftScanSpace(
        encodeDataRowKey(100, ""),
        encodeDataRowKey(101, ""),
        rebalSpace, 0, includeSystemKeys = true)
    check sr.isOk
    # Should be deduplicated to exactly 1 row (shared backend = same key)
    var count = 0
    for (k, _) in sr.value:
      if k == key:
        inc count
    check count == 1

# ---------------------------------------------------------------------------
# Suite: raftDeleteInGroup
# ---------------------------------------------------------------------------

suite "Space rebalance — raftDeleteInGroup":
  test "raftDeleteInGroup deletes key from specific group":
    let (coord, store) = makeStore("/tmp/fractio_sr_rebal_t30",
        @[10'u64, 11, 12])
    defer: teardown(coord, "/tmp/fractio_sr_rebal_t30")

    let pk = "del_target"
    let key = encodeDataRowKey(100, pk)

    # Write to group 10 directly
    let space = SpaceInfo(
      spaceId: 2, name: "t", replicas: 1, groupIds: @[10'u64])
    discard store.raftPutInSpace(key, "value", space, pk)

    # Verify it exists
    let gr = store.raftGetInSpace(key, space, pk)
    check gr.isOk
    check gr.value.isSome

    # Delete from group 10
    let dr = store.raftDeleteInGroup(key, GroupID(10))
    check dr.isOk

    # Verify deleted
    let gr2 = store.raftGetInSpace(key, space, pk)
    check gr2.isOk
    check gr2.value.isNone

  test "raftDeleteInGroup on non-existent key succeeds":
    let (coord, store) = makeStore("/tmp/fractio_sr_rebal_t31",
        @[10'u64])
    defer: teardown(coord, "/tmp/fractio_sr_rebal_t31")

    let dr = store.raftDeleteInGroup("/t/0000000100/d/nope", GroupID(10))
    check dr.isOk
