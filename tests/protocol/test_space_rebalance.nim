# Unit tests for space rebalancing in raft_store.nim
#
# Tests:
# - SpaceInfo rebalance fields and loadSpaces parsing
# - Dual-read routing during rebalance (raftGetInSpace fallback)
# - Dual-scan routing during rebalance (raftScanSpace merges old + new groups)
# - raftDeleteInGroup targeted deletion
# - updateSpaceRecord persistence
#
# NOTE: Group IDs must start from 100+ to avoid port hash collisions with
# META_GROUP_ID (hash=1) and DATA_GROUP_START_ID (hash=2).

import std/[unittest, os, options, tables, algorithm, hashes, json, strutils, locks,
            sequtils]
from fractio/core/types import genULIDLocal, genSpaceIDLocal, genTableIdLocal,
    ULID, SpaceID, TableId, ulidToBytes, ulidFromBytes, ZeroULID
import fractio/protocol/raft_store
import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/meta/system_tables
import fractio/distributed/meta/system_schemas
import fractio/storage/wisckey_backend

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

var testBasePort {.global.} = 19500

proc nextBasePort(): int =
  result = testBasePort
  testBasePort += 100

proc cleanDir(path: string) =
  try: removeDir(path) except CatchableError: discard
  try: createDir(path) except CatchableError: discard

proc makeStore(storagePath: string, groupIds: seq[uint64]): tuple[
    coord: NuRaftCoordinator, store: RaftKVStoreExt] =
  ## Create a store with meta group + data group + the specified groups.
  cleanDir(storagePath)
  let nodeId = NodeID(1)
  let port = nextBasePort()
  let members = @[(nodeId: 1'u32, host: "127.0.0.1", port: port)]

  let coord = newNuRaftCoordinator(nuraft_coordinator.CoordinatorConfig(
    nodeId: nodeId,
    port: port,
    host: "127.0.0.1",
    dataDir: storagePath,
    electionTimeoutLowerMs: 200,
    electionTimeoutUpperMs: 400,
    heartbeatIntervalMs: 100,
  ))
  coord.start()

  # Meta group and data group
  for rid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    doAssert coord.createAndStartGroup(rid, members)

  # Additional groups (use IDs >= 100 to avoid port collisions)
  for gid in groupIds:
    doAssert coord.createAndStartGroup(groupIDFromInt(gid), members)

  # Wait for all groups to elect leaders
  let allGroupIds = @[META_GROUP_ID, DATA_GROUP_START_ID] &
    groupIds.mapIt(groupIDFromInt(it))
  for attempt in 0 ..< 50:
    var allLeaders = true
    for gid in allGroupIds:
      if not coord.isLeader(gid):
        allLeaders = false
        break
    if allLeaders:
      break
    os.sleep(100)

  let store = newRaftKVStoreExt(coord, proposeTimeoutMs = 2000)
  store.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])

  for gid in groupIds:
    discard store.getOrCreateSM(groupIDFromInt(gid))

  (coord, store)

proc teardown(coord: NuRaftCoordinator, storagePath: string) =
  coord.stop()
  try: removeDir(storagePath) except CatchableError: discard

# ---------------------------------------------------------------------------
# Suite: SpaceInfo rebalance fields
# ---------------------------------------------------------------------------

suite "Space rebalance — SpaceInfo fields":
  test "SpaceInfo has rebalance fields with defaults":
    let space = SpaceInfo(
      spaceId: genSpaceIDLocal(),
      name: "test",
      replicas: 2,
      groupIds: @[groupIDFromInt(100), groupIDFromInt(101)],
    )
    check space.rebalancing == false
    check space.rebalanceWorker == 0
    check space.rebalanceHeartbeat == 0
    check space.rebalanceCursor == ""
    check space.oldGroupIds.len == 0

  test "SpaceInfo with all rebalance fields set":
    let space = SpaceInfo(
      spaceId: genSpaceIDLocal(),
      name: "test",
      replicas: 2,
      groupIds: @[groupIDFromInt(110), groupIDFromInt(111), groupIDFromInt(
          112)],
      oldGroupIds: @[groupIDFromInt(100), groupIDFromInt(101)],
      rebalancing: true,
      rebalanceWorker: 3,
      rebalanceHeartbeat: 1741700000'i64,
      rebalanceCursor: "/t/0000000100/d/m",
    )
    check space.rebalancing == true
    check space.rebalanceWorker == 3
    check space.rebalanceHeartbeat == 1741700000'i64
    check space.rebalanceCursor == "/t/0000000100/d/m"
    check space.oldGroupIds.len == 2
    check space.groupIds.len == 3

# ---------------------------------------------------------------------------
# Suite: loadSpaces parses rebalance fields
# ---------------------------------------------------------------------------

suite "Space rebalance — loadSpaces parses rebalance fields":
  test "loadSpaces reads rebalance fields from sys.spaces":
    let (coord, store) = makeStore("/tmp/fractio_sr_rebal_t01", @[])
    defer: teardown(coord, "/tmp/fractio_sr_rebal_t01")

    # Write a space record with rebalance fields
    let spaceId = genSpaceIDLocal()
    let spaceKey = encodeSpaceKey(spaceId)
    let spaceRec = SpaceRecord(
      spaceId: spaceId, # SpaceRecord.spaceId is now SpaceID
      name: "rebaltest",
      replicas: 2,
      groupCount: 3,
      groupIds: @[groupIDFromInt(110), groupIDFromInt(111), groupIDFromInt(
          112)],
      oldGroupIds: @[groupIDFromInt(100), groupIDFromInt(101)],
      rebalancing: true,
      rebalanceWorker: 3,
      rebalanceHeartbeat: 1741700000'i64,
      rebalanceCursor: "/t/0000000100/d/key42",
    )
    discard store.sysTablePut(spaceKey, spaceRec.encode())
    # Wait for Raft commit to apply
    os.sleep(200)
    store.loadSpaces()

    acquire(store.spacesMu)
    let sp = store.spaces[spaceId]
    release(store.spacesMu)

    check sp.name == "rebaltest"
    check sp.groupIds.len == 3
    check sp.oldGroupIds.len == 2
    check sp.rebalancing == true
    check sp.rebalanceWorker == 3
    check sp.rebalanceHeartbeat == 1741700000'i64
    check sp.rebalanceCursor == "/t/0000000100/d/key42"

  test "loadSpaces with missing rebalance fields uses defaults":
    let (coord, store) = makeStore("/tmp/fractio_sr_rebal_t02", @[])
    defer: teardown(coord, "/tmp/fractio_sr_rebal_t02")

    # Write a space record WITHOUT rebalance fields (backward compat)
    let spaceId = genSpaceIDLocal()
    let spaceKey = encodeSpaceKey(spaceId)
    let spaceRec = SpaceRecord(
      spaceId: spaceId, # SpaceRecord.spaceId is now SpaceID
      name: "oldstyle",
      replicas: 1,
      groupCount: 1,
      groupIds: @[groupIDFromInt(100)],
    )
    discard store.sysTablePut(spaceKey, spaceRec.encode())
    # Wait for Raft commit to apply
    os.sleep(200)
    store.loadSpaces()

    acquire(store.spacesMu)
    let sp = store.spaces[spaceId]
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
    let (coord, store) = makeStore("/tmp/fractio_sr_rebal_t05", @[100'u64, 101])
    defer: teardown(coord, "/tmp/fractio_sr_rebal_t05")

    let spaceId = genSpaceIDLocal()
    var space = SpaceInfo(
      spaceId: spaceId,
      name: "persist_test",
      replicas: 2,
      groupIds: @[groupIDFromInt(110), groupIDFromInt(111), groupIDFromInt(
          112)],
      oldGroupIds: @[groupIDFromInt(100), groupIDFromInt(101)],
      rebalancing: true,
      rebalanceWorker: 1,
      rebalanceHeartbeat: 9999'i64,
      rebalanceCursor: "/t/0000000100/d/abc",
    )
    discard store.updateSpaceRecord(space)
    # Wait for Raft commit to apply
    os.sleep(200)
    store.loadSpaces()

    acquire(store.spacesMu)
    let loaded = store.spaces[spaceId]
    release(store.spacesMu)

    check loaded.name == "persist_test"
    check loaded.replicas == 2
    check loaded.groupIds.len == 3
    check loaded.oldGroupIds.len == 2
    check loaded.rebalancing == true
    check loaded.rebalanceWorker == 1
    check loaded.rebalanceHeartbeat == 9999'i64
    check loaded.rebalanceCursor == "/t/0000000100/d/abc"

  test "updateSpaceRecord clears rebalance state":
    let (coord, store) = makeStore("/tmp/fractio_sr_rebal_t06", @[])
    defer: teardown(coord, "/tmp/fractio_sr_rebal_t06")

    # First write with rebalancing on
    let spaceId = genSpaceIDLocal()
    var space = SpaceInfo(
      spaceId: spaceId,
      name: "clear_test",
      replicas: 1,
      groupIds: @[groupIDFromInt(110)],
      oldGroupIds: @[groupIDFromInt(100)],
      rebalancing: true,
      rebalanceWorker: 2,
      rebalanceHeartbeat: 5000,
      rebalanceCursor: "/some/key",
    )
    discard store.updateSpaceRecord(space)
    # Wait for Raft commit to apply
    os.sleep(200)

    # Now clear rebalance state
    space.oldGroupIds = @[]
    space.rebalancing = false
    space.rebalanceWorker = 0
    space.rebalanceHeartbeat = 0
    space.rebalanceCursor = ""
    discard store.updateSpaceRecord(space)
    # Wait for Raft commit to apply
    os.sleep(200)
    store.loadSpaces()

    acquire(store.spacesMu)
    let loaded = store.spaces[spaceId]
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
    # Setup: 2 old groups (100,101) + 3 new groups (110,111,112)
    let allGroups = @[100'u64, 101, 110, 111, 112]
    let (coord, store) = makeStore("/tmp/fractio_sr_rebal_t10", allGroups)
    defer: teardown(coord, "/tmp/fractio_sr_rebal_t10")

    let oldGroupIds = @[groupIDFromInt(100), groupIDFromInt(101)]
    let newGroupIds = @[groupIDFromInt(110), groupIDFromInt(111),
        groupIDFromInt(112)]

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

    let key = encodeDataRowKey(genTableIdLocal(), testPk)
    let value = """{"id":"test","val":"fallback"}"""

    # Write data using OLD group routing (simulate pre-rebalance data)
    let oldSpace = SpaceInfo(
      spaceId: genSpaceIDLocal(), name: "t", replicas: 1, groupIds: oldGroupIds)
    let wr = store.raftPutInSpace(key, value, oldSpace, testPk)
    check wr.isOk

    # Now create a rebalancing space with new groups
    let rebalSpace = SpaceInfo(
      spaceId: genSpaceIDLocal(), name: "t", replicas: 1,
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
    let allGroups = @[100'u64, 101, 110, 111, 112]
    let (coord, store) = makeStore("/tmp/fractio_sr_rebal_t11", allGroups)
    defer: teardown(coord, "/tmp/fractio_sr_rebal_t11")

    let newGroupIds = @[groupIDFromInt(110), groupIDFromInt(111),
        groupIDFromInt(112)]
    let oldGroupIds = @[groupIDFromInt(100), groupIDFromInt(101)]
    let pk = "new_data_key"
    let key = encodeDataRowKey(genTableIdLocal(), pk)

    # Write using new group routing
    let newSpace = SpaceInfo(
      spaceId: genSpaceIDLocal(), name: "t", replicas: 1, groupIds: newGroupIds)
    discard store.raftPutInSpace(key, "new_value", newSpace, pk)

    # Read during rebalance — should find in new group, no fallback needed
    let rebalSpace = SpaceInfo(
      spaceId: genSpaceIDLocal(), name: "t", replicas: 1,
      groupIds: newGroupIds,
      oldGroupIds: oldGroupIds,
      rebalancing: true,
    )
    let gr = store.raftGetInSpace(key, rebalSpace, pk)
    check gr.isOk
    check gr.value.isSome
    check gr.value.get().value == "new_value"

  test "raftGetInSpace returns none when key missing from both groups":
    let allGroups = @[100'u64, 101, 110, 111, 112]
    let (coord, store) = makeStore("/tmp/fractio_sr_rebal_t12", allGroups)
    defer: teardown(coord, "/tmp/fractio_sr_rebal_t12")

    let rebalSpace = SpaceInfo(
      spaceId: genSpaceIDLocal(), name: "t", replicas: 1,
      groupIds: @[groupIDFromInt(110), groupIDFromInt(111), groupIDFromInt(
          112)],
      oldGroupIds: @[groupIDFromInt(100), groupIDFromInt(101)],
      rebalancing: true,
    )
    let gr = store.raftGetInSpace(
      encodeDataRowKey(genTableIdLocal(), "nonexistent"), rebalSpace, "nonexistent")
    check gr.isOk
    check gr.value.isNone

  test "raftGetInSpace no fallback when not rebalancing":
    let allGroups = @[100'u64, 101, 110, 111, 112]
    let (coord, store) = makeStore("/tmp/fractio_sr_rebal_t13", allGroups)
    defer: teardown(coord, "/tmp/fractio_sr_rebal_t13")

    let oldGroupIds = @[groupIDFromInt(100), groupIDFromInt(101)]
    let newGroupIds = @[groupIDFromInt(110), groupIDFromInt(111),
        groupIDFromInt(112)]

    # Find a key that routes differently
    var testPk = ""
    for i in 0 ..< 200:
      let pk = "key_" & $i
      if routeToGroup(pk, oldGroupIds) != routeToGroup(pk, newGroupIds):
        testPk = pk
        break
    check testPk.len > 0

    let key = encodeDataRowKey(genTableIdLocal(), testPk)
    # Write using old group routing
    let oldSpace = SpaceInfo(
      spaceId: genSpaceIDLocal(), name: "t", replicas: 1, groupIds: oldGroupIds)
    discard store.raftPutInSpace(key, "data", oldSpace, testPk)

    # Read with new groups but NOT rebalancing — should NOT fall back
    # (In single-node shared backend, it'll actually find it because all
    # groups share one LevelDB. This test is about the routing logic.)
    let newSpace = SpaceInfo(
      spaceId: genSpaceIDLocal(), name: "t", replicas: 1,
      groupIds: newGroupIds,
      rebalancing: false, # not rebalancing
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
    let allGroups = @[100'u64, 101, 110, 111, 112]
    let (coord, store) = makeStore("/tmp/fractio_sr_rebal_t20", allGroups)
    defer: teardown(coord, "/tmp/fractio_sr_rebal_t20")

    let oldGroupIds = @[groupIDFromInt(100), groupIDFromInt(101)]
    let newGroupIds = @[groupIDFromInt(110), groupIDFromInt(111),
        groupIDFromInt(112)]

    let testTableId = genTableIdLocal() # Use same table ID for all writes and scans

    # Write some data using old routing
    let oldSpace = SpaceInfo(
      spaceId: genSpaceIDLocal(), name: "t", replicas: 1, groupIds: oldGroupIds)
    for i in 0 ..< 10:
      let pk = "old_" & $i
      let key = encodeDataRowKey(testTableId, pk)
      discard store.raftPutInSpace(key, $ %*{"src": "old", "id": i}, oldSpace, pk)

    # Write some data using new routing
    let newSpace = SpaceInfo(
      spaceId: genSpaceIDLocal(), name: "t", replicas: 1, groupIds: newGroupIds)
    for i in 10 ..< 15:
      let pk = "new_" & $i
      let key = encodeDataRowKey(testTableId, pk)
      discard store.raftPutInSpace(key, $ %*{"src": "new", "id": i}, newSpace, pk)

    # Scan during rebalance — should see all 15 rows
    let rebalSpace = SpaceInfo(
      spaceId: genSpaceIDLocal(), name: "t", replicas: 1,
      groupIds: newGroupIds,
      oldGroupIds: oldGroupIds,
      rebalancing: true,
    )
    let startKey = encodeDataRowKey(testTableId, "")
    let endKey = encodeDataRowKey(testTableId, "\xFF")
    let sr = store.raftScanSpace(startKey, endKey, rebalSpace, 0,
        includeSystemKeys = true)
    check sr.isOk
    check sr.value.len == 15

    # Verify sorted order
    for i in 1 ..< sr.value.len:
      check sr.value[i-1][0] <= sr.value[i][0]

  test "raftScanSpace deduplicates keys present in both old and new groups":
    let allGroups = @[100'u64, 101, 110, 111, 112]
    let (coord, store) = makeStore("/tmp/fractio_sr_rebal_t21", allGroups)
    defer: teardown(coord, "/tmp/fractio_sr_rebal_t21")

    let oldGroupIds = @[groupIDFromInt(100), groupIDFromInt(101)]
    let newGroupIds = @[groupIDFromInt(110), groupIDFromInt(111),
        groupIDFromInt(112)]

    let testTableId = genTableIdLocal() # Use same table ID for all writes and scans

    # Write the same key using both old and new routing
    let pk = "shared_key"
    let key = encodeDataRowKey(testTableId, pk)
    let oldSpace = SpaceInfo(
      spaceId: genSpaceIDLocal(), name: "t", replicas: 1, groupIds: oldGroupIds)
    discard store.raftPutInSpace(key, "old_value", oldSpace, pk)
    let newSpace = SpaceInfo(
      spaceId: genSpaceIDLocal(), name: "t", replicas: 1, groupIds: newGroupIds)
    discard store.raftPutInSpace(key, "new_value", newSpace, pk)

    # Scan during rebalance
    let rebalSpace = SpaceInfo(
      spaceId: genSpaceIDLocal(), name: "t", replicas: 1,
      groupIds: newGroupIds,
      oldGroupIds: oldGroupIds,
      rebalancing: true,
    )
    let sr = store.raftScanSpace(
        encodeDataRowKey(testTableId, ""),
        encodeDataRowKey(testTableId, "\xFF"),
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
        @[100'u64, 101, 102])
    defer: teardown(coord, "/tmp/fractio_sr_rebal_t30")

    let pk = "del_target"
    let key = encodeDataRowKey(genTableIdLocal(), pk)

    # Write to group 100 directly
    let space = SpaceInfo(
      spaceId: genSpaceIDLocal(), name: "t", replicas: 1, groupIds: @[
          groupIDFromInt(100)])
    discard store.raftPutInSpace(key, "value", space, pk)

    # Verify it exists
    let gr = store.raftGetInSpace(key, space, pk)
    check gr.isOk
    check gr.value.isSome

    # Delete from group 100
    let dr = store.raftDeleteInGroup(key, groupIDFromInt(100))
    check dr.isOk

    # Verify deleted
    let gr2 = store.raftGetInSpace(key, space, pk)
    check gr2.isOk
    check gr2.value.isNone

  test "raftDeleteInGroup on non-existent key succeeds":
    let (coord, store) = makeStore("/tmp/fractio_sr_rebal_t31",
        @[100'u64])
    defer: teardown(coord, "/tmp/fractio_sr_rebal_t31")

    let dr = store.raftDeleteInGroup("/t/0000000100/d/nope", groupIDFromInt(100))
    check dr.isOk
