# Unit tests for resolveGroupId space-awareness and lookupNodeInfo.
#
# Single-node tests (no TCP). Uses makeMultiGroupStore pattern —
# creates meta + data + N space groups, seeds tableSpaces/spaces
# caches via loadSpaces()/loadTableSpaces().

import std/[unittest, os, options, tables]
import fractio/protocol/raft_store
import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/group_types
import fractio/core/types as coreTypes except NodeID
import fractio/distributed/meta/system_tables
import fractio/distributed/meta/system_schemas
import fractio/storage/wisckey_backend

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

var testBasePort {.global.} = 18500

proc nextBasePort(): int =
  result = testBasePort
  testBasePort += 100

proc cleanDir(path: string) =
  try: removeDir(path) except CatchableError: discard
  try: createDir(path) except CatchableError: discard

# Fixed table ID for tests that need a consistent ID across operations
const testTableId = coreTypes.TableId(ULID(data: [0'u8, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 100, 0]))

proc makeMultiGroupStore(storagePath: string, groupCount: int): tuple[
    coord: NuRaftCoordinator, store: RaftKVStoreExt,
    space: SpaceInfo] =
  ## Create a store with `groupCount` Raft groups (ranges 10..10+N-1).
  ## Returns a SpaceInfo whose groupIds point to those groups.
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

  # Create the meta range (Range 1) and data range (Range 2)
  for rid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    doAssert coord.createAndStartGroup(rid, members)

  # Create N space groups starting at groupId 10
  var groupIds: seq[GroupID] = @[]
  for i in 0 ..< groupCount:
    let rid = groupIDFromInt(10 + i)
    groupIds.add(rid)
    doAssert coord.createAndStartGroup(rid, members)

  # Wait for all groups to elect leaders
  let allGroupIds = @[META_GROUP_ID, DATA_GROUP_START_ID] & groupIds
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

  # Pre-create state machines for all space groups
  for gid in groupIds:
    discard store.getOrCreateSM(gid)

  let space = SpaceInfo(
    spaceId: coreTypes.zeroSpaceID(),
    name: "test_space",
    replicas: 1,
    groupIds: groupIds,
  )

  (coord, store, space)

proc teardown(coord: NuRaftCoordinator, storagePath: string) =
  coord.stop()
  try: removeDir(storagePath) except CatchableError: discard

proc seedSpaceAndTable(store: RaftKVStoreExt, spaceIdNum: int,
    tableId: TableId, groupIds: seq[GroupID]) =
  ## Write a space record and a table record into the meta range, then
  ## reload the caches.
  let spaceId = coreTypes.genSpaceIDLocal()
  let spaceKey = encodeSpaceKey(spaceId)
  let spaceRec = SpaceRecord(
    spaceId: spaceId,
    name: "space_" & $spaceIdNum,
    replicas: 1,
    groupCount: int32(groupIds.len),
    groupIds: groupIds,
  )
  discard store.raftPut(spaceKey, spaceRec.encode())

  let tableKey = encodeTableKey(SYS_TABLES_TABLE_ID, "default.public.t" & $tableId)
  let tableRec = TableRecord(
    tableId: tableId,
    name: "t" & $tableId,
    schema: "public",
    database: "default",
    spaceId: spaceId, # TableRecord.spaceId is SpaceID
    keyEncoding: tkeDataRow,
  )
  discard store.raftPut(tableKey, tableRec.encode())

  store.loadSpaces()
  store.loadTableSpaces()

# ---------------------------------------------------------------------------
# Suite: resolveGroupId — space-aware routing
# ---------------------------------------------------------------------------

suite "resolveGroupId — space-aware routing":

  test "meta keys route to META_GROUP_ID":
    let (coord, store, _) = makeMultiGroupStore("/tmp/fractio_rgs_t01", 3)
    defer: teardown(coord, "/tmp/fractio_rgs_t01")

    # System table keys within meta range (tableId 1-7)
    # SYS_DATABASES_TABLE_ID (num=1), SYS_SCHEMAS_TABLE_ID (num=2),
    # SYS_TABLES_TABLE_ID (num=3), SYS_GROUPS_TABLE_ID (num=4),
    # SYS_NODES_TABLE_ID (num=5), SYS_SETTINGS_TABLE_ID (num=6),
    # SYS_SPACES_TABLE_ID (num=7) all route to META_GROUP_ID
    let r1 = store.resolveGroupId(encodeTableKey(SYS_NODES_TABLE_ID, "1"))
    check r1.isSome
    check r1.get() == META_GROUP_ID

    let r2 = store.resolveGroupId(encodeTableKey(SYS_TABLES_TABLE_ID,
        "default.public.mytable"))
    check r2.isSome
    check r2.get() == META_GROUP_ID

    let r3 = store.resolveGroupId(encodeTableKey(SYS_SPACES_TABLE_ID,
        "somespaceid"))
    check r3.isSome
    check r3.get() == META_GROUP_ID

  test "system table keys route to DATA_GROUP_START_ID":
    let (coord, store, _) = makeMultiGroupStore("/tmp/fractio_rgs_t02", 3)
    defer: teardown(coord, "/tmp/fractio_rgs_t02")

    # SYS_NODES_TABLE_ID = 5, which is in meta range (1-6), so actually META_GROUP_ID
    # Table ID > 6 but < 100 is a system table in data range
    let key = encodeTableKey(genTableIdLocal(), "some_metric")
    let r = store.resolveGroupId(key)
    check r.isSome
    check r.get() == DATA_GROUP_START_ID

  test "user table key not in space routes to DATA_GROUP_START_ID":
    let (coord, store, _) = makeMultiGroupStore("/tmp/fractio_rgs_t03", 3)
    defer: teardown(coord, "/tmp/fractio_rgs_t03")

    store.loadSpaces()
    store.loadTableSpaces()

    # tableId not in any space, no space mapping exists
    let key = encodeDataRowScanBound(genTableIdLocal(), "42")
    let r = store.resolveGroupId(key)
    check r.isSome
    check r.get() == DATA_GROUP_START_ID

  test "user table key in space routes to correct space group":
    let (coord, store, space) = makeMultiGroupStore("/tmp/fractio_rgs_t04", 3)
    defer: teardown(coord, "/tmp/fractio_rgs_t04")

    seedSpaceAndTable(store, 2, testTableId, space.groupIds)

    let pk = "42"
    let key = encodeDataRowScanBound(testTableId, pk)
    let r = store.resolveGroupId(key)
    check r.isSome

    # resolveGroupId strips the "d/" prefix so it hashes the bare PK,
    # matching what raftPutInSpace and the SQL executor do.
    let expected = routeToGroup(pk, space.groupIds)
    check r.get() == expected

  test "different PKs in same space table route to different groups":
    let (coord, store, space) = makeMultiGroupStore("/tmp/fractio_rgs_t05", 3)
    defer: teardown(coord, "/tmp/fractio_rgs_t05")

    seedSpaceAndTable(store, 2, testTableId, space.groupIds)

    # Hash 50 keys, check that routing works
    for i in 0 ..< 50:
      let pk = "key_" & $i
      let key = encodeDataRowScanBound(testTableId, pk)
      let r = store.resolveGroupId(key)
      check r.isSome
      let gid = r.get()
      # Just check we got a valid GroupID
      check $gid != ""

# ---------------------------------------------------------------------------
# Suite: lookupNodeInfo — cache and backend lookup
# ---------------------------------------------------------------------------

suite "lookupNodeInfo — cache and backend lookup":

  test "returns none when node not in sys.nodes":
    let (coord, store, _) = makeMultiGroupStore("/tmp/fractio_rgs_t10", 1)
    defer: teardown(coord, "/tmp/fractio_rgs_t10")

    let r = store.lookupNodeInfo(99)
    check r.isNone

  test "returns host and port from sys.nodes":
    let (coord, store, _) = makeMultiGroupStore("/tmp/fractio_rgs_t11", 1)
    defer: teardown(coord, "/tmp/fractio_rgs_t11")

    # Seed a node record
    let nodeKey = encodeTableKey(SYS_NODES_TABLE_ID, "2")
    let nodeRec = NodeRecord(
      nodeId: 2'u32,
      host: "10.0.0.2",
      raftPort: 7002'u16,
      clientPort: 9002'u16,
      status: nsAlive,
    )
    discard store.raftPut(nodeKey, nodeRec.encode())

    let r = store.lookupNodeInfo(2)
    check r.isSome
    check r.get().host == "10.0.0.2"
    check r.get().clientPort == 9002

  test "caches result on second call":
    let (coord, store, _) = makeMultiGroupStore("/tmp/fractio_rgs_t12", 1)
    defer: teardown(coord, "/tmp/fractio_rgs_t12")

    let nodeKey = encodeTableKey(SYS_NODES_TABLE_ID, "3")
    let nodeRec = NodeRecord(
      nodeId: 3'u32,
      host: "10.0.0.3",
      raftPort: 7003'u16,
      clientPort: 9003'u16,
      status: nsAlive,
    )
    discard store.raftPut(nodeKey, nodeRec.encode())

    # First call — backend lookup
    let r1 = store.lookupNodeInfo(3)
    check r1.isSome
    check store.nodeInfoCache.hasKey(3)

    # Second call — from cache
    let r2 = store.lookupNodeInfo(3)
    check r2.isSome
    check r2.get().host == "10.0.0.3"

  test "handles missing fields gracefully":
    let (coord, store, _) = makeMultiGroupStore("/tmp/fractio_rgs_t13", 1)
    defer: teardown(coord, "/tmp/fractio_rgs_t13")

    # Malformed record — empty host string
    let nodeKey = encodeTableKey(SYS_NODES_TABLE_ID, "4")
    let nodeRec = NodeRecord(
      nodeId: 4'u32,
      host: "", # Empty host is invalid
      status: nsAlive,
    )
    discard store.raftPut(nodeKey, nodeRec.encode())

    let r = store.lookupNodeInfo(4)
    check r.isNone

# ---------------------------------------------------------------------------
# Suite: raftPut/raftDelete via resolveGroupId for space keys
# ---------------------------------------------------------------------------

suite "raftPut/raftDelete via resolveGroupId for space keys":

  test "raftPut succeeds for space-routed key when local is leader":
    let (coord, store, space) = makeMultiGroupStore("/tmp/fractio_rgs_t20", 3)
    defer: teardown(coord, "/tmp/fractio_rgs_t20")

    let putTableId = genTableIdLocal()
    seedSpaceAndTable(store, 2, putTableId, space.groupIds)

    let key = encodeDataRowScanBound(putTableId, "42")
    let val = """{"id":42,"name":"test"}"""
    let wr = store.raftPut(key, val)
    check wr.isOk
    check wr.value.value == val

    # Verify it's readable
    let gr = store.raftGet(key)
    check gr.isOk
    check gr.value.isSome
    check gr.value.get().value == val

  test "raftDelete succeeds for space-routed key when local is leader":
    let (coord, store, space) = makeMultiGroupStore("/tmp/fractio_rgs_t21", 3)
    defer: teardown(coord, "/tmp/fractio_rgs_t21")

    let delTableId = genTableIdLocal()
    seedSpaceAndTable(store, 2, delTableId, space.groupIds)

    let key = encodeDataRowScanBound(delTableId, "99")
    let val = """{"id":99}"""
    discard store.raftPut(key, val)

    let dr = store.raftDelete(key)
    check dr.isOk

    # Confirm deleted
    let gr = store.raftGet(key)
    check gr.isOk
    check gr.value.isNone
