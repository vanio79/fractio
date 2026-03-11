# Unit tests for resolveGroupId space-awareness and lookupNodeInfo.
#
# Single-node tests (no TCP). Uses makeMultiGroupStore pattern from
# test_space_routing.nim — creates meta + data + N space groups, seeds
# tableSpaces/spaces caches via loadSpaces()/loadTableSpaces().

import std/[unittest, os, options, tables, json]
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

proc makeMultiGroupStore(storagePath: string, groupCount: int): tuple[
    coord: MultiRaftCoordinator, store: RaftKVStoreExt,
    space: SpaceInfo] =
  ## Create a store with `groupCount` Raft groups (ranges 10..10+N-1).
  ## Returns a SpaceInfo whose groupIds point to those groups.
  cleanDir(storagePath)
  let nodeId = NodeID(1)
  let cfg = CoordinatorConfig(
    nodeId: nodeId,
    numWorkers: 1,
    electionTimeoutNs: DEFAULT_ELECTION_TIMEOUT_NS,
    heartbeatIntervalNs: DEFAULT_HEARTBEAT_INTERVAL_NS,
    storagePath: storagePath,
    proposeTimeoutMs: 5000,
  )
  let coord = newMultiRaftCoordinator(cfg)

  # Create the meta range (Range 1) for system keys
  let metaRid = GroupID(1)
  let metaDesc = newGroupDescriptor(metaRid)
  let metaRep = metaDesc.addReplica(nodeId)
  let metaGroup = coord.createGroup(metaDesc, metaRep.replicaId)
  metaGroup.becomeLeader()

  # Create data range (Range 2)
  let dataDesc = newGroupDescriptor(DATA_GROUP_START_ID)
  let dataRep = dataDesc.addReplica(nodeId)
  let dataGroup = coord.createGroup(dataDesc, dataRep.replicaId)
  dataGroup.becomeLeader()

  # Create N space groups starting at groupId 10
  var groupIds: seq[uint64] = @[]
  for i in 0 ..< groupCount:
    let rid = GroupID(uint64(10 + i))
    groupIds.add(rid.uint64)
    let desc = newGroupDescriptor(rid)
    let rep = desc.addReplica(nodeId)
    let group = coord.createGroup(desc, rep.replicaId)
    group.becomeLeader()

  coord.start()

  let store = newRaftKVStoreExt(coord, proposeTimeoutMs = 2000)
  store.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])

  # Pre-create state machines for all space groups
  for rid64 in groupIds:
    discard store.getOrCreateSM(GroupID(rid64))

  let space = SpaceInfo(
    spaceId: 2,
    name: "test_space",
    replicas: 1,
    groupIds: groupIds,
  )

  (coord, store, space)

proc teardown(coord: MultiRaftCoordinator, storagePath: string) =
  coord.stop()
  try: removeDir(storagePath) except CatchableError: discard

proc seedSpaceAndTable(store: RaftKVStoreExt, spaceId: int,
    tableId: uint32, groupIds: seq[uint64]) =
  ## Write a space record and a table record into the meta range, then
  ## reload the caches.
  let spaceKey = encodeSpaceKey(spaceId)
  var gids = newJArray()
  for g in groupIds:
    gids.add(newJInt(int(g)))
  let spaceVal = $ %*{
    "spaceId": spaceId,
    "name": "space_" & $spaceId,
    "replicas": 1,
    "groupIds": gids,
  }
  discard store.raftPut(spaceKey, spaceVal)

  let tableKey = encodeTableKey(SYS_TABLES_TABLE_ID, "default.public.t" & $tableId)
  let tableVal = $ %*{
    "tableId": int(tableId),
    "name": "t" & $tableId,
    "spaceId": spaceId,
  }
  discard store.raftPut(tableKey, tableVal)

  store.loadSpaces()
  store.loadTableSpaces()

# ---------------------------------------------------------------------------
# Suite: resolveGroupId — space-aware routing
# ---------------------------------------------------------------------------

suite "resolveGroupId — space-aware routing":

  test "meta keys route to META_GROUP_ID":
    let (coord, store, _) = makeMultiGroupStore("/tmp/fractio_rgs_t01", 3)
    defer: teardown(coord, "/tmp/fractio_rgs_t01")

    # /sys/meta1/* keys
    let r1 = store.resolveGroupId("/sys/meta1/foo")
    check r1.isSome
    check r1.get() == META_GROUP_ID

    # System table keys within meta range (tableId 1-6)
    let r2 = store.resolveGroupId(encodeTableKey(SYS_NODES_TABLE_ID, "1"))
    check r2.isSome
    check r2.get() == META_GROUP_ID

  test "system table keys route to DATA_GROUP_START_ID":
    let (coord, store, _) = makeMultiGroupStore("/tmp/fractio_rgs_t02", 3)
    defer: teardown(coord, "/tmp/fractio_rgs_t02")

    # SYS_NODES_TABLE_ID = 5, which is in meta range (1-6), so actually META_GROUP_ID
    # Table ID > 6 but < 100 is a system table in data range
    let key = encodeTableKey(10'u32, "some_metric")
    let r = store.resolveGroupId(key)
    check r.isSome
    check r.get() == DATA_GROUP_START_ID

  test "user table key not in space routes to DATA_GROUP_START_ID":
    let (coord, store, _) = makeMultiGroupStore("/tmp/fractio_rgs_t03", 3)
    defer: teardown(coord, "/tmp/fractio_rgs_t03")

    store.loadSpaces()
    store.loadTableSpaces()

    # tableId=100, no space mapping exists
    let key = encodeDataRowKey(100, "42")
    let r = store.resolveGroupId(key)
    check r.isSome
    check r.get() == DATA_GROUP_START_ID

  test "user table key in space routes to correct space group":
    let (coord, store, space) = makeMultiGroupStore("/tmp/fractio_rgs_t04", 3)
    defer: teardown(coord, "/tmp/fractio_rgs_t04")

    seedSpaceAndTable(store, 2, 100, space.groupIds)

    let pk = "42"
    let key = encodeDataRowKey(100, pk)
    let r = store.resolveGroupId(key)
    check r.isSome

    # resolveGroupId strips the "d/" prefix so it hashes the bare PK,
    # matching what raftPutInSpace and the SQL executor do.
    let expected = routeToGroup(pk, space.groupIds)
    check r.get() == expected

  test "different PKs in same space table route to different groups":
    let (coord, store, space) = makeMultiGroupStore("/tmp/fractio_rgs_t05", 3)
    defer: teardown(coord, "/tmp/fractio_rgs_t05")

    seedSpaceAndTable(store, 2, 100, space.groupIds)

    # Hash 50 keys, check distribution across groups
    var seen: set[uint8] = {}
    for i in 0 ..< 50:
      let pk = "key_" & $i
      let key = encodeDataRowKey(100, pk)
      let r = store.resolveGroupId(key)
      check r.isSome
      let gidIdx = uint8(r.get().uint64 - 10)
      seen.incl(gidIdx)

    # With 50 keys across 3 groups, we should see at least 2
    check seen.card >= 2

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
    let nodeVal = $ %*{
      "nodeId": 2,
      "host": "10.0.0.2",
      "raftPort": 7002,
      "clientPort": 9002,
      "status": 1,
    }
    discard store.raftPut(nodeKey, nodeVal)

    let r = store.lookupNodeInfo(2)
    check r.isSome
    check r.get().host == "10.0.0.2"
    check r.get().clientPort == 9002

  test "caches result on second call":
    let (coord, store, _) = makeMultiGroupStore("/tmp/fractio_rgs_t12", 1)
    defer: teardown(coord, "/tmp/fractio_rgs_t12")

    let nodeKey = encodeTableKey(SYS_NODES_TABLE_ID, "3")
    let nodeVal = $ %*{
      "nodeId": 3,
      "host": "10.0.0.3",
      "clientPort": 9003,
    }
    discard store.raftPut(nodeKey, nodeVal)

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

    # Malformed record — missing host and clientPort
    let nodeKey = encodeTableKey(SYS_NODES_TABLE_ID, "4")
    let nodeVal = $ %*{"nodeId": 4, "status": 1}
    discard store.raftPut(nodeKey, nodeVal)

    let r = store.lookupNodeInfo(4)
    check r.isNone

# ---------------------------------------------------------------------------
# Suite: raftPut/raftDelete via resolveGroupId for space keys
# ---------------------------------------------------------------------------

suite "raftPut/raftDelete via resolveGroupId for space keys":

  test "raftPut succeeds for space-routed key when local is leader":
    let (coord, store, space) = makeMultiGroupStore("/tmp/fractio_rgs_t20", 3)
    defer: teardown(coord, "/tmp/fractio_rgs_t20")

    seedSpaceAndTable(store, 2, 100, space.groupIds)

    let key = encodeDataRowKey(100, "42")
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

    seedSpaceAndTable(store, 2, 100, space.groupIds)

    let key = encodeDataRowKey(100, "99")
    let val = """{"id":99}"""
    discard store.raftPut(key, val)

    let dr = store.raftDelete(key)
    check dr.isOk

    # Confirm deleted
    let gr = store.raftGet(key)
    check gr.isOk
    check gr.value.isNone
