# Unit tests for space-aware KV operations in raft_store.nim
#
# Tests raftPutInSpace, raftGetInSpace, raftDeleteInSpace, raftScanSpace
# with multiple Raft groups simulating a multi-group space.
#
# Each test creates N Raft groups (ranges) on a single coordinator,
# populates a SpaceInfo with those range IDs, and exercises the
# space-aware routing through hash(primaryKey) mod numGroups.

import std/[unittest, os, options, tables, algorithm, hashes, json, strutils, locks]
import fractio/protocol/raft_store
import fractio/distributed/raft/multigroup_coordinator
import fractio/distributed/raft/multigroup_types
import fractio/distributed/range/types as rangeTypes
import fractio/distributed/meta/system_tables

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
  ## Returns a SpaceInfo whose rangeIds point to those groups.
  cleanDir(storagePath)
  let nodeId = RangeNodeID(1)
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
  let metaRid = RangeID(1)
  let metaDesc = newRangeDescriptor(metaRid, @[], @[])
  let metaRep = metaDesc.addReplica(nodeId)
  let metaGroup = coord.createGroup(metaDesc, metaRep.replicaId)
  metaGroup.becomeLeader()

  # Create N space groups starting at rangeId 10
  var rangeIds: seq[uint64] = @[]
  for i in 0 ..< groupCount:
    let rid = RangeID(uint64(10 + i))
    rangeIds.add(rid.uint64)
    let desc = newRangeDescriptor(rid, @[], @[])
    let rep = desc.addReplica(nodeId)
    let group = coord.createGroup(desc, rep.replicaId)
    group.becomeLeader()

  coord.start()

  let store = newRaftKVStoreExt(coord, proposeTimeoutMs = 2000)
  # Bootstrap meta range for system key routing
  store.bootstrapStore(@[META_RANGE_ID, RangeID(10)])

  # Pre-create state machines for all space groups
  for rid64 in rangeIds:
    discard store.getOrCreateSM(RangeID(rid64))

  let space = SpaceInfo(
    spaceId: 2,
    name: "test_space",
    replicas: 1,
    rangeIds: rangeIds,
  )

  (coord, store, space)

proc teardown(coord: MultiRaftCoordinator, storagePath: string) =
  coord.stop()
  try: removeDir(storagePath) except CatchableError: discard

# ---------------------------------------------------------------------------
# Suite: routeToGroup
# ---------------------------------------------------------------------------

suite "Space routing — routeToGroup":
  test "single group always returns that group":
    let rangeIds = @[42'u64]
    for pk in @["a", "b", "c", "key123", "999"]:
      check routeToGroup(pk, rangeIds) == RangeID(42)

  test "empty rangeIds returns META_RANGE_ID":
    check routeToGroup("anything", @[]) == META_RANGE_ID

  test "multiple groups distribute keys":
    let rangeIds = @[10'u64, 11, 12]
    var buckets: array[3, int]
    # Hash 100 keys and verify they spread across groups
    for i in 0 ..< 100:
      let pk = "key_" & $i
      let rid = routeToGroup(pk, rangeIds)
      let idx = int(rid.uint64) - 10
      check idx >= 0 and idx < 3
      inc buckets[idx]
    # Each bucket should have at least 1 key (probabilistic but very likely)
    for b in buckets:
      check b > 0

  test "deterministic routing — same key always same group":
    let rangeIds = @[10'u64, 11, 12, 13]
    for pk in @["user_1", "user_2", "order_99"]:
      let r1 = routeToGroup(pk, rangeIds)
      let r2 = routeToGroup(pk, rangeIds)
      check r1 == r2

# ---------------------------------------------------------------------------
# Suite: raftPutInSpace / raftGetInSpace
# ---------------------------------------------------------------------------

suite "Space routing — put and get with 3 groups":
  test "put and get routes to correct group":
    let (coord, store, space) = makeMultiGroupStore(
        "/tmp/fractio_sr_t01", 3)
    defer: teardown(coord, "/tmp/fractio_sr_t01")

    let key = "/t/0000000100/d/42"
    let val = """{"id":42,"name":"test"}"""
    let pkVal = "42"

    let wr = store.raftPutInSpace(key, val, space, pkVal)
    check wr.isOk
    check wr.value.value == val

    let gr = store.raftGetInSpace(key, space, pkVal)
    check gr.isOk
    check gr.value.isSome
    check gr.value.get().value == val

  test "get from wrong group returns none":
    let (coord, store, space) = makeMultiGroupStore(
        "/tmp/fractio_sr_t02", 3)
    defer: teardown(coord, "/tmp/fractio_sr_t02")

    let pkVal = "mykey"
    let key = "/t/0000000100/d/" & pkVal
    discard store.raftPutInSpace(key, "value", space, pkVal)

    # Reading with correct pk works
    let gr = store.raftGetInSpace(key, space, pkVal)
    check gr.isOk
    check gr.value.isSome

    # Reading with a different pk that routes to a different group should miss.
    # We need to find a pk that routes to a different group.
    var otherPk = ""
    let targetRid = routeToGroup(pkVal, space.rangeIds)
    for i in 0 ..< 100:
      let candidate = "alt_" & $i
      if routeToGroup(candidate, space.rangeIds) != targetRid:
        otherPk = candidate
        break
    check otherPk.len > 0  # found one
    let gr2 = store.raftGetInSpace(key, space, otherPk)
    check gr2.isOk
    check gr2.value.isNone  # not in this group's SM

  test "multiple keys distribute across groups":
    let (coord, store, space) = makeMultiGroupStore(
        "/tmp/fractio_sr_t03", 3)
    defer: teardown(coord, "/tmp/fractio_sr_t03")

    # Insert 30 keys
    for i in 0 ..< 30:
      let pk = $i
      let key = encodeDataRowKey(100, pk)
      let val = $ %*{"id": i, "val": "v" & $i}
      let wr = store.raftPutInSpace(key, val, space, pk)
      check wr.isOk

    # Verify each key is retrievable
    for i in 0 ..< 30:
      let pk = $i
      let key = encodeDataRowKey(100, pk)
      let gr = store.raftGetInSpace(key, space, pk)
      check gr.isOk
      check gr.value.isSome

    # Check that keys are spread across at least 2 of the 3 groups
    var groupKeys: array[3, int]
    for i in 0 ..< 30:
      let rid = routeToGroup($i, space.rangeIds)
      let idx = int(rid.uint64) - 10
      inc groupKeys[idx]
    var nonEmpty = 0
    for c in groupKeys:
      if c > 0: inc nonEmpty
    check nonEmpty >= 2  # at least 2 groups have data

# ---------------------------------------------------------------------------
# Suite: raftDeleteInSpace
# ---------------------------------------------------------------------------

suite "Space routing — delete with multiple groups":
  test "delete removes from correct group":
    let (coord, store, space) = makeMultiGroupStore(
        "/tmp/fractio_sr_t10", 3)
    defer: teardown(coord, "/tmp/fractio_sr_t10")

    let pk = "del_key"
    let key = encodeDataRowKey(100, pk)
    discard store.raftPutInSpace(key, "value", space, pk)

    let dr = store.raftDeleteInSpace(key, space, pk)
    check dr.isOk
    check dr.value.isSome
    check dr.value.get().value == "value"

    # Confirm deleted
    let gr = store.raftGetInSpace(key, space, pk)
    check gr.isOk
    check gr.value.isNone

  test "delete non-existent key returns none":
    let (coord, store, space) = makeMultiGroupStore(
        "/tmp/fractio_sr_t11", 2)
    defer: teardown(coord, "/tmp/fractio_sr_t11")

    let dr = store.raftDeleteInSpace("/t/100/d/nope", space, "nope")
    check dr.isOk
    check dr.value.isNone

# ---------------------------------------------------------------------------
# Suite: raftScanSpace — fan-out + merge-sort
# ---------------------------------------------------------------------------

suite "Space routing — fan-out scan with merge-sort":
  test "scan empty space returns empty":
    let (coord, store, space) = makeMultiGroupStore(
        "/tmp/fractio_sr_t20", 3)
    defer: teardown(coord, "/tmp/fractio_sr_t20")

    let sr = store.raftScanSpace(
        encodeDataRowKey(100, ""),
        encodeDataRowKey(101, ""),
        space, 0, includeSystemKeys = true)
    check sr.isOk
    check sr.value.len == 0

  test "scan returns all keys across groups in sorted order":
    let (coord, store, space) = makeMultiGroupStore(
        "/tmp/fractio_sr_t21", 3)
    defer: teardown(coord, "/tmp/fractio_sr_t21")

    # Insert 20 keys that will be distributed across 3 groups
    for i in 0 ..< 20:
      let pk = $i
      let key = encodeDataRowKey(100, pk)
      let val = $ %*{"id": i}
      discard store.raftPutInSpace(key, val, space, pk)

    let startKey = encodeDataRowKey(100, "")
    let endKey = encodeDataRowKey(101, "")
    let sr = store.raftScanSpace(startKey, endKey, space, 0,
        includeSystemKeys = true)
    check sr.isOk
    check sr.value.len == 20

    # Verify sorted order
    for i in 1 ..< sr.value.len:
      check sr.value[i-1][0] <= sr.value[i][0]

  test "scan with limit truncates merged results":
    let (coord, store, space) = makeMultiGroupStore(
        "/tmp/fractio_sr_t22", 3)
    defer: teardown(coord, "/tmp/fractio_sr_t22")

    for i in 0 ..< 15:
      let pk = $i
      let key = encodeDataRowKey(100, pk)
      discard store.raftPutInSpace(key, $ %*{"id": i}, space, pk)

    let sr = store.raftScanSpace(
        encodeDataRowKey(100, ""),
        encodeDataRowKey(101, ""),
        space, 5, includeSystemKeys = true)
    check sr.isOk
    check sr.value.len == 5

    # Should be the first 5 in sorted key order
    for i in 1 ..< sr.value.len:
      check sr.value[i-1][0] <= sr.value[i][0]

  test "scan with key range filters correctly":
    let (coord, store, space) = makeMultiGroupStore(
        "/tmp/fractio_sr_t23", 3)
    defer: teardown(coord, "/tmp/fractio_sr_t23")

    # Insert keys for table 100 and table 200
    for i in 0 ..< 5:
      let pk = $i
      discard store.raftPutInSpace(
          encodeDataRowKey(100, pk), $ %*{"id": i, "t": 100}, space, pk)
      discard store.raftPutInSpace(
          encodeDataRowKey(200, pk), $ %*{"id": i, "t": 200}, space, pk)

    # Scan only table 100
    let sr = store.raftScanSpace(
        encodeDataRowKey(100, ""),
        encodeDataRowKey(101, ""),
        space, 0, includeSystemKeys = true)
    check sr.isOk
    check sr.value.len == 5
    for (k, _) in sr.value:
      check k.contains("/0000000100/")

  test "scan single-group space works (fast path)":
    let (coord, store, space1) = makeMultiGroupStore(
        "/tmp/fractio_sr_t24", 1)
    defer: teardown(coord, "/tmp/fractio_sr_t24")

    for i in 0 ..< 5:
      let pk = $i
      let key = encodeDataRowKey(100, pk)
      discard store.raftPutInSpace(key, $ %*{"id": i}, space1, pk)

    let sr = store.raftScanSpace(
        encodeDataRowKey(100, ""),
        encodeDataRowKey(101, ""),
        space1, 0, includeSystemKeys = true)
    check sr.isOk
    check sr.value.len == 5

  test "scan excludes intent and coord keys":
    let (coord, store, space) = makeMultiGroupStore(
        "/tmp/fractio_sr_t25", 2)
    defer: teardown(coord, "/tmp/fractio_sr_t25")

    # Insert a real key
    let pk = "real"
    let key = encodeDataRowKey(100, pk)
    discard store.raftPutInSpace(key, "value", space, pk)

    # Inject intent key directly into the first group's SM
    let rid = RangeID(space.rangeIds[0])
    let sm = store.getOrCreateSM(rid)
    let intentKey = encodeIntentKey(99, key)
    acquire(store.smMu)
    sm.kvStore[intentKey] = "intent_val"
    release(store.smMu)

    let sr = store.raftScanSpace(
        "", "", space, 0, includeSystemKeys = true)
    check sr.isOk
    for (k, _) in sr.value:
      check not isIntentKey(k)
      check not isCoordKey(k)

# ---------------------------------------------------------------------------
# Suite: space cache load/lookup
# ---------------------------------------------------------------------------

suite "Space routing — cache loading":
  test "loadSpaces and getSpaceForTable round-trip":
    let (coord, store, _) = makeMultiGroupStore(
        "/tmp/fractio_sr_t30", 2)
    defer: teardown(coord, "/tmp/fractio_sr_t30")

    # Write a space record into the meta range
    let spaceKey = encodeSpaceKey(5)
    let spaceVal = $ %*{
      "spaceId": 5,
      "name": "myspace",
      "replicas": 1,
      "rangeIds": [10, 11],
    }
    discard store.raftPut(spaceKey, spaceVal)

    # Write a table record with spaceId
    let tableKey = encodeTableKey(SYS_TABLES_TABLE_ID, "default.public.mytable")
    let tableVal = $ %*{
      "tableId": 100,
      "name": "mytable",
      "spaceId": 5,
    }
    discard store.raftPut(tableKey, tableVal)

    # Load caches
    store.loadSpaces()
    store.loadTableSpaces()

    # Verify lookup
    let spaceOpt = store.getSpaceForTable(100)
    check spaceOpt.isSome
    check spaceOpt.get().name == "myspace"
    check spaceOpt.get().rangeIds.len == 2

  test "getSpaceForTable returns none for unknown table":
    let (coord, store, _) = makeMultiGroupStore(
        "/tmp/fractio_sr_t31", 1)
    defer: teardown(coord, "/tmp/fractio_sr_t31")

    store.loadSpaces()
    store.loadTableSpaces()

    let spaceOpt = store.getSpaceForTable(999)
    check spaceOpt.isNone
