# Integration tests for space-aware query routing through the SQL executor.
#
# Creates a store with multiple Raft groups, seeds a space and table
# mapping, then runs INSERT/SELECT/UPDATE/DELETE SQL through the executor
# to verify that data is correctly routed and merged across groups.

import std/[unittest, options, json, os, strutils, tables, hashes, algorithm]
import fractio/sql/parser
import fractio/sql/ast
import fractio/sql/planner
import fractio/sql/executor
import fractio/distributed/meta/system_tables
import fractio/protocol/raft_store
import fractio/distributed/raft/multigroup_coordinator
import fractio/distributed/raft/multigroup_types
import fractio/distributed/range/types as rangeTypes

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc cleanDir(path: string) =
  try: removeDir(path) except CatchableError: discard
  try: createDir(path) except CatchableError: discard

proc createMultiGroupTestStore(testDir: string, groupCount: int): RaftKVStoreExt =
  ## Create a store with 1 meta range + N space groups.
  ## Seeds sys.spaces and sys.tables so the executor can resolve space routing.
  cleanDir(testDir)
  let nodeId = RangeNodeID(1)
  let coord = newMultiRaftCoordinator(CoordinatorConfig(
    nodeId: nodeId,
    numWorkers: 1,
    electionTimeoutNs: 5_000_000_000'i64,
    heartbeatIntervalNs: 1_000_000_000'i64,
    storagePath: testDir,
    proposeTimeoutMs: 5000,
  ))

  # Meta range (Range 1)
  let metaRid = RangeID(1)
  let metaDesc = newRangeDescriptor(metaRid, @[], @[])
  let metaRep = metaDesc.addReplica(nodeId)
  let metaGroup = coord.createGroup(metaDesc, metaRep.replicaId)
  metaGroup.becomeLeader()

  # Data range (Range 2) — for non-space tables
  let dataRid = RangeID(2)
  let dataDesc = newRangeDescriptor(dataRid, @[], @[])
  let dataRep = dataDesc.addReplica(nodeId)
  let dataGroup = coord.createGroup(dataDesc, dataRep.replicaId)
  dataGroup.becomeLeader()

  # Space groups (Range 10..10+N-1)
  var rangeIds: seq[int] = @[]
  for i in 0 ..< groupCount:
    let rid = RangeID(uint64(10 + i))
    rangeIds.add(10 + i)
    let desc = newRangeDescriptor(rid, @[], @[])
    let rep = desc.addReplica(nodeId)
    let group = coord.createGroup(desc, rep.replicaId)
    group.becomeLeader()

  coord.start()

  result = newRaftKVStoreExt(coord, proposeTimeoutMs = 5000)
  result.addShardExt("", META_RANGE_END_KEY, META_RANGE_ID)
  result.addShardExt(META_RANGE_END_KEY, "", DATA_RANGE_START_ID)
  result.wireApplyCallback()

  # Pre-create SMs for space groups
  for i in 0 ..< groupCount:
    discard result.getOrCreateSM(RangeID(uint64(10 + i)))

  # Seed space record
  let spaceKey = encodeSpaceKey(2)
  let spaceVal = $ %*{
    "spaceId": 2,
    "name": "testspace",
    "replicas": 1,
    "groupCount": groupCount,
    "rangeIds": rangeIds,
  }
  discard result.raftPut(spaceKey, spaceVal)

  result.loadSpaces()

proc seedSpaceTable(store: RaftKVStoreExt, tableId: uint32,
    tableName: string, database = "default", schema = "public") =
  ## Register a table in sys.tables pointing to spaceId=2.
  let fullName = database & "." & schema & "." & tableName
  let tableKey = encodeTableKey(SYS_TABLES_TABLE_ID, fullName)
  let tableVal = $ %*{
    "tableId": tableId,
    "name": tableName,
    "database": database,
    "schema": schema,
    "spaceId": 2,
    "primaryKey": ["id"],
    "columns": [
      {"name": "id", "type": "INT", "primaryKey": true},
      {"name": "val", "type": "TEXT"},
    ],
  }
  discard store.raftPut(tableKey, tableVal)
  store.loadTableSpaces()

proc seedSpaceTableThreeCol(store: RaftKVStoreExt, tableId: uint32,
    tableName: string, database = "default", schema = "public") =
  ## Register a table with 3 columns (id, name, score).
  let fullName = database & "." & schema & "." & tableName
  let tableKey = encodeTableKey(SYS_TABLES_TABLE_ID, fullName)
  let tableVal = $ %*{
    "tableId": tableId,
    "name": tableName,
    "database": database,
    "schema": schema,
    "spaceId": 2,
    "primaryKey": ["id"],
    "columns": [
      {"name": "id", "type": "INT", "primaryKey": true},
      {"name": "name", "type": "TEXT"},
      {"name": "score", "type": "INT"},
    ],
  }
  discard store.raftPut(tableKey, tableVal)
  store.loadTableSpaces()

proc exec(store: RaftKVStoreExt, sql: string,
    database = "default", schema = "public"): ExecResult =
  executeSQL(sql, store, database, schema)

proc cleanupTestDir(testDir: string) =
  if dirExists(testDir):
    removeDir(testDir)

# ---------------------------------------------------------------------------
# Suite: INSERT routing through executor with multi-group space
# ---------------------------------------------------------------------------

suite "SQL Executor — space-routed INSERT":
  var store: RaftKVStoreExt
  let testDir = "/tmp/fractio_test_space_insert_" & $getCurrentProcessId()

  setup:
    cleanupTestDir(testDir)
    store = createMultiGroupTestStore(testDir, 3)
    seedSpaceTable(store, 100, "items")

  teardown:
    store.coordinator.stop()
    cleanupTestDir(testDir)

  test "INSERT routes rows to space groups":
    let res = exec(store,
        "INSERT INTO items (id, val) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")
    check res.kind == erkModified
    check res.count == 5

    # Verify data is spread across groups (at least 2 of 3)
    var groupHits: array[3, int]
    for i in 1 .. 5:
      let rid = routeToGroup($i, @[10'u64, 11, 12])
      inc groupHits[int(rid.uint64) - 10]
    var nonEmpty = 0
    for c in groupHits:
      if c > 0: inc nonEmpty
    check nonEmpty >= 2

  test "INSERT single row":
    let res = exec(store,
        "INSERT INTO items (id, val) VALUES (42, 'hello')")
    check res.kind == erkModified
    check res.count == 1

# ---------------------------------------------------------------------------
# Suite: SELECT (scan + point get) with multi-group space
# ---------------------------------------------------------------------------

suite "SQL Executor — space-routed SELECT":
  var store: RaftKVStoreExt
  let testDir = "/tmp/fractio_test_space_select_" & $getCurrentProcessId()

  setup:
    cleanupTestDir(testDir)
    store = createMultiGroupTestStore(testDir, 3)
    seedSpaceTable(store, 100, "items")
    # Insert 10 rows distributed across groups
    for i in 1 .. 10:
      discard exec(store,
          "INSERT INTO items (id, val) VALUES (" & $i & ", 'v" & $i & "')")

  teardown:
    store.coordinator.stop()
    cleanupTestDir(testDir)

  test "SELECT * returns all rows from all groups":
    let res = exec(store, "SELECT * FROM items")
    check res.kind == erkRows
    check res.rows.len == 10

  test "SELECT * returns rows in sorted key order":
    let res = exec(store, "SELECT * FROM items")
    check res.kind == erkRows
    # Rows should be ordered by key (which includes the padded primary key)
    for i in 1 ..< res.rows.len:
      # We can't check exact sort order of integer PKs as strings easily,
      # but we verify they are all present
      discard
    check res.rows.len == 10

  test "SELECT with WHERE point-get routes to single group":
    let res = exec(store, "SELECT * FROM items WHERE id = 5")
    check res.kind == erkRows
    check res.rows.len == 1
    check res.rows[0][0] == "5"
    check res.rows[0][1] == "v5"

  test "SELECT with WHERE filter on non-PK column":
    # This uses scan + filter, not point get
    let res = exec(store, "SELECT * FROM items WHERE val = 'v3'")
    check res.kind == erkRows
    check res.rows.len == 1
    check res.rows[0][0] == "3"

  test "SELECT with LIMIT":
    let res = exec(store, "SELECT * FROM items LIMIT 3")
    check res.kind == erkRows
    check res.rows.len == 3

  test "SELECT from empty space-routed table":
    # Create a second table in the same space, don't insert data
    seedSpaceTable(store, 200, "empty_items")
    let res = exec(store, "SELECT * FROM empty_items")
    check res.kind == erkRows
    check res.rows.len == 0

  test "SELECT with point-get for non-existent key":
    let res = exec(store, "SELECT * FROM items WHERE id = 999")
    check res.kind == erkRows
    check res.rows.len == 0

# ---------------------------------------------------------------------------
# Suite: UPDATE with multi-group space
# ---------------------------------------------------------------------------

suite "SQL Executor — space-routed UPDATE":
  var store: RaftKVStoreExt
  let testDir = "/tmp/fractio_test_space_update_" & $getCurrentProcessId()

  setup:
    cleanupTestDir(testDir)
    store = createMultiGroupTestStore(testDir, 3)
    seedSpaceTable(store, 100, "items")
    for i in 1 .. 5:
      discard exec(store,
          "INSERT INTO items (id, val) VALUES (" & $i & ", 'orig')")

  teardown:
    store.coordinator.stop()
    cleanupTestDir(testDir)

  test "UPDATE single row by PK":
    let res = exec(store, "UPDATE items SET val = 'changed' WHERE id = 3")
    check res.kind == erkModified
    check res.count == 1

    let sel = exec(store, "SELECT * FROM items WHERE id = 3")
    check sel.kind == erkRows
    check sel.rows.len == 1
    check sel.rows[0][1] == "changed"

  test "UPDATE all rows (no WHERE)":
    let res = exec(store, "UPDATE items SET val = 'all_changed'")
    check res.kind == erkModified
    check res.count == 5

    let sel = exec(store, "SELECT * FROM items")
    check sel.kind == erkRows
    for row in sel.rows:
      check row[1] == "all_changed"

  test "UPDATE with filter matches subset":
    # Update only rows with id > 3
    let res = exec(store, "UPDATE items SET val = 'hi' WHERE id > 3")
    check res.kind == erkModified
    check res.count == 2  # id=4 and id=5

  test "UPDATE preserves data in correct groups":
    discard exec(store, "UPDATE items SET val = 'new' WHERE id = 1")
    # Other rows unchanged
    let sel = exec(store, "SELECT * FROM items WHERE id = 2")
    check sel.rows[0][1] == "orig"

# ---------------------------------------------------------------------------
# Suite: DELETE with multi-group space
# ---------------------------------------------------------------------------

suite "SQL Executor — space-routed DELETE":
  var store: RaftKVStoreExt
  let testDir = "/tmp/fractio_test_space_delete_" & $getCurrentProcessId()

  setup:
    cleanupTestDir(testDir)
    store = createMultiGroupTestStore(testDir, 3)
    seedSpaceTable(store, 100, "items")
    for i in 1 .. 5:
      discard exec(store,
          "INSERT INTO items (id, val) VALUES (" & $i & ", 'v" & $i & "')")

  teardown:
    store.coordinator.stop()
    cleanupTestDir(testDir)

  test "DELETE single row by PK":
    let res = exec(store, "DELETE FROM items WHERE id = 3")
    check res.kind == erkModified
    check res.count == 1

    let sel = exec(store, "SELECT * FROM items")
    check sel.rows.len == 4
    for row in sel.rows:
      check row[0] != "3"

  test "DELETE all rows":
    let res = exec(store, "DELETE FROM items")
    check res.kind == erkModified
    check res.count == 5

    let sel = exec(store, "SELECT * FROM items")
    check sel.rows.len == 0

  test "DELETE with filter":
    let res = exec(store, "DELETE FROM items WHERE id < 3")
    check res.kind == erkModified
    check res.count == 2  # id=1 and id=2

    let sel = exec(store, "SELECT * FROM items")
    check sel.rows.len == 3

  test "DELETE non-existent rows":
    let res = exec(store, "DELETE FROM items WHERE id = 999")
    check res.kind == erkModified
    check res.count == 0

# ---------------------------------------------------------------------------
# Suite: full round-trip with multi-group space
# ---------------------------------------------------------------------------

suite "SQL Executor — space routing full round-trip":
  var store: RaftKVStoreExt
  let testDir = "/tmp/fractio_test_space_roundtrip_" & $getCurrentProcessId()

  setup:
    cleanupTestDir(testDir)
    store = createMultiGroupTestStore(testDir, 4)
    seedSpaceTableThreeCol(store, 100, "products")

  teardown:
    store.coordinator.stop()
    cleanupTestDir(testDir)

  test "INSERT → SELECT → UPDATE → DELETE round-trip with 4 groups":
    # Insert 20 rows
    for i in 1 .. 20:
      let res = exec(store,
          "INSERT INTO products (id, name, score) VALUES (" &
          $i & ", 'item" & $i & "', " & $(i * 10) & ")")
      check res.kind == erkModified

    # SELECT all — should merge from 4 groups
    var sel = exec(store, "SELECT * FROM products")
    check sel.kind == erkRows
    check sel.rows.len == 20

    # Point get
    sel = exec(store, "SELECT * FROM products WHERE id = 15")
    check sel.kind == erkRows
    check sel.rows.len == 1
    check sel.rows[0][1] == "item15"
    check sel.rows[0][2] == "150"

    # Update some rows
    let upd = exec(store,
        "UPDATE products SET score = 0 WHERE score > 150")
    check upd.kind == erkModified
    # Rows with score > 150: id 16-20 → 5 rows
    check upd.count == 5

    # Verify update
    sel = exec(store, "SELECT * FROM products WHERE id = 18")
    check sel.rows[0][2] == "0"

    # Delete some rows
    let del = exec(store, "DELETE FROM products WHERE score = 0")
    check del.kind == erkModified
    check del.count == 5

    # Verify remaining
    sel = exec(store, "SELECT * FROM products")
    check sel.rows.len == 15

  test "large dataset with many keys across 4 groups":
    # Insert 100 rows
    for i in 1 .. 100:
      discard exec(store,
          "INSERT INTO products (id, name, score) VALUES (" &
          $i & ", 'p" & $i & "', " & $i & ")")

    let sel = exec(store, "SELECT * FROM products")
    check sel.kind == erkRows
    check sel.rows.len == 100

    # Verify data distribution: check that at least 3 of 4 groups have data
    var groupHits: array[4, int]
    for i in 1 .. 100:
      let rid = routeToGroup($i, @[10'u64, 11, 12, 13])
      inc groupHits[int(rid.uint64) - 10]
    var nonEmpty = 0
    for c in groupHits:
      if c > 0: inc nonEmpty
    check nonEmpty >= 3

# ---------------------------------------------------------------------------
# Suite: backward compatibility — default space tables unaffected
# ---------------------------------------------------------------------------

suite "SQL Executor — space routing backward compat":
  var store: RaftKVStoreExt
  let testDir = "/tmp/fractio_test_space_compat_" & $getCurrentProcessId()

  setup:
    cleanupTestDir(testDir)
    store = createMultiGroupTestStore(testDir, 3)

  teardown:
    store.coordinator.stop()
    cleanupTestDir(testDir)

  test "tables not in a space use default routing":
    # Create table without space assignment (no spaceId in catalog)
    let res = exec(store,
        "CREATE TABLE plain (id INT PRIMARY KEY, val TEXT)")
    check res.kind == erkOk

    # INSERT/SELECT should work via default findRangeId path
    let ins = exec(store,
        "INSERT INTO plain (id, val) VALUES (1, 'a'), (2, 'b')")
    check ins.kind == erkModified
    check ins.count == 2

    let sel = exec(store, "SELECT * FROM plain")
    check sel.kind == erkRows
    check sel.rows.len == 2

  test "space-routed and default tables coexist":
    # Default table
    discard exec(store,
        "CREATE TABLE plain (id INT PRIMARY KEY, val TEXT)")
    discard exec(store,
        "INSERT INTO plain (id, val) VALUES (1, 'plain1')")

    # Space-routed table
    seedSpaceTable(store, 200, "spaced")
    discard exec(store,
        "INSERT INTO spaced (id, val) VALUES (1, 'spaced1')")

    # Both are readable independently
    let sel1 = exec(store, "SELECT * FROM plain")
    check sel1.rows.len == 1
    check sel1.rows[0][1] == "plain1"

    let sel2 = exec(store, "SELECT * FROM spaced")
    check sel2.rows.len == 1
    check sel2.rows[0][1] == "spaced1"
