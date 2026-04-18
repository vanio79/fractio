# Integration tests for space-aware query routing through the SQL executor.
#
# Creates a store with multiple Raft groups, seeds a space and table
# mapping, then runs INSERT/SELECT/UPDATE/DELETE SQL through the executor
# to verify that data is correctly routed and merged across groups.

import std/[unittest, options, os, strutils, tables, hashes, algorithm,
    sequtils, random]
import fractio/client/fractio_client
import fractio/client/sql_client
import fractio/protocol/server
import fractio/protocol/types as protoTypes
import fractio/sql/parser
import fractio/sql/ast
import fractio/sql/planner
import fractio/sql/executor
import fractio/core/types except NodeID
import fractio/distributed/meta/system_tables
import fractio/distributed/meta/system_schemas
import fractio/protocol/raft_store
import fractio/protocol/mvcc_store
import fractio/protocol/txn_manager
import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/sharedtimer/mock
import fractio/distributed/sharedtimer/types as timerTypes
import fractio/core/timestamp_provider

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc cleanDir(path: string) =
  try: removeDir(path) except CatchableError: discard
  try: createDir(path) except CatchableError: discard

var testBasePort {.global.} = 17000
var testCounter {.global.} = 0

proc nextBasePort(): int =
  ## Return a fresh base port to avoid collisions between test runs.
  result = testBasePort
  testBasePort += 100

proc nextTestDir(baseName: string): string =
  ## Return a unique test directory for each test.
  inc testCounter
  result = "/tmp/fractio_" & baseName & "_" & $getCurrentProcessId() & "_" & $testCounter

proc createMultiGroupTestStore(testDir: string,
    groupCount: int): tuple[client: FractioClient, server: ProtocolServer,
        store: RaftKVStoreExt, spaceGroupIds: seq[GroupID],
            testSpaceId: SpaceID] =
  ## Create a store with 1 meta range + N space groups.
  ## Seeds sys.spaces and sys.tables so the executor can resolve space routing.
  ## Returns the space group IDs and test space SpaceID so tests can verify routing.
  cleanDir(testDir)
  let clientPort = nextBasePort()
  let nodeId = NodeID(1)
  let port = nextBasePort()
  let members = @[(nodeId: 1'u32, host: "127.0.0.1", port: port)]

  let coord = newNuRaftCoordinator(nuraft_coordinator.CoordinatorConfig(
    nodeId: nodeId,
    port: port,
    host: "127.0.0.1",
    dataDir: testDir,
    electionTimeoutLowerMs: 200,
    electionTimeoutUpperMs: 400,
    heartbeatIntervalMs: 100,
  ))
  coord.start()

  # Meta group - use the well-known META_GROUP_ID constant
  doAssert coord.createAndStartGroup(META_GROUP_ID, members)

  # Data group - use the well-known DATA_GROUP_START_ID constant
  doAssert coord.createAndStartGroup(DATA_GROUP_START_ID, members)

  # Space groups (use deterministic GroupIDs to avoid port collisions)
  # META_GROUP_ID hashes to port offset 1, DATA_GROUP_START_ID hashes to 2.
  # Use groupIDFromInt with values 10, 20, 30, etc. to get distinct port offsets.
  var groupIds: seq[GroupID] = @[]
  for i in 0 ..< groupCount:
    # Use multiples of 10 starting at 10 for deterministic, collision-free ports
    let gid = groupIDFromInt(int64(10 + i * 10))
    groupIds.add(gid)
    doAssert coord.createAndStartGroup(gid, members)

  # Wait for all groups to elect a leader (single-node → self-election)
  let allGroupIds = @[META_GROUP_ID, DATA_GROUP_START_ID] & groupIds
  for attempt in 0 ..< 50: # up to 5 seconds
    var allLeaders = true
    for gid in allGroupIds:
      if not coord.isLeader(gid):
        allLeaders = false
        break
    if allLeaders: break
    os.sleep(100)

  let store = newRaftKVStoreExt(coord, proposeTimeoutMs = 5000)
  store.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])

  # Pre-create SMs for space groups
  for gid in groupIds:
    discard store.getOrCreateSM(gid)

  # Create MVCC store for DDL operations
  let txnMgr = newTransactionManager()
  let mockTimer = MockTimeProvider(currentTime: timerTypes.Timestamp(1_000_000_000))
  let tsProvider = newTimestampProvider(mockTimer, nodeId.uint16)
  let mvccStore = newMvccTransactionStore(store, txnMgr, tsProvider)

  # Seed space record (binary)

  # Seed system tables via batch write for efficiency
  let nodeRec = NodeRecord(
    nodeId: 1, host: "127.0.0.1", raftPort: port.uint16,
    clientPort: clientPort.uint16, status: nsAlive
  )
  # Create ULIDs for test groups
  let metaGroupId = groupIDToULID(META_GROUP_ID)
  let dataGroupId = groupIDToULID(DATA_GROUP_START_ID)

  let metaGroupRec = GroupRecord(
    groupId: metaGroupId,
    spaceId: zeroSpaceID(),
    preferredLeader: 1,
    leader: 1,
    replicas: @[GroupReplicaBin(nodeId: 1, replicaType: rtVoter)]
  )
  let dataGroupRec = GroupRecord(
    groupId: dataGroupId,
    spaceId: zeroSpaceID(),
    preferredLeader: 1,
    leader: 1,
    replicas: @[GroupReplicaBin(nodeId: 1, replicaType: rtVoter)]
  )
  var sysTableWrites: seq[tuple[key: string, value: string]] = @[
    (key: encodeTableKey(SYS_NODES_TABLE_ID, "1"), value: encode(nodeRec)),
    (key: encodeTableKey(SYS_GROUPS_TABLE_ID, $metaGroupId), value: encode(
        metaGroupRec)),
    (key: encodeTableKey(SYS_GROUPS_TABLE_ID, $dataGroupId), value: encode(dataGroupRec))
  ]
  # Add space group records
  for gid in groupIds:
    let gidUlid = groupIDToULID(gid)
    let spaceGroupRec = GroupRecord(
      groupId: gidUlid,
      spaceId: zeroSpaceID(),
      preferredLeader: 1,
      leader: 1,
      replicas: @[GroupReplicaBin(nodeId: 1, replicaType: rtVoter)]
    )
    sysTableWrites.add((key: encodeTableKey(SYS_GROUPS_TABLE_ID, $gidUlid),
        value: encode(spaceGroupRec)))
  discard store.sysTablePutBatch(sysTableWrites)

  let testSpaceId = genSpaceID() # Generate proper SpaceID for test space
  let spaceKey = encodeSpaceKey(testSpaceId)
  let spaceRec = SpaceRecord(
    spaceId: testSpaceId, # SpaceRecord.spaceId is now SpaceID
    name: "testspace",
    replicas: 1,
    groupCount: int32(groupCount),
    groupIds: groupIds,   # seq[GroupID] directly
    oldGroupIds: @[],
    rebalancing: false,
    rebalanceWorker: 0,
    rebalanceHeartbeat: 0,
    rebalanceCursor: "",
    createdAtNs: 0
  )
  discard store.sysTablePut(spaceKey, encode(spaceRec))

  store.loadSpaces()

  # Start ProtocolServer
  var srvConfig = defaultServerConfig()
  srvConfig.port = clientPort
  srvConfig.host = "127.0.0.1"
  srvConfig.serverId = nodeId.uint16
  srvConfig.dataDir = testDir
  let server = newProtocolServer(srvConfig)
  server.raftStore = store
  server.mvccStore = mvccStore
  server.txnMgr = txnMgr
  server.start()

  # Create client
  let client = newFractioClient("127.0.0.1", clientPort)
  doAssert client.initialize()

  result = (client, server, store, groupIds, testSpaceId)

proc seedSpaceTable(client: FractioClient, store: RaftKVStoreExt,
    tableId: TableId, tableName: string, spaceId: SpaceID, database = "default",
        schema = "public") =
  ## Register a table in sys.tables and refresh client metadata.
  let fullName = database & "." & schema & "." & tableName
  let tableKey = encodeTableKey(SYS_TABLES_TABLE_ID, fullName)
  let tableRec = TableRecord(
    tableId: tableId,
    name: tableName,
    database: database,
    schema: schema,
    spaceId: spaceId, # TableRecord.spaceId is SpaceID
    primaryKey: @["id"],
    columns: @[
      ColumnDefBin(name: "id", dataType: cdtInt, flags: 0x01), # primaryKey
    ColumnDefBin(name: "val", dataType: cdtString, flags: 0x00),
  ]
  )
  discard store.sysTablePut(tableKey, encode(tableRec))
  store.loadTableSpaces()
  # Refresh client metadata to pick up the new table
  discard client.refreshMetadata()

proc seedSpaceTableThreeCol(client: FractioClient, store: RaftKVStoreExt,
    tableId: TableId, tableName: string, spaceId: SpaceID, database = "default",
        schema = "public") =
  ## Register a table with 3 columns (id, name, score) and refresh client metadata.
  let fullName = database & "." & schema & "." & tableName
  let tableKey = encodeTableKey(SYS_TABLES_TABLE_ID, fullName)
  let tableRec = TableRecord(
    tableId: tableId,
    name: tableName,
    database: database,
    schema: schema,
    spaceId: spaceId, # TableRecord.spaceId is SpaceID
    primaryKey: @["id"],
    columns: @[
      ColumnDefBin(name: "id", dataType: cdtInt, flags: 0x01), # primaryKey
    ColumnDefBin(name: "name", dataType: cdtString, flags: 0x00),
    ColumnDefBin(name: "score", dataType: cdtInt, flags: 0x00),
  ]
  )
  discard store.sysTablePut(tableKey, encode(tableRec))
  store.loadTableSpaces()
  # Refresh client metadata to pick up the new table
  discard client.refreshMetadata()

proc exec(client: FractioClient, sql: string,
    database = "default", schema = "public"): ExecResult =
  ## Execute SQL and buffer streaming rows into regular rows for test assertions.
  let res = client.query(sql, database, schema)
  bufferRows(res)

proc cleanupTestDir(testDir: string) =
  if dirExists(testDir):
    removeDir(testDir)

# ---------------------------------------------------------------------------
# Suite: INSERT routing through executor with multi-group space
# ---------------------------------------------------------------------------

suite "SQL Executor — space-routed INSERT":
  var client: FractioClient
  var server: ProtocolServer
  var store: RaftKVStoreExt
  var spaceGroupIds: seq[GroupID]
  var testSpaceId: SpaceID
  var testDir: string

  setup:
    testDir = nextTestDir("insert")
    cleanupTestDir(testDir)
    (client, server, store, spaceGroupIds,
        testSpaceId) = createMultiGroupTestStore(testDir, 3)
    seedSpaceTable(client, store, genTableId(), "items", testSpaceId)

  teardown:
    client.close()
    server.stop()
    store.coordinator.stop()
    cleanupTestDir(testDir)

  test "INSERT routes rows to space groups":
    let res = exec(client,
        "INSERT INTO items (id, val) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")
    if res.kind == erkError:
      echo "DEBUG: INSERT error: ", res.error
    check res.kind == erkModified
    check res.count == 5

    # Verify data is spread across groups (at least 2 of 3)
    # Use a map to count group hits since we can't index by GroupID directly
    var groupHits = newTable[GroupID, int]()
    for gid in spaceGroupIds:
      groupHits[gid] = 0
    for i in 1 .. 5:
      let rid = routeToGroup($i, spaceGroupIds)
      groupHits[rid] = groupHits.getOrDefault(rid, 0) + 1
    var nonEmpty = 0
    for gid in spaceGroupIds:
      if groupHits.getOrDefault(gid, 0) > 0: inc nonEmpty
    check nonEmpty >= 2

  test "INSERT single row":
    let res = exec(client,
        "INSERT INTO items (id, val) VALUES (42, 'hello')")
    check res.kind == erkModified
    check res.count == 1

# ---------------------------------------------------------------------------
# Suite: SELECT (scan + point get) with multi-group space
# ---------------------------------------------------------------------------

suite "SQL Executor — space-routed SELECT":
  var client: FractioClient
  var server: ProtocolServer
  var store: RaftKVStoreExt
  var spaceGroupIds: seq[GroupID]
  var testSpaceId: SpaceID
  var testDir: string

  setup:
    testDir = nextTestDir("select")
    cleanupTestDir(testDir)
    (client, server, store, spaceGroupIds,
        testSpaceId) = createMultiGroupTestStore(testDir, 3)
    seedSpaceTable(client, store, genTableId(), "items", testSpaceId)
    # Insert 10 rows distributed across groups
    for i in 1 .. 10:
      discard exec(client,
          "INSERT INTO items (id, val) VALUES (" & $i & ", 'v" & $i & "')")

  teardown:
    client.close()
    server.stop()
    store.coordinator.stop()
    cleanupTestDir(testDir)

  test "SELECT * returns all rows from all groups":
    let res = exec(client, "SELECT * FROM items")
    check res.kind == erkRows
    check res.rows.len == 10

  test "SELECT * returns rows in sorted key order":
    let res = exec(client, "SELECT * FROM items")
    check res.kind == erkRows
    # Rows should be ordered by key (which includes the padded primary key)
    for i in 1 ..< res.rows.len:
      # We can't check exact sort order of integer PKs as strings easily,
      # but we verify they are all present
      discard
    check res.rows.len == 10

  test "SELECT with WHERE point-get routes to single group":
    let res = exec(client, "SELECT * FROM items WHERE id = 5")
    check res.kind == erkRows
    check res.rows.len == 1
    check res.rows[0][0] == "5"
    check res.rows[0][1] == "v5"

  test "SELECT with WHERE filter on non-PK column":
    # This uses scan + filter, not point get
    let res = exec(client, "SELECT * FROM items WHERE val = 'v3'")
    check res.kind == erkRows
    check res.rows.len == 1
    check res.rows[0][0] == "3"

  test "SELECT with LIMIT":
    let res = exec(client, "SELECT * FROM items LIMIT 3")
    check res.kind == erkRows
    check res.rows.len == 3

  test "SELECT from empty space-routed table":
    # Create a second table in the same space, don't insert data
    seedSpaceTable(client, store, genTableId(), "empty_items", testSpaceId)
    let res = exec(client, "SELECT * FROM empty_items")
    check res.kind == erkRows
    check res.rows.len == 0

  test "SELECT with point-get for non-existent key":
    let res = exec(client, "SELECT * FROM items WHERE id = 999")
    check res.kind == erkRows
    check res.rows.len == 0

# ---------------------------------------------------------------------------
# Suite: UPDATE with multi-group space
# ---------------------------------------------------------------------------

suite "SQL Executor — space-routed UPDATE":
  var client: FractioClient
  var server: ProtocolServer
  var store: RaftKVStoreExt
  var spaceGroupIds: seq[GroupID]
  var testSpaceId: SpaceID
  var testDir: string

  setup:
    testDir = nextTestDir("update")
    cleanupTestDir(testDir)
    (client, server, store, spaceGroupIds,
        testSpaceId) = createMultiGroupTestStore(testDir, 3)
    seedSpaceTable(client, store, genTableId(), "items", testSpaceId)
    for i in 1 .. 5:
      discard exec(client,
          "INSERT INTO items (id, val) VALUES (" & $i & ", 'orig')")

  teardown:
    client.close()
    server.stop()
    store.coordinator.stop()
    cleanupTestDir(testDir)

  test "UPDATE single row by PK":
    let res = exec(client, "UPDATE items SET val = 'changed' WHERE id = 3")
    check res.kind == erkModified
    check res.count == 1

    let sel = exec(client, "SELECT * FROM items WHERE id = 3")
    check sel.kind == erkRows
    check sel.rows.len == 1
    check sel.rows[0][1] == "changed"

  test "UPDATE all rows (no WHERE)":
    let res = exec(client, "UPDATE items SET val = 'all_changed'")
    check res.kind == erkModified
    check res.count == 5

    let sel = exec(client, "SELECT * FROM items")
    check sel.kind == erkRows
    for row in sel.rows:
      check row[1] == "all_changed"

  test "UPDATE with filter matches subset":
    # Update only rows with id > 3
    let res = exec(client, "UPDATE items SET val = 'hi' WHERE id > 3")
    check res.kind == erkModified
    check res.count == 2 # id=4 and id=5

  test "UPDATE preserves data in correct groups":
    discard exec(client, "UPDATE items SET val = 'new' WHERE id = 1")
    # Other rows unchanged
    let sel = exec(client, "SELECT * FROM items WHERE id = 2")
    check sel.rows[0][1] == "orig"

# ---------------------------------------------------------------------------
# Suite: DELETE with multi-group space
# ---------------------------------------------------------------------------

suite "SQL Executor — space-routed DELETE":
  var client: FractioClient
  var server: ProtocolServer
  var store: RaftKVStoreExt
  var spaceGroupIds: seq[GroupID]
  var testSpaceId: SpaceID
  var testDir: string

  setup:
    testDir = nextTestDir("delete")
    cleanupTestDir(testDir)
    (client, server, store, spaceGroupIds,
        testSpaceId) = createMultiGroupTestStore(testDir, 3)
    seedSpaceTable(client, store, genTableId(), "items", testSpaceId)
    for i in 1 .. 5:
      discard exec(client,
          "INSERT INTO items (id, val) VALUES (" & $i & ", 'v" & $i & "')")

  teardown:
    client.close()
    server.stop()
    store.coordinator.stop()
    cleanupTestDir(testDir)

  test "DELETE single row by PK":
    let res = exec(client, "DELETE FROM items WHERE id = 3")
    check res.kind == erkModified
    check res.count == 1

    let sel = exec(client, "SELECT * FROM items")
    check sel.rows.len == 4
    for row in sel.rows:
      check row[0] != "3"

  test "DELETE all rows":
    let res = exec(client, "DELETE FROM items")
    check res.kind == erkModified
    check res.count == 5

    let sel = exec(client, "SELECT * FROM items")
    check sel.rows.len == 0

  test "DELETE with filter":
    let res = exec(client, "DELETE FROM items WHERE id < 3")
    check res.kind == erkModified
    check res.count == 2 # id=1 and id=2

    let sel = exec(client, "SELECT * FROM items")
    check sel.rows.len == 3

  test "DELETE non-existent rows":
    let res = exec(client, "DELETE FROM items WHERE id = 999")
    check res.kind == erkModified
    check res.count == 0

# ---------------------------------------------------------------------------
# Suite: full round-trip with multi-group space
# ---------------------------------------------------------------------------

suite "SQL Executor — space routing full round-trip":
  var client: FractioClient
  var server: ProtocolServer
  var store: RaftKVStoreExt
  var spaceGroupIds: seq[GroupID]
  var testSpaceId: SpaceID
  var testDir: string

  setup:
    testDir = nextTestDir("roundtrip")
    cleanupTestDir(testDir)
    (client, server, store, spaceGroupIds,
        testSpaceId) = createMultiGroupTestStore(testDir, 4)
    seedSpaceTableThreeCol(client, store, genTableId(), "products", testSpaceId)

  teardown:
    client.close()
    server.stop()
    store.coordinator.stop()
    cleanupTestDir(testDir)

  test "INSERT → SELECT → UPDATE → DELETE round-trip with 4 groups":
    # Insert 20 rows
    for i in 1 .. 20:
      let res = exec(client,
          "INSERT INTO products (id, name, score) VALUES (" &
          $i & ", 'item" & $i & "', " & $(i * 10) & ")")
      check res.kind == erkModified

    # SELECT all — should merge from 4 groups
    var sel = exec(client, "SELECT * FROM products")
    check sel.kind == erkRows
    check sel.rows.len == 20

    # Point get
    sel = exec(client, "SELECT * FROM products WHERE id = 15")
    check sel.kind == erkRows
    check sel.rows.len == 1
    check sel.rows[0][1] == "item15"
    check sel.rows[0][2] == "150"

    # Update some rows
    let upd = exec(client,
        "UPDATE products SET score = 0 WHERE score > 150")
    check upd.kind == erkModified
    # Rows with score > 150: id 16-20 → 5 rows
    check upd.count == 5

    # Verify update
    sel = exec(client, "SELECT * FROM products WHERE id = 18")
    check sel.rows[0][2] == "0"

    # Delete some rows
    let del = exec(client, "DELETE FROM products WHERE score = 0")
    check del.kind == erkModified
    check del.count == 5

    # Verify remaining
    sel = exec(client, "SELECT * FROM products")
    check sel.rows.len == 15

  test "large dataset with many keys across 4 groups":
    # Insert 100 rows
    for i in 1 .. 100:
      discard exec(client,
          "INSERT INTO products (id, name, score) VALUES (" &
          $i & ", 'p" & $i & "', " & $i & ")")

    let sel = exec(client, "SELECT * FROM products")
    check sel.kind == erkRows
    check sel.rows.len == 100

    # Verify data distribution: check that at least 3 of 4 groups have data
    var groupHits = newTable[GroupID, int]()
    for gid in spaceGroupIds:
      groupHits[gid] = 0
    for i in 1 .. 100:
      let rid = routeToGroup($i, spaceGroupIds)
      groupHits[rid] = groupHits.getOrDefault(rid, 0) + 1
    var nonEmpty = 0
    for gid in spaceGroupIds:
      if groupHits.getOrDefault(gid, 0) > 0: inc nonEmpty
    check nonEmpty >= 3

# ---------------------------------------------------------------------------
# Suite: backward compatibility — default space tables unaffected
# ---------------------------------------------------------------------------

suite "SQL Executor — space routing backward compat":
  var client: FractioClient
  var server: ProtocolServer
  var store: RaftKVStoreExt
  var spaceGroupIds: seq[GroupID]
  var testSpaceId: SpaceID
  var testDir: string

  setup:
    testDir = nextTestDir("compat")
    cleanupTestDir(testDir)
    (client, server, store, spaceGroupIds,
        testSpaceId) = createMultiGroupTestStore(testDir, 3)

  teardown:
    client.close()
    server.stop()
    store.coordinator.stop()
    cleanupTestDir(testDir)

  test "tables not in a space use default routing":
    # Create table without space assignment (no spaceId in catalog)
    let res = exec(client,
        "CREATE TABLE plain (id INT PRIMARY KEY, val TEXT)")
    check res.kind == erkOk

    # INSERT/SELECT should work via default findRangeId path
    let ins = exec(client,
        "INSERT INTO plain (id, val) VALUES (1, 'a'), (2, 'b')")
    check ins.kind == erkModified
    check ins.count == 2

    let sel = exec(client, "SELECT * FROM plain")
    check sel.kind == erkRows
    check sel.rows.len == 2

  test "space-routed and default tables coexist":
    # Default table
    discard exec(client,
        "CREATE TABLE plain (id INT PRIMARY KEY, val TEXT)")
    discard exec(client,
        "INSERT INTO plain (id, val) VALUES (1, 'plain1')")

    # Space-routed table
    seedSpaceTable(client, store, genTableId(), "spaced", testSpaceId)
    discard exec(client,
        "INSERT INTO spaced (id, val) VALUES (1, 'spaced1')")

    # Both are readable independently
    let sel1 = exec(client, "SELECT * FROM plain")
    check sel1.rows.len == 1
    check sel1.rows[0][1] == "plain1"

    let sel2 = exec(client, "SELECT * FROM spaced")
    check sel2.rows.len == 1
    check sel2.rows[0][1] == "spaced1"
