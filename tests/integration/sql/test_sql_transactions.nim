# End-to-end tests for SQL transactions with MVCC
#
# Tests the complete flow from SQL statements through the executor
# with MVCC transaction support.
#
# Port range: 20700-20729

import std/[unittest, os, options, strutils, json, random, times]
import fractio/core/types except NodeID
import fractio/protocol/raft_store
import fractio/protocol/mvcc_store
import fractio/protocol/txn_manager
import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/meta/system_tables
import fractio/distributed/meta/system_schemas
import fractio/distributed/sharedtimer/mock
import fractio/distributed/sharedtimer/types as timerTypes
import fractio/core/timestamp_provider
import fractio/sql/executor
import fractio/sql/parser
import fractio/sql/planner
import fractio/storage/wisckey_backend
import fractio/protocol/server
import fractio/client/fractio_client
import fractio/client/sql_client

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

var testBasePort {.global.} = 20700

proc nextBasePort(): int =
  result = testBasePort
  testBasePort += 100

proc cleanDir(path: string) =
  try: removeDir(path) except CatchableError: discard
  try: createDir(path) except CatchableError: discard

proc makeTestEnv(suiteName: string): tuple[
    client: FractioClient, server: ProtocolServer, testDir: string] =
  let randomId = $rand(10000..99999)
  let testDir = "/tmp/fractio_sql_txn_" & suiteName & "_" & randomId
  cleanDir(testDir)

  let nodeId = rangeTypes.NodeID(1)
  let raftPort = nextBasePort()
  let clientPort = nextBasePort()
  let members = @[(nodeId: 1'u32, host: "127.0.0.1", port: raftPort)]

  let coord = newNuRaftCoordinator(nuraft_coordinator.CoordinatorConfig(
    nodeId: nodeId,
    port: raftPort,
    host: "127.0.0.1",
    dataDir: testDir,
    electionTimeoutLowerMs: 200,
    electionTimeoutUpperMs: 400,
    heartbeatIntervalMs: 100,
  ))
  coord.start()

  for rid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    doAssert coord.createAndStartGroup(rid, members)

  for attempt in 0 ..< 50:
    if coord.isLeader(META_GROUP_ID) and coord.isLeader(DATA_GROUP_START_ID):
      break
    os.sleep(100)

  let raftStore = newRaftKVStoreExt(coord, proposeTimeoutMs = 2000)
  raftStore.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])

  # Seed system tables via batch write for efficiency
  let nodeRec = NodeRecord(
    nodeId: 1, host: "127.0.0.1", raftPort: raftPort.uint16,
    clientPort: clientPort.uint16, status: nsAlive
  )
  let metaGroupRec = GroupRecord(
    groupId: groupIDToULID(META_GROUP_ID), spaceId: ZeroULID(), leader: 1,
    replicas: @[GroupReplicaBin(nodeId: 1, replicaType: rtVoter)]
  )
  let dataGroupRec = GroupRecord(
    groupId: groupIDToULID(DATA_GROUP_START_ID), spaceId: ZeroULID(), leader: 1,
    replicas: @[GroupReplicaBin(nodeId: 1, replicaType: rtVoter)]
  )
  discard raftStore.sysTablePutBatch(@[
    (key: encodeTableKey(SYS_NODES_TABLE_ID, "1"), value: encode(nodeRec)),
    (key: encodeTableKey(SYS_GROUPS_TABLE_ID, $META_GROUP_ID), value: encode(
        metaGroupRec)),
    (key: encodeTableKey(SYS_GROUPS_TABLE_ID, $DATA_GROUP_START_ID),
        value: encode(dataGroupRec))
  ])

  let txnMgr = newTransactionManager()
  let mvccStore = newMvccTransactionStore(raftStore, txnMgr, nil)

  # Start ProtocolServer
  var srvConfig = defaultServerConfig()
  srvConfig.port = clientPort
  srvConfig.host = "127.0.0.1"
  srvConfig.serverId = nodeId.uint16
  srvConfig.dataDir = testDir
  let server = newProtocolServer(srvConfig)
  server.raftStore = raftStore
  server.mvccStore = mvccStore
  server.txnMgr = txnMgr
  server.start()

  # Create client
  let client = newFractioClient("127.0.0.1", clientPort)
  if not client.initialize():
    raise newException(CatchableError, "Failed to initialize client")

  result = (client, server, testDir)

proc teardownTestEnv(client: FractioClient, server: ProtocolServer,
    testDir: string) =
  if client != nil: client.close()
  if server != nil:
    server.stop()
    if server.raftStore != nil and server.raftStore.coordinator != nil:
      server.raftStore.coordinator.stop()
  try: removeDir(testDir) except CatchableError: discard

# ---------------------------------------------------------------------------
# Suite: Basic Transaction Flow
# ---------------------------------------------------------------------------

suite "SQL Transactions - Basic Flow":
  test "BEGIN and COMMIT without changes":
    let (client, server, testDir) = makeTestEnv("basic_commit")
    defer: teardownTestEnv(client, server, testDir)

    let res1 = client.query("BEGIN")
    check res1.kind == erkOk

    let res2 = client.query("COMMIT")
    check res2.kind == erkOk

  test "BEGIN and ROLLBACK":
    let (client, server, testDir) = makeTestEnv("basic_rollback")
    defer: teardownTestEnv(client, server, testDir)

    let res1 = client.query("BEGIN")
    check res1.kind == erkOk

    let res2 = client.query("ROLLBACK")
    check res2.kind == erkOk

  test "Implicit Transaction per statement":
    let (client, server, testDir) = makeTestEnv("implicit_txn")
    defer: teardownTestEnv(client, server, testDir)

    discard client.query("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")

    # Each INSERT runs in its own implicit txn
    let res1 = client.query("INSERT INTO users (id, name) VALUES (1, 'Alice')")
    check res1.kind == erkModified
    check res1.count == 1

    let res2 = client.query("SELECT * FROM users WHERE id = 1")
    check res2.kind == erkRows
    check res2.rows.len == 1
    check res2.rows[0][1] == "Alice"

suite "SQL Transactions - Data Isolation":
  test "Snapshot Isolation (Read your own writes)":
    let (client, server, testDir) = makeTestEnv("isolation_own_writes")
    defer: teardownTestEnv(client, server, testDir)

    discard client.query("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")

    discard client.query("BEGIN")
    discard client.query("INSERT INTO users (id, name) VALUES (1, 'Alice')")

    # Should see its own insert
    let res = client.query("SELECT * FROM users WHERE id = 1")
    check res.kind == erkRows
    check res.rows.len == 1
    check res.rows[0][1] == "Alice"

    discard client.query("COMMIT")

  test "Snapshot Isolation (Dirty Read prevention)":
    let (client1, server, testDir) = makeTestEnv("isolation_dirty_read")
    let client2 = newFractioClient("127.0.0.1", server.config.port)
    doAssert client2.initialize()
    defer:
      client1.close()
      client2.close()
      server.stop()
      server.raftStore.coordinator.stop()
      try: removeDir(testDir) except: discard

    discard client1.query("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")

    discard client1.query("BEGIN")
    discard client1.query("INSERT INTO users (id, name) VALUES (1, 'Alice')")

    # Other client should NOT see uncommitted insert
    let res = client2.query("SELECT * FROM users WHERE id = 1")
    check res.kind == erkRows
    check res.rows.len == 0

    discard client1.query("COMMIT")

    # Now it should be visible
    let res2 = client2.query("SELECT * FROM users WHERE id = 1")
    check res2.rows.len == 1

suite "SQL Transactions - Conflict Detection":
  test "Write-Write Conflict detection":
    let (client1, server, testDir) = makeTestEnv("ww_conflict")
    let client2 = newFractioClient("127.0.0.1", server.config.port)
    doAssert client2.initialize()
    defer:
      client1.close()
      client2.close()
      server.stop()
      server.raftStore.coordinator.stop()
      try: removeDir(testDir) except: discard

    discard client1.query("CREATE TABLE counter (id INT PRIMARY KEY, val INT)")
    discard client1.query("INSERT INTO counter (id, val) VALUES (1, 10)")

    discard client1.query("BEGIN")
    discard client2.query("BEGIN")

    # Client 1 updates
    discard client1.query("UPDATE counter SET val = 11 WHERE id = 1")

    # Client 2 updates same row
    discard client2.query("UPDATE counter SET val = 12 WHERE id = 1")

    # Client 1 commits first - should succeed
    let c1 = client1.query("COMMIT")
    check c1.kind == erkOk

    # Client 2 commits - should fail with conflict
    let c2 = client2.query("COMMIT")
    check c2.kind == erkError
    check "conflict" in c2.error.toLowerAscii

suite "SQL Transactions - Complex Scenarios":
  test "Multi-statement explicit transaction":
    let (client, server, testDir) = makeTestEnv("multi_stmt")
    defer: teardownTestEnv(client, server, testDir)

    discard client.query("CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)")
    discard client.query("INSERT INTO accounts (id, balance) VALUES (1, 1000), (2, 500)")

    discard client.query("BEGIN")
    discard client.query("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
    discard client.query("UPDATE accounts SET balance = balance + 100 WHERE id = 2")
    discard client.query("COMMIT")

    let res1 = client.query("SELECT balance FROM accounts WHERE id = 1")
    check res1.rows[0][0] == "900"
    let res2 = client.query("SELECT balance FROM accounts WHERE id = 2")
    check res2.rows[0][0] == "600"

  test "Explicit rollback":
    let (client, server, testDir) = makeTestEnv("explicit_rollback")
    defer: teardownTestEnv(client, server, testDir)

    discard client.query("CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)")
    discard client.query("INSERT INTO accounts (id, balance) VALUES (1, 1000)")

    discard client.query("BEGIN")
    discard client.query("UPDATE accounts SET balance = 0 WHERE id = 1")
    discard client.query("ROLLBACK")

    let res = client.query("SELECT balance FROM accounts WHERE id = 1")
    check res.rows[0][0] == "1000"
