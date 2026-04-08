# Tests for the SQL Executor
#
# Integration-style tests: parse SQL → plan → execute → verify KV state.
# Uses a real single-node RaftKVStoreExt and a ProtocolServer.

import std/[unittest, options, json, os, strutils, times, random]
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
import fractio/distributed/raft/group_types
import fractio/distributed/raft/multigroup_types
import fractio/distributed/sharedtimer/mock
import fractio/distributed/sharedtimer/types as timerTypes
import fractio/core/timestamp_provider
import fractio/storage/mvcc/types as mvccTypes
import fractio/protocol/server
import fractio/client/fractio_client
import fractio/client/sql_client

# Helper for tests: create a deterministic test table ID
var testTableIdCounter {.global.} = 0
proc testTableId(): TableId =
  inc testTableIdCounter
  # Use a deterministic ULID for test purposes
  var ulid: ULID
  for i in 0..<5:
    ulid.data[i] = 0'u8 # timestamp part (zero for testing)
  for i in 5..<15:
    ulid.data[i] = 0'u8 # randomness part (zero for testing)
  ulid.data[15] = uint8(testTableIdCounter) # test number
  TableId(ulid)

# ---------------------------------------------------------------------------
# Test helper: create a single-node environment
# ---------------------------------------------------------------------------

var testBasePort {.global.} = 17000

proc nextBasePort(): int =
  result = testBasePort
  testBasePort += 100

proc createTestEnv(suiteName: string): tuple[client: FractioClient,
    server: ProtocolServer, testDir: string] =
  randomize()
  let randomId = $rand(10000..99999)
  let testDir = "/tmp/fractio_test_" & suiteName & "_" & randomId
  if dirExists(testDir): removeDir(testDir)
  createDir(testDir)

  let nodeId = NodeID(1)
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

  doAssert coord.createAndStartGroup(META_GROUP_ID, members)
  doAssert coord.createAndStartGroup(DATA_GROUP_START_ID, members)

  for attempt in 0 ..< 50:
    if coord.isLeader(META_GROUP_ID) and coord.isLeader(DATA_GROUP_START_ID):
      break
    os.sleep(100)

  let store = newRaftKVStoreExt(coord, proposeTimeoutMs = 5000)
  store.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])

  let txnMgr = newTransactionManager()
  let mvccStore = newMvccTransactionStore(store, txnMgr, nil)

  # Seed system tables via batch write for efficiency
  let nodeRec = NodeRecord(
    nodeId: 1,
    host: "127.0.0.1",
    raftPort: raftPort.uint16,
    clientPort: clientPort.uint16,
    status: nsAlive
  )
  let metaGroupRec = GroupRecord(
    groupId: groupIDToULID(META_GROUP_ID),
    spaceId: ZeroULID(),
    preferredLeader: 1, leader: 1,
    replicas: @[GroupReplicaBin(nodeId: 1, replicaType: rtVoter)]
  )
  let dataGroupRec = GroupRecord(
    groupId: groupIDToULID(DATA_GROUP_START_ID),
    spaceId: ZeroULID(),
    preferredLeader: 1, leader: 1,
    replicas: @[GroupReplicaBin(nodeId: 1, replicaType: rtVoter)]
  )
  discard store.sysTablePutBatch(@[
    (key: encodeTableKey(SYS_NODES_TABLE_ID, "1"), value: encode(nodeRec)),
    (key: encodeTableKey(SYS_GROUPS_TABLE_ID, $groupIDToULID(META_GROUP_ID)),
        value: encode(metaGroupRec)),
    (key: encodeTableKey(SYS_GROUPS_TABLE_ID, $groupIDToULID(
        DATA_GROUP_START_ID)), value: encode(dataGroupRec))
  ])

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
  if not client.initialize():
    raise newException(CatchableError, "Failed to initialize client")

  result = (client, server, testDir)

proc cleanupTestDir(testDir: string) =
  if dirExists(testDir):
    removeDir(testDir)

# ---------------------------------------------------------------------------
# Helper: execute SQL and return result
# ---------------------------------------------------------------------------

proc exec(client: FractioClient, sql: string,
    database = "default", schema = "public"): ExecResult =
  client.query(sql, database, schema)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

suite "SQL Executor — DDL":
  var client: FractioClient
  var server: ProtocolServer
  var testDir: string

  setup:
    (client, server, testDir) = createTestEnv("ddl")

  teardown:
    if client != nil: client.close()
    if server != nil:
      server.stop()
      if server.raftStore != nil and server.raftStore.coordinator != nil:
        server.raftStore.coordinator.stop()
    cleanupTestDir(testDir)

  test "CREATE DATABASE":
    let res = client.exec("CREATE DATABASE testdb")
    check res.kind == erkOk
    check res.okMessage == "CREATE DATABASE"

    # Verify in catalog (via client)
    let key = encodeTableKey(SYS_DATABASES_TABLE_ID, "testdb")
    let got = client.kvGet(key)
    check got.isOk
    check got.val.isSome

  test "CREATE DATABASE duplicate error":
    discard client.exec("CREATE DATABASE testdb")
    let res = client.exec("CREATE DATABASE testdb")
    check res.kind == erkError
    check "already exists" in res.error

  test "CREATE DATABASE IF NOT EXISTS":
    discard client.exec("CREATE DATABASE testdb")
    let res = client.exec("CREATE DATABASE IF NOT EXISTS testdb")
    check res.kind == erkOk

  test "DROP DATABASE":
    discard client.exec("CREATE DATABASE testdb")
    let res = client.exec("DROP DATABASE testdb")
    check res.kind == erkOk
    # Verify removed
    let key = encodeTableKey(SYS_DATABASES_TABLE_ID, "testdb")
    let got = client.kvGet(key)
    check got.isOk
    check got.val.isNone

  test "DROP DATABASE non-existent error":
    let res = client.exec("DROP DATABASE nope")
    check res.kind == erkError

  test "DROP DATABASE IF EXISTS":
    let res = client.exec("DROP DATABASE IF EXISTS nope")
    check res.kind == erkOk

  test "CREATE SCHEMA":
    # Need to create database first
    discard client.exec("CREATE DATABASE testdb")
    let res = client.exec("CREATE SCHEMA myschema",
        database = "testdb")
    check res.kind == erkOk

  test "DROP SCHEMA":
    discard client.exec("CREATE DATABASE testdb")
    discard client.exec("CREATE SCHEMA myschema",
        database = "testdb")
    let res = client.exec("DROP SCHEMA myschema",
        database = "testdb")
    check res.kind == erkOk

  test "CREATE TABLE":
    let res = client.exec(
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
    if res.kind == erkError:
      echo "DEBUG ERROR: ", res.error
    check res.kind == erkOk
    check res.okMessage == "CREATE TABLE"

    # Verify catalog entry - decode as binary TableRecord
    let key = encodeTableKey(SYS_TABLES_TABLE_ID,
        "default.public.users")
    let got = client.kvGet(key)
    check got.isOk
    check got.val.isSome
    let rec = decodeTableRecord(got.val.get())
    check rec.name == "users"
    check rec.columns.len == 3

  test "CREATE TABLE IF NOT EXISTS":
    discard client.exec(
        "CREATE TABLE users (id INT PRIMARY KEY)")
    let res = client.exec(
        "CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY)")
    check res.kind == erkOk

  test "CREATE TABLE duplicate error":
    discard client.exec(
        "CREATE TABLE users (id INT PRIMARY KEY)")
    let res = client.exec(
        "CREATE TABLE users (id INT PRIMARY KEY)")
    check res.kind == erkError
    check "already exists" in res.error

  test "DROP TABLE":
    discard client.exec(
        "CREATE TABLE users (id INT PRIMARY KEY)")
    let res = client.exec("DROP TABLE users")
    check res.kind == erkOk

  test "DROP TABLE IF EXISTS":
    let res = client.exec("DROP TABLE IF EXISTS nope")
    check res.kind == erkOk


suite "SQL Executor — DML":
  var client: FractioClient
  var server: ProtocolServer
  var testDir: string

  setup:
    (client, server, testDir) = createTestEnv("dml")
    # Create a test table
    discard client.exec(
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")

  teardown:
    if client != nil: client.close()
    if server != nil:
      server.stop()
      if server.raftStore != nil and server.raftStore.coordinator != nil:
        server.raftStore.coordinator.stop()
    cleanupTestDir(testDir)

  test "INSERT single row":
    let res = client.exec(
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
    if res.kind == erkError:
      echo "DEBUG ERROR: ", res.error
    check res.kind == erkModified
    check res.count == 1

    # Verify data row exists via SELECT
    let sel = client.exec("SELECT * FROM users WHERE id = 1")
    check sel.kind == erkRows
    check sel.rows.len == 1
    check sel.rows[0][1] == "Alice" # name column
    check sel.rows[0][2] == "30" # age column

  test "INSERT multiple rows":
    let res = client.exec(
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25)")
    check res.kind == erkModified
    check res.count == 2

  test "INSERT into non-existent table":
    let res = client.exec(
        "INSERT INTO nonexistent (id) VALUES (1)")
    check res.kind == erkError
    check ("not found" in res.error or "nonexistent" in res.error)

  test "SELECT all rows":
    discard client.exec(
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
    discard client.exec(
        "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")

    let res = client.exec("SELECT * FROM users")
    check res.kind == erkRows
    check res.columns == @["id", "name", "age"]
    check res.rows.len == 2

  test "SELECT with point get":
    discard client.exec(
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
    discard client.exec(
        "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")

    let res = client.exec("SELECT * FROM users WHERE id = 1")
    check res.kind == erkRows
    check res.rows.len == 1
    check res.rows[0][1] == "Alice" # name column

  test "SELECT with filter":
    discard client.exec(
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
    discard client.exec(
        "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")
    discard client.exec(
        "INSERT INTO users (id, name, age) VALUES (3, 'Carol', 35)")

    let res = client.exec("SELECT * FROM users WHERE age > 28")
    check res.kind == erkRows
    check res.rows.len == 2 # Alice (30) and Carol (35)

  test "SELECT with LIMIT":
    discard client.exec(
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
    discard client.exec(
        "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")
    discard client.exec(
        "INSERT INTO users (id, name, age) VALUES (3, 'Carol', 35)")

    let res = client.exec("SELECT * FROM users LIMIT 2")
    check res.kind == erkRows
    check res.rows.len == 2

  test "SELECT specific columns":
    discard client.exec(
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")

    let res = client.exec("SELECT name, age FROM users")
    check res.kind == erkRows
    check res.columns == @["name", "age"]
    check res.rows.len == 1
    check res.rows[0][0] == "Alice"
    check res.rows[0][1] == "30"

  test "SELECT from empty table":
    let res = client.exec("SELECT * FROM users")
    check res.kind == erkRows
    check res.rows.len == 0

  test "SELECT from non-existent table":
    let res = client.exec("SELECT * FROM nonexistent")
    check res.kind == erkError
    check ("not found" in res.error or "nonexistent" in res.error)

  test "UPDATE rows":
    discard client.exec(
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
    discard client.exec(
        "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")

    let res = client.exec("UPDATE users SET age = 31 WHERE id = 1")
    check res.kind == erkModified
    check res.count == 1

    # Verify the update
    let sel = client.exec("SELECT * FROM users WHERE id = 1")
    check sel.kind == erkRows
    check sel.rows[0][2] == "31" # age column

  test "UPDATE all rows (no WHERE)":
    discard client.exec(
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
    discard client.exec(
        "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")

    let res = client.exec("UPDATE users SET name = 'Unknown'")
    check res.kind == erkModified
    check res.count == 2

  test "DELETE rows":
    discard client.exec(
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
    discard client.exec(
        "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")

    let res = client.exec("DELETE FROM users WHERE id = 1")
    check res.kind == erkModified
    check res.count == 1

    # Verify deletion
    let sel = client.exec("SELECT * FROM users")
    check sel.kind == erkRows
    check sel.rows.len == 1

  test "DELETE all rows (no WHERE)":
    discard client.exec(
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
    discard client.exec(
        "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")

    let res = client.exec("DELETE FROM users")
    check res.kind == erkModified
    check res.count == 2

    let sel = client.exec("SELECT * FROM users")
    check sel.rows.len == 0


suite "SQL Executor — Transactions":
  var client: FractioClient
  var server: ProtocolServer
  var testDir: string

  setup:
    (client, server, testDir) = createTestEnv("txn")

  teardown:
    if client != nil: client.close()
    if server != nil:
      server.stop()
      if server.raftStore != nil and server.raftStore.coordinator != nil:
        server.raftStore.coordinator.stop()
    cleanupTestDir(testDir)

  test "BEGIN returns OK":
    let res = client.exec("BEGIN")
    check res.kind == erkOk
    check res.okMessage == "BEGIN"

  test "COMMIT returns OK":
    let res = client.exec("COMMIT")
    check res.kind == erkOk
    # COMMIT without active transaction returns a message
    check "COMMIT" in res.okMessage

  test "ROLLBACK returns OK":
    let res = client.exec("ROLLBACK")
    check res.kind == erkOk
    # ROLLBACK without active transaction returns a message
    check "ROLLBACK" in res.okMessage


suite "SQL Executor — SHOW statements":
  var client: FractioClient
  var server: ProtocolServer
  var testDir: string

  setup:
    (client, server, testDir) = createTestEnv("show")

  teardown:
    if client != nil: client.close()
    if server != nil:
      server.stop()
      if server.raftStore != nil and server.raftStore.coordinator != nil:
        server.raftStore.coordinator.stop()
    cleanupTestDir(testDir)

  test "SHOW DATABASES empty":
    let res = client.exec("SHOW DATABASES")
    check res.kind == erkRows
    check res.columns == @["database_name"]
    check res.rows.len == 0

  test "SHOW DATABASES after creating some":
    discard client.exec("CREATE DATABASE alpha")
    discard client.exec("CREATE DATABASE beta")
    discard client.exec("CREATE DATABASE gamma")
    let res = client.exec("SHOW DATABASES")
    check res.kind == erkRows
    check res.rows.len == 3
    # Check all names are present (order may vary by key sort)
    var names: seq[string]
    for row in res.rows:
      names.add(row[0])
    check "alpha" in names
    check "beta" in names
    check "gamma" in names

  test "SHOW DATABASES reflects drops":
    discard client.exec("CREATE DATABASE db1")
    discard client.exec("CREATE DATABASE db2")
    discard client.exec("DROP DATABASE db1")
    let res = client.exec("SHOW DATABASES")
    check res.rows.len == 1
    check res.rows[0][0] == "db2"

  test "SHOW SCHEMAS empty":
    let res = client.exec("SHOW SCHEMAS", database = "mydb")
    check res.kind == erkRows
    check res.columns == @["schema_name"]
    check res.rows.len == 0

  test "SHOW SCHEMAS after creating some":
    discard client.exec("CREATE DATABASE mydb")
    discard client.exec("CREATE DATABASE otherdb")
    discard client.exec("CREATE SCHEMA api", database = "mydb")
    discard client.exec("CREATE SCHEMA internal", database = "mydb")
    discard client.exec("CREATE SCHEMA other", database = "otherdb")
    let res = client.exec("SHOW SCHEMAS", database = "mydb")
    check res.kind == erkRows
    # Note: CREATE DATABASE auto-creates "public" schema, so we have 3 schemas
    check res.rows.len == 3
    var names: seq[string]
    for row in res.rows:
      names.add(row[0])
    check "public" in names # auto-created
    check "api" in names
    check "internal" in names

  test "SHOW SCHEMAS IN specific_db":
    discard client.exec("CREATE DATABASE db1")
    discard client.exec("CREATE DATABASE db2")
    discard client.exec("CREATE SCHEMA s1", database = "db1")
    discard client.exec("CREATE SCHEMA s2", database = "db2")
    let res = client.exec("SHOW SCHEMAS IN db1")
    # Note: CREATE DATABASE auto-creates "public" schema, so we have 2 schemas
    check res.rows.len == 2
    var names: seq[string]
    for row in res.rows:
      names.add(row[0])
    check "public" in names # auto-created
    check "s1" in names

  test "SHOW TABLES empty":
    let res = client.exec("SHOW TABLES")
    check res.kind == erkRows
    check res.columns == @["table_name"]
    check res.rows.len == 0

  test "SHOW TABLES after creating some":
    discard client.exec("CREATE TABLE users (id INT PRIMARY KEY)")
    discard client.exec("CREATE TABLE orders (id INT PRIMARY KEY)")
    let res = client.exec("SHOW TABLES")
    check res.kind == erkRows
    check res.rows.len == 2
    var names: seq[string]
    for row in res.rows:
      names.add(row[0])
    check "users" in names
    check "orders" in names

  test "SHOW TABLES filters by schema":
    discard client.exec("CREATE DATABASE mydb")
    discard client.exec("CREATE SCHEMA api", database = "mydb")
    discard client.exec("CREATE SCHEMA internal", database = "mydb")
    discard client.exec("CREATE TABLE t1 (id INT PRIMARY KEY)",
        database = "mydb", schema = "api")
    discard client.exec("CREATE TABLE t2 (id INT PRIMARY KEY)",
        database = "mydb", schema = "internal")
    let res = client.exec("SHOW TABLES IN api", database = "mydb")
    check res.rows.len == 1
    check res.rows[0][0] == "t1"

  test "SHOW TABLES IN db.schema":
    discard client.exec("CREATE DATABASE db1")
    discard client.exec("CREATE DATABASE db2")
    discard client.exec("CREATE SCHEMA s1", database = "db1")
    discard client.exec("CREATE SCHEMA s2", database = "db1")
    discard client.exec("CREATE SCHEMA s1", database = "db2")
    discard client.exec("CREATE TABLE t1 (id INT PRIMARY KEY)",
        database = "db1", schema = "s1")
    discard client.exec("CREATE TABLE t2 (id INT PRIMARY KEY)",
        database = "db1", schema = "s2")
    discard client.exec("CREATE TABLE t3 (id INT PRIMARY KEY)",
        database = "db2", schema = "s1")
    let res = client.exec("SHOW TABLES IN db1.s1")
    check res.rows.len == 1
    check res.rows[0][0] == "t1"

  test "SHOW TABLES reflects drops":
    discard client.exec("CREATE TABLE t1 (id INT PRIMARY KEY)")
    discard client.exec("CREATE TABLE t2 (id INT PRIMARY KEY)")
    discard client.exec("DROP TABLE t1")
    let res = client.exec("SHOW TABLES")
    check res.rows.len == 1
    check res.rows[0][0] == "t2"


suite "SQL Executor — USE statements":
  var client: FractioClient
  var server: ProtocolServer
  var testDir: string

  setup:
    (client, server, testDir) = createTestEnv("use")

  teardown:
    if client != nil: client.close()
    if server != nil:
      server.stop()
      if server.raftStore != nil and server.raftStore.coordinator != nil:
        server.raftStore.coordinator.stop()
    cleanupTestDir(testDir)

  test "USE DATABASE succeeds when database exists":
    discard client.exec("CREATE DATABASE mydb")
    let res = client.exec("USE DATABASE mydb")
    check res.kind == erkUseDatabase
    check res.newDatabase == "mydb"

  test "USE DATABASE fails when database does not exist":
    let res = client.exec("USE DATABASE nope")
    check res.kind == erkError
    check "does not exist" in res.error

  test "USE (bare) defaults to USE DATABASE":
    discard client.exec("CREATE DATABASE mydb")
    let res = client.exec("USE mydb")
    check res.kind == erkUseDatabase
    check res.newDatabase == "mydb"

  test "USE SCHEMA succeeds when schema exists":
    discard client.exec("CREATE DATABASE mydb")
    discard client.exec("CREATE SCHEMA api", database = "mydb")
    let res = client.exec("USE SCHEMA api", database = "mydb")
    check res.kind == erkUseSchema
    check res.newSchema == "api"

  test "USE SCHEMA fails when schema does not exist":
    discard client.exec("CREATE DATABASE mydb")
    let res = client.exec("USE SCHEMA nope", database = "mydb")
    check res.kind == erkError
    check "does not exist" in res.error

  test "USE SCHEMA fails when schema is in different database":
    discard client.exec("CREATE DATABASE db1")
    discard client.exec("CREATE DATABASE db2")
    discard client.exec("CREATE SCHEMA api", database = "db1")
    let res = client.exec("USE SCHEMA api", database = "db2")
    check res.kind == erkError


suite "SQL Executor — Full round-trip":
  var client: FractioClient
  var server: ProtocolServer
  var testDir: string

  setup:
    (client, server, testDir) = createTestEnv("roundtrip")

  teardown:
    if client != nil: client.close()
    if server != nil:
      server.stop()
      if server.raftStore != nil and server.raftStore.coordinator != nil:
        server.raftStore.coordinator.stop()
    cleanupTestDir(testDir)

  test "full DDL + DML round-trip":
    # Create database
    var res = client.exec("CREATE DATABASE myapp")
    check res.kind == erkOk

    # Create schema
    res = client.exec("CREATE SCHEMA api", database = "myapp")
    check res.kind == erkOk

    # Create table
    res = client.exec(
        "CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT)",
        database = "myapp", schema = "api")
    check res.kind == erkOk

    # Insert rows
    res = client.exec(
        "INSERT INTO products (id, name, price) VALUES (1, 'Widget', 999), (2, 'Gadget', 1999)",
        database = "myapp", schema = "api")
    check res.kind == erkModified
    check res.count == 2

    # Select all
    res = client.exec("SELECT * FROM products",
        database = "myapp", schema = "api")
    check res.kind == erkRows
    check res.rows.len == 2

    # Update one
    res = client.exec("UPDATE products SET price = 1099 WHERE id = 1",
        database = "myapp", schema = "api")
    check res.kind == erkModified
    check res.count == 1

    # Verify update
    res = client.exec("SELECT * FROM products WHERE id = 1",
        database = "myapp", schema = "api")
    check res.kind == erkRows
    check res.rows.len == 1
    check res.rows[0][2] == "1099"

    # Delete one
    res = client.exec("DELETE FROM products WHERE id = 2",
        database = "myapp", schema = "api")
    check res.kind == erkModified
    check res.count == 1

    # Verify only one remains
    res = client.exec("SELECT * FROM products",
        database = "myapp", schema = "api")
    check res.kind == erkRows
    check res.rows.len == 1

    # Drop table
    res = client.exec("DROP TABLE products",
        database = "myapp", schema = "api")
    check res.kind == erkOk

    # Drop schema
    res = client.exec("DROP SCHEMA api", database = "myapp")
    check res.kind == erkOk

    # Drop database
    res = client.exec("DROP DATABASE myapp")
    check res.kind == erkOk


suite "SQL Executor — Expression evaluation":
  var client: FractioClient
  var server: ProtocolServer
  var testDir: string

  setup:
    (client, server, testDir) = createTestEnv("expr")
    discard client.exec(
        "CREATE TABLE items (id INT PRIMARY KEY, name TEXT, qty INT, active BOOL)")
    discard client.exec(
        "INSERT INTO items (id, name, qty, active) VALUES (1, 'apple', 10, true)")
    discard client.exec(
        "INSERT INTO items (id, name, qty, active) VALUES (2, 'banana', 0, false)")
    discard client.exec(
        "INSERT INTO items (id, name, qty, active) VALUES (3, 'cherry', 5, true)")

  teardown:
    if client != nil: client.close()
    if server != nil:
      server.stop()
      if server.raftStore != nil and server.raftStore.coordinator != nil:
        server.raftStore.coordinator.stop()
    cleanupTestDir(testDir)

  test "WHERE with AND":
    let res = client.exec(
        "SELECT * FROM items WHERE qty > 0 AND active = true")
    check res.kind == erkRows
    check res.rows.len == 2 # apple and cherry

  test "WHERE with OR":
    let res = client.exec(
        "SELECT * FROM items WHERE qty = 0 OR qty = 10")
    check res.kind == erkRows
    check res.rows.len == 2 # apple and banana

  test "WHERE with comparison operators":
    var res = client.exec("SELECT * FROM items WHERE qty >= 5")
    check res.rows.len == 2 # apple (10) and cherry (5)

    res = client.exec("SELECT * FROM items WHERE qty <= 5")
    check res.rows.len == 2 # banana (0) and cherry (5)


suite "SQL Executor — EXPLAIN":
  var client: FractioClient
  var server: ProtocolServer
  var testDir: string

  setup:
    (client, server, testDir) = createTestEnv("explain")

  teardown:
    if client != nil: client.close()
    if server != nil:
      server.stop()
      if server.raftStore != nil and server.raftStore.coordinator != nil:
        server.raftStore.coordinator.stop()
    cleanupTestDir(testDir)

  test "EXPLAIN SELECT full scan":
    discard client.exec(
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
    let res = client.exec("EXPLAIN SELECT * FROM users")
    check res.kind == erkRows
    check res.columns == @["plan"]
    check res.rows.len == 1
    check "Scan" in res.rows[0][0]

  test "EXPLAIN SELECT point get":
    discard client.exec(
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
    let res = client.exec("EXPLAIN SELECT * FROM users WHERE id = 42")
    check res.kind == erkRows
    check res.rows.len == 1
    check "PointGet" in res.rows[0][0]
    check "42" in res.rows[0][0]

  test "EXPLAIN SELECT with filter":
    discard client.exec(
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
    let res = client.exec("EXPLAIN SELECT * FROM users WHERE age > 21")
    check res.kind == erkRows
    check "Scan" in res.rows[0][0]
    check "filter" in res.rows[0][0]

  test "EXPLAIN INSERT":
    discard client.exec(
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
    let res = client.exec(
        "EXPLAIN INSERT INTO users (id, name) VALUES (1, 'Alice')")
    check res.kind == erkRows
    check "Insert" in res.rows[0][0]
    check "rows=1" in res.rows[0][0]

  test "EXPLAIN CREATE TABLE":
    let res = client.exec(
        "EXPLAIN CREATE TABLE t1 (id INT PRIMARY KEY)")
    check res.kind == erkRows
    check "CreateTable" in res.rows[0][0]

  test "EXPLAIN does not execute the statement":
    let res = client.exec(
        "EXPLAIN CREATE TABLE invisible (id INT PRIMARY KEY)")
    check res.kind == erkRows
    # The table should NOT have been created
    let showRes = client.exec("SHOW TABLES")
    check showRes.kind == erkRows
    check showRes.rows.len == 0
