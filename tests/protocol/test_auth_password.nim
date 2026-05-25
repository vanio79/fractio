# Integration tests for Phase 4 Password Authentication.
#
# Covers:
#   - auth.nim unit tests: encodePasswordAuthData round-trip
#   - Authenticator: addUser, authenticate (correct / wrong password / unknown user)
#   - Server handshake rejects clients with wrong credentials
#   - Server handshake accepts clients with correct credentials
#   - Client can perform KV operations after authenticated connection
#   - Server with amNone ignores authData
#
# Port allocation: 20050-20074

import std/[unittest, os]
import fractio/protocol/types
import fractio/protocol/server
import fractio/protocol/client
import fractio/protocol/auth
import fractio/protocol/messages/admin as adminMsgs
import fractio/protocol/mvcc_store
import fractio/protocol/raft_store
import fractio/protocol/txn_manager
import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/group_types 
import fractio/distributed/raft/multigroup_types
import fractio/distributed/meta/system_tables
import fractio/distributed/sharedtimer/mock
import fractio/distributed/sharedtimer/types as timerTypes
import fractio/core/timestamp_provider

# ---------------------------------------------------------------------------
# Helper: start a server with password auth and register a test user
# ---------------------------------------------------------------------------

var testRaftPort {.global.} = 22000

proc nextRaftPort(): int =
  result = testRaftPort
  testRaftPort += 10

proc startPasswordServer(port: int, username,
    password: string): ProtocolServer =
  var cfg = defaultServerConfig()
  cfg.host = "127.0.0.1"
  cfg.port = port
  cfg.idleTimeoutSecs = 120
  cfg.authMethod = amPassword
  result = newProtocolServer(cfg)
  result.authenticator.addUser(username, password)

  # Set up MVCC store for KV operations (requires single-node Raft)
  let storagePath = "/tmp/fractio_authpw_test_" & $port
  try: removeDir(storagePath) except CatchableError: discard
  createDir(storagePath)

  let nodeId = NodeID(1)
  let raftPort = nextRaftPort()
  let members = @[(nodeId: 1'u32, host: "127.0.0.1", port: raftPort)]

  let coord = newNuRaftCoordinator(nuraft_coordinator.CoordinatorConfig(
    nodeId: nodeId,
    port: raftPort,
    host: "127.0.0.1",
    dataDir: storagePath,
    electionTimeoutLowerMs: 200,
    electionTimeoutUpperMs: 400,
    heartbeatIntervalMs: 100,
  ))
  coord.start()

  # Create meta + data groups
  for gid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    discard coord.createAndStartGroup(gid, members)

  # Wait for leader election on both groups
  for attempt in 0 ..< 30:
    if coord.isLeader(META_GROUP_ID) and coord.isLeader(DATA_GROUP_START_ID):
      break
    sleep(100)

  let raftStore = newRaftKVStoreExt(coord, proposeTimeoutMs = 2000)
  raftStore.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])

  let txnMgr = newTransactionManager()
  let mockTimer = MockTimeProvider(currentTime: timerTypes.Timestamp(1_000_000_000))
  let tsProvider = newTimestampProvider(mockTimer, nodeId.uint16)
  let mvccStore = newMvccTransactionStore(raftStore, txnMgr, tsProvider)

  result.raftStore = raftStore
  result.raftCoord = coord
  result.mvccStore = mvccStore
  result.txnMgr = txnMgr

  result.start()
  sleep(100)

proc connectWithPassword(port: int, username, password: string,
    expectOK: bool = true): ProtocolClient =
  var cfg = defaultClientConfig("127.0.0.1", port)
  cfg.timeoutMs = 5_000
  cfg.authMethod = amPassword
  cfg.authData = auth.encodePasswordAuthData(username, password)
  cfg.clientId = "test-pw-client"
  result = newProtocolClient(cfg)
  let r = result.connect()
  if expectOK:
    doAssert r.isOk, "expected connect to succeed but got: " & $r.err
  # If !expectOK we just return; caller checks connected state

# ---------------------------------------------------------------------------
# Suite: auth unit tests
# ---------------------------------------------------------------------------

suite "auth unit - Authenticator password":
  test "authenticate succeeds with correct credentials":
    let a = newAuthenticator(amPassword)
    a.addUser("alice", "s3cr3t")
    let authData = auth.encodePasswordAuthData("alice", "s3cr3t")
    check a.authenticate(uint8(amPassword), authData) == true

  test "authenticate fails with wrong password":
    let a = newAuthenticator(amPassword)
    a.addUser("alice", "correct")
    let authData = auth.encodePasswordAuthData("alice", "wrong")
    check a.authenticate(uint8(amPassword), authData) == false

  test "authenticate fails with unknown user":
    let a = newAuthenticator(amPassword)
    a.addUser("alice", "pw")
    let authData = auth.encodePasswordAuthData("bob", "pw")
    check a.authenticate(uint8(amPassword), authData) == false

  test "authenticate fails with empty password":
    let a = newAuthenticator(amPassword)
    a.addUser("alice", "nonempty")
    let authData = auth.encodePasswordAuthData("alice", "")
    check a.authenticate(uint8(amPassword), authData) == false

  test "authenticate succeeds after addUser called twice (overwrite)":
    let a = newAuthenticator(amPassword)
    a.addUser("alice", "old")
    a.addUser("alice", "new")
    let authData = auth.encodePasswordAuthData("alice", "new")
    check a.authenticate(uint8(amPassword), authData) == true

  test "authenticate fails with old password after overwrite":
    let a = newAuthenticator(amPassword)
    a.addUser("alice", "old")
    a.addUser("alice", "new")
    let authData = auth.encodePasswordAuthData("alice", "old")
    check a.authenticate(uint8(amPassword), authData) == false

  test "authenticate with empty authData returns false":
    let a = newAuthenticator(amPassword)
    a.addUser("alice", "pw")
    check a.authenticate(uint8(amPassword), "") == false

  test "amNone always authenticates regardless of authData":
    let a = newAuthenticator(amNone)
    check a.authenticate(uint8(amNone), "") == true
    check a.authenticate(uint8(amNone), "garbage") == true

  test "encodePasswordAuthData round-trip":
    let data = auth.encodePasswordAuthData("user123", "pass456")
    let a = newAuthenticator(amPassword)
    a.addUser("user123", "pass456")
    check a.authenticate(uint8(amPassword), data) == true

  test "multiple users can authenticate independently":
    let a = newAuthenticator(amPassword)
    a.addUser("alice", "apw")
    a.addUser("bob", "bpw")
    a.addUser("carol", "cpw")
    check a.authenticate(uint8(amPassword), auth.encodePasswordAuthData("alice",
        "apw")) == true
    check a.authenticate(uint8(amPassword), auth.encodePasswordAuthData("bob",
        "bpw")) == true
    check a.authenticate(uint8(amPassword), auth.encodePasswordAuthData("carol",
        "cpw")) == true
    check a.authenticate(uint8(amPassword), auth.encodePasswordAuthData("alice",
        "bpw")) == false

# ---------------------------------------------------------------------------
# Suite: server/client integration — password auth
# ---------------------------------------------------------------------------

suite "auth e2e - password authentication":
  test "correct credentials allow connection":
    let srv = startPasswordServer(20050, "admin", "secret")
    let cli = connectWithPassword(20050, "admin", "secret", expectOK = true)
    try:
      let r = cli.ping()
      check r.isOk
    finally:
      cli.disconnect()
      srv.stop()
      sleep(50)

  test "wrong password is rejected":
    let srv = startPasswordServer(20051, "admin", "secret")
    var cfg = defaultClientConfig("127.0.0.1", 20051)
    cfg.timeoutMs = 5_000
    cfg.authMethod = amPassword
    cfg.authData = auth.encodePasswordAuthData("admin", "wrong")
    let cli = newProtocolClient(cfg)
    let r = cli.connect()
    check r.isErr
    try: cli.disconnect() except CatchableError: discard
    srv.stop()
    sleep(50)

  test "unknown user is rejected":
    let srv = startPasswordServer(20052, "admin", "secret")
    var cfg = defaultClientConfig("127.0.0.1", 20052)
    cfg.timeoutMs = 5_000
    cfg.authMethod = amPassword
    cfg.authData = auth.encodePasswordAuthData("nobody", "secret")
    let cli = newProtocolClient(cfg)
    let r = cli.connect()
    check r.isErr
    try: cli.disconnect() except CatchableError: discard
    srv.stop()
    sleep(50)

  test "authenticated client can execute KV operations":
    let srv = startPasswordServer(20053, "kvuser", "kvpass")
    let cli = connectWithPassword(20053, "kvuser", "kvpass")
    try:
      let pr = cli.kvPut("auth-key", "auth-value")
      check pr.isOk
      let gr = cli.kvGet("auth-key")
      check gr.isOk
      check gr.value.found == true
      check gr.value.value == "auth-value"
    finally:
      cli.disconnect()
      srv.stop()
      sleep(50)

  test "authenticated client can query server info":
    let srv = startPasswordServer(20054, "infuser", "infpass")
    let cli = connectWithPassword(20054, "infuser", "infpass")
    try:
      let r = cli.serverInfo()
      check r.isOk
      check r.value.role == adminMsgs.RoleLeader
    finally:
      cli.disconnect()
      srv.stop()
      sleep(50)

  test "multiple authenticated clients connect simultaneously":
    let srv = startPasswordServer(20055, "multi", "mpw")
    var clients: seq[ProtocolClient] = @[]
    for i in 0..2:
      let c = connectWithPassword(20055, "multi", "mpw")
      clients.add(c)
    try:
      for c in clients:
        let r = c.ping()
        check r.isOk
    finally:
      for c in clients:
        c.disconnect()
      srv.stop()
      sleep(50)

  test "amNone server ignores authData (no credentials needed)":
    var cfg = defaultServerConfig()
    cfg.host = "127.0.0.1"
    cfg.port = 20056
    cfg.idleTimeoutSecs = 120
    cfg.authMethod = amNone
    let srv = newProtocolServer(cfg)
    srv.start()
    sleep(60)
    # Connect with garbage authData — should still work
    var ccfg = defaultClientConfig("127.0.0.1", 20056)
    ccfg.authMethod = amNone
    ccfg.authData = "totally-random-garbage"
    let cli = newProtocolClient(ccfg)
    let r = cli.connect()
    check r.isOk
    try:
      let pr = cli.ping()
      check pr.isOk
    finally:
      cli.disconnect()
      srv.stop()
      sleep(50)
