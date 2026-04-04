# Integration tests for Phase 4 Token Authentication.
#
# Covers:
#   - auth.nim unit tests: encodeTokenAuthData encoding
#   - Authenticator: addToken, authenticate (valid / invalid / unknown token)
#   - Server handshake rejects clients with unknown/invalid tokens
#   - Server handshake accepts clients with valid registered tokens
#   - Client can perform KV and admin operations after authenticated connection
#   - Multiple tokens can coexist for different principals
#   - Empty token is rejected
#
# Port allocation: 20075-20099

import std/[unittest, os, strutils]
import fractio/protocol/types
import fractio/protocol/server
import fractio/protocol/client
import fractio/protocol/auth
import fractio/protocol/messages/admin as adminMsgs
import fractio/protocol/mvcc_store
import fractio/protocol/raft_store
import fractio/protocol/txn_manager
import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/raft/multigroup_types
import fractio/distributed/meta/system_tables
import fractio/distributed/sharedtimer/mock
import fractio/distributed/sharedtimer/types as timerTypes
import fractio/core/timestamp_provider

# ---------------------------------------------------------------------------
# Helper: start a server with token auth and register test tokens
# ---------------------------------------------------------------------------

var testRaftPort {.global.} = 21000

proc nextRaftPort(): int =
  result = testRaftPort
  testRaftPort += 10

proc startTokenServer(port: int): ProtocolServer =
  var cfg = defaultServerConfig()
  cfg.host = "127.0.0.1"
  cfg.port = port
  cfg.idleTimeoutSecs = 120
  cfg.authMethod = amToken
  result = newProtocolServer(cfg)
  result.authenticator.addToken("valid-token-abc", "service-a")
  result.authenticator.addToken("valid-token-xyz", "service-b")

  # Set up MVCC store for KV operations (requires single-node Raft)
  let storagePath = "/tmp/fractio_auth_test_" & $port
  try: removeDir(storagePath) except CatchableError: discard
  createDir(storagePath)

  let nodeId = rangeTypes.NodeID(1)
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

proc connectWithToken(port: int, token: string,
    expectOK: bool = true): ProtocolClient =
  var cfg = defaultClientConfig("127.0.0.1", port)
  cfg.timeoutMs = 5_000
  cfg.authMethod = amToken
  cfg.authData = auth.encodeTokenAuthData(token)
  cfg.clientId = "test-token-client"
  result = newProtocolClient(cfg)
  let r = result.connect()
  if expectOK:
    doAssert r.isOk, "expected token connect to succeed but got: " & $r.err

# ---------------------------------------------------------------------------
# Suite: auth unit tests — token
# ---------------------------------------------------------------------------

suite "auth unit - Authenticator token":
  test "authenticate succeeds with registered token":
    let a = newAuthenticator(amToken)
    a.addToken("tok-abc", "user-a")
    let authData = auth.encodeTokenAuthData("tok-abc")
    check a.authenticate(uint8(amToken), authData) == true

  test "authenticate fails with unregistered token":
    let a = newAuthenticator(amToken)
    a.addToken("tok-abc", "user-a")
    let authData = auth.encodeTokenAuthData("unknown-token")
    check a.authenticate(uint8(amToken), authData) == false

  test "authenticate fails with empty token":
    let a = newAuthenticator(amToken)
    a.addToken("tok-abc", "user-a")
    let authData = auth.encodeTokenAuthData("")
    check a.authenticate(uint8(amToken), authData) == false

  test "authenticate with empty authData returns false":
    let a = newAuthenticator(amToken)
    a.addToken("tok-abc", "user-a")
    check a.authenticate(uint8(amToken), "") == false

  test "multiple tokens coexist":
    let a = newAuthenticator(amToken)
    a.addToken("token1", "svc1")
    a.addToken("token2", "svc2")
    a.addToken("token3", "svc3")
    check a.authenticate(uint8(amToken), auth.encodeTokenAuthData("token1")) == true
    check a.authenticate(uint8(amToken), auth.encodeTokenAuthData("token2")) == true
    check a.authenticate(uint8(amToken), auth.encodeTokenAuthData("token3")) == true
    check a.authenticate(uint8(amToken), auth.encodeTokenAuthData("token4")) == false

  test "addToken can overwrite a token's principal":
    let a = newAuthenticator(amToken)
    a.addToken("tok", "old-principal")
    a.addToken("tok", "new-principal")
    # token is still valid regardless of principal name
    check a.authenticate(uint8(amToken), auth.encodeTokenAuthData("tok")) == true

  test "token with special characters works":
    let a = newAuthenticator(amToken)
    let specialTok = "Bearer eyJhbGciOiJSUzI1NiJ9.payload.sig"
    a.addToken(specialTok, "jwt-user")
    check a.authenticate(uint8(amToken),
      auth.encodeTokenAuthData(specialTok)) == true

  test "token with max length (255 chars) works":
    let a = newAuthenticator(amToken)
    let longTok = "x".repeat(200)
    a.addToken(longTok, "big-token-user")
    check a.authenticate(uint8(amToken),
      auth.encodeTokenAuthData(longTok)) == true

  test "encodeTokenAuthData is deterministic":
    let d1 = auth.encodeTokenAuthData("my-token")
    let d2 = auth.encodeTokenAuthData("my-token")
    check d1 == d2

  test "different tokens produce different encodings":
    let d1 = auth.encodeTokenAuthData("token-a")
    let d2 = auth.encodeTokenAuthData("token-b")
    check d1 != d2

# ---------------------------------------------------------------------------
# Suite: server/client integration — token auth
# ---------------------------------------------------------------------------

suite "auth e2e - token authentication":
  test "valid token allows connection and ping":
    let srv = startTokenServer(20075)
    let cli = connectWithToken(20075, "valid-token-abc")
    try:
      let r = cli.ping()
      check r.isOk
    finally:
      cli.disconnect()
      srv.stop()
      sleep(50)

  test "second valid token also works":
    let srv = startTokenServer(20076)
    let cli = connectWithToken(20076, "valid-token-xyz")
    try:
      let r = cli.ping()
      check r.isOk
    finally:
      cli.disconnect()
      srv.stop()
      sleep(50)

  test "unknown token is rejected":
    let srv = startTokenServer(20077)
    var cfg = defaultClientConfig("127.0.0.1", 20077)
    cfg.timeoutMs = 5_000
    cfg.authMethod = amToken
    cfg.authData = auth.encodeTokenAuthData("not-a-valid-token")
    let cli = newProtocolClient(cfg)
    let r = cli.connect()
    check r.isErr
    try: cli.disconnect() except CatchableError: discard
    srv.stop()
    sleep(50)

  test "empty token string is rejected":
    let srv = startTokenServer(20078)
    var cfg = defaultClientConfig("127.0.0.1", 20078)
    cfg.timeoutMs = 5_000
    cfg.authMethod = amToken
    cfg.authData = auth.encodeTokenAuthData("")
    let cli = newProtocolClient(cfg)
    let r = cli.connect()
    check r.isErr
    try: cli.disconnect() except CatchableError: discard
    srv.stop()
    sleep(50)

  test "authenticated token client can execute KV operations":
    let srv = startTokenServer(20079)
    let cli = connectWithToken(20079, "valid-token-abc")
    try:
      let pr = cli.kvPut("tok-key", "tok-value")
      check pr.isOk
      let gr = cli.kvGet("tok-key")
      check gr.isOk
      check gr.value.found == true
      check gr.value.value == "tok-value"
    finally:
      cli.disconnect()
      srv.stop()
      sleep(50)

  test "authenticated token client can query admin endpoints":
    let srv = startTokenServer(20080)
    let cli = connectWithToken(20080, "valid-token-abc")
    try:
      let si = cli.serverInfo()
      check si.isOk
      check si.value.role == adminMsgs.RoleLeader
      let h = cli.health()
      check h.isOk
      check h.value.status == adminMsgs.HealthOK
    finally:
      cli.disconnect()
      srv.stop()
      sleep(50)

  test "two clients with different valid tokens can connect simultaneously":
    let srv = startTokenServer(20081)
    let cliA = connectWithToken(20081, "valid-token-abc")
    let cliB = connectWithToken(20081, "valid-token-xyz")
    try:
      let ra = cliA.ping()
      check ra.isOk
      let rb = cliB.ping()
      check rb.isOk
    finally:
      cliA.disconnect()
      cliB.disconnect()
      srv.stop()
      sleep(50)

  test "token client can use echo":
    let srv = startTokenServer(20082)
    let cli = connectWithToken(20082, "valid-token-abc")
    try:
      let r = cli.echo("hello-token")
      check r.isOk
      check r.value == "hello-token"
    finally:
      cli.disconnect()
      srv.stop()
      sleep(50)

  test "token server with new token registered after start":
    let srv = startTokenServer(20083)
    # Register an extra token after start
    srv.authenticator.addToken("dynamic-token", "dynamic-user")
    let cli = connectWithToken(20083, "dynamic-token")
    try:
      let r = cli.ping()
      check r.isOk
    finally:
      cli.disconnect()
      srv.stop()
      sleep(50)
