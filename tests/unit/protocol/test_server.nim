# Unit tests for fractio/protocol/server.nim
# Tests ServerConfig, ClientConnection, KVStore, NodeRegistry, ServerMetrics

import std/[unittest, tables, options, times, atomics, locks, net, os]
import fractio/protocol/server
import fractio/protocol/messages/cluster
import fractio/protocol/messages/admin
import fractio/protocol/types
import fractio/distributed/sharedtimer/types

suite "ServerConfig":

  test "defaultServerConfig":
    let cfg = defaultServerConfig()
    check cfg.host == "0.0.0.0"
    check cfg.port == 9000
    check cfg.maxConnections == 1024
    check cfg.maxFrameBytes > 0
    check cfg.maxKeyBytes > 0
    check cfg.maxValueBytes > 0
    check cfg.idleTimeoutSecs == 30
    check cfg.keepaliveIntervalSecs == 10
    check cfg.tlsEnabled == false
    check cfg.authMethod == amNone
    check cfg.serverName == "fractio"
    check cfg.serverId == 1
    check cfg.clusterId == 0
    check cfg.serverVersion == "1.0.0"
    check cfg.clusterName == "fractio"
    check cfg.sharedTimerEnabled == false
    check cfg.sharedTimerPeers.len == 0
    check cfg.dataDir == ""

  test "custom ServerConfig":
    let cfg = ServerConfig(
      host: "127.0.0.1",
      port: 8080,
      maxConnections: 512,
      serverName: "test-server",
      serverId: 42,
      clusterId: 12345,
      serverVersion: "2.0.0",
    )
    check cfg.host == "127.0.0.1"
    check cfg.port == 8080
    check cfg.maxConnections == 512
    check cfg.serverName == "test-server"
    check cfg.serverId == 42
    check cfg.clusterId == 12345
    check cfg.serverVersion == "2.0.0"

test "ServerConfig with SharedTimer":
  let cfg = ServerConfig(
    sharedTimerEnabled: true,
    sharedTimerNodeId: "node-1",
    sharedTimerNumericNodeId: 5,
    sharedTimerPeers: @[PeerConfig(peerId: "node-2", address: "10.0.0.2",
        port: 9001)],
  )
  check cfg.sharedTimerEnabled == true
  check cfg.sharedTimerNodeId == "node-1"
  check cfg.sharedTimerNumericNodeId == 5
  check cfg.sharedTimerPeers.len == 1

suite "ClientConnection":

  test "newClientConnection":
    let sock = newSocket()
    let conn = newClientConnection(1, sock, "127.0.0.1:12345")
    check conn.id == 1
    check conn.address == "127.0.0.1:12345"
    check conn.negotiatedFeatures == 0
    check conn.authenticated == false
    check conn.createdAt > 0
    check conn.lastActivityMs > 0
    sock.close()

  test "newClientConnection different IDs":
    let sock1 = newSocket()
    let sock2 = newSocket()
    let conn1 = newClientConnection(100, sock1, "addr1")
    let conn2 = newClientConnection(200, sock2, "addr2")
    check conn1.id == 100
    check conn2.id == 200
    check conn1.id != conn2.id
    sock1.close()
    sock2.close()

  test "touchActivity updates lastActivityMs":
    let sock = newSocket()
    let conn = newClientConnection(1, sock, "addr")
    let before = conn.lastActivityMs
    sleep(10)
    conn.touchActivity()
    let after = conn.lastActivityMs
    check after >= before
    sock.close()

  test "isIdle false for new connection":
    let sock = newSocket()
    let conn = newClientConnection(1, sock, "addr")
    check conn.isIdle(30) == false
    sock.close()

  test "isIdle true after simulated timeout":
    let sock = newSocket()
    let conn = newClientConnection(1, sock, "addr")
    conn.lastActivityMs = conn.lastActivityMs - 35_000
    check conn.isIdle(30) == true
    sock.close()

  test "isIdle threshold exactly at timeout":
    let sock = newSocket()
    let conn = newClientConnection(1, sock, "addr")
    conn.lastActivityMs = conn.lastActivityMs - 30_000
    check conn.isIdle(30) == false
    sock.close()

suite "KVStore":

  test "newKVStore":
    let store = newKVStore()
    check store != nil
    check store.data.len == 0
    check store.nextVersion.load() == 1

  test "kvPut basic":
    let store = newKVStore()
    let entry = store.kvPut("key1", "value1")
    check entry.value == "value1"
    check entry.version >= 1
    check entry.timestamp > 0
    check store.data.len == 1
    check store.data.hasKey("key1")

  test "kvPut multiple keys":
    let store = newKVStore()
    discard store.kvPut("key1", "value1")
    discard store.kvPut("key2", "value2")
    discard store.kvPut("key3", "value3")
    check store.data.len == 3
    check store.data.hasKey("key1")
    check store.data.hasKey("key2")
    check store.data.hasKey("key3")

  test "kvPut overwrites existing key":
    let store = newKVStore()
    let entry1 = store.kvPut("key1", "value1")
    let entry2 = store.kvPut("key1", "value2")
    check entry2.value == "value2"
    check entry2.version > entry1.version
    check store.data.len == 1

  test "kvGet existing key":
    let store = newKVStore()
    discard store.kvPut("key1", "value1")
    let opt = store.kvGet("key1")
    check opt.isSome
    check opt.get().value == "value1"
    check opt.get().version >= 1

  test "kvGet non-existing key":
    let store = newKVStore()
    let opt = store.kvGet("nonexistent")
    check opt.isNone

  test "kvGet empty store":
    let store = newKVStore()
    let opt = store.kvGet("anykey")
    check opt.isNone

  test "kvDelete existing key":
    let store = newKVStore()
    discard store.kvPut("key1", "value1")
    let opt = store.kvDelete("key1")
    check opt.isSome
    check opt.get().value == "value1"
    check store.data.len == 0

  test "kvDelete non-existing key":
    let store = newKVStore()
    let opt = store.kvDelete("nonexistent")
    check opt.isNone
    check store.data.len == 0

  test "kvScan empty store":
    let store = newKVStore()
    let pairs = store.kvScan("", "", 0)
    check pairs.len == 0

  test "kvScan no limit":
    let store = newKVStore()
    discard store.kvPut("a", "va")
    discard store.kvPut("b", "vb")
    discard store.kvPut("c", "vc")
    let pairs = store.kvScan("", "", 0)
    check pairs.len == 3

  test "kvScan with limit":
    let store = newKVStore()
    discard store.kvPut("a", "va")
    discard store.kvPut("b", "vb")
    discard store.kvPut("c", "vc")
    let pairs = store.kvScan("", "", 2)
    check pairs.len == 2

  test "kvScan with startKey":
    let store = newKVStore()
    discard store.kvPut("a", "va")
    discard store.kvPut("b", "vb")
    discard store.kvPut("c", "vc")
    discard store.kvPut("d", "vd")
    let pairs = store.kvScan("b", "", 0)
    check pairs.len == 3
    check pairs[0][0] == "b"
    check pairs[1][0] == "c"
    check pairs[2][0] == "d"

  test "kvScan with endKey":
    let store = newKVStore()
    discard store.kvPut("a", "va")
    discard store.kvPut("b", "vb")
    discard store.kvPut("c", "vc")
    discard store.kvPut("d", "vd")
    let pairs = store.kvScan("", "c", 0)
    check pairs.len == 2
    check pairs[0][0] == "a"
    check pairs[1][0] == "b"

  test "kvScan with both startKey and endKey":
    let store = newKVStore()
    discard store.kvPut("a", "va")
    discard store.kvPut("b", "vb")
    discard store.kvPut("c", "vc")
    discard store.kvPut("d", "vd")
    discard store.kvPut("e", "ve")
    let pairs = store.kvScan("b", "d", 0)
    check pairs.len == 2
    check pairs[0][0] == "b"
    check pairs[1][0] == "c"

  test "kvScan sorted order":
    let store = newKVStore()
    discard store.kvPut("z", "vz")
    discard store.kvPut("a", "va")
    discard store.kvPut("m", "vm")
    let pairs = store.kvScan("", "", 0)
    check pairs.len == 3
    check pairs[0][0] == "a"
    check pairs[1][0] == "m"
    check pairs[2][0] == "z"

  test "kvLen empty":
    let store = newKVStore()
    check store.kvLen() == 0

  test "kvLen after puts":
    let store = newKVStore()
    discard store.kvPut("k1", "v1")
    discard store.kvPut("k2", "v2")
    check store.kvLen() == 2

  test "kvLen after delete":
    let store = newKVStore()
    discard store.kvPut("k1", "v1")
    discard store.kvPut("k2", "v2")
    discard store.kvDelete("k1")
    check store.kvLen() == 1

suite "ClusterNodeEntry":

  test "ClusterNodeEntry construction":
    let entry = ClusterNodeEntry(
      nodeId: 1,
      host: "10.0.0.1",
      raftPort: 9001,
      clientPort: 9000,
      webPort: 8080,
      status: NodeStatusActive,
    )
    check entry.nodeId == 1
    check entry.host == "10.0.0.1"
    check entry.raftPort == 9001
    check entry.clientPort == 9000
    check entry.webPort == 8080
    check entry.status == NodeStatusActive

  test "ClusterNodeEntry with different status":
    let entry = ClusterNodeEntry(
      nodeId: 2,
      host: "10.0.0.2",
      raftPort: 9001,
      clientPort: 9000,
      webPort: 8080,
      status: NodeStatusDraining,
    )
    check entry.status == NodeStatusDraining

suite "NodeRegistry":

  test "newNodeRegistry":
    let reg = newNodeRegistry()
    check reg != nil
    check reg.nodes.len == 0

  test "addNode":
    let reg = newNodeRegistry()
    let entry = ClusterNodeEntry(
      nodeId: 1,
      host: "10.0.0.1",
      raftPort: 9001,
      clientPort: 9000,
      webPort: 8080,
      status: NodeStatusActive,
    )
    reg.addNode(entry)
    check reg.nodes.len == 1
    check reg.nodes.hasKey(1)
    check reg.nodes[1].host == "10.0.0.1"

  test "addNode multiple":
    let reg = newNodeRegistry()
    reg.addNode(ClusterNodeEntry(nodeId: 1, host: "h1", raftPort: 1,
        clientPort: 1, webPort: 1, status: NodeStatusActive))
    reg.addNode(ClusterNodeEntry(nodeId: 2, host: "h2", raftPort: 2,
        clientPort: 2, webPort: 2, status: NodeStatusActive))
    reg.addNode(ClusterNodeEntry(nodeId: 3, host: "h3", raftPort: 3,
        clientPort: 3, webPort: 3, status: NodeStatusActive))
    check reg.nodes.len == 3

  test "removeNode existing":
    let reg = newNodeRegistry()
    reg.addNode(ClusterNodeEntry(nodeId: 1, host: "h1", raftPort: 1,
        clientPort: 1, webPort: 1, status: NodeStatusActive))
    let removed = reg.removeNode(1)
    check removed == true
    check reg.nodes.len == 0

  test "removeNode non-existing":
    let reg = newNodeRegistry()
    let removed = reg.removeNode(999)
    check removed == false
    check reg.nodes.len == 0

  test "drainNode existing":
    let reg = newNodeRegistry()
    reg.addNode(ClusterNodeEntry(nodeId: 1, host: "h1", raftPort: 1,
        clientPort: 1, webPort: 1, status: NodeStatusActive))
    let drained = reg.drainNode(1)
    check drained == true
    check reg.nodes[1].status == NodeStatusDraining

  test "drainNode non-existing":
    let reg = newNodeRegistry()
    let drained = reg.drainNode(999)
    check drained == false

  test "listNodes empty":
    let reg = newNodeRegistry()
    let nodes = reg.listNodes()
    check nodes.len == 0

  test "listNodes multiple":
    let reg = newNodeRegistry()
    reg.addNode(ClusterNodeEntry(nodeId: 1, host: "h1", raftPort: 1,
        clientPort: 1, webPort: 1, status: NodeStatusActive))
    reg.addNode(ClusterNodeEntry(nodeId: 2, host: "h2", raftPort: 2,
        clientPort: 2, webPort: 2, status: NodeStatusActive))
    let nodes = reg.listNodes()
    check nodes.len == 2

suite "ServerMetrics":

  test "newServerMetrics":
    let m = newServerMetrics()
    check m.requestsTotal.load() == 0
    check m.requestsOK.load() == 0
    check m.requestsErr.load() == 0
    check m.bytesIn.load() == 0
    check m.bytesOut.load() == 0
    check m.kvGets.load() == 0
    check m.kvPuts.load() == 0
    check m.kvDeletes.load() == 0
    check m.committedTxns.load() == 0
    check m.abortedTxns.load() == 0

  test "atomic increments":
    let m = newServerMetrics()
    discard m.requestsTotal.fetchAdd(1)
    discard m.requestsTotal.fetchAdd(5)
    check m.requestsTotal.load() == 6

  test "snapshot":
    let m = newServerMetrics()
    discard m.requestsTotal.fetchAdd(100)
    discard m.requestsOK.fetchAdd(90)
    discard m.requestsErr.fetchAdd(10)
    discard m.bytesIn.fetchAdd(1024)
    discard m.bytesOut.fetchAdd(512)
    discard m.kvGets.fetchAdd(50)
    discard m.kvPuts.fetchAdd(30)
    discard m.kvDeletes.fetchAdd(10)
    discard m.committedTxns.fetchAdd(5)
    discard m.abortedTxns.fetchAdd(2)
    let snap = m.snapshot()
    check snap.requestsTotal == 100
    check snap.requestsOK == 90
    check snap.requestsErr == 10
    check snap.bytesIn == 1024
    check snap.bytesOut == 512
    check snap.kvGets == 50
    check snap.kvPuts == 30
    check snap.kvDeletes == 10
    check snap.committedTxns == 5
    check snap.abortedTxns == 2

  test "reset":
    let m = newServerMetrics()
    discard m.requestsTotal.fetchAdd(100)
    discard m.requestsOK.fetchAdd(90)
    discard m.kvGets.fetchAdd(50)
    m.reset()
    check m.requestsTotal.load() == 0
    check m.requestsOK.load() == 0
    check m.requestsErr.load() == 0
    check m.bytesIn.load() == 0
    check m.bytesOut.load() == 0
    check m.kvGets.load() == 0
    check m.kvPuts.load() == 0
    check m.kvDeletes.load() == 0
    check m.committedTxns.load() == 0
    check m.abortedTxns.load() == 0

suite "ProtocolServer Constructor":

  test "newProtocolServer basic":
    let cfg = defaultServerConfig()
    let srv = newProtocolServer(cfg)
    check srv != nil
    check srv.config.host == "0.0.0.0"
    check srv.config.port == 9000
    check srv.logger != nil
    check srv.running.load() == false
    check srv.nextClientId.load() == 1
    check srv.kvStore != nil
    check srv.txnMgr != nil
    check srv.metrics != nil
    check srv.authenticator != nil
    check srv.nodeRegistry != nil
    check srv.clients.len == 0
    check srv.handlers.len == 0

  test "newProtocolServer with custom config":
    let cfg = ServerConfig(
      host: "192.168.1.1",
      port: 7000,
      serverName: "custom-server",
      serverId: 99,
      clusterId: 12345,
      serverVersion: "3.0.0",
    )
    let srv = newProtocolServer(cfg)
    check srv.config.host == "192.168.1.1"
    check srv.config.port == 7000
    check srv.config.serverName == "custom-server"
    check srv.config.serverId == 99
    check srv.config.clusterId == 12345
    check srv.config.serverVersion == "3.0.0"

  test "newProtocolServer serverFeatures":
    let cfg = defaultServerConfig()
    let srv = newProtocolServer(cfg)
    let features = srv.serverFeatures
    check (features and FeatPipelining) != 0
    check (features and FeatTransactions) != 0
    check (features and FeatAsync) != 0
    check (features and FeatTLS) == 0

  test "newProtocolServer with TLS":
    let cfg = ServerConfig(
      host: "0.0.0.0",
      port: 9000,
      tlsEnabled: true,
    )
    let srv = newProtocolServer(cfg)
    let features = srv.serverFeatures
    check (features and FeatTLS) != 0

  test "clientCount empty":
    let cfg = defaultServerConfig()
    let srv = newProtocolServer(cfg)
    check srv.clientCount() == 0

suite "ProtocolServer Handler Registration":

  test "registerHandler":
    let cfg = defaultServerConfig()
    let srv = newProtocolServer(cfg)
    var handlerCalled = false
    proc testHandler(conn: ClientConnection, requestId: uint32,
        flags: uint16, payload: string) {.gcsafe, raises: [].} =
      handlerCalled = true
    srv.registerHandler(mtPing, testHandler)
    check srv.handlers.len == 1
    check srv.handlers.hasKey(int(mtPing))

  test "registerHandler multiple":
    let cfg = defaultServerConfig()
    let srv = newProtocolServer(cfg)
    proc handler1(conn: ClientConnection, requestId: uint32,
        flags: uint16, payload: string) {.gcsafe, raises: [].} = discard
    proc handler2(conn: ClientConnection, requestId: uint32,
        flags: uint16, payload: string) {.gcsafe, raises: [].} = discard
    srv.registerHandler(mtPing, handler1)
    srv.registerHandler(mtEcho, handler2)
    check srv.handlers.len == 2
    check srv.handlers.hasKey(int(mtPing))
    check srv.handlers.hasKey(int(mtEcho))

  test "registerHandler overwrite":
    let cfg = defaultServerConfig()
    let srv = newProtocolServer(cfg)
    proc handler1(conn: ClientConnection, requestId: uint32,
        flags: uint16, payload: string) {.gcsafe, raises: [].} = discard
    proc handler2(conn: ClientConnection, requestId: uint32,
        flags: uint16, payload: string) {.gcsafe, raises: [].} = discard
    srv.registerHandler(mtPing, handler1)
    srv.registerHandler(mtPing, handler2)
    check srv.handlers.len == 1

suite "KVEntry":

  test "KVEntry construction":
    let entry = KVEntry(value: "test", version: 5, timestamp: 12345)
    check entry.value == "test"
    check entry.version == 5
    check entry.timestamp == 12345

  test "KVEntry zero version":
    let entry = KVEntry(value: "", version: 0, timestamp: 0)
    check entry.value == ""
    check entry.version == 0
    check entry.timestamp == 0

suite "MessageHandler Type":

  test "MessageHandler signature":
    proc validHandler(conn: ClientConnection, requestId: uint32,
        flags: uint16, payload: string) {.gcsafe, raises: [].} =
      discard
    let cfg = defaultServerConfig()
    let srv = newProtocolServer(cfg)
    srv.registerHandler(mtPing, validHandler)
    check srv.handlers.len == 1
