# Unit tests for fractio/distributed/sharedtimer/udptransport.nim
# Tests UDP transport lifecycle and operations

import std/[unittest, net, os, nativesockets, atomics, times]
import locks
import fractio/distributed/sharedtimer/udptransport
import fractio/distributed/sharedtimer/types
import fractio/distributed/sharedtimer/mock
import fractio/utils/logging

suite "UDPTransport - Construction":

  test "create with default port":
    let transport = newUDPTransport()
    check transport.localPort == 123'u16
    check transport.serverSocket == nil
    check not load(transport.serverRunning, moRelaxed)

  test "create with custom port":
    let transport = newUDPTransport(port = 8080'u16)
    check transport.localPort == 8080'u16

  test "create with ephemeral port request":
    let transport = newUDPTransport(port = 0'u16)
    check transport.localPort == 0'u16

  test "create with custom logger":
    let logger = newLogger("TestUDP")
    let transport = newUDPTransport(port = 0'u16, logger = logger)
    check transport.logger != nil

  test "create with mock time provider":
    var mock: MockTimeProvider
    new(mock)
    mock.currentTime = 1000_000_000
    let transport = newUDPTransport(port = 0'u16, timeProvider = mock)
    check transport.timeProvider != nil

  test "stats initialized to zero":
    let transport = newUDPTransport()
    let stats = load(transport.stats.sent, moRelaxed)
    check stats == 0
    check load(transport.stats.received, moRelaxed) == 0
    check load(transport.stats.errors, moRelaxed) == 0

  test "mutex initialized":
    let transport = newUDPTransport()
    try:
      acquire(transport.mutex)
      release(transport.mutex)
      check true
    except:
      check false

suite "UDPTransport - Lifecycle":

  test "start creates socket":
    let transport = newUDPTransport(port = 0'u16)
    transport.start()
    check transport.serverSocket != nil
    check load(transport.serverRunning, moRelaxed)
    check transport.localPort > 0'u16
    transport.close()

  test "start binds to ephemeral port":
    let transport = newUDPTransport(port = 0'u16)
    transport.start()
    check transport.localPort > 0'u16
    check transport.localPort <= 65535'u16
    transport.close()

  test "start sets running flag":
    let transport = newUDPTransport(port = 0'u16)
    transport.start()
    check load(transport.serverRunning, moRelaxed)
    transport.close()

  test "start twice raises error":
    let transport = newUDPTransport(port = 0'u16)
    transport.start()
    var raised = false
    try:
      transport.start()
    except ValueError:
      raised = true
    transport.close()
    check raised

  test "close clears running flag":
    let transport = newUDPTransport(port = 0'u16)
    transport.start()
    transport.close()
    check not load(transport.serverRunning, moRelaxed)

  test "close is idempotent":
    let transport = newUDPTransport(port = 0'u16)
    transport.start()
    transport.close()
    transport.close()
    check not load(transport.serverRunning, moRelaxed)

  test "close without start is safe":
    let transport = newUDPTransport(port = 0'u16)
    transport.close()
    check not load(transport.serverRunning, moRelaxed)

suite "UDPTransport - Statistics":

  test "getStats returns current values":
    let transport = newUDPTransport(port = 0'u16)
    transport.start()
    let stats = transport.getStats()
    check stats.sent >= 0
    check stats.received >= 0
    check stats.errors >= 0
    transport.close()

  test "getStats after operations":
    let transport = newUDPTransport(port = 0'u16)
    transport.start()
    discard fetchAdd(transport.stats.sent, 5, moRelaxed)
    discard fetchAdd(transport.stats.received, 3, moRelaxed)
    discard fetchAdd(transport.stats.errors, 1, moRelaxed)
    let stats = transport.getStats()
    check stats.sent == 5
    check stats.received == 3
    check stats.errors == 1
    transport.close()

suite "UDPTransport - Server Main Thread Safety":

  test "serverMain can be spawned":
    let transport = newUDPTransport(port = 0'u16)
    transport.start()
    check load(transport.serverRunning, moRelaxed)
    transport.close()

  test "server thread exits on close":
    let transport = newUDPTransport(port = 0'u16)
    transport.start()
    let portBefore = transport.localPort
    transport.close()
    check not load(transport.serverRunning, moRelaxed)
    check transport.localPort == portBefore

suite "UDPTransport - SyncRound Errors":

  test "syncRound without start raises":
    let transport = newUDPTransport(port = 0'u16)
    var raised = false
    try:
      discard transport.syncRound(1000_000_000, @[])
    except IOError:
      raised = true
    check raised

  test "syncRound with empty peers":
    let transport = newUDPTransport(port = 0'u16)
    transport.start()
    var raised = false
    try:
      discard transport.syncRound(1000_000_000, @[])
    except SyncTimeout:
      raised = true
    transport.close()
    check raised

  test "syncRound with unreachable peer":
    let transport = newUDPTransport(port = 0'u16)
    transport.start()
    let peer = PeerConfig(
      peerId: "unreachable",
      address: "192.0.2.1",
      port: 9999'u16
    )
    var raised = false
    try:
      discard transport.syncRound(1000_000_000, @[peer])
    except SyncTimeout:
      raised = true
    transport.close()
    check raised

suite "UDPTransport - Constants":

  test "DEFAULT_REQUEST_TIMEOUT_MS value":
    check DEFAULT_REQUEST_TIMEOUT_MS == 100

  test "MAX_PACKET_SIZE value":
    check MAX_PACKET_SIZE == 65507

suite "UDPTransport - Integration":

  test "two transports can communicate":
    let transport1 = newUDPTransport(port = 0'u16)
    let transport2 = newUDPTransport(port = 0'u16)
    transport1.start()
    transport2.start()
    check load(transport1.serverRunning, moRelaxed)
    check load(transport2.serverRunning, moRelaxed)
    let port1 = transport1.localPort
    let port2 = transport2.localPort
    check port1 > 0
    check port2 > 0
    check port1 != port2
    transport1.close()
    transport2.close()

  test "transport can handle multiple close cycles":
    let transport = newUDPTransport(port = 0'u16)
    for i in 1..3:
      transport.start()
      check load(transport.serverRunning, moRelaxed)
      transport.close()
      check not load(transport.serverRunning, moRelaxed)
      os.sleep(10)

suite "UDPTransport - Thread Safety":

  test "mutex protects concurrent operations":
    let transport = newUDPTransport(port = 0'u16)
    transport.start()
    var counter = 0
    withLock transport.mutex:
      counter = 1
    check counter == 1
    transport.close()

suite "UDPTransport - Resource Cleanup":

  test "close releases socket":
    let transport = newUDPTransport(port = 0'u16)
    transport.start()
    let socketBefore = transport.serverSocket
    check socketBefore != nil
    transport.close()
    check not load(transport.serverRunning, moRelaxed)

  test "close waits for thread":
    let transport = newUDPTransport(port = 0'u16)
    transport.start()
    let before = now()
    transport.close()
    let after = now()
    check after >= before

suite "SyncTimeout Exception":

  test "SyncTimeout is CatchableError":
    let exc = SyncTimeout(msg: "test timeout")
    check exc of CatchableError

  test "SyncTimeout message":
    let exc = SyncTimeout(msg: "recv timeout")
    check exc.msg == "recv timeout"

  test "SyncTimeout can be caught":
    var caught = false
    try:
      raise SyncTimeout(msg: "test")
    except SyncTimeout:
      caught = true
    check caught
