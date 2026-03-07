# Integration tests for the Phase 1 core protocol.
#
# Starts a real ProtocolServer on a loopback port, connects a ProtocolClient,
# and exercises: handshake, Ping, Echo (small/large/binary), Close, and
# CancelStream.  The server runs in a detached acceptor thread; each client
# gets its own reader thread (internal to the server).
#
# Port allocation: each test uses its own unique port in the 19700-19799 range
# to avoid TIME_WAIT conflicts between consecutive tests.
#
# SO_REUSEADDR is set on all server sockets, but TIME_WAIT on Linux can still
# block rebinding when the port was used very recently.  Using per-test unique
# ports is the robust solution.

import std/[unittest, os, times, atomics, strutils, net]
import fractio/protocol/types
import fractio/protocol/server
import fractio/protocol/client
import fractio/protocol/frame
import fractio/protocol/messages/core

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc startTestServer(port: int): ProtocolServer =
  var cfg = defaultServerConfig()
  cfg.host = "127.0.0.1"
  cfg.port = port
  cfg.idleTimeoutSecs = 120 # long idle timeout so tests don't race
  result = newProtocolServer(cfg)
  result.start()
  sleep(60) # give acceptor thread time to bind + listen

proc connectTestClient(port: int): ProtocolClient =
  var cfg = defaultClientConfig("127.0.0.1", port)
  cfg.timeoutMs = 10_000 # 10s timeout for all socket ops
  result = newProtocolClient(cfg)
  let r = result.connect()
  doAssert r.isOk, "client.connect failed: " & $r.err

proc withServer(port: int, body: proc(srv: ProtocolServer,
    cli: ProtocolClient)) =
  let srv = startTestServer(port)
  let cli = connectTestClient(port)
  try:
    body(srv, cli)
  finally:
    cli.disconnect()
    srv.stop()
    sleep(50)

# ---------------------------------------------------------------------------
# Suite: Server lifecycle
# ---------------------------------------------------------------------------

suite "integration - server lifecycle":
  test "server starts and stops without error":
    let srv = startTestServer(19700)
    check srv.clientCount() == 0
    srv.stop()
    sleep(50)

  test "server accepts a connection":
    let srv = startTestServer(19701)
    let cli = connectTestClient(19701)
    sleep(30)
    check cli.connected.load()
    check srv.clientCount() == 1
    cli.disconnect()
    sleep(50)
    srv.stop()
    sleep(50)

  test "server accepts multiple sequential connections":
    let srv = startTestServer(19702)
    for i in 0 ..< 3:
      let cli = connectTestClient(19702)
      check cli.connected.load()
      cli.disconnect()
      sleep(30)
    srv.stop()
    sleep(50)

# ---------------------------------------------------------------------------
# Suite: Ping
# ---------------------------------------------------------------------------

suite "integration - ping":
  test "ping returns server timestamp in microseconds":
    withServer(19710) do (srv: ProtocolServer, cli: ProtocolClient):
      let r = cli.ping()
      check r.isOk
      # After 2020-01-01 in microseconds = 1_577_836_800_000_000
      check r.value > 1_577_836_800_000_000'u64

  test "multiple pings succeed and return non-decreasing timestamps":
    withServer(19711) do (srv: ProtocolServer, cli: ProtocolClient):
      let r1 = cli.ping()
      let r2 = cli.ping()
      let r3 = cli.ping()
      check r1.isOk
      check r2.isOk
      check r3.isOk
      check r2.value >= r1.value
      check r3.value >= r2.value

# ---------------------------------------------------------------------------
# Suite: Echo
# ---------------------------------------------------------------------------

suite "integration - echo":
  test "echo empty string":
    withServer(19720) do (srv: ProtocolServer, cli: ProtocolClient):
      let r = cli.echo("")
      check r.isOk
      check r.value == ""

  test "echo short ASCII string":
    withServer(19721) do (srv: ProtocolServer, cli: ProtocolClient):
      let r = cli.echo("hello, fractio!")
      check r.isOk
      check r.value == "hello, fractio!"

  test "echo unicode string":
    withServer(19722) do (srv: ProtocolServer, cli: ProtocolClient):
      let data = "こんにちは世界 🌍"
      let r = cli.echo(data)
      check r.isOk
      check r.value == data

  test "echo binary data with null bytes":
    withServer(19723) do (srv: ProtocolServer, cli: ProtocolClient):
      var data = newString(256)
      for i in 0 ..< 256:
        data[i] = char(i)
      let r = cli.echo(data)
      check r.isOk
      check r.value == data

  test "echo 64 KB payload":
    withServer(19724) do (srv: ProtocolServer, cli: ProtocolClient):
      let data = repeat('Z', 65536)
      let r = cli.echo(data)
      check r.isOk
      check r.value == data
      check r.value.len == 65536

  test "echo sequential requests preserve order":
    withServer(19725) do (srv: ProtocolServer, cli: ProtocolClient):
      for i in 1 .. 10:
        let msg = "message-" & $i
        let r = cli.echo(msg)
        check r.isOk
        check r.value == msg

# ---------------------------------------------------------------------------
# Suite: Close
# ---------------------------------------------------------------------------

suite "integration - close":
  test "closeConn sends close request and disconnects gracefully":
    let srv = startTestServer(19730)
    let cli = connectTestClient(19730)
    check cli.connected.load()
    cli.closeConn("test done")
    check not cli.connected.load()
    sleep(50)
    srv.stop()
    sleep(50)

  test "closeConn with empty reason":
    let srv = startTestServer(19731)
    let cli = connectTestClient(19731)
    cli.closeConn()
    check not cli.connected.load()
    sleep(50)
    srv.stop()
    sleep(50)

  test "send after disconnect returns error":
    let srv = startTestServer(19732)
    let cli = connectTestClient(19732)
    cli.disconnect()
    let r = cli.ping()
    check r.isErr
    check r.err.kind == peInternal
    srv.stop()
    sleep(50)

# ---------------------------------------------------------------------------
# Suite: Custom handler registration
# ---------------------------------------------------------------------------

suite "integration - custom handler":
  test "registered handler is called for matching message type":
    let srv = startTestServer(19740)
    var handlerCalled = false

    let handler: MessageHandler = proc(conn: ClientConnection,
        requestId: uint32, flags: uint16,
        payload: string) {.gcsafe, raises: [].} =
      handlerCalled = true
      let tsUs = uint64(getTime().toUnixFloat() * 1_000_000)
      try:
        net.send(conn.socket, encodeFrame(encodePingResponse(tsUs), requestId,
            FlagIsResponse))
      except CatchableError: discard

    srv.registerHandler(mtPing, handler)
    let cli = connectTestClient(19740)
    let r = cli.ping()
    check r.isOk
    check handlerCalled
    cli.disconnect()
    sleep(50)
    srv.stop()
    sleep(50)

# ---------------------------------------------------------------------------
# Suite: Protocol error scenarios
# ---------------------------------------------------------------------------

suite "integration - error paths":
  test "connect to wrong port returns error":
    var cfg = defaultClientConfig("127.0.0.1", 19798)
    cfg.timeoutMs = 1000
    let cli = newProtocolClient(cfg)
    let r = cli.connect()
    check r.isErr
    check r.err.kind == peInternal

  test "operations fail after explicit disconnect":
    let srv = startTestServer(19799)
    let cli = connectTestClient(19799)
    check cli.connected.load()
    # Explicitly disconnect: clears connected flag and closes socket
    cli.disconnect()
    check not cli.connected.load()
    # Any subsequent operation must return peInternal (not connected)
    let r1 = cli.ping()
    check r1.isErr
    check r1.err.kind == peInternal
    let r2 = cli.echo("data")
    check r2.isErr
    check r2.err.kind == peInternal
    srv.stop()
    sleep(50)
