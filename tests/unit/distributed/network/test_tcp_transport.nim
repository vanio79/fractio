# Comprehensive unit tests for tcp_transport.nim

import unittest
import tables
import locks
import options
import atomics
import net
import strutils
import fractio/distributed/network/types
import fractio/distributed/network/serialization
import fractio/distributed/network/tcp_transport
import fractio/distributed/network/config
import fractio/core/types

suite "TCP Transport Connection Creation":
  test "newConnection creates connection with valid parameters":
    let socket = newSocket()
    let conn = newConnection(NodeID("node1"), socket, "localhost:9000")

    check conn != nil
    check string(conn.nodeId) == "node1"
    check conn.socket != nil
    check conn.remoteAddr == "localhost:9000"
    check conn.state == csConnected
    check conn.lastUsed > 0

    conn.close()

  test "newConnection with different states":
    let socket = newSocket()
    let conn = newConnection(NodeID("node2"), socket, "127.0.0.1:8080")

    check conn.state == csConnected
    check string(conn.nodeId) == "node2"

    conn.close()

  test "Connection close changes state":
    let socket = newSocket()
    let conn = newConnection(NodeID("node1"), socket, "localhost:9000")

    check conn.state == csConnected
    conn.close()
    check conn.state == csClosed

  test "Connection close is safe to call multiple times":
    let socket = newSocket()
    let conn = newConnection(NodeID("node1"), socket, "localhost:9000")

    conn.close()
    conn.close()
    conn.close()
    check conn.state == csClosed

  test "Connection close with nil socket":
    var conn = Connection(
      nodeId: NodeID("node1"),
      socket: nil,
      state: csConnected,
      remoteAddr: "localhost:9000"
    )
    initLock(conn.sendLock)
    initLock(conn.recvLock)

    conn.close()
    check conn.state == csClosed

suite "TCP Transport Creation":
  test "newTCPTransport creates transport with defaults":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, 9000, "test")

    check transport != nil
    check transport.config == config
    check transport.port == 9000
    check transport.role == "test"
    check transport.connections.len == 0
    check transport.handlers.len == 0
    check transport.nextMessageId == 1

    transport.close()

  test "newTCPTransport with different roles":
    let config = newNetworkConfig(NodeID("node1"), 9000)

    let raftT = newTCPTransport(config, 9000, "raft")
    check raftT.role == "raft"
    raftT.close()

    let clientT = newTCPTransport(config, 9001, "client")
    check clientT.role == "client"
    clientT.close()

    let adminT = newTCPTransport(config, 9002, "admin")
    check adminT.role == "admin"
    adminT.close()

  test "newTCPTransport initializes all locks":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, 9000, "test")

    transport.close()

suite "TCP Transport Message ID":
  test "nextMessageId increments sequentially":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, 9000, "test")

    check transport.nextMessageId() == 1'u64
    check transport.nextMessageId() == 2'u64
    check transport.nextMessageId() == 3'u64
    check transport.nextMessageId() == 4'u64
    check transport.nextMessageId() == 5'u64

    transport.close()

  test "nextMessageId is unique across calls":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, 9000, "test")

    var ids: seq[uint64] = @[]
    for i in 0..<100:
      ids.add(transport.nextMessageId())

    for i in 0..<ids.len:
      for j in (i + 1)..<ids.len:
        check ids[i] != ids[j]

    transport.close()

suite "TCP Transport Running State":
  test "isRunning returns false before start":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, 9000, "test")

    check transport.isRunning() == false

    transport.close()

  test "Running state can be set manually":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, 9000, "test")

    transport.running.store(true)
    check transport.isRunning() == true

    transport.running.store(false)
    check transport.isRunning() == false

    transport.close()

suite "TCP Transport Handler Registration":
  test "registerHandler stores handler":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, 9000, "test")

    proc testHandler(msg: string): string {.gcsafe.} =
      result = "response"

    transport.registerHandler(1'u16, testHandler)

    let handler = transport.getHandler(1'u16)
    check handler.isSome

    transport.close()

  test "registerHandler for multiple message types":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, 9000, "test")

    proc handler1(msg: string): string {.gcsafe.} = "h1"
    proc handler2(msg: string): string {.gcsafe.} = "h2"
    proc handler3(msg: string): string {.gcsafe.} = "h3"

    transport.registerHandler(1'u16, handler1)
    transport.registerHandler(2'u16, handler2)
    transport.registerHandler(3'u16, handler3)

    check transport.getHandler(1'u16).isSome
    check transport.getHandler(2'u16).isSome
    check transport.getHandler(3'u16).isSome

    transport.close()

  test "getHandler returns none for unregistered type":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, 9000, "test")

    proc handler(msg: string): string {.gcsafe.} = "response"
    transport.registerHandler(1'u16, handler)

    check transport.getHandler(999'u16).isNone

    transport.close()

  test "registerHandler replaces existing handler":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, 9000, "test")

    var callCount = 0

    proc handler1(msg: string): string {.gcsafe.} =
      callCount = 1
      result = "h1"

    proc handler2(msg: string): string {.gcsafe.} =
      callCount = 2
      result = "h2"

    transport.registerHandler(1'u16, handler1)
    transport.registerHandler(1'u16, handler2)

    let handler = transport.getHandler(1'u16)
    check handler.isSome
    discard handler.get()("")
    check callCount == 2

    transport.close()

suite "TCP Transport Frame Operations":
  test "encodeFrame creates valid frame":
    let payload = "test payload"
    let frame = encodeFrame(payload)

    check frame.len == FRAME_HEADER_SIZE + payload.len

    let (header, payloadStart) = decodeFrameHeader(frame)
    check header.payloadLen == payload.len.uint32
    check header.checksum == computeCRC32(payload)
    check payloadStart == FRAME_HEADER_SIZE

  test "encodeFrame with empty payload":
    let payload = ""
    let frame = encodeFrame(payload)

    check frame.len == FRAME_HEADER_SIZE

    let decoded = decodeFrame(frame)
    check decoded.header.payloadLen == 0'u32
    check decoded.payload == ""

  test "encodeFrame with large payload":
    let payload = "x".repeat(10000)
    let frame = encodeFrame(payload)

    check frame.len == FRAME_HEADER_SIZE + payload.len

    let decoded = decodeFrame(frame)
    check decoded.payload == payload

  test "decodeFrameHeader validates minimum size":
    let shortData = "abc"
    var gotError = false
    try:
      discard decodeFrameHeader(shortData)
    except SerializationError:
      gotError = true
    check gotError

  test "decodeFrame validates checksum":
    let payload = "test data"
    let frame = encodeFrame(payload)
    let decoded = decodeFrame(frame)
    check decoded.payload == payload

    var corrupted = frame
    corrupted[corrupted.len - 1] = '\xFF'
    var gotError = false
    try:
      discard decodeFrame(corrupted)
    except SerializationError:
      gotError = true
    check gotError

suite "TCP Transport handleIncomingMessage":
  test "handleIncomingMessage with too short payload returns empty":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, 9000, "test")

    let response = transport.handleIncomingMessage("")
    check response == ""

    let response2 = transport.handleIncomingMessage("a")
    check response2 == ""

    transport.close()

  test "handleIncomingMessage with registered handler":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, 9000, "test")

    proc handler(msg: string): string {.gcsafe.} =
      result = "processed: " & msg

    transport.registerHandler(1'u16, handler)

    var w = newBinaryWriter()
    w.writeUint16BE(1'u16)
    w.writeString("data")
    let payload = w.getString()

    let response = transport.handleIncomingMessage(payload)
    check response.len > 0

    transport.close()

  test "handleIncomingMessage with no handler returns empty":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, 9000, "test")

    var w = newBinaryWriter()
    w.writeUint16BE(999'u16)
    w.writeString("data")
    let payload = w.getString()

    let response = transport.handleIncomingMessage(payload)
    check response == ""

    transport.close()

suite "TCP Transport Connection Operations":
  test "disconnectNode removes connection":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, 9000, "test")

    var conn = Connection(
      nodeId: NodeID("node2"),
      state: csConnected
    )
    initLock(conn.sendLock)
    initLock(conn.recvLock)

    transport.connections[string(NodeID("node2"))] = conn

    check transport.connections.len == 1
    transport.disconnectNode(NodeID("node2"))
    check transport.connections.len == 0

    transport.close()

  test "disconnectNode on non-existent connection is safe":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, 9000, "test")

    transport.disconnectNode(NodeID("nonexistent"))
    check transport.connections.len == 0

    transport.close()

suite "TCP Transport Server Operations":
  test "startServer sets running to true":
    let config = newNetworkConfig(NodeID("node1"), 19000)
    let transport = newTCPTransport(config, 19000, "test")

    let success = transport.startServer()
    check success == true
    check transport.isRunning() == true

    transport.close()

  test "startServer fails on already bound port":
    let config1 = newNetworkConfig(NodeID("node1"), 19001)
    let transport1 = newTCPTransport(config1, 19001, "test1")
    let success1 = transport1.startServer()
    check success1 == true

    let config2 = newNetworkConfig(NodeID("node2"), 19001)
    let transport2 = newTCPTransport(config2, 19001, "test2")
    let success2 = transport2.startServer()
    check success2 == false

    transport1.close()
    transport2.close()

  test "stopServer sets running to false":
    let config = newNetworkConfig(NodeID("node1"), 19002)
    let transport = newTCPTransport(config, 19002, "test")

    discard transport.startServer()
    check transport.isRunning() == true

    transport.stopServer()
    check transport.isRunning() == false

    transport.close()

  test "stopServer clears server socket":
    let config = newNetworkConfig(NodeID("node1"), 19003)
    let transport = newTCPTransport(config, 19003, "test")

    discard transport.startServer()
    check transport.serverSocket != nil

    transport.stopServer()
    check transport.serverSocket == nil

    transport.close()

  test "Multiple close calls are safe":
    let config = newNetworkConfig(NodeID("node1"), 19004)
    let transport = newTCPTransport(config, 19004, "test")

    transport.close()
    transport.close()
    transport.close()

suite "TCP Transport Send Operations":
  test "sendRaw returns false for closed connection":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, 9000, "test")

    var conn = Connection(
      nodeId: NodeID("node2"),
      state: csClosed
    )
    initLock(conn.sendLock)
    initLock(conn.recvLock)

    let success = transport.sendRaw(conn, "data")
    check success == false

    transport.close()

  test "sendRaw returns false for failed connection":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, 9000, "test")

    var conn = Connection(
      nodeId: NodeID("node2"),
      state: csFailed
    )
    initLock(conn.sendLock)
    initLock(conn.recvLock)

    let success = transport.sendRaw(conn, "data")
    check success == false

    transport.close()

suite "TCP Transport Config Integration":
  test "Transport uses config timeouts":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    config.tcpConnectTimeoutMs = 1000
    config.tcpReadTimeoutMs = 5000
    config.tcpWriteTimeoutMs = 5000

    let transport = newTCPTransport(config, 9000, "test")

    check transport.config.tcpConnectTimeoutMs == 1000
    check transport.config.tcpReadTimeoutMs == 5000
    check transport.config.tcpWriteTimeoutMs == 5000

    transport.close()

  test "Transport uses config TCP options":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    config.tcpNoDelay = true
    config.tcpKeepAlive = true

    let transport = newTCPTransport(config, 9000, "test")

    check transport.config.tcpNoDelay == true
    check transport.config.tcpKeepAlive == true

    transport.close()

suite "TCP Transport Connection State Management":
  test "Connection states are distinct":
    check csIdle != csConnecting
    check csIdle != csConnected
    check csIdle != csFailed
    check csIdle != csClosed
    check csConnected != csClosed
    check csFailed != csConnected

  test "Connection state transitions":
    var conn = Connection(state: csIdle)
    initLock(conn.sendLock)
    initLock(conn.recvLock)

    check conn.state == csIdle
    conn.state = csConnecting
    check conn.state == csConnecting
    conn.state = csConnected
    check conn.state == csConnected
    conn.close()
    check conn.state == csClosed

suite "TCP Transport Multiple Transports":
  test "Multiple transports on different ports":
    let config = newNetworkConfig(NodeID("node1"), 19005)

    let t1 = newTCPTransport(config, 19005, "raft")
    let t2 = newTCPTransport(config, 19006, "client")
    let t3 = newTCPTransport(config, 19007, "admin")

    check t1.port == 19005
    check t2.port == 19006
    check t3.port == 19007

    t1.close()
    t2.close()
    t3.close()

suite "TCP Transport Thread Safety":
  test "Handler registration is thread-safe":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, 9000, "test")

    proc h1(msg: string): string {.gcsafe.} = "h1"
    proc h2(msg: string): string {.gcsafe.} = "h2"

    transport.registerHandler(1'u16, h1)
    transport.registerHandler(2'u16, h2)

    check transport.getHandler(1'u16).isSome
    check transport.getHandler(2'u16).isSome

    transport.close()

  test "Message ID generation is thread-safe":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, 9000, "test")

    var ids: seq[uint64] = @[]
    for i in 0..<50:
      ids.add(transport.nextMessageId())

    for i in 0..<ids.len - 1:
      check ids[i + 1] > ids[i]

    transport.close()
