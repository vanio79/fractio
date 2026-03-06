# TCP Transport - TCP-based network transport for distributed Fractio
# Provides both server and client functionality for TCP communication

import std/[net, nativesockets, tables, locks, times, options]
import ./types
import ./serialization
import ./config
import ../../core/types as coretypes
import ../../utils/logging

when defined(posix):
  import std/posix

# =============================================================================
# Connection Types
# =============================================================================

type
  Connection* = ref object
    ## A TCP connection to a remote node
    nodeId*: NodeID
    socket*: Socket
    lastUsed*: int64
    sendLock*: Lock
    recvLock*: Lock
    state*: ConnectionState
    remoteAddr*: string

  TCPTransport* = ref object
    ## TCP transport for a specific protocol (Raft, Client, Admin)
    config*: NetworkConfig
    port*: int
    role*: string
    serverSocket*: Socket
    running*: bool
    runningLock*: Lock
    connections*: tables.Table[string, Connection] # Key is string(NodeID)
    connectionsLock*: Lock
    nextMessageId*: uint64
    messageIdLock*: Lock
    handlers*: tables.Table[uint16, proc(msg: string): string {.gcsafe.}]
    handlersLock*: Lock

# =============================================================================
# Connection Management
# =============================================================================

proc newConnection*(nodeId: NodeID, socket: Socket,
    remoteAddr: string): Connection =
  result = Connection(
    nodeId: nodeId,
    socket: socket,
    lastUsed: int64(getTime().toUnix() * 1000),
    state: csConnected,
    remoteAddr: remoteAddr
  )
  initLock(result.sendLock)
  initLock(result.recvLock)

proc close*(conn: Connection) =
  if conn.state != csClosed:
    conn.state = csClosed
    try:
      conn.socket.close()
    except:
      discard
    deinitLock(conn.sendLock)
    deinitLock(conn.recvLock)

# =============================================================================
# TCP Transport
# =============================================================================

proc newTCPTransport*(config: NetworkConfig, port: int,
    role: string): TCPTransport =
  result = TCPTransport(
    config: config,
    port: port,
    role: role,
    running: false,
    connections: tables.initTable[string, Connection](),
    nextMessageId: 1,
    handlers: tables.initTable[uint16, proc(msg: string): string {.gcsafe.}]()
  )
  initLock(result.runningLock)
  initLock(result.connectionsLock)
  initLock(result.messageIdLock)
  initLock(result.handlersLock)

proc nextMessageId*(t: TCPTransport): uint64 =
  withLock t.messageIdLock:
    result = t.nextMessageId
    t.nextMessageId += 1

proc isRunning*(t: TCPTransport): bool =
  withLock t.runningLock:
    result = t.running

proc registerHandler*(t: TCPTransport, msgType: uint16,
                      handler: proc(msg: string): string {.gcsafe.}) =
  withLock t.handlersLock:
    t.handlers[msgType] = handler

proc getHandler*(t: TCPTransport, msgType: uint16): Option[proc(
    msg: string): string {.gcsafe.}] =
  withLock t.handlersLock:
    if msgType in t.handlers:
      return some(t.handlers[msgType])
    return none(proc(msg: string): string {.gcsafe.})

# =============================================================================
# Low-level Socket Operations
# =============================================================================

proc readFrame*(socket: Socket, timeoutMs: int = 30000): Option[string] =
  try:
    var headerBuf = newString(FRAME_HEADER_SIZE)
    let n = socket.recv(headerBuf, FRAME_HEADER_SIZE, timeoutMs)
    if n == 0:
      return none(string)
    if n < FRAME_HEADER_SIZE:
      return none(string)

    let (header, _) = decodeFrameHeader(headerBuf)

    if header.payloadLen.int > MAX_MESSAGE_SIZE:
      var fields = tables.initTable[string, string]()
      fields["size"] = $header.payloadLen
      error("Message too large", fields)
      return none(string)

    var payload = newString(header.payloadLen.int)
    if header.payloadLen > 0:
      let n2 = socket.recv(payload, header.payloadLen.int)
      if n2 == 0 or n2 < header.payloadLen.int:
        return none(string)

    let computedChecksum = computeCRC32(payload)
    if computedChecksum != header.checksum:
      var fields = tables.initTable[string, string]()
      fields["expected"] = $header.checksum
      fields["got"] = $computedChecksum
      error("Checksum mismatch", fields)
      return none(string)

    return some(payload)

  except TimeoutError:
    return none(string)
  except CatchableError as e:
    var fields = tables.initTable[string, string]()
    fields["error"] = e.msg
    error("Error reading frame", fields)
    return none(string)

proc writeFrame*(socket: Socket, payload: string,
    timeoutMs: int = 30000): bool =
  try:
    let frame = encodeFrame(payload)
    socket.send(frame)
    return true
  except TimeoutError:
    return false
  except CatchableError as e:
    var fields = tables.initTable[string, string]()
    fields["error"] = e.msg
    error("Error writing frame", fields)
    return false

# =============================================================================
# Connection Operations
# =============================================================================

proc connectToNode*(t: TCPTransport, nodeId: NodeID, host: string,
    port: int): Option[Connection] =
  try:
    let socket = newSocket()
    socket.setSockOpt(OptReuseAddr, true)
    socket.setSockOpt(OptNoDelay, t.config.tcpNoDelay)
    socket.setSockOpt(OptKeepAlive, t.config.tcpKeepAlive)

    let remoteAddr = host & ":" & $port
    var fields = tables.initTable[string, string]()
    fields["nodeId"] = string(nodeId)
    fields["addr"] = remoteAddr
    info("Connecting to node", fields)

    socket.connect(host, Port(port), timeout = t.config.tcpConnectTimeoutMs)

    let conn = newConnection(nodeId, socket, remoteAddr)

    withLock t.connectionsLock:
      t.connections[string(nodeId)] = conn

    info("Connected to node", fields)
    return some(conn)

  except CatchableError as e:
    var fields = tables.initTable[string, string]()
    fields["nodeId"] = string(nodeId)
    fields["error"] = e.msg
    error("Failed to connect to node", fields)
    return none(Connection)

proc getConnection*(t: TCPTransport, nodeId: NodeID, host: string,
    port: int): Option[Connection] =
  withLock t.connectionsLock:
    let key = string(nodeId)
    if key in t.connections:
      let conn = t.connections[key]
      if conn.state == csConnected:
        return some(conn)

  return t.connectToNode(nodeId, host, port)

proc disconnectNode*(t: TCPTransport, nodeId: NodeID) =
  withLock t.connectionsLock:
    let key = string(nodeId)
    if key in t.connections:
      let conn = t.connections[key]
      conn.close()
      t.connections.del(key)
      var fields = tables.initTable[string, string]()
      fields["nodeId"] = string(nodeId)
      info("Disconnected from node", fields)

# =============================================================================
# Message Sending
# =============================================================================

proc sendRaw*(t: TCPTransport, conn: Connection, payload: string): bool =
  if conn.state != csConnected:
    return false

  withLock conn.sendLock:
    result = writeFrame(conn.socket, payload, t.config.tcpWriteTimeoutMs)
    if result:
      conn.lastUsed = int64(getTime().toUnix() * 1000)

proc sendMessage*(t: TCPTransport, nodeId: NodeID, host: string, port: int,
                  payload: string): bool =
  let connOpt = t.getConnection(nodeId, host, port)
  if connOpt.isNone:
    return false
  return t.sendRaw(connOpt.get(), payload)

# =============================================================================
# Message Receiving
# =============================================================================

proc handleIncomingMessage*(t: TCPTransport, payload: string): string =
  if payload.len < 2:
    return ""

  var r = newBinaryReader(payload)
  let msgType = r.readUint16BE()

  let handlerOpt = t.getHandler(msgType)
  if handlerOpt.isNone:
    var fields = tables.initTable[string, string]()
    fields["msgType"] = $msgType
    warn("No handler for message type", fields)
    return ""

  let handler = handlerOpt.get()
  return handler(payload)

# =============================================================================
# Server Operations
# =============================================================================

proc acceptLoop*(t: TCPTransport) =
  while t.isRunning():
    try:
      var client: Socket
      var clientAddr: string
      t.serverSocket.acceptAddr(client, clientAddr)

      var fields = tables.initTable[string, string]()
      fields["addr"] = clientAddr
      info("Accepted connection from", fields)

      client.setSockOpt(OptNoDelay, t.config.tcpNoDelay)
      client.setSockOpt(OptKeepAlive, t.config.tcpKeepAlive)

      let tempId = NodeID("unknown_" & clientAddr)
      let conn = newConnection(tempId, client, clientAddr)

      withLock t.connectionsLock:
        t.connections[string(tempId)] = conn

    except CatchableError as e:
      if t.isRunning():
        var fields = tables.initTable[string, string]()
        fields["error"] = e.msg
        error("Error accepting connection", fields)

proc startServer*(t: TCPTransport): bool =
  try:
    t.serverSocket = newSocket()
    t.serverSocket.setSockOpt(OptReuseAddr, true)
    t.serverSocket.bindAddr(Port(t.port), t.config.bindAddress)
    t.serverSocket.listen()

    withLock t.runningLock:
      t.running = true

    var fields = tables.initTable[string, string]()
    fields["role"] = t.role
    fields["port"] = $t.port
    info("TCP server started", fields)
    return true

  except CatchableError as e:
    var fields = tables.initTable[string, string]()
    fields["error"] = e.msg
    error("Failed to start TCP server", fields)
    return false

proc stopServer*(t: TCPTransport) =
  withLock t.runningLock:
    t.running = false

  withLock t.connectionsLock:
    for nodeId, conn in t.connections:
      conn.close()
    t.connections.clear()

  if t.serverSocket != nil:
    try:
      t.serverSocket.close()
    except:
      discard

  var fields = tables.initTable[string, string]()
  fields["role"] = t.role
  info("TCP server stopped", fields)

# =============================================================================
# Cleanup
# =============================================================================

proc close*(t: TCPTransport) =
  t.stopServer()

  deinitLock(t.runningLock)
  deinitLock(t.connectionsLock)
  deinitLock(t.messageIdLock)
  deinitLock(t.handlersLock)
