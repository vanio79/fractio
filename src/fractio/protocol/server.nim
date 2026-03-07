# Fractio protocol server — Phase 1: Core Protocol.
#
# Thread model:
#   - One acceptor thread (server.start spawns acceptLoop)
#   - One reader thread per client connection (clientLoop)
#   - Handlers are called on the reader thread; they must be gcsafe.
#
# All shared mutable state is protected by Locks.

import std/[net, tables, strformat, times, atomics, locks, options]
import posix as posixSys
import ./types
import ./frame
import ./handshake
import ./messages/core
import ../utils/logging

# ---------------------------------------------------------------------------
# Safe logging helper — swallows any logger exception so callers can be raises:[]
# ---------------------------------------------------------------------------

proc slog(logger: Logger, level: LogLevel, msg: string) {.gcsafe, raises: [].} =
  try: logger.log(level, msg)
  except CatchableError: discard
  except Exception: discard

template logInfo(logger: Logger, msg: string) = slog(logger, llInfo, msg)
template logWarn(logger: Logger, msg: string) = slog(logger, llWarn, msg)
template logError(logger: Logger, msg: string) = slog(logger, llError, msg)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

type
  ServerConfig* = object
    host*: string
    port*: int
    maxConnections*: int        ## default 1024
    maxFrameBytes*: uint32      ## default 16 MB
    maxKeyBytes*: uint32        ## default 4 KB
    maxValueBytes*: uint32      ## default 64 MB
    idleTimeoutSecs*: int       ## default 30
    keepaliveIntervalSecs*: int ## default 10
    tlsEnabled*: bool
    authMethod*: AuthMethod
    serverName*: string
    serverId*: uint16
    clusterId*: uint64

proc defaultServerConfig*(): ServerConfig =
  ServerConfig(
    host: "0.0.0.0",
    port: 9000,
    maxConnections: 1024,
    maxFrameBytes: uint32(MAX_FRAME_SIZE),
    maxKeyBytes: uint32(MAX_KEY_BYTES),
    maxValueBytes: uint32(MAX_VALUE_BYTES),
    idleTimeoutSecs: 30,
    keepaliveIntervalSecs: 10,
    tlsEnabled: false,
    authMethod: amNone,
    serverName: "fractio",
    serverId: 1,
    clusterId: 0,
  )

# ---------------------------------------------------------------------------
# Client connection
# ---------------------------------------------------------------------------

type
  ClientConnection* = ref object
    id*: uint32
    socket*: Socket
    address*: string
    negotiatedFeatures*: uint32
    createdAt*: int64      ## Unix ms
    lastActivityMs*: int64 ## protected by mu
    authenticated*: bool
    mu*: Lock

proc newClientConnection*(id: uint32, sock: Socket,
    address: string): ClientConnection =
  result = ClientConnection(
    id: id,
    socket: sock,
    address: address,
    createdAt: (getTime().toUnixFloat() * 1000).int64,
    lastActivityMs: (getTime().toUnixFloat() * 1000).int64,
  )
  initLock(result.mu)

proc touchActivity*(conn: ClientConnection) {.gcsafe, raises: [].} =
  withLock(conn.mu):
    conn.lastActivityMs = (getTime().toUnixFloat() * 1000).int64

proc isIdle*(conn: ClientConnection, timeoutSecs: int): bool {.gcsafe, raises: [].} =
  withLock(conn.mu):
    let nowMs = (getTime().toUnixFloat() * 1000).int64
    result = (nowMs - conn.lastActivityMs) > int64(timeoutSecs) * 1000

# ---------------------------------------------------------------------------
# Message handler type
# ---------------------------------------------------------------------------

type
  MessageHandler* = proc(conn: ClientConnection, requestId: uint32,
      flags: uint16, payload: string) {.gcsafe, raises: [].}

# ---------------------------------------------------------------------------
# Protocol server
# ---------------------------------------------------------------------------

type
  ProtocolServer* = ref object
    config*: ServerConfig
    logger*: Logger
    running*: Atomic[bool]
    clients*: Table[uint32, ClientConnection]
    clientsMu*: Lock
    handlers*: Table[int, MessageHandler]
    handlersMu*: Lock
    nextClientId*: Atomic[uint32]
    serverFeatures*: uint32

# ---------------------------------------------------------------------------
# Thread argument types — defined after ProtocolServer to avoid forward refs
# ---------------------------------------------------------------------------

type
  ClientLoopArgs* = tuple[srv: ProtocolServer, conn: ClientConnection]
  AcceptLoopArgs* = tuple[srv: ProtocolServer, sock: Socket]

# Module-level thread storage: keeps Thread objects alive for the process
# lifetime.  Protected by threadStoreMu.
var threadStore {.global.}: seq[ref Thread[ClientLoopArgs]] = @[]
var acceptThreadStore {.global.}: seq[ref Thread[AcceptLoopArgs]] = @[]
var threadStoreMu {.global.}: Lock
initLock(threadStoreMu)

# ---------------------------------------------------------------------------

proc newProtocolServer*(config: ServerConfig): ProtocolServer =
  result = ProtocolServer(
    config: config,
    logger: newLogger("protocol.server"),
    clients: initTable[uint32, ClientConnection](),
    handlers: initTable[int, MessageHandler](),
  )
  initLock(result.clientsMu)
  initLock(result.handlersMu)
  result.running.store(false)
  result.nextClientId.store(1)
  result.serverFeatures = FeatPipelining or FeatTransactions or FeatAsync
  if config.tlsEnabled:
    result.serverFeatures = result.serverFeatures or FeatTLS

proc registerHandler*(server: ProtocolServer, msgType: MessageType,
    handler: MessageHandler) =
  withLock(server.handlersMu):
    server.handlers[int(msgType)] = handler

proc addClient(server: ProtocolServer, conn: ClientConnection) {.gcsafe,
    raises: [].} =
  withLock(server.clientsMu):
    server.clients[conn.id] = conn

proc removeClient(server: ProtocolServer, id: uint32) {.gcsafe, raises: [].} =
  withLock(server.clientsMu):
    server.clients.del(id)

proc clientCount*(server: ProtocolServer): int {.gcsafe, raises: [].} =
  withLock(server.clientsMu):
    result = server.clients.len

# ---------------------------------------------------------------------------
# Low-level recv helper (posix.recv — truly blocking in multi-threaded context)
# ---------------------------------------------------------------------------

proc srvRecvExact(sock: Socket, buf: var string,
    size: int): int {.gcsafe, raises: [].} =
  ## Read exactly `size` bytes using posix.recv.  Returns bytes read; < size means EOF/error.
  buf.setLen(size)
  var total = 0
  while total < size:
    let got = posixSys.recv(sock.getFd(), addr buf[total], size - total, 0)
    if got <= 0:
      buf.setLen(total)
      return total
    total += got
  total

# ---------------------------------------------------------------------------
# Send helpers
# ---------------------------------------------------------------------------

proc sendRaw(conn: ClientConnection, data: string) {.gcsafe, raises: [].} =
  try: conn.socket.send(data)
  except CatchableError: discard

proc sendFrame(conn: ClientConnection, payload: string,
    requestId: uint32, flags: uint16 = FlagIsResponse) {.gcsafe, raises: [].} =
  sendRaw(conn, encodeFrame(payload, requestId, flags))

proc sendError(conn: ClientConnection, requestId: uint32,
    errCode: uint32, category: uint8, msg: string) {.gcsafe, raises: [].} =
  sendRaw(conn, encodeErrorFrame(requestId, errCode, category, msg))

# ---------------------------------------------------------------------------
# Handshake
# ---------------------------------------------------------------------------

proc performHandshake(server: ProtocolServer,
    conn: ClientConnection): bool {.gcsafe, raises: [].} =
  try:
    # 1. Send server greeting
    let greeting = ServerGreeting(
      magic: PROTOCOL_MAGIC,
      version: PROTOCOL_VERSION_1,
      features: server.serverFeatures,
      authMethods: @[uint8(server.config.authMethod)],
      serverId: server.config.serverId,
      clusterId: server.config.clusterId,
    )
    conn.socket.send(encodeGreeting(greeting))

    # 2. Read client handshake — parse streaming wire format:
    #   2 bytes version + 4 bytes features + 1 byte authType +
    #   4 bytes authDataLen + authData + 1 byte clientIdLen + clientId
    # Read fixed prefix (2+4+1+4 = 11 bytes) first.
    var hsPrefix = newString(11)
    let hsPrefixN = srvRecvExact(conn.socket, hsPrefix, 11)
    if hsPrefixN != 11: return false
    # Auth data length is a uint32 at bytes [7..10]
    let authDataLen = (int(uint8(hsPrefix[7])) shl 24) or
                      (int(uint8(hsPrefix[8])) shl 16) or
                      (int(uint8(hsPrefix[9])) shl 8) or
                      int(uint8(hsPrefix[10]))
    var hsAuthData = newString(authDataLen)
    if authDataLen > 0:
      let aN = srvRecvExact(conn.socket, hsAuthData, authDataLen)
      if aN != authDataLen: return false
    # Client ID length (1 byte uint8)
    var hsClientIdLen = newString(1)
    let clLenN = srvRecvExact(conn.socket, hsClientIdLen, 1)
    if clLenN != 1: return false
    let clientIdLen = int(uint8(hsClientIdLen[0]))
    var hsClientId = newString(clientIdLen)
    if clientIdLen > 0:
      let clN = srvRecvExact(conn.socket, hsClientId, clientIdLen)
      if clN != clientIdLen: return false
    # Reconstruct full buffer for decodeClientHandshake
    let buf = hsPrefix & hsAuthData & hsClientIdLen & hsClientId
    let hsR = decodeClientHandshake(buf)
    if hsR.isErr:
      server.logger.logWarn(&"[{conn.address}] bad handshake: {hsR.error}")
      conn.socket.send(encodeHandshakeResponse(HandshakeResponse(
        status: HandshakeError, errorMessage: $hsR.error)))
      return false

    let hs = hsR.value
    if hs.version != PROTOCOL_VERSION_1:
      conn.socket.send(encodeHandshakeResponse(HandshakeResponse(
        status: HandshakeError,
        errorMessage: &"unsupported protocol version {hs.version}")))
      return false

    if server.config.authMethod != amNone:
      if AuthMethod(hs.authType) != server.config.authMethod:
        conn.socket.send(encodeHandshakeResponse(HandshakeResponse(
          status: HandshakeError, errorMessage: "authentication required")))
        return false

    let negotiated = negotiateFeatures(server.serverFeatures, hs.features)
    conn.negotiatedFeatures = negotiated
    conn.authenticated = true

    # 3. Send handshake response
    conn.socket.send(encodeHandshakeResponse(HandshakeResponse(
      status: HandshakeOK,
      features: negotiated,
      serverName: server.config.serverName)))
    return true

  except CatchableError as e:
    server.logger.logWarn(&"[{conn.address}] handshake exception: {e.msg}")
    return false

# ---------------------------------------------------------------------------
# Frame reader
# ---------------------------------------------------------------------------

proc readOneFrame(sock: Socket,
    maxBytes: uint32): Result[Frame, ProtocolError] {.gcsafe, raises: [].} =
  var hdrBuf = newString(FRAME_HEADER_SIZE)
  let hn = srvRecvExact(sock, hdrBuf, FRAME_HEADER_SIZE)
  if hn != FRAME_HEADER_SIZE:
    return peErr(newProtocolError(peInvalidFrame,
      &"short header: got {hn}, need {FRAME_HEADER_SIZE}"))

  var pos = 0
  let hdrR = decodeFrameHeader(hdrBuf, pos)
  if hdrR.isErr: return peErr(hdrR.error)
  let hdr = hdrR.value

  if hdr.payloadLen > maxBytes:
    return peErr(newProtocolError(peFrameTooLarge,
      &"payload {hdr.payloadLen} > max {maxBytes}"))

  var payload = newString(int(hdr.payloadLen))
  if hdr.payloadLen > 0:
    let pn = srvRecvExact(sock, payload, int(hdr.payloadLen))
    if pn != int(hdr.payloadLen):
      return peErr(newProtocolError(peInvalidFrame,
        &"short payload: got {pn}, need {hdr.payloadLen}"))

  let computed = computeCRC16(payload)
  if computed != hdr.checksum:
    return peErr(newProtocolError(peChecksumMismatch,
      &"CRC16 mismatch: got {hdr.checksum:#06x}, computed {computed:#06x}"))

  peOk(Frame(header: hdr, payload: payload))

# ---------------------------------------------------------------------------
# Built-in core message handlers (Ping, Echo, Close, CancelStream)
# ---------------------------------------------------------------------------

proc handleBuiltinCore(server: ProtocolServer, conn: ClientConnection,
    requestId: uint32, flags: uint16,
    payload: string) {.gcsafe, raises: [].} =
  if payload.len < 2: return
  let typeVal = (uint16(payload[0]) shl 8) or uint16(payload[1])
  case typeVal
  of uint16(mtPing):
    let tsUs = uint64(getTime().toUnixFloat() * 1_000_000)
    sendFrame(conn, encodePingResponse(tsUs), requestId)
  of uint16(mtEcho):
    let dataR = decodeEchoData(payload)
    if dataR.isErr:
      sendError(conn, requestId, ErrProtocol, ErrCatProtocol, $dataR.error)
    else:
      sendFrame(conn, encodeEchoResponse(dataR.value), requestId)
  of uint16(mtClose):
    sendFrame(conn, encodeCloseRequest("bye"), requestId)
  of uint16(mtCancelStream):
    sendFrame(conn, encodeCancelStreamResponse(true), requestId)
  else:
    sendError(conn, requestId, ErrProtocol, ErrCatProtocol,
      &"unknown core message type 0x{typeVal:04X}")

# ---------------------------------------------------------------------------
# Per-connection loop
# ---------------------------------------------------------------------------

proc clientLoop(server: ProtocolServer,
    conn: ClientConnection) {.gcsafe, raises: [].} =
  defer:
    try: conn.socket.close() except CatchableError: discard
    removeClient(server, conn.id)
    server.logger.logInfo(&"[{conn.address}] disconnected (id={conn.id})")

  server.logger.logInfo(&"[{conn.address}] connected (id={conn.id})")

  if not performHandshake(server, conn):
    return

  server.logger.logInfo(&"[{conn.address}] handshake OK (id={conn.id})")

  while server.running.load():
    if conn.isIdle(server.config.idleTimeoutSecs):
      server.logger.logInfo(&"[{conn.address}] idle timeout")
      break

    let frameR = readOneFrame(conn.socket, server.config.maxFrameBytes)
    if frameR.isErr:
      let e = frameR.error
      if e.kind != peInternal:
        sendError(conn, 0, ErrProtocol, ErrCatProtocol, $e)
      break

    let f = frameR.value
    conn.touchActivity()

    if f.payload.len < 2:
      sendError(conn, f.header.requestId, ErrProtocol, ErrCatProtocol,
        "payload too short")
      continue

    let typeVal = int((uint16(f.payload[0]) shl 8) or uint16(f.payload[1]))

    var handler: Option[MessageHandler]
    withLock(server.handlersMu):
      if server.handlers.hasKey(typeVal):
        handler = some(server.handlers.getOrDefault(typeVal))

    if handler.isSome:
      handler.get()(conn, f.header.requestId, f.header.flags, f.payload)
    elif typeVal <= 0x00FF:
      handleBuiltinCore(server, conn, f.header.requestId, f.header.flags,
        f.payload)
    else:
      sendError(conn, f.header.requestId, ErrProtocol, ErrCatProtocol,
        &"no handler for message type 0x{typeVal:04X}")

# ---------------------------------------------------------------------------
# Thread entry points
# ---------------------------------------------------------------------------

proc clientLoopThread(args: ClientLoopArgs) {.thread.} =
  clientLoop(args.srv, args.conn)

proc acceptLoop(args: AcceptLoopArgs) {.thread.} =
  let server = args.srv
  let sock = args.sock
  while server.running.load():
    var clientSock: Socket
    var address = ""
    try:
      sock.accept(clientSock)
      let (peerAddr, _) = clientSock.getPeerAddr()
      address = peerAddr
    except CatchableError as e:
      if server.running.load():
        server.logger.logWarn("accept error: " & e.msg)
      break

    if server.clientCount() >= server.config.maxConnections:
      server.logger.logWarn(&"max connections reached, rejecting {address}")
      try: clientSock.close() except CatchableError: discard
      continue

    let id = server.nextClientId.fetchAdd(1)
    let conn = newClientConnection(id, clientSock, address)
    server.addClient(conn)

    # Allocate a heap-resident Thread so its lifetime is not tied to this
    # stack frame.  Store in the module-level threadStore so GC won't collect.
    let tRef = new Thread[ClientLoopArgs]
    {.cast(gcsafe).}:
      withLock(threadStoreMu):
        threadStore.add(tRef)
    createThread(tRef[], clientLoopThread, (server, conn))

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

proc start*(server: ProtocolServer) {.raises: [].} =
  server.running.store(true)
  var sock: Socket
  try:
    sock = newSocket()
    sock.setSockOpt(OptReuseAddr, true)
    sock.bindAddr(Port(server.config.port), server.config.host)
    sock.listen()
    server.logger.logInfo(
      &"listening on {server.config.host}:{server.config.port}")
  except CatchableError as e:
    server.logger.logError("failed to bind: " & e.msg)
    server.running.store(false)
    return

  let aRef = new Thread[AcceptLoopArgs]
  withLock(threadStoreMu):
    acceptThreadStore.add(aRef)
  try:
    createThread(aRef[], acceptLoop, (server, sock))
  except ResourceExhaustedError as e:
    server.logger.logError("failed to create accept thread: " & e.msg)
    server.running.store(false)

proc stop*(server: ProtocolServer) {.raises: [].} =
  server.running.store(false)
  server.logger.logInfo("server stopping")
