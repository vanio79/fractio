# TCP Transport - TCP-based network transport for distributed Fractio
# Provides both server and client functionality for TCP communication

import std/[net, nativesockets, tables, locks, times, options, atomics,
    typedthreads, endians]
import ./types
import ./serialization
import ./config
import ../../core/types as coretypes
import ../../utils/logging
import ../../utils/socket_utils

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

  ConnHandlerCtx* = object
    ## Context passed to per-connection handler threads.
    ## transportPtr is a raw pointer (not a traced ref) to break the
    ## TCPTransport → connThreads → ConnHandlerCtx → TCPTransport cycle
    ## that causes ORC's Bacon-Rajan collector to crash (SIGSEGV in rawDealloc).
    ## TCPTransport.stopServer() joins all threads before clearing connThreads,
    ## so the raw pointer is always valid while any handler thread is running.
    transportPtr*: pointer # untraced; cast to TCPTransport before use
    conn*: Connection

  TCPTransport* = ref object
    ## TCP transport for a specific protocol (Raft, Client, Admin)
    config*: NetworkConfig
    port*: int
    role*: string
    serverSocket*: Socket
    running*: Atomic[bool]
    runningLock*: Lock
    connections*: tables.Table[string, Connection] # Key is string(NodeID)
    connectionsLock*: Lock
    nextMessageId*: uint64
    messageIdLock*: Lock
    handlers*: tables.Table[uint16, proc(msg: string): string {.gcsafe.}]
    handlersLock*: Lock
    acceptThread*: Thread[TCPTransport]
    connThreads*: seq[ptr Thread[ConnHandlerCtx]] # raw ptr: Thread on shared heap, no ORC tracking
    connThreadsLock*: Lock

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
  ## Close the connection's socket.  The Locks are NOT deinitialized here
  ## because other threads may still hold a ref to this Connection and
  ## attempt to acquire/release them.  Under --mm:atomicArc the Lock
  ## storage lives inside the ref object and is freed when the last ref
  ## is dropped — deinitLock at that point is unnecessary (the memory
  ## is returned to the allocator as a whole).
  if conn.state != csClosed:
    conn.state = csClosed
    if conn.socket != nil:
      try:
        conn.socket.close()
      except:
        discard

# =============================================================================
# TCP Transport
# =============================================================================

proc newTCPTransport*(config: NetworkConfig, port: int,
    role: string): TCPTransport =
  result = TCPTransport(
    config: config,
    port: port,
    role: role,
    running: Atomic[bool](),
    connections: tables.initTable[string, Connection](),
    nextMessageId: 1,
    handlers: tables.initTable[uint16, proc(msg: string): string {.gcsafe.}]()
  )
  initLock(result.runningLock)
  initLock(result.connectionsLock)
  initLock(result.messageIdLock)
  initLock(result.handlersLock)
  initLock(result.connThreadsLock)

proc nextMessageId*(t: TCPTransport): uint64 =
  withLock t.messageIdLock:
    result = t.nextMessageId
    t.nextMessageId += 1

proc isRunning*(t: TCPTransport): bool =
  result = t.running.load(moRelaxed)

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

proc readFrameNoLog(socket: Socket, timeoutMs: int,
    connClosed: var bool): Option[string] =
  ## GC-safe version of readFrame without logging.
  ## Sets connClosed=true when the remote end closes the connection (recv=0),
  ## as distinct from a plain timeout which leaves connClosed=false.
  connClosed = false
  try:
    var headerBuf = newString(FRAME_HEADER_SIZE)
    let n = socket.recv(headerBuf, FRAME_HEADER_SIZE, timeoutMs)
    if n == 0:
      connClosed = true
      return none(string)
    if n < FRAME_HEADER_SIZE:
      return none(string)

    let (header, _) = decodeFrameHeader(headerBuf)

    if header.payloadLen.int > MAX_MESSAGE_SIZE:
      return none(string)

    var payload = newString(header.payloadLen.int)
    if header.payloadLen > 0:
      let n2 = socket.recv(payload, header.payloadLen.int)
      if n2 == 0 or n2 < header.payloadLen.int:
        return none(string)

    let computedChecksum = computeCRC32(payload)
    if computedChecksum != header.checksum:
      return none(string)

    return some(payload)

  except CatchableError:
    return none(string)

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
  var socket: Socket
  try:
    socket = newSocket()
    # NOTE: OptNoDelay (TCP_NODELAY) must NOT be set before connect() on Linux.
    # Setting TCP_NODELAY on a non-blocking socket before connect() causes the
    # kernel to return EPERM ("Permission denied") via select() instead of the
    # expected ECONNREFUSED when the remote port is closed.
    # We set it AFTER a successful connect() instead.
    socket.setLingerZero()
    socket.setSockOpt(OptKeepAlive, t.config.tcpKeepAlive)

    let remoteAddr = host & ":" & $port
    var fields = tables.initTable[string, string]()
    fields["nodeId"] = string(nodeId)
    fields["addr"] = remoteAddr
    info("Connecting to node", fields)

    socket.connect(host, Port(port), timeout = t.config.tcpConnectTimeoutMs)

    # Set TCP_NODELAY after connect; must pass IPPROTO_TCP level (default SOL_SOCKET
    # would set SO_DEBUG=1 which requires root and returns EACCES on Linux).
    socket.setSockOpt(OptNoDelay, t.config.tcpNoDelay, level = IPPROTO_TCP.cint)

    let conn = newConnection(nodeId, socket, remoteAddr)

    withLock t.connectionsLock:
      t.connections[string(nodeId)] = conn

    info("Connected to node", fields)
    return some(conn)

  except CatchableError as e:
    # Close the socket to avoid leaking the file descriptor.
    if socket != nil:
      try: socket.close() except CatchableError: discard
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

proc connHandlerProc(ctx: ConnHandlerCtx) {.thread.} =
  ## Per-connection handler thread.  Reads messages from the connection,
  ## dispatches to registered handlers, sends responses, and cleans up
  ## when the connection closes or the transport stops.
  let t = cast[TCPTransport](ctx.transportPtr)
  let conn = ctx.conn

  while t.running.load(moRelaxed) and conn.state == csConnected:
    try:
      var remoteClosed = false
      let payloadOpt = readFrameNoLog(conn.socket, 200, remoteClosed)
      if payloadOpt.isNone:
        if remoteClosed or not t.running.load(moRelaxed):
          break
        continue

      let payload = payloadOpt.get()

      if payload.len < 6:
        continue

      var headerLen: uint32
      bigEndian32(addr headerLen, payload[0].unsafeAddr)

      if payload.len < 4 + int(headerLen) or headerLen < 2:
        continue

      var msgType: uint16
      bigEndian16(addr msgType, payload[4].unsafeAddr)

      var response: string
      withLock t.handlersLock:
        if msgType in t.handlers:
          response = t.handlers[msgType](payload)
        else:
          response = ""

      if response.len > 0 and conn.state == csConnected:
        try:
          let frame = encodeFrame(response)
          conn.socket.send(frame)
        except Exception:
          break # socket closed or error; exit handler loop

    except Exception:
      break

  conn.state = csClosed
  conn.close()

  withLock t.connectionsLock:
    t.connections.del(string(conn.nodeId))

proc selectReadable(fd: SocketHandle, timeoutMs: int): bool =
  ## Poll a single file descriptor for readability using poll() with timeout.
  ## Returns true if the fd is readable (i.e. accept() will not block).
  ## Uses poll() instead of select() because select() crashes with
  ## FD_SET buffer overflow when fd >= FD_SETSIZE (1024).
  when defined(posix):
    var pfd: TPollfd
    pfd.fd = fd.cint
    pfd.events = POLLIN
    pfd.revents = 0
    let n = poll(addr pfd, 1, timeoutMs.cint)
    result = n > 0 and (pfd.revents and POLLIN) != 0
  else:
    # Fallback: always return true (will block on accept)
    result = true

proc acceptLoopWrapper(t: TCPTransport) {.thread.} =
  ## Accept loop — accepts incoming TCP connections and spawns a handler
  ## thread for each one so multiple peers can be served concurrently.
  ## Uses select() with a 200ms timeout before each accept() so the loop
  ## can check `running` periodically and exit promptly during shutdown
  ## (closing a socket from another thread does not reliably unblock a
  ## blocking accept() on Linux).
  while t.running.load(moRelaxed):
    try:
      # Guard: if serverSocket was closed by stopServer, exit immediately.
      if t.serverSocket.isNil:
        return

      # Poll the server socket for 200ms; if nothing arrives, loop back
      # and re-check the running flag. This avoids blocking indefinitely
      # in accept() which would prevent clean shutdown.
      let fd = t.serverSocket.getFd()
      if fd == osInvalidSocket:
        return
      if not selectReadable(fd, 200):
        continue

      # Re-check running flag after select returns — stopServer may have
      # closed the socket between selectReadable and acceptAddr.
      if not t.running.load(moRelaxed):
        return

      # Final nil check — stopServer may have closed the socket
      if t.serverSocket.isNil:
        return

      var client: Socket
      var clientAddr: string
      t.serverSocket.acceptAddr(client, clientAddr)

      if client.isNil:
        continue

      client.setSockOpt(OptNoDelay, t.config.tcpNoDelay,
          level = IPPROTO_TCP.cint)
      client.setSockOpt(OptKeepAlive, t.config.tcpKeepAlive)

      let tempId = NodeID("unknown_" & clientAddr)
      let conn = newConnection(tempId, client, clientAddr)

      withLock t.connectionsLock:
        t.connections[string(tempId)] = conn

      # Spawn a handler thread for this connection.
      # Allocate Thread on the shared heap (ptr, not ref) so ORC does not
      # track it and cannot corrupt the shared-heap free list.
      let thr = cast[ptr Thread[ConnHandlerCtx]](allocShared0(sizeof(Thread[
          ConnHandlerCtx])))
      let ctx = ConnHandlerCtx(transportPtr: cast[pointer](t), conn: conn)
      createThread(thr[], connHandlerProc, ctx)

      withLock t.connThreadsLock:
        t.connThreads.add(thr)

    except CatchableError:
      if not t.running.load(moRelaxed):
        return

proc startServer*(t: TCPTransport): bool =
  try:
    t.serverSocket = newSocket()
    t.serverSocket.setLingerZero()
    t.serverSocket.bindAddr(Port(t.port), t.config.bindAddress)
    t.serverSocket.listen()

    t.running.store(true)

    var fields = tables.initTable[string, string]()
    fields["role"] = t.role
    fields["port"] = $t.port
    info("TCP server started", fields)

    # Start accept thread
    createThread(t.acceptThread, acceptLoopWrapper, t)

    return true

  except CatchableError as e:
    var fields = tables.initTable[string, string]()
    fields["error"] = e.msg
    error("Failed to start TCP server", fields)
    return false

proc stopServer*(t: TCPTransport) =
  t.running.store(false)

  # Shutdown and close server socket to unblock accept.
  # shutdown(SHUT_RDWR) reliably wakes a blocking accept/select on Linux,
  # whereas close() alone may not unblock another thread.
  if t.serverSocket != nil:
    try:
      when defined(posix):
        discard posix.shutdown(t.serverSocket.getFd(), posix.SHUT_RDWR)
      t.serverSocket.close()
    except:
      discard
    t.serverSocket = nil

  # Wait for accept thread to finish (at most ~200ms for select timeout)
  if t.acceptThread.running:
    t.acceptThread.joinThread()

  # Close all active connections so handler threads unblock on recv
  withLock t.connectionsLock:
    for nodeId, conn in t.connections:
      conn.close()

  # Join all per-connection handler threads and free their shared-heap memory
  withLock t.connThreadsLock:
    for thr in t.connThreads:
      if thr[].running:
        joinThread(thr[])
      deallocShared(thr)
    t.connThreads.setLen(0)

  # Note: do NOT call t.connections.clear() here.  Under --mm:atomicArc,
  # clearing the table deallocates Connection refs.  If a pool thread
  # obtained a ref via getOrCreateConnection before the pool lock was
  # acquired and still holds it, the dealloc races in addToSharedFreeList
  # causing SIGSEGV.  The sockets are already closed above; the table
  # entries will be cleaned up when the TCPTransport ref is collected.

  var fields = tables.initTable[string, string]()
  fields["role"] = t.role
  fields["port"] = $t.port
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
  deinitLock(t.connThreadsLock)
