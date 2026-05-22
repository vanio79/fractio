# Multiplexed Raft Transport
#
# TCP transport that multiplexes all Raft groups over a single port.
# Messages are prefixed with GroupID for demuxing.
# 
# All sockets use NON-BLOCKING mode with select() polling to prevent hanging.

import std/[locks, nativesockets, net, os, hashes, strutils]
import std/atomics
import std/typedthreads
import std/tables as nimtables
import posix
from std/times import Time, getTime

# Use explicit module aliases to avoid NodeID ambiguity:
# - core_types.NodeID is distinct string (used here for transport identity)
# - group_types.NodeID is distinct uint32 (used in Raft group configuration)
import fractio/core/types as core_types
import fractio/distributed/network/types as network_types
import fractio/distributed/network/serialization
import fractio/distributed/raft/group_types
import fractio/utils/socket_utils

# =============================================================================
# Socket Options Helpers
# =============================================================================

type TLinger = object
  ## POSIX linger structure for SO_LINGER option
  l_onoff: cint
  l_linger: cint

proc setZeroLinger(socket: SocketHandle) =
  ## Set zero linger on socket to avoid TIME_WAIT state on close.
  ## This is aggressive and should only be used for test/ephemeral sockets.
  var linger = TLinger(l_onoff: 1, l_linger: 0)
  if setsockopt(socket, SOL_SOCKET, SO_LINGER, addr linger,
                sizeof(linger).SockLen) < 0:
    # Non-critical - just log warning
    discard

# =============================================================================
# Message Types for Internal Routing
# =============================================================================

type
  RaftMessageKind* = enum
    rmkRequestVote
    rmkRequestVoteResponse
    rmkAppendEntries
    rmkAppendEntriesResponse
    rmkInstallSnapshot
    rmkInstallSnapshotResponse
    rmkTimeoutNow
    rmkReadIndex
    rmkReadIndexResponse
    rmkCustom

  RaftMessage* = object
    case kind*: RaftMessageKind
    of rmkRequestVote:
      rvMsg*: RequestVoteMsg
    of rmkRequestVoteResponse:
      rvResp*: RequestVoteResponseMsg
    of rmkAppendEntries:
      aeMsg*: AppendEntriesMsg
    of rmkAppendEntriesResponse:
      aeResp*: AppendEntriesResponseMsg
    of rmkInstallSnapshot:
      isMsg*: InstallSnapshotMsg
    of rmkInstallSnapshotResponse:
      isResp*: InstallSnapshotResponseMsg
    of rmkTimeoutNow:
      tnMsg*: TimeoutNowMsg
    of rmkReadIndex:
      riMsg*: ReadIndexMsg
    of rmkReadIndexResponse:
      riResp*: ReadIndexResponseMsg
    of rmkCustom:
      customData*: string

  MessageHandler* = proc(groupId: GroupID, msg: RaftMessage) {.gcsafe, closure.}

# =============================================================================
# Connection State
# =============================================================================

type
  PeerConnection = ref object
    ## Represents a connection to a peer node
    nodeId*: core_types.NodeID ## core_types.NodeID (distinct string)
    syncSocket*: Socket        ## Synchronous socket for sending from callbacks
    lastActivity*: times.Time
    sendLock*: Lock

  PendingResponse = ref object
    ## Tracks a pending RPC response
    groupId*: GroupID
    msgId*: uint64
    timestamp*: times.Time
    handler*: proc(resp: RaftMessage) {.gcsafe, closure.}

# =============================================================================
# MultiplexedRaftTransport
# =============================================================================

type
  MultiplexedRaftTransport* = ref object
    ## TCP transport that multiplexes all Raft groups over a single port.

    # Identity
    nodeId*: core_types.NodeID ## core_types.NodeID (distinct string)
    host*: string
    port*: int

    # Server socket
    serverSocket*: Socket
    serverRunning*: Atomic[bool]
    acceptThread*: Thread[pointer]

    # Peer connections (nodeId -> connection)
    connections*: nimtables.Table[core_types.NodeID, PeerConnection]
    connectionsLock*: Lock

    # Group handlers (groupId -> handler)
    groupHandlers*: nimtables.Table[GroupID, MessageHandler]
    handlersLock*: Lock

    # Pending responses (msgId -> pending)
    pendingResponses*: nimtables.Table[uint64, PendingResponse]
    pendingLock*: Lock
    nextMsgId*: uint64

    # Message queue for async processing
    messageQueue*: seq[tuple[groupId: GroupID, conn: PeerConnection, data: string]]
    queueLock*: Lock
    queueCond*: bool

    # Coordinator callback for message delivery
    coordinatorCb*: proc(groupId: GroupID, msgData: cstring,
        msgLen: csize_t) {.gcsafe, closure.}

proc newMultiplexedRaftTransport*(nodeId: core_types.NodeID, host: string,
    port: int): MultiplexedRaftTransport =
  ## Create a new multiplexed transport.
  new(result)
  result.nodeId = nodeId
  result.host = host
  result.port = port
  result.serverRunning.store(false)
  result.connections = nimtables.initTable[core_types.NodeID, PeerConnection]()
  result.groupHandlers = nimtables.initTable[GroupID, MessageHandler]()
  result.pendingResponses = nimtables.initTable[uint64, PendingResponse]()
  result.messageQueue = @[]
  initLock(result.connectionsLock)
  initLock(result.handlersLock)
  initLock(result.pendingLock)
  initLock(result.queueLock)

# =============================================================================
# Message Framing
# =============================================================================

const
  RaftMagic = 0x52414654'u32 # "RAFT" in hex
  FrameHeaderSize = 20       # magic(4) + groupId(16)

proc encodeRaftFrame(groupId: GroupID, payload: string): string =
  ## Encode a message with GroupID prefix.
  # Frame format:
  # [4 bytes]  magic (0x52414654)
  # [16 bytes] GroupID (ULID)
  # [N bytes]  payload
  result = newString(FrameHeaderSize + payload.len)
  var pos = 0

  # Magic
  result[pos] = char((RaftMagic shr 24) and 0xFF)
  result[pos + 1] = char((RaftMagic shr 16) and 0xFF)
  result[pos + 2] = char((RaftMagic shr 8) and 0xFF)
  result[pos + 3] = char(RaftMagic and 0xFF)
  pos += 4

  # GroupID (16 bytes)
  let ulid = groupIDToULID(groupId)
  for i in 0..<16:
    result[pos + i] = char(ulid.data[i])
  pos += 16

  # Payload
  if payload.len > 0:
    copyMem(addr result[pos], addr payload[0], payload.len)

proc decodeRaftFrame(data: string): tuple[groupId: GroupID, payload: string] =
  ## Decode a message frame, extracting GroupID and payload.
  if data.len < FrameHeaderSize:
    raise newException(ValueError, "Frame too short")

  var pos = 0

  # Check magic
  let magic = uint32(data[pos]) shl 24 or
              uint32(data[pos + 1]) shl 16 or
              uint32(data[pos + 2]) shl 8 or
              uint32(data[pos + 3])
  pos += 4

  if magic != RaftMagic:
    raise newException(ValueError, "Invalid magic number")

  # Extract GroupID
  var ulid: core_types.ULID
  for i in 0..<16:
    ulid.data[i] = uint8(data[pos + i])
  pos += 16
  result.groupId = groupIDFromULID(ulid)

  # Extract payload
  result.payload = data[pos..^1]

# =============================================================================
# Connection Management
# =============================================================================

proc connectToPeer*(t: MultiplexedRaftTransport, nodeId: core_types.NodeID,
    host: string, port: int): bool =
  ## Connect to a peer node using synchronous blocking socket with timeout.
  ##
  ## Thread-safe: checks for existing connection under lock before connecting.

  # Check if connection already exists (avoid duplicate connections)
  withLock t.connectionsLock:
    if t.connections.hasKey(nodeId):
      return true

  var conn: PeerConnection
  new(conn)
  conn.nodeId = nodeId
  conn.lastActivity = getTime()
  initLock(conn.sendLock)

  try:
    # Create synchronous socket (used for all sends via sendSync)
    conn.syncSocket = newSocket()
    # Enable address/port reuse and zero linger for quick cleanup
    conn.syncSocket.setSockOpt(OptReuseAddr, true)
    conn.syncSocket.setSockOpt(OptReusePort, true)
    setZeroLinger(conn.syncSocket.getFd())

    # Connect in blocking mode (with timeout)
    let fd = conn.syncSocket.getFd()
    setBlocking(fd, true)

    # Set connect timeout using SO_SNDTIMEO.
    # On localhost, connects are sub-millisecond. 200ms is more than enough
    # and prevents blocking the timer thread if a peer is slow to respond.
    var tv = posix.Timeval(tv_sec: posix.Time(0), tv_usec: posix.Suseconds(
        200000)) # 200ms timeout
    if setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, addr tv, sizeof(tv).SockLen) < 0:
      discard

    conn.syncSocket.connect(host, Port(port))

    # Switch to non-blocking for all subsequent send operations
    setBlocking(fd, false)

    # Double-check: another thread may have connected while we were connecting
    withLock t.connectionsLock:
      if t.connections.hasKey(nodeId):
        # Another thread beat us — close our connection and return success
        try: conn.syncSocket.close() except: discard
        deinitLock(conn.sendLock)
        return true
      t.connections[nodeId] = conn
    return true
  except Exception as e:
    # Clean up on failure
    if conn.syncSocket != nil:
      try: conn.syncSocket.close() except: discard
    deinitLock(conn.sendLock)
    return false

proc disconnectPeer*(t: MultiplexedRaftTransport, nodeId: core_types.NodeID) =
  ## Disconnect from a peer.
  var conn: PeerConnection
  withLock t.connectionsLock:
    if t.connections.hasKey(nodeId):
      conn = t.connections[nodeId]
      t.connections.del(nodeId)

  if conn != nil:
    try:
      if conn.syncSocket != nil:
        conn.syncSocket.close()
    except:
      discard
    deinitLock(conn.sendLock)

proc getOrCreateConnection*(t: MultiplexedRaftTransport,
    nodeId: core_types.NodeID, host: string, port: int): PeerConnection =
  ## Get an existing connection or create a new one.
  withLock t.connectionsLock:
    if t.connections.hasKey(nodeId):
      return t.connections[nodeId]

  # Create new connection
  if t.connectToPeer(nodeId, host, port):
    withLock t.connectionsLock:
      return t.connections.getOrDefault(nodeId, nil)
  return nil

proc sendSync*(t: MultiplexedRaftTransport, nodeId: core_types.NodeID,
    data: string, host: string = "", port: int = 0): bool =
  ## Send data synchronously to a peer. Used from C++ callbacks.
  ##
  ## If no existing connection exists, tries to establish one using connectToPeer.
  ## On localhost, connects are sub-millisecond, so this is safe to call from
  ## any thread including the timer thread.
  ##
  ## If the connection is broken (EPIPE/ECONNRESET), disconnects and returns false.
  ## The caller (NuRaft) will retry on the next heartbeat/election cycle.

  var conn: PeerConnection
  withLock t.connectionsLock:
    conn = t.connections.getOrDefault(nodeId, nil)

  if conn == nil:
    # No existing connection — try to establish one.
    # connectToPeer uses a 200ms timeout, which is safe even from the timer
    # thread on localhost (connects are sub-ms).
    if host.len > 0 and port > 0:
      if not t.connectToPeer(nodeId, host, port):
        return false
      # Re-acquire the connection after connecting
      withLock t.connectionsLock:
        conn = t.connections.getOrDefault(nodeId, nil)
      if conn == nil:
        return false
    else:
      # No host/port info, can't connect
      return false

  if conn.syncSocket == nil:
    return false

  # Try to send on this connection
  var sendFailed = false
  var needReconnect = false

  withLock conn.sendLock:
    try:
      let fd = conn.syncSocket.getFd()
      var totalSent = 0
      var retryCount = 0
      const MaxRetries = 100
      const SendTimeoutMs = 500

      while totalSent < data.len and retryCount < MaxRetries:
        var writeFds: seq[SocketHandle] = @[fd]
        let ready = nativesockets.selectWrite(writeFds, SendTimeoutMs)

        if ready <= 0:
          sendFailed = true
          break

        let remaining = data.len - totalSent
        let sent = posix.send(fd, addr data[totalSent], remaining.cint, 0.cint)

        if sent < 0:
          let errno = posix.errno
          if errno == EAGAIN or errno == EWOULDBLOCK:
            inc retryCount
            sleep(10)
            continue
          if errno == EPIPE or errno == ECONNRESET or errno == ECONNABORTED:
            needReconnect = true
            sendFailed = true
            break
          sendFailed = true
          break
        elif sent == 0:
          sendFailed = true
          break
        else:
          totalSent += sent.int
          retryCount = 0

      if not sendFailed:
        if totalSent < data.len:
          sendFailed = true
        else:
          conn.lastActivity = getTime()
          return true
    except Exception:
      sendFailed = true

  # If send failed due to broken connection, disconnect
  if needReconnect:
    t.disconnectPeer(nodeId)

  return false

# =============================================================================
# Message Sending
# =============================================================================

proc sendMessage*(t: MultiplexedRaftTransport, groupId: GroupID, nodeId: core_types.NodeID,
                   msg: RaftMessage, host: string, port: int): bool =
  ## Send a message to a specific peer for a specific group.
  ## Delegates to sendSync which handles connection management.
  # Serialize message
  var payload: string
  case msg.kind
  of rmkRequestVote:
    payload = encodeRequestVoteMsg(msg.rvMsg)
  of rmkRequestVoteResponse:
    payload = encodeRequestVoteResponseMsg(msg.rvResp)
  of rmkAppendEntries:
    payload = encodeAppendEntriesMsg(msg.aeMsg)
  of rmkAppendEntriesResponse:
    payload = encodeAppendEntriesResponseMsg(msg.aeResp)
  of rmkInstallSnapshot:
    payload = encodeInstallSnapshotMsg(msg.isMsg)
  of rmkInstallSnapshotResponse:
    payload = encodeInstallSnapshotResponseMsg(msg.isResp)
  of rmkTimeoutNow:
    payload = encodeTimeoutNowMsg(msg.tnMsg)
  of rmkReadIndex:
    payload = encodeReadIndexMsg(msg.riMsg)
  of rmkReadIndexResponse:
    payload = encodeReadIndexResponseMsg(msg.riResp)
  of rmkCustom:
    payload = msg.customData

  # Encode frame
  let frame = encodeRaftFrame(groupId, payload)

  # Send via sendSync
  return t.sendSync(nodeId, frame, host, port)

# =============================================================================
# Message Receiving
# =============================================================================

proc handleMessage(t: MultiplexedRaftTransport, conn: PeerConnection,
    data: string) =
  ## Handle a received message.
  try:
    let (groupId, payload) = decodeRaftFrame(data)
    # Debug output disabled
    # echo "DEBUG: handleMessage received groupId=", groupId, " payloadLen=", payload.len

    # If we have a coordinator callback, deliver directly
    if t.coordinatorCb != nil:
      t.coordinatorCb(groupId, cstring(payload), csize_t(payload.len))
      return

    # Otherwise, look up handler for this group (legacy path)
    var handler: MessageHandler = nil
    withLock t.handlersLock:
      handler = t.groupHandlers.getOrDefault(groupId, nil)

    if handler != nil:
      # Parse message type from header
      if payload.len >= MESSAGE_HEADER_SIZE:
        let header = decodeHeader(payload)
        var msg: RaftMessage

        case header.messageType
        of uint16(rmtRequestVote):
          msg = RaftMessage(kind: rmkRequestVote, rvMsg: decodeRequestVoteMsg(payload))
        of uint16(rmtRequestVoteResponse):
          msg = RaftMessage(kind: rmkRequestVoteResponse,
              rvResp: decodeRequestVoteResponseMsg(payload))
        of uint16(rmtAppendEntries):
          msg = RaftMessage(kind: rmkAppendEntries,
              aeMsg: decodeAppendEntriesMsg(payload))
        of uint16(rmtAppendEntriesResponse):
          msg = RaftMessage(kind: rmkAppendEntriesResponse,
              aeResp: decodeAppendEntriesResponseMsg(payload))
        of uint16(rmtInstallSnapshot):
          msg = RaftMessage(kind: rmkInstallSnapshot,
              isMsg: decodeInstallSnapshotMsg(payload))
        of uint16(rmtInstallSnapshotResponse):
          msg = RaftMessage(kind: rmkInstallSnapshotResponse,
              isResp: decodeInstallSnapshotResponseMsg(payload))
        of uint16(rmtTimeoutNow):
          msg = RaftMessage(kind: rmkTimeoutNow, tnMsg: decodeTimeoutNowMsg(payload))
        of uint16(rmtReadIndex):
          msg = RaftMessage(kind: rmkReadIndex, riMsg: decodeReadIndexMsg(payload))
        of uint16(rmtReadIndexResponse):
          msg = RaftMessage(kind: rmkReadIndexResponse,
              riResp: decodeReadIndexResponseMsg(payload))
        else:
          msg = RaftMessage(kind: rmkCustom, customData: payload)

        handler(groupId, msg)
  except:
    discard

# =============================================================================
# Server
# =============================================================================

proc readOneMessage(client: Socket, t: MultiplexedRaftTransport): bool =
  ## Read a single message from a client socket.
  ## Returns true if a complete message was read and processed.
  ## Returns false on error or disconnect.
  ## For non-blocking sockets, returns true if no data available (caller should retry).
  ## Uses NON-BLOCKING recv with select polling.
  ##
  ## NOTE: This is called from the accept loop AFTER selectRead indicated data is
  ## available. So we use 0ms timeout for the initial selectRead (non-blocking check)
  ## and short timeouts for recv chunks (to handle partial reads).

  # Quick non-blocking check - data should be available since accept loop
  # already detected it with selectRead(fds, 0)
  var fds = @[client.getFd()]
  let ready = nativesockets.selectRead(fds, 0)       # Non-blocking
  if ready <= 0:
    # No data available - return true so caller continues polling
    return true

  # Check for socket errors (e.g., ECONNRESET after peer crash)
  # This avoids trying to read from a broken socket, which wastes time.
  var errVal: cint = cint(0)
  var errLen: SockLen = sizeof(errVal).SockLen
  if getsockopt(client.getFd(), SOL_SOCKET, SO_ERROR, addr errVal,
      addr errLen) == 0 and errVal != 0:
    # Socket has a pending error - connection is broken
    return false

  try:
    # Read frame header (magic + groupId = 20 bytes) using non-blocking recv
    let fd = client.getFd().cint
    var headerBuf = newString(FrameHeaderSize)
    let headerRead = recvExactNonBlocking(fd, headerBuf, FrameHeaderSize,
        100) # Short timeout - data should be available immediately
    if headerRead == 0:
      # Connection closed by peer
      return false
    if headerRead != FrameHeaderSize:
      # Incomplete read - could be a slow sender or connection error.
      # Since we already confirmed data was available via selectRead,
      # a partial read likely means the connection is broken.
      return false

    # Check magic
    let magic = uint32(headerBuf[0]) shl 24 or
                uint32(headerBuf[1]) shl 16 or
                uint32(headerBuf[2]) shl 8 or
                uint32(headerBuf[3])
    if magic != RaftMagic:
      return false

    # Extract GroupID
    var ulid: core_types.ULID
    for i in 0..<16:
      ulid.data[i] = uint8(headerBuf[4 + i])
    let groupId = groupIDFromULID(ulid)

    # Read length prefix (4 bytes)
    var lenBuf = newString(4)
    let lenRead = recvExactNonBlocking(fd, lenBuf, 4, 100)
    if lenRead != 4:
      return false

    let payloadLen = int(uint32(lenBuf[0]) shl 24 or
                         uint32(lenBuf[1]) shl 16 or
                         uint32(lenBuf[2]) shl 8 or
                         uint32(lenBuf[3]))

    if payloadLen > 10 * 1024 * 1024: # 10MB max
      return false

    # Read payload
    var payload = newString(payloadLen)
    if payloadLen > 0:
      let payloadRead = recvExactNonBlocking(fd, payload, payloadLen,
          1000) # 1 second timeout for payload
      if payloadRead != payloadLen:
        return false

    # Deliver to coordinator
    if t.coordinatorCb != nil:
      t.coordinatorCb(groupId, cstring(payload), csize_t(payloadLen))

    return true
  except:
    return false

proc acceptLoop(t: MultiplexedRaftTransport) {.thread.} =
  ## Accept incoming connections.
  ## Each connection can carry multiple RPC messages.
  ## We use non-blocking mode and poll all connections to handle concurrent clients.

  var activeClients: seq[Socket] = @[]
  var pollCount = 0

  while t.serverRunning.load():
    try:
      pollCount += 1

      # Check for new connections (with short timeout)
      var listenFds = @[t.serverSocket.getFd()]
      let listenReady = nativesockets.selectRead(listenFds, 10) # 10ms timeout

      if listenReady > 0:
        var client: Socket
        t.serverSocket.accept(client)
        # Set non-blocking mode for polling
        setBlocking(client.getFd(), false)
        activeClients.add(client)

      # Poll all active clients for data
      if activeClients.len > 0:
        var toRemove: seq[int] = @[]
        for i, client in activeClients:
          var fds = @[client.getFd()]
          let ready = nativesockets.selectRead(fds, 0) # No timeout - just check
          if ready > 0:
            # Data available - try to read a message
            if not readOneMessage(client, t):
              # Connection closed or error
              toRemove.add(i)

        # Remove closed connections (in reverse order to preserve indices)
        for i in countdown(toRemove.len - 1, 0):
          try:
            activeClients[toRemove[i]].close()
          except:
            discard
          activeClients.del(toRemove[i])

      sleep(1) # Small yield to prevent busy-waiting

    except Exception:
      if t.serverRunning.load():
        sleep(10)

  # Accept loop exited normally (server stopped)
  discard

proc startServer*(t: MultiplexedRaftTransport): bool =
  ## Start the TCP server.
  if t.serverRunning.load():
    return true

  try:
    t.serverSocket = newSocket()
    # Enable address reuse to allow quick rebinding after shutdown
    t.serverSocket.setSockOpt(OptReuseAddr, true)
    # Enable port reuse (Linux) to allow multiple binds to same port
    t.serverSocket.setSockOpt(OptReusePort, true)
    # Set zero linger to avoid TIME_WAIT on close (aggressive for tests)
    setZeroLinger(t.serverSocket.getFd())
    t.serverSocket.bindAddr(Port(t.port), t.host)
    t.serverSocket.listen()
    t.serverRunning.store(true)

    # Start accept thread
    createThread(t.acceptThread, proc(p: pointer) {.thread.} =
      let transport = cast[MultiplexedRaftTransport](p)
      transport.acceptLoop()
    , cast[pointer](t))

    return true
  except Exception as e:
    return false

proc stopServer*(t: MultiplexedRaftTransport) =
  ## Stop the TCP server.
  if not t.serverRunning.load():
    return

  t.serverRunning.store(false)

  # Use posix shutdown to unblock any threads waiting on the socket
  # This is more reliable than close() for unblocking blocked syscalls
  try:
    let fd = t.serverSocket.getFd()
    discard posix.shutdown(fd, SHUT_RDWR)
  except:
    discard

  # Wait for accept thread with timeout
  for _ in 0..<50: # 5 seconds max
    try:
      joinThread(t.acceptThread)
      break
    except:
      sleep(100)

  # Now close the socket
  try:
    t.serverSocket.close()
  except:
    discard

  # Close all connections
  withLock t.connectionsLock:
    for conn in t.connections.values:
      try:
        if conn.syncSocket != nil:
          conn.syncSocket.close()
      except:
        discard
    t.connections.clear()

# =============================================================================
# Group Handler Registration
# =============================================================================

proc setCoordinatorCallback*(t: MultiplexedRaftTransport,
    cb: proc(groupId: GroupID, msgData: cstring, msgLen: csize_t) {.gcsafe, closure.}) =
  ## Set the coordinator callback for message delivery.
  t.coordinatorCb = cb

proc registerGroupHandler*(t: MultiplexedRaftTransport, groupId: GroupID,
    handler: MessageHandler) =
  ## Register a handler for a specific group.
  withLock t.handlersLock:
    t.groupHandlers[groupId] = handler

proc unregisterGroupHandler*(t: MultiplexedRaftTransport, groupId: GroupID) =
  ## Unregister a handler for a specific group.
  withLock t.handlersLock:
    t.groupHandlers.del(groupId)

# =============================================================================
# Cleanup
# =============================================================================

proc destroy*(t: MultiplexedRaftTransport) =
  ## Clean up the transport.
  t.stopServer()

  deinitLock(t.connectionsLock)
  deinitLock(t.handlersLock)
  deinitLock(t.pendingLock)
  deinitLock(t.queueLock)
