# Fractio protocol client — Phase 1 + Phase 2 + Phase 3 + Phase 4: Core, KV, Transactions, Admin.
#
# Manages a single TCP connection, performs the handshake, and provides
# send/receive with automatic Request ID assignment.
#
# Thread safety: writes are serialised via writeMu. readOneFrame is called
# synchronously from send() — only one caller at a time in Phase 1/2.
#
# I/O Model: NON-BLOCKING sockets with poll() polling.
# All socket operations use non-blocking I/O with poll() to poll for
# readiness before recv/send. This prevents blocking the event loop when
# called from async contexts (like httpbeast handlers).
#
# We use poll() rather than select() to avoid the FD_SETSIZE (typically
# 1024) limit: under sustained HTTP load the web server creates a new
# socket per request, so file descriptors can easily exceed 1024 and
# the FD_SET macro inside select() would corrupt memory.

import std/[net, strformat, atomics, locks, options, strutils, nativesockets]
import posix
import ../utils/socket_utils
import ../distributed/raft/group_types
import ../core/types
import ./types
import ./codec
import ./frame
import ./messages/kv as kvMsgs
import ./handshake
import ./messages/core
import ./messages/txn as txnMsgs
import ./messages/admin as adminMsgs
import ./messages/cluster as clusterMsgs
import ./messages/space as spaceMsgs

# ---------------------------------------------------------------------------
# Safe logging helper (no Logger dep in client to keep it lightweight)
# ---------------------------------------------------------------------------

proc clientLog(msg: string) {.gcsafe, raises: [].} =
  try: echo "[protocol.client] " & msg
  except CatchableError: discard

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

type
  ClientConfig* = object
    host*: string
    port*: int
    timeoutMs*: int   ## socket operation timeout (0 = block forever, not recommended)
    clientId*: string
    authMethod*: AuthMethod
    authData*: string ## encoded credentials

proc defaultClientConfig*(host: string = "127.0.0.1",
    port: int = 9000): ClientConfig =
  ClientConfig(
    host: host,
    port: port,
    timeoutMs: 30_000,
    clientId: "fractio-client",
    authMethod: amNone,
    authData: "",
  )

# ---------------------------------------------------------------------------
# Protocol client
# ---------------------------------------------------------------------------

type
  ProtocolClient* = ref object
    config*: ClientConfig
    socket*: Socket
    fd*: cint ## Cached file descriptor for direct posix calls
    connected*: Atomic[bool]
    negotiatedFeatures*: uint32
    nextRequestId*: Atomic[uint32]
    writeMu*: Lock

proc newProtocolClient*(config: ClientConfig): ProtocolClient =
  result = ProtocolClient(config: config)
  initLock(result.writeMu)
  result.connected.store(false)
  result.nextRequestId.store(1)

# ---------------------------------------------------------------------------
# Socket blocking mode helper (fcntl)
# ---------------------------------------------------------------------------

proc setSocketNonBlocking(fd: cint): bool {.raises: [].} =
  ## Set socket to non-blocking mode using fcntl.
  ## Returns true if successful.
  let flags = fcntl(fd, F_GETFL)
  if flags == -1:
    return false
  let rc = fcntl(fd, F_SETFL, flags or O_NONBLOCK)
  rc != -1

# ---------------------------------------------------------------------------
# poll() polling helpers with timeout
# ---------------------------------------------------------------------------
#
# We use posix.poll() rather than posix.select()+FD_SET() because the
# latter silently writes past the end of the fd_set bitset when the
# file descriptor is >= FD_SETSIZE (1024 on Linux). The web server
# creates a new FractioClient (and hence a new TCP socket) per HTTP
# request, so under sustained load we routinely have fds > 1024 and
# the FD_SET macro would corrupt memory.
#
# poll() has no FD_SETSIZE limit: each fd is described by a
# heap/stack-allocated struct pollfd, so it scales to any fd value.
#
# Behavioural contract preserved from the old select()-based code:
#   - timeoutMs > 0  -> wait at most timeoutMs milliseconds
#   - timeoutMs <= 0 -> block indefinitely (mapped to poll() -1)
#   - EINTR is retried in a tight loop so callers don't have to
#   - On timeout (poll() returns 0) or error (poll() returns -1) we
#     return false; on ready (poll() returns 1) we return true iff
#     the relevant event bit is set in revents. POLLERR and POLLHUP
#     are always treated as "ready" because they signal a closed or
#     failing socket that the caller must observe.
# ---------------------------------------------------------------------------

proc pollForRead(fd: cint, timeoutMs: int): bool {.gcsafe, raises: [].} =
  ## Poll socket for read readiness using poll() with timeout.
  ## Returns true if data is available (or POLLERR/POLLHUP), false on
  ## timeout or error. Safe for any fd value (no FD_SETSIZE limit).
  let waitMs: cint = if timeoutMs <= 0: -1 else: timeoutMs.cint
  var pfd: TPollfd
  pfd.fd = fd
  pfd.events = cshort(POLLIN or POLLERR or POLLHUP)
  pfd.revents = 0

  var attempts = 0
  const maxAttempts = 16
  while true:
    let rc = posix.poll(addr pfd, Tnfds(1), waitMs)
    if rc > 0:
      # Ready. revents may include POLLERR/POLLHUP — treat as "woken".
      return (pfd.revents and cshort(POLLIN or POLLERR or POLLHUP)) != 0
    if rc == 0:
      # Timeout.
      return false
    # rc < 0 — EINTR is the only retry-worthy error here.
    let err = errno
    if err == EINTR:
      inc attempts
      if attempts >= maxAttempts: return false
      continue
    # Any other error (EFAULT, EINVAL, ENOMEM, …).
    return false

proc pollForWrite(fd: cint, timeoutMs: int): bool {.gcsafe, raises: [].} =
  ## Poll socket for write readiness using poll() with timeout.
  ## Returns true if socket is writable (or POLLERR/POLLHUP), false on
  ## timeout or error. Safe for any fd value (no FD_SETSIZE limit).
  let waitMs: cint = if timeoutMs <= 0: -1 else: timeoutMs.cint
  var pfd: TPollfd
  pfd.fd = fd
  pfd.events = cshort(POLLOUT or POLLERR or POLLHUP)
  pfd.revents = 0

  var attempts = 0
  const maxAttempts = 16
  while true:
    let rc = posix.poll(addr pfd, Tnfds(1), waitMs)
    if rc > 0:
      # Ready. revents may include POLLERR/POLLHUP — treat as "woken".
      return (pfd.revents and cshort(POLLOUT or POLLERR or POLLHUP)) != 0
    if rc == 0:
      # Timeout.
      return false
    # rc < 0 — EINTR is the only retry-worthy error here.
    let err = errno
    if err == EINTR:
      inc attempts
      if attempts >= maxAttempts: return false
      continue
    # Any other error (EFAULT, EINVAL, ENOMEM, …).
    return false

# ---------------------------------------------------------------------------
# Non-blocking recv with poll() polling
# ---------------------------------------------------------------------------

proc recvExactNonBlocking(fd: cint, buf: var string, size: int,
                          timeoutMs: int): int {.gcsafe, raises: [].} =
  ## Read exactly `size` bytes using non-blocking recv with poll() polling.
  ## Returns the number of bytes actually read (< size means timeout/error/closed).
  buf.setLen(size)
  var total = 0
  var retries = 0
  const maxRetries = 100 # Safety limit to prevent infinite loops
  let sockFd = SocketHandle(fd)

  while total < size and retries < maxRetries:
    # Poll for read readiness
    if not pollForRead(fd, timeoutMs):
      # Timeout or error
      buf.setLen(total)
      return total

    # Socket is ready - attempt recv
    let got = posix.recv(sockFd, addr buf[total], size - total, 0)

    if got > 0:
      total += got
      retries = 0 # Reset retry count on successful read
    elif got == 0:
      # Connection closed by peer
      buf.setLen(total)
      return total
    else:
      # got < 0 - check errno
      let err = errno
      if err == EAGAIN or err == EWOULDBLOCK:
        # Shouldn't happen since we polled, but handle it
        inc retries
        # Small yield to prevent CPU spinning
        when defined(posix):
          discard posix.usleep(1000) # 1ms
      else:
        # Real error (EPIPE, ECONNRESET, etc.)
        buf.setLen(total)
        return total

  buf.setLen(total)
  total

proc recvNNonBlocking(fd: cint, n: int, timeoutMs: int): string {.gcsafe,
    raises: [].} =
  ## Read exactly n bytes using non-blocking recv; returns shorter string on timeout/EOF/error.
  result = newString(n)
  let got = recvExactNonBlocking(fd, result, n, timeoutMs)
  result.setLen(got)

# ---------------------------------------------------------------------------
# Non-blocking send with poll() polling
# ---------------------------------------------------------------------------

proc sendNonBlocking(fd: cint, data: string, timeoutMs: int): int {.gcsafe,
    raises: [].} =
  ## Send data using non-blocking socket with poll() polling.
  ## Returns number of bytes sent (< data.len means timeout/error).
  var total = 0
  var retries = 0
  const maxRetries = 100
  let sockFd = SocketHandle(fd)

  while total < data.len and retries < maxRetries:
    # Poll for write readiness
    if not pollForWrite(fd, timeoutMs):
      # pollForWrite can spuriously report "not writable" on a freshly-
      # established socket (e.g., right after connect(), or with MSG_NOSIGNAL
      # semantics). Treat it like EAGAIN: brief yield, then retry.
      inc retries
      when defined(posix):
        discard posix.usleep(1000)
      continue

    # Socket is ready - attempt send
    let sent = posix.send(sockFd, addr data[total], data.len - total, 0)

    if sent > 0:
      total += sent
      retries = 0
    elif sent == 0:
      # posix.send() returning 0 is unusual but can occur on some platforms
      # (e.g., right after connect, or with MSG_NOSIGNAL) when the kernel has
      # no space to write. Treat it like EAGAIN: brief yield, then retry.
      inc retries
      when defined(posix):
        discard posix.usleep(1000)
    else:
      # sent < 0 - check errno
      let err = errno
      if err == EAGAIN or err == EWOULDBLOCK:
        inc retries
        when defined(posix):
          discard posix.usleep(1000)
      else:
        # Real error
        return total

  total

# ---------------------------------------------------------------------------
# Low-level send — returns PResult (void success)
# ---------------------------------------------------------------------------

proc sendRaw(client: ProtocolClient, data: string): PResult {.gcsafe, raises: [].} =
  ## Send raw data using non-blocking socket with poll() polling.
  acquire(client.writeMu)
  try:
    if not client.connected.load():
      return pErr(newProtocolError(peInternal, "not connected"))

    let sent = sendNonBlocking(client.fd, data, client.config.timeoutMs)
    if sent != data.len:
      # Partial send or timeout - treat as error
      client.connected.store(false)
      return pErr(newProtocolError(peInternal,
        "send incomplete: sent " & $sent & " of " & $data.len & " bytes"))

    result = pOk()
  finally:
    release(client.writeMu)

proc sendPayload(client: ProtocolClient, payload: string,
    requestId: uint32, flags: uint16 = 0): PResult {.gcsafe, raises: [].} =
  sendRaw(client, encodeFrame(payload, requestId, flags))

# ---------------------------------------------------------------------------
# Connect and handshake (non-blocking throughout)
# ---------------------------------------------------------------------------

proc connect*(client: ProtocolClient): PResult {.raises: [].} =
  ## Connect with timeout using non-blocking socket + poll().
  ## Socket remains non-blocking after connect for all subsequent I/O.
  if client.connected.load(): return pOk()

  try:
    client.socket = newSocket()
    let fd = client.socket.getFd()
    client.fd = cint(fd)

    # Set non-blocking mode for connect (and keep it for all I/O)
    if not setSocketNonBlocking(client.fd):
      client.socket.close()
      return pErr(newProtocolError(peInternal,
          "failed to set non-blocking mode"))

    # Initiate non-blocking connect (returns immediately)
    try:
      client.socket.connect(client.config.host, Port(client.config.port))
    except CatchableError as e:
      # On non-blocking socket, connect raises "Operation now in progress"
      # when the connection is being established - this is expected
      if "Operation now in progress" notin e.msg and "EINPROGRESS" notin e.msg:
        client.socket.close()
        return pErr(newProtocolError(peInternal, "connect failed: " & e.msg))

    # Wait for connection with timeout using poll()
    if not pollForWrite(client.fd, client.config.timeoutMs):
      client.socket.close()
      return pErr(newProtocolError(peInternal, "connect timeout"))

    # Check if connection succeeded (getsockopt SO_ERROR)
    var error: cint = 0
    var errorLen: SockLen = sizeof(error).SockLen
    if posix.getsockopt(SocketHandle(client.fd), SOL_SOCKET, SO_ERROR,
        addr error, addr errorLen) < 0:
      client.socket.close()
      return pErr(newProtocolError(peInternal, "getsockopt failed"))

    if error != 0:
      client.socket.close()
      return pErr(newProtocolError(peInternal,
        "connect failed: " & $strerror(error)))

    # Connection succeeded - socket stays non-blocking
    client.socket.setLingerZero()

  except CatchableError as e:
    return pErr(newProtocolError(peInternal, "connect failed: " & e.msg))

  # --- Read server greeting using non-blocking recv ---
  # Greeting wire layout:
  #   4 bytes magic
  #   2 bytes version
  #   4 bytes features
  #   1 byte auth-method count  ← variable; read this to know how many more bytes
  #   N bytes auth methods
  #   2 bytes serverId
  #   8 bytes clusterId
  # Read the fixed prefix (4+2+4+1 = 11 bytes) first.
  let greetPrefix = recvNNonBlocking(client.fd, 11, client.config.timeoutMs)
  if greetPrefix.len != 11:
    client.socket.close()
    return pErr(newProtocolError(peInternal, "server closed during greeting"))
  let authCount = int(uint8(greetPrefix[10]))
  # Then read the variable part + suffix (authCount + 2 + 8 = authCount+10).
  let greetRest = recvNNonBlocking(client.fd, authCount + 10,
      client.config.timeoutMs)
  if greetRest.len != authCount + 10:
    client.socket.close()
    return pErr(newProtocolError(peInternal,
        "server closed during greeting (rest)"))
  let greetBuf = greetPrefix & greetRest

  let greetR = decodeGreeting(greetBuf)
  if greetR.isErr:
    client.socket.close()
    return pErr(greetR.error)
  let greet = greetR.value

  if greet.version != PROTOCOL_VERSION_1:
    client.socket.close()
    return pErr(newProtocolError(peVersionMismatch,
      &"server version {greet.version} != {PROTOCOL_VERSION_1}"))

  # --- Send client handshake using non-blocking send ---
  let hs = ClientHandshake(
    version: PROTOCOL_VERSION_1,
    features: FeatPipelining or FeatTransactions,
    authType: uint8(client.config.authMethod),
    authData: client.config.authData,
    clientId: client.config.clientId,
  )
  let hsData = encodeClientHandshake(hs)
  let hsSent = sendNonBlocking(client.fd, hsData, client.config.timeoutMs)
  if hsSent != hsData.len:
    client.socket.close()
    return pErr(newProtocolError(peInternal, "handshake send incomplete"))

  # --- Read handshake response using non-blocking recv ---
  # Response wire layout:
  #   1 byte status
  #   4 bytes features
  #   1 byte serverName length (uint8)
  #   N bytes serverName
  #   if status != OK: 2 bytes errLen + errLen bytes errorMessage
  # Read fixed prefix: 1+4+1 = 6 bytes
  let rspPrefix = recvNNonBlocking(client.fd, 6, client.config.timeoutMs)
  if rspPrefix.len != 6:
    client.socket.close()
    return pErr(newProtocolError(peInternal, "server closed during handshake"))
  let rspStatus = uint8(rspPrefix[0])
  let nameLen = int(uint8(rspPrefix[5]))
  # Read serverName
  let rspName = if nameLen > 0: recvNNonBlocking(client.fd, nameLen,
      client.config.timeoutMs)
                else: ""
  if rspName.len != nameLen:
    client.socket.close()
    return pErr(newProtocolError(peInternal,
        "server closed during handshake (name)"))
  # If error: read error message
  var rspErrPart = ""
  if rspStatus != HandshakeOK:
    let errLenBuf = recvNNonBlocking(client.fd, 2, client.config.timeoutMs)
    if errLenBuf.len != 2:
      client.socket.close()
      return pErr(newProtocolError(peInternal,
          "server closed during handshake (errlen)"))
    let errLen = int((uint16(errLenBuf[0]) shl 8) or uint16(errLenBuf[1]))
    rspErrPart = recvNNonBlocking(client.fd, errLen, client.config.timeoutMs)
  let rspBuf = rspPrefix & rspName & rspErrPart

  let rspR = decodeHandshakeResponse(rspBuf)
  if rspR.isErr:
    client.socket.close()
    return pErr(rspR.error)
  let rsp = rspR.value

  if rsp.status != HandshakeOK:
    client.socket.close()
    return pErr(newProtocolError(peAuthFailed,
      "handshake rejected: " & rsp.errorMessage))

  client.negotiatedFeatures = rsp.features
  client.connected.store(true)
  clientLog("connected to " & client.config.host & ":" & $client.config.port)
  pOk()

proc disconnect*(client: ProtocolClient, reason: string = "") {.gcsafe,
    raises: [].} =
  if not client.connected.load(): return
  let r = if reason.len > 0: " (" & reason & ")" else: ""
  clientLog("disconnecting from " & client.config.host & ":" &
      $client.config.port & r)
  client.connected.store(false)
  if client.socket != nil:
    try: client.socket.close() except CatchableError: discard

# ---------------------------------------------------------------------------
# Read one response frame from socket (non-blocking with poll)
# ---------------------------------------------------------------------------

proc readOneFrame(client: ProtocolClient): Result[Frame,
    ProtocolError] {.gcsafe, raises: [].} =
  var hdrBuf = newString(FRAME_HEADER_SIZE)
  let hn = recvExactNonBlocking(client.fd, hdrBuf, FRAME_HEADER_SIZE,
                                client.config.timeoutMs)
  if hn != FRAME_HEADER_SIZE:
    client.connected.store(false)
    return peErr(newProtocolError(peInvalidFrame,
      &"short header: got {hn}, expected {FRAME_HEADER_SIZE}"))

  var pos = 0
  let hdrR = decodeFrameHeader(hdrBuf, pos)
  if hdrR.isErr:
    client.connected.store(false)
    return peErr(hdrR.error)
  let hdr = hdrR.value

  var payload = newString(int(hdr.payloadLen))
  if hdr.payloadLen > 0:
    let pn = recvExactNonBlocking(client.fd, payload, int(hdr.payloadLen),
                                  client.config.timeoutMs)
    if pn != int(hdr.payloadLen):
      client.connected.store(false)
      return peErr(newProtocolError(peInvalidFrame,
        &"short payload: got {pn}, expected {hdr.payloadLen}"))

  let computed = computeCRC16(payload)
  if computed != hdr.checksum:
    client.connected.store(false)
    return peErr(newProtocolError(peChecksumMismatch,
      &"CRC16 mismatch: got {hdr.checksum:#06x}, computed {computed:#06x}"))

  peOk(Frame(header: hdr, payload: payload))

# ---------------------------------------------------------------------------
# Send a message payload, wait for one response frame
# ---------------------------------------------------------------------------

proc send*(client: ProtocolClient,
    payload: string): Result[Frame, ProtocolError] {.gcsafe, raises: [].} =
  if not client.connected.load():
    return peErr(newProtocolError(peInternal, "not connected"))

  let reqId = client.nextRequestId.fetchAdd(1)
  let sr = sendPayload(client, payload, reqId)
  if sr.isErr:
    client.connected.store(false)
    return peErr(sr.error)

  let frameR = readOneFrame(client)
  if frameR.isErr:
    return peErr(frameR.error)

  let f = frameR.value
  # Verify the response's requestId matches the request we just sent. Without
  # this check, a response for an in-flight or stale request (e.g. a streaming
  # scan continuation frame) could be returned to the wrong caller, leading
  # to malformed-payload decode errors downstream.
  if f.header.requestId != reqId:
    return peErr(newProtocolError(peInvalidFrame,
      &"response requestId mismatch: sent {reqId}, got {f.header.requestId}"))

  if (f.header.flags and FlagIsError) != 0:
    var pos = 2 # skip MessageType prefix
    let codeR = readUint32BE(f.payload, pos)
    let code = if codeR.isOk: codeR.value else: ErrProtocol

    # For NOT_LEADER errors, parse the redirect info
    if code == ErrNotLeader:
      # Skip category (1 byte) + msgLen (2 bytes) + msg
      discard readUint8(f.payload, pos) # category byte, unused
      let msgLenR = readUint16BE(f.payload, pos)
      if msgLenR.isOk:
        pos += int(msgLenR.value) # skip message
        # Read details length and details
        let detailsLenR = readUint16BE(f.payload, pos)
        if detailsLenR.isOk:
          let detailsLen = int(detailsLenR.value)
          if pos + detailsLen <= f.payload.len:
            let details = f.payload[pos ..< pos + detailsLen]
            let redirect = decodeLeaderRedirect(details)
            return peErr(newProtocolError(peNotLeader, "not leader", redirect))
      return peErr(newProtocolError(peNotLeader,
          "not leader (no redirect info)"))

    return peErr(newProtocolError(peInternal, &"server error 0x{code:08X}"))

  peOk(f)

# ---------------------------------------------------------------------------
# Core convenience procs
# ---------------------------------------------------------------------------

proc ping*(client: ProtocolClient): Result[uint64, ProtocolError] {.gcsafe,
    raises: [].} =
  let r = client.send(encodePingRequest())
  if r.isErr: return peErr(r.error)
  decodePingResponse(r.value.payload)

proc echo*(client: ProtocolClient,
    data: string): Result[string, ProtocolError] {.gcsafe, raises: [].} =
  let r = client.send(encodeEchoRequest(data))
  if r.isErr: return peErr(r.error)
  decodeEchoData(r.value.payload)

proc closeConn*(client: ProtocolClient, reason: string = "") {.gcsafe, raises: [].} =
  if not client.connected.load(): return
  let reqId = client.nextRequestId.fetchAdd(1)
  discard sendNonBlocking(client.fd, encodeFrame(encodeCloseRequest(reason), reqId),
                          client.config.timeoutMs)
  disconnect(client)

# ---------------------------------------------------------------------------
# KV convenience procs (Phase 2)
# ---------------------------------------------------------------------------

# Forward declaration for streaming scan result type
type
  KeyExtractor* = proc(key: string): string {.closure, gcsafe, raises: [].}
    ## Extracts the comparable portion of a storage key for k-way merge.
    ## For data row keys, this strips the groupId prefix so that rows from
    ## different groups are compared by primary key value, not by groupId.
    ## For non-data keys, returns the full key unchanged.

  StreamWithKey* = object
    ## A stream paired with its current peek key for k-way merge.
    stream*: StreamingScanClient
    peekKey*: string
    peekPair*: Option[kvMsgs.ScanPair]
    exhausted*: bool

  StreamingScanClient* = ref object
    ## Client-side streaming scan state - reads multiple frames from server.
    ## Supports two modes:
    ##   1. Single-group: direct streaming from one group
    ##   2. K-way merge: opens all group streams and merges by key order

    # Single-group mode fields
    client*: ProtocolClient
    reqFlags*: uint16
    streamId*: uint32
    hasMore*: bool
    exhausted*: bool
    currentFrame*: ScanResponseFrame
    framePos*: int
    totalReceived*: int
    error*: Option[ProtocolError]

    # K-way merge mode fields
    pairsSent*: int
      ## Count of pairs returned (for limit enforcement)
    scanLimit*: uint32
      ## Original limit for multi-group scan (0 = no limit)

    # K-way merge mode fields (when kWayMergeMode is true)
    kWayMergeMode*: bool
      ## True when merging multiple group streams by key order
    mergeStreams*: seq[StreamWithKey]
      ## All group streams with their current peek state
    mergeInitialized*: bool
      ## Whether the initial peek has been done for all streams
    keyExtractor*: KeyExtractor
      ## Extracts the comparable key portion for k-way merge ordering.
      ## When nil, full storage key string comparison is used (correct for
      ## single-group and system table scans). For multi-group data table
      ## scans, set to primaryKeyFromDataRowKey to compare by PK value
      ## across groups instead of by groupId prefix.
    mergeReverse*: bool
      ## When true, the k-way merge selects the LARGEST key (descending order)
      ## instead of the smallest. Used for PK DESC + LIMIT pushdown.

proc kvGet*(client: ProtocolClient, key: string,
    flags: uint8 = 0, txnId: TransactionID = zeroTransactionID(),
    readTimestamp: uint64 = 0,
    filter: Option[WireFilterExpr] = none(WireFilterExpr)): Result[GetResponse,
        ProtocolError] {.gcsafe,
    raises: [].} =
  ## Send a Get request and return the decoded response.
  ## filter: optional server-side filter (PointGet optimization)
  let req = GetRequest(flags: flags, txnId: txnId,
                       readTimestamp: readTimestamp, key: key,
                       filter: filter)
  let r = client.send(encodeGetRequest(req))
  if r.isErr: return peErr(r.error)
  decodeGetResponse(r.value.payload)

proc kvPut*(client: ProtocolClient, key: string, value: string,
    flags: uint8 = 0, txnId: TransactionID = zeroTransactionID(),
    expectedVersion: uint64 = 0): Result[PutResponse, ProtocolError] {.gcsafe,
    raises: [].} =
  ## Send a Put request and return the decoded response.
  let req = PutRequest(flags: flags, txnId: txnId,
                       expectedVersion: expectedVersion,
                       key: key, value: value)
  let r = client.send(encodePutRequest(req))
  if r.isErr: return peErr(r.error)
  decodePutResponse(r.value.payload)

proc kvDelete*(client: ProtocolClient, key: string,
    flags: uint8 = 0,
    txnId: TransactionID = zeroTransactionID()): Result[DeleteResponse,
        ProtocolError] {.gcsafe,
    raises: [].} =
  ## Send a Delete request and return the decoded response.
  let req = DeleteRequest(flags: flags, txnId: txnId, key: key)
  let r = client.send(encodeDeleteRequest(req))
  if r.isErr: return peErr(r.error)
  decodeDeleteResponse(r.value.payload)

proc kvGetInGroup*(client: ProtocolClient, key: string,
    groupId: GroupID,
    flags: uint8 = 0, txnId: TransactionID = zeroTransactionID(),
    readTimestamp: uint64 = 0,
    filter: Option[WireFilterExpr] = none(WireFilterExpr)): Result[GetResponse,
        ProtocolError] {.gcsafe,
    raises: [].} =
  ## Send a Get request routed to a specific Raft group.
  ## filter: optional server-side filter (PointGet optimization)
  let req = GetRequest(flags: flags, txnId: txnId,
                       readTimestamp: readTimestamp, key: key,
                       groupId: groupId,
                       filter: filter)
  let r = client.send(encodeGetRequest(req))
  if r.isErr: return peErr(r.error)
  decodeGetResponse(r.value.payload)

proc kvRawPutInGroup*(client: ProtocolClient, key: string, value: string,
    groupId: GroupID,
    flags: uint8 = 0, txnId: TransactionID = zeroTransactionID(),
    expectedVersion: uint64 = 0): Result[PutResponse, ProtocolError] {.gcsafe,
    raises: [].} =
  let req = PutRequest(flags: flags, txnId: txnId,
                       expectedVersion: expectedVersion,
                       key: key, value: value,
                       groupId: groupId)
  let r = client.send(encodeRawPutRequest(req))
  if r.isErr: return peErr(r.error)
  decodePutResponse(r.value.payload)

proc kvPutInGroup*(client: ProtocolClient, key: string, value: string,
    groupId: GroupID,
    flags: uint8 = 0, txnId: TransactionID = zeroTransactionID(),
    expectedVersion: uint64 = 0): Result[PutResponse, ProtocolError] {.gcsafe,
    raises: [].} =
  ## Send a Put request routed to a specific Raft group.
  let req = PutRequest(flags: flags, txnId: txnId,
                       expectedVersion: expectedVersion,
                       key: key, value: value,
                       groupId: groupId)
  let r = client.send(encodePutRequest(req))
  if r.isErr: return peErr(r.error)
  decodePutResponse(r.value.payload)

proc kvDeleteInGroup*(client: ProtocolClient, key: string,
    groupId: GroupID,
    flags: uint8 = 0,
    txnId: TransactionID = zeroTransactionID()): Result[DeleteResponse,
        ProtocolError] {.gcsafe,
    raises: [].} =
  ## Send a Delete request routed to a specific Raft group.
  let req = DeleteRequest(flags: flags, txnId: txnId, key: key,
                          groupId: groupId)
  let r = client.send(encodeDeleteRequest(req))
  if r.isErr: return peErr(r.error)
  decodeDeleteResponse(r.value.payload)

proc kvBatch*(client: ProtocolClient,
    req: BatchRequest): Result[BatchResponse, ProtocolError] {.gcsafe,
    raises: [].} =
  ## Send a Batch request and return the decoded response.
  let r = client.send(encodeBatchRequest(req))
  if r.isErr: return peErr(r.error)
  decodeBatchResponse(r.value.payload)

proc kvScan*(client: ProtocolClient, startKey: string = "",
    endKey: string = "", limit: uint32 = 0,
    flags: uint16 = 0, txnId: TransactionID = zeroTransactionID(),
    readTimestamp: uint64 = 0,
    groupId: GroupID = ZeroGroupID()): Result[ScanResponseFrame,
        ProtocolError] {.
    gcsafe, raises: [].} =
  ## Send a Scan request and return the first (and only, in Phase 2) response frame.
  ## If groupId is provided, sets the GroupRouted flag for server-side routing filter.
  var actualFlags = flags
  if groupId != ZeroGroupID():
    actualFlags = actualFlags or kvMsgs.ScanFlagGroupRouted
  let req = ScanRequest(
    flags: actualFlags,
    txnId: txnId,
    readTimestamp: readTimestamp,
    startKey: startKey,
    endKey: endKey,
    limit: limit,
    groupId: groupId,
  )
  let r = client.send(encodeScanRequest(req))
  if r.isErr: return peErr(r.error)
  decodeScanResponseFrame(r.value.payload, actualFlags)

# ---------------------------------------------------------------------------
# Streaming Scan (Phase 2 - Extended)
# ---------------------------------------------------------------------------

proc newStreamingScanClient*(client: ProtocolClient): StreamingScanClient =
  ## Create a new streaming scan client state object for single-group mode.
  new(result)
  result.client = client
  if client != nil:
    result.streamId = client.nextRequestId.fetchAdd(1)
  else:
    result.streamId = 0
  result.hasMore = false
  result.exhausted = false
  result.currentFrame = ScanResponseFrame()
  result.framePos = 0
  result.totalReceived = 0
  result.error = none(ProtocolError)
  result.pairsSent = 0
  result.scanLimit = 0
  result.kWayMergeMode = false
  result.mergeStreams = @[]
  result.mergeInitialized = false

proc newKWayMergeScanClient*(streams: seq[StreamingScanClient],
    limit: uint32 = 0,
    keyExtractor: KeyExtractor = nil,
    reverse: bool = false): StreamingScanClient =
  ## Create a streaming scan client that merges multiple group streams
  ## using k-way merge. All streams must already be started (have their
  ## first frame loaded). Results are returned in globally sorted key order.
  ##
  ## streams: all group streams (already started, one per group)
  ## limit: total limit across all groups (0 = no limit)
  ## keyExtractor: extracts the comparable key portion for merge ordering.
  ##   When nil, full storage key string comparison is used. For multi-group
  ##   data table scans, pass primaryKeyFromDataRowKey to compare by PK value
  ##   instead of by groupId prefix.
  ## reverse: when true, the merge selects the LARGEST key first (descending).
  new(result)
  result.client = nil
  result.streamId = if streams.len > 0: streams[0].streamId else: 0
  result.hasMore = false
  result.exhausted = streams.len == 0
  result.currentFrame = ScanResponseFrame()
  result.framePos = 0
  result.totalReceived = 0
  result.error = none(ProtocolError)
  result.pairsSent = 0
  result.scanLimit = limit
  result.kWayMergeMode = true
  result.mergeInitialized = false
  result.keyExtractor = keyExtractor
  result.mergeReverse = reverse
  # Initialize merge streams - peek state will be filled on first nextPair() call
  for stream in streams:
    result.mergeStreams.add(StreamWithKey(
      stream: stream,
      peekKey: "",
      peekPair: none(kvMsgs.ScanPair),
      exhausted: false
    ))

proc startStreamScan*(ss: StreamingScanClient, startKey: string = "",
    endKey: string = "", limit: uint32 = 0,
    chunkSize: uint32 = 0,
    flags: uint16 = 0, txnId: TransactionID = zeroTransactionID(),
    readTimestamp: uint64 = 0,
    groupId: GroupID = ZeroGroupID(),
    filter: Option[WireFilterExpr] = none(WireFilterExpr),
    columns: Option[seq[string]] = none(seq[string]),
    topK: Option[kvMsgs.WireTopKSpec] = none(kvMsgs.WireTopKSpec)): Result[
        ScanResponseFrame, ProtocolError] {.
    gcsafe, raises: [].} =
  ## Start a streaming scan. Sends the initial request and receives the first frame.
  ## Returns the first frame of results.
  ## chunkSize: number of items per frame (0 = DEFAULT_SCAN_CHUNK_SIZE)
  ## filter: optional server-side filter for reducing network traffic
  ## columns: optional column names to project server-side (Tier-3a optimization);
  ##          when set and non-empty, server decodes each DataRow and re-emits
  ##          only the requested columns, reducing wire size for SELECT id,name
  ##          style queries on wide tables.
  ## topK: optional server-side top-K heap pushdown (Tier-3b). When set with
  ##       non-empty sortSpecs, server runs a bounded top-K heap locally and
  ##       ships only the K winners per group over the wire. Client merges
  ##       at most K×Ngroups candidates instead of N×Ngroups rows.
  ## Note: Only valid for single-group mode (not k-way merge mode).
  if ss.kWayMergeMode:
    return peErr(newProtocolError(peInternal,
        "startStreamScan not valid for multi-group mode"))
  if not ss.client.connected.load():
    return peErr(newProtocolError(peInternal, "not connected"))

  var actualFlags = flags or kvMsgs.ScanFlagStreaming
  if groupId != ZeroGroupID():
    actualFlags = actualFlags or kvMsgs.ScanFlagGroupRouted
  if filter.isSome:
    actualFlags = actualFlags or kvMsgs.ScanFlagHasFilter
  if columns.isSome and columns.get().len > 0:
    actualFlags = actualFlags or kvMsgs.ScanFlagHasColumns
  if topK.isSome and topK.get().sortSpecs.len > 0:
    actualFlags = actualFlags or kvMsgs.ScanFlagHasTopK

  ss.reqFlags = actualFlags

  let req = ScanRequest(
    flags: actualFlags,
    txnId: txnId,
    readTimestamp: readTimestamp,
    startKey: startKey,
    endKey: endKey,
    limit: limit,
    groupId: groupId,
    chunkSize: chunkSize,
    filter: filter,
    columns: columns,
    topK: topK,
  )

  let r = ss.client.send(encodeScanRequest(req))
  if r.isErr:
    ss.error = some(r.error)
    ss.exhausted = true
    return peErr(r.error)

  let frameR = decodeScanResponseFrame(r.value.payload, actualFlags)
  if frameR.isErr:
    ss.error = some(frameR.error)
    ss.exhausted = true
    return peErr(frameR.error)

  ss.currentFrame = frameR.value
  ss.framePos = 0
  ss.hasMore = (ss.currentFrame.respFlags and kvMsgs.ScanRespFlagHasMore) != 0
  ss.exhausted = (ss.currentFrame.respFlags and kvMsgs.ScanRespFlagEndOfScan) != 0
  ss.totalReceived = ss.currentFrame.pairs.len

  # Check for end of stream
  if ss.exhausted or not ss.hasMore:
    ss.exhausted = true

  peOk(ss.currentFrame)

proc nextFrame*(ss: StreamingScanClient): Result[ScanResponseFrame,
    ProtocolError] {.
    gcsafe, raises: [].} =
  ## Get the next frame from the streaming scan.
  ## Returns empty frame if stream is exhausted.
  if ss.exhausted:
    return peOk(ScanResponseFrame(respFlags: kvMsgs.ScanRespFlagEndOfScan,
                                  pairs: @[], reqFlags: ss.reqFlags))

  if not ss.hasMore:
    ss.exhausted = true
    return peOk(ScanResponseFrame(respFlags: kvMsgs.ScanRespFlagEndOfScan,
                                  pairs: @[], reqFlags: ss.reqFlags))

  # Read next frame from server (non-blocking)
  let frameR = ss.client.readOneFrame()
  if frameR.isErr:
    ss.error = some(frameR.error)
    ss.exhausted = true
    return peErr(frameR.error)

  let f = frameR.value
  if (f.header.flags and FlagIsError) != 0:
    var pos = 2
    let codeR = readUint32BE(f.payload, pos)
    let code = if codeR.isOk: codeR.value else: ErrProtocol
    let err = newProtocolError(peInternal, "server error during scan: 0x" &
                               code.toHex(8))
    ss.error = some(err)
    ss.exhausted = true
    return peErr(err)

  let decodedR = decodeScanResponseFrame(f.payload, ss.reqFlags)
  if decodedR.isErr:
    ss.error = some(decodedR.error)
    ss.exhausted = true
    return peErr(decodedR.error)

  ss.currentFrame = decodedR.value
  ss.framePos = 0
  ss.hasMore = (ss.currentFrame.respFlags and kvMsgs.ScanRespFlagHasMore) != 0
  ss.exhausted = (ss.currentFrame.respFlags and kvMsgs.ScanRespFlagEndOfScan) != 0
  ss.totalReceived += ss.currentFrame.pairs.len

  if ss.exhausted or not ss.hasMore:
    ss.exhausted = true

  peOk(ss.currentFrame)

# Forward declaration for closeStream (needed by nextPair)
proc closeStream*(ss: StreamingScanClient) {.gcsafe, raises: [].}

# Forward declarations (needed for k-way merge which calls hasNext)
proc hasNext*(ss: StreamingScanClient): bool {.gcsafe, raises: [].}

proc nextPair*(ss: StreamingScanClient): Option[kvMsgs.ScanPair] {.gcsafe,
    raises: [].} =
  ## Get the next individual KV pair from the stream.
  ## Returns some(pair) if available, none() if exhausted.
  ##
  ## Three modes:
  ##   1. Single-group: reads from one server stream
  ##   2. Multi-group sequential: iterates groups one at a time (legacy)
  ##   3. K-way merge: merges all group streams by key order

  # K-way merge mode: find the smallest key across all streams
  if ss.kWayMergeMode:
    if ss.exhausted:
      return none(kvMsgs.ScanPair)

    # Check limit
    if ss.scanLimit > 0 and ss.pairsSent >= int(ss.scanLimit):
      ss.exhausted = true
      return none(kvMsgs.ScanPair)

    # Lazy initialization: peek the first pair from each stream on first call
    if not ss.mergeInitialized:
      for i in 0 ..< ss.mergeStreams.len:
        if ss.mergeStreams[i].exhausted:
          continue
        let stream = ss.mergeStreams[i].stream
        if stream.hasNext():
          let pairOpt = stream.nextPair()
          if pairOpt.isSome:
            ss.mergeStreams[i].peekPair = pairOpt
            ss.mergeStreams[i].peekKey = pairOpt.get().key
          else:
            ss.mergeStreams[i].exhausted = true
        else:
          ss.mergeStreams[i].exhausted = true
      ss.mergeInitialized = true

    # Find the stream with the smallest peek key (or largest if reverse).
    # Use keyExtractor if available to compare by PK across groups
    var bestIdx = -1
    var bestCompareKey = ""
    for i in 0 ..< ss.mergeStreams.len:
      let swk = addr(ss.mergeStreams[i])
      if swk.exhausted:
        continue
      if swk.peekPair.isNone:
        continue
      let compareKey = if ss.keyExtractor != nil:
        ss.keyExtractor(swk.peekKey) else: swk.peekKey
      if bestIdx < 0:
        bestIdx = i
        bestCompareKey = compareKey
      else:
        if ss.mergeReverse:
          if compareKey > bestCompareKey:
            bestIdx = i
            bestCompareKey = compareKey
        else:
          if compareKey < bestCompareKey:
            bestIdx = i
            bestCompareKey = compareKey

    if bestIdx < 0:
      ss.exhausted = true
      return none(kvMsgs.ScanPair)

    # Return the best pair and advance that stream
    let result = ss.mergeStreams[bestIdx].peekPair
    ss.mergeStreams[bestIdx].peekPair = none(kvMsgs.ScanPair)
    ss.mergeStreams[bestIdx].peekKey = ""

    # Advance the chosen stream: peek next pair
    let stream = ss.mergeStreams[bestIdx].stream
    if stream.hasNext():
      let nextPairOpt = stream.nextPair()
      if nextPairOpt.isSome:
        ss.mergeStreams[bestIdx].peekPair = nextPairOpt
        ss.mergeStreams[bestIdx].peekKey = nextPairOpt.get().key
      else:
        ss.mergeStreams[bestIdx].exhausted = true
    else:
      ss.mergeStreams[bestIdx].exhausted = true

    # Check for errors in any stream
    for swk in ss.mergeStreams:
      if swk.stream.error.isSome:
        ss.error = swk.stream.error
        # Don't mark exhausted - allow caller to check error separately

    inc ss.totalReceived
    inc ss.pairsSent
    return result

  # Single-group mode - check currentFrame.pairs FIRST before exhausted
  # A stream can be marked exhausted (EndOfScan flag) but still have pairs
  # in currentFrame that need to be consumed.
  if ss.framePos < ss.currentFrame.pairs.len:
    let pair = ss.currentFrame.pairs[ss.framePos]
    ss.framePos += 1
    return some(pair)

  # No more pairs in current frame - now check if stream is exhausted
  if ss.exhausted:
    return none(kvMsgs.ScanPair)

  # Try to get next frame if server has more
  if not ss.hasMore:
    ss.exhausted = true
    return none(kvMsgs.ScanPair)

  let frameR = ss.nextFrame()
  if frameR.isErr or frameR.value.pairs.len == 0:
    ss.exhausted = true
    return none(kvMsgs.ScanPair)

  # Return first pair from new frame
  let pair = ss.currentFrame.pairs[0]
  ss.framePos = 1
  return some(pair)

proc hasNext*(ss: StreamingScanClient): bool {.gcsafe, raises: [].} =
  ## Check if more pairs are available without consuming them.
  ##
  ## Two modes:
  ##   1. Single-group: check current frame and server has_more
  ##   2. K-way merge: check if any stream has a peeked pair

  # K-way merge mode
  if ss.kWayMergeMode:
    if ss.exhausted:
      return false
    if ss.scanLimit > 0 and ss.pairsSent >= int(ss.scanLimit):
      return false
    # Lazy initialization: on first call, peek the first pair from each stream.
    # This must happen here (not just in nextPair) because callers check
    # hasNext() before calling nextPair().
    if not ss.mergeInitialized:
      for i in 0 ..< ss.mergeStreams.len:
        if ss.mergeStreams[i].exhausted:
          continue
        let stream = ss.mergeStreams[i].stream
        if stream.hasNext():
          let pairOpt = stream.nextPair()
          if pairOpt.isSome:
            ss.mergeStreams[i].peekPair = pairOpt
            ss.mergeStreams[i].peekKey = pairOpt.get().key
          else:
            ss.mergeStreams[i].exhausted = true
        else:
          ss.mergeStreams[i].exhausted = true
      ss.mergeInitialized = true
    # Check if any stream has a peeked pair
    for swk in ss.mergeStreams:
      if not swk.exhausted and swk.peekPair.isSome:
        return true
    return false

  # Single-group mode - check for pairs in current frame first
  # We can return pairs from currentFrame even if stream is exhausted
  if ss.framePos < ss.currentFrame.pairs.len:
    return true

  # No more pairs in current frame - check if server has more
  if ss.exhausted:
    return false
  return ss.hasMore

const MAX_DRAIN_FRAMES = 1000
  ## Maximum number of frames to drain before giving up and disconnecting.
  ## Prevents blocking the client if the server has an extremely large
  ## number of buffered frames. Typical scans have < 100 frames.

proc drainStreamFrames*(ss: StreamingScanClient) {.gcsafe, raises: [].} =
  ## Drain remaining frames from the server to leave the connection clean.
  ##
  ## When a stream is abandoned early (e.g., LIMIT hit, error), the server
  ## may still be sending frames into the TCP socket buffer. If we don't
  ## consume them, the next RPC on this connection would read a stale scan
  ## frame instead of the expected response, causing silent data corruption.
  ##
  ## This proc reads and discards all remaining frames until EndOfScan or
  ## error, leaving the ProtocolClient connection in a clean state for reuse.
  ## If too many frames remain (unlikely), we disconnect as a fallback.
  if ss.client == nil or not ss.client.connected.load(moRelaxed):
    return

  # Already exhausted or no more data — nothing to drain
  if ss.exhausted or not ss.hasMore:
    return

  # Read frames until EndOfScan or error, with a safety limit
  var drainedFrames = 0
  var drainedPairs = 0
  while not ss.exhausted and ss.hasMore and drainedFrames < MAX_DRAIN_FRAMES:
    let frameR = ss.nextFrame()
    if frameR.isErr:
      # Error during drain — connection state is uncertain, disconnect
      clientLog("drain error after " & $drainedFrames &
          " frames, disconnecting")
      ss.client.disconnect()
      return
    drainedFrames += 1
    drainedPairs += frameR.value.pairs.len

  if drainedFrames >= MAX_DRAIN_FRAMES and (not ss.exhausted or ss.hasMore):
    # Too many frames — give up and disconnect
    clientLog("drain limit (" & $MAX_DRAIN_FRAMES &
        " frames) reached, disconnecting")
    ss.client.disconnect()
  else:
    clientLog("drained " & $drainedFrames & " frames (" & $drainedPairs &
        " pairs) for clean connection reuse")

proc closeStream*(ss: StreamingScanClient) {.gcsafe, raises: [].} =
  ## Close the streaming scan and mark it exhausted.
  ##
  ## For non-exhausted streams, we drain remaining server frames instead of
  ## disconnecting the TCP connection. This preserves the connection cache
  ## and avoids the ~100ms reconnect cost on subsequent queries.
  let wasExhausted = ss.exhausted
  let wasKWay = ss.kWayMergeMode
  let hadMore = ss.hasMore

  # In k-way merge mode, close all group streams (recursive)
  if ss.kWayMergeMode:
    # Drain each sub-stream before marking exhausted
    clientLog("closeStream: k-way merge with " & $ss.mergeStreams.len &
        " sub-streams (exhausted=" & $wasExhausted & ")")
    for swk in mitems(ss.mergeStreams):
      if swk.stream != nil:
        swk.stream.closeStream()
        swk.stream = nil
      swk.peekPair = none(kvMsgs.ScanPair)
      swk.exhausted = true
    ss.mergeStreams = @[]
  else:
    # Single-group mode: drain remaining frames to keep connection clean.
    # This avoids the ~100ms reconnect cost of disconnect().
    if not wasExhausted:
      clientLog("closeStream: draining non-exhausted stream (hasMore=" &
          $hadMore & ", totalReceived=" & $ss.totalReceived & ")")
      ss.drainStreamFrames()
    else:
      clientLog("closeStream: stream already exhausted (kWay=" & $wasKWay &
          ", totalReceived=" & $ss.totalReceived & ")")

  ss.exhausted = true
  ss.hasMore = false

proc getError*(ss: StreamingScanClient): Option[ProtocolError] {.gcsafe,
    raises: [].} =
  ## Get any error that occurred during the scan.
  ss.error

proc getTotalReceived*(ss: StreamingScanClient): int {.gcsafe, raises: [].} =
  ## Get total number of pairs received across all frames/groups.
  ss.totalReceived

proc consumeStreamScan*(ss: StreamingScanClient): Result[seq[kvMsgs.ScanPair],
    ProtocolError] {.gcsafe, raises: [].} =
  ## Consume all remaining pairs from the stream and return them as a sequence.
  ## Warning: For large result sets, this defeats the purpose of streaming.
  var pairs: seq[kvMsgs.ScanPair] = @[]
  while ss.hasNext():
    let pairOpt = ss.nextPair()
    if pairOpt.isSome:
      pairs.add(pairOpt.get())
  ss.closeStream()
  if ss.error.isSome:
    return peErr(ss.error.get())
  peOk(pairs)

proc kvStreamScan*(client: ProtocolClient, startKey: string = "",
    endKey: string = "", limit: uint32 = 0,
    chunkSize: uint32 = 0,
    flags: uint16 = 0, txnId: TransactionID = zeroTransactionID(),
    readTimestamp: uint64 = 0,
    groupId: GroupID = ZeroGroupID(),
    filter: Option[WireFilterExpr] = none(WireFilterExpr),
    reverse: bool = false,
    columns: Option[seq[string]] = none(seq[string]),
    topK: Option[kvMsgs.WireTopKSpec] = none(kvMsgs.WireTopKSpec)): Result[
        StreamingScanClient, ProtocolError] {.
    gcsafe, raises: [].} =
  ## Start a streaming scan and return a StreamingScanClient for iteration.
  ## Use ss.nextPair() to get individual pairs, or ss.consumeStreamScan() to
  ## get all results as a sequence.
  ## filter: optional server-side filter for reducing network traffic
  ## reverse: when true, results are returned in descending key order.
  ##          The server interprets bounds as (startKey, endKey] in this case.
  ## columns: optional column-name list for server-side projection (Tier-3a);
  ##          when set and non-empty, server decodes each DataRow, projects
  ##          to the requested columns, and re-encodes. Cuts wire bytes by
  ##          N_requested / N_total for wide-table SELECT col1, col2 queries.
  ## topK: optional server-side top-K heap spec (Tier-3b). When set, each
  ##       group server runs a bounded top-K heap locally and ships only the
  ##       K winners over the wire. Client merges at most K×Ngroups candidates.
  var combinedFlags = flags
  if reverse:
    combinedFlags = combinedFlags or kvMsgs.ScanFlagReverse
  let ss = newStreamingScanClient(client)
  let firstFrameR = ss.startStreamScan(startKey, endKey, limit, chunkSize, combinedFlags,
                                        txnId, readTimestamp, groupId, filter,
                                        columns, topK)
  if firstFrameR.isErr:
    return peErr(firstFrameR.error)
  peOk(ss)

# ---------------------------------------------------------------------------
# Transaction convenience procs (Phase 3)
# ---------------------------------------------------------------------------

proc beginTxn*(client: ProtocolClient, flags: uint8 = 0,
    timeoutMs: uint32 = 0): Result[txnMsgs.BeginTxnResponse,
    ProtocolError] {.gcsafe, raises: [].} =
  ## Begin a new transaction. Returns (txnId, readTimestamp).
  let req = txnMsgs.BeginTxnRequest(flags: flags, timeoutMs: timeoutMs)
  let r = client.send(txnMsgs.encodeBeginTxnRequest(req))
  if r.isErr: return peErr(r.error)
  txnMsgs.decodeBeginTxnResponse(r.value.payload)

proc commitTxn*(client: ProtocolClient,
    txnId: TransactionID): Result[txnMsgs.CommitTxnResponse,
    ProtocolError] {.gcsafe, raises: [].} =
  ## Commit txnId. Check .status for TxnCommitOK / conflict / timeout.
  let req = txnMsgs.CommitTxnRequest(txnId: txnId)
  let r = client.send(txnMsgs.encodeCommitTxnRequest(req))
  if r.isErr: return peErr(r.error)
  txnMsgs.decodeCommitTxnResponse(r.value.payload)

proc rollbackTxn*(client: ProtocolClient,
    txnId: TransactionID): Result[txnMsgs.RollbackTxnResponse,
    ProtocolError] {.gcsafe, raises: [].} =
  ## Roll back txnId.
  let req = txnMsgs.RollbackTxnRequest(txnId: txnId)
  let r = client.send(txnMsgs.encodeRollbackTxnRequest(req))
  if r.isErr: return peErr(r.error)
  txnMsgs.decodeRollbackTxnResponse(r.value.payload)

proc txnStatus*(client: ProtocolClient,
    txnId: TransactionID): Result[txnMsgs.TxnStatusResponse,
    ProtocolError] {.gcsafe, raises: [].} =
  ## Query the status of txnId.
  let req = txnMsgs.TxnStatusRequest(txnId: txnId)
  let r = client.send(txnMsgs.encodeTxnStatusRequest(req))
  if r.isErr: return peErr(r.error)
  txnMsgs.decodeTxnStatusResponse(r.value.payload)

# ---------------------------------------------------------------------------
# Admin convenience procs (Phase 4)
# ---------------------------------------------------------------------------

proc serverInfo*(client: ProtocolClient): Result[adminMsgs.ServerInfoResponse,
    ProtocolError] {.gcsafe, raises: [].} =
  ## Request server identity, version, uptime, role, and connection counts.
  let r = client.send(adminMsgs.encodeServerInfoRequest())
  if r.isErr: return peErr(r.error)
  adminMsgs.decodeServerInfoResponse(r.value.payload)

proc metrics*(client: ProtocolClient,
    flags: uint8 = 0): Result[adminMsgs.MetricsResponse,
    ProtocolError] {.gcsafe, raises: [].} =
  ## Request server metrics. Set flags = MetricsFlagReset to reset counters
  ## after reading.
  let req = adminMsgs.MetricsRequest(flags: flags)
  let r = client.send(adminMsgs.encodeMetricsRequest(req))
  if r.isErr: return peErr(r.error)
  adminMsgs.decodeMetricsResponse(r.value.payload)

proc health*(client: ProtocolClient): Result[adminMsgs.HealthResponse,
    ProtocolError] {.gcsafe, raises: [].} =
  ## Request cluster health status.
  let r = client.send(adminMsgs.encodeHealthRequest())
  if r.isErr: return peErr(r.error)
  adminMsgs.decodeHealthResponse(r.value.payload)

# ---------------------------------------------------------------------------
# Cluster admin convenience procs (Phase 8)
# ---------------------------------------------------------------------------

proc joinNode*(client: ProtocolClient, nodeId: uint16, host: string,
    raftPort: uint16, clientPort: uint16): Result[clusterMsgs.JoinNodeResponse,
    ProtocolError] {.gcsafe, raises: [].} =
  ## Request that the server register a new cluster node.
  let req = clusterMsgs.JoinNodeRequest(
    nodeId: nodeId,
    host: host,
    raftPort: raftPort,
    clientPort: clientPort,
  )
  let r = client.send(clusterMsgs.encodeJoinNodeRequest(req))
  if r.isErr: return peErr(r.error)
  clusterMsgs.decodeJoinNodeResponse(r.value.payload)

proc removeNode*(client: ProtocolClient,
    nodeId: uint16): Result[clusterMsgs.RemoveNodeResponse,
    ProtocolError] {.gcsafe, raises: [].} =
  ## Request that the server deregister a cluster node.
  let req = clusterMsgs.RemoveNodeRequest(nodeId: nodeId)
  let r = client.send(clusterMsgs.encodeRemoveNodeRequest(req))
  if r.isErr: return peErr(r.error)
  clusterMsgs.decodeRemoveNodeResponse(r.value.payload)

proc listNodes*(client: ProtocolClient): Result[clusterMsgs.ListNodesResponse,
    ProtocolError] {.gcsafe, raises: [].} =
  ## Request the full list of cluster nodes known to the server.
  let r = client.send(clusterMsgs.encodeListNodesRequest())
  if r.isErr: return peErr(r.error)
  clusterMsgs.decodeListNodesResponse(r.value.payload)

proc rebalanceStatus*(client: ProtocolClient): Result[
    clusterMsgs.RebalanceStatusResponse, ProtocolError] {.gcsafe, raises: [].} =
  ## Query the server's rebalance operation counters.
  let r = client.send(clusterMsgs.encodeRebalanceStatusRequest())
  if r.isErr: return peErr(r.error)
  clusterMsgs.decodeRebalanceStatusResponse(r.value.payload)

proc drainNode*(client: ProtocolClient,
    nodeId: uint16): Result[clusterMsgs.DrainNodeResponse,
    ProtocolError] {.gcsafe, raises: [].} =
  ## Request that the server mark a cluster node as draining.
  let req = clusterMsgs.DrainNodeRequest(nodeId: nodeId)
  let r = client.send(clusterMsgs.encodeDrainNodeRequest(req))
  if r.isErr: return peErr(r.error)
  clusterMsgs.decodeDrainNodeResponse(r.value.payload)

# ---------------------------------------------------------------------------
# Space management convenience procs
# ---------------------------------------------------------------------------

proc createSpace*(client: ProtocolClient, name: string,
    replicas: int32 = 0): Result[spaceMsgs.CreateSpaceResponse,
    ProtocolError] {.gcsafe, raises: [].} =
  ## Send a CREATE SPACE request to the server.
  ## name: space name (max 65535 bytes)
  ## replicas: replication factor (0 = ALL nodes)
  ## The server will:
  ##   1. Validate the request
  ##   2. Create Raft groups for the space
  ##   3. Wait for leaders to be elected
  ##   4. Write space and group records to sys tables
  ## Returns CreateSpaceResponse with spaceId, groupCount, and updated sys table data.
  let req = spaceMsgs.CreateSpaceRequest(name: name, replicas: replicas)
  let r = client.send(spaceMsgs.encodeCreateSpaceRequest(req))
  if r.isErr: return peErr(r.error)
  spaceMsgs.decodeCreateSpaceResponse(r.value.payload)

proc dropSpace*(client: ProtocolClient, name: string): Result[
    spaceMsgs.DropSpaceResponse, ProtocolError] {.gcsafe, raises: [].} =
  ## Send a DROP SPACE request to the server.
  ## name: space name to drop
  ## The server will:
  ##   1. Validate the space exists and is not "default"
  ##   2. Mark space and group records as deleted
  ##   3. Stop Raft groups on all nodes
  ## Returns DropSpaceResponse with deleted groupIds.
  let req = spaceMsgs.DropSpaceRequest(name: name)
  let r = client.send(spaceMsgs.encodeDropSpaceRequest(req))
  if r.isErr: return peErr(r.error)
  spaceMsgs.decodeDropSpaceResponse(r.value.payload)

# ---------------------------------------------------------------------------
# Directed Group Creation convenience procs
# ---------------------------------------------------------------------------

proc createGroup*(client: ProtocolClient,
    req: clusterMsgs.CreateGroupRequest): Result[
        clusterMsgs.CreateGroupResponse,
    ProtocolError] {.gcsafe, raises: [].} =
  ## Send a CreateGroup request to the server.
  ## The server (if it's the preferred leader) will:
  ##   1. Create the Raft group
  ##   2. Start the server and wait for election (wins unopposed)
  ##   3. Return success with the groupId
  let r = client.send(clusterMsgs.encodeCreateGroupRequest(req))
  if r.isErr: return peErr(r.error)
  clusterMsgs.decodeCreateGroupResponse(r.value.payload)

proc joinGroup*(client: ProtocolClient,
    req: clusterMsgs.JoinGroupRequest): Result[clusterMsgs.JoinGroupResponse,
    ProtocolError] {.gcsafe, raises: [].} =
  ## Send a JoinGroup request to the server.
  ## The server will:
  ##   1. Connect to the creator node
  ##   2. Add itself as a member to the existing Raft group
  ##   3. Return success with the groupId
  let r = client.send(clusterMsgs.encodeJoinGroupRequest(req))
  if r.isErr: return peErr(r.error)
  clusterMsgs.decodeJoinGroupResponse(r.value.payload)

# ---------------------------------------------------------------------------
# Rejoin Protocol convenience procs
# ---------------------------------------------------------------------------

proc findMetaLeader*(client: ProtocolClient): Result[
    clusterMsgs.FindMetaLeaderResponse, ProtocolError] {.gcsafe, raises: [].} =
  ## Ask a node who the current meta leader is.
  ## Any node can answer this.
  let r = client.send(clusterMsgs.encodeFindMetaLeaderRequest())
  if r.isErr: return peErr(r.error)
  clusterMsgs.decodeFindMetaLeaderResponse(r.value.payload)

proc rejoinNode*(client: ProtocolClient,
    req: clusterMsgs.RejoinNodeRequest): Result[
        clusterMsgs.RejoinNodeResponse,
    ProtocolError] {.gcsafe, raises: [].} =
  ## Send a RejoinNode request to the meta leader.
  ## The meta leader will re-add this node to all groups it was a member of
  ## via add_srv and send JoinGroup RPCs so the node can create proper instances.
  let r = client.send(clusterMsgs.encodeRejoinNodeRequest(req))
  if r.isErr: return peErr(r.error)
  clusterMsgs.decodeRejoinNodeResponse(r.value.payload)

proc addServerToGroup*(client: ProtocolClient,
    req: clusterMsgs.AddServerToGroupRequest): Result[
        clusterMsgs.AddServerToGroupResponse,
    ProtocolError] {.gcsafe, raises: [].} =
  ## Send an AddServerToGroup request to the group leader.
  ## Used by the meta leader to forward add_srv to the data group leader
  ## when it is not the leader of that group.
  let r = client.send(clusterMsgs.encodeAddServerToGroupRequest(req))
  if r.isErr: return peErr(r.error)
  clusterMsgs.decodeAddServerToGroupResponse(r.value.payload)
