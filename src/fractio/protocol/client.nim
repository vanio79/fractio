# Fractio protocol client — Phase 1 + Phase 2 + Phase 3 + Phase 4: Core, KV, Transactions, Admin.
#
# Manages a single TCP connection, performs the handshake, and provides
# send/receive with automatic Request ID assignment.
#
# Thread safety: writes are serialised via writeMu. readOneFrame is called
# synchronously from send() — only one caller at a time in Phase 1/2.
#
# Receive I/O: uses posix.recv (truly blocking) with SO_RCVTIMEO set on the
# socket.  Nim's net.recv(timeout=...) variant uses select() which does not
# behave reliably in a multi-threaded context on Linux.

import std/[net, strformat, atomics, locks, options, strutils]
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
    timeoutMs*: int   ## socket operation timeout (0 = block forever)
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
# Socket receive timeout helper (SO_RCVTIMEO)
# ---------------------------------------------------------------------------

proc setSocketRecvTimeout(sock: Socket, timeoutMs: int) {.raises: [].} =
  ## Set SO_RCVTIMEO on the socket so that blocking posix.recv calls
  ## will time out after timeoutMs milliseconds (0 = no timeout).
  if timeoutMs <= 0: return
  var tv: Timeval
  tv.tv_sec = Time(timeoutMs div 1000)
  tv.tv_usec = Suseconds((timeoutMs mod 1000) * 1000)
  let rc = setsockopt(sock.getFd(), SOL_SOCKET, SO_RCVTIMEO,
                      addr tv, SockLen(sizeof(tv)))
  if rc != 0:
    try: clientLog("Warning: setsockopt(SO_RCVTIMEO) failed")
    except CatchableError: discard

# ---------------------------------------------------------------------------
# Low-level recv — truly blocking, respects SO_RCVTIMEO
# ---------------------------------------------------------------------------

proc recvExact(sock: Socket, buf: var string, size: int): int {.gcsafe,
    raises: [].} =
  ## Read exactly `size` bytes into buf using posix.recv.
  ## Returns the number of bytes actually read (< size means closed/error).
  buf.setLen(size)
  var total = 0
  while total < size:
    let got = posix.recv(sock.getFd(), addr buf[total], size - total, 0)
    if got <= 0:
      buf.setLen(total)
      return total
    total += got
  total

proc recvN(sock: Socket, n: int): string {.gcsafe, raises: [].} =
  ## Read exactly n bytes; returns shorter string on EOF/error.
  result = newString(n)
  let got = recvExact(sock, result, n)
  result.setLen(got)

# ---------------------------------------------------------------------------
# Low-level send — returns PResult (void success)
# ---------------------------------------------------------------------------

proc sendRaw(client: ProtocolClient, data: string): PResult {.gcsafe, raises: [].} =
  acquire(client.writeMu)
  try:
    client.socket.send(data)
    result = pOk()
  except CatchableError as e:
    result = pErr(newProtocolError(peInternal, "send failed: " & e.msg))
  finally:
    release(client.writeMu)

proc sendPayload(client: ProtocolClient, payload: string,
    requestId: uint32, flags: uint16 = 0): PResult {.gcsafe, raises: [].} =
  sendRaw(client, encodeFrame(payload, requestId, flags))

# ---------------------------------------------------------------------------
# Connect and handshake
# ---------------------------------------------------------------------------

proc connect*(client: ProtocolClient): PResult {.raises: [].} =
  if client.connected.load(): return pOk()

  try:
    client.socket = newSocket()
    client.socket.connect(client.config.host, Port(client.config.port))
    client.socket.setLingerZero()
  except CatchableError as e:
    return pErr(newProtocolError(peInternal, "connect failed: " & e.msg))

  # Apply SO_RCVTIMEO so all blocking posix.recv calls time out correctly
  setSocketRecvTimeout(client.socket, client.config.timeoutMs)

  # --- Read server greeting in streaming fashion ---
  # Greeting wire layout:
  #   4 bytes magic
  #   2 bytes version
  #   4 bytes features
  #   1 byte auth-method count  ← variable; read this to know how many more bytes
  #   N bytes auth methods
  #   2 bytes serverId
  #   8 bytes clusterId
  # Read the fixed prefix (4+2+4+1 = 11 bytes) first.
  let greetPrefix = recvN(client.socket, 11)
  if greetPrefix.len != 11:
    return pErr(newProtocolError(peInternal, "server closed during greeting"))
  let authCount = int(uint8(greetPrefix[10]))
  # Then read the variable part + suffix (authCount + 2 + 8 = authCount+10).
  let greetRest = recvN(client.socket, authCount + 10)
  if greetRest.len != authCount + 10:
    return pErr(newProtocolError(peInternal,
        "server closed during greeting (rest)"))
  let greetBuf = greetPrefix & greetRest

  let greetR = decodeGreeting(greetBuf)
  if greetR.isErr: return pErr(greetR.error)
  let greet = greetR.value

  if greet.version != PROTOCOL_VERSION_1:
    return pErr(newProtocolError(peVersionMismatch,
      &"server version {greet.version} != {PROTOCOL_VERSION_1}"))

  # --- Send client handshake ---
  let hs = ClientHandshake(
    version: PROTOCOL_VERSION_1,
    features: FeatPipelining or FeatTransactions,
    authType: uint8(client.config.authMethod),
    authData: client.config.authData,
    clientId: client.config.clientId,
  )
  let sr = sendRaw(client, encodeClientHandshake(hs))
  if sr.isErr: return sr

  # --- Read handshake response in streaming fashion ---
  # Response wire layout:
  #   1 byte status
  #   4 bytes features
  #   1 byte serverName length (uint8)
  #   N bytes serverName
  #   if status != OK: 2 bytes errLen + errLen bytes errorMessage
  # Read fixed prefix: 1+4+1 = 6 bytes
  let rspPrefix = recvN(client.socket, 6)
  if rspPrefix.len != 6:
    return pErr(newProtocolError(peInternal, "server closed during handshake"))
  let rspStatus = uint8(rspPrefix[0])
  let nameLen = int(uint8(rspPrefix[5]))
  # Read serverName
  let rspName = if nameLen > 0: recvN(client.socket, nameLen)
                else: ""
  if rspName.len != nameLen:
    return pErr(newProtocolError(peInternal,
        "server closed during handshake (name)"))
  # If error: read error message
  var rspErrPart = ""
  if rspStatus != HandshakeOK:
    let errLenBuf = recvN(client.socket, 2)
    if errLenBuf.len != 2:
      return pErr(newProtocolError(peInternal,
          "server closed during handshake (errlen)"))
    let errLen = int((uint16(errLenBuf[0]) shl 8) or uint16(errLenBuf[1]))
    rspErrPart = recvN(client.socket, errLen)
  let rspBuf = rspPrefix & rspName & rspErrPart

  let rspR = decodeHandshakeResponse(rspBuf)
  if rspR.isErr: return pErr(rspR.error)
  let rsp = rspR.value

  if rsp.status != HandshakeOK:
    return pErr(newProtocolError(peAuthFailed,
      "handshake rejected: " & rsp.errorMessage))

  client.negotiatedFeatures = rsp.features
  client.connected.store(true)
  clientLog("connected to " & client.config.host & ":" & $client.config.port)
  pOk()

proc disconnect*(client: ProtocolClient) {.gcsafe, raises: [].} =
  if not client.connected.load(): return
  client.connected.store(false)
  try: client.socket.close() except CatchableError: discard

# ---------------------------------------------------------------------------
# Read one response frame from socket (blocking, SO_RCVTIMEO respected)
# ---------------------------------------------------------------------------

proc readOneFrame(client: ProtocolClient): Result[Frame,
    ProtocolError] {.gcsafe, raises: [].} =
  var hdrBuf = newString(FRAME_HEADER_SIZE)
  let hn = recvExact(client.socket, hdrBuf, FRAME_HEADER_SIZE)
  if hn != FRAME_HEADER_SIZE:
    return peErr(newProtocolError(peInvalidFrame,
      &"short header: got {hn}, expected {FRAME_HEADER_SIZE}"))

  var pos = 0
  let hdrR = decodeFrameHeader(hdrBuf, pos)
  if hdrR.isErr: return peErr(hdrR.error)
  let hdr = hdrR.value

  var payload = newString(int(hdr.payloadLen))
  if hdr.payloadLen > 0:
    let pn = recvExact(client.socket, payload, int(hdr.payloadLen))
    if pn != int(hdr.payloadLen):
      return peErr(newProtocolError(peInvalidFrame,
        &"short payload: got {pn}, expected {hdr.payloadLen}"))

  let computed = computeCRC16(payload)
  if computed != hdr.checksum:
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
  if sr.isErr: return peErr(sr.error)

  let frameR = readOneFrame(client)
  if frameR.isErr: return peErr(frameR.error)

  let f = frameR.value
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
  discard sendRaw(client, encodeFrame(encodeCloseRequest(reason), reqId))
  disconnect(client)

# ---------------------------------------------------------------------------
# KV convenience procs (Phase 2)
# ---------------------------------------------------------------------------

# Forward declaration for streaming scan result type
type
  # Callback type for multi-group scan - starts next group's stream
  NextGroupCallback* = proc(): Result[StreamingScanClient,
      ProtocolError] {.closure, gcsafe, raises: [].}

  StreamingScanClient* = ref object
    ## Client-side streaming scan state - reads multiple frames from server.
    ## Supports both single-group (direct) and multi-group (aggregated) modes.

    # Single-group mode fields
    client*: ProtocolClient
    reqFlags*: uint8
    streamId*: uint32
    hasMore*: bool
    exhausted*: bool
    currentFrame*: ScanResponseFrame
    framePos*: int
    totalReceived*: int
    error*: Option[ProtocolError]

    # Multi-group mode fields (when multiGroupMode is true)
    multiGroupMode*: bool
      ## True when aggregating across multiple Raft groups
    currentGroupStream*: StreamingScanClient
      ## Current group's stream (nil when between groups)
    nextGroupCallback*: NextGroupCallback
      ## Callback to start next group's stream
    pairsSent*: int
      ## Count of pairs returned (for limit enforcement in multi-group mode)
    scanLimit*: uint32
      ## Original limit for multi-group scan (0 = no limit)

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
    flags: uint8 = 0, txnId: TransactionID = zeroTransactionID(),
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
  result.streamId = client.nextRequestId.fetchAdd(1)
  result.hasMore = false
  result.exhausted = false
  result.currentFrame = ScanResponseFrame()
  result.framePos = 0
  result.totalReceived = 0
  result.error = none(ProtocolError)
  result.multiGroupMode = false
  result.currentGroupStream = nil
  result.nextGroupCallback = nil
  result.pairsSent = 0
  result.scanLimit = 0

proc newMultiGroupStreamingScanClient*(firstStream: StreamingScanClient,
    nextGroupCallback: NextGroupCallback,
    limit: uint32 = 0): StreamingScanClient =
  ## Create a new streaming scan client for multi-group mode.
  ## firstStream: the initial group's streaming scan (already started)
  ## nextGroupCallback: callback to start the next group's stream when current exhausted
  ## limit: total limit across all groups (0 = no limit)
  new(result)
  result.client = nil # Not used in multi-group mode
  result.streamId = firstStream.streamId # Use first stream's ID for tracking
  result.hasMore = false # Not used in multi-group mode
  result.exhausted = false
  result.currentFrame = ScanResponseFrame()
  result.framePos = 0
  result.totalReceived = 0
  result.error = none(ProtocolError)
  result.multiGroupMode = true
  result.currentGroupStream = firstStream
  result.nextGroupCallback = nextGroupCallback
  result.pairsSent = 0
  result.scanLimit = limit

proc startStreamScan*(ss: StreamingScanClient, startKey: string = "",
    endKey: string = "", limit: uint32 = 0,
    chunkSize: uint32 = 0,
    flags: uint8 = 0, txnId: TransactionID = zeroTransactionID(),
    readTimestamp: uint64 = 0,
    groupId: GroupID = ZeroGroupID(),
    filter: Option[WireFilterExpr] = none(WireFilterExpr)): Result[
        ScanResponseFrame, ProtocolError] {.
    gcsafe, raises: [].} =
  ## Start a streaming scan. Sends the initial request and receives the first frame.
  ## Returns the first frame of results.
  ## chunkSize: number of items per frame (0 = DEFAULT_SCAN_CHUNK_SIZE)
  ## filter: optional server-side filter for reducing network traffic
  ## Note: Only valid for single-group mode (multiGroupMode must be false).
  if ss.multiGroupMode:
    return peErr(newProtocolError(peInternal,
        "startStreamScan not valid for multi-group mode"))
  if not ss.client.connected.load():
    return peErr(newProtocolError(peInternal, "not connected"))

  var actualFlags = flags or kvMsgs.ScanFlagStreaming
  if groupId != ZeroGroupID():
    actualFlags = actualFlags or kvMsgs.ScanFlagGroupRouted
  if filter.isSome:
    actualFlags = actualFlags or kvMsgs.ScanFlagHasFilter

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

  # Read next frame from server
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

proc nextPair*(ss: StreamingScanClient): Option[kvMsgs.ScanPair] {.gcsafe,
    raises: [].} =
  ## Get the next individual KV pair from the stream.
  ## Returns some(pair) if available, none() if exhausted.
  ## Automatically fetches next frame when current frame is exhausted.
  ## In multi-group mode, automatically switches to next group when current exhausted.
  ##
  ## Important: For single-group mode, we check currentFrame.pairs BEFORE checking
  ## exhausted, because a stream can be marked exhausted but still have pairs
  ## in currentFrame that need to be consumed.

  # Multi-group mode: delegate to current group stream with limit tracking
  if ss.multiGroupMode:
    if ss.exhausted:
      return none(kvMsgs.ScanPair)

    # Check limit
    if ss.scanLimit > 0 and ss.pairsSent >= int(ss.scanLimit):
      ss.exhausted = true
      return none(kvMsgs.ScanPair)

    # If no current stream, try to start one via callback
    if ss.currentGroupStream == nil:
      if ss.nextGroupCallback == nil:
        ss.exhausted = true
        return none(kvMsgs.ScanPair)
      let nextStreamR = ss.nextGroupCallback()
      if nextStreamR.isErr:
        ss.error = some(nextStreamR.error)
        ss.exhausted = true
        return none(kvMsgs.ScanPair)
      ss.currentGroupStream = nextStreamR.value

    # Get next pair from current group stream
    let pairOpt = ss.currentGroupStream.nextPair()
    if pairOpt.isSome:
      inc ss.totalReceived
      inc ss.pairsSent
      return pairOpt

    # Current group stream exhausted - try next group
    ss.currentGroupStream.closeStream()
    if ss.nextGroupCallback == nil:
      ss.exhausted = true
      return none(kvMsgs.ScanPair)

    # Check if current stream had an error
    if ss.currentGroupStream.error.isSome:
      ss.error = ss.currentGroupStream.error
      ss.exhausted = true
      return none(kvMsgs.ScanPair)

    # Try next group
    var attempts = 0
    while ss.nextGroupCallback != nil and attempts < 100:
      let nextStreamR = ss.nextGroupCallback()
      if nextStreamR.isErr:
        ss.error = some(nextStreamR.error)
        ss.exhausted = true
        return none(kvMsgs.ScanPair)

      ss.currentGroupStream = nextStreamR.value
      inc attempts

      # Get first pair from new stream
      let newPairOpt = ss.currentGroupStream.nextPair()
      if newPairOpt.isSome:
        inc ss.totalReceived
        inc ss.pairsSent
        return newPairOpt

      # This group also empty, close and continue
      ss.currentGroupStream.closeStream()

    # All groups exhausted
    ss.exhausted = true
    return none(kvMsgs.ScanPair)

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
  ## For single-group mode, we can still return pairs from currentFrame even if exhausted.
  ## For multi-group mode, exhausted means no more groups to try.

  # Multi-group mode
  if ss.multiGroupMode:
    if ss.exhausted:
      return false
    # Check limit
    if ss.scanLimit > 0 and ss.pairsSent >= int(ss.scanLimit):
      return false

    # If no current stream but have callback, we can potentially get more
    if ss.currentGroupStream == nil:
      let hasCallback = ss.nextGroupCallback != nil
      return hasCallback

    # Check if current stream has pairs
    if ss.currentGroupStream.hasNext():
      return true

    # Current stream exhausted - check if we have more groups
    let hasMoreGroups = ss.nextGroupCallback != nil
    return hasMoreGroups

  # Single-group mode - check for pairs in current frame first
  # We can return pairs from currentFrame even if stream is exhausted
  if ss.framePos < ss.currentFrame.pairs.len:
    return true

  # No more pairs in current frame - check if server has more
  if ss.exhausted:
    return false
  return ss.hasMore

proc closeStream*(ss: StreamingScanClient) {.gcsafe, raises: [].} =
  ## Close the streaming scan and mark it exhausted.
  ## Breaks reference cycles by clearing the nextGroupCallback closure.
  ss.exhausted = true
  ss.hasMore = false
  # In multi-group mode, close current group stream and clear callback to break cycles
  if ss.multiGroupMode:
    if ss.currentGroupStream != nil:
      ss.currentGroupStream.closeStream()
      ss.currentGroupStream = nil
    # Clear the callback to break the closure cycle (prevents ORC crash on cleanup)
    ss.nextGroupCallback = nil

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
    flags: uint8 = 0, txnId: TransactionID = zeroTransactionID(),
    readTimestamp: uint64 = 0,
    groupId: GroupID = ZeroGroupID(),
    filter: Option[WireFilterExpr] = none(WireFilterExpr)): Result[
        StreamingScanClient, ProtocolError] {.
    gcsafe, raises: [].} =
  ## Start a streaming scan and return a StreamingScanClient for iteration.
  ## Use ss.nextPair() to get individual pairs, or ss.consumeStreamScan() to
  ## get all results as a sequence.
  ## filter: optional server-side filter for reducing network traffic
  let ss = newStreamingScanClient(client)
  let firstFrameR = ss.startStreamScan(startKey, endKey, limit, chunkSize, flags,
                                        txnId, readTimestamp, groupId, filter)
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
