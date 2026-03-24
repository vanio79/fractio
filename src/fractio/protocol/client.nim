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

import std/[net, strformat, atomics, locks]
import posix
import ../utils/socket_utils
import ./types
import ./codec
import ./frame
import ./handshake
import ./messages/core
import ./messages/kv
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
      let catR = readUint8(f.payload, pos)
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

proc kvGet*(client: ProtocolClient, key: string,
    flags: uint8 = 0, txnId: uint64 = 0,
    readTimestamp: uint64 = 0): Result[GetResponse, ProtocolError] {.gcsafe,
    raises: [].} =
  ## Send a Get request and return the decoded response.
  let req = GetRequest(flags: flags, txnId: txnId,
                       readTimestamp: readTimestamp, key: key)
  let r = client.send(encodeGetRequest(req))
  if r.isErr: return peErr(r.error)
  decodeGetResponse(r.value.payload)

proc kvPut*(client: ProtocolClient, key: string, value: string,
    flags: uint8 = 0, txnId: uint64 = 0,
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
    txnId: uint64 = 0): Result[DeleteResponse, ProtocolError] {.gcsafe,
    raises: [].} =
  ## Send a Delete request and return the decoded response.
  let req = DeleteRequest(flags: flags, txnId: txnId, key: key)
  let r = client.send(encodeDeleteRequest(req))
  if r.isErr: return peErr(r.error)
  decodeDeleteResponse(r.value.payload)

proc kvGetInGroup*(client: ProtocolClient, key: string,
    groupId: uint64,
    flags: uint8 = 0, txnId: uint64 = 0,
    readTimestamp: uint64 = 0): Result[GetResponse, ProtocolError] {.gcsafe,
    raises: [].} =
  ## Send a Get request routed to a specific Raft group.
  let req = GetRequest(flags: flags, txnId: txnId,
                       readTimestamp: readTimestamp, key: key,
                       groupId: groupId)
  let r = client.send(encodeGetRequest(req))
  if r.isErr: return peErr(r.error)
  decodeGetResponse(r.value.payload)

proc kvRawPutInGroup*(client: ProtocolClient, key: string, value: string,
    groupId: uint64,
    flags: uint8 = 0, txnId: uint64 = 0,
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
    groupId: uint64,
    flags: uint8 = 0, txnId: uint64 = 0,
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
    groupId: uint64,
    flags: uint8 = 0,
    txnId: uint64 = 0): Result[DeleteResponse, ProtocolError] {.gcsafe,
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
    flags: uint8 = 0, txnId: uint64 = 0,
    readTimestamp: uint64 = 0): Result[ScanResponseFrame, ProtocolError] {.
    gcsafe, raises: [].} =
  ## Send a Scan request and return the first (and only, in Phase 2) response frame.
  let req = ScanRequest(
    flags: flags,
    txnId: txnId,
    readTimestamp: readTimestamp,
    startKey: startKey,
    endKey: endKey,
    limit: limit,
  )
  let r = client.send(encodeScanRequest(req))
  if r.isErr: return peErr(r.error)
  decodeScanResponseFrame(r.value.payload, flags)

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
    txnId: uint64): Result[txnMsgs.CommitTxnResponse,
    ProtocolError] {.gcsafe, raises: [].} =
  ## Commit txnId. Check .status for TxnCommitOK / conflict / timeout.
  let req = txnMsgs.CommitTxnRequest(txnId: txnId)
  let r = client.send(txnMsgs.encodeCommitTxnRequest(req))
  if r.isErr: return peErr(r.error)
  txnMsgs.decodeCommitTxnResponse(r.value.payload)

proc rollbackTxn*(client: ProtocolClient,
    txnId: uint64): Result[txnMsgs.RollbackTxnResponse,
    ProtocolError] {.gcsafe, raises: [].} =
  ## Roll back txnId.
  let req = txnMsgs.RollbackTxnRequest(txnId: txnId)
  let r = client.send(txnMsgs.encodeRollbackTxnRequest(req))
  if r.isErr: return peErr(r.error)
  txnMsgs.decodeRollbackTxnResponse(r.value.payload)

proc txnStatus*(client: ProtocolClient,
    txnId: uint64): Result[txnMsgs.TxnStatusResponse,
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
