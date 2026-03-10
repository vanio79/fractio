# Fractio protocol server — Phase 1 + Phase 2 + Phase 3 + Phase 4 + Phase 5:
#   Core, KV, Transactions, Admin/Metrics, Authentication, Raft Integration.
#
# Thread model:
#   - One acceptor thread (server.start spawns acceptLoop)
#   - One reader thread per client connection (clientLoop)
#   - Handlers are called on the reader thread; they must be gcsafe.
#
# Phase 5 changes:
#   - ProtocolServer.raftStore: optional RaftKVStoreExt field.
#     When set, all KV reads/writes go through Raft consensus.
#     When nil (default), the Phase 2 in-memory KVStore is used (backward compat).
#   - NOT_LEADER responses surface as ErrNotLeader wire errors.
#
# All shared mutable state is protected by Locks.

import std/[net, tables, strformat, strutils, times, atomics, locks, options, algorithm, os]
import posix as posixSys
import ./types
import ./codec as protoCodec
import ./frame
import ./handshake
import ./auth
import ./messages/core
import ./messages/kv
import ./messages/txn as txnMsgs
import ./messages/admin as adminMsgs
import ./messages/cluster as clusterMsgs
import ./txn_manager
import ./raft_store
import ../utils/logging
import ../distributed/sharedtimer
import ../distributed/raft/multigroup_coordinator
import ../distributed/raft/multigroup_transport
import ../distributed/raft/multigroup_types
import ../distributed/range/types as rangeTypes
import ../distributed/sharedtimer/udptransport as udpXport
import ../distributed/meta/system_tables
import std/json

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
    serverVersion*: string      ## reported in ServerInfo; default "1.0.0"
    clusterName*: string        ## reported in Health; default "fractio"
                                ## SharedTimer (P2P time synchronization) config:
    sharedTimerEnabled*: bool ## when true, create SharedTimer and wire into TransactionManager
    sharedTimerNodeId*: string  ## human-readable node ID for SharedTimer (default: serverName)
    sharedTimerNumericNodeId*: uint16 ## 10-bit numeric node ID for Snowflake transaction IDs (default: serverId)
    sharedTimerPeers*: seq[PeerConfig] ## peer nodes for NTP-style clock sync (empty = single-node mode)
    dataDir*: string ## directory for persistent state (registry, etc.); "" = no persistence
    webPort*: int   ## port for the HTTP management dashboard; 0 = disabled

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
    serverVersion: "1.0.0",
    clusterName: "fractio",
    sharedTimerEnabled: false,
    sharedTimerNodeId: "",
    sharedTimerNumericNodeId: 0,
    sharedTimerPeers: @[],
    dataDir: "",
    webPort: 0,
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
# In-memory KV store (Phase 2 — no persistence, no Raft integration yet)
# ---------------------------------------------------------------------------

type
  KVEntry* = object
    value*: string
    version*: uint64
    timestamp*: uint64

  KVStore* = ref object
    data*: Table[string, KVEntry]
    mu*: Lock
    nextVersion*: Atomic[uint64]

proc newKVStore*(): KVStore =
  result = KVStore(data: initTable[string, KVEntry]())
  initLock(result.mu)
  result.nextVersion.store(1)

proc kvGet*(store: KVStore, key: string): Option[KVEntry] {.gcsafe, raises: [].} =
  acquire(store.mu)
  defer: release(store.mu)
  let entry = store.data.getOrDefault(key)
  if entry.version > 0:
    some(entry)
  else:
    none(KVEntry)

proc kvPut*(store: KVStore, key: string,
    value: string): KVEntry {.gcsafe, raises: [].} =
  let ver = store.nextVersion.fetchAdd(1)
  let ts = uint64(getTime().toUnixFloat() * 1_000_000)
  let entry = KVEntry(value: value, version: ver, timestamp: ts)
  acquire(store.mu)
  defer: release(store.mu)
  store.data[key] = entry
  entry

proc kvDelete*(store: KVStore, key: string): Option[KVEntry] {.gcsafe, raises: [].} =
  acquire(store.mu)
  defer: release(store.mu)
  let entry = store.data.getOrDefault(key)
  if entry.version > 0:
    store.data.del(key)
    some(entry)
  else:
    none(KVEntry)

proc kvScan*(store: KVStore, startKey: string, endKey: string,
    limit: uint32): seq[(string, KVEntry)] {.gcsafe, raises: [].} =
  acquire(store.mu)
  defer: release(store.mu)
  var pairs: seq[(string, KVEntry)] = @[]
  for k, v in store.data:
    let afterStart = startKey.len == 0 or k >= startKey
    let beforeEnd = endKey.len == 0 or k < endKey
    if afterStart and beforeEnd:
      pairs.add((k, v))
  # sort pairs by key for deterministic order
  algorithm.sort(pairs, proc(a, b: (string, KVEntry)): int = cmp(a[0], b[0]))
  if limit > 0 and pairs.len > int(limit):
    pairs.setLen(int(limit))
  pairs

proc kvLen*(store: KVStore): int {.gcsafe, raises: [].} =
  acquire(store.mu)
  defer: release(store.mu)
  store.data.len

# ---------------------------------------------------------------------------
# Cluster node registry (Phase 8 — in-memory, protected by a Lock)
# ---------------------------------------------------------------------------

type
  ClusterNodeEntry* = object
    nodeId*: uint16
    host*: string
    raftPort*: uint16
    clientPort*: uint16
    webPort*: uint16
    status*: uint8 ## clusterMsgs.NodeStatus* constant

  NodeRegistry* = ref object
    nodes*: Table[uint16, ClusterNodeEntry]
    mu*: Lock
    ## Rebalance operation counters (atomic for lock-free reads)
    rebalancePending*: Atomic[uint32]
    rebalanceInProgress*: Atomic[uint32]
    rebalanceCompleted*: Atomic[uint32]
    rebalanceFailed*: Atomic[uint32]

proc newNodeRegistry*(): NodeRegistry =
  result = NodeRegistry(nodes: initTable[uint16, ClusterNodeEntry]())
  initLock(result.mu)
  result.rebalancePending.store(0)
  result.rebalanceInProgress.store(0)
  result.rebalanceCompleted.store(0)
  result.rebalanceFailed.store(0)

proc addNode*(reg: NodeRegistry, entry: ClusterNodeEntry) {.gcsafe, raises: [].} =
  acquire(reg.mu)
  defer: release(reg.mu)
  reg.nodes[entry.nodeId] = entry

proc removeNode*(reg: NodeRegistry, nodeId: uint16): bool {.gcsafe, raises: [].} =
  acquire(reg.mu)
  defer: release(reg.mu)
  if reg.nodes.hasKey(nodeId):
    reg.nodes.del(nodeId)
    return true
  false

proc drainNode*(reg: NodeRegistry, nodeId: uint16): bool {.gcsafe, raises: [].} =
  acquire(reg.mu)
  defer: release(reg.mu)
  reg.nodes.withValue(nodeId, entry):
    entry.status = clusterMsgs.NodeStatusDraining
    return true
  return false

proc listNodes*(reg: NodeRegistry): seq[ClusterNodeEntry] {.gcsafe, raises: [].} =
  acquire(reg.mu)
  defer: release(reg.mu)
  for _, v in reg.nodes:
    result.add(v)

proc saveRegistry*(reg: NodeRegistry, path: string) {.gcsafe, raises: [].} =
  ## Persist registry to disk using the ListNodes wire format.
  try:
    let entries = reg.listNodes()
    var nodes = newSeq[clusterMsgs.NodeInfo](entries.len)
    for i, e in entries:
      nodes[i] = clusterMsgs.NodeInfo(
        nodeId: e.nodeId,
        host: e.host,
        raftPort: e.raftPort,
        clientPort: e.clientPort,
        status: e.status,
      )
    let data = clusterMsgs.encodeListNodesResponse(
      clusterMsgs.ListNodesResponse(nodes: nodes))
    writeFile(path, data)
  except CatchableError: discard
  except Exception: discard

proc loadRegistry*(path: string): NodeRegistry {.gcsafe, raises: [].} =
  ## Load registry from disk; returns empty registry on missing or corrupt file.
  result = newNodeRegistry()
  try:
    if not fileExists(path): return
    let data = readFile(path)
    let respR = clusterMsgs.decodeListNodesResponse(data)
    if respR.isErr: return
    for n in respR.value.nodes:
      result.nodes[n.nodeId] = ClusterNodeEntry(
        nodeId: n.nodeId,
        host: n.host,
        raftPort: n.raftPort,
        clientPort: n.clientPort,
        status: n.status,
      )
  except CatchableError: discard
  except Exception: discard

# ---------------------------------------------------------------------------
# Metrics counters (Phase 4)
# ---------------------------------------------------------------------------

type
  ServerMetrics* = ref object
    requestsTotal*: Atomic[uint64]
    requestsOK*: Atomic[uint64]
    requestsErr*: Atomic[uint64]
    bytesIn*: Atomic[uint64]
    bytesOut*: Atomic[uint64]
    kvGets*: Atomic[uint64]
    kvPuts*: Atomic[uint64]
    kvDeletes*: Atomic[uint64]
    committedTxns*: Atomic[uint64]
    abortedTxns*: Atomic[uint64]

proc newServerMetrics*(): ServerMetrics =
  result = ServerMetrics()
  result.requestsTotal.store(0)
  result.requestsOK.store(0)
  result.requestsErr.store(0)
  result.bytesIn.store(0)
  result.bytesOut.store(0)
  result.kvGets.store(0)
  result.kvPuts.store(0)
  result.kvDeletes.store(0)
  result.committedTxns.store(0)
  result.abortedTxns.store(0)

proc snapshot*(m: ServerMetrics): adminMsgs.MetricsResponse {.gcsafe,
    raises: [].} =
  adminMsgs.MetricsResponse(
    requestsTotal: m.requestsTotal.load(),
    requestsOK: m.requestsOK.load(),
    requestsErr: m.requestsErr.load(),
    bytesIn: m.bytesIn.load(),
    bytesOut: m.bytesOut.load(),
    kvGets: m.kvGets.load(),
    kvPuts: m.kvPuts.load(),
    kvDeletes: m.kvDeletes.load(),
    activeTxns: 0'u32, # filled in from txnMgr at call site
    committedTxns: m.committedTxns.load(),
    abortedTxns: m.abortedTxns.load(),
  )

proc reset*(m: ServerMetrics) {.gcsafe, raises: [].} =
  m.requestsTotal.store(0)
  m.requestsOK.store(0)
  m.requestsErr.store(0)
  m.bytesIn.store(0)
  m.bytesOut.store(0)
  m.kvGets.store(0)
  m.kvPuts.store(0)
  m.kvDeletes.store(0)
  m.committedTxns.store(0)
  m.abortedTxns.store(0)

# ---------------------------------------------------------------------------
# Protocol server
# ---------------------------------------------------------------------------

type
  ProtocolServer* = ref object
    config*: ServerConfig
    logger*: Logger
    running*: Atomic[bool]
    startedAt*: int64             ## Unix seconds; set in start()
    clients*: Table[uint32, ClientConnection]
    clientsMu*: Lock
    handlers*: Table[int, MessageHandler]
    handlersMu*: Lock
    nextClientId*: Atomic[uint32]
    serverFeatures*: uint32
    kvStore*: KVStore             ## Phase 2: in-memory store (fallback when raftStore is nil)
    raftStore*: RaftKVStoreExt    ## Phase 5: Raft-backed KV store (nil = use kvStore)
    txnMgr*: TransactionManager   ## Phase 3: transaction manager
    metrics*: ServerMetrics       ## Phase 4: request counters
    authenticator*: Authenticator ## Phase 4: auth validator
    sharedTimer*: SharedTimer     ## Phase 7: P2P clock sync (nil when disabled)
    nodeRegistry*: NodeRegistry   ## Phase 8: in-memory cluster node registry
    raftCoord*: MultiRaftCoordinator  ## lifecycle owner; nil until setupRaftNode
    raftTransport*: RaftGroupTransport ## kept alive for ORC safety; nil in single-node

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
  let reg =
    if config.dataDir != "":
      loadRegistry(config.dataDir / "node_registry.dat")
    else:
      newNodeRegistry()
  result = ProtocolServer(
    config: config,
    logger: newLogger("protocol.server"),
    clients: initTable[uint32, ClientConnection](),
    handlers: initTable[int, MessageHandler](),
    kvStore: newKVStore(),
    txnMgr: newTransactionManager(),
    metrics: newServerMetrics(),
    authenticator: newAuthenticator(config.authMethod),
    nodeRegistry: reg,
    startedAt: getTime().toUnix(),
  )
  initLock(result.clientsMu)
  initLock(result.handlersMu)
  result.running.store(false)
  result.nextClientId.store(1)
  result.serverFeatures = FeatPipelining or FeatTransactions or FeatAsync
  if config.tlsEnabled:
    result.serverFeatures = result.serverFeatures or FeatTLS

  # Phase 7: wire SharedTimer into TransactionManager when enabled
  if config.sharedTimerEnabled:
    let nodeId = if config.sharedTimerNodeId.len > 0: config.sharedTimerNodeId
                 else: config.serverName
    let numericId = if config.sharedTimerNumericNodeId >
        0: config.sharedTimerNumericNodeId
                    else: config.serverId
    let timer = newSharedTimer(
      nodeId = nodeId,
      numericNodeId = numericId,
      peers = config.sharedTimerPeers,
      logger = result.logger,
    )
    result.sharedTimer = timer
    result.txnMgr.setTimeProvider(timer)

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
# Handshake (Phase 4: auth wired in)
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

    # Phase 4: authenticate when server requires it
    if server.config.authMethod != amNone:
      if not server.authenticator.authenticate(hs.authType, hsAuthData):
        conn.socket.send(encodeHandshakeResponse(HandshakeResponse(
          status: HandshakeError, errorMessage: "authentication failed")))
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
# Built-in KV message handlers (Get, Put, Delete, Batch, Scan)
# ---------------------------------------------------------------------------

proc handleBuiltinKV(server: ProtocolServer, conn: ClientConnection,
    requestId: uint32, flags: uint16,
    payload: string) {.gcsafe, raises: [].} =
  if payload.len < 2: return
  let typeVal = (uint16(payload[0]) shl 8) or uint16(payload[1])

  case typeVal

  of uint16(mtGet):
    discard server.metrics.kvGets.fetchAdd(1)
    let reqR = decodeGetRequest(payload)
    if reqR.isErr:
      sendError(conn, requestId, ErrProtocol, ErrCatProtocol, $reqR.error)
      return
    let req = reqR.value
    if req.key.len == 0 or req.key.len > int(server.config.maxKeyBytes):
      sendError(conn, requestId, ErrProtocol, ErrCatKV, "invalid key length")
      return
    # Transactional read: register the key as read by the transaction
    if req.txnId != 0:
      let rr = server.txnMgr.recordRead(req.txnId, req.key)
      if rr.isErr:
        sendError(conn, requestId, ErrTxnAborted, ErrCatTransaction,
          "txn expired or not found: " & rr.error.msg)
        return

    var resp: GetResponse
    if not server.raftStore.isNil:
      # Phase 5: Raft-backed read.
      # If the request is inside a transaction, use raftGetForTxn so that
      # writes buffered as intents by this txn are visible (reads-your-own-writes).
      let rr = if req.txnId != 0:
                 server.raftStore.raftGetForTxn(req.txnId, req.key)
               else:
                 server.raftStore.raftGet(req.key)
      if not rr.isOk:
        if rr.error.kind == rseNotLeader:
          sendError(conn, requestId, ErrNotLeader, ErrCatKV,
            "not the leader for key: " & req.key)
        else:
          sendError(conn, requestId, ErrInternal, ErrCatKV, rr.error.msg)
        return
      let entryOpt = rr.value
      if entryOpt.isSome:
        let entry = entryOpt.get()
        resp = GetResponse(
          found: true,
          hasTimestamp: (req.flags and GetFlagIncludeTimestamp) != 0,
          hasVersion: (req.flags and GetFlagIncludeVersion) != 0,
          timestamp: entry.timestamp,
          version: entry.version,
          value: entry.value,
        )
      else:
        resp = GetResponse(found: false)
    else:
      # Phase 2 fallback: in-memory read
      let entryOpt = server.kvStore.kvGet(req.key)
      if entryOpt.isSome:
        let entry = entryOpt.get()
        resp = GetResponse(
          found: true,
          hasTimestamp: (req.flags and GetFlagIncludeTimestamp) != 0,
          hasVersion: (req.flags and GetFlagIncludeVersion) != 0,
          timestamp: entry.timestamp,
          version: entry.version,
          value: entry.value,
        )
      else:
        resp = GetResponse(found: false)
    sendFrame(conn, encodeGetResponse(resp), requestId)

  of uint16(mtPut):
    discard server.metrics.kvPuts.fetchAdd(1)
    let reqR = decodePutRequest(payload)
    if reqR.isErr:
      sendError(conn, requestId, ErrProtocol, ErrCatProtocol, $reqR.error)
      return
    let req = reqR.value
    if req.key.len == 0 or req.key.len > int(server.config.maxKeyBytes):
      sendError(conn, requestId, ErrProtocol, ErrCatKV, "invalid key length")
      return
    if req.value.len > int(server.config.maxValueBytes):
      sendError(conn, requestId, ErrProtocol, ErrCatKV, "value too large")
      return
    # Transactional write: register the key as written by the transaction
    if req.txnId != 0:
      let wr = server.txnMgr.recordWrite(req.txnId, req.key)
      if wr.isErr:
        let resp = PutResponse(status: PutStatusTxnAborted,
                               timestamp: 0, version: 0)
        sendFrame(conn, encodePutResponse(resp), requestId)
        return

    if not server.raftStore.isNil:
      # Phase 5: Raft-backed write
      # Transactional writes: buffer as intent (no fsync); commit resolves later
      if req.txnId != 0:
        let wr = server.raftStore.raftBufferIntent(req.txnId, req.key, req.value)
        if not wr.isOk:
          sendError(conn, requestId, ErrInternal, ErrCatKV, wr.error.msg)
          return
        let ver = server.raftStore.nextVersion.fetchAdd(1)
        let ts = uint64(getTime().toUnixFloat() * 1_000_000_000)
        sendFrame(conn, encodePutResponse(PutResponse(
          status: PutStatusOK, timestamp: ts, version: ver)), requestId)
        return
      # Non-transactional: CAS check via raftGet
      if (req.flags and PutFlagCAS) != 0:
        let existR = server.raftStore.raftGet(req.key)
        let currentVer: uint64 = if existR.isOk and existR.value.isSome:
                                    existR.value.get().version
                                  else: 0'u64
        if currentVer != req.expectedVersion:
          sendFrame(conn, encodePutResponse(PutResponse(
            status: PutStatusCASFailed,
            timestamp: 0, version: 0)), requestId)
          return
      var prevEntry: Option[RaftKVEntry]
      if (req.flags and PutFlagReturnPrev) != 0:
        let pr = server.raftStore.raftGet(req.key)
        if pr.isOk: prevEntry = pr.value
      let wr = server.raftStore.raftPut(req.key, req.value)
      if not wr.isOk:
        if wr.error.kind == rseNotLeader:
          sendError(conn, requestId, ErrNotLeader, ErrCatKV,
            "not the leader for key: " & req.key)
        else:
          sendError(conn, requestId, ErrInternal, ErrCatKV, wr.error.msg)
        return
      let entry = wr.value
      var resp = PutResponse(
        status: PutStatusOK,
        timestamp: entry.timestamp,
        version: entry.version,
      )
      if (req.flags and PutFlagReturnPrev) != 0 and prevEntry.isSome:
        resp.hasPreviousValue = true
        resp.previousValue = prevEntry.get().value
      sendFrame(conn, encodePutResponse(resp), requestId)
    else:
      # Phase 2 fallback: in-memory write
      if (req.flags and PutFlagCAS) != 0:
        let existing = server.kvStore.kvGet(req.key)
        let currentVer: uint64 = if existing.isSome: existing.get().version else: 0
        if currentVer != req.expectedVersion:
          let resp = PutResponse(status: PutStatusCASFailed,
            timestamp: 0, version: 0)
          sendFrame(conn, encodePutResponse(resp), requestId)
          return
      var prevEntry: Option[KVEntry]
      if (req.flags and PutFlagReturnPrev) != 0:
        prevEntry = server.kvStore.kvGet(req.key)
      let entry = server.kvStore.kvPut(req.key, req.value)
      var resp = PutResponse(
        status: PutStatusOK,
        timestamp: entry.timestamp,
        version: entry.version,
      )
      if (req.flags and PutFlagReturnPrev) != 0 and prevEntry.isSome:
        resp.hasPreviousValue = true
        resp.previousValue = prevEntry.get().value
      sendFrame(conn, encodePutResponse(resp), requestId)

  of uint16(mtDelete):
    discard server.metrics.kvDeletes.fetchAdd(1)
    let reqR = decodeDeleteRequest(payload)
    if reqR.isErr:
      sendError(conn, requestId, ErrProtocol, ErrCatProtocol, $reqR.error)
      return
    let req = reqR.value
    if req.key.len == 0 or req.key.len > int(server.config.maxKeyBytes):
      sendError(conn, requestId, ErrProtocol, ErrCatKV, "invalid key length")
      return
    # Transactional write: register the key as written by the transaction
    if req.txnId != 0:
      let wr = server.txnMgr.recordWrite(req.txnId, req.key)
      if wr.isErr:
        let resp = DeleteResponse(status: DelStatusTxnAborted)
        sendFrame(conn, encodeDeleteResponse(resp), requestId)
        return

    if not server.raftStore.isNil:
      # Phase 5: Raft-backed delete
      # Transactional deletes: buffer as intent deletion (no fsync)
      if req.txnId != 0:
        let dr = server.raftStore.raftDeleteIntent(req.txnId, req.key)
        if not dr.isOk:
          sendError(conn, requestId, ErrInternal, ErrCatKV, dr.error.msg)
          return
        sendFrame(conn, encodeDeleteResponse(DeleteResponse(
          status: DelStatusDeleted)), requestId)
        return
      # Non-transactional delete
      let dr = server.raftStore.raftDelete(req.key)
      if not dr.isOk:
        if dr.error.kind == rseNotLeader:
          sendError(conn, requestId, ErrNotLeader, ErrCatKV,
            "not the leader for key: " & req.key)
        else:
          sendError(conn, requestId, ErrInternal, ErrCatKV, dr.error.msg)
        return
      var resp: DeleteResponse
      if dr.value.isNone:
        resp = DeleteResponse(status: DelStatusNotFound)
      else:
        resp = DeleteResponse(status: DelStatusDeleted)
        if (req.flags and DelFlagReturnPrev) != 0:
          resp.hasPreviousValue = true
          resp.previousValue = dr.value.get().value
      sendFrame(conn, encodeDeleteResponse(resp), requestId)
    else:
      # Phase 2 fallback
      let deleted = server.kvStore.kvDelete(req.key)
      var resp: DeleteResponse
      if deleted.isNone:
        resp = DeleteResponse(status: DelStatusNotFound)
      else:
        resp = DeleteResponse(status: DelStatusDeleted)
        if (req.flags and DelFlagReturnPrev) != 0:
          resp.hasPreviousValue = true
          resp.previousValue = deleted.get().value
      sendFrame(conn, encodeDeleteResponse(resp), requestId)

  of uint16(mtBatch):
    let reqR = decodeBatchRequest(payload)
    if reqR.isErr:
      sendError(conn, requestId, ErrProtocol, ErrCatProtocol, $reqR.error)
      return
    let req = reqR.value
    var results = newSeq[BatchOpResult](req.operations.len)
    var anyFailed = false
    var allFailed = req.operations.len > 0
    for i, op in req.operations:
      case op.kind
      of BatchOpGet:
        discard server.metrics.kvGets.fetchAdd(1)
        # Decode key from op.data (uint32-prefixed)
        var dpos = 0
        let keyR = protoCodec.readBytes(op.data, dpos)
        if keyR.isErr:
          results[i] = BatchOpResult(status: 0x01'u8, data: "")
          anyFailed = true
        else:
          let entryOpt = server.kvStore.kvGet(keyR.value)
          if entryOpt.isSome:
            allFailed = false
            var rdata = ""
            rdata.writeBytes(entryOpt.get().value)
            results[i] = BatchOpResult(status: 0x00'u8, data: rdata)
          else:
            anyFailed = true
            results[i] = BatchOpResult(status: 0x01'u8, data: "")
      of BatchOpPut:
        discard server.metrics.kvPuts.fetchAdd(1)
        var dpos = 0
        let keyR = protoCodec.readBytes(op.data, dpos)
        if keyR.isErr:
          results[i] = BatchOpResult(status: 0x01'u8, data: "")
          anyFailed = true
          continue
        let valR = protoCodec.readBytes(op.data, dpos)
        if valR.isErr:
          results[i] = BatchOpResult(status: 0x01'u8, data: "")
          anyFailed = true
          continue
        discard server.kvStore.kvPut(keyR.value, valR.value)
        allFailed = false
        results[i] = BatchOpResult(status: 0x00'u8, data: "")
      of BatchOpDelete:
        discard server.metrics.kvDeletes.fetchAdd(1)
        var dpos = 0
        let keyR = protoCodec.readBytes(op.data, dpos)
        if keyR.isErr:
          results[i] = BatchOpResult(status: 0x01'u8, data: "")
          anyFailed = true
          continue
        let deleted = server.kvStore.kvDelete(keyR.value)
        if deleted.isNone:
          anyFailed = true
          results[i] = BatchOpResult(status: 0x01'u8, data: "")
        else:
          allFailed = false
          results[i] = BatchOpResult(status: 0x00'u8, data: "")
      else:
        results[i] = BatchOpResult(status: 0x01'u8, data: "")
        anyFailed = true

    let batchStatus: uint8 =
      if not anyFailed: BatchStatusAllOK
      elif allFailed: BatchStatusAllFailed
      else: BatchStatusPartialFailure
    let resp = BatchResponse(status: batchStatus, results: results)
    sendFrame(conn, encodeBatchResponse(resp), requestId)

  of uint16(mtScan):
    let reqR = decodeScanRequest(payload)
    if reqR.isErr:
      sendError(conn, requestId, ErrProtocol, ErrCatProtocol, $reqR.error)
      return
    let req = reqR.value

    if not server.raftStore.isNil:
      # Phase 5: Raft-backed scan
      let sr = server.raftStore.raftScan(req.startKey, req.endKey, req.limit)
      if not sr.isOk:
        if sr.error.kind == rseNotLeader:
          sendError(conn, requestId, ErrNotLeader, ErrCatKV, "not the leader")
        else:
          sendError(conn, requestId, ErrInternal, ErrCatKV, sr.error.msg)
        return
      var scanPairs = newSeq[ScanPair](sr.value.len)
      for i, p in sr.value:
        let (k, entry) = p
        scanPairs[i] = ScanPair(
          key: k,
          value: entry.value,
          timestamp: entry.timestamp,
          version: entry.version,
        )
      let rf = ScanResponseFrame(
        respFlags: ScanRespFlagEndOfScan,
        pairs: scanPairs,
        reqFlags: req.flags,
      )
      sendFrame(conn, encodeScanResponseFrame(rf), requestId)
    else:
      # Phase 2 fallback
      let pairs = server.kvStore.kvScan(req.startKey, req.endKey, req.limit)
      var scanPairs = newSeq[ScanPair](pairs.len)
      for i, p in pairs:
        let (k, entry) = p
        scanPairs[i] = ScanPair(
          key: k,
          value: entry.value,
          timestamp: entry.timestamp,
          version: entry.version,
        )
      let rf = ScanResponseFrame(
        respFlags: ScanRespFlagEndOfScan,
        pairs: scanPairs,
        reqFlags: req.flags,
      )
      sendFrame(conn, encodeScanResponseFrame(rf), requestId)

  else:
    sendError(conn, requestId, ErrProtocol, ErrCatProtocol,
      &"unknown KV message type 0x{typeVal:04X}")

# ---------------------------------------------------------------------------
# Built-in Transaction handlers (BeginTxn, CommitTxn, RollbackTxn, TxnStatus)
# ---------------------------------------------------------------------------

proc handleBuiltinTxn(server: ProtocolServer, conn: ClientConnection,
    requestId: uint32, flags: uint16,
    payload: string) {.gcsafe, raises: [].} =
  if payload.len < 2: return
  let typeVal = (uint16(payload[0]) shl 8) or uint16(payload[1])

  case typeVal

  of uint16(mtBeginTxn):
    let reqR = txnMsgs.decodeBeginTxnRequest(payload)
    if reqR.isErr:
      sendError(conn, requestId, ErrProtocol, ErrCatProtocol, $reqR.error)
      return
    let req = reqR.value
    # Opportunistically expire timed-out transactions before starting new ones
    server.txnMgr.expireTimedOutTxns()
    let rec = server.txnMgr.beginTransaction(req.flags, req.timeoutMs)
    let resp = txnMsgs.BeginTxnResponse(
      txnId: rec.id,
      readTimestamp: rec.readTimestamp,
    )
    sendFrame(conn, txnMsgs.encodeBeginTxnResponse(resp), requestId)

  of uint16(mtCommitTxn):
    let reqR = txnMsgs.decodeCommitTxnRequest(payload)
    if reqR.isErr:
      sendError(conn, requestId, ErrProtocol, ErrCatProtocol, $reqR.error)
      return
    let txnId = reqR.value.txnId
    # Capture write-set BEFORE committing (txnMgr.commitTransaction may clear it)
    let writeSet = server.txnMgr.getWriteSet(txnId)
    let resp = server.txnMgr.commitTransaction(txnId)
    if resp.status == txnMsgs.TxnCommitOK:
      discard server.metrics.committedTxns.fetchAdd(1)
      # Resolve all buffered intents via pipelined commit: all shard proposals
      # are dispatched simultaneously so their fsyncs overlap (one fsync wall-time
      # regardless of shard count, vs Σ(fsync_i) in the old sequential path).
      if not server.raftStore.isNil and writeSet.len > 0:
        let cr = server.raftStore.raftCommitTxnPipelined(txnId, writeSet)
        if not cr.isOk:
          # Intent resolution failed — client sees commit OK but data may not
          # be durable; log the error. In a full impl we'd return a conflict.
          server.logger.logError(
            "raftCommitTxnPipelined failed for txn " & $txnId & ": " & cr.error.msg)
    else:
      discard server.metrics.abortedTxns.fetchAdd(1)
      # Clean up buffered intents on conflict/timeout (no fsync needed)
      if not server.raftStore.isNil and writeSet.len > 0:
        for key in writeSet:
          discard server.raftStore.raftDeleteIntent(txnId, key)
    sendFrame(conn, txnMsgs.encodeCommitTxnResponse(resp), requestId)

  of uint16(mtRollbackTxn):
    let reqR = txnMsgs.decodeRollbackTxnRequest(payload)
    if reqR.isErr:
      sendError(conn, requestId, ErrProtocol, ErrCatProtocol, $reqR.error)
      return
    let txnId = reqR.value.txnId
    # Capture write-set before rolling back
    let writeSet = server.txnMgr.getWriteSet(txnId)
    let resp = server.txnMgr.rollbackTransaction(txnId)
    discard server.metrics.abortedTxns.fetchAdd(1)
    # Delete all buffered intents (no fsync)
    if not server.raftStore.isNil and writeSet.len > 0:
      for key in writeSet:
        discard server.raftStore.raftDeleteIntent(txnId, key)
    sendFrame(conn, txnMsgs.encodeRollbackTxnResponse(resp), requestId)

  of uint16(mtTxnStatus):
    let reqR = txnMsgs.decodeTxnStatusRequest(payload)
    if reqR.isErr:
      sendError(conn, requestId, ErrProtocol, ErrCatProtocol, $reqR.error)
      return
    let resp = server.txnMgr.getTransactionStatus(reqR.value.txnId)
    sendFrame(conn, txnMsgs.encodeTxnStatusResponse(resp), requestId)

  else:
    sendError(conn, requestId, ErrProtocol, ErrCatTransaction,
      &"unknown txn message type 0x{typeVal:04X}")

# ---------------------------------------------------------------------------
# Built-in Admin handlers (ServerInfo, Metrics, Health)
# ---------------------------------------------------------------------------

proc handleBuiltinAdmin(server: ProtocolServer, conn: ClientConnection,
    requestId: uint32, flags: uint16,
    payload: string) {.gcsafe, raises: [].} =
  if payload.len < 2: return
  let typeVal = (uint16(payload[0]) shl 8) or uint16(payload[1])

  case typeVal

  of uint16(mtServerInfo):
    let nowSec = getTime().toUnix()
    let uptime = uint64(if nowSec > server.startedAt: nowSec -
        server.startedAt else: 0)
    let realShardCount: uint32 =
      if not server.raftStore.isNil: uint32(server.raftStore.shardCount())
      else: 1'u32
    let resp = adminMsgs.ServerInfoResponse(
      nodeId: server.config.serverId,
      version: server.config.serverVersion,
      uptimeSecs: uptime,
      role: adminMsgs.RoleLeader,
      shardCount: realShardCount,
      clientCount: uint32(server.clientCount()),
    )
    sendFrame(conn, adminMsgs.encodeServerInfoResponse(resp), requestId)

  of uint16(mtMetrics):
    let reqR = adminMsgs.decodeMetricsRequest(payload)
    if reqR.isErr:
      sendError(conn, requestId, ErrProtocol, ErrCatProtocol, $reqR.error)
      return
    var snap = server.metrics.snapshot()
    snap.activeTxns = uint32(server.txnMgr.activeTxnCount())
    sendFrame(conn, adminMsgs.encodeMetricsResponse(snap), requestId)
    # Reset counters if flag requested
    if (reqR.value.flags and adminMsgs.MetricsFlagReset) != 0:
      server.metrics.reset()

  of uint16(mtHealth):
    let status = adminMsgs.HealthOK
    let resp = adminMsgs.HealthResponse(
      status: status,
      leaderOK: true,
      replicaCount: 1,
      healthyReplicas: 1,
      clusterName: server.config.clusterName,
    )
    sendFrame(conn, adminMsgs.encodeHealthResponse(resp), requestId)

  else:
    sendError(conn, requestId, ErrProtocol, ErrCatSystem,
      &"unknown admin message type 0x{typeVal:04X}")

# ---------------------------------------------------------------------------
# Built-in Cluster Admin handlers (JoinNode, RemoveNode, ListNodes, RebalanceStatus)
# ---------------------------------------------------------------------------

proc handleBuiltinCluster(server: ProtocolServer, conn: ClientConnection,
    requestId: uint32, flags: uint16,
    payload: string) {.gcsafe, raises: [].} =
  if payload.len < 2: return
  let typeVal = (uint16(payload[0]) shl 8) or uint16(payload[1])

  case typeVal

  of uint16(mtJoinNode):
    let reqR = clusterMsgs.decodeJoinNodeRequest(payload)
    if reqR.isErr:
      sendError(conn, requestId, ErrProtocol, ErrCatProtocol, $reqR.error)
      return
    let req = reqR.value
    if req.nodeId == 0:
      let resp = clusterMsgs.JoinNodeResponse(success: false,
        message: "nodeId 0 is reserved")
      sendFrame(conn, clusterMsgs.encodeJoinNodeResponse(resp), requestId)
      return
    if req.host.len == 0:
      let resp = clusterMsgs.JoinNodeResponse(success: false,
        message: "host must not be empty")
      sendFrame(conn, clusterMsgs.encodeJoinNodeResponse(resp), requestId)
      return
    let entry = ClusterNodeEntry(
      nodeId: req.nodeId,
      host: req.host,
      raftPort: req.raftPort,
      clientPort: req.clientPort,
      status: clusterMsgs.NodeStatusActive,
    )
    server.nodeRegistry.addNode(entry)
    if server.config.dataDir != "":
      saveRegistry(server.nodeRegistry, server.config.dataDir / "node_registry.dat")
    let resp = clusterMsgs.JoinNodeResponse(success: true,
      message: "node " & $req.nodeId & " joined")
    sendFrame(conn, clusterMsgs.encodeJoinNodeResponse(resp), requestId)

  of uint16(mtRemoveNode):
    let reqR = clusterMsgs.decodeRemoveNodeRequest(payload)
    if reqR.isErr:
      sendError(conn, requestId, ErrProtocol, ErrCatProtocol, $reqR.error)
      return
    let req = reqR.value
    let removed = server.nodeRegistry.removeNode(req.nodeId)
    if removed and server.config.dataDir != "":
      saveRegistry(server.nodeRegistry, server.config.dataDir / "node_registry.dat")
    let resp = clusterMsgs.RemoveNodeResponse(
      success: removed,
      message: if removed: "node " & $req.nodeId & " removed"
               else: "node " & $req.nodeId & " not found",
    )
    sendFrame(conn, clusterMsgs.encodeRemoveNodeResponse(resp), requestId)

  of uint16(mtListNodes):
    let entries = server.nodeRegistry.listNodes()
    var nodes = newSeq[clusterMsgs.NodeInfo](entries.len)
    for i, e in entries:
      nodes[i] = clusterMsgs.NodeInfo(
        nodeId: e.nodeId,
        host: e.host,
        raftPort: e.raftPort,
        clientPort: e.clientPort,
        status: e.status,
      )
    let resp = clusterMsgs.ListNodesResponse(nodes: nodes)
    sendFrame(conn, clusterMsgs.encodeListNodesResponse(resp), requestId)

  of uint16(mtRebalanceStatus):
    let resp = clusterMsgs.RebalanceStatusResponse(
      pending: server.nodeRegistry.rebalancePending.load(),
      inProgress: server.nodeRegistry.rebalanceInProgress.load(),
      completed: server.nodeRegistry.rebalanceCompleted.load(),
      failed: server.nodeRegistry.rebalanceFailed.load(),
    )
    sendFrame(conn, clusterMsgs.encodeRebalanceStatusResponse(resp), requestId)

  of uint16(mtDrainNode):
    let reqR = clusterMsgs.decodeDrainNodeRequest(payload)
    if reqR.isErr:
      sendError(conn, requestId, ErrProtocol, ErrCatProtocol, $reqR.error)
      return
    let req = reqR.value
    let drained = server.nodeRegistry.drainNode(req.nodeId)
    if drained and server.config.dataDir != "":
      saveRegistry(server.nodeRegistry, server.config.dataDir / "node_registry.dat")
    let resp = clusterMsgs.DrainNodeResponse(
      success: drained,
      message: if drained: "node " & $req.nodeId & " is draining"
               else: "node " & $req.nodeId & " not found",
    )
    sendFrame(conn, clusterMsgs.encodeDrainNodeResponse(resp), requestId)

  else:
    sendError(conn, requestId, ErrProtocol, ErrCatSystem,
      &"unknown cluster message type 0x{typeVal:04X}")

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
        discard server.metrics.requestsErr.fetchAdd(1)
      break

    let f = frameR.value
    conn.touchActivity()

    discard server.metrics.requestsTotal.fetchAdd(1)
    discard server.metrics.bytesIn.fetchAdd(uint64(FRAME_HEADER_SIZE +
        f.payload.len))

    if f.payload.len < 2:
      sendError(conn, f.header.requestId, ErrProtocol, ErrCatProtocol,
        "payload too short")
      discard server.metrics.requestsErr.fetchAdd(1)
      continue

    let typeVal = int((uint16(f.payload[0]) shl 8) or uint16(f.payload[1]))

    var handler: Option[MessageHandler]
    withLock(server.handlersMu):
      if server.handlers.hasKey(typeVal):
        handler = some(server.handlers.getOrDefault(typeVal))

    if handler.isSome:
      handler.get()(conn, f.header.requestId, f.header.flags, f.payload)
      discard server.metrics.requestsOK.fetchAdd(1)
    elif typeVal <= 0x00FF:
      handleBuiltinCore(server, conn, f.header.requestId, f.header.flags,
        f.payload)
      discard server.metrics.requestsOK.fetchAdd(1)
    elif typeVal >= 0x0100 and typeVal <= 0x01FF:
      handleBuiltinKV(server, conn, f.header.requestId, f.header.flags,
        f.payload)
      discard server.metrics.requestsOK.fetchAdd(1)
    elif typeVal >= 0x0200 and typeVal <= 0x02FF:
      handleBuiltinTxn(server, conn, f.header.requestId, f.header.flags,
        f.payload)
      discard server.metrics.requestsOK.fetchAdd(1)
    elif typeVal >= 0x0700 and typeVal <= 0x0702:
      handleBuiltinAdmin(server, conn, f.header.requestId, f.header.flags,
        f.payload)
      discard server.metrics.requestsOK.fetchAdd(1)
    elif typeVal >= 0x0703 and typeVal <= 0x07FF:
      handleBuiltinCluster(server, conn, f.header.requestId, f.header.flags,
        f.payload)
      discard server.metrics.requestsOK.fetchAdd(1)
    else:
      sendError(conn, f.header.requestId, ErrProtocol, ErrCatProtocol,
        &"no handler for message type 0x{typeVal:04X}")
      discard server.metrics.requestsErr.fetchAdd(1)

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
# Cluster membership persistence
# ---------------------------------------------------------------------------

type
  ClusterMember = object
    nodeId: uint32
    host: string
    raftPort: int
    clientPort: int
    webPort: int

proc clusterStatePath(server: ProtocolServer): string =
  server.config.dataDir / "cluster.json"

proc saveClusterState*(server: ProtocolServer) =
  ## Persist current cluster membership to disk so a restarted node can
  ## rejoin as a follower without requiring --join.
  if server.config.dataDir == "": return
  if server.raftTransport.isNil: return

  var members: seq[ClusterMember] = @[]
  for p in server.raftTransport.peers:
    members.add(ClusterMember(
      nodeId: p.nodeId.uint32,
      host: p.host,
      raftPort: p.raftPort,
    ))

  # Also include self so we know our own identity
  let selfEntry = ClusterMember(
    nodeId: uint32(server.config.serverId),
    host: server.config.host,
    raftPort: 0, # filled by caller if needed
    clientPort: server.config.port,
    webPort: server.config.webPort,
  )

  var peersArr = newJArray()
  for m in members:
    peersArr.add(%* {
      "nodeId": m.nodeId.int,
      "host": m.host,
      "raftPort": m.raftPort,
    })

  let j = %* {
    "self": {
      "nodeId": selfEntry.nodeId.int,
      "host": selfEntry.host,
      "clientPort": selfEntry.clientPort,
      "webPort": selfEntry.webPort,
    },
    "peers": peersArr,
  }

  try:
    writeFile(server.clusterStatePath, $j)
  except CatchableError: discard

proc loadClusterState(dataDir: string): Option[JsonNode] =
  ## Load saved cluster membership. Returns none if no state exists.
  let path = dataDir / "cluster.json"
  if not fileExists(path): return none(JsonNode)
  try:
    result = some(parseJson(readFile(path)))
  except CatchableError:
    result = none(JsonNode)

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

proc addPeerToRaft*(server: ProtocolServer, peerNodeId: uint32,
                     host: string, raftPort: int) =
  ## Dynamically add a peer to the Raft transport and all group descriptors.
  ## Called when a new node joins the cluster.
  if server.raftTransport.isNil: return

  let rangeNodeId = rangeTypes.RangeNodeID(peerNodeId)

  # Add to transport layer (connection manager + peer list)
  server.raftTransport.addPeer(rangeNodeId, host, raftPort)

  # Add replica to all Raft groups
  let coord = server.raftCoord
  if coord.isNil: return
  withLock coord.groupsLock:
    for rangeId, group in coord.groups:
      discard group.descriptor.addReplica(rangeNodeId, rangeTypes.rtVoter)

  # Persist membership so restarts can rejoin without --join
  server.saveClusterState()

proc setupRaftNode*(server: ProtocolServer, raftPort: int,
                    rawPeers: seq[string],
                    startAsLeader: bool = true) {.raises: [Exception].} =
  ## Wire a real Raft + WiscKey stack into the server.
  ## rawPeers: each entry "ID:HOST:RAFT_PORT", empty = single-node mode.
  ## startAsLeader: when true and no peers, immediately become leader.

  let nodeId = rangeTypes.RangeNodeID(uint32(server.config.serverId))
  let raftDir = server.config.dataDir / "raft"

  # When joining with --join, clear Raft state so the leader's log
  # replays from scratch. This avoids stale entries from a previous
  # cluster incarnation sharing term numbers with new entries.
  # A killed node that simply restarts (without --join) keeps its log
  # and catches up incrementally via the leader's heartbeats.
  if not startAsLeader:
    removeDir(raftDir)

  createDir(raftDir)

  # Parse peer strings → PeerAddr
  var peers: seq[PeerAddr] = @[]
  for raw in rawPeers:
    let parts = raw.split(':')
    if parts.len < 3: continue
    try:
      peers.add(PeerAddr(
        nodeId: rangeTypes.RangeNodeID(uint32(parseInt(parts[0]))),
        host: parts[1],
        raftPort: parseInt(parts[2]),
      ))
    except ValueError: discard

  # Check for saved cluster state (from a previous run as part of a cluster).
  # If found and no explicit peers/join, load peers from disk and start as
  # follower so the existing leader's heartbeats catch us up incrementally.
  var isRejoining = false
  if peers.len == 0 and startAsLeader:
    let saved = loadClusterState(server.config.dataDir)
    if saved.isSome:
      let sj = saved.get
      let savedPeers = sj.getOrDefault("peers")
      if not savedPeers.isNil and savedPeers.kind == JArray and savedPeers.len > 0:
        isRejoining = true
        for p in savedPeers:
          let pNodeId = uint32(p.getOrDefault("nodeId").getInt(0))
          let pHost = p.getOrDefault("host").getStr("")
          let pRaftPort = p.getOrDefault("raftPort").getInt(0)
          if pNodeId > 0 and pHost != "" and pRaftPort > 0:
            peers.add(PeerAddr(
              nodeId: rangeTypes.RangeNodeID(pNodeId),
              host: pHost,
              raftPort: pRaftPort,
            ))
        if peers.len > 0:
          echo "recovered cluster membership from disk: " & $peers.len & " peers"

  # Always create transport (even for single-node) so joining nodes can connect
  let rgt = newRaftGroupTransport(nodeId, server.config.host, raftPort, peers)
  server.raftTransport = rgt
  let transport = newMultiRaftTransport(rgt)

  # Coordinator
  let coord = newMultiRaftCoordinator(CoordinatorConfig(
    nodeId: nodeId,
    numWorkers: DEFAULT_NUM_WORKERS,
    electionTimeoutNs: DEFAULT_ELECTION_TIMEOUT_NS,
    heartbeatIntervalNs: DEFAULT_HEARTBEAT_INTERVAL_NS,
    storagePath: raftDir,
    transport: transport,
    proposeTimeoutMs: 5000,
  ))
  server.raftCoord = coord

  # Raft group: single shard, all-keys
  let rangeId = rangeTypes.RangeID(1'u64)
  var desc = rangeTypes.newRangeDescriptor(rangeId, @[], @[])
  let myReplica = desc.addReplica(nodeId, rangeTypes.rtVoter)
  for p in peers:
    discard desc.addReplica(p.nodeId, rangeTypes.rtVoter)
  let group = coord.createGroup(desc, myReplica.replicaId)
  coord.start()

  # Become leader only for a truly fresh single-node cluster (no saved peers)
  if peers.len == 0 and startAsLeader and not isRejoining:
    group.becomeLeader()

  # KV store
  let store = newRaftKVStoreExt(coord)
  bootstrapSingleShardExt(store, rangeId)
  server.raftStore = store

  # Recovery: replay committed Raft log entries to rebuild in-memory state machine.
  # On a fresh start lastApplied=0 so this is a no-op. On restart after a kill,
  # we need to re-apply entries 1..lastApplied because the in-memory KVStateMachine
  # is empty (only WiscKey has the data on disk, but reads go through the in-memory SM).
  let lastApplied = group.lastApplied.load()
  if lastApplied > 0:
    echo "replaying " & $lastApplied & " committed log entries to rebuild state machine..."
    # Reset lastApplied to 0 so applyUpTo replays from the beginning
    group.lastApplied.store(0)
    coord.applyUpTo(rangeId, group, lastApplied)
    echo "state machine recovery complete (applied up to " & $group.lastApplied.load() & ")"

  # Seed system tables: sys.nodes (table 5) and sys.ranges (table 4)
  # Only seed when starting as fresh leader (not rejoining and not joining)
  if startAsLeader and not isRejoining:
    let nodeKey = encodeTableKey(SYS_NODES_TABLE_ID, $server.config.serverId)
    let nodeVal = $ %* {
      "nodeId": server.config.serverId.int,
      "host": server.config.host,
      "raftPort": raftPort,
      "clientPort": server.config.port,
      "webPort": server.config.webPort,
      "status": 1,
    }
    discard store.raftPut(nodeKey, nodeVal)

    let rangeKey = encodeTableKey(SYS_RANGES_TABLE_ID, $rangeId.uint64)
    let rangeVal = $ %* {
      "rangeId": rangeId.uint64.int,
      "startKey": "",
      "endKey": "",
      "replicas": [{"nodeId": server.config.serverId.int, "type": "voter"}],
    }
    discard store.raftPut(rangeKey, rangeVal)

    for p in peers:
      let peerKey = encodeTableKey(SYS_NODES_TABLE_ID, $p.nodeId.uint32)
      let peerVal = $ %* {
        "nodeId": p.nodeId.uint32.int,
        "host": p.host,
        "raftPort": p.raftPort.int,
        "clientPort": 0,
        "status": 1,
      }
      discard store.raftPut(peerKey, peerVal)

    # Seed default database and public schema
    let dbKey = encodeTableKey(SYS_DATABASES_TABLE_ID, "default")
    discard store.raftPut(dbKey, $ %* {"name": "default"})

    let scKey = encodeTableKey(SYS_SCHEMAS_TABLE_ID, "default.public")
    discard store.raftPut(scKey, $ %* {
      "name": "public", "database": "default",
    })

    # Seed default space (replicas=0 means ALL, single group = Range 1)
    let spaceKey = encodeTableKey(SYS_SPACES_TABLE_ID, "1")
    discard store.raftPut(spaceKey, $ %* {
      "spaceId": 1,
      "name": "default",
      "replicas": 0,
      "groupCount": 1,
      "rangeIds": [1],
      "createdAt": $now(),
    })

  # SharedTimer: enable when we have peers and timer not yet configured
  if peers.len > 0 and server.sharedTimer.isNil:
    var timerPeers: seq[PeerConfig] = @[]
    for p in peers:
      timerPeers.add(PeerConfig(
        peerId: $p.nodeId.uint32,
        address: p.host,
        port: uint16(p.raftPort + 1),
        weight: 1.0,
      ))
    let selfTimerPort = uint16(raftPort + 1)
    let timerNet = udpXport.newUDPTransport(selfTimerPort, server.logger)
    let timer = newSharedTimer(
      nodeId = server.config.serverName,
      numericNodeId = server.config.serverId,
      peers = timerPeers,
      network = timerNet,
      logger = server.logger,
    )
    server.sharedTimer = timer
    server.txnMgr.setTimeProvider(timer)

  # Persist cluster membership for restart recovery
  if peers.len > 0:
    server.saveClusterState()

proc start*(server: ProtocolServer) {.raises: [].} =
  server.running.store(true)
  server.startedAt = getTime().toUnix()
  # Start background SharedTimer sync thread if configured
  if not server.sharedTimer.isNil:
    try: server.sharedTimer.start()
    except Exception as e: server.logger.logError("SharedTimer start failed: " & e.msg)
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
  # Stop SharedTimer background sync thread and close network transport
  if not server.sharedTimer.isNil:
    try: server.sharedTimer.stop()
    except Exception as e: server.logger.logError("SharedTimer stop failed: " & e.msg)
