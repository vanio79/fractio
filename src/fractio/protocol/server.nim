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

import std/[net, tables as stdtables, strformat, strutils, times, atomics,
    locks, options, algorithm, os]
import posix as posixSys
import nativesockets
import ../utils/socket_utils
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
import ./client
import ./messages/space as spaceMsgs
import ./txn_manager
import ./raft_store
import ./mvcc_store
import ./cluster_state_binary
import ../core/types except NodeID
import ../utils/logging
import ../utils/socket_utils
import ../distributed/sharedtimer
import ../distributed/raft/nuraft_coordinator
import ../distributed/raft/multigroup_types
import ../distributed/raft/group_types
import ../distributed/sharedtimer/udptransport as udpXport
import ../distributed/meta/system_tables
import ../distributed/meta/system_schemas
import ../distributed/space_manager
import ../core/timestamp_provider
import ../storage/backend
import ../storage/mvcc/types as mvccValueTypes
import ./active_txn_registry

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
    webPort*: int               ## port for the HTTP management dashboard; 0 = disabled
    writeBufferSize*: int       ## LevelDB write buffer in bytes; 0 = default (4 MB)
    blockCacheSize*: int        ## LevelDB block cache in bytes; 0 = LevelDB default (8 MB)
    vlogMaxSize*: int64         ## Max vlog file size in bytes; 0 = default (1 GB)
    vlogCleanThreshold*: int64  ## Garbage records to trigger vlog GC; 0 = default (100000)
    vlogMinCleanThreshold*: int64 ## Minimum garbage records for manual cleanup; 0 = default (1000)
    vlogCleanBufferSize*: int64 ## Write buffer for vlog GC in bytes; 0 = default (64 MB)
    tempDir*: string            ## Base directory for temporary files (default: dataDir/tmp)
                                ## Operations use subdirectories: sort/, reverse/, etc.

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
    tempDir: "",
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
    mvccSessionId*: uint64 ## MVCC session ID (0 = no session)
    mu*: Lock

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
    data*: stdtables.Table[string, KVEntry]
    mu*: Lock
    nextVersion*: Atomic[uint64]

proc newKVStore*(): KVStore =
  result = KVStore(data: stdtables.initTable[string, KVEntry]())
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
    nodes*: stdtables.Table[uint16, ClusterNodeEntry]
    mu*: Lock

proc newNodeRegistry*(): NodeRegistry =
  result = NodeRegistry(nodes: stdtables.initTable[uint16, ClusterNodeEntry]())
  initLock(result.mu)

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
    connectionsAccepted*: Atomic[uint64]
    connectionsRejected*: Atomic[uint64]

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
  result.connectionsAccepted.store(0)
  result.connectionsRejected.store(0)

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
  m.connectionsAccepted.store(0)
  m.connectionsRejected.store(0)

# ---------------------------------------------------------------------------
# Protocol server
# ---------------------------------------------------------------------------

type
  ProtocolServer* = ref object
    config*: ServerConfig
    logger*: Logger
    running*: Atomic[bool]
    startedAt*: int64             ## Unix seconds; set in start()
    acceptSock*: Socket           ## Accept socket, closed in stop() to unblock accept thread
    clients*: stdtables.Table[uint32, ClientConnection]
    clientsMu*: Lock
    handlers*: stdtables.Table[int, MessageHandler]
    handlersMu*: Lock
    nextClientId*: Atomic[uint32]
    serverFeatures*: uint32
    kvStore*: KVStore             ## Phase 2: in-memory store (fallback when raftStore is nil)
    raftStore*: RaftKVStoreExt    ## Phase 5: Raft-backed KV store (nil = use kvStore)
    mvccStore*: MvccTransactionStore ## Full MVCC transaction store for all writes
    txnMgr*: TransactionManager   ## Phase 3: transaction manager
    metrics*: ServerMetrics       ## Phase 4: request counters
    authenticator*: Authenticator ## Phase 4: auth validator
    sharedTimer*: SharedTimer     ## Phase 7: P2P clock sync (nil when disabled)
    nodeRegistry*: NodeRegistry   ## Phase 8: in-memory cluster node registry
    raftCoord*: NuRaftCoordinator ## lifecycle owner; nil until setupRaftNode
    spaceManager*: SpaceManager   ## Space management (CREATE/DROP SPACE)
    activeTxnRegistry*: ActiveTxnRegistry ## Fast liveness tracking for conflict resolution
    # Per-server thread storage
    clientThreadCount*: Atomic[int]
    acceptThreadCount*: Atomic[int]
    threadsMu*: Lock

proc serverNowMs*(server: ProtocolServer): int64 {.gcsafe, raises: [].} =
  if server.sharedTimer != nil:
    try:
      return server.sharedTimer.now() div 1_000_000
    except Exception:
      discard
  let t = getTime()
  t.toUnix * 1000 + t.nanosecond() div 1_000_000

proc serverNowSec*(server: ProtocolServer): int64 {.gcsafe, raises: [].} =
  ## Get current time in seconds. Uses sharedTimer when available,
  ## falls back to local clock. For display-only timestamps.
  if server.sharedTimer != nil:
    try:
      return server.sharedTimer.now() div 1_000_000_000
    except Exception:
      discard
  getTime().toUnix()

proc newClientConnection*(id: uint32, sock: Socket,
    address: string, server: ProtocolServer = nil): ClientConnection =
  let nowMs = if server != nil: serverNowMs(server)
              else: (getTime().toUnixFloat() * 1000).int64
  result = ClientConnection(
    id: id,
    socket: sock,
    address: address,
    createdAt: nowMs,
    lastActivityMs: nowMs,
    mvccSessionId: 0,
  )
  initLock(result.mu)

proc touchActivity*(conn: ClientConnection,
    server: ProtocolServer = nil) {.gcsafe, raises: [].} =
  withLock(conn.mu):
    conn.lastActivityMs = if server != nil: serverNowMs(server)
                          else: (getTime().toUnixFloat() * 1000).int64

proc isIdle*(conn: ClientConnection, timeoutSecs: int,
    server: ProtocolServer = nil): bool {.gcsafe, raises: [].} =
  withLock(conn.mu):
    let nowMs = if server != nil: serverNowMs(server)
                else: (getTime().toUnixFloat() * 1000).int64
    result = (nowMs - conn.lastActivityMs) > int64(timeoutSecs) * 1000

# ---------------------------------------------------------------------------
# Thread argument types — defined after ProtocolServer to avoid forward refs
# ---------------------------------------------------------------------------

type
  ClientLoopArgs* = tuple[srv: ProtocolServer, conn: ClientConnection]
  AcceptLoopArgs* = tuple[srv: ProtocolServer, sock: Socket]

# Module-level thread storage: keeps Thread objects alive for the process
# lifetime.  Protected by threadStoreMu. Each thread references its server
# so we know which threads belong to which server.
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
    clients: stdtables.initTable[uint32, ClientConnection](),
    handlers: stdtables.initTable[int, MessageHandler](),
    kvStore: newKVStore(),
    txnMgr: newTransactionManager(),
    metrics: newServerMetrics(),
    authenticator: newAuthenticator(config.authMethod),
    nodeRegistry: reg,
    activeTxnRegistry: newActiveTxnRegistry(),
    startedAt: getTime().toUnix(), # Set before sharedTimer is available; updated in start()
  )
  initLock(result.clientsMu)
  initLock(result.handlersMu)
  initLock(result.threadsMu)
  result.running.store(false)
  result.nextClientId.store(1)
  result.clientThreadCount.store(0)
  result.acceptThreadCount.store(0)
  initLock(result.clientsMu)
  initLock(result.handlersMu)
  initLock(result.threadsMu)
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
# Low-level recv helper (non-blocking with select polling)
# ---------------------------------------------------------------------------

const SERVER_RECV_TIMEOUT_MS = 30_000 # 30 seconds default timeout

proc srvRecvExact(sock: Socket, buf: var string,
    size: int, timeoutMs: int = SERVER_RECV_TIMEOUT_MS): int {.gcsafe, raises: [].} =
  ## Read exactly `size` bytes using non-blocking recv with select polling.
  ## Returns bytes read; < size means EOF/error/timeout.
  ## Uses shared recvExactNonBlocking from socket_utils.
  let fd = sock.getFd().cint
  recvExactNonBlocking(fd, buf, size, timeoutMs)

# ---------------------------------------------------------------------------
# Send helpers
# ---------------------------------------------------------------------------

proc sendRaw(conn: ClientConnection, data: string) {.gcsafe, raises: [].} =
  ## Send raw data on the socket. Silently ignore errors if socket is closed.
  ##
  ## IMPORTANT: Nim's socket.send() with SafeDisconn flag has a bug where
  ## EPIPE causes an infinite loop (socketError returns without raising,
  ## but the while loop in send() continues forever). We use trySend()
  ## which calls the low-level send() directly and returns false on error.
  try:
    # Use trySend to avoid Nim's SafeDisconn infinite loop bug
    # trySend calls low-level send() directly and returns false on any error
    discard conn.socket.trySend(data)
  except CatchableError:
    discard
  except Defect:
    # AssertionDefect can be raised when socket is closed during shutdown
    discard

proc sendFrame(conn: ClientConnection, payload: string,
    requestId: uint32, flags: uint16 = FlagIsResponse) {.gcsafe, raises: [].} =
  sendRaw(conn, encodeFrame(payload, requestId, flags))

proc sendError(conn: ClientConnection, requestId: uint32,
    errCode: uint32, category: uint8, msg: string) {.gcsafe, raises: [].} =
  sendRaw(conn, encodeErrorFrame(requestId, errCode, category, msg))

# ---------------------------------------------------------------------------
# Leader redirect helpers
# ---------------------------------------------------------------------------

proc lookupNodeFromKVStore(server: ProtocolServer,
    nodeId: uint32): tuple[host: string, clientPort: uint16] {.gcsafe.} =
  ## Look up a node's host and client port from the local sys.nodes KV store.
  ## Returns empty string and 0 port if not found.
  try:
    if server.raftStore == nil:
      return
    let backend = server.raftStore.getBackend()
    if backend == nil or not backend.isOpen:
      return
    let nodeKey = encodeTableKey(SYS_NODES_TABLE_ID, $nodeId)
    let valOpt = backend.get(nodeKey)
    if valOpt.isNone:
      return
    var nodeVal = valOpt.get()
    # Strip MVCC encoding if present
    if mvccValueTypes.isLikelyMVCCValue(nodeVal):
      let mvccVal = mvccValueTypes.decodeMVCCValueFast(nodeVal)
      if not mvccVal.isDeleted:
        nodeVal = mvccVal.data
    let nodeRec = system_schemas.decodeNodeRecord(nodeVal)
    result.host = nodeRec.host
    result.clientPort = nodeRec.clientPort
  except Exception:
    discard

proc getLeaderRedirect(server: ProtocolServer,
    groupId: GroupID): LeaderRedirect {.gcsafe, raises: [].} =
  ## Get leader redirect info for a group.
  ## Queries the Raft coordinator for the current leader and returns
  ## the leader's node ID, host, and client port so the client can
  ## connect directly instead of retrying through followers.
  ##
  ## If this node doesn't have the group (not a member), falls back to
  ## looking up the group's replicas from sys.groups and redirecting to
  ## the first replica, which can forward the request to the actual leader.
  ##
  ## IMPORTANT: If the Raft-reported leader is THIS node, we must NOT
  ## redirect to ourselves (infinite loop). This can happen when the
  ## local NuRaft instance has stale leader state after a leadership
  ## change. In that case, we fall through to the sys.groups fallback.
  if server.raftCoord == nil or not server.raftCoord.running.load():
    return LeaderRedirect(leaderId: 0)

  let leaderId = server.raftCoord.getLeader(groupId)
  let myNodeId = uint32(server.config.serverId)
  if leaderId > 0 and uint32(leaderId) != myNodeId:
    let nodeId = uint32(leaderId)

    # Look up the leader's host and client port from multiple sources
    var leaderHost = ""
    var leaderClientPort = uint16(0)

    # 1. Try node registry first (in-memory, fast)
    if server.nodeRegistry != nil:
      withLock server.nodeRegistry.mu:
        try:
          if uint16(nodeId) in server.nodeRegistry.nodes:
            let entry = server.nodeRegistry.nodes[uint16(nodeId)]
            leaderHost = entry.host
            leaderClientPort = entry.clientPort
        except KeyError:
          discard

    # 2. Try sys.nodes table from local KV store (slower, but always up-to-date)
    if leaderClientPort == 0:
      let (kvHost, kvClientPort) = server.lookupNodeFromKVStore(nodeId)
      if kvClientPort > 0:
        leaderHost = kvHost
        leaderClientPort = kvClientPort

    # 3. Fall back to peerInfo from the Raft coordinator (host only, no client port)
    if leaderHost.len == 0 and server.raftCoord.peerInfo.len > 0:
      withLock server.raftCoord.groupsLock:
        try:
          if nodeId in server.raftCoord.peerInfo:
            let (peerHost, _) = server.raftCoord.peerInfo[nodeId]
            leaderHost = peerHost
        except KeyError:
          discard

    if leaderHost.len == 0:
      # Return just the leader ID without host/port
      return LeaderRedirect(leaderId: nodeId)

    LeaderRedirect(
      leaderId: nodeId,
      leaderHost: leaderHost,
      leaderClientPort: leaderClientPort
    )
  else:
    # This node doesn't know the leader for this group (either it's not
    # a member, or no leader has been elected yet). Try to find a replica
    # node from sys.groups so the client can connect to a group member.
    if server.raftStore != nil:
      let gidStr = $groupId
      let key = encodeTableKey(SYS_GROUPS_TABLE_ID, gidStr)
      let getRes = server.raftStore.raftGet(key)
      if getRes.isOk and getRes.value.isSome:
        try:
          let data = getRes.value.get().value
          var groupRec: GroupRecord
          # Handle MVCC-encoded data
          var payload = data
          if mvccValueTypes.isLikelyMVCCValue(payload):
            try:
              let mvccVal = mvccValueTypes.decodeMVCCValue(payload)
              if not mvccVal.isDeleted:
                payload = mvccVal.data
              else:
                return LeaderRedirect(leaderId: 0)
            except CatchableError:
              discard
          groupRec = decodeGroupRecord(payload)
          # Find a replica to redirect to, preferring nodes OTHER than this one.
          # Redirecting to the same node that doesn't have the group would
          # create an infinite retry loop.
          if groupRec.replicas.len > 0:
            var replicaNodeId = uint32(0)
            let myNodeId = uint32(server.config.serverId)
            # First try a replica that is NOT this node
            for rep in groupRec.replicas:
              if rep.nodeId != myNodeId:
                replicaNodeId = rep.nodeId
                break
            # Fall back to the first replica if all are this node (shouldn't happen)
            if replicaNodeId == 0:
              replicaNodeId = groupRec.replicas[0].nodeId
            var replicaHost = ""
            var replicaClientPort = uint16(0)

            # Look up the replica's address
            if server.nodeRegistry != nil:
              withLock server.nodeRegistry.mu:
                try:
                  if uint16(replicaNodeId) in server.nodeRegistry.nodes:
                    let entry = server.nodeRegistry.nodes[uint16(replicaNodeId)]
                    replicaHost = entry.host
                    replicaClientPort = entry.clientPort
                except KeyError:
                  discard

            if replicaClientPort == 0:
              let (kvHost, kvClientPort) = server.lookupNodeFromKVStore(replicaNodeId)
              if kvClientPort > 0:
                replicaHost = kvHost
                replicaClientPort = kvClientPort

            if replicaHost.len == 0 and server.raftCoord.peerInfo.len > 0:
              withLock server.raftCoord.groupsLock:
                try:
                  if replicaNodeId in server.raftCoord.peerInfo:
                    let (peerHost, _) = server.raftCoord.peerInfo[replicaNodeId]
                    replicaHost = peerHost
                except KeyError:
                  discard

            if replicaHost.len > 0 and replicaClientPort > 0:
              return LeaderRedirect(
                leaderId: replicaNodeId,
                leaderHost: replicaHost,
                leaderClientPort: replicaClientPort
              )
            elif replicaHost.len > 0:
              return LeaderRedirect(leaderId: replicaNodeId)
        except CatchableError:
          discard

    LeaderRedirect(leaderId: 0)

proc sendNotLeaderError(conn: ClientConnection, requestId: uint32,
    msg: string, redirect: LeaderRedirect) {.gcsafe, raises: [].} =
  ## Send a NOT_LEADER error with leader redirect info.
  try:
    {.cast(gcsafe).}:
      debug("sendNotLeaderError", {"leaderId": $redirect.leaderId,
          "leaderHost": redirect.leaderHost,
          "leaderClientPort": $redirect.leaderClientPort}.toTable)
  except:
    discard
  sendRaw(conn, encodeNotLeaderErrorFrame(requestId, msg, redirect))

proc getGroupIdForKey(server: ProtocolServer, key: string): GroupID {.gcsafe,
    raises: [].} =
  ## Get the GroupID that a key routes to.
  ## Falls back to META_GROUP_ID if routing fails.
  if server.raftStore.isNil:
    return META_GROUP_ID
  let gidOpt = server.raftStore.resolveGroupId(key)
  if gidOpt.isSome:
    gidOpt.get()
  else:
    META_GROUP_ID

proc checkLeadershipForGroup(server: ProtocolServer,
    groupId: GroupID): Option[LeaderRedirect] {.gcsafe, raises: [].} =
  ## Proactively check if this node is the leader for the given group.
  ## Returns none() if this node IS the leader (request can proceed).
  ## Returns some(LeaderRedirect) if this node is NOT the leader,
  ## containing redirect info so the client can retry on the correct node.
  ## This is called BEFORE processing KV operations to give fast NOT_LEADER
  ## responses without wasting work on MVCC/raft proposals that will fail.
  if server.raftCoord == nil or not server.raftCoord.running.load():
    return none(LeaderRedirect)

  # META group: check leadership directly
  if groupId == META_GROUP_ID:
    if not server.raftCoord.isLeader(groupId):
      return some(server.getLeaderRedirect(groupId))
    return none(LeaderRedirect)

  # Data groups: check if this node is the leader
  if not server.raftCoord.isLeader(groupId):
    return some(server.getLeaderRedirect(groupId))

  none(LeaderRedirect)

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
    sendRaw(conn, encodeGreeting(greeting))

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
      sendRaw(conn, encodeHandshakeResponse(HandshakeResponse(
        status: HandshakeError, errorMessage: $hsR.error)))
      return false

    let hs = hsR.value
    if hs.version != PROTOCOL_VERSION_1:
      sendRaw(conn, encodeHandshakeResponse(HandshakeResponse(
        status: HandshakeError,
        errorMessage: &"unsupported protocol version {hs.version}")))
      return false

    # Phase 4: authenticate when server requires it
    if server.config.authMethod != amNone:
      if not server.authenticator.authenticate(hs.authType, hsAuthData):
        sendRaw(conn, encodeHandshakeResponse(HandshakeResponse(
          status: HandshakeError, errorMessage: "authentication failed")))
        return false

    let negotiated = negotiateFeatures(server.serverFeatures, hs.features)
    conn.negotiatedFeatures = negotiated
    conn.authenticated = true

    # 3. Send handshake response
    sendRaw(conn, encodeHandshakeResponse(HandshakeResponse(
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
    maxBytes: uint32, timeoutMs: int = 500): Result[Frame,
        ProtocolError] {.gcsafe, raises: [].} =
  ## Read one frame from the socket using non-blocking recv with timeout.
  ## timeoutMs: how long to wait for data (default 500ms, short to allow periodic idle checks)
  ##
  ## Error kinds:
  ##   peInvalidFrame - EOF (0 bytes) or timeout (partial read). Check the
  ##                    message string: "eof" means the peer closed the connection,
  ##                    "short header" means a timeout with no data.
  ##   peChecksumMismatch - CRC mismatch
  ##   peFrameTooLarge - payload exceeds maxBytes
  var hdrBuf = newString(FRAME_HEADER_SIZE)
  let hn = srvRecvExact(sock, hdrBuf, FRAME_HEADER_SIZE, timeoutMs)
  if hn == 0:
    # EOF: peer closed the connection cleanly (recv returned 0)
    return peErr(newProtocolError(peInvalidFrame, "eof"))
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
    let pn = srvRecvExact(sock, payload, int(hdr.payloadLen), timeoutMs)
    if pn == 0:
      return peErr(newProtocolError(peInvalidFrame, "eof"))
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
  discard flags # Reserved for future use
  if payload.len < 2: return
  let typeVal = (uint16(payload[0]) shl 8) or uint16(payload[1])
  case typeVal
  of uint16(mtPing):
    let tsUs = if server.sharedTimer != nil:
      try: uint64(server.sharedTimer.now() div 1_000) except Exception:
        uint64(getTime().toUnixFloat() * 1_000_000)
    else: uint64(getTime().toUnixFloat() * 1_000_000)
    sendFrame(conn, encodePingResponse(tsUs), requestId)
  of uint16(mtEcho):
    let dataR = decodeEchoData(payload)
    if dataR.isErr:
      sendError(conn, requestId, ErrProtocol, ErrCatProtocol, $dataR.error)
    else:
      sendFrame(conn, encodeEchoResponse(dataR.value), requestId)
  of uint16(mtClose):
    sendFrame(conn, encodeCloseRequest("bye"), requestId)
    # Close the socket so the clientLoop main loop exits cleanly.
    # The defer block will handle MVCC session cleanup and lock deinit.
    try: conn.socket.close() except CatchableError: discard
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
  discard flags # Reserved for future use
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

    # Enforce leader-only reads for linearizability.
    # Use isWriteReady (not just isLeader) to ensure the leader has committed
    # its no-op entry and the state machine is up to date. Without this,
    # a newly elected leader may serve stale reads before it has caught up.
    if not server.raftStore.isNil and server.raftStore.coordinator != nil:
      # Use the client-provided groupId if present (GroupRouted flag),
      # otherwise resolve from the key. During rebalancing, the client
      # routes reads to old groups but the server's resolveGroupId would
      # return new groups — causing a routing mismatch and infinite retries.
      let groupId = if req.groupId != ZeroGroupID(): req.groupId
                    else: server.getGroupIdForKey(req.key)
      if not server.raftStore.coordinator.isWriteReady(groupId):
        let redirect = server.getLeaderRedirect(groupId)
        sendNotLeaderError(conn, requestId, "not leader for group", redirect)
        return

    # Transactional read: register the key as read by the transaction manager
    if not isZero(req.txnId):
      if not server.mvccStore.isNil:
        let rr = server.mvccStore.recordReadByTxnId(req.txnId, req.key)
        if not rr.isOk:
          sendError(conn, requestId, ErrTxnAborted, ErrCatTransaction,
            "txn expired or not found: " & rr.error.msg)
          return
      else:
        let rr = server.txnMgr.recordRead(req.txnId, req.key)
        if rr.isErr:
          sendError(conn, requestId, ErrTxnAborted, ErrCatTransaction,
            "txn expired or not found: " & rr.error.msg)
          return


    var resp: GetResponse
    if not server.mvccStore.isNil:
      # Use metadata-aware get for timestamps and versions
      # If in a transaction, check intents first
      let metaRes = if not isZero(req.txnId):
                      server.mvccStore.txnGetWithMetaByTxnId(req.txnId, req.key)
                    else:
                      server.mvccStore.latestGetWithMeta(req.key)

      if metaRes.isOk:
        if metaRes.value.isSome:
          let meta = metaRes.value.get()
          # Apply server-side filter if present (PointGet optimization)
          var passesFilter = true
          if req.filter.isSome:
            passesFilter = matchesWireFilterWithDecodedValue(req.filter, meta.value)

          if passesFilter:
            resp = GetResponse(
              found: true,
              hasTimestamp: (req.flags and GetFlagIncludeTimestamp) != 0,
              hasVersion: (req.flags and GetFlagIncludeVersion) != 0,
              timestamp: meta.timestamp,
              version: meta.version,
              value: meta.value,
            )
          else:
            # Row exists but doesn't pass filter - return as not found
            resp = GetResponse(found: false)
        else:
          resp = GetResponse(found: false)
      else:
        sendError(conn, requestId, ErrInternal, ErrCatKV, metaRes.error.msg)
        return
    else:
      sendError(conn, requestId, ErrInternal, ErrCatKV, "MVCC store not initialized")
      return
    sendFrame(conn, encodeGetResponse(resp), requestId)

  of uint16(mtRawPut):
    let reqR = decodePutRequest(payload)
    if reqR.isErr:
      sendError(conn, requestId, ErrProtocol, ErrCatProtocol, $reqR.error)
      return
    let req = reqR.value
    if req.key.len == 0 or req.key.len > int(server.config.maxKeyBytes):
      sendError(conn, requestId, ErrProtocol, ErrCatKV, "invalid key length")
      return

    let writeRes = raftPutInGroup(server.raftStore, req.key, req.value,
        req.groupId)
    if writeRes.isOk:
      sendFrame(conn, encodePutResponse(PutResponse(
        status: PutStatusOK, timestamp: 0, version: 1)), requestId)
    else:
      if writeRes.error.kind == rseNotLeader:
        let redirect = server.getLeaderRedirect(req.groupId)
        sendNotLeaderError(conn, requestId, $writeRes.error, redirect)
      else:
        sendError(conn, requestId, ErrInternal, ErrCatKV, $writeRes.error)

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

    # Proactive NOT_LEADER check: determine the group for this key and verify
    # this node is the leader. Return NOT_LEADER immediately if not, avoiding
    # unnecessary MVCC/raft work that would fail anyway.
    let groupId = if req.groupId != ZeroGroupID(): req.groupId
                  else: server.getGroupIdForKey(req.key)
    let redirectOpt = server.checkLeadershipForGroup(groupId)
    if redirectOpt.isSome:
      sendNotLeaderError(conn, requestId,
          "not the leader for group " & $groupId, redirectOpt.get())
      return

    if not server.mvccStore.isNil:
      if not isZero(req.txnId):
        let res = server.mvccStore.txnPutWithResultByTxnId(req.txnId, req.key, req.value,
                    req.flags, req.expectedVersion)
        if res.isOk:
          # Touch the transaction in the active registry (inline atomic store, ~1ns)
          if not server.activeTxnRegistry.isNil:
            server.activeTxnRegistry.touch(req.txnId)
          let pr = res.value
          var resp = PutResponse(
            status: pr.status,
            timestamp: pr.timestamp,
            version: pr.version,
          )
          if pr.previousValue.isSome:
            resp.hasPreviousValue = true
            resp.previousValue = pr.previousValue.get()
          sendFrame(conn, encodePutResponse(resp), requestId)
        else:
          # Check for "not leader" error and return appropriate error code
          if res.error.msg.contains("not the leader") or
             res.error.msg.contains("Not the leader") or
             res.error.msg.contains("Raft append failed (code -3)"):
            let groupId = if req.groupId != ZeroGroupID(): req.groupId
                         else: server.getGroupIdForKey(req.key)
            let redirect = server.getLeaderRedirect(groupId)
            sendNotLeaderError(conn, requestId, res.error.msg, redirect)
          elif res.error.msg.contains("Group not found"):
            # Group not found - node doesn't have this group (topology change)
            # Return not leader error to trigger retry on different node
            let groupId = if req.groupId != ZeroGroupID(): req.groupId
                         else: server.getGroupIdForKey(req.key)
            let redirect = server.getLeaderRedirect(groupId)
            sendNotLeaderError(conn, requestId, res.error.msg, redirect)
          else:
            sendError(conn, requestId, ErrInternal, ErrCatKV, res.error.msg)
      else:
        let res = server.mvccStore.autoPutWithResult(req.key, req.value,
                    req.flags, req.expectedVersion)
        if res.isOk:
          let pr = res.value
          var resp = PutResponse(
            status: pr.status,
            timestamp: pr.timestamp,
            version: pr.version,
          )
          if pr.previousValue.isSome:
            resp.hasPreviousValue = true
            resp.previousValue = pr.previousValue.get()
          sendFrame(conn, encodePutResponse(resp), requestId)
        else:
          # Check for "not leader" error and return appropriate error code
          if res.error.msg.contains("not the leader") or
             res.error.msg.contains("Not the leader") or
             res.error.msg.contains("Raft append failed (code -3)"):
            let groupId = if req.groupId != ZeroGroupID(): req.groupId
                         else: server.getGroupIdForKey(req.key)
            let redirect = server.getLeaderRedirect(groupId)
            sendNotLeaderError(conn, requestId, res.error.msg, redirect)
          elif res.error.msg.contains("Group not found"):
            # Group not found - node doesn't have this group (topology change)
            # Return not leader error to trigger retry on different node
            let groupId = if req.groupId != ZeroGroupID(): req.groupId
                         else: server.getGroupIdForKey(req.key)
            let redirect = server.getLeaderRedirect(groupId)
            sendNotLeaderError(conn, requestId, res.error.msg, redirect)
          else:
            sendError(conn, requestId, ErrInternal, ErrCatKV, res.error.msg)
    else:
      sendError(conn, requestId, ErrInternal, ErrCatKV, "MVCC store not initialized")

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

    # Proactive NOT_LEADER check: verify this node leads the target group
    # before processing the delete.
    let groupId = if req.groupId != ZeroGroupID(): req.groupId
                  else: server.getGroupIdForKey(req.key)
    let redirectOpt = server.checkLeadershipForGroup(groupId)
    if redirectOpt.isSome:
      sendNotLeaderError(conn, requestId,
          "not the leader for group " & $groupId, redirectOpt.get())
      return

    if not server.mvccStore.isNil:
      if not isZero(req.txnId):
        let res = server.mvccStore.txnDeleteWithResultByTxnId(req.txnId,
            req.key, req.flags)
        if res.isOk:
          # Touch the transaction in the active registry (inline atomic store, ~1ns)
          if not server.activeTxnRegistry.isNil:
            server.activeTxnRegistry.touch(req.txnId)
          let dr = res.value
          var resp = DeleteResponse(
            status: if dr.found: DelStatusDeleted else: DelStatusNotFound,
          )
          if dr.previousValue.isSome:
            resp.hasPreviousValue = true
            resp.previousValue = dr.previousValue.get()
          sendFrame(conn, encodeDeleteResponse(resp), requestId)
        else:
          # Check for "not leader" error and return appropriate error code
          if res.error.msg.contains("not the leader") or res.error.msg.contains("Not the leader"):
            let groupId = if req.groupId != ZeroGroupID(): req.groupId
                         else: server.getGroupIdForKey(req.key)
            let redirect = server.getLeaderRedirect(groupId)
            sendNotLeaderError(conn, requestId, res.error.msg, redirect)
          elif res.error.msg.contains("Group not found"):
            # Group not found - node doesn't have this group (topology change)
            let groupId = if req.groupId != ZeroGroupID(): req.groupId
                         else: server.getGroupIdForKey(req.key)
            let redirect = server.getLeaderRedirect(groupId)
            sendNotLeaderError(conn, requestId, res.error.msg, redirect)
          else:
            sendError(conn, requestId, ErrInternal, ErrCatKV, res.error.msg)
      else:
        let res = server.mvccStore.autoDeleteWithResult(req.key, req.flags)
        if res.isOk:
          let dr = res.value
          var resp = DeleteResponse(
            status: if dr.found: DelStatusDeleted else: DelStatusNotFound,
          )
          if dr.previousValue.isSome:
            resp.hasPreviousValue = true
            resp.previousValue = dr.previousValue.get()
          sendFrame(conn, encodeDeleteResponse(resp), requestId)
        else:
          # Check for "not leader" error and return appropriate error code
          if res.error.msg.contains("not the leader") or res.error.msg.contains("Not the leader"):
            let groupId = if req.groupId != ZeroGroupID(): req.groupId
                         else: server.getGroupIdForKey(req.key)
            let redirect = server.getLeaderRedirect(groupId)
            sendNotLeaderError(conn, requestId, res.error.msg, redirect)
          elif res.error.msg.contains("Group not found"):
            # Group not found - node doesn't have this group (topology change)
            let groupId = if req.groupId != ZeroGroupID(): req.groupId
                         else: server.getGroupIdForKey(req.key)
            let redirect = server.getLeaderRedirect(groupId)
            sendNotLeaderError(conn, requestId, res.error.msg, redirect)
          else:
            sendError(conn, requestId, ErrInternal, ErrCatKV, res.error.msg)
    else:
      sendError(conn, requestId, ErrInternal, ErrCatKV, "MVCC store not initialized")

  of uint16(mtBatch):
    let reqR = decodeBatchRequest(payload)
    if reqR.isErr:
      sendError(conn, requestId, ErrProtocol, ErrCatProtocol, $reqR.error)
      return
    let req = reqR.value
    if server.mvccStore.isNil:
      sendError(conn, requestId, ErrInternal, ErrCatKV, "MVCC store not initialized")
      return

    # Use an auto-transaction for the whole batch
    let res = server.mvccStore.withAutoTransactionResult(proc(
        sid: uint64): mvcc_store.MvccResult[seq[BatchOpResult]] =
      var opsResults = newSeq[BatchOpResult](req.operations.len)
      var hasFailure = false
      for i, op in req.operations:
        case op.kind
        of BatchOpGet:
          var dpos = 0
          let keyR = protoCodec.readBytes(op.data, dpos)
          if keyR.isErr: return mvccErr[seq[BatchOpResult]](MvccStoreError(
              msg: "invalid key in batch"))
          let getRes = server.mvccStore.txnGet(sid, keyR.value)
          if getRes.isOk and getRes.value.isSome:
            var rdata = ""
            rdata.writeBytes(getRes.value.get())
            opsResults[i] = BatchOpResult(status: 0x00'u8, data: rdata)
          else:
            opsResults[i] = BatchOpResult(status: 0x01'u8, data: "")
            hasFailure = true
        of BatchOpPut:
          var dpos = 0
          let keyR = protoCodec.readBytes(op.data, dpos)
          if keyR.isErr: return mvccErr[seq[BatchOpResult]](MvccStoreError(
              msg: "invalid key in batch"))
          let valR = protoCodec.readBytes(op.data, dpos)
          if valR.isErr: return mvccErr[seq[BatchOpResult]](MvccStoreError(
              msg: "invalid val in batch"))
          discard server.mvccStore.txnPut(sid, keyR.value, valR.value)
          opsResults[i] = BatchOpResult(status: 0x00'u8, data: "")
        of BatchOpDelete:
          var dpos = 0
          let keyR = protoCodec.readBytes(op.data, dpos)
          if keyR.isErr: return mvccErr[seq[BatchOpResult]](MvccStoreError(
              msg: "invalid key in batch"))
          discard server.mvccStore.txnDelete(sid, keyR.value)
          opsResults[i] = BatchOpResult(status: 0x00'u8, data: "")
        else:
          opsResults[i] = BatchOpResult(status: 0x01'u8, data: "")
          hasFailure = true
      # Store hasFailure flag in a threadvar or use a wrapper
      # For now, we'll just return results and check them after
      return mvccOk(opsResults)
    )

    if res.isOk:
      # Check for partial failures in results
      var hasFailure = false
      for r in res.value:
        if r.status != 0x00'u8:
          hasFailure = true
          break
      let batchStatus = if hasFailure: BatchStatusPartialFailure else: BatchStatusAllOK
      let resp = BatchResponse(status: batchStatus, results: res.value)
      sendFrame(conn, encodeBatchResponse(resp), requestId)
    else:
      sendError(conn, requestId, ErrInternal, ErrCatKV, res.error.msg)

  of uint16(mtScan):
    let reqR = decodeScanRequest(payload)
    if reqR.isErr:
      sendError(conn, requestId, ErrProtocol, ErrCatProtocol, $reqR.error)
      return
    let req = reqR.value

    # Reject scans routed to a specific group if this node is not the leader.
    # A follower serving stale data would cause missing rows in multi-group
    # k-way merge scans, so we redirect to the leader.
    if req.groupId != ZeroGroupID() and not server.raftCoord.isNil:
      if not server.raftCoord.isLeader(req.groupId):
        let redirect = server.getLeaderRedirect(req.groupId)
        sendNotLeaderError(conn, requestId,
            "not the leader for group " & $req.groupId, redirect)
        return

    if not server.mvccStore.isNil:
      let isStreaming = (req.flags and ScanFlagStreaming) != 0
      let chunkSize = if req.chunkSize > 0: int(req.chunkSize)
                      else: DEFAULT_SCAN_CHUNK_SIZE
      let needGroupFilter = req.groupId != ZeroGroupID()
      let needServerFilter = req.filter.isSome

      # Build filter procs
      var groupFilterProc: proc(key: string): bool {.gcsafe, raises: [].} = nil
      if needGroupFilter:
        groupFilterProc = proc(key: string): bool {.gcsafe, raises: [].} =
          server.raftStore.keyRoutesToGroupIdDuringRebalance(key, req.groupId)

      var serverFilterProc: proc(value: string): bool {.gcsafe, raises: [].} = nil
      if needServerFilter:
        serverFilterProc = proc(value: string): bool {.gcsafe, raises: [].} =
          matchesWireFilterWithDecodedValue(req.filter, value)

      let currentTs = server.mvccStore.getCurrentTimestamp()

      # Use streaming path for large scans to avoid buffering everything in
      # memory. The streaming callback sends frames incrementally.
      proc sendChunk(chunk: ScanChunk) {.gcsafe, raises: [].} =
        let chunkPairs = chunk.pairs
        var scanPairs = newSeq[ScanPair](chunkPairs.len)
        for i, p in chunkPairs:
          var ver: uint64 = 1
          withLock server.mvccStore.keyVersionsMu:
            ver = server.mvccStore.keyVersions.getOrDefault(p.key, 1'u64)
          scanPairs[i] = ScanPair(
            key: p.key,
            value: p.value,
            timestamp: uint64(currentTs),
            version: ver,
          )

        if chunk.hasMore:
          # More frames coming
          let rf = ScanResponseFrame(
            respFlags: ScanRespFlagHasMore,
            pairs: scanPairs,
            reqFlags: req.flags,
          )
          sendFrame(conn, encodeScanResponseFrame(rf), requestId,
              FlagIsResponse)
        else:
          # Final frame
          let rf = ScanResponseFrame(
            respFlags: ScanRespFlagEndOfScan,
            pairs: scanPairs,
            reqFlags: req.flags,
          )
          sendFrame(conn, encodeScanResponseFrame(rf), requestId)

      let readTs = if not isZero(req.txnId): LATEST_READ_TIMESTAMP
                   else: LATEST_READ_TIMESTAMP

      discard server.mvccStore.snapshotStreamScan(
        startKey = req.startKey,
        endKey = req.endKey,
        readTs = readTs,
        limit = req.limit,
        chunkSize = chunkSize,
        callback = sendChunk,
        groupFilter = groupFilterProc,
        serverFilter = serverFilterProc
      )
    else:
      sendError(conn, requestId, ErrInternal, ErrCatKV, "MVCC store not initialized")

  else:
    sendError(conn, requestId, ErrProtocol, ErrCatProtocol,
      &"unknown KV message type 0x{typeVal:04X}")

# ---------------------------------------------------------------------------
# Built-in Transaction handlers (BeginTxn, CommitTxn, RollbackTxn, TxnStatus)
# ---------------------------------------------------------------------------

proc handleBuiltinTxn(server: ProtocolServer, conn: ClientConnection,
    requestId: uint32, flags: uint16,
    payload: string) {.gcsafe, raises: [].} =
  discard flags # Reserved for future use
  if payload.len < 2: return
  let typeVal = (uint16(payload[0]) shl 8) or uint16(payload[1])

  case typeVal

  of uint16(mtBeginTxn):
    let reqR = txnMsgs.decodeBeginTxnRequest(payload)
    if reqR.isErr:
      sendError(conn, requestId, ErrProtocol, ErrCatProtocol, $reqR.error)
      return
    let req = reqR.value

    if not server.mvccStore.isNil:
      # Use MVCC store to manage the transaction session
      let sessionId = server.mvccStore.createSession()
      let res = server.mvccStore.beginTransaction(sessionId)
      if res.isOk:
        # The TransactionID is generated by beginTransaction
        let txnId = res.value

        # Associate the session with this connection so we can roll back
        # on disconnect. If the connection already has a session, the new
        # one replaces it (the old session is closed first).
        let oldSessionId = conn.mvccSessionId
        conn.mvccSessionId = sessionId
        if oldSessionId != 0:
          server.mvccStore.closeSession(oldSessionId)

        # Use the requested timeout or a default if 0.
        let timeout = if req.timeoutMs > 0: req.timeoutMs else: 300_000'u32
        # Register in txnMgr as well so recordRead/recordWrite can work if needed
        discard server.txnMgr.beginTransaction(req.flags, timeout,
                                              forcedId = some(txnId))

        # Register in the active txn registry for liveness tracking
        if not server.activeTxnRegistry.isNil:
          server.activeTxnRegistry.register(txnId, sessionId)

        let resp = txnMsgs.BeginTxnResponse(
          txnId: txnId,
          readTimestamp: uint64(server.mvccStore.getCurrentTimestamp()),
        )
        sendFrame(conn, txnMsgs.encodeBeginTxnResponse(resp), requestId)
      else:
        server.mvccStore.closeSession(sessionId)
        sendError(conn, requestId, ErrInternal, ErrCatTransaction, res.error.msg)
    else:
      # Legacy fallback
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

    # First check txnMgr for idempotency (already committed txns)
    let preCheck = server.txnMgr.getTransactionStatus(txnId)
    if preCheck.status == txnMsgs.TxnStatusCommitted:
      # Already committed - return idempotent OK with same timestamp
      sendFrame(conn, txnMsgs.encodeCommitTxnResponse(
        txnMsgs.CommitTxnResponse(
          status: txnMsgs.TxnCommitOK,
          commitTimestamp: preCheck.commitTimestamp)), requestId)
      return

    if not server.mvccStore.isNil:
      # Mark as committing in the registry (prevents stale cleaner from aborting)
      if not server.activeTxnRegistry.isNil:
        server.activeTxnRegistry.setCommitting(txnId)

      let res = server.mvccStore.commitTransactionByTxnId(txnId)
      # Also update txnMgr state for this txnId
      let txnMgrResp = server.txnMgr.commitTransaction(txnId)

      if res.isOk:
        discard server.metrics.committedTxns.fetchAdd(1)
        # Remove from registry after successful commit
        if not server.activeTxnRegistry.isNil:
          server.activeTxnRegistry.unregister(txnId)
        sendFrame(conn, txnMsgs.encodeCommitTxnResponse(
          txnMsgs.CommitTxnResponse(
            status: txnMsgs.TxnCommitOK,
            commitTimestamp: uint64(res.value))), requestId)
        server.mvccStore.closeSessionByTxnId(txnId)
      else:
        # Remove from registry after failed commit
        if not server.activeTxnRegistry.isNil:
          server.activeTxnRegistry.unregister(txnId)
        # Use txnMgr response for idempotency check
        if txnMgrResp.status == txnMsgs.TxnCommitOK:
          discard server.metrics.committedTxns.fetchAdd(1)
          sendFrame(conn, txnMsgs.encodeCommitTxnResponse(txnMgrResp), requestId)
        else:
          discard server.metrics.abortedTxns.fetchAdd(1)
          # Determine proper status based on error kind
          let status = if res.error.kind == mseTransactionNotFound:
                         txnMsgs.TxnCommitNotFound
                       elif res.error.kind == mseTimeout:
                         txnMsgs.TxnCommitTimeout
                       else:
                         txnMsgs.TxnCommitConflict
          sendFrame(conn, txnMsgs.encodeCommitTxnResponse(
            txnMsgs.CommitTxnResponse(status: status)), requestId)
          if res.error.kind != mseTransactionNotFound:
            server.mvccStore.closeSessionByTxnId(txnId)
    else:
      let resp = server.txnMgr.commitTransaction(txnId)
      if resp.status == txnMsgs.TxnCommitOK:
        discard server.metrics.committedTxns.fetchAdd(1)
      else:
        discard server.metrics.abortedTxns.fetchAdd(1)
      sendFrame(conn, txnMsgs.encodeCommitTxnResponse(resp), requestId)

  of uint16(mtRollbackTxn):
    let reqR = txnMsgs.decodeRollbackTxnRequest(payload)
    if reqR.isErr:
      sendError(conn, requestId, ErrProtocol, ErrCatProtocol, $reqR.error)
      return
    let txnId = reqR.value.txnId

    if not server.mvccStore.isNil:
      # Mark as aborted in registry and queue intent cleanup
      if not server.activeTxnRegistry.isNil:
        server.activeTxnRegistry.setAborted(txnId)
      let res = server.mvccStore.rollbackTransactionByTxnId(txnId)
      discard server.txnMgr.rollbackTransaction(txnId)
      if res.isOk:
        server.mvccStore.closeSessionByTxnId(txnId)
        if not server.activeTxnRegistry.isNil:
          server.activeTxnRegistry.unregister(txnId)
        sendFrame(conn, txnMsgs.encodeRollbackTxnResponse(
          txnMsgs.RollbackTxnResponse(status: txnMsgs.TxnRollbackOK)), requestId)
      else:
        # Determine proper status based on error kind
        let status = if res.error.kind == mseTransactionNotFound or
                        res.error.kind == mseNotInTransaction:
                       txnMsgs.TxnRollbackNotFound
                     else:
                       txnMsgs.TxnRollbackOK # Other errors still return OK for idempotency
        sendFrame(conn, txnMsgs.encodeRollbackTxnResponse(
          txnMsgs.RollbackTxnResponse(status: status)), requestId)
    else:
      discard server.metrics.abortedTxns.fetchAdd(1)
      let resp = server.txnMgr.rollbackTransaction(txnId)
      sendFrame(conn, txnMsgs.encodeRollbackTxnResponse(resp), requestId)

  of uint16(mtTxnStatus):
    let reqR = txnMsgs.decodeTxnStatusRequest(payload)
    if reqR.isErr:
      sendError(conn, requestId, ErrProtocol, ErrCatProtocol, $reqR.error)
      return
    let resp = server.txnMgr.getTransactionStatus(reqR.value.txnId)
    sendFrame(conn, txnMsgs.encodeTxnStatusResponse(resp), requestId)

  of uint16(mtTxnKeepalive):
    let reqR = txnMsgs.decodeTxnKeepaliveRequest(payload)
    if reqR.isErr:
      sendError(conn, requestId, ErrProtocol, ErrCatProtocol, $reqR.error)
      return
    let txnId = reqR.value.txnId
    if not server.activeTxnRegistry.isNil:
      if server.activeTxnRegistry.hasTransaction(txnId):
        server.activeTxnRegistry.touchAsync(txnId)
        sendFrame(conn, txnMsgs.encodeTxnKeepaliveResponse(
          txnMsgs.TxnKeepaliveResponse(status: txnMsgs.TxnKeepaliveOK)),
          requestId)
      else:
        sendFrame(conn, txnMsgs.encodeTxnKeepaliveResponse(
          txnMsgs.TxnKeepaliveResponse(status: txnMsgs.TxnKeepaliveNotFound)),
          requestId)
    else:
      # No registry — always return OK (legacy behavior)
      sendFrame(conn, txnMsgs.encodeTxnKeepaliveResponse(
        txnMsgs.TxnKeepaliveResponse(status: txnMsgs.TxnKeepaliveOK)),
        requestId)

  else:
    sendError(conn, requestId, ErrProtocol, ErrCatTransaction,
      &"unknown txn message type 0x{typeVal:04X}")

# ---------------------------------------------------------------------------
# Built-in Admin handlers (ServerInfo, Metrics, Health)
# ---------------------------------------------------------------------------

proc handleBuiltinAdmin(server: ProtocolServer, conn: ClientConnection,
    requestId: uint32, flags: uint16,
    payload: string) {.gcsafe, raises: [].} =
  discard flags # Reserved for future use
  if payload.len < 2: return
  let typeVal = (uint16(payload[0]) shl 8) or uint16(payload[1])

  case typeVal

  of uint16(mtServerInfo):
    let nowSec = serverNowSec(server)
    let uptime = uint64(if nowSec > server.startedAt: nowSec -
        server.startedAt else: 0)
    let realShardCount: uint32 =
      if not server.raftStore.isNil: uint32(server.raftStore.groupCount())
      else: 1'u32
    let resp = adminMsgs.ServerInfoResponse(
      nodeId: server.config.serverId,
      version: server.config.serverVersion,
      uptimeSecs: uptime,
      role: adminMsgs.RoleLeader,
      groupCount: realShardCount,
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

# Forward declaration (addPeerToRaft is defined after handleBuiltinCluster
# but the RejoinNode handler needs to call it)
proc addPeerToRaft*(server: ProtocolServer, peerNodeId: uint32,
                   host: string, raftPort: int, clientPort: int = 0,
                   webPort: int = 0) {.gcsafe.}

proc handleBuiltinCluster(server: ProtocolServer, conn: ClientConnection,
    requestId: uint32, flags: uint16,
    payload: string) {.gcsafe, raises: [].} =
  discard flags # Reserved for future use
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

  of uint16(mtCreateSpace):
    # Handle CREATE SPACE - requires SpaceManager
    server.logger.logInfo("mtCreateSpace: handler called")
    if server.spaceManager.isNil:
      server.logger.logError("mtCreateSpace: spaceManager is nil")
      sendError(conn, requestId, ErrInternal, ErrCatSystem,
        "space manager not initialized")
      return
    let reqR = decodeCreateSpaceRequest(payload)
    if reqR.isErr:
      server.logger.logError("mtCreateSpace: failed to decode request: " & $reqR.error)
      sendError(conn, requestId, ErrProtocol, ErrCatProtocol, $reqR.error)
      return
    server.logger.logInfo("mtCreateSpace: request decoded, calling createSpace")
    # Execute with error protection
    var resp: spaceMsgs.CreateSpaceResponse
    try:
      resp = server.spaceManager.createSpace(reqR.value)
      server.logger.logInfo("mtCreateSpace: createSpace returned, success=" & $resp.success)
    except Exception as e:
      server.logger.logError("mtCreateSpace: exception: " & e.msg)
      resp = spaceMsgs.CreateSpaceResponse(
        success: false,
        error: "internal error: " & e.msg
      )
    except Defect as e:
      server.logger.logError("mtCreateSpace: DEFECT: " & $e.name & " - " & e.msg)
      resp = spaceMsgs.CreateSpaceResponse(
        success: false,
        error: "internal defect: " & e.msg
      )
    # Check for NOT_LEADER error and include redirect info
    if not resp.success and isNotLeaderError(resp.error):
      let redirect = server.getLeaderRedirect(META_GROUP_ID)
      sendNotLeaderError(conn, requestId, resp.error, redirect)
    else:
      server.logger.logInfo("mtCreateSpace: sending response frame")
      sendFrame(conn, encodeCreateSpaceResponse(resp), requestId)

  of uint16(mtDropSpace):
    # Handle DROP SPACE - requires SpaceManager
    if server.spaceManager.isNil:
      sendError(conn, requestId, ErrInternal, ErrCatSystem,
        "space manager not initialized")
      return
    let reqR = decodeDropSpaceRequest(payload)
    if reqR.isErr:
      sendError(conn, requestId, ErrProtocol, ErrCatProtocol, $reqR.error)
      return
    # Execute with error protection
    var resp: spaceMsgs.DropSpaceResponse
    try:
      resp = server.spaceManager.dropSpace(reqR.value)
    except Exception as e:
      resp = spaceMsgs.DropSpaceResponse(
        success: false,
        error: "internal error: " & e.msg
      )
    # Check for NOT_LEADER error and include redirect info
    if not resp.success and isNotLeaderError(resp.error):
      let redirect = server.getLeaderRedirect(META_GROUP_ID)
      sendNotLeaderError(conn, requestId, resp.error, redirect)
    else:
      sendFrame(conn, encodeDropSpaceResponse(resp), requestId)

  of uint16(mtCreateGroup):
    # Handle CreateGroup - directed group creation request
    # The meta leader sends this to the preferred leader node
    let reqR = clusterMsgs.decodeCreateGroupRequest(payload)
    if reqR.isErr:
      sendError(conn, requestId, ErrProtocol, ErrCatProtocol, $reqR.error)
      return
    let req = reqR.value

    # Check if we have the raftStore
    if server.raftStore.isNil:
      let resp = clusterMsgs.CreateGroupResponse(
        success: false,
        error: "raft store not initialized"
      )
      sendFrame(conn, clusterMsgs.encodeCreateGroupResponse(resp), requestId)
      return

    # Convert groupId bytes to GroupID
    let groupId = groupIDFromULID(ulidFromBytes(req.groupId))

    # Check if group already exists
    if server.raftStore.coordinator.hasGroup(groupId):
      let resp = clusterMsgs.CreateGroupResponse(
        success: true,
        groupId: req.groupId
      )
      sendFrame(conn, clusterMsgs.encodeCreateGroupResponse(resp), requestId)
      return

    # Build member list for coordinator
    var nuraftMembers: seq[tuple[nodeId: uint32, host: string, port: int]] = @[]
    for m in req.members:
      nuraftMembers.add((nodeId: uint32(m.nodeId), host: m.host, port: int(m.raftPort)))

    # Create the group - this node becomes leader by winning election
    var resp: clusterMsgs.CreateGroupResponse
    try:
      let ok = server.raftStore.coordinator.createAndStartGroup(
        groupId, nuraftMembers, uint32(req.preferredLeaderId))
      if ok:
        server.raftStore.registerGroup(groupId)

        # CRITICAL: Send JoinGroup RPCs to all other members BEFORE election timer fires.
        # Wait for each response to ensure the member has created its instance.
        # This ensures all members can vote when the preferred leader's election timer fires.
        let myNodeIdForJoin = server.config.serverId
        for m in req.members:
          if uint32(m.nodeId) == uint32(req.preferredLeaderId):
            continue # Skip ourselves (we just created it)
          if m.host.len == 0:
            continue
          try:
            let joinReq = clusterMsgs.JoinGroupRequest(
              groupId: req.groupId,
              creatorNodeId: uint16(req.preferredLeaderId),
              creatorHost: server.config.host,
              creatorPort: uint16(server.raftCoord.port),
              members: req.members
            )
            let cfg = ClientConfig(
              host: m.host,
              port: int(m.clientPort),
              timeoutMs: 5000
            )
            let pc = newProtocolClient(cfg)
            let cr = pc.connect()
            if cr.isOk:
              defer: pc.disconnect()
              let jr = pc.joinGroup(joinReq)
              # Wait for response - member must have created instance before we continue
              discard jr
          except CatchableError:
            discard # Ignore errors - members can join later

        # Wait for leader election to complete (election timeout is 150-500ms)
        # A valid leader is either:
        # - This node is the leader (isLeader returns true), OR
        # - getLeader returns a positive ID that is NOT this node
        var leaderElected = false
        let myNodeId = int(server.config.serverId)
        for i in 0 ..< 50: # 50 * 20ms = 1 second max
          if server.raftCoord.isLeader(groupId):
            leaderElected = true
            break
          let leaderId = server.raftCoord.getLeader(groupId)
          if leaderId > 0 and leaderId != myNodeId:
            leaderElected = true
            break
          sleep(20)

        if not leaderElected:
          # Log warning but continue - leader may be elected later
          discard

        resp = clusterMsgs.CreateGroupResponse(
          success: true,
          groupId: req.groupId
        )
      else:
        resp = clusterMsgs.CreateGroupResponse(
          success: false,
          error: "failed to create Raft group"
        )
    except Exception as e:
      resp = clusterMsgs.CreateGroupResponse(
        success: false,
        error: "exception: " & e.msg
      )

    sendFrame(conn, clusterMsgs.encodeCreateGroupResponse(resp), requestId)

  of uint16(mtJoinGroup):
    # Handle JoinGroup - request to join an existing Raft group
    # The preferred leader sends this to other member nodes
    let reqR = clusterMsgs.decodeJoinGroupRequest(payload)
    if reqR.isErr:
      sendError(conn, requestId, ErrProtocol, ErrCatProtocol, $reqR.error)
      return
    let req = reqR.value

    # Check if we have the raftStore
    if server.raftStore.isNil:
      let resp = clusterMsgs.JoinGroupResponse(
        success: false,
        error: "raft store not initialized"
      )
      sendFrame(conn, clusterMsgs.encodeJoinGroupResponse(resp), requestId)
      return

    # Convert groupId bytes to GroupID
    let groupId = groupIDFromULID(ulidFromBytes(req.groupId))

    # Check if group already exists
    if server.raftStore.coordinator.hasGroup(groupId):
      let resp = clusterMsgs.JoinGroupResponse(
        success: true,
        groupId: req.groupId
      )
      sendFrame(conn, clusterMsgs.encodeJoinGroupResponse(resp), requestId)
      return

    # Build member list - we join the existing group created by creatorNodeId
    # IMPORTANT: First try to use the members from the request (most reliable).
    # If not available, fall back to looking up from sys.groups.
    var nuraftMembers: seq[tuple[nodeId: uint32, host: string, port: int]] = @[]

    let myNodeId = server.config.serverId
    let myRaftPort = server.raftCoord.port

    # First, try to use members from the request (if provided)
    if req.members.len > 0:
      for m in req.members:
        nuraftMembers.add((nodeId: uint32(m.nodeId),
                           host: m.host,
                           port: int(m.raftPort)))
        # Also add to node registry for correct leader redirect info
        if server.nodeRegistry != nil and m.clientPort > 0:
          server.nodeRegistry.addNode(ClusterNodeEntry(
            nodeId: m.nodeId,
            host: m.host,
            raftPort: m.raftPort,
            clientPort: m.clientPort,
            status: clusterMsgs.NodeStatusActive
          ))

    # If no members in request, look up the group record from sys.groups
    if nuraftMembers.len == 0:
      let groupKey = encodeTableKey(SYS_GROUPS_TABLE_ID, $groupId)
      let groupRes = server.raftStore.raftGet(groupKey)

      if groupRes.isOk and groupRes.value.isSome:
        # Found the group record - use all members from it
        let groupEntry = groupRes.value.get()
        let groupData = groupEntry.value
        var groupRec: GroupRecord
        var decoded = false

        # Try MVCC-aware binary decoding first
        try:
          let (rec, _) = decodeGroupRecordFromMVCC(groupData)
          groupRec = rec
          decoded = true
        except:
          discard

        if not decoded:
          # Try raw binary decoding
          try:
            groupRec = decodeGroupRecord(groupData)
            decoded = true
          except:
            discard

        if decoded:
          # Build member list from group record
          for rep in groupRec.replicas:
            let peerInfo = server.raftCoord.peerInfo.getOrDefault(rep.nodeId,
                (host: server.config.host, port: myRaftPort))
            nuraftMembers.add((nodeId: rep.nodeId,
                               host: peerInfo.host,
                               port: peerInfo.port))

    if nuraftMembers.len == 0:
      # Fallback: use creator + ourselves (this should not happen if sys.groups is correct)
      nuraftMembers.add((nodeId: uint32(req.creatorNodeId),
                         host: req.creatorHost,
                         port: int(req.creatorPort)))
      nuraftMembers.add((nodeId: uint32(myNodeId),
                         host: server.config.host,
                         port: myRaftPort))

    # Create the group - we join as a follower
    var resp: clusterMsgs.JoinGroupResponse
    try:
      let ok = server.raftStore.coordinator.createAndStartGroup(
        groupId, nuraftMembers, uint32(req.creatorNodeId))
      if ok:
        server.raftStore.registerGroup(groupId)
        resp = clusterMsgs.JoinGroupResponse(
          success: true,
          groupId: req.groupId
        )
      else:
        resp = clusterMsgs.JoinGroupResponse(
          success: false,
          error: "failed to join Raft group"
        )
    except Exception as e:
      resp = clusterMsgs.JoinGroupResponse(
        success: false,
        error: "exception: " & e.msg
      )

    sendFrame(conn, clusterMsgs.encodeJoinGroupResponse(resp), requestId)

  of uint16(mtFindMetaLeader):
    # Any node can answer this — return who we think the meta leader is
    var resp: clusterMsgs.FindMetaLeaderResponse
    if not server.raftCoord.isNil and server.raftCoord.running.load():
      let leaderId = server.raftCoord.getLeader(META_GROUP_ID)
      if leaderId > 0:
        resp.leaderKnown = true
        resp.leaderNodeId = uint16(leaderId)
        # Look up host and client port
        let redirect = server.getLeaderRedirect(META_GROUP_ID)
        resp.leaderHost = redirect.leaderHost
        resp.leaderClientPort = redirect.leaderClientPort
    sendFrame(conn, clusterMsgs.encodeFindMetaLeaderResponse(resp), requestId)

  of uint16(mtRejoinNode):
    # Meta-leader-only operation: re-add a returning node to all its groups
    let reqR = clusterMsgs.decodeRejoinNodeRequest(payload)
    if reqR.isErr:
      sendError(conn, requestId, ErrProtocol, ErrCatProtocol, $reqR.error)
      return
    let req = reqR.value

    # Only the meta leader can process rejoin requests
    if server.raftCoord.isNil or not server.raftCoord.isLeader(META_GROUP_ID):
      let redirect = server.getLeaderRedirect(META_GROUP_ID)
      sendNotLeaderError(conn, requestId,
          "not the meta leader, cannot process rejoin", redirect)
      return

    var resp: clusterMsgs.RejoinNodeResponse
    try:
      # Re-add the node via addPeerToRaft (handles add_srv + JoinGroup RPCs)
      server.addPeerToRaft(
        uint32(req.nodeId),
        req.host,
        int(req.raftPort),
        clientPort = int(req.clientPort),
        webPort = 0
      )
      # Collect all groups this node is now a member of
      var rejoinedGroups: seq[string] = @[]
      withLock server.raftCoord.groupsLock:
        for gid, inst in server.raftCoord.groups:
          # Convert GroupID to 16-byte ULID binary string (safe for null bytes)
          let ulid = groupIDToULID(gid)
          rejoinedGroups.add(ulidToBytes(ulid))
      resp = clusterMsgs.RejoinNodeResponse(
        success: true,
        groupIds: rejoinedGroups
      )
    except Exception as e:
      resp = clusterMsgs.RejoinNodeResponse(
        success: false,
        error: "rejoin failed: " & e.msg
      )
    sendFrame(conn, clusterMsgs.encodeRejoinNodeResponse(resp), requestId)

  of uint16(mtAddServerToGroup):
    # Forward add_srv to the group leader (called by meta leader when it's not
    # the leader of a specific group).
    let reqR = clusterMsgs.decodeAddServerToGroupRequest(payload)
    if reqR.isErr:
      sendError(conn, requestId, ErrProtocol, ErrCatProtocol, $reqR.error)
      return
    let req = reqR.value
    var resp: clusterMsgs.AddServerToGroupResponse
    try:
      let groupId = groupIDFromBytes(req.groupId)
      if server.raftCoord.isNil:
        resp = clusterMsgs.AddServerToGroupResponse(
          success: false, error: "coordinator not initialized")
      elif not server.raftCoord.hasGroup(groupId):
        resp = clusterMsgs.AddServerToGroupResponse(
          success: false, error: "group not found on this node")
      elif not server.raftCoord.isLeader(groupId):
        let redirect = server.getLeaderRedirect(groupId)
        resp = clusterMsgs.AddServerToGroupResponse(
          success: false,
          error: "not the leader of group, leader is " & $redirect.leaderId)
      else:
        let rc = server.raftCoord.addServerToGroup(
          groupId, uint32(req.serverId), req.host, int(req.raftPort))
        if rc >= 0:
          resp = clusterMsgs.AddServerToGroupResponse(success: true)
        else:
          resp = clusterMsgs.AddServerToGroupResponse(
            success: false, error: "add_srv returned " & $rc)
    except Exception as e:
      resp = clusterMsgs.AddServerToGroupResponse(
        success: false, error: "exception: " & e.msg)
    sendFrame(conn, clusterMsgs.encodeAddServerToGroupResponse(resp), requestId)

  else:
    sendError(conn, requestId, ErrProtocol, ErrCatSystem,
      &"unknown cluster message type 0x{typeVal:04X}")

# ---------------------------------------------------------------------------
# Per-connection loop
# ---------------------------------------------------------------------------

proc clientLoop(server: ProtocolServer,
    conn: ClientConnection) {.gcsafe, raises: [].} =
  defer:
    # Roll back any pending MVCC transaction when the client disconnects.
    # closeSession calls rollbackTransaction internally for any TXN_PENDING txn.
    if not server.mvccStore.isNil and conn.mvccSessionId != 0:
      server.mvccStore.closeSession(conn.mvccSessionId)
      conn.mvccSessionId = 0
    # Clean up the connection lock
    try: deinitLock(conn.mu) except CatchableError: discard
    try: conn.socket.close() except CatchableError: discard
    removeClient(server, conn.id)
    server.logger.logInfo(&"[{conn.address}] disconnected (id={conn.id})")

  server.logger.logInfo(&"[{conn.address}] connected (id={conn.id})")

  if not performHandshake(server, conn):
    return

  server.logger.logInfo(&"[{conn.address}] handshake OK (id={conn.id})")

  while server.running.load():
    if conn.isIdle(server.config.idleTimeoutSecs, server):
      server.logger.logInfo(&"[{conn.address}] idle timeout")
      break

    # Read frame with short timeout (500ms) to allow periodic idle checks
    let frameR = readOneFrame(conn.socket, server.config.maxFrameBytes, 500)
    if frameR.isErr:
      let e = frameR.error
      # On timeout, just loop back and check idle status again.
      # On EOF (peer closed connection), break out of the loop.
      if e.kind == peInvalidFrame and "eof" in $e:
        # Peer closed the connection — exit the loop
        break
      if e.kind == peInvalidFrame and "short header" in $e:
        # Timeout - no data received within 500ms
        # Loop back to check running flag and idle timeout
        continue
      if e.kind != peInternal:
        sendError(conn, 0, ErrProtocol, ErrCatProtocol, $e)
        discard server.metrics.requestsErr.fetchAdd(1)
      break

    let f = frameR.value
    conn.touchActivity(server)

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
  try:
    clientLoop(args.srv, args.conn)
  finally:
    discard args.srv.clientThreadCount.fetchSub(1)

proc acceptLoop(args: AcceptLoopArgs) {.thread.} =
  let server = args.srv
  let sock = args.sock
  try:
    while server.running.load():
      # Use select to poll with timeout so we can check running flag
      var fds = @[sock.getFd()]
      let ready = nativesockets.selectRead(fds, 100) # 100ms timeout
      if ready <= 0:
        # Timeout or no data, just loop again to check running flag
        continue

      var clientSock: Socket
      var address = ""
      try:
        sock.accept(clientSock)
        let (peerAddr, _) = clientSock.getPeerAddr()
        address = peerAddr

        # Set client socket to non-blocking mode for all subsequent recv operations
        # This prevents the per-client thread from blocking indefinitely
        let clientFd = clientSock.getFd().cint
        if not setSocketNonBlocking(clientFd):
          server.logger.logWarn("failed to set client socket non-blocking: " & address)
          try: clientSock.close() except CatchableError: discard
          continue
      except CatchableError as e:
        if server.running.load():
          server.logger.logWarn("accept error: " & e.msg)
        break

      if server.clientCount() >= server.config.maxConnections:
        server.logger.logWarn(&"max connections reached, rejecting {address}")
        discard server.metrics.connectionsRejected.fetchAdd(1)
        try: clientSock.close() except CatchableError: discard
        continue

      let id = server.nextClientId.fetchAdd(1)
      let conn = newClientConnection(id, clientSock, address, server)
      server.addClient(conn)
      discard server.metrics.connectionsAccepted.fetchAdd(1)

      # Allocate a heap-resident Thread so its lifetime is not tied to this
      # stack frame.  Store in the module-level threadStore so GC won't collect.
      let tRef = new Thread[ClientLoopArgs]
      {.cast(gcsafe).}:
        withLock(threadStoreMu):
          threadStore.add(tRef)
        discard server.clientThreadCount.fetchAdd(1)
      createThread(tRef[], clientLoopThread, (server, conn))
  finally:
    discard server.acceptThreadCount.fetchSub(1)

# ---------------------------------------------------------------------------
# Cluster membership persistence
# ---------------------------------------------------------------------------

proc clusterStatePath(server: ProtocolServer): string =
  server.config.dataDir / "cluster.bin"

proc saveClusterState*(server: ProtocolServer) =
  ## Persist current cluster membership to disk so a restarted node can
  ## rejoin as a follower without requiring --join.
  ## Uses binary serialization for efficiency.
  if server.config.dataDir == "":
    return
  let coord = server.raftCoord
  if coord.isNil:
    return

  # Build persisted cluster state
  var state = newPersistedClusterState()
  state.self = SelfNodeInfo(
    nodeId: uint32(server.config.serverId),
    host: server.config.host,
    clientPort: server.config.port,
    webPort: server.config.webPort
  )

  for nid, info in coord.peerInfo:
    # Look up client port from node registry or sys.nodes KV store
    var peerClientPort = 0
    if server.nodeRegistry != nil:
      withLock server.nodeRegistry.mu:
        if uint16(nid) in server.nodeRegistry.nodes:
          peerClientPort = int(server.nodeRegistry.nodes[uint16(
              nid)].clientPort)
    if peerClientPort == 0:
      let (_, kvPort) = server.lookupNodeFromKVStore(nid)
      if kvPort > 0:
        peerClientPort = int(kvPort)
    state.peers[nid] = (host: info.host, port: info.port,
        clientPort: peerClientPort)

  try:
    saveClusterStateToFile(state, server.clusterStatePath)
  except CatchableError:
    discard

proc loadClusterStateFromBinary(dataDir: string): Option[
    PersistedClusterState] =
  ## Load saved cluster membership from binary file.
  ## Returns none if no state exists or file is invalid.
  let path = dataDir / "cluster.bin"
  loadClusterStateFromFile(path)

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

proc addPeerToRaft*(server: ProtocolServer, peerNodeId: uint32,
                     host: string, raftPort: int, clientPort: int = 0,
                     webPort: int = 0) =
  ## Dynamically add a peer to the NuRaft coordinator for system groups.
  ## Called when a new node joins the cluster.
  ## Also inserts the node into sys.nodes table (only if this node is the leader).
  ## CRITICAL: Sends JoinGroup RPCs to the joining node BEFORE calling add_srv,
  ## so the joining node has its local groups ready when heartbeats arrive.
  let coord = server.raftCoord
  if coord.isNil: return

  # Only the meta leader should drive peer addition.
  # CRITICAL: During cluster startup, the seed node may not have won the META
  # group election yet (election timeout is 300-600ms). If we bail immediately,
  # JoinGroup RPCs are never sent and the joining node falls back to creating
  # a single-member group, causing split-brain with the same GroupID.
  # Wait up to 1.5 seconds for the election to complete.
  var isMetaLeader = false
  for attempt in 0 ..< 15:
    if coord.isLeader(META_GROUP_ID):
      isMetaLeader = true
      break
    if attempt == 0:
      echo "[addPeerToRaft] waiting for META leader election before adding peerNodeId=", peerNodeId
    sleep(100)
  if not isMetaLeader:
    echo "[addPeerToRaft] NOT meta leader after 1.5s, skipping add_srv for peerNodeId=", peerNodeId
    # Still register peer info so this node knows about the new peer
    coord.peerInfo[peerNodeId] = (host: host, port: raftPort)
    return

  echo "[addPeerToRaft] meta leader, adding peerNodeId=", peerNodeId, " host=",
      host, " raftPort=", raftPort

  # Register peer info for future group creation
  coord.peerInfo[peerNodeId] = (host: host, port: raftPort)

  # Also add to the node registry so redirect info includes correct client port
  if server.nodeRegistry != nil and clientPort > 0:
    server.nodeRegistry.addNode(ClusterNodeEntry(
      nodeId: uint16(peerNodeId),
      host: host,
      raftPort: uint16(raftPort),
      clientPort: uint16(clientPort),
      webPort: uint16(webPort),
      status: clusterMsgs.NodeStatusActive
    ))

  # Build members list - includes SELF (leader) + all known peers
  var members: seq[clusterMsgs.CreateGroupMember] = @[]

  # Helper to add a node to the members list
  template addMemberToList(mnid: uint32, mhost: string, mraftPort: int,
      mclientPort: int) =
    members.add(clusterMsgs.CreateGroupMember(
      nodeId: uint16(mnid),
      host: mhost,
      raftPort: uint16(mraftPort),
      clientPort: uint16(mclientPort)
    ))

  # Add ourselves (the leader) first
  addMemberToList(
    uint32(server.config.serverId),
    server.config.host,
    coord.port,
    server.config.port
  )

  # Add all known peers
  for (nodeId, peerData) in coord.peerInfo.pairs:
    # Look up the correct client port from the node registry or KV store.
    # peerData.port is the RAFT port, NOT the client port — deriving it
    # as raft+100 is wrong because the mapping is config-dependent.
    var memberClientPort = uint16(0)
    if nodeId == peerNodeId:
      memberClientPort = uint16(clientPort)
    elif nodeId == uint32(server.config.serverId):
      memberClientPort = uint16(server.config.port)
    else:
      # Try node registry first (fast, in-memory)
      if server.nodeRegistry != nil:
        withLock server.nodeRegistry.mu:
          if uint16(nodeId) in server.nodeRegistry.nodes:
            memberClientPort = server.nodeRegistry.nodes[uint16(
                nodeId)].clientPort
      # Fall back to sys.nodes KV store
      if memberClientPort == 0:
        let (kvHost, kvPort) = server.lookupNodeFromKVStore(nodeId)
        if kvPort > 0:
          memberClientPort = kvPort
    addMemberToList(nodeId, peerData.host, peerData.port, int(memberClientPort))

  # STEP 1: Send JoinGroup RPCs to the joining node BEFORE add_srv.
  # This ensures the joining node creates its local Raft groups before
  # the leader starts sending heartbeats. Without this, the leader may
  # try to send heartbeats to a node that doesn't have the group yet,
  # causing heartbeat failures and election timeouts that lead to
  # leader redirect loops.
  let myNodeId = server.config.serverId
  let myRaftPort = coord.port
  let myHost = server.config.host

  for groupId in [META_GROUP_ID, DATA_GROUP_START_ID]:
    let groupUlid = groupIDToULID(groupId)
    let groupIdBytes = ulidToBytes(groupUlid)

    let joinReq = clusterMsgs.JoinGroupRequest(
      groupId: groupIdBytes,
      creatorNodeId: uint16(myNodeId),
      creatorHost: myHost,
      creatorPort: uint16(myRaftPort),
      members: members
    )

    # Determine client port for connecting to the joining node
    var targetClientPort = clientPort
    if targetClientPort == 0 and server.nodeRegistry != nil:
      withLock server.nodeRegistry.mu:
        if uint16(peerNodeId) in server.nodeRegistry.nodes:
          targetClientPort = int(server.nodeRegistry.nodes[uint16(
              peerNodeId)].clientPort)
    if targetClientPort == 0:
      let (_, kvPort) = server.lookupNodeFromKVStore(peerNodeId)
      if kvPort > 0:
        targetClientPort = int(kvPort)
    if targetClientPort == 0:
      # Cannot determine client port — skip sending JoinGroup RPC
      continue
    try:
      let cfg = ClientConfig(
        host: host,
        port: targetClientPort,
        timeoutMs: 5000
      )
      let pc = newProtocolClient(cfg)
      let cr = pc.connect()
      if cr.isOk:
        defer: pc.disconnect()
        discard pc.joinGroup(joinReq)
    except CatchableError:
      discard

  # STEP 2: Add the peer as a server to the meta and default data groups.
  # Now that the joining node has its local groups, add_srv will trigger
  # heartbeats that the joining node can actually receive.
  # CRITICAL: The data group leader may not be elected yet (election timeout
  # 300-600ms). Retry up to 3 times with a short delay to avoid silently
  # skipping the data group addition.
  discard coord.addServerToGroup(META_GROUP_ID, peerNodeId, host, raftPort)
  for attempt in 0 .. 2:
    if coord.isLeader(DATA_GROUP_START_ID):
      let dataRc = coord.addServerToGroup(DATA_GROUP_START_ID, peerNodeId,
          host, raftPort)
      break
    else:
      let dataLeaderId = coord.getLeader(DATA_GROUP_START_ID)
      if dataLeaderId > 0:
        # Forward add_srv to the data group leader
        let dataLeaderInfo = coord.peerInfo.getOrDefault(uint32(dataLeaderId),
            (host: "", port: 0))
        if dataLeaderInfo.host.len > 0 and dataLeaderInfo.port > 0:
          var dataLeaderClientPort = 0
          # Look up client port from node registry or KV store
          if server.nodeRegistry != nil:
            withLock server.nodeRegistry.mu:
              if uint16(dataLeaderId) in server.nodeRegistry.nodes:
                dataLeaderClientPort = int(
                    server.nodeRegistry.nodes[uint16(dataLeaderId)].clientPort)
          if dataLeaderClientPort == 0:
            let (kvHost, kvPort) = server.lookupNodeFromKVStore(
                uint32(dataLeaderId))
            if kvPort > 0:
              dataLeaderClientPort = int(kvPort)
          if dataLeaderClientPort > 0:
            try:
              let cfg = ClientConfig(
                host: dataLeaderInfo.host,
                port: dataLeaderClientPort,
                timeoutMs: 5000
              )
              let pc = newProtocolClient(cfg)
              let cr = pc.connect()
              if cr.isOk:
                defer: pc.disconnect()
                let groupUlid = groupIDToULID(DATA_GROUP_START_ID)
                let groupIdBytes = ulidToBytes(groupUlid)
                let addSrvReq = clusterMsgs.AddServerToGroupRequest(
                  groupId: groupIdBytes,
                  serverId: uint16(peerNodeId),
                  host: host,
                  raftPort: uint16(raftPort)
                )
                let addSrvResp = pc.addServerToGroup(addSrvReq)
                if addSrvResp.isOk and addSrvResp.value.success:
                  break
            except CatchableError:
              discard
        break # data leader exists but we can't forward - don't retry
      # No data group leader yet, wait and retry
      sleep(200)

  # Only insert into sys.nodes and update sys.groups if we're the meta group leader
  if coord.isLeader(META_GROUP_ID):
    let raftStore = server.raftStore
    if not raftStore.isNil:
      let backend = raftStore.getBackend()
      if not backend.isNil and backend.isOpen:
        let nodeKey = encodeTableKey(SYS_NODES_TABLE_ID, $peerNodeId)
        let nodeRec = NodeRecord(
          nodeId: uint16(peerNodeId),
          host: host,
          raftPort: uint16(raftPort),
          clientPort: uint16(clientPort),
          webPort: uint16(webPort),
          status: nsAlive
        )

        # Use MicroTransaction for atomic node join:
        # write node record + update group replicas in a single Raft proposal
        try:
          let txn = raftStore.beginSysTxn()
          txn.put(nodeKey, encode(nodeRec))

          for groupId in [META_GROUP_ID, DATA_GROUP_START_ID]:
            let groupKey = encodeTableKey(SYS_GROUPS_TABLE_ID, $groupId)
            let valOpt = backend.get(groupKey)
            if valOpt.isSome:
              let (payload, isDeleted) = stripMVCCHeader(valOpt.get)
              if not isDeleted and payload.len > 0:
                let currentRec = decodeGroupRecord(payload)
                var alreadyExists = false
                for rep in currentRec.replicas:
                  if rep.nodeId == peerNodeId:
                    alreadyExists = true
                    break
                if not alreadyExists:
                  var updatedRec = currentRec
                  updatedRec.replicas.add(GroupReplicaBin(
                    nodeId: peerNodeId,
                    replicaType: rtVoter
                  ))
                  txn.put(groupKey, encode(updatedRec))

          let result = txn.commit()
          if not result.isOk:
            try:
              {.cast(gcsafe).}:
                var fields = initTable[string, string]()
                fields["error"] = $result.error
                error("Failed to commit node join transaction", fields)
            except CatchableError:
              discard
        except CatchableError:
          discard

  # Persist membership so restarts can rejoin without --join
  server.saveClusterState()

proc setupRaftNode*(server: ProtocolServer, raftPort: int,
                    startAsLeader: bool = true) {.raises: [Exception].} =
  ## Wire a NuRaft + WiscKey stack into the server.
  ## raftPort is the base port for NuRaft ASIO (group ports = raftPort + groupId).
  ## startAsLeader: when true and no peers, this is a fresh single-node cluster.
  ## When false (joining mode), groups are created later via addPeerToRaft.

  let nodeId = NodeID(uint32(server.config.serverId))
  let raftDir = server.config.dataDir / "raft"

  createDir(raftDir)

  # Peer info: (nodeId, host, raft port, client port) tuples
  type PeerInfo = tuple[nodeId: uint32, host: string, port: int,
      clientPort: int]
  var peers: seq[PeerInfo] = @[]

  # Check for saved cluster state (from a previous run as part of a cluster).
  var isRejoining = false
  if startAsLeader:
    let saved = loadClusterStateFromBinary(server.config.dataDir)
    if saved.isSome:
      let ss = saved.get
      if ss.peers.len > 0:
        isRejoining = true
        for nid, info in ss.peers.pairs():
          # Skip self - we don't want to include ourselves in the peers list
          if nid > 0 and nid != nodeId.uint32 and info.host != "" and
              info.port > 0:
            peers.add((nodeId: nid, host: info.host, port: info.port,
                      clientPort: info.clientPort))

  let isJoining = not startAsLeader

  # NuRaft Coordinator
  let coord = newNuRaftCoordinator(nuraft_coordinator.CoordinatorConfig(
    nodeId: nodeId,
    port: raftPort,
    host: server.config.host,
    dataDir: raftDir,
    electionTimeoutLowerMs: 300,
    electionTimeoutUpperMs: 600,
    heartbeatIntervalMs: 100,
    writeBufferSize: server.config.writeBufferSize,
    blockCacheSize: server.config.blockCacheSize,
    vlogMaxSize: server.config.vlogMaxSize,
    vlogCleanThreshold: server.config.vlogCleanThreshold,
    vlogMinCleanThreshold: server.config.vlogMinCleanThreshold,
    vlogCleanBufferSize: server.config.vlogCleanBufferSize,
  ))
  server.raftCoord = coord

  # Register peer info in coordinator
  for p in peers:
    coord.peerInfo[p.nodeId] = (host: p.host, port: p.port)

  # Build member lists for the initial Raft groups
  # Each group includes self + all known peers
  var initialMembers: seq[tuple[nodeId: uint32, host: string,
      port: int]] = @[]
  initialMembers.add((nodeId: nodeId.uint32, host: server.config.host,
      port: raftPort))
  for p in peers:
    initialMembers.add((nodeId: p.nodeId, host: p.host, port: p.port))

  # KV store MUST be created before groups start, otherwise log replay from
  # leader will be dropped by nuraftCommitCb because kvStorePtr is nil!
  let store = newRaftKVStoreExt(coord)
  server.raftStore = store

  # Create MvccTransactionStore for full MVCC semantics on all writes
  let tsProvider = newTimestampProvider(server.sharedTimer,
      server.config.serverId)
  let mvccStore = newMvccTransactionStore(store, server.txnMgr, tsProvider)
  server.mvccStore = mvccStore
  store.setTimestampProvider(tsProvider)

  # Wire the active txn registry into the MVCC store (for addIntentKey)
  if not server.activeTxnRegistry.isNil:
    server.activeTxnRegistry.setBackendPtr(cast[pointer](coord.store))
    mvccStore.setActiveTxnRegistryPtr(cast[pointer](server.activeTxnRegistry))

  # Wire the active transaction checker so the intent scavenger can
  # check if a transaction is still in-flight before removing its intents.
  store.setActiveTxnChecker(proc(txnId: TransactionID): bool {.gcsafe, raises: [].} =
    # Check both the MVCC session store and the in-memory txn manager
    let sessionOpt = mvccStore.getSessionIdByTxnId(txnId)
    if sessionOpt.isSome:
      let stateOpt = mvccStore.getSessionState(sessionOpt.get())
      if stateOpt.isSome and stateOpt.get().txn != nil and
          stateOpt.get().txn.status == mvccValueTypes.TXN_PENDING:
        return true
    return false
  )

  # Wire callbacks and pre-create state machines before starting NuRaft
  store.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])

  # When joining or rejoining, don't create groups yet - they already exist
  # on the cluster. We'll receive append_entries from the leaders.
  # - isJoining: explicit --join flag, groups created by addPeerToRaft
  # - isRejoining: saved cluster.bin with peers, groups exist on other nodes
  if not isJoining and not isRejoining:
    # Create NuRaft groups: meta group (Group 1) and data group (Group 2)
    # This is for a fresh single-node cluster start
    for groupId in [META_GROUP_ID, DATA_GROUP_START_ID]:
      discard coord.createAndStartGroup(groupId, initialMembers)
  elif isRejoining:
    # Rejoin protocol: create groups with members from cluster.bin so the
    # node can immediately receive heartbeats and append_entries from the
    # leader. Use the first peer as preferredLeader so the node starts in
    # follower mode (skipInitialElection=true) instead of triggering an
    # election that would disrupt the cluster.
    var rejoinPreferredLeader: uint32 = 0
    if peers.len > 0:
      rejoinPreferredLeader = peers[0].nodeId
    for groupId in [META_GROUP_ID, DATA_GROUP_START_ID]:
      discard coord.createAndStartGroup(groupId, initialMembers,
          rejoinPreferredLeader)

  coord.start()

  if not server.sharedTimer.isNil:
    coord.setTimeProvider(server.sharedTimer)

  if isRejoining and peers.len > 0:
    # RejoinNode RPC: tell the meta leader to re-add us to the Raft config
    # so it starts sending append_entries. This is an optimization — the
    # groups already exist (created above), but without add_srv the leader
    # doesn't know to replicate to us.
    # Retry a few times because the leader might be mid-election.
    var rejoinSucceeded = false
    for attempt in 0 ..< 3:
      if rejoinSucceeded: break
      if attempt > 0:
        sleep(500)
      # Ask each peer for the meta leader until we find one
      var metaLeaderHost = ""
      var metaLeaderClientPort = 0
      for p in peers:
        if metaLeaderHost.len > 0: break
        let peerClientPort = if p.clientPort > 0: p.clientPort else: p.port + 100
        try:
          let cfg = ClientConfig(
            host: p.host,
            port: peerClientPort,
            timeoutMs: 2000
          )
          let pc = newProtocolClient(cfg)
          let cr = pc.connect()
          if cr.isOk:
            defer: pc.disconnect()
            let leaderResp = pc.findMetaLeader()
            if leaderResp.isOk and leaderResp.value.leaderKnown:
              metaLeaderHost = leaderResp.value.leaderHost
              metaLeaderClientPort = int(leaderResp.value.leaderClientPort)
        except CatchableError:
          discard

      if metaLeaderHost.len > 0 and metaLeaderClientPort > 0:
        try:
          let cfg = ClientConfig(
            host: metaLeaderHost,
            port: metaLeaderClientPort,
            timeoutMs: 5000
          )
          let pc = newProtocolClient(cfg)
          let cr = pc.connect()
          if cr.isOk:
            defer: pc.disconnect()
            let rejoinReq = clusterMsgs.RejoinNodeRequest(
              nodeId: uint16(server.config.serverId),
              host: server.config.host,
              raftPort: uint16(raftPort),
              clientPort: uint16(server.config.port)
            )
            let rejoinResp = pc.rejoinNode(rejoinReq)
            if rejoinResp.isOk and rejoinResp.value.success:
              rejoinSucceeded = true
        except CatchableError:
          discard

    if not rejoinSucceeded:
      # Groups were created with cluster.bin members above, so the node can
      # still catch up via append_entries once the leader recognizes it.
      # This is a degraded path — the leader won't actively replicate to us
      # until it adds us via add_srv (which happens when it detects our
      # heartbeat response or when a human operator triggers add_srv).
      discard

  # For rejoining nodes, wait for leaders to be known and for some data to
  # replicate before loading space metadata. The groups already exist (created
  # above), so we just need to wait for append_entries to arrive.
  if isRejoining:
    # Wait for leaders to be known (max 5 seconds)
    for i in 0 ..< 50:
      let metaLeader = coord.getLeader(META_GROUP_ID)
      let dataLeader = coord.getLeader(DATA_GROUP_START_ID)
      if metaLeader > 0 and dataLeader > 0:
        break
      sleep(100)
    # Give Raft time to replicate data from the leader. With heartbeats at
    # 100ms interval, the leader needs a few rounds to catch up a restarted
    # node that may have missed multiple entries. 2 seconds covers 20 heartbeat
    # cycles plus log replay time.
    sleep(2000)

  # Load space caches from recovered state machine
  store.loadSpaces()
  store.loadTableSpaces()
  store.loadGroupMembers()

  # Recovery: re-create NuRaft groups for spaces from sys.groups metadata
  block spaceGroupRecovery:
    let grpStart = encodeTableKey(SYS_GROUPS_TABLE_ID, "")
    let grpEnd = makeScanEndKey(SYS_GROUPS_TABLE_ID)
    let grpScan = store.raftScan(grpStart, grpEnd, 0, includeSystemKeys = true)
    if grpScan.isOk:
      for (key, entry) in grpScan.value:
        try:
          # sys.groups values are MVCC-encoded (written via MicroTransaction).
          # We must decode the MVCC wrapper before passing to decodeGroupRecord.
          var groupValue = entry.value
          if mvccValueTypes.isLikelyMVCCValue(groupValue):
            let mvccVal = mvccValueTypes.decodeMVCCValueFast(groupValue)
            if mvccVal.isDeleted:
              continue # Skip tombstones
            groupValue = mvccVal.data

          let rec = decodeGroupRecord(groupValue)
          let gid = GroupID(rec.groupId)
          if gid == META_GROUP_ID or gid == DATA_GROUP_START_ID: continue
          if coord.hasGroup(gid): continue

          # Build member list from replicas
          var members: seq[tuple[nodeId: uint32, host: string,
              port: int]] = @[]
          for rep in rec.replicas:
            let nid = rep.nodeId
            let peerInfo = coord.peerInfo.getOrDefault(nid,
                (host: coord.host, port: coord.port))
            members.add((nodeId: nid, host: peerInfo.host,
                port: peerInfo.port))

          var isMember = false
          for m in members:
            if m.nodeId == nodeId.uint32:
              isMember = true
              break

          if isMember:
            let preferredLeader = rec.preferredLeader
            discard coord.createAndStartGroup(gid, members, preferredLeader)
            store.registerGroup(gid)
        except:
          discard

  # Reload group membership cache after space group recovery
  store.loadGroupMembers()

  # Check if we have existing persisted data (spaces already exist)
  # If so, skip seeding entirely - data was already seeded from a previous run
  let hasExistingSpaces = store.spaces.len > 0

# Wait for both meta and data group leaders before seeding (max 5 seconds).
  # Both groups must have leaders so that addPeerToRaft can add servers
  # to the data group (not just the meta group) when new nodes join.
  if startAsLeader and not isRejoining and not hasExistingSpaces:
    let waitDeadline = serverNowMs(server).float + 5000.0
    while serverNowMs(server).float < waitDeadline:
      let metaOk = coord.isLeader(META_GROUP_ID) or coord.getLeader(
          META_GROUP_ID) > 0
      let dataOk = coord.isLeader(DATA_GROUP_START_ID) or coord.getLeader(
          DATA_GROUP_START_ID) > 0
      if metaOk and dataOk:
        break
      sleep(100)

  # Seed system tables: sys.nodes (table 5) and sys.groups (table 4)
  # Only seed when starting as fresh leader with NO existing data
  if startAsLeader and not isRejoining and not hasExistingSpaces:
    let seedTsNs = try: server.sharedTimer.now() except Exception:
      let t = getTime(); t.toUnix * 1_000_000_000 + t.nanosecond.int64

    # Pre-build system table catalog entries outside the closure to avoid
    # GC-safety issues with accessing the global SYSTEM_TABLES_REGISTRY.
    var sysTableEntries: seq[tuple[key: string, value: string]] = @[]
    for info in SYSTEM_TABLES_REGISTRY:
      let tableRecord = systemTableInfoToTableRecord(info)
      let tablesKey = encodeTableKey(SYS_TABLES_TABLE_ID,
          info.database & "." & info.schema & "." & info.name)
      sysTableEntries.add((key: tablesKey, value: encode(tableRecord)))

    # Use a single transaction for all seeding operations
    discard mvccStore.withAutoTransaction(proc(
        sessionId: uint64): MvccVoidResult {.gcsafe.} =
      let nodeKey = encodeTableKey(SYS_NODES_TABLE_ID, $server.config.serverId)
      let nodeRec = NodeRecord(
        nodeId: server.config.serverId,
        host: server.config.host,
        raftPort: uint16(raftPort),
        clientPort: uint16(server.config.port),
        status: nsAlive
      )
      discard mvccStore.txnPut(sessionId, nodeKey, encode(nodeRec))

      for gid in [META_GROUP_ID, DATA_GROUP_START_ID]:
        let groupKey = encodeTableKey(SYS_GROUPS_TABLE_ID, $gid)
        let spaceIdVal = ({.cast(gcsafe).}: genSpaceID(seedTsNs))
        let groupRec = GroupRecord(
          groupId: groupIDToULID(gid),
          spaceId: spaceIdVal, # TODO: proper space ID for meta/data groups
          preferredLeader: server.config.serverId,
          leader: server.config.serverId,
          replicas: @[GroupReplicaBin(nodeId: server.config.serverId,
              replicaType: rtVoter)]
        )
        discard mvccStore.txnPut(sessionId, groupKey, encode(groupRec))

      for p in peers:
        let peerKey = encodeTableKey(SYS_NODES_TABLE_ID, $p.nodeId)
        let peerRec = NodeRecord(
          nodeId: p.nodeId,
          host: p.host,
          raftPort: uint16(p.port),
          clientPort: 0,
          status: nsAlive
        )
        discard mvccStore.txnPut(sessionId, peerKey, encode(peerRec))

      # Seed default database and public schema
      let dbKey = encodeTableKey(SYS_DATABASES_TABLE_ID, "default")
      let dbRec = DatabaseRecord(name: "default",
          createdAtNs: system_schemas.nowNs(
              if server.sharedTimer.isNil: nil else: server.sharedTimer))
      discard mvccStore.txnPut(sessionId, dbKey, encode(dbRec))

      let scKey = encodeTableKey(SYS_SCHEMAS_TABLE_ID, "default.public")
      let scRec = SchemaRecord(name: "public", database: "default",
        createdAtNs: system_schemas.nowNs(
            if server.sharedTimer.isNil: nil else: server.sharedTimer))
      discard mvccStore.txnPut(sessionId, scKey, encode(scRec))

      # Seed default space (replicas=0 means ALL, single group = META_GROUP_ID)
      let defaultSpaceId = ({.cast(gcsafe).}: genSpaceID(seedTsNs))
      let spaceKey = encodeTableKey(SYS_SPACES_TABLE_ID, $defaultSpaceId)
      let spaceRec = SpaceRecord(
        spaceId: defaultSpaceId,
        name: "default",
        replicas: 0,
        groupCount: 1,
        groupIds: @[META_GROUP_ID],
        oldGroupIds: @[],
        workerState: uint8(wsrIdle),
        workerNodeId: 0,
        workerHeartbeat: 0,
        checkpoint: MigrationCheckpointRecord(
          completedTables: @[],
          currentTable: zeroTableId(),
          currentCursor: "",
          keysMigrated: 0,
          startedAtNs: 0,
          lastProgressNs: 0,
        ),
        createdAtNs: system_schemas.nowNs(
            if server.sharedTimer.isNil: nil else: server.sharedTimer)
      )
      discard mvccStore.txnPut(sessionId, spaceKey, encode(spaceRec))

      # Seed system table entries into sys.tables so they can be queried
      # via the same catalog path as user tables. Each system table gets a
      # TableRecord with keyEncoding = tkeSystemTable.
      for (tablesKey, tableRecordValue) in sysTableEntries:
        discard mvccStore.txnPut(sessionId, tablesKey, tableRecordValue)

      return mvccVOk()
    )

  # Load space caches after seeding
  store.loadSpaces()
  store.loadTableSpaces()
  store.loadGroupMembers()

  # Initialize SpaceManager for CREATE/DROP SPACE operations
  server.spaceManager = newSpaceManager(
    store = store,
    coord = coord,
    nodeId = nodeId.uint32,
    logger = server.logger
  )

  # SharedTimer: enable when we have peers and timer not yet configured
  if peers.len > 0 and server.sharedTimer.isNil:
    var timerPeers: seq[PeerConfig] = @[]
    for p in peers:
      timerPeers.add(PeerConfig(
        peerId: $p.nodeId,
        address: p.host,
        port: uint16(p.port + 1),
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
    if not server.raftCoord.isNil:
      server.raftCoord.setTimeProvider(timer)

  # Persist cluster membership for restart recovery
  if peers.len > 0:
    server.saveClusterState()

proc start*(server: ProtocolServer) {.raises: [].} =
  server.running.store(true)
  server.startedAt = serverNowSec(server)
  # Start background SharedTimer sync thread if configured
  if not server.sharedTimer.isNil:
    try: server.sharedTimer.start()
    except Exception as e: server.logger.logError("SharedTimer start failed: " & e.msg)
  # Start the active txn registry cleaner thread
  if not server.activeTxnRegistry.isNil:
    server.activeTxnRegistry.start()
  try:
    server.acceptSock = newSocket()
    server.acceptSock.setLingerZero()
    server.acceptSock.bindAddr(Port(server.config.port), server.config.host)
    server.acceptSock.listen()
    server.logger.logInfo(
      &"listening on {server.config.host}:{server.config.port}")
  except CatchableError as e:
    server.logger.logError("failed to bind: " & e.msg)
    server.running.store(false)
    return

  let aRef = new Thread[AcceptLoopArgs]
  withLock(threadStoreMu):
    acceptThreadStore.add(aRef)
  discard server.acceptThreadCount.fetchAdd(1)
  try:
    createThread(aRef[], acceptLoop, (server, server.acceptSock))
  except ResourceExhaustedError as e:
    server.logger.logError("failed to create accept thread: " & e.msg)
    server.running.store(false)

proc stop*(server: ProtocolServer) {.raises: [].} =
  server.running.store(false)
  server.logger.logInfo("server stopping")

  # Close the accept socket FIRST to unblock the accept loop thread.
  if not server.acceptSock.isNil:
    try:
      server.acceptSock.close()
    except:
      discard
    server.acceptSock = nil

  if not server.sharedTimer.isNil:
    try: server.sharedTimer.stop()
    except Exception as e: server.logger.logError("SharedTimer stop failed: " & e.msg)

  # Stop the active txn registry cleaner thread
  if not server.activeTxnRegistry.isNil:
    server.activeTxnRegistry.stop()

  withLock server.clientsMu:
    for id, conn in server.clients:
      try:
        conn.socket.close()
      except:
        discard

  for _ in 0 ..< 50:
    if server.clientThreadCount.load() == 0:
      break
    sleep(100)

  for _ in 0 ..< 50:
    if server.acceptThreadCount.load() == 0:
      break
    sleep(100)

  if not server.raftStore.isNil:
    try: server.raftStore.stop()
    except Exception as e: server.logger.logError("RaftStore stop failed: " & e.msg)

  if not server.raftCoord.isNil:
    try: server.raftCoord.stop()
    except Exception as e: server.logger.logError("RaftCoord stop failed: " & e.msg)

  # Drain the leader persistence channel AFTER coordinator is stopped.
  # ASIO threads fire onLeaderChanged callbacks that send() to this channel.
  # The stopped flag prevents new sends; draining clears any remaining messages.
  # We do NOT close the channel because Nim's Channel.close() destroys the
  # channel lock while other threads may still reference it, causing SIGSEGV.
  if not server.raftStore.isNil:
    try: server.raftStore.closeChannels()
    except Exception as e: server.logger.logError(
        "RaftStore closeChannels failed: " & e.msg)

  withLock threadStoreMu:
    threadStore = @[]
    acceptThreadStore = @[]
