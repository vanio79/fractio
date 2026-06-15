## Fractio Client
## ===============
##
## A client for Fractio that:
## 1. Connects to any node and fetches system tables (nodes, groups)
## 2. Opens persistent connections to all Raft group leaders
## 3. Routes KV operations directly to group leaders
## 4. Reconnects to new leaders on-demand when leader changes
##
## This enables client-side SQL parsing, planning, and execution
## with direct KV operations to group leaders, avoiding internal rerouting.
##
## Implements KVStoreWithRouting interface for testable code.

import std/[options, tables as stdtables, sets, locks, atomics, strutils,
    hashes, algorithm, os, sequtils, monotimes, times, typedthreads]
import posix
import ../core/types
import ../core/kv_interface # KVStore interface
import ./routing # Pure routing functions
import ./connection_pool
import ../protocol/client
import ../protocol/types
import ../protocol/codec
import ../protocol/messages/kv as kvMsgs
import ../protocol/messages/txn as txnMsgs
import ../distributed/meta/system_tables
import ../distributed/meta/system_schemas
import ../distributed/raft/group_types
import ../storage/mvcc/types as mvccTypes
import ../utils/logging
import ../utils/rwlock
import ../utils/query_timer

# =============================================================================
# Types
# =============================================================================

type
  NodeInfo* = object
    ## Cached information about a cluster node
    nodeId*: uint32
    host*: string
    clientPort*: uint16
    status*: NodeStatus
    client*: ProtocolClient # Connection to this node (may be nil)

  GroupInfo* = object
    ## Cached information about a Raft group
    groupId*: GroupID
    spaceId*: SpaceID
    leaderNodeId*: uint32
    replicaNodeIds*: seq[uint32]

  TableInfo* = object
    ## Cached information about a table
    tableId*: TableId
    name*: string
    spaceId*: SpaceID

  SpaceInfo* = object
    ## Cached information about a space
    spaceId*: SpaceID
    name*: string
    groupIds*: seq[GroupID] ## Current (new) groups for the space
    oldGroupIds*: seq[GroupID] ## Old groups during rebalancing (empty if not rebalancing)
    rebalancing*: bool      ## Whether the space is currently rebalancing

  FractioClientConfig* = object
    ## Configuration for FractioClient
    initialHost*: string      ## Initial node to connect to
    initialPort*: int         ## Initial node's client port
    connectionTimeoutMs*: int ## Timeout for connections
    requestTimeoutMs*: int    ## Timeout for requests
    refreshIntervalMs*: int   ## How often to refresh metadata
    autoRefresh*: bool        ## Automatically refresh metadata
    maxKvRetries*: int        ## Max retries for KV operations (0 = use default per-method)

  FractioClient* = ref object of KVStoreWithRouting
    ## Main client for Fractio with leader-aware routing.
    ## Inherits from KVStoreWithRouting for mockable KV operations.
    config*: FractioClientConfig

    # Cached cluster metadata
    nodes*: stdtables.Table[uint32, NodeInfo] # nodeId -> NodeInfo (legacy uint32 for NuRaft)
    groups*: stdtables.Table[GroupID, GroupInfo] # groupId -> GroupInfo
    tables*: stdtables.Table[TableId, TableInfo] # tableId -> TableInfo
    spaces*: stdtables.Table[SpaceID, SpaceInfo] # spaceId -> SpaceInfo

    # Active connections to leaders
    leaderConnections*: stdtables.Table[GroupID,
        ProtocolClient] # groupId -> connection to leader
    leaderConnectionNodes*: stdtables.Table[GroupID,
        uint32] # groupId -> nodeId the connection belongs to

    # Connection pool — single source of truth for TCP conn lifecycle.
    # `nodes[nodeId].client` is deprecated: every new conn goes through
    # this pool, every release goes through it. See connection_pool.nim.
    connPool*: ConnectionPool

    # Key prefix to group mapping (for routing)
    keyPrefixToGroup*: stdtables.Table[string, GroupID]

    # Lock for thread-safe access (RWLock: concurrent reads, exclusive writes)
    lock*: RWLock

    # State
    initialized*: Atomic[bool]
    lastRefreshNs*: Atomic[int64]
    activeTxnId*: TransactionID
    activeReadTs*: uint64

    # Transaction group tracking: which groups participated in each txn
    txnGroups*: stdtables.Table[TransactionID, HashSet[GroupID]]

# =============================================================================
# Backward-compatible KV operation wrappers (kvGet, kvPut, kvDelete, kvScan)
# These call the interface methods (get, put, delete, scan).
# =============================================================================

proc kvGet*(client: FractioClient, key: string,
           txnId: TransactionID = zeroTransactionID(),
           readTimestamp: uint64 = 0): KVOpResult[Option[string]] =
  ## Get a value by key. Wrapper for backward compatibility.
  client.get(key, txnId, readTimestamp)

proc kvPut*(client: FractioClient, key: string, value: string,
           txnId: TransactionID = zeroTransactionID()): KVOpVoidResult =
  ## Put a key-value pair. Wrapper for backward compatibility.
  client.put(key, value, txnId)

proc kvDelete*(client: FractioClient, key: string,
              txnId: TransactionID = zeroTransactionID()): KVOpVoidResult =
  ## Delete a key. Wrapper for backward compatibility.
  client.delete(key, txnId)

proc kvScan*(client: FractioClient, startKey: string, endKey: string,
            limit: uint32 = 0,
            txnId: TransactionID = zeroTransactionID(),
            readTimestamp: uint64 = 0): KVOpResult[seq[tuple[key,
                value: string]]] =
  ## Scan a key range. Wrapper for backward compatibility.
  client.scan(startKey, endKey, limit, txnId, readTimestamp)

# =============================================================================
# Constructors
# =============================================================================

proc newFractioClientConfig*(host: string, port: int): FractioClientConfig =
  ## Create a default client configuration
  result = FractioClientConfig(
    initialHost: host,
    initialPort: port,
    connectionTimeoutMs: 5000,
    requestTimeoutMs: 30000,
    refreshIntervalMs: 30000,
    autoRefresh: true,
    maxKvRetries: 0 # 0 means use per-method defaults
  )

proc newFractioClient*(config: FractioClientConfig): FractioClient =
  ## Create a new FractioClient
  result = FractioClient(config: config)
  initRWLock(result.lock)
  result.initialized.store(false, moRelaxed)
  result.connPool = newConnectionPool(maxPerNode = 4, maxTotal = 32)

proc newFractioClient*(host: string, port: int): FractioClient =
  ## Create a new FractioClient with default configuration
  newFractioClient(newFractioClientConfig(host, port))

# =============================================================================
# Connection helpers
# =============================================================================

proc connectToNode(client: FractioClient, host: string, port: int): Option[
    ProtocolClient] =
  ## Connect to a specific node
  ## Uses connectionTimeoutMs for the initial connection attempt (shorter timeout
  ## to avoid blocking on dead nodes) and requestTimeoutMs for subsequent
  ## socket operations after connection is established.
  ## After successful connect, update the timeout to requestTimeoutMs for I/O.
  let cfg = ClientConfig(
    host: host,
    port: port,
    timeoutMs: client.config.connectionTimeoutMs,
    clientId: "fractio-client",
    authMethod: amNone,
    authData: ""
  )
  let protoClient = newProtocolClient(cfg)
  if protoClient.connect().isOk:
    # Upgrade timeout for normal I/O operations after connection established
    protoClient.config.timeoutMs = client.config.requestTimeoutMs
    return some(protoClient)
  return none(ProtocolClient)

proc poolFactory*(client: FractioClient): ConnectProc {.gcsafe.} =
  ## Build a `ConnectProc` closure that captures `client` and
  ## forwards to `client.connectToNode`. The pool calls this when
  ## it needs to mint a fresh conn.
  return proc (host: string, port: int): Option[ProtocolClient] {.gcsafe.} =
    return client.connectToNode(host, port)

proc getOrCreateNodeConn*(client: FractioClient, host: string,
    port: int): Option[ProtocolClient] =
  ## Acquire a connection to (host, port) from the pool. The pool
  ## reuses idle conns when possible and creates new conns via
  ## `client.connectToNode` when needed. The returned conn is
  ## "checked out" — the caller MUST eventually call
  ## `client.releaseNodeConn(conn, host, port)` (or
  ## `client.releaseNodeConn(conn, host, port, keepAlive=false)` if
  ## the conn is unfit for reuse).
  return client.connPool.acquire(host, port, client.poolFactory())

proc releaseNodeConn*(client: FractioClient, conn: ProtocolClient, host: string,
                     port: int, keepAlive: bool = true) =
  ## Return a connection to the pool. `keepAlive=false` disconnects
  ## it immediately (use for conns that hit fatal errors, hit
  ## NOT_LEADER, or are known to be in a bad state). The default
  ## `keepAlive=true` parks it for reuse.
  client.connPool.release(conn, host, port, keepAlive = keepAlive)

proc getNodeConnectionInternal(client: FractioClient, nodeId: uint32): Option[
    ProtocolClient] =
  ## Internal version of getNodeConnection that assumes lock is already held.
  ## Get or create a connection to a specific node.
  ##
  ## Uses the connection pool: the `nodes[nodeId].client` field is kept
  ## as a transient "this conn is currently checked out" cache so that
  ## `getNodeConnection` lookups can find a healthy conn without going
  ## through the pool. The pool still owns the conn's lifetime; the
  ## caller MUST release the conn back to the pool when done.
  if nodeId notin client.nodes:
    return none(ProtocolClient)
  let nodeInfo = client.nodes[nodeId]
  if nodeInfo.client != nil and nodeInfo.client.connected.load(moRelaxed):
    return some(nodeInfo.client)

  # Try the pool first — it may have an idle conn to this host/port
  let poolConnOpt = client.getOrCreateNodeConn(nodeInfo.host,
      int(nodeInfo.clientPort))
  if poolConnOpt.isSome:
    var mutableInfo = nodeInfo
    mutableInfo.client = poolConnOpt.get()
    client.nodes[nodeId] = mutableInfo
    return poolConnOpt
  return none(ProtocolClient)

proc getNodeConnection(client: FractioClient, nodeId: uint32): Option[
    ProtocolClient] =
  ## Get or create a connection to a specific node
  withWriteLock client.lock:
    return client.getNodeConnectionInternal(nodeId)

# =============================================================================
# Metadata fetch result
# =============================================================================

type
  MetadataFetchResult* = object
    ## Result of a metadata fetch operation.
    ## When the server returns NOT_LEADER, includes redirect info
    ## so the caller can connect directly to the correct leader.
    success*: bool
    leaderRedirect*: LeaderRedirect ## Set when success=false and reason is NOT_LEADER

proc okFetch*(): MetadataFetchResult =
  MetadataFetchResult(success: true)

proc errFetch*(redirect: LeaderRedirect = LeaderRedirect()): MetadataFetchResult =
  MetadataFetchResult(success: false, leaderRedirect: redirect)

# =============================================================================
# Metadata refresh
# =============================================================================

proc fetchNodesTable(client: FractioClient, conn: ProtocolClient,
    requireLeader: bool = false): MetadataFetchResult =
  ## Fetch the sys.nodes table and update the cache.
  ## If requireLeader is true, routes the scan to META_GROUP_ID so the
  ## server rejects it if this node is not the leader. Use this for
  ## refreshMetadata to avoid stale follower reads.
  ## Returns MetadataFetchResult with leaderRedirect when the server
  ## returns NOT_LEADER, so callers can redirect to the correct leader.
  let startKey = encodeTableKey(SYS_NODES_TABLE_ID, "")
  let endKey = makeScanEndKey(SYS_NODES_TABLE_ID)

  let gid = if requireLeader: META_GROUP_ID else: ZeroGroupID()
  let scanRes = conn.kvScan(startKey, endKey, limit = 1000, groupId = gid)
  if scanRes.isErr:
    # If this was a NOT_LEADER error, include redirect info
    if scanRes.error.kind == peNotLeader:
      return errFetch(scanRes.error.leaderRedirect)
    return errFetch()

  withWriteLock client.lock:
    for pair in scanRes.value.pairs:
      # Handle both MVCC-encoded and plain values defensively.
      # The server's snapshotStreamScan should strip MVCC headers, but
      # race conditions or replication lag can cause MVCC-encoded data
      # to appear in scan results.
      try:
        let (payload, isDeleted) = stripMVCCHeader(pair.value)
        if isDeleted: continue
        if payload.len < 10: continue
        let nodeRec = decodeNodeRecord(payload)
        # If we're overwriting an existing NodeInfo that already has a
        # checked-out conn, release that conn back to the pool (do NOT
        # disconnect — it may still be reusable for a moment). This
        # fixes the worst connection-leak site: every metadata refresh
        # used to silently drop a healthy conn by overwriting the
        # NodeInfo entry with `client: nil`.
        if nodeRec.nodeId in client.nodes:
          let prev = client.nodes[nodeRec.nodeId]
          if prev.client != nil and prev.client.connected.load(moRelaxed):
            client.releaseNodeConn(prev.client, prev.host,
                                   int(prev.clientPort), keepAlive = true)
        client.nodes[nodeRec.nodeId] = NodeInfo(
          nodeId: nodeRec.nodeId,
          host: nodeRec.host,
          clientPort: nodeRec.clientPort,
          status: nodeRec.status,
          client: nil # Will be created on-demand
        )
      except CatchableError:
        # Skip corrupt entries rather than crashing
        discard

  return okFetch()

proc fetchGroupsTable(client: FractioClient, conn: ProtocolClient,
    requireLeader: bool = false): MetadataFetchResult =
  ## Fetch the sys.groups table and update the cache
  let startKey = encodeTableKey(SYS_GROUPS_TABLE_ID, "")
  let endKey = makeScanEndKey(SYS_GROUPS_TABLE_ID)

  let gid = if requireLeader: META_GROUP_ID else: ZeroGroupID()
  let scanRes = conn.kvScan(startKey, endKey, limit = 1000, groupId = gid)
  if scanRes.isErr:
    if scanRes.error.kind == peNotLeader:
      return errFetch(scanRes.error.leaderRedirect)
    return errFetch()

  withWriteLock client.lock:
    for pair in scanRes.value.pairs:
      try:
        let (payload, isDeleted) = stripMVCCHeader(pair.value)
        if isDeleted: continue
        if payload.len < 8: continue
        let groupRec = decodeGroupRecord(payload)
        var replicaNodeIds: seq[uint32] = @[]
        for rep in groupRec.replicas:
          replicaNodeIds.add(rep.nodeId)

        client.groups[groupIDFromULID(groupRec.groupId)] = GroupInfo(
          groupId: groupIDFromULID(groupRec.groupId),
          spaceId: groupRec.spaceId,
          leaderNodeId: groupRec.leader,
          replicaNodeIds: replicaNodeIds
        )
      except CatchableError:
        discard

  return okFetch()

proc fetchTablesTable(client: FractioClient, conn: ProtocolClient,
    requireLeader: bool = false): MetadataFetchResult =
  ## Fetch the sys.tables table and update the cache
  let startKey = encodeTableKey(SYS_TABLES_TABLE_ID, "")
  let endKey = makeScanEndKey(SYS_TABLES_TABLE_ID)

  let gid = if requireLeader: META_GROUP_ID else: ZeroGroupID()
  let scanRes = conn.kvScan(startKey, endKey, limit = 1000, groupId = gid)
  if scanRes.isErr:
    if scanRes.error.kind == peNotLeader:
      return errFetch(scanRes.error.leaderRedirect)
    return errFetch()

  withWriteLock client.lock:
    for pair in scanRes.value.pairs:
      try:
        let (payload, isDeleted) = stripMVCCHeader(pair.value)
        if isDeleted: continue
        if payload.len < 4: continue
        let tableRec = decodeTableRecord(payload)
        client.tables[tableRec.tableId] = TableInfo(
          tableId: tableRec.tableId,
          name: tableRec.name,
          spaceId: tableRec.spaceId
        )
      except CatchableError:
        discard

  return okFetch()

proc fetchSpacesTable(client: FractioClient, conn: ProtocolClient,
    requireLeader: bool = false): MetadataFetchResult =
  ## Fetch the sys.spaces table and update the cache
  let startKey = encodeTableKey(SYS_SPACES_TABLE_ID, "")
  let endKey = makeScanEndKey(SYS_SPACES_TABLE_ID)

  let gid = if requireLeader: META_GROUP_ID else: ZeroGroupID()
  let scanRes = conn.kvScan(startKey, endKey, limit = 1000, groupId = gid)
  if scanRes.isErr:
    if scanRes.error.kind == peNotLeader:
      return errFetch(scanRes.error.leaderRedirect)
    return errFetch()

  withWriteLock client.lock:
    for pair in scanRes.value.pairs:
      try:
        let (payload, isDeleted) = stripMVCCHeader(pair.value)
        if isDeleted: continue
        let spaceRec = decodeSpaceRecord(payload)
        client.spaces[spaceRec.spaceId] = SpaceInfo(
          spaceId: spaceRec.spaceId,
          name: spaceRec.name,
          groupIds: spaceRec.groupIds,
          oldGroupIds: spaceRec.oldGroupIds,
          rebalancing: spaceRec.workerState != uint8(wsrIdle)
        )
      except CatchableError:
        discard

  return okFetch()

proc initialize*(client: FractioClient): bool =
  ## Initialize the client by fetching metadata from the cluster.
  ## Must be called before using the client.
  if client.initialized.load(moRelaxed):
    return true

  # Acquire a connection to the initial node from the pool. The pool
  # will hand it back on release; if initialize fails partway through,
  # we still release the conn (with keepAlive=true so it's parked for
  # the next caller) so we don't leak.
  let connOpt = client.getOrCreateNodeConn(client.config.initialHost,
      client.config.initialPort)
  if connOpt.isNone:
    return false

  let conn = connOpt.get()
  defer:
    # Release back to the pool. We do NOT use keepAlive=false here
    # because the conn is presumably healthy — we just finished using
    # it. Subsequent calls to getOrCreateNodeConn for the same host:port
    # can reuse it.
    client.releaseNodeConn(conn, client.config.initialHost,
                           client.config.initialPort, keepAlive = true)

  # Fetch system tables
  let nodesResult = client.fetchNodesTable(conn)
  if not nodesResult.success:
    return false
  let groupsResult = client.fetchGroupsTable(conn)
  if not groupsResult.success:
    return false
  let tablesResult = client.fetchTablesTable(conn)
  if not tablesResult.success:
    # Not fatal - tables may not exist yet
    discard
  let spacesResult = client.fetchSpacesTable(conn)
  if not spacesResult.success:
    # Not fatal - spaces may not exist yet
    discard

  client.initialized.store(true, moRelaxed)
  return true

# Forward declaration (defined in Leader connection management section)
proc getGroupLeaderConnection*(client: FractioClient, groupId: GroupID): Option[
    ProtocolClient] {.gcsafe.}
proc invalidateGroupLeader*(client: FractioClient, groupId: GroupID) {.gcsafe.}
proc updateLeaderFromRedirect*(client: FractioClient, groupId: GroupID,
    redirect: LeaderRedirect) {.gcsafe.}

proc refreshMetadata*(client: FractioClient): bool =
  ## Refresh cached metadata from the cluster.
  ## ONLY reads from the META group leader. Followers may have stale
  ## metadata; reading from them causes split-brain catalog views.
  ## Uses NOT_LEADER redirect info to quickly find the correct leader
  ## instead of blindly retrying through followers.
  if not client.initialized.load(moRelaxed):
    return client.initialize()

  # Use fewer attempts for dashboard mode (low maxKvRetries).
  # With NOT_LEADER redirect info, most attempts resolve in 1-2 tries.
  let maxAttempts = if client.config.maxKvRetries > 0 and
                       client.config.maxKvRetries <= 5:
                      client.config.maxKvRetries
                    elif client.config.maxKvRetries > 0 and
                         client.config.maxKvRetries <= 15:
                      10
                    else:
                      30
  const baseBackoffMs = 50
  var failedNodeIds = initHashSet[uint32]()

  for attempt in 0 ..< maxAttempts:
    var conn: ProtocolClient = nil
    var shouldDisconnect = false

    # 1. Try the META group leader first (freshest metadata)
    let metaConnOpt = client.getGroupLeaderConnection(META_GROUP_ID)
    if metaConnOpt.isSome:
      conn = metaConnOpt.get()
    else:
      # 2. Try all known nodes, skipping nodes that already returned NOT_LEADER
      var connected = false
      withReadLock client.lock:
        var nodeIds = newSeq[uint32]()
        for nid in client.nodes.keys:
          nodeIds.add(nid)
        nodeIds.sort()
        # Try nodes that haven't failed first
        for nid in nodeIds:
          if nid in failedNodeIds:
            continue
          let nodeInfo = client.nodes[nid]
          let connOpt = client.connectToNode(nodeInfo.host, int(
              nodeInfo.clientPort))
          if connOpt.isSome:
            conn = connOpt.get()
            shouldDisconnect = true
            connected = true
            break
        # If all known nodes failed, try the failed ones (leader may have changed)
        if not connected:
          for nid in nodeIds:
            if nid notin failedNodeIds:
              continue # Already tried above
            let nodeInfo = client.nodes[nid]
            let connOpt = client.connectToNode(nodeInfo.host, int(
                nodeInfo.clientPort))
            if connOpt.isSome:
              conn = connOpt.get()
              shouldDisconnect = true
              connected = true
              break
      if not connected:
        # 3. Last resort: connect to the initial node
        let connOpt = client.connectToNode(client.config.initialHost,
            client.config.initialPort)
        if connOpt.isSome:
          conn = connOpt.get()
          shouldDisconnect = true

    if conn.isNil:
      # No connection available - wait and retry
      if attempt < maxAttempts - 1:
        sleep(baseBackoffMs + attempt * 10)
      continue

    # Try fetching metadata. If this connection is NOT the leader,
    # the server will return NOT_LEADER with redirect info.
    let nodesResult = client.fetchNodesTable(conn, requireLeader = true)
    if not nodesResult.success:
      if nodesResult.leaderRedirect.leaderId != 0:
        # Server told us who the leader is! Use the redirect info
        # to update our cache and connect directly on the next attempt.
        client.updateLeaderFromRedirect(META_GROUP_ID,
            nodesResult.leaderRedirect)
        # Also try connecting directly to the redirected leader now
        if shouldDisconnect:
          conn.disconnect()
        let redirectConn = client.connectToNode(
            nodesResult.leaderRedirect.leaderHost,
            int(nodesResult.leaderRedirect.leaderClientPort))
        if redirectConn.isSome:
          let leaderConn = redirectConn.get()
          let nodesOk2 = client.fetchNodesTable(leaderConn,
              requireLeader = true)
          if nodesOk2.success:
            # Success! Fetch remaining tables from the leader
            discard client.fetchGroupsTable(leaderConn, requireLeader = true)
            discard client.fetchTablesTable(leaderConn, requireLeader = true)
            discard client.fetchSpacesTable(leaderConn, requireLeader = true)
            leaderConn.disconnect()
            return true
          elif nodesOk2.leaderRedirect.leaderId != 0:
            # Another redirect — update and retry
            client.updateLeaderFromRedirect(META_GROUP_ID,
                nodesOk2.leaderRedirect)
          leaderConn.disconnect()
        # Clear failed set since leader info changed
        failedNodeIds = initHashSet[uint32]()
        if attempt < maxAttempts - 1:
          sleep(baseBackoffMs)
        continue

      # No redirect info — connection is to a follower that doesn't know
      # the leader. Remember this node and try others.
      withReadLock client.lock:
        for nid, nodeInfo in client.nodes:
          if nodeInfo.client == conn or
             (nodeInfo.host == conn.config.host and
              nodeInfo.clientPort == conn.config.port.uint16):
            failedNodeIds.incl(nid)
            break
      client.invalidateGroupLeader(META_GROUP_ID)
      if shouldDisconnect:
        conn.disconnect()
      if attempt < maxAttempts - 1:
        sleep(baseBackoffMs + attempt * 10)
      continue

    # We have a verified leader connection. Fetch remaining tables.
    let groupsOk = client.fetchGroupsTable(conn, requireLeader = true)
    # If groups table fetch returned NOT_LEADER with redirect, follow it
    if not groupsOk.success and groupsOk.leaderRedirect.leaderId != 0:
      client.updateLeaderFromRedirect(META_GROUP_ID,
          groupsOk.leaderRedirect)
      if shouldDisconnect:
        conn.disconnect()
      continue

    discard client.fetchTablesTable(conn, requireLeader = true)
    discard client.fetchSpacesTable(conn, requireLeader = true)

    if shouldDisconnect:
      conn.disconnect()

    return true

  # All retries exhausted - could not find META leader.
  # Return false rather than reading stale follower data.
  false

# =============================================================================
# Leader connection management
# =============================================================================

proc getGroupLeaderConnection*(client: FractioClient, groupId: GroupID): Option[
    ProtocolClient] =
  ## Get a connection to the leader of a specific group.
  ## Uses cached leader info, falls back to trying all replicas.
  ##
  ## Optimized with RWLock: the fast path (cached connection hit) uses a
  ## read lock, allowing concurrent reads from multiple threads. Only the
  ## slow path (connection creation / cache miss) requires a write lock.

  # Fast path: read-only check for cached connection (concurrent readers OK)
  withReadLock client.lock:
    if groupId notin client.groups:
      return none(ProtocolClient)

    let groupInfo = client.groups[groupId]

    if groupInfo.leaderNodeId != 0:
      if groupId in client.leaderConnections and groupId in
          client.leaderConnectionNodes:
        let cached = client.leaderConnections[groupId]
        let cachedNodeId = client.leaderConnectionNodes[groupId]
        if cached != nil and cached.connected.load(moRelaxed) and
            cachedNodeId == groupInfo.leaderNodeId:
          # Cache hit — return without upgrading to write lock
          return some(cached)

  # Slow path: need write lock to create/update connection
  withWriteLock client.lock:
    # Re-check under write lock (another thread may have updated while we
    # were waiting for the write lock)
    if groupId notin client.groups:
      return none(ProtocolClient)

    let groupInfo = client.groups[groupId]

    # Re-check cached connection under write lock
    if groupInfo.leaderNodeId != 0:
      if groupId in client.leaderConnections and groupId in
          client.leaderConnectionNodes:
        let cached = client.leaderConnections[groupId]
        let cachedNodeId = client.leaderConnectionNodes[groupId]
        if cached != nil and cached.connected.load(moRelaxed) and
            cachedNodeId == groupInfo.leaderNodeId:
          return some(cached)
        # Stale cache - disconnect and remove
        try:
          cached.disconnect()
        except: discard
        client.leaderConnections.del(groupId)
        client.leaderConnectionNodes.del(groupId)

      # Try to connect to leader (use internal version since we hold the lock)
      let connOpt = client.getNodeConnectionInternal(groupInfo.leaderNodeId)
      if connOpt.isSome:
        client.leaderConnections[groupId] = connOpt.get()
        client.leaderConnectionNodes[groupId] = groupInfo.leaderNodeId
        return connOpt
      # Leader connection failed - fall through to try replicas

    # Leader unknown or connection to known leader failed - try all replicas
    for nodeId in groupInfo.replicaNodeIds:
      let connOpt = client.getNodeConnectionInternal(nodeId)
      if connOpt.isSome:
        # We'll try this connection; if it's not the leader,
        # the operation will fail and we'll retry
        return connOpt

  return none(ProtocolClient)

proc invalidateGroupLeader*(client: FractioClient, groupId: GroupID) =
  ## Invalidate the leader for a group, forcing getGroupLeaderConnection
  ## to try all replicas.
  withWriteLock client.lock:
    if groupId in client.leaderConnections:
      try:
        client.leaderConnections[groupId].disconnect()
      except: discard
      client.leaderConnections.del(groupId)
    if groupId in client.leaderConnectionNodes:
      client.leaderConnectionNodes.del(groupId)
    if groupId in client.groups:
      var info = client.groups[groupId]
      info.leaderNodeId = 0
      client.groups[groupId] = info

proc updateLeaderFromRedirect*(client: FractioClient, groupId: GroupID,
    redirect: LeaderRedirect) {.gcsafe.} =
  ## Update the cached leader info for a group based on a NOT_LEADER redirect.
  ## This allows the client to immediately connect to the correct leader on
  ## the next attempt, avoiding repeated NOT_LEADER errors.
  if redirect.leaderId == 0:
    return
  withWriteLock client.lock:
    if groupId in client.groups:
      var info = client.groups[groupId]
      info.leaderNodeId = redirect.leaderId
      client.groups[groupId] = info
    # Invalidate cached connection for this group (it points to a follower)
    if groupId in client.leaderConnections:
      try:
        client.leaderConnections[groupId].disconnect()
      except: discard
      client.leaderConnections.del(groupId)
    if groupId in client.leaderConnectionNodes:
      client.leaderConnectionNodes.del(groupId)
    # Also update the node cache with the redirect info so we can connect
    # directly to the leader on the next attempt.
    if redirect.leaderHost.len > 0 and redirect.leaderClientPort > 0:
      let nid = redirect.leaderId
      if nid in client.nodes:
        # Update existing node info with correct host/port if needed
        var nodeInfo = client.nodes[nid]
        if nodeInfo.host != redirect.leaderHost or
           nodeInfo.clientPort != redirect.leaderClientPort:
          # Disconnect stale connection if any
          if nodeInfo.client != nil:
            try: nodeInfo.client.disconnect() except: discard
            nodeInfo.client = nil
          nodeInfo.host = redirect.leaderHost
          nodeInfo.clientPort = redirect.leaderClientPort
          client.nodes[nid] = nodeInfo
      else:
        # Add this node to the cache
        client.nodes[nid] = NodeInfo(
          nodeId: nid,
          host: redirect.leaderHost,
          clientPort: redirect.leaderClientPort,
          status: nsAlive,
          client: nil
        )

proc invalidateAllLeaderConnections*(client: FractioClient) =
  ## Invalidate ALL cached leader connections. Used when a leadership
  ## change is detected (e.g., after errors that suggest stale metadata).
  withWriteLock client.lock:
    for groupId, conn in client.leaderConnections:
      try:
        conn.disconnect()
      except: discard
    client.leaderConnections.clear()
    client.leaderConnectionNodes.clear()
    # Also clear node-level cached connections (they may point to dead nodes)
    for nid, nodeInfo in client.nodes:
      if nodeInfo.client != nil:
        try:
          nodeInfo.client.disconnect()
        except: discard
        var mutableInfo = nodeInfo
        mutableInfo.client = nil
        client.nodes[nid] = mutableInfo
    # Reset all group leader IDs to force re-discovery
    for gid, info in client.groups:
      var mutableInfo = info
      mutableInfo.leaderNodeId = 0
      client.groups[gid] = mutableInfo

proc forceMetadataRefresh*(client: FractioClient): bool =
  ## Force a full metadata refresh by first invalidating all cached
  ## connections and leader info, then refreshing metadata from the cluster.
  ## This is more aggressive than refreshMetadata() and should be used
  ## when the client detects stale state (e.g., after connection failures).
  client.invalidateAllLeaderConnections()
  client.refreshMetadata()

proc refreshGroupLeader(client: FractioClient, groupId: GroupID): bool =
  ## Refresh leader info for a specific group after a "not leader" error

  # Clear cached connection
  withWriteLock client.lock:
    if groupId in client.leaderConnections:
      try:
        client.leaderConnections[groupId].disconnect()
      except: discard
      client.leaderConnections.del(groupId)
    if groupId in client.leaderConnectionNodes:
      client.leaderConnectionNodes.del(groupId)

  # Refresh metadata
  if not client.refreshMetadata():
    return false

  # Add a small delay to allow leadership to stabilize
  # This is a workaround for race conditions between Raft leadership changes
  # and sys.groups metadata updates
  when defined(posix):
    discard posix.sleep(50)

  true

proc getMaxRetries(client: FractioClient): int {.inline.} =
  ## Get the max retry count for KV operations.
  ## If maxKvRetries is configured (> 0), use it; otherwise use the default.
  if client.config.maxKvRetries > 0:
    return client.config.maxKvRetries
  return 100

# =============================================================================
# Routing state conversion
# =============================================================================

proc getRoutingState*(client: FractioClient): RoutingState =
  ## Get a snapshot of routing state for pure routing functions.
  ## This creates a RoutingState that can be used with routing.nim functions.
  withReadLock client.lock:
    result = initRoutingState()
    for tableId, tableInfo in client.tables:
      result.addTable(tableId, tableInfo.name, tableInfo.spaceId)
    for spaceId, spaceInfo in client.spaces:
      result.addSpace(spaceId, spaceInfo.name, spaceInfo.groupIds,
                      spaceInfo.oldGroupIds, spaceInfo.rebalancing)

# =============================================================================
# Group key routing (using routing.nim pure functions)
# =============================================================================

proc getGroupForKey*(client: FractioClient, key: string): GroupID =
  ## Determine which group owns a given key using routing state.
  ## Returns META_GROUP_ID if the group cannot be determined.
  let state = client.getRoutingState()
  getGroupForKey(state, key)

proc getGroupsForTable*(client: FractioClient, tableId: TableId): seq[GroupID] =
  ## Get all groups that store data for a given table using routing state.
  ## For multi-group spaces, returns ALL groups in the space.
  ## During rebalancing, includes BOTH old and new groups for dual-read mode.
  let state = client.getRoutingState()
  getGroupsForTable(state, tableId)

proc getTableIdFromKey*(client: FractioClient, key: string): TableId =
  ## Extract tableId from a key using routing.nim pure function.
  ## Returns zeroTableId if not parseable.
  getTableIdFromKey(key)

# =============================================================================
# KVStore interface implementations
# =============================================================================

method get*(client: FractioClient, key: string,
           txnId: TransactionID = zeroTransactionID(),
           readTimestamp: uint64 = 0): KVOpResult[Option[string]] =
  ## Get a value by key, routing to the correct group leader.
  ## Implements KVStore interface.
  ##
  ## Data row keys in scan-bound format (without groupId) are automatically
  ## converted to stored key format (with groupId).
  if not client.initialized.load(moRelaxed):
    return kvOpErr[Option[string]]("client not initialized")

  let groupId = client.getGroupForKey(key)

  # Add groupId to data row keys (scan-bound format → stored key format)
  let rewrittenKey = addGroupIdToKey(key, groupId)

  let maxRetries = client.getMaxRetries()
  const baseBackoffMs = 50

  # Try multiple times (in case of leader changes or connection failures)
  for attempt in 0 ..< maxRetries:
    let connOpt = client.getGroupLeaderConnection(groupId)
    if connOpt.isNone:
      if attempt < maxRetries - 1:
        discard client.refreshMetadata()
        sleep(baseBackoffMs + attempt * 5)
        continue
      return kvOpErr[Option[string]]("no connection to group leader")

    let conn = connOpt.get()
    let res = conn.kvGetInGroup(rewrittenKey, groupId, txnId = txnId,
        readTimestamp = readTimestamp)

    if res.isOk:
      if res.value.found:
        return kvOpOk(some(res.value.value))
      else:
        return kvOpOk(none(string))

    # Check for "not leader" error - use redirect info and refresh metadata
    if res.error.kind == peNotLeader:
      if res.error.leaderRedirect.leaderId != 0:
        client.updateLeaderFromRedirect(groupId, res.error.leaderRedirect)
      else:
        discard client.refreshMetadata()
      if attempt < maxRetries - 1:
        sleep(baseBackoffMs + attempt * 5)
      continue

    # Handle connection failures (peInternal) - invalidate connection and retry
    if res.error.kind == peInternal:
      # Invalidate the cached connection for this group
      client.invalidateGroupLeader(groupId)
      if attempt < maxRetries - 1:
        discard client.refreshMetadata()
        sleep(baseBackoffMs + attempt * 5)
        continue

    return kvOpErr[Option[string]](res.error.msg)

  return kvOpErr[Option[string]]("too many retries")

proc getWithFilter*(client: FractioClient, key: string,
                    filter: Option[kvMsgs.WireFilterExpr],
                    txnId: TransactionID = zeroTransactionID(),
                    readTimestamp: uint64 = 0): KVOpResult[Option[string]] =
  ## Get a value by key with server-side filter (PointGet optimization).
  ## Routes to the correct group leader and passes filter to server.
  ## Returns some(value) if found AND passes filter, none(string) otherwise.
  ## This is used for "pk = value AND other_cond" queries where the server
  ## can apply the "other_cond" filter to avoid returning unwanted rows.
  if not client.initialized.load(moRelaxed):
    return kvOpErr[Option[string]]("client not initialized")

  let groupId = client.getGroupForKey(key)

  # Add groupId to data row keys (scan-bound format → stored key format)
  let rewrittenKey = addGroupIdToKey(key, groupId)

  let maxRetries = client.getMaxRetries()
  const baseBackoffMs = 50

  # Try multiple times (in case of leader changes)
  for attempt in 0 ..< maxRetries:
    let connOpt = client.getGroupLeaderConnection(groupId)
    if connOpt.isNone:
      if attempt < maxRetries - 1:
        discard client.refreshMetadata()
        sleep(baseBackoffMs + attempt * 5)
        continue
      return kvOpErr[Option[string]]("no connection to group leader")

    let conn = connOpt.get()
    let res = conn.kvGetInGroup(rewrittenKey, groupId, filter = filter, txnId = txnId,
                         readTimestamp = readTimestamp)

    if res.isOk:
      if res.value.found:
        return kvOpOk(some(res.value.value))
      else:
        # Row either doesn't exist or doesn't pass filter
        return kvOpOk(none(string))

    # Check for "not leader" error - use redirect info and refresh metadata
    if res.error.kind == peNotLeader:
      if res.error.leaderRedirect.leaderId != 0:
        client.updateLeaderFromRedirect(groupId, res.error.leaderRedirect)
      else:
        discard client.refreshMetadata()
      if attempt < maxRetries - 1:
        sleep(baseBackoffMs + attempt * 5)
      continue

    if res.error.kind == peInternal:
      # Connection failure - invalidate cached connection and retry
      client.invalidateGroupLeader(groupId)
      discard client.refreshMetadata()
      if attempt < maxRetries - 1:
        sleep(baseBackoffMs + attempt * 5)
        continue

    return kvOpErr[Option[string]](res.error.msg)

  return kvOpErr[Option[string]]("too many retries")

method put*(client: FractioClient, key: string, value: string,
           txnId: TransactionID = zeroTransactionID()): KVOpVoidResult =
  ## Put a key-value pair, routing to the correct group leader.
  ## Implements KVStore interface.
  ##
  ## Data row keys in scan-bound format (without groupId) are automatically
  ## converted to stored key format (with groupId).
  if not client.initialized.load(moRelaxed):
    return kvVoidErr("client not initialized")

  let groupId = client.getGroupForKey(key)

  # Add groupId to data row keys (scan-bound format → stored key format)
  let rewrittenKey = addGroupIdToKey(key, groupId)

  # Track group participation for distributed transaction resolution
  if txnId != zeroTransactionID():
    withWriteLock client.lock:
      if txnId notin client.txnGroups:
        client.txnGroups[txnId] = initHashSet[GroupID]()
      client.txnGroups[txnId].incl(groupId)

  # Retry with backoff to handle leader election races during group creation.
  # New groups may need time for leader election, especially during CREATE SPACE.
  let maxRetries = client.getMaxRetries()
  const baseBackoffMs = 50

  for attempt in 0 ..< maxRetries:
    let connOpt = client.getGroupLeaderConnection(groupId)
    if connOpt.isNone:
      # No leader connection yet - group may still be initializing
      if attempt < maxRetries - 1:
        sleep(baseBackoffMs + attempt * 5) # Linear backoff: 10, 15, 20, ...ms
        continue
      return kvVoidErr("no connection to group leader")

    let conn = connOpt.get()
    let res = conn.kvPutInGroup(rewrittenKey, value, groupId, txnId = txnId)

    if res.isOk and res.value.status == kvMsgs.PutStatusOK:
      return kvVoidOk()

    # Check for "not leader" error
    if res.isErr:
      if res.error.kind == peNotLeader:
        # Use redirect info to find new leader quickly.
        # Only refresh metadata if we don't have valid redirect info,
        # since refreshMetadata can overwrite the redirect-based leader
        # with stale data from the groups table.
        if res.error.leaderRedirect.leaderId != 0:
          client.updateLeaderFromRedirect(groupId, res.error.leaderRedirect)
        else:
          discard client.refreshMetadata()
        if attempt < maxRetries - 1:
          sleep(baseBackoffMs + attempt * 5)
        continue
      elif isNotLeaderError(res.error.msg):
        # Legacy: check message content for backward compatibility
        discard client.refreshMetadata()
        if attempt < maxRetries - 1:
          sleep(baseBackoffMs + attempt * 5)
        continue
      elif res.error.kind == peInternal:
        # Connection failure (e.g. "send incomplete") - invalidate cached
        # connection and retry with refreshed metadata.
        client.invalidateGroupLeader(groupId)
        discard client.refreshMetadata()
        if attempt < maxRetries - 1:
          sleep(baseBackoffMs + attempt * 5)
        continue

    let errMsg = if res.isOk: "put failed with status " & $res.value.status
                 else: "server error: " & res.error.msg
    return kvVoidErr(errMsg)

  return kvVoidErr("too many retries")

method delete*(client: FractioClient, key: string,
              txnId: TransactionID = zeroTransactionID()): KVOpVoidResult =
  ## Delete a key, routing to the correct group leader.
  ## Implements KVStore interface.
  ##
  ## Data row keys in scan-bound format (without groupId) are automatically
  ## converted to stored key format (with groupId).
  if not client.initialized.load(moRelaxed):
    return kvVoidErr("client not initialized")

  let groupId = client.getGroupForKey(key)

  # Add groupId to data row keys (scan-bound format → stored key format)
  let rewrittenKey = addGroupIdToKey(key, groupId)

  # Track group participation for distributed transaction resolution
  if txnId != zeroTransactionID():
    withWriteLock client.lock:
      if txnId notin client.txnGroups:
        client.txnGroups[txnId] = initHashSet[GroupID]()
      client.txnGroups[txnId].incl(groupId)

  let maxRetries = client.getMaxRetries()
  const baseBackoffMs = 50

  for attempt in 0 ..< maxRetries:
    let connOpt = client.getGroupLeaderConnection(groupId)
    if connOpt.isNone:
      if attempt < maxRetries - 1:
        discard client.refreshMetadata()
        sleep(baseBackoffMs + attempt * 5)
        continue
      return kvVoidErr("no connection to group leader")

    let conn = connOpt.get()
    let res = conn.kvDeleteInGroup(rewrittenKey, groupId, txnId = txnId)

    if res.isOk and res.value.status in {kvMsgs.DelStatusDeleted,
        kvMsgs.DelStatusNotFound}:
      return kvVoidOk()

    if res.isErr and res.error.kind == peNotLeader:
      # Use redirect info to find new leader quickly
      if res.error.leaderRedirect.leaderId != 0:
        client.updateLeaderFromRedirect(groupId, res.error.leaderRedirect)
      else:
        discard client.refreshMetadata()
      if attempt < maxRetries - 1:
        sleep(baseBackoffMs + attempt * 5)
      continue

    if res.isErr and res.error.kind == peInternal:
      # Connection failure (e.g. "send incomplete") - invalidate cached
      # connection and retry with refreshed metadata.
      client.invalidateGroupLeader(groupId)
      discard client.refreshMetadata()
      if attempt < maxRetries - 1:
        sleep(baseBackoffMs + attempt * 5)
      continue

    let errMsg = if res.isOk: "delete failed" else: res.error.msg
    return kvVoidErr(errMsg)

  return kvVoidErr("too many retries")

# ---------------------------------------------------------------------------
# Internal helper: send a single BatchRequest to a group leader with retries
# ---------------------------------------------------------------------------

proc sendBatchToGroup(client: FractioClient, groupId: GroupID,
                      req: BatchRequest): Result[int, string] {.gcsafe.} =
  ## Send a BatchRequest to the leader of `groupId`, retry on not-leader
  ## / internal-error. Returns the number of successful ops in the response
  ## (or an error message on terminal failure).
  let maxRetries = client.getMaxRetries()
  const baseBackoffMs = 50

  for attempt in 0 ..< maxRetries:
    let connOpt = client.getGroupLeaderConnection(groupId)
    if connOpt.isNone:
      if attempt < maxRetries - 1:
        discard client.refreshMetadata()
        sleep(baseBackoffMs + attempt * 5)
        continue
      return Result[int, string](isOk: false,
        err: "no connection to group leader for " & $groupId)

    let conn = connOpt.get()
    let res = conn.kvBatch(req)

    if res.isOk:
      # Count successful ops in the response. We accept both AllOK and
      # PartialFailure (per-op status is non-zero for failures); for delete
      # operations, "not found" still counts as success from the caller's
      # perspective, so we count any per-op status 0x00 as success.
      var successCount = 0
      for opRes in res.value.results:
        if opRes.status == 0x00'u8:
          inc successCount
      return Result[int, string](isOk: true, val: successCount)

    if res.error.kind == peNotLeader:
      if res.error.leaderRedirect.leaderId != 0:
        client.updateLeaderFromRedirect(groupId, res.error.leaderRedirect)
      else:
        discard client.refreshMetadata()
      if attempt < maxRetries - 1:
        sleep(baseBackoffMs + attempt * 5)
      continue

    if res.error.kind == peInternal:
      client.invalidateGroupLeader(groupId)
      discard client.refreshMetadata()
      if attempt < maxRetries - 1:
        sleep(baseBackoffMs + attempt * 5)
      continue

    return Result[int, string](isOk: false,
      err: "batch RPC failed: " & res.error.msg)

  return Result[int, string](isOk: false,
    err: "too many retries for batch to group " & $groupId)

# ---------------------------------------------------------------------------
# KVStore.batch implementation
#
# Groups ops by destination group (via getGroupForKey) and dispatches one
# BatchRequest per group. Each per-group BatchRequest is chunked into
# sub-batches of MAX_BATCH_OPS (10K) so the protocol limit is respected.
# The op.key passed by the caller is the scan-bound form (no groupId);
# this method adds the groupId before transmitting so the leader can
# index into the correct shard.
# ---------------------------------------------------------------------------

method batch*(client: FractioClient, ops: seq[KVBatchOp],
              txnId: TransactionID = zeroTransactionID()): KVOpResult[
                  KVBatchResult] =
  ## Implements KVStore.batch: execute a batch of operations, routing each
  ## op to its group leader and using one BatchRequest per group.
  if not client.initialized.load(moRelaxed):
    return kvBatchErr("client not initialized")

  if ops.len == 0:
    return kvBatchOk(0)

  # Per-group bucket: original index in `ops` and the rewritten key (with groupId).
  type BatchEntry = object
    origIdx: int
    rewrittenKey: string
  var byGroup: stdtables.Table[GroupID, seq[BatchEntry]] =
    stdtables.initTable[GroupID, seq[BatchEntry]]()
  # Track transaction group participation for distributed transaction
  # resolution (matches the behaviour of per-op delete/put).
  if txnId != zeroTransactionID():
    withWriteLock client.lock:
      if txnId notin client.txnGroups:
        client.txnGroups[txnId] = initHashSet[GroupID]()
      for op in ops:
        let gid = client.getGroupForKey(op.key)
        client.txnGroups[txnId].incl(gid)

  for i, op in ops:
    let gid = client.getGroupForKey(op.key)
    let rewritten = addGroupIdToKey(op.key, gid)
    if gid notin byGroup:
      byGroup[gid] = @[]
    byGroup[gid].add(BatchEntry(origIdx: i, rewrittenKey: rewritten))

  var totalSuccess = 0
  var totalFailure = 0
  var firstError = ""

  # Send one BatchRequest per group (chunked by payload size to keep each
  # resulting Raft log entry under MAX_BATCH_PAYLOAD_BYTES).
  #
  # Why chunk by bytes, not by op count:
  # 1. ops vary wildly in size (small delete vs large value put), so a
  #    10K-op cap is misleading.
  # 2. Larger Raft log entries (> ~4KB) have been observed to trigger
  #    intermittent SIGSEGV in NuRaft's deliverMessage on the follower
  #    side, crashing the node. Chunking to small entries avoids the
  #    bug while still amortizing Raft consensus over many ops.
  for gid, indexedKeys in byGroup.pairs:
    var startIdx = 0
    while startIdx < indexedKeys.len:
      # Greedily extend the chunk while encoded size is under the limit.
      # Always include at least one op per chunk to avoid an infinite loop
      # on pathological inputs (e.g. a single op whose encoded form alone
      # exceeds the limit).
      var endIdx = startIdx + 1
      var batchOps = newSeqOfCap[BatchOp](min(indexedKeys.len - startIdx,
          MAX_BATCH_OPS))
      var totalBytes = 0
      while endIdx <= indexedKeys.len and endIdx - startIdx < MAX_BATCH_OPS:
        let entry = indexedKeys[endIdx - 1]
        let op = ops[entry.origIdx]
        var opData = ""
        case op.kind
        of bopDelete:
          opData.writeBytes(entry.rewrittenKey)
          batchOps.add(BatchOp(
            kind: BatchOpDelete,
            flags: 0,
            data: opData))
        of bopPut:
          # opData is two length-prefixed strings: key then value
          opData.writeBytes(entry.rewrittenKey)
          opData.writeBytes(op.value)
          batchOps.add(BatchOp(
            kind: BatchOpPut,
            flags: 0,
            data: opData))
        # Estimate encoded size: per-op prefix (kind:1 + flags:1 + len:4 = 6)
        # plus opData length. The request header is small (~10 bytes).
        totalBytes += 6 + opData.len
        if totalBytes > MAX_BATCH_PAYLOAD_BYTES and batchOps.len > 1:
          # Removing the last op keeps the chunk under the cap.
          discard batchOps.pop()
          endIdx -= 1
          break
        inc endIdx

      # Use batchOps.len (the actual number of ops we packed) rather than
      # endIdx - startIdx (which overcounts by 1 when the loop ran endIdx past
      # indexedKeys.len). See chunking loop above.
      let chunkLen = batchOps.len

      let req = BatchRequest(
        flags: BatchFlagContinueOnErr, # ContinueOnErr: per-op failures don't abort batch
        txnId: txnId,
        operations: batchOps)

      let res = client.sendBatchToGroup(gid, req)
      if not res.isOk:
        # Whole batch RPC failed: count all ops in this chunk as failed
        totalFailure += chunkLen
        if firstError.len == 0:
          firstError = res.err
      else:
        # Count successes: any op whose per-op status is 0x00
        let succ = res.val
        let fail = chunkLen - succ
        totalSuccess += succ
        totalFailure += fail
        if succ < chunkLen and firstError.len == 0:
          firstError = "partial failure in batch (group=" & $gid &
                       "): " & $succ & "/" & $chunkLen & " ops succeeded"

      startIdx = endIdx

  return kvBatchOk(totalSuccess, totalFailure, firstError)

method scan*(client: FractioClient, startKey: string, endKey: string,
            limit: uint32 = 0,
            txnId: TransactionID = zeroTransactionID(),
            readTimestamp: uint64 = 0): KVOpResult[seq[tuple[key,
                value: string]]] =
  ## Scan a key range across ALL groups in the space.
  ## For multi-group spaces, data is sharded across groups by primary key hash,
  ## so we must scan ALL groups and merge results.
  ##
  ## When the scan is for a data table, per-group scan bounds are computed
  ## using narrowScanBoundsToGroup() so the server only reads that group's
  ## key range instead of the entire table.
  ## Implements KVStore interface.
  if not client.initialized.load(moRelaxed):
    return kvOpErr[seq[tuple[key, value: string]]]("client not initialized")

  # Determine which table this scan is for
  let tableId = client.getTableIdFromKey(startKey)
  let groupIds = client.getGroupsForTable(tableId)

  # Use per-group scan bounds to reduce I/O: each group only reads
  # its own key range instead of scanning the entire table.
  let isDataTable = isUserTableId(tableId) and isDataRowKey(startKey)

  # Collect results from all groups, deduplicating by key
  var resultMap = stdtables.initTable[string, string]()
  var scanErrors: seq[string] = @[]

  for groupId in groupIds:
    # Compute per-group scan bounds for data tables
    let (groupStart, groupEnd) = if isDataTable:
      narrowScanBoundsToGroup(startKey, endKey, tableId, groupId)
    else:
      (startKey, endKey)

    var groupOk = false
    for attempt in 0 ..< 3:
      let connOpt = client.getGroupLeaderConnection(groupId)
      if connOpt.isNone:
        if attempt == 2:
          scanErrors.add($groupId & ": no connection to leader")
        continue

      let conn = connOpt.get()
      # Use per-group scan bounds for efficient reads.
      let res = conn.kvScan(groupStart, groupEnd, 0, txnId = txnId,
                            readTimestamp = readTimestamp,
                            groupId = groupId)

      if res.isOk:
        for pair in res.value.pairs:
          # Deduplicate: keep first occurrence
          if pair.key notin resultMap:
            resultMap[pair.key] = pair.value
        groupOk = true
        break # Success, move to next group

      if res.error.kind == peNotLeader:
        # Connection is pointing to a non-leader. Invalidate the cached
        # connection so getGroupLeaderConnection picks a different replica,
        # then refresh metadata to learn the new leader. Without the
        # invalidate, the next attempt would reuse the same broken
        # connection and loop forever.
        client.invalidateGroupLeader(groupId)
        if res.error.leaderRedirect.leaderId != 0:
          client.updateLeaderFromRedirect(groupId, res.error.leaderRedirect)
        else:
          discard client.refreshMetadata()
        continue # Retry this group

      if res.error.kind == peInternal:
        # Connection failure - invalidate cached connection and retry
        client.invalidateGroupLeader(groupId)
        discard client.refreshMetadata()
        continue

      # Other error — record and stop retrying this group
      scanErrors.add($groupId & ": " & res.error.msg)
      break

    if not groupOk and scanErrors.len == 0:
      scanErrors.add($groupId & ": failed after 3 attempts")

  # If any group failed in a multi-group scan, return error instead of partial data
  if scanErrors.len > 0 and groupIds.len > 1:
    return kvOpErr[seq[tuple[key, value: string]]](
        "partial scan failure: " & scanErrors.join("; ") &
        " (all groups required for correct results)")

  # Convert to result sequence
  var entries: seq[tuple[key, value: string]] = @[]
  for key, value in resultMap.pairs:
    entries.add((key: key, value: value))

  # Sort by key for consistent ordering
  # For data table keys, compare by PK to produce globally sorted output
  # across groups (full storage key comparison groups by groupId prefix).
  if isDataTable:
    entries.sort(proc(a, b: tuple[key, value: string]): int =
      cmp(primaryKeyFromDataRowKey(a.key), primaryKeyFromDataRowKey(b.key)))
  else:
    entries.sort(proc(a, b: tuple[key, value: string]): int = cmp(a.key, b.key))

  # Apply limit if specified
  if limit > 0 and entries.len > int(limit):
    entries.setLen(int(limit))

  return kvOpOk(entries)

method streamScan*(client: FractioClient, startKey: string, endKey: string,
                  limit: uint32 = 0,
                  txnId: TransactionID = zeroTransactionID(),
                  readTimestamp: uint64 = 0,
                  filter: Option[kvMsgs.WireFilterExpr] = none(
                      kvMsgs.WireFilterExpr),
                  reverse: bool = false,
                  columns: Option[seq[string]] = none(
                      seq[string]),
                  topK: Option[kvMsgs.WireTopKSpec] = none(
                      kvMsgs.WireTopKSpec)): Result[StreamingScanClient,

ProtocolError] =
  ## Streaming scan across ALL groups in the space.
  ## For multi-group spaces, data is sharded across groups by primary key hash.
  ## This method creates a streaming client that merges results from all groups
  ## in key order using k-way merge.
  ## filter: optional server-side filter for reducing network traffic
  ## reverse: when true, each group scans in descending key order; the k-way
  ##          merge also runs in descending order. This is used by the planner
  ##          for PK DESC + LIMIT pushdown to avoid scanning all N rows.
  ## columns: optional column names for server-side projection (Tier-3a).
  ##          When set and non-empty, each group server decodes DataRows and
  ##          re-emits only the requested columns. Reduces wire size for
  ##          SELECT col1, col2 ... FROM wide_table.
  ## topK: optional server-side top-K heap spec (Tier-3b). When set, each
  ##       group server runs a bounded top-K heap locally and ships only the
  ##       K winners over the wire. Client merges at most K×Ngroups candidates
  ##       (typically a few dozen for K=5, Ngroups=3).
  let scanTimer = newQueryTimer()
  if not client.initialized.load(moRelaxed):
    return peErr(newProtocolError(peInternal, "client not initialized"))

  # Determine which table this scan is for
  let tableId = client.getTableIdFromKey(startKey)
  let groupIds = client.getGroupsForTable(tableId)
  scanTimer.stamp("resolve_groups")

  # Use per-group scan bounds to reduce I/O: each group only reads
  # its own key range instead of scanning the entire table.
  let isDataTable = isUserTableId(tableId) and isDataRowKey(startKey)

  # For single group, use direct streaming.
  if groupIds.len == 1:
    let groupId = groupIds[0]
    let (groupStart, groupEnd) = if isDataTable:
      narrowScanBoundsToGroup(startKey, endKey, tableId, groupId)
    else:
      (startKey, endKey)

    let connOpt = client.getGroupLeaderConnection(groupId)
    if connOpt.isNone:
      return peErr(newProtocolError(peInternal,
          "no connection to group leader"))
    let conn = connOpt.get()
    return conn.kvStreamScan(groupStart, groupEnd, limit, 0, 0, txnId,
        readTimestamp, groupId, filter, reverse, columns, topK)

  # For multi-group, use k-way merge: open all group streams in PARALLEL
  # and merge results by key order. This produces globally sorted output
  # without needing a post-hoc sort.
  #
  # Previously, streams were opened sequentially in a for-loop, which meant
  # 3 groups × ~110ms = ~330ms just for setup. By opening them in parallel,
  # the setup time drops to ~110ms (the slowest group's first-frame time).
  let chunkSize = 100'u32 # Default chunk size for streaming

  # Step 1: Resolve connections and compute per-group scan bounds on the main thread.
  #
  # CRITICAL: ProtocolClient wraps a single TCP socket. If two groups share the
  # same leader node, getGroupLeaderConnection returns the SAME ProtocolClient
  # instance for both. Two concurrent kvStreamScan calls on the same socket would
  # interleave their frames, corrupting both streams. This causes wrong ordering
  # results in k-way merge because the streams receive each other's data.
  #
  # Fix: for groups sharing a leader node, create a separate TCP connection for
  # each group so that parallel streams don't interfere.
  type GroupScanArgs = object
    groupId: GroupID
    groupStart: string
    groupEnd: string
    conn: ProtocolClient
    filter: Option[kvMsgs.WireFilterExpr]
    ownsConnection: bool ## True if we created a dedicated connection (must close after)
    limit: uint32 ## Per-group scan limit. 0 means no limit (full scan).
    columns: Option[seq[string]] ## Column names for server-side projection (Tier-3a)
    topK: Option[kvMsgs.WireTopKSpec] ## Top-K spec for server-side heap pushdown (Tier-3b)

  var groupArgs: seq[GroupScanArgs] = @[]
  var resolveErrors: seq[string] = @[]
  var seenConns: seq[ProtocolClient] = @[] ## Track connections already in use

  debug "[scan_client] streamScan: " & $groupIds.len & " groups, isDataTable=" &
      (if isDataTable: "true" else: "false")

  for groupId in groupIds:
    let (groupStart, groupEnd) = if isDataTable:
      narrowScanBoundsToGroup(startKey, endKey, tableId, groupId)
    else:
      (startKey, endKey)

    # Get connection to group leader (uses cached connection)
    var connOpt: Option[ProtocolClient] = none(ProtocolClient)
    try:
      connOpt = client.getGroupLeaderConnection(groupId)
    except KeyError:
      discard

    if connOpt.isNone:
      resolveErrors.add($groupId & ": no connection to leader")
      continue

    let cachedConn = connOpt.get()

    # Check if this connection is already used by another group.
    # If so, create a dedicated connection to the same node to avoid
    # frame interleaving when parallel streams run concurrently.
    var conn: ProtocolClient = cachedConn
    var ownsConn: bool = false

    if seenConns.contains(cachedConn):
      # Shared connection — create a new one for this group to avoid
      # frame interleaving when parallel streams run concurrently on
      # the same TCP socket.
      var leaderHost: string
      var leaderPort: uint16
      withReadLock client.lock:
        if groupId in client.groups:
          let nodeId = client.groups[groupId].leaderNodeId
          if nodeId != 0 and nodeId in client.nodes:
            leaderHost = client.nodes[nodeId].host
            leaderPort = client.nodes[nodeId].clientPort
      if leaderHost.len > 0:
        # Acquire from the pool instead of opening a fresh conn. The
        # pool will hand back an idle conn if one exists, or open a
        # new one. The conn is "owned" by the scan and MUST be
        # released back to the pool when the scan finishes (success
        # or error path).
        let newConnOpt = client.getOrCreateNodeConn(leaderHost,
            int(leaderPort))
        if newConnOpt.isSome:
          conn = newConnOpt.get()
          ownsConn = true
        else:
          # Failed to create a dedicated connection for this group.
          # We MUST NOT reuse the shared connection — it would cause
          # frame interleaving with the other group's stream. Instead,
          # skip this group and report an error.
          resolveErrors.add($groupId & ": failed to create dedicated connection to " &
              leaderHost & ":" & $leaderPort & " (shared leader connection already in use)")
          continue

    seenConns.add(cachedConn)

    groupArgs.add(GroupScanArgs(
      groupId: groupId,
      groupStart: groupStart,
      groupEnd: groupEnd,
      conn: conn,
      filter: filter,
      ownsConnection: ownsConn,
      limit: limit,
      columns: columns,
      topK: topK
    ))

  if groupArgs.len == 0:
    if resolveErrors.len > 0:
      return peErr(newProtocolError(peInternal,
          "failed to connect to any group: " & resolveErrors.join("; ")))
    return peErr(newProtocolError(peInternal,
        "no groups available for scan"))

  # Step 2: Open streams. For single group, do it directly on the main thread.
  # For multiple groups, open them concurrently using threads.
  #
  # Each kvStreamScan call takes ~110ms (server processes first batch).
  # Running them concurrently drops total setup from sum(N×110ms) to max(110ms).
  #
  # Thread communication: each thread writes to its own slot in a shared
  # result array via raw pointer. ProtocolClient and StreamingScanClient are
  # ref objects that can't cross Thread[T] boundaries, so we use pointer casts.
  # GroupID/TransactionID are distinct ULID (value types, array[16,uint8])
  # and CAN be passed directly, but we serialize as strings to avoid issues
  # with distinct-type handling in Nim's thread system.
  var groupStreams: seq[StreamingScanClient] = @[]
  var errors: seq[string] = @[]

  type StreamSetupResult = object
    ok: bool
    streamPtr: pointer # StreamingScanClient cast to pointer (valid only if ok)
    errorMsg: string

  if groupArgs.len == 1:
    # Single group — no need for threads
    let args = groupArgs[0]
    # Use args.limit (carried from caller) so LIMIT can be pushed all
    # the way down to the server's LevelDB iterator.
    let streamRes = args.conn.kvStreamScan(args.groupStart, args.groupEnd,
        args.limit,
        chunkSize, 0, txnId, readTimestamp, args.groupId, args.filter, reverse,
        args.columns, args.topK)
    if streamRes.isOk:
      groupStreams.add(streamRes.value)
    else:
      if args.ownsConnection:
        # Release the dedicated conn back to the pool with keepAlive=false
        # (we want it closed, not parked — it just failed). We need the
        # leaderHost/leaderPort to call releaseNodeConn; the simpler fix
        # is to store them in args at setup time. For now, just disconnect
        # directly (still inside the pool's accounting via closeConn).
        try: args.conn.disconnect("scan_failed_single")
        except: discard
      errors.add($args.groupId & ": " & streamRes.error.msg)
  else:
    # Multiple groups — open streams in parallel using threads.
    # Each thread calls kvStreamScan and writes to its own result slot
    # via a raw pointer, avoiding closure captures.
    var setupResults = newSeq[StreamSetupResult](groupArgs.len)
    var threads = newSeq[Thread[pointer]](groupArgs.len)

    # Thread arg: all data needed to open one group stream. Must be a
    # plain object (no GC refs) for safe cross-thread passing via pointer.
    type SetupArg = object
      idx: int
      connPtr: pointer
      groupStart: string
      groupEnd: string
      groupIdStr: string
      chunkSizeVal: uint32
      txnIdStr: string
      readTs: uint64
      reverseVal: bool
      limitVal: uint32      ## Per-group scan limit. 0 means no limit (full scan).
      columnsJoined: string ## "\0"-joined column names (empty = no projection)
      topKJoined: string    ## Tier-3b: serialized WireTopKSpec. Format:
                           ## "limit|columnIndex,descending|columnIndex,descending|..." (empty = no topK)
      filterSerialized: string ## Server-side filter (WireFilterExpr) serialized via
                            ## encodeWireFilterExpr. Empty string = no filter.
      resultPtr: pointer

    var setupArgs = newSeq[SetupArg](groupArgs.len)
    for i in 0 ..< groupArgs.len:
      let args = groupArgs[i]
      var colsJoined = ""
      if args.columns.isSome and args.columns.get().len > 0:
        colsJoined = args.columns.get().join("\0")
      # Serialize topK for cross-thread passing. Format: "limit|spec1|spec2|..."
      # where each spec is "columnIndex,descending" (descending: 0/1).
      var topKJoined = ""
      if args.topK.isSome and args.topK.get().sortSpecs.len > 0:
        let t = args.topK.get()
        topKJoined = $t.limit
        for s in t.sortSpecs:
          topKJoined.add('|')
          topKJoined.add($s.columnIndex)
          topKJoined.add(',')
          topKJoined.add(if s.descending: "1" else: "0")
      # Serialize server-side filter for cross-thread passing. Uses the
      # wire-format encoder (encodeWireFilterExpr) so the worker thread
      # can decode it back into a WireFilterExpr with the same type.
      # FIX: previously this was hardcoded to `none` in the k-way merge
      # path, which silently dropped the WHERE filter on multi-group scans
      # (the bug behind test 1 T-L `WHERE id=1 OR id=5000 OR id=8465
      # ORDER BY name DESC LIMIT 5` returning 5 unfiltered rows instead
      # of 3 filtered ones).
      var filterSerialized = ""
      if args.filter.isSome:
        encodeWireFilterExpr(args.filter.get(), filterSerialized)
      setupArgs[i] = SetupArg(
        idx: i,
        connPtr: cast[pointer](args.conn),
        groupStart: args.groupStart,
        groupEnd: args.groupEnd,
        groupIdStr: $args.groupId,
        chunkSizeVal: chunkSize,
        txnIdStr: $txnId,
        readTs: readTimestamp,
        reverseVal: reverse,
        limitVal: args.limit,
        columnsJoined: colsJoined,
        topKJoined: topKJoined,
        filterSerialized: filterSerialized,
        resultPtr: addr(setupResults[i])
      )

    {.cast(raises: []).}:
      for i in 0 ..< groupArgs.len:
        let arg = setupArgs[i]
        createThread(threads[i], proc(a: pointer) {.thread.} =
          {.cast(gcsafe).}:
            let sa = cast[ptr SetupArg](a)[]
            let conn = cast[ProtocolClient](sa.connPtr)
            let groupId = parseGroupID(sa.groupIdStr)
            let txnIdVal = transactionIDFromString(sa.txnIdStr)
            # Decode the server-side filter (WireFilterExpr) from its wire-format
            # serialization. Empty string means no filter. This is the fix for
            # the multi-group k-way merge path that previously hardcoded `none`
            # here, silently dropping WHERE filters on multi-group scans.
            let filterOpt: Option[kvMsgs.WireFilterExpr] =
              if sa.filterSerialized.len > 0:
                var pos = 0
                let decoded = decodeWireFilterExpr(sa.filterSerialized, pos)
                if decoded.isOk:
                  some(decoded.value)
                else:
                  # Decode failure — fall back to no filter (server returns
                  # all rows). Better than crashing the scan.
                  none(kvMsgs.WireFilterExpr)
              else:
                none(kvMsgs.WireFilterExpr)
            let colsOpt: Option[seq[string]] =
              if sa.columnsJoined.len > 0:
                some(sa.columnsJoined.split('\0'))
              else:
                none(seq[string])
            # Deserialize topK from "limit|spec1|spec2|..." format where each
            # spec is "columnIndex,descending" (descending: 0/1).
            let topKOpt: Option[kvMsgs.WireTopKSpec] =
              if sa.topKJoined.len > 0:
                let parts = sa.topKJoined.split('|')
                if parts.len > 0:
                  let limVal = parseUInt(parts[0])
                  var specs: seq[kvMsgs.WireSortSpec] = @[]
                  for i in 1 ..< parts.len:
                    let specParts = parts[i].split(',')
                    if specParts.len == 2:
                      let ci = parseInt(specParts[0])
                      let desc = specParts[1] == "1"
                      specs.add(kvMsgs.WireSortSpec(columnIndex: ci.int32,
                          descending: desc))
                  some(kvMsgs.WireTopKSpec(limit: limVal.uint32,
                      sortSpecs: specs))
                else:
                  none(kvMsgs.WireTopKSpec)
              else:
                none(kvMsgs.WireTopKSpec)
            let streamRes = conn.kvStreamScan(sa.groupStart, sa.groupEnd,
                sa.limitVal,
                sa.chunkSizeVal, 0, txnIdVal, sa.readTs, groupId, filterOpt,
                sa.reverseVal, colsOpt, topKOpt)
            let slot = cast[ptr StreamSetupResult](sa.resultPtr)
            if streamRes.isOk:
              # CRITICAL: Prevent use-after-free when the thread's local
              # streamRes goes out of scope. With --mm:atomicArc, ref
              # objects use atomic reference counting. When streamRes is
              # destroyed at thread exit, it decrements the ref count.
              # Without GC_ref, the count drops to 0 and the object is freed
              # before the main thread can adopt it via cast[pointer].
              # GC_ref adds +1 so the count stays >= 1 after streamRes
              # destruction. The main thread's groupStreams.add() will
              # adopt the reference, and the extra GC_ref is balanced by
              # streamRes's =destroy decrement.
              GC_ref(streamRes.value)
              slot[] = StreamSetupResult(
                ok: true,
                streamPtr: cast[pointer](streamRes.value),
                errorMsg: ""
              )
            else:
              slot[] = StreamSetupResult(
                ok: false,
                streamPtr: nil,
                errorMsg: streamRes.error.msg
              )
        , cast[pointer](addr(setupArgs[i])))

    # Wait for all threads to complete
    for i in 0 ..< groupArgs.len:
      joinThread(threads[i])

    # Collect results — cast pointers back to ref types
    for i in 0 ..< groupArgs.len:
      let res = setupResults[i]
      if res.ok:
        groupStreams.add(cast[StreamingScanClient](res.streamPtr))
      else:
        # Stream failed — disconnect owned connections to prevent leaks
        if groupArgs[i].ownsConnection:
          try: groupArgs[i].conn.disconnect("scan_failed_parallel")
          except: discard
        errors.add(setupArgs[i].groupIdStr & ": " & res.errorMsg)

    # If any stream failed in a multi-group scan, return an error instead
    # of producing partial results. Partial data silently returns wrong
    # answers (e.g., 322 rows instead of 999) and corrupts ORDER BY results.
    if errors.len > 0:
      # Clean up any successfully opened streams before returning error
      for stream in groupStreams:
        stream.closeStream()
      groupStreams = @[]
      return peErr(newProtocolError(peInternal,
          "partial stream failure: " & errors.join("; ") &
          " (all groups required for correct results)"))

  scanTimer.stamp("merge_setup")

  if groupStreams.len == 0:
    if errors.len > 0:
      return peErr(newProtocolError(peInternal,
          "failed to connect to any group: " & errors.join("; ")))
    return peErr(newProtocolError(peInternal,
        "no groups available for scan"))

  if groupStreams.len == 1:
    # Single group stream — return directly
    return peOk(groupStreams[0])

  # K-way merge: merge all group streams by key order
  # For data table scans, use PK-based comparison to produce globally sorted
  # output. Without the extractor, the k-way merge compares full storage keys
  # (including groupId), which groups rows by groupId instead of by PK value.
  let extractor: KeyExtractor = if isDataTable:
    proc(key: string): string {.closure, gcsafe, raises: [].} =
      primaryKeyFromDataRowKey(key)
  else:
    nil
  let mergeClient = newKWayMergeScanClient(groupStreams, limit, extractor, reverse)
  debug "[scan_client] groups=" & $groupIds.len & " streams=" &
      $groupStreams.len & " " & scanTimer.formatBreakdown()
  return peOk(mergeClient)

method beginTxn*(client: FractioClient): KVOpResult[TxnBeginResult] =
  ## Begin a new transaction by contacting the meta group leader.
  ## Implements KVStore interface with retry for leader changes and
  ## connection failures.
  if not client.initialized.load(moRelaxed):
    if not client.initialize():
      return kvOpErr[TxnBeginResult]("failed to initialize client")

  const maxRetries = 10
  const baseBackoffMs = 50

  for attempt in 0 ..< maxRetries:
    let connOpt = client.getGroupLeaderConnection(META_GROUP_ID)
    if connOpt.isNone:
      if attempt < maxRetries - 1:
        discard client.refreshMetadata()
        sleep(baseBackoffMs + attempt * 10)
        continue
      return kvOpErr[TxnBeginResult]("no connection for beginTxn")

    let conn = connOpt.get()
    let res = conn.beginTxn()
    if res.isOk:
      return kvOpOk((txnId: res.value.txnId,
          readTimestamp: res.value.readTimestamp))

    # Handle "not leader" error - use redirect info and retry
    if res.error.kind == peNotLeader or isNotLeaderError(res.error.msg):
      if res.error.kind == peNotLeader:
        if res.error.leaderRedirect.leaderId != 0:
          client.updateLeaderFromRedirect(META_GROUP_ID,
              res.error.leaderRedirect)
        else:
          discard client.refreshMetadata()
      else:
        discard client.refreshMetadata()
      if attempt < maxRetries - 1:
        sleep(baseBackoffMs + attempt * 10)
      continue

    # Handle connection failure (peInternal) - invalidate and retry
    if res.error.kind == peInternal:
      client.invalidateGroupLeader(META_GROUP_ID)
      discard client.refreshMetadata()
      if attempt < maxRetries - 1:
        sleep(baseBackoffMs + attempt * 10)
      continue

    return kvOpErr[TxnBeginResult](res.error.msg)

  return kvOpErr[TxnBeginResult]("too many retries for beginTxn")

method commitTxn*(client: FractioClient, txnId: TransactionID): KVOpVoidResult =
  ## Commit a transaction by sending commit to all group leaders that
  ## participated in writes. This ensures intents are resolved even when
  ## the META leader does not replicate the target group.
  var groupsToCommit: seq[GroupID] = @[]
  withWriteLock client.lock:
    if txnId in client.txnGroups:
      for gid in client.txnGroups[txnId]:
        groupsToCommit.add(gid)
      client.txnGroups.del(txnId)

  if groupsToCommit.len == 0:
    # Fallback: no tracked groups (legacy non-transactional path)
    let connOpt = client.getGroupLeaderConnection(META_GROUP_ID)
    if connOpt.isNone:
      return kvVoidErr("no connection for commitTxn")
    let conn = connOpt.get()
    let res = conn.commitTxn(txnId)
    if res.isOk and res.value.status == txnMsgs.TxnCommitOK:
      return kvVoidOk()
    else:
      let errMsg = if res.isOk:
                     if res.value.status == txnMsgs.TxnCommitConflict:
                       "commit failed due to conflict"
                     else:
                       "commit failed with status " & $res.value.status
                   else: res.error.msg
      return kvVoidErr(errMsg)

  var lastErr = ""
  var anyOk = false
  for gid in groupsToCommit:
    for attempt in 0 ..< 3:
      let connOpt = client.getGroupLeaderConnection(gid)
      if connOpt.isNone:
        break
      let conn = connOpt.get()
      let res = conn.commitTxn(txnId)
      if res.isOk and res.value.status == txnMsgs.TxnCommitOK:
        anyOk = true
        break
      if res.isOk and res.value.status == txnMsgs.TxnCommitConflict:
        lastErr = "commit failed due to conflict"
        break
      if res.isErr:
        if isNotLeaderError(res.error.msg) or res.error.kind == peNotLeader:
          if res.error.kind == peNotLeader:
            if res.error.leaderRedirect.leaderId != 0:
              client.updateLeaderFromRedirect(gid, res.error.leaderRedirect)
            else:
              discard client.refreshMetadata()
          else:
            discard client.refreshMetadata()
          sleep(50)
          continue
        if res.error.kind == peInternal:
          client.invalidateGroupLeader(gid)
          discard client.refreshMetadata()
          sleep(50)
          continue
        lastErr = res.error.msg
        break

  if anyOk:
    return kvVoidOk()
  if lastErr.len > 0:
    return kvVoidErr(lastErr)
  return kvVoidErr("commit failed on all groups")

method rollbackTxn*(client: FractioClient,
    txnId: TransactionID): KVOpVoidResult =
  ## Rollback a transaction by sending rollback to all group leaders that
  ## participated in writes.
  var groupsToRollback: seq[GroupID] = @[]
  withWriteLock client.lock:
    if txnId in client.txnGroups:
      for gid in client.txnGroups[txnId]:
        groupsToRollback.add(gid)
      client.txnGroups.del(txnId)

  if groupsToRollback.len == 0:
    # Fallback: no tracked groups (legacy path)
    let connOpt = client.getGroupLeaderConnection(META_GROUP_ID)
    if connOpt.isNone:
      return kvVoidErr("no connection for rollbackTxn")
    let conn = connOpt.get()
    let res = conn.rollbackTxn(txnId)
    if res.isOk:
      return kvVoidOk()
    else:
      return kvVoidErr(res.error.msg)

  var lastErr = ""
  var anyOk = false
  for gid in groupsToRollback:
    for attempt in 0 ..< 3:
      let connOpt = client.getGroupLeaderConnection(gid)
      if connOpt.isNone:
        break
      let conn = connOpt.get()
      let res = conn.rollbackTxn(txnId)
      if res.isOk:
        anyOk = true
        break
      if res.isErr:
        if isNotLeaderError(res.error.msg) or res.error.kind == peNotLeader:
          if res.error.kind == peNotLeader:
            if res.error.leaderRedirect.leaderId != 0:
              client.updateLeaderFromRedirect(gid, res.error.leaderRedirect)
            else:
              discard client.refreshMetadata()
          else:
            discard client.refreshMetadata()
          sleep(50)
          continue
        if res.error.kind == peInternal:
          client.invalidateGroupLeader(gid)
          discard client.refreshMetadata()
          sleep(50)
          continue
        lastErr = res.error.msg
        break

  if anyOk:
    return kvVoidOk()
  if lastErr.len > 0:
    return kvVoidErr(lastErr)
  return kvVoidErr("rollback failed on all groups")

# =============================================================================
# KVStoreWithRouting group-specific operations
# =============================================================================

method getInGroup*(client: FractioClient, key: string, groupId: GroupID,
                  txnId: TransactionID = zeroTransactionID(),
                  readTimestamp: uint64 = 0): KVOpResult[Option[string]] =
  ## Get from a specific group.
  ## Implements KVStoreWithRouting interface.
  if not client.initialized.load(moRelaxed):
    return kvOpErr[Option[string]]("client not initialized")

  let maxRetries = client.getMaxRetries()
  const baseBackoffMs = 50

  for attempt in 0 ..< maxRetries:
    let connOpt = client.getGroupLeaderConnection(groupId)
    if connOpt.isNone:
      if attempt < maxRetries - 1:
        discard client.refreshMetadata()
        sleep(baseBackoffMs + attempt * 5)
        continue
      return kvOpErr[Option[string]]("no connection to group leader")

    let conn = connOpt.get()
    let res = conn.kvGetInGroup(key, groupId, txnId = txnId,
        readTimestamp = readTimestamp)

    if res.isOk:
      if res.value.found:
        return kvOpOk(some(res.value.value))
      else:
        return kvOpOk(none(string))

    if res.error.kind == peNotLeader:
      if res.error.leaderRedirect.leaderId != 0:
        client.updateLeaderFromRedirect(groupId, res.error.leaderRedirect)
      else:
        discard client.refreshMetadata()
      if attempt < maxRetries - 1:
        sleep(baseBackoffMs + attempt * 5)
      continue

    if res.error.kind == peInternal:
      client.invalidateGroupLeader(groupId)
      discard client.refreshMetadata()
      if attempt < maxRetries - 1:
        sleep(baseBackoffMs + attempt * 5)
      continue

    return kvOpErr[Option[string]](res.error.msg)

  return kvOpErr[Option[string]]("too many retries")

method putInGroup*(client: FractioClient, key: string, value: string, groupId: GroupID,
                  txnId: TransactionID = zeroTransactionID()): KVOpVoidResult =
  ## Put to a specific group.
  ## Implements KVStoreWithRouting interface.
  if not client.initialized.load(moRelaxed):
    return kvVoidErr("client not initialized")

  let maxRetries = client.getMaxRetries()
  const baseBackoffMs = 50

  for attempt in 0 ..< maxRetries:
    let connOpt = client.getGroupLeaderConnection(groupId)
    if connOpt.isNone:
      if attempt < maxRetries - 1:
        sleep(baseBackoffMs + attempt * 5)
        continue
      return kvVoidErr("no connection to group leader")

    let conn = connOpt.get()
    let res = conn.kvPutInGroup(key, value, groupId, txnId = txnId)

    if res.isOk and res.value.status == kvMsgs.PutStatusOK:
      return kvVoidOk()

    if res.isErr:
      if res.error.kind == peNotLeader:
        if res.error.leaderRedirect.leaderId != 0:
          client.updateLeaderFromRedirect(groupId, res.error.leaderRedirect)
        else:
          discard client.refreshMetadata()
        if attempt < maxRetries - 1:
          sleep(baseBackoffMs + attempt * 5)
        continue
      elif isNotLeaderError(res.error.msg):
        discard client.refreshMetadata()
        if attempt < maxRetries - 1:
          sleep(baseBackoffMs + attempt * 5)
        continue
      elif res.error.kind == peInternal:
        client.invalidateGroupLeader(groupId)
        discard client.refreshMetadata()
        if attempt < maxRetries - 1:
          sleep(baseBackoffMs + attempt * 5)
        continue

    let errMsg = if res.isOk: "put failed with status " & $res.value.status
                 else: "server error: " & res.error.msg
    return kvVoidErr(errMsg)

  return kvVoidErr("too many retries")

method deleteInGroup*(client: FractioClient, key: string, groupId: GroupID,
                     txnId: TransactionID = zeroTransactionID()): KVOpVoidResult =
  ## Delete from a specific group.
  ## Implements KVStoreWithRouting interface.
  if not client.initialized.load(moRelaxed):
    return kvVoidErr("client not initialized")

  let maxRetries = client.getMaxRetries()
  const baseBackoffMs = 50

  for attempt in 0 ..< maxRetries:
    let connOpt = client.getGroupLeaderConnection(groupId)
    if connOpt.isNone:
      if attempt < maxRetries - 1:
        discard client.refreshMetadata()
        sleep(baseBackoffMs + attempt * 5)
        continue
      return kvVoidErr("no connection to group leader")

    let conn = connOpt.get()
    let res = conn.kvDeleteInGroup(key, groupId, txnId = txnId)

    if res.isOk and res.value.status in {kvMsgs.DelStatusDeleted,
        kvMsgs.DelStatusNotFound}:
      return kvVoidOk()

    if res.isErr and res.error.kind == peNotLeader:
      if res.error.leaderRedirect.leaderId != 0:
        client.updateLeaderFromRedirect(groupId, res.error.leaderRedirect)
      else:
        discard client.refreshMetadata()
      if attempt < maxRetries - 1:
        sleep(baseBackoffMs + attempt * 5)
      continue

    if res.isErr and res.error.kind == peInternal:
      client.invalidateGroupLeader(groupId)
      discard client.refreshMetadata()
      if attempt < maxRetries - 1:
        sleep(baseBackoffMs + attempt * 5)
      continue

    let errMsg = if res.isOk: "delete failed" else: res.error.msg
    return kvVoidErr(errMsg)

  return kvVoidErr("too many retries")

# =============================================================================
# Space Management
# =============================================================================

type
  SpaceOpResult* = object
    ## Result type for space operations
    isOk*: bool
    err*: string
    ## On success:
    spaceId*: SpaceID
    groupCount*: int32
    groupIds*: seq[GroupID]

proc spaceOpOk(spaceId: SpaceID, groupCount: int32, groupIds: seq[
    GroupID]): SpaceOpResult =
  SpaceOpResult(isOk: true, spaceId: spaceId, groupCount: groupCount,
      groupIds: groupIds)

proc spaceOpErr(msg: string): SpaceOpResult =
  SpaceOpResult(isOk: false, err: msg)

proc createSpace*(client: FractioClient, name: string,
    replicas: int32 = 0): SpaceOpResult =
  ## Create a new space on the server.
  ## This operation:
  ##   1. Sends a CreateSpace request to the META leader
  ##   2. Server creates Raft groups and waits for leaders
  ##   3. Server writes space/group records to sys tables
  ##   4. Returns updated sys table data for client cache
  ##
  ## name: space name (must be unique)
  ## replicas: replication factor (0 = ALL nodes)

  # Retry loop for leader redirect
  for attempt in 0 ..< 10:
    # Get connection to META group leader
    let connOpt = client.getGroupLeaderConnection(META_GROUP_ID)
    if connOpt.isNone:
      # No known leader — try refreshing metadata
      discard client.refreshMetadata()
      sleep(500)
      continue

    let conn = connOpt.get()

    # Send createSpace request
    let res = conn.createSpace(name, replicas)
    if res.isErr:
      # Refresh metadata on not leader error
      if res.error.kind == peNotLeader:
        if res.error.leaderRedirect.leaderId != 0:
          client.updateLeaderFromRedirect(META_GROUP_ID,
              res.error.leaderRedirect)
        else:
          discard client.refreshMetadata()
        # Small backoff before retry to avoid leader redirect loops
        sleep(100 * (attempt + 1))
        continue
      # Connection failure (e.g. "send incomplete" on freshly-established
      # socket) — invalidate the cached META leader connection and retry.
      # This is the same pattern used by kvPut/kvDelete. The previous behavior
      # of returning immediately caused intermittent CREATE SPACE failures on
      # the first request after client initialization.
      if res.error.kind == peInternal:
        client.invalidateGroupLeader(META_GROUP_ID)
        discard client.refreshMetadata()
        sleep(100 * (attempt + 1))
        continue
      return spaceOpErr(res.error.msg)

    let resp = res.value
    if not resp.success:
      return spaceOpErr(resp.error)

    # Update local cache with new space and group records
    withWriteLock client.lock:
      # Parse and cache the space record
      let spaceRec = decodeSpaceRecord(resp.spaceRecord)

      client.spaces[spaceRec.spaceId] = SpaceInfo(
        spaceId: spaceRec.spaceId,
        name: spaceRec.name,
        groupIds: spaceRec.groupIds,
        oldGroupIds: spaceRec.oldGroupIds,
        rebalancing: spaceRec.workerState != uint8(wsrIdle)
      )

      # Parse and cache all group records
      for gr in resp.groupRecords:
        let groupRec = decodeGroupRecord(gr.record)
        var replicaNodeIds: seq[uint32] = @[]
        for rep in groupRec.replicas:
          replicaNodeIds.add(rep.nodeId)

        client.groups[groupIDFromULID(groupRec.groupId)] = GroupInfo(
          groupId: groupIDFromULID(groupRec.groupId),
          spaceId: groupRec.spaceId,
          leaderNodeId: groupRec.leader,
          replicaNodeIds: replicaNodeIds
        )

    var resultGroupIds: seq[GroupID] = @[]
    for gr in resp.groupRecords:
      resultGroupIds.add(groupIDFromULID(gr.groupId))
    return spaceOpOk(resp.spaceId, resp.groupCount, resultGroupIds)

  return spaceOpErr("too many retries")

proc dropSpace*(client: FractioClient, name: string): SpaceOpResult =
  ## Drop an existing space on the server.
  ## This operation:
  ##   1. Sends a DropSpace request to the META leader
  ##   2. Server marks space/group records as deleted
  ##   3. Server stops Raft groups on all nodes
  ##   4. Returns deleted groupIds for client cache cleanup

  # Retry loop for leader redirect
  for attempt in 0 ..< 3:
    # Get connection to META group leader
    let connOpt = client.getGroupLeaderConnection(META_GROUP_ID)
    if connOpt.isNone:
      return spaceOpErr("no connection to META group leader")

    let conn = connOpt.get()

    # Send dropSpace request
    let res = conn.dropSpace(name)
    if res.isErr:
      # Refresh metadata on not leader error
      if res.error.kind == peNotLeader:
        if res.error.leaderRedirect.leaderId != 0:
          client.updateLeaderFromRedirect(META_GROUP_ID,
              res.error.leaderRedirect)
        else:
          discard client.refreshMetadata()
        continue
      # Connection failure (e.g. "send incomplete" on freshly-established
      # socket) — invalidate the cached META leader connection and retry.
      if res.error.kind == peInternal:
        client.invalidateGroupLeader(META_GROUP_ID)
        discard client.refreshMetadata()
        continue
      return spaceOpErr(res.error.msg)

    let resp = res.value
    if not resp.success:
      return spaceOpErr(resp.error)

    # Update local cache - remove space and groups
    withWriteLock client.lock:
      # Find and remove the space by name (since resp.spaceId is ULID)
      var spaceIdToRemove: SpaceID
      var found = false
      for sid, sinfo in client.spaces:
        if sinfo.name == name:
          spaceIdToRemove = sid
          found = true
          break

      if found:
        client.spaces.del(spaceIdToRemove)

      # Remove all deleted groups
      for gid in resp.deletedGroupIds:
        client.groups.del(gid)
        if gid in client.leaderConnections:
          try:
            client.leaderConnections[gid].disconnect()
          except: discard
          client.leaderConnections.del(gid)
        if gid in client.leaderConnectionNodes:
          client.leaderConnectionNodes.del(gid)

    return spaceOpOk(resp.spaceId, resp.deletedGroupIds.len.int32,
      resp.deletedGroupIds)

  return spaceOpErr("too many retries")

# =============================================================================
# Cleanup
# =============================================================================

proc close*(client: FractioClient) =
  ## Close all connections and clean up resources
  withWriteLock client.lock:
    # Disconnect all cached connections
    for nodeId, nodeInfo in client.nodes:
      if nodeInfo.client != nil:
        try:
          nodeInfo.client.disconnect()
        except: discard

    for groupId, conn in client.leaderConnections:
      if conn != nil:
        try:
          conn.disconnect()
        except: discard

    client.nodes.clear()
    client.groups.clear()
    client.tables.clear()
    client.spaces.clear()
    client.leaderConnections.clear()
    client.leaderConnectionNodes.clear()
    client.txnGroups.clear()
    client.initialized.store(false, moRelaxed)

  # Close the connection pool. This sends disconnect frames to every
  # idle conn in the pool. The pool is the single source of truth for
  # conn lifecycle; `nodes` and `leaderConnections` were just advisory
  # caches that could point to the same conns the pool already owns.
  # Closing the pool second ensures no further acquires can race with
  # shutdown (closeAll is atomic under the pool's internal lock).
  client.connPool.closeAll()

  deinitRWLock(client.lock)
