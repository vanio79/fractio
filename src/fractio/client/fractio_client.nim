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
    hashes, algorithm, os, sequtils]
import posix
import ../core/types
import ../core/kv_interface # KVStore interface
import ./routing # Pure routing functions
import ../protocol/client
import ../protocol/types
import ../protocol/messages/kv as kvMsgs
import ../protocol/messages/txn as txnMsgs
import ../distributed/meta/system_tables
import ../distributed/meta/system_schemas
import ../distributed/raft/group_types
import ../storage/mvcc/types as mvccTypes
import ../utils/logging

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

    # Key prefix to group mapping (for routing)
    keyPrefixToGroup*: stdtables.Table[string, GroupID]

    # Lock for thread-safe access
    lock*: Lock

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
  initLock(result.lock)
  result.initialized.store(false, moRelaxed)

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

proc getNodeConnectionInternal(client: FractioClient, nodeId: uint32): Option[
    ProtocolClient] =
  ## Internal version of getNodeConnection that assumes lock is already held.
  ## Get or create a connection to a specific node.
  if nodeId in client.nodes:
    let nodeInfo = client.nodes[nodeId]
    if nodeInfo.client != nil and nodeInfo.client.connected.load(moRelaxed):
      return some(nodeInfo.client)

    # Create new connection
    let connOpt = client.connectToNode(nodeInfo.host, int(
        nodeInfo.clientPort))
    if connOpt.isSome:
      # Update the cached connection
      client.nodes[nodeId] = NodeInfo(
        nodeId: nodeInfo.nodeId,
        host: nodeInfo.host,
        clientPort: nodeInfo.clientPort,
        status: nodeInfo.status,
        client: connOpt.get()
      )
      return connOpt
  return none(ProtocolClient)

proc getNodeConnection(client: FractioClient, nodeId: uint32): Option[
    ProtocolClient] =
  ## Get or create a connection to a specific node
  withLock client.lock:
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

  withLock client.lock:
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

  withLock client.lock:
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

  withLock client.lock:
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

  withLock client.lock:
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

  # Connect to initial node
  let connOpt = client.connectToNode(client.config.initialHost,
      client.config.initialPort)
  if connOpt.isNone:
    return false

  let conn = connOpt.get()
  defer: conn.disconnect()

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
      withLock client.lock:
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
      withLock client.lock:
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

  withLock client.lock:
    # Check if we have cached group info
    if groupId notin client.groups:
      return none(ProtocolClient)

    let groupInfo = client.groups[groupId]

    # If we know the leader and have a connection, use it
    if groupInfo.leaderNodeId != 0:
      # Check cached connection - validate it matches current leader
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
  withLock client.lock:
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
  withLock client.lock:
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
  withLock client.lock:
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
  withLock client.lock:
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
  withLock client.lock:
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
    let res = conn.kvGetInGroup(key, groupId, filter = filter, txnId = txnId,
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
    withLock client.lock:
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
    withLock client.lock:
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

  for groupId in groupIds:
    # Compute per-group scan bounds for data tables
    let (groupStart, groupEnd) = if isDataTable:
      narrowScanBoundsToGroup(startKey, endKey, tableId, groupId)
    else:
      (startKey, endKey)

    for attempt in 0 ..< 3:
      let connOpt = client.getGroupLeaderConnection(groupId)
      if connOpt.isNone:
        # Skip this group if we can't connect - it may not have data for this range
        when defined(debug):
          try:
            {.cast(gcsafe).}:
              debug("kvScan: no connection for group", {
                  "groupId": $groupId}.toTable)
          except:
            discard
        break

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
        break # Success, move to next group

      if res.error.kind == peNotLeader:
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

      break # Other error, skip this group

  # Convert to result sequence
  var entries: seq[tuple[key, value: string]] = @[]
  for key, value in resultMap.pairs:
    entries.add((key: key, value: value))

  # Sort by key for consistent ordering
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
                      kvMsgs.WireFilterExpr)): Result[StreamingScanClient,

ProtocolError] =
  ## Streaming scan across ALL groups in the space.
  ## For multi-group spaces, data is sharded across groups by primary key hash.
  ## This method creates a streaming client that merges results from all groups
  ## in key order using k-way merge.
  ## filter: optional server-side filter for reducing network traffic
  if not client.initialized.load(moRelaxed):
    return peErr(newProtocolError(peInternal, "client not initialized"))

  # Determine which table this scan is for
  let tableId = client.getTableIdFromKey(startKey)
  let groupIds = client.getGroupsForTable(tableId)

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
        readTimestamp, groupId, filter)

  # For multi-group, use k-way merge: open all group streams simultaneously
  # and merge results by key order. This produces globally sorted output
  # without needing a post-hoc sort.
  let chunkSize = 100'u32 # Default chunk size for streaming
  var groupStreams: seq[StreamingScanClient] = @[]
  var errors: seq[string] = @[]

  for groupId in groupIds:
    # Compute per-group scan bounds for efficient reads
    let (groupStart, groupEnd) = if isDataTable:
      narrowScanBoundsToGroup(startKey, endKey, tableId, groupId)
    else:
      (startKey, endKey)

    var connOpt: Option[ProtocolClient] = none(ProtocolClient)
    try:
      connOpt = client.getGroupLeaderConnection(groupId)
    except KeyError:
      discard

    if connOpt.isNone:
      # Skip this group if we can't connect
      continue

    let conn = connOpt.get()
    let streamRes = conn.kvStreamScan(groupStart, groupEnd, 0,
        chunkSize, 0, txnId, readTimestamp, groupId, filter)
    if streamRes.isOk:
      groupStreams.add(streamRes.value)
    elif streamRes.error.kind == peNotLeader:
      # Try to update leader and retry once
      if streamRes.error.leaderRedirect.leaderId != 0:
        client.updateLeaderFromRedirect(groupId, streamRes.error.leaderRedirect)
      else:
        discard client.refreshMetadata()
      try:
        let connOpt2 = client.getGroupLeaderConnection(groupId)
        if connOpt2.isSome:
          let conn2 = connOpt2.get()
          let streamRes2 = conn2.kvStreamScan(groupStart, groupEnd, 0,
              chunkSize, 0, txnId, readTimestamp, groupId, filter)
          if streamRes2.isOk:
            groupStreams.add(streamRes2.value)
      except KeyError:
        discard
    else:
      errors.add($groupId & ": " & streamRes.error.msg)

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
  let mergeClient = newKWayMergeScanClient(groupStreams, limit)
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
  withLock client.lock:
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
  withLock client.lock:
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
  for attempt in 0 ..< 3:
    # Get connection to META group leader
    let connOpt = client.getGroupLeaderConnection(META_GROUP_ID)
    if connOpt.isNone:
      return spaceOpErr("no connection to META group leader")

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
        continue
      return spaceOpErr(res.error.msg)

    let resp = res.value
    if not resp.success:
      return spaceOpErr(resp.error)

    # Update local cache with new space and group records
    withLock client.lock:
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
      return spaceOpErr(res.error.msg)

    let resp = res.value
    if not resp.success:
      return spaceOpErr(resp.error)

    # Update local cache - remove space and groups
    withLock client.lock:
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
  withLock client.lock:
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
