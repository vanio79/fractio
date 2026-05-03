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

    # Key prefix to group mapping (for routing)
    keyPrefixToGroup*: stdtables.Table[string, GroupID]

    # Lock for thread-safe access
    lock*: Lock

    # State
    initialized*: Atomic[bool]
    lastRefreshNs*: Atomic[int64]
    activeTxnId*: TransactionID
    activeReadTs*: uint64

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
    autoRefresh: true
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
  ## Use requestTimeoutMs for socket operations since requests (like CREATE SPACE)
  ## can take longer than connection establishment.
  let cfg = ClientConfig(
    host: host,
    port: port,
    timeoutMs: client.config.requestTimeoutMs,
    clientId: "fractio-client",
    authMethod: amNone,
    authData: ""
  )
  let protoClient = newProtocolClient(cfg)
  if protoClient.connect().isOk:
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
# Metadata refresh
# =============================================================================

proc fetchNodesTable(client: FractioClient, conn: ProtocolClient): bool =
  ## Fetch the sys.nodes table and update the cache
  let startKey = encodeTableKey(SYS_NODES_TABLE_ID, "")
  let endKey = makeScanEndKey(SYS_NODES_TABLE_ID)

  let scanRes = conn.kvScan(startKey, endKey, limit = 1000)
  if scanRes.isErr:
    return false

  withLock client.lock:
    for pair in scanRes.value.pairs:
      # Server already stripped MVCC header - use direct decoder
      let nodeRec = decodeNodeRecord(pair.value)
      # Always add to cache (server filters out deleted entries)
      client.nodes[nodeRec.nodeId] = NodeInfo(
        nodeId: nodeRec.nodeId,
        host: nodeRec.host,
        clientPort: nodeRec.clientPort,
        status: nodeRec.status,
        client: nil # Will be created on-demand
      )

  return true

proc fetchGroupsTable(client: FractioClient, conn: ProtocolClient): bool =
  ## Fetch the sys.groups table and update the cache
  let startKey = encodeTableKey(SYS_GROUPS_TABLE_ID, "")
  let endKey = makeScanEndKey(SYS_GROUPS_TABLE_ID)

  let scanRes = conn.kvScan(startKey, endKey, limit = 1000)
  if scanRes.isErr:
    return false

  withLock client.lock:
    for pair in scanRes.value.pairs:
      # Server already stripped MVCC header - use direct decoder
      let groupRec = decodeGroupRecord(pair.value)
      # Always add to cache (server filters out deleted entries)
      var replicaNodeIds: seq[uint32] = @[]
      for rep in groupRec.replicas:
        replicaNodeIds.add(rep.nodeId)

      client.groups[groupIDFromULID(groupRec.groupId)] = GroupInfo(
        groupId: groupIDFromULID(groupRec.groupId),
        spaceId: groupRec.spaceId,
        leaderNodeId: groupRec.leader,
        replicaNodeIds: replicaNodeIds
      )

  return true

proc fetchTablesTable(client: FractioClient, conn: ProtocolClient): bool =
  ## Fetch the sys.tables table and update the cache
  let startKey = encodeTableKey(SYS_TABLES_TABLE_ID, "")
  let endKey = makeScanEndKey(SYS_TABLES_TABLE_ID)

  let scanRes = conn.kvScan(startKey, endKey, limit = 1000)
  if scanRes.isErr:
    return false

  withLock client.lock:
    for pair in scanRes.value.pairs:
      # Server already stripped MVCC header - use direct decoder
      let tableRec = decodeTableRecord(pair.value)
      # Always add to cache (server filters out deleted entries)
      client.tables[tableRec.tableId] = TableInfo(
        tableId: tableRec.tableId,
        name: tableRec.name,
        spaceId: tableRec.spaceId
      )

  return true

proc fetchSpacesTable(client: FractioClient, conn: ProtocolClient): bool =
  ## Fetch the sys.spaces table and update the cache
  let startKey = encodeTableKey(SYS_SPACES_TABLE_ID, "")
  let endKey = makeScanEndKey(SYS_SPACES_TABLE_ID)

  let scanRes = conn.kvScan(startKey, endKey, limit = 1000)
  if scanRes.isErr:
    return false

  withLock client.lock:
    for pair in scanRes.value.pairs:
      # Server already stripped MVCC header - use direct decoder
      let spaceRec = decodeSpaceRecord(pair.value)
      # Always add to cache (server filters out deleted entries)
      client.spaces[spaceRec.spaceId] = SpaceInfo(
        spaceId: spaceRec.spaceId,
        name: spaceRec.name,
        groupIds: spaceRec.groupIds,
        oldGroupIds: spaceRec.oldGroupIds,
        rebalancing: spaceRec.rebalancing
      )

  return true

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
  if not client.fetchNodesTable(conn):
    return false
  if not client.fetchGroupsTable(conn):
    return false
  if not client.fetchTablesTable(conn):
    # Not fatal - tables may not exist yet
    discard
  if not client.fetchSpacesTable(conn):
    # Not fatal - spaces may not exist yet
    discard

  client.initialized.store(true, moRelaxed)
  return true

proc refreshMetadata*(client: FractioClient): bool =
  ## Refresh cached metadata from the cluster
  if not client.initialized.load(moRelaxed):
    return client.initialize()

  # Try to use an existing connection, or connect to initial node
  var conn: ProtocolClient = nil
  var shouldDisconnect = false

  withLock client.lock:
    # Try to find an existing connection
    for nodeId, nodeInfo in client.nodes:
      if nodeInfo.client != nil and nodeInfo.client.connected.load(moRelaxed):
        conn = nodeInfo.client
        break

  if conn.isNil:
    # Connect to initial node
    let connOpt = client.connectToNode(client.config.initialHost,
        client.config.initialPort)
    if connOpt.isNone:
      return false
    conn = connOpt.get()
    shouldDisconnect = true

  # Perform the refresh
  result = client.fetchNodesTable(conn) and client.fetchGroupsTable(conn) and
           client.fetchTablesTable(conn) and client.fetchSpacesTable(conn)

  # Disconnect if we created a temporary connection
  if shouldDisconnect:
    conn.disconnect()

# =============================================================================
# Leader connection management
# =============================================================================

proc getGroupLeaderConnection*(client: FractioClient, groupId: GroupID): Option[
    ProtocolClient] =
  ## Get a connection to the leader of a specific group.
  ## Uses cached leader info, falls back to trying all replicas.

  {.cast(gcsafe).}: echo "[Nim] getGroupLeaderConnection: groupId=", groupId

  withLock client.lock:
    # Check if we have cached group info
    if groupId notin client.groups:
      {.cast(gcsafe).}: echo "[Nim] getGroupLeaderConnection: groupId not in client.groups"
      return none(ProtocolClient)

    let groupInfo = client.groups[groupId]
    {.cast(gcsafe).}: echo "[Nim] getGroupLeaderConnection: leaderNodeId=",
        groupInfo.leaderNodeId, " replicas=", groupInfo.replicaNodeIds.len

    # If we know the leader and have a connection, use it
    if groupInfo.leaderNodeId != 0:
      # Check cached connection
      if groupId in client.leaderConnections:
        let cached = client.leaderConnections[groupId]
        if cached != nil and cached.connected.load(moRelaxed):
          {.cast(gcsafe).}: echo "[Nim] getGroupLeaderConnection: using cached connection"
          return some(cached)

      # Try to connect to leader (use internal version since we hold the lock)
      {.cast(gcsafe).}: echo "[Nim] getGroupLeaderConnection: trying to connect to leader nodeId=",
          groupInfo.leaderNodeId
      let connOpt = client.getNodeConnectionInternal(groupInfo.leaderNodeId)
      if connOpt.isSome:
        client.leaderConnections[groupId] = connOpt.get()
        {.cast(gcsafe).}: echo "[Nim] getGroupLeaderConnection: connected to leader nodeId=",
            groupInfo.leaderNodeId
        return connOpt
      {.cast(gcsafe).}: echo "[Nim] getGroupLeaderConnection: leader connection FAILED nodeId=",
          groupInfo.leaderNodeId
      # Leader connection failed - fall through to try replicas

    # Leader unknown or connection to known leader failed - try all replicas
    {.cast(gcsafe).}: echo "[Nim] getGroupLeaderConnection: trying replicas"
    for nodeId in groupInfo.replicaNodeIds:
      let connOpt = client.getNodeConnectionInternal(nodeId)
      if connOpt.isSome:
        {.cast(gcsafe).}: echo "[Nim] getGroupLeaderConnection: connected to replica nodeId=", nodeId
        # We'll try this connection; if it's not the leader,
        # the operation will fail and we'll retry
        return connOpt

  {.cast(gcsafe).}: echo "[Nim] getGroupLeaderConnection: returning none"
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
    if groupId in client.groups:
      var info = client.groups[groupId]
      info.leaderNodeId = 0
      client.groups[groupId] = info

proc refreshGroupLeader(client: FractioClient, groupId: GroupID): bool =
  ## Refresh leader info for a specific group after a "not leader" error

  # Clear cached connection
  withLock client.lock:
    if groupId in client.leaderConnections:
      try:
        client.leaderConnections[groupId].disconnect()
      except: discard
      client.leaderConnections.del(groupId)

  # Refresh metadata
  if not client.refreshMetadata():
    return false

  # Add a small delay to allow leadership to stabilize
  # This is a workaround for race conditions between Raft leadership changes
  # and sys.groups metadata updates
  when defined(posix):
    discard posix.sleep(50)

  true

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
  if not client.initialized.load(moRelaxed):
    return kvOpErr[Option[string]]("client not initialized")

  let groupId = client.getGroupForKey(key)

  const maxRetries = 100
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
    let res = conn.kvGet(key, txnId = txnId, readTimestamp = readTimestamp)

    if res.isOk:
      if res.value.found:
        return kvOpOk(some(res.value.value))
      else:
        return kvOpOk(none(string))

    # Check for "not leader" error - refresh metadata to find new leader
    if res.error.kind == peNotLeader:
      if not client.refreshMetadata():
        discard
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

  const maxRetries = 100
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
    let res = conn.kvGet(key, filter = filter, txnId = txnId,
                         readTimestamp = readTimestamp)

    if res.isOk:
      if res.value.found:
        return kvOpOk(some(res.value.value))
      else:
        # Row either doesn't exist or doesn't pass filter
        return kvOpOk(none(string))

    # Check for "not leader" error - refresh metadata to find new leader
    if res.error.kind == peNotLeader:
      if not client.refreshMetadata():
        discard
      if attempt < maxRetries - 1:
        sleep(baseBackoffMs + attempt * 5)
      continue

    return kvOpErr[Option[string]](res.error.msg)

  return kvOpErr[Option[string]]("too many retries")

method put*(client: FractioClient, key: string, value: string,
           txnId: TransactionID = zeroTransactionID()): KVOpVoidResult =
  ## Put a key-value pair, routing to the correct group leader.
  ## Implements KVStore interface.
  if not client.initialized.load(moRelaxed):
    return kvVoidErr("client not initialized")

  let groupId = client.getGroupForKey(key)

  # Retry with backoff to handle leader election races during group creation.
  # New groups may need time for leader election, especially during CREATE SPACE.
  const maxRetries = 100
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
    let res = conn.kvPut(key, value, txnId = txnId)

    if res.isOk and res.value.status == kvMsgs.PutStatusOK:
      return kvVoidOk()

    # Check for "not leader" error
    if res.isErr:
      if res.error.kind == peNotLeader:
        # Refresh metadata to find new leader
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

    let errMsg = if res.isOk: "put failed with status " & $res.value.status
                 else: "server error: " & res.error.msg
    return kvVoidErr(errMsg)

  return kvVoidErr("too many retries")

method delete*(client: FractioClient, key: string,
              txnId: TransactionID = zeroTransactionID()): KVOpVoidResult =
  ## Delete a key, routing to the correct group leader.
  ## Implements KVStore interface.
  if not client.initialized.load(moRelaxed):
    return kvVoidErr("client not initialized")

  let groupId = client.getGroupForKey(key)

  const maxRetries = 100
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
    let res = conn.kvDelete(key, txnId = txnId)

    if res.isOk and res.value.status in {kvMsgs.DelStatusDeleted,
        kvMsgs.DelStatusNotFound}:
      return kvVoidOk()

    if res.isErr and res.error.kind == peNotLeader:
      # Refresh metadata to find new leader
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
  ## Implements KVStore interface.
  if not client.initialized.load(moRelaxed):
    return kvOpErr[seq[tuple[key, value: string]]]("client not initialized")

  # Determine which table this scan is for
  let tableId = client.getTableIdFromKey(startKey)
  let groupIds = client.getGroupsForTable(tableId)

  # Collect results from all groups, deduplicating by key
  var resultMap = stdtables.initTable[string, string]()

  for groupId in groupIds:
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
      # Pass groupId for server-side routing filter
      let res = conn.kvScan(startKey, endKey, 0, txnId = txnId,
                            readTimestamp = readTimestamp,
                            groupId = groupId)

      if res.isOk:
        for pair in res.value.pairs:
          # Deduplicate: keep first occurrence
          if pair.key notin resultMap:
            resultMap[pair.key] = pair.value
        break # Success, move to next group

      if res.error.kind == peNotLeader:
        discard client.refreshMetadata()
        continue # Retry this group

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

# =============================================================================
# Multi-Group Streaming Scan Helpers
# =============================================================================

type
  MultiGroupScanContext* = ref object
    ## Context for multi-group streaming scan, passed via callback closure.
    ## This holds the state needed to iterate through groups sequentially.
    fractioClient*: FractioClient
    groupIds*: seq[GroupID]
    currentGroupIndex*: int
    startKey*: string
    endKey*: string
    txnId*: TransactionID
    readTimestamp*: uint64
    chunkSize*: uint32
    filter*: Option[kvMsgs.WireFilterExpr] ## Server-side filter for reducing network traffic

proc startNextGroupStream(ctx: MultiGroupScanContext): Result[
    StreamingScanClient, ProtocolError] =
  ## Start streaming scan for the next group in the sequence.
  ## Returns the StreamingScanClient for that group, or error if no more groups.
  if ctx.currentGroupIndex >= ctx.groupIds.len:
    return peErr(newProtocolError(peInternal, "no more groups to scan"))

  let groupId = ctx.groupIds[ctx.currentGroupIndex]
  inc ctx.currentGroupIndex

  var connOpt: Option[ProtocolClient] = none(ProtocolClient)
  try:
    connOpt = ctx.fractioClient.getGroupLeaderConnection(groupId)
  except KeyError:
    discard

  if connOpt.isNone:
    # Try to refresh metadata and retry once
    discard ctx.fractioClient.refreshMetadata()
    try:
      connOpt = ctx.fractioClient.getGroupLeaderConnection(groupId)
    except KeyError:
      return peErr(newProtocolError(peInternal,
          "group not found in metadata for group " & $groupId))
    if connOpt.isNone:
      return peErr(newProtocolError(peInternal,
          "no connection to group leader for group " & $groupId))
    let conn = connOpt.get()
    return conn.kvStreamScan(ctx.startKey, ctx.endKey, 0,
        ctx.chunkSize, 0, ctx.txnId, ctx.readTimestamp, groupId, ctx.filter)

  let conn = connOpt.get()
  let streamRes = conn.kvStreamScan(ctx.startKey, ctx.endKey, 0,
      ctx.chunkSize, 0, ctx.txnId, ctx.readTimestamp, groupId, ctx.filter)

  if streamRes.isErr:
    # Check if it's a not-leader error and retry
    if streamRes.error.kind == peNotLeader:
      discard ctx.fractioClient.refreshMetadata()
      try:
        connOpt = ctx.fractioClient.getGroupLeaderConnection(groupId)
      except KeyError:
        discard
      if connOpt.isSome:
        let conn2 = connOpt.get()
        let streamRes2 = conn2.kvStreamScan(ctx.startKey, ctx.endKey, 0,
            ctx.chunkSize, 0, ctx.txnId, ctx.readTimestamp, groupId, ctx.filter)
        if streamRes2.isOk:
          return streamRes2
    return peErr(streamRes.error)

  return streamRes

proc createNextGroupCallback(ctx: MultiGroupScanContext): NextGroupCallback =
  ## Create a closure callback for starting the next group's stream.
  ## The callback captures the context and advances through groups.
  result = proc(): Result[StreamingScanClient, ProtocolError] {.closure, gcsafe,
      raises: [].} =
    try:
      startNextGroupStream(ctx)
    except CatchableError as e:
      peErr(newProtocolError(peInternal, "exception in multi-group callback: " & e.msg))

proc consumeMultiGroupStream(ss: StreamingScanClient): seq[kvMsgs.ScanPair] =
  ## Consume all pairs from a multi-group stream.
  ## Warning: For large result sets, this defeats the purpose of streaming.
  var pairs: seq[kvMsgs.ScanPair] = @[]
  while ss.hasNext():
    let pairOpt = ss.nextPair()
    if pairOpt.isSome:
      pairs.add(pairOpt.get())
  ss.closeStream()
  # Sort by key for consistent ordering
  pairs.sort(proc(a, b: kvMsgs.ScanPair): int = cmp(a.key, b.key))
  pairs

method streamScan*(client: FractioClient, startKey: string, endKey: string,
                  limit: uint32 = 0,
                  txnId: TransactionID = zeroTransactionID(),
                  readTimestamp: uint64 = 0,
                  filter: Option[kvMsgs.WireFilterExpr] = none(
                      kvMsgs.WireFilterExpr)): Result[StreamingScanClient,

ProtocolError] =
  ## Streaming scan across ALL groups in the space.
  ## For multi-group spaces, data is sharded across groups by primary key hash.
  ## This method creates a streaming client that fetches results from one group
  ## at a time, merging results in key order.
  ## For multi-group scans, the client iterates through groups sequentially.
  ## filter: optional server-side filter for reducing network traffic
  if not client.initialized.load(moRelaxed):
    return peErr(newProtocolError(peInternal, "client not initialized"))

  # Determine which table this scan is for
  let tableId = client.getTableIdFromKey(startKey)
  let groupIds = client.getGroupsForTable(tableId)

  # For single group, use direct streaming
  if groupIds.len == 1:
    let groupId = groupIds[0]
    let connOpt = client.getGroupLeaderConnection(groupId)
    if connOpt.isNone:
      return peErr(newProtocolError(peInternal,
          "no connection to group leader"))
    let conn = connOpt.get()
    return conn.kvStreamScan(startKey, endKey, limit, 0, 0, txnId,
        readTimestamp, groupId, filter)

  # For multi-group, create a multi-group streaming scan client
  # that iterates through groups sequentially
  let chunkSize = 100'u32 # Default chunk size for streaming

  # Create context for multi-group scan
  let ctx = new(MultiGroupScanContext)
  ctx.fractioClient = client
  ctx.groupIds = groupIds
  ctx.currentGroupIndex = 0
  ctx.startKey = startKey
  ctx.endKey = endKey
  ctx.txnId = txnId
  ctx.readTimestamp = readTimestamp
  ctx.chunkSize = chunkSize
  ctx.filter = filter

  # Start the first group's stream
  let firstStreamR = startNextGroupStream(ctx)
  if firstStreamR.isErr:
    return peErr(firstStreamR.error)

  let firstStream = firstStreamR.value

  # Create the callback for subsequent groups
  let callback = createNextGroupCallback(ctx)

  # Create multi-group streaming client
  let multiGroupClient = newMultiGroupStreamingScanClient(firstStream, callback, limit)

  return peOk(multiGroupClient)

method beginTxn*(client: FractioClient): KVOpResult[TxnBeginResult] =
  ## Begin a new transaction by contacting any node (prefers meta group leader).
  ## Implements KVStore interface.
  if not client.initialized.load(moRelaxed):
    if not client.initialize():
      return kvOpErr[TxnBeginResult]("failed to initialize client")

  # Use meta group leader if possible, otherwise any connection
  let connOpt = client.getGroupLeaderConnection(META_GROUP_ID)
  if connOpt.isNone:
    return kvOpErr[TxnBeginResult]("no connection for beginTxn")

  let conn = connOpt.get()
  let res = conn.beginTxn()
  if res.isOk:
    return kvOpOk((txnId: res.value.txnId,
        readTimestamp: res.value.readTimestamp))
  else:
    return kvOpErr[TxnBeginResult](res.error.msg)

method commitTxn*(client: FractioClient, txnId: TransactionID): KVOpVoidResult =
  ## Commit a transaction.
  ## Implements KVStore interface.
  # We should send commit to the node that started it, or any node if they share txn state.
  # For now, use meta group leader.
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

method rollbackTxn*(client: FractioClient,
    txnId: TransactionID): KVOpVoidResult =
  ## Rollback a transaction.
  ## Implements KVStore interface.
  let connOpt = client.getGroupLeaderConnection(META_GROUP_ID)
  if connOpt.isNone:
    return kvVoidErr("no connection for rollbackTxn")

  let conn = connOpt.get()
  let res = conn.rollbackTxn(txnId)
  if res.isOk:
    return kvVoidOk()
  else:
    return kvVoidErr(res.error.msg)

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

  const maxRetries = 100
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
    let res = conn.kvGet(key, txnId = txnId, readTimestamp = readTimestamp)

    if res.isOk:
      if res.value.found:
        return kvOpOk(some(res.value.value))
      else:
        return kvOpOk(none(string))

    if res.error.kind == peNotLeader:
      if not client.refreshMetadata():
        discard
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

  const maxRetries = 100
  const baseBackoffMs = 50

  for attempt in 0 ..< maxRetries:
    let connOpt = client.getGroupLeaderConnection(groupId)
    if connOpt.isNone:
      if attempt < maxRetries - 1:
        sleep(baseBackoffMs + attempt * 5)
        continue
      return kvVoidErr("no connection to group leader")

    let conn = connOpt.get()
    let res = conn.kvPut(key, value, txnId = txnId)

    if res.isOk and res.value.status == kvMsgs.PutStatusOK:
      return kvVoidOk()

    if res.isErr:
      if res.error.kind == peNotLeader:
        discard client.refreshMetadata()
        if attempt < maxRetries - 1:
          sleep(baseBackoffMs + attempt * 5)
        continue
      elif isNotLeaderError(res.error.msg):
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

  const maxRetries = 100
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
    let res = conn.kvDelete(key, txnId = txnId)

    if res.isOk and res.value.status in {kvMsgs.DelStatusDeleted,
        kvMsgs.DelStatusNotFound}:
      return kvVoidOk()

    if res.isErr and res.error.kind == peNotLeader:
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
        rebalancing: spaceRec.rebalancing
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
        client.leaderConnections.del(gid)

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
    client.initialized.store(false, moRelaxed)
