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

import std/[options, tables as stdtables, sets, locks, atomics, strutils,
    hashes, algorithm, os, sequtils]
import posix
import ../core/types
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
    spaceId*: ULID
    leaderNodeId*: uint32
    replicaNodeIds*: seq[uint32]

  TableInfo* = object
    ## Cached information about a table
    tableId*: uint32
    name*: string
    spaceId*: ULID

  SpaceInfo* = object
    ## Cached information about a space
    spaceId*: ULID
    name*: string
    groupIds*: seq[GroupID]

  FractioClientConfig* = object
    ## Configuration for FractioClient
    initialHost*: string      ## Initial node to connect to
    initialPort*: int         ## Initial node's client port
    connectionTimeoutMs*: int ## Timeout for connections
    requestTimeoutMs*: int    ## Timeout for requests
    refreshIntervalMs*: int   ## How often to refresh metadata
    autoRefresh*: bool        ## Automatically refresh metadata

  FractioClient* = ref object
    ## Main client for Fractio with leader-aware routing
    config*: FractioClientConfig

    # Cached cluster metadata
    nodes*: stdtables.Table[uint32, NodeInfo]    # nodeId -> NodeInfo
    groups*: stdtables.Table[GroupID, GroupInfo] # groupId -> GroupInfo
    tables*: stdtables.Table[uint32, TableInfo]  # tableId -> TableInfo
    spaces*: stdtables.Table[ULID, SpaceInfo]    # spaceId -> SpaceInfo

    # Active connections to leaders
    leaderConnections*: stdtables.Table[GroupID,
        ProtocolClient]                          # groupId -> connection to leader

    # Key prefix to group mapping (for routing)
    keyPrefixToGroup*: stdtables.Table[string, GroupID]

    # Lock for thread-safe access
    lock*: Lock

    # State
    initialized*: Atomic[bool]
    lastRefreshNs*: Atomic[int64]
    activeTxnId*: uint64
    activeReadTs*: uint64

# =============================================================================
# Result type helpers
# =============================================================================

type
  KVOpResult*[T] = Result[T, string]
    ## Result type for KV operations that return a value

  KVOpVoidResult* = object
    ## Result type for KV operations that don't return a value (put, delete)
    isOk*: bool
    err*: string

proc kvOpOk*[T](v: T): KVOpResult[T] =
  KVOpResult[T](isOk: true, val: v)

proc kvOpErr*[T](msg: string): KVOpResult[T] =
  KVOpResult[T](isOk: false, err: msg)

proc isErr*[T](r: KVOpResult[T]): bool =
  not r.isOk

# Void result constructors
proc kvVoidOk*(): KVOpVoidResult =
  KVOpVoidResult(isOk: true)

proc kvVoidErr*(msg: string): KVOpVoidResult =
  KVOpVoidResult(isOk: false, err: msg)

proc isErr*(r: KVOpVoidResult): bool =
  not r.isOk

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
  let endKey = encodeTableKey(SYS_NODES_TABLE_ID + 1, "")

  let scanRes = conn.kvScan(startKey, endKey, limit = 1000)
  if scanRes.isErr:
    return false

  withLock client.lock:
    for pair in scanRes.value.pairs:
      # Use MVCC decoder
      let (nodeRec, isDeleted) = decodeNodeRecordFromMVCC(pair.value)
      if not isDeleted:
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
  let endKey = encodeTableKey(SYS_GROUPS_TABLE_ID + 1, "")

  let scanRes = conn.kvScan(startKey, endKey, limit = 1000)
  if scanRes.isErr:
    return false

  withLock client.lock:
    for pair in scanRes.value.pairs:
      # Use MVCC decoder
      let (groupRec, isDeleted) = decodeGroupRecordFromMVCC(pair.value)
      if not isDeleted:
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
  let endKey = encodeTableKey(SYS_TABLES_TABLE_ID + 1, "")

  let scanRes = conn.kvScan(startKey, endKey, limit = 1000)
  if scanRes.isErr:
    return false

  withLock client.lock:
    for pair in scanRes.value.pairs:
      # Use MVCC decoder
      let (tableRec, isDeleted) = decodeTableRecordFromMVCC(pair.value)
      if not isDeleted:
        client.tables[tableRec.tableId] = TableInfo(
          tableId: tableRec.tableId,
          name: tableRec.name,
          spaceId: tableRec.spaceId
        )

  return true

proc fetchSpacesTable(client: FractioClient, conn: ProtocolClient): bool =
  ## Fetch the sys.spaces table and update the cache
  let startKey = encodeTableKey(SYS_SPACES_TABLE_ID, "")
  let endKey = encodeTableKey(SYS_SPACES_TABLE_ID + 1, "")

  let scanRes = conn.kvScan(startKey, endKey, limit = 1000)
  if scanRes.isErr:
    return false

  withLock client.lock:
    for pair in scanRes.value.pairs:
      # Use MVCC decoder
      let (spaceRec, isDeleted) = decodeSpaceRecordFromMVCC(pair.value)
      if not isDeleted:
        var groupIds: seq[GroupID] = @[]
        for gid in spaceRec.groupIds:
          groupIds.add(groupIDFromULID(gid))

        client.spaces[spaceRec.spaceId] = SpaceInfo(
          spaceId: spaceRec.spaceId,
          name: spaceRec.name,
          groupIds: groupIds
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

  withLock client.lock:
    # Check if we have cached group info
    if groupId notin client.groups:
      return none(ProtocolClient)

    let groupInfo = client.groups[groupId]

    # If we know the leader and have a connection, use it
    if groupInfo.leaderNodeId != 0:
      # Check cached connection
      if groupId in client.leaderConnections:
        let cached = client.leaderConnections[groupId]
        if cached != nil and cached.connected.load(moRelaxed):
          return some(cached)

      # Try to connect to leader (use internal version since we hold the lock)
      let connOpt = client.getNodeConnectionInternal(groupInfo.leaderNodeId)
      if connOpt.isSome:
        client.leaderConnections[groupId] = connOpt.get()
        return connOpt

    # Leader unknown - try all replicas until we find one that works
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

  return true

proc updateLeaderFromRedirect*(client: FractioClient, groupId: GroupID,
    redirect: LeaderRedirect): bool =
  ## Update the leader connection based on redirect info from a NOT_LEADER error.
  ## This is faster than refreshing all metadata because we connect directly
  ## to the new leader.
  if redirect.leaderId == 0:
    return false

  # Debug logging
  try:
    {.cast(gcsafe).}:
      debug("updateLeaderFromRedirect", {"groupId": $groupId,
          "leaderId": $redirect.leaderId,
          "leaderHost": redirect.leaderHost,
          "leaderClientPort": $redirect.leaderClientPort}.toTable)
  except:
    discard

  withLock client.lock:
    # Clear old connection if any
    if groupId in client.leaderConnections:
      try:
        client.leaderConnections[groupId].disconnect()
      except: discard
      client.leaderConnections.del(groupId)

    # Update group info with new leader
    if groupId in client.groups:
      var groupInfo = client.groups[groupId]
      groupInfo.leaderNodeId = redirect.leaderId
      client.groups[groupId] = groupInfo

    # Connect to the new leader
    let connOpt = client.connectToNode(redirect.leaderHost, int(
        redirect.leaderClientPort))
    if connOpt.isSome:
      client.leaderConnections[groupId] = connOpt.get()

      # Also update nodes cache if this node is known
      if redirect.leaderId in client.nodes:
        var nodeInfo = client.nodes[redirect.leaderId]
        nodeInfo.client = connOpt.get()
        client.nodes[redirect.leaderId] = nodeInfo

      return true

  return false

# =============================================================================
# Key routing
# =============================================================================

proc routeToGroup(primaryKey: string, groupIds: seq[GroupID]): GroupID =
  ## Hash-route a primary key to one of the space's groups.
  ## primaryKey should be the bare key value (e.g., "1" not "/t/0000000100/d/1")
  if groupIds.len == 0:
    return META_GROUP_ID
  if groupIds.len == 1:
    return groupIds[0]
  let h = hash(primaryKey)
  let idx = abs(h) mod groupIds.len
  groupIds[idx]

proc getGroupForKey*(client: FractioClient, key: string): GroupID =
  ## Determine which group owns a given key.
  ## Returns empty GroupID (all zeros) if the group cannot be determined.

  # System tables (tableId 1-7) are in the meta group
  if key.startsWith(TABLE_KEY_PREFIX):
    let afterPrefix = key[TABLE_KEY_PREFIX.len .. ^1]
    if afterPrefix.len >= TABLE_ID_WIDTH:
      try:
        let tableIdStr = afterPrefix[0 ..< TABLE_ID_WIDTH]
        let tableId = parseUInt(tableIdStr).uint32
        if tableId <= MAX_META_GROUP_TABLE_ID:
          return META_GROUP_ID
        else:
          # For data tables, look up table->space->group mapping
          withLock client.lock:
            if tableId in client.tables:
              let tableInfo = client.tables[tableId]
              let spaceId = tableInfo.spaceId
              if spaceId in client.spaces:
                let spaceInfo = client.spaces[spaceId]
                if spaceInfo.groupIds.len > 0:
                  # Extract the primary key portion for hashing
                  # Key format: /t/0000000100/d/<pk> or /t/0000000100/<pk>
                  let afterTableId = afterPrefix[TABLE_ID_WIDTH .. ^1]
                  var pk = afterTableId
                  # Strip "/d/" prefix if present (data rows)
                  if pk.startsWith("/d/"):
                    pk = pk[3 .. ^1]
                  # Hash-based routing for multi-group spaces
                  let groupId = routeToGroup(pk, spaceInfo.groupIds)
                  return groupId
          # Fall back to default data group for tables without space assignment
          return DATA_GROUP_START_ID
      except ValueError:
        discard

  # Default to meta group for non-table keys or if parsing failed
  return META_GROUP_ID

proc getGroupsForTable*(client: FractioClient, tableId: uint32): seq[GroupID] =
  ## Get all groups that store data for a given table.
  ## For multi-group spaces, returns ALL groups in the space.
  ## Returns empty seq if the table is not found.

  withLock client.lock:
    if tableId in client.tables:
      let tableInfo = client.tables[tableId]
      let spaceId = tableInfo.spaceId
      if spaceId in client.spaces:
        let spaceInfo = client.spaces[spaceId]
        return spaceInfo.groupIds

  # Fall back to meta group
  return @[META_GROUP_ID]

proc getTableIdFromKey*(client: FractioClient, key: string): uint32 =
  ## Extract tableId from a key, returns 0 if not parseable.
  if not key.startsWith(TABLE_KEY_PREFIX):
    return 0

  let afterPrefix = key[TABLE_KEY_PREFIX.len .. ^1]
  if afterPrefix.len < TABLE_ID_WIDTH:
    return 0

  try:
    let tableIdStr = afterPrefix[0 ..< TABLE_ID_WIDTH]
    return parseUInt(tableIdStr).uint32
  except ValueError:
    return 0

# =============================================================================
# KV Operations
# =============================================================================

proc kvGet*(client: FractioClient, key: string, txnId: uint64 = 0,
    readTimestamp: uint64 = 0): KVOpResult[Option[string]] =
  ## Get a value by key, routing to the correct group leader
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
    let res = conn.kvGet(key, txnId = txnId, readTimestamp = readTimestamp)

    if res.isOk:
      if res.value.found:
        return kvOpOk(some(res.value.value))
      else:
        return kvOpOk(none(string))

    # Check for "not leader" error with redirect info
    if res.error.kind == peNotLeader:
      # Try to use redirect info for faster recovery
      if res.error.leaderRedirect.leaderId != 0:
        if client.updateLeaderFromRedirect(groupId, res.error.leaderRedirect):
          continue
      # Fall back to metadata refresh if no redirect info
      if not client.refreshGroupLeader(groupId):
        return kvOpErr[Option[string]]("failed to refresh group leader")
      if attempt < maxRetries - 1:
        sleep(baseBackoffMs + attempt * 5)
      continue

    return kvOpErr[Option[string]](res.error.msg)

  return kvOpErr[Option[string]]("too many retries")

proc kvPut*(client: FractioClient, key, value: string,
    txnId: uint64 = 0): KVOpVoidResult =
  ## Put a key-value pair, routing to the correct group leader
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
        # Try to use redirect info for faster recovery
        if res.error.leaderRedirect.leaderId != 0:
          if client.updateLeaderFromRedirect(groupId, res.error.leaderRedirect):
            continue
        # Fall back to metadata refresh
        if not client.refreshGroupLeader(groupId):
          return kvVoidErr("failed to refresh group leader")
        if attempt < maxRetries - 1:
          sleep(baseBackoffMs + attempt * 5)
        continue
      elif isNotLeaderError(res.error.msg):
        # Legacy: check message content for backward compatibility
        if not client.refreshGroupLeader(groupId):
          return kvVoidErr("failed to refresh group leader")
        if attempt < maxRetries - 1:
          sleep(baseBackoffMs + attempt * 5)
        continue

    let errMsg = if res.isOk: "put failed with status " & $res.value.status
                 else: "server error: " & res.error.msg
    return kvVoidErr(errMsg)

  return kvVoidErr("too many retries")

proc kvDelete*(client: FractioClient, key: string,
    txnId: uint64 = 0): KVOpVoidResult =
  ## Delete a key, routing to the correct group leader
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
      # Try to use redirect info for faster recovery
      if res.error.leaderRedirect.leaderId != 0:
        if client.updateLeaderFromRedirect(groupId, res.error.leaderRedirect):
          continue
      # Fall back to metadata refresh
      if not client.refreshGroupLeader(groupId):
        return kvVoidErr("failed to refresh group leader")
      if attempt < maxRetries - 1:
        sleep(baseBackoffMs + attempt * 5)
      continue

    let errMsg = if res.isOk: "delete failed" else: res.error.msg
    return kvVoidErr(errMsg)

  return kvVoidErr("too many retries")

proc kvScan*(client: FractioClient, startKey, endKey: string,
    limit: uint32 = 0, txnId: uint64 = 0,
    readTimestamp: uint64 = 0): KVOpResult[seq[tuple[key, value: string]]] =
  ## Scan a key range across ALL groups in the space.
  ## For multi-group spaces, data is sharded across groups by primary key hash,
  ## so we must scan ALL groups and merge results.
  if not client.initialized.load(moRelaxed):
    return kvOpErr[seq[tuple[key, value: string]]]("client not initialized")

  # Determine which table this scan is for
  let tableId = client.getTableIdFromKey(startKey)
  let groupIds = client.getGroupsForTable(tableId)

  echo "DEBUG kvScan: tableId=", tableId, " groupIds=", groupIds.len,
      " startKey=", startKey, " endKey=", endKey

  # Collect results from all groups, deduplicating by key
  var resultMap = stdtables.initTable[string, string]()

  for groupId in groupIds:
    echo "DEBUG kvScan: scanning groupId=", groupId
    for attempt in 0 ..< 3:
      let connOpt = client.getGroupLeaderConnection(groupId)
      if connOpt.isNone:
        echo "DEBUG kvScan: no connection for groupId=", groupId
        # Skip this group if we can't connect - it may not have data for this range
        break

      let conn = connOpt.get()
      let res = conn.kvScan(startKey, endKey, 0, txnId = txnId,
                            readTimestamp = readTimestamp)

      if res.isOk:
        echo "DEBUG kvScan: scan succeeded, entries=", res.value.pairs.len
        for pair in res.value.pairs:
          # Deduplicate: keep first occurrence
          if pair.key notin resultMap:
            resultMap[pair.key] = pair.value
        break # Success, move to next group

      if res.error.kind == peNotLeader:
        echo "DEBUG kvScan: not leader for groupId=", groupId
        if not client.refreshGroupLeader(groupId):
          break # Give up on this group
        continue # Retry this group

      echo "DEBUG kvScan: error for groupId=", groupId, " error=", res.error.msg
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
# Transaction Operations
# =============================================================================

proc beginTxn*(client: FractioClient): KVOpResult[tuple[txnId,
    readTimestamp: uint64]] =
  ## Begin a new transaction by contacting any node (prefers meta group leader)
  if not client.initialized.load(moRelaxed):
    if not client.initialize():
      return kvOpErr[tuple[txnId, readTimestamp: uint64]]("failed to initialize client")

  # Use meta group leader if possible, otherwise any connection
  let connOpt = client.getGroupLeaderConnection(META_GROUP_ID)
  if connOpt.isNone:
    echo "DEBUG: beginTxn failed: no connection to meta group leader"
    return kvOpErr[tuple[txnId, readTimestamp: uint64]]("no connection for beginTxn")

  let conn = connOpt.get()
  let res = conn.beginTxn()
  if res.isOk:
    return kvOpOk((txnId: res.value.txnId,
        readTimestamp: res.value.readTimestamp))
  else:
    echo "DEBUG: beginTxn failed: ", res.error.msg
    return kvOpErr[tuple[txnId, readTimestamp: uint64]](res.error.msg)

proc commitTxn*(client: FractioClient, txnId: uint64): KVOpVoidResult =
  ## Commit a transaction.
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

proc rollbackTxn*(client: FractioClient, txnId: uint64): KVOpVoidResult =
  ## Rollback a transaction.
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
# Space Management
# =============================================================================

type
  SpaceOpResult* = object
    ## Result type for space operations
    isOk*: bool
    err*: string
    ## On success:
    spaceId*: ULID
    groupCount*: int32
    groupIds*: seq[GroupID]

proc spaceOpOk(spaceId: ULID, groupCount: int32, groupIds: seq[
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
      # Check for redirect
      if res.error.kind == peNotLeader and res.error.leaderRedirect.leaderId != 0:
        if client.updateLeaderFromRedirect(META_GROUP_ID,
            res.error.leaderRedirect):
          continue
      return spaceOpErr(res.error.msg)

    let resp = res.value
    if not resp.success:
      return spaceOpErr(resp.error)

    # Update local cache with new space and group records
    withLock client.lock:
      # Parse and cache the space record
      let spaceRec = decodeSpaceRecord(resp.spaceRecord)
      var groupIds: seq[GroupID] = @[]
      for gid in spaceRec.groupIds:
        groupIds.add(groupIDFromULID(gid))

      client.spaces[spaceRec.spaceId] = SpaceInfo(
        spaceId: spaceRec.spaceId,
        name: spaceRec.name,
        groupIds: groupIds
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
      # Check for redirect
      if res.error.kind == peNotLeader and res.error.leaderRedirect.leaderId != 0:
        if client.updateLeaderFromRedirect(META_GROUP_ID,
            res.error.leaderRedirect):
          continue
      return spaceOpErr(res.error.msg)

    let resp = res.value
    if not resp.success:
      return spaceOpErr(resp.error)

    # Update local cache - remove space and groups
    withLock client.lock:
      # Find and remove the space by name (since resp.spaceId is ULID)
      var spaceIdToRemove: ULID
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
        let groupId = groupIDFromULID(gid)
        client.groups.del(groupId)
        client.leaderConnections.del(groupId)

    var deletedGroupIds: seq[GroupID] = @[]
    for gid in resp.deletedGroupIds:
      deletedGroupIds.add(groupIDFromULID(gid))
    return spaceOpOk(resp.spaceId, resp.deletedGroupIds.len.int32,
      deletedGroupIds)

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
