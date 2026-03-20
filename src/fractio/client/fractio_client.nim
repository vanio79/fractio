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

import std/[options, tables, sets, locks, atomics, strutils]
import ../protocol/client
import ../protocol/types
import ../protocol/messages/kv as kvMsgs
import ../protocol/messages/txn as txnMsgs
import ../distributed/meta/system_tables
import ../distributed/meta/system_schemas
import ../storage/mvcc/types as mvccTypes

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
    groupId*: uint64
    spaceId*: int32
    leaderNodeId*: uint32
    replicaNodeIds*: seq[uint32]

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
    nodes*: Table[uint32, NodeInfo]   # nodeId -> NodeInfo
    groups*: Table[uint64, GroupInfo] # groupId -> GroupInfo

    # Active connections to leaders
    leaderConnections*: Table[uint64, ProtocolClient] # groupId -> connection to leader

    # Key prefix to group mapping (for routing)
    keyPrefixToGroup*: Table[string, uint64]

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

  echo "DEBUG: fetching nodes table, range: ", startKey, " to ", endKey
  let scanRes = conn.kvScan(startKey, endKey, limit = 1000)
  if scanRes.isErr:
    echo "DEBUG: fetchNodesTable scan failed: ", scanRes.error.msg
    return false

  echo "DEBUG: fetchNodesTable got ", scanRes.value.pairs.len, " entries"
  withLock client.lock:
    for pair in scanRes.value.pairs:
      # Hex log the record
      var hex = ""
      for i in 0 ..< min(pair.value.len, 32):
        hex &= " " & toHex(uint8(pair.value[i]))
      echo "DEBUG: raw record hex:", hex
      
      # Use MVCC decoder
      let (nodeRec, isDeleted) = decodeNodeRecordFromMVCC(pair.value)
      if not isDeleted:
        echo "DEBUG: caching node ", nodeRec.nodeId, " host=", nodeRec.host, " port=", nodeRec.clientPort
        client.nodes[nodeRec.nodeId] = NodeInfo(
          nodeId: nodeRec.nodeId,
          host: nodeRec.host,
          clientPort: nodeRec.clientPort,
          status: nodeRec.status,
          client: nil # Will be created on-demand
        )
      else:
        echo "DEBUG: skipping deleted node record"

  return true

proc fetchGroupsTable(client: FractioClient, conn: ProtocolClient): bool =
  ## Fetch the sys.groups table and update the cache
  let startKey = encodeTableKey(SYS_GROUPS_TABLE_ID, "")
  let endKey = encodeTableKey(SYS_GROUPS_TABLE_ID + 1, "")

  echo "DEBUG: fetching groups table, range: ", startKey, " to ", endKey
  let scanRes = conn.kvScan(startKey, endKey, limit = 1000)
  if scanRes.isErr:
    echo "DEBUG: fetchGroupsTable scan failed: ", scanRes.error.msg
    return false

  echo "DEBUG: fetchGroupsTable got ", scanRes.value.pairs.len, " entries"
  withLock client.lock:
    for pair in scanRes.value.pairs:
      # Use MVCC decoder
      let (groupRec, isDeleted) = decodeGroupRecordFromMVCC(pair.value)
      if not isDeleted:
        var replicaNodeIds: seq[uint32] = @[]
        for rep in groupRec.replicas:
          replicaNodeIds.add(rep.nodeId)

        echo "DEBUG: caching group ", groupRec.groupId, " leader=", groupRec.leader
        client.groups[groupRec.groupId] = GroupInfo(
          groupId: groupRec.groupId,
          spaceId: groupRec.spaceId,
          leaderNodeId: groupRec.leader,
          replicaNodeIds: replicaNodeIds
        )

  return true

proc initialize*(client: FractioClient): bool =
  ## Initialize the client by fetching metadata from the cluster.
  ## Must be called before using the client.
  if client.initialized.load(moRelaxed):
    return true

  echo "DEBUG: initializing client, connecting to ", client.config.initialHost, ":", client.config.initialPort
  # Connect to initial node
  let connOpt = client.connectToNode(client.config.initialHost,
      client.config.initialPort)
  if connOpt.isNone:
    echo "DEBUG: failed to connect to initial node"
    return false

  let conn = connOpt.get()
  defer: conn.disconnect()

  # Fetch system tables
  if not client.fetchNodesTable(conn):
    echo "DEBUG: fetchNodesTable failed"
    return false
  if not client.fetchGroupsTable(conn):
    echo "DEBUG: fetchGroupsTable failed"
    return false

  client.initialized.store(true, moRelaxed)
  echo "DEBUG: client initialized successfully"
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
  result = client.fetchNodesTable(conn) and client.fetchGroupsTable(conn)

  # Disconnect if we created a temporary connection
  if shouldDisconnect:
    conn.disconnect()

# =============================================================================
# Leader connection management
# =============================================================================

proc getGroupLeaderConnection*(client: FractioClient, groupId: uint64): Option[
    ProtocolClient] =
  ## Get a connection to the leader of a specific group.
  ## Uses cached leader info, falls back to trying all replicas.

  withLock client.lock:
    # Check if we have cached group info
    if groupId notin client.groups:
      echo "DEBUG: groupId ", groupId, " not in client.groups"
      return none(ProtocolClient)

    let groupInfo = client.groups[groupId]
    echo "DEBUG: groupInfo for ", groupId, ": leader=", groupInfo.leaderNodeId, " replicas=", groupInfo.replicaNodeIds

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
        echo "DEBUG: connected to leader ", groupInfo.leaderNodeId
        client.leaderConnections[groupId] = connOpt.get()
        return connOpt
      else:
        echo "DEBUG: failed to connect to leader ", groupInfo.leaderNodeId

    # Leader unknown - try all replicas until we find one that works
    echo "DEBUG: leader unknown for group ", groupId, ", trying replicas"
    for nodeId in groupInfo.replicaNodeIds:
      let connOpt = client.getNodeConnectionInternal(nodeId)
      if connOpt.isSome:
        echo "DEBUG: connected to replica ", nodeId
        # We'll try this connection; if it's not the leader,
        # the operation will fail and we'll retry
        return connOpt

  echo "DEBUG: no connection found for group ", groupId
  return none(ProtocolClient)

proc refreshGroupLeader(client: FractioClient, groupId: uint64): bool =
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

  return true

# =============================================================================
# Key routing
# =============================================================================

proc getGroupForKey*(client: FractioClient, key: string): uint64 =
  ## Determine which group owns a given key.
  ## Returns 0 if the group cannot be determined.

  # System tables (tableId 1-7) are in the meta group (groupId 1)
  if key.startsWith(TABLE_KEY_PREFIX):
    let afterPrefix = key[TABLE_KEY_PREFIX.len .. ^1]
    if afterPrefix.len >= TABLE_ID_WIDTH:
      try:
        let tableIdStr = afterPrefix[0 ..< TABLE_ID_WIDTH]
        let tableId = parseUInt(tableIdStr).uint32
        if tableId <= MAX_META_GROUP_TABLE_ID:
          return 1'u64 # Meta group
        else:
          # For data tables, we'd need more sophisticated routing
          # For now, assume group 2 for all data tables (tableId >= 100)
          return 2'u64
      except ValueError:
        discard

  # Default to group 2 (data group) for non-table keys or if parsing failed
  return 2'u64

# =============================================================================
# KV Operations
# =============================================================================

proc kvGet*(client: FractioClient, key: string, txnId: uint64 = 0,
    readTimestamp: uint64 = 0): KVOpResult[Option[string]] =
  ## Get a value by key, routing to the correct group leader
  if not client.initialized.load(moRelaxed):
    return kvOpErr[Option[string]]("client not initialized")

  let groupId = client.getGroupForKey(key)

  # Try up to 3 times (in case of leader changes)
  for attempt in 0 ..< 3:
    let connOpt = client.getGroupLeaderConnection(groupId)
    if connOpt.isNone:
      return kvOpErr[Option[string]]("no connection to group leader")

    let conn = connOpt.get()
    let res = conn.kvGet(key, txnId = txnId, readTimestamp = readTimestamp)

    if res.isOk:
      if res.value.found:
        return kvOpOk(some(res.value.value))
      else:
        return kvOpOk(none(string))

    # Check for "not leader" error
    if res.error.kind == peNotLeader:
      # Refresh leader and retry
      if not client.refreshGroupLeader(groupId):
        return kvOpErr[Option[string]]("failed to refresh group leader")
      continue

    return kvOpErr[Option[string]](res.error.msg)

  return kvOpErr[Option[string]]("too many retries")

proc kvPut*(client: FractioClient, key, value: string,
    txnId: uint64 = 0): KVOpVoidResult =
  ## Put a key-value pair, routing to the correct group leader
  if not client.initialized.load(moRelaxed):
    return kvVoidErr("client not initialized")

  let groupId = client.getGroupForKey(key)

  for attempt in 0 ..< 3:
    let connOpt = client.getGroupLeaderConnection(groupId)
    if connOpt.isNone:
      return kvVoidErr("no connection to group leader")

    let conn = connOpt.get()
    let res = conn.kvPut(key, value, txnId = txnId)

    if res.isOk and res.value.status == kvMsgs.PutStatusOK:
      return kvVoidOk()

    # Check for "not leader" error
    if res.isErr and res.error.kind == peNotLeader:
      if not client.refreshGroupLeader(groupId):
        return kvVoidErr("failed to refresh group leader")
      continue

    let errMsg = if res.isOk: "put failed with status " & $res.value.status
                 else: res.error.msg
    return kvVoidErr(errMsg)

  return kvVoidErr("too many retries")

proc kvDelete*(client: FractioClient, key: string,
    txnId: uint64 = 0): KVOpVoidResult =
  ## Delete a key, routing to the correct group leader
  if not client.initialized.load(moRelaxed):
    return kvVoidErr("client not initialized")

  let groupId = client.getGroupForKey(key)

  for attempt in 0 ..< 3:
    let connOpt = client.getGroupLeaderConnection(groupId)
    if connOpt.isNone:
      return kvVoidErr("no connection to group leader")

    let conn = connOpt.get()
    let res = conn.kvDelete(key, txnId = txnId)

    if res.isOk and res.value.status in {kvMsgs.DelStatusDeleted,
        kvMsgs.DelStatusNotFound}:
      return kvVoidOk()

    if res.isErr and res.error.kind == peNotLeader:
      if not client.refreshGroupLeader(groupId):
        return kvVoidErr("failed to refresh group leader")
      continue

    let errMsg = if res.isOk: "delete failed" else: res.error.msg
    return kvVoidErr(errMsg)

  return kvVoidErr("too many retries")

proc kvScan*(client: FractioClient, startKey, endKey: string,
    limit: uint32 = 0, txnId: uint64 = 0,
    readTimestamp: uint64 = 0): KVOpResult[seq[tuple[key, value: string]]] =
  ## Scan a key range, routing to the correct group leader
  if not client.initialized.load(moRelaxed):
    return kvOpErr[seq[tuple[key, value: string]]]("client not initialized")

  let groupId = client.getGroupForKey(startKey)

  for attempt in 0 ..< 3:
    let connOpt = client.getGroupLeaderConnection(groupId)
    if connOpt.isNone:
      return kvOpErr[seq[tuple[key, value: string]]]("no connection to group leader")

    let conn = connOpt.get()
    let res = conn.kvScan(startKey, endKey, limit, txnId = txnId,
                          readTimestamp = readTimestamp)

    if res.isOk:
      var entries: seq[tuple[key, value: string]] = @[]
      for pair in res.value.pairs:
        entries.add((key: pair.key, value: pair.value))
      return kvOpOk(entries)

    if res.error.kind == peNotLeader:
      if not client.refreshGroupLeader(groupId):
        return kvOpErr[seq[tuple[key, value: string]]]("failed to refresh group leader")
      continue

    return kvOpErr[seq[tuple[key, value: string]]](res.error.msg)

  return kvOpErr[seq[tuple[key, value: string]]]("too many retries")

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
  let connOpt = client.getGroupLeaderConnection(1) # Group 1 is meta
  if connOpt.isNone:
    echo "DEBUG: beginTxn failed: no connection to meta group leader"
    return kvOpErr[tuple[txnId, readTimestamp: uint64]]("no connection for beginTxn")

  let conn = connOpt.get()
  let res = conn.beginTxn()
  if res.isOk:
    return kvOpOk((txnId: res.value.txnId, readTimestamp: res.value.readTimestamp))
  else:
    echo "DEBUG: beginTxn failed: ", res.error.msg
    return kvOpErr[tuple[txnId, readTimestamp: uint64]](res.error.msg)

proc commitTxn*(client: FractioClient, txnId: uint64): KVOpVoidResult =
  ## Commit a transaction.
  # We should send commit to the node that started it, or any node if they share txn state.
  # For now, use meta group leader.
  let connOpt = client.getGroupLeaderConnection(1)
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
  let connOpt = client.getGroupLeaderConnection(1)
  if connOpt.isNone:
    return kvVoidErr("no connection for rollbackTxn")

  let conn = connOpt.get()
  let res = conn.rollbackTxn(txnId)
  if res.isOk:
    return kvVoidOk()
  else:
    return kvVoidErr(res.error.msg)

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
    client.leaderConnections.clear()
    client.initialized.store(false, moRelaxed)
