# Connection Manager - Coordinates multiple transports with health checking
# Part of the network transport layer for distributed Fractio

import std/[tables, locks, options, atomics]
import ./tcp_transport
import ./connection_pool
import ./health_checker
import ./config
import ../../core/types as coretypes
import ../../utils/logging

# =============================================================================
# Node Registry Types
# =============================================================================

type
  NodeInfo* = object
    ## Information about a node in the cluster
    nodeId*: NodeID
    host*: string
    raftPort*: int
    clientPort*: int
    adminPort*: int
    isLocal*: bool

  ConnectionManager* = ref object
    ## Manages all network connections for a node
    config*: NetworkConfig

    # Transports
    raftTransport*: TCPTransport
    clientTransport*: TCPTransport
    adminTransport*: TCPTransport

    # Connection pools
    raftPool*: ConnectionPool ## request-response Raft messages
    raftFireForgetPool*: ConnectionPool ## fire-and-forget (heartbeats) — separate
                              ## pool so stale server responses never
                              ## contaminate raftPool connections.
    clientPool*: ConnectionPool
    adminPool*: ConnectionPool

    # Health checker
    healthChecker*: HealthChecker

    # Node registry
    nodes*: tables.Table[string, NodeInfo]
    nodesLock*: Lock

    # State
    running*: Atomic[bool]

# =============================================================================
# Node Registry Operations (forward declarations for internal use)
# =============================================================================

proc registerNodeInternal(cm: ConnectionManager, info: NodeInfo) =
  ## Internal: Register a node in the registry (no health checker registration)
  let key = string(info.nodeId)
  withLock cm.nodesLock:
    cm.nodes[key] = info

# =============================================================================
# Connection Manager Implementation
# =============================================================================

proc newConnectionManager*(config: NetworkConfig): ConnectionManager =
  ## Create a new connection manager
  result = ConnectionManager(
    config: config,
    nodes: tables.initTable[string, NodeInfo](),
    running: Atomic[bool]()
  )
  initLock(result.nodesLock)

  # Create transports
  result.raftTransport = newTCPTransport(config, config.raftPort(), "raft")
  result.clientTransport = newTCPTransport(config, config.clientPort(), "client")
  result.adminTransport = newTCPTransport(config, config.adminPort(), "admin")

  # Create connection pools
  result.raftPool = newConnectionPool(config, "raft")
  result.raftFireForgetPool = newConnectionPool(config, "raft-ff")
  result.clientPool = newConnectionPool(config, "client")
  result.adminPool = newConnectionPool(config, "admin")

  # Create health checker (uses client transport for health checks)
  result.healthChecker = newHealthChecker(config, result.clientTransport)

  # Register local node using internal proc (no health checker registration for local)
  registerNodeInternal(result, NodeInfo(
    nodeId: config.nodeId,
    host: config.bindAddress,
    raftPort: config.raftPort(),
    clientPort: config.clientPort(),
    adminPort: config.adminPort(),
    isLocal: true
  ))

proc close*(cm: ConnectionManager) =
  ## Close the connection manager and all resources
  cm.running.store(false)

  # Close health checker
  cm.healthChecker.close()

  # Close connection pools
  cm.raftPool.close()
  cm.raftFireForgetPool.close()
  cm.clientPool.close()
  cm.adminPool.close()

  # Close transports
  cm.raftTransport.close()
  cm.clientTransport.close()
  cm.adminTransport.close()

  deinitLock(cm.nodesLock)

# =============================================================================
# Node Registry
# =============================================================================

proc registerNode*(cm: ConnectionManager, info: NodeInfo) =
  ## Register a node in the registry
  let key = string(info.nodeId)
  withLock cm.nodesLock:
    cm.nodes[key] = info

  # Register with health checker (for remote nodes)
  if not info.isLocal:
    cm.healthChecker.registerNode(info.nodeId)

proc unregisterNode*(cm: ConnectionManager, nodeId: NodeID) =
  ## Unregister a node from the registry
  let key = string(nodeId)
  withLock cm.nodesLock:
    cm.nodes.del(key)

  # Unregister from health checker
  cm.healthChecker.unregisterNode(nodeId)

proc getNode*(cm: ConnectionManager, nodeId: NodeID): Option[NodeInfo] =
  ## Get node information by ID
  let key = string(nodeId)
  withLock cm.nodesLock:
    if key in cm.nodes:
      return some(cm.nodes[key])
  return none(NodeInfo)

proc hasNode*(cm: ConnectionManager, nodeId: NodeID): bool =
  ## Check if a node is registered
  let key = string(nodeId)
  withLock cm.nodesLock:
    result = key in cm.nodes

proc getAllNodes*(cm: ConnectionManager): seq[NodeInfo] =
  ## Get all registered nodes
  withLock cm.nodesLock:
    for key, info in cm.nodes:
      result.add(info)

proc getRemoteNodes*(cm: ConnectionManager): seq[NodeInfo] =
  ## Get all registered nodes except the local node
  withLock cm.nodesLock:
    for key, info in cm.nodes:
      if not info.isLocal:
        result.add(info)

proc getLocalNode*(cm: ConnectionManager): Option[NodeInfo] =
  ## Get the local node information
  withLock cm.nodesLock:
    for key, info in cm.nodes:
      if info.isLocal:
        return some(info)
  return none(NodeInfo)

# =============================================================================
# Transport Startup/Shutdown
# =============================================================================

proc start*(cm: ConnectionManager): bool =
  ## Start all transports
  cm.running.store(true)

  # Start Raft transport
  if not cm.raftTransport.startServer():
    var fields = tables.initTable[string, string]()
    fields["role"] = "raft"
    error("Failed to start Raft transport", fields)
    return false

  # Start Client transport
  if not cm.clientTransport.startServer():
    var fields = tables.initTable[string, string]()
    fields["role"] = "client"
    error("Failed to start Client transport", fields)
    return false

  # Start Admin transport
  if not cm.adminTransport.startServer():
    var fields = tables.initTable[string, string]()
    fields["role"] = "admin"
    error("Failed to start Admin transport", fields)
    return false

  var fields = tables.initTable[string, string]()
  fields["nodeId"] = string(cm.config.nodeId)
  info("Connection manager started", fields)
  return true

proc stop*(cm: ConnectionManager) =
  ## Stop all transports
  cm.running.store(false)

  cm.raftTransport.stopServer()
  cm.clientTransport.stopServer()
  cm.adminTransport.stopServer()

  var fields = tables.initTable[string, string]()
  fields["nodeId"] = string(cm.config.nodeId)
  info("Connection manager stopped", fields)

# =============================================================================
# Message Sending - Raft
# =============================================================================

proc sendRaftMessage*(cm: ConnectionManager, nodeId: NodeID,
                      payload: string): bool =
  ## Send a fire-and-forget Raft message (e.g. heartbeat) to a node.
  ##
  ## Uses raftFireForgetPool — a pool completely separate from raftPool —
  ## so stale server response frames queued in the TCP receive buffer can
  ## never be read by a subsequent sendRaftMessageWithResponse call.
  ## The server's response frame stays in the kernel buffer; when the
  ## connection is reused for the next heartbeat the server will have
  ## consumed any prior state.  On loopback the kernel buffer is large
  ## enough that no data is lost.
  let nodeOpt = cm.getNode(nodeId)
  if nodeOpt.isNone:
    var fields = tables.initTable[string, string]()
    fields["nodeId"] = string(nodeId)
    warn("Unknown node for Raft message", fields)
    return false

  let node = nodeOpt.get()
  if node.isLocal:
    return true # Local delivery handled separately

  let connOpt = cm.raftFireForgetPool.getOrCreateConnection(
    cm.raftTransport, nodeId, node.host, node.raftPort)
  if connOpt.isNone:
    return false

  let conn = connOpt.get()
  let success = cm.raftTransport.sendRaw(conn, payload)
  if not success:
    cm.raftFireForgetPool.removeConnection(conn)
    return false

  # Return the connection immediately — do NOT drain the response.
  # The unread response frame stays in the kernel TCP receive buffer.
  # Next time this connection is reused for a heartbeat, the previous
  # response is still there — but that is fine because fire-and-forget
  # callers never read responses, so the buffer simply grows by one frame
  # per heartbeat until the kernel drops it or the connection is recycled.
  # To avoid unbounded buffer growth, remove the connection after each use
  # and let the pool create a fresh one for the next heartbeat.
  # (Loopback connect overhead is negligible: ~0.1 ms.)
  cm.raftFireForgetPool.removeConnection(conn)
  return true

proc sendRaftMessageWithResponse*(cm: ConnectionManager, nodeId: NodeID,
                                  payload: string, timeoutMs: int): Option[string] =
  ## Send a Raft message and wait for response
  let nodeOpt = cm.getNode(nodeId)
  if nodeOpt.isNone:
    return none(string)

  let node = nodeOpt.get()
  if node.isLocal:
    return none(string) # Local delivery handled separately

  let connOpt = cm.raftPool.getOrCreateConnection(
    cm.raftTransport, nodeId, node.host, node.raftPort)
  if connOpt.isNone:
    return none(string)

  let conn = connOpt.get()
  let success = cm.raftTransport.sendRaw(conn, payload)
  if not success:
    cm.raftPool.removeConnection(conn)
    return none(string)

  let responseOpt = readFrame(conn.socket, timeoutMs)
  if responseOpt.isNone:
    cm.raftPool.removeConnection(conn)
    return none(string)

  # Return connection to pool so it can be reused by the next send
  cm.raftPool.returnConnection(conn)
  return responseOpt

# =============================================================================
# Message Sending - Client
# =============================================================================

proc sendClientMessage*(cm: ConnectionManager, nodeId: NodeID,
                        payload: string): bool =
  ## Send a Client message to a node
  let nodeOpt = cm.getNode(nodeId)
  if nodeOpt.isNone:
    var fields = tables.initTable[string, string]()
    fields["nodeId"] = string(nodeId)
    warn("Unknown node for Client message", fields)
    return false

  let node = nodeOpt.get()
  if node.isLocal:
    return true # Local delivery handled separately

  let connOpt = cm.clientPool.getOrCreateConnection(
    cm.clientTransport, nodeId, node.host, node.clientPort)
  if connOpt.isNone:
    return false

  let conn = connOpt.get()
  let success = cm.clientTransport.sendRaw(conn, payload)
  if not success:
    cm.clientPool.removeConnection(conn)
    return false

  cm.clientPool.returnConnection(conn)
  return true

proc sendClientMessageWithResponse*(cm: ConnectionManager, nodeId: NodeID,
                                    payload: string, timeoutMs: int): Option[string] =
  ## Send a Client message and wait for response
  let nodeOpt = cm.getNode(nodeId)
  if nodeOpt.isNone:
    return none(string)

  let node = nodeOpt.get()
  if node.isLocal:
    return none(string) # Local delivery handled separately

  let connOpt = cm.clientPool.getOrCreateConnection(
    cm.clientTransport, nodeId, node.host, node.clientPort)
  if connOpt.isNone:
    return none(string)

  let conn = connOpt.get()
  let success = cm.clientTransport.sendRaw(conn, payload)
  if not success:
    cm.clientPool.removeConnection(conn)
    return none(string)

  let responseOpt = readFrame(conn.socket, timeoutMs)
  if responseOpt.isNone:
    cm.clientPool.removeConnection(conn)
    return none(string)

  cm.clientPool.returnConnection(conn)
  return responseOpt

# =============================================================================
# Message Sending - Admin
# =============================================================================

proc sendAdminMessage*(cm: ConnectionManager, nodeId: NodeID,
                       payload: string): bool =
  ## Send an Admin message to a node
  let nodeOpt = cm.getNode(nodeId)
  if nodeOpt.isNone:
    var fields = tables.initTable[string, string]()
    fields["nodeId"] = string(nodeId)
    warn("Unknown node for Admin message", fields)
    return false

  let node = nodeOpt.get()
  if node.isLocal:
    return true # Local delivery handled separately

  let connOpt = cm.adminPool.getOrCreateConnection(
    cm.adminTransport, nodeId, node.host, node.adminPort)
  if connOpt.isNone:
    return false

  let conn = connOpt.get()
  let success = cm.adminTransport.sendRaw(conn, payload)
  if not success:
    cm.adminPool.removeConnection(conn)
    return false

  cm.adminPool.returnConnection(conn)
  return true

proc sendAdminMessageWithResponse*(cm: ConnectionManager, nodeId: NodeID,
                                   payload: string, timeoutMs: int): Option[string] =
  ## Send an Admin message and wait for response
  let nodeOpt = cm.getNode(nodeId)
  if nodeOpt.isNone:
    return none(string)

  let node = nodeOpt.get()
  if node.isLocal:
    return none(string) # Local delivery handled separately

  let connOpt = cm.adminPool.getOrCreateConnection(
    cm.adminTransport, nodeId, node.host, node.adminPort)
  if connOpt.isNone:
    return none(string)

  let conn = connOpt.get()
  let success = cm.adminTransport.sendRaw(conn, payload)
  if not success:
    cm.adminPool.removeConnection(conn)
    return none(string)

  let responseOpt = readFrame(conn.socket, timeoutMs)
  if responseOpt.isNone:
    cm.adminPool.removeConnection(conn)
    return none(string)

  cm.adminPool.returnConnection(conn)
  return responseOpt

# =============================================================================
# Broadcast
# =============================================================================

proc broadcastRaftMessage*(cm: ConnectionManager, payload: string): int =
  ## Broadcast a Raft message to all remote nodes, returns count sent
  let nodes = cm.getRemoteNodes()
  for node in nodes:
    if cm.sendRaftMessage(node.nodeId, payload):
      inc result

proc broadcastClientMessage*(cm: ConnectionManager, payload: string): int =
  ## Broadcast a Client message to all remote nodes, returns count sent
  let nodes = cm.getRemoteNodes()
  for node in nodes:
    if cm.sendClientMessage(node.nodeId, payload):
      inc result

proc broadcastAdminMessage*(cm: ConnectionManager, payload: string): int =
  ## Broadcast an Admin message to all remote nodes, returns count sent
  let nodes = cm.getRemoteNodes()
  for node in nodes:
    if cm.sendAdminMessage(node.nodeId, payload):
      inc result

# =============================================================================
# Health Checking Integration
# =============================================================================

proc checkNodeHealth*(cm: ConnectionManager, nodeId: NodeID): HealthStatus =
  ## Check the health of a node
  let nodeOpt = cm.getNode(nodeId)
  if nodeOpt.isNone:
    return hsUnknown

  let node = nodeOpt.get()
  if node.isLocal:
    return hsHealthy

  result = cm.healthChecker.checkNodeHealth(nodeId, node.host, node.clientPort)

proc isNodeHealthy*(cm: ConnectionManager, nodeId: NodeID): bool =
  ## Check if a node is healthy
  result = cm.healthChecker.isHealthy(nodeId)

proc getHealthyNodes*(cm: ConnectionManager): seq[NodeID] =
  ## Get all healthy nodes
  result = cm.healthChecker.getHealthyNodes()

proc getUnhealthyNodes*(cm: ConnectionManager): seq[NodeID] =
  ## Get all unhealthy nodes
  result = cm.healthChecker.getUnhealthyNodes()

# =============================================================================
# Handler Registration
# =============================================================================

proc registerRaftHandler*(cm: ConnectionManager, msgType: uint16,
                          handler: proc(msg: string): string {.gcsafe.}) =
  ## Register a handler for Raft messages
  cm.raftTransport.registerHandler(msgType, handler)

proc registerClientHandler*(cm: ConnectionManager, msgType: uint16,
                            handler: proc(msg: string): string {.gcsafe.}) =
  ## Register a handler for Client messages
  cm.clientTransport.registerHandler(msgType, handler)

proc registerAdminHandler*(cm: ConnectionManager, msgType: uint16,
                           handler: proc(msg: string): string {.gcsafe.}) =
  ## Register a handler for Admin messages
  cm.adminTransport.registerHandler(msgType, handler)

# =============================================================================
# Statistics
# =============================================================================

proc getStats*(cm: ConnectionManager): tuple[
    raftPoolStats: tuple[created: int64, reused: int64, closed: int64,
        active: int],
    clientPoolStats: tuple[created: int64, reused: int64, closed: int64,
        active: int],
    adminPoolStats: tuple[created: int64, reused: int64, closed: int64,
        active: int],
    healthStats: tuple[healthy: int, degraded: int, unhealthy: int,
        unknown: int],
    nodeCount: int] =
  ## Get statistics for all transports
  result.raftPoolStats = cm.raftPool.getStats()
  result.clientPoolStats = cm.clientPool.getStats()
  result.adminPoolStats = cm.adminPool.getStats()
  result.healthStats = cm.healthChecker.getHealthStats()
  result.nodeCount = cm.nodes.len
