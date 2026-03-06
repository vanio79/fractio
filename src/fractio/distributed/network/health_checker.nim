# Health Checker - Monitors node health for the network transport
# Part of the network transport layer for distributed Fractio

import std/[tables, locks, times, options, atomics]
import ./types
import ./tcp_transport
import ./config
import ./serialization
import ../../core/types as coretypes

# =============================================================================
# Health Status Types
# =============================================================================

type
  HealthStatus* = enum
    hsHealthy
    hsDegraded
    hsUnhealthy
    hsUnknown

  NodeHealth* = object
    ## Health information for a single node
    nodeId*: NodeID
    status*: HealthStatus
    lastCheck*: int64       # Milliseconds since epoch
    lastHealthy*: int64     # Last time node was healthy
    consecutiveFailures*: int
    consecutiveSuccesses*: int
    roundTripTimeMs*: int64 # Last measured RTT
    errorMessage*: string

  HealthChecker* = ref object
    ## Health checker for monitoring nodes
    config*: NetworkConfig
    transport*: TCPTransport

    # Health data
    nodeHealth*: tables.Table[string, NodeHealth]
    healthLock*: Lock

    # State
    running*: Atomic[bool]

    # Thresholds
    failureThreshold*: int
    recoveryThreshold*: int
    checkIntervalMs*: int
    timeoutMs*: int

# =============================================================================
# Health Checker Implementation
# =============================================================================

proc newHealthChecker*(config: NetworkConfig,
    transport: TCPTransport): HealthChecker =
  ## Create a new health checker
  result = HealthChecker(
    config: config,
    transport: transport,
    nodeHealth: tables.initTable[string, NodeHealth](),
    running: Atomic[bool](),
    failureThreshold: config.failureThreshold,
    recoveryThreshold: config.recoveryThreshold,
    checkIntervalMs: config.healthCheckIntervalMs,
    timeoutMs: config.tcpReadTimeoutMs
  )
  initLock(result.healthLock)

proc close*(hc: HealthChecker) =
  ## Close the health checker
  hc.running.store(false)
  deinitLock(hc.healthLock)

# =============================================================================
# Node Registration
# =============================================================================

proc registerNode*(hc: HealthChecker, nodeId: NodeID) =
  ## Register a node for health monitoring
  let key = string(nodeId)
  withLock hc.healthLock:
    if key notin hc.nodeHealth:
      hc.nodeHealth[key] = NodeHealth(
        nodeId: nodeId,
        status: hsUnknown,
        lastCheck: 0,
        lastHealthy: 0,
        consecutiveFailures: 0,
        consecutiveSuccesses: 0,
        roundTripTimeMs: 0,
        errorMessage: ""
      )

proc unregisterNode*(hc: HealthChecker, nodeId: NodeID) =
  ## Unregister a node from health monitoring
  let key = string(nodeId)
  withLock hc.healthLock:
    hc.nodeHealth.del(key)

# =============================================================================
# Health Check Operations
# =============================================================================

proc checkNodeHealth*(hc: HealthChecker, nodeId: NodeID, host: string,
    port: int): HealthStatus =
  ## Perform a health check on a specific node
  let key = string(nodeId)
  let startTime = int64(times.getTime().toUnix() * 1000)

  # Create heartbeat message
  var msg: HeartbeatMsg
  msg.header = newMessageHeader(uint16(cmtHeartbeat), 0'u64, hc.config.nodeId,
      nodeId, 0'u64)
  msg.ping = true
  let payload = encodeHeartbeatMsg(msg)

  # Try to send and receive response
  let connOpt = hc.transport.getConnection(nodeId, host, port)
  if connOpt.isNone:
    withLock hc.healthLock:
      if key in hc.nodeHealth:
        var health = hc.nodeHealth[key]
        health.status = hsUnhealthy
        health.consecutiveFailures += 1
        health.consecutiveSuccesses = 0
        health.errorMessage = "Failed to connect"
        health.lastCheck = startTime
        hc.nodeHealth[key] = health
    return hsUnhealthy

  let conn = connOpt.get()
  let success = hc.transport.sendRaw(conn, payload)

  if not success:
    withLock hc.healthLock:
      if key in hc.nodeHealth:
        var health = hc.nodeHealth[key]
        health.status = hsUnhealthy
        health.consecutiveFailures += 1
        health.consecutiveSuccesses = 0
        health.errorMessage = "Failed to send heartbeat"
        health.lastCheck = startTime
        hc.nodeHealth[key] = health
    return hsUnhealthy

  # Read response
  let responseOpt = readFrame(conn.socket, hc.timeoutMs)
  let endTime = int64(times.getTime().toUnix() * 1000)
  let rtt = endTime - startTime

  withLock hc.healthLock:
    if key notin hc.nodeHealth:
      return hsUnknown

    var health = hc.nodeHealth[key]
    health.lastCheck = endTime
    health.roundTripTimeMs = rtt

    if responseOpt.isNone:
      health.status = hsUnhealthy
      health.consecutiveFailures += 1
      health.consecutiveSuccesses = 0
      health.errorMessage = "No response to heartbeat"
    else:
      health.consecutiveFailures = 0
      health.consecutiveSuccesses += 1
      health.errorMessage = ""

      # Determine status based on thresholds
      if health.consecutiveSuccesses >= hc.recoveryThreshold:
        health.status = hsHealthy
        health.lastHealthy = endTime
      elif health.consecutiveSuccesses > 0:
        health.status = hsDegraded

    hc.nodeHealth[key] = health
    result = health.status

proc getHealth*(hc: HealthChecker, nodeId: NodeID): NodeHealth =
  ## Get the current health status of a node
  let key = string(nodeId)
  withLock hc.healthLock:
    if key in hc.nodeHealth:
      result = hc.nodeHealth[key]
    else:
      result = NodeHealth(
        nodeId: nodeId,
        status: hsUnknown,
        lastCheck: 0,
        lastHealthy: 0,
        consecutiveFailures: 0,
        consecutiveSuccesses: 0,
        roundTripTimeMs: 0,
        errorMessage: "Node not registered"
      )

proc isHealthy*(hc: HealthChecker, nodeId: NodeID): bool =
  ## Check if a node is healthy
  let health = hc.getHealth(nodeId)
  result = health.status == hsHealthy or health.status == hsDegraded

proc markUnhealthy*(hc: HealthChecker, nodeId: NodeID, reason: string) =
  ## Manually mark a node as unhealthy
  let key = string(nodeId)
  let now = int64(times.getTime().toUnix() * 1000)
  withLock hc.healthLock:
    if key in hc.nodeHealth:
      var health = hc.nodeHealth[key]
      health.status = hsUnhealthy
      health.consecutiveFailures += 1
      health.consecutiveSuccesses = 0
      health.errorMessage = reason
      health.lastCheck = now
      hc.nodeHealth[key] = health

proc markHealthy*(hc: HealthChecker, nodeId: NodeID) =
  ## Manually mark a node as healthy
  let key = string(nodeId)
  let now = int64(times.getTime().toUnix() * 1000)
  withLock hc.healthLock:
    if key in hc.nodeHealth:
      var health = hc.nodeHealth[key]
      health.status = hsHealthy
      health.consecutiveFailures = 0
      health.consecutiveSuccesses = hc.recoveryThreshold
      health.lastHealthy = now
      health.lastCheck = now
      health.errorMessage = ""
      hc.nodeHealth[key] = health

# =============================================================================
# Bulk Operations
# =============================================================================

proc getHealthyNodes*(hc: HealthChecker): seq[NodeID] =
  ## Get all healthy nodes
  withLock hc.healthLock:
    for key, health in hc.nodeHealth:
      if health.status == hsHealthy or health.status == hsDegraded:
        result.add(NodeID(key))

proc getUnhealthyNodes*(hc: HealthChecker): seq[NodeID] =
  ## Get all unhealthy nodes
  withLock hc.healthLock:
    for key, health in hc.nodeHealth:
      if health.status == hsUnhealthy:
        result.add(NodeID(key))

proc getHealthStats*(hc: HealthChecker): tuple[healthy: int, degraded: int,
    unhealthy: int, unknown: int] =
  ## Get health statistics for all nodes
  withLock hc.healthLock:
    for key, health in hc.nodeHealth:
      case health.status
      of hsHealthy: inc result.healthy
      of hsDegraded: inc result.degraded
      of hsUnhealthy: inc result.unhealthy
      of hsUnknown: inc result.unknown
