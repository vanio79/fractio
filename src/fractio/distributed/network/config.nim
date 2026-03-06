# Network Configuration - Configuration types for network transport
# TCP-based network communication for distributed Fractio

import std/[options, math, random]
import ../../core/types

# =============================================================================
# Peer Configuration
# =============================================================================

type
  PeerConfig* = object
    ## Configuration for a peer node
    nodeId*: NodeID
    host*: string
    basePort*: int
    raftPort*: int   ## Calculated: basePort + 0
    clientPort*: int ## Calculated: basePort + 1
    adminPort*: int  ## Calculated: basePort + 2
    timerPort*: int  ## Calculated: basePort + 3 (UDP)

proc newPeerConfig*(nodeId: NodeID, host: string, basePort: int): PeerConfig =
  ## Create a new peer configuration
  result.nodeId = nodeId
  result.host = host
  result.basePort = basePort
  result.raftPort = basePort + 0
  result.clientPort = basePort + 1
  result.adminPort = basePort + 2
  result.timerPort = basePort + 3

# =============================================================================
# Network Configuration
# =============================================================================

type
  NetworkConfig* = ref object
    ## Complete network configuration for a node
    nodeId*: NodeID
    basePort*: int
    bindAddress*: string

    # TCP settings
    tcpNoDelay*: bool
    tcpKeepAlive*: bool
    tcpSendBufferSize*: int
    tcpRecvBufferSize*: int
    tcpConnectTimeoutMs*: int
    tcpReadTimeoutMs*: int
    tcpWriteTimeoutMs*: int
    tcpMaxMessageSize*: int

    # Connection pooling
    maxConnectionsPerNode*: int
    idleTimeoutMs*: int

    # Health checking
    healthCheckIntervalMs*: int
    failureThreshold*: int
    recoveryThreshold*: int

    # Thread pools
    raftWorkers*: int
    clientWorkers*: int
    adminWorkers*: int

    # Peers
    peers*: seq[PeerConfig]

const
  DEFAULT_BASE_PORT* = 9000
  DEFAULT_BIND_ADDRESS* = "0.0.0.0"

  DEFAULT_TCP_NO_DELAY* = true
  DEFAULT_TCP_KEEP_ALIVE* = true
  DEFAULT_TCP_SEND_BUFFER_SIZE* = 4 * 1024 * 1024        # 4MB
  DEFAULT_TCP_RECV_BUFFER_SIZE* = 4 * 1024 * 1024        # 4MB
  DEFAULT_TCP_CONNECT_TIMEOUT_MS* = 5000
  DEFAULT_TCP_READ_TIMEOUT_MS* = 30000
  DEFAULT_TCP_WRITE_TIMEOUT_MS* = 30000
  DEFAULT_TCP_MAX_MESSAGE_SIZE* = 16 * 1024 * 1024       # 16MB

  DEFAULT_MAX_CONNECTIONS_PER_NODE* = 4
  DEFAULT_IDLE_TIMEOUT_MS* = 60000

  DEFAULT_HEALTH_CHECK_INTERVAL_MS* = 1000
  DEFAULT_FAILURE_THRESHOLD* = 3
  DEFAULT_RECOVERY_THRESHOLD* = 2

  DEFAULT_RAFT_WORKERS* = 4
  DEFAULT_CLIENT_WORKERS* = 8
  DEFAULT_ADMIN_WORKERS* = 2

proc newNetworkConfig*(nodeId: NodeID, basePort: int = DEFAULT_BASE_PORT,
                       bindAddress: string = DEFAULT_BIND_ADDRESS): NetworkConfig =
  ## Create a new network configuration with defaults
  result = NetworkConfig(
    nodeId: nodeId,
    basePort: basePort,
    bindAddress: bindAddress,

    tcpNoDelay: DEFAULT_TCP_NO_DELAY,
    tcpKeepAlive: DEFAULT_TCP_KEEP_ALIVE,
    tcpSendBufferSize: DEFAULT_TCP_SEND_BUFFER_SIZE,
    tcpRecvBufferSize: DEFAULT_TCP_RECV_BUFFER_SIZE,
    tcpConnectTimeoutMs: DEFAULT_TCP_CONNECT_TIMEOUT_MS,
    tcpReadTimeoutMs: DEFAULT_TCP_READ_TIMEOUT_MS,
    tcpWriteTimeoutMs: DEFAULT_TCP_WRITE_TIMEOUT_MS,
    tcpMaxMessageSize: DEFAULT_TCP_MAX_MESSAGE_SIZE,

    maxConnectionsPerNode: DEFAULT_MAX_CONNECTIONS_PER_NODE,
    idleTimeoutMs: DEFAULT_IDLE_TIMEOUT_MS,

    healthCheckIntervalMs: DEFAULT_HEALTH_CHECK_INTERVAL_MS,
    failureThreshold: DEFAULT_FAILURE_THRESHOLD,
    recoveryThreshold: DEFAULT_RECOVERY_THRESHOLD,

    raftWorkers: DEFAULT_RAFT_WORKERS,
    clientWorkers: DEFAULT_CLIENT_WORKERS,
    adminWorkers: DEFAULT_ADMIN_WORKERS,

    peers: @[]
  )

# =============================================================================
# Port Helpers
# =============================================================================

proc raftPort*(config: NetworkConfig): int =
  ## Get Raft TCP port for this node
  result = config.basePort + 0

proc clientPort*(config: NetworkConfig): int =
  ## Get Client TCP port for this node
  result = config.basePort + 1

proc adminPort*(config: NetworkConfig): int =
  ## Get Admin TCP port for this node
  result = config.basePort + 2

proc timerPort*(config: NetworkConfig): int =
  ## Get SharedTimer UDP port for this node
  result = config.basePort + 3

proc raftAddr*(config: NetworkConfig): string =
  ## Get Raft TCP address (host:port)
  result = config.bindAddress & ":" & $config.raftPort()

proc clientAddr*(config: NetworkConfig): string =
  ## Get Client TCP address (host:port)
  result = config.bindAddress & ":" & $config.clientPort()

proc adminAddr*(config: NetworkConfig): string =
  ## Get Admin TCP address (host:port)
  result = config.bindAddress & ":" & $config.adminPort()

# =============================================================================
# Peer Management
# =============================================================================

proc addPeer*(config: NetworkConfig, peer: PeerConfig) =
  ## Add a peer to the configuration
  config.peers.add(peer)

proc removePeer*(config: NetworkConfig, nodeId: NodeID) =
  ## Remove a peer from the configuration
  var newPeers: seq[PeerConfig] = @[]
  for p in config.peers:
    if string(p.nodeId) != string(nodeId):
      newPeers.add(p)
  config.peers = newPeers

proc getPeer*(config: NetworkConfig, nodeId: NodeID): Option[PeerConfig] =
  ## Get a peer by node ID
  for p in config.peers:
    if string(p.nodeId) == string(nodeId):
      return some(p)
  return none(PeerConfig)

proc hasPeer*(config: NetworkConfig, nodeId: NodeID): bool =
  ## Check if a peer exists
  for p in config.peers:
    if string(p.nodeId) == string(nodeId):
      return true
  result = false

# =============================================================================
# Backoff Policy
# =============================================================================

type
  BackoffPolicy* = object
    ## Policy for exponential backoff on retries
    initialDelayMs*: int
    maxDelayMs*: int
    multiplier*: float
    jitter*: bool

const
  DEFAULT_BACKOFF_INITIAL_MS* = 100
  DEFAULT_BACKOFF_MAX_MS* = 5000
  DEFAULT_BACKOFF_MULTIPLIER* = 1.5

proc newBackoffPolicy*(initialMs: int = DEFAULT_BACKOFF_INITIAL_MS,
                       maxMs: int = DEFAULT_BACKOFF_MAX_MS,
                       multiplier: float = DEFAULT_BACKOFF_MULTIPLIER,
                       jitter: bool = true): BackoffPolicy =
  ## Create a new backoff policy
  result.initialDelayMs = initialMs
  result.maxDelayMs = maxMs
  result.multiplier = multiplier
  result.jitter = jitter

proc calculateBackoff*(policy: BackoffPolicy, attempt: int): int =
  ## Calculate backoff delay for given attempt number
  var delay = float(policy.initialDelayMs) * pow(policy.multiplier, float(attempt))
  delay = min(delay, float(policy.maxDelayMs))
  result = int(delay)
  if policy.jitter and result > 0:
    result = result + rand(result div 2)
