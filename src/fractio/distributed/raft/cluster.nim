# Raft Cluster Management

import std/tables
import std/sequtils
import std/options
import std/strutils

import fractio/utils/logging
import fractio/utils/binary
import fractio/distributed/raft/types

# =============================================================================
# Binary Serialization Constants
# =============================================================================

const
  CLUSTER_MAGIC* = [0x52'u8, 0x43'u8, 0x4C'u8] # "RCL" - Raft Cluster binary marker
  CLUSTER_VERSION* = 0x01'u8 # Current binary format version

# =============================================================================
# Raft Cluster Type
# =============================================================================

type
  RaftCluster* = ref object
    ## Raft cluster management
    servers*: Table[int32, string]
    config*: RaftConfig
    selfId*: int32

proc newRaftCluster*(config: RaftConfig): RaftCluster =
  ## Create a new raft cluster
  new(result)
  result.servers = initTable[int32, string]()
  result.config = config
  result.selfId = config.serverId

proc addServer*(cluster: RaftCluster, serverId: int32, endpoint: string): bool =
  ## Add a server to the cluster
  if cluster.servers.hasKey(serverId):
    return false

  cluster.servers[serverId] = endpoint
  var fields = initTable[string, string]()
  fields["serverId"] = $serverId
  fields["endpoint"] = endpoint
  debug("Added server to cluster", fields)
  return true

proc removeServer*(cluster: RaftCluster, serverId: int32): bool =
  ## Remove a server from the cluster
  if not cluster.servers.hasKey(serverId):
    return false

  cluster.servers.del(serverId)
  var fields = initTable[string, string]()
  fields["serverId"] = $serverId
  debug("Removed server from cluster", fields)
  return true

proc getServerEndpoint*(cluster: RaftCluster, serverId: int32): Option[string] =
  ## Get the endpoint for a server
  if cluster.servers.hasKey(serverId):
    return some(cluster.servers[serverId])
  else:
    return none(string)

proc getServers*(cluster: RaftCluster): seq[int32] =
  ## Get all server IDs in the cluster
  result = cluster.servers.keys().toSeq

proc getServerCount*(cluster: RaftCluster): int =
  ## Get the number of servers in the cluster
  return cluster.servers.len

proc getMajority*(cluster: RaftCluster): int =
  ## Calculate the majority count for the cluster
  return (cluster.servers.len div 2) + 1

proc getQuorum*(cluster: RaftCluster): int =
  ## Calculate the quorum size for the cluster
  return cluster.getMajority()

proc isSelfLeader*(cluster: RaftCluster, leaderId: int32): bool =
  ## Check if the given leader ID is the self server
  return leaderId == cluster.selfId

proc getSelfEndpoint*(cluster: RaftCluster): Option[string] =
  ## Get the endpoint for the self server
  return cluster.getServerEndpoint(cluster.selfId)

proc encodeCluster*(cluster: RaftCluster): string =
  ## Encode a RaftCluster to binary format.
  ##
  ## Binary format (little-endian):
  ## - Magic: 3 bytes (0x52 0x43 0x4C = "RCL")
  ## - Version: 1 byte (0x01)
  ## - Self ID: 4 bytes (int32)
  ## - Config:
  ##   - ServerId: 4 bytes (int32)
  ##   - ElectionTimeout: 4 bytes (int32)
  ##   - HeartbeatInterval: 4 bytes (int32)
  ##   - SnapshotEnabled: 1 byte (bool: 0 or 1)
  ##   - SnapshotDistance: 4 bytes (int32)
  ##   - MaxAppendSize: 4 bytes (int32)
  ##   - Endpoint: length-prefixed string
  ##   - LogStoragePath: length-prefixed string
  ## - Servers:
  ##   - Server count: 4 bytes (uint32)
  ##   - For each server:
  ##     - ServerId: 4 bytes (int32)
  ##     - Endpoint: length-prefixed string
  ##
  ## Total minimum: 34 bytes (empty endpoints/paths, 0 servers)
  var w = initBinaryWriter()
  w.writeBytes(CLUSTER_MAGIC)
  w.writeU8(CLUSTER_VERSION)
  w.writeI32(cluster.selfId)

  # Config fields (fixed-size first, then variable)
  w.writeI32(cluster.config.serverId)
  w.writeI32(int32(cluster.config.electionTimeout))
  w.writeI32(int32(cluster.config.heartbeatInterval))
  w.writeU8(if cluster.config.snapshotEnabled: 1'u8 else: 0'u8)
  w.writeI32(int32(cluster.config.snapshotDistance))
  w.writeI32(int32(cluster.config.maxAppendSize))
  w.writeString(cluster.config.endpoint)
  w.writeString(cluster.config.logStoragePath)

  # Servers
  w.writeU32(uint32(cluster.servers.len))
  for serverId, endpoint in cluster.servers.pairs():
    w.writeI32(serverId)
    w.writeString(endpoint)

  w.finish()

proc decodeCluster*(data: string): RaftCluster =
  ## Decode binary data to a RaftCluster.
  ## Raises ValueError if data is invalid or not binary format.
  var r = initBinaryReader(data)

  # Verify magic header
  if r.remaining < 4:
    raise newException(ValueError, "Cluster: data too small for header")
  let magic0 = r.readU8()
  let magic1 = r.readU8()
  let magic2 = r.readU8()
  if magic0 != CLUSTER_MAGIC[0] or magic1 != CLUSTER_MAGIC[1] or magic2 !=
      CLUSTER_MAGIC[2]:
    raise newException(ValueError, "Cluster: invalid magic header")

  # Verify version
  let version = r.readU8()
  if version != CLUSTER_VERSION:
    raise newException(ValueError, "Cluster: unsupported version " & $version)

  # Read self ID
  let selfId = r.readI32()

  # Read config
  let configServerId = r.readI32()
  let electionTimeout = r.readI32()
  let heartbeatInterval = r.readI32()
  let snapshotEnabled = r.readU8() != 0
  let snapshotDistance = r.readI32()
  let maxAppendSize = r.readI32()
  let endpoint = r.readString()
  let logStoragePath = r.readString()

  # Create config
  let config = RaftConfig(
    serverId: configServerId,
    endpoint: endpoint,
    electionTimeout: electionTimeout,
    heartbeatInterval: heartbeatInterval,
    logStoragePath: logStoragePath,
    snapshotEnabled: snapshotEnabled,
    snapshotDistance: snapshotDistance,
    maxAppendSize: maxAppendSize
  )

  result = newRaftCluster(config)
  result.selfId = selfId

  # Read servers
  let serverCount = int(r.readU32())
  for i in 0..<serverCount:
    let serverId = r.readI32()
    let serverEndpoint = r.readString()
    result.servers[serverId] = serverEndpoint

proc getClusterInfo*(cluster: RaftCluster): string =
  ## Get human-readable cluster information
  result = "Raft Cluster Info:\n"
  result.add "Self ID: $#, Endpoint: $#!\n".format($cluster.selfId,
    cluster.getSelfEndpoint().get("unknown"))
  result.add "Server Count: $#!\n".format($cluster.servers.len)
  result.add "Majority: $#!\n".format($cluster.getMajority())
  result.add "Servers:\n"

  for serverId, endpoint in cluster.servers.pairs():
    result.add " Server #, Endpoint: $#!\n".format($serverId, endpoint)

  result.add "\n"

proc printClusterInfo*(cluster: RaftCluster) =
  ## Print cluster information to console
  echo getClusterInfo(cluster)

proc isValidCluster*(cluster: RaftCluster): bool =
  ## Check if the cluster configuration is valid
  if cluster.selfId <= 0:
    return false

  if cluster.servers.len == 0:
    return false

  if not cluster.getServerEndpoint(cluster.selfId).isSome:
    return false

  return true
