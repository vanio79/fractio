# Raft Cluster Management

import std/sets
import std/tables
import std/sequtils
import std/json
import std/options
import std/strutils

import fractio/utils/logging
import fractio/distributed/raft/types

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

proc serializeCluster*(cluster: RaftCluster): string =
  ## Serialize the cluster configuration
  var jsonNodes: seq[JsonNode] = @[]

  for serverId, endpoint in cluster.servers.pairs():
    jsonNodes.add( %* {
      "server_id": serverId,
      "endpoint": endpoint
    })

  return $( %* {
    "servers": %jsonNodes,
    "self_id": cluster.selfId,
    "config": %* {
      "server_id": cluster.config.serverId,
      "endpoint": cluster.config.endpoint,
      "election_timeout": cluster.config.electionTimeout,
      "heartbeat_interval": cluster.config.heartbeatInterval,
      "log_storage_path": cluster.config.logStoragePath,
      "snapshot_enabled": cluster.config.snapshotEnabled,
      "snapshot_distance": cluster.config.snapshotDistance
    }
  })

proc deserializeCluster*(jsonData: string): RaftCluster =
  ## Deserialize a cluster configuration
  let jsonNode = parseJson(jsonData)
  result = newRaftCluster(RaftConfig())

  for serverNode in jsonNode["servers"].getElems():
    let serverId = serverNode["server_id"].getInt().int32
    let endpoint = serverNode["endpoint"].getStr()
    result.servers[serverId] = endpoint

  result.selfId = jsonNode["self_id"].getInt().int32
  result.config = RaftConfig(
    serverId: result.selfId,
    endpoint: jsonNode["config"]["endpoint"].getStr(),
    electionTimeout: jsonNode["config"]["election_timeout"].getInt(),
    heartbeatInterval: jsonNode["config"]["heartbeat_interval"].getInt(),
    logStoragePath: jsonNode["config"]["log_storage_path"].getStr(),
    snapshotEnabled: jsonNode["config"]["snapshot_enabled"].getBool(),
    snapshotDistance: jsonNode["config"]["snapshot_distance"].getInt()
  )

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
