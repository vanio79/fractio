# Cluster Bootstrap - Utilities to start and manage a Fractio cluster
# Part of the network transport layer for distributed Fractio

import std/[os, tables, atomics, options, times, json]
import ./network/config
import ./network/network_raft_node
import ./network/client_handler
import ./network/raft_transport
import ./raft/types as raft_types
import ./raft/group_types as rangeTypes
import ./meta/system_tables
import ../utils/logging

# =============================================================================
# Cluster Configuration
# =============================================================================

type
  NodeConfig* = object
    ## Configuration for a single node
    serverId*: int32
    host*: string
    basePort*: int
    dataDir*: string

  ClusterConfig* = object
    ## Configuration for the entire cluster
    nodes*: seq[NodeConfig]
    replicationFactor*: int
    electionTimeoutMs*: int
    heartbeatIntervalMs*: int

  ClusterNode* = ref object
    ## A running node in the cluster
    config*: NodeConfig
    raftNode*: NetworkRaftNode
    clientHandler*: ClientHandler
    running*: Atomic[bool]

  SimpleRange* = object
    ## Simplified range info for cluster management
    groupId*: int32
    startKey*: string
    endKey*: string
    replicaNodes*: seq[int32] # Server IDs of replicas

  Cluster* = ref object
    ## A running cluster
    config*: ClusterConfig
    nodes*: seq[ClusterNode]
    ranges*: seq[SimpleRange]
    running*: Atomic[bool]

# =============================================================================
# Configuration Helpers
# =============================================================================

proc newNodeConfig*(serverId: int32, host: string, basePort: int,
                    dataDir: string = ""): NodeConfig =
  ## Create a node configuration
  result.serverId = serverId
  result.host = host
  result.basePort = basePort
  if dataDir == "":
    result.dataDir = "tmp/cluster_node_" & $serverId & "/"
  else:
    result.dataDir = dataDir

proc newClusterConfig*(nodes: seq[NodeConfig],
                       replicationFactor: int = 3,
                       electionTimeoutMs: int = 150,
                       heartbeatIntervalMs: int = 50): ClusterConfig =
  ## Create a cluster configuration
  result.nodes = nodes
  result.replicationFactor = replicationFactor
  result.electionTimeoutMs = electionTimeoutMs
  result.heartbeatIntervalMs = heartbeatIntervalMs

# =============================================================================
# Default Configurations
# =============================================================================

proc defaultSingleNodeConfig*(basePort: int = 9000): ClusterConfig =
  ## Create a default single-node cluster config
  result = newClusterConfig(@[
    newNodeConfig(1, "127.0.0.1", basePort)
  ])

proc defaultThreeNodeConfig*(basePort: int = 9000): ClusterConfig =
  ## Create a default 3-node cluster config
  result = newClusterConfig(@[
    newNodeConfig(1, "127.0.0.1", basePort),
    newNodeConfig(2, "127.0.0.1", basePort + 100),
    newNodeConfig(3, "127.0.0.1", basePort + 200)
  ])

proc defaultFiveNodeConfig*(basePort: int = 9000): ClusterConfig =
  ## Create a default 5-node cluster config
  result = newClusterConfig(@[
    newNodeConfig(1, "127.0.0.1", basePort),
    newNodeConfig(2, "127.0.0.1", basePort + 100),
    newNodeConfig(3, "127.0.0.1", basePort + 200),
    newNodeConfig(4, "127.0.0.1", basePort + 300),
    newNodeConfig(5, "127.0.0.1", basePort + 400)
  ])

# =============================================================================
# Node Bootstrap
# =============================================================================

proc createNetworkConfig(nodeCfg: NodeConfig): NetworkConfig =
  ## Create network config from node config
  result = newNetworkConfig(
    nodeId = toNodeID(nodeCfg.serverId),
    basePort = nodeCfg.basePort,
    bindAddress = nodeCfg.host
  )

proc createRaftConfig(nodeCfg: NodeConfig,
    clusterCfg: ClusterConfig): raft_types.RaftConfig =
  ## Create Raft config from node and cluster configs
  result = raft_types.RaftConfig(
    serverId: nodeCfg.serverId,
    endpoint: nodeCfg.host & ":" & $nodeCfg.basePort,
    electionTimeout: clusterCfg.electionTimeoutMs,
    heartbeatInterval: clusterCfg.heartbeatIntervalMs,
    logStoragePath: nodeCfg.dataDir & "raft/",
    snapshotEnabled: false,
    snapshotDistance: 1000,
    maxAppendSize: 100
  )

proc startNode*(nodeCfg: NodeConfig, clusterCfg: ClusterConfig,
                peers: seq[NodeConfig]): ClusterNode =
  ## Start a single node with knowledge of its peers
  result = ClusterNode(
    config: nodeCfg,
    running: Atomic[bool]()
  )

  # Create and start the network Raft node
  let netConfig = createNetworkConfig(nodeCfg)
  let raftConfig = createRaftConfig(nodeCfg, clusterCfg)

  result.raftNode = newNetworkRaftNode(raftConfig, netConfig)

  # Add peers
  for peer in peers:
    if peer.serverId != nodeCfg.serverId:
      result.raftNode.addPeer(
        peer.serverId,
        peer.host,
        peer.basePort,
        peer.basePort + 1,
        peer.basePort + 2
      )

  # Start the Raft node
  if not result.raftNode.start():
    var fields = initTable[string, string]()
    fields["serverId"] = $nodeCfg.serverId
    error("Failed to start Raft node", fields)
    return nil

  # Create and setup client handler
  result.clientHandler = newClientHandler(result.raftNode.connManager)
  result.clientHandler.setupHandlers()

  result.running.store(true)

  var fields = initTable[string, string]()
  fields["serverId"] = $nodeCfg.serverId
  fields["basePort"] = $nodeCfg.basePort
  info("Node started", fields)

proc stopNode*(node: ClusterNode) =
  ## Stop a single node
  if node.running.load():
    node.running.store(false)
    node.clientHandler.close()
    node.raftNode.stop()
    node.raftNode.close()

    var fields = initTable[string, string]()
    fields["serverId"] = $node.config.serverId
    info("Node stopped", fields)

# =============================================================================
# Cluster Bootstrap
# =============================================================================

proc bootstrapCluster*(config: ClusterConfig): Cluster =
  ## Bootstrap a cluster from configuration
  result = Cluster(
    config: config,
    running: Atomic[bool]()
  )

  var fields = initTable[string, string]()
  fields["nodeCount"] = $config.nodes.len
  info("Bootstrapping cluster", fields)

  # Start all nodes
  for nodeCfg in config.nodes:
    let node = startNode(nodeCfg, config, config.nodes)
    if node == nil:
      # Clean up already started nodes
      for n in result.nodes:
        stopNode(n)
      return nil
    result.nodes.add(node)

  result.running.store(true)

  fields["nodeCount"] = $result.nodes.len
  info("Cluster bootstrapped", fields)

proc stopCluster*(cluster: Cluster) =
  ## Stop all nodes in a cluster
  if cluster.running.load():
    cluster.running.store(false)
    for node in cluster.nodes:
      stopNode(node)

    var fields = initTable[string, string]()
    info("Cluster stopped", fields)

# =============================================================================
# Range Creation
# =============================================================================

proc createRange*(cluster: Cluster, startKey, endKey: string,
                  numReplicas: int = 3): SimpleRange =
  ## Create a new range with specified replicas
  ## Returns the range info

  if numReplicas > cluster.nodes.len:
    var fields = initTable[string, string]()
    fields["requested"] = $numReplicas
    fields["available"] = $cluster.nodes.len
    error("Not enough nodes for replication", fields)
    return

  result = SimpleRange(
    groupId: int32(cluster.ranges.len + 1),
    startKey: startKey,
    endKey: endKey
  )

  # Select first numReplicas nodes as replicas
  for i in 0..<numReplicas:
    result.replicaNodes.add(cluster.nodes[i].config.serverId)

  cluster.ranges.add(result)

  var fields = initTable[string, string]()
  fields["groupId"] = $result.groupId
  fields["replicas"] = $numReplicas
  info("Range created", fields)

proc createDefaultRange*(cluster: Cluster): SimpleRange =
  ## Create the default first range spanning all keys
  result = createRange(cluster, "", "\xFF\xFF\xFF\xFF", min(3,
      cluster.nodes.len))

# =============================================================================
# Cluster Status
# =============================================================================

type
  ClusterStatus* = object
    ## Status of a cluster
    nodeCount*: int
    runningNodes*: int
    leaders*: int
    followers*: int
    candidates*: int
    rangeCount*: int

proc getStatus*(cluster: Cluster): ClusterStatus =
  ## Get cluster status
  result.nodeCount = cluster.nodes.len
  result.rangeCount = cluster.ranges.len

  for node in cluster.nodes:
    if node.running.load():
      inc result.runningNodes
      case node.raftNode.getRole()
      of SR_LEADER:
        inc result.leaders
      of SR_FOLLOWER:
        inc result.followers
      of SR_CANDIDATE:
        inc result.candidates

proc getLeader*(cluster: Cluster): Option[ClusterNode] =
  ## Get the current leader node
  for node in cluster.nodes:
    if node.running.load() and node.raftNode.isLeader():
      return some(node)
  return none(ClusterNode)

proc getHealthyNodes*(cluster: Cluster): seq[ClusterNode] =
  ## Get all healthy (running) nodes
  for node in cluster.nodes:
    if node.running.load():
      result.add(node)

# =============================================================================
# Cleanup
# =============================================================================

proc cleanupClusterData*(config: ClusterConfig) =
  ## Clean up all cluster data directories
  for nodeCfg in config.nodes:
    if dirExists(nodeCfg.dataDir):
      try:
        removeDir(nodeCfg.dataDir)
      except:
        discard

# =============================================================================
# Convenience Procs
# =============================================================================

proc waitForLeader*(cluster: Cluster, timeoutMs: int = 5000): bool =
  ## Wait for a leader to be elected
  let startMs = int64(getTime().toUnix() * 1000)

  while true:
    let leader = cluster.getLeader()
    if leader.isSome():
      return true

    let nowMs = int64(getTime().toUnix() * 1000)
    if nowMs - startMs > timeoutMs:
      return false

    sleep(100)

proc waitForReplication*(cluster: Cluster, timeoutMs: int = 10000): bool =
  ## Wait for cluster to be fully replicated
  ## (all nodes have same commit index)
  let startMs = int64(getTime().toUnix() * 1000)

  while true:
    var commitIndices: seq[int64] = @[]
    for node in cluster.nodes:
      if node.running.load():
        commitIndices.add(node.raftNode.getCommitIndex())

    if commitIndices.len > 0:
      # Check if all commit indices are the same
      let firstIdx = commitIndices[0]
      var allSame = true
      for idx in commitIndices:
        if idx != firstIdx:
          allSame = false
          break
      if allSame:
        return true

    let nowMs = int64(getTime().toUnix() * 1000)
    if nowMs - startMs > timeoutMs:
      return false

    sleep(100)

# =============================================================================
# Meta Range Bootstrap
# =============================================================================

proc createMetaGroupDescriptor*(cluster: Cluster): GroupDescriptor =
  ## Create the GroupDescriptor for Group 1 (the meta group).
  ## The meta group has a replica on every node in the cluster.
  result = newGroupDescriptor(META_GROUP_ID)
  for node in cluster.nodes:
    discard result.addReplica(NodeID(node.config.serverId.uint32))

proc createDataGroupDescriptor*(cluster: Cluster,
    numReplicas: int = 3): GroupDescriptor =
  ## Create the GroupDescriptor for Group 2 (first data group).
  ## Uses standard replication factor.
  result = newGroupDescriptor(DATA_GROUP_START_ID)
  let n = min(numReplicas, cluster.nodes.len)
  for i in 0 ..< n:
    discard result.addReplica(NodeID(cluster.nodes[i].config.serverId.uint32))

proc buildInitialCatalog*(nodeConfigs: seq[NodeConfig]): seq[
    tuple[key, value: string]] =
  ## Build the initial system catalog entries written to the meta range
  ## during first bootstrap. Returns a list of (key, value) pairs.
  ##
  ## Creates:
  ##   - sys.databases: "default" database entry
  ##   - sys.nodes: one entry per founding cluster member
  var pairs: seq[tuple[key, value: string]] = @[]

  # Default database
  let dbKey = encodeTableKey(SYS_DATABASES_TABLE_ID, "default")
  let dbVal = $(%*{
    "id": 1,
    "name": "default",
    "owner": "system",
    "createdAt": $getTime().toUnix()
  })
  pairs.add((key: dbKey, value: dbVal))

  # Node entries
  for cfg in nodeConfigs:
    let nodeKey = encodeTableKey(SYS_NODES_TABLE_ID, $cfg.serverId)
    let nodeVal = $(%*{
      "nodeId": cfg.serverId,
      "host": cfg.host,
      "basePort": cfg.basePort,
      "dataDir": cfg.dataDir,
      "status": "active",
      "joinedAt": $getTime().toUnix()
    })
    pairs.add((key: nodeKey, value: nodeVal))

  result = pairs
