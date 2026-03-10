# Integration Tests for 5-Node Cluster
# Tests full cluster operations with TCP network transport

import unittest
import std/[os, times, atomics, options]

import fractio/distributed/cluster_bootstrap
import fractio/distributed/network/raft_transport
import fractio/distributed/network/network_raft_node
import fractio/distributed/raft/types as raft_types

proc uniqueBasePort(): int =
  ## Generate a unique base port in the range 24000–25900, spaced 100 apart.
  ## Avoids ports used by MySQL (3306/33060), protocol tests (19700–20099),
  ## and cluster_bootstrap default (33060).
  ## The formula yields 19 distinct values cycling every 19 seconds.
  let bucket = int(getTime().toUnix() mod 19)
  result = 24000 + bucket * 100

suite "5-Node Cluster Integration Tests":

  test "Create 5-node cluster configuration":
    let config = defaultFiveNodeConfig(basePort = 9500)

    check config.nodes.len == 5
    check config.replicationFactor == 3
    check config.nodes[0].serverId == 1
    check config.nodes[4].serverId == 5

    # Verify ports are separated by 100
    check config.nodes[0].basePort == 9500
    check config.nodes[1].basePort == 9600
    check config.nodes[4].basePort == 9900

  test "Bootstrap and stop 3-node cluster":
    let port = uniqueBasePort()
    let config = defaultThreeNodeConfig(basePort = port)

    # Bootstrap cluster
    let cluster = bootstrapCluster(config)
    check cluster != nil
    check cluster.nodes.len == 3
    check load(cluster.running, moRelaxed) == true

    # Give nodes time to start
    sleep(200)

    # Stop cluster
    stopCluster(cluster)
    check load(cluster.running, moRelaxed) == false

    # Cleanup
    cleanupClusterData(config)

  test "Create range in cluster":
    let port = uniqueBasePort()
    let config = defaultThreeNodeConfig(basePort = port)
    let cluster = bootstrapCluster(config)

    check cluster != nil

    # Create a range
    let rangeInfo = createRange(cluster, "a", "z", 3)
    check rangeInfo.groupId == 1
    check rangeInfo.startKey == "a"
    check rangeInfo.endKey == "z"
    check rangeInfo.replicaNodes.len == 3

    # Create default range
    let defaultRange = createDefaultRange(cluster)
    check defaultRange.groupId == 2

    check cluster.ranges.len == 2

    stopCluster(cluster)
    cleanupClusterData(config)

  test "Get cluster status":
    let port = uniqueBasePort()
    let config = defaultThreeNodeConfig(basePort = port)
    let cluster = bootstrapCluster(config)

    check cluster != nil

    # Give time for nodes to start
    sleep(200)

    let status = getStatus(cluster)
    check status.nodeCount == 3
    check status.runningNodes == 3
    check status.rangeCount == 0

    stopCluster(cluster)
    cleanupClusterData(config)

  test "Node role transitions in cluster":
    let port = uniqueBasePort()
    let config = defaultThreeNodeConfig(basePort = port)
    let cluster = bootstrapCluster(config)

    check cluster != nil

    # Make node 1 a candidate
    cluster.nodes[0].raftNode.becomeCandidate()
    check cluster.nodes[0].raftNode.isCandidate() == true
    check cluster.nodes[0].raftNode.getTerm() == 1

    # Make it leader
    cluster.nodes[0].raftNode.becomeLeader()
    check cluster.nodes[0].raftNode.isLeader() == true

    # Step down
    cluster.nodes[0].raftNode.becomeFollower(2)
    check cluster.nodes[0].raftNode.isFollower() == true
    check cluster.nodes[0].raftNode.getTerm() == 2

    stopCluster(cluster)
    cleanupClusterData(config)

  test "Cluster with different replication factors":
    let port = uniqueBasePort()
    let config = defaultFiveNodeConfig(basePort = port)
    let cluster = bootstrapCluster(config)

    check cluster != nil

    # Create range with 5 replicas
    let range5 = createRange(cluster, "a", "m", 5)
    check range5.replicaNodes.len == 5

    # Create range with 3 replicas
    let range3 = createRange(cluster, "m", "z", 3)
    check range3.replicaNodes.len == 3

    check cluster.ranges.len == 2

    stopCluster(cluster)
    cleanupClusterData(config)

  test "Get healthy nodes":
    let port = uniqueBasePort()
    let config = defaultThreeNodeConfig(basePort = port)
    let cluster = bootstrapCluster(config)

    check cluster != nil

    sleep(200)

    let healthyNodes = getHealthyNodes(cluster)
    check healthyNodes.len == 3

    # Stop one node
    stopNode(cluster.nodes[1])

    let healthyNodes2 = getHealthyNodes(cluster)
    check healthyNodes2.len == 2

    stopCluster(cluster)
    cleanupClusterData(config)

  test "Node configuration helpers":
    let nodeCfg = newNodeConfig(42, "192.168.1.100", 9000, "/data/node42")

    check nodeCfg.serverId == 42
    check nodeCfg.host == "192.168.1.100"
    check nodeCfg.basePort == 9000
    check nodeCfg.dataDir == "/data/node42"

  test "Default configurations":
    let singleConfig = defaultSingleNodeConfig()
    check singleConfig.nodes.len == 1

    let threeConfig = defaultThreeNodeConfig()
    check threeConfig.nodes.len == 3

    let fiveConfig = defaultFiveNodeConfig()
    check fiveConfig.nodes.len == 5

  test "5-node cluster startup":
    let port = uniqueBasePort()
    let config = defaultFiveNodeConfig(basePort = port)
    let cluster = bootstrapCluster(config)

    check cluster != nil
    check cluster.nodes.len == 5

    # Give nodes time to start
    sleep(500)

    # Check all nodes are running
    for node in cluster.nodes:
      check load(node.running, moRelaxed) == true

    # Get status
    let status = getStatus(cluster)
    check status.nodeCount == 5
    check status.runningNodes == 5

    stopCluster(cluster)
    cleanupClusterData(config)

  test "Cluster node voting":
    let port = uniqueBasePort()
    let config = defaultThreeNodeConfig(basePort = port)
    let cluster = bootstrapCluster(config)

    check cluster != nil

    # Manually make node 1 a candidate and record votes
    cluster.nodes[0].raftNode.becomeCandidate()

    # Add peers to the cluster nodes for vote tracking
    cluster.nodes[0].raftNode.addPeer(2, "127.0.0.1", port + 100, port + 101,
        port + 102)
    cluster.nodes[0].raftNode.addPeer(3, "127.0.0.1", port + 200, port + 201,
        port + 202)

    # Simulate receiving votes
    let hasMajority = cluster.nodes[0].raftNode.recordVote(1, true) # Self vote
    check hasMajority == false # Not majority with 3 nodes yet

    let hasMajority2 = cluster.nodes[0].raftNode.recordVote(2, true) # Second vote
    check hasMajority2 == true # Now has majority (2 out of 3)

    stopCluster(cluster)
    cleanupClusterData(config)

  test "Cluster cleanup":
    let port = uniqueBasePort()
    var config = defaultThreeNodeConfig(basePort = port)

    # Set data directories
    for i in 0..<config.nodes.len:
      config.nodes[i].dataDir = "tmp/test_cleanup_" & $config.nodes[
          i].serverId & "/"

    # Create directories
    for nodeCfg in config.nodes:
      createDir(nodeCfg.dataDir)
      check dirExists(nodeCfg.dataDir)

    # Cleanup
    cleanupClusterData(config)

    # Verify directories are removed
    for nodeCfg in config.nodes:
      check not dirExists(nodeCfg.dataDir)
