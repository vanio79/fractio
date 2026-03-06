# Unit tests for health_checker.nim

import unittest
import fractio/distributed/network/types
import fractio/distributed/network/tcp_transport
import fractio/distributed/network/health_checker
import fractio/distributed/network/config
import fractio/core/types

suite "Health Checker Tests":
  test "Create health checker":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, config.clientPort(), "client")
    let hc = newHealthChecker(config, transport)

    check hc != nil
    check hc.config == config
    check hc.transport == transport
    check hc.failureThreshold == config.failureThreshold
    check hc.recoveryThreshold == config.recoveryThreshold
    check hc.checkIntervalMs == config.healthCheckIntervalMs

    hc.close()
    transport.close()

  test "Register node":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, config.clientPort(), "client")
    let hc = newHealthChecker(config, transport)

    hc.registerNode(NodeID("node2"))

    let health = hc.getHealth(NodeID("node2"))
    check string(health.nodeId) == "node2"
    check health.status == hsUnknown
    check health.consecutiveFailures == 0
    check health.consecutiveSuccesses == 0

    hc.close()
    transport.close()

  test "Unregister node":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, config.clientPort(), "client")
    let hc = newHealthChecker(config, transport)

    hc.registerNode(NodeID("node2"))
    check string(hc.getHealth(NodeID("node2")).nodeId) == "node2"

    hc.unregisterNode(NodeID("node2"))
    let health = hc.getHealth(NodeID("node2"))
    check health.status == hsUnknown
    check health.errorMessage == "Node not registered"

    hc.close()
    transport.close()

  test "Get health for unknown node":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, config.clientPort(), "client")
    let hc = newHealthChecker(config, transport)

    let health = hc.getHealth(NodeID("unknown"))
    check health.status == hsUnknown
    check health.errorMessage == "Node not registered"

    hc.close()
    transport.close()

  test "Is healthy for unknown node":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, config.clientPort(), "client")
    let hc = newHealthChecker(config, transport)

    check hc.isHealthy(NodeID("unknown")) == false

    hc.close()
    transport.close()

  test "Mark node unhealthy":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, config.clientPort(), "client")
    let hc = newHealthChecker(config, transport)

    hc.registerNode(NodeID("node2"))
    hc.markHealthy(NodeID("node2"))

    check hc.isHealthy(NodeID("node2")) == true

    hc.markUnhealthy(NodeID("node2"), "Test failure")
    let health = hc.getHealth(NodeID("node2"))
    check health.status == hsUnhealthy
    check health.errorMessage == "Test failure"
    check health.consecutiveFailures == 1

    hc.close()
    transport.close()

  test "Mark node healthy":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, config.clientPort(), "client")
    let hc = newHealthChecker(config, transport)

    hc.registerNode(NodeID("node2"))
    hc.markHealthy(NodeID("node2"))

    let health = hc.getHealth(NodeID("node2"))
    check health.status == hsHealthy
    check health.consecutiveFailures == 0
    check health.consecutiveSuccesses == hc.recoveryThreshold

    hc.close()
    transport.close()

  test "Get healthy nodes":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, config.clientPort(), "client")
    let hc = newHealthChecker(config, transport)

    hc.registerNode(NodeID("node2"))
    hc.registerNode(NodeID("node3"))

    hc.markHealthy(NodeID("node2"))
    hc.markUnhealthy(NodeID("node3"), "Failed")

    let healthyNodes = hc.getHealthyNodes()
    check healthyNodes.len == 1
    check string(healthyNodes[0]) == "node2"

    hc.close()
    transport.close()

  test "Get unhealthy nodes":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, config.clientPort(), "client")
    let hc = newHealthChecker(config, transport)

    hc.registerNode(NodeID("node2"))
    hc.registerNode(NodeID("node3"))

    hc.markHealthy(NodeID("node2"))
    hc.markUnhealthy(NodeID("node3"), "Failed")

    let unhealthyNodes = hc.getUnhealthyNodes()
    check unhealthyNodes.len == 1
    check string(unhealthyNodes[0]) == "node3"

    hc.close()
    transport.close()

  test "Health stats":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, config.clientPort(), "client")
    let hc = newHealthChecker(config, transport)

    hc.registerNode(NodeID("node2"))
    hc.registerNode(NodeID("node3"))
    hc.registerNode(NodeID("node4"))

    hc.markHealthy(NodeID("node2"))
    hc.markUnhealthy(NodeID("node3"), "Failed")
    # node4 is unknown

    let stats = hc.getHealthStats()
    check stats.healthy == 1
    check stats.unhealthy == 1
    check stats.unknown == 1

    hc.close()
    transport.close()

  test "Consecutive failures increment":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, config.clientPort(), "client")
    let hc = newHealthChecker(config, transport)

    hc.registerNode(NodeID("node2"))

    hc.markUnhealthy(NodeID("node2"), "Fail 1")
    check hc.getHealth(NodeID("node2")).consecutiveFailures == 1

    hc.markUnhealthy(NodeID("node2"), "Fail 2")
    check hc.getHealth(NodeID("node2")).consecutiveFailures == 2

    hc.close()
    transport.close()

  test "Mark healthy resets failures":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, config.clientPort(), "client")
    let hc = newHealthChecker(config, transport)

    hc.registerNode(NodeID("node2"))

    hc.markUnhealthy(NodeID("node2"), "Fail 1")
    hc.markUnhealthy(NodeID("node2"), "Fail 2")
    check hc.getHealth(NodeID("node2")).consecutiveFailures == 2

    hc.markHealthy(NodeID("node2"))
    check hc.getHealth(NodeID("node2")).consecutiveFailures == 0
    check hc.getHealth(NodeID("node2")).consecutiveSuccesses ==
        hc.recoveryThreshold

    hc.close()
    transport.close()

  test "Multiple close calls are safe":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, config.clientPort(), "client")
    let hc = newHealthChecker(config, transport)

    hc.close()
    hc.close() # Should not crash
    transport.close()

suite "Health Status Transitions":
  test "Unknown to healthy via markHealthy":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, config.clientPort(), "client")
    let hc = newHealthChecker(config, transport)

    hc.registerNode(NodeID("node2"))
    check hc.getHealth(NodeID("node2")).status == hsUnknown

    hc.markHealthy(NodeID("node2"))
    check hc.getHealth(NodeID("node2")).status == hsHealthy

    hc.close()
    transport.close()

  test "Healthy to unhealthy via markUnhealthy":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, config.clientPort(), "client")
    let hc = newHealthChecker(config, transport)

    hc.registerNode(NodeID("node2"))
    hc.markHealthy(NodeID("node2"))
    check hc.getHealth(NodeID("node2")).status == hsHealthy

    hc.markUnhealthy(NodeID("node2"), "Failure")
    check hc.getHealth(NodeID("node2")).status == hsUnhealthy

    hc.close()
    transport.close()

  test "Unhealthy to healthy via markHealthy":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let transport = newTCPTransport(config, config.clientPort(), "client")
    let hc = newHealthChecker(config, transport)

    hc.registerNode(NodeID("node2"))
    hc.markUnhealthy(NodeID("node2"), "Failure")
    check hc.getHealth(NodeID("node2")).status == hsUnhealthy

    hc.markHealthy(NodeID("node2"))
    check hc.getHealth(NodeID("node2")).status == hsHealthy

    hc.close()
    transport.close()
