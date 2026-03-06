# Unit tests for connection_pool.nim

import unittest
import tables
import fractio/distributed/network/types
import fractio/distributed/network/tcp_transport
import fractio/distributed/network/connection_pool
import fractio/distributed/network/config
import fractio/core/types

suite "Connection Pool Tests":
  test "Create connection pool":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")
    check pool != nil
    check pool.role == "test"
    check pool.totalCreated == 0
    check pool.totalReused == 0
    check pool.totalClosed == 0
    pool.close()

  test "Pool statistics":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    let stats = pool.getStats()
    check stats.created == 0
    check stats.reused == 0
    check stats.closed == 0
    check stats.active == 0

    pool.close()

  test "Connection count for unknown node":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    check pool.getConnectionCount(NodeID("unknown")) == 0
    check pool.getAvailableConnectionCount(NodeID("unknown")) == 0

    pool.close()

  test "Prune idle connections on empty pool":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    # Should not crash on empty pool
    pool.pruneIdleConnections(60000)

    check pool.totalClosed == 0
    pool.close()

  test "Return connection that doesn't exist":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    # Should not crash when returning a connection that's not in the pool
    let conn = Connection(
      nodeId: NodeID("node2"),
      state: csConnected
    )
    pool.returnConnection(conn)

    check pool.totalClosed == 0
    pool.close()

  test "Remove connection that doesn't exist":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    # Should not crash when removing a connection that's not in the pool
    let conn = Connection(
      nodeId: NodeID("node2"),
      state: csConnected
    )
    pool.removeConnection(conn)

    pool.close()

  test "New pooled connection":
    let config = newNetworkConfig(NodeID("node1"), 9000)

    var conn = Connection(
      nodeId: NodeID("node2"),
      state: csConnected
    )

    let pc = newPooledConnection(conn)
    check pc.conn == conn
    check pc.inUse == false
    check pc.lastUsed > 0
    check pc.createdAt > 0

  test "Connection pool with config defaults":
    let config = newNetworkConfig(NodeID("node1"))

    # Check that config has the expected default values
    check config.maxConnectionsPerNode > 0
    check config.idleTimeoutMs > 0

    let pool = newConnectionPool(config, "raft")
    check pool.config == config
    pool.close()

  test "Pool role assignment":
    let config = newNetworkConfig(NodeID("node1"), 9000)

    let raftPool = newConnectionPool(config, "raft")
    let clientPool = newConnectionPool(config, "client")
    let adminPool = newConnectionPool(config, "admin")

    check raftPool.role == "raft"
    check clientPool.role == "client"
    check adminPool.role == "admin"

    raftPool.close()
    clientPool.close()
    adminPool.close()

suite "Connection Pool Internal Tests":
  test "Pool table is initialized":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    check pool.pool.len == 0
    pool.close()

  test "Multiple close calls are safe":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    pool.close()
    pool.close() # Should not crash
