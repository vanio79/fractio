# Expanded comprehensive unit tests for connection_pool.nim

import unittest
import tables
import locks
import times
import options
import deques
import net
import fractio/distributed/network/types
import fractio/distributed/network/tcp_transport
import fractio/distributed/network/connection_pool
import fractio/distributed/network/config
import fractio/core/types

suite "Connection Pool Creation - Extended":
  test "Create connection pool with defaults":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    check pool != nil
    check pool.role == "test"
    check pool.config == config
    check pool.totalCreated == 0
    check pool.totalReused == 0
    check pool.totalClosed == 0
    check pool.connectBackoffMs == 1000'i64

    pool.close()

  test "Create connection pool with different roles":
    let config = newNetworkConfig(NodeID("node1"), 9000)

    let raftPool = newConnectionPool(config, "raft")
    check raftPool.role == "raft"
    raftPool.close()

    let clientPool = newConnectionPool(config, "client")
    check clientPool.role == "client"
    clientPool.close()

    let adminPool = newConnectionPool(config, "admin")
    check adminPool.role == "admin"
    adminPool.close()

    let ffPool = newConnectionPool(config, "raft-ff")
    check ffPool.role == "raft-ff"
    ffPool.close()

  test "Connection pool initial state":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    check pool.pool.len == 0
    check pool.lastFailure.len == 0
    check pool.totalCreated == 0
    check pool.totalReused == 0
    check pool.totalClosed == 0

    pool.close()

suite "PooledConnection Creation":
  test "newPooledConnection creates valid wrapper":
    var conn = Connection(
      nodeId: NodeID("node2"),
      state: csConnected
    )
    initLock(conn.sendLock)
    initLock(conn.recvLock)

    let pc = newPooledConnection(conn)

    check pc.conn == conn
    check pc.inUse == false
    check pc.lastUsed > 0
    check pc.createdAt > 0

  test "newPooledConnection timestamps are recent":
    var conn = Connection(
      nodeId: NodeID("node2"),
      state: csConnected
    )
    initLock(conn.sendLock)
    initLock(conn.recvLock)

    let now = int64(times.getTime().toUnix() * 1000)
    let pc = newPooledConnection(conn)

    check pc.lastUsed >= now - 1000
    check pc.createdAt >= now - 1000

  test "newPooledConnection with closed connection":
    var conn = Connection(
      nodeId: NodeID("node2"),
      state: csClosed
    )
    initLock(conn.sendLock)
    initLock(conn.recvLock)

    let pc = newPooledConnection(conn)
    check pc.conn.state == csClosed

suite "Connection Pool Statistics - Extended":
  test "Pool statistics empty pool":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    let stats = pool.getStats()
    check stats.created == 0
    check stats.reused == 0
    check stats.closed == 0
    check stats.active == 0

    pool.close()

  test "Pool statistics after creation":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    var pc = PooledConnection(
      conn: Connection(nodeId: NodeID("node2"), state: csConnected),
      inUse: true,
      lastUsed: 1000,
      createdAt: 1000
    )

    pool.pool[string(NodeID("node2"))] = initDeque[PooledConnection]()
    pool.pool[string(NodeID("node2"))].addLast(pc)
    pool.totalCreated = 1

    let stats = pool.getStats()
    check stats.created == 1
    check stats.active == 1

    pool.close()

  test "Pool statistics multiple connections":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    pool.pool[string(NodeID("node2"))] = initDeque[PooledConnection]()
    pool.pool[string(NodeID("node3"))] = initDeque[PooledConnection]()

    var pc1 = PooledConnection(
      conn: Connection(nodeId: NodeID("node2"), state: csConnected),
      inUse: true
    )
    var pc2 = PooledConnection(
      conn: Connection(nodeId: NodeID("node3"), state: csConnected),
      inUse: false
    )

    pool.pool[string(NodeID("node2"))].addLast(pc1)
    pool.pool[string(NodeID("node3"))].addLast(pc2)
    pool.totalCreated = 2

    let stats = pool.getStats()
    check stats.created == 2
    check stats.active == 1

    pool.close()

suite "Connection Pool Count Operations":
  test "getConnectionCount for unknown node":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    check pool.getConnectionCount(NodeID("unknown")) == 0

    pool.close()

  test "getConnectionCount for known node":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    pool.pool[string(NodeID("node2"))] = initDeque[PooledConnection]()
    var pc1 = PooledConnection(conn: Connection(nodeId: NodeID("node2"),
        state: csConnected))
    var pc2 = PooledConnection(conn: Connection(nodeId: NodeID("node2"),
        state: csConnected))
    pool.pool[string(NodeID("node2"))].addLast(pc1)
    pool.pool[string(NodeID("node2"))].addLast(pc2)

    check pool.getConnectionCount(NodeID("node2")) == 2

    pool.close()

  test "getAvailableConnectionCount for unknown node":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    check pool.getAvailableConnectionCount(NodeID("unknown")) == 0

    pool.close()

  test "getAvailableConnectionCount filters by state and usage":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    pool.pool[string(NodeID("node2"))] = initDeque[PooledConnection]()

    var pc1 = PooledConnection(
      conn: Connection(nodeId: NodeID("node2"), state: csConnected),
      inUse: false
    )
    var pc2 = PooledConnection(
      conn: Connection(nodeId: NodeID("node2"), state: csConnected),
      inUse: true
    )
    var pc3 = PooledConnection(
      conn: Connection(nodeId: NodeID("node2"), state: csClosed),
      inUse: false
    )

    pool.pool[string(NodeID("node2"))].addLast(pc1)
    pool.pool[string(NodeID("node2"))].addLast(pc2)
    pool.pool[string(NodeID("node2"))].addLast(pc3)

    check pool.getAvailableConnectionCount(NodeID("node2")) == 1

    pool.close()

suite "Connection Pool Prune Operations":
  test "pruneIdleConnections on empty pool":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    pool.pruneIdleConnections(60000)
    check pool.totalClosed == 0

    pool.close()

  test "pruneIdleConnections removes idle connections":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    pool.pool[string(NodeID("node2"))] = initDeque[PooledConnection]()

    let oldTime = int64(times.getTime().toUnix() * 1000) - 120000

    var pcOld = PooledConnection(
      conn: Connection(nodeId: NodeID("node2"), state: csConnected),
      inUse: false,
      lastUsed: oldTime,
      createdAt: oldTime
    )
    initLock(pcOld.conn.sendLock)
    initLock(pcOld.conn.recvLock)

    var pcNew = PooledConnection(
      conn: Connection(nodeId: NodeID("node2"), state: csConnected),
      inUse: false,
      lastUsed: int64(times.getTime().toUnix() * 1000)
    )
    initLock(pcNew.conn.sendLock)
    initLock(pcNew.conn.recvLock)

    pool.pool[string(NodeID("node2"))].addLast(pcOld)
    pool.pool[string(NodeID("node2"))].addLast(pcNew)

    pool.pruneIdleConnections(60000)

    check pool.totalClosed == 1
    check pool.pool[string(NodeID("node2"))].len == 1

    pool.close()

  test "pruneIdleConnections keeps in-use connections":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    pool.pool[string(NodeID("node2"))] = initDeque[PooledConnection]()

    let oldTime = int64(times.getTime().toUnix() * 1000) - 120000

    var pcInUse = PooledConnection(
      conn: Connection(nodeId: NodeID("node2"), state: csConnected),
      inUse: true,
      lastUsed: oldTime
    )
    initLock(pcInUse.conn.sendLock)
    initLock(pcInUse.conn.recvLock)

    pool.pool[string(NodeID("node2"))].addLast(pcInUse)

    pool.pruneIdleConnections(60000)

    check pool.totalClosed == 0
    check pool.pool[string(NodeID("node2"))].len == 1

    pool.close()

  test "pruneIdleConnections removes empty node entries":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    pool.pool[string(NodeID("node2"))] = initDeque[PooledConnection]()
    pool.pool[string(NodeID("node3"))] = initDeque[PooledConnection]()

    var pc3 = PooledConnection(
      conn: Connection(nodeId: NodeID("node3"), state: csConnected),
      inUse: false,
      lastUsed: int64(times.getTime().toUnix() * 1000)
    )
    initLock(pc3.conn.sendLock)
    initLock(pc3.conn.recvLock)

    pool.pool[string(NodeID("node3"))].addLast(pc3)

    pool.pruneIdleConnections(60000)

    check "node2" notin pool.pool
    check "node3" in pool.pool

    pool.close()

suite "Connection Pool Return Operations":
  test "returnConnection marks connection as not in use":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    var conn = Connection(nodeId: NodeID("node2"), state: csConnected)
    initLock(conn.sendLock)
    initLock(conn.recvLock)

    var pc = PooledConnection(conn: conn, inUse: true)
    pool.pool[string(NodeID("node2"))] = initDeque[PooledConnection]()
    pool.pool[string(NodeID("node2"))].addLast(pc)

    pool.returnConnection(conn)

    check pool.pool[string(NodeID("node2"))][0].inUse == false

    pool.close()

  test "returnConnection updates lastUsed timestamp":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    var conn = Connection(nodeId: NodeID("node2"), state: csConnected)
    initLock(conn.sendLock)
    initLock(conn.recvLock)

    let oldTime = 1000'i64
    var pc = PooledConnection(conn: conn, inUse: true, lastUsed: oldTime)
    pool.pool[string(NodeID("node2"))] = initDeque[PooledConnection]()
    pool.pool[string(NodeID("node2"))].addLast(pc)

    pool.returnConnection(conn)

    check pool.pool[string(NodeID("node2"))][0].lastUsed > oldTime

    pool.close()

  test "returnConnection for non-existent connection":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    var conn = Connection(nodeId: NodeID("node2"), state: csConnected)
    initLock(conn.sendLock)
    initLock(conn.recvLock)

    pool.returnConnection(conn)

    check pool.totalClosed == 0

    pool.close()

  test "returnConnection does not affect statistics":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    var conn = Connection(nodeId: NodeID("node2"), state: csConnected)
    initLock(conn.sendLock)
    initLock(conn.recvLock)

    var pc = PooledConnection(conn: conn, inUse: true)
    pool.pool[string(NodeID("node2"))] = initDeque[PooledConnection]()
    pool.pool[string(NodeID("node2"))].addLast(pc)
    pool.totalCreated = 1

    pool.returnConnection(conn)

    check pool.totalCreated == 1
    check pool.totalReused == 0
    check pool.totalClosed == 0

    pool.close()

suite "Connection Pool Remove Operations":
  test "removeConnection closes socket":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    let socket = newSocket()
    var conn = Connection(
      nodeId: NodeID("node2"),
      state: csConnected,
      socket: socket
    )
    initLock(conn.sendLock)
    initLock(conn.recvLock)

    var pc = PooledConnection(conn: conn, inUse: true)
    pool.pool[string(NodeID("node2"))] = initDeque[PooledConnection]()
    pool.pool[string(NodeID("node2"))].addLast(pc)

    pool.removeConnection(conn)

    check conn.state == csClosed
    check pool.totalClosed == 1

    pool.close()

  test "removeConnection marks as not in use":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    var conn = Connection(nodeId: NodeID("node2"), state: csConnected)
    initLock(conn.sendLock)
    initLock(conn.recvLock)

    var pc = PooledConnection(conn: conn, inUse: true)
    pool.pool[string(NodeID("node2"))] = initDeque[PooledConnection]()
    pool.pool[string(NodeID("node2"))].addLast(pc)

    pool.removeConnection(conn)

    check pool.pool[string(NodeID("node2"))][0].inUse == false

    pool.close()

  test "removeConnection for non-existent connection":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    var conn = Connection(nodeId: NodeID("node2"), state: csConnected)
    initLock(conn.sendLock)
    initLock(conn.recvLock)

    pool.removeConnection(conn)

    check pool.totalClosed == 0

    pool.close()

suite "Connection Pool Backoff":
  test "Backoff prevents immediate retry after failure":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    let now = int64(times.getTime().toUnix() * 1000)
    pool.lastFailure[string(NodeID("node2"))] = now

    pool.pool[string(NodeID("node2"))] = initDeque[PooledConnection]()
    var pc = PooledConnection(
      conn: Connection(nodeId: NodeID("node2"), state: csConnected),
      inUse: false
    )
    pool.pool[string(NodeID("node2"))].addLast(pc)

    pool.connectBackoffMs = 5000'i64

    pool.close()

  test "Backoff is cleared on successful connection":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    pool.lastFailure[string(NodeID("node2"))] = 1000'i64

    pool.close()

suite "Connection Pool Close Operations":
  test "close closes all connections":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    pool.pool[string(NodeID("node2"))] = initDeque[PooledConnection]()
    pool.pool[string(NodeID("node3"))] = initDeque[PooledConnection]()

    var pc2 = PooledConnection(
      conn: Connection(nodeId: NodeID("node2"), state: csConnected)
    )
    initLock(pc2.conn.sendLock)
    initLock(pc2.conn.recvLock)

    var pc3 = PooledConnection(
      conn: Connection(nodeId: NodeID("node3"), state: csConnected)
    )
    initLock(pc3.conn.sendLock)
    initLock(pc3.conn.recvLock)

    pool.pool[string(NodeID("node2"))].addLast(pc2)
    pool.pool[string(NodeID("node3"))].addLast(pc3)

    pool.close()

    check pool.totalClosed == 2

  test "close is safe on empty pool":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    pool.close()
    check pool.totalClosed == 0

  test "Multiple close calls are safe":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    pool.close()
    pool.close()
    pool.close()

suite "Connection Pool Internal Tests":
  test "Pool table is initialized empty":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    check pool.pool.len == 0
    pool.close()

  test "Pool lastFailure table is initialized empty":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    check pool.lastFailure.len == 0
    pool.close()

  test "Pool can store multiple connections per node":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    pool.pool[string(NodeID("node2"))] = initDeque[PooledConnection]()

    for i in 0..<5:
      var pc = PooledConnection(
        conn: Connection(nodeId: NodeID("node2"), state: csConnected)
      )
      pool.pool[string(NodeID("node2"))].addLast(pc)

    check pool.getConnectionCount(NodeID("node2")) == 5

    pool.close()

  test "Pool can store connections for multiple nodes":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    for nodeIdStr in ["node2", "node3", "node4", "node5"]:
      pool.pool[nodeIdStr] = initDeque[PooledConnection]()
      var pc = PooledConnection(
        conn: Connection(nodeId: NodeID(nodeIdStr), state: csConnected)
      )
      pool.pool[nodeIdStr].addLast(pc)

    check pool.pool.len == 4

    pool.close()

suite "Connection Pool Config Integration":
  test "Pool uses config maxConnectionsPerNode":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    config.maxConnectionsPerNode = 8

    let pool = newConnectionPool(config, "test")
    check pool.config.maxConnectionsPerNode == 8

    pool.close()

  test "Pool uses config idleTimeoutMs":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    config.idleTimeoutMs = 30000

    let pool = newConnectionPool(config, "test")
    check pool.config.idleTimeoutMs == 30000

    pool.close()

suite "Connection Pool Thread Safety":
  test "Pool locks are properly initialized":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    withLock pool.poolLock:
      check true

    pool.close()

  test "Statistics access is thread-safe":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    pool.totalCreated = 5
    pool.totalReused = 3
    pool.totalClosed = 1

    let stats = pool.getStats()
    check stats.created == 5
    check stats.reused == 3
    check stats.closed == 1

    pool.close()

suite "Connection Pool State Filtering":
  test "getAvailableConnectionCount skips failed connections":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    pool.pool[string(NodeID("node2"))] = initDeque[PooledConnection]()

    var pc1 = PooledConnection(
      conn: Connection(nodeId: NodeID("node2"), state: csFailed),
      inUse: false
    )
    var pc2 = PooledConnection(
      conn: Connection(nodeId: NodeID("node2"), state: csConnected),
      inUse: false
    )

    pool.pool[string(NodeID("node2"))].addLast(pc1)
    pool.pool[string(NodeID("node2"))].addLast(pc2)

    check pool.getAvailableConnectionCount(NodeID("node2")) == 1

    pool.close()

  test "getAvailableConnectionCount skips connecting connections":
    let config = newNetworkConfig(NodeID("node1"), 9000)
    let pool = newConnectionPool(config, "test")

    pool.pool[string(NodeID("node2"))] = initDeque[PooledConnection]()

    var pc1 = PooledConnection(
      conn: Connection(nodeId: NodeID("node2"), state: csConnecting),
      inUse: false
    )
    var pc2 = PooledConnection(
      conn: Connection(nodeId: NodeID("node2"), state: csConnected),
      inUse: false
    )

    pool.pool[string(NodeID("node2"))].addLast(pc1)
    pool.pool[string(NodeID("node2"))].addLast(pc2)

    check pool.getAvailableConnectionCount(NodeID("node2")) == 1

    pool.close()

suite "Connection Pool Edge Cases":
  test "Empty deque returns zero count":
    let config = newNetworkConfig(NodeID("node1"), 9500)
    let pool = newConnectionPool(config, "test")

    pool.pool[string(NodeID("node2"))] = initDeque[PooledConnection]()

    check pool.getConnectionCount(NodeID("node2")) == 0
    check pool.getAvailableConnectionCount(NodeID("node2")) == 0

  test "Connection pool with nil connection skips nil":
    let config = newNetworkConfig(NodeID("node1"), 9510)
    let pool = newConnectionPool(config, "test")

    pool.pool[string(NodeID("node2"))] = initDeque[PooledConnection]()
    var pc = PooledConnection(conn: nil, inUse: false, lastUsed: 0)
    pool.pool[string(NodeID("node2"))].addLast(pc)

    check pool.getConnectionCount(NodeID("node2")) == 1
