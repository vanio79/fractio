# Connection Pool - Manages TCP connections with pooling and reuse
# Part of the network transport layer for distributed Fractio

import std/[tables, locks, times, options, deques]
import ./types
import ./tcp_transport
import ./config
import ../../core/types as coretypes

# =============================================================================
# Pool Entry
# =============================================================================

type
  PooledConnection* = ref object
    ## A pooled connection with metadata
    conn*: Connection
    lastUsed*: int64 # Milliseconds since epoch
    inUse*: bool
    createdAt*: int64

  ConnectionPool* = ref object
    ## Connection pool for a single transport type
    config*: NetworkConfig
    role*: string

    # Pool storage: nodeId -> list of pooled connections
    pool*: tables.Table[string, Deque[PooledConnection]]
    poolLock*: Lock

    # Statistics
    totalCreated*: int64
    totalReused*: int64
    totalClosed*: int64

# =============================================================================
# Connection Pool Implementation
# =============================================================================

proc newPooledConnection*(conn: Connection): PooledConnection =
  ## Create a new pooled connection wrapper
  let now = int64(times.getTime().toUnix() * 1000)
  result = PooledConnection(
    conn: conn,
    lastUsed: now,
    inUse: false,
    createdAt: now
  )

proc newConnectionPool*(config: NetworkConfig, role: string): ConnectionPool =
  ## Create a new connection pool
  result = ConnectionPool(
    config: config,
    role: role,
    pool: tables.initTable[string, Deque[PooledConnection]](),
    totalCreated: 0,
    totalReused: 0,
    totalClosed: 0
  )
  initLock(result.poolLock)

proc close*(pool: ConnectionPool) =
  ## Close the pool and all connections
  withLock pool.poolLock:
    for nodeId, conns in pool.pool:
      for pc in conns:
        pc.conn.close()
        pool.totalClosed += 1
    pool.pool.clear()
  deinitLock(pool.poolLock)

# =============================================================================
# Connection Management
# =============================================================================

proc pruneIdleConnections*(pool: ConnectionPool, maxIdleMs: int64) =
  ## Remove connections that have been idle for too long
  let now = int64(times.getTime().toUnix() * 1000)

  withLock pool.poolLock:
    var keysToRemove: seq[string] = @[]

    for nodeId, conns in pool.pool:
      var newConns = initDeque[PooledConnection]()
      for pc in conns:
        if not pc.inUse and (now - pc.lastUsed) > maxIdleMs:
          pc.conn.close()
          pool.totalClosed += 1
        else:
          newConns.addLast(pc)

      if newConns.len == 0:
        keysToRemove.add(nodeId)
      else:
        pool.pool[nodeId] = newConns

    for key in keysToRemove:
      pool.pool.del(key)

proc getOrCreateConnection*(pool: ConnectionPool, transport: TCPTransport,
                            nodeId: NodeID, host: string, port: int): Option[Connection] =
  ## Get an existing connection or create a new one
  let key = string(nodeId)

  withLock pool.poolLock:
    # Check if we have available connections
    if key in pool.pool:
      var conns = pool.pool[key]
      for i in 0 ..< conns.len:
        let pc = conns[i]
        if not pc.inUse and pc.conn.state == csConnected:
          pc.inUse = true
          pc.lastUsed = int64(times.getTime().toUnix() * 1000)
          pool.totalReused += 1
          return some(pc.conn)

  # Need to create a new connection
  let connOpt = transport.connectToNode(nodeId, host, port)
  if connOpt.isNone:
    return none(Connection)

  let conn = connOpt.get()
  let pc = newPooledConnection(conn)
  pc.inUse = true

  withLock pool.poolLock:
    if key notin pool.pool:
      pool.pool[key] = initDeque[PooledConnection]()
    pool.pool[key].addLast(pc)
    pool.totalCreated += 1

  return some(conn)

proc returnConnection*(pool: ConnectionPool, conn: Connection) =
  ## Return a connection to the pool
  let key = string(conn.nodeId)

  withLock pool.poolLock:
    if key in pool.pool:
      var conns = pool.pool[key]
      for i in 0 ..< conns.len:
        if conns[i].conn == conn:
          conns[i].inUse = false
          conns[i].lastUsed = int64(times.getTime().toUnix() * 1000)
          return

proc removeConnection*(pool: ConnectionPool, conn: Connection) =
  ## Remove a connection from the pool
  let key = string(conn.nodeId)

  withLock pool.poolLock:
    if key in pool.pool:
      var conns = pool.pool[key]
      var newConns = initDeque[PooledConnection]()
      var found = false
      for pc in conns:
        if pc.conn == conn:
          pc.conn.close()
          pool.totalClosed += 1
          found = true
        else:
          newConns.addLast(pc)

      if found:
        if newConns.len == 0:
          pool.pool.del(key)
        else:
          pool.pool[key] = newConns

proc getStats*(pool: ConnectionPool): tuple[created: int64, reused: int64,
    closed: int64, active: int] =
  ## Get pool statistics
  withLock pool.poolLock:
    result.created = pool.totalCreated
    result.reused = pool.totalReused
    result.closed = pool.totalClosed

    var active = 0
    for nodeId, conns in pool.pool:
      for pc in conns:
        if pc.inUse:
          inc active
    result.active = active

proc getConnectionCount*(pool: ConnectionPool, nodeId: NodeID): int =
  ## Get the number of connections for a specific node
  let key = string(nodeId)
  withLock pool.poolLock:
    if key in pool.pool:
      result = pool.pool[key].len

proc getAvailableConnectionCount*(pool: ConnectionPool, nodeId: NodeID): int =
  ## Get the number of available (not in use) connections for a node
  let key = string(nodeId)
  withLock pool.poolLock:
    if key in pool.pool:
      for pc in pool.pool[key]:
        if not pc.inUse and pc.conn.state == csConnected:
          inc result
