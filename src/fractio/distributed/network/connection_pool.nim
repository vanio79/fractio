# Connection Pool - Manages TCP connections with pooling and reuse
# Part of the network transport layer for distributed Fractio

import std/[tables, locks, times, options, deques]
import ./types
import ./tcp_transport
import ./config
import ../../core/types as coretypes
import ../../distributed/sharedtimer/timeprovider

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
    timeProvider*: TimeProvider

    # Pool storage: nodeId -> list of pooled connections
    pool*: tables.Table[string, Deque[PooledConnection]]
    poolLock*: Lock

    # Backoff: nodeId -> epoch-ms of last failed connect attempt.
    # getOrCreateConnection skips new connections to a node if the last
    # failure was less than connectBackoffMs ago.
    lastFailure*: tables.Table[string, int64]
    connectBackoffMs*: int64 ## cooldown between retries (default 1000ms)

    # Statistics
    totalCreated*: int64
    totalReused*: int64
    totalClosed*: int64

proc poolTimeMs(tp: TimeProvider): int64 {.inline, raises: [].} =
  if tp != nil:
    try:
      return tp.now() div 1_000_000
    except Exception:
      discard
  let t = times.getTime()
  t.toUnix * 1000 + t.nanosecond() div 1_000_000

# =============================================================================
# Connection Pool Implementation
# =============================================================================

proc newPooledConnection*(conn: Connection,
    tp: TimeProvider = nil): PooledConnection =
  ## Create a new pooled connection wrapper
  let now = poolTimeMs(tp)
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
    lastFailure: tables.initTable[string, int64](),
    connectBackoffMs: 1000, # 1 second cooldown after failed connect
    totalCreated: 0,
    totalReused: 0,
    totalClosed: 0
  )
  initLock(result.poolLock)

proc close*(pool: ConnectionPool) =
  ## Close the pool — close all sockets but leave ref cleanup to GC.
  withLock pool.poolLock:
    for nodeId, conns in pool.pool:
      for pc in conns:
        pc.conn.close()
        pool.totalClosed += 1
    # Don't clear the table here — under atomicArc, clearing triggers
    # dealloc of Connection refs which races with other threads that
    # may still hold refs.  The table is cleaned up when the pool
    # itself is collected.

  # =============================================================================
  # Connection Management
  # =============================================================================

proc pruneIdleConnections*(pool: ConnectionPool, maxIdleMs: int64) =
  ## Remove connections that have been idle for too long
  let now = poolTimeMs(pool.timeProvider)

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
          pc.lastUsed = poolTimeMs(pool.timeProvider)
          pool.totalReused += 1
          return some(pc.conn)

  # Backoff: skip if we recently failed to connect to this node
  let now2 = poolTimeMs(pool.timeProvider)
  withLock pool.poolLock:
    if key in pool.lastFailure:
      let lastFail = pool.lastFailure[key]
      if (now2 - lastFail) < pool.connectBackoffMs:
        return none(Connection)

  # Need to create a new connection
  let connOpt = transport.connectToNode(nodeId, host, port)
  if connOpt.isNone:
    # Record failure time for backoff
    withLock pool.poolLock:
      pool.lastFailure[key] = poolTimeMs(pool.timeProvider)
    return none(Connection)

  let conn = connOpt.get()
  let pc = newPooledConnection(conn, pool.timeProvider)
  pc.inUse = true

  withLock pool.poolLock:
    if key notin pool.pool:
      pool.pool[key] = initDeque[PooledConnection]()
    pool.pool[key].addLast(pc)
    pool.totalCreated += 1
    # Clear backoff on successful connection
    pool.lastFailure.del(key)

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
          conns[i].lastUsed = poolTimeMs(pool.timeProvider)
          return

proc removeConnection*(pool: ConnectionPool, conn: Connection) =
  ## Mark a connection as closed and unusable within the pool.
  ## The socket is closed but the PooledConnection entry is NOT removed
  ## from the pool's deque.  Under --mm:atomicArc, removing/deallocating
  ## a PooledConnection (and its Connection ref) on a different thread than
  ## the one that allocated it can SIGSEGV in addToSharedFreeList.  The stale
  ## entry stays in the deque; getOrCreateConnection skips it (state != csConnected)
  ## and pruneIdleConnections will clean it up on the allocating thread.
  conn.close()

  withLock pool.poolLock:
    let key = string(conn.nodeId)
    if key in pool.pool:
      for pc in pool.pool[key]:
        if pc.conn == conn:
          pc.inUse = false
          pool.totalClosed += 1
          break

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
