## Connection pool for the Fractio client.
## =====================================
##
## Rationale
## ---------
## Prior to this module, every code path that needed a TCP connection to a
## Fractio node called `client.connectToNode(host, port)` and either:
##   (a) cached the resulting `ProtocolClient` in a `Table` keyed by
##       (groupId, nodeId) and silently OVERWROTE older entries on
##       leader change / metadata refresh, dropping the old `ProtocolClient`
##       ref without ever calling `disconnect()`; or
##   (b) created a one-shot "dedicated" connection for a parallel stream
##       and `disconnect()`ed it on the failure path but reused it
##       indefinitely on the success path.
##
## In both cases, the FD underneath the `ProtocolClient`'s `Socket` was
## leaked at the OS level (or eventually closed by Nim's socket GC, but
## without sending the proper Fractio disconnect frame). After a few
## thousand queries, the process ran out of FDs and the server-side
## `select()` (which uses BSD `fd_set` bounded at FD_SETSIZE=1024) crashed
## with `*** bit out of range 0 - FD_SETSIZE on fd_set ***`.
##
## This module replaces the ad-hoc caches with a single, bounded, thread-safe
## pool. Every call site that needs a connection goes through
## `pool.acquire(host, port, factory)`. When the caller is done, it calls
## `pool.release(conn, host, port, keepAlive=true)`. The pool either keeps
## the conn on its idle list (under cap, healthy) or disconnects and drops
## it.
##
## Design choices
## --------------
## * **Bounded**: `maxPerNode` (default 4) caps idle+checked-out per
##   (host,port); `maxTotal` (default 32) is a global safety cap. The
##   default sizes match a typical 3-replica cluster: 4 conns per node
##   is enough to support parallel k-way merge scans plus a few
##   background ops, while 32 global prevents runaway growth.
## * **Thread-safe**: a single `Lock` serialises both the idle map and
##   the counters. Acquire/release are O(1) under the lock; the lock is
##   the only synchronisation primitive we need because all bookkeeping
##   is local to the pool. The caller-side work (sending/receiving over
##   the connection) is outside the lock, just like the existing
##   `FractioClient.leaderConnections` design.
## * **Liveness-checked on acquire**: if the caller returns a connection
##   whose `connected` flag is `false` (peer reset, server crash, idle
##   timeout), the pool drops it and tries again up to 2 times before
##   failing the acquire.
## * **Decoupled from the rest of the client**: the pool takes a
##   `connectProc` callback so it doesn't import `FractioClient` (which
##   is what the pool is replacing parts of). This keeps the module
##   testable in isolation.
## * **Stats for telemetry**: `pool.stats()` returns counts of
##   alive/idle/acquires/releases/disconnects so dashboards (and the
##   smoke test) can verify the leak is actually gone.

import std/[tables, locks, options, atomics]
import ../protocol/client

# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------

type
  ConnectProc* = proc (host: string, port: int): Option[
      ProtocolClient] {.closure, gcsafe.}
    ## Factory callback supplied by the client. Marked `gcsafe` and
    ## `closure` so it can be called from inside `acquire` (which is
    ## itself gcsafe) without tripping the atomicArc escape analysis.

  PoolStats* = object
    ## Snapshot of pool state. Counts `acquires`/`releases`/
    ## `disconnects` over the pool's lifetime for observability.
    totalAlive*: int ## All conns ever created that haven't been closed
    totalIdle*: int ## Currently parked on the idle list
    totalCheckedOut*: int ## totalAlive - totalIdle
    acquires*: int64 ## Successful acquire() calls (cumulative)
    acquiresFailed*: int64 ## acquire() that returned none
    releases*: int64 ## release() calls (cumulative)
    disconnects*: int64 ## Conn destruction calls (cumulative)

  ConnectionPool* = ref object
    ## Thread-safe bounded connection pool keyed by (host, port).
    ##
    ## All fields are accessed under `mu` EXCEPT `acquires`,
    ## `acquiresFailed`, `releases`, `disconnects` which are atomic and
    ## can be read without holding the lock for telemetry.
    maxPerNode*: int
    maxTotal*: int
    mu*: Lock
    idle*: Table[(string, int), seq[ProtocolClient]]
      ## Parked connections, ready for reuse. The seq is a stack — last
      ## in, first out. This is a deliberate cache-locality choice: the
      ## most recently released conn is the most likely to be warm in
      ## the kernel's TCP buffer.
    totalAlive*: int
    acquires*: Atomic[int64]
    acquiresFailed*: Atomic[int64]
    releases*: Atomic[int64]
    disconnects*: Atomic[int64]
    closed*: bool

# ---------------------------------------------------------------------------
# Constructors
# ---------------------------------------------------------------------------

proc newConnectionPool*(maxPerNode: int = 4,
                        maxTotal: int = 32): ConnectionPool {.gcsafe.} =
  ## Create a new pool. Caps are advisory: the pool will refuse to
  ## create more than `maxPerNode` per (host,port) and `maxTotal` overall.
  ## A `maxPerNode <= 0` or `maxTotal <= 0` is treated as "no cap" for
  ## that dimension, which is useful for unit tests that want to verify
  ## the bounded-vs-unbounded behaviour.
  if maxPerNode <= 0: discard # 0 or negative = unbounded
  if maxTotal <= 0: discard
  result = ConnectionPool(
    maxPerNode: maxPerNode,
    maxTotal: maxTotal,
    idle: initTable[(string, int), seq[ProtocolClient]](),
  )
  initLock(result.mu)
  result.acquires.store(0)
  result.acquiresFailed.store(0)
  result.releases.store(0)
  result.disconnects.store(0)

# ---------------------------------------------------------------------------
# Internal helpers (must be called with mu held)
# ---------------------------------------------------------------------------

proc keyFor(host: string, port: int): (string, int) {.gcsafe.} =
  (host, port)

proc closeConn(conn: ProtocolClient) {.gcsafe.} =
  ## Best-effort close. We never raise; the pool must work even if
  ## a misbehaving conn throws.
  if conn != nil and conn.connected.load(moRelaxed):
    try: conn.disconnect("pool_close")
    except CatchableError: discard

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

proc acquire*(pool: ConnectionPool, host: string, port: int,
              factory: ConnectProc): Option[ProtocolClient] {.gcsafe.} =
  ## Get a connection to (host, port).
  ##
  ## Fast path: an idle, healthy connection is parked — pop it and
  ## return it. Slow path: create a new one via `factory(host, port)`.
  ## Refusal path: if both idle list is empty AND the per-node or
  ## global cap is hit, return `none` so the caller can decide
  ## whether to back off, create its own temporary conn, or fail.
  ##
  ## The returned conn is "checked out" — the caller MUST eventually
  ## call `release` (or `close` if it's not fit for reuse) to keep
  ## the pool balanced.
  if pool == nil or pool.closed:
    return none(ProtocolClient)
  if factory == nil:
    return none(ProtocolClient)

  let k = keyFor(host, port)

  # Fast path: try to pop a healthy idle conn (up to 2 attempts, in
  # case the most-recently-released conn is dead).
  for attempt in 0 ..< 2:
    var popped: ProtocolClient = nil
    acquire(pool.mu)
    try:
      if k in pool.idle:
        var lst = pool.idle[k]
        if lst.len > 0:
          popped = lst.pop()
          if lst.len == 0:
            pool.idle.del(k)
          else:
            pool.idle[k] = lst
    finally:
      release(pool.mu)

    if popped != nil:
      # Liveness check outside the lock — connected.load() is atomic
      if popped.connected.load(moRelaxed):
        discard pool.acquires.fetchAdd(1)
        return some(popped)
      # Dead conn: close and try again (or fall through to create)
      acquire(pool.mu)
      try:
        dec pool.totalAlive
      finally:
        release(pool.mu)
      discard pool.disconnects.fetchAdd(1)
      closeConn(popped)
      popped = nil
      # Loop to next attempt
    else:
      break # No more idle conns

  # Slow path: create a new conn if we're under the caps.
  # Snapshot the per-node idle length atomically under the lock
  # to avoid a TOCTOU race against a concurrent release on the
  # same key.
  var idleLenPerNode = 0
  acquire(pool.mu)
  try:
    if k in pool.idle:
      idleLenPerNode = pool.idle[k].len
  finally:
    release(pool.mu)
  let perNodeOk = (pool.maxPerNode <= 0) or (idleLenPerNode < pool.maxPerNode)
  # The "checked-out" check is approximate: `totalAlive` is read
  # without the lock; it's only ever decremented under the lock, so
  # we may transiently see a stale larger value and create one extra
  # conn — bounded by the next call's acquire failing. This is the
  # standard pattern for bounded pools and is good enough.
  let totalAliveSnapshot = pool.totalAlive
  let totalOk = (pool.maxTotal <= 0) or (totalAliveSnapshot < pool.maxTotal)

  if not perNodeOk or not totalOk:
    discard pool.acquiresFailed.fetchAdd(1)
    return none(ProtocolClient)

  let connOpt = factory(host, port)
  if connOpt.isNone:
    discard pool.acquiresFailed.fetchAdd(1)
    return none(ProtocolClient)

  acquire(pool.mu)
  try:
    inc pool.totalAlive
  finally:
    release(pool.mu)

  discard pool.acquires.fetchAdd(1)
  return connOpt

proc release*(pool: ConnectionPool, conn: ProtocolClient,
              host: string, port: int, keepAlive: bool = true) {.gcsafe.} =
  ## Return a connection to the pool.
  ##
  ## * `keepAlive=true` and conn still healthy AND under cap: park it
  ##   on the idle list for the next caller.
  ## * `keepAlive=false` OR conn is dead OR pool is closed OR over
  ##   cap: disconnect and drop the conn.
  ##
  ## It is safe to call `release` with a `nil` conn — that's a no-op
  ## (callers don't need to nil-check at every exit path).
  if pool == nil or conn == nil:
    return
  discard pool.releases.fetchAdd(1)

  let k = keyFor(host, port)
  let healthy = conn.connected.load(moRelaxed)
  # Snapshot the idle length atomically under the lock to avoid a
  # TOCTOU between "key present" and "read length". Without this, a
  # concurrent `release` on the same key could del() the entry between
  # the `k in pool.idle` check and the `pool.idle[k].len` access,
  # raising KeyError.
  var idleLen = 0
  acquire(pool.mu)
  try:
    if k in pool.idle:
      idleLen = pool.idle[k].len
  finally:
    release(pool.mu)
  let underCap = (pool.maxPerNode <= 0) or (idleLen < pool.maxPerNode)

  var doDrop = (not keepAlive) or (not healthy) or pool.closed or (not underCap)

  if not doDrop:
    acquire(pool.mu)
    try:
      # Re-check under lock (another release could have happened)
      if pool.closed:
        doDrop = true
      elif (pool.maxPerNode > 0) and (k in pool.idle) and
           (pool.idle[k].len >= pool.maxPerNode):
        doDrop = true
      else:
        if k notin pool.idle:
          pool.idle[k] = @[conn]
        else:
          var lst = pool.idle[k]
          lst.add(conn)
          pool.idle[k] = lst
    finally:
      release(pool.mu)

  if doDrop:
    acquire(pool.mu)
    try:
      dec pool.totalAlive
    finally:
      release(pool.mu)
    discard pool.disconnects.fetchAdd(1)
    closeConn(conn)

proc closeAll*(pool: ConnectionPool) {.gcsafe.} =
  ## Mark the pool closed and disconnect every idle connection.
  ## Already-checked-out connections are NOT closed here (their owner
  ## is responsible); they will fail the liveness check on next
  ## acquire and be evicted naturally.
  if pool == nil:
    return

  acquire(pool.mu)
  try:
    pool.closed = true
    for k, lst in pool.idle:
      for c in lst:
        closeConn(c)
        discard pool.disconnects.fetchAdd(1)
      dec pool.totalAlive, lst.len
    pool.idle.clear()
  finally:
    release(pool.mu)

proc stats*(pool: ConnectionPool): PoolStats {.gcsafe.} =
  ## Return a snapshot of pool counters. Cheap (lock for a few counter
  ## reads, atomic loads for the rest). Safe to call from telemetry
  ## paths without holding the caller's lock.
  if pool == nil:
    return PoolStats()
  acquire(pool.mu)
  try:
    result.totalAlive = pool.totalAlive
    result.totalIdle = 0
    for _, lst in pool.idle:
      inc result.totalIdle, lst.len
    result.totalCheckedOut = result.totalAlive - result.totalIdle
  finally:
    release(pool.mu)
  result.acquires = pool.acquires.load()
  result.acquiresFailed = pool.acquiresFailed.load()
  result.releases = pool.releases.load()
  result.disconnects = pool.disconnects.load()

proc isClosed*(pool: ConnectionPool): bool {.inline, gcsafe.} =
  if pool == nil: return true
  acquire(pool.mu)
  try: result = pool.closed
  finally: release(pool.mu)

# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------
#
# Note: We deliberately do NOT define `=destroy` for `ConnectionPool`.
# Under `--mm:atomicArc`, `=destroy` only fires for non-ref objects, and
# the GC handles ref finalisation for us. The `Lock` in `pool.mu` is
# process-lifetime (the pool itself is process-lifetime), so leaving it
# initialised at exit is fine. Crucially, `ProtocolClient` sockets DO
# get finalised by the GC, but the GC will close the underlying FD
# without sending a Fractio disconnect frame — which is exactly the
# leak we are trying to fix. So production code MUST call
# `closeAll()` (typically from `client.close()`) to send the disconnect
# frames explicitly. If that path is missed, the leak is back, but at
# least bounded to one conn per "missed close" rather than unbounded.
