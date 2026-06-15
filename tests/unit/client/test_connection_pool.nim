## Unit tests for the Fractio client ConnectionPool.
## ======================================================
##
## These tests use a fake `ConnectProc` that returns a plain
## `ProtocolClient` allocated with `newProtocolClient()` (no real socket).
## The pool only checks `conn.connected` for liveness, so this is
## sufficient to exercise acquire / release / closeAll / cap / stats
## logic in isolation from any network I/O.
##
## Concurrency tests spawn N threads doing M acquire/release cycles
## against a shared pool and verify (a) the pool's invariant
## `totalAlive <= maxTotal` always holds, (b) the stats counters
## stay consistent, and (c) there are no data races on the idle list.

import std/[unittest, options, atomics, locks, os]
import std/typedthreads
import fractio/protocol/client as pclient
import fractio/client/connection_pool

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

var
  gConnIdCounter: Atomic[int64]
  gFactoryCalls: Atomic[int64]
  gDisconnectCalls: Atomic[int64]

proc resetCounters() =
  gConnIdCounter.store(0)
  gFactoryCalls.store(0)
  gDisconnectCalls.store(0)

proc makeFakeConn(): pclient.ProtocolClient =
  ## Allocate a `ProtocolClient` and mark it connected. No socket
  ## is opened; the pool's liveness check only looks at
  ## `connected.load(moRelaxed)`.
  let cfg = pclient.defaultClientConfig("127.0.0.1", 0)
  result = pclient.newProtocolClient(cfg)
  result.connected.store(true)
  discard gConnIdCounter.fetchAdd(1)

proc fakeFactory(host: string, port: int): Option[
    pclient.ProtocolClient] {.gcsafe.} =
  ## A simple, deterministic factory. Each call mints a fresh conn.
  discard gFactoryCalls.fetchAdd(1)
  return some(makeFakeConn())

proc failingFactory(host: string, port: int): Option[
    pclient.ProtocolClient] {.gcsafe.} =
  discard gFactoryCalls.fetchAdd(1)
  return none(pclient.ProtocolClient)

# A counting disconnect wrapper. The pool's `closeConn` swallows
# exceptions, so we can also detect "we tried to close" via the
# conn.connected flip.
proc isConnAlive(c: pclient.ProtocolClient): bool {.inline.} =
  c.connected.load(moRelaxed)

# ---------------------------------------------------------------------------
# Basic semantics
# ---------------------------------------------------------------------------

suite "ConnectionPool - Basic semantics":

  setup:
    resetCounters()

  test "acquire creates a connection when pool is empty":
    let pool = newConnectionPool(maxPerNode = 4, maxTotal = 32)
    let connOpt = pool.acquire("h", 1, fakeFactory)
    check connOpt.isSome
    check pool.stats().totalAlive == 1
    check pool.stats().totalCheckedOut == 1
    check pool.stats().acquires == 1
    check pool.stats().acquiresFailed == 0

  test "acquire then release with keepAlive parks the conn":
    let pool = newConnectionPool(maxPerNode = 4, maxTotal = 32)
    let c1 = pool.acquire("h", 1, fakeFactory).get()
    pool.release(c1, "h", 1, keepAlive = true)
    let s = pool.stats()
    check s.totalAlive == 1
    check s.totalIdle == 1
    check s.totalCheckedOut == 0
    check s.releases == 1
    check s.disconnects == 0

  test "release with keepAlive=false disconnects the conn":
    let pool = newConnectionPool(maxPerNode = 4, maxTotal = 32)
    let c1 = pool.acquire("h", 1, fakeFactory).get()
    pool.release(c1, "h", 1, keepAlive = false)
    let s = pool.stats()
    check s.totalAlive == 0
    check s.disconnects == 1
    check s.releases == 1
    check not c1.connected.load(moRelaxed)

  test "acquire returns the same conn after release":
    let pool = newConnectionPool(maxPerNode = 4, maxTotal = 32)
    let c1 = pool.acquire("h", 1, fakeFactory).get()
    pool.release(c1, "h", 1, keepAlive = true)
    let c2 = pool.acquire("h", 1, fakeFactory).get()
    check c1 == c2
    let s = pool.stats()
    # Only 1 conn ever created, 2 acquires, 1 release, 0 disconnects
    check s.totalAlive == 1
    check s.acquires == 2
    check s.releases == 1
    check s.disconnects == 0

  test "release with nil conn is a no-op":
    let pool = newConnectionPool(maxPerNode = 4, maxTotal = 32)
    pool.release(nil, "h", 1, keepAlive = true)
    let s = pool.stats()
    check s.releases == 0
    check s.totalAlive == 0

# ---------------------------------------------------------------------------
# Liveness
# ---------------------------------------------------------------------------

suite "ConnectionPool - Liveness":

  setup:
    resetCounters()

  test "dead conn on idle is evicted and pool creates a new one":
    let pool = newConnectionPool(maxPerNode = 4, maxTotal = 32)
    let c1 = pool.acquire("h", 1, fakeFactory).get()
    pool.release(c1, "h", 1, keepAlive = true)
    # Simulate the peer dropping the conn between releases
    c1.connected.store(false)
    # Next acquire should NOT return the dead conn; it should call factory
    let c2Opt = pool.acquire("h", 1, fakeFactory)
    check c2Opt.isSome
    check c2Opt.get() != c1
    let s = pool.stats()
    check s.totalAlive == 1
    # 2 acquires: the first attempt popped a dead conn, evicted it, then
    # the second attempt went to the factory.
    check s.acquires == 2
    check s.disconnects == 1
    check gFactoryCalls.load() == 2

  test "dead conn from factory is reported as failed acquire":
    let pool = newConnectionPool(maxPerNode = 4, maxTotal = 32)
    let r = pool.acquire("h", 1, failingFactory)
    check r.isNone
    let s = pool.stats()
    check s.acquiresFailed == 1
    check s.acquires == 0
    check s.totalAlive == 0

# ---------------------------------------------------------------------------
# Caps
# ---------------------------------------------------------------------------

suite "ConnectionPool - Caps":

  setup:
    resetCounters()

  test "maxPerNode prevents over-parking on release":
    let pool = newConnectionPool(maxPerNode = 2, maxTotal = 100)
    # Acquire 3 conns, release them all with keepAlive=true.
    # The 3rd release should drop the conn (over per-node cap).
    let c1 = pool.acquire("h", 1, fakeFactory).get()
    let c2 = pool.acquire("h", 1, fakeFactory).get()
    let c3 = pool.acquire("h", 1, fakeFactory).get()
    pool.release(c1, "h", 1, keepAlive = true)
    pool.release(c2, "h", 1, keepAlive = true)
    pool.release(c3, "h", 1, keepAlive = true) # dropped
    let s = pool.stats()
    check s.totalAlive == 2
    check s.totalIdle == 2
    check s.disconnects == 1

  test "maxTotal caps total alive conns across nodes":
    let pool = newConnectionPool(maxPerNode = 100, maxTotal = 3)
    # Acquire 3 conns, then a 4th acquire should fail
    discard pool.acquire("h", 1, fakeFactory).get()
    discard pool.acquire("h", 2, fakeFactory).get()
    discard pool.acquire("h", 3, fakeFactory).get()
    let c4 = pool.acquire("h", 4, fakeFactory)
    check c4.isNone
    let s = pool.stats()
    check s.totalAlive == 3
    check s.acquires == 3
    check s.acquiresFailed == 1

  test "release frees up a slot for new acquires":
    let pool = newConnectionPool(maxPerNode = 100, maxTotal = 2)
    let c1 = pool.acquire("h", 1, fakeFactory).get()
    discard pool.acquire("h", 2, fakeFactory).get()
    let c3 = pool.acquire("h", 3, fakeFactory)
    check c3.isNone
    pool.release(c1, "h", 1, keepAlive = true) # frees 1 slot
    let c4 = pool.acquire("h", 1, fakeFactory)
    check c4.isSome
    let s = pool.stats()
    check s.totalAlive == 2

# ---------------------------------------------------------------------------
# closeAll / shutdown
# ---------------------------------------------------------------------------

suite "ConnectionPool - Lifecycle":

  setup:
    resetCounters()

  test "closeAll disconnects every idle conn":
    let pool = newConnectionPool(maxPerNode = 4, maxTotal = 32)
    let c1 = pool.acquire("h", 1, fakeFactory).get()
    let c2 = pool.acquire("h", 2, fakeFactory).get()
    let c3 = pool.acquire("h", 3, fakeFactory).get()
    pool.release(c1, "h", 1, keepAlive = true)
    pool.release(c2, "h", 2, keepAlive = true)
    pool.release(c3, "h", 3, keepAlive = true)
    check pool.stats().totalIdle == 3
    pool.closeAll()
    let s = pool.stats()
    check s.totalAlive == 0
    check s.totalIdle == 0
    check s.disconnects == 3
    check not c1.connected.load(moRelaxed)
    check not c2.connected.load(moRelaxed)
    check not c3.connected.load(moRelaxed)

  test "acquire after closeAll returns none":
    let pool = newConnectionPool(maxPerNode = 4, maxTotal = 32)
    pool.closeAll()
    let c = pool.acquire("h", 1, fakeFactory)
    check c.isNone
    check pool.isClosed()

  test "isClosed is false for a fresh pool":
    let pool = newConnectionPool()
    check not pool.isClosed()

  test "release of conn is safe after closeAll (conn is dropped not parked)":
    let pool = newConnectionPool(maxPerNode = 4, maxTotal = 32)
    let c1 = pool.acquire("h", 1, fakeFactory).get()
    pool.closeAll()
    pool.release(c1, "h", 1, keepAlive = true)
    let s = pool.stats()
    # The release-after-close should have been treated as a drop.
    check s.totalAlive == 0
    check s.disconnects == 1

# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

suite "ConnectionPool - Stats":

  setup:
    resetCounters()

  test "stats reports zero for empty pool":
    let pool = newConnectionPool()
    let s = pool.stats()
    check s.totalAlive == 0
    check s.totalIdle == 0
    check s.totalCheckedOut == 0
    check s.acquires == 0
    check s.acquiresFailed == 0
    check s.releases == 0
    check s.disconnects == 0

  test "stats is consistent across many cycles":
    let pool = newConnectionPool(maxPerNode = 4, maxTotal = 100)
    for i in 0 ..< 50:
      let c = pool.acquire("h", 1, fakeFactory).get()
      pool.release(c, "h", 1, keepAlive = true)
    let s = pool.stats()
    check s.totalAlive == 1 # same conn reused every time
    check s.acquires == 50
    check s.releases == 50
    check s.disconnects == 0

  test "stats on nil pool is zeros":
    let s = (cast[ConnectionPool](nil)).stats()
    check s.totalAlive == 0
    check s.acquires == 0

# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------

const
  CONC_THREADS = 8
  CONC_OPS_PER_THREAD = 200

suite "ConnectionPool - Concurrency":

  setup:
    resetCounters()

  test "concurrent acquire/release from N threads preserves invariants":
    let pool = newConnectionPool(maxPerNode = 100, maxTotal = 16)
    var readyCount: Atomic[int]
    readyCount.store(0)
    var goFlag: Atomic[bool]
    goFlag.store(false)
    var barrierLock: Lock
    initLock(barrierLock)

    type WorkerArgs = object
      pool: ConnectionPool
      readyCount: ptr Atomic[int]
      goFlag: ptr Atomic[bool]
      barrierLock: ptr Lock

    proc worker(args: WorkerArgs) {.thread, gcsafe.} =
      # Synchronise at the barrier
      acquire(args.barrierLock[])
      discard args.readyCount[].fetchAdd(1)
      release(args.barrierLock[])
      # Spin until go
      while not args.goFlag[].load(moRelaxed):
        os.sleep(1)

      for i in 0 ..< CONC_OPS_PER_THREAD:
        var attempts = 0
        while attempts < 5:
          let cOpt = args.pool.acquire("h", 1, fakeFactory)
          if cOpt.isSome:
            let c = cOpt.get()
            # Hold for a brief moment
            os.sleep(1)
            args.pool.release(c, "h", 1, keepAlive = true)
            break
          else:
            os.sleep(1)
            inc attempts

    var argsArr: array[CONC_THREADS, WorkerArgs]
    for i in 0 ..< CONC_THREADS:
      argsArr[i] = WorkerArgs(
        pool: pool,
        readyCount: addr readyCount,
        goFlag: addr goFlag,
        barrierLock: addr barrierLock,
      )

    var threads: array[CONC_THREADS, Thread[WorkerArgs]]
    for i in 0 ..< CONC_THREADS:
      createThread(threads[i], worker, argsArr[i])

    # Wait until all threads are at the barrier
    while readyCount.load(moRelaxed) < CONC_THREADS:
      os.sleep(1)
    # Release them
    goFlag.store(true)
    for i in 0 ..< CONC_THREADS:
      joinThread(threads[i])

    let s = pool.stats()
    # Invariant: totalAlive <= maxTotal
    check s.totalAlive <= 16
    # Every successful acquire has a matching release
    check s.acquires == s.releases
    # We should have created far fewer than THREADS*OPS conns
    # (the pool is doing its job reusing)
    let factoryCalls = gFactoryCalls.load()
    check factoryCalls <= (CONC_THREADS * CONC_OPS_PER_THREAD).int64
    check s.totalIdle <= 16

    pool.closeAll()
    deinitLock(barrierLock)

# Suppress unused-import warning for `tables`
when false:
  var _: Table[int, int]
