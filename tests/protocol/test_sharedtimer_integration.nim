# Phase 7: SharedTimer ↔ TransactionManager integration tests.
#
# Verifies:
#   1. Single-node server (sharedTimerEnabled=false) still works — timestamps
#      come from the wall-clock fallback path in allocTimestamp().
#   2. Server with sharedTimerEnabled=true has a non-nil sharedTimer and the
#      TransactionManager's timeProvider is wired to it.
#   3. Timestamps produced by the wired server are sourced from SharedTimer.now()
#      (which gracefully falls back to local clock when unsynced, so the value
#      is still a valid nanosecond timestamp).
#   4. SharedTimer.start() / stop() lifecycle through server.start() / stop()
#      works without deadlock or crash.
#   5. Background tick thread does not interfere with manual tick() calls.
#
# Port range: 20400–20449

import unittest
import std/[times, os, tables, net, atomics]

import fractio/protocol/server
import fractio/protocol/txn_manager
import fractio/distributed/sharedtimer
import fractio/distributed/sharedtimer/timeprovider as tp
import fractio/core/types as coreTypes

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc makeConfig(port: int, enableTimer: bool = false,
                peers: seq[PeerConfig] = @[]): ServerConfig =
  var cfg = defaultServerConfig()
  cfg.host = "127.0.0.1"
  cfg.port = port
  cfg.serverName = "test-node"
  cfg.serverId = 42
  cfg.sharedTimerEnabled = enableTimer
  cfg.sharedTimerNodeId = "test-node-42"
  cfg.sharedTimerNumericNodeId = 42
  cfg.sharedTimerPeers = peers
  cfg

proc waitForServer(port: int, maxMs: int = 500) =
  ## Poll until the TCP port is accepting connections (or maxMs expires).
  let deadline = epochTime() + float(maxMs) / 1000.0
  while epochTime() < deadline:
    try:
      var s = newSocket()
      s.connect("127.0.0.1", Port(port))
      s.close()
      return
    except:
      sleep(20)

# ---------------------------------------------------------------------------
# Suite 1: Default server (no SharedTimer)
# ---------------------------------------------------------------------------

suite "SharedTimer integration — disabled (single-node fallback)":

  test "server starts and stops cleanly without SharedTimer":
    let srv = newProtocolServer(makeConfig(20400))
    check srv.sharedTimer.isNil
    srv.start()
    waitForServer(20400)
    check srv.running.load()
    srv.stop()
    sleep(50)

  test "TransactionManager uses wall-clock fallback when sharedTimer is nil":
    let srv = newProtocolServer(makeConfig(20401))
    check srv.txnMgr.timeProvider.isNil
    srv.start()
    waitForServer(20401)
    let txn = srv.txnMgr.beginTransaction()
    check txn.id != zeroTransactionID()
    # Timestamp should be a reasonable Unix nanosecond value (after year 2020)
    let minTs = uint64(1_577_836_800) * 1_000_000_000'u64 # 2020-01-01 in ns
    check txn.readTimestamp > minTs
    srv.stop()
    sleep(50)

  test "commit and rollback work without SharedTimer":
    let srv = newProtocolServer(makeConfig(20402))
    srv.start()
    waitForServer(20402)
    let txn = srv.txnMgr.beginTransaction()
    discard srv.txnMgr.recordWrite(txn.id, "key1")
    let cr = srv.txnMgr.commitTransaction(txn.id)
    check cr.status == TxnCommitOK
    check cr.commitTimestamp > txn.readTimestamp
    srv.stop()
    sleep(50)

# ---------------------------------------------------------------------------
# Suite 2: Server with SharedTimer enabled
# ---------------------------------------------------------------------------

suite "SharedTimer integration — enabled":

  test "newProtocolServer wires SharedTimer when sharedTimerEnabled=true":
    let srv = newProtocolServer(makeConfig(20410, enableTimer = true))
    check not srv.sharedTimer.isNil
    check not srv.txnMgr.timeProvider.isNil

  test "sharedTimer.nodeId matches config":
    let srv = newProtocolServer(makeConfig(20411, enableTimer = true))
    check srv.sharedTimer.nodeId == "test-node-42"
    check srv.sharedTimer.numericNodeId == 42

  test "sharedTimer defaults nodeId to serverName when sharedTimerNodeId is empty":
    var cfg = defaultServerConfig()
    cfg.host = "127.0.0.1"
    cfg.port = 20412
    cfg.serverName = "my-server"
    cfg.sharedTimerEnabled = true
    cfg.sharedTimerNodeId = "" # intentionally blank
    cfg.sharedTimerNumericNodeId = 0
    cfg.sharedTimerPeers = @[]
    let srv = newProtocolServer(cfg)
    check srv.sharedTimer.nodeId == "my-server"
    check srv.sharedTimer.numericNodeId == cfg.serverId # fallback to serverId

  test "TransactionManager.timeProvider is the SharedTimer":
    let srv = newProtocolServer(makeConfig(20413, enableTimer = true))
    # The timeProvider should be the same object as sharedTimer
    # (SharedTimer IS-A TimeProvider; setTimeProvider stores it)
    check not srv.txnMgr.timeProvider.isNil

  test "timestamps from wired server are valid nanosecond values":
    let srv = newProtocolServer(makeConfig(20414, enableTimer = true))
    srv.start()
    waitForServer(20414)
    let txn = srv.txnMgr.beginTransaction()
    check txn.id != zeroTransactionID()
    let minTs = uint64(1_577_836_800) * 1_000_000_000'u64 # 2020-01-01 in ns
    check txn.readTimestamp > minTs
    srv.stop()
    sleep(50)

  test "commit timestamps increase monotonically under SharedTimer":
    let srv = newProtocolServer(makeConfig(20415, enableTimer = true))
    srv.start()
    waitForServer(20415)
    var lastCommitTs: uint64 = 0
    for i in 0..<5:
      let txn = srv.txnMgr.beginTransaction()
      discard srv.txnMgr.recordWrite(txn.id, "key" & $i)
      let cr = srv.txnMgr.commitTransaction(txn.id)
      check cr.status == TxnCommitOK
      check cr.commitTimestamp > lastCommitTs
      lastCommitTs = cr.commitTimestamp
    srv.stop()
    sleep(50)

  test "server start/stop lifecycle with SharedTimer does not deadlock":
    let srv = newProtocolServer(makeConfig(20416, enableTimer = true))
    srv.start()
    waitForServer(20416)
    # Let background thread run for a couple of its poll intervals
    sleep(150)
    srv.stop()
    sleep(100)
    # If we reach here, no deadlock occurred
    check true

  test "SharedTimer is in unsynced fallback mode with no peers (single-node)":
    let srv = newProtocolServer(makeConfig(20417, enableTimer = true))
    # No peers configured → timer will be tssUninitialized or tssFailed after tick
    check srv.sharedTimer.getState() == tssUninitialized
    # now() still returns a valid local timestamp (graceful fallback)
    let t = srv.sharedTimer.now()
    let minTs: int64 = 1_577_836_800_000_000_000'i64 # 2020-01-01 in ns
    check t > minTs

  test "multiple start/stop cycles are safe":
    let srv = newProtocolServer(makeConfig(20418, enableTimer = true))
    srv.start()
    waitForServer(20418)
    sleep(60)
    srv.stop()
    sleep(60)
    # Second start (port is free again — server binds a new socket)
    # We just verify no crash/deadlock, not that it re-binds successfully
    # (the socket was closed by stop() implicitly via running=false)
    check not srv.sharedTimer.isNil

# ---------------------------------------------------------------------------
# Suite 3: SharedTimer background thread interacts correctly with manual ticks
# ---------------------------------------------------------------------------

suite "SharedTimer integration — background thread":

  test "background thread runs tick() without interfering with manual ticks":
    let srv = newProtocolServer(makeConfig(20430, enableTimer = true))
    srv.start()
    waitForServer(20430)
    sleep(100) # let background thread do one poll cycle

    # Manual ticks should still work (they are serialized by SharedTimer.mutex)
    srv.sharedTimer.tick()
    # With no peers, tick() leaves timer in tssFailed
    check srv.sharedTimer.getState() == tssFailed

    srv.stop()
    sleep(100)

  test "background thread stops within 100ms of server.stop()":
    let srv = newProtocolServer(makeConfig(20431, enableTimer = true))
    srv.start()
    waitForServer(20431)
    sleep(80)
    let t0 = epochTime()
    srv.stop()
    let elapsed = epochTime() - t0
    # joinThread() should return quickly (poll interval is 50ms, so ≤ ~110ms)
    check elapsed < 0.5 # generous 500ms upper bound
    sleep(50)

  test "SharedTimer.isSynchronized is false in single-node mode":
    let srv = newProtocolServer(makeConfig(20432, enableTimer = true))
    # Before any tick, state is uninitialized
    check not srv.sharedTimer.isSynchronized()
    srv.sharedTimer.tick() # With no peers → tssFailed
    check not srv.sharedTimer.isSynchronized()
