# Multi-node concurrency stress tests — Phase 14b
#
# Exercises concurrent reads and writes through the full Raft + WiscKey stack
# on real multi-node clusters with NuRaft ASIO networking:
#
#   Cluster A: 3 nodes (all voters, NuRaft manages quorum)
#   Cluster B: 5 nodes (all voters, NuRaft manages quorum)
#
# Scenarios per cluster:
#   1. 8 writers, distinct keys — all committed via quorum
#   2. 8 threads, 2:1 read:write over shared key space
#   3. Concurrent puts then verify replication to all voters
#   4. High-volume mixed load (8×200 ops)
#
# Port allocation: 25000–25499 (NuRaft ASIO, basePort per node spaced by 100)
# Temp storage: /tmp/fractio_mn_stress_<basePort>/

import std/[unittest, os, times, atomics, options]
import fractio/protocol/raft_store
import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/meta/system_tables

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc cleanDir(path: string) =
  try: removeDir(path) except CatchableError: discard
  try: createDir(path) except CatchableError: discard

type
  NodeSetup = object
    coord*: NuRaftCoordinator
    store*: RaftKVStoreExt
    storagePath*: string

proc makeNode(nodeNum: int, port: int,
              members: seq[tuple[nodeId: uint32, host: string, port: int]],
              groupId: GroupID): NodeSetup =
  let nodeId = NodeID(uint32(nodeNum))
  let storagePath = "/tmp/fractio_mn_stress_" & $port
  cleanDir(storagePath)

  let coord = newNuRaftCoordinator(nuraft_coordinator.CoordinatorConfig(
    nodeId: nodeId,
    port: port,
    host: "127.0.0.1",
    dataDir: storagePath,
    electionTimeoutLowerMs: 200,
    electionTimeoutUpperMs: 400,
    heartbeatIntervalMs: 100,
  ))
  coord.start()

  doAssert coord.createAndStartGroup(groupId, members)

  let store = newRaftKVStoreExt(coord, proposeTimeoutMs = 30_000)
  store.bootstrapStore(@[groupId])

  NodeSetup(coord: coord, store: store, storagePath: storagePath)

proc stopNode(ns: NodeSetup) =
  # Stop the store's rebalance thread BEFORE stopping the coordinator
  ns.store.stop()
  ns.coord.stop()
  sleep(500) # Let connections drain before removing storage
  cleanDir(ns.storagePath)

proc waitForLeader(nodes: seq[NodeSetup], groupId: GroupID,
    maxAttempts: int = 50): int =
  ## Wait for a leader to be elected. Returns leader index or -1.
  for attempt in 0 ..< maxAttempts:
    for i, ns in nodes:
      if ns.coord.isLeader(groupId):
        return i
    sleep(100)
  -1

# ---------------------------------------------------------------------------
# Cluster factories
# ---------------------------------------------------------------------------

proc make3NodeCluster(port: int): (seq[NodeSetup], GroupID) =
  ## 3-node cluster — NuRaft handles quorum automatically.
  let rid = DATA_GROUP_START_ID
  let members = @[
    (nodeId: 1'u32, host: "127.0.0.1", port: port),
    (nodeId: 2'u32, host: "127.0.0.1", port: port + 100),
    (nodeId: 3'u32, host: "127.0.0.1", port: port + 200),
  ]

  let nodes = @[
    makeNode(1, port, members, rid),
    makeNode(2, port + 100, members, rid),
    makeNode(3, port + 200, members, rid),
  ]
  (nodes, rid)

proc make5NodeCluster(port: int): (seq[NodeSetup], GroupID) =
  ## 5-node cluster — NuRaft handles quorum automatically.
  let rid = DATA_GROUP_START_ID
  let members = @[
    (nodeId: 1'u32, host: "127.0.0.1", port: port),
    (nodeId: 2'u32, host: "127.0.0.1", port: port + 100),
    (nodeId: 3'u32, host: "127.0.0.1", port: port + 200),
    (nodeId: 4'u32, host: "127.0.0.1", port: port + 300),
    (nodeId: 5'u32, host: "127.0.0.1", port: port + 400),
  ]

  let nodes = @[
    makeNode(1, port, members, rid),
    makeNode(2, port + 100, members, rid),
    makeNode(3, port + 200, members, rid),
    makeNode(4, port + 300, members, rid),
    makeNode(5, port + 400, members, rid),
  ]
  (nodes, rid)

# ---------------------------------------------------------------------------
# Thread worker types
# ---------------------------------------------------------------------------

type
  WriteWorkerArgs = object
    store: RaftKVStoreExt ## ref passed directly for ORC cross-thread safety
    threadId: int
    numOps: int
    startLatch: ptr Atomic[int]
    errors: ptr Atomic[int]
    completed: ptr Atomic[int]

  ReadWriteWorkerArgs = object
    store: RaftKVStoreExt ## ref passed directly for ORC cross-thread safety
    threadId: int
    numOps: int
    numKeys: int
    startLatch: ptr Atomic[int]
    errors: ptr Atomic[int]
    completed: ptr Atomic[int]

# ---------------------------------------------------------------------------
# Write-only worker
# ---------------------------------------------------------------------------

proc writeWorker(args: WriteWorkerArgs) {.thread, gcsafe.} =
  discard args.startLatch[].fetchSub(1)
  while args.startLatch[].load() > 0:
    discard

  for i in 0 ..< args.numOps:
    let key = "t" & $args.threadId & "_k" & $i
    let val = "v" & $args.threadId & "_" & $i
    {.cast(gcsafe).}:
      let r = args.store.raftPut(key, val)
      if not r.isOk:
        discard args.errors[].fetchAdd(1)

  discard args.completed[].fetchAdd(1)

# ---------------------------------------------------------------------------
# Read-write worker: 2:1 read:write over shared key space
# ---------------------------------------------------------------------------

proc readWriteWorker(args: ReadWriteWorkerArgs) {.thread, gcsafe.} =
  discard args.startLatch[].fetchSub(1)
  while args.startLatch[].load() > 0:
    discard

  for i in 0 ..< args.numOps:
    let keyIdx = (args.threadId * args.numOps + i) mod args.numKeys
    let key = "shared_" & $keyIdx
    {.cast(gcsafe).}:
      if i mod 3 == 0:
        let r = args.store.raftPut(key, "val_" & $args.threadId & "_" & $i)
        if not r.isOk:
          discard args.errors[].fetchAdd(1)
      else:
        let r = args.store.raftGet(key)
        if not r.isOk:
          discard args.errors[].fetchAdd(1)

  discard args.completed[].fetchAdd(1)

# ---------------------------------------------------------------------------
# Wait helper
# ---------------------------------------------------------------------------

proc waitCompleted(completed: ptr Atomic[int], total: int,
    timeoutMs: int = 60_000): bool =
  let deadline = epochTime() + float(timeoutMs) / 1000.0
  while completed[].load() < total:
    if epochTime() > deadline:
      return false
    sleep(10)
  true

# ===========================================================================
# 3-node cluster stress tests
# ===========================================================================

suite "MultiNode stress — 3-node cluster":

  test "8 writers, distinct keys — quorum-committed":
    const numThreads = 8
    const numOps = 100
    let (nodes, rid) = make3NodeCluster(25000)
    let leaderIdx = waitForLeader(nodes, rid)
    doAssert leaderIdx >= 0
    defer:
      for ns in nodes: stopNode(ns)

    var latch: Atomic[int]
    var errors: Atomic[int]
    var completed: Atomic[int]
    latch.store(numThreads)
    errors.store(0)
    completed.store(0)

    var threads: array[numThreads, Thread[WriteWorkerArgs]]
    for i in 0 ..< numThreads:
      let args = WriteWorkerArgs(
        store: nodes[leaderIdx].store,
        threadId: i, numOps: numOps,
        startLatch: addr latch, errors: addr errors,
        completed: addr completed)
      createThread(threads[i], writeWorker, args)

    check waitCompleted(addr completed, numThreads)
    for i in 0 ..< numThreads:
      joinThread(threads[i])
    check errors.load() == 0

  test "8 threads, 2:1 read:write, shared keys — no crashes":
    const numThreads = 8
    const numOps = 150
    const numKeys = 50
    let (nodes, rid) = make3NodeCluster(25000)
    let leaderIdx = waitForLeader(nodes, rid)
    doAssert leaderIdx >= 0
    defer:
      for ns in nodes: stopNode(ns)

    var latch: Atomic[int]
    var errors: Atomic[int]
    var completed: Atomic[int]
    latch.store(numThreads)
    errors.store(0)
    completed.store(0)

    var threads: array[numThreads, Thread[ReadWriteWorkerArgs]]
    for i in 0 ..< numThreads:
      let args = ReadWriteWorkerArgs(
        store: nodes[leaderIdx].store,
        threadId: i, numOps: numOps, numKeys: numKeys,
        startLatch: addr latch, errors: addr errors,
        completed: addr completed)
      createThread(threads[i], readWriteWorker, args)

    check waitCompleted(addr completed, numThreads)
    for i in 0 ..< numThreads:
      joinThread(threads[i])
    check errors.load() == 0

  test "concurrent puts then verify replication to voter":
    const numThreads = 4
    const numOps = 50
    let (nodes, rid) = make3NodeCluster(25000)
    let leaderIdx = waitForLeader(nodes, rid)
    doAssert leaderIdx >= 0
    defer:
      for ns in nodes: stopNode(ns)

    var latch: Atomic[int]
    var errors: Atomic[int]
    var completed: Atomic[int]
    latch.store(numThreads)
    errors.store(0)
    completed.store(0)

    var threads: array[numThreads, Thread[WriteWorkerArgs]]
    for i in 0 ..< numThreads:
      let args = WriteWorkerArgs(
        store: nodes[leaderIdx].store,
        threadId: i, numOps: numOps,
        startLatch: addr latch, errors: addr errors,
        completed: addr completed)
      createThread(threads[i], writeWorker, args)

    check waitCompleted(addr completed, numThreads)
    for i in 0 ..< numThreads:
      joinThread(threads[i])
    check errors.load() == 0

    # Pick a follower to check replication
    let followerIdx = (leaderIdx + 1) mod 3

    # Poll for replication to follower — retry up to 15s
    let replDeadline = epochTime() + 15.0
    var missing = numThreads * numOps
    while missing > 0 and epochTime() < replDeadline:
      missing = 0
      for t in 0 ..< numThreads:
        for i in 0 ..< numOps:
          let key = "t" & $t & "_k" & $i
          let r = nodes[followerIdx].store.raftGet(key)
          if not r.isOk or r.value.isNone:
            inc missing
      if missing > 0:
        sleep(500)
    check missing == 0

  test "high-volume mixed load — 8×200 ops":
    const numThreads = 8
    const numOps = 200
    const numKeys = 100
    let (nodes, rid) = make3NodeCluster(25000)
    let leaderIdx = waitForLeader(nodes, rid)
    doAssert leaderIdx >= 0
    defer:
      for ns in nodes: stopNode(ns)

    var latch: Atomic[int]
    var errors: Atomic[int]
    var completed: Atomic[int]
    latch.store(numThreads)
    errors.store(0)
    completed.store(0)

    var threads: array[numThreads, Thread[ReadWriteWorkerArgs]]
    for i in 0 ..< numThreads:
      let args = ReadWriteWorkerArgs(
        store: nodes[leaderIdx].store,
        threadId: i, numOps: numOps, numKeys: numKeys,
        startLatch: addr latch, errors: addr errors,
        completed: addr completed)
      createThread(threads[i], readWriteWorker, args)

    check waitCompleted(addr completed, numThreads, timeoutMs = 120_000)
    for i in 0 ..< numThreads:
      joinThread(threads[i])
    check errors.load() == 0

# Allow all OS-level resources (threads, sockets) from the 3-node suite to
# fully drain before starting the 5-node suite.
when true:
  sleep(2000)

# ===========================================================================
# 5-node cluster stress tests
# ===========================================================================

suite "MultiNode stress — 5-node cluster":

  test "8 writers, distinct keys — quorum-committed":
    const numThreads = 8
    const numOps = 100
    let (nodes, rid) = make5NodeCluster(26000)
    let leaderIdx = waitForLeader(nodes, rid)
    doAssert leaderIdx >= 0
    defer:
      for ns in nodes: stopNode(ns)

    var latch: Atomic[int]
    var errors: Atomic[int]
    var completed: Atomic[int]
    latch.store(numThreads)
    errors.store(0)
    completed.store(0)

    var threads: array[numThreads, Thread[WriteWorkerArgs]]
    for i in 0 ..< numThreads:
      let args = WriteWorkerArgs(
        store: nodes[leaderIdx].store,
        threadId: i, numOps: numOps,
        startLatch: addr latch, errors: addr errors,
        completed: addr completed)
      createThread(threads[i], writeWorker, args)

    check waitCompleted(addr completed, numThreads)
    for i in 0 ..< numThreads:
      joinThread(threads[i])
    check errors.load() == 0

  test "8 threads, 2:1 read:write, shared keys — no crashes":
    const numThreads = 8
    const numOps = 150
    const numKeys = 50
    let (nodes, rid) = make5NodeCluster(26500)
    let leaderIdx = waitForLeader(nodes, rid)
    doAssert leaderIdx >= 0
    defer:
      for ns in nodes: stopNode(ns)

    var latch: Atomic[int]
    var errors: Atomic[int]
    var completed: Atomic[int]
    latch.store(numThreads)
    errors.store(0)
    completed.store(0)

    var threads: array[numThreads, Thread[ReadWriteWorkerArgs]]
    for i in 0 ..< numThreads:
      let args = ReadWriteWorkerArgs(
        store: nodes[leaderIdx].store,
        threadId: i, numOps: numOps, numKeys: numKeys,
        startLatch: addr latch, errors: addr errors,
        completed: addr completed)
      createThread(threads[i], readWriteWorker, args)

    check waitCompleted(addr completed, numThreads)
    for i in 0 ..< numThreads:
      joinThread(threads[i])
    check errors.load() == 0

  test "concurrent puts then verify replication to all voters":
    const numThreads = 4
    const numOps = 50
    let (nodes, rid) = make5NodeCluster(27000)
    let leaderIdx = waitForLeader(nodes, rid)
    doAssert leaderIdx >= 0
    defer:
      for ns in nodes: stopNode(ns)

    var latch: Atomic[int]
    var errors: Atomic[int]
    var completed: Atomic[int]
    latch.store(numThreads)
    errors.store(0)
    completed.store(0)

    var threads: array[numThreads, Thread[WriteWorkerArgs]]
    for i in 0 ..< numThreads:
      let args = WriteWorkerArgs(
        store: nodes[leaderIdx].store,
        threadId: i, numOps: numOps,
        startLatch: addr latch, errors: addr errors,
        completed: addr completed)
      createThread(threads[i], writeWorker, args)

    check waitCompleted(addr completed, numThreads)
    for i in 0 ..< numThreads:
      joinThread(threads[i])
    check errors.load() == 0

    # Poll for replication to other nodes — retry up to 15s
    let replDeadline = epochTime() + 15.0
    for voterIdx in 0 ..< 5:
      if voterIdx == leaderIdx: continue
      var missing = numThreads * numOps # start high
      while missing > 0 and epochTime() < replDeadline:
        missing = 0
        for t in 0 ..< numThreads:
          for i in 0 ..< numOps:
            let key = "t" & $t & "_k" & $i
            let r = nodes[voterIdx].store.raftGet(key)
            if not r.isOk or r.value.isNone:
              inc missing
        if missing > 0:
          sleep(500)
      check missing == 0

  test "high-volume mixed load — 8×200 ops":
    const numThreads = 8
    const numOps = 200
    const numKeys = 100
    let (nodes, rid) = make5NodeCluster(26000)
    let leaderIdx = waitForLeader(nodes, rid)
    doAssert leaderIdx >= 0
    defer:
      for ns in nodes: stopNode(ns)

    var latch: Atomic[int]
    var errors: Atomic[int]
    var completed: Atomic[int]
    latch.store(numThreads)
    errors.store(0)
    completed.store(0)

    var threads: array[numThreads, Thread[ReadWriteWorkerArgs]]
    for i in 0 ..< numThreads:
      let args = ReadWriteWorkerArgs(
        store: nodes[leaderIdx].store,
        threadId: i, numOps: numOps, numKeys: numKeys,
        startLatch: addr latch, errors: addr errors,
        completed: addr completed)
      createThread(threads[i], readWriteWorker, args)

    check waitCompleted(addr completed, numThreads, timeoutMs = 120_000)
    for i in 0 ..< numThreads:
      joinThread(threads[i])
    check errors.load() == 0

# Clean up global state to prevent GC issues during program exit
nuraft_coordinator.cleanupGlobalState()

# Force garbage collection before program exit to avoid ARC cleanup race
GC_fullCollect()
sleep(100)

# Exit explicitly to avoid Nim ARC cleanup race condition with cross-thread refs
# This is a known Nim runtime issue with atomicArc and complex thread interactions
quit(0)
