# Multi-node concurrency stress tests — Phase 14b
#
# Exercises concurrent reads and writes through the full Raft + WiscKey stack
# on real multi-node clusters with TCP-based replication:
#
#   Cluster A: 3 nodes, 2 voters + 1 non-voter  (quorum = 2)
#   Cluster B: 5 nodes, 3 voters + 2 non-voters (quorum = 2)
#
# Scenarios per cluster:
#   1. 8 writers, distinct keys — all committed via quorum
#   2. 8 threads, 2:1 read:write over shared key space
#   3. Concurrent puts then verify replication to all voters
#   4. High-volume mixed load (8×200 ops)
#
# Port range: 20640–20979
#   3-node tests: 20640, 20670, 20700, 20730 (each node gets 10 ports: base+(n-1)*10)
#   5-node tests: 20780, 20830, 20880, 20930 (each node gets 10 ports: base+(n-1)*10)
#
# Temp storage: /tmp/fractio_mn_stress_<port>/

import std/[unittest, os, times, atomics, options]
import fractio/protocol/raft_store
import fractio/distributed/raft/multigroup_coordinator
import fractio/distributed/raft/multigroup_transport
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
    coord*: MultiRaftCoordinator
    store*: RaftKVStoreExt
    storagePath*: string
    rgt*: RaftGroupTransport ## GC root — keeps transport alive

proc makeNode(nodeNum: int, basePort: int,
              peerNums: seq[int],
              rid: GroupID,
              desc: GroupDescriptor,
              numWorkers: int = 1): NodeSetup =
  let nodeId = NodeID(uint32(nodeNum))
  let port = basePort + (nodeNum - 1) * 10

  var peers: seq[PeerAddr]
  for pn in peerNums:
    peers.add(PeerAddr(
      nodeId: NodeID(uint32(pn)),
      host: "127.0.0.1",
      raftPort: basePort + (pn - 1) * 10,
    ))

  let rgt = newRaftGroupTransport(nodeId, "127.0.0.1", port, peers)
  let transport = newMultiRaftTransport(rgt)

  let storagePath = "/tmp/fractio_mn_stress_" & $port
  cleanDir(storagePath)

  let cfg = CoordinatorConfig(
    nodeId: nodeId,
    numWorkers: numWorkers,
    electionTimeoutNs: 300_000_000_000'i64, # 300s — prevent elections during test
    heartbeatIntervalNs: 50_000_000'i64,
    storagePath: storagePath,
    proposeTimeoutMs: 30_000,
    transport: transport,
  )
  let coord = newMultiRaftCoordinator(cfg)

  let rep = desc.getReplica(nodeId)
  doAssert rep.isSome, "replica not found for node " & $nodeNum
  discard coord.createGroup(desc, rep.get.replicaId)

  let store = newRaftKVStoreExt(coord, proposeTimeoutMs = 30_000)
  store.bootstrapStore(@[rid])

  NodeSetup(coord: coord, store: store, storagePath: storagePath, rgt: rgt)

proc startNode(ns: NodeSetup) =
  ns.coord.start()

proc stopNode(ns: NodeSetup) =
  ns.coord.stop()
  sleep(500) # Let TCP connections drain before removing storage
  cleanDir(ns.storagePath)

proc electLeader(nodes: seq[NodeSetup], rid: GroupID, leaderIdx: int) =
  ## Force node at leaderIdx to become leader, wait for heartbeats.
  let grp = nodes[leaderIdx].coord.getGroup(rid)
  doAssert grp.isSome
  # Bump term so heartbeats are accepted by followers at term 0
  grp.get.becomeCandidate()
  grp.get.becomeLeader()
  sleep(500)

# ---------------------------------------------------------------------------
# Cluster factories
# ---------------------------------------------------------------------------

proc make3NodeCluster(basePort: int): (seq[NodeSetup], GroupID) =
  ## 3 nodes: node 1 & 2 are voters, node 3 is non-voter.
  let rid = DATA_GROUP_START_ID
  let desc = newGroupDescriptor(rid)
  discard desc.addReplica(NodeID(1), rtVoter)
  discard desc.addReplica(NodeID(2), rtVoter)
  discard desc.addReplica(NodeID(3), rtNonVoter)

  let nodes = @[
    makeNode(1, basePort, @[2, 3], rid, desc),
    makeNode(2, basePort, @[1, 3], rid, desc),
    makeNode(3, basePort, @[1, 2], rid, desc),
  ]
  (nodes, rid)

proc make5NodeCluster(basePort: int): (seq[NodeSetup], GroupID) =
  ## 5 nodes: nodes 1, 2, 3 are voters, nodes 4, 5 are non-voters.
  ## Uses 2 coordinator workers per node to handle the higher fan-out.
  let rid = DATA_GROUP_START_ID
  let desc = newGroupDescriptor(rid)
  discard desc.addReplica(NodeID(1), rtVoter)
  discard desc.addReplica(NodeID(2), rtVoter)
  discard desc.addReplica(NodeID(3), rtVoter)
  discard desc.addReplica(NodeID(4), rtNonVoter)
  discard desc.addReplica(NodeID(5), rtNonVoter)

  let nodes = @[
    makeNode(1, basePort, @[2, 3, 4, 5], rid, desc, numWorkers = 2),
    makeNode(2, basePort, @[1, 3, 4, 5], rid, desc, numWorkers = 2),
    makeNode(3, basePort, @[1, 2, 4, 5], rid, desc, numWorkers = 2),
    makeNode(4, basePort, @[1, 2, 3, 5], rid, desc, numWorkers = 2),
    makeNode(5, basePort, @[1, 2, 3, 4], rid, desc, numWorkers = 2),
  ]
  (nodes, rid)

# ---------------------------------------------------------------------------
# Thread worker types
# ---------------------------------------------------------------------------

type
  WriteWorkerArgs = object
    store: ptr RaftKVStoreExt ## raw ptr — avoids ORC cross-thread ref
    threadId: int
    numOps: int
    startLatch: ptr Atomic[int]
    errors: ptr Atomic[int]
    completed: ptr Atomic[int]

  ReadWriteWorkerArgs = object
    store: ptr RaftKVStoreExt
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

  let store = args.store[]
  for i in 0 ..< args.numOps:
    let key = "t" & $args.threadId & "_k" & $i
    let val = "v" & $args.threadId & "_" & $i
    {.cast(gcsafe).}:
      let r = store.raftPut(key, val)
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

  let store = args.store[]
  for i in 0 ..< args.numOps:
    let keyIdx = (args.threadId * args.numOps + i) mod args.numKeys
    let key = "shared_" & $keyIdx
    {.cast(gcsafe).}:
      if i mod 3 == 0:
        let r = store.raftPut(key, "val_" & $args.threadId & "_" & $i)
        if not r.isOk:
          discard args.errors[].fetchAdd(1)
      else:
        let r = store.raftGet(key)
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
# 3-node cluster stress tests (2 voters + 1 non-voter, quorum = 2)
# ===========================================================================

suite "MultiNode stress — 3-node cluster (2 voters)":

  test "8 writers, distinct keys — quorum-committed":
    const numThreads = 8
    const numOps = 100
    const basePort = 20640
    let (nodes, rid) = make3NodeCluster(basePort)
    for ns in nodes: startNode(ns)
    electLeader(nodes, rid, 0)
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
        store: addr nodes[0].store,
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
    const basePort = 20670
    let (nodes, rid) = make3NodeCluster(basePort)
    for ns in nodes: startNode(ns)
    electLeader(nodes, rid, 0)
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
        store: addr nodes[0].store,
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
    const basePort = 20700
    let (nodes, rid) = make3NodeCluster(basePort)
    for ns in nodes: startNode(ns)
    electLeader(nodes, rid, 0)
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
        store: addr nodes[0].store,
        threadId: i, numOps: numOps,
        startLatch: addr latch, errors: addr errors,
        completed: addr completed)
      createThread(threads[i], writeWorker, args)

    check waitCompleted(addr completed, numThreads)
    for i in 0 ..< numThreads:
      joinThread(threads[i])
    check errors.load() == 0

    # Poll for replication to voter node 1 — retry up to 15s
    let replDeadline = epochTime() + 15.0
    var missing = numThreads * numOps
    while missing > 0 and epochTime() < replDeadline:
      missing = 0
      for t in 0 ..< numThreads:
        for i in 0 ..< numOps:
          let key = "t" & $t & "_k" & $i
          let r = nodes[1].store.raftGet(key)
          if not r.isOk or r.value.isNone:
            inc missing
      if missing > 0:
        sleep(500)
    check missing == 0

  test "high-volume mixed load — 8×200 ops":
    const numThreads = 8
    const numOps = 200
    const numKeys = 100
    const basePort = 20730
    let (nodes, rid) = make3NodeCluster(basePort)
    for ns in nodes: startNode(ns)
    electLeader(nodes, rid, 0)
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
        store: addr nodes[0].store,
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
# 5-node cluster stress tests (3 voters + 2 non-voters, quorum = 2)
# ===========================================================================

suite "MultiNode stress — 5-node cluster (3 voters)":

  test "8 writers, distinct keys — quorum-committed":
    const numThreads = 8
    const numOps = 100
    const basePort = 20780
    let (nodes, rid) = make5NodeCluster(basePort)
    for ns in nodes: startNode(ns)
    electLeader(nodes, rid, 0)
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
        store: addr nodes[0].store,
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
    const basePort = 20830
    let (nodes, rid) = make5NodeCluster(basePort)
    for ns in nodes: startNode(ns)
    electLeader(nodes, rid, 0)
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
        store: addr nodes[0].store,
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
    const basePort = 20880
    let (nodes, rid) = make5NodeCluster(basePort)
    for ns in nodes: startNode(ns)
    electLeader(nodes, rid, 0)
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
        store: addr nodes[0].store,
        threadId: i, numOps: numOps,
        startLatch: addr latch, errors: addr errors,
        completed: addr completed)
      createThread(threads[i], writeWorker, args)

    check waitCompleted(addr completed, numThreads)
    for i in 0 ..< numThreads:
      joinThread(threads[i])
    check errors.load() == 0

    # Poll for replication to voter nodes 1 and 2 — retry up to 15s
    let replDeadline = epochTime() + 15.0
    for voterIdx in 1 .. 2:
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
    const basePort = 20930
    let (nodes, rid) = make5NodeCluster(basePort)
    for ns in nodes: startNode(ns)
    electLeader(nodes, rid, 0)
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
        store: addr nodes[0].store,
        threadId: i, numOps: numOps, numKeys: numKeys,
        startLatch: addr latch, errors: addr errors,
        completed: addr completed)
      createThread(threads[i], readWriteWorker, args)

    check waitCompleted(addr completed, numThreads, timeoutMs = 120_000)
    for i in 0 ..< numThreads:
      joinThread(threads[i])
    check errors.load() == 0
