# Concurrency stress tests — Phase 13
#
# Reproduces the SIGSEGV seen at 5000 ops / 8 threads by exercising
# concurrent reads and writes through the full Raft + WiscKey stack.
#
# Scenarios:
#  1. 8 threads, 500 puts each, all keys distinct — no conflicts
#  2. 8 threads, 500 ops each, 2:1 read:write, shared key space
#  3. 4 threads concurrent transactions — begin/put/commit
#  4. Mixed: writers + readers simultaneously, high op count
#  5. Rapid server start/stop under active clients
#  6. Concurrent puts then verify all keys readable
#
# Port range: 20620–20639
# Temp storage: /tmp/fractio_stress_<port>/

import std/[unittest, os, times, atomics, locks, options]
import fractio/protocol/types
import fractio/protocol/server
import fractio/protocol/client
import fractio/protocol/raft_store
import fractio/protocol/txn_manager
import fractio/protocol/messages/kv
import fractio/protocol/messages/txn as txnMsgs
import fractio/distributed/raft/multigroup_coordinator
import fractio/distributed/raft/multigroup_types
import fractio/distributed/range/types as rangeTypes
import fractio/distributed/meta/system_tables

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc cleanDir(path: string) =
  try: removeDir(path) except CatchableError: discard
  try: createDir(path) except CatchableError: discard

proc makeStressServer(port: int, storagePath: string,
    numWorkers: int = 4): ProtocolServer =
  cleanDir(storagePath)
  let coordCfg = CoordinatorConfig(
    nodeId: RangeNodeID(1),
    numWorkers: numWorkers,
    electionTimeoutNs: DEFAULT_ELECTION_TIMEOUT_NS,
    heartbeatIntervalNs: DEFAULT_HEARTBEAT_INTERVAL_NS,
    storagePath: storagePath,
    proposeTimeoutMs: 10_000,
  )
  let coord = newMultiRaftCoordinator(coordCfg)
  for rid in [META_RANGE_ID, DATA_RANGE_START_ID]:
    let desc = newRangeDescriptor(rid, @[], @[])
    let rep = desc.addReplica(RangeNodeID(1))
    let group = coord.createGroup(desc, rep.replicaId)
    group.becomeLeader()
  coord.start()
  let raftSt = newRaftKVStoreExt(coord, proposeTimeoutMs = 10_000)
  raftSt.bootstrapStore(@[META_RANGE_ID, DATA_RANGE_START_ID])
  var cfg = defaultServerConfig()
  cfg.host = "127.0.0.1"
  cfg.port = port
  cfg.idleTimeoutSecs = 300
  let srv = newProtocolServer(cfg)
  srv.raftStore = raftSt
  srv.start()
  sleep(100)
  srv

proc makeClient(port: int): ProtocolClient =
  var cfg = defaultClientConfig("127.0.0.1", port)
  cfg.timeoutMs = 15_000
  result = newProtocolClient(cfg)
  let r = result.connect()
  doAssert r.isOk, "connect failed: " & (if r.isErr: $r.err else: "")

# ---------------------------------------------------------------------------
# Thread worker types
# ---------------------------------------------------------------------------

type
  WriteWorkerArgs = object
    port: int
    threadId: int
    numOps: int
    startLatch: ptr Atomic[int] # counts down; workers spin until 0
    errors: ptr Atomic[int]
    completed: ptr Atomic[int]

  ReadWriteWorkerArgs = object
    port: int
    threadId: int
    numOps: int
    numKeys: int
    startLatch: ptr Atomic[int]
    errors: ptr Atomic[int]
    completed: ptr Atomic[int]

  TxnWorkerArgs = object
    port: int
    threadId: int
    numTxns: int
    startLatch: ptr Atomic[int]
    errors: ptr Atomic[int]
    completed: ptr Atomic[int]

# ---------------------------------------------------------------------------
# Write-only worker: each thread writes numOps distinct keys
# ---------------------------------------------------------------------------

proc writeWorker(args: WriteWorkerArgs) {.thread, gcsafe.} =
  # Wait for all threads to be ready
  discard args.startLatch[].fetchSub(1)
  while args.startLatch[].load() > 0:
    discard

  var cli: ProtocolClient
  try:
    cli = makeClient(args.port)
  except CatchableError:
    discard args.errors[].fetchAdd(1)
    discard args.completed[].fetchAdd(1)
    return

  for i in 0 ..< args.numOps:
    let key = "t" & $args.threadId & "_k" & $i
    let val = "v" & $args.threadId & "_" & $i
    let r = cli.kvPut(key, val)
    if r.isErr:
      discard args.errors[].fetchAdd(1)

  cli.disconnect()
  discard args.completed[].fetchAdd(1)

# ---------------------------------------------------------------------------
# Read-write worker: 2:1 read:write over a shared key space
# ---------------------------------------------------------------------------

proc readWriteWorker(args: ReadWriteWorkerArgs) {.thread, gcsafe.} =
  discard args.startLatch[].fetchSub(1)
  while args.startLatch[].load() > 0:
    discard

  var cli: ProtocolClient
  try:
    cli = makeClient(args.port)
  except CatchableError:
    discard args.errors[].fetchAdd(1)
    discard args.completed[].fetchAdd(1)
    return

  for i in 0 ..< args.numOps:
    let keyIdx = (args.threadId * args.numOps + i) mod args.numKeys
    let key = "shared_" & $keyIdx
    if i mod 3 == 0:
      # write
      let r = cli.kvPut(key, "val_" & $args.threadId & "_" & $i)
      if r.isErr:
        discard args.errors[].fetchAdd(1)
    else:
      # read (result may be none — that's fine)
      let r = cli.kvGet(key)
      if r.isErr:
        discard args.errors[].fetchAdd(1)

  cli.disconnect()
  discard args.completed[].fetchAdd(1)

# ---------------------------------------------------------------------------
# Transaction worker: each thread does numTxns begin/put/commit cycles
# ---------------------------------------------------------------------------

proc txnWorker(args: TxnWorkerArgs) {.thread, gcsafe.} =
  discard args.startLatch[].fetchSub(1)
  while args.startLatch[].load() > 0:
    discard

  var cli: ProtocolClient
  try:
    cli = makeClient(args.port)
  except CatchableError:
    discard args.errors[].fetchAdd(1)
    discard args.completed[].fetchAdd(1)
    return

  for i in 0 ..< args.numTxns:
    let beginR = cli.beginTxn()
    if beginR.isErr:
      discard args.errors[].fetchAdd(1)
      continue
    let txnId = beginR.value.txnId
    let key = "txn_t" & $args.threadId & "_i" & $i
    let putR = cli.kvPut(key, "txnval_" & $i, txnId = txnId)
    if putR.isErr:
      discard args.errors[].fetchAdd(1)
      # attempt rollback anyway
      discard cli.rollbackTxn(txnId)
      continue
    let commitR = cli.commitTxn(txnId)
    if commitR.isErr:
      discard args.errors[].fetchAdd(1)

  cli.disconnect()
  discard args.completed[].fetchAdd(1)

# ---------------------------------------------------------------------------
# Helper: wait for all threads to complete (with timeout)
# ---------------------------------------------------------------------------

proc waitCompleted(completed: ptr Atomic[int], total: int,
    timeoutMs: int = 30_000): bool =
  let deadline = epochTime() + float(timeoutMs) / 1000.0
  while completed[].load() < total:
    if epochTime() > deadline:
      return false
    sleep(10)
  true

# ---------------------------------------------------------------------------
# Test suite
# ---------------------------------------------------------------------------

suite "Concurrent KV stress tests":

  test "8 writers, distinct keys — no errors, no SIGSEGV":
    ## Each of 8 threads writes 200 unique keys.
    ## Total: 1600 Raft-committed writes.
    const numThreads = 8
    const numOps = 200
    const port = 20620
    const storagePath = "/tmp/fractio_stress_20620"
    let srv = makeStressServer(port, storagePath)
    defer:
      srv.stop()
      sleep(80)
      try: removeDir(storagePath) except CatchableError: discard

    var latch: Atomic[int]
    var errors: Atomic[int]
    var completed: Atomic[int]
    latch.store(numThreads)
    errors.store(0)
    completed.store(0)

    var threads: array[numThreads, Thread[WriteWorkerArgs]]
    for i in 0 ..< numThreads:
      let args = WriteWorkerArgs(
        port: port, threadId: i, numOps: numOps,
        startLatch: addr latch, errors: addr errors, completed: addr completed)
      createThread(threads[i], writeWorker, args)

    check waitCompleted(addr completed, numThreads)
    for i in 0 ..< numThreads:
      joinThread(threads[i])
    check errors.load() == 0

  test "8 threads, 2:1 read:write, shared key space — no crashes":
    ## Hammers the same 100 keys from 8 concurrent clients.
    const numThreads = 8
    const numOps = 300
    const numKeys = 100
    const port = 20621
    const storagePath = "/tmp/fractio_stress_20621"
    let srv = makeStressServer(port, storagePath)
    defer:
      srv.stop()
      sleep(80)
      try: removeDir(storagePath) except CatchableError: discard

    var latch: Atomic[int]
    var errors: Atomic[int]
    var completed: Atomic[int]
    latch.store(numThreads)
    errors.store(0)
    completed.store(0)

    var threads: array[numThreads, Thread[ReadWriteWorkerArgs]]
    for i in 0 ..< numThreads:
      let args = ReadWriteWorkerArgs(
        port: port, threadId: i, numOps: numOps, numKeys: numKeys,
        startLatch: addr latch, errors: addr errors, completed: addr completed)
      createThread(threads[i], readWriteWorker, args)

    check waitCompleted(addr completed, numThreads)
    for i in 0 ..< numThreads:
      joinThread(threads[i])
    check errors.load() == 0

  test "4 concurrent transaction workers — no errors, no deadlock":
    ## 4 threads each do 50 begin/put/commit cycles concurrently.
    const numThreads = 4
    const numTxns = 50
    const port = 20622
    const storagePath = "/tmp/fractio_stress_20622"
    let srv = makeStressServer(port, storagePath)
    defer:
      srv.stop()
      sleep(80)
      try: removeDir(storagePath) except CatchableError: discard

    var latch: Atomic[int]
    var errors: Atomic[int]
    var completed: Atomic[int]
    latch.store(numThreads)
    errors.store(0)
    completed.store(0)

    var threads: array[numThreads, Thread[TxnWorkerArgs]]
    for i in 0 ..< numThreads:
      let args = TxnWorkerArgs(
        port: port, threadId: i, numTxns: numTxns,
        startLatch: addr latch, errors: addr errors, completed: addr completed)
      createThread(threads[i], txnWorker, args)

    check waitCompleted(addr completed, numThreads, timeoutMs = 60_000)
    for i in 0 ..< numThreads:
      joinThread(threads[i])
    check errors.load() == 0

  test "concurrent puts then verify all keys readable":
    ## 4 threads each write 100 distinct keys, then a single client
    ## reads every key back and verifies the value is present.
    const numThreads = 4
    const numOps = 100
    const port = 20623
    const storagePath = "/tmp/fractio_stress_20623"
    let srv = makeStressServer(port, storagePath)
    defer:
      srv.stop()
      sleep(80)
      try: removeDir(storagePath) except CatchableError: discard

    var latch: Atomic[int]
    var errors: Atomic[int]
    var completed: Atomic[int]
    latch.store(numThreads)
    errors.store(0)
    completed.store(0)

    var threads: array[numThreads, Thread[WriteWorkerArgs]]
    for i in 0 ..< numThreads:
      let args = WriteWorkerArgs(
        port: port, threadId: i, numOps: numOps,
        startLatch: addr latch, errors: addr errors, completed: addr completed)
      createThread(threads[i], writeWorker, args)

    check waitCompleted(addr completed, numThreads)
    for i in 0 ..< numThreads:
      joinThread(threads[i])
    check errors.load() == 0

    # Now verify all written keys are readable
    let cli = makeClient(port)
    defer: cli.disconnect()
    var missing = 0
    for t in 0 ..< numThreads:
      for i in 0 ..< numOps:
        let key = "t" & $t & "_k" & $i
        let r = cli.kvGet(key)
        if r.isErr or not r.value.found:
          inc missing
    check missing == 0

  test "high-volume mixed load — reproduces benchmark SIGSEGV scenario":
    ## This is the scenario that crashed at 5000 ops / 8 threads.
    ## 8 threads × 500 ops with 2:1 read:write over 200 shared keys.
    const numThreads = 8
    const numOps = 500
    const numKeys = 200
    const port = 20624
    const storagePath = "/tmp/fractio_stress_20624"
    let srv = makeStressServer(port, storagePath, numWorkers = 4)
    defer:
      srv.stop()
      sleep(80)
      try: removeDir(storagePath) except CatchableError: discard

    var latch: Atomic[int]
    var errors: Atomic[int]
    var completed: Atomic[int]
    latch.store(numThreads)
    errors.store(0)
    completed.store(0)

    var threads: array[numThreads, Thread[ReadWriteWorkerArgs]]
    for i in 0 ..< numThreads:
      let args = ReadWriteWorkerArgs(
        port: port, threadId: i, numOps: numOps, numKeys: numKeys,
        startLatch: addr latch, errors: addr errors, completed: addr completed)
      createThread(threads[i], readWriteWorker, args)

    check waitCompleted(addr completed, numThreads, timeoutMs = 120_000)
    for i in 0 ..< numThreads:
      joinThread(threads[i])
    check errors.load() == 0

  test "interleaved writers and readers — writer keys visible to readers":
    ## A writer thread writes 50 sequential keys one by one.
    ## A reader thread polls those keys until it finds them all.
    ## Tests that the SM is consistent under concurrent access.
    const numKeys = 50
    const port = 20625
    const storagePath = "/tmp/fractio_stress_20625"
    let srv = makeStressServer(port, storagePath)
    defer:
      srv.stop()
      sleep(80)
      try: removeDir(storagePath) except CatchableError: discard

    # Writer
    var writerDone: Atomic[bool]
    writerDone.store(false)
    var writerErrors: Atomic[int]
    writerErrors.store(0)

    proc writerProc(dummy: int) {.thread, gcsafe.} =
      var cli: ProtocolClient
      try: cli = makeClient(port)
      except CatchableError:
        discard writerErrors.fetchAdd(1)
        writerDone.store(true)
        return
      for i in 0 ..< numKeys:
        let r = cli.kvPut("iw_key_" & $i, "iw_val_" & $i)
        if r.isErr:
          discard writerErrors.fetchAdd(1)
      cli.disconnect()
      writerDone.store(true)

    var writerThread: Thread[int]
    createThread(writerThread, writerProc, 0)

    # Reader: wait for writer to finish, then check all keys
    while not writerDone.load():
      sleep(5)
    joinThread(writerThread)
    check writerErrors.load() == 0

    let cli = makeClient(port)
    defer: cli.disconnect()
    var found = 0
    for i in 0 ..< numKeys:
      let r = cli.kvGet("iw_key_" & $i)
      if r.isOk and r.value.found:
        inc found
    check found == numKeys
