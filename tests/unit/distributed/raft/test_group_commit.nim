# Phase 9 — Group Commit Batcher tests
#
# Tests the GroupCommitBatcher in isolation and integrated with the
# MultiRaftCoordinator + RaftKVStoreExt.
#
# Port range: 20500+ (no ports actually needed — all in-memory/local).
# Temp storage: /tmp/fractio_gc_test_<N>/ (cleaned per test).

import std/[unittest, os, times, options, locks, tables, atomics, typedthreads]
import fractio/distributed/raft/group_commit
import fractio/distributed/raft/multigroup_coordinator
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/meta/system_tables
import fractio/protocol/raft_store

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc cleanDir(path: string) =
  try: removeDir(path) except CatchableError: discard
  try: createDir(path) except CatchableError: discard

proc makeStore(storagePath: string,
    groupCommitEnabled: bool = false,
    gcMaxBatch: int = 0,
    gcMaxDelayNs: int64 = 0): tuple[
    coord: MultiRaftCoordinator, store: RaftKVStoreExt, rid: GroupID] =
  cleanDir(storagePath)
  let cfg = CoordinatorConfig(
    nodeId: NodeID(1),
    numWorkers: 1,
    electionTimeoutNs: DEFAULT_ELECTION_TIMEOUT_NS,
    heartbeatIntervalNs: DEFAULT_HEARTBEAT_INTERVAL_NS,
    storagePath: storagePath,
    groupCommitEnabled: groupCommitEnabled,
    groupCommitMaxBatch: gcMaxBatch,
    groupCommitMaxDelayNs: gcMaxDelayNs,
  )
  let coord = newMultiRaftCoordinator(cfg)
  for rid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    let desc = newGroupDescriptor(rid)
    let rep = desc.addReplica(NodeID(1))
    let group = coord.createGroup(desc, rep.replicaId)
    group.becomeLeader()
  let store = newRaftKVStoreExt(coord, proposeTimeoutMs = 2000)
  coord.start()
  store.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])
  (coord, store, DATA_GROUP_START_ID)

proc teardownStore(coord: MultiRaftCoordinator, storagePath: string) =
  coord.stop()
  try: removeDir(storagePath) except CatchableError: discard


# ===========================================================================
# Suite 1: GroupCommitBatcher unit tests (no coordinator)
# ===========================================================================

suite "GroupCommitBatcher - unit":
  test "init and deinit":
    let b = cast[ptr GroupCommitBatcher](
      allocShared0(sizeof(GroupCommitBatcher)))
    initGroupCommitBatcher(b, 128, 1_000_000'i64)
    check b[].maxBatchSize == 128
    check b[].maxDelayNs == 1_000_000'i64
    check b[].running.load() == false
    deinitGroupCommitBatcher(b)
    deallocShared(b)

  test "init with defaults":
    let b = cast[ptr GroupCommitBatcher](
      allocShared0(sizeof(GroupCommitBatcher)))
    initGroupCommitBatcher(b)
    check b[].maxBatchSize == GC_DEFAULT_MAX_BATCH_SIZE
    check b[].maxDelayNs == GC_DEFAULT_MAX_DELAY_NS
    deinitGroupCommitBatcher(b)
    deallocShared(b)

  test "start and stop without flushFn sends error to callers":
    let b = cast[ptr GroupCommitBatcher](
      allocShared0(sizeof(GroupCommitBatcher)))
    initGroupCommitBatcher(b, 16, 500_000'i64)
    # flushFn is nil — callers should get an error result
    startBatcher(b)
    check b[].running.load() == true

    # Enqueue one item with a result channel
    let prc = cast[ptr ProposalResultChannel](
      allocShared0(sizeof(ProposalResultChannel)))
    prc[].ch.open(1)

    let wb = newWriteBatch()
    wb.put(@[byte(1)], @[byte(2)])
    let cmd = RaftCommand(kind: ckWrite, writeBatch: wb)
    enqueue(b, GroupID(1), cmd, prc)

    # Wait for the flush thread to process (max ~50ms)
    var gotResult = false
    for _ in 0 ..< 100:
      let (avail, res) = prc[].ch.tryRecv()
      if avail:
        check res.success == false
        check res.error.len > 0
        gotResult = true
        break
      sleep(1)
    check gotResult == true

    stopBatcher(b)
    check b[].running.load() == false
    prc[].ch.close()
    deallocShared(prc)
    deinitGroupCommitBatcher(b)
    deallocShared(b)

  test "start is idempotent":
    let b = cast[ptr GroupCommitBatcher](
      allocShared0(sizeof(GroupCommitBatcher)))
    initGroupCommitBatcher(b)
    startBatcher(b)
    startBatcher(b) # second call should be no-op
    check b[].running.load() == true
    stopBatcher(b)
    deinitGroupCommitBatcher(b)
    deallocShared(b)

  test "stop is idempotent":
    let b = cast[ptr GroupCommitBatcher](
      allocShared0(sizeof(GroupCommitBatcher)))
    initGroupCommitBatcher(b)
    startBatcher(b)
    stopBatcher(b)
    stopBatcher(b) # second call should be no-op
    check b[].running.load() == false
    deinitGroupCommitBatcher(b)
    deallocShared(b)


  test "enqueue with custom flushFn receives items":
    let b = cast[ptr GroupCommitBatcher](
      allocShared0(sizeof(GroupCommitBatcher)))
    initGroupCommitBatcher(b, 16, 500_000'i64)

    # Track how many items the flushFn receives
    var flushedCount: Atomic[int]
    flushedCount.store(0)
    let counterPtr = addr flushedCount

    proc testFlush(groupId: GroupID, batch: WriteBatch,
        items: seq[ptr ProposalResultChannel]) {.gcsafe, raises: [].} =
      {.cast(gcsafe).}:
        counterPtr[].atomicInc(items.len)
      let res = RaftResult(success: true, index: 1)
      for rp in items:
        if rp != nil:
          rp[].ch.send(res)

    b[].flushFn = testFlush
    startBatcher(b)

    # Enqueue 3 items
    var prcs: array[3, ptr ProposalResultChannel]
    for i in 0 ..< 3:
      prcs[i] = cast[ptr ProposalResultChannel](
        allocShared0(sizeof(ProposalResultChannel)))
      prcs[i][].ch.open(1)
      let wb = newWriteBatch()
      wb.put(@[byte(i)], @[byte(i)])
      let cmd = RaftCommand(kind: ckWrite, writeBatch: wb)
      enqueue(b, GroupID(1), cmd, prcs[i])

    # Wait for all results
    for i in 0 ..< 3:
      var got = false
      for _ in 0 ..< 200:
        let (avail, res) = prcs[i][].ch.tryRecv()
        if avail:
          check res.success == true
          got = true
          break
        sleep(1)
      check got == true

    check flushedCount.load() == 3

    stopBatcher(b)
    for i in 0 ..< 3:
      prcs[i][].ch.close()
      deallocShared(prcs[i])
    deinitGroupCommitBatcher(b)
    deallocShared(b)

  test "batching merges WriteBatches for same GroupID":
    let b = cast[ptr GroupCommitBatcher](
      allocShared0(sizeof(GroupCommitBatcher)))
    # Large delay so items accumulate before flush
    initGroupCommitBatcher(b, 256, 50_000_000'i64) # 50ms

    var receivedPuts: Atomic[int]
    receivedPuts.store(0)
    let putsPtr = addr receivedPuts

    proc mergeFlush(groupId: GroupID, batch: WriteBatch,
        items: seq[ptr ProposalResultChannel]) {.gcsafe, raises: [].} =
      # Count total puts in the merged batch
      {.cast(gcsafe).}:
        putsPtr[].atomicInc(batch.puts.len)
      let res = RaftResult(success: true, index: 1)
      for rp in items:
        if rp != nil:
          rp[].ch.send(res)

    b[].flushFn = mergeFlush
    startBatcher(b)

    # Rapidly enqueue 5 items for the same GroupID
    var prcs: array[5, ptr ProposalResultChannel]
    for i in 0 ..< 5:
      prcs[i] = cast[ptr ProposalResultChannel](
        allocShared0(sizeof(ProposalResultChannel)))
      prcs[i][].ch.open(1)
      let wb = newWriteBatch()
      wb.put(@[byte(i)], @[byte(i + 10)])
      let cmd = RaftCommand(kind: ckWrite, writeBatch: wb)
      enqueue(b, GroupID(1), cmd, prcs[i])

    # Wait for all results
    for i in 0 ..< 5:
      var got = false
      for _ in 0 ..< 500:
        let (avail, res) = prcs[i][].ch.tryRecv()
        if avail:
          check res.success == true
          got = true
          break
        sleep(1)
      check got == true

    # Total puts across all flush calls should be 5
    check receivedPuts.load() == 5

    stopBatcher(b)
    for i in 0 ..< 5:
      prcs[i][].ch.close()
      deallocShared(prcs[i])
    deinitGroupCommitBatcher(b)
    deallocShared(b)


  test "items for different GroupIDs are grouped separately":
    let b = cast[ptr GroupCommitBatcher](
      allocShared0(sizeof(GroupCommitBatcher)))
    initGroupCommitBatcher(b, 256, 50_000_000'i64) # 50ms

    var rangeCount: Atomic[int]
    rangeCount.store(0)
    let rcPtr = addr rangeCount

    proc groupFlush(groupId: GroupID, batch: WriteBatch,
        items: seq[ptr ProposalResultChannel]) {.gcsafe, raises: [].} =
      # Each call is for one GroupID
      {.cast(gcsafe).}:
        rcPtr[].atomicInc(1)
      let res = RaftResult(success: true, index: 1)
      for rp in items:
        if rp != nil:
          rp[].ch.send(res)

    b[].flushFn = groupFlush
    startBatcher(b)

    # Enqueue 2 items for GroupID(1) and 2 for GroupID(2)
    var prcs: array[4, ptr ProposalResultChannel]
    for i in 0 ..< 4:
      prcs[i] = cast[ptr ProposalResultChannel](
        allocShared0(sizeof(ProposalResultChannel)))
      prcs[i][].ch.open(1)
      let wb = newWriteBatch()
      wb.put(@[byte(i)], @[byte(i)])
      let cmd = RaftCommand(kind: ckWrite, writeBatch: wb)
      let rid = if i < 2: GroupID(1) else: GroupID(2)
      enqueue(b, rid, cmd, prcs[i])

    # Wait for all results
    for i in 0 ..< 4:
      var got = false
      for _ in 0 ..< 500:
        let (avail, res) = prcs[i][].ch.tryRecv()
        if avail:
          check res.success == true
          got = true
          break
        sleep(1)
      check got == true

    # At least 2 separate flushFn calls (one per GroupID per batch window)
    check rangeCount.load() >= 2

    stopBatcher(b)
    for i in 0 ..< 4:
      prcs[i][].ch.close()
      deallocShared(prcs[i])
    deinitGroupCommitBatcher(b)
    deallocShared(b)

  test "noop commands pass through without crash":
    let b = cast[ptr GroupCommitBatcher](
      allocShared0(sizeof(GroupCommitBatcher)))
    initGroupCommitBatcher(b, 16, 500_000'i64)

    proc noopFlush(groupId: GroupID, batch: WriteBatch,
        items: seq[ptr ProposalResultChannel]) {.gcsafe, raises: [].} =
      let res = RaftResult(success: true, index: 1)
      for rp in items:
        if rp != nil:
          rp[].ch.send(res)

    b[].flushFn = noopFlush
    startBatcher(b)

    let prc = cast[ptr ProposalResultChannel](
      allocShared0(sizeof(ProposalResultChannel)))
    prc[].ch.open(1)

    let cmd = RaftCommand(kind: ckNoop)
    enqueue(b, GroupID(1), cmd, prc)

    var got = false
    for _ in 0 ..< 200:
      let (avail, res) = prc[].ch.tryRecv()
      if avail:
        check res.success == true
        got = true
        break
      sleep(1)
    check got == true

    stopBatcher(b)
    prc[].ch.close()
    deallocShared(prc)
    deinitGroupCommitBatcher(b)
    deallocShared(b)


# ===========================================================================
# Suite 2: Coordinator integration — group commit enabled
# ===========================================================================

suite "GroupCommit - coordinator integration":
  test "coordinator allocates batcher when groupCommitEnabled":
    cleanDir("/tmp/fractio_gc_t10")
    let cfg = CoordinatorConfig(
      nodeId: NodeID(1),
      numWorkers: 1,
      electionTimeoutNs: DEFAULT_ELECTION_TIMEOUT_NS,
      heartbeatIntervalNs: DEFAULT_HEARTBEAT_INTERVAL_NS,
      storagePath: "/tmp/fractio_gc_t10",
      groupCommitEnabled: true,
    )
    let coord = newMultiRaftCoordinator(cfg)
    check coord.groupCommitEnabled == true
    check coord.groupCommitBatcherPtr != nil
    # Don't start — just verify allocation
    deinitGroupCommitBatcher(coord.groupCommitBatcherPtr)
    deallocShared(coord.groupCommitBatcherPtr)
    coord.groupCommitBatcherPtr = nil
    coord.groupCommitEnabled = false
    coord.stop()
    try: removeDir("/tmp/fractio_gc_t10") except CatchableError: discard

  test "coordinator does NOT allocate batcher when disabled":
    cleanDir("/tmp/fractio_gc_t11")
    let cfg = CoordinatorConfig(
      nodeId: NodeID(1),
      numWorkers: 1,
      electionTimeoutNs: DEFAULT_ELECTION_TIMEOUT_NS,
      heartbeatIntervalNs: DEFAULT_HEARTBEAT_INTERVAL_NS,
      storagePath: "/tmp/fractio_gc_t11",
      groupCommitEnabled: false,
    )
    let coord = newMultiRaftCoordinator(cfg)
    check coord.groupCommitEnabled == false
    check coord.groupCommitBatcherPtr == nil
    coord.stop()
    try: removeDir("/tmp/fractio_gc_t11") except CatchableError: discard

  test "put and get via group commit path":
    let (coord, store, rid) = makeStore("/tmp/fractio_gc_t12",
        groupCommitEnabled = true)
    defer: teardownStore(coord, "/tmp/fractio_gc_t12")
    let wr = store.raftPut("gc_key", "gc_val")
    check wr.isOk
    check wr.value.value == "gc_val"
    let gr = store.raftGet("gc_key")
    check gr.isOk
    check gr.value.isSome
    check gr.value.get().value == "gc_val"

  test "overwrite key via group commit":
    let (coord, store, _) = makeStore("/tmp/fractio_gc_t13",
        groupCommitEnabled = true)
    defer: teardownStore(coord, "/tmp/fractio_gc_t13")
    discard store.raftPut("k", "v1")
    discard store.raftPut("k", "v2")
    let gr = store.raftGet("k")
    check gr.isOk
    check gr.value.get().value == "v2"

  test "delete via group commit":
    let (coord, store, _) = makeStore("/tmp/fractio_gc_t14",
        groupCommitEnabled = true)
    defer: teardownStore(coord, "/tmp/fractio_gc_t14")
    discard store.raftPut("del_me", "gone")
    let dr = store.raftDelete("del_me")
    check dr.isOk
    check dr.value.isSome
    check dr.value.get().value == "gone"
    let gr = store.raftGet("del_me")
    check gr.isOk
    check gr.value.isNone

  test "multiple independent keys via group commit":
    let (coord, store, _) = makeStore("/tmp/fractio_gc_t15",
        groupCommitEnabled = true)
    defer: teardownStore(coord, "/tmp/fractio_gc_t15")
    discard store.raftPut("a", "1")
    discard store.raftPut("b", "2")
    discard store.raftPut("c", "3")
    check store.raftGet("a").value.get().value == "1"
    check store.raftGet("b").value.get().value == "2"
    check store.raftGet("c").value.get().value == "3"

  test "scan works with group commit":
    let (coord, store, _) = makeStore("/tmp/fractio_gc_t16",
        groupCommitEnabled = true)
    defer: teardownStore(coord, "/tmp/fractio_gc_t16")
    discard store.raftPut("x1", "a")
    discard store.raftPut("x2", "b")
    discard store.raftPut("x3", "c")
    let sr = store.raftScan("x1", "x4", 0)
    check sr.isOk
    check sr.value.len == 3

  test "get missing key returns none with group commit":
    let (coord, store, _) = makeStore("/tmp/fractio_gc_t17",
        groupCommitEnabled = true)
    defer: teardownStore(coord, "/tmp/fractio_gc_t17")
    let gr = store.raftGet("nonexistent")
    check gr.isOk
    check gr.value.isNone


# ===========================================================================
# Suite 3: Concurrent writes via group commit
# ===========================================================================

suite "GroupCommit - concurrent writes":
  test "concurrent puts produce correct results":
    let (coord, store, _) = makeStore("/tmp/fractio_gc_t20",
        groupCommitEnabled = true)
    defer: teardownStore(coord, "/tmp/fractio_gc_t20")

    const NUM_WRITERS = 4
    const KEYS_PER_WRITER = 10
    var threads: array[NUM_WRITERS, Thread[tuple[store: RaftKVStoreExt,
        writerId: int]]]
    var allOk: Atomic[int]
    allOk.store(0)
    let okPtr = addr allOk

    proc writerProc(args: tuple[store: RaftKVStoreExt,
        writerId: int]) {.thread.} =
      var localOk = 0
      for i in 0 ..< KEYS_PER_WRITER:
        let key = "w" & $args.writerId & "_k" & $i
        let val = "v" & $args.writerId & "_" & $i
        let wr = args.store.raftPut(key, val)
        if wr.isOk:
          inc localOk
      {.cast(gcsafe).}:
        okPtr[].atomicInc(localOk)

    for i in 0 ..< NUM_WRITERS:
      createThread(threads[i], writerProc, (store, i))
    for i in 0 ..< NUM_WRITERS:
      joinThread(threads[i])

    check allOk.load() == NUM_WRITERS * KEYS_PER_WRITER

    # Verify all keys are readable
    var readOk = 0
    for w in 0 ..< NUM_WRITERS:
      for k in 0 ..< KEYS_PER_WRITER:
        let key = "w" & $w & "_k" & $k
        let val = "v" & $w & "_" & $k
        let gr = store.raftGet(key)
        if gr.isOk and gr.value.isSome and gr.value.get().value == val:
          inc readOk
    check readOk == NUM_WRITERS * KEYS_PER_WRITER

  test "concurrent puts and deletes":
    let (coord, store, _) = makeStore("/tmp/fractio_gc_t21",
        groupCommitEnabled = true)
    defer: teardownStore(coord, "/tmp/fractio_gc_t21")

    # First, write some keys
    for i in 0 ..< 10:
      discard store.raftPut("cd_" & $i, "val_" & $i)

    const NUM_OPS = 2
    var threads: array[NUM_OPS, Thread[tuple[store: RaftKVStoreExt, idx: int]]]

    proc putProc(args: tuple[store: RaftKVStoreExt, idx: int]) {.thread.} =
      for i in 10 ..< 20:
        discard args.store.raftPut("cd_" & $i, "new_" & $i)

    proc delProc(args: tuple[store: RaftKVStoreExt, idx: int]) {.thread.} =
      for i in 0 ..< 5:
        discard args.store.raftDelete("cd_" & $i)

    createThread(threads[0], putProc, (store, 0))
    createThread(threads[1], delProc, (store, 1))
    joinThread(threads[0])
    joinThread(threads[1])

    # Keys 0..4 should be deleted
    for i in 0 ..< 5:
      let gr = store.raftGet("cd_" & $i)
      check gr.isOk
      check gr.value.isNone

    # Keys 10..19 should exist
    for i in 10 ..< 20:
      let gr = store.raftGet("cd_" & $i)
      check gr.isOk
      check gr.value.isSome


# ===========================================================================
# Suite 4: Classic path (group commit disabled) still works
# ===========================================================================

suite "GroupCommit - classic path unchanged":
  test "put and get without group commit":
    let (coord, store, _) = makeStore("/tmp/fractio_gc_t30",
        groupCommitEnabled = false)
    defer: teardownStore(coord, "/tmp/fractio_gc_t30")
    let wr = store.raftPut("classic_k", "classic_v")
    check wr.isOk
    check wr.value.value == "classic_v"
    let gr = store.raftGet("classic_k")
    check gr.isOk
    check gr.value.get().value == "classic_v"

  test "delete without group commit":
    let (coord, store, _) = makeStore("/tmp/fractio_gc_t31",
        groupCommitEnabled = false)
    defer: teardownStore(coord, "/tmp/fractio_gc_t31")
    discard store.raftPut("del_classic", "val")
    let dr = store.raftDelete("del_classic")
    check dr.isOk
    check dr.value.isSome
    let gr = store.raftGet("del_classic")
    check gr.isOk
    check gr.value.isNone

  test "scan without group commit":
    let (coord, store, _) = makeStore("/tmp/fractio_gc_t32",
        groupCommitEnabled = false)
    defer: teardownStore(coord, "/tmp/fractio_gc_t32")
    discard store.raftPut("s1", "a")
    discard store.raftPut("s2", "b")
    let sr = store.raftScan("s1", "s3", 0)
    check sr.isOk
    check sr.value.len == 2

  test "multiple keys without group commit":
    let (coord, store, _) = makeStore("/tmp/fractio_gc_t33",
        groupCommitEnabled = false)
    defer: teardownStore(coord, "/tmp/fractio_gc_t33")
    discard store.raftPut("m1", "v1")
    discard store.raftPut("m2", "v2")
    discard store.raftPut("m3", "v3")
    check store.raftGet("m1").value.get().value == "v1"
    check store.raftGet("m2").value.get().value == "v2"
    check store.raftGet("m3").value.get().value == "v3"

  test "concurrent puts without group commit":
    let (coord, store, _) = makeStore("/tmp/fractio_gc_t34",
        groupCommitEnabled = false)
    defer: teardownStore(coord, "/tmp/fractio_gc_t34")

    const N = 2
    const KEYS = 5
    var threads: array[N, Thread[tuple[store: RaftKVStoreExt, wid: int]]]
    var allOk: Atomic[int]
    allOk.store(0)
    let okPtr = addr allOk

    proc classicWriter(args: tuple[store: RaftKVStoreExt,
        wid: int]) {.thread.} =
      var localOk = 0
      for i in 0 ..< KEYS:
        let key = "cw" & $args.wid & "_" & $i
        let val = "cv" & $args.wid & "_" & $i
        let wr = args.store.raftPut(key, val)
        if wr.isOk: inc localOk
      {.cast(gcsafe).}:
        okPtr[].atomicInc(localOk)

    for i in 0 ..< N:
      createThread(threads[i], classicWriter, (store, i))
    for i in 0 ..< N:
      joinThread(threads[i])

    check allOk.load() == N * KEYS


# ===========================================================================
# Suite 5: Configuration variants
# ===========================================================================

suite "GroupCommit - configuration":
  test "custom maxBatch is respected":
    cleanDir("/tmp/fractio_gc_t40")
    let cfg = CoordinatorConfig(
      nodeId: NodeID(1),
      numWorkers: 1,
      electionTimeoutNs: DEFAULT_ELECTION_TIMEOUT_NS,
      heartbeatIntervalNs: DEFAULT_HEARTBEAT_INTERVAL_NS,
      storagePath: "/tmp/fractio_gc_t40",
      groupCommitEnabled: true,
      groupCommitMaxBatch: 32,
    )
    let coord = newMultiRaftCoordinator(cfg)
    check coord.groupCommitBatcherPtr != nil
    check coord.groupCommitBatcherPtr[].maxBatchSize == 32
    deinitGroupCommitBatcher(coord.groupCommitBatcherPtr)
    deallocShared(coord.groupCommitBatcherPtr)
    coord.groupCommitBatcherPtr = nil
    coord.groupCommitEnabled = false
    coord.stop()
    try: removeDir("/tmp/fractio_gc_t40") except CatchableError: discard

  test "custom maxDelayNs is respected":
    cleanDir("/tmp/fractio_gc_t41")
    let cfg = CoordinatorConfig(
      nodeId: NodeID(1),
      numWorkers: 1,
      electionTimeoutNs: DEFAULT_ELECTION_TIMEOUT_NS,
      heartbeatIntervalNs: DEFAULT_HEARTBEAT_INTERVAL_NS,
      storagePath: "/tmp/fractio_gc_t41",
      groupCommitEnabled: true,
      groupCommitMaxDelayNs: 5_000_000'i64,           # 5ms
    )
    let coord = newMultiRaftCoordinator(cfg)
    check coord.groupCommitBatcherPtr != nil
    check coord.groupCommitBatcherPtr[].maxDelayNs == 5_000_000'i64
    deinitGroupCommitBatcher(coord.groupCommitBatcherPtr)
    deallocShared(coord.groupCommitBatcherPtr)
    coord.groupCommitBatcherPtr = nil
    coord.groupCommitEnabled = false
    coord.stop()
    try: removeDir("/tmp/fractio_gc_t41") except CatchableError: discard

  test "zero maxBatch falls back to default":
    cleanDir("/tmp/fractio_gc_t42")
    let cfg = CoordinatorConfig(
      nodeId: NodeID(1),
      numWorkers: 1,
      electionTimeoutNs: DEFAULT_ELECTION_TIMEOUT_NS,
      heartbeatIntervalNs: DEFAULT_HEARTBEAT_INTERVAL_NS,
      storagePath: "/tmp/fractio_gc_t42",
      groupCommitEnabled: true,
      groupCommitMaxBatch: 0,
      groupCommitMaxDelayNs: 0,
    )
    let coord = newMultiRaftCoordinator(cfg)
    check coord.groupCommitBatcherPtr != nil
    check coord.groupCommitBatcherPtr[].maxBatchSize == GC_DEFAULT_MAX_BATCH_SIZE
    check coord.groupCommitBatcherPtr[].maxDelayNs == GC_DEFAULT_MAX_DELAY_NS
    deinitGroupCommitBatcher(coord.groupCommitBatcherPtr)
    deallocShared(coord.groupCommitBatcherPtr)
    coord.groupCommitBatcherPtr = nil
    coord.groupCommitEnabled = false
    coord.stop()
    try: removeDir("/tmp/fractio_gc_t42") except CatchableError: discard

  test "group commit with small batch size works correctly":
    let (coord, store, _) = makeStore("/tmp/fractio_gc_t43",
        groupCommitEnabled = true,
        gcMaxBatch = 2,
        gcMaxDelayNs = 500_000'i64)
    defer: teardownStore(coord, "/tmp/fractio_gc_t43")
    # Write more keys than the batch size to force multiple flushes
    for i in 0 ..< 6:
      let wr = store.raftPut("sb_" & $i, "val_" & $i)
      check wr.isOk
    # Verify all reads
    for i in 0 ..< 6:
      let gr = store.raftGet("sb_" & $i)
      check gr.isOk
      check gr.value.get().value == "val_" & $i


# ===========================================================================
# Suite 6: Edge cases and stress
# ===========================================================================

suite "GroupCommit - edge cases":
  test "empty WriteBatch via group commit":
    let (coord, store, rid) = makeStore("/tmp/fractio_gc_t50",
        groupCommitEnabled = true)
    defer: teardownStore(coord, "/tmp/fractio_gc_t50")
    # Propose an empty write batch directly through the coordinator
    let emptyBatch = newWriteBatch()
    let cmd = RaftCommand(kind: ckWrite, writeBatch: emptyBatch)
    let res = coord.proposeAndWait(rid, cmd, 2000)
    check res.success == true

  test "large values via group commit":
    let (coord, store, _) = makeStore("/tmp/fractio_gc_t51",
        groupCommitEnabled = true)
    defer: teardownStore(coord, "/tmp/fractio_gc_t51")
    var bigVal = newString(4096)
    for i in 0 ..< bigVal.len:
      bigVal[i] = chr(ord('A') + (i mod 26))
    let wr = store.raftPut("big_key", bigVal)
    check wr.isOk
    let gr = store.raftGet("big_key")
    check gr.isOk
    check gr.value.get().value == bigVal

  test "many sequential writes via group commit":
    let (coord, store, _) = makeStore("/tmp/fractio_gc_t52",
        groupCommitEnabled = true)
    defer: teardownStore(coord, "/tmp/fractio_gc_t52")
    for i in 0 ..< 50:
      let wr = store.raftPut("seq_" & $i, "v" & $i)
      check wr.isOk
    check store.raftLen() == 50

  test "noop command goes through classic path even with group commit enabled":
    let (coord, store, rid) = makeStore("/tmp/fractio_gc_t53",
        groupCommitEnabled = true)
    defer: teardownStore(coord, "/tmp/fractio_gc_t53")
    # ckNoop should NOT go through group commit (only ckWrite does)
    let cmd = RaftCommand(kind: ckNoop)
    let res = coord.proposeAndWait(rid, cmd, 2000)
    check res.success == true

  test "start and stop coordinator with group commit multiple times":
    cleanDir("/tmp/fractio_gc_t54")
    # First cycle
    block:
      let (coord, store, _) = makeStore("/tmp/fractio_gc_t54",
          groupCommitEnabled = true)
      discard store.raftPut("cycle1", "val1")
      check store.raftGet("cycle1").value.get().value == "val1"
      coord.stop()
    # Second cycle — fresh coordinator on same storage path
    cleanDir("/tmp/fractio_gc_t54")
    block:
      let (coord, store, _) = makeStore("/tmp/fractio_gc_t54",
          groupCommitEnabled = true)
      discard store.raftPut("cycle2", "val2")
      check store.raftGet("cycle2").value.get().value == "val2"
      coord.stop()
    try: removeDir("/tmp/fractio_gc_t54") except CatchableError: discard

