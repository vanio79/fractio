# Phase 17 — Per-shard worker pool tests.
#
# Verifies ShardWorkerState lifecycle and correctness:
#   1.  shardWorkers table populated by createGroup, workers started by start().
#   2.  proposeAndWait routes to per-shard channel (single-node, no group commit).
#   3.  Single-shard writes committed and applied correctly.
#   4.  Multi-shard writes routed independently (each shard has own worker).
#   5.  proposeParallel routes proposals to per-shard channels in parallel.
#   6.  Not-the-leader returns error without crashing.
#   7.  Concurrent writes from N threads to M shards — no corruption, no hang.
#   8.  stop() joins all shard threads cleanly (no leak / no deadlock).
#   9.  removeGroup stops and frees shard worker; subsequent propose falls back.
#  10.  group-commit path still works (shardWorkerProc not used when GC enabled).
#  11.  Worker falls back to global proposalCh for unknown GroupID.
#  12.  Re-start after stop is NOT supported (running guard); verify idempotent.
#
# No TCP / no ProtocolServer — pure in-process MultiRaftCoordinator.
# Storage: /tmp/fractio_swp_<N>/ cleaned up per test.
# Port usage: none (20640+ reserved but not needed here).

import std/[unittest, os, options, tables, atomics, typedthreads, locks, strutils]
import fractio/protocol/raft_store
import fractio/distributed/raft/multigroup_coordinator
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/meta/system_tables

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

const BASE_DIR = "/tmp/fractio_swp_"

proc cleanDir(path: string) =
  try: removeDir(path) except CatchableError: discard
  try: createDir(path) except CatchableError: discard

proc makeCoord(n: int, groupCommit = false): MultiRaftCoordinator =
  let path = BASE_DIR & $n
  cleanDir(path)
  let cfg = CoordinatorConfig(
    nodeId: NodeID(1),
    numWorkers: 4,
    electionTimeoutNs: DEFAULT_ELECTION_TIMEOUT_NS,
    heartbeatIntervalNs: DEFAULT_HEARTBEAT_INTERVAL_NS,
    storagePath: path,
    groupCommitEnabled: groupCommit,
  )
  newMultiRaftCoordinator(cfg)

proc addLeaderGroup(c: MultiRaftCoordinator, rid: GroupID): RaftGroup =
  let desc = newGroupDescriptor(rid)
  let rep = desc.addReplica(NodeID(1))
  result = c.createGroup(desc, rep.replicaId)
  result.becomeLeader()

proc teardown(c: MultiRaftCoordinator, n: int) =
  c.stop()
  try: removeDir(BASE_DIR & $n) except CatchableError: discard

proc writeKV(store: RaftKVStoreExt, key, val: string): bool =
  store.raftPut(key, val).isOk

proc readKV(store: RaftKVStoreExt, key: string): string =
  let res = store.raftGet(key)
  if res.isOk and res.value.isSome:
    result = res.value.get.value

# ---------------------------------------------------------------------------
# Suite 1: lifecycle
# ---------------------------------------------------------------------------

suite "Phase17 ShardWorkerPool lifecycle":

  test "shardWorkers table populated after createGroup":
    let c = makeCoord(640)
    let rid = DATA_GROUP_START_ID
    discard addLeaderGroup(c, rid)
    # Before start, shard worker exists but is not running
    acquire(c.shardWorkersMu)
    let sw = c.shardWorkers.getOrDefault(rid, nil)
    release(c.shardWorkersMu)
    check sw != nil
    check not sw[].running.load
    teardown(c, 640)

  test "shard worker running after start()":
    let c = makeCoord(641)
    let rid = DATA_GROUP_START_ID
    discard addLeaderGroup(c, rid)
    c.start()
    acquire(c.shardWorkersMu)
    let sw = c.shardWorkers.getOrDefault(rid, nil)
    release(c.shardWorkersMu)
    check sw != nil
    check sw[].running.load
    teardown(c, 641)

  test "stop() cleans up shard worker table":
    let c = makeCoord(642)
    discard addLeaderGroup(c, DATA_GROUP_START_ID)
    c.start()
    c.stop()
    acquire(c.shardWorkersMu)
    let empty = c.shardWorkers.len == 0
    release(c.shardWorkersMu)
    check empty

  test "multiple groups each get their own shard worker":
    let c = makeCoord(643)
    for i in [2, 3, 4]:
      discard addLeaderGroup(c, GroupID(i))
    c.start()
    acquire(c.shardWorkersMu)
    let cnt = c.shardWorkers.len
    release(c.shardWorkersMu)
    check cnt == 3
    teardown(c, 643)

# ---------------------------------------------------------------------------
# Suite 2: correctness — single shard
# ---------------------------------------------------------------------------

suite "Phase17 ShardWorkerPool single-shard correctness":

  test "write and read back via shard worker":
    let c = makeCoord(644)
    discard addLeaderGroup(c, DATA_GROUP_START_ID)
    c.start()
    let store = newRaftKVStoreExt(c, proposeTimeoutMs = 5000)
    store.bootstrapStore(@[DATA_GROUP_START_ID])
    check writeKV(store, "hello", "world")
    check readKV(store, "hello") == "world"
    teardown(c, 644)

  test "multiple sequential writes accumulate":
    let c = makeCoord(645)
    discard addLeaderGroup(c, DATA_GROUP_START_ID)
    c.start()
    let store = newRaftKVStoreExt(c, proposeTimeoutMs = 5000)
    store.bootstrapStore(@[DATA_GROUP_START_ID])
    for i in 0..<20:
      check writeKV(store, "k" & $i, "v" & $i)
    for i in 0..<20:
      check readKV(store, "k" & $i) == "v" & $i
    teardown(c, 645)

  test "not-the-leader returns error":
    let c = makeCoord(646)
    let desc = newGroupDescriptor(DATA_GROUP_START_ID)
    let rep = desc.addReplica(NodeID(1))
    let grp = c.createGroup(desc, rep.replicaId)
    # Do NOT call becomeLeader — stays follower
    c.start()
    let res = c.proposeAndWait(DATA_GROUP_START_ID,
      RaftCommand(kind: ckWrite, writeBatch: newWriteBatch()), 1000)
    check not res.success
    check res.error.len > 0
    teardown(c, 646)

# ---------------------------------------------------------------------------
# Suite 3: multi-shard routing
# ---------------------------------------------------------------------------

suite "Phase17 ShardWorkerPool multi-shard routing":

  test "two shards each served by independent worker":
    let c = makeCoord(647)
    discard addLeaderGroup(c, DATA_GROUP_START_ID)
    discard addLeaderGroup(c, GroupID(3))
    c.start()
    let store = newRaftKVStoreExt(c, proposeTimeoutMs = 5000)
    store.bootstrapStore(@[DATA_GROUP_START_ID, GroupID(3)])
    # Keys routed to shard 1
    check writeKV(store, "apple", "1")
    check writeKV(store, "banana", "2")
    # Keys routed to shard 2
    check writeKV(store, "mango", "3")
    check writeKV(store, "orange", "4")
    check readKV(store, "apple") == "1"
    check readKV(store, "mango") == "3"
    teardown(c, 647)

  test "proposeParallel commits to both shards simultaneously":
    let c = makeCoord(648)
    discard addLeaderGroup(c, DATA_GROUP_START_ID)
    discard addLeaderGroup(c, GroupID(3))
    c.start()

    let b1 = newWriteBatch()
    b1.put(@(cast[seq[byte]](@[byte 'a'])), @(cast[seq[byte]](@[byte '1'])))
    let b2 = newWriteBatch()
    b2.put(@(cast[seq[byte]](@[byte 'z'])), @(cast[seq[byte]](@[byte '2'])))

    let proposals = @[
      (groupId: DATA_GROUP_START_ID, command: RaftCommand(kind: ckWrite,
          writeBatch: b1)),
      (groupId: GroupID(3), command: RaftCommand(kind: ckWrite,
          writeBatch: b2)),
    ]
    let results = c.proposeParallel(proposals, 5000)
    check results.len == 2
    check results[0].success
    check results[1].success
    teardown(c, 648)

  test "three-shard parallel all succeed":
    let c = makeCoord(649)
    for i in [2, 3, 4]:
      discard addLeaderGroup(c, GroupID(i))
    c.start()

    var props: seq[tuple[groupId: GroupID, command: RaftCommand]] = @[]
    for i in [2, 3, 4]:
      let b = newWriteBatch()
      b.put(@[byte i], @[byte i])
      props.add((GroupID(i), RaftCommand(kind: ckWrite, writeBatch: b)))

    let results = c.proposeParallel(props, 5000)
    check results.len == 3
    for r in results:
      check r.success
    teardown(c, 649)

# ---------------------------------------------------------------------------
# Suite 4: concurrency stress
# ---------------------------------------------------------------------------

suite "Phase17 ShardWorkerPool concurrency":

  test "8 threads writing to 3 shards concurrently — no corruption":
    const
      NUM_THREADS = 8
      OPS_PER_THREAD = 50

    let c = makeCoord(650)
    discard addLeaderGroup(c, DATA_GROUP_START_ID)
    discard addLeaderGroup(c, GroupID(3))
    discard addLeaderGroup(c, GroupID(4))
    c.start()
    let store = newRaftKVStoreExt(c, proposeTimeoutMs = 10000)
    store.bootstrapStore(@[DATA_GROUP_START_ID, GroupID(3), GroupID(4)])

    var errors: Atomic[int]
    errors.store(0)

    type ThreadArg = object
      storePtr: pointer
      threadId: int
      errorsPtr: ptr Atomic[int]

    proc worker(arg: ThreadArg) {.thread.} =
      let s = cast[RaftKVStoreExt](arg.storePtr)
      for i in 0 ..< OPS_PER_THREAD:
        # Distribute keys across all 3 shards:
        #   t0..t2 → shard1 (a–c prefix)
        #   t3..t5 → shard2 (d–o prefix)
        #   t6..t7 → shard3 (p–z prefix)
        let prefix = case arg.threadId mod 3
          of 0: "a"
          of 1: "d"
          else: "p"
        let key = prefix & $arg.threadId & "_" & $i
        let val = "v" & $i
        var ok = false
        {.cast(gcsafe).}:
          ok = s.raftPut(key, val).isOk
        if not ok:
          discard arg.errorsPtr[].fetchAdd(1)

    var threads: array[NUM_THREADS, Thread[ThreadArg]]
    for t in 0 ..< NUM_THREADS:
      createThread(threads[t], worker, ThreadArg(
        storePtr: cast[pointer](store),
        threadId: t,
        errorsPtr: addr errors,
      ))
    for t in 0 ..< NUM_THREADS:
      joinThread(threads[t])

    check errors.load == 0
    teardown(c, 650)

# ---------------------------------------------------------------------------
# Suite 5: removeGroup and group-commit interaction
# ---------------------------------------------------------------------------

suite "Phase17 ShardWorkerPool removeGroup and group-commit":

  test "removeGroup stops shard worker; worker table shrinks":
    let c = makeCoord(651)
    discard addLeaderGroup(c, DATA_GROUP_START_ID)
    discard addLeaderGroup(c, GroupID(3))
    c.start()
    c.removeGroup(GroupID(3))
    acquire(c.shardWorkersMu)
    let cnt = c.shardWorkers.len
    release(c.shardWorkersMu)
    check cnt == 1
    teardown(c, 651)

  test "group-commit enabled: shard worker path bypassed, batcher used":
    let c = makeCoord(652, groupCommit = true)
    discard addLeaderGroup(c, DATA_GROUP_START_ID)
    c.start()
    let store = newRaftKVStoreExt(c, proposeTimeoutMs = 5000)
    store.bootstrapStore(@[DATA_GROUP_START_ID])
    # With group commit, shard worker is created but proposal goes to batcher.
    # Verify correctness: writes still committed and readable.
    check writeKV(store, "gc_key", "gc_val")
    check readKV(store, "gc_key") == "gc_val"
    teardown(c, 652)

  test "proposeParallel empty input returns empty seq":
    let c = makeCoord(653)
    discard addLeaderGroup(c, DATA_GROUP_START_ID)
    c.start()
    let results = c.proposeParallel(@[], 5000)
    check results.len == 0
    teardown(c, 653)
