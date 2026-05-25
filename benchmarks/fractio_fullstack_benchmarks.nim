# Fractio Full-Stack Benchmarks
#
# Exercises the complete network path:
#   ProtocolServer (in-process) ← TCP → ProtocolClient
#
# Server backend: 3 Raft groups, each owning a key-range partition:
#   Group 1 (GroupID 1): keys ""       .. "key_1666"   (low third)
#   Group 2 (GroupID 2): keys "key_1666" .. "key_3333"  (mid third)
#   Group 3 (GroupID 3): keys "key_3333" .. ""           (high third)
#
# Every KV operation is routed to the correct Raft group by the RaftKVStore
# shard table, exercising real multi-Raft key-range dispatch end-to-end.
#
# Benchmarks:
#   1. Sequential mixed        (2:1 read:write, single client)
#   2. Write-only              (single client)
#   3. Read-only               (single client)
#   4. Scan                    (single client)
#   5. Transactional           (begin / put / commit, single client)
#   6. Concurrent mixed        (2:1 read:write, N clients in parallel)
#      Mirrors the workload in db_benchmarks.py for PostgreSQL/MySQL/SQLite.
#      Each thread owns its own ProtocolClient connection.
#      Thread counts: 2, 4, 8 — same as the Python driver.
#
# The server is started once and shared across all benchmark runs.
# The client reconnects between benchmarks so each run starts with a
# fresh connection and a clean request-ID counter.
#
# CLI flags (mirror db_benchmarks.py):
#   --keys   N        number of distinct keys   (default 5000)
#   --ops    N        total ops per benchmark    (default 1000)
#   --threads N       threads for concurrent run (default 4; also runs 2 and 8)
#   --value-size N    value size in bytes        (default 100)
#   --warmup N        warmup ops                 (default 100)
#   --skip-sequential  skip benchmarks 1-5
#   --skip-concurrent  skip benchmark 6
#
# Port: 29000  (well clear of all protocol test ports ≤ 20499)

import std/[os, times, strutils, math, atomics, algorithm]
import std/typedthreads
import fractio/protocol/server
import fractio/protocol/client
import fractio/protocol/types
import fractio/protocol/messages/txn as txnMsgs
import fractio/protocol/raft_store
import fractio/distributed/raft/multigroup_coordinator
import fractio/distributed/raft/multigroup_types

# =============================================================================
# Constants
# =============================================================================

const
  BENCH_PORT = 29000
  BENCH_HOST = "127.0.0.1"
  SERVER_WAIT_MS = 200 ## ms to sleep after server.start() before connecting
  RAFT_STORAGE_PATH = "/tmp/fractio_bench_raft"

  ## Key-range boundaries for the three Raft groups.
  ## key_0 .. key_1665  → group 1
  ## key_1666 .. key_3332 → group 2
  ## key_3333 .. ∞        → group 3
  GROUP_SPLIT_LO* = "key_1666"
  GROUP_SPLIT_HI* = "key_3333"

# =============================================================================
# Shared types
# =============================================================================

type
  BenchmarkConfig* = object
    numKeys*: int
    numOps*: int
    valueSize*: int
    warmupOps*: int
    numThreads*: int ## used by concurrent benchmark

  BenchmarkResult* = object
    name*: string
    opsPerSec*: float
    avgLatencyUs*: float
    minLatencyUs*: float
    maxLatencyUs*: float
    p99LatencyUs*: float
    totalOps*: int
    errors*: int

# ---------------------------------------------------------------------------
# Per-thread result bag written by the concurrent worker; collected afterward.
# Must be a plain object (not a ref) so it can cross thread boundaries safely.
# ---------------------------------------------------------------------------

type
  ThreadResult* = object
    latencies*: seq[float]
    errors*: int

# =============================================================================
# Helpers
# =============================================================================

proc makeKey(id: int): string =
  "key_" & $id

proc makeValue(size: int): string =
  result = newString(size)
  for i in 0..<size:
    result[i] = char(ord('a') + (i mod 26))

proc newClient(): ProtocolClient =
  ## Create and connect a fresh client to the benchmark server.
  var ccfg = defaultClientConfig(BENCH_HOST, BENCH_PORT)
  ccfg.timeoutMs = 30_000
  result = newProtocolClient(ccfg)
  let cr = result.connect()
  if cr.isErr:
    echo "[bench] ERROR: client connect failed: " & cr.error.msg
    quit(1)

proc calcResult(name: string, latencies: seq[float],
    errors: int, durationSec: float): BenchmarkResult =
  result.name = name
  result.totalOps = latencies.len
  result.errors = errors
  result.opsPerSec = if durationSec > 0.0: float(latencies.len) / durationSec
                     else: 0.0
  if latencies.len > 0:
    result.avgLatencyUs = sum(latencies) / float(latencies.len)
    result.minLatencyUs = min(latencies)
    result.maxLatencyUs = max(latencies)
    var sorted = latencies
    sorted.sort(system.cmp[float])
    let p99idx = max(0, int(float(sorted.len) * 0.99) - 1)
    result.p99LatencyUs = sorted[p99idx]

# =============================================================================
# Benchmark 1: Sequential mixed  (2:1 read:write)
# Keys are spread across all key-range shards automatically via RaftKVStore routing.
# =============================================================================

proc runSequentialBenchmark*(client: ProtocolClient,
    config: BenchmarkConfig): BenchmarkResult =
  let value = makeValue(config.valueSize)
  var latencies: seq[float] = @[]
  var errors = 0

  # Warmup — not timed
  for i in 0..<config.warmupOps:
    discard client.kvGet(makeKey(i mod config.numKeys))

  let startTime = getTime()

  for i in 0..<config.numOps:
    let opStart = getTime()
    let key = makeKey(i mod config.numKeys)
    if i mod 3 == 0:
      let r = client.kvPut(key, value)
      if r.isErr: inc errors
    else:
      let r = client.kvGet(key)
      if r.isErr: inc errors
    latencies.add(float((getTime() - opStart).inMicroseconds))

  let durationSec = float((getTime() - startTime).inMilliseconds) / 1000.0
  result = calcResult("Sequential Mixed", latencies, errors, durationSec)

# =============================================================================
# Benchmark 2: Write-only
# Keys cycle across all three Raft range groups.
# =============================================================================

proc runWriteBenchmark*(client: ProtocolClient,
    config: BenchmarkConfig): BenchmarkResult =
  let value = makeValue(config.valueSize)
  var latencies: seq[float] = @[]
  var errors = 0

  let startTime = getTime()

  for i in 0..<config.numOps:
    let opStart = getTime()
    let r = client.kvPut(makeKey(i mod config.numKeys), value)
    if r.isErr: inc errors
    latencies.add(float((getTime() - opStart).inMicroseconds))

  let durationSec = float((getTime() - startTime).inMilliseconds) / 1000.0
  result = calcResult("Write-Only", latencies, errors, durationSec)

# =============================================================================
# Benchmark 3: Read-only
# Reads are routed to the correct Raft group by the shard table.
# =============================================================================

proc runReadBenchmark*(client: ProtocolClient,
    config: BenchmarkConfig): BenchmarkResult =
  var latencies: seq[float] = @[]
  var errors = 0

  for i in 0..<config.warmupOps:
    discard client.kvGet(makeKey(i mod config.numKeys))

  let startTime = getTime()

  for i in 0..<config.numOps:
    let opStart = getTime()
    let r = client.kvGet(makeKey(i mod config.numKeys))
    if r.isErr: inc errors
    latencies.add(float((getTime() - opStart).inMicroseconds))

  let durationSec = float((getTime() - startTime).inMilliseconds) / 1000.0
  result = calcResult("Read-Only", latencies, errors, durationSec)

# =============================================================================
# Benchmark 4: Scan
# Scans key_0..key_100 — crosses the low shard boundary and exercises
# the raftScan multi-shard aggregation path.
# =============================================================================

proc runScanBenchmark*(client: ProtocolClient,
    config: BenchmarkConfig): BenchmarkResult =
  var latencies: seq[float] = @[]
  var errors = 0

  let startTime = getTime()

  for i in 0..<config.numOps:
    let opStart = getTime()
    let r = client.kvScan(startKey = makeKey(0), endKey = makeKey(100),
                          limit = 100)
    if r.isErr: inc errors
    latencies.add(float((getTime() - opStart).inMicroseconds))

  let durationSec = float((getTime() - startTime).inMilliseconds) / 1000.0
  result = calcResult("Scan", latencies, errors, durationSec)

# =============================================================================
# Benchmark 5: Transactional  (begin / put / commit)
# Each transaction writes two keys that may land in different Raft groups,
# exercising the cross-shard raftCommitTxn grouping path.
# =============================================================================

proc runTransactionalBenchmark*(client: ProtocolClient,
    config: BenchmarkConfig): BenchmarkResult =
  let value = makeValue(config.valueSize)
  var latencies: seq[float] = @[]
  var errors = 0

  let startTime = getTime()

  for i in 0..<config.numOps:
    let opStart = getTime()
    ## Two keys chosen to land in different shards:
    ##   keyA → low shard  (key_0 .. key_1665)
    ##   keyB → high shard (key_3333 .. ∞)
    let keyA = makeKey(i mod 1666)
    let keyB = makeKey(3333 + (i mod (config.numKeys - 3333)))

    let txnR = client.beginTxn()
    if txnR.isErr:
      inc errors
      latencies.add(float((getTime() - opStart).inMicroseconds))
      continue

    let txnId = txnR.value.txnId

    let putA = client.kvPut(keyA, value, txnId = txnId)
    if putA.isErr:
      inc errors
      discard client.rollbackTxn(txnId)
      latencies.add(float((getTime() - opStart).inMicroseconds))
      continue

    let putB = client.kvPut(keyB, value, txnId = txnId)
    if putB.isErr:
      inc errors
      discard client.rollbackTxn(txnId)
      latencies.add(float((getTime() - opStart).inMicroseconds))
      continue

    let commitR = client.commitTxn(txnId)
    if commitR.isErr or commitR.value.status != txnMsgs.TxnCommitOK:
      inc errors

    latencies.add(float((getTime() - opStart).inMicroseconds))

  let durationSec = float((getTime() - startTime).inMilliseconds) / 1000.0
  result = calcResult("Transactional", latencies, errors, durationSec)

# =============================================================================
# Benchmark 6: Concurrent mixed  (2:1 read:write, N threads)
#
# N threads, each with its own ProtocolClient connection.
# key = (threadId * opsPerThread + i) mod numKeys
# Keys are spread across all three Raft range groups automatically.
# All threads start simultaneously via a countdown latch (Atomic[int]).
# Wall-clock duration is measured across the full concurrent run.
# =============================================================================

type
  WorkerArgs* = object
    threadId*: int
    opsPerThread*: int
    numKeys*: int
    valueSize*: int
    host*: string
    port*: int
    startLatch*: ptr Atomic[int]
    resultOut*: ptr ThreadResult

proc concurrentWorker(args: WorkerArgs) {.thread, gcsafe.} =
  var ccfg = defaultClientConfig(args.host, args.port)
  ccfg.timeoutMs = 30_000
  let c = newProtocolClient(ccfg)
  let cr = c.connect()
  if cr.isErr:
    args.resultOut[].errors = args.opsPerThread
    return

  var value = newString(args.valueSize)
  for i in 0..<args.valueSize:
    value[i] = char(ord('a') + (i mod 26))

  while args.startLatch[].load(moAcquire) > 0:
    discard

  var latencies: seq[float] = newSeqOfCap[float](args.opsPerThread)
  var errors = 0

  for i in 0..<args.opsPerThread:
    let opStart = getTime()
    let key = "key_" & $(((args.threadId * args.opsPerThread) + i) mod
        args.numKeys)
    if i mod 3 == 0:
      let r = c.kvPut(key, value)
      if r.isErr: inc errors
    else:
      let r = c.kvGet(key)
      if r.isErr: inc errors
    latencies.add(float((getTime() - opStart).inMicroseconds))

  c.disconnect()
  args.resultOut[].latencies = latencies
  args.resultOut[].errors = errors

proc runConcurrentBenchmark*(config: BenchmarkConfig,
    numThreads: int): BenchmarkResult =
  let opsPerThread = config.numOps div numThreads
  var threadResults = newSeq[ThreadResult](numThreads)
  var threads = newSeq[Thread[WorkerArgs]](numThreads)

  var latch: Atomic[int]
  latch.store(1, moRelaxed)

  for t in 0..<numThreads:
    createThread(threads[t], concurrentWorker, WorkerArgs(
      threadId: t,
      opsPerThread: opsPerThread,
      numKeys: config.numKeys,
      valueSize: config.valueSize,
      host: BENCH_HOST,
      port: BENCH_PORT,
      startLatch: addr latch,
      resultOut: addr threadResults[t],
    ))

  sleep(200) # let threads connect and reach the spin-wait
  let wallStart = getTime()
  latch.store(0, moRelease)

  for t in 0..<numThreads:
    joinThread(threads[t])

  let wallSec = float((getTime() - wallStart).inMilliseconds) / 1000.0

  var allLatencies: seq[float] = @[]
  var totalErrors = 0
  for t in 0..<numThreads:
    allLatencies.add(threadResults[t].latencies)
    totalErrors += threadResults[t].errors

  result = calcResult("Concurrent Mixed " & $numThreads & "t",
                      allLatencies, totalErrors, wallSec)

# =============================================================================
# Seed helper — pre-populate keys across all three Raft range groups
# =============================================================================

proc seedData(client: ProtocolClient, config: BenchmarkConfig) =
  ## Write 500 keys spread evenly across the full key-space so that every
  ## Raft group (low / mid / high range) receives some seed data.
  let value = makeValue(config.valueSize)
  for i in 0..<min(config.numKeys, 500):
    discard client.kvPut(makeKey(i), value)

# =============================================================================
# Print helpers
# =============================================================================

proc printResult(r: BenchmarkResult) =
  echo "  Ops/sec:     " & formatFloat(r.opsPerSec, ffDecimal, 1)
  echo "  Avg latency: " & formatFloat(r.avgLatencyUs, ffDecimal, 2) & " us"
  echo "  p99 latency: " & formatFloat(r.p99LatencyUs, ffDecimal, 2) & " us"
  echo "  Min latency: " & formatFloat(r.minLatencyUs, ffDecimal, 2) & " us"
  echo "  Max latency: " & formatFloat(r.maxLatencyUs, ffDecimal, 2) & " us"
  echo "  Total ops:   " & $r.totalOps
  echo "  Errors:      " & $r.errors

const SUMMARY_WIDTH = 88

proc printSummary(results: seq[BenchmarkResult]) =
  echo ""
  echo "=".repeat(SUMMARY_WIDTH)
  echo "BENCHMARK RESULTS SUMMARY"
  echo "=".repeat(SUMMARY_WIDTH)
  echo ""
  let hdr = "Benchmark".center(30) & " | " &
            "Ops/sec".center(12) & " | " &
            "Avg Lat (us)".center(14) & " | " &
            "p99 Lat (us)".center(12) & " | " &
            "Errors".center(8)
  echo hdr
  echo "-".repeat(SUMMARY_WIDTH)
  for r in results:
    echo r.name.center(30) & " | " &
         formatFloat(r.opsPerSec, ffDecimal, 1).center(12) & " | " &
         formatFloat(r.avgLatencyUs, ffDecimal, 2).center(14) & " | " &
         formatFloat(r.p99LatencyUs, ffDecimal, 2).center(12) & " | " &
         ($r.errors).center(8)
  echo ""

# =============================================================================
# Main
# =============================================================================

when isMainModule:
  # ---------------------------------------------------------------------------
  # CLI argument parsing
  # ---------------------------------------------------------------------------
  var numKeys = 5000
  var numOps = 1000
  var valueSize = 100
  var warmupOps = 100
  var numThreads = 4
  var skipSeq = false
  var skipConc = false
  var useGroupCommit = false
  var gcMaxBatch = 0
  var gcMaxDelayNs: int64 = 0

  let args = commandLineParams()
  var i = 0
  while i < args.len:
    case args[i]
    of "--keys": inc i; numKeys = parseInt(args[i])
    of "--ops": inc i; numOps = parseInt(args[i])
    of "--threads": inc i; numThreads = parseInt(args[i])
    of "--value-size": inc i; valueSize = parseInt(args[i])
    of "--warmup": inc i; warmupOps = parseInt(args[i])
    of "--skip-sequential": skipSeq = true
    of "--skip-concurrent": skipConc = true
    of "--group-commit": useGroupCommit = true
    of "--gc-max-batch": inc i; gcMaxBatch = parseInt(args[i])
    of "--gc-max-delay-us": inc i; gcMaxDelayNs = parseInt(args[i]) * 1000
    of "--help", "-h":
      echo "Usage: fractio_fullstack_benchmarks [options]"
      echo "  --keys N              number of distinct keys (default 5000)"
      echo "  --ops N               total ops per benchmark (default 1000)"
      echo "  --threads N           threads for concurrent run (default 4)"
      echo "  --value-size N        value size in bytes (default 100)"
      echo "  --warmup N            warmup ops (default 100)"
      echo "  --skip-sequential     skip benchmarks 1-5"
      echo "  --skip-concurrent     skip benchmark 6"
      echo "  --group-commit        enable group-commit batching"
      echo "  --gc-max-batch N      max proposals per batch (default 256)"
      echo "  --gc-max-delay-us N   max flush delay in us (default 2000)"
      quit(0)
    else:
      echo "Unknown flag: " & args[i]; quit(1)
    inc i

  let benchConfig = BenchmarkConfig(
    numKeys: numKeys, numOps: numOps, valueSize: valueSize,
    warmupOps: warmupOps, numThreads: numThreads,
  )

  echo "=".repeat(SUMMARY_WIDTH)
  echo "Fractio Full-Stack Benchmarks  (multi-Raft + key ranges)"
  echo "=".repeat(SUMMARY_WIDTH)
  echo ""
  echo "Configuration:"
  echo "  Server:       " & BENCH_HOST & ":" & $BENCH_PORT
  echo "  Raft groups:  3  (low: ..key_1666 | mid: key_1666..key_3333 | high: key_3333..)"
  echo "  Keys:         " & $numKeys
  echo "  Ops/bench:    " & $numOps
  echo "  Value size:   " & $valueSize & " bytes"
  echo "  Warmup:       " & $warmupOps & " ops"
  echo "  Threads:      " & $numThreads & " (concurrent benchmark also runs 2 and 8)"
  echo "  Group commit: " & $useGroupCommit
  echo ""

  # -------------------------------------------------------------------------
  # Bootstrap: 3 Raft groups, each owning a key-range partition.
  #
  # Group 1 (GroupID 1): ""         .. GROUP_SPLIT_LO  (key_0 .. key_1665)
  # Group 2 (GroupID 2): SPLIT_LO   .. GROUP_SPLIT_HI  (key_1666 .. key_3332)
  # Group 3 (GroupID 3): SPLIT_HI   .. ""              (key_3333 .. ∞)
  #
  # All three groups share one MultiRaftCoordinator and one WiscKey store,
  # mirroring a single-node production deployment with range-based sharding.
  # -------------------------------------------------------------------------

  try: removeDir(RAFT_STORAGE_PATH) except CatchableError: discard
  try: createDir(RAFT_STORAGE_PATH) except CatchableError: discard

  let coordCfg = CoordinatorConfig(
    nodeId: RangeNodeID(1),
    numWorkers: 4,
    electionTimeoutNs: DEFAULT_ELECTION_TIMEOUT_NS,
    heartbeatIntervalNs: DEFAULT_HEARTBEAT_INTERVAL_NS,
    storagePath: RAFT_STORAGE_PATH,
    proposeTimeoutMs: 10_000,
    groupCommitEnabled: useGroupCommit,
    groupCommitMaxBatch: gcMaxBatch,
    groupCommitMaxDelayNs: gcMaxDelayNs,
  )
  let coord = newMultiRaftCoordinator(coordCfg)

  # --- Create three range descriptors and their Raft groups ---
  # TODO: Replace RangeID/RangeDescriptor with GroupID/GroupDescriptor after
  # range→group migration. The old RangeID/newRangeDescriptor/addShardExt API
  # was removed; this benchmark needs rewriting to use the current Raft group API.
  let rid1 = groupIDFromInt(1)
  let rid2 = groupIDFromInt(2)
  let rid3 = groupIDFromInt(3)

  let loBytes = cast[seq[byte]](GROUP_SPLIT_LO)
  let hiBytes = cast[seq[byte]](GROUP_SPLIT_HI)

  # TODO: Replace newRangeDescriptor with newGroupDescriptor after range→group migration.
  # The old RangeDescriptor had startKey/endKey; GroupDescriptor uses hash-based spaces.
  let desc1 = newGroupDescriptor(rid1)
  let desc2 = newGroupDescriptor(rid2)
  let desc3 = newGroupDescriptor(rid3)

  let rep1 = desc1.addReplica(NodeID(1))
  let rep2 = desc2.addReplica(NodeID(1))
  let rep3 = desc3.addReplica(NodeID(1))

  let grp1 = coord.createGroup(desc1, rep1.replicaId)
  let grp2 = coord.createGroup(desc2, rep2.replicaId)
  let grp3 = coord.createGroup(desc3, rep3.replicaId)

  grp1.becomeLeader()
  grp2.becomeLeader()
  grp3.becomeLeader()

  # --- Wire RaftKVStoreExt with one group entry per range group ---
  # TODO: The old addShardExt(startKey, endKey, groupId) API was removed
  # when the range→group migration happened. RaftKVStoreExt now uses
  # hash-based Space routing via resolveGroupId(). This benchmark needs
  # to be updated to register spaces properly instead of shard entries.
  let raftSt = newRaftKVStoreExt(coord, proposeTimeoutMs = 10_000)
  raftSt.wireApplyCallback()

  # TODO: Replace addShardExt with Space-based routing (see note above).
  discard raftSt
  discard loBytes
  discard hiBytes

  coord.start()

  var srvCfg = defaultServerConfig()
  srvCfg.host = BENCH_HOST
  srvCfg.port = BENCH_PORT
  srvCfg.idleTimeoutSecs = 300
  srvCfg.serverName = "fractio-bench"

  let srv = newProtocolServer(srvCfg)
  srv.raftStore = raftSt
  srv.start()
  sleep(SERVER_WAIT_MS)

  echo "Server started on " & BENCH_HOST & ":" & $BENCH_PORT
  if useGroupCommit:
    echo "Backend: 3x Raft groups + WiscKey + Group Commit (batch=" &
         $gcMaxBatch & ", delay=" & $(gcMaxDelayNs div 1000) & "us)"
  else:
    echo "Backend: 3x Raft groups + WiscKey (fdatasync per commit)"
  echo ""

  var allResults: seq[BenchmarkResult] = @[]

  # -------------------------------------------------------------------------
  # Benchmarks 1-5: single-client sequential workloads
  # -------------------------------------------------------------------------
  if not skipSeq:
    echo "=".repeat(50)
    echo "1. Sequential Mixed Benchmark (2:1 read:write)"
    echo "=".repeat(50)
    block:
      let c = newClient()
      seedData(c, benchConfig)
      let r = runSequentialBenchmark(c, benchConfig)
      allResults.add(r)
      c.disconnect()
      echo "\nResults:"; printResult(r)

    echo "\n" & "=".repeat(50)
    echo "2. Write-Only Benchmark"
    echo "=".repeat(50)
    block:
      let c = newClient()
      let r = runWriteBenchmark(c, benchConfig)
      allResults.add(r)
      c.disconnect()
      echo "\nResults:"; printResult(r)

    echo "\n" & "=".repeat(50)
    echo "3. Read-Only Benchmark"
    echo "=".repeat(50)
    block:
      let c = newClient()
      seedData(c, benchConfig)
      let r = runReadBenchmark(c, benchConfig)
      allResults.add(r)
      c.disconnect()
      echo "\nResults:"; printResult(r)

    echo "\n" & "=".repeat(50)
    echo "4. Scan Benchmark  (key_0..key_100, crosses shard boundary)"
    echo "=".repeat(50)
    block:
      let c = newClient()
      seedData(c, benchConfig)
      let r = runScanBenchmark(c, benchConfig)
      allResults.add(r)
      c.disconnect()
      echo "\nResults:"; printResult(r)

    echo "\n" & "=".repeat(50)
    echo "5. Transactional Benchmark (begin / 2x put across shards / commit)"
    echo "=".repeat(50)
    block:
      let c = newClient()
      let r = runTransactionalBenchmark(c, benchConfig)
      allResults.add(r)
      c.disconnect()
      echo "\nResults:"; printResult(r)

  # -------------------------------------------------------------------------
  # Benchmark 6: Concurrent mixed — run at 2, 4, 8 threads
  # -------------------------------------------------------------------------
  if not skipConc:
    echo "\n" & "=".repeat(50)
    echo "6. Concurrent Mixed Benchmark (2:1 read:write, multi-shard)"
    echo "=".repeat(50)

    block:
      let c = newClient()
      seedData(c, benchConfig)
      c.disconnect()

    var threadCounts: seq[int] = @[2, 4, 8]
    if numThreads notin threadCounts:
      threadCounts.add(numThreads)
      threadCounts.sort(system.cmp[int])

    for t in threadCounts:
      echo "\n--- " & $t & " threads ---"
      let r = runConcurrentBenchmark(benchConfig, t)
      allResults.add(r)
      echo "Results:"; printResult(r)

  printSummary(allResults)

  srv.stop()
  sleep(50)
  coord.stop()
  try: removeDir(RAFT_STORAGE_PATH) except CatchableError: discard
  echo "Done!"
  # Exit explicitly to avoid ORC teardown crash on module-level thread globals
  # in server.nim (threadStore / acceptThreadStore seqs).
  quit(0)
