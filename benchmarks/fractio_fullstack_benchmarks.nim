# Fractio Full-Stack Benchmarks
#
# Exercises the complete network path:
#   ProtocolServer (in-process) ← TCP → ProtocolClient
#
# Benchmarks:
#   1. Sequential mixed        (2:1 read:write, single client)
#   2. Write-only              (single client)
#   3. Read-only               (single client)
#   4. Scan                    (single client)
#   5. Transactional           (begin / put / commit, single client)
#   6. Concurrent mixed        (2:1 read:write, N clients in parallel)
#      Mirrors the workload in db_benchmarks.py for PostgreSQL/MySQL/SQLite.
#      Each thread owns its own ProtocolClient connection.  Key space is
#      partitioned identically to the Python benchmark:
#        key = (threadId * opsPerThread + i) mod numKeys
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
import fractio/distributed/range/types as rangeTypes

# =============================================================================
# Constants
# =============================================================================

const
  BENCH_PORT = 29000
  BENCH_HOST = "127.0.0.1"
  SERVER_WAIT_MS = 120 ## ms to sleep after server.start() before connecting
  RAFT_STORAGE_PATH = "/tmp/fractio_bench_raft"

# =============================================================================
# Shared types (compatible with fractio_benchmarks.nim)
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
    # p99: sort a copy and take the 99th-percentile element
    var sorted = latencies
    sorted.sort(system.cmp[float])
    let p99idx = max(0, int(float(sorted.len) * 0.99) - 1)
    result.p99LatencyUs = sorted[p99idx]

# =============================================================================
# Benchmark 1: Sequential mixed  (2:1 read:write)
# =============================================================================

proc runSequentialBenchmark*(client: ProtocolClient,
    config: BenchmarkConfig): BenchmarkResult =
  let value = makeValue(config.valueSize)
  var latencies: seq[float] = @[]
  var errors = 0

  # Warmup — not timed
  for i in 0..<config.warmupOps:
    let key = makeKey(i mod config.numKeys)
    discard client.kvGet(key)

  let startTime = getTime()

  for i in 0..<config.numOps:
    let opStart = getTime()
    let key = makeKey(i mod config.numKeys)

    if i mod 3 == 0:
      # Write
      let r = client.kvPut(key, value)
      if r.isErr: inc errors
    else:
      # Read
      let r = client.kvGet(key)
      if r.isErr: inc errors

    let latencyUs = float((getTime() - opStart).inMicroseconds)
    latencies.add(latencyUs)

  let durationSec = float((getTime() - startTime).inMilliseconds) / 1000.0
  result = calcResult("Sequential Mixed", latencies, errors, durationSec)

# =============================================================================
# Benchmark 2: Write-only
# =============================================================================

proc runWriteBenchmark*(client: ProtocolClient,
    config: BenchmarkConfig): BenchmarkResult =
  let value = makeValue(config.valueSize)
  var latencies: seq[float] = @[]
  var errors = 0

  let startTime = getTime()

  for i in 0..<config.numOps:
    let opStart = getTime()
    let key = makeKey(i mod config.numKeys)

    let r = client.kvPut(key, value)
    if r.isErr: inc errors

    latencies.add(float((getTime() - opStart).inMicroseconds))

  let durationSec = float((getTime() - startTime).inMilliseconds) / 1000.0
  result = calcResult("Write-Only", latencies, errors, durationSec)

# =============================================================================
# Benchmark 3: Read-only
# =============================================================================

proc runReadBenchmark*(client: ProtocolClient,
    config: BenchmarkConfig): BenchmarkResult =
  var latencies: seq[float] = @[]
  var errors = 0

  # Warmup
  for i in 0..<config.warmupOps:
    let key = makeKey(i mod config.numKeys)
    discard client.kvGet(key)

  let startTime = getTime()

  for i in 0..<config.numOps:
    let opStart = getTime()
    let key = makeKey(i mod config.numKeys)

    let r = client.kvGet(key)
    if r.isErr: inc errors

    latencies.add(float((getTime() - opStart).inMicroseconds))

  let durationSec = float((getTime() - startTime).inMilliseconds) / 1000.0
  result = calcResult("Read-Only", latencies, errors, durationSec)

# =============================================================================
# Benchmark 4: Scan
# =============================================================================

proc runScanBenchmark*(client: ProtocolClient,
    config: BenchmarkConfig): BenchmarkResult =
  ## Each operation is one Scan over the first 100 keys.
  var latencies: seq[float] = @[]
  var errors = 0

  let startTime = getTime()

  for i in 0..<config.numOps:
    let opStart = getTime()
    let startKey = makeKey(0)
    let endKey = makeKey(100)

    let r = client.kvScan(startKey = startKey, endKey = endKey, limit = 100)
    if r.isErr: inc errors

    latencies.add(float((getTime() - opStart).inMicroseconds))

  let durationSec = float((getTime() - startTime).inMilliseconds) / 1000.0
  result = calcResult("Scan", latencies, errors, durationSec)

# =============================================================================
# Benchmark 5: Transactional  (begin / put / commit)
# =============================================================================

proc runTransactionalBenchmark*(client: ProtocolClient,
    config: BenchmarkConfig): BenchmarkResult =
  ## Each "operation" is one full transaction: begin → put → commit.
  let value = makeValue(config.valueSize)
  var latencies: seq[float] = @[]
  var errors = 0

  let startTime = getTime()

  for i in 0..<config.numOps:
    let opStart = getTime()
    let key = makeKey(i mod config.numKeys)

    let txnR = client.beginTxn()
    if txnR.isErr:
      inc errors
      latencies.add(float((getTime() - opStart).inMicroseconds))
      continue

    let txnId = txnR.value.txnId

    let putR = client.kvPut(key, value, txnId = txnId)
    if putR.isErr:
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
# Mirrors db_benchmarks.py exactly:
#   - N threads, each with its own ProtocolClient connection
#   - ops_per_thread = numOps div numThreads
#   - key = (threadId * opsPerThread + i) mod numKeys
#   - every 3rd op is a Put, the rest are Gets
#   - all threads start simultaneously (countdown latch via Atomic[int])
#   - wall-clock duration measured across the full concurrent run
# =============================================================================

type
  WorkerArgs* = object
    threadId*: int
    opsPerThread*: int
    numKeys*: int
    valueSize*: int
    host*: string
    port*: int
    ## Countdown latch: main decrements to 0 after spawning all threads,
    ## each worker spins until it reads 0, then starts.
    startLatch*: ptr Atomic[int]
    ## Output written by the worker (main reads after joinThread).
    resultOut*: ptr ThreadResult

proc concurrentWorker(args: WorkerArgs) {.thread, gcsafe.} =
  ## Thread entry point: connects its own client, waits for the latch,
  ## runs the mixed workload, writes results into args.resultOut.
  var ccfg = defaultClientConfig(args.host, args.port)
  ccfg.timeoutMs = 30_000
  let c = newProtocolClient(ccfg)
  let cr = c.connect()
  if cr.isErr:
    args.resultOut[].errors = args.opsPerThread
    return

  let value = block:
    var v = newString(args.valueSize)
    for i in 0..<args.valueSize:
      v[i] = char(ord('a') + (i mod 26))
    v

  # Spin-wait for start latch to reach 0 (all threads ready)
  while args.startLatch[].load(moAcquire) > 0:
    discard

  var latencies: seq[float] = newSeqOfCap[float](args.opsPerThread)
  var errors = 0

  for i in 0..<args.opsPerThread:
    let opStart = getTime()
    let key = "key_" & $(((args.threadId * args.opsPerThread) +
        i) mod args.numKeys)

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
  ## Spin up numThreads clients simultaneously, run the mixed workload,
  ## collect latencies across all threads, report wall-clock throughput.
  let opsPerThread = config.numOps div numThreads

  # Allocate per-thread result storage on the heap so threads can write safely
  var threadResults = newSeq[ThreadResult](numThreads)
  var threads = newSeq[Thread[WorkerArgs]](numThreads)

  var latch: Atomic[int]
  latch.store(1, moRelaxed) # workers spin until we store 0

  for t in 0..<numThreads:
    let args = WorkerArgs(
      threadId: t,
      opsPerThread: opsPerThread,
      numKeys: config.numKeys,
      valueSize: config.valueSize,
      host: BENCH_HOST,
      port: BENCH_PORT,
      startLatch: addr latch,
      resultOut: addr threadResults[t],
    )
    createThread(threads[t], concurrentWorker, args)

  # Give threads a moment to connect and reach the spin-wait, then release
  sleep(120)
  let wallStart = getTime()
  latch.store(0, moRelease)

  for t in 0..<numThreads:
    joinThread(threads[t])

  let wallSec = float((getTime() - wallStart).inMilliseconds) / 1000.0

  # Merge all per-thread latencies
  var allLatencies: seq[float] = @[]
  var totalErrors = 0
  for t in 0..<numThreads:
    allLatencies.add(threadResults[t].latencies)
    totalErrors += threadResults[t].errors

  result = calcResult(
    "Concurrent Mixed " & $numThreads & "t",
    allLatencies,
    totalErrors,
    wallSec,
  )

# =============================================================================
# Seed helper — pre-populate keys so reads have data to find
# =============================================================================

proc seedData(client: ProtocolClient, config: BenchmarkConfig) =
  let value = makeValue(config.valueSize)
  for i in 0..<min(config.numKeys, 500):
    let key = makeKey(i)
    discard client.kvPut(key, value)

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

# Width: 30 + 3 + 12 + 3 + 14 + 3 + 12 + 3 + 8 = 88
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
    let row = r.name.center(30) & " | " &
              formatFloat(r.opsPerSec, ffDecimal, 1).center(12) & " | " &
              formatFloat(r.avgLatencyUs, ffDecimal, 2).center(14) & " | " &
              formatFloat(r.p99LatencyUs, ffDecimal, 2).center(12) & " | " &
              ($r.errors).center(8)
    echo row
  echo ""

# =============================================================================
# Main
# =============================================================================

when isMainModule:
  # ---------------------------------------------------------------------------
  # CLI argument parsing (mirrors db_benchmarks.py flags)
  # ---------------------------------------------------------------------------
  var numKeys = 5000
  var numOps = 1000
  var valueSize = 100
  var warmupOps = 100
  var numThreads = 4
  var skipSeq = false
  var skipConc = false
  var useGroupCommit = false
  var gcMaxBatch = 0 ## 0 → use library default (256)
  var gcMaxDelayNs: int64 = 0 ## 0 → use library default (2 ms)

  let args = commandLineParams()
  var i = 0
  while i < args.len:
    case args[i]
    of "--keys":
      inc i; numKeys = parseInt(args[i])
    of "--ops":
      inc i; numOps = parseInt(args[i])
    of "--threads":
      inc i; numThreads = parseInt(args[i])
    of "--value-size":
      inc i; valueSize = parseInt(args[i])
    of "--warmup":
      inc i; warmupOps = parseInt(args[i])
    of "--skip-sequential":
      skipSeq = true
    of "--skip-concurrent":
      skipConc = true
    of "--group-commit":
      useGroupCommit = true
    of "--gc-max-batch":
      inc i; gcMaxBatch = parseInt(args[i])
    of "--gc-max-delay-us":
      inc i; gcMaxDelayNs = parseInt(args[i]) * 1000
    of "--help", "-h":
      echo "Usage: fractio_fullstack_benchmarks [options]"
      echo "  --keys N            number of distinct keys (default 5000)"
      echo "  --ops N             total ops per benchmark (default 1000)"
      echo "  --threads N         threads for concurrent run (default 4)"
      echo "  --value-size N      value size in bytes (default 100)"
      echo "  --warmup N          warmup ops (default 100)"
      echo "  --skip-sequential   skip benchmarks 1-5"
      echo "  --skip-concurrent   skip benchmark 6 (concurrent mixed)"
      echo "  --group-commit      enable group commit batching (target 500-5000 writes/sec)"
      echo "  --gc-max-batch N    max proposals per group-commit batch (default 256)"
      echo "  --gc-max-delay-us N max delay before flush in microseconds (default 2000)"
      quit(0)
    else:
      echo "Unknown flag: " & args[i]
      quit(1)
    inc i

  let benchConfig = BenchmarkConfig(
    numKeys: numKeys,
    numOps: numOps,
    valueSize: valueSize,
    warmupOps: warmupOps,
    numThreads: numThreads,
  )

  echo "=".repeat(SUMMARY_WIDTH)
  echo "Fractio Full-Stack Benchmarks"
  echo "=".repeat(SUMMARY_WIDTH)
  echo ""
  echo "Configuration:"
  echo "  Server:       " & BENCH_HOST & ":" & $BENCH_PORT
  echo "  Keys:         " & $numKeys
  echo "  Ops/bench:    " & $numOps
  echo "  Value size:   " & $valueSize & " bytes"
  echo "  Warmup:       " & $warmupOps & " ops"
  echo "  Threads:      " & $numThreads & " (concurrent benchmark also runs 2 and 8)"
  echo "  Group commit: " & $useGroupCommit
  echo ""

  # -------------------------------------------------------------------------
  # Start server with Raft + WiscKey (syncWrites=true) backend
  #
  # Write path:
  #   client kvPut → server handleBuiltinKV (raftStore branch)
  #     → raftPut → proposeAndWait (Raft consensus, quorum=1)
  #     → applyBatchToSM → WiscKey.put (fdatasync) + in-memory SM
  #
  # This puts real fsync I/O on every write, making the comparison with
  # PostgreSQL/MySQL/SQLite apples-to-apples.
  # -------------------------------------------------------------------------

  # Clean up any leftover storage from a previous run
  try: removeDir(RAFT_STORAGE_PATH) except CatchableError: discard
  try: createDir(RAFT_STORAGE_PATH) except CatchableError: discard

  # Build coordinator (WiscKey opened with syncWrites=true inside newMultiRaftCoordinator)
  let coordCfg = CoordinatorConfig(
    nodeId: RangeNodeID(1),
    numWorkers: 2,
    electionTimeoutNs: DEFAULT_ELECTION_TIMEOUT_NS,
    heartbeatIntervalNs: DEFAULT_HEARTBEAT_INTERVAL_NS,
    storagePath: RAFT_STORAGE_PATH,
    proposeTimeoutMs: 10_000,
    groupCommitEnabled: useGroupCommit,
    groupCommitMaxBatch: gcMaxBatch,
    groupCommitMaxDelayNs: gcMaxDelayNs,
  )
  let coord = newMultiRaftCoordinator(coordCfg)

  # Bootstrap a single shard covering the full key-space
  let rid = RangeID(1)
  let desc = newRangeDescriptor(rid, @[], @[])
  let rep = desc.addReplica(RangeNodeID(1))
  let group = coord.createGroup(desc, rep.replicaId)
  group.becomeLeader()

  # Create RaftKVStoreExt and wire the apply callback BEFORE coord.start()
  let raftSt = newRaftKVStoreExt(coord, proposeTimeoutMs = 10_000)
  raftSt.wireApplyCallback()
  raftSt.bootstrapSingleShardExt(rid)

  coord.start()

  # Protocol server
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
    echo "Backend: Raft + WiscKey + Group Commit (batch up to " &
      $gcMaxBatch & " writes per fsync, delay " & $(gcMaxDelayNs div 1000) & " us)"
  else:
    echo "Backend: Raft + WiscKey (LevelDB syncWrites=true / fdatasync per commit)"
  echo ""

  var allResults: seq[BenchmarkResult] = @[]

  # -------------------------------------------------------------------------
  # Benchmarks 1-5: single-client sequential workloads
  # -------------------------------------------------------------------------
  if not skipSeq:
    # Benchmark 1: Sequential mixed
    echo "=".repeat(50)
    echo "1. Sequential Mixed Benchmark (2:1 read:write)"
    echo "=".repeat(50)
    block:
      let c = newClient()
      seedData(c, benchConfig)
      let r = runSequentialBenchmark(c, benchConfig)
      allResults.add(r)
      c.disconnect()
      echo "\nResults:"
      printResult(r)

    # Benchmark 2: Write-only
    echo "\n" & "=".repeat(50)
    echo "2. Write-Only Benchmark"
    echo "=".repeat(50)
    block:
      let c = newClient()
      let r = runWriteBenchmark(c, benchConfig)
      allResults.add(r)
      c.disconnect()
      echo "\nResults:"
      printResult(r)

    # Benchmark 3: Read-only
    echo "\n" & "=".repeat(50)
    echo "3. Read-Only Benchmark"
    echo "=".repeat(50)
    block:
      let c = newClient()
      seedData(c, benchConfig)
      let r = runReadBenchmark(c, benchConfig)
      allResults.add(r)
      c.disconnect()
      echo "\nResults:"
      printResult(r)

    # Benchmark 4: Scan
    echo "\n" & "=".repeat(50)
    echo "4. Scan Benchmark"
    echo "=".repeat(50)
    block:
      let c = newClient()
      seedData(c, benchConfig)
      let r = runScanBenchmark(c, benchConfig)
      allResults.add(r)
      c.disconnect()
      echo "\nResults:"
      printResult(r)

    # Benchmark 5: Transactional
    echo "\n" & "=".repeat(50)
    echo "5. Transactional Benchmark (begin/put/commit)"
    echo "=".repeat(50)
    block:
      let c = newClient()
      let r = runTransactionalBenchmark(c, benchConfig)
      allResults.add(r)
      c.disconnect()
      echo "\nResults:"
      printResult(r)

  # -------------------------------------------------------------------------
  # Benchmark 6: Concurrent mixed  (mirrors db_benchmarks.py)
  # Run at thread counts 2, 4, 8 just like the Python driver does.
  # If the user passed --threads T we include T in the set (deduped).
  # -------------------------------------------------------------------------
  if not skipConc:
    echo "\n" & "=".repeat(50)
    echo "6. Concurrent Mixed Benchmark (2:1 read:write)"
    echo "   Mirrors db_benchmarks.py PostgreSQL/MySQL/SQLite workload"
    echo "=".repeat(50)

    # Seed once with a dedicated client before spawning workers
    block:
      let c = newClient()
      seedData(c, benchConfig)
      c.disconnect()

    # Build the set of thread counts to run, always including 2, 4, 8
    var threadCounts: seq[int] = @[]
    for t in [2, 4, 8]:
      threadCounts.add(t)
    if numThreads notin threadCounts:
      threadCounts.add(numThreads)
      threadCounts.sort(system.cmp[int])

    for t in threadCounts:
      echo "\n--- " & $t & " threads ---"
      let r = runConcurrentBenchmark(benchConfig, t)
      allResults.add(r)
      echo "Results:"
      printResult(r)

  # -------------------------------------------------------------------------
  # Summary table
  # -------------------------------------------------------------------------
  printSummary(allResults)

  # -------------------------------------------------------------------------
  # Shutdown
  # -------------------------------------------------------------------------
  srv.stop()
  sleep(50)
  coord.stop()
  try: removeDir(RAFT_STORAGE_PATH) except CatchableError: discard
  echo "Done!"
  # Exit explicitly to avoid ORC teardown crash on module-level thread globals
  # in server.nim (threadStore / acceptThreadStore seqs).
  quit(0)
