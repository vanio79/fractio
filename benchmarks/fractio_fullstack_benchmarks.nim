# Fractio Full-Stack Benchmarks
#
# Exercises the complete network path:
#   ProtocolServer (in-process) ← TCP → ProtocolClient
#
# Benchmarks:
#   1. Sequential mixed   (2:1 read:write)
#   2. Write-only
#   3. Read-only
#   4. Scan
#   5. Transactional      (begin / put / commit)
#
# The server is started once and shared across all benchmark runs.
# The client reconnects between benchmarks so each run starts with a
# fresh connection and a clean request-ID counter.
#
# Port: 29000  (well clear of all protocol test ports ≤ 20499)

import std/[os, times, strutils, math]
import fractio/protocol/server
import fractio/protocol/client
import fractio/protocol/types
import fractio/protocol/messages/txn as txnMsgs

# =============================================================================
# Constants
# =============================================================================

const
  BENCH_PORT = 29000
  BENCH_HOST = "127.0.0.1"
  NUM_KEYS = 2000
  VALUE_SIZE = 100
  WARMUP_OPS = 50
  BENCH_OPS = 500
  SERVER_WAIT_MS = 80 ## ms to sleep after server.start() before connecting

# =============================================================================
# Shared types (compatible with fractio_benchmarks.nim)
# =============================================================================

type
  BenchmarkConfig* = object
    numKeys*: int
    numOps*: int
    valueSize*: int
    warmupOps*: int

  BenchmarkResult* = object
    name*: string
    opsPerSec*: float
    avgLatencyUs*: float
    minLatencyUs*: float
    maxLatencyUs*: float
    totalOps*: int
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
  echo "  Min latency: " & formatFloat(r.minLatencyUs, ffDecimal, 2) & " us"
  echo "  Max latency: " & formatFloat(r.maxLatencyUs, ffDecimal, 2) & " us"
  echo "  Total ops:   " & $r.totalOps
  echo "  Errors:      " & $r.errors

proc printSummary(results: seq[BenchmarkResult]) =
  echo ""
  echo "=".repeat(64)
  echo "BENCHMARK RESULTS SUMMARY"
  echo "=".repeat(64)
  echo ""
  let hdr = "Benchmark".center(22) & " | " &
            "Ops/sec".center(12) & " | " &
            "Avg Lat (us)".center(14) & " | " &
            "Errors".center(8)
  echo hdr
  echo "-".repeat(64)
  for r in results:
    let row = r.name.center(22) & " | " &
              formatFloat(r.opsPerSec, ffDecimal, 1).center(12) & " | " &
              formatFloat(r.avgLatencyUs, ffDecimal, 2).center(14) & " | " &
              ($r.errors).center(8)
    echo row
  echo ""

# =============================================================================
# Main
# =============================================================================

when isMainModule:
  echo "========================================"
  echo "Fractio Full-Stack Benchmarks"
  echo "========================================"
  echo ""
  echo "Configuration:"
  echo "  Server:    " & BENCH_HOST & ":" & $BENCH_PORT
  echo "  Keys:      " & $NUM_KEYS
  echo "  Ops/bench: " & $BENCH_OPS
  echo "  Value size:" & $VALUE_SIZE & " bytes"
  echo "  Warmup:    " & $WARMUP_OPS & " ops"
  echo ""

  # -------------------------------------------------------------------------
  # Start server (single instance, shared across all benchmarks)
  # -------------------------------------------------------------------------
  var cfg = defaultServerConfig()
  cfg.host = BENCH_HOST
  cfg.port = BENCH_PORT
  cfg.idleTimeoutSecs = 300
  cfg.serverName = "fractio-bench"

  let srv = newProtocolServer(cfg)
  srv.start()
  sleep(SERVER_WAIT_MS) # allow accept loop to come up

  echo "Server started on " & BENCH_HOST & ":" & $BENCH_PORT
  echo ""

  let benchConfig = BenchmarkConfig(
    numKeys: NUM_KEYS,
    numOps: BENCH_OPS,
    valueSize: VALUE_SIZE,
    warmupOps: WARMUP_OPS,
  )

  var allResults: seq[BenchmarkResult] = @[]

  # -------------------------------------------------------------------------
  # Benchmark 1: Sequential mixed
  # -------------------------------------------------------------------------
  echo "=".repeat(50)
  echo "Sequential Mixed Benchmark (2:1 read:write)"
  echo "=".repeat(50)

  block:
    let c = newClient()
    seedData(c, benchConfig)
    let r = runSequentialBenchmark(c, benchConfig)
    allResults.add(r)
    c.disconnect()
    echo "\nResults:"
    printResult(r)

  # -------------------------------------------------------------------------
  # Benchmark 2: Write-only
  # -------------------------------------------------------------------------
  echo "\n" & "=".repeat(50)
  echo "Write-Only Benchmark"
  echo "=".repeat(50)

  block:
    let c = newClient()
    let r = runWriteBenchmark(c, benchConfig)
    allResults.add(r)
    c.disconnect()
    echo "\nResults:"
    printResult(r)

  # -------------------------------------------------------------------------
  # Benchmark 3: Read-only
  # -------------------------------------------------------------------------
  echo "\n" & "=".repeat(50)
  echo "Read-Only Benchmark"
  echo "=".repeat(50)

  block:
    let c = newClient()
    seedData(c, benchConfig)
    let r = runReadBenchmark(c, benchConfig)
    allResults.add(r)
    c.disconnect()
    echo "\nResults:"
    printResult(r)

  # -------------------------------------------------------------------------
  # Benchmark 4: Scan
  # -------------------------------------------------------------------------
  echo "\n" & "=".repeat(50)
  echo "Scan Benchmark"
  echo "=".repeat(50)

  block:
    let c = newClient()
    seedData(c, benchConfig)
    let r = runScanBenchmark(c, benchConfig)
    allResults.add(r)
    c.disconnect()
    echo "\nResults:"
    printResult(r)

  # -------------------------------------------------------------------------
  # Benchmark 5: Transactional
  # -------------------------------------------------------------------------
  echo "\n" & "=".repeat(50)
  echo "Transactional Benchmark (begin/put/commit)"
  echo "=".repeat(50)

  block:
    let c = newClient()
    let r = runTransactionalBenchmark(c, benchConfig)
    allResults.add(r)
    c.disconnect()
    echo "\nResults:"
    printResult(r)

  # -------------------------------------------------------------------------
  # Summary
  # -------------------------------------------------------------------------
  printSummary(allResults)

  # -------------------------------------------------------------------------
  # Shutdown
  # -------------------------------------------------------------------------
  srv.stop()
  sleep(50)
  echo "Done!"
  # Exit explicitly to avoid ORC teardown crash on module-level thread globals
  # in server.nim (threadStore / acceptThreadStore seqs).
  quit(0)
