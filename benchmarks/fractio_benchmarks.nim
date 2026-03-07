# Transactional Benchmarks for Fractio Storage Layer
# Compares sequential workloads using WiscKey storage backend

import std/[os, times, strutils, math]
import fractio/storage/backend as storage_backend
import fractio/storage/wisckey_backend

# =============================================================================
# Benchmark Configuration
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

const
  NUM_KEYS = 5000
  VALUE_SIZE = 100
  WARMUP_OPS = 100

# =============================================================================
# Key/Value Generation
# =============================================================================

proc makeKey(id: int): string =
  result = "key_" & $id

proc makeValue(size: int): string =
  result = newString(size)
  for i in 0..<size:
    result[i] = char(ord('a') + (i mod 26))

# =============================================================================
# Sequential Benchmark
# =============================================================================

proc runSequentialBenchmark*(backend: storage_backend.StorageBackend,
                             config: BenchmarkConfig): BenchmarkResult =
  ## Run sequential (single-threaded) benchmark
  result.name = "Sequential"

  let value = makeValue(config.valueSize)
  var latencies: seq[float] = @[]

  # Warmup
  for i in 0..<config.warmupOps:
    let key = makeKey(i mod config.numKeys)
    discard backend.get(key)

  # Actual benchmark
  let startTime = getTime()

  for i in 0..<config.numOps:
    let opStart = getTime()

    let key = makeKey(i mod config.numKeys)

    # Mix of reads and writes (1/3 writes, 2/3 reads)
    if i mod 3 == 0:
      if not backend.put(key, value):
        inc result.errors
    else:
      discard backend.get(key)

    let opEnd = getTime()
    let latencyUs = float((opEnd - opStart).inMicroseconds)
    latencies.add(latencyUs)
    inc result.totalOps

  let endTime = getTime()
  let durationSec = float((endTime - startTime).inMilliseconds) / 1000.0

  result.opsPerSec = float(result.totalOps) / durationSec
  if latencies.len > 0:
    result.avgLatencyUs = sum(latencies) / float(latencies.len)
    result.minLatencyUs = min(latencies)
    result.maxLatencyUs = max(latencies)

# =============================================================================
# Write-Only Benchmark
# =============================================================================

proc runWriteBenchmark*(backend: storage_backend.StorageBackend,
                        config: BenchmarkConfig): BenchmarkResult =
  ## Run write-only benchmark
  result.name = "Write-Only"

  let value = makeValue(config.valueSize)
  var latencies: seq[float] = @[]

  let startTime = getTime()

  for i in 0..<config.numOps:
    let opStart = getTime()

    let key = makeKey(i mod config.numKeys)

    if not backend.put(key, value):
      inc result.errors

    let opEnd = getTime()
    let latencyUs = float((opEnd - opStart).inMicroseconds)
    latencies.add(latencyUs)
    inc result.totalOps

  let endTime = getTime()
  let durationSec = float((endTime - startTime).inMilliseconds) / 1000.0

  result.opsPerSec = float(result.totalOps) / durationSec
  if latencies.len > 0:
    result.avgLatencyUs = sum(latencies) / float(latencies.len)
    result.minLatencyUs = min(latencies)
    result.maxLatencyUs = max(latencies)

# =============================================================================
# Read-Only Benchmark
# =============================================================================

proc runReadBenchmark*(backend: storage_backend.StorageBackend,
                       config: BenchmarkConfig): BenchmarkResult =
  ## Run read-only benchmark
  result.name = "Read-Only"

  var latencies: seq[float] = @[]

  # Warmup
  for i in 0..<config.warmupOps:
    let key = makeKey(i mod config.numKeys)
    discard backend.get(key)

  let startTime = getTime()

  for i in 0..<config.numOps:
    let opStart = getTime()

    let key = makeKey(i mod config.numKeys)
    discard backend.get(key)

    let opEnd = getTime()
    let latencyUs = float((opEnd - opStart).inMicroseconds)
    latencies.add(latencyUs)
    inc result.totalOps

  let endTime = getTime()
  let durationSec = float((endTime - startTime).inMilliseconds) / 1000.0

  result.opsPerSec = float(result.totalOps) / durationSec
  if latencies.len > 0:
    result.avgLatencyUs = sum(latencies) / float(latencies.len)
    result.minLatencyUs = min(latencies)
    result.maxLatencyUs = max(latencies)

# =============================================================================
# Setup and Teardown
# =============================================================================

proc setupStorage*(testPath: string): storage_backend.StorageBackend =
  ## Create and open storage backend
  removeDir(testPath)
  createDir(testPath)

  let backend = newWiscKeyBackend(storage_backend.StorageConfig(
    path: testPath,
    createIfMissing: true,
    syncWrites: false
  ))

  if not backend.open(storage_backend.StorageConfig(
    path: testPath,
    createIfMissing: true,
    syncWrites: false
  )):
    quit("Failed to open storage backend")

  return backend

proc teardownStorage*(backend: storage_backend.StorageBackend,
    testPath: string) =
  ## Close and cleanup storage
  backend.close()
  discard backend.destroy()
  removeDir(testPath)

proc seedData*(backend: storage_backend.StorageBackend,
    config: BenchmarkConfig) =
  ## Seed initial data
  let value = makeValue(config.valueSize)
  for i in 0..<min(config.numKeys, 1000):
    let key = makeKey(i)
    discard backend.put(key, value)

# =============================================================================
# Main
# =============================================================================

when isMainModule:
  echo "========================================"
  echo "Fractio Storage Benchmarks"
  echo "========================================"
  echo ""
  echo "Configuration:"
  echo "  Keys: ", NUM_KEYS
  echo "  Value size: ", VALUE_SIZE, " bytes"
  echo "  Warmup ops: ", WARMUP_OPS
  echo ""

  let testPath = "/tmp/fractio_bench_storage"

  let config = BenchmarkConfig(
    numKeys: NUM_KEYS,
    numOps: 1000,
    valueSize: VALUE_SIZE,
    warmupOps: WARMUP_OPS
  )

  var allResults: seq[BenchmarkResult] = @[]

  # Sequential mixed benchmark
  echo "\n" & "=".repeat(50)
  echo "Sequential Mixed Benchmark (2:1 read:write)"
  echo "=".repeat(50)

  var backend = setupStorage(testPath)
  seedData(backend, config)

  let seqResult = runSequentialBenchmark(backend, config)
  allResults.add(seqResult)

  teardownStorage(backend, testPath)

  echo "\nResults:"
  echo "  Ops/sec: ", formatFloat(seqResult.opsPerSec, ffDecimal, 1)
  echo "  Avg latency: ", formatFloat(seqResult.avgLatencyUs, ffDecimal, 2), " μs"
  echo "  Min latency: ", formatFloat(seqResult.minLatencyUs, ffDecimal, 2), " μs"
  echo "  Max latency: ", formatFloat(seqResult.maxLatencyUs, ffDecimal, 2), " μs"
  echo "  Total ops: ", seqResult.totalOps
  echo "  Errors: ", seqResult.errors

  # Write-only benchmark
  echo "\n" & "=".repeat(50)
  echo "Write-Only Benchmark"
  echo "=".repeat(50)

  backend = setupStorage(testPath)

  let writeResult = runWriteBenchmark(backend, config)
  allResults.add(writeResult)

  teardownStorage(backend, testPath)

  echo "\nResults:"
  echo "  Ops/sec: ", formatFloat(writeResult.opsPerSec, ffDecimal, 1)
  echo "  Avg latency: ", formatFloat(writeResult.avgLatencyUs, ffDecimal, 2), " μs"
  echo "  Total ops: ", writeResult.totalOps

  # Read-only benchmark
  echo "\n" & "=".repeat(50)
  echo "Read-Only Benchmark"
  echo "=".repeat(50)

  backend = setupStorage(testPath)
  seedData(backend, config)

  let readResult = runReadBenchmark(backend, config)
  allResults.add(readResult)

  teardownStorage(backend, testPath)

  echo "\nResults:"
  echo "  Ops/sec: ", formatFloat(readResult.opsPerSec, ffDecimal, 1)
  echo "  Avg latency: ", formatFloat(readResult.avgLatencyUs, ffDecimal, 2), " μs"
  echo "  Total ops: ", readResult.totalOps

  # Print summary table
  echo "\n" & "=".repeat(60)
  echo "BENCHMARK RESULTS SUMMARY"
  echo "=".repeat(60)
  echo ""
  echo "Benchmark".center(25), " | ", "Ops/sec".center(12), " | ",
       "Avg Lat (μs)".center(14)
  echo "-".repeat(60)

  for r in allResults:
    echo r.name.center(25), " | ", formatFloat(r.opsPerSec, ffDecimal, 1).center(12),
         " | ", formatFloat(r.avgLatencyUs, ffDecimal, 2).center(14)

  echo "\nDone!"
