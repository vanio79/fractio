# Performance benchmarks for MVCC Transactions
# Measures throughput and latency of transaction operations

import std/[times, strformat, os]
import fractio/core/types
import fractio/core/transaction
import fractio/core/timestamp_provider
import fractio/core/transaction_manager
import fractio/storage/mvcc/types
import fractio/storage/mvcc/engine
import fractio/storage/wisckey_backend

# Constants
const
  DEFAULT_MAX_OFFSET_NS* = 100_000_000
  DEFAULT_PRIORITY* = 500

type
  BenchmarkResult* = object
    name*: string
    operations*: int
    durationMs*: int64
    throughput*: float # operations per second
    avgLatencyMs*: float
    minLatencyMs*: float
    maxLatencyMs*: float

  BenchmarkConfig* = object
    numTransactions*: int
    numWritesPerTxn*: int
    numReadsPerTxn*: int
    testPath*: string

proc runBenchmark*(name: string, config: BenchmarkConfig,
    benchmarkProc: proc(backend: WiscKeyBackend,
                        engine: MVCCEngine,
                        tm: TransactionManager,
                        config: BenchmarkConfig): int64): BenchmarkResult =
  ## Run a benchmark and return results

  # Setup
  removeDir(config.testPath)
  createDir(config.testPath)

  try:
    let backend = newWiscKeyBackend(StorageConfig(
      path: config.testPath,
      createIfMissing: true,
      syncWrites: false
    ))

    check backend.open(StorageConfig(
      path: config.testPath,
      createIfMissing: true,
      syncWrites: false
    ))

    let tsProvider = TimestampProvider(
      timer: nil,
      lastTimestamp: 1000,
      lastCounter: 0,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )

    let engine = newMVCCEngine(backend, tsProvider)
    let tm = newTransactionManager(tsProvider, engine)

    # Run benchmark
    let startTime = epochTime()
    let totalOperations = benchmarkProc(backend, engine, tm, config)
    let endTime = epochTime()

    let durationMs = int64((endTime - startTime) * 1000)
    let throughput = totalOperations.float / (durationMs.float / 1000.0)
    let avgLatencyMs = durationMs.float / totalOperations.float

    result = BenchmarkResult(
      name: name,
      operations: totalOperations,
      durationMs: durationMs,
      throughput: throughput,
      avgLatencyMs: avgLatencyMs,
      minLatencyMs: 0.0,
      maxLatencyMs: 0.0
    )

    # Cleanup
    discard backend.close()
    discard backend.destroy()

  finally:
    removeDir(config.testPath)

proc benchmarkSimpleWrites*(backend: WiscKeyBackend,
    engine: MVCCEngine,
    tm: TransactionManager,
    config: BenchmarkConfig): int64 =
  ## Benchmark simple write transactions

  var totalOps = 0

  for i in 0 ..< config.numTransactions:
    let txn = tm.beginTransaction()

    for j in 0 ..< config.numWritesPerTxn:
      let key = fmt"key_{i}_{j}"
      let value = fmt"value_{i}_{j}"
      if engine.mvccPut(txn, key, value).success:
        totalOps += 1

    if tm.commitTransaction(txn).success:
      totalOps += 1

  return totalOps

proc benchmarkSimpleReads*(backend: WiscKeyBackend,
    engine: MVCCEngine,
    tm: TransactionManager,
    config: BenchmarkConfig): int64 =
  ## Benchmark simple read transactions
  ## First populate data, then read it

  var totalOps = 0

  # Populate data
  for i in 0 ..< config.numTransactions:
    let txn = tm.beginTransaction()
    let key = fmt"key_{i}"
    let value = fmt"value_{i}"
    if engine.mvccPut(txn, key, value).success:
      totalOps += 1
    discard tm.commitTransaction(txn)

  # Read data
  for i in 0 ..< config.numTransactions:
    let txn = tm.beginTransaction()
    let key = fmt"key_{i}"
    if engine.mvccGet(key, txn.startTimestamp).success:
      totalOps += 1
    discard tm.commitTransaction(txn)

  return totalOps

proc benchmarkReadWriteMix*(backend: WiscKeyBackend,
    engine: MVCCEngine,
    tm: TransactionManager,
    config: BenchmarkConfig): int64 =
  ## Benchmark mixed read/write transactions

  var totalOps = 0

  for i in 0 ..< config.numTransactions:
    let txn = tm.beginTransaction()

    # Write some data
    for j in 0 ..< config.numWritesPerTxn:
      let key = fmt"key_{i}_{j}"
      let value = fmt"value_{i}_{j}"
      if engine.mvccPut(txn, key, value).success:
        totalOps += 1

    # Read some data
    for j in 0 ..< config.numReadsPerTxn:
      let key = fmt"key_{i}_{j}"
      if engine.mvccGet(key, txn.startTimestamp).success:
        totalOps += 1

    if tm.commitTransaction(txn).success:
      totalOps += 1

  return totalOps

proc benchmarkConflictingWrites*(backend: WiscKeyBackend,
    engine: MVCCEngine,
    tm: TransactionManager,
    config: BenchmarkConfig): int64 =
  ## Benchmark transactions with write conflicts

  var totalOps = 0

  # First, write initial data
  let txnInit = tm.beginTransaction()
  for i in 0 ..< 100:
    let key = fmt"key_{i}"
    let value = fmt"initial_value_{i}"
    discard engine.mvccPut(txnInit, key, value)
    discard tm.commitTransaction(txnInit)

  # Now try to write to same keys with concurrent transactions
  for i in 0 ..< config.numTransactions:
    let txn = tm.beginTransaction()

    # Write to a random subset of keys (creating conflicts)
    let keyIndex = i mod 100
    let key = fmt"key_{keyIndex}"
    let value = fmt"value_{i}"

    if engine.mvccPut(txn, key, value).success:
      totalOps += 1

    # Some will succeed, some will fail due to conflicts
    if tm.commitTransaction(txn).success:
      totalOps += 1

  return totalOps

proc printResult*(result: BenchmarkResult) =
  ## Print benchmark results

  echo "\n" & "=".repeat(80)
  echo "Benchmark: " & result.name
  echo "=".repeat(80)
  echo "Operations:     " & $result.operations
  echo "Duration:       " & $result.durationMs & " ms"
  echo "Throughput:     " & fmt"{result.throughput:.2f}" & " ops/sec"
  echo "Avg Latency:    " & fmt"{result.avgLatencyMs:.4f}" & " ms"
  echo "=".repeat(80) & "\n"

proc runAllBenchmarks*() =
  ## Run all benchmarks

  echo "\n" & "*".repeat(80)
  echo "MVCC Transaction Performance Benchmarks"
  echo "*".repeat(80) & "\n"

  # Benchmark 1: Simple writes
  let config1 = BenchmarkConfig(
    numTransactions: 1000,
    numWritesPerTxn: 10,
    numReadsPerTxn: 0,
    testPath: "/tmp/benchmark_simple_writes"
  )

  let result1 = runBenchmark("Simple Writes (1000 txns, 10 writes each)", config1,
    benchmarkSimpleWrites)
  printResult(result1)

  # Benchmark 2: Simple reads
  let config2 = BenchmarkConfig(
    numTransactions: 1000,
    numWritesPerTxn: 0,
    numReadsPerTxn: 10,
    testPath: "/tmp/benchmark_simple_reads"
  )

  let result2 = runBenchmark("Simple Reads (1000 txns, 10 reads each)", config2,
    benchmarkSimpleReads)
  printResult(result2)

  # Benchmark 3: Read/write mix
  let config3 = BenchmarkConfig(
    numTransactions: 1000,
    numWritesPerTxn: 5,
    numReadsPerTxn: 5,
    testPath: "/tmp/benchmark_read_write_mix"
  )

  let result3 = runBenchmark("Read/Write Mix (1000 txns, 5 writes + 5 reads each)", config3,
    benchmarkReadWriteMix)
  printResult(result3)

  # Benchmark 4: Conflicting writes
  let config4 = BenchmarkConfig(
    numTransactions: 1000,
    numWritesPerTxn: 1,
    numReadsPerTxn: 0,
    testPath: "/tmp/benchmark_conflicting_writes"
  )

  let result4 = runBenchmark("Conflicting Writes (1000 txns, 1 write each)", config4,
    benchmarkConflictingWrites)
  printResult(result4)

  # Summary
  echo "\n" & "*".repeat(80)
  echo "Benchmark Summary"
  echo "*".repeat(80)
  echo fmt"Simple Writes:       {result1.throughput:10.2f} ops/sec"
  echo fmt"Simple Reads:        {result2.throughput:10.2f} ops/sec"
  echo fmt"Read/Write Mix:      {result3.throughput:10.2f} ops/sec"
  echo fmt"Conflicting Writes:  {result4.throughput:10.2f} ops/sec"
  echo "*".repeat(80) & "\n"

  # Check if we meet performance targets
  echo "Performance Targets:"
  echo "  Simple Writes:       > 10,000 ops/sec"
  echo "  Simple Reads:        > 10,000 ops/sec"
  echo "  Read/Write Mix:      > 5,000 ops/sec"
  echo "\n"

  if result1.throughput > 10000.0:
    echo "✓ Simple writes target met"
  else:
    echo "✗ Simple writes target NOT met"

  if result2.throughput > 10000.0:
    echo "✓ Simple reads target met"
  else:
    echo "✗ Simple reads target NOT met"

  if result3.throughput > 5000.0:
    echo "✓ Read/write mix target met"
  else:
    echo "✗ Read/write mix target NOT met"

when isMainModule:
  runAllBenchmarks()
