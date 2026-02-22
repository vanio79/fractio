# Fractio (Nim) Storage Benchmark
#
# Measures performance of:
# - Sequential writes
# - Random writes
# - Sequential reads
# - Random reads
# - Range scans
# - Deletions
# - Mixed workload

import std/[os, strutils, strformat, times, random, tables, parseopt, json]

import fractio/storage/[db, keyspace, db_config, types, error, batch]

# Benchmark result for a single operation type
type
  BenchResult = object
    name: string
    totalOps: uint64
    durationMs: uint64
    opsPerSec: float64
    latencyUs: float64

proc newBenchResult(name: string, totalOps: uint64,
    duration: Duration): BenchResult =
  let durationMs = duration.inMilliseconds.uint64
  let durationSecs = duration.inMicroseconds.float64 / 1_000_000.0
  let opsPerSec = if durationSecs > 0.0: totalOps.float64 /
      durationSecs else: 0.0
  let latencyUs = if totalOps > 0: duration.inMicroseconds.float64 /
      totalOps.float64 else: 0.0

  BenchResult(
    name: name,
    totalOps: totalOps,
    durationMs: durationMs,
    opsPerSec: opsPerSec,
    latencyUs: latencyUs
  )

proc print(self: BenchResult) =
  echo fmt"  {self.name:<25} {self.totalOps:>10} ops in {self.durationMs:>6} ms | {self.opsPerSec:>12.2f} ops/s | {self.latencyUs:>8.2f} us/op"

# Generate a key of the specified size
proc makeKey(prefix: string, i: uint64, keySize: int): string =
  let suffix = fmt"_{i}"
  let paddingLen = keySize - prefix.len - suffix.len
  if paddingLen > 0:
    prefix & "k".repeat(paddingLen) & suffix
  else:
    prefix & suffix

# Generate a value of the specified size
proc makeValue(valueSize: int): string =
  "v".repeat(valueSize)

# Benchmark configuration
type
  Config = object
    numOps: uint64
    keySize: int
    valueSize: int
    batchSize: int
    dbPath: string

proc defaultConfig(): Config =
  Config(
    numOps: 100_000,
    keySize: 16,
    valueSize: 100,
    batchSize: 1000,
    dbPath: "/tmp/bench_fractio"
  )

proc parseArgs(): Config =
  result = defaultConfig()

  var p = initOptParser()
  while true:
    p.next()
    case p.kind
    of cmdArgument:
      discard
    of cmdLongOption, cmdShortOption:
      case p.key
      of "ops", "n":
        if p.val.len > 0:
          result.numOps = p.val.parseUInt()
        else:
          p.next()
          if p.kind == cmdArgument:
            result.numOps = p.key.parseUInt()
      of "key-size", "k":
        if p.val.len > 0:
          result.keySize = p.val.parseInt()
        else:
          p.next()
          if p.kind == cmdArgument:
            result.keySize = p.key.parseInt()
      of "value-size", "v":
        if p.val.len > 0:
          result.valueSize = p.val.parseInt()
        else:
          p.next()
          if p.kind == cmdArgument:
            result.valueSize = p.key.parseInt()
      of "path", "p":
        if p.val.len > 0:
          result.dbPath = p.val
        else:
          p.next()
          if p.kind == cmdArgument:
            result.dbPath = p.key
      of "help", "h":
        echo "Fractio benchmark"
        echo ""
        echo "Usage: fractio_bench [OPTIONS]"
        echo ""
        echo "Options:"
        echo "  --ops, -n <NUM>       Number of operations (default: 100000)"
        echo "  --key-size, -k <NUM>  Key size in bytes (default: 16)"
        echo "  --value-size, -v <NUM> Value size in bytes (default: 100)"
        echo "  --path, -p <PATH>     Database path (default: /tmp/bench_fractio)"
        quit(0)
      else:
        discard
    of cmdEnd:
      break


proc main() =
  let config = parseArgs()

  echo "=== Fractio (Nim) Storage Benchmark ==="
  echo ""
  echo "Configuration:"
  echo fmt"  Operations:  {config.numOps}"
  echo fmt"  Key size:    {config.keySize} bytes"
  echo fmt"  Value size:  {config.valueSize} bytes"
  echo fmt"  DB path:     {config.dbPath}"
  echo ""

  # Clean up existing database
  if dirExists(config.dbPath):
    removeDir(config.dbPath)

  var results: seq[BenchResult] = @[]

  # Create database
  let dbConfig = newConfig(config.dbPath)
  let dbResult = open(dbConfig)

  if dbResult.isErr:
    echo "Failed to open database: ", dbResult.error
    quit(1)

  let db = dbResult.value

  # Create keyspace
  let ksResult = db.keyspace("default")
  if ksResult.isErr:
    echo "Failed to create keyspace: ", ksResult.error
    quit(1)

  let ks = ksResult.value

  # ===========================================================================
  # Benchmark 1: Sequential Writes
  # ===========================================================================
  echo "Running sequential write benchmark..."

  let startTime = cpuTime()
  for i in 0'u64 ..< config.numOps:
    let key = makeKey("seq", i, config.keySize)
    let value = makeValue(config.valueSize)
    let insertResult = ks.insert(key, value)
    if insertResult.isErr:
      echo "Insert error: ", insertResult.error
      break
  let endTime = cpuTime()
  let duration = initDuration(nanoseconds = ((endTime - startTime) *
      1_000_000_000).int64)
  let result = newBenchResult("sequential_writes", config.numOps, duration)
  result.print()
  results.add(result)

  # ===========================================================================
  # Benchmark 2: Random Writes
  # ===========================================================================
  echo "Running random write benchmark..."

  randomize(42)
  var indices: seq[uint64] = @[]
  for i in 0'u64 ..< config.numOps:
    indices.add(i)
  shuffle(indices)

  let startTime2 = cpuTime()
  for i in indices:
    let key = makeKey("rand", i, config.keySize)
    let value = makeValue(config.valueSize)
    let insertResult = ks.insert(key, value)
    if insertResult.isErr:
      echo "Insert error: ", insertResult.error
      break
  let endTime2 = cpuTime()
  let duration2 = initDuration(nanoseconds = ((endTime2 - startTime2) *
      1_000_000_000).int64)
  let result2 = newBenchResult("random_writes", config.numOps, duration2)
  result2.print()
  results.add(result2)

  # ===========================================================================
  # Benchmark 3: Sequential Reads
  # ===========================================================================
  echo "Running sequential read benchmark..."

  let startTime3 = cpuTime()
  for i in 0'u64 ..< config.numOps:
    let key = makeKey("seq", i, config.keySize)
    discard ks.get(key)
  let endTime3 = cpuTime()
  let duration3 = initDuration(nanoseconds = ((endTime3 - startTime3) *
      1_000_000_000).int64)
  let result3 = newBenchResult("sequential_reads", config.numOps, duration3)
  result3.print()
  results.add(result3)

  # ===========================================================================
  # Benchmark 4: Random Reads
  # ===========================================================================
  echo "Running random read benchmark..."

  let startTime4 = cpuTime()
  for i in indices:
    let key = makeKey("rand", i, config.keySize)
    discard ks.get(key)
  let endTime4 = cpuTime()
  let duration4 = initDuration(nanoseconds = ((endTime4 - startTime4) *
      1_000_000_000).int64)
  let result4 = newBenchResult("random_reads", config.numOps, duration4)
  result4.print()
  results.add(result4)

  # ===========================================================================
  # Benchmark 5: Range Scan
  # ===========================================================================
  echo "Running range scan benchmark..."

  # Insert sequential keys for range scan
  let scanCount = min(10_000'u64, config.numOps)
  for i in 0'u64 ..< scanCount:
    let key = fmt"scan_{i:08d}"
    let value = makeValue(config.valueSize)
    discard ks.insert(key, value)

  let startTime5 = cpuTime()
  let iter = ks.rangeIter("scan_00000000", "scan_00009999")
  var scanned: uint64 = 0
  for entry in iter.entries:
    if entry.valueType == vtValue:
      scanned += 1
  let endTime5 = cpuTime()
  let duration5 = initDuration(nanoseconds = ((endTime5 - startTime5) *
      1_000_000_000).int64)
  let result5 = newBenchResult("range_scan", scanned, duration5)
  result5.print()
  results.add(result5)

  # ===========================================================================
  # Benchmark 6: Prefix Scan
  # ===========================================================================
  echo "Running prefix scan benchmark..."

  let startTime6 = cpuTime()
  let prefixIter = ks.prefixIter("scan_")
  var prefixScanned: uint64 = 0
  for entry in prefixIter.entries:
    if entry.valueType == vtValue:
      prefixScanned += 1
  let endTime6 = cpuTime()
  let duration6 = initDuration(nanoseconds = ((endTime6 - startTime6) *
      1_000_000_000).int64)
  let result6 = newBenchResult("prefix_scan", prefixScanned, duration6)
  result6.print()
  results.add(result6)

  # ===========================================================================
  # Benchmark 7: Deletions
  # ===========================================================================
  echo "Running deletion benchmark..."

  let deleteCount = config.numOps div 2
  let startTime7 = cpuTime()
  for i in 0'u64 ..< deleteCount:
    let key = makeKey("seq", i, config.keySize)
    discard ks.remove(key)
  let endTime7 = cpuTime()
  let duration7 = initDuration(nanoseconds = ((endTime7 - startTime7) *
      1_000_000_000).int64)
  let result7 = newBenchResult("deletions", deleteCount, duration7)
  result7.print()
  results.add(result7)

  # ===========================================================================
  # Benchmark 8: Batch Writes
  # ===========================================================================
  echo "Running batch write benchmark..."

  let batchOps = config.numOps div 10
  var batchOpsCount: uint64 = 0

  let startTime8 = cpuTime()
  var batchStart = 0'u64
  while batchStart < batchOps:
    let batchEnd = min(batchStart + config.batchSize.uint64, batchOps)
    var wb = db.batch()

    for i in batchStart ..< batchEnd:
      let key = makeKey("batch", i, config.keySize)
      let value = makeValue(config.valueSize)
      batch.insert(wb, ks, key, value)
      batchOpsCount += 1

    let commitResult = db.commit(wb)
    if commitResult.isErr:
      echo "Batch commit error: ", commitResult.error

    batchStart = batchEnd
  let endTime8 = cpuTime()
  let duration8 = initDuration(nanoseconds = ((endTime8 - startTime8) *
      1_000_000_000).int64)
  let result8 = newBenchResult("batch_writes", batchOpsCount, duration8)
  result8.print()
  results.add(result8)

  # ===========================================================================
  # Benchmark 9: Contains Key
  # ===========================================================================
  echo "Running contains_key benchmark..."

  let startTime9 = cpuTime()
  for i in 0'u64 ..< config.numOps:
    let key = makeKey("rand", i, config.keySize)
    discard ks.containsKey(key)
  let endTime9 = cpuTime()
  let duration9 = initDuration(nanoseconds = ((endTime9 - startTime9) *
      1_000_000_000).int64)
  let result9 = newBenchResult("contains_key", config.numOps, duration9)
  result9.print()
  results.add(result9)

  # ===========================================================================
  # Summary
  # ===========================================================================
  echo ""
  echo "=== Summary ==="
  echo ""
  echo "Benchmark                   Total Ops        Ops/sec Latency (us)"
  echo "-".repeat(65)
  for r in results:
    echo fmt"{r.name:<25} {r.totalOps:>12} {r.opsPerSec:>15.2f} {r.latencyUs:>12.2f}"

  # Output results in JSON format for comparison
  echo ""
  echo "=== JSON Output ==="
  echo "{"
  echo "  \"engine\": \"fractio\","
  echo "  \"config\": {"
  echo "    \"num_ops\": " & $config.numOps & ","
  echo "    \"key_size\": " & $config.keySize & ","
  echo "    \"value_size\": " & $config.valueSize
  echo "  },"
  echo "  \"results\": {"
  for i, r in results:
    let comma = if i < results.len - 1: "," else: ""
    let opsPerSecStr = fmt"{r.opsPerSec:.2f}"
    let latencyUsStr = fmt"{r.latencyUs:.2f}"
    let line = "    \"" & r.name & "\": { \"ops\": " & $r.totalOps &
               ", \"ops_per_sec\": " & opsPerSecStr &
               ", \"latency_us\": " & latencyUsStr & " }" & comma
    echo line
  echo "  }"
  echo "}"

  # Clean up
  db.close()
  if dirExists(config.dbPath):
    removeDir(config.dbPath)

when isMainModule:
  main()
