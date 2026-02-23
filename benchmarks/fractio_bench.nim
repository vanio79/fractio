# Fractio (Nim) Storage Benchmark with OS-Level Metrics
#
# Measures performance using /proc filesystem:
# - CPU time from /proc/self/stat
# - Memory from /proc/self/status  
# - I/O from /proc/self/io
# - Disk space from directory listing

import std/[os, strutils, strformat, times, random, tables, parseopt, json,
    cpuinfo, algorithm]

import fractio/storage/[db, keyspace, db_config, types, error, batch]

# System resource usage metrics from /proc filesystem
type
  ResourceMetrics = object
    cpuUserMs: uint64      # User CPU time in milliseconds
    cpuSystemMs: uint64    # System CPU time in milliseconds
    cpuTotalMs: uint64     # Total CPU time
    memoryRssKb: uint64    # Resident set size in KB
    memoryPeakKb: uint64   # Peak memory (high water mark) in KB
    diskReadBytes: uint64  # Bytes read from disk
    diskWriteBytes: uint64 # Bytes written to disk
    diskReadOps: uint64    # Number of read syscalls
    diskWriteOps: uint64   # Number of write syscalls

proc readResourceMetrics(): ResourceMetrics =
  result = ResourceMetrics()

  # Read /proc/self/stat for CPU time
  # Fields: pid (1), comm (2), state (3), ... utime (14), stime (15)
  # Times are in jiffies (typically 100 Hz)
  try:
    let stat = readFile("/proc/self/stat")
    let parts = stat.splitWhitespace()
    if parts.len >= 17:
      let clkTck = 100'u64 # sysconf(_SC_CLK_TCK), typically 100
      let utime = parts[13].parseUInt()
      let stime = parts[14].parseUInt()
      result.cpuUserMs = utime * 1000'u64 div clkTck
      result.cpuSystemMs = stime * 1000'u64 div clkTck
      result.cpuTotalMs = result.cpuUserMs + result.cpuSystemMs
  except:
    discard

  # Read /proc/self/status for memory
  try:
    let status = readFile("/proc/self/status")
    for line in status.splitLines():
      if line.startsWith("VmRSS:"):
        let parts = line.splitWhitespace()
        if parts.len >= 2:
          result.memoryRssKb = parts[1].parseUInt()
      elif line.startsWith("VmHWM:"):
        let parts = line.splitWhitespace()
        if parts.len >= 2:
          result.memoryPeakKb = parts[1].parseUInt()
  except:
    discard

  # Read /proc/self/io for I/O stats
  try:
    let io = readFile("/proc/self/io")
    for line in io.splitLines():
      if line.startsWith("read_bytes:"):
        let parts = line.split(':')
        if parts.len >= 2:
          result.diskReadBytes = parts[1].strip().parseUInt()
      elif line.startsWith("write_bytes:"):
        let parts = line.split(':')
        if parts.len >= 2:
          result.diskWriteBytes = parts[1].strip().parseUInt()
      elif line.startsWith("syscr:"):
        let parts = line.split(':')
        if parts.len >= 2:
          result.diskReadOps = parts[1].strip().parseUInt()
      elif line.startsWith("syscw:"):
        let parts = line.split(':')
        if parts.len >= 2:
          result.diskWriteOps = parts[1].strip().parseUInt()
  except:
    discard

proc diff(a, b: ResourceMetrics): ResourceMetrics =
  result = ResourceMetrics(
    cpuUserMs: a.cpuUserMs - b.cpuUserMs,
    cpuSystemMs: a.cpuSystemMs - b.cpuSystemMs,
    cpuTotalMs: a.cpuTotalMs - b.cpuTotalMs,
    memoryRssKb: a.memoryRssKb,
    memoryPeakKb: a.memoryPeakKb,
    diskReadBytes: a.diskReadBytes - b.diskReadBytes,
    diskWriteBytes: a.diskWriteBytes - b.diskWriteBytes,
    diskReadOps: a.diskReadOps - b.diskReadOps,
    diskWriteOps: a.diskWriteOps - b.diskWriteOps
  )

# Latency histogram for percentile calculation
type
  LatencyHistogram = object
    samples: seq[uint64] # in microseconds

proc initLatencyHistogram(): LatencyHistogram =
  result.samples = @[]

proc record(h: var LatencyHistogram, latencyUs: uint64) =
  h.samples.add(latencyUs)

proc percentile(h: LatencyHistogram, p: float): uint64 =
  if h.samples.len == 0:
    return 0
  var sorted = h.samples
  sorted.sort(system.cmp[uint64])
  let idx = int((p / 100.0) * float(sorted.len - 1) + 0.5)
  return sorted[min(idx, sorted.len - 1)]

proc avg(h: LatencyHistogram): float =
  if h.samples.len == 0:
    return 0.0
  var sum: uint64 = 0
  for s in h.samples:
    sum += s
  return float(sum) / float(h.samples.len)

proc minLatency(h: LatencyHistogram): uint64 =
  if h.samples.len == 0:
    return 0
  result = high(uint64)
  for s in h.samples:
    if s < result:
      result = s

proc maxLatency(h: LatencyHistogram): uint64 =
  if h.samples.len == 0:
    return 0
  result = 0
  for s in h.samples:
    if s > result:
      result = s

# Benchmark result for a single operation type
type
  BenchResult = object
    name: string
    totalOps: uint64
    durationMs: uint64
    opsPerSec: float64
    latencyAvgUs: float64
    latencyP50Us: uint64
    latencyP95Us: uint64
    latencyP99Us: uint64
    latencyMinUs: uint64
    latencyMaxUs: uint64
    cpuUserMs: uint64
    cpuSystemMs: uint64
    cpuTotalMs: uint64
    cpuPercent: float64
    memoryRssMb: float64
    memoryPeakMb: float64
    diskReadMb: float64
    diskWriteMb: float64
    diskReadOps: uint64
    diskWriteOps: uint64
    iops: float64
    diskTotalMb: float64
    throughputMbPerSec: float64
    diskReadBytes: uint64
    diskWriteBytes: uint64

proc print(self: BenchResult) =
  echo fmt"  {self.name:<25} {self.totalOps:>10} ops in {self.durationMs:>6} ms | {self.opsPerSec:>12.2f} ops/s"
  echo fmt"    Latency: avg={self.latencyAvgUs:.2f}us p50={self.latencyP50Us}us p95={self.latencyP95Us}us p99={self.latencyP99Us}us [min={self.latencyMinUs} max={self.latencyMaxUs}]"
  echo fmt"    CPU: user={self.cpuUserMs}ms sys={self.cpuSystemMs}ms total={self.cpuTotalMs}ms ({self.cpuPercent:.1f}%)"
  echo fmt"    Memory: RSS={self.memoryRssMb:.2f}MB Peak={self.memoryPeakMb:.2f}MB"
  echo fmt"    Disk: read={self.diskReadMb:.2f}MB write={self.diskWriteMb:.2f}MB IOPS={self.iops:.0f} throughput={self.throughputMbPerSec:.2f}MB/s"

proc toJson(self: BenchResult): string =
  fmt"""{{ "ops": {self.totalOps}, "ops_per_sec": {self.opsPerSec:.2f}, "latency_us": {self.latencyAvgUs:.2f}, "cpu_percent": {self.cpuPercent:.2f}, "memory_mb": {self.memoryRssMb:.2f}, "disk_mb": {self.diskTotalMb:.2f}, "disk_read_bytes": {self.diskReadBytes}, "disk_write_bytes": {self.diskWriteBytes} }}"""

# Get actual disk usage in bytes (using block count, not file size)
# This properly accounts for sparse files
proc getDirDiskUsage(path: string): uint64 =
  result = 0
  if dirExists(path):
    for kind, entry in walkDir(path):
      if kind == pcFile:
        try:
          # Use stat to get actual block count
          let info = getFileInfo(entry)
          # Block size is typically 512 bytes on Linux
          # blocks * 512 = actual disk usage
          let blockSize = 512'u64
          let blocks = (info.size.uint64 + blockSize - 1) div blockSize
          result += blocks * blockSize
        except:
          discard
      elif kind == pcDir:
        result += getDirDiskUsage(entry)

# Legacy function for file size (kept for reference)
proc getDirSize(path: string): uint64 =
  result = 0
  if dirExists(path):
    for kind, entry in walkDir(path):
      if kind == pcFile:
        try:
          let info = getFileInfo(entry)
          result += info.size.uint64
        except:
          discard
      elif kind == pcDir:
        result += getDirSize(entry)

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
        echo "Fractio benchmark with OS-level metrics"
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

proc makeResult(name: string, totalOps: uint64, duration: Duration,
                latency: LatencyHistogram,
                    startRes: ResourceMetrics): BenchResult =
  let endRes = readResourceMetrics()
  let diff = endRes.diff(startRes)
  let durationMs = duration.inMilliseconds.uint64
  let durationSecs = duration.inMicroseconds.float64 / 1_000_000.0

  let cpuPercent = if durationMs > 0:
    (diff.cpuTotalMs.float64 / durationMs.float64) * 100.0
  else:
    0.0

  let diskTotalMb = (diff.diskReadBytes + diff.diskWriteBytes).float64 / (
      1024.0 * 1024.0)
  let throughputMb = if durationSecs > 0.0: diskTotalMb / durationSecs else: 0.0
  let iops = if durationSecs > 0.0:
    (diff.diskReadOps + diff.diskWriteOps).float64 / durationSecs
  else:
    0.0

  result = BenchResult(
    name: name,
    totalOps: totalOps,
    durationMs: durationMs,
    opsPerSec: if durationSecs > 0.0: totalOps.float64 / durationSecs else: 0.0,
    latencyAvgUs: latency.avg(),
    latencyP50Us: latency.percentile(50.0),
    latencyP95Us: latency.percentile(95.0),
    latencyP99Us: latency.percentile(99.0),
    latencyMinUs: latency.minLatency(),
    latencyMaxUs: latency.maxLatency(),
    cpuUserMs: diff.cpuUserMs,
    cpuSystemMs: diff.cpuSystemMs,
    cpuTotalMs: diff.cpuTotalMs,
    cpuPercent: cpuPercent,
    memoryRssMb: endRes.memoryRssKb.float64 / 1024.0,
    memoryPeakMb: endRes.memoryPeakKb.float64 / 1024.0,
    diskReadMb: diff.diskReadBytes.float64 / (1024.0 * 1024.0),
    diskWriteMb: diff.diskWriteBytes.float64 / (1024.0 * 1024.0),
    diskReadOps: diff.diskReadOps,
    diskWriteOps: diff.diskWriteOps,
    iops: iops,
    diskTotalMb: diskTotalMb,
    throughputMbPerSec: throughputMb,
    diskReadBytes: diff.diskReadBytes,
    diskWriteBytes: diff.diskWriteBytes
  )

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

  # Print system information
  echo "System Information:"
  echo fmt"  CPU:         {countProcessors()} cores"
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

  # Track initial resources
  discard readResourceMetrics()

  # ===========================================================================
  # Benchmark 1: Sequential Writes
  # ===========================================================================
  echo "Running sequential write benchmark..."
  var latency = initLatencyHistogram()
  var startRes = readResourceMetrics()
  var startTime = cpuTime()

  for i in 0'u64 ..< config.numOps:
    let opStart = cpuTime()
    let key = makeKey("seq", i, config.keySize)
    let value = makeValue(config.valueSize)
    let insertResult = ks.insert(key, value)
    if insertResult.isErr:
      echo "Insert error: ", insertResult.error
      break
    latency.record(uint64((cpuTime() - opStart) * 1_000_000))

  var endTime = cpuTime()
  var duration = initDuration(nanoseconds = ((endTime - startTime) *
      1_000_000_000).int64)
  var result = makeResult("sequential_writes", config.numOps, duration, latency, startRes)
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

  latency = initLatencyHistogram()
  startRes = readResourceMetrics()
  startTime = cpuTime()

  for i in indices:
    let opStart = cpuTime()
    let key = makeKey("rand", i, config.keySize)
    let value = makeValue(config.valueSize)
    let insertResult = ks.insert(key, value)
    if insertResult.isErr:
      echo "Insert error: ", insertResult.error
      break
    latency.record(uint64((cpuTime() - opStart) * 1_000_000))

  endTime = cpuTime()
  duration = initDuration(nanoseconds = ((endTime - startTime) *
      1_000_000_000).int64)
  result = makeResult("random_writes", config.numOps, duration, latency, startRes)
  result.print()
  results.add(result)

  # ===========================================================================
  # Benchmark 3: Sequential Reads
  # ===========================================================================
  echo "Running sequential read benchmark..."

  latency = initLatencyHistogram()
  startRes = readResourceMetrics()
  startTime = cpuTime()

  for i in 0'u64 ..< config.numOps:
    let opStart = cpuTime()
    let key = makeKey("seq", i, config.keySize)
    discard ks.get(key)
    latency.record(uint64((cpuTime() - opStart) * 1_000_000))

  endTime = cpuTime()
  duration = initDuration(nanoseconds = ((endTime - startTime) *
      1_000_000_000).int64)
  result = makeResult("sequential_reads", config.numOps, duration, latency, startRes)
  result.print()
  results.add(result)

  # ===========================================================================
  # Benchmark 4: Random Reads
  # ===========================================================================
  echo "Running random read benchmark..."

  latency = initLatencyHistogram()
  startRes = readResourceMetrics()
  startTime = cpuTime()

  for i in indices:
    let opStart = cpuTime()
    let key = makeKey("rand", i, config.keySize)
    discard ks.get(key)
    latency.record(uint64((cpuTime() - opStart) * 1_000_000))

  endTime = cpuTime()
  duration = initDuration(nanoseconds = ((endTime - startTime) *
      1_000_000_000).int64)
  result = makeResult("random_reads", config.numOps, duration, latency, startRes)
  result.print()
  results.add(result)

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

  latency = initLatencyHistogram()
  startRes = readResourceMetrics()
  startTime = cpuTime()

  var scanned: uint64 = 0
  let opStart = cpuTime()
  let iter = ks.rangeIter("scan_00000000", "scan_00009999")
  for entry in iter.entries:
    if entry.valueType == vtValue:
      scanned += 1
  latency.record(uint64((cpuTime() - opStart) * 1_000_000))

  endTime = cpuTime()
  duration = initDuration(nanoseconds = ((endTime - startTime) *
      1_000_000_000).int64)
  result = makeResult("range_scan", scanned, duration, latency, startRes)
  result.print()
  results.add(result)

  # ===========================================================================
  # Benchmark 6: Prefix Scan
  # ===========================================================================
  echo "Running prefix scan benchmark..."

  latency = initLatencyHistogram()
  startRes = readResourceMetrics()
  startTime = cpuTime()

  var prefixScanned: uint64 = 0
  let opStart2 = cpuTime()
  let prefixIter = ks.prefixIter("scan_")
  for entry in prefixIter.entries:
    if entry.valueType == vtValue:
      prefixScanned += 1
  latency.record(uint64((cpuTime() - opStart2) * 1_000_000))

  endTime = cpuTime()
  duration = initDuration(nanoseconds = ((endTime - startTime) *
      1_000_000_000).int64)
  result = makeResult("prefix_scan", prefixScanned, duration, latency, startRes)
  result.print()
  results.add(result)

  # ===========================================================================
  # Benchmark 7: Deletions
  # ===========================================================================
  echo "Running deletion benchmark..."

  let deleteCount = config.numOps div 2

  latency = initLatencyHistogram()
  startRes = readResourceMetrics()
  startTime = cpuTime()

  for i in 0'u64 ..< deleteCount:
    let opStart = cpuTime()
    let key = makeKey("seq", i, config.keySize)
    discard ks.remove(key)
    latency.record(uint64((cpuTime() - opStart) * 1_000_000))

  endTime = cpuTime()
  duration = initDuration(nanoseconds = ((endTime - startTime) *
      1_000_000_000).int64)
  result = makeResult("deletions", deleteCount, duration, latency, startRes)
  result.print()
  results.add(result)

  # ===========================================================================
  # Benchmark 8: Batch Writes
  # ===========================================================================
  echo "Running batch write benchmark..."

  let batchOps = config.numOps div 10
  var batchOpsCount: uint64 = 0

  latency = initLatencyHistogram()
  startRes = readResourceMetrics()
  startTime = cpuTime()

  var batchStart = 0'u64
  while batchStart < batchOps:
    let batchEnd = min(batchStart + config.batchSize.uint64, batchOps)
    var wb = db.batch()
    let opStart = cpuTime()

    for i in batchStart ..< batchEnd:
      let key = makeKey("batch", i, config.keySize)
      let value = makeValue(config.valueSize)
      batch.insert(wb, ks, key, value)
      batchOpsCount += 1

    let commitResult = db.commit(wb)
    if commitResult.isErr:
      echo "Batch commit error: ", commitResult.error
    latency.record(uint64((cpuTime() - opStart) * 1_000_000))

    batchStart = batchEnd

  endTime = cpuTime()
  duration = initDuration(nanoseconds = ((endTime - startTime) *
      1_000_000_000).int64)
  result = makeResult("batch_writes", batchOpsCount, duration, latency, startRes)
  result.print()
  results.add(result)

  # ===========================================================================
  # Benchmark 9: Contains Key
  # ===========================================================================
  echo "Running contains_key benchmark..."

  latency = initLatencyHistogram()
  startRes = readResourceMetrics()
  startTime = cpuTime()

  for i in 0'u64 ..< config.numOps:
    let opStart = cpuTime()
    let key = makeKey("rand", i, config.keySize)
    discard ks.containsKey(key)
    latency.record(uint64((cpuTime() - opStart) * 1_000_000))

  endTime = cpuTime()
  duration = initDuration(nanoseconds = ((endTime - startTime) *
      1_000_000_000).int64)
  result = makeResult("contains_key", config.numOps, duration, latency, startRes)
  result.print()
  results.add(result)

  # ===========================================================================
  # Disk Space Summary
  # ===========================================================================
  let diskSize = getDirDiskUsage(config.dbPath) # Use actual disk blocks, not file size
  echo ""
  echo fmt"Disk Space Usage (actual blocks): {diskSize.float64 / (1024.0 * 1024.0):.2f} MB"

  # ===========================================================================
  # Summary
  # ===========================================================================
  echo ""
  echo "=== Summary ==="
  echo ""
  echo "Benchmark                   Total Ops        Ops/sec Latency (us)   CPU%   Memory(MB)"
  echo "-".repeat(85)
  for r in results:
    echo fmt"{r.name:<25} {r.totalOps:>12} {r.opsPerSec:>15.2f} {r.latencyAvgUs:>12.2f} {r.cpuPercent:>6.1f}% {r.memoryRssMb:>12.2f}"

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
  echo "  \"disk_space_mb\": " & fmt"{diskSize.float64 / (1024.0 * 1024.0):.2f}" & ","
  echo "  \"results\": {"
  for i, r in results:
    let comma = if i < results.len - 1: "," else: ""
    echo "    \"" & r.name & "\": " & r.toJson() & comma
  echo "  }"
  echo "}"

  # Clean up
  db.close()
  if dirExists(config.dbPath):
    removeDir(config.dbPath)

when isMainModule:
  main()
