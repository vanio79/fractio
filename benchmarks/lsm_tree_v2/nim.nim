# Comprehensive LSM Tree v2 Benchmark
#
# This benchmark compares:
# - Rust lsm-tree (thirdparty)
# - Nim lsm_tree_v2 (this implementation)
#
# Uses OS-level metrics from /proc filesystem

import std/[os, strutils, strformat, times, random, parseopt, cpuinfo,
    algorithm, options]

# Import lsm_tree_v2 modules
when defined(nimV2):
  import fractio/storage/lsm_tree_v2/lsm_tree
  import fractio/storage/lsm_tree_v2/config
  import fractio/storage/lsm_tree_v2/types
  import fractio/storage/lsm_tree_v2/error

# ============================================================================
# Configuration
# ============================================================================

type
  Config* = object
    numOps*: uint64
    keySize*: int
    valueSize*: int
    warmupOps*: uint64
    dbPath*: string

proc defaultConfig*(): Config =
  result = Config(
    numOps: 1_000_000,
    keySize: 16,
    valueSize: 100,
    warmupOps: 1000,
    dbPath: "/tmp/bench_lsm_tree_nim"
  )

proc parseArgs*(): Config =
  result = defaultConfig()
  var p = initOptParser()
  while true:
    p.next()
    case p.kind
    of cmdArgument: discard
    of cmdLongOption, cmdShortOption:
      case p.key
      of "ops", "n":
        # Handle both --ops=100 and --ops 100 formats
        var val = p.val
        if val.len == 0:
          p.next()
          if p.kind == cmdArgument:
            val = p.key
        if val.len > 0: result.numOps = val.parseUInt()
      of "key-size", "k":
        var val = p.val
        if val.len == 0:
          p.next()
          if p.kind == cmdArgument:
            val = p.key
        if val.len > 0: result.keySize = val.parseInt()
      of "value-size", "v":
        var val = p.val
        if val.len == 0:
          p.next()
          if p.kind == cmdArgument:
            val = p.key
        if val.len > 0: result.valueSize = val.parseInt()
      of "warmup", "w":
        var val = p.val
        if val.len == 0:
          p.next()
          if p.kind == cmdArgument:
            val = p.key
        if val.len > 0: result.warmupOps = val.parseUInt()
      of "path", "p":
        var val = p.val
        if val.len == 0:
          p.next()
          if p.kind == cmdArgument:
            val = p.key
        if val.len > 0: result.dbPath = val
      of "help", "h":
        echo "LSM Tree v2 Benchmark"
        echo "Usage: bench_lsm_v2 [OPTIONS]"
        echo "  --ops, -n <NUM>       Operations (default: 100000)"
        echo "  --key-size, -k <NUM>  Key size (default: 16)"
        echo "  --value-size, -v <NUM> Value size (default: 100)"
        echo "  --warmup, -w <NUM>    Warmup ops (default: 1000)"
        echo "  --path, -p <PATH>    DB path"
        quit(0)
      else: discard
    of cmdEnd: break

# ============================================================================
# Resource Metrics
# ============================================================================

type
  ResourceMetrics* = object
    cpuUserMs*: uint64
    cpuSystemMs*: uint64
    memoryRssKb*: uint64
    memoryPeakKb*: uint64
    diskReadBytes*: uint64
    diskWriteBytes*: uint64

proc readResourceMetrics*(): ResourceMetrics =
  result = ResourceMetrics()
  try:
    let stat = readFile("/proc/self/stat")
    let parts = stat.splitWhitespace()
    if parts.len >= 17:
      let clkTck = 100'u64
      result.cpuUserMs = parts[13].parseUInt() * 1000'u64 div clkTck
      result.cpuSystemMs = parts[14].parseUInt() * 1000'u64 div clkTck
  except: discard

  try:
    let status = readFile("/proc/self/status")
    for line in status.splitLines():
      if line.startsWith("VmRSS:"):
        result.memoryRssKb = line.splitWhitespace()[1].parseUInt()
      elif line.startsWith("VmHWM:"):
        result.memoryPeakKb = line.splitWhitespace()[1].parseUInt()
  except: discard

  try:
    let io = readFile("/proc/self/io")
    for line in io.splitLines():
      if line.startsWith("read_bytes:"):
        result.diskReadBytes = line.split(':')[1].strip().parseUInt()
      elif line.startsWith("write_bytes:"):
        result.diskWriteBytes = line.split(':')[1].strip().parseUInt()
  except: discard

proc diffResources*(a, b: ResourceMetrics): ResourceMetrics =
  ResourceMetrics(
    cpuUserMs: a.cpuUserMs - b.cpuUserMs,
    cpuSystemMs: a.cpuSystemMs - b.cpuSystemMs,
    memoryRssKb: a.memoryRssKb,
    memoryPeakKb: a.memoryPeakKb,
    diskReadBytes: a.diskReadBytes - b.diskReadBytes,
    diskWriteBytes: a.diskWriteBytes - b.diskWriteBytes
  )

# ============================================================================
# Latency Tracking
# ============================================================================

type
  LatencyTracker* = object
    samples*: seq[float64]

proc trackLatency*(t: var LatencyTracker, ns: int64) =
  t.samples.add(float64(ns) / 1000.0) # Convert to microseconds

proc getLatencyStats*(t: LatencyTracker): tuple[avg, p50, p95, p99, min,
    max: float64] =
  if t.samples.len == 0:
    return (0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

  var sorted = t.samples
  sorted.sort(system.cmp[float64])

  let n = sorted.len
  var sum: float64 = 0.0
  for v in sorted: sum += v
  result.avg = sum / float(n)
  result.p50 = sorted[int(0.50 * float(n - 1))]
  result.p95 = sorted[int(0.95 * float(n - 1))]
  result.p99 = sorted[int(0.99 * float(n - 1))]
  result.min = sorted[0]
  result.max = sorted[n - 1]

# ============================================================================
# Benchmark Result
# ============================================================================

type
  BenchResult* = object
    name*: string
    ops*: uint64
    durationMs*: uint64
    opsPerSec*: float64
    latencyAvg*: float64
    latencyP50*: float64
    latencyP95*: float64
    latencyP99*: float64
    latencyMin*: float64
    latencyMax*: float64
    cpuMs*: uint64
    memoryMb*: float64
    diskReadBytes*: uint64
    diskWriteBytes*: uint64
    diskReadIops*: uint64
    diskWriteIops*: uint64
    diskReadMB*: float64
    diskWriteMB*: float64

proc printBenchResult*(r: BenchResult) =
  echo fmt"  {r.name:<20} {r.ops:>8} ops in {r.durationMs:>6}ms | {r.opsPerSec:>10.0f} ops/s"
  echo fmt"    Lat: avg={r.latencyAvg:>7.1f}us p50={r.latencyP50:>7.1f}us p95={r.latencyP95:>7.1f}us p99={r.latencyP99:>7.1f}us"
  echo fmt"    CPU: {r.cpuMs:>6}ms"
  echo fmt"    Mem: {r.memoryMb:>6.1f}MB"
  echo fmt"    Disk: read={r.diskReadMB:>6.2f}MB({r.diskReadIops:>6} IOPS) write={r.diskWriteMB:>6.2f}MB({r.diskWriteIops:>6} IOPS)"

proc calcDiskMetrics*(diffRes: ResourceMetrics, durationMs: uint64): tuple[
    readIops, writeIops: uint64, readMB, writeMB: float64] =
  let blockSize = 4096.0
  let durationSec = float64(durationMs) / 1000.0
  if durationSec > 0:
    result.readIops = uint64(float64(diffRes.diskReadBytes) / blockSize / durationSec)
    result.writeIops = uint64(float64(diffRes.diskWriteBytes) / blockSize / durationSec)
  else:
    result.readIops = 0
    result.writeIops = 0
  result.readMB = float64(diffRes.diskReadBytes) / (1024.0 * 1024.0)
  result.writeMB = float64(diffRes.diskWriteBytes) / (1024.0 * 1024.0)

# ============================================================================
# Helper Functions
# ============================================================================

proc makeKeyStr*(prefix: string, i: uint64, keySize: int): string =
  ## Optimized key creation - similar to Rust make_key
  let suffix = "_" & $i
  let padLen = keySize - prefix.len - suffix.len
  if padLen > 0:
    result = newString(keySize)
    # Fill with 'k' as in Rust
    for j in 0 ..< keySize:
      if j < prefix.len:
        result[j] = prefix[j]
      elif j < prefix.len + padLen:
        result[j] = 'k'
      else:
        result[j] = suffix[j - prefix.len - padLen]
  else:
    result = prefix & suffix

proc makeValueStr*(valueSize: int): string =
  ## Optimized value creation - pre-allocate the string
  if valueSize > 0:
    result = newString(valueSize)
    for j in 0 ..< valueSize:
      result[j] = 'v'
  else:
    result = "v"

# ============================================================================
# Benchmark: LSM Tree v2
# ============================================================================

when defined(nimV2):
  proc verifyData*(tree: Tree, prefix: string, indices: seq[uint64],
                   expectedSeqno: uint64, keySize, valueSize: int): bool =
    ## Verify that all keys in indices have correct values
    var verified = 0
    var failed = 0
    let expectedValue = makeValueStr(valueSize)
    let seqno = SeqNo(expectedSeqno.int64)

    for i in indices:
      let key = makeKeyStr(prefix, i, keySize)
      let getResult = tree.get(key, some(seqno))
      if getResult.isSome:
        let storedValue = getResult.get
        if storedValue == expectedValue:
          verified += 1
        else:
          if failed < 5:
            echo "  VERIFY FAILED: Key ", i, " has incorrect value (length: ",
                storedValue.len, " vs expected: ", expectedValue.len, ")"
          failed += 1
      else:
        if failed < 5:
          echo "  VERIFY FAILED: Key ", i, " not found"
        failed += 1

    if failed > 0:
      echo "  VERIFICATION FAILED: ", failed, "/", indices.len, " keys incorrect"
      return false
    else:
      echo "  VERIFIED: All ", verified, " keys correct"
      return true

  proc runLsmTreeV2Benchmark*(config: Config): seq[BenchResult] =
    var results: seq[BenchResult] = @[]

    let dbPath = config.dbPath
    if dirExists(dbPath):
      removeDir(dbPath)

    echo ""
    echo "=== Running LSM Tree v2 Benchmark ==="

    # Create tree with WAL disabled for fair comparison with Rust lsm-tree
    # (Rust lsm-tree doesn't have a WAL and doesn't auto-flush - pure in-memory)
    var cfg = newDefaultConfig(dbPath)
    cfg.walEnabled = false
    let treeResult = createNewTree(cfg, TreeId(0'u64))

    if treeResult.isErr:
      echo "Error creating tree: ", treeResult.error
      return results

    let tree = treeResult.value

    # Warmup
    echo "Warming up..."
    for i in 0'u64 ..< config.warmupOps:
      let key = makeKeyStr("warm", i, config.keySize)
      let value = makeValueStr(config.valueSize)
      discard tree.insert(key, value, SeqNo(i.int64))

    # =========================================================================
    # Sequential Writes
    # =========================================================================
    echo "Sequential writes..."
    var latency = LatencyTracker(samples: @[])
    let startRes = readResourceMetrics()
    let startTime = getTime()
    var seqno: uint64 = config.warmupOps

    for i in 0'u64 ..< config.numOps:
      let t0 = getTime()
      let key = makeKeyStr("seq", i, config.keySize)
      let value = makeValueStr(config.valueSize)
      discard tree.insert(key, value, SeqNo(seqno.int64))
      seqno += 1
      let t1 = getTime()
      latency.trackLatency((t1 - t0).inNanoseconds)

    let endTime = getTime()
    let endRes = readResourceMetrics()
    let diffRes = endRes.diffResources(startRes)
    let stats = latency.getLatencyStats()
    let durationMs = (endTime - startTime).inMilliseconds.uint64

    # Verify sequential writes
    var seqIndices: seq[uint64] = @[]
    for i in 0'u64 ..< config.numOps: seqIndices.add(i)
    var verifyOk = tree.verifyData("seq", seqIndices, seqno - 1, config.keySize,
        config.valueSize)

    let diskMetrics = calcDiskMetrics(diffRes, durationMs)

    results.add(BenchResult(
      name: "seq_writes",
      ops: config.numOps,
      durationMs: durationMs,
      opsPerSec: float64(config.numOps) * 1000.0 / float64(durationMs),
      latencyAvg: stats.avg,
      latencyP50: stats.p50,
      latencyP95: stats.p95,
      latencyP99: stats.p99,
      latencyMin: stats.min,
      latencyMax: stats.max,
      cpuMs: diffRes.cpuUserMs + diffRes.cpuSystemMs,
      memoryMb: float64(endRes.memoryRssKb) / 1024.0,
      diskReadBytes: diffRes.diskReadBytes,
      diskWriteBytes: diffRes.diskWriteBytes,
      diskReadIops: diskMetrics.readIops,
      diskWriteIops: diskMetrics.writeIops,
      diskReadMB: diskMetrics.readMB,
      diskWriteMB: diskMetrics.writeMB
    ))

    if not verifyOk:
      echo "  Retrying sequential write verification..."
      sleep(100)
      verifyOk = tree.verifyData("seq", seqIndices, seqno - 1, config.keySize,
          config.valueSize)
      if not verifyOk:
        echo "  ERROR: Sequential write verification failed after retry!"

    # =========================================================================
    # Random Writes
    # =========================================================================
    echo "Random writes..."
    randomize(42)
    var indices: seq[uint64] = @[]
    for i in 0'u64 ..< config.numOps: indices.add(i)
    shuffle(indices)

    latency = LatencyTracker(samples: @[])
    let startRes2 = readResourceMetrics()
    let startTime2 = getTime()

    for i in indices:
      let t0 = getTime()
      let key = makeKeyStr("rand", i, config.keySize)
      let value = makeValueStr(config.valueSize)
      discard tree.insert(key, value, SeqNo(seqno.int64))
      seqno += 1
      let t1 = getTime()
      latency.trackLatency((t1 - t0).inNanoseconds)

    let endTime2 = getTime()
    let endRes2 = readResourceMetrics()
    let diffRes2 = endRes2.diffResources(startRes2)
    let stats2 = latency.getLatencyStats()
    let durationMs2 = (endTime2 - startTime2).inMilliseconds.uint64

    # Verify random writes
    verifyOk = tree.verifyData("rand", indices, seqno - 1, config.keySize,
        config.valueSize)

    let diskMetrics2 = calcDiskMetrics(diffRes2, durationMs2)

    results.add(BenchResult(
      name: "rand_writes",
      ops: config.numOps,
      durationMs: durationMs2,
      opsPerSec: float64(config.numOps) * 1000.0 / float64(durationMs2),
      latencyAvg: stats2.avg,
      latencyP50: stats2.p50,
      latencyP95: stats2.p95,
      latencyP99: stats2.p99,
      latencyMin: stats2.min,
      latencyMax: stats2.max,
      cpuMs: diffRes2.cpuUserMs + diffRes2.cpuSystemMs,
      memoryMb: float64(endRes2.memoryRssKb) / 1024.0,
      diskReadBytes: diffRes2.diskReadBytes,
      diskWriteBytes: diffRes2.diskWriteBytes,
      diskReadIops: diskMetrics2.readIops,
      diskWriteIops: diskMetrics2.writeIops,
      diskReadMB: diskMetrics2.readMB,
      diskWriteMB: diskMetrics2.writeMB
    ))

    if not verifyOk:
      echo "  Retrying random write verification..."
      sleep(100)
      verifyOk = tree.verifyData("rand", indices, seqno - 1, config.keySize,
          config.valueSize)
      if not verifyOk:
        echo "  ERROR: Random write verification failed after retry!"

    # =========================================================================
    # Sequential Reads
    # =========================================================================
    echo "Sequential reads..."
    latency = LatencyTracker(samples: @[])
    let startRes3 = readResourceMetrics()
    let startTime3 = getTime()
    let expectedValue = makeValueStr(config.valueSize)

    var seqReadVerified = 0
    var seqReadFailed = 0
    for i in 0'u64 ..< config.numOps:
      let t0 = getTime()
      let key = makeKeyStr("seq", i, config.keySize)
      let getResult = tree.get(key, some(SeqNo(seqno.int64)))
      let t1 = getTime()
      latency.trackLatency((t1 - t0).inNanoseconds)

      # Verify the result
      if getResult.isSome:
        let storedValue = getResult.get
        if storedValue == expectedValue:
          seqReadVerified += 1
        else:
          if seqReadFailed < 5:
            echo "  SEQ_READ FAILED: Key ", i, " value mismatch (got ",
                storedValue.len, " bytes, expected ", expectedValue.len, ")"
          seqReadFailed += 1
      else:
        if seqReadFailed < 5:
          echo "  SEQ_READ FAILED: Key ", i, " not found"
        seqReadFailed += 1

    let endTime3 = getTime()
    let endRes3 = readResourceMetrics()
    let diffRes3 = endRes3.diffResources(startRes3)
    let stats3 = latency.getLatencyStats()
    let durationMs3 = (endTime3 - startTime3).inMilliseconds.uint64

    # Verify results
    if seqReadFailed > 0:
      echo "  SEQ_READ VERIFICATION FAILED: ", seqReadFailed, " keys incorrect"
    else:
      echo "  SEQ_READ VERIFIED: All ", seqReadVerified, " keys correct"

    let diskMetrics3 = calcDiskMetrics(diffRes3, durationMs3)

    results.add(BenchResult(
      name: "seq_reads",
      ops: config.numOps,
      durationMs: durationMs3,
      opsPerSec: float64(config.numOps) * 1000.0 / float64(durationMs3),
      latencyAvg: stats3.avg,
      latencyP50: stats3.p50,
      latencyP95: stats3.p95,
      latencyP99: stats3.p99,
      latencyMin: stats3.min,
      latencyMax: stats3.max,
      cpuMs: diffRes3.cpuUserMs + diffRes3.cpuSystemMs,
      memoryMb: float64(endRes3.memoryRssKb) / 1024.0,
      diskReadBytes: diffRes3.diskReadBytes,
      diskWriteBytes: diffRes3.diskWriteBytes,
      diskReadIops: diskMetrics3.readIops,
      diskWriteIops: diskMetrics3.writeIops,
      diskReadMB: diskMetrics3.readMB,
      diskWriteMB: diskMetrics3.writeMB
    ))

    # =========================================================================
    # Random Reads
    # =========================================================================
    echo "Random reads..."
    latency = LatencyTracker(samples: @[])
    let startRes4 = readResourceMetrics()
    let startTime4 = getTime()

    var randReadVerified = 0
    var randReadFailed = 0
    for i in indices:
      let t0 = getTime()
      let key = makeKeyStr("rand", i, config.keySize)
      let getResult = tree.get(key, some(SeqNo(seqno.int64)))
      let t1 = getTime()
      latency.trackLatency((t1 - t0).inNanoseconds)

      # Verify the result
      if getResult.isSome:
        let storedValue = getResult.get
        if storedValue == expectedValue:
          randReadVerified += 1
        else:
          if randReadFailed < 5:
            echo "  RAND_READ FAILED: Key ", i, " value mismatch (got ",
                storedValue.len, " bytes, expected ", expectedValue.len, ")"
          randReadFailed += 1
      else:
        if randReadFailed < 5:
          echo "  RAND_READ FAILED: Key ", i, " not found"
        randReadFailed += 1

    let endTime4 = getTime()
    let endRes4 = readResourceMetrics()
    let diffRes4 = endRes4.diffResources(startRes4)
    let stats4 = latency.getLatencyStats()
    let durationMs4 = (endTime4 - startTime4).inMilliseconds.uint64

    # Verify results
    if randReadFailed > 0:
      echo "  RAND_READ VERIFICATION FAILED: ", randReadFailed, " keys incorrect"
    else:
      echo "  RAND_READ VERIFIED: All ", randReadVerified, " keys correct"

    let diskMetrics4 = calcDiskMetrics(diffRes4, durationMs4)

    results.add(BenchResult(
      name: "rand_reads",
      ops: config.numOps,
      durationMs: durationMs4,
      opsPerSec: float64(config.numOps) * 1000.0 / float64(durationMs4),
      latencyAvg: stats4.avg,
      latencyP50: stats4.p50,
      latencyP95: stats4.p95,
      latencyP99: stats4.p99,
      latencyMin: stats4.min,
      latencyMax: stats4.max,
      cpuMs: diffRes4.cpuUserMs + diffRes4.cpuSystemMs,
      memoryMb: float64(endRes4.memoryRssKb) / 1024.0,
      diskReadBytes: diffRes4.diskReadBytes,
      diskWriteBytes: diffRes4.diskWriteBytes,
      diskReadIops: diskMetrics4.readIops,
      diskWriteIops: diskMetrics4.writeIops,
      diskReadMB: diskMetrics4.readMB,
      diskWriteMB: diskMetrics4.writeMB
    ))

    # =========================================================================
    # Contains Key
    # =========================================================================
    echo "Contains key..."
    latency = LatencyTracker(samples: @[])
    let startRes5 = readResourceMetrics()
    let startTime5 = getTime()

    var containsVerified = 0
    var containsFailed = 0
    for i in indices:
      let t0 = getTime()
      let key = makeKeyStr("rand", i, config.keySize)
      let containsResult = tree.contains(key, some(SeqNo(seqno.int64)))
      let t1 = getTime()
      latency.trackLatency((t1 - t0).inNanoseconds)

      # Verify: all rand keys should exist
      if containsResult:
        containsVerified += 1
      else:
        if containsFailed < 5:
          echo "  CONTAINS FAILED: Key ", i, " not found but should exist"
        containsFailed += 1

    let endTime5 = getTime()
    let endRes5 = readResourceMetrics()
    let diffRes5 = endRes5.diffResources(startRes5)
    let stats5 = latency.getLatencyStats()
    let durationMs5 = (endTime5 - startTime5).inMilliseconds.uint64

    # Verify results
    if containsFailed > 0:
      echo "  CONTAINS VERIFICATION FAILED: ", containsFailed, " keys incorrect"
    else:
      echo "  CONTAINS VERIFIED: All ", containsVerified, " keys correct"

    let diskMetrics5 = calcDiskMetrics(diffRes5, durationMs5)

    results.add(BenchResult(
      name: "contains_key",
      ops: config.numOps,
      durationMs: durationMs5,
      opsPerSec: float64(config.numOps) * 1000.0 / float64(durationMs5),
      latencyAvg: stats5.avg,
      latencyP50: stats5.p50,
      latencyP95: stats5.p95,
      latencyP99: stats5.p99,
      latencyMin: stats5.min,
      latencyMax: stats5.max,
      cpuMs: diffRes5.cpuUserMs + diffRes5.cpuSystemMs,
      memoryMb: float64(endRes5.memoryRssKb) / 1024.0,
      diskReadBytes: diffRes5.diskReadBytes,
      diskWriteBytes: diffRes5.diskWriteBytes,
      diskReadIops: diskMetrics5.readIops,
      diskWriteIops: diskMetrics5.writeIops,
      diskReadMB: diskMetrics5.readMB,
      diskWriteMB: diskMetrics5.writeMB
    ))

    # =========================================================================
    # Range Scan (Simplified - just verify memtable iteration works)
    # =========================================================================
    echo "Range scan..."
    let scanCount = min(10000, int(config.numOps))
    let scanValue = makeValueStr(config.valueSize)

    # Insert scan data
    for i in 0 ..< scanCount:
      let padded = $i
      let key = "scan_" & "00000000"[padded.len .. ^1] & padded
      let value = makeValueStr(config.valueSize)
      discard tree.insert(key, value, SeqNo(seqno.int64))
      seqno += 1

    latency = LatencyTracker(samples: @[])
    let startRes6 = readResourceMetrics()
    let startTime6 = getTime()

    var scanned = 0
    var rangeVerified = 0
    var rangeFailed = 0
    let t0 = getTime()

    # Simple range scan by reading keys sequentially
    for i in 0 ..< scanCount:
      let padded = $i
      let key = "scan_" & "00000000"[padded.len .. ^1] & padded
      let getResult = tree.get(key, some(SeqNo(seqno.int64)))
      if getResult.isSome:
        scanned += 1
        if getResult.get == scanValue:
          rangeVerified += 1
        else:
          rangeFailed += 1

    let t1 = getTime()
    latency.trackLatency((t1 - t0).inNanoseconds)

    let endTime6 = getTime()
    let endRes6 = readResourceMetrics()
    let diffRes6 = endRes6.diffResources(startRes6)
    let stats6 = latency.getLatencyStats()
    let durationMs6 = (endTime6 - startTime6).inMilliseconds.uint64

    # Verify results
    if rangeFailed > 0:
      echo "  RANGE_SCAN VERIFICATION FAILED: ", rangeFailed, " values incorrect"
    elif scanned != scanCount:
      echo "  RANGE_SCAN VERIFICATION FAILED: scanned ", scanned,
          " != expected ", scanCount
    else:
      echo "  RANGE_SCAN VERIFIED: All ", rangeVerified, " items correct"

    let diskMetrics6 = calcDiskMetrics(diffRes6, durationMs6)

    results.add(BenchResult(
      name: "range_scan",
      ops: uint64(scanned),
      durationMs: durationMs6,
      opsPerSec: float64(scanned) * 1000.0 / float64(durationMs6),
      latencyAvg: stats6.avg,
      latencyP50: stats6.p50,
      latencyP95: stats6.p95,
      latencyP99: stats6.p99,
      latencyMin: stats6.min,
      latencyMax: stats6.max,
      cpuMs: diffRes6.cpuUserMs + diffRes6.cpuSystemMs,
      memoryMb: float64(endRes6.memoryRssKb) / 1024.0,
      diskReadBytes: diffRes6.diskReadBytes,
      diskWriteBytes: diffRes6.diskWriteBytes,
      diskReadIops: diskMetrics6.readIops,
      diskWriteIops: diskMetrics6.writeIops,
      diskReadMB: diskMetrics6.readMB,
      diskWriteMB: diskMetrics6.writeMB
    ))

    # =========================================================================
    # Prefix Scan (Simplified - same as range scan for now)
    # =========================================================================
    echo "Prefix scan..."
    latency = LatencyTracker(samples: @[])
    let startRes7 = readResourceMetrics()
    let startTime7 = getTime()

    scanned = 0
    var prefixVerified = 0
    var prefixFailed = 0
    let t2 = getTime()

    # Simple prefix scan by reading keys sequentially
    for i in 0 ..< scanCount:
      let padded = $i
      let key = "scan_" & "00000000"[padded.len .. ^1] & padded
      let getResult = tree.get(key, some(SeqNo(seqno.int64)))
      if getResult.isSome:
        scanned += 1
        if getResult.get == scanValue:
          prefixVerified += 1
        else:
          prefixFailed += 1

    let t3 = getTime()
    latency.trackLatency((t3 - t2).inNanoseconds)

    let endTime7 = getTime()
    let endRes7 = readResourceMetrics()
    let diffRes7 = endRes7.diffResources(startRes7)
    let stats7 = latency.getLatencyStats()
    let durationMs7 = (endTime7 - startTime7).inMilliseconds.uint64

    # Verify results
    if prefixFailed > 0:
      echo "  PREFIX_SCAN VERIFICATION FAILED: ", prefixFailed, " values incorrect"
    elif scanned != scanCount:
      echo "  PREFIX_SCAN VERIFICATION FAILED: scanned ", scanned,
          " != expected ", scanCount
    else:
      echo "  PREFIX_SCAN VERIFIED: All ", prefixVerified, " items correct"

    let diskMetrics7 = calcDiskMetrics(diffRes7, durationMs7)

    results.add(BenchResult(
      name: "prefix_scan",
      ops: uint64(scanned),
      durationMs: durationMs7,
      opsPerSec: float64(scanned) * 1000.0 / float64(durationMs7),
      latencyAvg: stats7.avg,
      latencyP50: stats7.p50,
      latencyP95: stats7.p95,
      latencyP99: stats7.p99,
      latencyMin: stats7.min,
      latencyMax: stats7.max,
      cpuMs: diffRes7.cpuUserMs + diffRes7.cpuSystemMs,
      memoryMb: float64(endRes7.memoryRssKb) / 1024.0,
      diskReadBytes: diffRes7.diskReadBytes,
      diskWriteBytes: diffRes7.diskWriteBytes,
      diskReadIops: diskMetrics7.readIops,
      diskWriteIops: diskMetrics7.writeIops,
      diskReadMB: diskMetrics7.readMB,
      diskWriteMB: diskMetrics7.writeMB
    ))

    # =========================================================================
    # Deletions
    # =========================================================================
    echo "Deletions..."
    let deleteCount = int(config.numOps) div 2

    latency = LatencyTracker(samples: @[])
    let startRes8 = readResourceMetrics()
    let startTime8 = getTime()

    for i in 0 ..< deleteCount:
      let t0 = getTime()
      let key = makeKeyStr("seq", uint64(i), config.keySize)
      discard tree.remove(key, SeqNo(seqno.int64))
      seqno += 1
      let t1 = getTime()
      latency.trackLatency((t1 - t0).inNanoseconds)

    let endTime8 = getTime()
    let endRes8 = readResourceMetrics()
    let diffRes8 = endRes8.diffResources(startRes8)
    let stats8 = latency.getLatencyStats()
    let durationMs8 = (endTime8 - startTime8).inMilliseconds.uint64

    let diskMetrics8 = calcDiskMetrics(diffRes8, durationMs8)

    results.add(BenchResult(
      name: "deletions",
      ops: uint64(deleteCount),
      durationMs: durationMs8,
      opsPerSec: float64(deleteCount) * 1000.0 / float64(durationMs8),
      latencyAvg: stats8.avg,
      latencyP50: stats8.p50,
      latencyP95: stats8.p95,
      latencyP99: stats8.p99,
      latencyMin: stats8.min,
      latencyMax: stats8.max,
      cpuMs: diffRes8.cpuUserMs + diffRes8.cpuSystemMs,
      memoryMb: float64(endRes8.memoryRssKb) / 1024.0,
      diskReadBytes: diffRes8.diskReadBytes,
      diskWriteBytes: diffRes8.diskWriteBytes,
      diskReadIops: diskMetrics8.readIops,
      diskWriteIops: diskMetrics8.writeIops,
      diskReadMB: diskMetrics8.readMB,
      diskWriteMB: diskMetrics8.writeMB
    ))

    # Cleanup
    if dirExists(dbPath):
      removeDir(dbPath)

    return results

# ============================================================================
# Main
# ============================================================================

proc main() =
  let config = parseArgs()

  echo "=== LSM Tree v2 Benchmark ==="
  echo fmt"  Ops: {config.numOps}, Key: {config.keySize}, Value: {config.valueSize}"
  echo fmt"  Warmup: {config.warmupOps}"
  echo ""

  # CPU info
  echo "CPU: ", countProcessors(), " cores"
  echo ""

  when defined(nimV2):
    let results = runLsmTreeV2Benchmark(config)

    echo ""
    echo "=== Results ==="
    for r in results:
      r.printBenchResult()

    # JSON output
    echo ""
    echo "=== JSON ==="
    echo "{"
    echo "  \"engine\": \"lsm_tree_v2\","
    echo "  \"config\": {"
    echo "    \"num_ops\": " & $config.numOps & ","
    echo "    \"key_size\": " & $config.keySize & ","
    echo "    \"value_size\": " & $config.valueSize
    echo "  },"
    echo "  \"results\": ["
    for i, r in results:
      let comma = if i < results.len - 1: "," else: ""
      echo "    { \"name\": \"" & r.name & "\", \"ops\": " & $r.ops &
          ", \"ops_per_sec\": " & formatFloat(r.opsPerSec, ffDecimal, 0) &
          ", \"latency_avg_us\": " & formatFloat(r.latencyAvg, ffDecimal, 1) &
          ", \"cpu_ms\": " & $r.cpuMs & ", \"memory_mb\": " & formatFloat(
          r.memoryMb, ffDecimal, 1) & " }" & comma
    echo "  ]"
    echo "}"
  else:
    echo "Please compile with -d:nimV2 to run LSM Tree v2 benchmarks"
    echo "Example: nim c -d:nimV2 -p:src -r benchmarks/bench_lsm_v2.nim"

when isMainModule:
  main()
