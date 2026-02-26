# crossbeam-skiplist (Nim) benchmark
#
# Measures:
# - Insert throughput
# - Lookup throughput
# - Iteration throughput
# - Combined insert/remove throughput
# - Concurrent mixed workload

import std/[times, random, atomics, options, threadpool]

const NUM_ELEMENTS = 100_000
const NUM_WORKERS = 200
const OPS_PER_WORKER = 10000
const CONCURRENT_INITIAL_KEYS = 20_000

# Helper to convert Duration to seconds as float
proc toSeconds(d: Duration): float =
  d.inNanoseconds().float64() / 1_000_000_000.0

# Import our crossbeam-skiplist implementation
import fractio/storage/lsm_tree_v2/crossbeam_skiplist

proc makeKey(i: int): uint64 =
  uint64(i) * 17 + 255

proc makeValue(key: uint64): uint64 =
  # Simply use key + 1 to avoid any overflow issues
  key + 1

# =========================================================================
# Concurrent Mixed Workload Benchmark
# =========================================================================

type
  WorkerResult = object
    reads: int
    writes: int
    verified: int

proc concurrentWorker(list: SkipList[uint64, uint64],
                     keys: seq[uint64],
                     workerId: int): WorkerResult {.thread, gcsafe.} =
  var rng = workerId + 1
  var localReads = 0
  var localWrites = 0
  var localVerified = 0
  let numKeys = keys.len

  # Do mixed read/write workload
  for i in 0 ..< OPS_PER_WORKER:
    let op = rng mod 10
    let keyIdx = rng mod numKeys
    let key = keys[keyIdx]

    if op < 7: # 70% reads
      let val = list.get(key)
      localReads += 1
      if val.isSome():
        if val.get() == makeValue(key):
          localVerified += 1
    else: # 30% writes
      discard list.insert(key, makeValue(key))
      localWrites += 1

    # Simple RNG update - avoid overflow
    rng = cast[int]((cast[uint](rng) * 1103515245'u + 1235'u) shr 16)

  result.reads = localReads
  result.writes = localWrites
  result.verified = localVerified

proc runConcurrentBenchmark(): tuple[reads, writes, verified: int] =
  var list = newSkipList[uint64, uint64]()

  # Pre-populate with some data
  var initKeys: seq[uint64] = newSeq[uint64](CONCURRENT_INITIAL_KEYS)
  for i in 0 ..< initKeys.len:
    initKeys[i] = makeKey(i)
    discard list.insert(initKeys[i], makeValue(initKeys[i]))

  # Spawn workers - only do reads first
  var workers: seq[FlowVar[WorkerResult]] = newSeq[FlowVar[WorkerResult]](NUM_WORKERS)

  for i in 0 ..< NUM_WORKERS:
    workers[i] = spawn concurrentWorker(list, initKeys, i)

  # Wait for completion
  var totalReads = 0
  var totalWrites = 0
  var totalVerified = 0

  for i in 0 ..< NUM_WORKERS:
    let result = ^workers[i]
    totalReads += result.reads
    totalWrites += result.writes
    totalVerified += result.verified

  result = (totalReads, totalWrites, totalVerified)

# =========================================================================
# Main Benchmark
# =========================================================================

proc main() =
  echo "=== crossbeam-skiplist (Nim) Benchmark ==="
  echo ""
  echo "Elements: ", NUM_ELEMENTS
  echo ""

  # =========================================================================
  # Benchmark 1: Sequential Insert
  # =========================================================================
  echo "Running sequential insert benchmark..."
  var start = getTime()

  var list = newSkipList[uint64, uint64]()
  for i in 0 ..< NUM_ELEMENTS:
    let key = makeKey(i)
    discard list.insert(key, makeValue(key))

  var duration = getTime() - start
  var opsPerSec = float64(NUM_ELEMENTS) / duration.toSeconds()
  echo " sequential_insert: ", NUM_ELEMENTS, " ops in ", duration, " | ",
    opsPerSec, " ops/s"

  # =========================================================================
  # Benchmark 2: Sequential Lookup (with value verification)
  # =========================================================================
  echo "Running sequential lookup benchmark..."
  start = getTime()
  var found = 0
  var verified = 0
  for i in 0 ..< NUM_ELEMENTS:
    let key = makeKey(i)
    let val = list.get(key)
    if val.isSome():
      found += 1
      # Verify value is correct
      if val.get() == makeValue(key):
        verified += 1
  duration = getTime() - start
  opsPerSec = float64(NUM_ELEMENTS) / duration.toSeconds()
  echo " sequential_lookup: ", NUM_ELEMENTS, " ops in ", duration, " | ",
    opsPerSec, " ops/s (", found, " found, ", verified, " verified)"

  # =========================================================================
  # Benchmark 3: Random Lookup (with value verification)
  # =========================================================================
  echo "Running random lookup benchmark..."
  randomize(42)
  var keys: seq[uint64] = newSeq[uint64](NUM_ELEMENTS)
  for i in 0 ..< NUM_ELEMENTS:
    keys[i] = makeKey(rand(0 ..< NUM_ELEMENTS))

  start = getTime()
  found = 0
  verified = 0
  for key in keys:
    let val = list.get(key)
    if val.isSome():
      found += 1
      # Verify value is correct
      if val.get() == makeValue(key):
        verified += 1
  duration = getTime() - start
  opsPerSec = float64(NUM_ELEMENTS) / duration.toSeconds()
  echo " random_lookup: ", NUM_ELEMENTS, " ops in ", duration, " | ",
    opsPerSec, " ops/s (", found, " found, ", verified, " verified)"

  # =========================================================================
  # Benchmark 4: Iteration
  # =========================================================================
  echo "Running iteration benchmark..."
  start = getTime()
  var visited = 0
  let iter = list.iter()
  while iter.hasNext():
    discard iter.next()
    visited += 1
  duration = getTime() - start
  opsPerSec = float64(visited) / duration.toSeconds()
  echo " iteration: ", visited, " ops in ", duration, " | ", opsPerSec, " ops/s"

  # =========================================================================
  # Benchmark 5: Insert + Remove
  # =========================================================================
  echo "Running insert+remove benchmark..."
  start = getTime()
  for i in 0 ..< NUM_ELEMENTS:
    let key = makeKey(i)
    discard list.insert(key, makeValue(key))
  for i in 0 ..< NUM_ELEMENTS:
    let key = makeKey(i)
    discard list.remove(key)
  duration = getTime() - start
  opsPerSec = float64(NUM_ELEMENTS * 2) / duration.toSeconds()
  echo " insert_remove: ", NUM_ELEMENTS * 2, " ops in ", duration, " | ",
    opsPerSec, " ops/s"

  # =========================================================================
  # Benchmark 6: Concurrent Mixed Workload
  # =========================================================================
  echo "Running concurrent mixed benchmark (", NUM_WORKERS, " workers)..."
  start = getTime()
  let concurrentResult = runConcurrentBenchmark()
  duration = getTime() - start
  let totalOps = concurrentResult.reads + concurrentResult.writes
  opsPerSec = float64(totalOps) / duration.toSeconds()
  echo " concurrent_mixed: ", totalOps, " ops in ", duration, " | ",
    opsPerSec, " ops/s (", concurrentResult.reads, " reads, ",
    concurrentResult.writes, " writes, ", concurrentResult.verified, " verified)"

  echo ""
  echo "=== JSON Output ==="
  echo "{"
  echo "  \"engine\": \"crossbeam_skiplist_nim\","
  echo "  \"elements\": ", NUM_ELEMENTS, ","
  echo "  \"results\": {"

  # Re-run to get JSON-compatible times
  var list2 = newSkipList[uint64, uint64]()
  var startSeq = getTime()
  for i in 0 ..< NUM_ELEMENTS:
    let key = makeKey(i)
    discard list2.insert(key, makeValue(key))
  var seqInsert = getTime() - startSeq

  var startLookup = getTime()
  for i in 0 ..< NUM_ELEMENTS:
    let key = makeKey(i)
    discard list2.get(key)
  var seqLookup = getTime() - startLookup

  echo "    \"sequential_insert\": ", float64(NUM_ELEMENTS) /
    seqInsert.toSeconds(), ","
  echo "    \"sequential_lookup\": ", float64(NUM_ELEMENTS) /
    seqLookup.toSeconds(), ","
  echo "    \"iteration\": ", visited
  echo "  }"
  echo "}"

when isMainModule:
  main()
