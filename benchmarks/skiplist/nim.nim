# crossbeam-skiplist (Nim) benchmark
#
# Measures:
# - Insert throughput
# - Lookup throughput
# - Iteration throughput
# - Combined insert/remove throughput

import std/[times, random, atomics, options]

const NUM_ELEMENTS = 100_000

# Helper to convert Duration to seconds as float
proc toSeconds(d: Duration): float =
  d.inNanoseconds().float64() / 1_000_000_000.0

# Import our crossbeam-skiplist implementation
import fractio/storage/lsm_tree_v2/crossbeam_skiplist

proc makeKey(i: int): uint64 =
  (uint64(i) * 17) + 255

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
    discard list.insert(key, not key)

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
      # Verify value is correct (should be not key)
      if val.get() == (not key):
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
      # Verify value is correct (should be not key)
      if val.get() == (not key):
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
    discard list.insert(key, not key)
  for i in 0 ..< NUM_ELEMENTS:
    let key = makeKey(i)
    discard list.remove(key)
  duration = getTime() - start
  opsPerSec = float64(NUM_ELEMENTS * 2) / duration.toSeconds()
  echo " insert_remove: ", NUM_ELEMENTS * 2, " ops in ", duration, " | ",
    opsPerSec, " ops/s"

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
    discard list2.insert(key, not key)
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
