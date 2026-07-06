## Memory budget enforcement for Fractio.
##
## Provides a centralized mechanism to track and limit the server's RSS
## (resident set size). The budget flows through the system as follows:
##
##   memoryBudgetMB (config)
##     ├── Storage caps:
##     │     LevelDB block cache    = budgetMB * 5%   (min 1 MB)
##     │     LevelDB write buffer   = budgetMB * 5%   (min 1 MB)
##     │     WiscKey vlog buffer    = budgetMB * 5%   (min 4 MB)
##     │     Stream prefetch buffer = budgetMB * 2%   (min 100 entries)
##     │
##     ├── Protocol LRU caps (per-store):
##     │     keyVersions            = maxEntriesFromBudget(50%)
##     │     commitIndex            = maxEntriesFromBudget(50%)
##     │
##     └── Admission control:
##           When RSS >= budget, refuse new transactions and connections.
##
## The intent is **predictable, bounded memory** so that the server never
## OOMs the host. The default values are conservative; users tune
## memoryBudgetMB in `[storage]` of fractio.toml.
##
## Thread-safety: all reads of the current RSS go through `getCurrentRSSBytes()`
## which reads `/proc/self/statm` and is safe to call from any thread.

import std/[os, strutils, atomics, times]

type
  MemoryBudget* = ref object
    ## Process-wide memory budget tracker.
    ## Single instance per server, shared across threads.
    budgetBytes*: int64 ## Total RSS budget in bytes (0 = unlimited)
    storageCacheBytes*: int ## Derived cap for LevelDB block cache
    storageWriteBufferBytes*: int ## Derived cap for LevelDB write buffer
    vlogBufferBytes*: int64 ## Derived cap for WiscKey vlog GC buffer
    streamBufferEntries*: int ## Derived cap for streaming prefetch buffer
    overBudgetCount*: Atomic[int64] ## Times admission control refused a request

const
  DEFAULT_MIN_BLOCK_CACHE_BYTES* = 1 * 1024 * 1024                    ## 1 MB
  DEFAULT_MIN_WRITE_BUFFER_BYTES* = 1 * 1024 * 1024                   ## 1 MB
  DEFAULT_MIN_VLOG_BUFFER_BYTES*: int64 = 4 * 1024 * 1024             ## 4 MB
  DEFAULT_MIN_STREAM_ENTRIES* = 100

proc newMemoryBudget*(budgetMB: int): MemoryBudget =
  ## Create a memory budget tracker.
  ## budgetMB <= 0 means unlimited — all caps fall back to defaults.
  new(result)
  if budgetMB <= 0:
    result.budgetBytes = 0
    result.storageCacheBytes = 8 * 1024 * 1024
    result.storageWriteBufferBytes = 4 * 1024 * 1024
    result.vlogBufferBytes = 64 * 1024 * 1024
    result.streamBufferEntries = 1000
  else:
    result.budgetBytes = int64(budgetMB) * 1024 * 1024
    # Storage: 5% of budget for block cache, 5% for write buffer.
    # These are the two dominant WiscKey/LevelDB in-memory structures.
    let fivePct = int(result.budgetBytes div 20)
    result.storageCacheBytes = max(fivePct, DEFAULT_MIN_BLOCK_CACHE_BYTES)
    result.storageWriteBufferBytes = max(fivePct, DEFAULT_MIN_WRITE_BUFFER_BYTES)
    # WiscKey vlog GC buffer: 5% of budget
    result.vlogBufferBytes = max(fivePct.int64, DEFAULT_MIN_VLOG_BUFFER_BYTES)
    # Stream prefetch: 2% of budget / per-entry estimate (~200 bytes).
    # Floor at 100 entries; cap at 5000 (existing large config).
    let twoPct = int(result.budgetBytes div 50)
    let entries = twoPct div 200
    result.streamBufferEntries = clamp(entries,
      DEFAULT_MIN_STREAM_ENTRIES, 5000)
  result.overBudgetCount.store(0, moRelaxed)

proc budgetEnabled*(mb: MemoryBudget): bool {.inline.} =
  ## Returns true when the budget is configured (non-zero).
  mb != nil and mb.budgetBytes > 0

proc getCurrentRSSBytes*(): int64 =
  ## Read the current RSS (resident set size) of this process from
  ## /proc/self/statm. Returns 0 on failure (e.g. macOS, missing procfs).
  ## Cost: one open+read+close syscall, ~microseconds.
  try:
    if not fileExists("/proc/self/statm"): return 0
    let content = readFile("/proc/self/statm")
    let parts = content.splitWhitespace()
    if parts.len < 2: return 0
    # statm format: size resident shared text lib data dirty
    # RSS is field 2, in pages. Page size is typically 4 KB.
    let pages = parseInt(parts[1])
    return int64(pages) * 4096
  except CatchableError, IOError, ValueError:
    return 0

proc isOverBudget*(mb: MemoryBudget): bool =
  ## Returns true when the process RSS is at or above the configured budget.
  ## When the budget is disabled (unlimited), always returns false.
  if not mb.budgetEnabled: return false
  let rss = getCurrentRSSBytes()
  if rss == 0: return false # Can't measure → don't refuse
  return rss >= mb.budgetBytes

proc recordOverBudgetRefusal*(mb: MemoryBudget) =
  ## Bump the over-budget refusal counter. Cheap, lock-free.
  if mb != nil:
    discard mb.overBudgetCount.fetchAdd(1, moRelaxed)

proc getOverBudgetCount*(mb: MemoryBudget): int64 =
  if mb == nil: return 0
  return mb.overBudgetCount.load(moRelaxed)

# ---------------------------------------------------------------------------
# Standalone helper: derive caps directly from an MB value, useful for
# callers that don't want to keep a MemoryBudget reference around.
# ---------------------------------------------------------------------------

proc deriveStorageCaps*(budgetMB: int): tuple[blockCacheBytes,
    writeBufferBytes: int] =
  ## Pure-function variant for callers that only need the storage caps.
  if budgetMB <= 0:
    return (8 * 1024 * 1024, 4 * 1024 * 1024)
  let fivePct = int(int64(budgetMB) * 1024 * 1024 div 20)
  result.blockCacheBytes = max(fivePct, DEFAULT_MIN_BLOCK_CACHE_BYTES)
  result.writeBufferBytes = max(fivePct, DEFAULT_MIN_WRITE_BUFFER_BYTES)

proc deriveStreamEntries*(budgetMB: int): int =
  if budgetMB <= 0: return 1000
  let twoPct = int(int64(budgetMB) * 1024 * 1024 div 50)
  let entries = twoPct div 200
  result = clamp(entries, DEFAULT_MIN_STREAM_ENTRIES, 5000)

# ---------------------------------------------------------------------------
# Heap trimming via madvise(MADV_DONTNEED).
#
# PROBLEM: Under both --mm:atomicArc and --mm:orc, the default Nim allocator
# keeps freed pages in its free list (mapped but unused) to avoid the cost
# of munmap/mmap on every deallocation. These pages count as RSS even though
# the GC has marked them free. A long-running server with bursty allocation
# patterns (e.g., mtBatch processing 1000+ ops per RPC) can grow RSS to
# 1.5-2GB even when only a few hundred MB of logical data is alive.
# Empirically measured: 100K-row test grew node1 from 14MB to 1837MB RSS.
#
# FIX: Walk /proc/self/maps and call madvise(MADV_DONTNEED) on large
# anonymous rw regions. This tells the kernel "these pages are idle" and
# the kernel reclaims them. Pages still in use are skipped (madvise
# silently no-ops on pages that can't be released). The call is safe to
# make periodically from any thread.
#
# Cost: a single read of /proc/self/maps (~50KB) + N madvise syscalls
# where N is the number of large anonymous regions (typically 5-20). Total
# latency ~1-5 ms per call.
#
# Threshold: only trim regions >= MIN_TRIM_REGION_BYTES (16 MB) to avoid
# thrashing small allocation arenas. Smaller regions are usually active
# thread-local arenas and trimming them would force re-mmap on next use.
# ---------------------------------------------------------------------------

const MIN_TRIM_REGION_BYTES* = 16 * 1024 * 1024     ## 16 MB

# MADV_DONTNEED = 4 (from /usr/include/x86_64-linux-gnu/bits/mman-linux.h).
# Declared as a `let` rather than `const` to avoid header dependency
# at compile time; the value is the same on all Linux kernels.
let madviseDontneed* = cint(4)

proc c_madvise(p: pointer, length: csize_t, advice: cint): cint {.
    importc: "madvise", header: "<sys/mman.h>".}

type
  TrimResult* = object
    regionsScanned*: int ## Total anonymous rw regions found
    regionsTrimmed*: int ## Regions that madvise accepted
    bytesTrimmed*: int64 ## Total bytes released to the OS
    elapsedMicros*: int  ## Wall clock time for the operation

proc trimHeapMemory*(minRegionBytes: int = MIN_TRIM_REGION_BYTES): TrimResult {.
    gcsafe, raises: [].} =
  ## Walk /proc/self/maps and call madvise(MADV_DONTNEED) on all anonymous
  ## rw regions >= minRegionBytes. Returns counts and elapsed time.
  ##
  ## Use this after operations that allocate large transient bursts (e.g.,
  ## after a `mtBatch` RPC that processed 1000+ ops, or after a full GC)
  ## to return the freed pages to the OS. Pages still in use are silently
  ## skipped by madvise, so this is safe to call from any thread.
  result = TrimResult()
  let startNanos = epochTime() * 1_000_000
  try:
    if not fileExists("/proc/self/maps"):
      return
    let content = readFile("/proc/self/maps")
    for line in content.splitLines:
      if line.len == 0: continue
      let parts = line.split(' ')
      if parts.len < 2: continue
      # parts[0] = "start-end", parts[1] = "rwxp...", rest = path info.
      # Anonymous rw regions have parts[1] starting with "rw" and
      # either no parts[5] (truly anonymous) or parts[5] = "[heap]".
      let perms = parts[1]
      if perms.len < 2 or perms[0] != 'r' or perms[1] != 'w': continue
      if parts.len > 5 and parts[5].len > 0:
        # Named mappings (e.g., shared libraries) are skipped.
        continue
      let rangeParts = parts[0].split('-')
      if rangeParts.len != 2: continue
      try:
        let startAddr = parseHexInt(rangeParts[0])
        let endAddr = parseHexInt(rangeParts[1])
        let size = endAddr - startAddr
        inc result.regionsScanned
        if size < minRegionBytes: continue
        let regionPtr = cast[pointer](startAddr)
        let rc = c_madvise(regionPtr, size.csize_t, madviseDontneed)
        if rc == 0:
          inc result.regionsTrimmed
          result.bytesTrimmed += size
      except ValueError, OSError: discard
  except IOError, OSError: discard
  result.elapsedMicros = int(epochTime() * 1_000_000 - startNanos)

proc trimHeapMemoryIfOverBudget*(mb: MemoryBudget,
    minRegionBytes: int = MIN_TRIM_REGION_BYTES): TrimResult {.
    gcsafe, raises: [].} =
  ## Convenience: only trim when the process is over its configured budget.
  ## When the budget is disabled (unlimited), this is a no-op.
  result = TrimResult()
  if not mb.budgetEnabled: return
  let rss = getCurrentRSSBytes()
  if rss == 0 or rss < mb.budgetBytes: return
  result = trimHeapMemory(minRegionBytes)

# SAFETY NOTE: The trim functions above are effective for releasing
# reclaimed pages but can crash Nim programs that share the heap across
# threads (e.g., under --mm:atomicArc, freed chunks may be temporarily
# retained in per-thread free lists that other threads can race on).
# DO NOT call trimHeapMemory from a thread that shares the heap with
# allocator worker threads. Prefer trimHeapMemoryIfOverBudget which is
# only invoked when the budget is exceeded AND the process is otherwise
# likely to be killed by the OOM killer.
