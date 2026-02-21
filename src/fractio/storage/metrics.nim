# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Metrics API for LSM Tree Monitoring
##
## Provides comprehensive metrics for monitoring LSM tree performance:
## - LSM tree structure metrics (table counts, sizes)
## - Memory usage metrics (memtables, block cache, bloom filters)
## - I/O metrics (reads, writes, cache hits/misses)
## - Compaction metrics (active, completed, time spent)

import std/[atomics, strutils]

# LSM Tree Level Metrics
type
  LevelMetrics* = object
    ## Per-level metrics for the LSM tree
    tableCount*: int         ## Number of SSTables in this level
    sizeBytes*: uint64       ## Total size of SSTables in bytes
    indexSizeBytes*: uint64  ## Total index block size
    filterSizeBytes*: uint64 ## Total bloom filter size

# LSM Tree Metrics
type
  LsmTreeMetrics* = object
    ## Comprehensive LSM tree metrics
    #
    # Structure metrics
    levelCount*: int ## Number of levels
    levelMetrics*: seq[LevelMetrics] ## Per-level metrics
    activeMemtableSize*: uint64 ## Active memtable size in bytes
    sealedMemtableCount*: int ## Number of sealed memtables
    sealedMemtableSize*: uint64 ## Total sealed memtable size

    # Disk metrics
    diskSpaceBytes*: uint64 ## Total disk space used
    blobDiskSpaceBytes*: uint64 ## Blob file disk space (if KV separation)

    # Memory metrics (approximate)
    bloomFilterMemoryBytes*: uint64 ## Memory used by bloom filters
    blockCacheMemoryBytes*: uint64 ## Memory used by block cache

    # Read metrics
    reads*: uint64 ## Total read operations
    cacheHits*: uint64 ## Block cache hits
    cacheMisses*: uint64 ## Block cache misses

    # Write metrics
    writes*: uint64 ## Total write operations
    writeBytes*: uint64 ## Total bytes written
    batches*: uint64 ## Number of batch commits

  # Keyspace Metrics (includes LsmTreeMetrics plus keyspace-specific data)
  KeyspaceMetrics* = object
    ## Metrics for a single keyspace
    name*: string         ## Keyspace name
    id*: uint64           ## Keyspace ID
    tree*: LsmTreeMetrics ## LSM tree metrics

  # Database Metrics (aggregate across all keyspaces)
  DatabaseMetrics* = object
    ## Aggregate metrics for the entire database
    keyspaceCount*: int        ## Number of keyspaces
    totalDiskSpace*: uint64    ## Total disk space across keyspaces
    totalJournalSpace*: uint64 ## Journal disk space
    writeBufferSize*: uint64   ## Current write buffer size

    # Aggregated LSM metrics
    totalReads*: uint64        ## Total reads across all keyspaces
    totalWrites*: uint64       ## Total writes across all keyspaces
    totalCacheHits*: uint64    ## Total cache hits
    totalCacheMisses*: uint64  ## Total cache misses

    # Compaction metrics
    activeCompactions*: int    ## Currently active compactions
    compactionsCompleted*: int ## Total completed compactions
    timeCompactingUs*: uint64  ## Time spent compacting (microseconds)

# Atomic counters for thread-safe metric updates
type
  MetricCounters* = object
    ## Thread-safe counters for metric updates
    reads*: Atomic[uint64]
    writes*: Atomic[uint64]
    writeBytes*: Atomic[uint64]
    batches*: Atomic[uint64]
    cacheHits*: Atomic[uint64]
    cacheMisses*: Atomic[uint64]

# Constructor for counters
proc newMetricCounters*(): MetricCounters =
  result = MetricCounters()
  result.reads.store(0, moRelaxed)
  result.writes.store(0, moRelaxed)
  result.writeBytes.store(0, moRelaxed)
  result.batches.store(0, moRelaxed)
  result.cacheHits.store(0, moRelaxed)
  result.cacheMisses.store(0, moRelaxed)

# Increment operations (thread-safe using fetchAdd)
proc incReads*(counters: var MetricCounters, n: uint64 = 1) =
  discard fetchAdd(counters.reads, n, moRelaxed)

proc incWrites*(counters: var MetricCounters, bytes: uint64 = 0) =
  discard fetchAdd(counters.writes, 1'u64, moRelaxed)
  if bytes > 0:
    discard fetchAdd(counters.writeBytes, bytes, moRelaxed)

proc incBatches*(counters: var MetricCounters) =
  discard fetchAdd(counters.batches, 1'u64, moRelaxed)

proc incCacheHit*(counters: var MetricCounters) =
  discard fetchAdd(counters.cacheHits, 1'u64, moRelaxed)

proc incCacheMiss*(counters: var MetricCounters) =
  discard fetchAdd(counters.cacheMisses, 1'u64, moRelaxed)

# Get snapshot of counters as individual values
proc getReads*(counters: var MetricCounters): uint64 =
  counters.reads.load(moRelaxed)

proc getWrites*(counters: var MetricCounters): uint64 =
  counters.writes.load(moRelaxed)

proc getWriteBytes*(counters: var MetricCounters): uint64 =
  counters.writeBytes.load(moRelaxed)

proc getBatches*(counters: var MetricCounters): uint64 =
  counters.batches.load(moRelaxed)

proc getCacheHits*(counters: var MetricCounters): uint64 =
  counters.cacheHits.load(moRelaxed)

proc getCacheMisses*(counters: var MetricCounters): uint64 =
  counters.cacheMisses.load(moRelaxed)

# Calculate cache hit rate
proc cacheHitRate*(metrics: LsmTreeMetrics): float =
  ## Returns the cache hit rate as a percentage (0.0 - 100.0)
  let total = metrics.cacheHits + metrics.cacheMisses
  if total == 0:
    return 0.0
  return (metrics.cacheHits.float / total.float) * 100.0

proc cacheHitRate*(metrics: DatabaseMetrics): float =
  ## Returns the aggregate cache hit rate as a percentage (0.0 - 100.0)
  let total = metrics.totalCacheHits + metrics.totalCacheMisses
  if total == 0:
    return 0.0
  return (metrics.totalCacheHits.float / total.float) * 100.0

# L0 metrics (commonly accessed)
proc l0TableCount*(metrics: LsmTreeMetrics): int =
  ## Returns the number of L0 tables
  if metrics.levelMetrics.len > 0:
    return metrics.levelMetrics[0].tableCount
  return 0

proc l0SizeBytes*(metrics: LsmTreeMetrics): uint64 =
  ## Returns the total size of L0 tables
  if metrics.levelMetrics.len > 0:
    return metrics.levelMetrics[0].sizeBytes
  return 0

# Format metrics for display
proc `$`*(metrics: LevelMetrics): string =
  result = "Level(tables=" & $metrics.tableCount
  result &= ", size=" & $metrics.sizeBytes
  result &= ", index=" & $metrics.indexSizeBytes
  result &= ", filter=" & $metrics.filterSizeBytes & ")"

proc `$`*(metrics: LsmTreeMetrics): string =
  let hitRate = cacheHitRate(metrics)
  result = "LsmTreeMetrics(\n"
  result &= "  levels: " & $metrics.levelCount & "\n"
  for i, level in metrics.levelMetrics:
    result &= "    L" & $i & ": " & $level & "\n"
  result &= "  memtable: " & $metrics.activeMemtableSize & " bytes\n"
  result &= "  sealed: " & $metrics.sealedMemtableCount & " (" &
      $metrics.sealedMemtableSize & " bytes)\n"
  result &= "  disk: " & $metrics.diskSpaceBytes & " bytes\n"
  result &= "  cache: " & formatFloat(hitRate, ffDecimal, 2) & "% hit rate\n"
  result &= "  reads: " & $metrics.reads & ", writes: " & $metrics.writes & "\n"
  result &= ")"

proc `$`*(metrics: DatabaseMetrics): string =
  let hitRate = cacheHitRate(metrics)
  result = "DatabaseMetrics(\n"
  result &= "  keyspaces: " & $metrics.keyspaceCount & "\n"
  result &= "  disk: " & $metrics.totalDiskSpace & " bytes\n"
  result &= "  journal: " & $metrics.totalJournalSpace & " bytes\n"
  result &= "  write_buffer: " & $metrics.writeBufferSize & " bytes\n"
  result &= "  cache: " & formatFloat(hitRate, ffDecimal, 2) & "% hit rate\n"
  result &= "  reads: " & $metrics.totalReads & ", writes: " &
      $metrics.totalWrites & "\n"
  result &= "  compactions: " & $metrics.activeCompactions & " active, " &
      $metrics.compactionsCompleted & " completed\n"
  result &= ")"
