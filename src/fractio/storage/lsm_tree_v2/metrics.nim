# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Metrics
##
## Runtime metrics for the LSM tree.

import std/atomics

type
  Metrics* = ref object
    ## Runtime metrics
    tableFileOpenedUncached*: Atomic[int]
    tableFileOpenedCached*: Atomic[int]
    indexBlockLoadIo*: Atomic[int]
    filterBlockLoadIo*: Atomic[int]
    dataBlockLoadIo*: Atomic[int]
    indexBlockLoadCached*: Atomic[int]
    filterBlockLoadCached*: Atomic[int]
    dataBlockLoadCached*: Atomic[int]
    filterQueries*: Atomic[int]
    ioSkippedByFilter*: Atomic[int]
    dataBlockIoRequested*: Atomic[uint64]
    indexBlockIoRequested*: Atomic[uint64]
    filterBlockIoRequested*: Atomic[uint64]

proc newMetrics*(): Metrics =
  var tableFileOpenedUncached: Atomic[int]
  var tableFileOpenedCached: Atomic[int]
  var indexBlockLoadIo: Atomic[int]
  var filterBlockLoadIo: Atomic[int]
  var dataBlockLoadIo: Atomic[int]
  var indexBlockLoadCached: Atomic[int]
  var filterBlockLoadCached: Atomic[int]
  var dataBlockLoadCached: Atomic[int]
  var filterQueries: Atomic[int]
  var ioSkippedByFilter: Atomic[int]
  var dataBlockIoRequested: Atomic[uint64]
  var indexBlockIoRequested: Atomic[uint64]
  var filterBlockIoRequested: Atomic[uint64]

  Metrics(
    tableFileOpenedUncached: tableFileOpenedUncached,
    tableFileOpenedCached: tableFileOpenedCached,
    indexBlockLoadIo: indexBlockLoadIo,
    filterBlockLoadIo: filterBlockLoadIo,
    dataBlockLoadIo: dataBlockLoadIo,
    indexBlockLoadCached: indexBlockLoadCached,
    filterBlockLoadCached: filterBlockLoadCached,
    dataBlockLoadCached: dataBlockLoadCached,
    filterQueries: filterQueries,
    ioSkippedByFilter: ioSkippedByFilter,
    dataBlockIoRequested: dataBlockIoRequested,
    indexBlockIoRequested: indexBlockIoRequested,
    filterBlockIoRequested: filterBlockIoRequested
  )

proc tableFileCacheHitRate*(m: Metrics): float =
  let uncached = load(m.tableFileOpenedUncached)
  let cached = load(m.tableFileOpenedCached)
  if cached + uncached == 0:
    return 1.0
  return float(cached) / float(cached + uncached)

proc dataBlockIo*(m: Metrics): uint64 = load(m.dataBlockIoRequested)
proc indexBlockIo*(m: Metrics): uint64 = load(m.indexBlockIoRequested)
proc filterBlockIo*(m: Metrics): uint64 = load(m.filterBlockIoRequested)
proc blockIo*(m: Metrics): uint64 = dataBlockIo(m) + indexBlockIo(m) +
    filterBlockIo(m)

proc dataBlockLoadCount*(m: Metrics): int = load(m.dataBlockLoadCached) + load(
    m.dataBlockLoadIo)
proc indexBlockLoadCount*(m: Metrics): int = load(m.indexBlockLoadCached) +
    load(m.indexBlockLoadIo)

proc filterQueryCount*(m: Metrics): int = load(m.filterQueries)
proc ioSkippedByFilterCount*(m: Metrics): int = load(m.ioSkippedByFilter)
