# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Abstract Tree
##
## Generic Tree API - the core trait for LSM tree operations.

import std/[options, locks]
import types
import error
import iter_guard
import version
import table_ext

type
  RangeItem* = LsmResult[KvPair]
  FlushToTablesResult* = tuple[tables: seq[pointer], blobFiles: Option[seq[pointer]]]

# Abstract Tree concept
type
  AbstractTree* = ref object of RootObj
    ## Generic Tree API

    # Core operations
    id*: TreeId
    config*: pointer               # Config

    # State
    activeMemtable*: pointer       # Arc[Memtable]
    sealedMemtables*: seq[pointer] # Vec<Arc<Memtable>>
    tables*: seq[pointer]          # Vec<Table>

    # Locks
    flushLock*: pointer            # Mutex
    versionLock*: pointer          # RwLock

# Range bounds concept
type
  RangeBounds*[T] = concept r
    r.startBound() is BoundKind
    r.endBound() is BoundKind

# AbstractTree methods
proc printTrace*(t: AbstractTree, key: Slice): LsmResult[void] =
  ## Debug method for tracing MVCC history of a key
  errVoid(newIoError("Not implemented"))

proc tableFileCacheSize*(t: AbstractTree): int =
  ## Returns the number of cached table file descriptors
  0

proc versionMemtableSizeSum*(t: AbstractTree): uint64 =
  ## Sum of memtable sizes
  0

proc nextTableId*(t: AbstractTree): TableId =
  ## Get next table ID
  TableId(0)

proc treeId*(t: AbstractTree): TreeId =
  ## Get tree ID
  t.id

proc getInternalEntry*(t: AbstractTree, key: Slice, seqno: SeqNo): LsmResult[
    Option[InternalValue]] =
  ## Get internal entry (not just user value)
  err[Option[InternalValue]](newIoError("Not implemented"))

proc currentVersion*(t: AbstractTree): pointer =
  ## Get current version
  nil

proc getVersionHistoryLock*(t: AbstractTree): pointer =
  ## Get version history lock
  nil

proc getFlushLock*(t: AbstractTree): pointer =
  ## Acquire flush lock
  nil

proc flushActiveMemtable*(t: AbstractTree, evictionSeqno: SeqNo): LsmResult[void] =
  ## Seal active memtable and flush to table(s)
  errVoid(newIoError("Not implemented"))

proc flush*(t: AbstractTree, lock: pointer, seqnoThreshold: SeqNo): LsmResult[
    Option[uint64]] =
  ## Synchronously flush pending sealed memtables to tables
  err[Option[uint64]](newIoError("Not implemented"))

proc iter*(t: AbstractTree, seqno: SeqNo, index: Option[tuple[memtable: pointer,
    seqno: SeqNo]]): pointer =
  ## Return iterator scanning entire tree
  nil

proc prefixIt*(t: AbstractTree, prefix: Slice, seqno: SeqNo, index: Option[
    tuple[memtable: pointer, seqno: SeqNo]]): pointer =
  ## Return iterator over prefixed set of items
  nil

proc rangeIt*(t: AbstractTree, startKey, endKey: Slice, seqno: SeqNo,
    index: Option[tuple[memtable: pointer, seqno: SeqNo]]): pointer =
  ## Return iterator over range of items
  nil

proc tombstoneCount*(t: AbstractTree): uint64 =
  ## Approximate number of tombstones
  0

proc weakTombstoneCount*(t: AbstractTree): uint64 =
  ## Approximate number of weak tombstones
  0

proc weakTombstoneReclaimableCount*(t: AbstractTree): uint64 =
  ## Reclaimable weak tombstones
  0

proc dropRange*(t: AbstractTree, startKey, endKey: Slice): LsmResult[void] =
  ## Drop tables fully contained in range
  errVoid(newIoError("Not implemented"))

proc treeClear*(t: AbstractTree): LsmResult[void] =
  ## Atomically drop all tables and clear memtables
  errVoid(newIoError("Not implemented"))

proc majorCompact*(t: AbstractTree, targetSize: uint64,
    seqnoThreshold: SeqNo): LsmResult[void] =
  ## Perform major compaction
  errVoid(newIoError("Not implemented"))

proc staleBlobBytes*(t: AbstractTree): uint64 =
  ## Disk space used by stale blobs
  0

proc filterSize*(t: AbstractTree): uint64 =
  ## Space usage of all filters
  0

proc pinnedFilterSize*(t: AbstractTree): int =
  ## Memory usage of pinned filters
  0

proc pinnedBlockIndexSize*(t: AbstractTree): int =
  ## Memory usage of pinned index blocks
  0

proc versionFreeListLen*(t: AbstractTree): int =
  ## Length of version free list
  0

proc flushToTables*(t: AbstractTree, stream: pointer): LsmResult[Option[
    FlushToTablesResult]] =
  ## Flush memtable to tables
  err[Option[FlushToTablesResult]](newIoError("Not implemented"))

proc registerTables*(t: AbstractTree, tables: seq[pointer], blobFiles: Option[
    seq[pointer]], fragMap: pointer, sealedMemtableIds: seq[MemtableId],
        gcWatermark: SeqNo): LsmResult[void] =
  ## Register flushed tables into tree
  errVoid(newIoError("Not implemented"))

proc clearActiveMemtable*(t: AbstractTree) =
  ## Clear active memtable atomically
  discard

proc sealedMemtableCount*(t: AbstractTree): int =
  ## Number of sealed memtables
  t.sealedMemtables.len

proc compactIt*(t: AbstractTree, strategy: pointer,
    seqnoThreshold: SeqNo): LsmResult[void] =
  ## Perform compaction on levels
  errVoid(newIoError("Not implemented"))

proc getNextTableId*(t: AbstractTree): TableId =
  ## Next table's ID
  TableId(0)

proc treeConfig*(t: AbstractTree): pointer =
  ## Get tree config
  t.config

proc highestSeqno*(t: AbstractTree): Option[SeqNo] =
  ## Highest sequence number
  none(SeqNo)

proc activeMemtableRef*(t: AbstractTree): pointer =
  ## Active memtable
  t.activeMemtable

proc treeType*(t: AbstractTree): int =
  ## Tree type (standard or blob)
  0

proc rotateMemtableIt*(t: AbstractTree): Option[pointer] =
  ## Seal active memtable
  none(pointer)

proc tableCountIt*(t: AbstractTree): int =
  ## Number of tables
  t.tables.len

proc levelTableCount*(t: AbstractTree, idx: int): Option[int] =
  ## Number of tables in level idx
  if idx < 7:
    some(0)
  else:
    none(int)

proc l0RunCount*(t: AbstractTree): int =
  ## Number of disjoint runs in L0
  0

proc blobFileCount*(t: AbstractTree): int =
  ## Number of blob files
  0

proc approximateLenIt*(t: AbstractTree): int =
  ## Approximate number of items
  0

proc diskSpaceIt*(t: AbstractTree): uint64 =
  ## Disk space usage
  0

proc highestMemtableSeqno*(t: AbstractTree): Option[SeqNo] =
  ## Highest seqno in memtable
  none(SeqNo)

proc highestPersistedSeqno*(t: AbstractTree): Option[SeqNo] =
  ## Highest seqno flushed to disk
  none(SeqNo)

proc lenIt*(t: AbstractTree, seqno: SeqNo, index: Option[tuple[
    memtable: pointer, seqno: SeqNo]]): LsmResult[int] =
  ## Count items in tree
  err[int](newIoError("Not implemented"))

proc isEmptyIt*(t: AbstractTree, seqno: SeqNo, index: Option[tuple[
    memtable: pointer, seqno: SeqNo]]): LsmResult[bool] =
  ## Check if tree is empty
  err[bool](newIoError("Not implemented"))

proc firstKeyValue*(t: AbstractTree, seqno: SeqNo, index: Option[tuple[
    memtable: pointer, seqno: SeqNo]]): Option[IterGuard] =
  ## First key-value pair
  none(IterGuard)

proc lastKeyValue*(t: AbstractTree, seqno: SeqNo, index: Option[tuple[
    memtable: pointer, seqno: SeqNo]]): Option[IterGuard] =
  ## Last key-value pair
  none(IterGuard)

proc sizeOf*(t: AbstractTree, key: Slice, seqno: SeqNo): LsmResult[Option[uint32]] =
  ## Size of a value
  err[Option[uint32]](newIoError("Not implemented"))

proc getIt*(t: AbstractTree, key: Slice, seqno: SeqNo): LsmResult[Option[Slice]] =
  ## Retrieve item from tree
  err[Option[Slice]](newIoError("Not implemented"))

proc containsKeyIt*(t: AbstractTree, key: Slice, seqno: SeqNo): LsmResult[bool] =
  ## Check if key exists
  err[bool](newIoError("Not implemented"))

proc insertIt*(t: AbstractTree, key, value: Slice, seqno: SeqNo): tuple[
    itemSize, memtableSize: uint64] =
  ## Insert key-value pair
  (0.uint64, 0.uint64)

proc removeIt*(t: AbstractTree, key: Slice, seqno: SeqNo): tuple[itemSize,
    memtableSize: uint64] =
  ## Remove item (adds tombstone)
  (0.uint64, 0.uint64)

proc removeWeakIt*(t: AbstractTree, key: Slice, seqno: SeqNo): tuple[itemSize,
    memtableSize: uint64] =
  ## Remove with weak tombstone
  (0.uint64, 0.uint64)
