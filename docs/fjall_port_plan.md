# Plan: Direct Nim Port of fjall Storage Engine

Based on the detailed gap analysis between the current Fractio storage implementation and the **fjall Rust reference**, this plan outlines the steps to bring the Nim implementation back into alignment while maintaining functional parity, zero-copy reads, Zstd compression, skip-list memtable, and avoiding the TxDatabase layer (Fractio will handle distributed MVCC above storage).

---

## Guiding Principles

- **Functional parity** – match fjall’s behavior and on-disk layout (with Nim-specific adjustments).
- **Zero-copy reads** – values stay in cached blocks; `get` returns a borrowed view into the block buffer.
- **Zstd compression** – LZ4 in fjall; we will use Zstd as primary compression codec.
- **Skip-list memtable** – keep existing design (already implemented).
- **MVCC snapshots** – provide `Snapshot` and GC watermark for version garbage collection.
- **Background workers** – flush and compaction run in a thread pool; operations non-blocking.
- **Journaling** – multi-segment WAL with rotation and safe deletion (as in fjall).
- **Compaction** – level-based strategy (Leveled or FIFO) to bound file count and enable GC.
- **Iterators** – support prefix and range scans, forward and reverse.

---

## Phase 0: Foundation (small, high-impact changes)

### 0.1 Extend SSTable footer
Add seqno range to file footer:
```
[index_offset: u64][min_seqno: u64][max_seqno: u64] (big-endian, 24 bytes total)
```
- `sstable_builder.nim`: write these during build.
- `sstable.nim`: read in `openSSTable`, store in `SSTableInfo`.
- Enables compaction correctness and WAL deletion.

### 0.2 Zero-copy read design
- Define `ValueView` (borrowed data) and `BlockRef` (cached block):
  ```nim
  type
    BlockRef* = ref object
      data*: seq[uint8]
      # additional metadata
    ValueView* = object
      data*: ptr uint8
      len*: int
      block*: BlockRef  # keeps block alive
  ```
- Implement LRU `BlockCache` (configurable size).
- `SSTable.get` uses cache; returns `Option[ValueView]`.
- `Block.get` updated to return `ValueView` (no copy of value bytes).
- Can be introduced incrementally; initially optional.

### 0.3 Extend TreeConfig
Add fields:
- `compactionStrategy`: enum (`Leveled`, `FIFO`)
- `maxLevel0Files`: int (default 4)
- `sizeRatioPerLevel`: float (default 10.0)
- `ttl`: Duration (for FIFO)
- `blockCacheSize`: int (already exists)
- `compression`: `CompressionType` (already exists)
- `fpRate`: float (bloom filter false positive rate)
- `hashRatio`: float (bloom bits proportion; may be ignored)
- `persistMode`: already exists

---

## Phase 1: WAL Management – Segments, Rotation & Deletion

Goal: Match fjall’s journal manager (multiple segment files, automatic rotation, safe deletion).

### Tasks

1. **Segment naming & numbering**
   - Journal files: `journal.<seqno>` (6-digit monotonic).
   - Active segment is highest number; writes go there.
   - When segment reaches `config.maxJournalSize` (default 64 MiB), close, seal, open new segment with next seqno.

2. **WAL format**
   - `WalWriter` and `WalReader` with record types:
     - `0`: single entry
     - `1`: batch (multiple entries)
   - Header per record: `[type: u8][length: u32][crc: u32][payload]`.
   - CRC over payload.

3. **JournalManager** (new module)
   - Tracks all segment files in directory.
   - Knows which segments are sealed (complete) and can be deleted.
   - Deletion condition:
     - All entries in segment have been flushed to SSTable(s).
     - Segment’s max seqno < current GC watermark (no snapshot depends on it).
   - On each `rotate` (memtable flush completion), manager re-evaluates deletable segments.

4. **Recovery**
   - During `newTree`, journal manager scans directory, orders segments by number, replays into fresh memtable.
   - Corrupted segment/record: skip with error logging; configurable tolerance.

5. **WAL deletion**
   - Journal manager periodically (or after each flush) attempts to delete obsolete files.

### Files
- Modify `wal.nim`
- Create `journal_manager.nim`

---

## Phase 2: Snapshots & GC Watermark

Goal: Provide MVCC snapshot handles that pin a seqno for garbage collection.

### Tasks

1. **Snapshot type**
   ```nim
   type
     Snapshot* = ref object
       seqno*: SeqNo
       refCount*: int
   ```
   - `newSnapshot(seqno: SeqNo)` returns snapshot; caller must `release()`.
   - When `refCount` drops to 0, remove from tracker.

2. **SnapshotTracker**
   ```nim
   type
     SnapshotTracker* = object
       snapshots*: seq[Snapshot]
       lock*: Lock
   ```
   - `track(snap)` and `untrack(snap)` maintain list and recompute `gcWatermark` (minimum seqno among all snapshots).
   - `getWatermark(): SeqNo`

3. **Integration with LSMTree**
   - Add `snapTracker*: SnapshotTracker` to `LSMTree`.
   - `getWatermark()` used by compaction and WAL manager.
   - `rotate` must record flushed memtable’s `maxSeqno` in `SSTableInfo`; helps WAL manager know up to which seqno a journal segment is needed.

4. **Public API**
   - `tree.openSnapshot(seqno: SeqNo): Snapshot` – Fractio transaction layer will use this at transaction start.

### Files
- Modify `types.nim` (already includes seqno in `SSTableInfo`)
- Modify `tree.nim` (add tracker, methods)
- Create `snapshot.nim`

---

## Phase 3: Block Cache, Bloom Filters & Zstd Compression

### 3.1 Block Cache (enables zero-copy)

#### Tasks

1. **Implement LRUCache**
   ```nim
   type
     CacheEntry* = object
       block*: BlockRef
       lastUsed*: MonotonicTime
     LruCache* = object
       map*: Table[BlockKey, CacheEntry]
       order*: DoublyLinkedList[BlockKey]
       capacity*: int
       lock*: Lock
   ```
   - `BlockKey = (path: string, offset: int)` identifies a block.
   - `get(key)`, `put(key, block)`, LRU eviction when over capacity.

2. **Integrate into `SSTable.get`**
   - Compute blockKey from index offset.
   - Look up in cache; miss → read from disk, parse, wrap in `BlockRef`, cache, then use.
   - `Block.get` returns `ValueView`; view holds reference to `BlockRef`.

3. **Update `Block.get` to return `ValueView`**
   - Pointer arithmetic into `block.data`.
   - Keep `block` reference inside view to prevent eviction while view exists.

### 3.2 Bloom Filters

#### Tasks

1. **Complete `bloom.nim`**
   - Implement standard bloom: `newBloom(expectedItems, fpRate)`, `add(key)`, `mightContain(key)`.
   - Use a fast hash (e.g., xxhash, murmur3).

2. **Block format extension**
   After data section, insert bloom section:
   ```
   [data_len: u32]
   [data: ...]
   [bloom_len: u32]   # 0 if no bloom
   [bloom_bytes]
   [restart_count: u32]
   [restart_offsets: u32[]]
   [checksum: u32]    # covers everything up to checksum
   ```
   - `BlockBuilder` creates bloom for all keys in block if `config.fpRate > 0`.
   - `parseBlock` reads optional bloom, stores in `Block.bloom: Option[BloomFilter]`.
   - `Block.get`: early exit if bloom present and `!mightContain(key)`.

3. **Configuration**
   - Use `fpRate` and `hashRatio` to compute bloom size per block based on expected entries (`restartInterval`) and block size.
   - If `fpRate == 0`, skip bloom.

### 3.3 Zstd Compression

#### Tasks

1. **Add Nim Zstd dependency**
   - Add `zstd` to `fractio.nimble`.
   - Create `compression/zstd_compression.nim`:
     ```nim
     proc compress*(src: openArray[uint8]): seq[uint8]
     proc decompress*(src: openArray[uint8], originalLen: int): seq[uint8]
     ```

2. **Whole-block compression**
   - After `BlockBuilder.finish()` produces uncompressed block bytes:
     - If compression enabled, compress entire block.
     - Write on disk: `[compressed_len: u32][compressed_bytes][checksum: u32]`.
   - `parseBlock` is not used directly; instead `readBlock` first reads compressed chunk, decompresses, then calls `parseBlock` on uncompressed data.

3. **Wiring**
   - `SSTableBuilder` uses compression per-block (data and index blocks).
   - `SSTable.get`: read and decompress block before parsing.
   - `TreeConfig` `compression` field drives this.

### Files
- Modify `blk.nim`
- Complete `bloom.nim`
- Create `compression/zstd_compression.nim`
- Modify `sstable_builder.nim`, `sstable.nim`
- Create `block_cache.nim` and integrate in `tree.nim` / `sstable.nim`

---

## Phase 4: Background Supervisor & Workers

Goal: Make `put`/`rotate` non‑blocking; run compaction in separate threads.

### Tasks

1. **Worker pool** (`worker_pool.nim`)
   - Spawn configurable number of threads.
   - Channel‑based work queue: `submit(task: proc())`.
   - Workers: `task = recv(); task()`.

2. **Supervisor** (`supervisor.nim`)
   - Owns the worker pool.
   - Provides:
     - `scheduleFlush(oldMem: MemTable, walPath: string)`
     - `scheduleCompaction(level: int)`
   - Tracks ongoing work to avoid overlapping flushes/compactions on same level.
   - Started during `newTree`; alive until `tree.close`.

3. **Make `tree.rotate` asynchronous**
   - Swap memtable under lock, close current WAL segment, then `supervisor.scheduleFlush(oldMem, currentWalPath)`.
   - Initialize new memtable and WAL.
   - Return immediately; any error from flush is logged.

4. **Compaction task**
   - Supervisor periodically checks (timer or after flush) if compaction needed.
   - `CompactionTask(level)` picks candidate tables per strategy (see Phase 7).
   - Compaction worker merges tables, writes new SSTable, swaps atomically into levels, deletes old files.
   - After new SSTable added, may trigger WAL segment deletion.

5. **Shutdown**
   - `tree.close` submits shutdown tasks, waits for workers to finish, joins threads.

### Files
- Modify `tree.nim` (add `supervisor` field, remove synchronous flush logic)
- Create `worker_pool.nim`
- Create `supervisor.nim`
- Create `compaction_worker.nim` (details in Phase 7)

---

## Phase 5: Database & Keyspace API

Goal: Provide fjall-like public API.

### Tasks

1. **Database type**
   ```nim
   type
     Database* = ref object
       rootPath*: string
       config*: DatabaseConfig
       blockCache*: LruCache
       supervisor*: Supervisor
       keyspaces*: Table[string, Keyspace]
       lock*: Lock
   ```
   - `newDatabase(dir: string, config: DatabaseConfig): Result[Database, StorageError]`
   - `open()`: create root dir, init block cache and supervisor, load metadata.
   - `keyspace(name: string, opts: KeyspaceCreateOptions): Result[Keyspace, StorageError]`
   - `persist(mode: PersistMode)` – flush all WALs, sync files.

2. **Keyspace type**
   ```nim
   type
     Keyspace* = ref object
       tree*: LSMTree
       name*: string
   ```
   - Methods:
     - `insert(key, value)`
     - `get(key) -> Option[ValueView]`
     - `remove(key)` (tombstone)
     - `writeBatch(batch: WriteBatch)`
     - `prefix(prefix)`: iterator
     - `range(start, end)`: iterator
   - `iter(maxSeqno: SeqNo)`: base iterator over merged streams.

3. **WriteBatch**
   - `WriteBatch` = seq of `InternalEntry` (same seqno or individual? fjall uses increasing seqnos).
   - `keyspace.write(batch)` writes entries atomically (under lock, to WAL, memtable).

4. **Migration from current API**
   - Direct `LSMTree` use in tests will be replaced with `Database`/`Keyspace`.

### Files
- Create `database.nim`
- Create `keyspace.nim`

---

## Phase 6: Iterators & Range Queries

Goal: Support `prefix` and `range` scans, forward and reverse.

### Tasks

1. **SSTableIterator** (new)
   - Iterates over blocks in order, within each block over visible entries.
   - Uses block cache when loading next block.
   - Yields `(key, value)` with seqno metadata if needed.

2. **TreeIterator** (merge iterator) (new)
   - Sources: memtable iterator + one SSTableIterator per level (from newest to oldest).
   - Merge using priority queue (min‑heap) keyed by current entry’s key.
   - On tie (same key), keep entry with highest seqno ≤ maxSeqno, drop others.
   - Advances sources until no source has current key ≤ current max.
   - Supports `next()` and `hasNext()`.
   - Reverse iterator: similar but max‑heap and iterate blocks/entries backwards.

3. **Keyspace helpers**
   - `prefix(p)`: create `TreeIterator` with `filter = key StartsWith p`.
   - `range(a..b)`: create `TreeIterator` with `filter = key in [a, b]`.

4. **Efficiency**
   - Merge iterator can skip entire levels if bloom filter test fails for the current key; future optimization.

### Files
- Create `sstable_iterator.nim`
- Create `merge_iterator.nim`
- Modify `tree.nim` to provide iterator creation.
- Extend `keyspace.nim` with `prefix`/`range`.

---

## Phase 7: Compaction (Leveled Strategy)

Goal: Bounded SSTable count, reclaim space, drop obsolete versions.

### Tasks

1. **CompactionStrategy interface**
   ```nim
   type
     CompactionStrategy* = object
       isNeeded(levels: array[seq[SSTableInfo]]): bool
       pickFiles(level: int, levels: ...): seq[SSTableInfo]  # returns input tables
       targetLevel(level: int): int
   ```
   - **Leveled**: if `levels[0].len > maxLevel0Files`, pick the oldest `maxLevel0Files` files to compact into level 1. Also compact level 1 if its total size > `sizeRatioPerLevel` * base size of level 0, etc.
   - **FIFO**: any table older than `ttl` is eligible; compact to same level (just drop if fully superseded? Actually FIFO may just drop old files; but we need to ensure superseded keys are not lost. Simpler: implement Leveled first.

2. **Compaction worker**
   - Given list of `SSTableInfo` (from one or more levels) and a target level:
     - Merge‑sort all entries from all tables (priority queue).
     - Keep only latest visible version per key (seqno ≤ current watermark? Actually we include all visible versions; but obsolete versions (< watermark) can be dropped entirely. Also drop entries that are shadows (older versions of same key).
     - Write result via `SSTableBuilder` to a new file.
   - Success: under `tree.lock`:
     - Add new `SSTableInfo` to `levels[targetLevel]`.
     - Remove input files from their levels.
     - Delete input files from disk.
   - Failure: log, abort; inputs remain.

3. **Triggering**
   - Supervisor’s periodic check calls `strategy.isNeeded(levels)`.
   - If true, `scheduleCompaction(level)`.

### Files
- Create `compaction_strategy.nim`
- Expand `compaction_worker.nim` (create)
- Modify `tree.nim` to add levels and strategy.

---

## Phase 8: Testing & Performance

### Tasks

1. **Unit tests**
   - WAL rotation, recovery, corruption handling.
   - SnapshotTracker watermark updates.
   - Block cache hit/miss/eviction.
   - Bloom filter accuracy.
   - Zstd round‑trip.
   - Compaction correctness, file count reduction, GC.
   - Iterators: order, deduplication, reverse, prefix/range.

2. **Integration tests**
   - Multi‑threaded put/get with flushes and compactions.
   - Crash recovery: write, kill, reopen, validate.
   - Snapshot isolation: multiple snapshots, different seqnos.

3. **Benchmarks**
   - Throughput (ops/sec) for put/get.
   - Latency percentiles.
   - Space amplification.

4. **Concurrency stress**
   - Many writer threads, many reader threads, mixed with compaction.
   - Verify no data races or deadlocks.

5. **Coverage**
   - Aim for >95% line coverage; use `koch test --metrics`.

---

## Current Storage Module Structure (Before)

```
src/fractio/storage/
├── types.nim
├── tree.nim                # LSMTree (memtable, WAL, immutableTables)
├── memtable.nim            # SkipList + lock
├── wal.nim                 # Simple WAL (single file, no rotation)
├── sstable.nim             # Reader (no cache, no bloom, no compression)
├── sstable_builder.nim     # Builder (no bloom, no compression, no seqno in footer)
├── blk.nim                 # Block format (no bloom)
├── bloom.nim               # Unused stub
├── compression/
│   └── lz4_compression.nim # stub (identity)
├── util.nim
└── result.nim
```

---

## Target Structure (After)

```
src/fractio/storage/
├── types.nim
├── tree.nim                      # LSMTree with supervisor, snapshots, levels
├── memtable.nim                  # unchanged (skip-list)
├── wal.nim                       # enhanced: segmented, rotation
├── journal_manager.nim           # new: segment lifecycle, deletion
├── sstable.nim                   # with block cache, Zstd, seqno footer
├── sstable_builder.nim           # with bloom, Zstd, seqno in footer
├── sstable_iterator.nim          # new
├── blk.nim                       # with optional bloom
├── bloom.nim                     # completed implementation
├── block_cache.nim               # new LRU cache
├── compression/
│   ├── zstd_compression.nim     # new
│   └── (maybe lz4 removed)
├── util.nim
├── result.nim
├── snapshot.nim                  # new
├── supervisor.nim                # new (worker pool + task scheduling)
├── worker_pool.nim               # new
├── compaction_strategy.nim       # new (Leveled/FIFO)
├── compaction_worker.nim         # new
├── merge_iterator.nim            # new (tree-level iterator)
├── database.nim                  # new (top-level API)
└── keyspace.nim                  # new (user Keyspace)
```

---

## Open Questions / Decisions

1. **Binary compatibility with fjall files** – **Not required** (functional parity only, separate on-disk format acceptable).  
2. **Transactional API** – **Omitted**; Fractio will build on top of storage’s MVCC (use `Snapshot` and `WriteBatch`).  
3. **Compression** – **Zstd** chosen over LZ4.  
4. **Memtable** – **Keep skip‑list** (already implemented).  
5. **Reverse iteration** – Deferred until basic forward works.  
6. **Zero-copy** – Implement gradually; initially may return copies, later switch to `ValueView`.  
7. **Block cache capacity** – Use `config.blockCacheSize` (bytes).  
8. **Threading model** – Supervisor with fixed thread pool; I/O blocking on disk reads okay.

---

## Implementation Order (Prioritized)

1. Phase 0 (footer, config, zero-copy groundwork)
2. Phase 1 (WAL segmentation & rotation)
3. Phase 2 (Snapshots & GC watermark)
4. Phase 3 (Block cache + bloom + Zstd) – can be iterative: cache first, then bloom, then compression.
5. Phase 4 (Supervisor + async flush)
6. Phase 7 (Compaction – Leveled first)
7. Phase 5 (Database & Keyspace API)
8. Phase 6 (Iterators – needed for prefix/range; may be done concurrently with Database)
9. Phase 8 (Testing, benchmarks)
10. Phase 9 (Optimization: lock‑free reads, advanced policies)

Phase 7 (compaction) can be streamlined once the supervisor is in place. Phases can overlap where independent (e.g., bloom while Zstd).

---

## Expected Outcomes

- A storage engine that functionally mirrors fjall’s capabilities: durability via WAL, fast point reads via memtable + block cache + bloom, efficient scans via iterators, bounded storage via compaction, MVCC via seqnos and snapshots.
- Thread‑safe, production‑ready implementation with clear separation of concerns.
- Test suite with high coverage, including concurrency and crash recovery.
- Ready for integration into Fractio’s distributed layer (sharding, replication, transaction manager).

---

*Document created: 2026-02-18*
*Status: Approved, ready for implementation*
