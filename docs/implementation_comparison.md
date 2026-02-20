# Fractio Storage vs Fjall Rust - Implementation Comparison

## Overview

This document compares the Fractio Nim storage implementation with the Fjall Rust implementation, identifying areas where we diverge from the original.

---

## 1. DATABASE MODULE (db.rs vs db.nim)

### Implemented ✅
- `open()` - creates or recovers database
- `keyspace()` - creates/returns keyspaces
- `keyspaceExists()` - check if keyspace exists
- `keyspaceCount()` - number of keyspaces
- `listKeyspaceNames()` - list keyspace names
- `persist()` - persist database
- `close()` - close database
- `writeBufferSize()` - get write buffer size
- `outstandingFlushes()` - pending flush count
- `timeCompacting()` - time spent compacting
- `activeCompactions()` - active compaction count
- `compactionsCompleted()` - completed compactions
- `diskSpace()` - total disk space
- `journalDiskSpace()` - journal disk space
- `seqno()` - current sequence number
- `visibleSeqno()` - visible sequence number

### Missing ❌
- `delete_keyspace()` - delete a keyspace (Rust has this)
- `batch()` - create a write batch (Rust has WriteBatch)
- `cache_capacity()` - cache capacity setting
- `journal_count()` - number of journals

### Differences
- Rust uses `create_or_recover()` which combines logic; we have separate `createNew()` and `recover()`
- Rust has transaction support (`TxDatabase`) - we don't have transactions yet

---

## 2. SUPERVISOR MODULE (supervisor.rs vs supervisor.nim)

### Implemented ✅
- `dbConfig` - database configuration
- `writeBufferSize` - write buffer manager
- `flushManager` - flush task manager
- `seqno` - sequence number counter
- `snapshotTracker` - snapshot tracking
- `journal` - journal reference
- `backpressureLock` - backpressure lock
- `journalManager` - journal manager for sealed journal tracking
- `journalManagerLock` - lock for journal manager access

### Status
Supervisor now includes JournalManager with its own lock for thread-safe 
journal maintenance. The JournalManager uses keyspace IDs instead of 
references to avoid GC cycles.

---

## 3. JOURNAL MANAGER (journal/manager.rs vs journal/manager.nim)

### Status
- **Stub implementation** - We have the type and basic methods but it's not integrated
- JournalManager tracks sealed journals for garbage collection
- In Rust, this is used for:
  - Tracking sealed journal files
  - Knowing when journals can be deleted
  - Managing journal rotation

### Missing Integration ❌
- Not wired to supervisor
- `maintenance()` doesn't properly check keyspace seqnos
- Not used during flush operations

---

## 4. KEYSPACE MODULE (keyspace/mod.rs vs keyspace.nim)

### Implemented ✅
- `id()` - get keyspace ID
- `name()` - get keyspace name
- `clear()` - clear keyspace
- `diskSpace()` - disk space used
- `approximateLen()` - approximate length
- `isEmpty()` - check if empty
- `containsKey()` - check if key exists
- `get()` - get value
- `insert()` - insert key-value
- `remove()` - remove key (tombstone)
- `removeWeak()` - weak remove
- `rotateMemtable()` - rotate memtable
- `l0TableCount()` - L0 table count
- `tableCount()` - total table count
- `majorCompaction()` - major compaction
- `iter()` - iterate all entries
- `rangeIter()` - range iterator
- `prefixIter()` - prefix iterator
- `requestRotation()` - request memtable rotation
- `requestFlush()` - request flush

### Missing ❌
- `fragmented_blob_bytes()` - for KV separation
- `start_ingestion()` - bulk ingestion
- `metrics()` - LSM tree metrics
- `path()` - keyspace path
- `size_of()` - get size of a key
- `first_key_value()` - get first key-value
- `last_key_value()` - get last key-value
- `is_kv_separated()` - check KV separation
- `rotate_memtable_and_wait()` - blocking rotation

### Differences
- We use callbacks for rotation (`requestRotationCb`), Rust sends messages directly to worker pool
- Our iterators load all data into memory; Rust uses lazy iteration

---

## 5. LSM TREE MODULE

### Note
- Fjall uses a separate `lsm-tree` crate
- We have our own implementation in `lsm_tree/`

### Implemented ✅
- Memtable with sorted entries
- SSTable write with prefix compression
- SSTable read with index/data blocks
- Basic get/insert/remove
- Memtable rotation
- Flush to SSTable

### Missing ❌
- **Leveled compaction** - we only have stub major_compact
- **Tiered compaction** - not implemented
- **FIFO compaction** - not implemented
- **Bloom filters** - not implemented
- **Block cache** - not implemented
- **Compression** (LZ4, etc.) - not implemented
- **SSTable block compression** - not implemented
- **KV separation** (blob files) - not implemented

---

## 6. FLUSH MODULE

### Implemented ✅
- FlushManager with task queue
- FlushWorker that processes tasks
- After flush, compaction is triggered if L0 threshold exceeded

### Differences
- Rust flush worker gets flush lock from LSM tree
- Our implementation is simpler but may miss some edge cases

---

## 7. COMPACTION MODULE

### Implemented ✅
- Compaction worker skeleton
- Stats tracking
- Compaction trigger on L0 threshold
- **majorCompact()** - Merges SSTables from one level to the next
- **Tombstone GC** - Removes old tombstones during compaction
- **K-way merge** - Heap-based merging of multiple SSTables
- **CompactionStrategy** - Leveled, Tiered, FIFO strategies
- **Strategy selection** - Keyspace options include compaction strategy

### Missing ❌
- **Tiered compaction strategy** - Implemented but not fully integrated
- **FIFO compaction strategy** - Implemented but not fully integrated
- **Compaction filters** - Not implemented
- **Choice of compaction** (smart table picking based on size/score)

### Status
Leveled compaction is working. L0 tables are compacted into L1, 
overlapping tables are merged, and tombstones are garbage collected based 
on the GC watermark. Compaction strategies (Leveled, Tiered, FIFO) are 
implemented and selectable via keyspace options.

---

## 8. WORKER POOL

### Implemented ✅
- Background worker threads (4 workers)
- Message queue for flush/compact/rotate
- L0 threshold triggers compaction
- GC-safe cleanup

### Differences
- Rust has more sophisticated worker coordination
- Rust workers prioritize flush over compaction when pool size > 1
- Rust has `JournalManager` integration in worker pool

---

## 9. SNAPSHOT MODULE

### Implemented ✅
- Snapshot nonce
- Snapshot tracker
- Sequence number tracking
- GC watermark

### Missing ❌
- Snapshot isolation for reads (our iterators load all data at creation time)

---

## 10. ITERATORS

### Implemented ✅
- `iter()` - all entries
- `rangeIter()` - range query
- `prefixIter()` - prefix scan

### Differences
- **CRITICAL**: Our iterators are **eager** (load all data into memory)
- Rust iterators are **lazy** (stream from disk)
- This is a significant performance difference for large datasets

### Missing ❌
- Reverse iteration
- Double-ended iteration
- Lazy loading from SSTables

---

## 11. TRANSACTIONS

### Status: NOT IMPLEMENTED ❌

Rust has two transaction modes:
1. `SingleWriterTxDatabase` - single writer transactions
2. `OptimisticTxDatabase` - optimistic concurrency control

We have no transaction support.

---

## 12. WRITE BATCH

### Status: STUB ⚠️

We have `batch.nim` but it's not fully integrated with the database.

---

## 13. BLOOM FILTERS

### Status: NOT IMPLEMENTED ❌

Rust uses bloom filters for faster key lookups.
Our implementation does linear/binary search without filters.

---

## 14. COMPRESSION

### Status: NOT IMPLEMENTED ❌

Rust supports LZ4 (default), Zstd, etc.
Our SSTables store uncompressed data.

---

## 15. BLOCK CACHE

### Status: NOT IMPLEMENTED ❌

Rust has an LRU block cache for frequently accessed blocks.
We re-read blocks from disk on each access.

---

## Critical Missing Features for Production

1. ~~**Working Compaction**~~ - DONE: majorCompact() now merges SSTables with tombstone GC
2. ~~**JournalManager Integration**~~ - DONE: Journal cleanup after flush with GC-safe design
3. ~~**Compaction Strategy Selection**~~ - DONE: Leveled, Tiered, FIFO strategies implemented
4. **Lazy Iterators** - Current implementation loads all data into memory
5. **Block Cache** - Performance issue for repeated reads
6. **Bloom Filters** - Performance issue for point lookups
7. **Compression** - Storage size and I/O performance
8. **Transactions** - ACID guarantees

## Moderate Priority

8. `delete_keyspace()` - Can't delete keyspaces
9. Write batch integration
10. Key-value separation for large values

## Lower Priority

11. Metrics
12. Ingestion API
13. `first_key_value` / `last_key_value`
14. `size_of()` for individual keys
