# Fractio Storage vs Fjall Rust - Implementation Comparison

## Overview

This document compares the Fractio Nim storage implementation with the Fjall Rust implementation, identifying areas where we diverge from the original.

---

## Module Mapping

| Rust (fjall) | Nim (fractio) | Status |
|-------------|---------------|--------|
| src/db.rs | storage/db.nim | ✅ Implemented |
| src/keyspace/mod.rs | storage/keyspace.nim | ✅ Implemented |
| src/supervisor.rs | storage/supervisor.nim | ✅ Implemented |
| src/journal/mod.rs | storage/journal.nim | ✅ Implemented |
| src/journal/writer.rs | storage/journal/writer.nim | ✅ Implemented |
| src/journal/reader.rs | storage/journal/reader.nim | ✅ Implemented |
| src/journal/manager.rs | storage/journal/manager.nim | ✅ Implemented |
| src/journal/entry.rs | storage/journal/entry.nim | ✅ Implemented |
| src/journal/batch_reader.rs | storage/journal/batch_reader.nim | ✅ Implemented |
| src/flush/worker.rs | storage/flush/worker.nim | ✅ Implemented |
| src/flush/manager.rs | storage/flush/manager.nim | ✅ Implemented |
| src/flush/task.rs | storage/flush/task.nim | ✅ Implemented |
| src/compaction/worker.rs | storage/compaction/worker.nim | ✅ Implemented |
| src/batch/mod.rs | storage/batch.nim | ✅ Implemented |
| src/batch/item.rs | storage/batch/item.nim | ✅ Implemented |
| src/worker_pool.rs | storage/worker_pool.nim | ✅ Implemented |
| src/snapshot.rs | storage/snapshot.nim | ✅ Implemented |
| src/snapshot_tracker.rs | storage/snapshot_tracker.nim | ✅ Implemented |
| src/stats.rs | storage/stats.nim | ✅ Implemented |
| src/write_buffer_manager.rs | storage/write_buffer_manager.nim | ✅ Implemented |
| src/locked_file.rs | storage/locked_file.nim | ✅ Implemented |
| src/poison_dart.rs | storage/poison_dart.nim | ✅ Implemented |
| src/error.rs | storage/error.nim | ✅ Implemented |
| src/file.rs | storage/file.nim | ✅ Implemented |
| src/path.rs | storage/path.nim | ✅ Implemented |
| src/version.rs | storage/version.nim | ✅ Implemented |
| src/recovery.rs | storage/recovery.nim | ✅ Implemented |
| src/meta_keyspace.rs | storage/meta_keyspace.nim | ✅ Implemented |
| src/guard.rs | storage/guard.nim | ✅ Implemented |
| src/iter.rs | storage/iter.nim | ✅ Implemented |
| src/readable.rs | storage/readable.nim | ✅ Implemented |
| src/ingestion.rs | storage/ingestion.nim | ⚠️ Stub |
| src/drop.rs | - | ❌ Not needed (GC handles cleanup) |
| src/builder.rs | storage/builder.nim | ✅ Implemented |
| src/db_config.rs | storage/db_config.nim | ✅ Implemented |
| src/keyspace/options.rs | storage/keyspace/options.nim | ⚠️ Partial |
| src/keyspace/config/*.rs | storage/keyspace/config/*.nim | ⚠️ Partial |
| src/tx/*.rs | - | ❌ Not implemented |
| lsm-tree crate | storage/lsm_tree/*.nim | ✅ Custom implementation |

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
- `snapshot()` - open a cross-keyspace snapshot
- `batch()` - create a write batch
- `deleteKeyspace()` - delete a keyspace

### Differences

| Feature | Rust (fjall) | Nim (fractio) | Notes |
|---------|-------------|---------------|-------|
| Thread management | Uses Arc, RwLock, Mutex | Uses Lock, Atomic | Nim uses different concurrency primitives |
| Background thread counter | `active_thread_counter: Arc<AtomicUsize>` | `activeThreadCounter: Atomic[int]` | Same concept, different type |
| Drop behavior | Custom Drop impl clears cyclic Arcs | GC handles cleanup | Nim's GC handles most cleanup automatically |
| Temp DB cleanup | `clean_path_on_drop` config option | Not implemented | Could add |
| Version check | V3 format required, V2 migration tool | Simple version marker | Rust has more sophisticated versioning |

---

## 2. KEYSPACE MODULE (keyspace/mod.rs vs keyspace.nim)

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

### Missing ❌
- `fragmented_blob_bytes()` - for KV separation
- `start_ingestion()` - bulk ingestion (stub exists)
- `size_of()` - get size of a key
- `is_kv_separated()` - check KV separation
- `rotate_memtable_and_wait()` - blocking rotation

### Added 2026-02-21 ✅
- `metrics()` - LSM tree metrics
- `path()` - keyspace path
- `firstKeyValue()` - get first key-value
- `lastKeyValue()` - get last key-value

### Differences

| Feature | Rust (fjall) | Nim (fractio) | Notes |
|---------|-------------|---------------|-------|
| LSM tree | Uses external lsm-tree crate | Custom implementation | Different architecture |
| Config options | Extensive (block size, compression, filter policies) | Basic | Rust has more fine-grained control |
| Worker messaging | Uses flume channel | Uses custom message queue | Similar concept |
| Write stall | `local_backpressure()` with thresholds | Basic implementation | Rust has more sophisticated flow control |
| Lock file | Per-keyspace lock file | Shared database lock | Different locking strategy |

---

## 3. JOURNAL MODULE

### Status
- ✅ Basic journal implementation
- ✅ Writer with batch support
- ✅ Reader for recovery
- ✅ Entry serialization
- ✅ JournalManager for sealed journals
- ✅ Batch reader

### Differences

| Feature | Rust (fjall) | Nim (fractio) | Notes |
|---------|-------------|---------------|-------|
| Compression | Configurable compression type | Not implemented | Could add |
| Compression threshold | Configurable threshold | Not implemented | |
| Journal rotation | Automatic based on size | Manual rotation | Rust is more automated |
| Persist modes | Buffer, SyncData, SyncAll | Same modes | ✅ Aligned |

---

## 4. LSM TREE MODULE (lsm-tree crate vs lsm_tree/*.nim)

**Note:** Fjall uses a separate `lsm-tree` crate. Fractio has its own implementation.

### Implemented ✅
- Memtable with skip list (using sorted seq for simplicity)
- SSTable with prefix compression
- Data blocks with restart points
- Index blocks
- Bloom filters
- Block cache
- Lazy iterators
- Compaction

### Missing ❌
- **KV Separation (Blob files)** - Large values stored separately
- **Level count configuration** - Fixed number of levels
- **Descriptor table** - File handle cache
- **Partitioned index/filter blocks** - For very large SSTables
- **Block hash index** - Hash-based block lookup optimization

### Differences

| Feature | Rust (lsm-tree) | Nim (fractio) | Notes |
|---------|----------------|---------------|-------|
| Memtable | Skip list | Sorted sequence | Rust has O(log n) ops |
| Compression | LZ4, Zstd configurable | Zlib (zippy) | Different compression |
| Filter policy | Per-level bloom filter config | Single bloom filter per SSTable | Rust more configurable |
| Block size policy | Per-level block sizes | Single block size | |
| Restart interval | Per-level restart intervals | Single restart interval | |
| Index block compression | Configurable | Not compressed | |
| Version history | Full version tracking | Basic version tracking | |

---

## 5. FLUSH MODULE

### Status: ✅ Complete

| Feature | Rust (fjall) | Nim (fractio) | Notes |
|---------|-------------|---------------|-------|
| Flush worker | run() function | run() function | ✅ Same |
| Flush manager | Queue-based | Queue-based | ✅ Same |
| Flush task | Arc<Keyspace> | Keyspace reference | ✅ Same |
| GC watermark | Uses snapshot tracker | Uses snapshot tracker | ✅ Same |

---

## 6. COMPACTION MODULE

### Status: ✅ Mostly Complete

| Feature | Rust (fjall) | Nim (fractio) | Notes |
|---------|-------------|---------------|-------|
| Leveled compaction | ✅ | ✅ | Aligned |
| Tiered compaction | ✅ | ⚠️ Stub | Rust has full impl |
| FIFO compaction | ✅ | ⚠️ Stub | Rust has full impl |
| Compaction strategy | Arc<dyn CompactionStrategy> | enum CompactionStrategy | Different approach |
| Tombstone GC | Uses gc_watermark | Uses gc_watermark | ✅ Same |
| Table target size | 64MB default | Not configurable | |
| Feedback from strategy | Planned | Not implemented | |

---

## 7. WORKER POOL

### Status: ✅ Complete

| Feature | Rust (fjall) | Nim (fractio) | Notes |
|---------|-------------|---------------|-------|
| Thread count | Configurable | 4 threads default | |
| Message types | Flush, Compact, RotateMemtable, Close | Same | ✅ Aligned |
| Priority | Worker 0 prioritizes flush | No priority | Rust has optimization |
| Journal rotation | In flush worker | In flush worker | ✅ Same |
| Backpressure | Checked in worker | Checked in worker | ✅ Same |

---

## 8. WRITE BATCH

### Status: ✅ Complete (Updated 2026-02-21)

| Feature | Rust (fjall) | Nim (fractio) | Notes |
|---------|-------------|---------------|-------|
| Atomic commit | Uses journal | Uses journal | ✅ Aligned |
| Durability mode | Configurable | Configurable | ✅ Aligned |
| Write buffer update | Updates size after commit | Updates size | ✅ Aligned |
| Write stall check | Per-keyspace backpressure | Memtable rotation check | Partial |
| Single seqno | All items same seqno | All items same seqno | ✅ Aligned |

**Implementation Details (db.nim commit()):**
1. Acquires journal lock
2. Checks poisoned flag (TOCTOU)
3. Gets single batch seqno
4. Writes entire batch to journal
5. Persists if durability mode set
6. Applies to memtables with batch seqno
7. Publishes seqno to snapshot tracker
8. Updates write buffer size
9. Checks memtable rotation on affected keyspaces

---

## 9. BLOOM FILTERS

### Status: ✅ Complete

| Feature | Rust (fjall) | Nim (fractio) | Notes |
|---------|-------------|---------------|-------|
| Implementation | In lsm-tree crate | bloom_filter.nim | |
| Hash function | Multiple options | MurmurHash3 | |
| FPR config | Per-level config | 1% default | |
| Last level optimization | Can disable for last level | Not optimized | Saves ~90% filter space |

---

## 10. BLOCK CACHE

### Status: ✅ Complete

| Feature | Rust (fjall) | Nim (fractio) | Notes |
|---------|-------------|---------------|-------|
| LRU eviction | ✅ | ✅ | Aligned |
| Capacity config | ✅ | ✅ | Aligned |
| Statistics | ✅ | ✅ | Aligned |
| SSTable invalidation | ✅ | ✅ | Aligned |
| Handle cache | Uses Arc | Uses ref | Different ownership |

---

## 11. SSTABLE COMPRESSION

### Status: ✅ Complete

| Feature | Rust (fjall) | Nim (fractio) | Notes |
|---------|-------------|---------------|-------|
| Algorithm | LZ4 (default), Zstd | Zlib (zippy) | Different algorithms |
| Per-level config | ✅ | ❌ | |
| Index compression | Configurable | Not compressed | |
| Threshold | Skip if not worth it | Skip if not worth it | ✅ Same |

---

## 12. LAZY ITERATORS

### Status: ✅ Complete

| Feature | Rust (fjall) | Nim (fractio) | Notes |
|---------|-------------|---------------|-------|
| Memtable iterator | ✅ | ✅ | Aligned |
| SSTable iterator | ✅ | ✅ | Aligned |
| Merge iterator | K-way merge | K-way merge | ✅ Aligned |
| Range iterator | ✅ | ✅ | Aligned |
| Prefix iterator | ✅ | ✅ | Aligned |
| Snapshot isolation | ✅ | ⚠️ Basic | |

---

## 13. WRITE STALL / BACKPRESSURE

### Status: ✅ Complete (Added 2026-02-21)

| Feature | Rust (fjall) | Nim (fractio) | Notes |
|---------|-------------|---------------|-------|
| L0 throttle threshold | 20 tables | 20 tables | ✅ Aligned |
| L0 halt threshold | 30 tables | 30 tables | ✅ Aligned |
| Sealed memtable halt | 4 memtables | 4 memtables | ✅ Aligned |
| Throttle method | CPU busy-wait | CPU busy-wait | ✅ Aligned |
| Halt method | sleep(10ms) | sleep(10ms) | ✅ Aligned |
| Memtable halt | sleep(100ms) | sleep(100ms) | ✅ Aligned |

**Implementation (keyspace.nim):**
- `checkWriteHalt()`: Blocks while L0 >= 30, sleeps 10ms per iteration
- `localBackpressure()`: 
  - Level 1: Throttle at L0 >= 20 (CPU busy-wait)
  - Level 2: Halt at L0 >= 30 (sleep-based)
  - Level 3: Halt at sealed >= 4 (sleep 100ms)

**Constants (write_delay.nim):**
- `Threshold = 20`
- `HaltThreshold = 30`
- `MaxSealedMemtables = 4`
- `StepSize = 10000`

---

## 14. SNAPSHOTS

### Status: ⚠️ Partial

| Feature | Rust (fjall) | Nim (fractio) | Notes |
|---------|-------------|---------------|-------|
| Cross-keyspace snapshot | ✅ | ❌ | |
| Nonce tracking | SnapshotNonce | Basic tracker | |
| GC watermark | ✅ | ✅ | Aligned |
| Snapshot iterator | Uses nonce | Uses seqno | Different approach |

---

## 15. TRANSACTIONS

### Status: ❌ Not Implemented

Rust has two transaction modes:
1. `SingleWriterTxDatabase` - single writer transactions
2. `OptimisticTxDatabase` - optimistic concurrency control

We have no transaction support.

**Files missing:**
- tx/mod.rs
- tx/optimistic/*.rs
- tx/single_writer/*.rs
- tx/write_tx.rs

---

## 16. KV SEPARATION (BLOB STORAGE)

### Status: ❌ Not Implemented

Rust supports storing large values in separate blob files to keep SSTables small.

**Missing:**
- Blob file writer/reader
- Blob garbage collection
- `kv_separation_opts` in keyspace config
- `fragmented_blob_bytes()` method
- `blob_file_count()` method

---

## Critical Missing Features for Production Parity

### High Priority
1. **True atomic batch commit** - Use journal for atomicity
2. **Transactions** - At least single-writer transactions
3. **Write stall/throttle** - Proper backpressure

### Medium Priority
4. **KV Separation** - For large value workloads
5. **Level-specific configurations** - Block size, compression, filters
6. **Metrics** - Detailed LSM tree metrics
7. **First/last key-value** - Efficient min/max access

### Lower Priority
8. **Descriptor table** - File handle caching
9. **Partitioned blocks** - For very large SSTables
10. **Block hash index** - Optimization for point lookups
11. **Ingestion API** - Bulk loading optimization
12. **Cross-keyspace snapshots**

---

## Test Coverage Comparison

| Module | Rust Tests | Nim Tests | Notes |
|--------|-----------|-----------|-------|
| Database | db_test.rs, db_open.rs, recovery_*.rs | 20 integration tests | |
| Keyspace | keyspace_*.rs | Covered in integration | |
| Journal | journal/test.rs, batch_recovery.rs | Basic tests | |
| Batch | batch.rs | 9 unit tests | |
| Transactions | tx_*.rs, write_tx.rs | 0 | Not implemented |
| Iterators | prefix_complex.rs, iter lifetime | 8 lazy iter tests | |
| Flush/Compaction | write_during_read.rs, fifo_*.rs | Covered in integration | |

---

## Architecture Differences

### Rust (Fjall)
- Uses external `lsm-tree` crate for core LSM functionality
- Heavy use of `Arc<RwLock<T>>` for shared state
- Flume channels for worker messaging
- Custom Drop implementations for cleanup
- Async-friendly design (can be used with tokio)

### Nim (Fractio)
- Custom LSM tree implementation integrated into storage module
- Uses Lock and Atomic for shared state
- Custom message queue for worker pool
- Garbage collector handles most cleanup
- Synchronous design with background threads

---

## Recommendations

1. ~~**Priority 1: Journal-based batch commit**~~ - ✅ COMPLETED (2026-02-21)
2. ~~**Priority 2: Write stall**~~ - ✅ COMPLETED (2026-02-21)
3. **Priority 3: Transactions** - Start with single-writer transactions
4. **Priority 4: Metrics** - Add detailed metrics for monitoring
5. **Priority 5: KV Separation** - For workloads with large values

---

## File-by-File Detailed Comparison

### db.rs vs db.nim

**Methods in Rust NOT in Nim:**
- `cache_capacity()` - Returns cache capacity
- `journal_count()` - Returns number of journals
- `create_or_recover()` - Combined create/recover (internal)
- `check_version()` - Version compatibility check (more sophisticated)

**Methods in Nim NOT in Rust:**
- None (Nim is a subset)

### keyspace/mod.rs vs keyspace.nim

**Methods in Rust NOT in Nim:**
- `fragmented_blob_bytes()` - Blob GC metrics
- `start_ingestion()` - Bulk load
- `metrics()` - LSM tree metrics (gated by feature flag)
- `path()` - Get keyspace path
- `size_of()` - Get size of a key
- `first_key_value()` - Get first KV pair
- `last_key_value()` - Get last KV pair
- `is_kv_separated()` - Check if blob-enabled
- `rotate_memtable_and_wait()` - Blocking rotation

**Methods NOW in Nim (matching Rust):**
- `checkWriteHalt()` - Write stall check ✅
- `localBackpressure()` - Per-keyspace flow control ✅

### batch/mod.rs vs batch.nim

**Rust batch commit:**
1. Acquires journal lock
2. Writes entire batch to journal with single seqno
3. Applies to memtables
4. Updates write buffer size
5. Checks for write stall per-keyspace

**Nim batch commit (UPDATED 2026-02-21):**
1. ✅ Acquires journal lock
2. ✅ Writes entire batch to journal with single seqno
3. ✅ Applies to memtables
4. ✅ Updates write buffer size
5. ✅ Checks for memtable rotation

### worker_pool.rs vs worker_pool.nim

**Key differences:**
- Rust uses `flume::bounded` channel
- Rust has worker 0 prioritize flush over compaction
- Rust has more sophisticated journal rotation logic in flush worker
- Nim has similar message types but simpler implementation

---

## Conclusion

The Fractio Nim implementation now covers most core functionality of Fjall Rust:

1. ~~**Atomicity**: Batch writes are not atomic~~ - ✅ FIXED (journal-based commit)
2. ~~**Flow control**: Missing write stall/throttle~~ - ✅ FIXED (3-level backpressure)
3. **Transactions**: Not implemented
4. **Large value support**: No KV separation
5. **Fine-grained configuration**: Per-level settings not supported

For basic key-value workloads with small values, Fractio is now feature-complete with proper atomicity and flow control. For production use with large datasets or high write throughput, transactions and KV separation may still be needed.
