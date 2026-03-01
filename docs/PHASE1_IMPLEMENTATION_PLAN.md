# Phase 1 Implementation Plan: Critical Fixes

## Overview

Phase 1 focuses on implementing the critical architectural changes needed to bring the Nim LSM tree implementation in line with the Rust original. This is a **major refactoring** affecting core data structures.

## Critical Changes Required

### 1. Version History System (CRITICAL)

**Current State:**
- Single `SuperVersion` pointer in Tree
- No historical version tracking
- No snapshot isolation

**Target State (Rust-compatible):**
```nim
type
  VersionHistory = ref object
    versions: Deque[SuperVersion]  # Historical versions
    lock: RwLock  # Protects version list
    latestSeqno: Atomic[SeqNo]
  
  SuperVersion = ref object
    activeMemtable: Memtable
    sealedMemtables: seq[Memtable]
    tables: seq[SsTable]
    seqno: SeqNo  # Version's sequence number
```

**Implementation Steps:**

1. ✅ Create `version_history.nim` module (DONE - see file)
2. ⏳ Modify `lsm_tree.nim` to use `VersionHistory` instead of single `SuperVersion`
3. ⏳ Update all read operations to use `getVersionForSnapshot(seqno)`
4. ⏳ Implement version upgrade during flush/compaction

**Files to Modify:**
- `lsm_tree.nim` - Main tree structure
- `memtable.nim` - Already has seqno tracking
- `table.nim` - SSTable operations
- All iterators - Must use snapshot versions

### 2. Proper Locking (CRITICAL)

**Current State:**
- No locks around shared state
- Direct pointer access to SuperVersion
- Race conditions possible

**Target State (Rust-compatible):**
```nim
type
  Tree = ref object
    versionHistory: VersionHistory  # Has RwLock
    flushLock: Mutex  # Protects flush operations
```

**Implementation Steps:**

1. ✅ Add `RwLock` to VersionHistory (DONE in version_history.nim)
2. ⏳ Add `flushLock: Mutex` to Tree
3. ⏳ Wrap all version access with `acquireRead()/releaseRead()`
4. ⏳ Wrap flush operations with `flushLock.acquire()/release()`

**Files to Modify:**
- `lsm_tree.nim` - Add locks
- `flush/worker.nim` - Use flushLock
- All read paths - Use version history locks

### 3. Memory Model - Slice Instead of String (HIGH PRIORITY)

**Current State:**
```nim
InternalKey = ref object
  userKey: string  # Owned, allocated
  seqno: SeqNo
  valueType: ValueType
```

**Target State (Rust-compatible):**
```nim
InternalKey = object  # value type, not ref
  userKey: Slice  # Borrowed reference
  seqno: SeqNo
  valueType: ValueType
```

**Impact:**
- Every insert currently allocates a new string
- With Slice: zero-copy, borrows from input
- **Performance improvement: 5-10x**

**Implementation Steps:**

1. ⏳ Change `InternalKey` from `ref object` to `object`
2. ⏳ Change `userKey: string` to `userKey: Slice`
3. ⏳ Update all constructors to use borrowed slices
4. ⏳ Fix all string operations to work with Slice
5. ⏳ Update skip list to work with value types

**Files to Modify:**
- `types.nim` - Core type definitions
- `crossbeam_skiplist.nim` - May need adjustment for value types
- `memtable.nim` - All key handling
- `table.nim` - SSTable key handling
- Every file that creates or uses InternalKey

### 4. Atomic Operations Fix (MEDIUM)

**Current State:**
```nim
# CAS loop for highest_seqno (inefficient)
var currentSeqno = load(m.highestSeqno, moRelaxed)
while item.key.seqno > currentSeqno:
  if compareExchange(m.highestSeqno, currentSeqno, item.key.seqno, moRelaxed, moRelaxed):
    break
  currentSeqno = load(m.highestSeqno, moRelaxed)
```

**Target State (Rust-compatible):**
```nim
# Single atomic max operation
self.highest_seqno.fetch_max(item.key.seqno, Ordering::AcqRel)
```

**Implementation Steps:**

1. ⏳ Implement `atomicMax` proc for SeqNo
2. ⏳ Replace CAS loop with atomicMax
3. ⏳ Verify memory ordering is correct

**Files to Modify:**
- `memtable.nim` - Insert operation
- `atomics.nim` - May need to add atomicMax helper

## Implementation Order

### Week 1-2: Foundation
1. ✅ Create `version_history.nim` module
2. ⏳ Add `RwLock` and `Mutex` support
3. ⏳ Modify Tree structure to use VersionHistory
4. ⏳ Basic tests for version history

### Week 3-4: Memory Model
5. ⏳ Change `InternalKey` to use Slice
6. ⏳ Update all constructors and comparators
7. ⏳ Fix skip list for value types
8. ⏳ Performance tests

### Week 5-6: Integration
9. ⏳ Update all read paths to use version history
10. ⏳ Add flush lock protection
11. ⏳ Fix atomic operations
12. ⏳ Comprehensive testing

## Risk Assessment

### High Risk Changes:
1. **Slice conversion** - Every key operation needs updating
   - Mitigation: Extensive testing, start with non-critical paths
   
2. **Version history** - Changes how all reads work
   - Mitigation: Keep old API, add new versioned API alongside

### Medium Risk:
1. **Locking** - May introduce deadlocks
   - Mitigation: Use RwLock where possible, simple Mutex for flush

### Low Risk:
1. **Atomic operations** - Localized change
   - Mitigation: Well-understood pattern from Rust

## Testing Strategy

### Unit Tests:
- Version history creation and management
- Snapshot isolation correctness
- Lock-free read correctness
- Slice vs string behavior

### Integration Tests:
- Concurrent read/write with snapshot isolation
- Flush operations with version history
- Recovery with multiple versions

### Performance Tests:
- Compare before/after allocation counts
- Measure latency improvement from Slice
- Verify no regression from locking

## Estimated Timeline

| Task | Complexity | Time Estimate |
|------|-----------|---------------|
| Version History | High | 1-2 weeks |
| Locking | Medium | 1 week |
| Slice Conversion | Very High | 2-3 weeks |
| Atomic Fixes | Low | 2-3 days |
| Testing | High | 1-2 weeks |
| **Total** | | **5-9 weeks** |

## Next Steps

1. **Start with low-risk changes**:
   - Add atomicMax helper
   - Create version_history.nim tests
   
2. **Then medium-risk**:
   - Add locks to Tree
   - Integrate version history

3. **Finally high-risk**:
   - Convert to Slice (biggest change)
   - Update all call sites

## Conclusion

Phase 1 is a **significant refactoring** that touches nearly every file in the LSM tree implementation. The changes are necessary to match the Rust implementation's guarantees but require careful, systematic implementation.

**Recommendation**: Implement incrementally with extensive testing at each step. Start with version history (isolated change), then locking, then the more invasive Slice conversion.
