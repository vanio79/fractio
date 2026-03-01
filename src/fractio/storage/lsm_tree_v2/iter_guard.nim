# Copyright (c) 2026-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License

## Iterator Guard
##
## Prevents use-after-free by tracking live iterators and delaying SSTable deletion.
##
## Each iterator (TableRangeIter) registers itself with the global IteratorGuard
## when created. SSTables can only be deleted when no active iterators reference them.

import std/[atomics, locks]
import types
import table

# ============================================================================
# IteratorGuard - Global tracker for live iterators
# ============================================================================

# Each iterator registers its associated SsTable when created
# The guard holds a reference count per SsTable

# We use a global singleton to avoid passing guard around

# Track which SsTable is referenced by how many iterators
# Key: SsTable pointer, Value: count of active iterators

type
  IteratorGuard* = ref object
    tableRefCounts*: Table[ptr SsTable, Atomic[int]]
    lock*: Lock

proc newIteratorGuard*(): IteratorGuard =
  result = new(IteratorGuard)
  initLock(result.lock)
  result.tableRefCounts = initTable[ptr SsTable, Atomic[int]]()

# Global singleton
var globalIteratorGuard*: IteratorGuard

proc initGlobalIteratorGuard*() =
  globalIteratorGuard = newIteratorGuard()

# Register an iterator that references a table
proc registerIterator*(table: ptr SsTable) =
  if globalIteratorGuard == nil:
    initGlobalIteratorGuard()

  withLock globalIteratorGuard.lock:
    if not globalIteratorGuard.tableRefCounts.hasKey(table):
      globalIteratorGuard.tableRefCounts[table] = Atomic[int](0)
    inc(globalIteratorGuard.tableRefCounts[table].value, 1)

# Unregister an iterator that no longer references a table
proc unregisterIterator*(table: ptr SsTable) =
  if globalIteratorGuard == nil:
    return

  withLock globalIteratorGuard.lock:
    if globalIteratorGuard.tableRefCounts.hasKey(table):
      let count = dec(globalIteratorGuard.tableRefCounts[table].value, 1)
      if count == 0:
        globalIteratorGuard.tableRefCounts.del(table)

# Check if a table can be safely deleted (no active iterators)
proc canDeleteTable*(table: ptr SsTable): bool =
  if globalIteratorGuard == nil:
    return true

  withLock globalIteratorGuard.lock:
    return not globalIteratorGuard.tableRefCounts.hasKey(table)

# Get current reference count for debugging
proc refCount*(table: ptr SsTable): int =
  if globalIteratorGuard == nil:
    return 0

  withLock globalIteratorGuard.lock:
    if globalIteratorGuard.tableRefCounts.hasKey(table):
      return globalIteratorGuard.tableRefCounts[table].load(moRelaxed)
    else:
      return 0

# ============================================================================
# Extend TableRangeIter to register/unregister with guard
# ============================================================================

# We'll modify TableRangeIter in table.nim to call register/unregister
# This is the only place iterators are created

# We don't need to modify TableRangeIter here — we'll modify table.nim

# ============================================================================
# Tests
# ============================================================================

when isMainModule:
  echo "Testing IteratorGuard..."

  # Create a dummy table
  let table = newSsTable("/tmp/test.sst")
  let tablePtr = table.addr

  # Register two iterators
  registerIterator(tablePtr)
  registerIterator(tablePtr)

  echo "Ref count: ", refCount(tablePtr)
  assert refCount(tablePtr) == 2

  # Unregister one
  unregisterIterator(tablePtr)
  echo "Ref count after one unregister: ", refCount(tablePtr)
  assert refCount(tablePtr) == 1

  # Can we delete? No
  assert not canDeleteTable(tablePtr)

  # Unregister the last one
  unregisterIterator(tablePtr)
  echo "Ref count after last unregister: ", refCount(tablePtr)
  assert refCount(tablePtr) == 0

  # Now we can delete
  assert canDeleteTable(tablePtr)

  echo "IteratorGuard tests passed!"
