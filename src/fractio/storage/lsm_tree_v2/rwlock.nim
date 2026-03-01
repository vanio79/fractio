## Read-Write Lock Implementation for LSM Tree
##
## Provides a simple read-write lock for version history management.
## Based on Nim's Lock implementation with reader-writer semantics.

import std/[locks, atomics]

# ============================================================================
# RwLock - Read-Write Lock (non-generic base)
# ============================================================================

type
  RwLockBase* = ref object
    lock*: Lock
    readerCount*: Atomic[int32]
    writerCount*: Atomic[int32]

proc newRwLockBase*(): RwLockBase =
  ## Create a new RwLockBase
  var lock: Lock
  initLock(lock)
  result = RwLockBase(
    lock: lock,
    readerCount: default(Atomic[int32]),
    writerCount: default(Atomic[int32])
  )
  store(result.readerCount, 0.int32)
  store(result.writerCount, 0.int32)

proc acquireRead*(r: RwLockBase) =
  ## Acquire read lock (multiple readers allowed)
  atomicInc(r.readerCount, 1.int32)

proc releaseRead*(r: RwLockBase) =
  ## Release read lock
  atomicDec(r.readerCount, 1.int32)

proc acquireWrite*(r: RwLockBase) =
  ## Acquire write lock (exclusive)
  atomicInc(r.writerCount, 1.int32)
  r.lock.acquire()

proc releaseWrite*(r: RwLockBase) =
  ## Release write lock
  r.lock.release()
  atomicDec(r.writerCount, 1.int32)

# ============================================================================
# RwLock - Generic wrapper with value storage
# ============================================================================

type
  RwLock*[T] = object
    base*: RwLockBase
    value*: T

proc newRwLock*[T](value: T): RwLock[T] =
  ## Create a new RwLock with initial value
  result = RwLock[T](
    base: newRwLockBase(),
    value: value
  )

proc acquireRead*[T](r: RwLock[T]) =
  ## Acquire read lock (multiple readers allowed)
  r.base.acquireRead()

proc releaseRead*[T](r: RwLock[T]) =
  ## Release read lock
  r.base.releaseRead()

proc acquireWrite*[T](r: RwLock[T]) =
  ## Acquire write lock (exclusive)
  r.base.acquireWrite()

proc releaseWrite*[T](r: RwLock[T]) =
  ## Release write lock
  r.base.releaseWrite()

# ============================================================================
# Tests
# ============================================================================

when isMainModule:
  echo "Testing RwLock..."

  var rwLock = newRwLock(0)

  # Test read lock
  rwLock.acquireRead()
  assert rwLock.value == 0
  rwLock.releaseRead()

  # Test write lock
  rwLock.acquireWrite()
  rwLock.value = 1
  rwLock.releaseWrite()

  # Test read after write
  rwLock.acquireRead()
  assert rwLock.value == 1
  rwLock.releaseRead()

  echo "RwLock tests passed!"
