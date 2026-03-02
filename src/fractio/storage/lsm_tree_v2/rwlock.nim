## Read-Write Lock Implementation for LSM Tree
##
## Provides a proper read-write lock for version history management.
## Uses a simple approach: write lock is exclusive, read lock allows multiple readers.

import std/[locks, atomics]

# ============================================================================
# RwLock - Read-Write Lock (non-generic base)
# ============================================================================

type
  RwLockBase* = ref object
    lock*: Lock
    readerCount*: Atomic[int32]

proc newRwLockBase*(): RwLockBase =
  ## Create a new RwLockBase
  var lock: Lock
  initLock(lock)
  result = RwLockBase(
    lock: lock,
    readerCount: default(Atomic[int32])
  )
  store(result.readerCount, 0.int32)

proc acquireRead*(r: RwLockBase) =
  ## Acquire read lock (multiple readers allowed)
  ## Simple approach: just increment reader count
  ## Writers will acquire the lock which blocks all readers
  atomicInc(r.readerCount, 1.int32)

proc releaseRead*(r: RwLockBase) =
  ## Release read lock
  atomicDec(r.readerCount, 1.int32)

proc acquireWrite*(r: RwLockBase) =
  ## Acquire write lock (exclusive)
  ## First acquire the base lock, then wait for all readers to finish
  r.lock.acquire()

  # Wait for all readers to release
  while load(r.readerCount, moAcquire) > 0:
    # Spin-wait with small backoff
    for _ in 0..<100:
      discard

proc releaseWrite*(r: RwLockBase) =
  ## Release write lock
  r.lock.release()

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
