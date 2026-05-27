# Read-Write Lock for Fractio
#
# Wraps pthread_rwlock_t to provide shared (read) / exclusive (write) locking.
# Multiple readers can hold the lock concurrently; writers get exclusive access.
#
# On non-POSIX platforms, falls back to a regular Lock (exclusive-only).

import std/locks

when defined(posix):
  import posix

type
  RWLock* = object
    ## Read-write lock: allows concurrent readers or a single writer.
    when defined(posix):
      rwlock: Pthread_rwlock
      initialized: bool
    else:
      fallback: Lock

proc initRWLock*(rw: var RWLock) =
  ## Initialize the read-write lock.
  when defined(posix):
    discard posix.pthread_rwlock_init(addr rw.rwlock, nil)
    rw.initialized = true
  else:
    initLock(rw.fallback)

proc deinitRWLock*(rw: var RWLock) =
  ## Destroy the read-write lock.
  when defined(posix):
    if rw.initialized:
      discard posix.pthread_rwlock_destroy(addr rw.rwlock)
      rw.initialized = false
  else:
    deinitLock(rw.fallback)

proc readLock*(rw: var RWLock) =
  ## Acquire the lock for reading (shared access).
  ## Multiple readers can hold this concurrently.
  when defined(posix):
    discard posix.pthread_rwlock_rdlock(addr rw.rwlock)
  else:
    acquire(rw.fallback)

proc writeLock*(rw: var RWLock) =
  ## Acquire the lock for writing (exclusive access).
  ## Blocks until all readers and writers have released the lock.
  when defined(posix):
    discard posix.pthread_rwlock_wrlock(addr rw.rwlock)
  else:
    acquire(rw.fallback)

proc readUnlock*(rw: var RWLock) =
  ## Release a read lock.
  when defined(posix):
    discard posix.pthread_rwlock_unlock(addr rw.rwlock)
  else:
    release(rw.fallback)

proc writeUnlock*(rw: var RWLock) =
  ## Release a write lock.
  when defined(posix):
    discard posix.pthread_rwlock_unlock(addr rw.rwlock)
  else:
    release(rw.fallback)

template withReadLock*(rw: var RWLock, body: untyped) =
  ## Execute body while holding a read lock (shared access).
  readLock(rw)
  try:
    body
  finally:
    readUnlock(rw)

template withWriteLock*(rw: var RWLock, body: untyped) =
  ## Execute body while holding a write lock (exclusive access).
  writeLock(rw)
  try:
    body
  finally:
    writeUnlock(rw)
