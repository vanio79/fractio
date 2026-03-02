## Read-Write Lock Implementation for LSM Tree
##
## Multiple readers can hold the lock simultaneously.
## Writer has exclusive access and priority over new readers.
##
## State encoding:
##   -1 or less: writer holds lock (nested writers use -2, -3, etc.)
##   0: unlocked
##   1 or more: number of active readers

import std/atomics

type
  SpinRwLock* = object
    state*: Atomic[int]

proc initSpinRwLock*(): SpinRwLock =
  result.state.store(0, moRelaxed)

template read*(rw: var SpinRwLock, body: untyped) =
  ## Acquire read lock, execute body, release read lock.
  var acquired = false

  while true:
    let current = rw.state.load(moAcquire)
    if current < 0:
      # Writer is active - spin
      cpuRelax()
      continue

    # Try to increment reader count
    var expected = current
    if rw.state.compareExchange(expected, current + 1, moAcquire, moRelaxed):
      acquired = true
      break

  if acquired:
    try:
      body
    finally:
      discard rw.state.fetchSub(1, moRelease)

template write*(rw: var SpinRwLock, body: untyped) =
  ## Acquire write lock, execute body, release write lock.
  var acquired = false

  while true:
    let current = rw.state.load(moAcquire)
    if current == 0:
      # No readers - try to acquire as writer
      var expected = 0
      if rw.state.compareExchange(expected, -1, moAcquire, moRelaxed):
        acquired = true
        break
    # Either readers exist or another writer is active
    cpuRelax()

  if acquired:
    try:
      body
    finally:
      rw.state.store(0, moRelease)
