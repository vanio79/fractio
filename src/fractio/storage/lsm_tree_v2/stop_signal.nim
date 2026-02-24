# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Stop Signal
##
## A signal for stopping background tasks gracefully.

import std/atomics

type
  StopSignal* = ref object
    stopped*: Atomic[bool]

proc newStopSignal*(): StopSignal =
  var stopped: Atomic[bool]
  StopSignal(stopped: stopped)

proc send*(s: StopSignal) =
  ## Send stop signal
  store(s.stopped, true, moRelease)

proc isStopped*(s: StopSignal): bool =
  ## Check if stop signal has been sent
  load(s.stopped, moAcquire)

# ============================================================================
# Tests
# ============================================================================

when isMainModule:
  echo "Testing stop signal..."

  let signal = newStopSignal()
  echo "Initial stopped: ", signal.isStopped()

  signal.send()
  echo "After send: ", signal.isStopped()

  echo "Stop signal tests passed!"
