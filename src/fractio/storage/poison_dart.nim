# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import std/atomics

type
  PoisonSignal* = ref Atomic[bool]
  PoisonDart* = ref object
    signal*: PoisonSignal

proc newPoisonDart*(signal: PoisonSignal): PoisonDart =
  PoisonDart(signal: signal)

proc poison*(dart: PoisonDart) =
  dart.signal.store(true, moRelease)

# In Nim, we don't have Drop trait, but we can use destructors
proc `=destroy`*(dart: var PoisonDart) =
  # In Nim, we don't have direct access to thread panic status
  # This is a simplified implementation
  # In practice, you'd need to check for errors in background workers
  discard


