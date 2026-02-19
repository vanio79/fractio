# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import std/atomics

type
  PoisonSignal* = ref object
    value*: Atomic[bool]

  PoisonDart* = ref object
    signal*: PoisonSignal

proc newPoisonSignal*(): PoisonSignal =
  result = PoisonSignal()
  result.value.store(false, moRelaxed)

proc newPoisonDart*(signal: PoisonSignal): PoisonDart =
  PoisonDart(signal: signal)

proc poison*(dart: PoisonDart) =
  dart.signal.value.store(true, moRelease)


