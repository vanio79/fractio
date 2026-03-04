# Unit tests for poison_dart module
# Tests for PoisonDart functionality

import unittest
import fractio/storage/poison_dart
import std/atomics

suite "PoisonDart Unit Tests":

  test "PoisonSignal creation":
    let signal = newPoisonSignal()
    check signal.value.load(moRelaxed) == false

  test "PoisonDart creation":
    let signal = newPoisonSignal()
    let dart = newPoisonDart(signal)
    check dart.signal == signal

  test "PoisonDart poison":
    let signal = newPoisonSignal()
    let dart = newPoisonDart(signal)

    check signal.value.load(moRelaxed) == false
    dart.poison()
    check signal.value.load(moRelaxed) == true

  test "PoisonDart atomic operations":
    let signal = newPoisonSignal()
    let dart1 = newPoisonDart(signal)
    let dart2 = newPoisonDart(signal)

    check signal.value.load(moRelaxed) == false
    dart1.poison()
    check signal.value.load(moRelaxed) == true

    # Second poison should have no effect on value but still work
    dart2.poison()
    check signal.value.load(moRelaxed) == true
