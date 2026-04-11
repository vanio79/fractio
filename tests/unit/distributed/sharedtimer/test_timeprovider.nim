# Unit tests for fractio/distributed/sharedtimer/timeprovider.nim
# Tests abstract TimeProvider base class

import std/unittest
import fractio/distributed/sharedtimer/timeprovider
import fractio/distributed/sharedtimer/types
import fractio/core/types

type TestProvider1 = ref object of TimeProvider
  customTime: Timestamp

method now(self: TestProvider1): Timestamp =
  self.customTime

type TestProvider2 = ref object of TimeProvider

method now(self: TestProvider2): Timestamp = 0'i64

type TestProvider3 = ref object of TimeProvider
  negTime: Timestamp

method now(self: TestProvider3): Timestamp = self.negTime

type MockProvider = ref object of TimeProvider
  currentTime: Timestamp

method now(self: MockProvider): Timestamp = self.currentTime

proc setTime(self: MockProvider, t: Timestamp) =
  self.currentTime = t

type CallCountProvider = ref object of TimeProvider
  callCount: int

method now(self: CallCountProvider): Timestamp =
  inc self.callCount
  self.callCount.int64

type IndependentProvider = ref object of TimeProvider
  time: Timestamp

method now(self: IndependentProvider): Timestamp = self.time

type RefProvider = ref object of TimeProvider
  time: Timestamp

method now(self: RefProvider): Timestamp = self.time

suite "TimeProvider - Construction":

  test "create base time provider":
    var provider: TimeProvider
    new(provider)
    check provider != nil

suite "TimeProvider - Default now()":

  test "default now returns zero":
    var provider: TimeProvider
    new(provider)
    let ts = provider.now()
    check ts == 0

  test "default now returns Timestamp":
    var provider: TimeProvider
    new(provider)
    let ts: Timestamp = provider.now()
    check ts is Timestamp

  test "default now is deterministic":
    var provider: TimeProvider
    new(provider)
    let ts1 = provider.now()
    let ts2 = provider.now()
    check ts1 == ts2
    check ts1 == 0

suite "TimeProvider - Method Signature":

  test "now() is gcsafe":
    var provider: TimeProvider
    new(provider)
    {.cast(gcsafe).}:
      let ts = provider.now()
    check ts == 0

suite "TimeProvider - Inheritance":

  test "derived type can override now()":
    var provider: TestProvider1
    new(provider)
    provider.customTime = 1000_000_000
    check provider.now() == 1000_000_000

  test "derived type is TimeProvider":
    var provider: TestProvider2
    new(provider)
    check provider of TimeProvider

  test "base reference can hold derived":
    var derived: TestProvider1
    new(derived)
    derived.customTime = 42
    var base: TimeProvider = derived
    check base of TestProvider1

  test "polymorphic dispatch works":
    var derived: CallCountProvider
    new(derived)
    derived.callCount = 0
    var base: TimeProvider = derived
    let result = base.now()
    check result == 1
    check derived.callCount == 1

suite "TimeProvider - Timestamp Type":

  test "now() returns int64":
    var provider: TimeProvider
    new(provider)
    let ts = provider.now()
    check ts is int64

  test "Timestamp can be negative":
    let negative: Timestamp = -1
    check negative < 0

  test "Timestamp can be large":
    let large: Timestamp = 0x7FFFFFFFFFFFFFFF
    check large > 0

suite "TimeProvider - Multiple Providers":

  test "each instance independent":
    var p1: IndependentProvider
    new(p1)
    p1.time = 100

    var p2: IndependentProvider
    new(p2)
    p2.time = 200

    check p1.now() == 100
    check p2.now() == 200

suite "TimeProvider - Edge Cases":

  test "zero timestamp":
    var provider: TimeProvider
    new(provider)
    check provider.now() == 0

  test "derived with zero override":
    var provider: TestProvider2
    new(provider)
    check provider.now() == 0

  test "derived with negative time":
    var provider: TestProvider3
    new(provider)
    provider.negTime = -1000
    check provider.now() == -1000

suite "TimeProvider - Type Conversions":

  test "Timestamp arithmetic":
    let ts: Timestamp = 1000
    let doubled = ts * 2
    check doubled == 2000

  test "Timestamp comparison":
    let ts1: Timestamp = 100
    let ts2: Timestamp = 200
    check ts1 < ts2
    check ts2 > ts1

suite "TimeProvider - Interface Contract":

  test "now() always returns valid Timestamp":
    var provider: TimeProvider
    new(provider)
    let ts = provider.now()
    check ts is int64

suite "TimeProvider - Mock Integration":

  test "mock provider pattern":
    var mock: MockProvider
    new(mock)
    mock.setTime(1000_000_000)
    check mock.now() == 1000_000_000

    mock.setTime(2000_000_000)
    check mock.now() == 2000_000_000

suite "TimeProvider - Object Identity":

  test "providers are reference types":
    var p1: RefProvider
    new(p1)
    p1.time = 100

    var p2: TimeProvider = p1
    p1.time = 200

    check p2.now() == 200

suite "TimeProvider - Constants":

  test "nanoseconds per second":
    let nsPerSec: Timestamp = 1_000_000_000
    check nsPerSec == 1_000_000_000

  test "nanoseconds per millisecond":
    let nsPerMs: Timestamp = 1_000_000
    check nsPerMs == 1_000_000

  test "nanoseconds per microsecond":
    let nsPerUs: Timestamp = 1_000
    check nsPerUs == 1_000
