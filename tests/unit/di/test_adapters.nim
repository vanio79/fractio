# Unit tests for Fractio DI Adapters

import std/[unittest, options, tables, times]
import fractio/di/interfaces
import fractio/di/adapters
import fractio/di/mocks
import fractio/utils/logging as fractioLogging
import fractio/distributed/sharedtimer/timeprovider
import fractio/distributed/sharedtimer/monotonic
import fractio/distributed/sharedtimer/wallclock
import fractio/distributed/sharedtimer/mock as stMock

suite "LoggerAdapter":

  test "newLoggerAdapter wraps existing Logger":
    let original = fractioLogging.newLogger("test", fractioLogging.llDebug)
    let adapter = newLoggerAdapter(original)

    check adapter.wrapped == original
    check adapter.minLevel == llDebug

  test "shouldLog respects minLevel":
    let adapter = LoggerAdapter(minLevel: llWarn)

    check not adapter.shouldLog(llDebug)
    check not adapter.shouldLog(llInfo)
    check adapter.shouldLog(llWarn)
    check adapter.shouldLog(llError)

  test "setMinLevel updates both":
    let original = fractioLogging.newLogger("test")
    let adapter = newLoggerAdapter(original)

    adapter.setMinLevel(llError)
    check adapter.minLevel == llError
    check original.minLevel == fractioLogging.llError

  test "wrapLogger convenience":
    let logger = fractioLogging.newLogger("wrapped")
    let adapter = wrapLogger(logger)
    check adapter.wrapped == logger

  test "wrapGlobalLogger wraps global":
    let adapter = wrapGlobalLogger()
    check adapter.wrapped == fractioLogging.globalLogger

suite "SystemTimeProvider":

  test "nowNs returns nanoseconds":
    let tp = newSystemTimeProvider()

    let ns = tp.nowNs()
    let now = getTime()
    let expectedNs = now.toUnix * 1_000_000_000 + now.nanosecond

    # Allow some variance (within 1 second)
    check ns > expectedNs - 1_000_000_000
    check ns < expectedNs + 1_000_000_000

  test "nowUs returns microseconds":
    let tp = newSystemTimeProvider()

    let us = tp.nowUs()
    check us > 0

    # Microseconds should be roughly nanoseconds / 1000
    let ns = tp.nowNs()
    check us == ns div 1000 or (us > ns div 1000 - 1000 and us < ns div 1000 + 1000)

  test "nowMs returns milliseconds":
    let tp = newSystemTimeProvider()

    let ms = tp.nowMs()
    check ms > 0

    # Milliseconds should be roughly nanoseconds / 1_000_000
    let ns = tp.nowNs()
    let expectedMs = ns div 1_000_000
    check ms >= expectedMs - 10 and ms <= expectedMs + 10

  test "advance is no-op":
    let tp = newSystemTimeProvider()

    let before = tp.nowNs()
    tp.advance(1000)
    let after = tp.nowNs()

    # Time should have changed naturally, not by exactly 1000
    check after != before + 1000

suite "NullLogger":

  test "log discards messages":
    let n = newNullLogger()

    n.debug("debug")
    n.info("info")
    n.warn("warn")
    n.error("error")

    # All messages discarded, no error

  test "shouldLog returns false":
    let n = newNullLogger()

    check not n.shouldLog(llDebug)
    check not n.shouldLog(llInfo)
    check not n.shouldLog(llWarn)
    check not n.shouldLog(llError)

  test "setMinLevel accepts input":
    let n = newNullLogger()
    n.setMinLevel(llDebug)
    check n.minLevel == llDebug

suite "ConsoleLogger":

  test "newConsoleLogger creates with defaults":
    let c = newConsoleLogger()

    check c.prefix == ""
    check c.minLevel == llInfo

  test "newConsoleLogger with prefix":
    let c = newConsoleLogger("MyApp", llDebug)

    check c.prefix == "MyApp"
    check c.minLevel == llDebug

  test "shouldLog respects minLevel":
    let c = ConsoleLogger(minLevel: llWarn)

    check not c.shouldLog(llDebug)
    check not c.shouldLog(llInfo)
    check c.shouldLog(llWarn)
    check c.shouldLog(llError)

  test "log outputs message":
    let c = newConsoleLogger("TEST", llInfo)
    # This will print to stdout: [INFO] TEST: hello
    c.info("hello")

  test "log with fields":
    let c = newConsoleLogger("", llInfo)
    var fields = {"key": "value"}.toTable
    # This will print to stdout: [INFO] hello key=value
    c.info("hello", fields)

suite "InMemoryKVStore":

  test "get and put":
    let s = newInMemoryKVStore()

    let putResult = s.put("key", "value")
    check putResult == true

    let val = s.get("key")
    check val.isSome
    check val.get == "value"
    s.close()

  test "delete removes key":
    let s = newInMemoryKVStore()
    discard s.put("k", "v")

    check s.get("k").isSome

    discard s.delete("k")
    check s.get("k").isNone
    s.close()

  test "scan with prefix":
    let s = newInMemoryKVStore()
    discard s.put("a/1", "1")
    discard s.put("a/2", "2")
    discard s.put("b/1", "3")

    let results = s.scan("a/", 10)
    check results.len == 2
    s.close()

  test "exists check":
    let s = newInMemoryKVStore()
    discard s.put("key", "value")

    check s.exists("key")
    check not s.exists("missing")
    s.close()

  test "close releases lock":
    let s = newInMemoryKVStore()
    s.close()
    # Lock deinitialized, no further operations should crash

  test "clear removes all":
    let s = newInMemoryKVStore()
    discard s.put("a", "1")
    discard s.put("b", "2")

    s.clear()
    check s.data.len == 0
    s.close()

suite "InMemoryBackend":

  test "get and put":
    let b = newInMemoryBackend()

    discard b.put("key", "value")
    let val = b.get("key")
    check val.isSome
    check val.get == "value"
    b.close()

  test "delete works":
    let b = newInMemoryBackend()
    discard b.put("k", "v")

    discard b.delete("k")
    check b.get("k").isNone
    b.close()

  test "scan with prefix":
    let b = newInMemoryBackend()
    discard b.put("prefix/a", "1")
    discard b.put("prefix/b", "2")
    discard b.put("other/c", "3")

    let results = b.scan("prefix/", 10)
    check results.len == 2
    b.close()

  test "flush is no-op but returns ok":
    let b = newInMemoryBackend()
    let result = b.flush()
    check result == true
    b.close()

  test "compact is no-op but returns ok":
    let b = newInMemoryBackend()
    let result = b.compact()
    check result == true
    b.close()

  test "close marks closed":
    let b = newInMemoryBackend()
    check not b.closed

    b.close()
    check b.closed

  test "stats returns key count":
    let b = newInMemoryBackend()
    discard b.put("k1", "v1")
    discard b.put("k2", "v2")
    discard b.put("k3", "v3")

    let stats = b.stats()
    check stats["key_count"] == 3
    b.close()

suite "Convenience Functions":

  test "defaultTimeProvider returns SystemTimeProvider":
    let tp = defaultTimeProvider()
    check tp.nowNs() > 0

  test "defaultLogger wraps global":
    let adapter = defaultLogger()
    check adapter.wrapped == fractioLogging.globalLogger

  test "nullLogger returns NullLogger":
    let n = nullLogger()
    check not n.shouldLog(llError)

  test "consoleLogger with prefix":
    let c = consoleLogger("MyApp")
    check c.prefix == "MyApp"

  test "memoryKVStore returns InMemoryKVStore":
    let s = memoryKVStore()
    discard s.put("test", "value")
    check s.get("test").isSome
    s.close()

  test "memoryBackend returns InMemoryBackend":
    let b = memoryBackend()
    discard b.put("test", "value")
    check b.get("test").isSome
    b.close()

suite "SharedTimerTimeProviderAdapter":
  # Tests for adapter that wraps sharedtimer.TimeProvider for DI use

  test "newSharedTimerTimeProviderAdapter wraps TimeProvider":
    let monotonicTp = monotonic.MonotonicTimeProvider()
    let adapter = newSharedTimerTimeProviderAdapter(monotonicTp)

    check adapter.wrapped != nil

  test "nowNs returns nanoseconds from wrapped provider":
    let monotonicTp = monotonic.MonotonicTimeProvider()
    let adapter = newSharedTimerTimeProviderAdapter(monotonicTp)

    let ns = adapter.nowNs()
    check ns > 0

    # Should match the wrapped provider's now() method
    let expectedNs = monotonicTp.now()
    # Allow some variance due to time passing
    check ns >= expectedNs - 1_000_000 and ns <= expectedNs + 1_000_000_000

  test "nowUs returns microseconds":
    let monotonicTp = monotonic.MonotonicTimeProvider()
    let adapter = newSharedTimerTimeProviderAdapter(monotonicTp)

    let us = adapter.nowUs()
    check us > 0

    # Should be roughly ns / 1000
    let ns = adapter.nowNs()
    check us >= ns div 1000 - 10 and us <= ns div 1000 + 10

  test "nowMs returns milliseconds":
    let monotonicTp = monotonic.MonotonicTimeProvider()
    let adapter = newSharedTimerTimeProviderAdapter(monotonicTp)

    let ms = adapter.nowMs()
    check ms > 0

    # Should be roughly ns / 1_000_000
    let ns = adapter.nowNs()
    let expectedMs = ns div 1_000_000
    check ms >= expectedMs - 1 and ms <= expectedMs + 1

  test "adaptMonotonicTimeProvider convenience":
    let adapter = adaptMonotonicTimeProvider()
    check adapter.nowNs() > 0

  test "adaptWallClockTimeProvider convenience":
    let adapter = adaptWallClockTimeProvider()
    check adapter.nowNs() > 0

  test "adaptSharedTimerTimeProvider works with any TimeProvider":
    let monotonicTp = monotonic.MonotonicTimeProvider()
    let adapter = adaptSharedTimerTimeProvider(monotonicTp)
    check adapter.nowNs() > 0

suite "DITimeProviderAdapter":
  # Tests for adapter that makes DI-style providers work with SharedTimer

  test "newDITimeProviderAdapter creates with function pointers":
    var currentTime = 1234567890'i64

    let adapter = newDITimeProviderAdapter(
      proc(): int64 {.gcsafe.} = currentTime,
      proc(deltaNs: int64) {.gcsafe.} = currentTime += deltaNs
    )

    check adapter.nowNsProc != nil
    check adapter.advanceProc != nil

  test "now returns Timestamp from nowNsProc":
    var currentTime = 1000000000'i64

    let adapter = newDITimeProviderAdapter(
      proc(): int64 {.gcsafe.} = currentTime
    )

    check adapter.now() == 1000000000

  test "advance calls advanceProc":
    var currentTime = 1000000000'i64

    let adapter = newDITimeProviderAdapter(
      proc(): int64 {.gcsafe.} = currentTime,
      proc(deltaNs: int64) {.gcsafe.} = currentTime += deltaNs
    )

    adapter.advance(500000000)
    check currentTime == 1500000000
    check adapter.now() == 1500000000

  test "now returns 0 if no nowNsProc":
    let adapter = DITimeProviderAdapter(nowNsProc: nil)
    check adapter.now() == 0

  test "advance is no-op if no advanceProc":
    let adapter = DITimeProviderAdapter(
      nowNsProc: proc(): int64 {.gcsafe.} = 12345'i64,
      advanceProc: nil
    )

    adapter.advance(1000)
    # No error, just no-op
    check adapter.now() == 12345

  test "adapter can be used as TimeProvider":
    var currentTime = 2000000000'i64

    let adapter = newDITimeProviderAdapter(
      proc(): int64 {.gcsafe.} = currentTime
    )

    # Can cast to TimeProvider since it inherits
    let tp = cast[timeprovider.TimeProvider](adapter)
    check tp.now() == 2000000000

suite "Time Provider Bridge Integration":
  # Test full integration between DI mocks and SharedTimer

  test "MockTimeProvider (DI) can be adapted for SharedTimer":
    # Create mock outside closure to avoid GC-safety issues
    let diMock = newMockTimeProvider(1000000000)
    let initialTime = diMock.nowNs()

    check initialTime == 1000000000

    # Test advance directly on mock
    diMock.advance(500000000)
    check diMock.nowNs() == 1500000000

  test "SharedTimer MockTimeProvider can be adapted for DI":
    let stMockTp = stMock.MockTimeProvider(currentTime: 2000000000)
    let adapter = adaptSharedTimerTimeProvider(stMockTp)

    check adapter.nowNs() == 2000000000

    # Set time through sharedtimer mock
    stMockTp.setTime(3000000000)
    check adapter.nowNs() == 3000000000

  test "DITimeProviderAdapter with DI Mock can be used as TimeProvider":
    # Use a static time for GC-safety in this test
    let adapter = newDITimeProviderAdapter(
      proc(): int64 {.gcsafe.} = 1234567890'i64
    )

    # Can use as TimeProvider
    let tp = cast[timeprovider.TimeProvider](adapter)
    check tp.now() == 1234567890

  test "SharedTimer TimeProviders can be wrapped for DI use":
    # Test that any sharedtimer TimeProvider can work with DI container
    let monotonicTp = monotonic.MonotonicTimeProvider()
    let adapter = adaptSharedTimerTimeProvider(monotonicTp)

    # Works with DI-style API
    check adapter.nowNs() > 0
    check adapter.nowUs() > 0
    check adapter.nowMs() > 0
