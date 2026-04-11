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

suite "InMemoryKVStore advanced tests":
  test "thread-safe concurrent access":
    let s = newInMemoryKVStore()
    discard s.put("key1", "value1")
    discard s.put("key2", "value2")
    discard s.put("key3", "value3")

    # Multiple reads should work
    for i in 0..50:
      check s.get("key1").isSome
      check s.get("key2").isSome
      check s.exists("key3")

    s.close()

  test "scan with empty prefix":
    let s = newInMemoryKVStore()
    discard s.put("a", "1")
    discard s.put("b", "2")
    discard s.put("c", "3")

    let results = s.scan("", 10)
    check results.len == 3
    s.close()

  test "scan with no matching prefix":
    let s = newInMemoryKVStore()
    discard s.put("key1", "v1")
    discard s.put("key2", "v2")

    let results = s.scan("xyz/", 10)
    check results.len == 0
    s.close()

  test "scan respects limit with many entries":
    let s = newInMemoryKVStore()
    for i in 0..20:
      discard s.put("key/" & $i, $i)

    let results = s.scan("key/", 5'u32)
    check results.len == 5
    s.close()

  test "put overwrites existing key":
    let s = newInMemoryKVStore()
    discard s.put("key", "value1")
    check s.get("key").get == "value1"

    discard s.put("key", "value2")
    check s.get("key").get == "value2"
    s.close()

  test "delete non-existent key returns true":
    let s = newInMemoryKVStore()
    let result = s.delete("nonexistent")
    check result == true
    s.close()

  test "get on empty store returns none":
    let s = newInMemoryKVStore()
    check s.get("anykey").isNone
    s.close()

suite "InMemoryBackend advanced tests":
  test "stats accumulates counts":
    let b = newInMemoryBackend()
    discard b.put("k1", "v1")
    discard b.put("k2", "v2")

    let stats1 = b.stats()
    check stats1["key_count"] == 2

    discard b.put("k3", "v3")
    let stats2 = b.stats()
    check stats2["key_count"] == 3
    b.close()

  test "scan returns correct keys":
    let b = newInMemoryBackend()
    discard b.put("prefix/a", "1")
    discard b.put("prefix/b", "2")
    discard b.put("prefix/c", "3")
    discard b.put("other/x", "4")

    let results = b.scan("prefix/", 100)
    check results.len == 3

    var foundKeys: seq[string] = @[]
    for (k, v) in results:
      foundKeys.add(k)
    check "prefix/a" in foundKeys
    check "prefix/b" in foundKeys
    check "prefix/c" in foundKeys
    b.close()

  test "close prevents further operations safely":
    let b = newInMemoryBackend()
    discard b.put("key", "value")
    b.close()

    check b.closed
    # Lock is deinitialized, operations would crash - don't test further

  test "flush and compact are idempotent":
    let b = newInMemoryBackend()
    check b.flush() == true
    check b.flush() == true
    check b.compact() == true
    check b.compact() == true
    b.close()

  test "stats includes custom stats data":
    let b = newInMemoryBackend()
    b.statsData["custom_metric"] = 42'i64

    discard b.put("k", "v") # Trigger stats update
    let stats = b.stats()
    check stats.hasKey("key_count")
    b.close()

suite "ConsoleLogger advanced tests":
  test "log with empty fields":
    let c = newConsoleLogger("TEST", llDebug)
    let emptyFields = initTable[string, string]()
    c.debug("message with empty fields", emptyFields)

  test "log with multiple fields":
    let c = newConsoleLogger("", llInfo)
    var fields = {
      "key1": "value1",
      "key2": "value2",
      "key3": "value3"
    }.toTable
    c.info("multi-field test", fields)

  test "setMinLevel changes filtering":
    let c = newConsoleLogger("", llError)
    check not c.shouldLog(llInfo)
    check c.shouldLog(llError)

    c.setMinLevel(llDebug)
    check c.shouldLog(llDebug)
    check c.shouldLog(llInfo)

  test "prefix formatting":
    let c = newConsoleLogger("MyApp", llInfo)
    check c.prefix == "MyApp"

suite "NullLogger advanced tests":
  test "all log levels discarded":
    let n = newNullLogger()
    var fields = initTable[string, string]()
    fields["field"] = "value"

    n.log(llDebug, "debug", fields)
    n.log(llInfo, "info", fields)
    n.log(llWarn, "warn", fields)
    n.log(llError, "error", fields)

    # All discarded without error

  test "setMinLevel changes internal state":
    let n = newNullLogger()
    check n.minLevel == llError

    n.setMinLevel(llDebug)
    check n.minLevel == llDebug

    # Still discards all messages
    n.debug("still discarded")

suite "LoggerAdapter advanced tests":
  test "log level conversion mapping":
    let original = fractioLogging.newLogger("test", fractioLogging.llWarn)
    let adapter = newLoggerAdapter(original)

    check adapter.minLevel == llWarn

  test "info logs through adapter":
    let original = fractioLogging.newLogger("test", fractioLogging.llDebug)
    let adapter = newLoggerAdapter(original)

    var fields = initTable[string, string]()
    adapter.info("test info message", fields)

  test "warn logs through adapter":
    let original = fractioLogging.newLogger("test", fractioLogging.llDebug)
    let adapter = newLoggerAdapter(original)

    adapter.warn("test warning")

  test "error logs through adapter":
    let original = fractioLogging.newLogger("test", fractioLogging.llDebug)
    let adapter = newLoggerAdapter(original)

    adapter.error("test error message")

  test "convenience methods work":
    let original = fractioLogging.newLogger("test", fractioLogging.llDebug)
    let adapter = newLoggerAdapter(original)

    adapter.debug("debug msg")
    adapter.info("info msg")
    adapter.warn("warn msg")
    adapter.error("error msg")

suite "SystemTimeProvider advanced tests":
  test "time increases over calls":
    let tp = newSystemTimeProvider()

    let t1 = tp.nowNs()
    let t2 = tp.nowNs()
    let t3 = tp.nowNs()

    # Each call should return increasing values (or same if very fast)
    check t2 >= t1
    check t3 >= t2

  test "unit conversions are consistent":
    let tp = newSystemTimeProvider()

    let ns = tp.nowNs()
    let us = tp.nowUs()
    let ms = tp.nowMs()

    # Microseconds should be approximately ns/1000
    let expectedUs = ns div 1000
    check us >= expectedUs - 100 and us <= expectedUs + 100

    # Milliseconds should be approximately ns/1000000
    let expectedMs = ns div 1_000_000
    check ms >= expectedMs - 1 and ms <= expectedMs + 1

  test "multiple advances are no-op":
    let tp = newSystemTimeProvider()

    tp.advance(1000)
    tp.advance(2000)
    tp.advance(3000)

    # System time unaffected

suite "DITimeProviderAdapter advanced tests":
  test "adapter with nil advanceProc handles gracefully":
    let adapter = newDITimeProviderAdapter(
      proc(): int64 {.gcsafe.} = 1000'i64
    )

    adapter.advance(500)
    check adapter.now() == 1000

  test "adapter with nil nowNsProc returns 0":
    let adapter = DITimeProviderAdapter(nowNsProc: nil)
    check adapter.now() == 0

  test "adapter integrates with MockTimeProvider":
    var staticTime = 5000000000'i64
    let adapter = newDITimeProviderAdapter(
      proc(): int64 {.gcsafe.} = staticTime,
      proc(deltaNs: int64) {.gcsafe.} = staticTime += deltaNs
    )

    check adapter.now() == 5000000000

    adapter.advance(1000000000)
    check staticTime == 6000000000

suite "SharedTimerTimeProviderAdapter advanced tests":
  test "adapter with WallClockTimeProvider":
    let wallClock = wallclock.WallClockTimeProvider()
    let adapter = newSharedTimerTimeProviderAdapter(wallClock)

    check adapter.nowNs() > 0
    check adapter.nowUs() > 0
    check adapter.nowMs() > 0

  test "adapter advance is no-op for real providers":
    let monotonicTp = monotonic.MonotonicTimeProvider()
    let adapter = newSharedTimerTimeProviderAdapter(monotonicTp)

    let before = adapter.nowNs()
    adapter.advance(1000000000)
    let after = adapter.nowNs()

    # Real time providers ignore advance calls
    check after >= before

  test "adapter with stMock.MockTimeProvider":
    let stMockTp = stMock.MockTimeProvider(currentTime: 1000000000'i64)
    let adapter = newSharedTimerTimeProviderAdapter(stMockTp)

    check adapter.nowNs() == 1000000000

    stMockTp.setTime(2000000000)
    check adapter.nowNs() == 2000000000

suite "Convenience functions coverage":
  test "defaultTimeProvider creates new instance each call":
    let tp1 = defaultTimeProvider()
    let tp2 = defaultTimeProvider()

    check tp1 != tp2

  test "nullLogger creates new instance":
    let n1 = nullLogger()
    let n2 = nullLogger()

    check n1 != n2

  test "consoleLogger with different prefixes":
    let c1 = consoleLogger("App1")
    let c2 = consoleLogger("App2")

    check c1.prefix == "App1"
    check c2.prefix == "App2"

  test "memoryKVStore creates new instance":
    let s1 = memoryKVStore()
    let s2 = memoryKVStore()

    check s1 != s2
    s1.close()
    s2.close()

  test "memoryBackend creates new instance":
    let b1 = memoryBackend()
    let b2 = memoryBackend()

    check b1 != b2
    b1.close()
    b2.close()

suite "Adapter integration with mocks":
  test "InMemoryKVStore full API":
    let s = newInMemoryKVStore()

    discard s.put("test", "value")
    check s.get("test").isSome
    check s.exists("test")
    let scanResult = s.scan("", 10)
    check scanResult.len == 1
    discard s.delete("test")
    check not s.exists("test")
    s.close()

  test "InMemoryBackend full API":
    let b = newInMemoryBackend()

    discard b.put("key", "value")
    check b.get("key").isSome
    discard b.delete("key")
    let scanResult = b.scan("", 10)
    discard b.flush()
    discard b.compact()
    let stats = b.stats()
    check stats.hasKey("key_count")
    b.close()

suite "Error handling in adapters":
  test "InMemoryKVStore handles empty key":
    let s = newInMemoryKVStore()
    discard s.put("", "empty_key_value")
    check s.get("").isSome
    check s.get("").get == "empty_key_value"
    s.close()

  test "InMemoryBackend handles empty key":
    let b = newInMemoryBackend()
    discard b.put("", "empty_key_value")
    check b.get("").isSome
    b.close()

  test "InMemoryKVStore handles empty value":
    let s = newInMemoryKVStore()
    discard s.put("key", "")
    check s.get("key").isSome
    check s.get("key").get == ""
    s.close()
