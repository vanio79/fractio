# Unit tests for fractio/utils/logging.nim
# Tests Logger, LogLevel, handlers, and message formatting

import std/[unittest, tables, strformat, strutils]
import fractio/utils/logging

# Use a thread-local counter for handler tracking
var handlerCallCount {.threadvar.}: int

suite "LogLevel":

  test "level ordering":
    check llDebug < llInfo
    check llInfo < llWarn
    check llWarn < llError
    check llDebug < llError

  test "level ordinals":
    check llDebug.ord == 0
    check llInfo.ord == 1
    check llWarn.ord == 2
    check llError.ord == 3

suite "newLogger":

  test "creates logger with default settings":
    let logger = newLogger()
    check logger.name == ""
    check logger.minLevel == llInfo
    check logger.handlers.len == 1 # Default stdout handler

  test "creates logger with name":
    let logger = newLogger("test-logger")
    check logger.name == "test-logger"

  test "creates logger with custom level":
    let logger = newLogger("", llDebug)
    check logger.minLevel == llDebug
    let logger2 = newLogger("", llError)
    check logger2.minLevel == llError

suite "Logger setMinLevel":

  test "setMinLevel changes level":
    let logger = newLogger("test", llInfo)
    check logger.minLevel == llInfo
    logger.setMinLevel(llDebug)
    check logger.minLevel == llDebug
    logger.setMinLevel(llError)
    check logger.minLevel == llError

suite "Logger shouldLog":

  test "shouldLog filters by level":
    let logger = newLogger("", llInfo)

    # Below threshold - should not log
    check not logger.shouldLog(llDebug)

    # At threshold - should log
    check logger.shouldLog(llInfo)

    # Above threshold - should log
    check logger.shouldLog(llWarn)
    check logger.shouldLog(llError)

  test "shouldLog with llDebug threshold":
    let logger = newLogger("", llDebug)
    check logger.shouldLog(llDebug)
    check logger.shouldLog(llInfo)
    check logger.shouldLog(llWarn)
    check logger.shouldLog(llError)

  test "shouldLog with llError threshold":
    let logger = newLogger("", llError)
    check not logger.shouldLog(llDebug)
    check not logger.shouldLog(llInfo)
    check not logger.shouldLog(llWarn)
    check logger.shouldLog(llError)

suite "Logger formatMessage":

  test "formatMessage without fields":
    let logger = newLogger("myapp")
    let msg = logger.formatMessage(llInfo, "hello world", initTable[string,
        string]())
    check msg.contains("myapp")
    check msg.contains("hello world")

  test "formatMessage with fields":
    let logger = newLogger("myapp")
    var fields = {"key1": "value1", "key2": "value2"}.toTable
    let msg = logger.formatMessage(llInfo, "test", fields)
    check msg.contains("key1=value1")
    check msg.contains("key2=value2")

  test "formatMessage with empty name":
    let logger = newLogger()
    let msg = logger.formatMessage(llInfo, "message", initTable[string, string]())
    check msg.contains("message")

suite "Logger addHandler":

  test "addHandler adds handler":
    let logger = newLogger()
    check logger.handlers.len == 1

    logger.addHandler(proc(level: LogLevel, msg: string, fields: Table[string,
        string]) {.gcsafe.} =
      discard
    )
    check logger.handlers.len == 2

  test "handlers can be cleared":
    let logger = newLogger()
    logger.handlers = @[]
    check logger.handlers.len == 0

suite "Logger log methods":

  test "debug logs at debug level - check shouldLog":
    let logger = newLogger("", llDebug)
    check logger.shouldLog(llDebug)

  test "info logs at info level - check shouldLog":
    let logger = newLogger("", llInfo)
    check logger.shouldLog(llInfo)

  test "warn logs at warn level - check shouldLog":
    let logger = newLogger("", llWarn)
    check logger.shouldLog(llWarn)

  test "error logs at error level - check shouldLog":
    let logger = newLogger("", llError)
    check logger.shouldLog(llError)

  test "filtered messages don't pass shouldLog":
    let logger = newLogger("", llInfo) # Debug filtered
    check not logger.shouldLog(llDebug)
    check logger.shouldLog(llInfo)

suite "Logger with fields":

  test "formatMessage includes fields":
    let logger = newLogger("test")
    var fields = {"key": "value"}.toTable
    let msg = logger.formatMessage(llInfo, "message", fields)
    check msg.contains("key=value")

  test "formatMessage with many fields":
    let logger = newLogger("test")
    var fields = {"a": "1", "b": "2", "c": "3"}.toTable
    let msg = logger.formatMessage(llInfo, "msg", fields)
    check msg.contains("a=1")
    check msg.contains("b=2")
    check msg.contains("c=3")

suite "Global Logger":

  test "globalLogger exists":
    check globalLogger != nil
    check globalLogger.name == "fractio"

  test "globalLogger has handlers":
    check globalLogger.handlers.len >= 1

suite "Thread Safety":

  test "handlers are gcsafe":
    # Compile-time check - handler signature must be gcsafe
    let logger = newLogger()
    logger.addHandler(proc(level: LogLevel, msg: string, fields: Table[string,
        string]) {.gcsafe.} =
      discard
    )
    check logger.handlers.len == 2

suite "Edge Cases":

  test "formatMessage with empty fields":
    let logger = newLogger("test")
    let msg = logger.formatMessage(llInfo, "msg", initTable[string, string]())
    check msg.contains("msg")

  test "formatMessage with long message":
    let longMsg = "x".repeat(1000)
    let logger = newLogger("test")
    let msg = logger.formatMessage(llInfo, longMsg, initTable[string, string]())
    check msg.contains(longMsg)

  test "formatMessage with special chars in fields":
    let logger = newLogger("test")
    var fields = {"path": "/var/log/fractio", "file": "app.log"}.toTable
    let msg = logger.formatMessage(llInfo, "log", fields)
    check msg.contains("path=/var/log/fractio")
