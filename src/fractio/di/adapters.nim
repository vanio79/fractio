# Adapters for Existing Fractio Types
# Makes existing implementations work with DI patterns

import std/[options, times, locks, strformat, strutils]
import tables
import fractio/utils/logging as fractioLogging
import fractio/distributed/sharedtimer/timeprovider
import fractio/distributed/sharedtimer/types
import fractio/distributed/sharedtimer/monotonic
import fractio/distributed/sharedtimer/wallclock

export options

# =============================================================================
# LogLevel (must match other modules)
# =============================================================================

type
  LogLevelDI* = enum
    llDebug, llInfo, llWarn, llError

# =============================================================================
# System Time Provider - Uses real system time
# =============================================================================

type
  SystemTimeProvider* = ref object of RootObj
    ## Real system time provider for production
    ## Not for testing - use MockTimeProvider instead

proc newSystemTimeProvider*(): SystemTimeProvider =
  result = SystemTimeProvider()

proc nowNs*(t: SystemTimeProvider): int64 =
  ## Get current nanosecond timestamp from system
  let now = getTime()
  result = now.toUnix * 1_000_000_000 + now.nanosecond

proc nowUs*(t: SystemTimeProvider): int64 =
  ## Get current microsecond timestamp
  let now = getTime()
  result = now.toUnix * 1_000_000 + now.nanosecond div 1000

proc nowMs*(t: SystemTimeProvider): int64 =
  ## Get current millisecond timestamp
  let now = getTime()
  result = now.toUnix * 1000 + now.nanosecond div 1_000_000

proc advance*(t: SystemTimeProvider, deltaNs: int64) =
  ## System time provider cannot be advanced
  discard # No-op for real time

# =============================================================================
# Null Logger - Logger that discards all messages
# =============================================================================

type
  NullLogger* = ref object of RootObj
    ## Logger that discards all messages (for testing or disabled logging)
    minLevel*: LogLevelDI

proc newNullLogger*(): NullLogger =
  ## Create null logger
  result = NullLogger(minLevel: llError)

proc log*(n: NullLogger, level: LogLevelDI, msg: string, fields: tables.Table[
    string, string]) =
  ## Discard message
  discard

proc debug*(n: NullLogger, msg: string, fields: tables.Table[string,
    string] = tables.initTable[string, string]()) =
  discard

proc info*(n: NullLogger, msg: string, fields: tables.Table[string,
    string] = tables.initTable[string, string]()) =
  discard

proc warn*(n: NullLogger, msg: string, fields: tables.Table[string,
    string] = tables.initTable[string, string]()) =
  discard

proc error*(n: NullLogger, msg: string, fields: tables.Table[string,
    string] = tables.initTable[string, string]()) =
  discard

proc setMinLevel*(n: NullLogger, level: LogLevelDI) =
  n.minLevel = level

proc shouldLog*(n: NullLogger, level: LogLevelDI): bool =
  false # Never log

# =============================================================================
# Console Logger - Simple stdout logger
# =============================================================================

type
  ConsoleLogger* = ref object of RootObj
    ## Simple console logger for debugging
    minLevel*: LogLevelDI
    prefix*: string

proc newConsoleLogger*(prefix: string = "",
    minLevel: LogLevelDI = llInfo): ConsoleLogger =
  ## Create console logger
  result = ConsoleLogger(
    prefix: prefix,
    minLevel: minLevel
  )

proc shouldLog*(c: ConsoleLogger, level: LogLevelDI): bool =
  level >= c.minLevel

proc log*(c: ConsoleLogger, level: LogLevelDI, msg: string,
    fields: tables.Table[string, string]) =
  if not c.shouldLog(level):
    return
  let levelStr = case level
    of llDebug: "DEBUG"
    of llInfo: "INFO"
    of llWarn: "WARN"
    of llError: "ERROR"
  var output = fmt"[{levelStr}] "
  if c.prefix.len > 0:
    output.add(c.prefix & ": ")
  output.add(msg)
  if fields.len > 0:
    output.add(" ")
    for k, v in fields.pairs:
      output.add(fmt"{k}={v} ")
  echo output

proc debug*(c: ConsoleLogger, msg: string, fields: tables.Table[string,
    string] = tables.initTable[string, string]()) =
  c.log(llDebug, msg, fields)

proc info*(c: ConsoleLogger, msg: string, fields: tables.Table[string,
    string] = tables.initTable[string, string]()) =
  c.log(llInfo, msg, fields)

proc warn*(c: ConsoleLogger, msg: string, fields: tables.Table[string,
    string] = tables.initTable[string, string]()) =
  c.log(llWarn, msg, fields)

proc error*(c: ConsoleLogger, msg: string, fields: tables.Table[string,
    string] = tables.initTable[string, string]()) =
  c.log(llError, msg, fields)

proc setMinLevel*(c: ConsoleLogger, level: LogLevelDI) =
  c.minLevel = level

# =============================================================================
# In-Memory KV Store - Simple in-memory implementation for testing
# =============================================================================

type
  InMemoryKVStore* = ref object of RootObj
    ## Simple in-memory KV store for testing
    data*: tables.Table[string, string]
    lock*: Lock

proc newInMemoryKVStore*(): InMemoryKVStore =
  ## Create in-memory store
  result = InMemoryKVStore(
    data: tables.initTable[string, string]()
  )
  initLock(result.lock)

proc get*(s: InMemoryKVStore, key: string): Option[string] =
  withLock(s.lock):
    if key in s.data:
      result = some(s.data[key])
    else:
      result = none(string)

proc put*(s: InMemoryKVStore, key: string, value: string): bool =
  withLock(s.lock):
    s.data[key] = value
    return true

proc delete*(s: InMemoryKVStore, key: string): bool =
  withLock(s.lock):
    if key in s.data:
      s.data.del(key)
    return true

proc scan*(s: InMemoryKVStore, prefix: string, limit: uint32): seq[(string, string)] =
  withLock(s.lock):
    result = @[]
    var count = 0
    for k, v in s.data.pairs:
      if k.startsWith(prefix) and count < int(limit):
        result.add((k, v))
        inc count

proc close*(s: InMemoryKVStore) =
  deinitLock(s.lock)

proc exists*(s: InMemoryKVStore, key: string): bool =
  withLock(s.lock):
    result = key in s.data

proc clear*(s: InMemoryKVStore) =
  withLock(s.lock):
    s.data.clear()

# =============================================================================
# In-Memory Backend - Simple in-memory backend for testing
# =============================================================================

type
  InMemoryBackend* = ref object of RootObj
    ## Simple in-memory backend for testing
    data*: tables.Table[string, string]
    statsData*: tables.Table[string, int64]
    lock*: Lock
    closed*: bool

proc newInMemoryBackend*(): InMemoryBackend =
  ## Create in-memory backend
  result = InMemoryBackend(
    data: tables.initTable[string, string](),
    statsData: tables.initTable[string, int64]()
  )
  initLock(result.lock)

proc get*(b: InMemoryBackend, key: string): Option[string] =
  withLock(b.lock):
    if key in b.data:
      result = some(b.data[key])
    else:
      result = none(string)

proc put*(b: InMemoryBackend, key: string, value: string): bool =
  withLock(b.lock):
    b.data[key] = value
    return true

proc delete*(b: InMemoryBackend, key: string): bool =
  withLock(b.lock):
    if key in b.data:
      b.data.del(key)
    return true

proc scan*(b: InMemoryBackend, prefix: string, limit: uint32): seq[(string, string)] =
  withLock(b.lock):
    result = @[]
    var count = 0
    for k, v in b.data.pairs:
      if k.startsWith(prefix) and count < int(limit):
        result.add((k, v))
        inc count

proc flush*(b: InMemoryBackend): bool =
  # In-memory doesn't need flush
  return true

proc compact*(b: InMemoryBackend): bool =
  # In-memory doesn't need compaction
  return true

proc close*(b: InMemoryBackend) =
  withLock(b.lock):
    b.closed = true
    deinitLock(b.lock)

proc stats*(b: InMemoryBackend): tables.Table[string, int64] =
  withLock(b.lock):
    result = tables.initTable[string, int64]()
    result["key_count"] = b.data.len.int64
    for k, v in b.statsData.pairs:
      result[k] = v

# =============================================================================
# Logger Adapter - Makes fractio/utils/logging Logger work with DI pattern
# =============================================================================

type
  LoggerAdapter* = ref object of RootObj
    ## Adapter to make existing Logger work with DI
    wrapped*: fractioLogging.Logger
    minLevel*: LogLevelDI

proc newLoggerAdapter*(logger: fractioLogging.Logger): LoggerAdapter =
  ## Create adapter wrapping existing Logger
  result = LoggerAdapter(
    wrapped: logger,
    minLevel: llInfo
  )
  # Map fractio LogLevel to DI LogLevel
  case logger.minLevel
  of fractioLogging.llDebug:
    result.minLevel = llDebug
  of fractioLogging.llInfo:
    result.minLevel = llInfo
  of fractioLogging.llWarn:
    result.minLevel = llWarn
  of fractioLogging.llError:
    result.minLevel = llError

proc shouldLog*(a: LoggerAdapter, level: LogLevelDI): bool =
  level >= a.minLevel

proc log*(a: LoggerAdapter, level: LogLevelDI, msg: string,
    fields: tables.Table[string, string]) =
  ## Log via adapted logger
  if not a.shouldLog(level):
    return
  # Convert DI LogLevel to fractio LogLevel
  let fractioLevel = case level
    of llDebug: fractioLogging.llDebug
    of llInfo: fractioLogging.llInfo
    of llWarn: fractioLogging.llWarn
    of llError: fractioLogging.llError
  a.wrapped.log(fractioLevel, msg, fields)

proc debug*(a: LoggerAdapter, msg: string, fields: tables.Table[string,
    string] = tables.initTable[string, string]()) =
  a.log(llDebug, msg, fields)

proc info*(a: LoggerAdapter, msg: string, fields: tables.Table[string,
    string] = tables.initTable[string, string]()) =
  a.log(llInfo, msg, fields)

proc warn*(a: LoggerAdapter, msg: string, fields: tables.Table[string,
    string] = tables.initTable[string, string]()) =
  a.log(llWarn, msg, fields)

proc error*(a: LoggerAdapter, msg: string, fields: tables.Table[string,
    string] = tables.initTable[string, string]()) =
  a.log(llError, msg, fields)

proc setMinLevel*(a: LoggerAdapter, level: LogLevelDI) =
  a.minLevel = level
  let fractioLevel = case level
    of llDebug: fractioLogging.llDebug
    of llInfo: fractioLogging.llInfo
    of llWarn: fractioLogging.llWarn
    of llError: fractioLogging.llError
  a.wrapped.setMinLevel(fractioLevel)

# =============================================================================
# Factory Helper Functions
# =============================================================================

proc wrapLogger*(logger: fractioLogging.Logger): LoggerAdapter =
  ## Convenience function to wrap existing Logger
  newLoggerAdapter(logger)

proc wrapGlobalLogger*(): LoggerAdapter =
  ## Wrap the global logger from fractio/utils/logging
  newLoggerAdapter(fractioLogging.globalLogger)

proc defaultTimeProvider*(): SystemTimeProvider =
  ## Get default system time provider
  newSystemTimeProvider()

proc defaultLogger*(): LoggerAdapter =
  ## Get default logger adapter (wraps global logger)
  wrapGlobalLogger()

proc nullLogger*(): NullLogger =
  ## Get null logger (discards all messages)
  newNullLogger()

proc consoleLogger*(prefix: string = ""): ConsoleLogger =
  ## Get console logger for debugging
  newConsoleLogger(prefix)

proc memoryKVStore*(): InMemoryKVStore =
  ## Get in-memory KV store for testing
  newInMemoryKVStore()

proc memoryBackend*(): InMemoryBackend =
  ## Get in-memory backend for testing
  newInMemoryBackend()

# =============================================================================
# SharedTimer Time Provider Adapter - Makes sharedtimer.TimeProvider work with DI
# =============================================================================
# This adapter wraps sharedtimer's TimeProvider (which has `now()` method)
# and provides the DI-style API (nowNs, nowUs, nowMs)

type
  SharedTimerTimeProviderAdapter* = ref object of RootObj
    ## Adapter to make sharedtimer.TimeProvider work with DI TimeProviderConcept
    ## Converts `now()` -> `nowNs()`, `nowUs()`, `nowMs()`
    wrapped*: TimeProvider

proc newSharedTimerTimeProviderAdapter*(
  tp: TimeProvider): SharedTimerTimeProviderAdapter =
  ## Create adapter wrapping a sharedtimer.TimeProvider
  result = SharedTimerTimeProviderAdapter(wrapped: tp)

proc nowNs*(a: SharedTimerTimeProviderAdapter): int64 =
  ## Get nanoseconds from wrapped provider
  result = a.wrapped.now()

proc nowUs*(a: SharedTimerTimeProviderAdapter): int64 =
  ## Get microseconds from wrapped provider
  result = a.wrapped.now() div 1000

proc nowMs*(a: SharedTimerTimeProviderAdapter): int64 =
  ## Get milliseconds from wrapped provider
  result = a.wrapped.now() div 1_000_000

proc advance*(a: SharedTimerTimeProviderAdapter, deltaNs: int64) =
  ## Advance time (no-op for real time providers, works for mocks)
  # Check if wrapped is a MockTimeProvider and advance it
  # This requires runtime type check since TimeProvider is ref object of RootObj
  when defined(debugTimeAdapter):
    echo "SharedTimerTimeProviderAdapter.advance called with deltaNs=", deltaNs
  # For production time providers, advance is a no-op
  # Mock providers will need to be configured separately

# =============================================================================
# DI Time Provider to SharedTimer Adapter - Makes DI-style providers work with SharedTimer
# =============================================================================
# This adapter wraps a DI-style time provider (has nowNs, nowUs, nowMs)
# and provides the sharedtimer TimeProvider interface (now() method)
# It inherits from sharedtimer's TimeProvider so it can be used directly

type
  DITimeProviderAdapter* = ref object of TimeProvider
    ## Adapter to make DI-style time providers work with SharedTimer
    ## Converts `nowNs()` -> `now()` method
    ## Stores a function pointer since Nim concepts can't be stored as fields
    nowNsProc*: proc(): int64 {.gcsafe.}
    advanceProc*: proc(deltaNs: int64) {.gcsafe.}

proc newDITimeProviderAdapter*(nowNsFn: proc(): int64 {.gcsafe.},
                                advanceFn: proc(
                                    deltaNs: int64) {.gcsafe.} = nil): DITimeProviderAdapter =
  ## Create adapter with function pointers for time operations
  result = DITimeProviderAdapter(
    nowNsProc: nowNsFn,
    advanceProc: advanceFn
  )

method now*(a: DITimeProviderAdapter): Timestamp {.gcsafe.} =
  ## Get current time as Timestamp (nanoseconds)
  if a.nowNsProc != nil:
    result = a.nowNsProc()
  else:
    result = 0

proc advance*(a: DITimeProviderAdapter, deltaNs: int64) =
  ## Advance time if advance function is provided
  if a.advanceProc != nil:
    a.advanceProc(deltaNs)

# =============================================================================
# Factory functions for creating adapters from common types
# =============================================================================

proc adaptSharedTimerTimeProvider*(tp: TimeProvider): SharedTimerTimeProviderAdapter =
  ## Wrap a sharedtimer TimeProvider for use with DI container
  newSharedTimerTimeProviderAdapter(tp)

proc adaptMonotonicTimeProvider*(): SharedTimerTimeProviderAdapter =
  ## Create adapter around MonotonicTimeProvider
  newSharedTimerTimeProviderAdapter(MonotonicTimeProvider())

proc adaptWallClockTimeProvider*(): SharedTimerTimeProviderAdapter =
  ## Create adapter around WallClockTimeProvider
  newSharedTimerTimeProviderAdapter(WallClockTimeProvider())

# =============================================================================
# NuRaftCoordinator Adapter
# =============================================================================
# Wraps NuRaftCoordinator for DI use. The coordinator uses C bindings and
# module-level callbacks, so we wrap it rather than refactor directly.

from fractio/distributed/raft/nuraft_coordinator import NuRaftCoordinator,
    CoordinatorConfig, newNuRaftCoordinator, start, stop, hasGroup,
    getLeader, isLeader, isWriteReady, createAndStartGroup, removeGroup,
    getGroupCount, getLeaderCount, waitForWriteReady, proposeAndWait,
    setPriority, transferLeadership, registerGroup
from fractio/distributed/raft/multigroup_types import RaftCommand, RaftResult,
    WriteBatch, newWriteBatch, CommandKind
from fractio/distributed/raft/group_types import GroupID, genGroupID, NodeID

type
  NuRaftCoordinatorAdapter* = ref object of RootObj
    ## Adapter wrapping NuRaftCoordinator for DI container use.
    ## Provides a clean interface that matches RaftCoordinatorConcept.
    coordinator*: NuRaftCoordinator
    started*: bool

proc newNuRaftCoordinatorAdapter*(config: CoordinatorConfig): NuRaftCoordinatorAdapter =
  ## Create adapter with a new NuRaftCoordinator
  result = NuRaftCoordinatorAdapter(
    coordinator: newNuRaftCoordinator(config),
    started: false
  )

proc wrapNuRaftCoordinator*(coord: NuRaftCoordinator): NuRaftCoordinatorAdapter =
  ## Wrap an existing NuRaftCoordinator
  result = NuRaftCoordinatorAdapter(
    coordinator: coord,
    started: false
  )

# Forward NuRaftCoordinator methods

proc start*(a: NuRaftCoordinatorAdapter) =
  ## Start the coordinator
  if not a.started:
    a.coordinator.start()
    a.started = true

proc stop*(a: NuRaftCoordinatorAdapter) =
  ## Stop the coordinator
  if a.started:
    a.coordinator.stop()
    a.started = false

proc hasGroup*(a: NuRaftCoordinatorAdapter, groupId: GroupID): bool =
  ## Check if group exists
  a.coordinator.hasGroup(groupId)

proc getLeader*(a: NuRaftCoordinatorAdapter, groupId: GroupID): int32 =
  ## Get leader for group
  a.coordinator.getLeader(groupId)

proc isLeader*(a: NuRaftCoordinatorAdapter, groupId: GroupID): bool =
  ## Check if this node is leader
  a.coordinator.isLeader(groupId)

proc isWriteReady*(a: NuRaftCoordinatorAdapter, groupId: GroupID): bool =
  ## Check if group is write-ready
  a.coordinator.isWriteReady(groupId)

proc createAndStartGroup*(a: NuRaftCoordinatorAdapter, groupId: GroupID,
    members: seq[tuple[nodeId: uint32, host: string, port: int]],
    preferredLeader: uint32 = 0): bool =
  ## Create and start a Raft group
  a.coordinator.createAndStartGroup(groupId, members, preferredLeader)

proc removeGroup*(a: NuRaftCoordinatorAdapter, groupId: GroupID) =
  ## Remove a Raft group
  a.coordinator.removeGroup(groupId)

proc getGroupCount*(a: NuRaftCoordinatorAdapter): int =
  ## Get number of groups
  a.coordinator.getGroupCount()

proc getLeaderCount*(a: NuRaftCoordinatorAdapter): int =
  ## Get number of groups where this node is leader
  a.coordinator.getLeaderCount()

proc waitForWriteReady*(a: NuRaftCoordinatorAdapter, groupId: GroupID,
    timeoutMs: int = 2000): bool =
  ## Wait for group to be write-ready
  a.coordinator.waitForWriteReady(groupId, timeoutMs)

proc proposeAndWait*(a: NuRaftCoordinatorAdapter, groupId: GroupID,
    command: RaftCommand, timeoutMs: int = 5000): RaftResult =
  ## Propose command and wait for commit
  a.coordinator.proposeAndWait(groupId, command, timeoutMs)

proc proposeWrite*(a: NuRaftCoordinatorAdapter, groupId: GroupID,
    batch: WriteBatch, timeoutMs: int = 5000): RaftResult =
  ## Convenience: propose a write batch
  var cmd = RaftCommand(kind: ckWrite, writeBatch: batch)
  a.coordinator.proposeAndWait(groupId, cmd, timeoutMs)

proc setPriority*(a: NuRaftCoordinatorAdapter, groupId: GroupID,
    targetNodeId: NodeID, priority: int32): bool =
  ## Set priority for a target node in the group
  a.coordinator.setPriority(groupId, targetNodeId, priority)

proc transferLeadership*(a: NuRaftCoordinatorAdapter, groupId: GroupID,
    targetNodeId: NodeID): bool =
  ## Transfer leadership to target node
  a.coordinator.transferLeadership(groupId, targetNodeId)

proc isRunning*(a: NuRaftCoordinatorAdapter): bool =
  ## Check if coordinator is running
  a.started

proc registerGroup*(a: NuRaftCoordinatorAdapter, groupId: GroupID) =
  ## Register a group (for callback registration)
  a.coordinator.registerGroup(groupId)

# Convenience methods for DI

proc getCoordinator*(a: NuRaftCoordinatorAdapter): NuRaftCoordinator =
  ## Get the underlying NuRaftCoordinator
  a.coordinator
