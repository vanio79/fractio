# Mock Implementations for Fractio DI Testing
# Thread-safe mocks with call tracking and assertions

import std/[options, locks, deques, strformat, strutils, sequtils]
import tables
import fractio/core/types
import fractio/core/errors

# Import GroupID with all operators from distributed layer
from fractio/distributed/raft/group_types import GroupID, `==`, hash, `$`,
    ZeroGroupID, genGroupID

# Import SQL execution types from interfaces
from fractio/di/interfaces import ExecutionResultKind, ExecutionResult

export options, deques
export ExecutionResultKind, ExecutionResult

# =============================================================================
# LogLevel (must match context.nim)
# =============================================================================

type
  LogLevel* = enum
    llDebug, llInfo, llWarn, llError

# =============================================================================
# Mock Time Provider
# =============================================================================

type
  MockTimeProvider* = ref object of RootObj
    ## Mock time provider for deterministic testing
    currentTimeNs*: int64
    callCount*: int
    lock*: Lock

proc newMockTimeProvider*(startTimeNs: int64 = 0): MockTimeProvider =
  ## Create mock time provider with optional start time
  result = MockTimeProvider(
    currentTimeNs: startTimeNs,
    callCount: 0
  )
  initLock(result.lock)

proc nowNs*(m: MockTimeProvider): int64 =
  ## Get current nanosecond timestamp
  withLock(m.lock):
    inc m.callCount
    result = m.currentTimeNs

proc nowUs*(m: MockTimeProvider): int64 =
  ## Get current microsecond timestamp
  withLock(m.lock):
    inc m.callCount
    result = m.currentTimeNs div 1000

proc nowMs*(m: MockTimeProvider): int64 =
  ## Get current millisecond timestamp
  withLock(m.lock):
    inc m.callCount
    result = m.currentTimeNs div 1_000_000

proc advance*(m: MockTimeProvider, deltaNs: int64) =
  ## Advance mock time by delta nanoseconds
  withLock(m.lock):
    m.currentTimeNs += deltaNs

proc setTime*(m: MockTimeProvider, timeNs: int64) =
  ## Set mock time to specific value
  withLock(m.lock):
    m.currentTimeNs = timeNs

proc reset*(m: MockTimeProvider) =
  ## Reset mock state
  withLock(m.lock):
    m.currentTimeNs = 0
    m.callCount = 0

proc close*(m: MockTimeProvider) =
  ## Clean up mock
  deinitLock(m.lock)

# Assertion helpers
proc assertCalled*(m: MockTimeProvider, times: int) =
  ## Assert that nowNs was called exactly times times
  withLock(m.lock):
    doAssert m.callCount == times,
      fmt"Expected {times} calls to time provider, got {m.callCount}"

proc assertTimeEquals*(m: MockTimeProvider, expected: int64) =
  ## Assert current time equals expected
  withLock(m.lock):
    doAssert m.currentTimeNs == expected,
      fmt"Expected time {expected}, got {m.currentTimeNs}"

# =============================================================================
# Mock Log Provider
# =============================================================================

type
  LogEntry* = object
    ## Single log entry for tracking
    level*: LogLevel
    message*: string
    fields*: tables.Table[string, string]
    timestampNs*: int64

  MockLogProvider* = ref object of RootObj
    ## Mock logger that captures all log messages
    entries*: Deque[LogEntry]
    minLevel*: LogLevel
    callCount*: int
    lock*: Lock

proc newMockLogProvider*(minLevel: LogLevel = llDebug): MockLogProvider =
  ## Create mock log provider
  result = MockLogProvider(
    entries: initDeque[LogEntry](),
    minLevel: minLevel,
    callCount: 0
  )
  initLock(result.lock)

proc shouldLog*(m: MockLogProvider, level: LogLevel): bool =
  level >= m.minLevel

proc setMinLevel*(m: MockLogProvider, level: LogLevel) =
  withLock(m.lock):
    m.minLevel = level

proc log*(m: MockLogProvider, level: LogLevel, msg: string,
    fields: tables.Table[string, string]) =
  ## Log a message (captured for testing)
  if not m.shouldLog(level):
    return
  withLock(m.lock):
    inc m.callCount
    m.entries.addLast(LogEntry(
      level: level,
      message: msg,
      fields: fields,
      timestampNs: 0
    ))

proc debug*(m: MockLogProvider, msg: string, fields: tables.Table[string,
    string] = tables.initTable[string, string]()) =
  m.log(llDebug, msg, fields)

proc info*(m: MockLogProvider, msg: string, fields: tables.Table[string,
    string] = tables.initTable[string, string]()) =
  m.log(llInfo, msg, fields)

proc warn*(m: MockLogProvider, msg: string, fields: tables.Table[string,
    string] = tables.initTable[string, string]()) =
  m.log(llWarn, msg, fields)

proc error*(m: MockLogProvider, msg: string, fields: tables.Table[string,
    string] = tables.initTable[string, string]()) =
  m.log(llError, msg, fields)

proc reset*(m: MockLogProvider) =
  ## Clear all captured entries
  withLock(m.lock):
    m.entries.clear()
    m.callCount = 0

proc close*(m: MockLogProvider) =
  deinitLock(m.lock)

# Assertion helpers
proc assertLogged*(m: MockLogProvider, level: LogLevel, msg: string) =
  ## Assert that a specific log message was logged
  withLock(m.lock):
    for entry in m.entries:
      if entry.level == level and entry.message == msg:
        return
    doAssert false, fmt"Expected log entry [{level}] '{msg}' not found"

proc assertLoggedCount*(m: MockLogProvider, count: int) =
  ## Assert total number of log entries
  withLock(m.lock):
    doAssert m.entries.len == count,
      fmt"Expected {count} log entries, got {m.entries.len}"

proc assertNoErrors*(m: MockLogProvider) =
  ## Assert no error-level logs
  withLock(m.lock):
    for entry in m.entries:
      doAssert entry.level != llError,
        fmt"Unexpected error log: '{entry.message}'"

proc assertLoggedContains*(m: MockLogProvider, level: LogLevel,
    msgPart: string) =
  ## Assert that a log entry contains specific text
  withLock(m.lock):
    for entry in m.entries:
      if entry.level == level and entry.message.contains(msgPart):
        return
    doAssert false, fmt"Expected log entry [{level}] containing '{msgPart}' not found"

proc getEntries*(m: MockLogProvider): seq[LogEntry] =
  ## Get all captured log entries
  withLock(m.lock):
    result = @[]
    for entry in m.entries:
      result.add(entry)

proc getErrorEntries*(m: MockLogProvider): seq[LogEntry] =
  ## Get only error-level entries
  withLock(m.lock):
    result = @[]
    for entry in m.entries:
      if entry.level == llError:
        result.add(entry)

# =============================================================================
# Mock KV Store
# =============================================================================

type
  KVStoreOperation* = enum
    kvoGet, kvoPut, kvoDelete, kvoScan, kvoExists

  KVStoreCall* = object
    ## Record of a single call to the store
    operation*: KVStoreOperation
    key*: string
    value*: Option[string]
    prefix*: string
    limit*: uint32
    resultSuccess*: bool

  MockKVStore* = ref object of RootObj
    ## Mock KV store with call tracking
    data*: tables.Table[string, string]
    calls*: Deque[KVStoreCall]
    getCallCount*: int
    putCallCount*: int
    deleteCallCount*: int
    scanCallCount*: int
    existsCallCount*: int
    closed*: bool
    lock*: Lock
    ## Error injection for testing
    forceError*: bool
    errorMessage*: string

proc newMockKVStore*(): MockKVStore =
  ## Create mock KV store
  result = MockKVStore(
    data: tables.initTable[string, string](),
    calls: initDeque[KVStoreCall](),
    closed: false,
    forceError: false,
    errorMessage: ""
  )
  initLock(result.lock)

proc get*(m: MockKVStore, key: string): Option[string] =
  ## Get value by key
  withLock(m.lock):
    inc m.getCallCount
    m.calls.addLast(KVStoreCall(
      operation: kvoGet,
      key: key,
      resultSuccess: key in m.data
    ))
    if m.forceError:
      return none(string)
    if key in m.data:
      result = some(m.data[key])
    else:
      result = none(string)

proc put*(m: MockKVStore, key: string, value: string): bool =
  ## Put key-value pair, returns true on success
  withLock(m.lock):
    inc m.putCallCount
    m.calls.addLast(KVStoreCall(
      operation: kvoPut,
      key: key,
      value: some(value),
      resultSuccess: not m.forceError
    ))
    if m.forceError:
      return false
    m.data[key] = value
    return true

proc delete*(m: MockKVStore, key: string): bool =
  ## Delete by key, returns true on success
  withLock(m.lock):
    inc m.deleteCallCount
    m.calls.addLast(KVStoreCall(
      operation: kvoDelete,
      key: key,
      resultSuccess: not m.forceError
    ))
    if m.forceError:
      return false
    if key in m.data:
      m.data.del(key)
    return true

proc scan*(m: MockKVStore, prefix: string, limit: uint32): seq[(string, string)] =
  ## Scan keys with prefix
  withLock(m.lock):
    inc m.scanCallCount
    m.calls.addLast(KVStoreCall(
      operation: kvoScan,
      prefix: prefix,
      limit: limit,
      resultSuccess: true
    ))
    result = @[]
    var count = 0
    for key, value in m.data.pairs:
      if key.startsWith(prefix) and count < int(limit):
        result.add((key, value))
        inc count

proc exists*(m: MockKVStore, key: string): bool =
  ## Check if key exists
  withLock(m.lock):
    inc m.existsCallCount
    m.calls.addLast(KVStoreCall(
      operation: kvoExists,
      key: key,
      resultSuccess: key in m.data
    ))
    result = key in m.data

proc close*(m: MockKVStore) =
  ## Close the store
  withLock(m.lock):
    m.closed = true

proc reset*(m: MockKVStore) =
  ## Reset mock state
  withLock(m.lock):
    m.data.clear()
    m.calls.clear()
    m.getCallCount = 0
    m.putCallCount = 0
    m.deleteCallCount = 0
    m.scanCallCount = 0
    m.existsCallCount = 0
    m.closed = false
    m.forceError = false

proc setForceError*(m: MockKVStore, enable: bool, msg: string = "mock error") =
  ## Enable/disable forced errors for testing error handling
  withLock(m.lock):
    m.forceError = enable
    m.errorMessage = msg

# Assertion helpers
proc assertGetCalled*(m: MockKVStore, times: int) =
  withLock(m.lock):
    doAssert m.getCallCount == times,
      fmt"Expected {times} get calls, got {m.getCallCount}"

proc assertPutCalled*(m: MockKVStore, times: int) =
  withLock(m.lock):
    doAssert m.putCallCount == times,
      fmt"Expected {times} put calls, got {m.putCallCount}"

proc assertKeyExists*(m: MockKVStore, key: string) =
  withLock(m.lock):
    doAssert key in m.data, fmt"Expected key '{key}' to exist"

proc assertKeyNotExists*(m: MockKVStore, key: string) =
  withLock(m.lock):
    doAssert key notin m.data, fmt"Expected key '{key}' to not exist"

proc assertKeyValue*(m: MockKVStore, key: string, value: string) =
  withLock(m.lock):
    doAssert key in m.data, fmt"Key '{key}' not found"
    doAssert m.data[key] == value,
      fmt"Expected value '{value}' for key '{key}', got '{m.data[key]}'"

proc assertClosed*(m: MockKVStore) =
  withLock(m.lock):
    doAssert m.closed, "Expected store to be closed"

# =============================================================================
# Mock Transaction Manager
# =============================================================================

type
  MockTransaction* = ref object
    ## Internal transaction tracking
    id*: TransactionID
    status*: TransactionStatus
    snapshot*: int64

  MockTransactionManager* = ref object of RootObj
    ## Mock transaction manager for testing
    transactions*: tables.Table[TransactionID, MockTransaction]
    activeTxns*: Deque[TransactionID]
    beginCallCount*: int
    commitCallCount*: int
    rollbackCallCount*: int
    currentTimestamp*: int64
    lock*: Lock
    ## Error injection
    forceCommitError*: bool
    forceRollbackError*: bool

proc newMockTransactionManager*(): MockTransactionManager =
  ## Create mock transaction manager
  result = MockTransactionManager(
    transactions: tables.initTable[TransactionID, MockTransaction](),
    activeTxns: initDeque[TransactionID](),
    currentTimestamp: 0
  )
  initLock(result.lock)

proc begin*(m: MockTransactionManager): TransactionID =
  ## Begin a new transaction
  withLock(m.lock):
    inc m.beginCallCount
    let id = genTransactionID()
    inc m.currentTimestamp
    let txn = MockTransaction(
      id: id,
      status: tsActive,
      snapshot: m.currentTimestamp
    )
    m.transactions[id] = txn
    m.activeTxns.addLast(id)
    result = id

proc commit*(m: MockTransactionManager, txnId: TransactionID): bool =
  ## Commit a transaction, returns true on success
  withLock(m.lock):
    inc m.commitCallCount
    if m.forceCommitError:
      return false
    if txnId in m.transactions:
      m.transactions[txnId].status = tsCommitted
      # Remove from active
      var newActive = initDeque[TransactionID]()
      for id in m.activeTxns:
        if id != txnId:
          newActive.addLast(id)
      m.activeTxns = newActive
    return true

proc rollback*(m: MockTransactionManager, txnId: TransactionID): bool =
  ## Rollback a transaction, returns true on success
  withLock(m.lock):
    inc m.rollbackCallCount
    if m.forceRollbackError:
      return false
    if txnId in m.transactions:
      m.transactions[txnId].status = tsAborted
      # Remove from active
      var newActive = initDeque[TransactionID]()
      for id in m.activeTxns:
        if id != txnId:
          newActive.addLast(id)
      m.activeTxns = newActive
    return true

proc getStatus*(m: MockTransactionManager,
    txnId: TransactionID): TransactionStatus =
  ## Get transaction status
  withLock(m.lock):
    if txnId in m.transactions:
      result = m.transactions[txnId].status
    else:
      result = tsAborted # Unknown txn treated as aborted

proc getActiveCount*(m: MockTransactionManager): int =
  ## Count active transactions
  withLock(m.lock):
    result = m.activeTxns.len

proc getOldestSnapshot*(m: MockTransactionManager): int64 =
  ## Get oldest active snapshot
  withLock(m.lock):
    result = m.currentTimestamp
    for txnId in m.activeTxns:
      if txnId in m.transactions:
        let snapshot = m.transactions[txnId].snapshot
        if snapshot < result:
          result = snapshot

proc reset*(m: MockTransactionManager) =
  ## Reset mock state
  withLock(m.lock):
    m.transactions.clear()
    m.activeTxns.clear()
    m.beginCallCount = 0
    m.commitCallCount = 0
    m.rollbackCallCount = 0
    m.currentTimestamp = 0
    m.forceCommitError = false
    m.forceRollbackError = false

# Assertion helpers
proc assertBeginCalled*(m: MockTransactionManager, times: int) =
  withLock(m.lock):
    doAssert m.beginCallCount == times,
      fmt"Expected {times} begin calls, got {m.beginCallCount}"

proc assertCommitCalled*(m: MockTransactionManager, times: int) =
  withLock(m.lock):
    doAssert m.commitCallCount == times,
      fmt"Expected {times} commit calls, got {m.commitCallCount}"

proc assertActiveCount*(m: MockTransactionManager, count: int) =
  withLock(m.lock):
    doAssert m.activeTxns.len == count,
      fmt"Expected {count} active transactions, got {m.activeTxns.len}"

proc assertTxnStatus*(m: MockTransactionManager, txnId: TransactionID,
    status: TransactionStatus) =
  withLock(m.lock):
    doAssert txnId in m.transactions, "Transaction not found"
    doAssert m.transactions[txnId].status == status,
      fmt"Expected status {status}, got {m.transactions[txnId].status}"

# =============================================================================
# Mock Backend
# =============================================================================

type
  MockBackend* = ref object of RootObj
    ## Mock storage backend
    data*: tables.Table[string, string]
    getCallCount*: int
    putCallCount*: int
    deleteCallCount*: int
    scanCallCount*: int
    flushCallCount*: int
    compactCallCount*: int
    closed*: bool
    lock*: Lock
    statsData*: tables.Table[string, int64]

proc newMockBackend*(): MockBackend =
  ## Create mock backend
  result = MockBackend(
    data: tables.initTable[string, string](),
    statsData: tables.initTable[string, int64]()
  )
  initLock(result.lock)

proc get*(m: MockBackend, key: string): Option[string] =
  withLock(m.lock):
    inc m.getCallCount
    if key in m.data:
      result = some(m.data[key])
    else:
      result = none(string)

proc put*(m: MockBackend, key: string, value: string): bool =
  withLock(m.lock):
    inc m.putCallCount
    m.data[key] = value
    return true

proc delete*(m: MockBackend, key: string): bool =
  withLock(m.lock):
    inc m.deleteCallCount
    if key in m.data:
      m.data.del(key)
    return true

proc scan*(m: MockBackend, prefix: string, limit: uint32): seq[(string, string)] =
  withLock(m.lock):
    inc m.scanCallCount
    result = @[]
    var count = 0
    for key, value in m.data.pairs:
      if key.startsWith(prefix) and count < int(limit):
        result.add((key, value))
        inc count

proc flush*(m: MockBackend): bool =
  withLock(m.lock):
    inc m.flushCallCount
    return true

proc compact*(m: MockBackend): bool =
  withLock(m.lock):
    inc m.compactCallCount
    return true

proc close*(m: MockBackend) =
  withLock(m.lock):
    m.closed = true

proc stats*(m: MockBackend): tables.Table[string, int64] =
  withLock(m.lock):
    result = m.statsData
    result["get_count"] = m.getCallCount.int64
    result["put_count"] = m.putCallCount.int64
    result["delete_count"] = m.deleteCallCount.int64

proc reset*(m: MockBackend) =
  withLock(m.lock):
    m.data.clear()
    m.getCallCount = 0
    m.putCallCount = 0
    m.deleteCallCount = 0
    m.scanCallCount = 0
    m.flushCallCount = 0
    m.compactCallCount = 0
    m.closed = false

# =============================================================================
# Mock Connection Handle
# =============================================================================

type
  MockConnectionHandle* = ref object of RootObj
    ## Mock connection handle
    connected*: bool
    address*: string
    sentData*: Deque[seq[uint8]]
    recvQueue*: Deque[seq[uint8]]
    sendCallCount*: int
    recvCallCount*: int
    closeCallCount*: int
    lock*: Lock

proc newMockConnectionHandle*(address: string = "localhost:8080"): MockConnectionHandle =
  result = MockConnectionHandle(
    connected: true,
    address: address,
    sentData: initDeque[seq[uint8]](),
    recvQueue: initDeque[seq[uint8]]()
  )
  initLock(result.lock)

proc send*(h: MockConnectionHandle, data: seq[uint8]): bool =
  withLock(h.lock):
    inc h.sendCallCount
    if not h.connected:
      return false
    h.sentData.addLast(data)
    return true

proc recv*(h: MockConnectionHandle, maxSize: int): seq[uint8] =
  withLock(h.lock):
    inc h.recvCallCount
    if not h.connected:
      return @[]
    if h.recvQueue.len > 0:
      result = h.recvQueue.popFirst()
    else:
      result = @[] # Empty response

proc close*(h: MockConnectionHandle): bool =
  withLock(h.lock):
    inc h.closeCallCount
    h.connected = false
    return true

proc isConnected*(h: MockConnectionHandle): bool =
  withLock(h.lock):
    result = h.connected

proc remoteAddress*(h: MockConnectionHandle): string =
  withLock(h.lock):
    result = h.address

proc queueResponse*(h: MockConnectionHandle, data: seq[uint8]) =
  ## Queue data to be returned by recv()
  withLock(h.lock):
    h.recvQueue.addLast(data)

proc getSentData*(h: MockConnectionHandle): seq[seq[uint8]] =
  ## Get all data sent through this connection
  withLock(h.lock):
    result = @[]
    for data in h.sentData:
      result.add(data)

# =============================================================================
# Mock Connection Manager
# =============================================================================

type
  MockConnectionManager* = ref object of RootObj
    ## Mock connection pool manager
    connections*: tables.Table[string, MockConnectionHandle]
    acquireCallCount*: int
    releaseCallCount*: int
    closeAllCallCount*: int
    lock*: Lock

proc newMockConnectionManager*(): MockConnectionManager =
  result = MockConnectionManager(
    connections: tables.initTable[string, MockConnectionHandle]()
  )
  initLock(result.lock)

proc acquire*(m: MockConnectionManager, address: string,
    port: uint16): MockConnectionHandle =
  withLock(m.lock):
    inc m.acquireCallCount
    let key = fmt"{address}:{port}"
    if key in m.connections:
      result = m.connections[key]
    else:
      result = newMockConnectionHandle(key)
      m.connections[key] = result

proc release*(m: MockConnectionManager, conn: MockConnectionHandle) =
  withLock(m.lock):
    inc m.releaseCallCount

proc closeAll*(m: MockConnectionManager) =
  withLock(m.lock):
    inc m.closeAllCallCount
    for conn in m.connections.values:
      discard conn.close()
    m.connections.clear()

proc poolSize*(m: MockConnectionManager): int =
  withLock(m.lock):
    result = m.connections.len

proc activeCount*(m: MockConnectionManager): int =
  withLock(m.lock):
    result = 0
    for conn in m.connections.values:
      if conn.isConnected():
        inc result

# =============================================================================
# Mock SQL Executor (Phase 1 - Enhanced in Phase 6)
# =============================================================================

type
  MockSqlExecutor* = ref object of RootObj
    ## Mock SQL executor for testing
    ## Uses simplified interface with SQL strings for testing
    executeCallCount*: int
    executeInTxnCallCount*: int
    lastSql*: string
    lastTxnId*: TransactionID
    results*: tables.Table[string, ExecutionResult]
    defaultResult*: ExecutionResult
    lock*: Lock
    ## Error injection for testing error handling
    forceError*: bool
    errorMessage*: string

proc newMockSqlExecutor*(): MockSqlExecutor =
  result = MockSqlExecutor(
    results: tables.initTable[string, ExecutionResult](),
    defaultResult: ExecutionResult(kind: erkEmpty),
    forceError: false,
    errorMessage: ""
  )
  initLock(result.lock)

proc execute*(e: MockSqlExecutor, sql: string): ExecutionResult =
  ## Execute SQL statement (mock implementation)
  withLock(e.lock):
    inc e.executeCallCount
    e.lastSql = sql
    if e.forceError:
      result = ExecutionResult(
        kind: erkError,
        error: some(syntaxError(e.errorMessage, "MockSqlExecutor"))
      )
    elif sql in e.results:
      result = e.results[sql]
    else:
      result = e.defaultResult

proc executeInTxn*(e: MockSqlExecutor, sql: string,
    txnId: TransactionID): ExecutionResult =
  ## Execute SQL statement in transaction (mock implementation)
  withLock(e.lock):
    inc e.executeInTxnCallCount
    e.lastSql = sql
    e.lastTxnId = txnId
    if e.forceError:
      result = ExecutionResult(
        kind: erkError,
        error: some(syntaxError(e.errorMessage, "MockSqlExecutor"))
      )
    elif sql in e.results:
      result = e.results[sql]
    else:
      result = e.defaultResult

proc setResult*(e: MockSqlExecutor, sql: string, result: ExecutionResult) =
  ## Set a predefined result for a specific SQL statement
  withLock(e.lock):
    e.results[sql] = result

proc setDefaultResult*(e: MockSqlExecutor, result: ExecutionResult) =
  ## Set default result for all queries
  withLock(e.lock):
    e.defaultResult = result

proc setForceError*(e: MockSqlExecutor, enable: bool,
    msg: string = "mock error") =
  ## Enable/disable forced errors for testing error handling
  withLock(e.lock):
    e.forceError = enable
    e.errorMessage = msg

proc reset*(e: MockSqlExecutor) =
  ## Reset mock state
  withLock(e.lock):
    e.executeCallCount = 0
    e.executeInTxnCallCount = 0
    e.lastSql = ""
    e.lastTxnId = zeroTransactionID()
    e.results.clear()
    e.defaultResult = ExecutionResult(kind: erkEmpty)
    e.forceError = false
    e.errorMessage = ""

# =============================================================================
# Mock SQL Planner (Phase 6)
# =============================================================================

type
  MockSqlPlanner* = ref object of RootObj
    ## Mock SQL planner for testing
    ## Uses simplified interface with SQL strings for testing
    planCallCount*: int
    planWithDbCallCount*: int
    lastSql*: string
    lastDatabase*: string
    lastSchema*: string
    planIdCounter*: int64
    results*: tables.Table[string, int64] # SQL -> plan ID
    defaultPlanId*: int64
    lock*: Lock
    ## Error injection for testing error handling
    forceError*: bool
    errorMessage*: string

proc newMockSqlPlanner*(): MockSqlPlanner =
  result = MockSqlPlanner(
    results: tables.initTable[string, int64](),
    defaultPlanId: 0,
    planIdCounter: 0,
    forceError: false,
    errorMessage: ""
  )
  initLock(result.lock)

proc planSql*(p: MockSqlPlanner, sql: string): int64 =
  ## Plan SQL statement (mock implementation)
  ## Returns a plan ID for tracking
  withLock(p.lock):
    inc p.planCallCount
    p.lastSql = sql
    if p.forceError:
      result = -1 # Error indicator
    elif sql in p.results:
      result = p.results[sql]
    else:
      inc p.planIdCounter
      result = p.planIdCounter
      p.results[sql] = result

proc planSqlWithDb*(p: MockSqlPlanner, sql: string, database: string,
    schema: string): int64 =
  ## Plan SQL statement with database/schema context (mock implementation)
  withLock(p.lock):
    inc p.planWithDbCallCount
    p.lastSql = sql
    p.lastDatabase = database
    p.lastSchema = schema
    if p.forceError:
      result = -1 # Error indicator
    elif sql in p.results:
      result = p.results[sql]
    else:
      inc p.planIdCounter
      result = p.planIdCounter
      p.results[sql] = result

proc setPlanId*(p: MockSqlPlanner, sql: string, planId: int64) =
  ## Set a predefined plan ID for a specific SQL statement
  withLock(p.lock):
    p.results[sql] = planId

proc setDefaultPlanId*(p: MockSqlPlanner, planId: int64) =
  ## Set default plan ID for all statements
  withLock(p.lock):
    p.defaultPlanId = planId

proc setForceError*(p: MockSqlPlanner, enable: bool,
    msg: string = "mock error") =
  ## Enable/disable forced errors for testing error handling
  withLock(p.lock):
    p.forceError = enable
    p.errorMessage = msg

proc reset*(p: MockSqlPlanner) =
  ## Reset mock state
  withLock(p.lock):
    p.planCallCount = 0
    p.planWithDbCallCount = 0
    p.lastSql = ""
    p.lastDatabase = ""
    p.lastSchema = ""
    p.planIdCounter = 0
    p.results.clear()
    p.defaultPlanId = 0
    p.forceError = false
    p.errorMessage = ""

# =============================================================================
# Assertion helpers for SQL mocks
# =============================================================================

proc assertExecuteCalled*(e: MockSqlExecutor, times: int) =
  withLock(e.lock):
    doAssert e.executeCallCount == times,
      fmt"Expected {times} execute calls, got {e.executeCallCount}"

proc assertExecuteInTxnCalled*(e: MockSqlExecutor, times: int) =
  withLock(e.lock):
    doAssert e.executeInTxnCallCount == times,
      fmt"Expected {times} executeInTxn calls, got {e.executeInTxnCallCount}"

proc assertLastSql*(e: MockSqlExecutor, sql: string) =
  withLock(e.lock):
    doAssert e.lastSql == sql,
      fmt"Expected last SQL '{sql}', got '{e.lastSql}'"

proc assertLastTxnId*(e: MockSqlExecutor, txnId: TransactionID) =
  withLock(e.lock):
    doAssert e.lastTxnId == txnId,
      fmt"Expected last txnId {txnId}, got {e.lastTxnId}"

proc assertPlanCalled*(p: MockSqlPlanner, times: int) =
  withLock(p.lock):
    doAssert p.planCallCount == times,
      fmt"Expected {times} plan calls, got {p.planCallCount}"

proc assertPlanWithDbCalled*(p: MockSqlPlanner, times: int) =
  withLock(p.lock):
    doAssert p.planWithDbCallCount == times,
      fmt"Expected {times} planWithDb calls, got {p.planWithDbCallCount}"

proc assertLastPlanSql*(p: MockSqlPlanner, sql: string) =
  withLock(p.lock):
    doAssert p.lastSql == sql,
      fmt"Expected last SQL '{sql}', got '{p.lastSql}'"

proc assertLastDatabase*(p: MockSqlPlanner, database: string) =
  withLock(p.lock):
    doAssert p.lastDatabase == database,
      fmt"Expected last database '{database}', got '{p.lastDatabase}'"

proc assertLastSchema*(p: MockSqlPlanner, schema: string) =
  withLock(p.lock):
    doAssert p.lastSchema == schema,
      fmt"Expected last schema '{schema}', got '{p.lastSchema}'"

# =============================================================================
# Distributed Layer Mocks (Phase 4)
# =============================================================================

type
  MockRaftCoordinator* = ref object of RootObj
    ## Mock Raft coordinator for testing distributed components
    groups*: tables.Table[GroupID, tuple[leader: int32, running: bool]]
    running*: bool
    startCallCount*: int
    stopCallCount*: int
    lock*: Lock

proc newMockRaftCoordinator*(): MockRaftCoordinator =
  result = MockRaftCoordinator(
    groups: tables.initTable[GroupID, tuple[leader: int32, running: bool]](),
    running: false
  )
  initLock(result.lock)

proc start*(rc: MockRaftCoordinator) =
  withLock(rc.lock):
    inc rc.startCallCount
    rc.running = true

proc stop*(rc: MockRaftCoordinator) =
  withLock(rc.lock):
    inc rc.stopCallCount
    rc.running = false

proc hasGroup*(rc: MockRaftCoordinator, groupId: GroupID): bool =
  withLock(rc.lock):
    result = groupId in rc.groups

proc getLeader*(rc: MockRaftCoordinator, groupId: GroupID): int32 =
  withLock(rc.lock):
    if groupId in rc.groups:
      result = rc.groups[groupId].leader
    else:
      result = -1

proc isLeader*(rc: MockRaftCoordinator, groupId: GroupID): bool =
  withLock(rc.lock):
    result = false

proc isRunning*(rc: MockRaftCoordinator): bool =
  withLock(rc.lock):
    result = rc.running

proc addGroup*(rc: MockRaftCoordinator, groupId: GroupID, leader: int32) =
  ## Add a group to the mock coordinator
  withLock(rc.lock):
    rc.groups[groupId] = (leader: leader, running: true)

proc removeGroup*(rc: MockRaftCoordinator, groupId: GroupID) =
  ## Remove a group from the mock coordinator
  withLock(rc.lock):
    rc.groups.del(groupId)

proc setLeader*(rc: MockRaftCoordinator, groupId: GroupID, leader: int32) =
  ## Set leader for a specific group
  withLock(rc.lock):
    if groupId in rc.groups:
      rc.groups[groupId] = (leader: leader, running: true)

proc reset*(rc: MockRaftCoordinator) =
  withLock(rc.lock):
    rc.groups.clear()
    rc.running = false
    rc.startCallCount = 0
    rc.stopCallCount = 0

# =============================================================================
# Mock Raft Transport
# =============================================================================

type
  MockRaftTransport* = ref object of RootObj
    ## Mock Raft transport for testing
    serverRunning*: bool
    messagesSent*: seq[tuple[target: NodeID, data: seq[uint8]]]
    messagesReceived*: seq[seq[uint8]]
    sendCallCount*: int
    lock*: Lock

proc newMockRaftTransport*(): MockRaftTransport =
  result = MockRaftTransport(
    serverRunning: false,
    messagesSent: @[],
    messagesReceived: @[]
  )
  initLock(result.lock)

proc send*(rt: MockRaftTransport, targetNodeId: NodeID, data: seq[uint8]): bool =
  withLock(rt.lock):
    inc rt.sendCallCount
    rt.messagesSent.add((target: targetNodeId, data: data))
    result = true

proc startServer*(rt: MockRaftTransport) =
  withLock(rt.lock):
    rt.serverRunning = true

proc stopServer*(rt: MockRaftTransport) =
  withLock(rt.lock):
    rt.serverRunning = false

proc isServerRunning*(rt: MockRaftTransport): bool =
  withLock(rt.lock):
    result = rt.serverRunning

proc receiveMessage*(rt: MockRaftTransport, data: seq[uint8]) =
  ## Simulate receiving a message (for testing)
  withLock(rt.lock):
    rt.messagesReceived.add(data)

proc reset*(rt: MockRaftTransport) =
  withLock(rt.lock):
    rt.serverRunning = false
    rt.messagesSent = @[]
    rt.messagesReceived = @[]
    rt.sendCallCount = 0

# =============================================================================
# Mock Raft State Machine
# =============================================================================

type
  MockRaftStateMachine* = ref object of RootObj
    ## Mock Raft state machine
    appliedEntries*: seq[seq[uint8]]
    lastAppliedIndex*: int64
    snapshots*: seq[seq[uint8]]
    applyCallCount*: int
    lock*: Lock

proc newMockRaftStateMachine*(): MockRaftStateMachine =
  result = MockRaftStateMachine(
    appliedEntries: @[],
    lastAppliedIndex: 0,
    snapshots: @[]
  )
  initLock(result.lock)

proc apply*(sm: MockRaftStateMachine, data: seq[uint8]): bool =
  withLock(sm.lock):
    inc sm.applyCallCount
    sm.appliedEntries.add(data)
    inc sm.lastAppliedIndex
    result = true

proc getLastAppliedIndex*(sm: MockRaftStateMachine): int64 =
  withLock(sm.lock):
    result = sm.lastAppliedIndex

proc snapshot*(sm: MockRaftStateMachine): seq[uint8] =
  ## Return a snapshot of current state
  withLock(sm.lock):
    # Simple concatenation of all applied entries
    var resultSeq: seq[uint8] = @[]
    for entry in sm.appliedEntries:
      resultSeq.add(entry)
    sm.snapshots.add(resultSeq)
    result = resultSeq

proc reset*(sm: MockRaftStateMachine) =
  withLock(sm.lock):
    sm.appliedEntries = @[]
    sm.lastAppliedIndex = 0
    sm.snapshots = @[]
    sm.applyCallCount = 0

# =============================================================================
# Mock Raft Log
# =============================================================================

type
  MockRaftLog* = ref object of RootObj
    ## Mock Raft log storage
    entries*: seq[tuple[term: int64, index: int64, data: seq[uint8]]]
    lastIndex*: int64
    lastTerm*: int64
    lock*: Lock

proc newMockRaftLog*(): MockRaftLog =
  result = MockRaftLog(
    entries: @[],
    lastIndex: 0,
    lastTerm: 0
  )
  initLock(result.lock)

proc append*(rl: MockRaftLog, term: int64, data: seq[uint8]): int64 =
  withLock(rl.lock):
    inc rl.lastIndex
    rl.lastTerm = term
    rl.entries.add((term: term, index: rl.lastIndex, data: data))
    result = rl.lastIndex

proc get*(rl: MockRaftLog, index: int64): Option[seq[uint8]] =
  withLock(rl.lock):
    for entry in rl.entries:
      if entry.index == index:
        return some(entry.data)
    result = none(seq[uint8])

proc truncate*(rl: MockRaftLog, index: int64): bool =
  withLock(rl.lock):
    if index > rl.lastIndex:
      return false
    # Remove entries after the given index
    rl.entries = rl.entries.filterIt(it.index <= index)
    rl.lastIndex = index
    if rl.entries.len > 0:
      rl.lastTerm = rl.entries[^1].term
    else:
      rl.lastTerm = 0
    result = true

proc getLastIndex*(rl: MockRaftLog): int64 =
  withLock(rl.lock):
    result = rl.lastIndex

proc getLastTerm*(rl: MockRaftLog): int64 =
  withLock(rl.lock):
    result = rl.lastTerm

proc reset*(rl: MockRaftLog) =
  withLock(rl.lock):
    rl.entries = @[]
    rl.lastIndex = 0
    rl.lastTerm = 0

# =============================================================================
# Mock Space Manager
# =============================================================================

type
  MockSpaceInfo* = ref object of RootObj
    ## Mock space info record
    spaceId*: GroupID
    name*: string
    groupIds*: seq[GroupID]

  MockSpaceManager* = ref object of RootObj
    ## Mock space manager
    spaces*: tables.Table[GroupID, MockSpaceInfo]
    nextSpaceId*: GroupID
    createCallCount*: int
    dropCallCount*: int
    lock*: Lock

proc newMockSpaceManager*(): MockSpaceManager =
  result = MockSpaceManager(
    spaces: tables.initTable[GroupID, MockSpaceInfo](),
    nextSpaceId: ZeroGroupID() # Zero GroupID as starting point
  )
  initLock(result.lock)

proc createSpace*(sm: MockSpaceManager, spaceName: string): GroupID =
  withLock(sm.lock):
    inc sm.createCallCount
    # Generate a new space ID (simple increment for testing)
    let spaceId = sm.nextSpaceId
    sm.spaces[spaceId] = MockSpaceInfo(
      spaceId: spaceId,
      name: spaceName,
      groupIds: @[spaceId]
    )
    result = spaceId

proc dropSpace*(sm: MockSpaceManager, spaceId: GroupID): bool =
  withLock(sm.lock):
    inc sm.dropCallCount
    result = spaceId in sm.spaces
    if result:
      sm.spaces.del(spaceId)

proc getSpaceInfo*(sm: MockSpaceManager, spaceId: GroupID): Option[RootRef] =
  withLock(sm.lock):
    if spaceId in sm.spaces:
      result = some(cast[RootRef](sm.spaces[spaceId]))
    else:
      result = none(RootRef)

proc listSpaces*(sm: MockSpaceManager): seq[GroupID] =
  withLock(sm.lock):
    result = @[]
    for spaceId in sm.spaces.keys:
      result.add(spaceId)

proc addSpace*(sm: MockSpaceManager, spaceId: GroupID, name: string) =
  ## Add a pre-defined space (for testing)
  withLock(sm.lock):
    sm.spaces[spaceId] = MockSpaceInfo(
      spaceId: spaceId,
      name: name,
      groupIds: @[spaceId]
    )

proc reset*(sm: MockSpaceManager) =
  withLock(sm.lock):
    sm.spaces.clear()
    sm.createCallCount = 0
    sm.dropCallCount = 0

# =============================================================================
# Mock Network Transport
# =============================================================================

type
  MockNetworkTransport* = ref object of RootObj
    ## Mock network transport for testing
    connected*: bool
    currentHost*: string
    currentPort*: uint16
    messagesSent*: seq[seq[uint8]]
    messagesToReceive*: seq[seq[uint8]]
    receiveIndex*: int
    connectCallCount*: int
    disconnectCallCount*: int
    lock*: Lock

proc newMockNetworkTransport*(): MockNetworkTransport =
  result = MockNetworkTransport(
    connected: false,
    messagesSent: @[],
    messagesToReceive: @[],
    receiveIndex: 0
  )
  initLock(result.lock)

proc connect*(nt: MockNetworkTransport, host: string, port: uint16): bool =
  withLock(nt.lock):
    inc nt.connectCallCount
    nt.connected = true
    nt.currentHost = host
    nt.currentPort = port
    result = true

proc disconnect*(nt: MockNetworkTransport) =
  withLock(nt.lock):
    inc nt.disconnectCallCount
    nt.connected = false
    nt.currentHost = ""
    nt.currentPort = 0

proc isConnected*(nt: MockNetworkTransport): bool =
  withLock(nt.lock):
    result = nt.connected

proc send*(nt: MockNetworkTransport, data: seq[uint8]): bool =
  withLock(nt.lock):
    if not nt.connected:
      return false
    nt.messagesSent.add(data)
    result = true

proc recv*(nt: MockNetworkTransport, timeoutMs: int): Option[seq[uint8]] =
  withLock(nt.lock):
    if not nt.connected or nt.receiveIndex >= nt.messagesToReceive.len:
      result = none(seq[uint8])
    else:
      result = some(nt.messagesToReceive[nt.receiveIndex])
      inc nt.receiveIndex

proc queueReceive*(nt: MockNetworkTransport, data: seq[uint8]) =
  ## Queue a message to be received (for testing)
  withLock(nt.lock):
    nt.messagesToReceive.add(data)

proc reset*(nt: MockNetworkTransport) =
  withLock(nt.lock):
    nt.connected = false
    nt.currentHost = ""
    nt.currentPort = 0
    nt.messagesSent = @[]
    nt.messagesToReceive = @[]
    nt.receiveIndex = 0
    nt.connectCallCount = 0
    nt.disconnectCallCount = 0

# =============================================================================
# Protocol Layer Mocks (Phase 5)
# =============================================================================

type
  MockProtocolServer* = ref object of RootObj
    ## Mock protocol server for testing protocol behavior
    running*: bool
    clients*: tables.Table[uint32, RootRef] # Mock client connections
    handlers*: tables.Table[int, proc(conn: RootRef, requestId: uint32,
        flags: uint16, payload: string) {.gcsafe.}]
    startCallCount*: int
    stopCallCount*: int
    registerHandlerCallCount*: int
    lock*: Lock
    ## KV store simulation for testing
    kvData*: tables.Table[string, string]

proc newMockProtocolServer*(): MockProtocolServer =
  result = MockProtocolServer(
    running: false,
    clients: tables.initTable[uint32, RootRef](),
    handlers: tables.initTable[int, proc(conn: RootRef, requestId: uint32,
        flags: uint16, payload: string) {.gcsafe.}](),
    kvData: tables.initTable[string, string]()
  )
  initLock(result.lock)

proc start*(ps: MockProtocolServer) =
  withLock(ps.lock):
    inc ps.startCallCount
    ps.running = true

proc stop*(ps: MockProtocolServer) =
  withLock(ps.lock):
    inc ps.stopCallCount
    ps.running = false

proc isRunning*(ps: MockProtocolServer): bool =
  withLock(ps.lock):
    result = ps.running

proc clientCount*(ps: MockProtocolServer): int =
  withLock(ps.lock):
    result = ps.clients.len

proc registerHandler*(ps: MockProtocolServer, msgType: int,
    handler: proc(conn: RootRef, requestId: uint32, flags: uint16,
        payload: string) {.gcsafe.}) =
  withLock(ps.lock):
    inc ps.registerHandlerCallCount
    ps.handlers[msgType] = handler

proc addClient*(ps: MockProtocolServer, clientId: uint32, conn: RootRef) =
  ## Add a mock client connection
  withLock(ps.lock):
    ps.clients[clientId] = conn

proc removeClient*(ps: MockProtocolServer, clientId: uint32) =
  ## Remove a mock client connection
  withLock(ps.lock):
    ps.clients.del(clientId)

proc kvGet*(ps: MockProtocolServer, key: string): Option[string] =
  ## Simulate KV get operation
  withLock(ps.lock):
    if key in ps.kvData:
      result = some(ps.kvData[key])
    else:
      result = none(string)

proc kvPut*(ps: MockProtocolServer, key: string, value: string) =
  ## Simulate KV put operation
  withLock(ps.lock):
    ps.kvData[key] = value

proc kvDelete*(ps: MockProtocolServer, key: string): bool =
  ## Simulate KV delete operation
  withLock(ps.lock):
    result = key in ps.kvData
    if result:
      ps.kvData.del(key)

proc reset*(ps: MockProtocolServer) =
  withLock(ps.lock):
    ps.running = false
    ps.clients.clear()
    ps.handlers.clear()
    ps.kvData.clear()
    ps.startCallCount = 0
    ps.stopCallCount = 0
    ps.registerHandlerCallCount = 0

# =============================================================================
# Mock Protocol Client
# =============================================================================

type
  MockProtocolClient* = ref object of RootObj
    ## Mock protocol client for testing client behavior
    connected*: bool
    connectCallCount*: int
    disconnectCallCount*: int
    pingCallCount*: int
    getCallCount*: int
    putCallCount*: int
    deleteCallCount*: int
    scanCallCount*: int
    beginTxnCallCount*: int
    commitTxnCallCount*: int
    rollbackTxnCallCount*: int
    lock*: Lock
    ## KV store simulation for testing server responses
    kvData*: tables.Table[string, string]
    ## Transaction simulation
    activeTxns*: Deque[TransactionID]
    ## Response simulation
    forceConnectError*: bool
    forceGetError*: bool
    forcePutError*: bool

proc newMockProtocolClient*(): MockProtocolClient =
  result = MockProtocolClient(
    connected: false,
    kvData: tables.initTable[string, string](),
    activeTxns: initDeque[TransactionID]()
  )
  initLock(result.lock)

proc connect*(pc: MockProtocolClient): bool =
  withLock(pc.lock):
    inc pc.connectCallCount
    if pc.forceConnectError:
      result = false
    else:
      pc.connected = true
      result = true

proc disconnect*(pc: MockProtocolClient) =
  withLock(pc.lock):
    inc pc.disconnectCallCount
    pc.connected = false

proc isConnected*(pc: MockProtocolClient): bool =
  withLock(pc.lock):
    result = pc.connected

proc ping*(pc: MockProtocolClient): bool =
  withLock(pc.lock):
    inc pc.pingCallCount
    result = pc.connected

proc kvGet*(pc: MockProtocolClient, key: string): Option[string] =
  withLock(pc.lock):
    inc pc.getCallCount
    if not pc.connected or pc.forceGetError:
      result = none(string)
    elif key in pc.kvData:
      result = some(pc.kvData[key])
    else:
      result = none(string)

proc kvPut*(pc: MockProtocolClient, key: string, value: string): bool =
  withLock(pc.lock):
    inc pc.putCallCount
    if not pc.connected or pc.forcePutError:
      result = false
    else:
      pc.kvData[key] = value
      result = true

proc kvDelete*(pc: MockProtocolClient, key: string): bool =
  withLock(pc.lock):
    inc pc.deleteCallCount
    if not pc.connected:
      result = false
    elif key in pc.kvData:
      pc.kvData.del(key)
      result = true
    else:
      result = false

proc kvScan*(pc: MockProtocolClient, prefix: string, limit: uint32): seq[(
    string, string)] =
  withLock(pc.lock):
    inc pc.scanCallCount
    result = @[]
    if not pc.connected:
      return
    var count = 0
    for key, value in pc.kvData.pairs:
      if key.startsWith(prefix) and count < int(limit):
        result.add((key, value))
        inc count

proc beginTxn*(pc: MockProtocolClient): TransactionID =
  withLock(pc.lock):
    inc pc.beginTxnCallCount
    if not pc.connected:
      result = zeroTransactionID()
    else:
      # Generate a new unique transaction ID using the standard generator
      result = genTransactionID()
      pc.activeTxns.addLast(result)

proc commitTxn*(pc: MockProtocolClient, txnId: TransactionID): bool =
  withLock(pc.lock):
    inc pc.commitTxnCallCount
    if not pc.connected:
      result = false
    else:
      # Remove from active transactions
      var newActive = initDeque[TransactionID]()
      for id in pc.activeTxns:
        if id != txnId:
          newActive.addLast(id)
      pc.activeTxns = newActive
      result = true

proc rollbackTxn*(pc: MockProtocolClient, txnId: TransactionID): bool =
  withLock(pc.lock):
    inc pc.rollbackTxnCallCount
    if not pc.connected:
      result = false
    else:
      # Remove from active transactions
      var newActive = initDeque[TransactionID]()
      for id in pc.activeTxns:
        if id != txnId:
          newActive.addLast(id)
      pc.activeTxns = newActive
      result = true

proc setForceConnectError*(pc: MockProtocolClient, enable: bool) =
  ## Enable/disable forced connect errors for testing
  withLock(pc.lock):
    pc.forceConnectError = enable

proc setForceGetError*(pc: MockProtocolClient, enable: bool) =
  ## Enable/disable forced get errors for testing
  withLock(pc.lock):
    pc.forceGetError = enable

proc setForcePutError*(pc: MockProtocolClient, enable: bool) =
  ## Enable/disable forced put errors for testing
  withLock(pc.lock):
    pc.forcePutError = enable

proc reset*(pc: MockProtocolClient) =
  withLock(pc.lock):
    pc.connected = false
    pc.kvData.clear()
    pc.activeTxns.clear()
    pc.connectCallCount = 0
    pc.disconnectCallCount = 0
    pc.pingCallCount = 0
    pc.getCallCount = 0
    pc.putCallCount = 0
    pc.deleteCallCount = 0
    pc.scanCallCount = 0
    pc.beginTxnCallCount = 0
    pc.commitTxnCallCount = 0
    pc.rollbackTxnCallCount = 0
    pc.forceConnectError = false
    pc.forceGetError = false
    pc.forcePutError = false

# =============================================================================
# Assertion helpers for Protocol mocks
# =============================================================================

proc assertStartCalled*(ps: MockProtocolServer, times: int) =
  withLock(ps.lock):
    doAssert ps.startCallCount == times,
      fmt"Expected {times} start calls, got {ps.startCallCount}"

proc assertStopCalled*(ps: MockProtocolServer, times: int) =
  withLock(ps.lock):
    doAssert ps.stopCallCount == times,
      fmt"Expected {times} stop calls, got {ps.stopCallCount}"

proc assertRunning*(ps: MockProtocolServer) =
  withLock(ps.lock):
    doAssert ps.running, "Expected server to be running"

proc assertNotRunning*(ps: MockProtocolServer) =
  withLock(ps.lock):
    doAssert not ps.running, "Expected server to not be running"

proc assertConnectCalled*(pc: MockProtocolClient, times: int) =
  withLock(pc.lock):
    doAssert pc.connectCallCount == times,
      fmt"Expected {times} connect calls, got {pc.connectCallCount}"

proc assertDisconnectCalled*(pc: MockProtocolClient, times: int) =
  withLock(pc.lock):
    doAssert pc.disconnectCallCount == times,
      fmt"Expected {times} disconnect calls, got {pc.disconnectCallCount}"

proc assertPingCalled*(pc: MockProtocolClient, times: int) =
  withLock(pc.lock):
    doAssert pc.pingCallCount == times,
      fmt"Expected {times} ping calls, got {pc.pingCallCount}"

proc assertGetCalled*(pc: MockProtocolClient, times: int) =
  withLock(pc.lock):
    doAssert pc.getCallCount == times,
      fmt"Expected {times} get calls, got {pc.getCallCount}"

proc assertPutCalled*(pc: MockProtocolClient, times: int) =
  withLock(pc.lock):
    doAssert pc.putCallCount == times,
      fmt"Expected {times} put calls, got {pc.putCallCount}"

proc assertKVData*(pc: MockProtocolClient, key: string, value: string) =
  withLock(pc.lock):
    doAssert key in pc.kvData, fmt"Key '{key}' not found in mock KV data"
    doAssert pc.kvData[key] == value,
      fmt"Expected value '{value}' for key '{key}', got '{pc.kvData[key]}'"

proc assertTxnCount*(pc: MockProtocolClient, count: int) =
  withLock(pc.lock):
    doAssert pc.activeTxns.len == count,
      fmt"Expected {count} active transactions, got {pc.activeTxns.len}"
