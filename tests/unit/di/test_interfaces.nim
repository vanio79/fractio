# Unit tests for Fractio DI Interfaces

import std/[unittest, options, tables]
import fractio/di/interfaces as diInterfaces
import fractio/di/mocks
import fractio/di/adapters as diAdapters
import fractio/core/types
import fractio/core/errors
from fractio/distributed/raft/group_types import genGroupID, groupIDFromInt, GroupID

suite "ExecutionResult type":
  test "erkRows result":
    var r = diInterfaces.ExecutionResult(kind: diInterfaces.erkRows)
    r.rows = @[@["col1", "col2"], @["val1", "val2"]]
    r.count = 2
    check r.kind == diInterfaces.erkRows
    check r.rows.len == 2
    check r.count == 2

  test "erkModified result":
    var r = diInterfaces.ExecutionResult(kind: diInterfaces.erkModified, count: 5)
    check r.kind == diInterfaces.erkModified
    check r.count == 5
    check r.rows.len == 0

  test "erkEmpty result":
    var r = diInterfaces.ExecutionResult(kind: diInterfaces.erkEmpty)
    check r.kind == diInterfaces.erkEmpty
    check r.count == 0

  test "erkError result":
    var err = syntaxError("test error", "test_context")
    var r = diInterfaces.ExecutionResult(kind: diInterfaces.erkError,
        error: some(err))
    check r.kind == diInterfaces.erkError
    check r.error.isSome

suite "Error types":
  test "KVStoreError is FractioError":
    var e = diInterfaces.KVStoreError(msg: "kv error")
    check e of FractioError

  test "TransactionManagerError is FractioError":
    var e = diInterfaces.TransactionManagerError(msg: "txn error")
    check e of FractioError

  test "BackendError is FractioError":
    var e = diInterfaces.BackendError(msg: "backend error")
    check e of FractioError

  test "ConnectionError is FractioError":
    var e = diInterfaces.ConnectionError(msg: "connection error")
    check e of FractioError

  test "SqlExecutorError is FractioError":
    var e = diInterfaces.SqlExecutorError(msg: "sql error")
    check e of FractioError

suite "Mock implementations API":
  test "MockTimeProvider API":
    let m = newMockTimeProvider(1000)
    check m.nowNs() == 1000
    check m.nowUs() == 1
    check m.nowMs() == 0
    m.advance(500)
    check m.nowNs() == 1500
    m.close()

  test "SystemTimeProvider API":
    let s = diAdapters.newSystemTimeProvider()
    check s.nowNs() > 0
    check s.nowUs() > 0
    check s.nowMs() > 0

  test "SharedTimerTimeProviderAdapter API":
    let a = diAdapters.adaptMonotonicTimeProvider()
    check a.nowNs() > 0

  test "DITimeProviderAdapter API":
    let adapted = diAdapters.newDITimeProviderAdapter(
      proc(): int64 {.gcsafe.} = 5000'i64
    )
    check adapted.now() == 5000

  test "MockLogProvider API":
    let m = newMockLogProvider()
    m.info("test")
    check m.entries.len == 1
    m.close()

  test "NullLogger API":
    let n = diAdapters.newNullLogger()
    n.info("discarded")
    check not n.shouldLog(diAdapters.llInfo)

  test "ConsoleLogger API":
    let c = diAdapters.newConsoleLogger("test", diAdapters.llInfo)
    check c.shouldLog(diAdapters.llInfo)

  test "MockKVStore API":
    let m = newMockKVStore()
    discard m.put("key", "value")
    check m.get("key").isSome
    check m.exists("key")
    discard m.delete("key")
    check not m.exists("key")
    m.close()

  test "InMemoryKVStore API":
    let s = diAdapters.newInMemoryKVStore()
    discard s.put("k", "v")
    check s.get("k").isSome
    discard s.delete("k")
    check s.get("k").isNone
    s.close()

  test "MockTransactionManager API":
    let m = newMockTransactionManager()
    let txnId = m.begin()
    check m.getStatus(txnId) == tsActive
    check m.getActiveCount() == 1
    discard m.commit(txnId)
    check m.getStatus(txnId) == tsCommitted

  test "MockBackend API":
    let m = newMockBackend()
    discard m.put("k", "v")
    check m.get("k").isSome
    discard m.delete("k")
    check m.get("k").isNone
    let stats = m.stats()
    check stats.len > 0

  test "InMemoryBackend API":
    let b = diAdapters.newInMemoryBackend()
    discard b.put("k", "v")
    check b.get("k").isSome
    discard b.flush()
    discard b.compact()
    b.close()

  test "MockConnectionHandle API":
    let h = newMockConnectionHandle("localhost:8080")
    check h.isConnected()
    check h.remoteAddress() == "localhost:8080"
    discard h.send(@[1'u8, 2'u8])
    h.queueResponse(@[3'u8])
    check h.recv(100) == @[3'u8]
    discard h.close()
    check not h.isConnected()

  test "MockConnectionManager API":
    let m = newMockConnectionManager()
    let conn = m.acquire("host", 80'u16)
    check m.poolSize() >= 0
    check m.activeCount() >= 0
    m.release(conn)
    m.closeAll()

  test "MockRaftCoordinator API":
    let m = newMockRaftCoordinator()
    check not m.isRunning()
    m.start()
    check m.isRunning()
    m.stop()
    check not m.isRunning()

  test "MockRaftTransport API":
    let m = newMockRaftTransport()
    check not m.isServerRunning()
    m.startServer()
    check m.isServerRunning()
    discard m.send(NodeID("1"), @[1'u8, 2'u8])
    m.stopServer()
    check not m.isServerRunning()

  test "MockRaftStateMachine API":
    let m = newMockRaftStateMachine()
    discard m.apply(@[1'u8])
    check m.getLastAppliedIndex() >= 0
    let snap = m.snapshot()
    check snap.len >= 0

  test "MockRaftLog API":
    let m = newMockRaftLog()
    let idx = m.append(1, @[1'u8])
    check idx >= 0
    check m.getLastIndex() >= 0
    check m.getLastTerm() >= 0
    let data = m.get(idx)
    check data.isSome

  test "MockSpaceManager API":
    let m = newMockSpaceManager()
    let spaceId = m.createSpace("test")
    check m.listSpaces().len >= 1
    check m.getSpaceInfo(spaceId).isSome
    discard m.dropSpace(spaceId)

  test "MockNetworkTransport API":
    let m = newMockNetworkTransport()
    check m.connect("localhost", 8080'u16)
    check m.isConnected()
    discard m.send(@[1'u8])
    m.disconnect()
    check not m.isConnected()

  test "MockProtocolServer API":
    let m = newMockProtocolServer()
    check not m.isRunning()
    m.start()
    check m.isRunning()
    check m.clientCount() >= 0
    m.stop()
    check not m.isRunning()

  test "MockProtocolClient API":
    let m = newMockProtocolClient()
    check m.connect()
    check m.isConnected()
    check m.ping()
    check m.kvGet("key").isNone
    check m.kvPut("key", "value")
    m.disconnect()
    check not m.isConnected()

  test "MockSqlExecutor API":
    let m = newMockSqlExecutor()
    let result = m.execute("SELECT 1")
    check result.kind == diInterfaces.erkEmpty
    m.reset()
    check m.executeCallCount == 0

  test "MockSqlPlanner API":
    let m = newMockSqlPlanner()
    let planId = m.planSql("SELECT * FROM t")
    check planId >= 0
    m.reset()
    check m.planCallCount == 0

suite "Edge cases":
  test "ExecutionResult with empty rows":
    var r = diInterfaces.ExecutionResult(kind: diInterfaces.erkRows, rows: @[], count: 0)
    check r.kind == diInterfaces.erkRows
    check r.rows.len == 0

  test "ExecutionResult with no error":
    var r = diInterfaces.ExecutionResult(kind: diInterfaces.erkEmpty)
    check r.error.isNone

  test "Multiple error types inheritance":
    var kvErr = diInterfaces.KVStoreError(msg: "kv")
    var txnErr = diInterfaces.TransactionManagerError(msg: "txn")
    var backendErr = diInterfaces.BackendError(msg: "backend")
    check kvErr of FractioError
    check txnErr of FractioError
    check backendErr of FractioError
