# Tests for Fractio DI Bootstrap Module (Phase 7)

import unittest
import std/options
import fractio/app/bootstrap
import fractio/di/interfaces
import fractio/di/mocks
import fractio/di/container
import fractio/core/types

# Import LogLevel values explicitly to resolve ambiguity
from fractio/di/mocks import LogLevel, llDebug, llInfo, llWarn, llError

suite "createTestContainer Tests":

  test "creates container with all basic mocks":
    let c = createTestContainer()
    check c != nil
    check c.hasService(ServiceNameTimeProvider)
    check c.hasService(ServiceNameLogger)
    check c.hasService(ServiceNameKVStore)
    check c.hasService(ServiceNameBackend)
    check c.hasService(ServiceNameTxnManager)
    check c.hasService(ServiceNameConnManager)
    check c.hasService(ServiceNameExecutor)
    c.close()

  test "time provider is mock":
    let c = createTestContainer()
    let mockTime = getMockTimeProvider(c)
    check mockTime.nowNs() == 0
    mockTime.advance(1000)
    check mockTime.nowNs() == 1000
    c.close()

  test "logger is mock":
    let c = createTestContainer()
    let mockLogger = getMockLogProvider(c)
    mockLogger.info("test message")
    check mockLogger.entries.len == 1
    c.close()

  test "KV store is mock":
    let c = createTestContainer()
    let mockKV = getMockKVStore(c)
    discard mockKV.put("key1", "value1")
    check mockKV.get("key1").isSome
    check mockKV.get("key1").get() == "value1"
    c.close()

  test "backend is mock":
    let c = createTestContainer()
    let mockBackend = getMockBackend(c)
    discard mockBackend.put("key1", "value1")
    check mockBackend.get("key1").isSome
    c.close()

  test "transaction manager is mock":
    let c = createTestContainer()
    let mockTxn = getMockTransactionManager(c)
    let txnId = mockTxn.begin()
    check txnId != zeroTransactionID()
    check mockTxn.getActiveCount() == 1
    c.close()

  test "connection manager is mock":
    let c = createTestContainer()
    let mockConn = getMockConnectionManager(c)
    check mockConn.poolSize() == 0
    c.close()

  test "SQL executor is mock":
    let c = createTestContainer()
    let mockExec = getMockSqlExecutor(c)
    let result = mockExec.execute("SELECT 1")
    check result.kind == erkEmpty
    c.close()

suite "createMinimalTestContainer Tests":

  test "creates container with essential mocks only":
    let c = createMinimalTestContainer()
    check c.hasService(ServiceNameTimeProvider)
    check c.hasService(ServiceNameLogger)
    check not c.hasService(ServiceNameKVStore)
    check not c.hasService(ServiceNameBackend)
    c.close()

  test "time provider works":
    let c = createMinimalTestContainer()
    advanceTime(c, 5000)
    let mockTime = getMockTimeProvider(c)
    check mockTime.nowNs() == 5000
    c.close()

suite "createEmptyTestContainer Tests":

  test "creates empty container":
    let c = createEmptyTestContainer()
    check c != nil
    check not c.hasService(ServiceNameTimeProvider)
    check not c.hasService(ServiceNameLogger)
    c.close()

  test "can register custom mocks":
    let c = createEmptyTestContainer()
    let mockTime = newMockTimeProvider(12345)
    c.registerInstance(ServiceNameTimeProvider, cast[RootRef](mockTime))
    check c.hasService(ServiceNameTimeProvider)
    let resolved = getMockTimeProvider(c)
    check resolved.nowNs() == 12345
    c.close()

suite "createTestContainerWithTime Tests":

  test "creates container with preset time":
    let c = createTestContainerWithTime(1000000)
    let mockTime = getMockTimeProvider(c)
    check mockTime.nowNs() == 1000000
    c.close()

suite "createTestContextWithMocks Tests":

  test "creates test context with container":
    let tc = createTestContextWithMocks()
    check tc != nil
    check tc.container != nil
    check tc.container.hasService(ServiceNameTimeProvider)
    tc.cleanup()

suite "Mock Access Helpers Tests":

  test "getMockTimeProvider":
    let c = createTestContainer()
    let mock = getMockTimeProvider(c)
    mock.advance(100)
    check mock.nowNs() == 100
    c.close()

  test "getMockLogProvider":
    let c = createTestContainer()
    let mock = getMockLogProvider(c)
    mock.info("test")
    mock.assertLogged(llInfo, "test")
    c.close()

  test "getMockKVStore":
    let c = createTestContainer()
    let mock = getMockKVStore(c)
    discard mock.put("test", "value")
    mock.assertKeyExists("test")
    c.close()

  test "getMockBackend":
    let c = createTestContainer()
    let mock = getMockBackend(c)
    check mock.getCallCount == 0
    discard mock.get("key")
    check mock.getCallCount == 1
    c.close()

  test "getMockTransactionManager":
    let c = createTestContainer()
    let mock = getMockTransactionManager(c)
    discard mock.begin()
    mock.assertBeginCalled(1)
    c.close()

  test "getMockConnectionManager":
    let c = createTestContainer()
    let mock = getMockConnectionManager(c)
    check mock.acquireCallCount == 0
    discard mock.acquire("localhost", 8080)
    check mock.acquireCallCount == 1
    c.close()

  test "getMockSqlExecutor":
    let c = createTestContainer()
    let mock = getMockSqlExecutor(c)
    check mock.executeCallCount == 0
    discard mock.execute("SELECT 1")
    check mock.executeCallCount == 1
    c.close()

suite "Test Convenience Helpers Tests":

  test "advanceTime":
    let c = createTestContainer()
    advanceTime(c, 1000)
    advanceTime(c, 500)
    let mock = getMockTimeProvider(c)
    check mock.nowNs() == 1500
    c.close()

  test "setTime":
    let c = createTestContainer()
    setTime(c, 999999)
    let mock = getMockTimeProvider(c)
    check mock.nowNs() == 999999
    c.close()

  test "setKVData":
    let c = createTestContainer()
    setKVData(c, "key1", "value1")
    setKVData(c, "key2", "value2")
    let mock = getMockKVStore(c)
    check mock.get("key1").get() == "value1"
    check mock.get("key2").get() == "value2"
    c.close()

  test "setExecutorResult":
    let c = createTestContainer()
    let rowsResult = ExecutionResult(
      kind: erkRows,
      rows: @[@["1", "Alice"]],
      count: 1
    )
    setExecutorResult(c, "SELECT * FROM users", rowsResult)
    let result = getMockSqlExecutor(c).execute("SELECT * FROM users")
    check result.kind == erkRows
    check result.rows.len == 1
    c.close()

  test "assertLogged":
    let c = createTestContainer()
    let mock = getMockLogProvider(c)
    mock.info("test message")
    assertLogged(c, llInfo, "test message")
    c.close()

  test "assertKVGetCalled":
    let c = createTestContainer()
    let mock = getMockKVStore(c)
    discard mock.get("key1")
    discard mock.get("key2")
    assertKVGetCalled(c, 2)
    c.close()

  test "assertTxnBeginCalled":
    let c = createTestContainer()
    let mock = getMockTransactionManager(c)
    discard mock.begin()
    discard mock.begin()
    assertTxnBeginCalled(c, 2)
    c.close()

  test "resetAllMocks":
    let c = createTestContainer()
    # Use all mocks
    discard getMockTimeProvider(c).nowNs()
    getMockLogProvider(c).info("test")
    discard getMockKVStore(c).put("key", "value")
    discard getMockBackend(c).put("key", "value")
    discard getMockTransactionManager(c).begin()
    discard getMockSqlExecutor(c).execute("SELECT 1")

    # Reset all
    resetAllMocks(c)

    # Verify all reset
    check getMockTimeProvider(c).callCount == 0
    check getMockLogProvider(c).entries.len == 0
    check getMockKVStore(c).putCallCount == 0
    check getMockBackend(c).putCallCount == 0
    check getMockTransactionManager(c).beginCallCount == 0
    check getMockSqlExecutor(c).executeCallCount == 0
    c.close()

suite "Distributed Test Container Tests":

  test "creates container with distributed mocks":
    let c = createDistributedTestContainer()
    check c.hasService(ServiceNameRaftCoordinator)
    check c.hasService(ServiceNameRaftTransport)
    check c.hasService(ServiceNameRaftStateMachine)
    check c.hasService(ServiceNameRaftLog)
    check c.hasService(ServiceNameSpaceManager)
    check c.hasService(ServiceNameNetworkTransport)
    c.close()

  test "getMockRaftCoordinator":
    let c = createDistributedTestContainer()
    let mock = getMockRaftCoordinator(c)
    check not mock.isRunning()
    mock.start()
    check mock.isRunning()
    c.close()

  test "getMockRaftTransport":
    let c = createDistributedTestContainer()
    let mock = getMockRaftTransport(c)
    check not mock.isServerRunning()
    mock.startServer()
    check mock.isServerRunning()
    c.close()

  test "getMockRaftStateMachine":
    let c = createDistributedTestContainer()
    let mock = getMockRaftStateMachine(c)
    check mock.getLastAppliedIndex() == 0
    discard mock.apply(@[1.uint8, 2.uint8, 3.uint8])
    check mock.getLastAppliedIndex() == 1
    c.close()

  test "getMockRaftLog":
    let c = createDistributedTestContainer()
    let mock = getMockRaftLog(c)
    check mock.getLastIndex() == 0
    let idx = mock.append(1, @[1.uint8, 2.uint8, 3.uint8])
    check idx == 1
    c.close()

  test "getMockSpaceManager":
    let c = createDistributedTestContainer()
    let mock = getMockSpaceManager(c)
    let spaceId = mock.createSpace("test_space")
    check mock.getSpaceInfo(spaceId).isSome
    c.close()

  test "getMockNetworkTransport":
    let c = createDistributedTestContainer()
    let mock = getMockNetworkTransport(c)
    check not mock.isConnected()
    discard mock.connect("localhost", 8080)
    check mock.isConnected()
    c.close()

suite "Protocol Test Container Tests":

  test "creates container with protocol mocks":
    let c = createProtocolTestContainer()
    check c.hasService(ServiceNameProtocolServer)
    check c.hasService(ServiceNameProtocolClient)
    c.close()

  test "getMockProtocolServer":
    let c = createProtocolTestContainer()
    let mock = getMockProtocolServer(c)
    check not mock.isRunning()
    mock.start()
    check mock.isRunning()
    c.close()

  test "getMockProtocolClient":
    let c = createProtocolTestContainer()
    let mock = getMockProtocolClient(c)
    check not mock.isConnected()
    discard mock.connect()
    check mock.isConnected()
    c.close()

suite "SQL Test Container Tests":

  test "creates container with SQL mocks":
    let c = createSqlTestContainer()
    check c.hasService(ServiceNameSqlPlanner)
    c.close()

  test "getMockSqlPlanner":
    let c = createSqlTestContainer()
    let mock = getMockSqlPlanner(c)
    let planId = mock.planSql("SELECT 1")
    check planId > 0
    c.close()

suite "Full Stack Test Container Tests":

  test "creates container with all mocks":
    let c = createFullStackTestContainer()
    # Basic mocks
    check c.hasService(ServiceNameTimeProvider)
    check c.hasService(ServiceNameLogger)
    check c.hasService(ServiceNameKVStore)
    # Distributed mocks
    check c.hasService(ServiceNameRaftCoordinator)
    check c.hasService(ServiceNameRaftTransport)
    # Protocol mocks
    check c.hasService(ServiceNameProtocolServer)
    check c.hasService(ServiceNameProtocolClient)
    # SQL mocks
    check c.hasService(ServiceNameSqlPlanner)
    c.close()

  test "all mocks accessible in full stack container":
    let c = createFullStackTestContainer()
    # Access all mocks to verify they're registered correctly
    discard getMockTimeProvider(c).nowNs()
    getMockLogProvider(c).info("test")
    discard getMockKVStore(c).put("key", "value")
    discard getMockBackend(c).get("key")
    discard getMockTransactionManager(c).begin()
    discard getMockConnectionManager(c).poolSize()
    discard getMockSqlExecutor(c).execute("SELECT 1")
    discard getMockRaftCoordinator(c).isRunning()
    discard getMockRaftTransport(c).isServerRunning()
    discard getMockRaftStateMachine(c).getLastAppliedIndex()
    discard getMockRaftLog(c).getLastIndex()
    discard getMockSpaceManager(c).listSpaces()
    discard getMockNetworkTransport(c).isConnected()
    discard getMockProtocolServer(c).isRunning()
    discard getMockProtocolClient(c).isConnected()
    discard getMockSqlPlanner(c).planSql("SELECT 1")
    c.close()

suite "Container Thread Safety Tests":

  test "concurrent access to test container":
    let c = createTestContainer()
    var threads: array[4, Thread[Container]]

    proc worker(cont: Container) {.thread.} =
      for i in 0..<10:
        let mock = getMockTimeProvider(cont)
        mock.advance(1)

    for i in 0..<4:
      createThread(threads[i], worker, c)

    joinThreads(threads)

    let mock = getMockTimeProvider(c)
    check mock.nowNs() == 40 # 4 threads * 10 advances * 1 ns each
    c.close()
