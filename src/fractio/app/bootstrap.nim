# Bootstrap Module for Fractio DI Container
# Provides factory functions for creating configured containers

import fractio/di/container
import fractio/di/interfaces
import fractio/di/mocks
import fractio/di/context

# =============================================================================
# Service Name Constants (re-exported from context.nim)
# =============================================================================

export ServiceNameTimeProvider, ServiceNameLogger, ServiceNameKVStore,
    ServiceNameBackend, ServiceNameTxnManager, ServiceNameConnManager,
    ServiceNameExecutor

# Re-export TestContext and cleanup for convenience
export TestContext, newTestContext, cleanup

# =============================================================================
# Container Creation - Test Environment
# =============================================================================

proc createTestContainer*(): Container =
  ## Create a container pre-configured with mock implementations for testing
  ## This is useful for unit tests that need DI services
  result = newContainer()

  # Register mock time provider
  let mockTime = newMockTimeProvider(0)
  result.registerInstance(ServiceNameTimeProvider, cast[RootRef](mockTime))

  # Register mock log provider
  let mockLogger = newMockLogProvider()
  result.registerInstance(ServiceNameLogger, cast[RootRef](mockLogger))

  # Register mock KV store
  let mockKVStore = newMockKVStore()
  result.registerInstance(ServiceNameKVStore, cast[RootRef](mockKVStore))

  # Register mock backend
  let mockBackend = newMockBackend()
  result.registerInstance(ServiceNameBackend, cast[RootRef](mockBackend))

  # Register mock transaction manager
  let mockTxnMgr = newMockTransactionManager()
  result.registerInstance(ServiceNameTxnManager, cast[RootRef](mockTxnMgr))

  # Register mock connection manager
  let mockConnMgr = newMockConnectionManager()
  result.registerInstance(ServiceNameConnManager, cast[RootRef](mockConnMgr))

  # Register mock SQL executor
  let mockExecutor = newMockSqlExecutor()
  result.registerInstance(ServiceNameExecutor, cast[RootRef](mockExecutor))

proc createMinimalTestContainer*(): Container =
  ## Create a minimal test container with only essential mock services
  ## Useful for tests that don't need all services
  result = newContainer()

  # Only register the most commonly needed mocks
  let mockTime = newMockTimeProvider(0)
  result.registerInstance(ServiceNameTimeProvider, cast[RootRef](mockTime))

  let mockLogger = newMockLogProvider()
  result.registerInstance(ServiceNameLogger, cast[RootRef](mockLogger))

proc createTestContextWithMocks*(): TestContext =
  ## Create a TestContext with all mock services pre-registered
  ## This is a convenience function for test setup
  result = newTestContext("auto")
  result.container = createTestContainer()

# =============================================================================
# Container Creation - Custom Registration
# =============================================================================

proc createEmptyTestContainer*(): Container =
  ## Create an empty container for custom test configurations
  ## Tests can register their own mocks as needed
  result = newContainer()

proc createTestContainerWithTime*(startTimeNs: int64): Container =
  ## Create test container with time provider at specific start time
  result = newContainer()

  let mockTime = newMockTimeProvider(startTimeNs)
  result.registerInstance(ServiceNameTimeProvider, cast[RootRef](mockTime))

  let mockLogger = newMockLogProvider()
  result.registerInstance(ServiceNameLogger, cast[RootRef](mockLogger))

# =============================================================================
# Mock Access Helpers
# =============================================================================

proc getMockTimeProvider*(c: Container): MockTimeProvider =
  ## Get the mock time provider from container (for test manipulation)
  cast[MockTimeProvider](c.resolveRaw(ServiceNameTimeProvider))

proc getMockLogProvider*(c: Container): MockLogProvider =
  ## Get the mock log provider from container (for test assertions)
  cast[MockLogProvider](c.resolveRaw(ServiceNameLogger))

proc getMockKVStore*(c: Container): MockKVStore =
  ## Get the mock KV store from container (for test data setup)
  cast[MockKVStore](c.resolveRaw(ServiceNameKVStore))

proc getMockBackend*(c: Container): MockBackend =
  ## Get the mock backend from container
  cast[MockBackend](c.resolveRaw(ServiceNameBackend))

proc getMockTransactionManager*(c: Container): MockTransactionManager =
  ## Get the mock transaction manager from container
  cast[MockTransactionManager](c.resolveRaw(ServiceNameTxnManager))

proc getMockConnectionManager*(c: Container): MockConnectionManager =
  ## Get the mock connection manager from container
  cast[MockConnectionManager](c.resolveRaw(ServiceNameConnManager))

proc getMockSqlExecutor*(c: Container): MockSqlExecutor =
  ## Get the mock SQL executor from container
  cast[MockSqlExecutor](c.resolveRaw(ServiceNameExecutor))

# =============================================================================
# Test Setup Helpers
# =============================================================================

proc advanceTime*(c: Container, deltaNs: int64) =
  ## Advance time in mock time provider (convenience for tests)
  let mockTime = getMockTimeProvider(c)
  mockTime.advance(deltaNs)

proc setTime*(c: Container, timeNs: int64) =
  ## Set time in mock time provider (convenience for tests)
  let mockTime = getMockTimeProvider(c)
  mockTime.setTime(timeNs)

proc setKVData*(c: Container, key: string, value: string) =
  ## Set data in mock KV store (convenience for tests)
  let mockKV = getMockKVStore(c)
  discard mockKV.put(key, value)

proc setExecutorResult*(c: Container, sql: string, result: ExecutionResult) =
  ## Set predefined result for SQL executor (convenience for tests)
  let mockExec = getMockSqlExecutor(c)
  mockExec.setResult(sql, result)

proc assertLogged*(c: Container, level: mocks.LogLevel, msg: string) =
  ## Assert that log entry was made (convenience for tests)
  let mockLogger = getMockLogProvider(c)
  mockLogger.assertLogged(level, msg)

proc assertKVGetCalled*(c: Container, times: int) =
  ## Assert KV get was called (convenience for tests)
  let mockKV = getMockKVStore(c)
  mockKV.assertGetCalled(times)

proc assertTxnBeginCalled*(c: Container, times: int) =
  ## Assert transaction begin was called (convenience for tests)
  let mockTxn = getMockTransactionManager(c)
  mockTxn.assertBeginCalled(times)

proc resetAllMocks*(c: Container) =
  ## Reset all mock implementations to initial state
  let mockTime = getMockTimeProvider(c)
  mockTime.reset()

  let mockLogger = getMockLogProvider(c)
  mockLogger.reset()

  let mockKV = getMockKVStore(c)
  mockKV.reset()

  let mockBackend = getMockBackend(c)
  mockBackend.reset()

  let mockTxn = getMockTransactionManager(c)
  mockTxn.reset()

  let mockConnMgr = getMockConnectionManager(c)
  # Connection manager doesn't have reset, just clear connections
  mockConnMgr.closeAll()

  let mockExec = getMockSqlExecutor(c)
  mockExec.reset()

# =============================================================================
# Distributed Layer Mock Container
# =============================================================================

const
  ServiceNameRaftCoordinator* = "raftCoordinator"
  ServiceNameRaftTransport* = "raftTransport"
  ServiceNameRaftStateMachine* = "raftStateMachine"
  ServiceNameRaftLog* = "raftLog"
  ServiceNameSpaceManager* = "spaceManager"
  ServiceNameNetworkTransport* = "networkTransport"

proc createDistributedTestContainer*(): Container =
  ## Create a test container with distributed layer mocks
  ## Useful for testing Raft and distributed components
  result = createTestContainer()

  # Register mock Raft coordinator
  let mockRaftCoord = newMockRaftCoordinator()
  result.registerInstance(ServiceNameRaftCoordinator, cast[RootRef](mockRaftCoord))

  # Register mock Raft transport
  let mockRaftTransport = newMockRaftTransport()
  result.registerInstance(ServiceNameRaftTransport, cast[RootRef](mockRaftTransport))

  # Register mock Raft state machine
  let mockRaftSM = newMockRaftStateMachine()
  result.registerInstance(ServiceNameRaftStateMachine, cast[RootRef](mockRaftSM))

  # Register mock Raft log
  let mockRaftLog = newMockRaftLog()
  result.registerInstance(ServiceNameRaftLog, cast[RootRef](mockRaftLog))

  # Register mock space manager
  let mockSpaceMgr = newMockSpaceManager()
  result.registerInstance(ServiceNameSpaceManager, cast[RootRef](mockSpaceMgr))

  # Register mock network transport
  let mockNetTransport = newMockNetworkTransport()
  result.registerInstance(ServiceNameNetworkTransport, cast[RootRef](mockNetTransport))

proc getMockRaftCoordinator*(c: Container): MockRaftCoordinator =
  cast[MockRaftCoordinator](c.resolveRaw(ServiceNameRaftCoordinator))

proc getMockRaftTransport*(c: Container): MockRaftTransport =
  cast[MockRaftTransport](c.resolveRaw(ServiceNameRaftTransport))

proc getMockRaftStateMachine*(c: Container): MockRaftStateMachine =
  cast[MockRaftStateMachine](c.resolveRaw(ServiceNameRaftStateMachine))

proc getMockRaftLog*(c: Container): MockRaftLog =
  cast[MockRaftLog](c.resolveRaw(ServiceNameRaftLog))

proc getMockSpaceManager*(c: Container): MockSpaceManager =
  cast[MockSpaceManager](c.resolveRaw(ServiceNameSpaceManager))

proc getMockNetworkTransport*(c: Container): MockNetworkTransport =
  cast[MockNetworkTransport](c.resolveRaw(ServiceNameNetworkTransport))

# =============================================================================
# Protocol Layer Mock Container
# =============================================================================

const
  ServiceNameProtocolServer* = "protocolServer"
  ServiceNameProtocolClient* = "protocolClient"

proc createProtocolTestContainer*(): Container =
  ## Create a test container with protocol layer mocks
  ## Useful for testing server/client interactions
  result = createTestContainer()

  # Register mock protocol server
  let mockServer = newMockProtocolServer()
  result.registerInstance(ServiceNameProtocolServer, cast[RootRef](mockServer))

  # Register mock protocol client
  let mockClient = newMockProtocolClient()
  result.registerInstance(ServiceNameProtocolClient, cast[RootRef](mockClient))

proc getMockProtocolServer*(c: Container): MockProtocolServer =
  cast[MockProtocolServer](c.resolveRaw(ServiceNameProtocolServer))

proc getMockProtocolClient*(c: Container): MockProtocolClient =
  cast[MockProtocolClient](c.resolveRaw(ServiceNameProtocolClient))

# =============================================================================
# SQL Layer Mock Container
# =============================================================================

const
  ServiceNameSqlPlanner* = "sqlPlanner"

proc createSqlTestContainer*(): Container =
  ## Create a test container with SQL layer mocks
  ## Useful for testing SQL execution without real client
  result = createTestContainer()

  # Register mock SQL planner
  let mockPlanner = newMockSqlPlanner()
  result.registerInstance(ServiceNameSqlPlanner, cast[RootRef](mockPlanner))

proc getMockSqlPlanner*(c: Container): MockSqlPlanner =
  cast[MockSqlPlanner](c.resolveRaw(ServiceNameSqlPlanner))

# =============================================================================
# Full Stack Test Container
# =============================================================================

proc createFullStackTestContainer*(): Container =
  ## Create a test container with ALL mock implementations
  ## Useful for integration-like tests without real infrastructure
  result = createDistributedTestContainer()

  # Add protocol mocks
  let mockServer = newMockProtocolServer()
  result.registerInstance(ServiceNameProtocolServer, cast[RootRef](mockServer))

  let mockClient = newMockProtocolClient()
  result.registerInstance(ServiceNameProtocolClient, cast[RootRef](mockClient))

  # Add SQL mocks
  let mockPlanner = newMockSqlPlanner()
  result.registerInstance(ServiceNameSqlPlanner, cast[RootRef](mockPlanner))
