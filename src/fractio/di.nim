# Fractio Dependency Injection Module
# Main entry point - exports all DI components

# Container (thread-safe service registry)
import fractio/di/container as diContainer
# Contexts (typed application contexts per layer)
import fractio/di/context as diContext
# Mocks (testing implementations)
import fractio/di/mocks as diMocks
# Adapters (wrap existing implementations)
import fractio/di/adapters as diAdapters

# =============================================================================
# Re-export Container types and procs
# =============================================================================
export diContainer.Container
export diContainer.ServiceLifecycle
export diContainer.ServiceEntry
export diContainer.newContainer
export diContainer.close
export diContainer.registerService
export diContainer.registerSingleton
export diContainer.registerScoped
export diContainer.registerTransient
export diContainer.registerInstance
export diContainer.resolveRaw
export diContainer.resolve
export diContainer.tryResolve
export diContainer.beginScope
export diContainer.endScope
export diContainer.hasService
export diContainer.getServiceNames
export diContainer.getLifecycle
export diContainer.createChildContainer
export diContainer.overrideService
export diContainer.ContainerBuilder
export diContainer.newContainerBuilder
export diContainer.addSingleton
export diContainer.addScoped
export diContainer.addTransient
export diContainer.addInstance
export diContainer.build

# =============================================================================
# Re-export Context types and procs
# =============================================================================
export diContext.LogLevel
export diContext.ServiceNameTimeProvider
export diContext.ServiceNameLogger
export diContext.ServiceNameKVStore
export diContext.ServiceNameBackend
export diContext.ServiceNameTxnManager
export diContext.ServiceNameConnManager
export diContext.ServiceNameExecutor
export diContext.AppConfig
export diContext.newAppConfig
export diContext.AppContext
export diContext.newAppContext
export diContext.resolveTimeProvider
export diContext.resolveLogger
export diContext.resolveKVStore
export diContext.resolveBackend
export diContext.resolveTxnManager
export diContext.resolveConnManager
export diContext.resolveExecutor
export diContext.TestConfig
export diContext.newTestConfig
export diContext.TestContext
export diContext.newTestContext
export diContext.cleanup
export diContext.registerMock
export diContext.registerFactory

# =============================================================================
# Re-export Mock types and procs
# =============================================================================
export diMocks.LogLevel
export diMocks.MockTimeProvider
export diMocks.newMockTimeProvider
export diMocks.nowNs
export diMocks.nowUs
export diMocks.nowMs
export diMocks.advance
export diMocks.setTime
export diMocks.reset
export diMocks.close
export diMocks.assertCalled
export diMocks.assertTimeEquals
export diMocks.LogEntry
export diMocks.MockLogProvider
export diMocks.newMockLogProvider
export diMocks.log
export diMocks.debug
export diMocks.info
export diMocks.warn
export diMocks.error
export diMocks.setMinLevel
export diMocks.shouldLog
export diMocks.assertLogged
export diMocks.assertLoggedCount
export diMocks.assertNoErrors
export diMocks.getEntries
export diMocks.getErrorEntries
export diMocks.KVStoreOperation
export diMocks.KVStoreCall
export diMocks.MockKVStore
export diMocks.newMockKVStore
export diMocks.get
export diMocks.put
export diMocks.delete
export diMocks.scan
export diMocks.exists
export diMocks.setForceError
export diMocks.assertGetCalled
export diMocks.assertPutCalled
export diMocks.assertKeyExists
export diMocks.assertKeyNotExists
export diMocks.assertKeyValue
export diMocks.assertClosed
export diMocks.MockTransaction
export diMocks.MockTransactionManager
export diMocks.newMockTransactionManager
export diMocks.begin
export diMocks.commit
export diMocks.rollback
export diMocks.getStatus
export diMocks.getActiveCount
export diMocks.getOldestSnapshot
export diMocks.assertBeginCalled
export diMocks.assertCommitCalled
export diMocks.assertActiveCount
export diMocks.assertTxnStatus
export diMocks.MockBackend
export diMocks.newMockBackend
export diMocks.flush
export diMocks.compact
export diMocks.stats
export diMocks.MockConnectionHandle
export diMocks.newMockConnectionHandle
export diMocks.send
export diMocks.recv
export diMocks.isConnected
export diMocks.remoteAddress
export diMocks.queueResponse
export diMocks.getSentData
export diMocks.MockConnectionManager
export diMocks.newMockConnectionManager
export diMocks.acquire
export diMocks.release
export diMocks.poolSize
export diMocks.activeCount
export diMocks.ExecutionResultKind
export diMocks.ExecutionResult
export diMocks.MockSqlExecutor
export diMocks.newMockSqlExecutor
export diMocks.execute
export diMocks.executeInTxn
export diMocks.setResult
export diMocks.setDefaultResult

# =============================================================================
# Re-export Adapter types and procs
# =============================================================================
export diAdapters.LogLevelDI
export diAdapters.SystemTimeProvider
export diAdapters.newSystemTimeProvider
export diAdapters.NullLogger
export diAdapters.newNullLogger
export diAdapters.ConsoleLogger
export diAdapters.newConsoleLogger
export diAdapters.InMemoryKVStore
export diAdapters.newInMemoryKVStore
export diAdapters.InMemoryBackend
export diAdapters.newInMemoryBackend
export diAdapters.LoggerAdapter
export diAdapters.newLoggerAdapter
export diAdapters.wrapLogger
export diAdapters.wrapGlobalLogger
export diAdapters.defaultTimeProvider
export diAdapters.defaultLogger
export diAdapters.nullLogger
export diAdapters.consoleLogger
export diAdapters.memoryKVStore
export diAdapters.memoryBackend

# =============================================================================
# Module Overview
# =============================================================================
#
# The DI module provides:
#
# Container:
#   - Container: Thread-safe service registry
#   - ContainerBuilder: Fluent builder API
#   - ServiceLifecycle: Singleton/Scoped/Transient
#   - registerSingleton, registerScoped, registerTransient
#   - resolve[T]: Type-safe service resolution
#
# Contexts:
#   - AppConfig: Application configuration
#   - AppContext: Root context with DI container
#   - TestContext: Testing helper with mock setup
#
# Mocks:
#   - MockTimeProvider: Deterministic time for tests
#   - MockLogProvider: Log capturing for tests
#   - MockKVStore: In-memory KV for tests
#   - MockTransactionManager: Transaction mock
#   - MockBackend: Storage mock
#   - MockConnectionManager: Network mock
#   - MockSqlExecutor: SQL mock
#
# Adapters:
#   - SystemTimeProvider: Real system time
#   - NullLogger: Discard all logs
#   - ConsoleLogger: Simple stdout logger
#   - InMemoryKVStore: Simple KV for tests
#   - InMemoryBackend: Simple backend for tests
#   - LoggerAdapter: Wrap existing Logger

# =============================================================================
# Quick Start Example
# =============================================================================
#
# Production setup:
#   let config = newAppConfig(nodeId = "node1")
#   let container = newContainerBuilder()
#     .addSingleton("timeProvider", proc(): SystemTimeProvider = newSystemTimeProvider())
#     .addSingleton("logger", proc(): LoggerAdapter = wrapGlobalLogger())
#     .addSingleton("backend", proc(): InMemoryBackend = newInMemoryBackend())
#     .build()
#
#   let ctx = newAppContext(config, container)
#
# Test setup:
#   let mockTime = newMockTimeProvider()
#   let mockLogger = newMockLogProvider()
#   let mockKV = newMockKVStore()
#
#   let container = newContainerBuilder()
#     .addInstance("timeProvider", mockTime)
#     .addInstance("logger", mockLogger)
#     .addInstance("kvStore", mockKV)
#     .build()
#
#   let testCtx = newTestContext("myTest")
#   testCtx.registerMock("timeProvider", mockTime)
#   testCtx.registerMock("logger", mockLogger)
#   mockTime.advance(1000)  # Control time in tests
