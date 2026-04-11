# Unit tests for Fractio DI Context

import std/[unittest, options, tables]
import fractio/di/container
import fractio/di/context
import fractio/di/mocks
import fractio/di/adapters
import fractio/di/interfaces

suite "LogLevel in context":
  test "LogLevel enum matches interfaces":
    check ord(context.LogLevel.llDebug) == 0
    check ord(context.LogLevel.llInfo) == 1
    check ord(context.LogLevel.llWarn) == 2
    check ord(context.LogLevel.llError) == 3

suite "Service name constants":
  test "all service names defined":
    check ServiceNameTimeProvider == "timeProvider"
    check ServiceNameLogger == "logger"
    check ServiceNameKVStore == "kvStore"
    check ServiceNameBackend == "backend"
    check ServiceNameTxnManager == "txnManager"
    check ServiceNameConnManager == "connManager"
    check ServiceNameExecutor == "executor"

suite "AppConfig":
  test "newAppConfig with defaults":
    let config = newAppConfig()

    check config.nodeId == "node1"
    check config.listenPort == 9000
    check config.advertiseAddr == "localhost"
    check config.dataDir == "./data"
    check config.raftEnabled == true
    check config.raftPeers.len == 0
    check config.logLevel == context.LogLevel.llInfo
    check config.metricsEnabled == false
    check config.metricsPort == 9090

  test "newAppConfig with custom nodeId":
    let config = newAppConfig(nodeId = "customNode")

    check config.nodeId == "customNode"
    check config.listenPort == 9000

  test "newAppConfig with custom port":
    let config = newAppConfig(listenPort = 8080'u16)

    check config.listenPort == 8080

  test "newAppConfig with custom dataDir":
    let config = newAppConfig(dataDir = "/var/data")

    check config.dataDir == "/var/data"

  test "AppConfig is mutable":
    var config = newAppConfig()
    config.logLevel = context.LogLevel.llDebug
    config.metricsEnabled = true

    check config.logLevel == context.LogLevel.llDebug
    check config.metricsEnabled == true

  test "AppConfig raftPeers can be set":
    var config = newAppConfig()
    config.raftPeers = @["peer1:9000", "peer2:9000"]

    check config.raftPeers.len == 2
    check config.raftPeers[0] == "peer1:9000"

suite "AppContext":
  test "newAppContext creates with config and container":
    let config = newAppConfig()
    let container = newContainer()
    let ctx = newAppContext(config, container)

    check ctx.config == config
    check ctx.container == container

  test "close releases container":
    let config = newAppConfig()
    let container = newContainer()
    let ctx = newAppContext(config, container)

    ctx.close()

  test "close handles nil container":
    let config = newAppConfig()
    let ctx = AppContext(config: config, container: nil)

    ctx.close()

  test "resolveTimeProvider returns RootRef":
    let config = newAppConfig()
    let container = newContainer()
    let mockTime = newMockTimeProvider(1000)
    container.registerInstance(ServiceNameTimeProvider, mockTime)

    let ctx = newAppContext(config, container)
    let resolved = ctx.resolveTimeProvider()

    check resolved != nil
    ctx.close()
    mockTime.close()

  test "resolveLogger returns RootRef":
    let config = newAppConfig()
    let container = newContainer()
    let mockLogger = newMockLogProvider()
    container.registerInstance(ServiceNameLogger, mockLogger)

    let ctx = newAppContext(config, container)
    let resolved = ctx.resolveLogger()

    check resolved != nil
    ctx.close()
    mockLogger.close()

  test "resolveKVStore returns RootRef":
    let config = newAppConfig()
    let container = newContainer()
    let mockKV = newMockKVStore()
    container.registerInstance(ServiceNameKVStore, mockKV)

    let ctx = newAppContext(config, container)
    let resolved = ctx.resolveKVStore()

    check resolved != nil
    ctx.close()
    mockKV.close()

  test "resolveBackend returns RootRef":
    let config = newAppConfig()
    let container = newContainer()
    let mockBackend = newMockBackend()
    container.registerInstance(ServiceNameBackend, mockBackend)

    let ctx = newAppContext(config, container)
    let resolved = ctx.resolveBackend()

    check resolved != nil
    ctx.close()

  test "resolveTxnManager returns RootRef":
    let config = newAppConfig()
    let container = newContainer()
    let mockTxn = newMockTransactionManager()
    container.registerInstance(ServiceNameTxnManager, mockTxn)

    let ctx = newAppContext(config, container)
    let resolved = ctx.resolveTxnManager()

    check resolved != nil
    ctx.close()

  test "resolveConnManager returns RootRef":
    let config = newAppConfig()
    let container = newContainer()
    let mockConn = newMockConnectionManager()
    container.registerInstance(ServiceNameConnManager, mockConn)

    let ctx = newAppContext(config, container)
    let resolved = ctx.resolveConnManager()

    check resolved != nil
    ctx.close()

  test "resolveExecutor returns RootRef":
    let config = newAppConfig()
    let container = newContainer()
    let mockExecutor = newMockSqlExecutor()
    container.registerInstance(ServiceNameExecutor, mockExecutor)

    let ctx = newAppContext(config, container)
    let resolved = ctx.resolveExecutor()

    check resolved != nil
    ctx.close()

suite "Type-safe resolution":
  test "resolve[T] returns typed service from AppContext":
    let config = newAppConfig()
    let container = newContainer()
    let mockTime = newMockTimeProvider(5000)
    container.registerInstance("timeProvider", mockTime)

    let ctx = newAppContext(config, container)
    let resolved = resolve[MockTimeProvider](ctx.container, "timeProvider")

    check resolved.nowNs() == 5000
    ctx.close()
    mockTime.close()

  test "tryResolve[T] returns some for registered":
    let config = newAppConfig()
    let container = newContainer()
    let mockLogger = newMockLogProvider()
    container.registerInstance("logger", mockLogger)

    let ctx = newAppContext(config, container)
    let result = tryResolve[MockLogProvider](ctx.container, "logger")

    check result.isSome
    ctx.close()
    mockLogger.close()

  test "tryResolve[T] returns none for missing":
    let config = newAppConfig()
    let container = newContainer()
    let ctx = newAppContext(config, container)

    let result = tryResolve[MockTimeProvider](ctx.container, "missing")

    check result.isNone
    ctx.close()

  test "resolve[T] for multiple types":
    let config = newAppConfig()
    let container = newContainer()
    let mockKV = newMockKVStore()
    let mockBackend = newMockBackend()
    container.registerInstance("kvStore", mockKV)
    container.registerInstance("backend", mockBackend)

    let ctx = newAppContext(config, container)
    let kv = resolve[MockKVStore](ctx.container, "kvStore")
    let backend = resolve[MockBackend](ctx.container, "backend")

    check kv != nil
    check backend != nil
    ctx.close()
    mockKV.close()

suite "TestConfig":
  test "newTestConfig with defaults":
    let config = newTestConfig()

    check config.name == "test"
    check config.tempDir == ""
    check config.mockTime == true
    check config.mockStorage == true
    check config.mockNetwork == true

  test "newTestConfig with custom name":
    let config = newTestConfig("myTest")

    check config.name == "myTest"

  test "TestConfig can be modified":
    var config = newTestConfig()
    config.tempDir = "/tmp/test"
    config.mockTime = false

    check config.tempDir == "/tmp/test"
    check config.mockTime == false

suite "TestContext":
  test "newTestContext creates empty container":
    let tc = newTestContext()

    check tc.config.name == "test"
    check tc.container != nil
    check tc.container.services.len == 0

    tc.cleanup()

  test "newTestContext with custom name":
    let tc = newTestContext("integrationTest")

    check tc.config.name == "integrationTest"

    tc.cleanup()

  test "cleanup releases container":
    let tc = newTestContext()
    tc.cleanup()

  test "cleanup handles nil container":
    let tc = TestContext(config: newTestConfig(), container: nil)
    tc.cleanup()

  test "resolve[T] from test context":
    let tc = newTestContext()
    let mockTime = newMockTimeProvider(10000)
    tc.container.registerInstance("timeProvider", mockTime)

    let resolved = resolve[MockTimeProvider](tc.container, "timeProvider")
    check resolved.nowNs() == 10000

    tc.cleanup()
    mockTime.close()

  test "tryResolve[T] from test context":
    let tc = newTestContext()
    let mockKV = newMockKVStore()
    tc.container.registerInstance("kvStore", mockKV)

    let result = tryResolve[MockKVStore](tc.container, "kvStore")
    check result.isSome

    let missing = tryResolve[MockTimeProvider](tc.container, "missing")
    check missing.isNone

    tc.cleanup()
    mockKV.close()

suite "Scope helpers":
  test "beginScope on AppContext":
    let config = newAppConfig()
    let container = newContainer()
    let ctx = newAppContext(config, container)

    ctx.beginScope("requestScope")
    check ctx.container.currentScope == "requestScope"

    ctx.endScope()
    check ctx.container.currentScope == ""

    ctx.close()

  test "beginScope on TestContext":
    let tc = newTestContext()

    tc.beginScope("testScope")
    check tc.container.currentScope == "testScope"

    tc.endScope()
    check tc.container.currentScope == ""

    tc.cleanup()

  test "multiple scopes sequentially":
    let tc = newTestContext()

    tc.beginScope("scope1")
    check tc.container.currentScope == "scope1"

    tc.endScope()
    tc.beginScope("scope2")
    check tc.container.currentScope == "scope2"

    tc.endScope()
    tc.cleanup()

  test "scoped service resolution":
    let tc = newTestContext()

    tc.container.registerScoped("scopedTime", proc(): RootRef {.gcsafe.} = cast[
        RootRef](newMockTimeProvider(100)))

    tc.beginScope("scopeA")
    let t1 = resolve[MockTimeProvider](tc.container, "scopedTime")
    let t2 = resolve[MockTimeProvider](tc.container, "scopedTime")
    check t1 == t2

    tc.endScope()
    tc.beginScope("scopeB")
    let t3 = resolve[MockTimeProvider](tc.container, "scopedTime")
    check t3 != t1

    tc.endScope()
    tc.cleanup()
    t1.close()
    t3.close()

suite "Service registration helpers":
  test "registerMock with MockTimeProvider":
    let tc = newTestContext()
    let mockTime = newMockTimeProvider(5000)

    tc.registerMock("timeProvider", mockTime)

    check tc.container.hasService("timeProvider")
    let resolved = resolve[MockTimeProvider](tc.container, "timeProvider")
    check resolved.nowNs() == 5000

    tc.cleanup()
    mockTime.close()

  test "registerMock with MockLogProvider":
    let tc = newTestContext()
    let mockLogger = newMockLogProvider()

    tc.registerMock("logger", mockLogger)

    check tc.container.hasService("logger")
    let resolved = resolve[MockLogProvider](tc.container, "logger")
    check resolved != nil

    tc.cleanup()
    mockLogger.close()

  test "registerMock with MockKVStore":
    let tc = newTestContext()
    let mockKV = newMockKVStore()

    tc.registerMock("kvStore", mockKV)

    check tc.container.hasService("kvStore")
    let resolved = resolve[MockKVStore](tc.container, "kvStore")
    discard resolved.put("test", "value")
    check resolved.get("test").isSome

    tc.cleanup()
    mockKV.close()

  test "registerFactory for singleton":
    let tc = newTestContext()

    tc.container.registerSingleton("timeProvider",
      proc(): RootRef {.gcsafe.} = cast[RootRef](newMockTimeProvider(1000)))

    let t1 = resolve[MockTimeProvider](tc.container, "timeProvider")
    let t2 = resolve[MockTimeProvider](tc.container, "timeProvider")
    check t1 == t2
    check t1.nowNs() == 1000

    tc.cleanup()
    t1.close()

  test "registerFactory for transient":
    let tc = newTestContext()

    tc.container.registerTransient("timeProvider",
      proc(): RootRef {.gcsafe.} = cast[RootRef](newMockTimeProvider(0)))

    let t1 = resolve[MockTimeProvider](tc.container, "timeProvider")
    let t2 = resolve[MockTimeProvider](tc.container, "timeProvider")
    check t1 != t2

    tc.cleanup()
    t1.close()
    t2.close()

  test "registerFactory for scoped":
    let tc = newTestContext()

    tc.container.registerScoped("scopedService",
      proc(): RootRef {.gcsafe.} = cast[RootRef](newMockTimeProvider(0)))

    tc.beginScope("scope1")
    let s1a = resolve[MockTimeProvider](tc.container, "scopedService")
    let s1b = resolve[MockTimeProvider](tc.container, "scopedService")
    check s1a == s1b

    tc.endScope()
    tc.beginScope("scope2")
    let s2 = resolve[MockTimeProvider](tc.container, "scopedService")
    check s2 != s1a

    tc.endScope()
    tc.cleanup()
    s1a.close()
    s2.close()

suite "Integration tests":
  test "full AppContext workflow":
    let config = newAppConfig(nodeId = "node1", listenPort = 9000'u16)
    let container = newContainer()

    container.registerInstance(ServiceNameTimeProvider, newMockTimeProvider(1000000))
    container.registerInstance(ServiceNameLogger, newMockLogProvider())
    container.registerInstance(ServiceNameKVStore, newMockKVStore())

    let ctx = newAppContext(config, container)

    check ctx.config.nodeId == "node1"
    check ctx.resolveTimeProvider() != nil
    check ctx.resolveLogger() != nil
    check ctx.resolveKVStore() != nil

    ctx.close()

  test "full TestContext workflow":
    let tc = newTestContext("integration")

    tc.registerMock("timeProvider", newMockTimeProvider(0))
    tc.registerMock("kvStore", newMockKVStore())
    tc.registerMock("backend", newMockBackend())

    check resolve[MockTimeProvider](tc.container, "timeProvider") != nil
    check resolve[MockKVStore](tc.container, "kvStore") != nil
    check resolve[MockBackend](tc.container, "backend") != nil

    tc.cleanup()

  test "AppContext with ContainerBuilder":
    let config = newAppConfig()
    let container = newContainerBuilder()
      .addSingleton("timeProvider", proc(): MockTimeProvider = newMockTimeProvider(5000))
      .addSingleton("logger", proc(): MockLogProvider = newMockLogProvider())
      .build()

    let ctx = newAppContext(config, container)

    let time = resolve[MockTimeProvider](ctx.container, "timeProvider")
    check time.nowNs() == 5000

    ctx.close()
    time.close()

  test "TestContext with real adapters":
    let tc = newTestContext()

    tc.registerMock("logger", nullLogger())
    tc.registerMock("kvStore", memoryKVStore())

    let logger = resolve[NullLogger](tc.container, "logger")
    let kvStore = resolve[InMemoryKVStore](tc.container, "kvStore")

    logger.info("test message")
    discard kvStore.put("key", "value")
    check kvStore.get("key").isSome

    tc.cleanup()
    kvStore.close()

suite "Error conditions":
  test "resolve raises KeyError for missing service in AppContext":
    let config = newAppConfig()
    let container = newContainer()
    let ctx = newAppContext(config, container)

    var caught = false
    try:
      discard resolve[MockTimeProvider](ctx.container, "nonexistent")
    except KeyError:
      caught = true

    check caught
    ctx.close()

  test "resolve raises KeyError for missing service in TestContext":
    let tc = newTestContext()

    var caught = false
    try:
      discard resolve[MockKVStore](tc.container, "nonexistent")
    except KeyError:
      caught = true

    check caught
    tc.cleanup()

  test "resolveRaw raises KeyError for missing":
    let config = newAppConfig()
    let container = newContainer()
    let ctx = newAppContext(config, container)

    var caught = false
    try:
      discard ctx.container.resolveRaw("missing")
    except KeyError:
      caught = true

    check caught
    ctx.close()

  test "endScope without beginScope is safe":
    let tc = newTestContext()
    tc.endScope()
    check tc.container.currentScope == ""
    tc.cleanup()

  test "multiple endScope calls are safe":
    let tc = newTestContext()
    tc.beginScope("scope1")
    tc.endScope()
    tc.endScope()
    check tc.container.currentScope == ""
    tc.cleanup()

suite "Context with hierarchical containers":
  test "AppContext with child container":
    let parentConfig = newAppConfig(nodeId = "parent")
    let parentContainer = newContainer()
    parentContainer.registerInstance("sharedLogger", newMockLogProvider())

    let childConfig = newAppConfig(nodeId = "child")
    let childContainer = parentContainer.createChildContainer()

    let parentCtx = newAppContext(parentConfig, parentContainer)
    let childCtx = newAppContext(childConfig, childContainer)

    check resolve[MockLogProvider](childCtx.container, "sharedLogger") != nil

    parentCtx.close()
    childCtx.close()

  test "TestContext can override parent service":
    let parent = newContainer()
    parent.registerInstance("timeProvider", newMockTimeProvider(100))

    let tc = newTestContext()
    tc.container.parent = parent

    check resolve[MockTimeProvider](tc.container, "timeProvider").nowNs() == 100

    tc.registerMock("timeProvider", newMockTimeProvider(200))
    check resolve[MockTimeProvider](tc.container, "timeProvider").nowNs() == 200

    tc.cleanup()
    parent.close()

suite "AppConfig edge cases":
  test "AppConfig with all custom values":
    var config = newAppConfig(
      nodeId = "custom",
      listenPort = 5000'u16,
      dataDir = "/custom/path"
    )
    config.advertiseAddr = "192.168.1.100"
    config.raftEnabled = false
    config.raftPeers = @["peer1", "peer2", "peer3"]
    config.logLevel = context.LogLevel.llDebug
    config.metricsEnabled = true
    config.metricsPort = 9091'u16

    check config.nodeId == "custom"
    check config.listenPort == 5000
    check config.dataDir == "/custom/path"
    check config.advertiseAddr == "192.168.1.100"
    check config.raftEnabled == false
    check config.raftPeers.len == 3
    check config.logLevel == context.LogLevel.llDebug
    check config.metricsEnabled == true
    check config.metricsPort == 9091

  test "AppConfig empty raftPeers":
    let config = newAppConfig()
    check config.raftPeers.len == 0
