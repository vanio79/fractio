# Application Contexts for Fractio DI
# Context provides convenient access to resolved services from container

import std/[options, tables]
import fractio/di/container

# =============================================================================
# Log Level (must be defined here for use in contexts)
# =============================================================================

type
  LogLevel* = enum
    llDebug, llInfo, llWarn, llError

# =============================================================================
# Service Names (constants for consistent naming)
# =============================================================================

const
  ServiceNameTimeProvider* = "timeProvider"
  ServiceNameLogger* = "logger"
  ServiceNameKVStore* = "kvStore"
  ServiceNameBackend* = "backend"
  ServiceNameTxnManager* = "txnManager"
  ServiceNameConnManager* = "connManager"
  ServiceNameExecutor* = "executor"

# =============================================================================
# AppConfig
# =============================================================================

type
  AppConfig* = ref object
    ## Application configuration
    nodeId*: string
    listenPort*: uint16
    advertiseAddr*: string
    dataDir*: string
    raftEnabled*: bool
    raftPeers*: seq[string]
    logLevel*: LogLevel
    metricsEnabled*: bool
    metricsPort*: uint16

proc newAppConfig*(nodeId: string = "node1",
                   listenPort: uint16 = 9000,
                   dataDir: string = "./data"): AppConfig =
  ## Create default app config
  result = AppConfig(
    nodeId: nodeId,
    listenPort: listenPort,
    advertiseAddr: "localhost",
    dataDir: dataDir,
    raftEnabled: true,
    raftPeers: @[],
    logLevel: llInfo,
    metricsEnabled: false,
    metricsPort: 9090
  )

# =============================================================================
# AppContext - Root Context
# =============================================================================

type
  AppContext* = ref object
    ## Root application context - holds DI container reference
    ## Services are resolved on demand from container
    config*: AppConfig
    container*: Container

proc newAppContext*(config: AppConfig, container: Container): AppContext =
  ## Create application context with config and container
  result = AppContext(
    config: config,
    container: container
  )

proc close*(ctx: AppContext) =
  ## Clean up application context
  if ctx.container != nil:
    ctx.container.close()

# =============================================================================
# Generic Service Resolution Helpers
# =============================================================================

proc resolveTimeProvider*(ctx: AppContext): RootRef =
  ## Resolve time provider from container
  ctx.container.resolveRaw(ServiceNameTimeProvider)

proc resolveLogger*(ctx: AppContext): RootRef =
  ## Resolve logger from container
  ctx.container.resolveRaw(ServiceNameLogger)

proc resolveKVStore*(ctx: AppContext): RootRef =
  ## Resolve KV store from container
  ctx.container.resolveRaw(ServiceNameKVStore)

proc resolveBackend*(ctx: AppContext): RootRef =
  ## Resolve backend from container
  ctx.container.resolveRaw(ServiceNameBackend)

proc resolveTxnManager*(ctx: AppContext): RootRef =
  ## Resolve transaction manager from container
  ctx.container.resolveRaw(ServiceNameTxnManager)

proc resolveConnManager*(ctx: AppContext): RootRef =
  ## Resolve connection manager from container
  ctx.container.resolveRaw(ServiceNameConnManager)

proc resolveExecutor*(ctx: AppContext): RootRef =
  ## Resolve SQL executor from container
  ctx.container.resolveRaw(ServiceNameExecutor)

# =============================================================================
# Type-safe Resolution
# =============================================================================

proc resolve*[T](ctx: AppContext, serviceName: string): T =
  ## Resolve a typed service from the context's container
  ctx.container.resolve[T](serviceName)

proc tryResolve*[T](ctx: AppContext, serviceName: string): Option[T] =
  ## Try to resolve a typed service
  ctx.container.tryResolve[T](serviceName)

# =============================================================================
# TestContext - Testing Helper
# =============================================================================

type
  TestConfig* = ref object
    ## Test-specific configuration
    name*: string
    tempDir*: string
    mockTime*: bool
    mockStorage*: bool
    mockNetwork*: bool

proc newTestConfig*(name: string = "test"): TestConfig =
  ## Create test config
  result = TestConfig(
    name: name,
    tempDir: "",
    mockTime: true,
    mockStorage: true,
    mockNetwork: true
  )

type
  TestContext* = ref object
    ## Test context with pre-configured mocks
    ## Services are registered with mock implementations
    config*: TestConfig
    container*: Container

proc newTestContext*(name: string = "test"): TestContext =
  ## Create a test context with empty container
  ## Mocks will be registered by test setup
  result = TestContext(
    config: newTestConfig(name),
    container: newContainer()
  )

proc cleanup*(tc: TestContext) =
  ## Clean up test context resources
  if tc.container != nil:
    tc.container.close()

proc resolve*[T](tc: TestContext, serviceName: string): T =
  ## Resolve a typed service from test context
  tc.container.resolve[T](serviceName)

proc tryResolve*[T](tc: TestContext, serviceName: string): Option[T] =
  ## Try to resolve a typed service
  tc.container.tryResolve[T](serviceName)

# =============================================================================
# Scope Helpers
# =============================================================================

proc beginScope*(ctx: AppContext, scopeId: string) =
  ## Begin a new scope in the container
  ctx.container.beginScope(scopeId)

proc endScope*(ctx: AppContext) =
  ## End current scope
  ctx.container.endScope()

proc beginScope*(tc: TestContext, scopeId: string) =
  ## Begin a new scope in test container
  tc.container.beginScope(scopeId)

proc endScope*(tc: TestContext) =
  ## End current scope in test container
  tc.container.endScope()

# =============================================================================
# Service Registration Helpers (for test setup)
# =============================================================================

proc registerMock*[T](tc: TestContext, name: string, mock: T) =
  ## Register a mock instance in test container
  when T is ref:
    tc.container.registerInstance(name, cast[RootRef](mock))
  else:
    {.error: "registerMock requires a ref type".}

proc registerFactory*[T](tc: TestContext,
                         name: string,
                         factory: proc(c: Container): T {.gcsafe.},
                         lifecycle: ServiceLifecycle = slSingleton) =
  ## Register a factory in test container
  tc.container.registerService(name, lifecycle,
    proc(c: Container): RootRef {.gcsafe.} = cast[RootRef](factory(c)))
