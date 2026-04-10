# Production Bootstrap for Fractio
# Creates DI containers with real implementations for production use

import std/[os, tables]
import fractio/di/container
import fractio/di/interfaces
import fractio/di/adapters
import fractio/di/context
import fractio/core/types
import fractio/core/timestamp_provider
import fractio/storage/backend
import fractio/storage/wisckey_backend
import fractio/utils/logging as fractioLogging
import fractio/cli/config
import fractio/distributed/sharedtimer/wallclock

export container, context, types

# =============================================================================
# Production Configuration
# =============================================================================

type
  ProductionConfig* = object
    ## Production configuration for DI container
    nodeId*: uint16
    dataDir*: string
    logLevel*: fractioLogging.LogLevel
    raftEnabled*: bool
    raftPort*: uint16
    clientPort*: uint16
    webPort*: uint16
    host*: string
    joinAddr*: string      # Address of existing node to join cluster
    timeSyncEnabled*: bool # Enable P2P time synchronization
    timeSyncPort*: uint16  # Port for time sync UDP

  ServiceLifecycle* = interfaces.ServiceLifecycle

# =============================================================================
# Default Configuration
# =============================================================================

proc defaultProductionConfig*(nodeId: uint16 = 1,
                              dataDir: string = "/var/lib/fractio"): ProductionConfig =
  ## Create default production configuration
  result = ProductionConfig(
    nodeId: nodeId,
    dataDir: dataDir,
    logLevel: fractioLogging.llInfo,
    raftEnabled: true,
    raftPort: 8300,
    clientPort: 9000,
    webPort: 9876,
    host: "127.0.0.1",
    joinAddr: "",
    timeSyncEnabled: true,
    timeSyncPort: 8400
  )

proc productionConfigFromFractioConfig*(fc: FractioConfig): ProductionConfig =
  ## Convert FractioConfig (TOML loaded) to ProductionConfig
  result = ProductionConfig(
    nodeId: fc.nodeId.uint16,
    dataDir: fc.dataDir,
    logLevel: fractioLogging.llInfo,
    raftEnabled: true,
    raftPort: fc.raftPort.uint16,
    clientPort: fc.clientPort.uint16,
    webPort: fc.webPort.uint16,
    host: fc.host,
    joinAddr: "",
    timeSyncEnabled: true,
    timeSyncPort: fc.raftPort.uint16 + 100 # Time sync port offset from raft
  )

# =============================================================================
# Null Adapters (Placeholder for Production)
# =============================================================================

type
  NullKVAdapter* = ref object of RootObj
  NullTxnAdapter* = ref object of RootObj
  NullConnAdapter* = ref object of RootObj
  NullExecAdapter* = ref object of RootObj
  NullBackendAdapter* = ref object of RootObj

proc newNullKVAdapter*(): NullKVAdapter = NullKVAdapter()
proc newNullTxnAdapter*(): NullTxnAdapter = NullTxnAdapter()
proc newNullConnAdapter*(): NullConnAdapter = NullConnAdapter()
proc newNullExecAdapter*(): NullExecAdapter = NullExecAdapter()
proc newNullBackendAdapter*(): NullBackendAdapter = NullBackendAdapter()

# =============================================================================
# Production Container Factory
# =============================================================================

proc createProductionContainer*(config: ProductionConfig): Container =
  ## Create DI container with all real production components
  ## This is the main entry point for starting a Fractio node
  result = newContainer()

  # 1. Register Time Provider (system time)
  let systemTime = newSystemTimeProvider()
  result.registerInstance(ServiceNameTimeProvider, cast[RootRef](systemTime))

  # 2. Register Logger (production logger with config level)
  let logger = newLogger("fractio", config.logLevel)
  let loggerAdapter = newLoggerAdapter(logger)
  result.registerInstance(ServiceNameLogger, cast[RootRef](loggerAdapter))

# 3. Register Storage Backend (WiscKey)
  var storageConfig = defaultStorageConfig(config.dataDir / "storage")
  storageConfig.writeBufferSize = 4 * 1024 * 1024 # 4 MB
  storageConfig.blockCacheSize = 8 * 1024 * 1024 # 8 MB
  let backend = newWiscKeyBackend(storageConfig)
  if not backend.open(storageConfig):
    raise newException(IOError, "Failed to open storage backend at " &
        config.dataDir)
  # Note: WiscKeyBackend doesn't fit the mock interface directly
  # We'll need to create an adapter for production use
  # For now, register as a service that can be retrieved
  result.registerInstance("storageBackend", cast[RootRef](backend))

# 4. Timestamp Provider (HLC using system time)
  # Note: TimestampProvider is not RootRef-compatible, store in ProductionContext

  # 5. Register placeholder services (not yet fully integrated)
  # These will be replaced with real implementations as integration progresses

  # KV Store placeholder - will use real KV store via backend
  let nullKVAdapter = newNullKVAdapter()
  result.registerInstance(ServiceNameKVStore, cast[RootRef](nullKVAdapter))

  # Transaction Manager placeholder - will use real TransactionManager
  let nullTxnAdapter = newNullTxnAdapter()
  result.registerInstance(ServiceNameTxnManager, cast[RootRef](nullTxnAdapter))

  # Connection Manager placeholder - will use real connection pool
  let nullConnAdapter = newNullConnAdapter()
  result.registerInstance(ServiceNameConnManager, cast[RootRef](nullConnAdapter))

  # SQL Executor placeholder - will use real executor
  let nullExecAdapter = newNullExecAdapter()
  result.registerInstance(ServiceNameExecutor, cast[RootRef](nullExecAdapter))

  # Backend placeholder for mock interface compatibility
  let nullBackendAdapter = newNullBackendAdapter()
  result.registerInstance(ServiceNameBackend, cast[RootRef](nullBackendAdapter))

# =============================================================================
# Production Context
# =============================================================================

type
  ProductionContext* = ref object
    ## Production context with DI container and configuration
    config*: ProductionConfig
    container*: Container
    backend*: WiscKeyBackend
    logger*: Logger
    timestampProvider*: TimestampProvider

proc newProductionContext*(config: ProductionConfig): ProductionContext =
  ## Create production context with all components initialized
  result = ProductionContext(
    config: config,
    container: createProductionContainer(config)
  )

  # Get references to key components
  result.backend = cast[WiscKeyBackend](result.container.resolveRaw("storageBackend"))
  let loggerAdapter = cast[LoggerAdapter](result.container.resolveRaw(ServiceNameLogger))
  result.logger = loggerAdapter.wrapped

  # Create timestamp provider separately (not stored in container)
  let wallClockProvider = new WallClockTimeProvider
  result.timestampProvider = newTimestampProvider(wallClockProvider, config.nodeId)

proc close*(ctx: ProductionContext) =
  ## Clean up production context
  if ctx.backend != nil:
    ctx.backend.close()
  if ctx.container != nil:
    ctx.container.close()

# =============================================================================
# Production Service Resolution
# =============================================================================

proc getBackend*(ctx: ProductionContext): WiscKeyBackend =
  ## Get storage backend from context
  ctx.backend

proc getLogger*(ctx: ProductionContext): Logger =
  ## Get logger from context
  ctx.logger

proc getTimestampProvider*(ctx: ProductionContext): TimestampProvider =
  ## Get timestamp provider from context
  ctx.timestampProvider

proc getSystemTime*(ctx: ProductionContext): SystemTimeProvider =
  ## Get system time provider from context
  cast[SystemTimeProvider](ctx.container.resolveRaw(ServiceNameTimeProvider))

# =============================================================================
# Service Status Helpers
# =============================================================================

proc isBackendOpen*(ctx: ProductionContext): bool =
  ## Check if backend is open
  ctx.backend != nil and ctx.backend.isOpen

proc getBackendStats*(ctx: ProductionContext): tables.Table[string, int64] =
  ## Get backend statistics
  # TODO: Implement stats for WiscKeyBackend using getProperty
  # For now, return empty table
  if ctx.backend != nil:
    initTable[string, int64]()
  else:
    initTable[string, int64]()

# =============================================================================
# Container Lifecycle
# =============================================================================

proc startServices*(ctx: ProductionContext) =
  ## Start all services in production context
  # This is a placeholder - actual service startup (Raft, etc.) happens elsewhere
  ctx.logger.log(fractioLogging.llInfo,
    fmt"Production services starting for node {ctx.config.nodeId}", initTable[
        string, string]())

proc stopServices*(ctx: ProductionContext) =
  ## Stop all services gracefully
  ctx.logger.log(fractioLogging.llInfo,
    fmt"Production services stopping for node {ctx.config.nodeId}", initTable[
        string, string]())

# =============================================================================
# Example: Partial Integration Container
# =============================================================================

proc createPartialProductionContainer*(config: ProductionConfig,
                                        useRealBackend: bool = true): Container =
  ## Create a container that can mix real and placeholder components
  ## Useful for gradual integration of production components
  result = newContainer()

  # Time provider - always real
  let systemTime = newSystemTimeProvider()
  result.registerInstance(ServiceNameTimeProvider, cast[RootRef](systemTime))

  # Logger - always real
  let logger = newLogger("fractio", config.logLevel)
  let loggerAdapter = newLoggerAdapter(logger)
  result.registerInstance(ServiceNameLogger, cast[RootRef](loggerAdapter))

  # Backend - configurable
  if useRealBackend:
    let storageConfig = defaultStorageConfig(config.dataDir / "storage")
    let backend = newWiscKeyBackend(storageConfig)
    if not backend.open(storageConfig):
      raise newException(IOError, "Failed to open storage backend")
    result.registerInstance("storageBackend", cast[RootRef](backend))
    result.registerInstance(ServiceNameBackend, cast[RootRef](
        newNullBackendAdapter()))
  else:
    result.registerInstance(ServiceNameBackend, cast[RootRef](
        newNullBackendAdapter()))

  # Placeholder services
  result.registerInstance(ServiceNameKVStore, cast[RootRef](newNullKVAdapter()))
  result.registerInstance(ServiceNameTxnManager, cast[RootRef](
      newNullTxnAdapter()))
  result.registerInstance(ServiceNameConnManager, cast[RootRef](
      newNullConnAdapter()))
  result.registerInstance(ServiceNameExecutor, cast[RootRef](newNullExecAdapter()))
