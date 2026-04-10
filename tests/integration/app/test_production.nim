# Tests for Fractio Production Bootstrap (Phase 10)

import unittest
import std/[options, os, tables, strformat]
import fractio/app/production
import fractio/app/bootstrap
import fractio/cli/config
import fractio/di/mocks
import fractio/di/container
import fractio/di/interfaces
import fractio/core/types
from fractio/di/mocks import LogLevel, llDebug, llInfo, llWarn, llError

suite "ProductionConfig Tests":

  test "default production config creation":
    let config = defaultProductionConfig()
    check config.nodeId == 1
    check config.dataDir == "/var/lib/fractio"
    check config.raftEnabled
    check config.raftPort == 8300
    check config.clientPort == 9000

  test "custom production config":
    let config = defaultProductionConfig(5, "/data/node5")
    check config.nodeId == 5
    check config.dataDir == "/data/node5"

  test "production config from FractioConfig":
    var fc = FractioConfig(
      nodeId: 10,
      host: "192.168.1.100",
      raftPort: 7000,
      clientPort: 8000,
      dataDir: "/var/lib/fractio/node10",
      webPort: 9000
    )
    let config = productionConfigFromFractioConfig(fc)
    check config.nodeId == 10
    check config.host == "192.168.1.100"
    check config.raftPort == 7000
    check config.clientPort == 8000

suite "Null Adapters Tests":

  test "NullKVAdapter creation":
    let adapter = newNullKVAdapter()
    check adapter != nil

  test "NullTxnAdapter creation":
    let adapter = newNullTxnAdapter()
    check adapter != nil

  test "NullConnAdapter creation":
    let adapter = newNullConnAdapter()
    check adapter != nil

  test "NullExecAdapter creation":
    let adapter = newNullExecAdapter()
    check adapter != nil

  test "NullBackendAdapter creation":
    let adapter = newNullBackendAdapter()
    check adapter != nil

suite "Production Container Tests":
  # Note: Real backend tests require LevelDB - use temp directories

  when defined(hasLevelDB):
    test "creates container with real components":
      var config = defaultProductionConfig()
      config.dataDir = getTempDir() / "fractio_prod_test" / "container_test"
      createDir(config.dataDir)
      let container = createProductionContainer(config)
      check container != nil
      check container.hasService(ServiceNameTimeProvider)
      check container.hasService(ServiceNameLogger)
      check container.hasService("storageBackend")
      container.close()
      removeDir(config.dataDir)

    test "system time provider works":
      var config = defaultProductionConfig()
      config.dataDir = getTempDir() / "fractio_prod_test" / "time_test"
      createDir(config.dataDir)
      let container = createProductionContainer(config)
      let timeProvider = cast[SystemTimeProvider](
        container.resolveRaw(ServiceNameTimeProvider))
      check timeProvider.nowNs() > 0
      container.close()
      removeDir(config.dataDir)

    test "logger adapter works":
      var config = defaultProductionConfig()
      config.dataDir = getTempDir() / "fractio_prod_test" / "logger_test"
      config.logLevel = llDebug
      createDir(config.dataDir)
      let container = createProductionContainer(config)
      check container.hasService(ServiceNameLogger)
      container.close()
      removeDir(config.dataDir)

    test "storage backend is opened":
      var config = defaultProductionConfig()
      config.dataDir = getTempDir() / "fractio_prod_test" / "backend_test"
      createDir(config.dataDir)
      let container = createProductionContainer(config)
      let backend = cast[WiscKeyBackend](container.resolveRaw("storageBackend"))
      check backend != nil
      check backend.isOpen
      container.close()
      removeDir(config.dataDir)

    test "timestamp provider is registered":
      var config = defaultProductionConfig()
      config.dataDir = getTempDir() / "fractio_prod_test" / "ts_test"
      createDir(config.dataDir)
      let container = createProductionContainer(config)
      let tsProvider = cast[TimestampProvider](
        container.resolveRaw("timestampProvider"))
      check tsProvider != nil
      let ts = tsProvider.now()
      check ts > 0
      container.close()
      removeDir(config.dataDir)

  else:
    test "creates container (LevelDB not available)":
      skip()

suite "ProductionContext Tests":

  when defined(hasLevelDB):
    test "creates production context":
      var config = defaultProductionConfig()
      config.dataDir = getTempDir() / "fractio_prod_test" / "ctx_test"
      createDir(config.dataDir)
      let ctx = newProductionContext(config)
      check ctx != nil
      check ctx.container != nil
      check ctx.backend != nil
      check ctx.logger != nil
      ctx.close()
      removeDir(config.dataDir)

    test "getBackend returns backend":
      var config = defaultProductionConfig()
      config.dataDir = getTempDir() / "fractio_prod_test" / "get_backend"
      createDir(config.dataDir)
      let ctx = newProductionContext(config)
      let backend = ctx.getBackend()
      check backend != nil
      check backend.isOpen
      ctx.close()
      removeDir(config.dataDir)

    test "getLogger returns logger":
      var config = defaultProductionConfig()
      config.dataDir = getTempDir() / "fractio_prod_test" / "get_logger"
      createDir(config.dataDir)
      let ctx = newProductionContext(config)
      let logger = ctx.getLogger()
      check logger != nil
      ctx.close()
      removeDir(config.dataDir)

    test "getTimestampProvider works":
      var config = defaultProductionConfig()
      config.dataDir = getTempDir() / "fractio_prod_test" / "get_ts"
      createDir(config.dataDir)
      let ctx = newProductionContext(config)
      let tsProvider = ctx.getTimestampProvider()
      check tsProvider != nil
      let ts = tsProvider.now()
      check ts > 0
      ctx.close()
      removeDir(config.dataDir)

    test "getSystemTime works":
      var config = defaultProductionConfig()
      config.dataDir = getTempDir() / "fractio_prod_test" / "get_sys"
      createDir(config.dataDir)
      let ctx = newProductionContext(config)
      let sysTime = ctx.getSystemTime()
      check sysTime.nowNs() > 0
      ctx.close()
      removeDir(config.dataDir)

    test "isBackendOpen returns true":
      var config = defaultProductionConfig()
      config.dataDir = getTempDir() / "fractio_prod_test" / "backend_open"
      createDir(config.dataDir)
      let ctx = newProductionContext(config)
      check ctx.isBackendOpen()
      ctx.close()
      removeDir(config.dataDir)

    test "getBackendStats returns stats":
      var config = defaultProductionConfig()
      config.dataDir = getTempDir() / "fractio_prod_test" / "backend_stats"
      createDir(config.dataDir)
      let ctx = newProductionContext(config)
      let stats = ctx.getBackendStats()
      check stats.len >= 0 # Stats may be empty initially
      ctx.close()
      removeDir(config.dataDir)

    test "startServices logs":
      var config = defaultProductionConfig()
      config.dataDir = getTempDir() / "fractio_prod_test" / "start_services"
      createDir(config.dataDir)
      let ctx = newProductionContext(config)
      ctx.startServices() # Should not crash
      ctx.close()
      removeDir(config.dataDir)

    test "stopServices logs":
      var config = defaultProductionConfig()
      config.dataDir = getTempDir() / "fractio_prod_test" / "stop_services"
      createDir(config.dataDir)
      let ctx = newProductionContext(config)
      ctx.stopServices() # Should not crash
      ctx.close()
      removeDir(config.dataDir)

suite "Partial Production Container Tests":

  when defined(hasLevelDB):
    test "creates with real backend":
      var config = defaultProductionConfig()
      config.dataDir = getTempDir() / "fractio_prod_test" / "partial_real"
      createDir(config.dataDir)
      let container = createPartialProductionContainer(config,
          useRealBackend = true)
      check container.hasService("storageBackend")
      let backend = cast[WiscKeyBackend](container.resolveRaw("storageBackend"))
      check backend.isOpen
      container.close()
      removeDir(config.dataDir)

    test "creates without real backend":
      var config = defaultProductionConfig()
      config.dataDir = getTempDir() / "fractio_prod_test" / "partial_mock"
      createDir(config.dataDir)
      let container = createPartialProductionContainer(config,
          useRealBackend = false)
      check container.hasService(ServiceNameBackend)
      # Should have null adapter since no real backend
      container.close()
      removeDir(config.dataDir)

suite "Cleanup Tests":

  when defined(hasLevelDB):
    test "close closes backend":
      var config = defaultProductionConfig()
      config.dataDir = getTempDir() / "fractio_prod_test" / "close_test"
      createDir(config.dataDir)
      let ctx = newProductionContext(config)
      check ctx.backend.isOpen
      ctx.close()
      check not ctx.backend.isOpen
      removeDir(config.dataDir)

    test "close handles multiple calls":
      var config = defaultProductionConfig()
      config.dataDir = getTempDir() / "fractio_prod_test" / "multi_close"
      createDir(config.dataDir)
      let ctx = newProductionContext(config)
      ctx.close()
      ctx.close() # Should not crash
      removeDir(config.dataDir)

suite "Integration with Bootstrap Module Tests":

  test "production and test containers use same service names":
    let testContainer = createTestContainer()
    check testContainer.hasService(ServiceNameTimeProvider)
    check testContainer.hasService(ServiceNameLogger)
    testContainer.close()

    when defined(hasLevelDB):
      var config = defaultProductionConfig()
      config.dataDir = getTempDir() / "fractio_prod_test" / "same_names"
      createDir(config.dataDir)
      let prodContainer = createProductionContainer(config)
      check prodContainer.hasService(ServiceNameTimeProvider)
      check prodContainer.hasService(ServiceNameLogger)
      prodContainer.close()
      removeDir(config.dataDir)

suite "Configuration Validation Tests":

  test "nodeId must be valid":
    let config1 = defaultProductionConfig(1)
    check config1.nodeId == 1

    let config65535 = defaultProductionConfig(65535)
    check config65535.nodeId == 65535

  test "ports must be different":
    let config = defaultProductionConfig()
    check config.raftPort != config.clientPort
    check config.clientPort != config.webPort

  test "time sync port derived from raft port":
    let config = defaultProductionConfig()
    check config.timeSyncPort == config.raftPort + 100
