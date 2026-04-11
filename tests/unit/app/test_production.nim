# Unit tests for app/production module
#
# Tests for:
# - ProductionConfig type and defaults
# - Null adapters
# - ProductionConfigFromFractioConfig conversion
#
# Note: Full production context tests require integration with real storage

import std/[unittest, os, tables]
import fractio/app/production
import fractio/cli/config as fractioConfig
import fractio/utils/logging as fractioLogging

suite "ProductionConfig Type":
  test "ProductionConfig construction":
    let cfg = ProductionConfig(
      nodeId: 1,
      dataDir: "/var/lib/fractio",
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
    check cfg.nodeId == 1
    check cfg.dataDir == "/var/lib/fractio"
    check cfg.raftEnabled == true
    check cfg.raftPort == 8300
    check cfg.clientPort == 9000
    check cfg.webPort == 9876
    check cfg.host == "127.0.0.1"
    check cfg.timeSyncEnabled == true
    check cfg.timeSyncPort == 8400

  test "ProductionConfig minimal":
    let cfg = ProductionConfig(nodeId: 100, dataDir: "/tmp")
    check cfg.nodeId == 100
    check cfg.dataDir == "/tmp"

suite "defaultProductionConfig":
  test "default with nodeId 1":
    let cfg = defaultProductionConfig()
    check cfg.nodeId == 1
    check cfg.dataDir == "/var/lib/fractio"
    check cfg.logLevel == fractioLogging.llInfo
    check cfg.raftEnabled == true
    check cfg.raftPort == 8300
    check cfg.clientPort == 9000
    check cfg.webPort == 9876
    check cfg.host == "127.0.0.1"
    check cfg.joinAddr == ""
    check cfg.timeSyncEnabled == true
    check cfg.timeSyncPort == 8400

  test "default with custom nodeId":
    let cfg = defaultProductionConfig(nodeId = 42)
    check cfg.nodeId == 42
    check cfg.dataDir == "/var/lib/fractio"

  test "default with custom dataDir":
    let cfg = defaultProductionConfig(dataDir = "/custom/path")
    check cfg.dataDir == "/custom/path"

  test "default with both custom":
    let cfg = defaultProductionConfig(nodeId = 10, dataDir = "/tmp/fractio")
    check cfg.nodeId == 10
    check cfg.dataDir == "/tmp/fractio"

suite "productionConfigFromFractioConfig":
  test "conversion from basic FractioConfig":
    let fc = fractioConfig.FractioConfig(
      nodeId: 1,
      host: "192.168.1.100",
      raftPort: 7001,
      clientPort: 9001,
      dataDir: "/var/lib/fractio/node1",
      webPort: 9876
    )
    let pc = productionConfigFromFractioConfig(fc)
    check pc.nodeId == 1
    check pc.host == "192.168.1.100"
    check pc.raftPort == 7001
    check pc.clientPort == 9001
    check pc.dataDir == "/var/lib/fractio/node1"
    check pc.webPort == 9876
    check pc.raftEnabled == true
    check pc.timeSyncEnabled == true
    check pc.timeSyncPort == 7101 # raftPort + 100

  test "conversion with minimal FractioConfig":
    let fc = fractioConfig.FractioConfig(nodeId: 5, dataDir: "/data")
    let pc = productionConfigFromFractioConfig(fc)
    check pc.nodeId == 5
    check pc.dataDir == "/data"

  test "conversion preserves raftPort offset":
    let fc = fractioConfig.FractioConfig(
      nodeId: 1,
      raftPort: 8000,
      dataDir: "/data"
    )
    let pc = productionConfigFromFractioConfig(fc)
    check pc.raftPort == 8000
    check pc.timeSyncPort == 8100 # 8000 + 100

  test "conversion sets default timeSyncEnabled":
    let fc = fractioConfig.FractioConfig(nodeId: 1, dataDir: "/data")
    let pc = productionConfigFromFractioConfig(fc)
    check pc.timeSyncEnabled == true

  test "conversion sets default raftEnabled":
    let fc = fractioConfig.FractioConfig(nodeId: 1, dataDir: "/data")
    let pc = productionConfigFromFractioConfig(fc)
    check pc.raftEnabled == true

  test "conversion sets empty joinAddr":
    let fc = fractioConfig.FractioConfig(nodeId: 1, dataDir: "/data")
    let pc = productionConfigFromFractioConfig(fc)
    check pc.joinAddr == ""

suite "Null Adapters":
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

  test "multiple null adapters independent":
    let kv1 = newNullKVAdapter()
    let kv2 = newNullKVAdapter()
    check kv1 != kv2 # Different instances

suite "ServiceLifecycle Enum":
  test "slSingleton value":
    check int(slSingleton) == 0

  test "slScoped value":
    check int(slScoped) == 1

  test "slTransient value":
    check int(slTransient) == 2

suite "ProductionConfig Edge Cases":
  test "nodeId zero":
    let cfg = ProductionConfig(nodeId: 0, dataDir: "/data")
    check cfg.nodeId == 0

  test "nodeId max uint16":
    let cfg = ProductionConfig(nodeId: 65535, dataDir: "/data")
    check cfg.nodeId == 65535

  test "raftDisabled":
    let cfg = ProductionConfig(
      nodeId: 1,
      dataDir: "/data",
      raftEnabled: false
    )
    check cfg.raftEnabled == false

  test "timeSync disabled":
    let cfg = ProductionConfig(
      nodeId: 1,
      dataDir: "/data",
      timeSyncEnabled: false
    )
    check cfg.timeSyncEnabled == false

  test "joinAddr set":
    let cfg = ProductionConfig(
      nodeId: 1,
      dataDir: "/data",
      joinAddr: "192.168.1.1:8300"
    )
    check cfg.joinAddr == "192.168.1.1:8300"

  test "empty dataDir":
    let cfg = ProductionConfig(nodeId: 1, dataDir: "")
    check cfg.dataDir == ""

suite "Port Calculations":
  test "timeSyncPort offset from raftPort":
    let fc = fractioConfig.FractioConfig(
      nodeId: 1,
      raftPort: 9000,
      dataDir: "/data"
    )
    let pc = productionConfigFromFractioConfig(fc)
    check pc.timeSyncPort == pc.raftPort + 100

  test "various raftPort offsets":
    for raftPort in [1000, 5000, 10000, 65000]:
      let fc = fractioConfig.FractioConfig(
        nodeId: 1,
        raftPort: raftPort,
        dataDir: "/data"
      )
      let pc = productionConfigFromFractioConfig(fc)
      check pc.timeSyncPort == uint16(raftPort + 100)
