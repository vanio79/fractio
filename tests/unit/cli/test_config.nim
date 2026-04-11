# Unit tests for CLI config module
#
# Tests for:
# - FractioConfig type
# - Default values
# - TOML config loading (using temporary files)
#
# Note: loadConfig uses die() which calls quit(), making error testing difficult
# in unit tests. Integration tests would cover error conditions.

import std/[unittest, os, strutils]
import parsetoml
import fractio/cli/config

suite "FractioConfig Type":
  test "config type construction":
    let cfg = FractioConfig(
      nodeId: 1,
      host: "localhost",
      raftPort: 8300,
      clientPort: 9000,
      dataDir: "/tmp/fractio",
      webPort: 9876,
      writeBufferSizeMB: 4,
      blockCacheSizeMB: 8,
      vlogMaxSizeMB: 1024,
      vlogCleanThreshold: 100_000,
      vlogMinCleanThreshold: 1000,
      vlogCleanBufferSizeMB: 64
    )
    check cfg.nodeId == 1
    check cfg.host == "localhost"
    check cfg.raftPort == 8300
    check cfg.clientPort == 9000
    check cfg.dataDir == "/tmp/fractio"
    check cfg.webPort == 9876

  test "config with minimal fields":
    let cfg = FractioConfig(
      nodeId: 100,
      host: "",
      raftPort: 0,
      clientPort: 0,
      dataDir: "/data",
      webPort: 0
    )
    check cfg.nodeId == 100
    check cfg.dataDir == "/data"

  test "config with max nodeId":
    let cfg = FractioConfig(nodeId: 65535, dataDir: "/data")
    check cfg.nodeId == 65535

suite "Storage Defaults from loadConfig":
  # Note: Object defaults are 0, but loadConfig sets specific defaults
  test "writeBufferSizeMB from loadConfig":
    let tmpDir = "/tmp/fractio-test-config"
    createDir(tmpDir)
    let configPath = tmpDir / "defaults.toml"
    writeFile(configPath, """
[node]
id = 1
data-dir = "/tmp/fractio"
""")
    let cfg = loadConfig(configPath)
    check cfg.writeBufferSizeMB == 4
    check cfg.blockCacheSizeMB == 8
    check cfg.vlogMaxSizeMB == 1024
    check cfg.vlogCleanThreshold == 100_000
    check cfg.vlogMinCleanThreshold == 1000
    check cfg.vlogCleanBufferSizeMB == 64
    removeFile(configPath)
    removeDir(tmpDir)

suite "Network Defaults from loadConfig":
  test "loadConfig sets default host":
    let tmpDir = "/tmp/fractio-test-config"
    createDir(tmpDir)
    let configPath = tmpDir / "host.toml"
    writeFile(configPath, """
[node]
id = 1
data-dir = "/tmp/fractio"
""")
    let cfg = loadConfig(configPath)
    check cfg.host == "127.0.0.1"
    removeFile(configPath)

  test "loadConfig sets default raftPort":
    let tmpDir = "/tmp/fractio-test-config"
    createDir(tmpDir)
    let configPath = tmpDir / "raft.toml"
    writeFile(configPath, """
[node]
id = 1
data-dir = "/tmp/fractio"
""")
    let cfg = loadConfig(configPath)
    check cfg.raftPort == 8300
    removeFile(configPath)

  test "loadConfig sets default clientPort":
    let tmpDir = "/tmp/fractio-test-config"
    createDir(tmpDir)
    let configPath = tmpDir / "client.toml"
    writeFile(configPath, """
[node]
id = 1
data-dir = "/tmp/fractio"
""")
    let cfg = loadConfig(configPath)
    check cfg.clientPort == 9000
    removeFile(configPath)

  test "loadConfig sets default webPort to 0":
    let tmpDir = "/tmp/fractio-test-config"
    createDir(tmpDir)
    let configPath = tmpDir / "web.toml"
    writeFile(configPath, """
[node]
id = 1
data-dir = "/tmp/fractio"
""")
    let cfg = loadConfig(configPath)
    check cfg.webPort == 0
    removeFile(configPath)
    removeDir(tmpDir)

suite "loadConfig with Valid TOML":
  test "minimal valid config":
    let tmpDir = "/tmp/fractio-test-config"
    createDir(tmpDir)
    let configPath = tmpDir / "minimal.toml"
    writeFile(configPath, """
[node]
id = 1
data-dir = "/tmp/fractio"
""")
    let cfg = loadConfig(configPath)
    check cfg.nodeId == 1
    check cfg.dataDir == "/tmp/fractio"
    check cfg.host == "127.0.0.1" # Default
    check cfg.raftPort == 8300 # Default
    check cfg.clientPort == 9000 # Default
    removeFile(configPath)

  test "full valid config":
    let tmpDir = "/tmp/fractio-test-config"
    createDir(tmpDir)
    let configPath = tmpDir / "full.toml"
    writeFile(configPath, """
[node]
id = 42
host = "192.168.1.100"
raft-port = 7001
client-port = 9001
data-dir = "/var/lib/fractio/node42"
web-port = 9876

[storage]
write-buffer-size-mb = 8
block-cache-size-mb = 16
vlog-max-size-mb = 2048
vlog-clean-threshold = 200000
vlog-min-clean-threshold = 2000
vlog-clean-buffer-size-mb = 128
""")
    let cfg = loadConfig(configPath)
    check cfg.nodeId == 42
    check cfg.host == "192.168.1.100"
    check cfg.raftPort == 7001
    check cfg.clientPort == 9001
    check cfg.dataDir == "/var/lib/fractio/node42"
    check cfg.webPort == 9876
    check cfg.writeBufferSizeMB == 8
    check cfg.blockCacheSizeMB == 16
    check cfg.vlogMaxSizeMB == 2048
    check cfg.vlogCleanThreshold == 200000
    check cfg.vlogMinCleanThreshold == 2000
    check cfg.vlogCleanBufferSizeMB == 128
    removeFile(configPath)

  test "config without storage section uses defaults":
    let tmpDir = "/tmp/fractio-test-config"
    createDir(tmpDir)
    let configPath = tmpDir / "no_storage.toml"
    writeFile(configPath, """
[node]
id = 1
data-dir = "/tmp/fractio"
""")
    let cfg = loadConfig(configPath)
    check cfg.writeBufferSizeMB == 4
    check cfg.blockCacheSizeMB == 8
    check cfg.vlogMaxSizeMB == 1024
    check cfg.vlogCleanThreshold == 100_000
    check cfg.vlogMinCleanThreshold == 1000
    check cfg.vlogCleanBufferSizeMB == 64
    removeFile(configPath)

  test "partial storage config":
    let tmpDir = "/tmp/fractio-test-config"
    createDir(tmpDir)
    let configPath = tmpDir / "partial_storage.toml"
    writeFile(configPath, """
[node]
id = 1
data-dir = "/tmp/fractio"

[storage]
write-buffer-size-mb = 16
""")
    let cfg = loadConfig(configPath)
    check cfg.writeBufferSizeMB == 16
    check cfg.blockCacheSizeMB == 8 # Default
    check cfg.vlogMaxSizeMB == 1024 # Default
    removeFile(configPath)

  test "config with custom ports":
    let tmpDir = "/tmp/fractio-test-config"
    createDir(tmpDir)
    let configPath = tmpDir / "ports.toml"
    writeFile(configPath, """
[node]
id = 1
data-dir = "/tmp/fractio"
raft-port = 9001
client-port = 8001
""")
    let cfg = loadConfig(configPath)
    check cfg.raftPort == 9001
    check cfg.clientPort == 8001
    removeFile(configPath)

suite "NodeId Validation":
  test "nodeId at minimum":
    let tmpDir = "/tmp/fractio-test-config"
    createDir(tmpDir)
    let configPath = tmpDir / "min_id.toml"
    writeFile(configPath, """
[node]
id = 1
data-dir = "/tmp/fractio"
""")
    let cfg = loadConfig(configPath)
    check cfg.nodeId == 1
    removeFile(configPath)

  test "nodeId at maximum":
    let tmpDir = "/tmp/fractio-test-config"
    createDir(tmpDir)
    let configPath = tmpDir / "max_id.toml"
    writeFile(configPath, """
[node]
id = 65535
data-dir = "/tmp/fractio"
""")
    let cfg = loadConfig(configPath)
    check cfg.nodeId == 65535
    removeFile(configPath)

suite "TOML Parsing Edge Cases":
  test "config with extra sections ignored":
    let tmpDir = "/tmp/fractio-test-config"
    createDir(tmpDir)
    let configPath = tmpDir / "extra.toml"
    writeFile(configPath, """
[node]
id = 1
data-dir = "/tmp/fractio"

[unknown_section]
some_field = "value"

[another_unknown]
another_field = 123
""")
    let cfg = loadConfig(configPath)
    check cfg.nodeId == 1
    check cfg.dataDir == "/tmp/fractio"
    removeFile(configPath)

suite "Cleanup":
  test "remove test directory":
    let tmpDir = "/tmp/fractio-test-config"
    if dirExists(tmpDir):
      removeDir(tmpDir)
    check not dirExists(tmpDir)
