## TOML config loader for Fractio node configuration.
##
## Example fractio.toml:
##
##   [node]
##   id = 1
##   host = "127.0.0.1"
##   raft-port = 7001
##   client-port = 9001
##   data-dir = "/var/lib/fractio/node1"
##   web-port = 9876
##   temp-dir = "/var/lib/fractio/node1/tmp"  # Optional, defaults to data-dir/tmp
##
##   [storage]
##   write-buffer-size-mb = 4
##   block-cache-size-mb = 8
##   vlog-max-size-mb = 1024
##   vlog-clean-threshold = 100000
##   vlog-min-clean-threshold = 1000
##   vlog-clean-buffer-size-mb = 64
##
##   [daemon]
##   foreground = false      # Run in foreground (daemonizes by default on Unix)
##   pid-file = "/var/run/fractio/node1.pid"
##   log-file = "/var/log/fractio/node1.log"

import std/os
import parsetoml

type
  FractioConfig* = object
    nodeId*: int
    host*: string
    raftPort*: int
    clientPort*: int
    dataDir*: string
    tempDir*: string     ## Directory for temporary files (default: dataDir/tmp)
                         ## Subdirectories are used per operation: sort/, etc.
    webPort*: int
    writeBufferSizeMB*: int
    blockCacheSizeMB*: int
    maxFileSizeMB*: int
      ## LevelDB max SST file size in MB. Larger files = fewer L0 SSTs
      ## in flight, which reduces L0 compaction backlog. 0 = LevelDB default
      ## (2 MB). Recommended 16 MB for memory-budgeted deployments.
    l0CompactionTrigger*: int
      ## L0 file count that triggers manual c_leveldb_compact_range().
      ## The fork's LevelDB C API does not expose level0_slowdown_writes_trigger,
      ## so this is our manual throttling knob. 0 = disabled. 8 = standard default.
    vlogMaxSizeMB*: int
    vlogCleanThreshold*: int
    vlogMinCleanThreshold*: int
    vlogCleanBufferSizeMB*: int
    memoryBudgetMB*: int ## Total RSS budget in MB. 0 = unlimited (default).
                           ## When set, the server:
                           ##   - caps LevelDB block cache + write buffer
                           ##   - caps the streaming prefetch buffers
                           ##   - enables LRU eviction on protocol-layer
                           ##     unbounded maps (keyVersions, commitIndex)
                           ##   - refuses new transactions/connections
                           ##     when RSS exceeds the budget
                           ## Daemonization options
    foreground*: bool ## Run in foreground instead of daemonizing (default: false)
    pidFile*: string ## Path to PID file (default: /var/run/fractio/node{id}.pid)
    logFile*: string ## Path to log file for stdout/stderr redirection

proc die(msg: string, code: int = 1) {.noreturn.} =
  writeLine(stderr, "error: " & msg)
  quit(code)

proc loadConfig*(path: string): FractioConfig =
  ## Parse a TOML config file and return a validated FractioConfig.
  ## Required fields: node.id, node.data-dir.
  var toml: TomlValueRef
  try:
    toml = parsetoml.parseFile(path)
  except IOError as e:
    die("cannot read config file '" & path & "': " & e.msg)
  except TomlError as e:
    die("invalid TOML in '" & path & "': " & e.msg)

  let node = toml.getOrDefault("node")
  if node.isNil or node.kind != TomlValueKind.Table:
    die("config file missing [node] section")

  # Required: node.id
  let idVal = node.getOrDefault("id")
  if idVal.isNil:
    die("config: node.id is required")
  result.nodeId = idVal.getInt().int
  if result.nodeId < 1 or result.nodeId > 65535:
    die("config: node.id must be 1..65535")

  # Required: node.data-dir
  let dataDirVal = node.getOrDefault("data-dir")
  if dataDirVal.isNil or dataDirVal.getStr() == "":
    die("config: node.data-dir is required")
  result.dataDir = dataDirVal.getStr()

  # Optional: temp-dir (defaults to data-dir/tmp)
  let tempDirVal = node.getOrDefault("temp-dir")
  result.tempDir = if tempDirVal.isNil or tempDirVal.getStr() == "":
                     result.dataDir / "tmp"
                   else:
                     tempDirVal.getStr()

  # Optional fields with defaults
  let hostVal = node.getOrDefault("host")
  result.host = if hostVal.isNil: "127.0.0.1" else: hostVal.getStr()

  let raftPortVal = node.getOrDefault("raft-port")
  result.raftPort = if raftPortVal.isNil: 8300 else: raftPortVal.getInt().int

  let clientPortVal = node.getOrDefault("client-port")
  result.clientPort = if clientPortVal.isNil: 9000 else: clientPortVal.getInt().int

  let webPortVal = node.getOrDefault("web-port")
  result.webPort = if webPortVal.isNil: 0 else: webPortVal.getInt().int

  # Storage section (optional)
  let storage = toml.getOrDefault("storage")
  if not storage.isNil and storage.kind == TomlValueKind.Table:
    let wbVal = storage.getOrDefault("write-buffer-size-mb")
    result.writeBufferSizeMB = if wbVal.isNil: 4 else: wbVal.getInt().int

    let bcVal = storage.getOrDefault("block-cache-size-mb")
    result.blockCacheSizeMB = if bcVal.isNil: 8 else: bcVal.getInt().int

    let mfsVal = storage.getOrDefault("max-file-size-mb")
    result.maxFileSizeMB = if mfsVal.isNil: 0 else: mfsVal.getInt().int

    let l0Val = storage.getOrDefault("l0-compaction-trigger")
    result.l0CompactionTrigger = if l0Val.isNil: 0 else: l0Val.getInt().int

    let vmVal = storage.getOrDefault("vlog-max-size-mb")
    result.vlogMaxSizeMB = if vmVal.isNil: 1024 else: vmVal.getInt().int

    let vctVal = storage.getOrDefault("vlog-clean-threshold")
    result.vlogCleanThreshold = if vctVal.isNil: 100_000 else: vctVal.getInt().int

    let vmctVal = storage.getOrDefault("vlog-min-clean-threshold")
    result.vlogMinCleanThreshold = if vmctVal.isNil: 1000 else: vmctVal.getInt().int

    let vcbVal = storage.getOrDefault("vlog-clean-buffer-size-mb")
    result.vlogCleanBufferSizeMB = if vcbVal.isNil: 64 else: vcbVal.getInt().int

    let mbVal = storage.getOrDefault("memory-budget-mb")
    result.memoryBudgetMB = if mbVal.isNil: 0 else: mbVal.getInt().int
  else:
    result.writeBufferSizeMB = 4
    result.blockCacheSizeMB = 8
    result.maxFileSizeMB = 0
    result.l0CompactionTrigger = 0
    result.vlogMaxSizeMB = 1024
    result.vlogCleanThreshold = 100_000
    result.vlogMinCleanThreshold = 1000
    result.vlogCleanBufferSizeMB = 64
    result.memoryBudgetMB = 0

  # Validate memory budget: must be > 0 if set, and >= 64MB to be meaningful
  if result.memoryBudgetMB != 0 and result.memoryBudgetMB < 64:
    die("config: memory-budget-mb must be >= 64 (got " &
        $result.memoryBudgetMB & ")")

# Daemon section (optional)
  let daemon = toml.getOrDefault("daemon")
  if not daemon.isNil and daemon.kind == TomlValueKind.Table:
    let fgVal = daemon.getOrDefault("foreground")
    result.foreground = not fgVal.isNil and fgVal.getBool()

    let pidVal = daemon.getOrDefault("pid-file")
    result.pidFile = if pidVal.isNil: "" else: pidVal.getStr()

    let logVal = daemon.getOrDefault("log-file")
    result.logFile = if logVal.isNil: "" else: logVal.getStr()
  else:
    result.foreground = false
    result.pidFile = ""
    result.logFile = ""
