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
##
##   [storage]
##   write-buffer-size-mb = 4
##   block-cache-size-mb = 8
##   vlog-max-size-mb = 1024
##   vlog-clean-threshold = 100000
##   vlog-min-clean-threshold = 1000
##   vlog-clean-buffer-size-mb = 64

import std/[strutils]
import parsetoml

type
  FractioConfig* = object
    nodeId*: int
    host*: string
    raftPort*: int
    clientPort*: int
    dataDir*: string
    webPort*: int
    writeBufferSizeMB*: int
    blockCacheSizeMB*: int
    vlogMaxSizeMB*: int
    vlogCleanThreshold*: int
    vlogMinCleanThreshold*: int
    vlogCleanBufferSizeMB*: int

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

    let vmVal = storage.getOrDefault("vlog-max-size-mb")
    result.vlogMaxSizeMB = if vmVal.isNil: 1024 else: vmVal.getInt().int

    let vctVal = storage.getOrDefault("vlog-clean-threshold")
    result.vlogCleanThreshold = if vctVal.isNil: 100_000 else: vctVal.getInt().int

    let vmctVal = storage.getOrDefault("vlog-min-clean-threshold")
    result.vlogMinCleanThreshold = if vmctVal.isNil: 1000 else: vmctVal.getInt().int

    let vcbVal = storage.getOrDefault("vlog-clean-buffer-size-mb")
    result.vlogCleanBufferSizeMB = if vcbVal.isNil: 64 else: vcbVal.getInt().int
  else:
    result.writeBufferSizeMB = 4
    result.blockCacheSizeMB = 8
    result.vlogMaxSizeMB = 1024
    result.vlogCleanThreshold = 100_000
    result.vlogMinCleanThreshold = 1000
    result.vlogCleanBufferSizeMB = 64
