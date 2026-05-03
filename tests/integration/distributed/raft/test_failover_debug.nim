# Minimal test to debug failover issue
#
# Build: nim c -d:beast --mm:atomicArc --threads:on -p:src -o:bin/fractio_web src/fractio/cli/main.nim
# Run: nim c -r --mm:atomicArc --threads:on -p:src -p:tests tests/integration/distributed/raft/test_failover_debug.nim

import unittest
import std/[os, osproc, strutils, json, httpclient, times, strformat, posix]
import fractio/protocol/types
import ../../../test_config

const
  BinaryPath = "bin/fractio_web"
  TestHost = "127.0.0.1"
  BaseRaftPort = 29000
  BaseClientPort = 19001
  BaseWebPort = 19876
  JOIN_STABILIZE_MS = 1000
  ELECTION_WAIT_MS = 15000 # Extra time for debugging

type
  TestNode = object
    id: int
    raftPort: int
    clientPort: int
    webPort: int
    dataDir: string
    process: Process

proc nodeDataDir(id: int): string =
  "/tmp/fractio-failover-debug-node" & $id

proc writeNodeConfig(id: int; raftPort, clientPort, webPort: int;
                     dataDir: string): string =
  let configPath = dataDir / "fractio.toml"
  createDir(dataDir)
  writeFile(configPath, &"""[node]
id = {id}
host = "{TestHost}"
raft-port = {raftPort}
client-port = {clientPort}
web-port = {webPort}
data-dir = "{dataDir}"

[daemon]
log-file = "{dataDir}/node.log"
""")
  configPath

proc startNode(id: int; join = ""): TestNode =
  result.id = id
  result.raftPort = BaseRaftPort + (id - 1) * 1000
  result.clientPort = BaseClientPort + (id - 1)
  result.webPort = BaseWebPort + (id - 1)
  result.dataDir = nodeDataDir(id)

  let configPath = writeNodeConfig(id, result.raftPort, result.clientPort,
                                   result.webPort, result.dataDir)

  var args = @[
    "start",
    &"--config={configPath}",
    &"--pid-file={result.dataDir}/node.pid",
  ]
  if join != "":
    args.add(&"--join={join}")

  result.process = startProcess(
    BinaryPath,
    workingDir = getCurrentDir(),
    args = args,
    options = {poStdErrToStdOut},
  )

proc stopNode(node: var TestNode) =
  let pidFile = node.dataDir / "node.pid"
  if fileExists(pidFile):
    try:
      let pidStr = readFile(pidFile).strip()
      if pidStr.len > 0:
        let daemonPid = parseInt(pidStr)
        if daemonPid > 1:
          echo "Killing node ", node.id, " PID=", daemonPid
          discard execShellCmd("kill -9 " & $daemonPid & " 2>/dev/null")
          sleep(100)
      removeFile(pidFile)
    except:
      discard
  if node.process != nil and node.process.running:
    discard kill(Pid(node.process.processID), cint(SIGTERM))
    discard node.process.waitForExit(timeout = 5000)
    node.process.close()
    node.process = nil

proc webUrl(node: TestNode): string =
  &"http://{TestHost}:{node.webPort}"

proc sqlQuery(node: TestNode; sql: string; timeoutMs = 5000): JsonNode =
  let client = newHttpClient(timeout = timeoutMs)
  client.headers = newHttpHeaders({"Content-Type": "application/json"})
  let body = $ %* {"sql": sql}
  let resp = client.request(webUrl(node) & "/api/sql",
                            httpMethod = HttpPost, body = body)
  client.close()
  result = parseJson(resp.body)

proc waitForReady(node: TestNode; timeoutMs = 10_000) =
  let deadline = epochTime() + timeoutMs.float / 1000.0
  while epochTime() < deadline:
    try:
      let client = newHttpClient(timeout = 1000)
      discard client.request(webUrl(node) & "/api/nodes", httpMethod = HttpGet)
      client.close()
      return
    except CatchableError:
      sleep(TEST_POLL_INTERVAL_MS)
  raise newException(IOError, &"node {node.id} did not become ready")

proc getNodes(node: TestNode): JsonNode =
  let client = newHttpClient(timeout = 5000)
  let resp = client.request(webUrl(node) & "/api/nodes", httpMethod = HttpGet)
  client.close()
  result = parseJson(resp.body)

proc waitForNodeCount(node: TestNode; expected: int; timeoutMs = 15_000) =
  let deadline = epochTime() + timeoutMs.float / 1000.0
  while epochTime() < deadline:
    try:
      let nodes = getNodes(node)
      if nodes.kind == JArray and nodes.len == expected:
        return
    except CatchableError:
      discard
    sleep(TEST_POLL_INTERVAL_MS)
  raise newException(IOError,
    &"node {node.id} did not reach {expected} members")

proc waitForData(node: TestNode; sql: string; expectedRows: int;
                 timeoutMs = 15_000) =
  let deadline = epochTime() + timeoutMs.float / 1000.0
  while epochTime() < deadline:
    try:
      let res = sqlQuery(node, sql)
      if res.getOrDefault("kind").getStr("") == "rows":
        let rows = res.getOrDefault("rows")
        if not rows.isNil and rows.kind == JArray and rows.len == expectedRows:
          return
    except CatchableError:
      discard
    sleep(TEST_POLL_INTERVAL_MS)
  raise newException(IOError,
    &"node {node.id} did not reach {expectedRows} rows")

proc cleanNodeData(id: int) =
  removeDir(nodeDataDir(id))

suite "Failover Debug":
  var nodes: array[3, TestNode]

  setup:
    for i in 1..3:
      cleanNodeData(i)

  teardown:
    for i in 0..2:
      stopNode(nodes[i])
    # Keep logs for analysis
    echo "Logs preserved at:"
    for i in 1..3:
      echo "  Node ", i, ": ", nodeDataDir(i), "/node.log"

  test "Debug: leader kill and check election":
    # Start 3-node cluster
    echo "\n=== Starting node 1 (initial leader) ==="
    nodes[0] = startNode(1)
    waitForReady(nodes[0])
    echo "Node 1 ready"

    echo "\n=== Starting node 2 (joining) ==="
    nodes[1] = startNode(2, join = &"{TestHost}:{nodes[0].webPort}")
    sleep(JOIN_STABILIZE_MS)
    echo "Node 2 joined"

    echo "\n=== Starting node 3 (joining) ==="
    nodes[2] = startNode(3, join = &"{TestHost}:{nodes[0].webPort}")
    sleep(JOIN_STABILIZE_MS)
    echo "Node 3 joined"

    waitForNodeCount(nodes[0], 3)
    echo "All 3 nodes see each other"

    # Create table and insert data
    echo "\n=== Creating table and inserting data ==="
    let createRes = sqlQuery(nodes[0], "CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT)")
    echo "CREATE result: ", createRes
    check createRes.getOrDefault("kind").getStr("") == "ok"

    discard sqlQuery(nodes[0], "INSERT INTO t1 VALUES (1, 'alpha')")
    waitForData(nodes[1], "SELECT * FROM t1", 1)
    waitForData(nodes[2], "SELECT * FROM t1", 1)
    echo "Data replicated to all nodes"

    # Kill leader (node 1)
    echo "\n=== Killing node 1 (leader) ==="
    stopNode(nodes[0])
    echo "Node 1 killed, waiting ", ELECTION_WAIT_MS, "ms for election..."

    sleep(ELECTION_WAIT_MS)

    # Check what happened - query nodes 2 and 3
    echo "\n=== Checking state after wait ==="

    # Try simple queries first
    echo "\n=== Trying simple SELECT on each node ==="
    for i in [1, 2]:
      try:
        let res = sqlQuery(nodes[i], "SELECT * FROM t1", timeoutMs = 2000)
        echo "Node ", i + 1, " SELECT result: ", res
      except CatchableError as e:
        echo "Node ", i + 1, " SELECT error: ", e.msg

    # Try insert on each node
    echo "\n=== Trying INSERT on each surviving node ==="
    var success = false
    for i in [1, 2]:
      try:
        let res = sqlQuery(nodes[i], "INSERT INTO t1 VALUES (2, 'gamma')",
            timeoutMs = 3000)
        echo "Node ", i + 1, " INSERT result: ", res
        if res.getOrDefault("kind").getStr("") == "modified":
          echo "SUCCESS! Node ", i + 1, " accepted insert"
          success = true
          break
        else:
          let err = res.getOrDefault("error").getStr("")
          echo "Node ", i + 1, " rejected: ", err
      except CatchableError as e:
        echo "Node ", i + 1, " INSERT error: ", e.msg

    if not success:
      echo "\n=== No leader elected - check logs ==="

    echo "\n=== Test complete - check logs ==="
