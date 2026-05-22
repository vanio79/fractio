# Integration Tests for Cluster Dynamic Join and Node Restart/Rejoin
#
# These tests spawn actual fractio_web processes and communicate via HTTP
# to verify end-to-end cluster join, failover, and incremental restart.
#
# Build the binary first:
#   nim c -d:beast --mm:atomicArc --threads:on -p:src -o:bin/fractio_web src/fractio/cli/main.nim
#
# Run:
#   nim c -r --mm:atomicArc --threads:on -p:src tests/integration/distributed/raft/test_cluster_rejoin.nim
#
# To debug failures, set PRESERVE_LOGS=1 to keep test directories:
#   PRESERVE_LOGS=1 nim c -r --mm:atomicArc -p:src tests/integration/distributed/raft/test_cluster_rejoin.nim

import unittest
import std/[os, osproc, strutils, json, httpclient, times, strformat, posix]
import ../../../test_config

# Set PRESERVE_LOGS=1 environment variable to keep test directories for debugging
const PreserveLogs = existsEnv("PRESERVE_LOGS")

# Kill orphaned fractio_web processes from previous runs.
# Must be called in setup before every test because leaked processes
# from a prior failed test hold the same TCP ports and cause
# EADDRINUSE / connection refused in subsequent tests.
proc cleanupOrphanProcesses() =
  try:
    # 1. Kill by PID files (graceful)
    for dir in walkDirs("/tmp/fractio-rejoin-test-node*"):
      if dirExists(dir):
        let pidFile = dir / "node.pid"
        if fileExists(pidFile):
          let pidStr = readFile(pidFile).strip()
          if pidStr.len > 0:
            let pid = parseInt(pidStr)
            discard execShellCmd("kill -9 " & $pid & " 2>/dev/null")
        when not PreserveLogs:
          removeDir(dir)
    # 2. Kill ALL fractio_web processes — these tests are the only user
    discard execShellCmd("pkill -9 fractio_web 2>/dev/null")
    # 3. Wait for processes to actually die
    sleep(500)
    # 4. Kill any fractio_web processes on our test ports (belt and suspenders)
    for port in [29000, 29001, 29002, 30000, 30001, 30002, 31000, 31001, 31002,
                 19001, 19002, 19003, 19876, 19877, 19878]:
      discard execShellCmd("kill -9 $(lsof -ti :" & $port & " 2>/dev/null) 2>/dev/null || true")
    # 5. Wait for OS to release ports (TIME_WAIT, etc.)
    sleep(500)
  except:
    discard

proc waitForPortsFree(timeoutMs = 3_000) =
  ## Wait until all test ports are free (not held by any process).
  ## Uses hardcoded port numbers matching the const block below.
  let deadline = epochTime() + timeoutMs.float / 1000.0
  let ports = [29000, 30000, 31000, # raft ports (nodes 1-3)
    19001, 19002, 19003,            # client ports (nodes 1-3)
    19876, 19877, 19878]            # web ports (nodes 1-3)
  while epochTime() < deadline:
    var allFree = true
    for port in ports:
      # lsof returns 0 if port is in use, non-zero if free
      let exitCode = execShellCmd("lsof -ti :" & $port & " 2>/dev/null")
      if exitCode == 0:
        allFree = false
        break
    if allFree:
      return
    sleep(50)
  # Don't raise — just log and continue
  discard

# Clean up at startup
cleanupOrphanProcesses()

const
  BinaryPath = "bin/fractio_web"
  TestHost = "127.0.0.1"
  # Use port range below ephemeral (32768) to avoid conflicts.
  # Each node needs space for its raft groups.
  # Space them 1000 apart to be safe.
  # Uses same ports since SO_REUSEADDR/SO_REUSEPORT/SO_LINGER=0 allow immediate reuse
  BaseRaftPort = 29000
  BaseClientPort = 19001
  BaseWebPort = 19876

  # ---------------------------------------------------------------------------
  # Test Timing Constants
  # ---------------------------------------------------------------------------
  # These tests spawn external fractio_web processes which use production-like
  # Raft timeouts internally. Adjust these constants if the binary is configured
  # with faster election timeouts.

  # Time for a node to join cluster and stabilize (used after startNode with --join)
  JOIN_STABILIZE_MS = 200

  # Time for leader election after killing the leader.
  # With 300-600ms election timeout, election should complete in < 1s.
  # We use waitForLeader() which polls, so this is just extra margin.
  ELECTION_WAIT_MS = 500

  # Time for follower removal to propagate (no election needed)
  FOLLOWER_DOWN_MS = 100

type
  TestNode = object
    id: int
    raftPort: int
    clientPort: int
    webPort: int
    dataDir: string
    process: Process

proc nodeDataDir(id: int): string =
  "/tmp/fractio-rejoin-test-node" & $id

proc writeNodeConfig(id: int; raftPort, clientPort, webPort: int;
                     dataDir: string): string =
  ## Write a TOML config file for a node and return its path.
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
  ## Start a fractio_web node as a background process.
  ## Writes PID file for crash recovery cleanup.
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
    &"--pid-file={result.dataDir}/node.pid", # Write PID file for cleanup
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
  ## Stop a running node process.
  ##
  ## IMPORTANT: Since fractio daemonizes via double-fork, the PID in Nim's
  ## process.handle is the parent PID which exits immediately. The ACTUAL
  ## daemon PID is written to the PID file by the daemon itself. We must
  ## read from PID file first.
  let pidFile = node.dataDir / "node.pid"

  # PRIMARY: Read actual daemon PID from PID file and kill it
  if fileExists(pidFile):
    try:
      let pidStr = readFile(pidFile).strip()
      if pidStr.len > 0:
        let daemonPid = parseInt(pidStr)
        if daemonPid > 1:
          discard execShellCmd("kill -9 " & $daemonPid & " 2>/dev/null")
          sleep(100)
      # Clean up PID file
      removeFile(pidFile)
    except:
      discard

  # BACKUP: Try process handle (parent PID, likely already exited)
  if node.process != nil and node.process.running:
    discard kill(Pid(node.process.processID), cint(SIGTERM))
    discard node.process.waitForExit(timeout = 5000)
    node.process.close()
    node.process = nil

proc cleanNodeData(id: int) =
  removeDir(nodeDataDir(id))

proc webUrl(node: TestNode): string =
  &"http://{TestHost}:{node.webPort}"

proc sqlQuery(node: TestNode; sql: string; timeoutMs = 30_000): JsonNode =
  ## Execute a SQL query on a node via its web API.
  ## Retries on transient connection errors (node starting up, leader failover, etc).
  ## Default 30s timeout because the server may block on FractioClient initialization
  ## which can take several seconds when connecting to busy cluster nodes.
  let deadline = epochTime() + timeoutMs.float / 1000.0
  var attempt = 0
  var lastErr = ""
  while epochTime() < deadline:
    attempt.inc()
    try:
      let client = newHttpClient(timeout = 5000)
      client.headers = newHttpHeaders({"Content-Type": "application/json"})
      let body = $ %* {"sql": sql}
      let resp = client.request(webUrl(node) & "/api/sql",
                                httpMethod = HttpPost, body = body)
      client.close()
      return parseJson(resp.body)
    except CatchableError as e:
      lastErr = e.msg
      if epochTime() >= deadline:
        raise newException(IOError,
          &"sqlQuery node {node.id} failed after {attempt} attempts: {sql} lastErr={lastErr[0..<min(lastErr.len, 100)]}")
      sleep(TEST_POLL_INTERVAL_MS * 10) # 100ms between retries
  raise newException(IOError,
    &"sqlQuery node {node.id} timed out after {timeoutMs}ms: {sql}")

proc getNodes(node: TestNode; timeoutMs = 5_000): JsonNode =
  ## Get the node list from a node via its web API.
  ## Retries on transient connection errors.
  let deadline = epochTime() + timeoutMs.float / 1000.0
  while epochTime() < deadline:
    try:
      let client = newHttpClient(timeout = 2000)
      let resp = client.request(webUrl(node) & "/api/nodes",
                                httpMethod = HttpGet)
      client.close()
      return parseJson(resp.body)
    except CatchableError:
      sleep(TEST_POLL_INTERVAL_MS)
  raise newException(IOError,
    &"getNodes node {node.id} timed out after {timeoutMs}ms")

proc waitForReady(node: TestNode; timeoutMs = 10_000) =
  ## Wait until a node's web server is reachable.
  ## Default 10s timeout because restarted nodes need time to rejoin.
  let deadline = epochTime() + timeoutMs.float / 1000.0
  var attempt = 0
  while epochTime() < deadline:
    attempt.inc()
    try:
      let client = newHttpClient(timeout = 1000)
      discard client.request(webUrl(node) & "/api/nodes", httpMethod = HttpGet)
      client.close()
      return
    except CatchableError:
      sleep(TEST_POLL_INTERVAL_MS)
  raise newException(IOError, &"node {node.id} did not become ready within {timeoutMs}ms after {attempt} attempts")

proc waitForNodeCount(node: TestNode; expected: int; timeoutMs = 5_000) =
  ## Wait until the node reports the expected number of cluster members.
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
    &"node {node.id} did not reach {expected} members within {timeoutMs}ms")

proc waitForLeader(node: TestNode; timeoutMs = 10_000) =
  ## Wait until the node reports that both meta and data groups have a leader.
  ## Polls the /api/health endpoint until metaLeaderOK and dataLeaderOK are true.
  let deadline = epochTime() + timeoutMs.float / 1000.0
  while epochTime() < deadline:
    try:
      let client = newHttpClient(timeout = 2000)
      let resp = client.request(webUrl(node) & "/api/health",
          httpMethod = HttpGet)
      let data = parseJson(resp.body)
      client.close()
      let metaOK = data.getOrDefault("metaLeaderOK").getBool(false)
      let dataOK = data.getOrDefault("dataLeaderOK").getBool(false)
      if metaOK and dataOK:
        return
    except CatchableError:
      discard
    sleep(TEST_POLL_INTERVAL_MS)
  raise newException(IOError,
    &"node {node.id} did not report both meta and data leaders within {timeoutMs}ms")

proc waitForDataGroupMembers(node: TestNode; expectedServers: int;
    timeoutMs = 10_000) =
  ## Wait until the node reports the expected number of servers in the data group.
  ## This ensures all nodes have been added via add_srv before proceeding.
  let deadline = epochTime() + timeoutMs.float / 1000.0
  while epochTime() < deadline:
    try:
      let client = newHttpClient(timeout = 2000)
      let resp = client.request(webUrl(node) & "/api/health",
          httpMethod = HttpGet)
      let data = parseJson(resp.body)
      client.close()
      let dataSrv = data.getOrDefault("dataServerCount").getInt(-1)
      if dataSrv >= expectedServers:
        return
    except CatchableError:
      discard
    sleep(TEST_POLL_INTERVAL_MS)
  raise newException(IOError,
    &"node {node.id} did not reach {expectedServers} data group servers within {timeoutMs}ms")

proc waitForClusterStable(node: TestNode; expectedServers = 3;
    timeoutMs = 15_000) =
  ## Wait until the cluster is fully stable: data group has all expected
  ## members AND both meta and data groups have leaders.
  ## This MUST be called before any SQL operations because node 1 can be
  ## data group leader even before nodes 2/3 are added to the data group.
  waitForDataGroupMembers(node, expectedServers, timeoutMs)
  waitForLeader(node, timeoutMs)
  # Warm up the server's FractioClient by sending a lightweight SQL query.
  # The first SQL query on a fresh server triggers getClient() -> initialize()
  # which can block the event loop for several seconds. Doing this during
  # the stability wait ensures the first real SQL query won't time out.
  discard sqlQuery(node, "SELECT 1", timeoutMs = 15_000)
  # Small delay to let the server finish processing any pending Raft
  # operations before we start sending SQL queries.
  sleep(100)

proc waitForData(node: TestNode; sql: string; expectedRows: int;
                 timeoutMs = 5_000) =
  ## Wait until a SELECT query returns the expected number of rows.
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
    &"node {node.id} did not reach {expectedRows} rows within {timeoutMs}ms")

proc tryInsert(nodes: openArray[TestNode]; sql: string;
                timeoutMs = 5_000): JsonNode =
  ## Try inserting on each node until one succeeds (finds the leader).
  ## Retries with a timeout to handle elections that are still in progress.
  ## ALL errors (including non-leader ones) are retried because after a
  ## failover, connection and metadata issues are transient.
  let deadline = epochTime() + timeoutMs.float / 1000.0
  while epochTime() < deadline:
    for node in nodes:
      try:
        let res = sqlQuery(node, sql)
        let kind = res.getOrDefault("kind").getStr("")
        if kind != "error":
          return res
      except CatchableError:
        continue
    sleep(TEST_POLL_INTERVAL_MS * 5) # 50ms - poll faster for leader
  raise newException(IOError, "no node accepted the insert: " & sql)


# ============================================================================
# Test Suite
# ============================================================================

suite "Cluster Dynamic Join and Node Restart":

  var nodes: array[3, TestNode]

  setup:
    # Clean up old data AND kill any leaked processes from previous test runs
    cleanupOrphanProcesses()
    for i in 1..3:
      cleanNodeData(i)

  teardown:
    # Stop all nodes
    for i in 0..2:
      stopNode(nodes[i])
    # Kill ALL fractio_web processes
    discard execShellCmd("pkill -9 fractio_web 2>/dev/null")
    # Wait for ports to be released
    waitForPortsFree(timeoutMs = 5_000)
    # Clean up data dirs (unless PRESERVE_LOGS=1 for debugging)
    when PreserveLogs:
      discard
    else:
      for i in 1..3:
        cleanNodeData(i)

  test "3-node cluster: join, data replication, verify all nodes":
    ## Start node 1 as standalone, join nodes 2 and 3, verify data replicates.
    nodes[0] = startNode(1)
    waitForReady(nodes[0])

    nodes[1] = startNode(2, join = &"{TestHost}:{nodes[0].webPort}")
    waitForReady(nodes[1])
    sleep(JOIN_STABILIZE_MS)

    nodes[2] = startNode(3, join = &"{TestHost}:{nodes[0].webPort}")
    waitForReady(nodes[2])
    sleep(JOIN_STABILIZE_MS)

    # All 3 nodes should see all 3 members
    waitForNodeCount(nodes[0], 3)
    waitForNodeCount(nodes[1], 3)
    waitForNodeCount(nodes[2], 3)

    # Ensure cluster is fully stable before SQL operations
    waitForClusterStable(nodes[0])

    # Insert data on leader (node 1)
    let createRes = sqlQuery(nodes[0], "CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT)")
    check createRes.getOrDefault("kind").getStr("") == "ok"

    discard sqlQuery(nodes[0], "INSERT INTO t1 VALUES (1, 'alpha')")
    discard sqlQuery(nodes[0], "INSERT INTO t1 VALUES (2, 'beta')")

    # Verify replication to all nodes
    waitForData(nodes[0], "SELECT * FROM t1", 2)
    waitForData(nodes[1], "SELECT * FROM t1", 2)
    waitForData(nodes[2], "SELECT * FROM t1", 2)

  test "leader kill → failover → insert on new leader":
    ## Kill the leader, verify a follower takes over, insert new data.
    nodes[0] = startNode(1)
    waitForReady(nodes[0])

    nodes[1] = startNode(2, join = &"{TestHost}:{nodes[0].webPort}")
    waitForReady(nodes[1])
    sleep(JOIN_STABILIZE_MS)
    nodes[2] = startNode(3, join = &"{TestHost}:{nodes[0].webPort}")
    waitForReady(nodes[2])
    sleep(JOIN_STABILIZE_MS)

    waitForNodeCount(nodes[0], 3)

    # Wait for data group to have all 3 members BEFORE any SQL operations.
    # waitForLeader alone is not sufficient because node 1 is leader of
    # the data group even before nodes 2/3 are added to it.
    waitForDataGroupMembers(nodes[0], 3, timeoutMs = 5_000)
    waitForLeader(nodes[0], timeoutMs = 5_000)

    # Insert initial data
    discard sqlQuery(nodes[0], "CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT)")
    discard sqlQuery(nodes[0], "INSERT INTO t1 VALUES (1, 'alpha')")
    waitForData(nodes[1], "SELECT * FROM t1", 1)
    waitForData(nodes[2], "SELECT * FROM t1", 1)

    # Double-check before kill
    waitForDataGroupMembers(nodes[1], 3, timeoutMs = 3_000)

    # Kill leader (node 1)
    stopNode(nodes[0])
    sleep(ELECTION_WAIT_MS) # Wait for election
    # Verify that at least one surviving node has a meta leader
    waitForLeader(nodes[1], timeoutMs = 3_000)

    # Insert on surviving nodes (one should be the new leader)
    let insertRes = tryInsert([nodes[1], nodes[2]],
      "INSERT INTO t1 VALUES (2, 'gamma')")
    check insertRes.getOrDefault("kind").getStr("") == "modified"

    # Both surviving nodes should have both rows
    waitForData(nodes[1], "SELECT * FROM t1", 2)
    waitForData(nodes[2], "SELECT * FROM t1", 2)

  test "killed node restarts without --join and catches up incrementally":
    ## The core test: a killed leader restarts and catches up via Raft
    ## heartbeats WITHOUT requiring --join or full log replay.
    nodes[0] = startNode(1)
    waitForReady(nodes[0])

    nodes[1] = startNode(2, join = &"{TestHost}:{nodes[0].webPort}")
    waitForReady(nodes[1])
    sleep(JOIN_STABILIZE_MS)
    nodes[2] = startNode(3, join = &"{TestHost}:{nodes[0].webPort}")
    waitForReady(nodes[2])
    sleep(JOIN_STABILIZE_MS)

    waitForNodeCount(nodes[0], 3)

    # Ensure cluster is fully stable before SQL operations
    waitForClusterStable(nodes[0])

    # Insert initial data on leader (node 1)
    discard sqlQuery(nodes[0], "CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT)")
    discard sqlQuery(nodes[0], "INSERT INTO t1 VALUES (1, 'alpha')")
    discard sqlQuery(nodes[0], "INSERT INTO t1 VALUES (2, 'beta')")
    waitForData(nodes[1], "SELECT * FROM t1", 2)
    waitForData(nodes[2], "SELECT * FROM t1", 2)

    # Verify data group is fully formed before proceeding to kill
    waitForDataGroupMembers(nodes[1], 3, timeoutMs = 3_000)

    # Kill leader (node 1)
    stopNode(nodes[0])
    sleep(ELECTION_WAIT_MS)
    # Verify that at least one surviving node has a meta leader
    waitForLeader(nodes[1], timeoutMs = 3_000)

    # Insert new data on the new leader
    let insertRes = tryInsert([nodes[1], nodes[2]],
      "INSERT INTO t1 VALUES (3, 'gamma')")
    check insertRes.getOrDefault("kind").getStr("") == "modified"

    # Verify surviving nodes have 3 rows
    waitForData(nodes[1], "SELECT * FROM t1", 3)
    waitForData(nodes[2], "SELECT * FROM t1", 3)

    # Restart node 1 WITHOUT --join (should use saved cluster.bin)
    nodes[0] = startNode(1)
    waitForReady(nodes[0])

    # Verify cluster.bin was loaded
    check fileExists(nodeDataDir(1) / "cluster.bin")

    # Node 1 should catch up incrementally and have all 3 rows
    waitForData(nodes[0], "SELECT * FROM t1", 3, timeoutMs = 10_000)

    # Verify it also sees all cluster members
    waitForNodeCount(nodes[0], 3)

  test "restarted node sees data inserted while it was down":
    ## Verify that not just the count, but actual row values survive.
    nodes[0] = startNode(1)
    waitForReady(nodes[0])

    nodes[1] = startNode(2, join = &"{TestHost}:{nodes[0].webPort}")
    waitForReady(nodes[1])
    sleep(JOIN_STABILIZE_MS)
    nodes[2] = startNode(3, join = &"{TestHost}:{nodes[0].webPort}")
    waitForReady(nodes[2])
    sleep(JOIN_STABILIZE_MS)

    waitForNodeCount(nodes[0], 3)

    # Ensure cluster is fully stable before SQL operations
    waitForClusterStable(nodes[0])

    discard sqlQuery(nodes[0], "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
    discard sqlQuery(nodes[0], "INSERT INTO users VALUES (1, 'Alice')")
    discard sqlQuery(nodes[0], "INSERT INTO users VALUES (2, 'Charlie')")
    waitForData(nodes[1], "SELECT * FROM users", 2)

    # Verify data group is fully formed before proceeding to kill
    waitForDataGroupMembers(nodes[1], 3, timeoutMs = 3_000)

    # Kill node 1
    stopNode(nodes[0])
    sleep(ELECTION_WAIT_MS)
    # Verify that at least one surviving node has a meta leader
    waitForLeader(nodes[1], timeoutMs = 3_000)

    # Insert "Bob" on new leader
    discard tryInsert([nodes[1], nodes[2]],
      "INSERT INTO users VALUES (3, 'Bob')")
    waitForData(nodes[1], "SELECT * FROM users", 3)

    # Restart node 1 without --join
    nodes[0] = startNode(1)
    waitForReady(nodes[0])
    waitForData(nodes[0], "SELECT * FROM users", 3, timeoutMs = 10_000)

    # Verify all names are present
    let res = sqlQuery(nodes[0], "SELECT * FROM users")
    check res.getOrDefault("kind").getStr("") == "rows"
    let rows = res["rows"]
    var names: seq[string]
    for row in rows:
      names.add(row["name"].getStr(""))
    check "Alice" in names
    check "Charlie" in names
    check "Bob" in names

  test "cluster.bin is created on join and persists across restart":
    ## Verify the cluster membership file is correctly created and used.
    nodes[0] = startNode(1)
    waitForReady(nodes[0])

    nodes[1] = startNode(2, join = &"{TestHost}:{nodes[0].webPort}")
    waitForReady(nodes[1])
    sleep(JOIN_STABILIZE_MS)

    waitForNodeCount(nodes[0], 2)
    waitForNodeCount(nodes[1], 2)

    # Both nodes should have cluster.bin (binary format)
    check fileExists(nodeDataDir(1) / "cluster.bin")
    check fileExists(nodeDataDir(2) / "cluster.bin")

  test "multiple kill-restart cycles":
    ## Verify a node can be killed and restarted multiple times.
    nodes[0] = startNode(1)
    waitForReady(nodes[0])
    nodes[1] = startNode(2, join = &"{TestHost}:{nodes[0].webPort}")
    waitForReady(nodes[1])
    sleep(JOIN_STABILIZE_MS)
    nodes[2] = startNode(3, join = &"{TestHost}:{nodes[0].webPort}")
    waitForReady(nodes[2])
    sleep(JOIN_STABILIZE_MS)

    waitForNodeCount(nodes[0], 3)

    # Ensure cluster is fully stable before SQL operations
    waitForClusterStable(nodes[0])

    discard sqlQuery(nodes[0], "CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT)")
    discard sqlQuery(nodes[0], "INSERT INTO t1 VALUES (1, 'round0')")
    waitForData(nodes[1], "SELECT * FROM t1", 1)
    waitForData(nodes[2], "SELECT * FROM t1", 1)

    # Verify data group is fully formed before proceeding to kill
    waitForDataGroupMembers(nodes[1], 3, timeoutMs = 3_000)

    # Kill-restart cycle: kill node 1, insert, restart, verify
    for round in 1..2:
      stopNode(nodes[0])
      sleep(ELECTION_WAIT_MS)
      # Verify that at least one surviving node has a meta leader
      waitForLeader(nodes[1], timeoutMs = 3_000)

      let val = &"round{round}"
      discard tryInsert([nodes[1], nodes[2]],
        &"INSERT INTO t1 VALUES ({round + 1}, '{val}')")

      # Verify survivors have the new row
      waitForData(nodes[1], "SELECT * FROM t1", round + 1)
      waitForData(nodes[2], "SELECT * FROM t1", round + 1)

      # Restart node 1
      nodes[0] = startNode(1)
      waitForReady(nodes[0])

      # Verify it catches up
      waitForData(nodes[0], "SELECT * FROM t1", round + 1, timeoutMs = 10_000)

      # Wait for the restarted node to fully stabilize and for the
      # cluster to rebalance leadership before the next kill cycle.
      # The restarted node must rejoin as a follower and the cluster
      # must stabilize before we kill the leader again.
      waitForClusterStable(nodes[0])

  test "follower kill and restart (not the leader)":
    ## Verify a killed follower can also rejoin without --join.
    nodes[0] = startNode(1)
    waitForReady(nodes[0])
    nodes[1] = startNode(2, join = &"{TestHost}:{nodes[0].webPort}")
    waitForReady(nodes[1])
    sleep(JOIN_STABILIZE_MS)
    nodes[2] = startNode(3, join = &"{TestHost}:{nodes[0].webPort}")
    waitForReady(nodes[2])
    sleep(JOIN_STABILIZE_MS)

    waitForNodeCount(nodes[0], 3)

    # Ensure cluster is fully stable before SQL operations
    waitForClusterStable(nodes[0])

    discard sqlQuery(nodes[0], "CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT)")
    discard sqlQuery(nodes[0], "INSERT INTO t1 VALUES (1, 'before')")
    waitForData(nodes[1], "SELECT * FROM t1", 1)
    waitForData(nodes[2], "SELECT * FROM t1", 1)

    # Verify data group is fully formed before proceeding to kill
    waitForDataGroupMembers(nodes[0], 3, timeoutMs = 3_000)

    # Kill follower (node 2) — leader (node 1) stays up
    stopNode(nodes[1])
    sleep(FOLLOWER_DOWN_MS)

    # Leader can still write (quorum of 2/3 with nodes 1 + 3)
    discard sqlQuery(nodes[0], "INSERT INTO t1 VALUES (2, 'during')")
    waitForData(nodes[0], "SELECT * FROM t1", 2)
    waitForData(nodes[2], "SELECT * FROM t1", 2)

    # Restart the killed follower
    nodes[1] = startNode(2)
    waitForReady(nodes[1])

    # It should catch up with both rows
    waitForData(nodes[1], "SELECT * FROM t1", 2, timeoutMs = 5_000)

  test "addReplica deduplication prevents quorum inflation":
    ## Start 3 nodes, verify quorum works after multiple addPeerToRaft calls
    ## (which internally call addReplica — now deduplicated).
    nodes[0] = startNode(1)
    waitForReady(nodes[0])
    nodes[1] = startNode(2, join = &"{TestHost}:{nodes[0].webPort}")
    waitForReady(nodes[1])
    sleep(JOIN_STABILIZE_MS)
    nodes[2] = startNode(3, join = &"{TestHost}:{nodes[0].webPort}")
    waitForReady(nodes[2])
    sleep(JOIN_STABILIZE_MS)

    waitForNodeCount(nodes[0], 3)

    # Ensure cluster is fully stable before SQL operations
    waitForClusterStable(nodes[0])

    # Insert multiple rows to exercise quorum consensus
    discard sqlQuery(nodes[0], "CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT)")
    for i in 1..5:
      let res = sqlQuery(nodes[0], &"INSERT INTO t1 VALUES ({i}, 'row{i}')")
      check res.getOrDefault("kind").getStr("") == "modified"

    # All nodes should have all 5 rows
    waitForData(nodes[0], "SELECT * FROM t1", 5)
    waitForData(nodes[1], "SELECT * FROM t1", 5)
    waitForData(nodes[2], "SELECT * FROM t1", 5)

# Force exit to clean up leaked daemon processes that nim's test framework
# may not properly terminate. This is a workaround for process leaks that
# cause EADDRINUSE in subsequent test runs.
proc exitNow(status: cint) {.importc: "_exit", header: "<unistd.h>".}
exitNow(0)
