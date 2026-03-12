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

import unittest
import std/[os, osproc, strutils, json, httpclient, times, strformat, posix]

const
  BinaryPath = "bin/fractio_web"
  TestHost = "127.0.0.1"
  # Use high port range to avoid conflicts with other tests.
  # Each node needs space for its raft groups.
  # Space them 1000 apart to be safe.
  BaseRaftPort = 27000
  BaseClientPort = 19001
  BaseWebPort = 19876

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
""")
  configPath

proc startNode(id: int; join = ""): TestNode =
  ## Start a fractio_web node as a background process.
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
  if node.process != nil and node.process.running:
    discard kill(Pid(node.process.processID), cint(SIGTERM))
    discard node.process.waitForExit(timeout = 5000)
    node.process.close()
    node.process = nil

proc cleanNodeData(id: int) =
  removeDir(nodeDataDir(id))

proc webUrl(node: TestNode): string =
  &"http://{TestHost}:{node.webPort}"

proc sqlQuery(node: TestNode; sql: string): JsonNode =
  ## Execute a SQL query on a node via its web API.
  let client = newHttpClient(timeout = 5000)
  client.headers = newHttpHeaders({"Content-Type": "application/json"})
  let body = $ %* {"sql": sql}
  let resp = client.request(webUrl(node) & "/api/sql",
                            httpMethod = HttpPost, body = body)
  client.close()
  result = parseJson(resp.body)

proc getNodes(node: TestNode): JsonNode =
  ## Get the node list from a node via its web API.
  let client = newHttpClient(timeout = 5000)
  let resp = client.request(webUrl(node) & "/api/nodes",
                            httpMethod = HttpGet)
  client.close()
  result = parseJson(resp.body)

proc waitForReady(node: TestNode; timeoutMs = 10_000) =
  ## Wait until a node's web server is reachable.
  let deadline = epochTime() + timeoutMs.float / 1000.0
  while epochTime() < deadline:
    try:
      let client = newHttpClient(timeout = 1000)
      discard client.request(webUrl(node) & "/api/nodes", httpMethod = HttpGet)
      client.close()
      return
    except CatchableError:
      sleep(200)
  raise newException(IOError, &"node {node.id} did not become ready within {timeoutMs}ms")

proc waitForNodeCount(node: TestNode; expected: int; timeoutMs = 15_000) =
  ## Wait until the node reports the expected number of cluster members.
  let deadline = epochTime() + timeoutMs.float / 1000.0
  while epochTime() < deadline:
    try:
      let nodes = getNodes(node)
      if nodes.kind == JArray and nodes.len == expected:
        return
    except CatchableError:
      discard
    sleep(300)
  raise newException(IOError,
    &"node {node.id} did not reach {expected} members within {timeoutMs}ms")

proc waitForData(node: TestNode; sql: string; expectedRows: int;
                 timeoutMs = 15_000) =
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
    sleep(300)
  raise newException(IOError,
    &"node {node.id} did not reach {expectedRows} rows within {timeoutMs}ms")

proc tryInsert(nodes: openArray[TestNode]; sql: string;
               timeoutMs = 10_000): JsonNode =
  ## Try inserting on each node until one succeeds (finds the leader).
  ## Retries with a timeout to handle elections that are still in progress.
  let deadline = epochTime() + timeoutMs.float / 1000.0
  while epochTime() < deadline:
    for node in nodes:
      try:
        let res = sqlQuery(node, sql)
        if res.getOrDefault("kind").getStr("") != "error":
          return res
        let err = res.getOrDefault("error").getStr("")
        let errLower = err.toLowerAscii
        if "leader" notin errLower and "0x07000001" notin errLower and "code -3" notin errLower:
          echo "tryInsert encountered real error: ", err
          return res  # A real error, not just "not the leader"
      except CatchableError:
        continue
    sleep(500)
  raise newException(IOError, "no node accepted the insert: " & sql)


# ============================================================================
# Test Suite
# ============================================================================

suite "Cluster Dynamic Join and Node Restart":

  var nodes: array[3, TestNode]

  setup:
    # Clean up old data
    for i in 1..3:
      cleanNodeData(i)

  teardown:
    # Stop all nodes
    for i in 0..2:
      stopNode(nodes[i])
    # Clean up data dirs
    for i in 1..3:
      cleanNodeData(i)

  test "3-node cluster: join, data replication, verify all nodes":
    ## Start node 1 as standalone, join nodes 2 and 3, verify data replicates.
    nodes[0] = startNode(1)
    waitForReady(nodes[0])

    nodes[1] = startNode(2, join = &"{TestHost}:{nodes[0].webPort}")
    sleep(2000)

    nodes[2] = startNode(3, join = &"{TestHost}:{nodes[0].webPort}")
    sleep(2000)

    # All 3 nodes should see all 3 members
    waitForNodeCount(nodes[0], 3)
    waitForNodeCount(nodes[1], 3)
    waitForNodeCount(nodes[2], 3)

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
    sleep(2000)
    nodes[2] = startNode(3, join = &"{TestHost}:{nodes[0].webPort}")
    sleep(2000)

    waitForNodeCount(nodes[0], 3)

    # Insert initial data
    discard sqlQuery(nodes[0], "CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT)")
    discard sqlQuery(nodes[0], "INSERT INTO t1 VALUES (1, 'alpha')")
    waitForData(nodes[1], "SELECT * FROM t1", 1)
    waitForData(nodes[2], "SELECT * FROM t1", 1)

    # Kill leader (node 1)
    stopNode(nodes[0])
    sleep(4000) # Wait for election

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
    sleep(2000)
    nodes[2] = startNode(3, join = &"{TestHost}:{nodes[0].webPort}")
    sleep(2000)

    waitForNodeCount(nodes[0], 3)

    # Insert initial data on leader (node 1)
    discard sqlQuery(nodes[0], "CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT)")
    discard sqlQuery(nodes[0], "INSERT INTO t1 VALUES (1, 'alpha')")
    discard sqlQuery(nodes[0], "INSERT INTO t1 VALUES (2, 'beta')")
    waitForData(nodes[1], "SELECT * FROM t1", 2)
    waitForData(nodes[2], "SELECT * FROM t1", 2)

    # Kill leader (node 1)
    stopNode(nodes[0])
    sleep(4000)

    # Insert new data on the new leader
    let insertRes = tryInsert([nodes[1], nodes[2]],
      "INSERT INTO t1 VALUES (3, 'gamma')")
    check insertRes.getOrDefault("kind").getStr("") == "modified"

    # Verify surviving nodes have 3 rows
    waitForData(nodes[1], "SELECT * FROM t1", 3)
    waitForData(nodes[2], "SELECT * FROM t1", 3)

    # Restart node 1 WITHOUT --join (should use saved cluster.json)
    nodes[0] = startNode(1)
    waitForReady(nodes[0])

    # Verify cluster.json was loaded
    check fileExists(nodeDataDir(1) / "cluster.json")

    # Node 1 should catch up incrementally and have all 3 rows
    waitForData(nodes[0], "SELECT * FROM t1", 3, timeoutMs = 15_000)

    # Verify it also sees all cluster members
    waitForNodeCount(nodes[0], 3)

  test "restarted node sees data inserted while it was down":
    ## Verify that not just the count, but actual row values survive.
    nodes[0] = startNode(1)
    waitForReady(nodes[0])

    nodes[1] = startNode(2, join = &"{TestHost}:{nodes[0].webPort}")
    sleep(2000)
    nodes[2] = startNode(3, join = &"{TestHost}:{nodes[0].webPort}")
    sleep(2000)

    waitForNodeCount(nodes[0], 3)

    discard sqlQuery(nodes[0], "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
    discard sqlQuery(nodes[0], "INSERT INTO users VALUES (1, 'Alice')")
    discard sqlQuery(nodes[0], "INSERT INTO users VALUES (2, 'Charlie')")
    waitForData(nodes[1], "SELECT * FROM users", 2)

    # Kill node 1
    stopNode(nodes[0])
    sleep(4000)

    # Insert "Bob" on new leader
    discard tryInsert([nodes[1], nodes[2]],
      "INSERT INTO users VALUES (3, 'Bob')")
    waitForData(nodes[1], "SELECT * FROM users", 3)

    # Restart node 1 without --join
    nodes[0] = startNode(1)
    waitForReady(nodes[0])
    waitForData(nodes[0], "SELECT * FROM users", 3, timeoutMs = 15_000)

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

  test "cluster.json is created on join and persists across restart":
    ## Verify the cluster membership file is correctly created and used.
    nodes[0] = startNode(1)
    waitForReady(nodes[0])

    nodes[1] = startNode(2, join = &"{TestHost}:{nodes[0].webPort}")
    sleep(2000)

    waitForNodeCount(nodes[0], 2)
    waitForNodeCount(nodes[1], 2)

    # Both nodes should have cluster.json
    check fileExists(nodeDataDir(1) / "cluster.json")
    check fileExists(nodeDataDir(2) / "cluster.json")

    # Verify cluster.json content for node 1
    let j1 = parseJson(readFile(nodeDataDir(1) / "cluster.json"))
    let peers1 = j1["peers"]
    check peers1.kind == JArray
    check peers1.len >= 1  # At least node 2

    # Verify cluster.json content for node 2
    let j2 = parseJson(readFile(nodeDataDir(2) / "cluster.json"))
    let peers2 = j2["peers"]
    check peers2.kind == JArray
    check peers2.len >= 1  # At least node 1

  test "multiple kill-restart cycles":
    ## Verify a node can be killed and restarted multiple times.
    nodes[0] = startNode(1)
    waitForReady(nodes[0])
    nodes[1] = startNode(2, join = &"{TestHost}:{nodes[0].webPort}")
    sleep(2000)
    nodes[2] = startNode(3, join = &"{TestHost}:{nodes[0].webPort}")
    sleep(2000)

    waitForNodeCount(nodes[0], 3)

    discard sqlQuery(nodes[0], "CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT)")
    discard sqlQuery(nodes[0], "INSERT INTO t1 VALUES (1, 'round0')")
    waitForData(nodes[1], "SELECT * FROM t1", 1)
    waitForData(nodes[2], "SELECT * FROM t1", 1)

    # Kill-restart cycle: kill node 1, insert, restart, verify
    for round in 1..2:
      stopNode(nodes[0])
      sleep(4000)

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
      waitForData(nodes[0], "SELECT * FROM t1", round + 1, timeoutMs = 15_000)

  test "follower kill and restart (not the leader)":
    ## Verify a killed follower can also rejoin without --join.
    nodes[0] = startNode(1)
    waitForReady(nodes[0])
    nodes[1] = startNode(2, join = &"{TestHost}:{nodes[0].webPort}")
    sleep(2000)
    nodes[2] = startNode(3, join = &"{TestHost}:{nodes[0].webPort}")
    sleep(2000)

    waitForNodeCount(nodes[0], 3)

    discard sqlQuery(nodes[0], "CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT)")
    discard sqlQuery(nodes[0], "INSERT INTO t1 VALUES (1, 'before')")
    waitForData(nodes[1], "SELECT * FROM t1", 1)
    waitForData(nodes[2], "SELECT * FROM t1", 1)

    # Kill follower (node 2) — leader (node 1) stays up
    stopNode(nodes[1])
    sleep(1000)

    # Leader can still write (quorum of 2/3 with nodes 1 + 3)
    discard sqlQuery(nodes[0], "INSERT INTO t1 VALUES (2, 'during')")
    waitForData(nodes[0], "SELECT * FROM t1", 2)
    waitForData(nodes[2], "SELECT * FROM t1", 2)

    # Restart the killed follower
    nodes[1] = startNode(2)
    waitForReady(nodes[1])

    # It should catch up with both rows
    waitForData(nodes[1], "SELECT * FROM t1", 2, timeoutMs = 15_000)

  test "addReplica deduplication prevents quorum inflation":
    ## Start 3 nodes, verify quorum works after multiple addPeerToRaft calls
    ## (which internally call addReplica — now deduplicated).
    nodes[0] = startNode(1)
    waitForReady(nodes[0])
    nodes[1] = startNode(2, join = &"{TestHost}:{nodes[0].webPort}")
    sleep(2000)
    nodes[2] = startNode(3, join = &"{TestHost}:{nodes[0].webPort}")
    sleep(2000)

    waitForNodeCount(nodes[0], 3)

    # Insert multiple rows to exercise quorum consensus
    discard sqlQuery(nodes[0], "CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT)")
    for i in 1..5:
      let res = sqlQuery(nodes[0], &"INSERT INTO t1 VALUES ({i}, 'row{i}')")
      check res.getOrDefault("kind").getStr("") == "modified"

    # All nodes should have all 5 rows
    waitForData(nodes[0], "SELECT * FROM t1", 5)
    waitForData(nodes[1], "SELECT * FROM t1", 5)
    waitForData(nodes[2], "SELECT * FROM t1", 5)
