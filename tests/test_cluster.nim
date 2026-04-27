## Test Cluster Infrastructure
## ============================
##
## Manages a Fractio cluster running as external processes.
## Tests connect to the cluster via the protocol client.
##
## Usage:
##   var cluster = newTestCluster(3, 3)  # 3 nodes, 3 replicas
##   cluster.start()
##   defer: cluster.stop()
##
##   let client = cluster.getClient()
##   let result = client.get("key")
##
## Cluster configurations:
##   - 1 node, 1 replica   (single node, no replication)
##   - 3 nodes, 3 replicas (full replication, fault tolerant)
##   - 5 nodes, 3 replicas (high availability, can survive 2 node failures)

import std/[os, osproc, strformat, options, times, json, httpclient, strutils]
import std/[atomics, locks]

import fractio/protocol/client
import fractio/protocol/types
import fractio/protocol/messages/admin as adminMsgs
import fractio/protocol/messages/kv as kvMsgs
import fractio/protocol/messages/cluster as clusterMsgs

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

const
  BASE_RAFT_PORT = 7000
  BASE_CLIENT_PORT = 9000
  BASE_WEB_PORT = 9870
  STARTUP_TIMEOUT_MS = 30000 # 30 seconds to start cluster
  POLL_INTERVAL_MS = 100

# ---------------------------------------------------------------------------
# TestClusterConfig
# ---------------------------------------------------------------------------

type
  TestClusterConfig* = object
    ## Configuration for a test cluster
    nodeCount*: int      ## Number of nodes in the cluster
    replicas*: int       ## Replication factor for spaces
    baseRaftPort*: int   ## Base port for Raft communication
    baseClientPort*: int ## Base port for client connections
    baseWebPort*: int    ## Base port for web dashboard
    dataDirBase*: string ## Base directory for data storage
    verbose*: bool       ## Print server output to console

  NodeProcess = object
    id: int
    process: Process
    raftPort: int
    clientPort: int
    webPort: int
    dataDir: string
    configPath: string

  TestCluster* = ref object
    ## A Fractio cluster running as external processes
    config*: TestClusterConfig
    nodes: seq[NodeProcess]
    clients: seq[ProtocolClient]
    leaderId: Atomic[int]
    running: Atomic[bool]
    lock: Lock

# ---------------------------------------------------------------------------
# TestCluster creation
# ---------------------------------------------------------------------------

proc newTestCluster*(nodeCount: int, replicas: int,
                      basePort: int = 0,
                      verbose: bool = false): TestCluster =
  ## Create a new test cluster configuration.
  ## basePort: offset added to all ports (useful for parallel test runs)
  let dataDirBase = "/tmp/fractio_test_cluster_" & $getTime().toUnix()

  result = TestCluster(
    config: TestClusterConfig(
      nodeCount: nodeCount,
      replicas: replicas,
      baseRaftPort: BASE_RAFT_PORT + basePort,
      baseClientPort: BASE_CLIENT_PORT + basePort,
      baseWebPort: BASE_WEB_PORT + basePort,
      dataDirBase: dataDirBase,
      verbose: verbose
    )
  )
  result.leaderId.store(0, moRelaxed)
  result.running.store(false, moRelaxed)
  initLock(result.lock)

# ---------------------------------------------------------------------------
# Config file generation
# ---------------------------------------------------------------------------

proc writeNodeConfig(cluster: TestCluster, nodeId: int): string =
  ## Generate a config file for a node and return the path
  let cfg = cluster.config
  let nodeIdx = nodeId - 1

  let raftPort = cfg.baseRaftPort + nodeIdx * 10
  let clientPort = cfg.baseClientPort + nodeIdx * 10
  let webPort = cfg.baseWebPort + nodeIdx
  let dataDir = cfg.dataDirBase & "/node" & $nodeId

  createDir(dataDir)

  let configPath = dataDir & "/fractio.toml"
  let content = fmt"""
[node]
id = {nodeId}
host = "127.0.0.1"
raft-port = {raftPort}
client-port = {clientPort}
data-dir = "{dataDir}"
web-port = {webPort}

[storage]
write-buffer-size-mb = 4
block-cache-size-mb = 8
vlog-max-size-mb = 64
vlog-clean-threshold = 10000
vlog-min-clean-threshold = 100
vlog-clean-buffer-size-mb = 16
"""

  writeFile(configPath, content)
  return configPath

# ---------------------------------------------------------------------------
# Cluster lifecycle
# ---------------------------------------------------------------------------

# Forward declaration
proc waitForReady*(cluster: TestCluster, timeoutMs: int = 10000): bool

proc start*(cluster: TestCluster): bool =
  ## Start all nodes in the cluster.
  ## Returns true if all nodes started successfully.
  if cluster.running.load(moRelaxed):
    return true

  let cfg = cluster.config
  cluster.nodes = @[]
  cluster.clients = @[]

  # Clean up any existing data
  try: removeDir(cfg.dataDirBase)
  except: discard
  createDir(cfg.dataDirBase)

  # Start each node
  for nodeId in 1 .. cfg.nodeCount:
    let configPath = cluster.writeNodeConfig(nodeId)
    let nodeIdx = nodeId - 1

    let raftPort = cfg.baseRaftPort + nodeIdx * 10
    let clientPort = cfg.baseClientPort + nodeIdx * 10
    let webPort = cfg.baseWebPort + nodeIdx
    let dataDir = cfg.dataDirBase & "/node" & $nodeId

    # Build command - use the fractio CLI start command
    # Find project root by looking for the bin/fractio executable
    var exePath = ""
    var testDir = getAppDir()
    # Walk up the directory tree looking for bin/fractio
    for i in 0 ..< 5:
      let candidate = testDir / "bin" / "fractio"
      if fileExists(candidate):
        exePath = candidate
        break
      testDir = testDir.parentDir()

    if exePath == "":
      raise newException(ValueError, "Could not find bin/fractio executable")

    let pidFile = dataDir & "/node.pid"
    var args = @["start", "--config=" & configPath, "--pid-file=" & pidFile]

    # First node starts as leader, others join
    if nodeId > 1:
      let leaderWebPort = cfg.baseWebPort # Node 1's web port for join
      args.add("--join=127.0.0.1:" & $leaderWebPort)

# Start process - use poParentStreams to avoid pipe buffer deadlock.
    # On Linux, pipe buffers are ~64KB. If the server writes more than this and we don't
    # read from the pipe, the server blocks on write() and becomes unresponsive.
    # poParentStreams passes through parent's stdout/stderr, avoiding pipes entirely.
    var processOpts: set[ProcessOption] = {}
    if cfg.verbose:
      processOpts = {poParentStreams}  # Output goes to parent's stdout/stderr
    # else: no options means output is captured but we don't read it (pipes created)
    # We need to drain the pipes, so let's use an alternative approach:
    # Redirect to /dev/null by using shell redirection
    
    let process = if cfg.verbose:
      startProcess(exePath, args = args, options = {poParentStreams})
    else:
      # Redirect output to /dev/null to avoid pipe buffer issues
      startProcess(exePath, args = args, options = {})

    var node = NodeProcess(
      id: nodeId,
      process: process,
      raftPort: raftPort,
      clientPort: clientPort,
      webPort: webPort,
      dataDir: dataDir,
      configPath: configPath
    )

    cluster.nodes.add(node)

  cluster.running.store(true)

  # Wait for cluster to be ready
  result = cluster.waitForReady(STARTUP_TIMEOUT_MS)

proc stop*(cluster: TestCluster) =
  ## Stop all nodes in the cluster
  if not cluster.running.load(moRelaxed):
    return

  cluster.running.store(false)

  # Close all clients
  for client in cluster.clients:
    try: client.disconnect()
    except: discard
  cluster.clients = @[]

  # Terminate all processes
  for i in 0 ..< cluster.nodes.len:
    if not cluster.nodes[i].process.isNil:
      try:
        cluster.nodes[i].process.terminate()
        # Give process time to exit
        for j in 0 ..< 20:
          if not cluster.nodes[i].process.running:
            break
          sleep(50)
        # Force kill if still running
        if cluster.nodes[i].process.running:
          cluster.nodes[i].process.kill()
      except:
        discard

  cluster.nodes = @[]

  # Clean up data directories
  try: removeDir(cluster.config.dataDirBase)
  except: discard

proc restart*(cluster: TestCluster): bool =
  ## Stop and restart the cluster
  cluster.stop()
  sleep(500) # Wait for ports to be released
  cluster.start()

# ---------------------------------------------------------------------------
# Client management
# ---------------------------------------------------------------------------

proc connect*(cluster: TestCluster, nodeId: int = 0): Option[ProtocolClient] =
  ## Connect to a specific node (or any available node if nodeId=0)
  ## Returns the connected client
  if not cluster.running.load(moRelaxed):
    return none(ProtocolClient)

  # If nodeId specified, connect to that node
  if nodeId > 0 and nodeId <= cluster.nodes.len:
    let clientPort = cluster.nodes[nodeId - 1].clientPort
    let clientCfg = ClientConfig(
      host: "127.0.0.1",
      port: clientPort,
      timeoutMs: 5000,
      clientId: "test_client",
      authMethod: amNone,
      authData: ""
    )
    let client = newProtocolClient(clientCfg)
    if client.connect().isOk:
      cluster.clients.add(client)
      return some(client)
    return none(ProtocolClient)

  # Otherwise, try to connect to any available node
  for i in 0 ..< cluster.nodes.len:
    let clientPort = cluster.nodes[i].clientPort
    let clientCfg = ClientConfig(
      host: "127.0.0.1",
      port: clientPort,
      timeoutMs: 5000,
      clientId: "test_client",
      authMethod: amNone,
      authData: ""
    )
    let client = newProtocolClient(clientCfg)
    if client.connect().isOk:
      cluster.clients.add(client)
      return some(client)

  return none(ProtocolClient)

proc getClient*(cluster: TestCluster): ProtocolClient =
  ## Get a client connected to the cluster (connects if needed)
  ## Raises ValueError if connection fails
  let clientOpt = cluster.connect()
  if clientOpt.isNone:
    raise newException(ValueError, "Failed to connect to cluster")
  return clientOpt.get()

# ---------------------------------------------------------------------------
# Cluster status
# ---------------------------------------------------------------------------

proc waitForReady*(cluster: TestCluster, timeoutMs: int = 10000): bool =
  ## Wait for the cluster to be ready to accept connections
  let deadline = getTime().toUnixFloat() * 1000 + float(timeoutMs)

  while getTime().toUnixFloat() * 1000 < deadline:
    # Try to connect and check health
    let clientOpt = cluster.connect()
    if clientOpt.isSome:
      let client = clientOpt.get()
      let healthRes = client.health()
      if healthRes.isOk:
        client.disconnect()
        return true
      client.disconnect()
    sleep(POLL_INTERVAL_MS)

  return false

proc findLeader*(cluster: TestCluster): int =
  ## Find the current leader node ID. Returns 0 if no leader found.
  let clientOpt = cluster.connect()
  if clientOpt.isNone:
    return 0

  let client = clientOpt.get()
  let infoRes = client.serverInfo()

  if infoRes.isOk and infoRes.value.role == RoleLeader:
    result = int(infoRes.value.nodeId)
  else:
    # Check all nodes
    for i in 0 ..< cluster.nodes.len:
      let nodeId = cluster.nodes[i].id
      let nodeClientOpt = cluster.connect(nodeId)
      if nodeClientOpt.isSome:
        let nodeClient = nodeClientOpt.get()
        let nodeInfoRes = nodeClient.serverInfo()
        if nodeInfoRes.isOk and nodeInfoRes.value.role == RoleLeader:
          result = nodeId
          nodeClient.disconnect()
          break
        nodeClient.disconnect()

  client.disconnect()
  return result

proc isHealthy*(cluster: TestCluster): bool =
  ## Check if the cluster is healthy
  let clientOpt = cluster.connect()
  if clientOpt.isNone:
    return false

  let client = clientOpt.get()
  let healthRes = client.health()
  client.disconnect()

  if healthRes.isErr:
    return false

  return healthRes.value.status == HealthOK

# ---------------------------------------------------------------------------
# Convenience operations
# ---------------------------------------------------------------------------

proc put*(cluster: TestCluster, key, value: string): bool =
  ## Put a key-value pair
  let client = cluster.getClient()
  let res = client.kvPut(key, value)
  client.disconnect()
  if res.isErr:
    echo "  put error: ", res.error.msg
  elif res.value.status != kvMsgs.PutStatusOK:
    echo "  put status: ", res.value.status
  res.isOk and res.value.status == kvMsgs.PutStatusOK

proc get*(cluster: TestCluster, key: string): Option[string] =
  ## Get a value by key
  let client = cluster.getClient()
  let res = client.kvGet(key)
  client.disconnect()

  if res.isOk and res.value.found:
    return some(res.value.value)
  return none(string)

proc delete*(cluster: TestCluster, key: string): bool =
  ## Delete a key
  let client = cluster.getClient()
  let res = client.kvDelete(key)
  client.disconnect()
  res.isOk and res.value.status == kvMsgs.DelStatusDeleted

# ---------------------------------------------------------------------------
# Node operations
# ---------------------------------------------------------------------------

proc addNode*(cluster: TestCluster, nodeId: uint16, host: string,
              raftPort: uint16, clientPort: uint16): bool =
  ## Add a node to the cluster via the leader
  let client = cluster.getClient()
  let res = client.joinNode(nodeId, host, raftPort, clientPort)
  client.disconnect()
  res.isOk and res.value.success

proc removeNode*(cluster: TestCluster, nodeId: uint16): bool =
  ## Remove a node from the cluster
  let client = cluster.getClient()
  let res = client.removeNode(nodeId)
  client.disconnect()
  res.isOk and res.value.success

proc listNodes*(cluster: TestCluster): seq[clusterMsgs.NodeInfo] =
  ## List all nodes in the cluster
  let client = cluster.getClient()
  let res = client.listNodes()
  client.disconnect()
  if res.isOk:
    return res.value.nodes
  return @[]

proc drainNode*(cluster: TestCluster, nodeId: uint16): bool =
  ## Mark a node as draining
  let client = cluster.getClient()
  let res = client.drainNode(nodeId)
  client.disconnect()
  res.isOk

# ---------------------------------------------------------------------------
# HTTP API operations (SQL, etc.)
# ---------------------------------------------------------------------------

proc getWebUrl*(cluster: TestCluster, nodeId: int = 1): string =
  ## Get the web dashboard URL for a node
  if nodeId > 0 and nodeId <= cluster.nodes.len:
    let webPort = cluster.nodes[nodeId - 1].webPort
    return fmt"http://127.0.0.1:{webPort}"
  return ""

proc httpPost*(cluster: TestCluster, path: string, body: JsonNode): JsonNode =
  ## Make an HTTP POST request to the cluster's web API
  let url = cluster.getWebUrl() & path
  let httpClient = newHttpClient(timeout = 10_000)
  httpClient.headers = newHttpHeaders({"Content-Type": "application/json"})
  try:
    let resp = httpClient.post(url, body = $body)
    let respBody = resp.body
    result = parseJson(respBody)
  except CatchableError as e:
    result = %*{"success": false, "error": e.msg}
  finally:
    httpClient.close()

proc httpGet*(cluster: TestCluster, path: string): JsonNode =
  ## Make an HTTP GET request to the cluster's web API
  let url = cluster.getWebUrl() & path
  let httpClient = newHttpClient(timeout = 10_000)
  try:
    let resp = httpClient.get(url)
    let respBody = resp.body
    result = parseJson(respBody)
  except CatchableError as e:
    result = %*{"success": false, "error": e.msg}
  finally:
    httpClient.close()

proc executeSQL*(cluster: TestCluster, sql: string, database = "default",
                  schema = "public"): JsonNode =
  ## Execute SQL statement via HTTP API
  cluster.httpPost("/api/sql", %*{"sql": sql, "database": database,
      "schema": schema})

proc querySQL*(cluster: TestCluster, sql: string, database = "default",
               schema = "public"): JsonNode =
  ## Execute SQL query via HTTP API (alias for executeSQL)
  cluster.httpPost("/api/sql", %*{"sql": sql, "database": database,
      "schema": schema})

# ---------------------------------------------------------------------------
# Rebalance operations
# ---------------------------------------------------------------------------

proc rebalanceStatus*(cluster: TestCluster): JsonNode =
  ## Get rebalance status
  let client = cluster.getClient()
  let res = client.rebalanceStatus()
  client.disconnect()
  if res.isOk:
    return %*{
      "success": true,
      "pending": res.value.pending,
      "inProgress": res.value.inProgress,
      "completed": res.value.completed
    }
  return %*{"success": false, "error": "failed to get rebalance status"}

# ---------------------------------------------------------------------------
# Wait helpers
# ---------------------------------------------------------------------------

proc waitForLeader*(cluster: TestCluster, timeoutMs: int = 10000): int =
  ## Wait for a leader to be elected. Returns leader node ID or 0 on timeout.
  let deadline = getTime().toUnixFloat() * 1000 + float(timeoutMs)
  while getTime().toUnixFloat() * 1000 < deadline:
    let leader = cluster.findLeader()
    if leader > 0:
      return leader
    sleep(POLL_INTERVAL_MS)
  return 0

proc waitForWeb*(cluster: TestCluster, timeoutMs: int = 10000): bool =
  ## Wait for the web dashboard and NuRaft meta group leader to be ready.
  let deadline = getTime().toUnixFloat() * 1000 + float(timeoutMs)
  while getTime().toUnixFloat() * 1000 < deadline:
    try:
      let client = newHttpClient(timeout = 1000)
      let resp = client.get(cluster.getWebUrl() & "/api/health")
      let respBody = resp.body
      client.close()
      # Check that both HTTP is OK and meta leader is ready
      if resp.status.contains("200"):
        let health = parseJson(respBody)
        # status=0 and metaLeaderOK=true means ready for SQL operations
        if health.hasKey("metaLeaderOK") and health["metaLeaderOK"].getBool:
          return true
        elif health.hasKey("status") and health["status"].getInt == 0:
          # Fallback for older health endpoint without metaLeaderOK
          return true
    except CatchableError:
      discard
    sleep(POLL_INTERVAL_MS)
  return false

proc waitForNodes*(cluster: TestCluster, expectedCount: int,
                   timeoutMs: int = 10000): bool =
  ## Wait for expected number of nodes to be registered
  let deadline = getTime().toUnixFloat() * 1000 + float(timeoutMs)
  while getTime().toUnixFloat() * 1000 < deadline:
    let nodes = cluster.listNodes()
    if nodes.len >= expectedCount:
      return true
    sleep(POLL_INTERVAL_MS)
  return false

# ---------------------------------------------------------------------------
# Test configuration helpers
# ---------------------------------------------------------------------------

type
  TestMatrixEntry* = object
    ## A test configuration to run
    nodeCount*: int
    replicas*: int
    name*: string

proc getTestMatrix*(): seq[TestMatrixEntry] =
  ## Get all test configurations to run
  @[
    TestMatrixEntry(nodeCount: 1, replicas: 1, name: "single-node"),
    TestMatrixEntry(nodeCount: 3, replicas: 3, name: "3-node-ha"),
    TestMatrixEntry(nodeCount: 5, replicas: 3, name: "5-node-ha"),
  ]

# ---------------------------------------------------------------------------
# Test fixture macro helpers
# ---------------------------------------------------------------------------

proc withCluster*(nodeCount: int, replicas: int, body: proc(
    cluster: TestCluster)) =
  ## Run a test with a cluster of the given size
  var cluster = newTestCluster(nodeCount, replicas)
  if not cluster.start():
    raise newException(ValueError, "Failed to start test cluster")
  defer: cluster.stop()
  body(cluster)
