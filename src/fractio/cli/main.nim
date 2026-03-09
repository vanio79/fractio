# Fractio CLI — operator tool for managing a Fractio cluster.
#
# Usage:
#   fractio [OPTIONS] COMMAND [SUBCOMMAND] [ARGS]
#
# Global options:
#   --host HOST      Server host to connect to (default: 127.0.0.1)
#   --port PORT      Server port (default: 9000)
#   --timeout MS     Socket timeout in milliseconds (default: 10000)
#   --format FORMAT  Output format: table (default) | json
#
# Commands:
#   start          --id ID --host HOST --raft-port PORT --client-port PORT
#                  --data-dir DIR [--web-port PORT] [--join HOST:PORT]
#
#   node ls
#   node status [ID]
#   node add       --id ID --host HOST --raft-port PORT --client-port PORT
#   node drain     ID
#   node decommission ID
#
#   cluster info
#   cluster health
#   cluster metrics
#   cluster rebalance
#
#   version
#
# Exit codes:
#   0 — success
#   1 — usage error
#   2 — connection / protocol error
#   3 — server returned failure response

import std/[os, parseopt, strformat, strutils, tables, times, json]
import fractio/protocol/types
import fractio/protocol/client
import fractio/protocol/server
import fractio/protocol/messages/cluster as clusterMsgs
import fractio/protocol/messages/admin as adminMsgs
import fractio/web/dashboard

const FractioVersion = "0.1.0"

# ---------------------------------------------------------------------------
# Output format
# ---------------------------------------------------------------------------

type OutputFormat = enum fmtTable, fmtJson

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc die(msg: string, code: int = 1) {.noreturn.} =
  writeLine(stderr, "error: " & msg)
  quit(code)

proc printUsage() =
  echo """
fractio """ & FractioVersion & """ — Fractio cluster operator CLI

Usage:
  fractio [OPTIONS] COMMAND

Global options:
  --host HOST      Server host (default: 127.0.0.1)
  --port PORT      Server port (default: 9000)
  --timeout MS     Socket timeout ms (default: 10000)
  --format FORMAT  Output format: table (default) | json

Commands:
  start           Start a Fractio node (daemon)
    --id ID           Node ID (1-65535, required)
    --host HOST       Bind address (default: 127.0.0.1)
    --raft-port PORT  Raft RPC port (default: 8300)
    --client-port PORT  Client listen port (default: 9000)
    --data-dir DIR    Persistent data directory (required)
    --web-port PORT   Web dashboard port (0 = disabled)
    --join HOST:PORT  Peer to register with on startup
    --peer ID:HOST:RAFT_PORT  Raft cluster peer (repeatable; omit for single-node)

  node ls           List all cluster nodes
  node status [ID]  Show status of all nodes, or detail for one node
  node add          Register a new node in the cluster
    --id ID, --node-host HOST, --raft-port PORT, --client-port PORT
  node drain ID     Mark a node as draining (graceful shutdown signal)
  node decommission ID  Remove a node from the cluster registry

  cluster info      Show this node's identity, role, and uptime
  cluster health    Show cluster health status
  cluster metrics   Show request and KV operation counters
  cluster rebalance Show rebalance operation status

  version           Print version and exit
"""

proc nodeStatusStr(s: uint8): string =
  case s
  of clusterMsgs.NodeStatusActive: "active"
  of clusterMsgs.NodeStatusDraining: "draining"
  of clusterMsgs.NodeStatusDown: "down"
  else: "unknown"

proc roleStr(r: uint8): string =
  case r
  of adminMsgs.RoleLeader: "leader"
  of adminMsgs.RoleFollower: "follower"
  of adminMsgs.RoleCandidate: "candidate"
  else: "unknown"

proc healthStatusStr(s: uint8): string =
  case s
  of adminMsgs.HealthOK: "OK"
  of adminMsgs.HealthDegraded: "DEGRADED"
  of adminMsgs.HealthCritical: "CRITICAL"
  else: "UNKNOWN"

# ---------------------------------------------------------------------------
# start
# ---------------------------------------------------------------------------

proc cmdStart(flags: Table[string, string], globalHost: string,
              peerFlags: seq[string]) =
  let idStr = flags.getOrDefault("id", "")
  let host = flags.getOrDefault("host", globalHost)
  let raftPortStr = flags.getOrDefault("raft-port", "8300")
  let clientPortStr = flags.getOrDefault("client-port", "9000")
  let dataDir = flags.getOrDefault("data-dir", "")
  let webPortStr = flags.getOrDefault("web-port", "0")
  let joinPeer = flags.getOrDefault("join", "")

  if idStr == "": die("start requires --id")
  if dataDir == "": die("start requires --data-dir")

  var nodeId, raftPort, clientPort, webPort: int
  try:
    nodeId = parseInt(idStr)
    raftPort = parseInt(raftPortStr)
    clientPort = parseInt(clientPortStr)
    webPort = parseInt(webPortStr)
  except ValueError as e:
    die("invalid numeric argument: " & e.msg)

  if nodeId < 1 or nodeId > 65535: die("--id must be 1..65535")

  try: createDir(dataDir)
  except CatchableError as e: die("cannot create data-dir: " & e.msg)

  var cfg = defaultServerConfig()
  cfg.host = host
  cfg.port = clientPort
  cfg.serverId = uint16(nodeId)
  cfg.serverName = "fractio-" & idStr
  cfg.dataDir = dataDir
  cfg.webPort = webPort
  cfg.idleTimeoutSecs = 120

  let server = newProtocolServer(cfg)
  server.start()

  if dataDir != "":
    try:
      server.setupRaftNode(raftPort, peerFlags)
    except Exception as e:
      writeLine(stderr, "warning: raft setup failed: " & e.msg)

  if webPort > 0:
    launchWebDashboard(server)
    echo &"web dashboard: http://{host}:{webPort}"

  if joinPeer != "":
    sleep(200)
    let colonIdx = joinPeer.rfind(':')
    var peerHost: string
    var peerPort: int
    if colonIdx < 0:
      peerHost = joinPeer
      peerPort = 9000
    else:
      peerHost = joinPeer[0..<colonIdx]
      try: peerPort = parseInt(joinPeer[colonIdx+1..^1])
      except ValueError: die("invalid join address: " & joinPeer)

    let peerCfg = ClientConfig(
      host: peerHost,
      port: peerPort,
      timeoutMs: 10_000,
      clientId: "fractio-cli-start",
      authMethod: amNone,
      authData: "",
    )
    let peer = newProtocolClient(peerCfg)
    let connR = peer.connect()
    if connR.isErr:
      writeLine(stderr, "warning: could not connect to peer " & joinPeer &
        ": " & $connR.error)
    else:
      let r = peer.joinNode(uint16(nodeId), host, uint16(raftPort),
        uint16(clientPort))
      if r.isErr:
        writeLine(stderr, "warning: self-registration failed: " & $r.error)
      elif not r.value.success:
        writeLine(stderr, "warning: self-registration refused: " &
          r.value.message)
      peer.disconnect()

  while true:
    sleep(1000)

# ---------------------------------------------------------------------------
# node ls
# ---------------------------------------------------------------------------

proc cmdNodeLs(c: ProtocolClient, fmt: OutputFormat) =
  let r = c.listNodes()
  if r.isErr: die("protocol error: " & $r.error, 2)
  let resp = r.value

  if fmt == fmtJson:
    var arr = newJArray()
    for n in resp.nodes:
      arr.add(%* {
        "nodeId": n.nodeId.int,
        "host": n.host,
        "raftPort": n.raftPort.int,
        "clientPort": n.clientPort.int,
        "status": nodeStatusStr(n.status),
      })
    echo $arr
    return

  if resp.nodes.len == 0:
    echo "(no nodes registered)"
    return
  echo "    ID  HOST                              RAFT  CLIENT  STATUS"
  echo "------  --------------------------------  ----  ------  --------"
  for n in resp.nodes:
    echo &"{n.nodeId:>6}  {n.host:<32}  {n.raftPort:>6}  {n.clientPort:>6}  {nodeStatusStr(n.status)}"

# ---------------------------------------------------------------------------
# node status [ID]
# ---------------------------------------------------------------------------

proc cmdNodeStatus(c: ProtocolClient, nodeIdStr: string, fmt: OutputFormat) =
  let r = c.listNodes()
  if r.isErr: die("protocol error: " & $r.error, 2)
  let resp = r.value

  # Filter to single node if ID given
  if nodeIdStr != "":
    var targetId: int
    try: targetId = parseInt(nodeIdStr)
    except ValueError: die("node ID must be an integer")
    if targetId < 1 or targetId > 65535: die("node ID must be 1..65535")

    for n in resp.nodes:
      if n.nodeId == uint16(targetId):
        if fmt == fmtJson:
          echo $(%* {
            "nodeId": n.nodeId.int,
            "host": n.host,
            "raftPort": n.raftPort.int,
            "clientPort": n.clientPort.int,
            "status": nodeStatusStr(n.status),
          })
        else:
          echo &"node_id:      {n.nodeId}"
          echo &"host:         {n.host}"
          echo &"raft_port:    {n.raftPort}"
          echo &"client_port:  {n.clientPort}"
          echo &"status:       {nodeStatusStr(n.status)}"
        return
    die("node " & nodeIdStr & " not found", 3)

  # All nodes (same as ls but status column wider for context)
  cmdNodeLs(c, fmt)

# ---------------------------------------------------------------------------
# node add
# ---------------------------------------------------------------------------

proc cmdNodeAdd(c: ProtocolClient, args: Table[string, string]) =
  let idStr = args.getOrDefault("id", "")
  # Accept --node-host (preferred) or --host as the new node's address
  let host = if args.hasKey("node-host"): args["node-host"]
             else: args.getOrDefault("host", "")
  let raftP = args.getOrDefault("raft-port", "0")
  let cliP = args.getOrDefault("client-port", "0")

  if idStr == "" or host == "":
    die("node add requires --id and --node-host")

  var nodeId, raftPort, clientPort: int
  try:
    nodeId = parseInt(idStr)
    raftPort = parseInt(raftP)
    clientPort = parseInt(cliP)
  except ValueError as e:
    die("invalid numeric argument: " & e.msg)

  if nodeId < 1 or nodeId > 65535: die("--id must be 1..65535")

  let r = c.joinNode(uint16(nodeId), host, uint16(raftPort), uint16(clientPort))
  if r.isErr: die("protocol error: " & $r.error, 2)
  let resp = r.value
  if not resp.success:
    writeLine(stderr, "server refused: " & resp.message)
    quit(3)
  echo resp.message

# ---------------------------------------------------------------------------
# node drain
# ---------------------------------------------------------------------------

proc cmdNodeDrain(c: ProtocolClient, nodeIdStr: string) =
  if nodeIdStr == "": die("node drain requires a node ID")
  var nodeId: int
  try: nodeId = parseInt(nodeIdStr)
  except ValueError as e: die("invalid node ID: " & e.msg)
  if nodeId < 1 or nodeId > 65535: die("node ID must be 1..65535")

  let r = c.drainNode(uint16(nodeId))
  if r.isErr: die("protocol error: " & $r.error, 2)
  let resp = r.value
  if not resp.success:
    writeLine(stderr, "server refused: " & resp.message)
    quit(3)
  echo resp.message

# ---------------------------------------------------------------------------
# node decommission
# ---------------------------------------------------------------------------

proc cmdNodeDecommission(c: ProtocolClient, nodeIdStr: string) =
  if nodeIdStr == "": die("node decommission requires a node ID")
  var nodeId: int
  try: nodeId = parseInt(nodeIdStr)
  except ValueError as e: die("invalid node ID: " & e.msg)
  if nodeId < 1 or nodeId > 65535: die("node ID must be 1..65535")

  let r = c.removeNode(uint16(nodeId))
  if r.isErr: die("protocol error: " & $r.error, 2)
  let resp = r.value
  if not resp.success:
    writeLine(stderr, "server refused: " & resp.message)
    quit(3)
  echo resp.message

# ---------------------------------------------------------------------------
# cluster info
# ---------------------------------------------------------------------------

proc cmdClusterInfo(c: ProtocolClient, fmt: OutputFormat) =
  let r = c.serverInfo()
  if r.isErr: die("protocol error: " & $r.error, 2)
  let resp = r.value
  if fmt == fmtJson:
    echo $(%* {
      "nodeId": resp.nodeId.int,
      "version": resp.version,
      "uptimeSecs": resp.uptimeSecs,
      "role": roleStr(resp.role),
      "shardCount": resp.shardCount,
      "clientCount": resp.clientCount,
    })
    return
  echo &"node_id:      {resp.nodeId}"
  echo &"version:      {resp.version}"
  echo &"uptime_secs:  {resp.uptimeSecs}"
  echo &"role:         {roleStr(resp.role)}"
  echo &"shard_count:  {resp.shardCount}"
  echo &"client_count: {resp.clientCount}"

# ---------------------------------------------------------------------------
# cluster health
# ---------------------------------------------------------------------------

proc cmdClusterHealth(c: ProtocolClient, fmt: OutputFormat) =
  let r = c.health()
  if r.isErr: die("protocol error: " & $r.error, 2)
  let resp = r.value
  if fmt == fmtJson:
    echo $(%* {
      "status": healthStatusStr(resp.status),
      "leaderOK": resp.leaderOK,
      "replicaCount": resp.replicaCount.int,
      "healthyReplicas": resp.healthyReplicas.int,
      "clusterName": resp.clusterName,
    })
    return
  echo &"status:            {healthStatusStr(resp.status)}"
  echo &"leader_ok:         {resp.leaderOK}"
  echo &"replica_count:     {resp.replicaCount}"
  echo &"healthy_replicas:  {resp.healthyReplicas}"
  echo &"cluster_name:      {resp.clusterName}"

# ---------------------------------------------------------------------------
# cluster metrics
# ---------------------------------------------------------------------------

proc cmdClusterMetrics(c: ProtocolClient, fmt: OutputFormat) =
  let r = c.metrics()
  if r.isErr: die("protocol error: " & $r.error, 2)
  let resp = r.value
  if fmt == fmtJson:
    echo $(%* {
      "requestsTotal": resp.requestsTotal,
      "requestsOK": resp.requestsOK,
      "requestsErr": resp.requestsErr,
      "bytesIn": resp.bytesIn,
      "bytesOut": resp.bytesOut,
      "kvGets": resp.kvGets,
      "kvPuts": resp.kvPuts,
      "kvDeletes": resp.kvDeletes,
      "activeTxns": resp.activeTxns,
      "committedTxns": resp.committedTxns,
      "abortedTxns": resp.abortedTxns,
    })
    return
  echo &"requests_total:    {resp.requestsTotal}"
  echo &"requests_ok:       {resp.requestsOK}"
  echo &"requests_err:      {resp.requestsErr}"
  echo &"bytes_in:          {resp.bytesIn}"
  echo &"bytes_out:         {resp.bytesOut}"
  echo &"kv_gets:           {resp.kvGets}"
  echo &"kv_puts:           {resp.kvPuts}"
  echo &"kv_deletes:        {resp.kvDeletes}"
  echo &"active_txns:       {resp.activeTxns}"
  echo &"committed_txns:    {resp.committedTxns}"
  echo &"aborted_txns:      {resp.abortedTxns}"

# ---------------------------------------------------------------------------
# cluster rebalance
# ---------------------------------------------------------------------------

proc cmdClusterRebalance(c: ProtocolClient, fmt: OutputFormat) =
  let r = c.rebalanceStatus()
  if r.isErr: die("protocol error: " & $r.error, 2)
  let resp = r.value
  if fmt == fmtJson:
    echo $(%* {
      "pending": resp.pending,
      "inProgress": resp.inProgress,
      "completed": resp.completed,
      "failed": resp.failed,
    })
    return
  echo &"pending:     {resp.pending}"
  echo &"in_progress: {resp.inProgress}"
  echo &"completed:   {resp.completed}"
  echo &"failed:      {resp.failed}"

# ---------------------------------------------------------------------------
# Argument parsing and dispatch
# ---------------------------------------------------------------------------

proc main() =
  var globalHost = "127.0.0.1"
  var globalPort = 9000
  var globalTimeout = 10_000
  var globalFmt = fmtTable

  var positional: seq[string] = @[]
  var flags: Table[string, string] = initTable[string, string]()
  var peerFlags: seq[string] = @[]

  var p = initOptParser(commandLineParams())
  while true:
    p.next()
    case p.kind
    of cmdEnd: break
    of cmdShortOption, cmdLongOption:
      let key = p.key
      let val = p.val
      case key
      of "host":
        globalHost = val
        flags[key] = val
      of "port":
        try: globalPort = parseInt(val)
        except ValueError: die("--port must be an integer")
      of "timeout":
        try: globalTimeout = parseInt(val)
        except ValueError: die("--timeout must be an integer")
      of "format":
        case val.toLowerAscii
        of "json": globalFmt = fmtJson
        of "table", "": globalFmt = fmtTable
        else: die("--format must be table or json")
      of "h", "help":
        printUsage()
        quit(0)
      of "peer":
        peerFlags.add(val)
      else:
        flags[key] = val
    of cmdArgument:
      positional.add(p.key)

  if positional.len == 0:
    printUsage()
    quit(1)

  let cmd = positional[0]

  # Handle commands that don't need a server connection first
  case cmd
  of "version":
    echo "fractio " & FractioVersion
    quit(0)
  of "start":
    cmdStart(flags, globalHost, peerFlags)
    quit(0)
  of "-h", "--help", "help":
    printUsage()
    quit(0)
  else: discard

  # Connect to server for all remaining commands
  let cfg = ClientConfig(
    host: globalHost,
    port: globalPort,
    timeoutMs: globalTimeout,
    clientId: "fractio-cli",
    authMethod: amNone,
    authData: "",
  )
  let client = newProtocolClient(cfg)
  let connR = client.connect()
  if connR.isErr:
    die("cannot connect to " & globalHost & ":" & $globalPort & ": " &
        $connR.error, 2)

  case cmd

  of "node":
    if positional.len < 2:
      die("node requires a subcommand: ls | status | add | drain | decommission")
    let sub = positional[1]
    case sub
    of "ls":
      cmdNodeLs(client, globalFmt)
    of "status":
      let idArg = if positional.len >= 3: positional[2] else: ""
      cmdNodeStatus(client, idArg, globalFmt)
    of "add":
      cmdNodeAdd(client, flags)
    of "drain":
      let idArg = if positional.len >= 3: positional[2] else: ""
      cmdNodeDrain(client, idArg)
    of "decommission":
      let idArg = if positional.len >= 3: positional[2] else: ""
      cmdNodeDecommission(client, idArg)
    # Backward-compat aliases
    of "list":
      cmdNodeLs(client, globalFmt)
    of "join":
      # Backward compat: old interface used --host for the new node's host
      var joinFlags = flags
      if joinFlags.hasKey("host") and not joinFlags.hasKey("node-host"):
        joinFlags["node-host"] = joinFlags["host"]
      cmdNodeAdd(client, joinFlags)
    of "remove":
      let idArg = flags.getOrDefault("id", "")
      cmdNodeDecommission(client, idArg)
    else:
      die("unknown node subcommand: " & sub)

  of "cluster":
    if positional.len < 2:
      die("cluster requires a subcommand: info | health | metrics | rebalance")
    let sub = positional[1]
    case sub
    of "info":
      cmdClusterInfo(client, globalFmt)
    of "health":
      cmdClusterHealth(client, globalFmt)
    of "metrics":
      cmdClusterMetrics(client, globalFmt)
    of "rebalance":
      cmdClusterRebalance(client, globalFmt)
    else:
      die("unknown cluster subcommand: " & sub)

  # Backward-compat top-level aliases (not shown in help)
  of "info":
    cmdClusterInfo(client, globalFmt)
  of "health":
    cmdClusterHealth(client, globalFmt)
  of "metrics":
    cmdClusterMetrics(client, globalFmt)
  of "rebalance":
    if positional.len >= 2 and positional[1] == "status":
      cmdClusterRebalance(client, globalFmt)
    else:
      cmdClusterRebalance(client, globalFmt)

  else:
    die("unknown command: " & cmd)

  client.disconnect()

main()
