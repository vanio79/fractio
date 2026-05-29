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

import std/[os, parseopt, strformat, strutils, tables, json, httpclient]
import fractio/protocol/types
import fractio/protocol/client
import fractio/protocol/server
import fractio/protocol/messages/cluster as clusterMsgs
import fractio/protocol/messages/admin as adminMsgs
import fractio/web/dashboard
import fractio/cli/config
import fractio/distributed/meta/system_tables
import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/group_types

when defined(posix):
  import fractio/cli/daemon

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
  start           Start a Fractio node (daemonizes by default on Unix)
    --config=PATH     Path to fractio.toml config file (required)
    --join=HOST:PORT  Peer to register with on startup
    --foreground      Run in foreground (don't daemonize)
    --pid-file=PATH   PID file path (overrides config, default: /var/run/fractio/node{id}.pid)
    --log-file=PATH   Log file for daemon output (overrides config)

  stop            Stop a running daemon (Unix only)
    --config=PATH     Path to fractio.toml config file
    --pid-file=PATH   PID file path (overrides config)

  status          Check if daemon is running (Unix only)
    --config=PATH     Path to fractio.toml config file
    --pid-file=PATH   PID file path (overrides config)

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

proc cmdStart(flags: Table[string, string]) =
  let configPath = flags.getOrDefault("config", "")
  let joinPeer = flags.getOrDefault("join", "")

  # Check for --foreground CLI flag (prevents daemonization)
  let foregroundFlag = flags.getOrDefault("foreground", "false").toLowerAscii() == "true" or
                       flags.hasKey("foreground") and flags["foreground"] == ""
  let pidFileOverride = flags.getOrDefault("pid-file", "")
  let logFileOverride = flags.getOrDefault("log-file", "")

  if configPath == "":
    die("start requires --config PATH")

  let conf = loadConfig(configPath)

  try: createDir(conf.dataDir)
  except CatchableError as e: die("cannot create data-dir: " & e.msg)

  # Create temp directory (used for sort, reverse, and other operations)
  try: createDir(conf.tempDir)
  except CatchableError as e: die("cannot create temp-dir: " & e.msg)

  # Determine foreground setting (CLI overrides config)
  # Default: daemonize on POSIX unless foreground is specified
  let runForeground = when defined(posix): foregroundFlag or
      conf.foreground else: true
  let pidFile = when defined(posix):
    if pidFileOverride != "": pidFileOverride
    elif conf.pidFile != "": conf.pidFile
    else: "/var/run/fractio/node" & $conf.nodeId & ".pid"
  else: ""
  let logFile = when defined(posix):
    if logFileOverride != "": logFileOverride
    elif conf.logFile != "": conf.logFile
    else: ""
  else: ""

  # Daemonize before initializing server (unless foreground mode)
  when defined(posix):
    if not runForeground:
      # Check if daemon is already running
      if isDaemonRunning(pidFile):
        die("daemon already running with PID " & $getDaemonPid(pidFile))

      echo "Starting fractio daemon (node " & $conf.nodeId & ")..."
      echo "PID file: " & pidFile
      if logFile != "":
        echo "Log file: " & logFile

      let daemonCfg = DaemonConfig(
        pidFile: pidFile,
        logFile: logFile,
        workingDir: conf.dataDir,
        umask: 0o022
      )

      try:
        daemonize(daemonCfg)
        # After daemonize(), this process is the daemon child
        # stdout/stderr are now redirected, so any logging goes to log file
      except DaemonError as e:
        die("daemonization failed: " & e.msg)

  var cfg = defaultServerConfig()
  cfg.host = conf.host
  cfg.port = conf.clientPort
  cfg.serverId = uint16(conf.nodeId)
  cfg.serverName = "fractio-" & $conf.nodeId
  cfg.dataDir = conf.dataDir
  cfg.tempDir = conf.tempDir # Base temp directory for sort, reverse, etc.
  cfg.webPort = conf.webPort
  cfg.writeBufferSize = conf.writeBufferSizeMB * 1024 * 1024
  cfg.blockCacheSize = conf.blockCacheSizeMB * 1024 * 1024
  cfg.vlogMaxSize = int64(conf.vlogMaxSizeMB) * 1024 * 1024
  cfg.vlogCleanThreshold = int64(conf.vlogCleanThreshold)
  cfg.vlogMinCleanThreshold = int64(conf.vlogMinCleanThreshold)
  cfg.vlogCleanBufferSize = int64(conf.vlogCleanBufferSizeMB) * 1024 * 1024
  cfg.idleTimeoutSecs = 120

  let server = newProtocolServer(cfg)
  server.start()

  let isJoining = joinPeer != ""

  try:
    # When joining, start Raft but don't become leader — the existing
    # cluster leader will replicate its log to us.
    server.setupRaftNode(conf.raftPort,
                         startAsLeader = not isJoining)
  except Exception as e:
    writeLine(stderr, "warning: raft setup failed: " & e.msg)

  if conf.webPort > 0:
    launchWebDashboard(server)
    # Only print web dashboard URL in foreground mode
    when defined(posix):
      if runForeground:
        echo &"web dashboard: http://{conf.host}:{conf.webPort}"
    else:
      echo &"web dashboard: http://{conf.host}:{conf.webPort}"

  if isJoining:
    sleep(200)
    let colonIdx = joinPeer.rfind(':')
    var peerHost: string
    var peerWebPort: int
    if colonIdx < 0:
      peerHost = joinPeer
      peerWebPort = 9876
    else:
      peerHost = joinPeer[0..<colonIdx]
      try: peerWebPort = parseInt(joinPeer[colonIdx+1..^1])
      except ValueError: die("invalid join address: " & joinPeer)

    # Send HTTP join request to the existing cluster's web port
    let joinUrl = "http://" & peerHost & ":" & $peerWebPort & "/api/cluster/join"
    let body = $ %* {
      "nodeId": conf.nodeId,
      "host": conf.host,
      "raftPort": conf.raftPort,
      "clientPort": conf.clientPort,
      "webPort": conf.webPort,
    }

    # Only print join message in foreground mode
    when defined(posix):
      if runForeground:
        echo &"joining cluster via {joinUrl} ..."
    else:
      echo &"joining cluster via {joinUrl} ..."

    try:
      let httpClient = newHttpClient(timeout = 10_000)
      httpClient.headers = newHttpHeaders({"Content-Type": "application/json"})
      let resp = httpClient.request(joinUrl, httpMethod = HttpPost, body = body)
      let respBody = resp.body
      let rj = parseJson(respBody)

      if rj.getOrDefault("success").getBool(false):
        when defined(posix):
          if runForeground:
            echo "join successful"
        else:
          echo "join successful"
# Add all returned cluster members as Raft peers
        let members = rj.getOrDefault("members")
        let preferredLeader = uint32(rj.getOrDefault("preferredLeader").getInt(0))
        if not members.isNil and members.kind == JArray:
          # Build peer info table from join response, but create Raft groups
          # with ONLY ourselves (single-member config for join protocol).
          var leaderHost = ""
          var leaderPort = 0
          for m in members:
            let mNodeId = uint32(m.getOrDefault("nodeId").getInt(0))
            let mHost = m.getOrDefault("host").getStr("")
            let mRaftPort = m.getOrDefault("raftPort").getInt(0)
            if mNodeId > 0 and mHost != "" and mRaftPort > 0:
              # Populate peerInfo for future RPC, but don't include in Raft config
              server.raftCoord.peerInfo[mNodeId] = (host: mHost, port: mRaftPort)
              if mNodeId == preferredLeader:
                leaderHost = mHost
                leaderPort = mRaftPort
          
          # Create single-member group: just ourselves
          var singleMember: seq[tuple[nodeId: uint32, host: string, port: int]] = @[]
          singleMember.add((nodeId: uint32(conf.nodeId), host: conf.host, port: conf.raftPort))
          
          echo "[DEBUG] preferredLeader=" & $preferredLeader & " creating single-member config for join"
          
          # CRITICAL: Wait for the leader's JoinGroup RPC before creating
          # any local Raft groups. The leader's addPeerToRaft sends JoinGroup
          # RPCs which create multi-member groups with the correct config.
          # If we create single-member groups first, we create split-brain
          # where both the leader and us think we're the leader of the same
          # GroupID but with different configs, causing infinite redirect loops.
          #
          # Only create single-member groups as a fallback if the leader's
          # JoinGroup RPC doesn't arrive (e.g. leader crashed after HTTP join).
          for groupId in [META_GROUP_ID, DATA_GROUP_START_ID]:
            let ulid = groupIDToULID(groupId)
            # Wait up to 5 seconds for JoinGroup RPC to create the group
            var groupCreated = false
            for waitAttempt in 0 ..< 25:
              if server.raftCoord.hasGroup(groupId):
                groupCreated = true
                break
              sleep(200)
            if groupCreated:
              echo "[DEBUG] Group " & $ulid & " created by JoinGroup RPC from leader"
            else:
              # Fallback: create single-member group if leader never sent JoinGroup
              echo "[DEBUG] JoinGroup RPC did not arrive for " & $ulid & ", creating single-member fallback"
              discard server.raftCoord.createAndStartGroup(groupId, singleMember,
                  preferredLeader)
          
          # Save cluster state for restart recovery
          server.saveClusterState()
      else:
        let errMsg = rj.getOrDefault("error").getStr("unknown error")
        writeLine(stderr, "warning: join refused: " & errMsg)
      httpClient.close()
    except CatchableError as e:
      writeLine(stderr, "warning: join request failed: " & e.msg)

  while true:
    sleep(1000)

# ---------------------------------------------------------------------------
# stop daemon
# ---------------------------------------------------------------------------

proc cmdStop(flags: Table[string, string]) =
  when defined(posix):
    let configPath = flags.getOrDefault("config", "")
    let pidFileOverride = flags.getOrDefault("pid-file", "")

    var pidFile: string
    if pidFileOverride != "":
      pidFile = pidFileOverride
    elif configPath != "":
      let conf = loadConfig(configPath)
      pidFile = if conf.pidFile != "": conf.pidFile
                else: "/var/run/fractio/node" & $conf.nodeId & ".pid"
    else:
      die("stop requires --config PATH or --pid-file PATH")

    if not isDaemonRunning(pidFile):
      echo "Daemon is not running (PID file: " & pidFile & ")"
      quit(0)

    let pid = getDaemonPid(pidFile)
    echo "Stopping daemon with PID " & $pid & "..."

    if stopDaemon(pidFile):
      echo "Daemon stopped successfully"
      quit(0)
    else:
      die("failed to stop daemon")
  else:
    die("stop command is only supported on POSIX systems")

# ---------------------------------------------------------------------------
# daemon status
# ---------------------------------------------------------------------------

proc cmdDaemonStatus(flags: Table[string, string]) =
  when defined(posix):
    let configPath = flags.getOrDefault("config", "")
    let pidFileOverride = flags.getOrDefault("pid-file", "")

    var pidFile: string
    if pidFileOverride != "":
      pidFile = pidFileOverride
    elif configPath != "":
      let conf = loadConfig(configPath)
      pidFile = if conf.pidFile != "": conf.pidFile
                else: "/var/run/fractio/node" & $conf.nodeId & ".pid"
    else:
      die("status requires --config PATH or --pid-file PATH")

    let pid = getDaemonPid(pidFile)
    if pid < 0:
      echo "Daemon is not running"
      quit(1)
    else:
      echo "Daemon is running with PID " & $pid
      quit(0)
  else:
    die("status command is only supported on POSIX systems")

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
      arr.add( %* {
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
          echo $( %* {
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
    echo $( %* {
      "nodeId": resp.nodeId.int,
      "version": resp.version,
      "uptimeSecs": resp.uptimeSecs,
      "role": roleStr(resp.role),
      "groupCount": resp.groupCount,
      "clientCount": resp.clientCount,
    })
    return
  echo &"node_id:      {resp.nodeId}"
  echo &"version:      {resp.version}"
  echo &"uptime_secs:  {resp.uptimeSecs}"
  echo &"role:         {roleStr(resp.role)}"
  echo &"group_count:  {resp.groupCount}"
  echo &"client_count: {resp.clientCount}"

# ---------------------------------------------------------------------------
# cluster health
# ---------------------------------------------------------------------------

proc cmdClusterHealth(c: ProtocolClient, fmt: OutputFormat) =
  let r = c.health()
  if r.isErr: die("protocol error: " & $r.error, 2)
  let resp = r.value
  if fmt == fmtJson:
    echo $( %* {
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
    echo $( %* {
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
    echo $( %* {
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
    cmdStart(flags)
    quit(0)
  of "stop":
    when defined(posix):
      cmdStop(flags)
      quit(0)
    else:
      die("stop command is only supported on POSIX systems")
  of "status":
    when defined(posix):
      cmdDaemonStatus(flags)
    else:
      die("status command is only supported on POSIX systems")
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
