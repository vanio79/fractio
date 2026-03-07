# Fractio server CLI — operator tool for managing a Fractio cluster.
#
# Usage:
#   fractio [--host HOST] [--port PORT] <command> [args...]
#
# Global flags:
#   --host HOST      Server host (default: 127.0.0.1)
#   --port PORT      Server admin port (default: 9000)
#   --timeout MS     Socket timeout in milliseconds (default: 10000)
#
# Commands:
#   node join   --id ID --host HOST --raft-port PORT --client-port PORT
#   node remove --id ID
#   node list
#   rebalance status
#   info
#   health
#   metrics
#
# Exit codes:
#   0 — success
#   1 — usage error
#   2 — connection / protocol error
#   3 — server returned failure response

import std/[os, parseopt, strformat, strutils, tables]
import fractio/protocol/types
import fractio/protocol/client
import fractio/protocol/messages/cluster as clusterMsgs
import fractio/protocol/messages/admin as adminMsgs

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc die(msg: string, code: int = 1) {.noreturn.} =
  writeLine(stderr, "error: " & msg)
  quit(code)

proc printUsage() =
  echo """
fractio — Fractio cluster operator CLI

Usage:
  fractio [OPTIONS] COMMAND

Global options:
  --host HOST      Server host (default: 127.0.0.1)
  --port PORT      Server port (default: 9000)
  --timeout MS     Socket timeout ms (default: 10000)

Commands:
  node join   --id ID --host HOST --raft-port PORT --client-port PORT
  node remove --id ID
  node list
  rebalance status
  info
  health
  metrics
"""

proc statusStr(s: uint8): string =
  case s
  of clusterMsgs.NodeStatusActive: "active"
  of clusterMsgs.NodeStatusDraining: "draining"
  of clusterMsgs.NodeStatusDown: "down"
  else: "unknown"

# ---------------------------------------------------------------------------
# Command implementations
# ---------------------------------------------------------------------------

proc cmdNodeJoin(c: ProtocolClient, args: Table[string, string]) =
  let idStr = args.getOrDefault("id", "")
  let host = args.getOrDefault("host", "")
  let raftP = args.getOrDefault("raft-port", "0")
  let cliP = args.getOrDefault("client-port", "0")

  if idStr == "" or host == "":
    die("node join requires --id and --host")

  var nodeId: int
  var raftPort: int
  var clientPort: int
  try:
    nodeId = parseInt(idStr)
    raftPort = parseInt(raftP)
    clientPort = parseInt(cliP)
  except ValueError as e:
    die("invalid numeric argument: " & e.msg)

  if nodeId < 1 or nodeId > 65535:
    die("--id must be 1..65535")

  let r = c.joinNode(uint16(nodeId), host, uint16(raftPort), uint16(clientPort))
  if r.isErr:
    die("protocol error: " & $r.error, 2)
  let resp = r.value
  if not resp.success:
    writeLine(stderr, "server refused: " & resp.message)
    quit(3)
  echo resp.message

proc cmdNodeRemove(c: ProtocolClient, args: Table[string, string]) =
  let idStr = args.getOrDefault("id", "")
  if idStr == "":
    die("node remove requires --id")
  var nodeId: int
  try: nodeId = parseInt(idStr)
  except ValueError as e: die("invalid --id: " & e.msg)
  if nodeId < 1 or nodeId > 65535:
    die("--id must be 1..65535")

  let r = c.removeNode(uint16(nodeId))
  if r.isErr:
    die("protocol error: " & $r.error, 2)
  let resp = r.value
  if not resp.success:
    writeLine(stderr, "server refused: " & resp.message)
    quit(3)
  echo resp.message

proc cmdNodeList(c: ProtocolClient) =
  let r = c.listNodes()
  if r.isErr:
    die("protocol error: " & $r.error, 2)
  let resp = r.value
  if resp.nodes.len == 0:
    echo "(no nodes registered)"
    return
  echo "    ID  HOST                              RAFT  CLIENT  STATUS"
  echo "------  --------------------------------  ----  ------  ------"
  for n in resp.nodes:
    echo &"{n.nodeId:>6}  {n.host:<32}  {n.raftPort:>6}  {n.clientPort:>6}  {statusStr(n.status)}"

proc cmdRebalanceStatus(c: ProtocolClient) =
  let r = c.rebalanceStatus()
  if r.isErr:
    die("protocol error: " & $r.error, 2)
  let resp = r.value
  echo &"pending:     {resp.pending}"
  echo &"in_progress: {resp.inProgress}"
  echo &"completed:   {resp.completed}"
  echo &"failed:      {resp.failed}"

proc cmdInfo(c: ProtocolClient) =
  let r = c.serverInfo()
  if r.isErr:
    die("protocol error: " & $r.error, 2)
  let resp = r.value
  let roleStr = case resp.role
    of adminMsgs.RoleLeader: "leader"
    of adminMsgs.RoleFollower: "follower"
    of adminMsgs.RoleCandidate: "candidate"
    else: "unknown"
  echo &"node_id:      {resp.nodeId}"
  echo &"version:      {resp.version}"
  echo &"uptime_secs:  {resp.uptimeSecs}"
  echo &"role:         {roleStr}"
  echo &"shard_count:  {resp.shardCount}"
  echo &"client_count: {resp.clientCount}"

proc cmdHealth(c: ProtocolClient) =
  let r = c.health()
  if r.isErr:
    die("protocol error: " & $r.error, 2)
  let resp = r.value
  let statusStr2 = case resp.status
    of adminMsgs.HealthOK: "OK"
    of adminMsgs.HealthDegraded: "DEGRADED"
    of adminMsgs.HealthCritical: "CRITICAL"
    else: "UNKNOWN"
  echo &"status:            {statusStr2}"
  echo &"leader_ok:         {resp.leaderOK}"
  echo &"replica_count:     {resp.replicaCount}"
  echo &"healthy_replicas:  {resp.healthyReplicas}"
  echo &"cluster_name:      {resp.clusterName}"

proc cmdMetrics(c: ProtocolClient) =
  let r = c.metrics()
  if r.isErr:
    die("protocol error: " & $r.error, 2)
  let resp = r.value
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
# Argument parsing and dispatch
# ---------------------------------------------------------------------------

proc main() =
  var globalHost = "127.0.0.1"
  var globalPort = 9000
  var globalTimeout = 10_000

  # Collect positional args and named flags separately
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
      of "host": globalHost = val
      of "port":
        try: globalPort = parseInt(val)
        except ValueError: die("--port must be an integer")
      of "timeout":
        try: globalTimeout = parseInt(val)
        except ValueError: die("--timeout must be an integer")
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

  # Connect to server
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

  # Dispatch command
  let cmd = positional[0]
  case cmd

  of "node":
    if positional.len < 2:
      die("node requires a subcommand: join | remove | list")
    case positional[1]
    of "join":
      cmdNodeJoin(client, flags)
    of "remove":
      cmdNodeRemove(client, flags)
    of "list":
      cmdNodeList(client)
    else:
      die("unknown node subcommand: " & positional[1])

  of "rebalance":
    if positional.len < 2:
      die("rebalance requires a subcommand: status")
    case positional[1]
    of "status":
      cmdRebalanceStatus(client)
    else:
      die("unknown rebalance subcommand: " & positional[1])

  of "info":
    cmdInfo(client)

  of "health":
    cmdHealth(client)

  of "metrics":
    cmdMetrics(client)

  else:
    die("unknown command: " & cmd)

  client.disconnect()

main()
