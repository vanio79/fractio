# Test INSERT routing via FractioClient.
# For each row, prints which group the primary key hashes to.
import std/[os, strutils, atomics, tables as stdtables]
import fractio/client/fractio_client
import fractio/client/routing
import fractio/client/sql_client
import fractio/core/types
import fractio/distributed/meta/system_tables
import fractio/distributed/raft/group_types
import fractio/sql/executor
import fractio/sql/planner
import fractio/sql/ast

proc main() =
  let host = "127.0.0.1"
  let port = 9001

  echo "Connecting to ", host, ":", port, "..."
  var cfg = newFractioClientConfig(host, port)
  cfg.connectionTimeoutMs = 5000
  cfg.requestTimeoutMs = 10000
  cfg.maxKvRetries = 10
  let client = newFractioClient(cfg)

  echo "Initializing..."
  if not client.initialize():
    echo "Failed to initialize client"
    quit(1)
  echo "Client initialized"

  echo "Refreshing metadata..."
  discard client.forceMetadataRefresh()
  echo "Metadata refreshed"

  # Print metadata
  echo "Spaces:"
  for sid, sinfo in client.spaces:
    echo "  ", sinfo.name, " (", sinfo.spaceId, "): groups=", sinfo.groupIds.len
    for gid in sinfo.groupIds:
      let ginfo = client.groups.getOrDefault(gid)
      echo "    group ", gid, " -> leader=", ginfo.leaderNodeId

  echo "Tables:"
  for tid, tinfo in client.tables:
    echo "  ", tinfo.name, " (", tinfo.tableId, "): space=", tinfo.spaceId

  # Show routing for sample keys
  let state = client.getRoutingState()
  var groupCounts = stdtables.initTable[GroupID, int]()

  # Find the users table's space
  var usersSpaceId: SpaceID
  var usersTableId: TableId
  for tid, tinfo in client.tables:
    if tinfo.name == "users":
      usersSpaceId = tinfo.spaceId
      usersTableId = tid
      break

  if usersSpaceId.isValidSpaceId and usersSpaceId in state.spaces:
    let spaceInfo = state.spaces[usersSpaceId]
    echo "Users table space groupIds: "
    for gid in spaceInfo.groupIds:
      echo "  ", gid

    for i in 1..30:
      let pk = $i
      let gid = routeToGroup(pk, spaceInfo.groupIds)
      groupCounts[gid] = groupCounts.getOrDefault(gid, 0) + 1
      echo "  pk=", pk, " -> group=", gid

    echo ""
    echo "Group distribution (routing):"
    for gid, count in groupCounts:
      echo "  group ", gid, ": ", count, " rows"

  # Now insert rows via FractioClient and check
  echo ""
  echo "Inserting 30 rows via FractioClient..."
  for i in 1..30:
    let sql = "INSERT INTO myspace.public.users (id, name, email) VALUES (" &
        $i & ", 'user" & $i & "', 'user" & $i & "@example.com')"
    let res = client.query(sql, database = "myspace", schema = "public")
    if res.kind == erkError:
      echo "  INSERT ", i, " ERROR: ", res.error
    elif res.kind == erkModified:
      echo "  INSERT ", i, " OK (", res.count, ")"
    else:
      echo "  INSERT ", i, " unexpected result kind: ", res.kind

  client.close()
  echo "Done"

when isMainModule:
  main()
