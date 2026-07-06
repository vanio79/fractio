## Diagnostic test for DELETE operations
## Tests if GET on scan-bound keys works correctly after heavy INSERT load

import std/[os, strutils, times, tables as stdtables]
import fractio/client/fractio_client
import fractio/core/types
import fractio/distributed/meta/system_tables
import fractio/distributed/meta/system_schemas

proc main() =
  echo "=== DELETE Diagnostic Test ==="
  echo ""

  # Connect to first node
  let client = newFractioClient("127.0.0.1", 9001)

  # Initialize connection and wait for metadata refresh
  echo "Initializing client..."
  var initialized = false
  for attempt in 0 ..< 30:
    if client.initialize():
      initialized = true
      break
    echo "Attempt " & $ (attempt + 1) & " failed, retrying in 500ms..."
    sleep(500)

  if not initialized:
    echo "ERROR: Failed to initialize client after 30 attempts"
    quit(1)

  echo "Client initialized successfully"
  echo ""

  # Check routing state for the users table
  let state = client.getRoutingState()
  echo "=== Routing State ==="
  var foundUsersTable = false
  for tableId, tableInfo in stdtables.pairs(state.tables):
    if "users" in tableInfo.name:
      if not foundUsersTable:
        echo "Found users table (first match):"
        foundUsersTable = true
      # Just check if space exists and has groups
      let spaceExists = tableInfo.spaceId in state.spaces
      echo "  Space: " & $tableInfo.spaceId & ": " & $(
          if spaceExists: "exists" else: "missing")
      if spaceExists:
        let space = state.spaces[tableInfo.spaceId]
        echo "  Has " & $space.groupIds.len & " groups, rebalancing=" &
            $space.rebalancing

  if not foundUsersTable:
    echo "No users table found in routing state!"

  # Try to GET a scan-bound key for the users table by scanning actual data
  echo ""
  echo "=== Testing Key Formats ==="

  # Get system tables table ID (SYS_TABLES = 1)
  let sysTablesTableId = TableId(systemTableULID(1))

  # Scan system tables to find our user table
  let catalogStart = encodeTableKey(sysTablesTableId, "smoke.public.")
  let catalogEnd = encodeTableKey(sysTablesTableId, "smoke.public.{")
  echo "Scanning catalog: [" & catalogStart & ", " & catalogEnd & ")"

  let catalogScan = client.kvScan(catalogStart, catalogEnd, 0)
  var usersTableId: TableId = zeroTableId()
  if catalogScan.isOk and catalogScan.val.len > 0:
    for entry in catalogScan.val:
      try:
        let rec = decodeTableRecord(entry.value)
        echo "Found table: " & rec.name & " with ID " & $rec.tableId
        usersTableId = rec.tableId

        # Try to GET a scan-bound key for this table with PK=1
        let pkBinary = encodeInt32(1)
        let scanBoundKey = encodeDataRowScanBound(rec.tableId, pkBinary)
        echo "Scan-bound key format: " & $scanBoundKey

        # Get group for this key
        let groupId = client.getGroupForKey(scanBoundKey)
        echo "Routed to group: " & $groupId

        # Rewrite with groupId
        let rewrittenKey = addGroupIdToKey(scanBoundKey, groupId)
        echo "Rewritten key format: " & $rewrittenKey

        # Try GET on scan-bound key
        let getRes1 = client.kvGet(scanBoundKey)
        if getRes1.isOk and getRes1.val.isSome:
          echo "SUCCESS: GET on scan-bound key returned value"
          echo "  Value length: " & $getRes1.val.get().len & " bytes"
        else:
          echo "RESULT: GET on scan-bound key - OK=" & $getRes1.isOk &
              ", Some=" & $(if getRes1.val.isSome: "yes" else: "no") &
              ", Err=" & getRes1.err

        # Try GET on rewritten key with groupId
        let getRes2 = client.kvGet(rewrittenKey)
        if getRes2.isOk and getRes2.val.isSome:
          echo "SUCCESS: GET on rewritten key returned value"
        else:
          echo "RESULT: GET on rewritten key - OK=" & $getRes2.isOk &
              ", Some=" & $(if getRes2.val.isSome: "yes" else: "no") &
              ", Err=" & getRes2.err

        # Scan a small range to see actual stored keys
        let scanStart = encodeDataRowScanBound(rec.tableId, "")
        let scanEnd = encodeDataRowScanBound(rec.tableId, "0123456789")
        echo ""
        echo "Scanning first 5 rows: [" & scanStart & ", " & scanEnd & ")"

        let dataScan = client.kvScan(scanStart, scanEnd, 5)
        if dataScan.isOk and dataScan.val.len > 0:
          for i in 0 ..< min(dataScan.val.len, 5):
            echo "Key " & $i & ": " & $dataScan.val[i].key

      except Exception as e:
        echo "Error processing entry: " & e.msg

  if usersTableId.data == 0:
    echo ""
    echo "ERROR: Could not find users table in catalog"

  echo ""
  echo "=== Test Complete ==="

main()
