# Client Routing Logic
#
# Pure routing functions extracted from fractio_client.nim.
# These functions are fully testable without network I/O.
# They operate on RoutingState which can be mocked for testing.

import std/[tables as stdtables, hashes, strutils]
import ../core/types
import ../distributed/raft/group_types
import ../distributed/meta/system_tables

# ---------------------------------------------------------------------------
# Routing state (snapshot of client metadata)
# ---------------------------------------------------------------------------

type
  TableRoutingInfo* = object
    ## Information about a table needed for routing
    name*: string
    spaceId*: SpaceID

  SpaceRoutingInfo* = object
    ## Information about a space needed for routing
    name*: string
    groupIds*: seq[GroupID]
    oldGroupIds*: seq[GroupID]
      ## Old groups during rebalancing (empty if not rebalancing)
    rebalancing*: bool
      ## Whether the space is currently rebalancing

  RoutingState* = object
    ## Snapshot of client state needed for routing decisions.
    ## This is a pure data structure that can be constructed for testing.
    tables*: stdtables.Table[TableId, TableRoutingInfo]
    spaces*: stdtables.Table[SpaceID, SpaceRoutingInfo]

proc initRoutingState*(): RoutingState =
  ## Create an empty routing state.
  result = RoutingState(
    tables: stdtables.initTable[TableId, TableRoutingInfo](),
    spaces: stdtables.initTable[SpaceID, SpaceRoutingInfo]()
  )

proc addTable*(state: var RoutingState, tableId: TableId, name: string,
    spaceId: SpaceID) =
  ## Add a table to the routing state.
  state.tables[tableId] = TableRoutingInfo(name: name, spaceId: spaceId)

proc addSpace*(state: var RoutingState, spaceId: SpaceID, name: string,
               groupIds: seq[GroupID], oldGroupIds: seq[GroupID] = @[],
               rebalancing: bool = false) =
  ## Add a space to the routing state.
  state.spaces[spaceId] = SpaceRoutingInfo(
    name: name,
    groupIds: groupIds,
    oldGroupIds: oldGroupIds,
    rebalancing: rebalancing
  )

proc isValidSpaceId*(spaceId: SpaceID): bool =
  ## Check if a SpaceID is valid (not all zeros).
  var ulid = ULID(spaceId)
  for b in ulid.data:
    if b != 0:
      return true
  false

# ---------------------------------------------------------------------------
# Pure routing functions
# ---------------------------------------------------------------------------

proc routeToGroup*(primaryKey: string, groupIds: seq[GroupID]): GroupID =
  ## Hash-route a primary key to one of the space's groups.
  ## Pure function - fully testable.
  ##
  ## primaryKey should be the bare key value (e.g., "1" not "/t/0000000100/d/1")
  if groupIds.len == 0:
    return META_GROUP_ID
  if groupIds.len == 1:
    return groupIds[0]
  let h = hash(primaryKey)
  let idx = abs(h) mod groupIds.len
  groupIds[idx]

proc getGroupForKey*(state: RoutingState, key: string): GroupID =
  ## Determine which group owns a given key using routing state.
  ## Pure function - fully testable with RoutingState.
  ##
  ## For data row keys with embedded groupId, extracts the groupId directly.
  ## For scan-bound keys without groupId (e.g. /t/<tableId>/d/<pk>), uses
  ## hash routing on the primary key.
  ##
  ## Returns META_GROUP_ID if the group cannot be determined.

  # System tables (tableId 1-7) are in the meta group
  if key.startsWith(TABLE_KEY_PREFIX):
    let afterPrefix = key[TABLE_KEY_PREFIX.len .. ^1]
    if afterPrefix.len >= TABLE_ID_WIDTH:
      try:
        let tableIdStr = afterPrefix[0 ..< TABLE_ID_WIDTH]
        let tableId = tableIdFromString(tableIdStr)
        if isMetaGroupTableId(tableId):
          return META_GROUP_ID
        else:
          # For data tables, look up table->space->group mapping
          if tableId in state.tables:
            let tableInfo = state.tables[tableId]
            let spaceId = tableInfo.spaceId
            if isValidSpaceId(spaceId) and spaceId in state.spaces:
              let spaceInfo = state.spaces[spaceId]
              let activeGroupIds = if spaceInfo.rebalancing and
                  spaceInfo.oldGroupIds.len > 0:
                # During rebalancing, data still lives in old groups; route reads there
                spaceInfo.oldGroupIds
              else:
                spaceInfo.groupIds

              if activeGroupIds.len > 0:
                # All stored data row keys include the group ID:
                #   /t/<tableId>/d/<groupId>/<pk>
                # Scan-bound keys (start/end of ranges) may omit it:
                #   /t/<tableId>/d/<pk>
                #   /t/<tableId>/d/
                let afterTableId = afterPrefix[TABLE_ID_WIDTH .. ^1]
                var pk = afterTableId

                # Strip "/" prefix if present
                if pk.len > 0 and pk[0] == '/':
                  pk = pk[1 .. ^1]

                # Strip "d/" prefix if present (data rows)
                if pk.startsWith("d/"):
                  pk = pk[2 .. ^1]

                  # Check for embedded groupId (26-char ULID + "/")
                  if pk.len >= 27 and pk[26] == '/':
                    # Key has embedded groupId — extract and route directly
                    try:
                      let embeddedGroupId = parseGroupID(pk[0 ..< 26])
                      # Verify the embedded group is in the active groups
                      for gid in activeGroupIds:
                        if gid == embeddedGroupId:
                          return embeddedGroupId
                      # During rebalancing, check old groups too
                      if spaceInfo.rebalancing:
                        for gid in spaceInfo.oldGroupIds:
                          if gid == embeddedGroupId:
                            return embeddedGroupId
                    except ValueError:
                      discard
                    # Embedded groupId not in any active group — fall through
                    # to hash routing using the bare pk
                    pk = pk[27 .. ^1] # Skip groupId + "/" for hashing

                # Hash-based routing for scan-bound keys without groupId
                return routeToGroup(pk, activeGroupIds)

          # Fall back to default data group for tables without space assignment
          return DATA_GROUP_START_ID
      except ValueError:
        discard

  # Default to meta group for non-table keys or if parsing failed
  return META_GROUP_ID

proc getGroupsForTable*(state: RoutingState, tableId: TableId): seq[GroupID] =
  ## Get all groups that store data for a given table.
  ## Pure function - fully testable with RoutingState.
  ##
  ## For multi-group spaces, returns ALL groups in the space.
  ## During rebalancing, includes BOTH old and new groups for dual-read mode.
  ## Returns empty seq if the table is not found.

  if tableId in state.tables:
    let tableInfo = state.tables[tableId]
    let spaceId = tableInfo.spaceId

    # Check if spaceId is valid
    if isValidSpaceId(spaceId) and spaceId in state.spaces:
      let spaceInfo = state.spaces[spaceId]

      # During rebalancing, return only old groups for full scans.
      # Old groups contain all existing data; new groups are empty
      # until migration runs. Scanning both would return duplicates
      # because keyRoutesToGroupIdDuringRebalance allows old-group
      # keys to appear in new-group scans (dual-read for point gets).
      if spaceInfo.rebalancing and spaceInfo.oldGroupIds.len > 0:
        return spaceInfo.oldGroupIds
      else:
        return spaceInfo.groupIds

  # System tables (1-7) are in META_GROUP_ID
  if isMetaGroupTableId(tableId):
    return @[META_GROUP_ID]

  # Fall back to default data group for tables without space assignment
  return @[DATA_GROUP_START_ID]

proc getTableIdFromKey*(key: string): TableId =
  ## Extract tableId from a key.
  ## Pure function - fully testable.
  ##
  ## Returns zeroTableId if not parseable.
  if not key.startsWith(TABLE_KEY_PREFIX):
    return zeroTableId()

  let afterPrefix = key[TABLE_KEY_PREFIX.len .. ^1]
  if afterPrefix.len < TABLE_ID_WIDTH:
    return zeroTableId()

  try:
    let tableIdStr = afterPrefix[0 ..< TABLE_ID_WIDTH]
    return tableIdFromString(tableIdStr)
  except ValueError:
    return zeroTableId()

# ---------------------------------------------------------------------------
# Key parsing helpers
# ---------------------------------------------------------------------------

proc parseTableKey*(key: string): tuple[tableId: TableId, pk: string] =
  ## Parse a table key into tableId and primary key.
  ## Pure function - fully testable.
  ##
  ## Returns (zeroTableId, "") if not a valid table key.
  if not key.startsWith(TABLE_KEY_PREFIX):
    return (zeroTableId(), "")

  let afterPrefix = key[TABLE_KEY_PREFIX.len .. ^1]
  if afterPrefix.len < TABLE_ID_WIDTH:
    return (zeroTableId(), "")

  try:
    let tableIdStr = afterPrefix[0 ..< TABLE_ID_WIDTH]
    let tableId = tableIdFromString(tableIdStr)

    # Skip "/" after tableId
    var pk = ""
    if afterPrefix.len > TABLE_ID_WIDTH:
      pk = afterPrefix[TABLE_ID_WIDTH .. ^1]
      # Strip leading "/" if present
      if pk.len > 0 and pk[0] == '/':
        pk = pk[1 .. ^1]
      # Strip "d/" prefix for data rows
      if pk.startsWith("d/"):
        pk = pk[2 .. ^1]

    return (tableId: tableId, pk: pk)
  except ValueError:
    return (zeroTableId(), "")

# ---------------------------------------------------------------------------
# Routing validation helpers
# ---------------------------------------------------------------------------

proc isRoutingKey*(key: string): bool =
  ## Check if a key can be routed (starts with /t/).
  key.startsWith(TABLE_KEY_PREFIX)

proc needsRouting*(state: RoutingState, key: string): bool =
  ## Check if a key needs routing (not a meta group key).
  getGroupForKey(state, key) != META_GROUP_ID

proc isMultiGroup*(state: RoutingState, tableId: TableId): bool =
  ## Check if a table is in a multi-group space.
  let groups = getGroupsForTable(state, tableId)
  groups.len > 1

proc isRebalancing*(state: RoutingState, tableId: TableId): bool =
  ## Check if a table's space is currently rebalancing.
  if tableId in state.tables:
    let tableInfo = state.tables[tableId]
    let spaceId = tableInfo.spaceId
    if isValidSpaceId(spaceId) and spaceId in state.spaces:
      return state.spaces[spaceId].rebalancing
  false
