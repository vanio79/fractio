# Space Manager for Fractio
#
# Handles server-side CREATE SPACE and DROP SPACE operations.
# These operations require coordination across all nodes and creation
# of Raft groups, so they must be handled on the server rather than
# via direct client KV operations.
#
# Flow for CREATE SPACE:
#   1. Client sends CreateSpaceRequest to any node
#   2. If not META leader, return ErrNotLeader
#   3. META leader validates request (no duplicate name, valid replicas)
#   4. META leader writes group records to sys.groups via Raft
#   5. All nodes observe sys.groups writes via applyBatchToSM callback
#   6. Each node creates local Raft group instances
#   7. META leader waits for all new groups to have leaders
#   8. META leader writes space record to sys.spaces via Raft
#   9. Return success with updated sys table data to client
#
# Flow for DROP SPACE:
#   1. Client sends DropSpaceRequest to any node
#   2. If not META leader, return ErrNotLeader
#   3. META leader validates space exists and is not "default"
#   4. META leader marks space record as deleted in sys.spaces
#   5. META leader marks all group records as deleted in sys.groups
#   6. All nodes observe deletions and stop local Raft group instances
#   7. Return success with deleted groupIds to client

import std/[tables, options, os, times, sequtils, strutils]
import ../protocol/raft_store
import ../protocol/messages/space
import ../distributed/raft/nuraft_coordinator
import ../distributed/raft/group_types
import ../distributed/meta/system_tables
import ../distributed/meta/system_schemas
import ../storage/mvcc/types as mvccTypes
import ../utils/logging
import ../core/types

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

const
  DEFAULT_LEADER_WAIT_MS = 5000 ## Max time to wait for leaders
  LEADER_POLL_INTERVAL_MS = 50  ## How often to check for leaders
  DEFAULT_SPACE_REPLICAS = 3    ## Default replication factor

# ---------------------------------------------------------------------------
# Space Manager
# ---------------------------------------------------------------------------

type
  SpaceManager* = ref object
    ## Manages space creation and deletion on a single node.
    ## Must be created on the META leader node.
    store*: RaftKVStoreExt ## Raft KV store for sys table writes
    coord*: NuRaftCoordinator ## Coordinator for group management
    nodeId*: uint32 ## This node's ID
    logger*: Logger ## Optional logger

proc newSpaceManager*(store: RaftKVStoreExt, coord: NuRaftCoordinator,
    nodeId: uint32, logger: Logger = nil): SpaceManager =
  ## Create a new SpaceManager.
  result = SpaceManager(
    store: store,
    coord: coord,
    nodeId: nodeId,
    logger: logger
  )

# ---------------------------------------------------------------------------
# Helper procs
# ---------------------------------------------------------------------------

proc safeLog(sm: SpaceManager, level: LogLevel, msg: string) {.raises: [].} =
  ## Safe logging that catches any exceptions.
  if sm.logger != nil:
    try:
      sm.logger.log(level, msg)
    except Exception:
      discard

proc logInfo(sm: SpaceManager, msg: string) {.raises: [].} =
  sm.safeLog(llInfo, msg)

proc logError(sm: SpaceManager, msg: string) {.raises: [].} =
  sm.safeLog(llError, msg)

proc safeFmt(fmtStr: string, args: varargs[string, `$`]): string {.raises: [].} =
  ## Safe string formatting that doesn't raise.
  try:
    case args.len
    of 0: result = fmtStr
    of 1: result = fmtStr % [args[0]]
    of 2: result = fmtStr % [args[0], args[1]]
    of 3: result = fmtStr % [args[0], args[1], args[2]]
    of 4: result = fmtStr % [args[0], args[1], args[2], args[3]]
    else: result = fmtStr & " (too many args)"
  except CatchableError:
    result = fmtStr

proc deriveULID(base: ULID, index: int): ULID =
  ## Derive a deterministic ULID from a base ULID and an index.
  ## This ensures groups created for a space have predictable, collision-free ports.
  ## The port hash is based on the ULID bytes, so we use the last 8 bytes for the index
  ## to get distinct port offsets (0-999) for different indices.
  result = base
  # XOR the index into the last 8 bytes to create distinct but deterministic IDs
  let idxBytes = cast[array[8, uint8]](uint64(index))
  for i in 0 ..< 8:
    result.data[8 + i] = result.data[8 + i] xor idxBytes[i]

proc isMetaLeader(sm: SpaceManager): bool =
  ## Check if this node is the leader of the META group.
  sm.coord.isLeader(META_GROUP_ID)

proc getNodes*(sm: SpaceManager): seq[NodeRecord] =
  ## Get all nodes from sys.nodes.
  let startKey = encodeTableKey(SYS_NODES_TABLE_ID, "")
  let endKey = encodeTableKey(SYS_NODES_TABLE_ID + 1, "")
  let scanRes = sm.store.raftScan(startKey, endKey, 0, includeSystemKeys = true)
  if scanRes.isOk:
    for (key, entry) in scanRes.value:
      var data = entry.value
      # Strip MVCC header if present
      if mvccTypes.isLikelyMVCCValue(data):
        let mvccVal = mvccTypes.decodeMVCCValue(data)
        if not mvccVal.isDeleted:
          data = mvccVal.data
        else:
          continue
      try:
        result.add(decodeNodeRecord(data))
      except CatchableError:
        discard

proc getGroups*(sm: SpaceManager): seq[GroupRecord] =
  ## Get all groups from sys.groups.
  let startKey = encodeTableKey(SYS_GROUPS_TABLE_ID, "")
  let endKey = encodeTableKey(SYS_GROUPS_TABLE_ID + 1, "")
  let scanRes = sm.store.raftScan(startKey, endKey, 0, includeSystemKeys = true)
  if scanRes.isOk:
    for (key, entry) in scanRes.value:
      var data = entry.value
      if mvccTypes.isLikelyMVCCValue(data):
        let mvccVal = mvccTypes.decodeMVCCValue(data)
        if not mvccVal.isDeleted:
          data = mvccVal.data
        else:
          continue
      try:
        result.add(decodeGroupRecord(data))
      except CatchableError:
        discard

proc getSpaces*(sm: SpaceManager): seq[SpaceRecord] =
  ## Get all spaces from sys.spaces.
  let startKey = encodeTableKey(SYS_SPACES_TABLE_ID, "")
  let endKey = encodeTableKey(SYS_SPACES_TABLE_ID + 1, "")
  let scanRes = sm.store.raftScan(startKey, endKey, 0, includeSystemKeys = true)
  if scanRes.isOk:
    for (key, entry) in scanRes.value:
      var data = entry.value
      if mvccTypes.isLikelyMVCCValue(data):
        let mvccVal = mvccTypes.decodeMVCCValue(data)
        if not mvccVal.isDeleted:
          data = mvccVal.data
        else:
          continue
      try:
        result.add(decodeSpaceRecord(data))
      except CatchableError:
        discard

proc findSpaceByName*(sm: SpaceManager, name: string): Option[SpaceRecord] =
  ## Find a space by name.
  for space in sm.getSpaces():
    if space.name == name:
      return some(space)
  return none(SpaceRecord)

proc computeGroupPlacement(nodeIds: seq[uint32], replicas: int,
    spaceId: ULID): seq[GroupRecord] {.gcsafe.} =
  ## Compute group placement using ring algorithm.
  ## N nodes → N groups, each with R replicas placed in a ring.
  let nodeCount = nodeIds.len
  let actualReplicas = if replicas <= 0 or replicas >
      nodeCount: nodeCount else: replicas
  let groupCount = nodeCount

  result = newSeqOfCap[GroupRecord](groupCount)

  for g in 0 ..< groupCount:
    # Use deterministic ULID derived from spaceId + group index
    # This ensures predictable port assignment and avoids collisions
    let groupId = deriveULID(spaceId, g)

    # Compute members using ring algorithm
    var members: seq[GroupReplicaBin] = @[]
    for j in 0 ..< actualReplicas:
      let nodeIdx = (g + j) mod nodeCount
      members.add(GroupReplicaBin(
        nodeId: nodeIds[nodeIdx],
        replicaType: rtVoter
      ))

    result.add(GroupRecord(
      groupId: groupId,
      spaceId: spaceId,
      preferredLeader: members[0].nodeId,
      leader: 0,
      replicas: members
    ))

proc waitForGroupLeaders(sm: SpaceManager, groupIds: seq[GroupID],
    timeoutMs: int = DEFAULT_LEADER_WAIT_MS): bool =
  ## Wait for all specified groups to have elected leaders.
  let deadlineMs = int(getTime().toUnix() * 1000) + timeoutMs

  while true:
    var allHaveLeaders = true
    var anyGroupMissing = false
    for gid in groupIds:
      # First check if group exists
      if not sm.coord.hasGroup(gid):
        anyGroupMissing = true
        allHaveLeaders = false
        break
      let leaderId = sm.coord.getLeader(gid)
      if leaderId < 0:
        allHaveLeaders = false
        break

    if allHaveLeaders:
      return true

    let nowMs = int(getTime().toUnix() * 1000)
    if nowMs >= deadlineMs:
      # Log which groups are missing or have no leader
      for gid in groupIds:
        if not sm.coord.hasGroup(gid):
          sm.logError(safeFmt("waitForGroupLeaders: group $# does not exist", $gid))
        elif sm.coord.getLeader(gid) < 0:
          sm.logError(safeFmt("waitForGroupLeaders: group $# has no leader", $gid))
      return false

    sleep(LEADER_POLL_INTERVAL_MS)

proc getMemberEndpoints(sm: SpaceManager, groupRec: GroupRecord): seq[tuple[
    nodeId: uint32, host: string, basePort: int]] =
  ## Get endpoints for all members of a group.
  let nodes = sm.getNodes()
  var nodeMap = initTable[uint32, NodeRecord]()
  for n in nodes:
    nodeMap[n.nodeId] = n

  for rep in groupRec.replicas:
    if rep.nodeId in nodeMap:
      let n = nodeMap[rep.nodeId]
      result.add((nodeId: n.nodeId, host: n.host, basePort: int(n.raftPort)))

# ---------------------------------------------------------------------------
# CREATE SPACE
# ---------------------------------------------------------------------------

proc createSpace*(sm: SpaceManager, req: CreateSpaceRequest): CreateSpaceResponse {.gcsafe,
    raises: [].} =
  ## Execute CREATE SPACE on the server.
  ## Must be called on the META leader node.

  # Declare variables needed after try block
  var spaceId: ULID
  var groupCount = 0
  var groupRecs: seq[GroupRecord] = @[]
  var groupIds: seq[ULID] = @[]
  var replicas = 0

  let t0 {.used.} = times.getTime()
  template timedLog(msg: string) =
    when false: # Disabled for gcsafe
      let t1 = times.getTime()
      sm.logInfo(safeFmt("[$# ms] $#", $(t1 - t0).inMilliseconds, msg))

  timedLog(safeFmt("createSpace: starting for '$#'", req.name))

  try:
    # 1. Check if we're the META leader
    timedLog("checking META leadership")
    if not sm.isMetaLeader():
      timedLog("not META leader, returning error")
      return CreateSpaceResponse(
        success: false,
        error: "not the leader for META group"
      )
    timedLog("we are META leader")

    # 2. Check for duplicate name
    timedLog("checking for duplicate space name")
    if sm.findSpaceByName(req.name).isSome:
      return CreateSpaceResponse(
        success: false,
        error: safeFmt("space '$#' already exists", req.name)
      )

    # 3. Get cluster nodes
    timedLog("getting nodes from sys.nodes")
    let nodes = sm.getNodes()
    timedLog(safeFmt("found $# nodes", $nodes.len))
    if nodes.len == 0:
      sm.logError("createSpace: no nodes found, returning error")
      return CreateSpaceResponse(
        success: false,
        error: "no nodes in cluster"
      )

    let nodeIds = nodes.mapIt(it.nodeId)
    let nodeCount = nodeIds.len
    replicas = if req.replicas <= 0: nodeCount else: min(req.replicas.int, nodeCount)
    timedLog(safeFmt("nodeCount=$# replicas=$#", $nodeCount, $replicas))

    if replicas > nodeCount:
      return CreateSpaceResponse(
        success: false,
        error: safeFmt("REPLICAS ($#) exceeds node count ($#)", $replicas, $nodeCount)
      )

    # 4. Generate ULID for spaceId
    timedLog("generating spaceId ULID")
    spaceId = ({.cast(gcsafe).}: genULID())
    timedLog(safeFmt("spaceId=$#", $spaceId))
    groupCount = nodeCount

    # 5. Compute group placement
    timedLog("computing group placement")
    groupRecs = computeGroupPlacement(nodeIds, replicas, spaceId)
    for gr in groupRecs:
      groupIds.add(gr.groupId)

    timedLog(safeFmt("Creating space '$#' with $# groups, replicas=$#",
        req.name, $groupCount, $replicas))

    # 6. Write group records to sys.groups via Raft
    var groupWrites: seq[tuple[key: string, value: string]] = @[]
    for gr in groupRecs:
      let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $gr.groupId)
      let value = encode(gr)
      groupWrites.add((key: key, value: value))

    # Write each group record with MVCC encoding
    timedLog(safeFmt("writing $# group records", $groupWrites.len))
    var writeIdx = 0
    for (key, value) in groupWrites:
      writeIdx += 1
      timedLog(safeFmt("writing group record $#", $writeIdx))
      let ts = int64(times.getTime().toUnixFloat() * 1_000_000_000)
      let encoded = mvccTypes.encodeMVCCValue(value, ts, false)
      timedLog(safeFmt("calling raftPut for group record $#", $writeIdx))
      let res = sm.store.raftPut(key, encoded)
      timedLog(safeFmt("raftPut returned for group record $#", $writeIdx))
      if not res.isOk:
        return CreateSpaceResponse(
          success: false,
          error: safeFmt("failed to write group record to Raft: $#", $res.error)
        )
    timedLog("group records written")

    # 7. Groups are created asynchronously via onGroupMetadataApplied callback
    #    when the Raft entries are committed. We return immediately because
    #    waiting would block the handler thread.

    # 8. Write space record to sys.spaces via Raft
    timedLog("writing space record")
    let spaceRec = SpaceRecord(
      spaceId: spaceId,
      name: req.name,
      replicas: int32(replicas),
      groupCount: int32(groupCount),
      groupIds: groupIds,
      oldGroupIds: @[],
      rebalancing: false,
      createdAtNs: nowNs()
    )
    let spaceKey = encodeTableKey(SYS_SPACES_TABLE_ID, $spaceId)
    let spaceValue = encode(spaceRec)

    if not sm.store.sysTablePut(spaceKey, spaceValue):
      sm.logError("createSpace: sysTablePut failed")
      return CreateSpaceResponse(
        success: false,
        error: "failed to write space record to Raft"
      )

    timedLog(safeFmt("Created space '$#' (spaceId=$#)", req.name, $spaceId))

    # 10. Build response with updated sys table data
    timedLog("building response")
    var groupRecords: seq[GroupRecordData] = @[]
    for gr in groupRecs:
      groupRecords.add(GroupRecordData(
        groupId: gr.groupId,
        record: encode(gr)
      ))

    timedLog("returning success")
    result = CreateSpaceResponse(
      success: true,
      spaceId: spaceId,
      groupCount: int32(groupCount),
      spaceRecord: spaceValue,
      groupRecords: groupRecords
    )
    timedLog("done")
  except CatchableError as e:
    sm.logError(safeFmt("createSpace error: $#", e.msg))
    result = CreateSpaceResponse(
      success: false,
      error: safeFmt("internal error: $#", e.msg)
    )

# ---------------------------------------------------------------------------
# DROP SPACE
# ---------------------------------------------------------------------------

proc dropSpace*(sm: SpaceManager, req: DropSpaceRequest): DropSpaceResponse {.gcsafe,
    raises: [].} =
  ## Execute DROP SPACE on the server.
  ## Must be called on the META leader node.

  try:
    # 1. Check if we're the META leader
    if not sm.isMetaLeader():
      return DropSpaceResponse(
        success: false,
        error: "not the leader for META group"
      )

    # 2. Cannot drop default space
    if req.name == "default":
      return DropSpaceResponse(
        success: false,
        error: "cannot drop the default space"
      )

    # 3. Find the space
    let spaceOpt = sm.findSpaceByName(req.name)
    if spaceOpt.isNone:
      return DropSpaceResponse(
        success: false,
        error: safeFmt("space '$#' not found", req.name)
      )

    let space = spaceOpt.get()
    let spaceId = space.spaceId

    sm.logInfo(safeFmt("Dropping space '$#' (spaceId=$#)", req.name, $spaceId))

    # 4. Mark space record as deleted
    let spaceKey = encodeTableKey(SYS_SPACES_TABLE_ID, $spaceId)
    if not sm.store.sysTableDeleteBatch(@[spaceKey]):
      return DropSpaceResponse(
        success: false,
        error: "failed to delete space record"
      )

    # 5. Mark all group records as deleted
    var groupKeys: seq[string] = @[]
    for gid in space.groupIds:
      groupKeys.add(encodeTableKey(SYS_GROUPS_TABLE_ID, $gid))

    if groupKeys.len > 0:
      if not sm.store.sysTableDeleteBatch(groupKeys):
        sm.logError(safeFmt("Failed to delete some group records for space $#", $spaceId))
        # Continue anyway - space record is deleted

    sm.logInfo(safeFmt("Deleted space '$#' and $# groups", req.name,
        $space.groupIds.len))

    # 6. Build response - ULIDs are used directly
    var deletedGroupIds: seq[ULID] = @[]
    for gid in space.groupIds:
      deletedGroupIds.add(gid)

    result = DropSpaceResponse(
      success: true,
      spaceId: spaceId,
      deletedGroupIds: deletedGroupIds
    )
  except CatchableError as e:
    sm.logError(safeFmt("dropSpace error: $#", e.msg))
    result = DropSpaceResponse(
      success: false,
      error: safeFmt("internal error: $#", e.msg)
    )
