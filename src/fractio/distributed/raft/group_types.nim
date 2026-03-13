# Group Types for Multi-Group Raft
#
# This module defines the core types for group-based replication.
# Each group is an independent Raft group, enabling horizontal scalability.

import std/hashes
import std/strutils
import std/json
import std/options

# ============================================================================
# Byte sequence comparison utilities
# ============================================================================

proc `<`*(a, b: seq[byte]): bool =
  ## Lexicographic comparison of byte sequences
  let minLen = min(a.len, b.len)
  for i in 0..<minLen:
    if a[i] < b[i]:
      return true
    if a[i] > b[i]:
      return false
  return a.len < b.len

proc `<=`*(a, b: seq[byte]): bool =
  ## Lexicographic comparison of byte sequences
  let minLen = min(a.len, b.len)
  for i in 0..<minLen:
    if a[i] < b[i]:
      return true
    if a[i] > b[i]:
      return false
  return a.len <= b.len

proc `>=`*(a, b: seq[byte]): bool =
  ## Lexicographic comparison of byte sequences
  not (a < b)

proc `>`*(a, b: seq[byte]): bool =
  ## Lexicographic comparison of byte sequences
  not (a <= b)

type
  NodeID* = distinct uint32
    ## Unique identifier for a node in the cluster.
    ## Valid range: 1..uint32.high (0 is reserved for invalid/unknown)

  GroupID* = distinct uint64
    ## Unique identifier for a Raft group.
    ## Group IDs are monotonically increasing and never reused.

  ReplicaID* = distinct uint32
    ## Unique identifier for a replica within a group.
    ## Each replica has a unique ID within its group, even if on the same node.

  ReplicaType* = enum
    ## Type of replica in a Raft group
    rtVoter    ## Participates in Raft quorum (default)
    rtNonVoter ## For follower reads, no quorum participation

  ReplicaDescriptor* = object
    ## Describes a single replica of a group
    nodeId*: NodeID           ## Which node hosts this replica
    replicaId*: ReplicaID     ## Unique ID within the group
    replicaType*: ReplicaType ## Voter or non-voter

  GroupDescriptor* = ref object
    ## Metadata describing a Raft group.
    ## This is the authoritative source of truth for group configuration.
    groupId*: GroupID ## Unique group identifier
    replicas*: seq[ReplicaDescriptor] ## All replicas of this group
    nextReplicaId*: ReplicaID ## Next replica ID to allocate
    generation*: uint64 ## Incremented on every change
    preferredLeader*: NodeID ## Optional: target node for leadership rebalancing
    leader*: NodeID ## Last known leader node (updated via AE heartbeats or election)

# ============================================================================
# NodeID operations
# ============================================================================

proc `$`*(id: NodeID): string =
  ## String representation of NodeID
  result = "n" & $id.uint32

proc parseNodeID*(s: string): NodeID =
  ## Parse NodeID from string format "n<number>"
  if s.len < 2 or s[0] != 'n':
    raise newException(ValueError, "Invalid NodeID format: " & s)
  result = NodeID(parseInt(s[1..^1]))

proc hash*(id: NodeID): Hash =
  ## Hash for use in tables
  result = hash(id.uint32)

proc `==`*(a, b: NodeID): bool =
  ## Equality comparison
  a.uint32 == b.uint32

proc `<`*(a, b: NodeID): bool =
  ## Less than comparison
  a.uint32 < b.uint32

proc `<=`*(a, b: NodeID): bool =
  ## Less than or equal comparison
  a.uint32 <= b.uint32

proc invalidNodeID*: NodeID =
  ## Returns an invalid/unknown NodeID (0)
  NodeID(0)

proc isValid*(id: NodeID): bool =
  ## Check if NodeID is valid (non-zero)
  id.uint32 > 0

# ============================================================================
# GroupID operations
# ============================================================================

proc `$`*(id: GroupID): string =
  ## String representation of GroupID
  result = "r" & $id.uint64

proc parseGroupID*(s: string): GroupID =
  ## Parse GroupID from string format "r<number>"
  if s.len < 2 or s[0] != 'r':
    raise newException(ValueError, "Invalid GroupID format: " & s)
  result = GroupID(parseInt(s[1..^1]))

proc hash*(id: GroupID): Hash =
  ## Hash for use in tables
  result = hash(id.uint64)

proc `==`*(a, b: GroupID): bool =
  ## Equality comparison
  a.uint64 == b.uint64

proc `<`*(a, b: GroupID): bool =
  ## Less than comparison
  a.uint64 < b.uint64

proc `<=`*(a, b: GroupID): bool =
  ## Less than or equal comparison
  a.uint64 <= b.uint64

proc firstGroupID*: GroupID =
  ## Returns the first valid GroupID
  GroupID(1)

# ============================================================================
# ReplicaID operations
# ============================================================================

proc `$`*(id: ReplicaID): string =
  ## String representation of ReplicaID
  result = "rep" & $id.uint32

proc parseReplicaID*(s: string): ReplicaID =
  ## Parse ReplicaID from string format "rep<number>"
  if s.len < 4 or s[0..2] != "rep":
    raise newException(ValueError, "Invalid ReplicaID format: " & s)
  result = ReplicaID(parseInt(s[3..^1]))

proc hash*(id: ReplicaID): Hash =
  ## Hash for use in tables
  result = hash(id.uint32)

proc `==`*(a, b: ReplicaID): bool =
  ## Equality comparison
  a.uint32 == b.uint32

proc `<`*(a, b: ReplicaID): bool =
  ## Less than comparison
  a.uint32 < b.uint32

proc firstReplicaID*: ReplicaID =
  ## Returns the first valid ReplicaID
  ReplicaID(1)

proc next*(id: var ReplicaID): ReplicaID =
  ## Get current ReplicaID and increment for next use
  result = id
  inc id.uint32

# ============================================================================
# ReplicaDescriptor operations
# ============================================================================

proc newReplicaDescriptor*(nodeId: NodeID, replicaId: ReplicaID,
                           replicaType: ReplicaType = rtVoter): ReplicaDescriptor =
  ## Create a new ReplicaDescriptor
  result = ReplicaDescriptor(
    nodeId: nodeId,
    replicaId: replicaId,
    replicaType: replicaType
  )

proc isVoter*(rep: ReplicaDescriptor): bool =
  ## Check if this replica is a voter
  rep.replicaType == rtVoter

proc `==`*(a, b: ReplicaDescriptor): bool =
  ## Equality comparison
  a.nodeId == b.nodeId and a.replicaId == b.replicaId

proc hash*(rep: ReplicaDescriptor): Hash =
  ## Hash for use in tables
  var h: Hash = 0
  h = h !& hash(rep.nodeId)
  h = h !& hash(rep.replicaId)
  result = !$h

proc toJson*(rep: ReplicaDescriptor): JsonNode =
  ## Serialize ReplicaDescriptor to JSON
  result = %*{
    "nodeId": rep.nodeId.uint32,
    "replicaId": rep.replicaId.uint32,
    "replicaType": ord(rep.replicaType)
  }

proc parseReplicaDescriptor*(json: JsonNode): ReplicaDescriptor =
  ## Parse ReplicaDescriptor from JSON
  result = ReplicaDescriptor(
    nodeId: NodeID(json["nodeId"].getInt()),
    replicaId: ReplicaID(json["replicaId"].getInt()),
    replicaType: ReplicaType(json["replicaType"].getInt())
  )

# ============================================================================
# GroupDescriptor operations
# ============================================================================

proc newGroupDescriptor*(groupId: GroupID,
                         replicas: seq[ReplicaDescriptor] = @[]): GroupDescriptor =
  ## Create a new GroupDescriptor
  new(result)
  result.groupId = groupId
  result.replicas = replicas
  result.nextReplicaId = firstReplicaID()
  result.generation = 1

proc addReplica*(desc: GroupDescriptor, nodeId: NodeID,
                  replicaType: ReplicaType = rtVoter): ReplicaDescriptor =
  ## Add a new replica to the group. Returns the new replica descriptor.
  ## If a replica with this nodeId already exists, returns the existing one.
  for rep in desc.replicas:
    if rep.nodeId == nodeId:
      return rep
  result = ReplicaDescriptor(
    nodeId: nodeId,
    replicaId: desc.nextReplicaId,
    replicaType: replicaType
  )
  desc.replicas.add(result)
  inc desc.nextReplicaId.uint32
  inc desc.generation

proc removeReplica*(desc: GroupDescriptor, replicaId: ReplicaID): bool =
  ## Remove a replica from the group. Returns true if found and removed.
  for i, rep in desc.replicas:
    if rep.replicaId == replicaId:
      desc.replicas.delete(i)
      inc desc.generation
      return true
  return false

proc getReplica*(desc: GroupDescriptor, nodeId: NodeID): Option[
    ReplicaDescriptor] =
  ## Get replica by node ID
  for rep in desc.replicas:
    if rep.nodeId == nodeId:
      return some(rep)
  return none(ReplicaDescriptor)

proc getVoters*(desc: GroupDescriptor): seq[ReplicaDescriptor] =
  ## Get all voter replicas
  for rep in desc.replicas:
    if rep.isVoter:
      result.add(rep)

proc getNonVoters*(desc: GroupDescriptor): seq[ReplicaDescriptor] =
  ## Get all non-voter replicas
  for rep in desc.replicas:
    if not rep.isVoter:
      result.add(rep)

proc isInitialized*(desc: GroupDescriptor): bool =
  ## Check if the descriptor is properly initialized
  desc.groupId.uint64 > 0 and desc.replicas.len > 0

proc quorumSize*(desc: GroupDescriptor): int =
  ## Calculate quorum size (majority of voters)
  let voters = desc.getVoters()
  result = (voters.len div 2) + 1

proc toJson*(desc: GroupDescriptor): JsonNode =
  ## Serialize GroupDescriptor to JSON
  var replicasJson = newJArray()
  for rep in desc.replicas:
    replicasJson.add(rep.toJson())

  result = %*{
    "groupId": desc.groupId.uint64,
    "replicas": replicasJson,
    "nextReplicaId": desc.nextReplicaId.uint32,
    "generation": desc.generation
  }
  if desc.preferredLeader.isValid:
    result["preferredLeader"] = newJInt(int(desc.preferredLeader.uint32))
  if desc.leader.isValid:
    result["leader"] = newJInt(int(desc.leader.uint32))

proc parseGroupDescriptor*(json: JsonNode): GroupDescriptor =
  ## Parse GroupDescriptor from JSON
  new(result)
  result.groupId = GroupID(json["groupId"].getBiggestInt().uint64)

  # Parse replicas
  for repJson in json["replicas"]:
    result.replicas.add(parseReplicaDescriptor(repJson))

  result.nextReplicaId = ReplicaID(json["nextReplicaId"].getInt())
  result.generation = uint64(json["generation"].getBiggestInt())

  if json.hasKey("preferredLeader"):
    result.preferredLeader = NodeID(uint32(json["preferredLeader"].getInt()))
  if json.hasKey("leader"):
    result.leader = NodeID(uint32(json["leader"].getInt()))

proc `$`*(desc: GroupDescriptor): string =
  ## String representation of GroupDescriptor
  result = "GroupDescriptor(" & $desc.groupId & ", " &
    "replicas=" & $desc.replicas.len & ", " &
    "gen=" & $desc.generation & ")"

proc isMetaGroup*(desc: GroupDescriptor): bool =
  ## Check whether this GroupDescriptor covers the meta group (Group 1).
  ## The meta group stores system catalog tables and is replicated on all nodes.
  desc.groupId == GroupID(1)

# ============================================================================
# Key encoding utilities
# ============================================================================

proc encodeGroupPrefix*(groupId: GroupID): string =
  ## Encode group prefix for keys: /range/<group_id>/
  result = "/range/" & $groupId.uint64 & "/"

proc encodeDataKey*(groupId: GroupID, key: seq[byte]): string =
  ## Encode a data key with group prefix: /range/<group_id>/data/<key>
  var keyStr = newString(key.len)
  for i, b in key:
    keyStr[i] = char(b)
  result = encodeGroupPrefix(groupId) & "data/" & keyStr

proc encodeLogKey*(groupId: GroupID, index: uint64): string =
  ## Encode a log key: /raft/<group_id>/log/<index>
  result = "/raft/" & $groupId.uint64 & "/log/" & $index

proc encodeStateKey*(groupId: GroupID): string =
  ## Encode a state key for persistent Raft state: /raft/<group_id>/state
  result = "/raft/" & $groupId.uint64 & "/state"

proc encodeSnapshotKey*(groupId: GroupID): string =
  ## Encode a snapshot key: /raft/<group_id>/snapshot
  result = "/raft/" & $groupId.uint64 & "/snapshot"

proc parseLogIndex*(key: string): uint64 =
  ## Parse log index from a log key
  ## Expected format: /raft/<group_id>/log/<index>
  let parts = key.split('/')
  if parts.len >= 5 and parts[^2] == "log":
    result = parseBiggestUInt(parts[^1])
  else:
    raise newException(ValueError, "Invalid log key format: " & key)
