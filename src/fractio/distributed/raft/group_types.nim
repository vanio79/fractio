# Group Types for Multi-Group Raft
#
# This module defines the core types for group-based replication.
# Each group is an independent Raft group, enabling horizontal scalability.

import std/hashes
import std/strutils
import std/options
import fractio/core/types
import fractio/utils/binary

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

  GroupID* = distinct ULID
    ## Unique identifier for a Raft group using ULID.
    ## ULIDs are lexicographically sortable and globally unique.

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
  ## String representation of GroupID (26-char ULID)
  result = $id.ULID

proc parseGroupID*(s: string): GroupID =
  ## Parse GroupID from 26-character ULID string
  result = GroupID(ulidFromString(s))

proc hash*(id: GroupID): Hash =
  ## Hash for use in tables
  var h: Hash = 0
  for b in id.ULID.data:
    h = h !& int(b)
  result = !$h

proc `==`*(a, b: GroupID): bool =
  ## Equality comparison
  a.ULID == b.ULID

proc `<`*(a, b: GroupID): bool =
  ## Less than comparison (lexicographic by timestamp)
  a.ULID < b.ULID

proc `<=`*(a, b: GroupID): bool =
  ## Less than or equal comparison
  a.ULID == b.ULID or a.ULID < b.ULID

proc genGroupID*(tsNs: int64): GroupID =
  ## Generate a new unique GroupID using ULID with nanosecond timestamp (from SharedTimer).
  GroupID(genULID(tsNs))

proc genGroupIDLocal*(): GroupID =
  ## Generate GroupID using LOCAL clock. Only for tests.
  GroupID(genULIDLocal())

proc groupIDFromInt*(n: int64 | uint64): GroupID =
  ## Create a deterministic GroupID from an integer.
  ## This embeds the integer in the ULID bytes for testing purposes.
  ## NOT for production use - use genGroupID(tsNs) instead.
  var ulid: ULID
  # Embed the integer in the last 8 bytes, first 8 bytes are zero
  let val = uint64(n)
  for i in 0..<8:
    ulid.data[i + 8] = uint8((val shr (i * 8)) and 0xFF)
  result = GroupID(ulid)

proc ZeroGroupID*(): GroupID =
  ## Return a zero/null GroupID (all bytes = 0)
  ## Used as a sentinel value to indicate "no group specified"
  GroupID(ZeroULID())

proc groupIDFromULID*(u: ULID): GroupID =
  ## Create GroupID from ULID
  GroupID(u)

proc groupIDToULID*(id: GroupID): ULID =
  ## Extract ULID from GroupID
  id.ULID

proc groupIDToBytes*(id: GroupID): string =
  ## Convert GroupID to 16-byte binary for storage
  ulidToBytes(id.ULID)

proc groupIDFromBytes*(data: string): GroupID =
  ## Parse 16-byte binary to GroupID
  GroupID(ulidFromBytes(data))

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

# =============================================================================
# ReplicaDescriptor Binary Serialization
# =============================================================================

const
  REPLICA_DESC_MAGIC* = [0x52'u8, 0x50'u8, 0x44'u8] # "RPD" - Replica Descriptor
  REPLICA_DESC_VERSION* = 0x01'u8

proc encodeReplicaDescriptor*(rep: ReplicaDescriptor): string =
  ## Encode a ReplicaDescriptor to binary format.
  ##
  ## Binary format (little-endian):
  ## - Magic: 3 bytes ("RPD")
  ## - Version: 1 byte
  ## - NodeId: 4 bytes (uint32)
  ## - ReplicaId: 4 bytes (uint32)
  ## - ReplicaType: 1 byte (uint8 ordinal)
  ## Total: 13 bytes
  var w = initBinaryWriter()
  w.writeBytes(REPLICA_DESC_MAGIC)
  w.writeU8(REPLICA_DESC_VERSION)
  w.writeU32(rep.nodeId.uint32)
  w.writeU32(rep.replicaId.uint32)
  w.writeU8(uint8(ord(rep.replicaType)))
  w.finish()

proc decodeReplicaDescriptor*(data: string): ReplicaDescriptor =
  ## Decode binary data to a ReplicaDescriptor.
  ## Raises ValueError if data is invalid.
  if data.len < 13:
    raise newException(ValueError, "ReplicaDescriptor: data too small")
  var r = initBinaryReader(data)

  # Verify magic header
  let magic0 = r.readU8()
  let magic1 = r.readU8()
  let magic2 = r.readU8()
  if magic0 != REPLICA_DESC_MAGIC[0] or magic1 != REPLICA_DESC_MAGIC[1] or
     magic2 != REPLICA_DESC_MAGIC[2]:
    raise newException(ValueError, "ReplicaDescriptor: invalid magic header")

  # Verify version
  let version = r.readU8()
  if version != REPLICA_DESC_VERSION:
    raise newException(ValueError, "ReplicaDescriptor: unsupported version " & $version)

  result.nodeId = NodeID(r.readU32())
  result.replicaId = ReplicaID(r.readU32())
  let typeVal = int(r.readU8())
  result.replicaType = if typeVal >= 0 and typeVal <= ord(rtNonVoter):
                         ReplicaType(typeVal)
                       else:
                         rtVoter

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
  ## Check that groupId has non-zero bytes
  var hasNonZero = false
  for b in desc.groupId.ULID.data:
    if b != 0:
      hasNonZero = true
      break
  hasNonZero and desc.replicas.len > 0

proc quorumSize*(desc: GroupDescriptor): int =
  ## Calculate quorum size (majority of voters)
  let voters = desc.getVoters()
  result = (voters.len div 2) + 1

# =============================================================================
# GroupDescriptor Binary Serialization
# =============================================================================

const
  GROUP_DESC_MAGIC* = [0x47'u8, 0x50'u8, 0x44'u8] # "GPD" - Group Descriptor
  GROUP_DESC_VERSION* = 0x01'u8

proc encodeGroupDescriptor*(desc: GroupDescriptor): string =
  ## Encode a GroupDescriptor to binary format.
  ##
  ## Binary format (little-endian):
  ## - Magic: 3 bytes ("GPD")
  ## - Version: 1 byte
  ## - GroupId: 16 bytes (ULID)
  ## - Generation: 8 bytes (uint64)
  ## - NextReplicaId: 4 bytes (uint32)
  ## - PreferredLeader: 4 bytes (uint32, 0 if invalid)
  ## - Leader: 4 bytes (uint32, 0 if invalid)
  ## - Replica count: 4 bytes (uint32)
  ## - Replicas: N * 13 bytes (each ReplicaDescriptor)
  var w = initBinaryWriter()
  w.writeBytes(GROUP_DESC_MAGIC)
  w.writeU8(GROUP_DESC_VERSION)
  w.writeBytes(groupIDToBytes(desc.groupId))
  w.writeU64(desc.generation)
  w.writeU32(desc.nextReplicaId.uint32)
  w.writeU32(desc.preferredLeader.uint32)
  w.writeU32(desc.leader.uint32)
  w.writeU32(uint32(desc.replicas.len))
  for rep in desc.replicas:
    w.writeBytes(encodeReplicaDescriptor(rep))
  w.finish()

proc decodeGroupDescriptor*(data: string): GroupDescriptor =
  ## Decode binary data to a GroupDescriptor.
  ## Raises ValueError if data is invalid.
  if data.len < 44: # 3 + 1 + 16 + 8 + 4 + 4 + 4 + 4 = 44 minimum
    raise newException(ValueError, "GroupDescriptor: data too small")
  var r = initBinaryReader(data)

  # Verify magic header
  let magic0 = r.readU8()
  let magic1 = r.readU8()
  let magic2 = r.readU8()
  if magic0 != GROUP_DESC_MAGIC[0] or magic1 != GROUP_DESC_MAGIC[1] or
     magic2 != GROUP_DESC_MAGIC[2]:
    raise newException(ValueError, "GroupDescriptor: invalid magic header")

  # Verify version
  let version = r.readU8()
  if version != GROUP_DESC_VERSION:
    raise newException(ValueError, "GroupDescriptor: unsupported version " & $version)

  new(result)
  let groupIdBytes = r.readFixedString(16)
  result.groupId = groupIDFromBytes(groupIdBytes)
  result.generation = r.readU64()
  result.nextReplicaId = ReplicaID(r.readU32())
  result.preferredLeader = NodeID(r.readU32())
  result.leader = NodeID(r.readU32())

  let replicaCount = int(r.readU32())
  result.replicas = newSeq[ReplicaDescriptor](replicaCount)
  for i in 0..<replicaCount:
    let repBytes = r.readFixedString(13)
    result.replicas[i] = decodeReplicaDescriptor(repBytes)

proc `$`*(desc: GroupDescriptor): string =
  ## String representation of GroupDescriptor
  result = "GroupDescriptor(" & $desc.groupId & ", " &
    "replicas=" & $desc.replicas.len & ", " &
    "gen=" & $desc.generation & ")"

# Special ULID for META_GROUP_ID - all zeros except last byte = 1
# This is a well-known ULID for the meta group
proc metaGroupULID(): ULID =
  result.data[15] = 1'u8

proc isMetaGroup*(desc: GroupDescriptor): bool =
  ## Check whether this GroupDescriptor covers the meta group (Group 1).
  ## The meta group stores system catalog tables and is replicated on all nodes.
  desc.groupId.ULID == metaGroupULID()

# ============================================================================
# Key encoding utilities
# ============================================================================

proc encodeGroupPrefix*(groupId: GroupID): string =
  ## Encode group prefix for keys: /range/<group_ulid>/
  result = "/range/" & $(groupId) & "/"

proc encodeDataKey*(groupId: GroupID, key: seq[byte]): string =
  ## Encode a data key with group prefix: /range/<group_ulid>/data/<key>
  var keyStr = newString(key.len)
  for i, b in key:
    keyStr[i] = char(b)
  result = encodeGroupPrefix(groupId) & "data/" & keyStr

proc encodeLogKey*(groupId: GroupID, index: uint64): string =
  ## Encode a log key: /raft/<group_ulid>/log/<index>
  result = "/raft/" & $(groupId) & "/log/" & $index

proc encodeStateKey*(groupId: GroupID): string =
  ## Encode a state key for persistent Raft state: /raft/<group_ulid>/state
  result = "/raft/" & $(groupId) & "/state"

proc encodeSnapshotKey*(groupId: GroupID): string =
  ## Encode a snapshot key: /raft/<group_ulid>/snapshot
  result = "/raft/" & $(groupId) & "/snapshot"

proc parseLogIndex*(key: string): uint64 =
  ## Parse log index from a log key
  ## Expected format: /raft/<group_id>/log/<index>
  let parts = key.split('/')
  if parts.len >= 5 and parts[^2] == "log":
    result = parseBiggestUInt(parts[^1])
  else:
    raise newException(ValueError, "Invalid log key format: " & key)
