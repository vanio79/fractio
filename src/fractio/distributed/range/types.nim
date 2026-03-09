# Range Types for Multi-Group Raft
#
# This module defines the core types for range-based replication.
# Each range is an independent Raft group, enabling horizontal scalability.

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
  RangeNodeID* = distinct uint32
    ## Unique identifier for a node in the cluster.
    ## Valid range: 1..uint32.high (0 is reserved for invalid/unknown)

  RangeID* = distinct uint64
    ## Unique identifier for a range.
    ## Each range is a contiguous chunk of the key-space.
    ## Range IDs are monotonically increasing and never reused.

  ReplicaID* = distinct uint32
    ## Unique identifier for a replica within a range.
    ## Each replica has a unique ID within its range, even if on the same node.

  ReplicaType* = enum
    ## Type of replica in a Raft group
    rtVoter    ## Participates in Raft quorum (default)
    rtNonVoter ## For follower reads, no quorum participation

  ReplicaDescriptor* = object
    ## Describes a single replica of a range
    nodeId*: RangeNodeID           ## Which node hosts this replica
    replicaId*: ReplicaID     ## Unique ID within the range
    replicaType*: ReplicaType ## Voter or non-voter

  RangeDescriptor* = ref object
    ## Metadata describing a range.
    ## This is the authoritative source of truth for range configuration.
    rangeId*: RangeID ## Unique range identifier
    startKey*: seq[byte] ## Inclusive start of key range
    endKey*: seq[byte] ## Exclusive end of key range
    replicas*: seq[ReplicaDescriptor] ## All replicas of this range
    nextReplicaId*: ReplicaID ## Next replica ID to allocate
    generation*: uint64 ## Incremented on every change

# ============================================================================
# RangeNodeID operations
# ============================================================================

proc `$`*(id: RangeNodeID): string =
  ## String representation of RangeNodeID
  result = "n" & $id.uint32

proc parseNodeID*(s: string): RangeNodeID =
  ## Parse RangeNodeID from string format "n<number>"
  if s.len < 2 or s[0] != 'n':
    raise newException(ValueError, "Invalid RangeNodeID format: " & s)
  result = RangeNodeID(parseInt(s[1..^1]))

proc hash*(id: RangeNodeID): Hash =
  ## Hash for use in tables
  result = hash(id.uint32)

proc `==`*(a, b: RangeNodeID): bool =
  ## Equality comparison
  a.uint32 == b.uint32

proc `<`*(a, b: RangeNodeID): bool =
  ## Less than comparison
  a.uint32 < b.uint32

proc `<=`*(a, b: RangeNodeID): bool =
  ## Less than or equal comparison
  a.uint32 <= b.uint32

proc invalidNodeID*: RangeNodeID =
  ## Returns an invalid/unknown RangeNodeID (0)
  RangeNodeID(0)

proc isValid*(id: RangeNodeID): bool =
  ## Check if RangeNodeID is valid (non-zero)
  id.uint32 > 0

# ============================================================================
# RangeID operations
# ============================================================================

proc `$`*(id: RangeID): string =
  ## String representation of RangeID
  result = "r" & $id.uint64

proc parseRangeID*(s: string): RangeID =
  ## Parse RangeID from string format "r<number>"
  if s.len < 2 or s[0] != 'r':
    raise newException(ValueError, "Invalid RangeID format: " & s)
  result = RangeID(parseInt(s[1..^1]))

proc hash*(id: RangeID): Hash =
  ## Hash for use in tables
  result = hash(id.uint64)

proc `==`*(a, b: RangeID): bool =
  ## Equality comparison
  a.uint64 == b.uint64

proc `<`*(a, b: RangeID): bool =
  ## Less than comparison
  a.uint64 < b.uint64

proc `<=`*(a, b: RangeID): bool =
  ## Less than or equal comparison
  a.uint64 <= b.uint64

proc firstRangeID*: RangeID =
  ## Returns the first valid RangeID
  RangeID(1)

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

proc newReplicaDescriptor*(nodeId: RangeNodeID, replicaId: ReplicaID,
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
    nodeId: RangeNodeID(json["nodeId"].getInt()),
    replicaId: ReplicaID(json["replicaId"].getInt()),
    replicaType: ReplicaType(json["replicaType"].getInt())
  )

# ============================================================================
# RangeDescriptor operations
# ============================================================================

proc newRangeDescriptor*(rangeId: RangeID, startKey, endKey: seq[byte],
                         replicas: seq[ReplicaDescriptor] = @[]): RangeDescriptor =
  ## Create a new RangeDescriptor
  new(result)
  result.rangeId = rangeId
  result.startKey = startKey
  result.endKey = endKey
  result.replicas = replicas
  result.nextReplicaId = firstReplicaID()
  result.generation = 1

proc addReplica*(desc: RangeDescriptor, nodeId: RangeNodeID,
                  replicaType: ReplicaType = rtVoter): ReplicaDescriptor =
  ## Add a new replica to the range. Returns the new replica descriptor.
  result = ReplicaDescriptor(
    nodeId: nodeId,
    replicaId: desc.nextReplicaId,
    replicaType: replicaType
  )
  desc.replicas.add(result)
  inc desc.nextReplicaId.uint32
  inc desc.generation

proc removeReplica*(desc: RangeDescriptor, replicaId: ReplicaID): bool =
  ## Remove a replica from the range. Returns true if found and removed.
  for i, rep in desc.replicas:
    if rep.replicaId == replicaId:
      desc.replicas.delete(i)
      inc desc.generation
      return true
  return false

proc getReplica*(desc: RangeDescriptor, nodeId: RangeNodeID): Option[
    ReplicaDescriptor] =
  ## Get replica by node ID
  for rep in desc.replicas:
    if rep.nodeId == nodeId:
      return some(rep)
  return none(ReplicaDescriptor)

proc getVoters*(desc: RangeDescriptor): seq[ReplicaDescriptor] =
  ## Get all voter replicas
  for rep in desc.replicas:
    if rep.isVoter:
      result.add(rep)

proc getNonVoters*(desc: RangeDescriptor): seq[ReplicaDescriptor] =
  ## Get all non-voter replicas
  for rep in desc.replicas:
    if not rep.isVoter:
      result.add(rep)

proc containsKey*(desc: RangeDescriptor, key: seq[byte]): bool =
  ## Check if a key falls within this range
  result = key >= desc.startKey and key < desc.endKey

proc isInitialized*(desc: RangeDescriptor): bool =
  ## Check if the descriptor is properly initialized
  desc.rangeId.uint64 > 0 and desc.replicas.len > 0

proc quorumSize*(desc: RangeDescriptor): int =
  ## Calculate quorum size (majority of voters)
  let voters = desc.getVoters()
  result = (voters.len div 2) + 1

proc toJson*(desc: RangeDescriptor): JsonNode =
  ## Serialize RangeDescriptor to JSON
  var replicasJson = newJArray()
  for rep in desc.replicas:
    replicasJson.add(rep.toJson())

  result = %*{
    "rangeId": desc.rangeId.uint64,
    "startKey": desc.startKey,
    "endKey": desc.endKey,
    "replicas": replicasJson,
    "nextReplicaId": desc.nextReplicaId.uint32,
    "generation": desc.generation
  }

proc parseRangeDescriptor*(json: JsonNode): RangeDescriptor =
  ## Parse RangeDescriptor from JSON
  new(result)
  result.rangeId = RangeID(json["rangeId"].getInt())

  # Parse startKey
  if json["startKey"].kind == JString:
    let s = json["startKey"].getStr()
    for c in s:
      result.startKey.add(byte(c))
  else:
    for v in json["startKey"]:
      result.startKey.add(byte(v.getInt()))

  # Parse endKey
  if json["endKey"].kind == JString:
    let s = json["endKey"].getStr()
    for c in s:
      result.endKey.add(byte(c))
  else:
    for v in json["endKey"]:
      result.endKey.add(byte(v.getInt()))

  # Parse replicas
  for repJson in json["replicas"]:
    result.replicas.add(parseReplicaDescriptor(repJson))

  result.nextReplicaId = ReplicaID(json["nextReplicaId"].getInt())
  result.generation = uint64(json["generation"].getInt())

proc `$`*(desc: RangeDescriptor): string =
  ## String representation of RangeDescriptor
  result = "RangeDescriptor(" & $desc.rangeId & ", " &
    "keys=[" & $desc.startKey.len & " bytes, " & $desc.endKey.len &
        " bytes], " &
    "replicas=" & $desc.replicas.len & ", " &
    "gen=" & $desc.generation & ")"

proc isMetaRange*(desc: RangeDescriptor): bool =
  ## Check whether this RangeDescriptor covers the meta range (Range 1).
  ## The meta range stores system catalog tables and is replicated on all nodes.
  desc.rangeId == RangeID(1)

# ============================================================================
# Key encoding utilities
# ============================================================================

proc encodeRangePrefix*(rangeId: RangeID): string =
  ## Encode range prefix for keys: /range/<range_id>/
  result = "/range/" & $rangeId.uint64 & "/"

proc encodeDataKey*(rangeId: RangeID, key: seq[byte]): string =
  ## Encode a data key with range prefix: /range/<range_id>/data/<key>
  var keyStr = newString(key.len)
  for i, b in key:
    keyStr[i] = char(b)
  result = encodeRangePrefix(rangeId) & "data/" & keyStr

proc encodeLogKey*(rangeId: RangeID, index: uint64): string =
  ## Encode a log key: /raft/<range_id>/log/<index>
  result = "/raft/" & $rangeId.uint64 & "/log/" & $index

proc encodeStateKey*(rangeId: RangeID): string =
  ## Encode a state key for persistent Raft state: /raft/<range_id>/state
  result = "/raft/" & $rangeId.uint64 & "/state"

proc encodeSnapshotKey*(rangeId: RangeID): string =
  ## Encode a snapshot key: /raft/<range_id>/snapshot
  result = "/raft/" & $rangeId.uint64 & "/snapshot"

proc parseLogIndex*(key: string): uint64 =
  ## Parse log index from a log key
  ## Expected format: /raft/<range_id>/log/<index>
  let parts = key.split('/')
  if parts.len >= 5 and parts[^2] == "log":
    result = parseBiggestUInt(parts[^1])
  else:
    raise newException(ValueError, "Invalid log key format: " & key)
