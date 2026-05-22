# Binary Serialization for Persisted Cluster State
#
# Used by ProtocolServer to save/load cluster membership to/from disk.
# Replaces the previous JSON format (cluster.json) with a binary format
# for better performance and consistency with other Fractio serialization.

import std/[tables, options, os]
import fractio/utils/binary

# =============================================================================
# Binary Serialization Constants
# =============================================================================

const
  CLUSTER_STATE_MAGIC* = [0x43'u8, 0x53'u8, 0x42'u8] # "CSB" - Cluster State Binary
  CLUSTER_STATE_VERSION* = 0x02'u8 # v2: adds clientPort to PeerInfo

# =============================================================================
# Persisted Cluster State Types
# =============================================================================

type
  PeerInfo* = tuple[host: string, port: int, clientPort: int]
    ## Information about a peer node in the cluster
    ## port: Raft port, clientPort: client protocol port

  SelfNodeInfo* = object
    ## Information about this node (self)
    nodeId*: uint32
    host*: string
    clientPort*: int
    webPort*: int

  PersistedClusterState* = object
    ## Complete cluster state persisted to disk
    ## Allows a restarted node to rejoin without --join flag
    self*: SelfNodeInfo
    peers*: Table[uint32, PeerInfo]

# =============================================================================
# Constructor
# =============================================================================

proc newPersistedClusterState*(): PersistedClusterState =
  ## Create an empty persisted cluster state
  result.peers = initTable[uint32, PeerInfo]()

proc initPersistedClusterState*(self: SelfNodeInfo,
    peers: Table[uint32, PeerInfo]): PersistedClusterState =
  ## Create a persisted cluster state with given data
  result.self = self
  result.peers = peers

# =============================================================================
# Binary Encoding
# =============================================================================

proc encodeClusterState*(state: PersistedClusterState): string =
  ## Encode a PersistedClusterState to binary format.
  ##
  ## Binary format (little-endian):
  ## - Magic: 3 bytes (0x43 0x53 0x42 = "CSB")
  ## - Version: 1 byte (0x02)
  ## - Self node info:
  ##   - NodeId: 4 bytes (uint32)
  ##   - Host: length-prefixed string
  ##   - ClientPort: 4 bytes (int32)
  ##   - WebPort: 4 bytes (int32)
  ## - Peers:
  ##   - Peer count: 4 bytes (uint32)
  ##   - For each peer:
  ##     - NodeId: 4 bytes (uint32)
  ##     - Host: length-prefixed string
  ##     - Port: 4 bytes (int32, Raft port)
  ##     - ClientPort: 4 bytes (int32)
  ##
  ## Total minimum: 27 bytes (empty host strings, 0 peers)
  var w = initBinaryWriter()

  # Magic and version
  w.writeBytes(CLUSTER_STATE_MAGIC)
  w.writeU8(CLUSTER_STATE_VERSION)

  # Self node info
  w.writeU32(state.self.nodeId)
  w.writeString(state.self.host)
  w.writeI32(int32(state.self.clientPort))
  w.writeI32(int32(state.self.webPort))

  # Peers
  w.writeU32(uint32(state.peers.len))
  for nodeId, info in state.peers.pairs():
    w.writeU32(nodeId)
    w.writeString(info.host)
    w.writeI32(int32(info.port))
    w.writeI32(int32(info.clientPort))

  w.finish()

# =============================================================================
# Binary Decoding
# =============================================================================

proc decodeClusterState*(data: string): PersistedClusterState =
  ## Decode binary data to a PersistedClusterState.
  ## Raises ValueError if data is invalid or not binary format.
  var r = initBinaryReader(data)

  # Verify magic header
  if r.remaining < 4:
    raise newException(ValueError, "ClusterState: data too small for header")
  let magic0 = r.readU8()
  let magic1 = r.readU8()
  let magic2 = r.readU8()
  if magic0 != CLUSTER_STATE_MAGIC[0] or magic1 != CLUSTER_STATE_MAGIC[1] or
      magic2 != CLUSTER_STATE_MAGIC[2]:
    raise newException(ValueError, "ClusterState: invalid magic header")

  # Verify version
  let version = r.readU8()
  if version != 0x01'u8 and version != 0x02'u8:
    raise newException(ValueError, "ClusterState: unsupported version " & $version)

  # Read self node info
  let selfNodeId = r.readU32()
  let selfHost = r.readString()
  let selfClientPort = int(r.readI32())
  let selfWebPort = int(r.readI32())

  result = newPersistedClusterState()
  result.self = SelfNodeInfo(
    nodeId: selfNodeId,
    host: selfHost,
    clientPort: selfClientPort,
    webPort: selfWebPort
  )

  # Read peers
  let peerCount = int(r.readU32())
  for i in 0..<peerCount:
    let nodeId = r.readU32()
    let host = r.readString()
    let port = int(r.readI32())
    # v2 format includes clientPort; v1 does not (default to 0)
    let clientPort = if version >= 0x02: int(r.readI32()) else: 0
    result.peers[nodeId] = (host: host, port: port, clientPort: clientPort)

# =============================================================================
# File I/O Helpers
# =============================================================================

proc saveClusterStateToFile*(state: PersistedClusterState, path: string) =
  ## Save cluster state to a binary file
  let data = encodeClusterState(state)
  writeFile(path, data)

proc loadClusterStateFromFile*(path: string): Option[PersistedClusterState] =
  ## Load cluster state from a binary file.
  ## Returns none if file doesn't exist or data is invalid.
  if not fileExists(path):
    return none(PersistedClusterState)
  try:
    let data = readFile(path)
    result = some(decodeClusterState(data))
  except ValueError, CatchableError:
    result = none(PersistedClusterState)

# =============================================================================
# Utility Functions
# =============================================================================

proc getPeerCount*(state: PersistedClusterState): int =
  ## Get the number of peers in the cluster state
  state.peers.len

proc hasPeers*(state: PersistedClusterState): bool =
  ## Check if the cluster state has any peers
  state.peers.len > 0

proc getPeer*(state: PersistedClusterState, nodeId: uint32): Option[PeerInfo] =
  ## Get peer info for a specific node ID
  if state.peers.hasKey(nodeId):
    some(state.peers[nodeId])
  else:
    none(PeerInfo)
