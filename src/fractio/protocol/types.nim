# Protocol types, constants, and error definitions for Fractio client/server protocol.
# All types here are value objects (no GC refs) so they can be safely shared across threads.

import std/[strformat, strutils]

# ---------------------------------------------------------------------------
# Protocol version
# ---------------------------------------------------------------------------

type
  ProtocolVersion* = distinct uint16

const
  PROTOCOL_VERSION_1*: ProtocolVersion = ProtocolVersion(0x0001)

proc `==`*(a, b: ProtocolVersion): bool {.borrow.}
proc `$`*(v: ProtocolVersion): string = &"0x{uint16(v):04X}"

# ---------------------------------------------------------------------------
# Magic bytes
# ---------------------------------------------------------------------------

const
  PROTOCOL_MAGIC* = "FRC1"

# ---------------------------------------------------------------------------
# Server feature flags (bit positions in uint32)
# ---------------------------------------------------------------------------

const
  FeatTLS* = 1'u32 shl 0
  FeatCompression* = 1'u32 shl 1
  FeatPipelining* = 1'u32 shl 2
  FeatAsync* = 1'u32 shl 3
  FeatTransactions* = 1'u32 shl 4
  FeatSQL* = 1'u32 shl 5
  FeatGraph* = 1'u32 shl 6
  FeatVector* = 1'u32 shl 7
  FeatRedirect* = 1'u32 shl 8 # NOT_LEADER sends redirect addr
  FeatProxy* = 1'u32 shl 9    # Server proxies to leader transparently

# ---------------------------------------------------------------------------
# Frame flags (bit positions in uint16)
# ---------------------------------------------------------------------------

const
  FlagCompressed* = 1'u16 shl 0
  FlagRequiresAck* = 1'u16 shl 1
  FlagIsResponse* = 1'u16 shl 2
  FlagIsError* = 1'u16 shl 3
  FlagEndOfStream* = 1'u16 shl 4

# ---------------------------------------------------------------------------
# Message types
# ---------------------------------------------------------------------------

type
  MessageType* = enum
    # Core / Control (0x0000-0x00FF)
    mtPing = 0x0001
    mtEcho = 0x0002
    mtClose = 0x0003
    mtCancelStream = 0x0004

    # KV Operations (0x0100-0x01FF)
    mtGet = 0x0100
    mtPut = 0x0101
    mtDelete = 0x0102
    mtBatch = 0x0103
    mtScan = 0x0104
    mtRawPut = 0x0105

    # Transactions (0x0200-0x02FF)
    mtBeginTxn = 0x0200
    mtCommitTxn = 0x0201
    mtRollbackTxn = 0x0202
    mtTxnStatus = 0x0203

    # Admin / Metrics (0x0700-0x07FF)
    mtServerInfo = 0x0700
    mtMetrics = 0x0701
    mtHealth = 0x0702

    # Cluster Admin (0x0703-0x0706)
    mtJoinNode = 0x0703        ## Add a node to the cluster
    mtRemoveNode = 0x0704      ## Remove a node from the cluster
    mtListNodes = 0x0705       ## List all known cluster nodes
    mtRebalanceStatus = 0x0706 ## Query rebalance operation status
    mtDrainNode = 0x0707       ## Mark a node as draining (graceful shutdown)

    # Space Management (0x0708-0x0709)
    mtCreateSpace = 0x0708     ## Create a new space with Raft groups
    mtDropSpace = 0x0709       ## Drop an existing space and its groups

    # Directed Group Creation (0x070A-0x070B)
    mtCreateGroup = 0x070A     ## Request a node to create a Raft group as leader
    mtJoinGroup = 0x070B       ## Request a node to join an existing Raft group

# ---------------------------------------------------------------------------
# Authentication methods
# ---------------------------------------------------------------------------

type
  AuthMethod* = enum
    amNone = 0x00
    amPassword = 0x01
    amToken = 0x02
    amTLS = 0x03

# ---------------------------------------------------------------------------
# Wire-level error codes (uint32, big-endian on the wire)
# ---------------------------------------------------------------------------

const
  ErrOK* = 0x00000000'u32
  ErrProtocol* = 0x00000001'u32
  ErrVersion* = 0x00000002'u32
  ErrAuthRequired* = 0x00000003'u32
  ErrAuthFailed* = 0x00000004'u32
  ErrNotFound* = 0x01000001'u32
  ErrAlreadyExists* = 0x01000002'u32
  ErrTxnAborted* = 0x02000001'u32
  ErrTxnTimeout* = 0x02000002'u32
  ErrTxnConflict* = 0x02000003'u32
  ErrTxnNotFound* = 0x02000004'u32
  ErrNotLeader* = 0x07000001'u32
  ErrClusterDown* = 0x07000002'u32
  ErrOverloaded* = 0x07000003'u32
  ErrInternal* = 0x07000004'u32   ## Phase 5: internal/unexpected server error
  ErrBadRouting* = 0x07000005'u32 ## Key does not hash to the specified group

# ---------------------------------------------------------------------------
# Leader redirect info (returned with ErrNotLeader)
# ---------------------------------------------------------------------------

type
  LeaderRedirect* = object
    ## Sent by server when it's not the leader for a group.
    ## Client should retry the request to this node.
    leaderId*: uint32 ## Node ID of the current leader
    leaderHost*: string ## Host address (e.g., "127.0.0.1")
    leaderClientPort*: uint16 ## Client protocol port

proc `$`*(r: LeaderRedirect): string =
  if r.leaderId == 0:
    "no leader known"
  else:
    &"node {r.leaderId} at {r.leaderHost}:{r.leaderClientPort}"

# ---------------------------------------------------------------------------
# Error checking helpers
# ---------------------------------------------------------------------------

proc isNotLeaderError*(errorMsg: string): bool {.inline.} =
  ## Check if an error message indicates a "not leader" error or a routing error
  ## that should trigger retry on a different node.
  ## Handles wire error codes, string messages, and NuRaft internal codes.
  let msgLower = errorMsg.toLowerAscii()
  if "not leader" in msgLower or "not the leader" in msgLower:
    return true
  # Check wire error code format (e.g., "server error 0x07000001")
  if "0x07000001" in errorMsg:
    return true
  # NuRaft internal error code
  if "code -3" in errorMsg:
    return true
  # "Group not found" indicates the target node doesn't have the group
  # This can happen when topology changes (new node added, node killed)
  # and the client connected to a node that isn't part of the group
  if "group not found" in msgLower:
    return true
  false

# Wire-level error categories
const
  ErrCatProtocol* = 0x00'u8
  ErrCatKV* = 0x01'u8
  ErrCatTransaction* = 0x02'u8
  ErrCatSQL* = 0x03'u8
  ErrCatGraph* = 0x04'u8
  ErrCatVector* = 0x05'u8
  ErrCatAuth* = 0x06'u8
  ErrCatSystem* = 0x07'u8

# ---------------------------------------------------------------------------
# Protocol-layer error (Nim-side, not wire)
# Must be defined before Result/PResult so PResult can reference it.
# ---------------------------------------------------------------------------

type
  ProtocolErrorKind* = enum
    peInvalidFrame
    peChecksumMismatch
    peFrameTooLarge
    peUnknownMessageType
    peVersionMismatch
    peAuthFailed
    peNotLeader
    peGroupNotFound ## Target node doesn't have the group (topology change)
    peTimeout
    peBoundsOverflow
    peInternal

  ProtocolError* = object
    kind*: ProtocolErrorKind
    msg*: string
    leaderRedirect*: LeaderRedirect ## Set when kind == peNotLeader

proc newProtocolError*(kind: ProtocolErrorKind, msg: string,
    redirect: LeaderRedirect = LeaderRedirect()): ProtocolError =
  ProtocolError(kind: kind, msg: msg, leaderRedirect: redirect)

proc `$`*(e: ProtocolError): string =
  result = &"ProtocolError[{e.kind}]: {e.msg}"
  if e.kind == peNotLeader and e.leaderRedirect.leaderId != 0:
    result &= &" (redirect to {e.leaderRedirect})"

proc isNotLeader*(e: ProtocolError): bool {.inline.} =
  ## Check if this is a "not leader" error or a routing error that should
  ## trigger retry on a different node.
  e.kind == peNotLeader or e.kind == peGroupNotFound

# ---------------------------------------------------------------------------
# Result types — no external dependencies.
# Result[T, E]  — for operations that return a value on success.
# PResult       — for operations that return nothing (void) on success.
# ---------------------------------------------------------------------------

type
  Result*[T, E] = object
    isOk*: bool
    val*: T
    err*: E

  ## Void result — used instead of Result[void, ProtocolError].
  PResult* = object
    isOk*: bool
    err*: ProtocolError

# Result[T, E] constructors
proc ok*[T, E](v: T): Result[T, E] = Result[T, E](isOk: true, val: v)
proc isErr*[T, E](r: Result[T, E]): bool = not r.isOk

proc value*[T, E](r: Result[T, E]): T =
  doAssert r.isOk, "called .value on Err result: " & $r.err
  r.val

proc error*[T, E](r: Result[T, E]): E =
  doAssert not r.isOk, "called .error on Ok result"
  r.err

# Context-sensitive constructors for Result[T, ProtocolError].
# These templates infer the full return type from the calling proc's `result`,
# avoiding Nim's inability to instantiate T/E from a partial call.
template peErr*(e: ProtocolError): untyped =
  typeof(result)(isOk: false, err: e)

template peOk*(v: untyped): untyped =
  typeof(result)(isOk: true, val: v)

# PResult constructors
proc pOk*(): PResult = PResult(isOk: true)
proc pErr*(e: ProtocolError): PResult = PResult(isOk: false, err: e)
proc isErr*(r: PResult): bool = not r.isOk
proc isOk*(r: PResult): bool = r.isOk
proc error*(r: PResult): ProtocolError =
  doAssert not r.isOk, "called .error on Ok PResult"
  r.err

# ---------------------------------------------------------------------------
# Transaction state
# ---------------------------------------------------------------------------

type
  TxnState* = enum
    tsActive
    tsCommitted
    tsAborted
    tsTimedOut

  CommitResult* = object
    committed*: bool
    commitTimestamp*: uint64
    conflictKey*: string # non-empty when committed=false due to conflict

# ---------------------------------------------------------------------------
# Limits
# ---------------------------------------------------------------------------

const
  MAX_KEY_BYTES* = 4 * 1024           # 4 KB
  MAX_VALUE_BYTES* = 64 * 1024 * 1024 # 64 MB
  MAX_BATCH_OPS* = 10_000
