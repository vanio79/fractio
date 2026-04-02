# Nim Bindings for NuRaft Shim
#
# Provides Nim types and procs that call into the minimal NuRaft C++ shim.
# All business logic (logging, serialization) is in Nim.

const wrapperPath = "src/fractio/distributed/raft/wrapper/build"
const nuraftPath = "thirdparty/NuRaft/build"

{.passL: "-L" & wrapperPath & " -lnuraft_shim".}
{.passL: "-L" & nuraftPath & " -lnuraft".}
{.passL: "-lssl -lcrypto -lpthread -ldl -lstdc++".}
{.passL: "-Wl,-rpath," & wrapperPath & " -Wl,-rpath," & nuraftPath.}
{.passC: "-I" & "src/fractio/distributed/raft/wrapper".}
{.passC: "-I" & "thirdparty/NuRaft/src".}

# ============================================
# Opaque types
# ============================================

type
  NuRaftParams* = distinct pointer
    ## Raft configuration parameters (election timeout, heartbeat, etc.)

  NuRaftSM* = distinct pointer
    ## Callback-based state machine (wraps nuraft::state_machine)

  NuRaftSMgr* = distinct pointer
    ## Dynamic state manager (wraps nuraft::state_mgr)

  NuRaftServer* = distinct pointer
    ## Reference to the underlying raft_server

  MultiplexedContext* = distinct pointer
    ## Multiplexed RPC context for sharing single port across groups

# ============================================
# Callback types
# ============================================

type
  SmCommitCb* = proc(ctx: pointer, logIdx: uint64,
      data: cstring, len: csize_t) {.cdecl.}
    ## Called when a log entry is committed in the state machine.
    ## data/len is the raw payload passed to append_entries.

  RaftEventCb* = proc(ctx: pointer, eventType: int32,
      leaderId: int32, term: uint64) {.cdecl.}
    ## Called on leader/follower transitions.
    ## eventType: 6 = BecomeLeader, 11 = BecomeFollower

  SendCb* = proc(ctx: pointer, groupIdBytes: cstring,
      srcNodeId: int32, dstNodeId: int32,
      msgData: cstring, msgLen: csize_t): int32 {.cdecl.}
    ## Called when NuRaft wants to send a message to a peer.
    ## Returns 0 on success, non-zero on failure.

  ScheduleTimerCb* = proc(ctx: pointer, timerId: int32,
      delayMs: int32) {.cdecl.}
    ## Called when NuRaft wants to schedule a timer.

  CancelTimerCb* = proc(ctx: pointer, timerId: int32) {.cdecl.}
    ## Called when NuRaft wants to cancel a timer.

# ============================================
# NuRaft event type constants
# ============================================

const
  NuRaftBecomeLeader* = 6'i32
  NuRaftBecomeFollower* = 11'i32

# Nil comparison helpers for distinct pointer types
proc isNil*(p: NuRaftParams): bool {.inline.} = pointer(p) == nil
proc isNil*(p: NuRaftSM): bool {.inline.} = pointer(p) == nil
proc isNil*(p: NuRaftSMgr): bool {.inline.} = pointer(p) == nil
proc isNil*(p: NuRaftServer): bool {.inline.} = pointer(p) == nil
proc isNil*(p: MultiplexedContext): bool {.inline.} = pointer(p) == nil
proc `==`*(p: NuRaftServer, n: typeof(nil)): bool {.inline.} = pointer(p) == nil
proc `!=`*(p: NuRaftServer, n: typeof(nil)): bool {.inline.} = pointer(p) != nil

# ============================================
# Raft Parameters
# ============================================

proc nuraftParamsCreate*(): NuRaftParams {.importc: "nuraft_params_create".}
proc nuraftParamsDestroy*(params: NuRaftParams) {.importc: "nuraft_params_destroy".}
proc nuraftParamsSetElectionTimeout*(params: NuRaftParams, lowerMs: int32,
    upperMs: int32) {.importc: "nuraft_params_set_election_timeout".}
proc nuraftParamsSetHeartbeatInterval*(params: NuRaftParams,
    ms: int32) {.importc: "nuraft_params_set_heartbeat_interval".}
proc nuraftParamsSetReturnMethod*(params: NuRaftParams,
    retMethod: int32) {.importc: "nuraft_params_set_return_method".}
proc nuraftParamsSetSnapshotDistance*(params: NuRaftParams,
    distance: int32) {.importc: "nuraft_params_set_snapshot_distance".}
proc nuraftParamsSetReservedLogItems*(params: NuRaftParams,
    count: int32) {.importc: "nuraft_params_set_reserved_log_items".}
proc nuraftParamsSetClientReqTimeout*(params: NuRaftParams,
    ms: int32) {.importc: "nuraft_params_set_client_req_timeout".}
proc nuraftParamsSetMaxAppendSize*(params: NuRaftParams,
    size: int32) {.importc: "nuraft_params_set_max_append_size".}
proc nuraftParamsSetLeadershipTransferMinWaitTime*(params: NuRaftParams,
    ms: int32) {.importc: "nuraft_params_set_leadership_transfer_min_wait_time".}

# ============================================
# State Machine
# ============================================

proc nuraftSmCreate*(commitCb: SmCommitCb,
    ctx: pointer): NuRaftSM {.importc: "nuraft_sm_create".}
proc nuraftSmDestroy*(sm: NuRaftSM) {.importc: "nuraft_sm_destroy".}
proc nuraftSmLastCommitIndex*(
    sm: NuRaftSM): uint64 {.importc: "nuraft_sm_last_commit_index".}

# ============================================
# State Manager
# ============================================

proc nuraftSmgrCreate*(myServerId: int32, myEndpoint: cstring,
    numServers: int32, serverIds: ptr int32,
    endpoints: ptr cstring): NuRaftSMgr {.importc: "nuraft_smgr_create".}
proc nuraftSmgrDestroy*(smgr: NuRaftSMgr) {.importc: "nuraft_smgr_destroy".}

# ============================================
# Multiplexed Context
# ============================================

proc nuraftMpContextCreate*(serverId: int32,
                            transportCtx: pointer,
                            sendCb: SendCb,
                            timerCtx: pointer,
                            scheduleCb: ScheduleTimerCb,
                            cancelCb: CancelTimerCb): MultiplexedContext
  {.importc: "nuraft_mp_context_create".}

proc nuraftMpContextDestroy*(ctx: MultiplexedContext)
  {.importc: "nuraft_mp_context_destroy".}

proc nuraftMpContextSetGroupId*(ctx: MultiplexedContext, groupIdBytes: cstring)
  {.importc: "nuraft_mp_context_set_group_id".}

# ============================================
# Listener Helpers
# ============================================

type
  MultiplexedListener* = distinct pointer
    ## Opaque handle to the RPC listener

proc isNil*(p: MultiplexedListener): bool {.inline.} = pointer(p) == nil

proc nuraftMpGetListener*(ctx: MultiplexedContext): MultiplexedListener
  {.importc: "nuraft_mp_get_listener".}

proc nuraftMpListenerSetSrcNodeId*(listener: MultiplexedListener,
    srcNodeId: int32)
  {.importc: "nuraft_mp_listener_set_src_node_id".}

proc nuraftMpListenerSetSendResponseCallback*(listener: MultiplexedListener,
                                               ctx: pointer, cb: SendCb)
  {.importc: "nuraft_mp_listener_set_send_response_callback".}

proc nuraftMpListenerDestroy*(listener: MultiplexedListener)
  {.importc: "nuraft_mp_listener_destroy".}

# ============================================
# Message Delivery
# ============================================

proc nuraftMpDeliverMessage*(ctx: MultiplexedContext, server: NuRaftServer,
                              msgData: cstring, msgLen: csize_t)
  {.importc: "nuraft_mp_deliver_message".}

# Timer invocation (called by Nim when timer fires)
proc nuraftMpInvokeTimer*(ctx: MultiplexedContext, timerId: int32): bool
  {.importc: "nuraft_mp_invoke_timer".}

# ============================================
# Raft Server
# ============================================

proc nuraftServerCreate*(mpContext: MultiplexedContext,
                         sm: NuRaftSM,
                         smgr: NuRaftSMgr,
                         params: NuRaftParams,
                         eventCb: RaftEventCb,
                         eventCtx: pointer,
                         skipInitialElection: bool): NuRaftServer
  {.importc: "nuraft_server_create".}

proc nuraftServerDestroy*(server: NuRaftServer) {.importc: "nuraft_server_destroy".}
proc nuraftServerShutdown*(server: NuRaftServer) {.importc: "nuraft_server_shutdown".}

proc nuraftServerIsLeader*(server: NuRaftServer): bool {.importc: "nuraft_server_is_leader".}
proc nuraftServerGetLeader*(server: NuRaftServer): int32 {.importc: "nuraft_server_get_leader".}
proc nuraftServerGetId*(server: NuRaftServer): int32 {.importc: "nuraft_server_get_id".}
proc nuraftServerGetTerm*(server: NuRaftServer): uint64 {.importc: "nuraft_server_get_term".}
proc nuraftServerGetCommittedLogIdx*(server: NuRaftServer): uint64 {.importc: "nuraft_server_get_committed_log_idx".}
proc nuraftServerGetLastLogIdx*(server: NuRaftServer): uint64 {.importc: "nuraft_server_get_last_log_idx".}
proc nuraftServerIsInitialized*(server: NuRaftServer): bool {.importc: "nuraft_server_is_initialized".}

proc nuraftServerAppendEntry*(server: NuRaftServer, data: cstring,
    len: csize_t, outLogIdx: ptr uint64): int32 {.importc: "nuraft_server_append_entry".}
proc nuraftServerAddSrv*(server: NuRaftServer, srvId: int32,
    endpoint: cstring): int32 {.importc: "nuraft_server_add_srv".}
proc nuraftServerRemoveSrv*(server: NuRaftServer,
    srvId: int32): int32 {.importc: "nuraft_server_remove_srv".}
proc nuraftServerSetPriority*(server: NuRaftServer, srvId: int32,
    priority: int32): int32 {.importc: "nuraft_server_set_priority".}
proc nuraftServerYieldLeadership*(server: NuRaftServer, immediate: bool,
    successorId: int32) {.importc: "nuraft_server_yield_leadership".}
