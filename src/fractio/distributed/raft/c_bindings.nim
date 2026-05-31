# Nim Bindings for NuRaft Shim
#
# Provides Nim types and procs that call into the minimal NuRaft C++ shim.
# All business logic (logging, serialization) is in Nim.

const wrapperPath = "src/fractio/distributed/raft/wrapper/build"
const nuraftPath = "thirdparty/NuRaft"

{.passL: "-L" & wrapperPath & " -lnuraft_shim".}
{.passL: "-L" & nuraftPath & " -lnuraft".}
{.passL: "-lssl -lcrypto -lpthread -ldl -lstdc++".}
# Use absolute rpath to ensure local library is found before system library
{.passL: "-Wl,-rpath,/home/ingrid/devel/fractio/" & wrapperPath &
    " -Wl,-rpath,/home/ingrid/devel/fractio/" & nuraftPath.}
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
    ## Callback-based state manager backed by WiscKey (wraps nuraft::state_mgr)

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

  ConfigChangeCb* = proc(ctx: pointer, serverId: int32,
      endpoint: cstring): int32 {.cdecl.}
    ## Called when NuRaft configuration changes (new server added).
    ## endpoint format: "serverId@host:port" - we parse and update peerInfo.
    ## Returns 0 on success.

  QuorumUpdateCb* = proc(ctx: pointer, serverId: int32,
      quorumSize: int32) {.cdecl.}
    ## Called when quorum should be updated based on new server count.
    ## quorumSize = majority + 1 = floor(N/2) + 2

  # ============================================
  # Log Store Callback Types
  # ============================================

  LogAppendCb* = proc(ctx: pointer, term: uint64, valType: int32,
      entryData: cstring, entryLen: csize_t): uint64 {.cdecl.}
    ## Append a log entry. Returns the log index where it was stored.

  LogWriteAtCb* = proc(ctx: pointer, index: uint64, term: uint64,
      valType: int32, entryData: cstring, entryLen: csize_t) {.cdecl.}
    ## Write a log entry at the given index, truncating all entries after it.

  LogGetCb* = proc(ctx: pointer, index: uint64, outTerm: ptr uint64,
      outValType: ptr int32, outData: cstring,
          outCapacity: csize_t): csize_t {.cdecl.}
    ## Get a log entry at the given index.
    ## Nim writes entry data into outData (up to outCapacity bytes).
    ## Returns: actual size of entry data, or 0 if not found.

  LogTermAtCb* = proc(ctx: pointer, index: uint64): uint64 {.cdecl.}
    ## Get the term at the given index. Returns 0 if not found.

  LogNextSlotCb* = proc(ctx: pointer): uint64 {.cdecl.}
    ## Get the next available log slot (1-based).

  LogStartIndexCb* = proc(ctx: pointer): uint64 {.cdecl.}
    ## Get the start index of the log.

  LogPackCb* = proc(ctx: pointer, index: uint64, count: int32,
      outData: cstring, outCapacity: csize_t): csize_t {.cdecl.}
    ## Pack log entries. Returns size of packed data, or 0 on error.

  LogApplyPackCb* = proc(ctx: pointer, index: uint64,
      packData: cstring, packLen: csize_t) {.cdecl.}
    ## Apply packed log entries starting at index.

  LogCompactCb* = proc(ctx: pointer, lastLogIndex: uint64): int32 {.cdecl.}
    ## Compact the log store. Returns 0 on success.

  LogFlushCb* = proc(ctx: pointer): int32 {.cdecl.}
    ## Flush all pending writes. Returns 0 on success.

  # ============================================
  # State Manager Callback Types
  # ============================================

  StateSaveCb* = proc(ctx: pointer, term: uint64, votedFor: int32,
      configHwm: uint64) {.cdecl.}
    ## Save Raft state (term, voted_for, config_hwm) to persistent storage.

  StateReadCb* = proc(ctx: pointer, outTerm: ptr uint64,
      outVotedFor: ptr int32, outConfigHwm: ptr uint64): int32 {.cdecl.}
    ## Load Raft state from persistent storage.
    ## Returns 1 if state was found, 0 if not found.

  ConfigSaveCb* = proc(ctx: pointer, configData: cstring,
      configLen: csize_t) {.cdecl.}
    ## Save cluster config to persistent storage.

  ConfigLoadCb* = proc(ctx: pointer, outData: cstring,
      outCapacity: csize_t): csize_t {.cdecl.}
    ## Load cluster config from persistent storage.
    ## Returns size of config data, or 0 if not found.

# ============================================
# NuRaft event type constants
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
proc nuraftParamsSetCustomElectionQuorumSize*(params: NuRaftParams,
    size: int32) {.importc: "nuraft_params_set_custom_election_quorum_size".}
proc nuraftParamsSetAutoAdjustQuorum*(params: NuRaftParams,
    enable: int32) {.importc: "nuraft_params_set_auto_adjust_quorum".}

# ============================================
# Limits (global settings)
# ============================================

proc nuraftLimitsSetBusyConnectionLimit*(limit: int32)
    {.importc: "nuraft_limits_set_busy_connection_limit".}
  ## Set the busy connection limit for NuRaft.
  ## When set to 0, disables the system_exit(-22) behavior during connection failures.
  ## This is useful during tests where shutdown causes expected connection failures.

proc nuraftPurgeExpiredHandlers*() {.importc: "nuraft_purge_expired_handlers".}
  ## Purge expired RPC handlers from the global pending handlers registry.
  ## Should be called periodically (e.g., every 10 seconds) to prevent
  ## unbounded growth from lost responses.

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

proc nuraftSmgrCreateWithCallbacks*(
    myServerId: int32, myEndpoint: cstring,
    numServers: int32, serverIds: ptr int32,
    endpoints: ptr cstring, catchingUp: bool,
    # Log store callbacks
    logStoreCtx: pointer,
    logAppendCb: LogAppendCb,
    logWriteAtCb: LogWriteAtCb,
    logGetCb: LogGetCb,
    logTermAtCb: LogTermAtCb,
    logNextSlotCb: LogNextSlotCb,
    logStartIndexCb: LogStartIndexCb,
    logPackCb: LogPackCb,
    logApplyPackCb: LogApplyPackCb,
    logCompactCb: LogCompactCb,
    logFlushCb: LogFlushCb,
    # State callbacks
    stateCbCtx: pointer,
    stateSaveCb: StateSaveCb,
    stateReadCb: StateReadCb,
    configSaveCb: ConfigSaveCb,
    configLoadCb: ConfigLoadCb
): NuRaftSMgr {.importc: "nuraft_smgr_create_with_callbacks".}
  ## Create state manager with callback-based persistence (WiscKey-backed).
  ## No file I/O is used — all persistence goes through the Nim callbacks.
  ## The log store callbacks delegate to Nim's RaftPersistentStore.

proc nuraftSmgrDestroy*(smgr: NuRaftSMgr) {.importc: "nuraft_smgr_destroy".}
proc nuraftSmgrSetConfigCb*(smgr: NuRaftSMgr, ctx: pointer,
    cb: ConfigChangeCb) {.importc: "nuraft_smgr_set_config_cb".}
  ## Set callback for configuration changes (called when add_srv is committed).
proc nuraftSmgrSetQuorumCb*(smgr: NuRaftSMgr, ctx: pointer,
    cb: QuorumUpdateCb) {.importc: "nuraft_smgr_set_quorum_cb".}
  ## Set callback for quorum updates (called when config changes affect server count).
proc nuraftSmgrSetRaftServer*(smgr: NuRaftSMgr, server: NuRaftServer)
  {.importc: "nuraft_smgr_set_raft_server".}
  ## Set raft server pointer for dynamic state manager (needed for quorum updates).

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
                              msgData: pointer, msgLen: csize_t)
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

proc nuraftServerAppendEntry*(server: NuRaftServer, data: pointer,
    len: csize_t, outLogIdx: ptr uint64): int32 {.importc: "nuraft_server_append_entry".}
proc nuraftServerAddSrv*(server: NuRaftServer, srvId: int32,
    endpoint: cstring): int32 {.importc: "nuraft_server_add_srv".}
proc nuraftServerRemoveSrv*(server: NuRaftServer,
    srvId: int32): int32 {.importc: "nuraft_server_remove_srv".}
proc nuraftServerSetPriority*(server: NuRaftServer, srvId: int32,
    priority: int32): int32 {.importc: "nuraft_server_set_priority".}
proc nuraftServerYieldLeadership*(server: NuRaftServer, immediate: bool,
    successorId: int32) {.importc: "nuraft_server_yield_leadership".}
proc nuraftServerUpdateQuorum*(server: NuRaftServer, quorumSize: int32)
  {.importc: "nuraft_server_update_quorum".}

# Peer info

type
  NuRaftPeerInfo* = object
    lastLogIdx*: uint64
    lastSuccRespUs*: uint64
    exists*: int32

proc nuraftServerGetPeerInfo*(server: NuRaftServer, peerId: int32,
    outInfo: ptr NuRaftPeerInfo): int32
  {.importc: "nuraft_server_get_peer_info".}
  ## Dynamically update the election quorum size for a running server.

proc nuraftServerGetServerCount*(server: NuRaftServer): int32
  {.importc: "nuraft_server_get_server_count".}
  ## Get the number of servers in the current cluster config (peers + self).

# =============================================================================
# Global Manager: Shared thread pool for all Raft groups
# =============================================================================

proc nuraftGlobalMgrInit*(numCommitThreads: int32 = 1,
    numAppendThreads: int32 = 1): int32
  {.importc: "nuraft_global_mgr_init".}
  ## Initialize the global NuRaft manager with a shared thread pool.
  ##
  ## Without this, each raft_server creates 2 dedicated threads
  ## (bg_commit_thread_ and bg_append_thread_). With N groups, that's
  ## 2N threads × 8MB stack = 16N MB of stack memory alone.
  ##
  ## With the global manager, all groups share numCommitThreads +
  ## numAppendThreads threads total (default 2 threads), reducing
  ## stack memory to ~16MB regardless of group count.
  ##
  ## MUST be called before creating any raft_server instances.
  ##
  ## Returns: 1 if initialized successfully, 0 if already initialized, -1 on error.

proc nuraftGlobalMgrShutdown*() {.importc: "nuraft_global_mgr_shutdown".}
  ## Shut down the global NuRaft manager and free resources.
  ## All raft_server instances MUST be destroyed before calling this.
