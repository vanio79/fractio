# Nim Bindings for NuRaft C Wrapper
#
# Provides Nim types and procs that call into the NuRaft C wrapper library
# (libnuraft_c_wrapper.so). This replaces the hand-rolled Raft implementation
# with the production-grade NuRaft library.

const wrapperPath = "thirdparty/NuRaft/wrapper/build"
const nuraftPath = "thirdparty/NuRaft/build"

{.passL: "-L" & wrapperPath & " -lnuraft_c_wrapper".}
{.passL: "-L" & nuraftPath & " -lnuraft".}
{.passL: "-lssl -lcrypto -lpthread -ldl -lstdc++".}
{.passL: "-Wl,-rpath," & wrapperPath & " -Wl,-rpath," & nuraftPath.}
{.passC: "-I" & "thirdparty/NuRaft/wrapper".}

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

  NuRaftLauncher* = distinct pointer
    ## Launcher that bundles raft_server + ASIO service

  NuRaftServer* = distinct pointer
    ## Reference to the underlying raft_server

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
proc isNil*(p: NuRaftLauncher): bool {.inline.} = pointer(p) == nil
proc isNil*(p: NuRaftServer): bool {.inline.} = pointer(p) == nil
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
proc nuraftParamsSetClientReqTimeout*(params: NuRaftParams,
    ms: int32) {.importc: "nuraft_params_set_client_req_timeout".}
proc nuraftParamsSetAutoForwarding*(params: NuRaftParams,
    enabled: bool) {.importc: "nuraft_params_set_auto_forwarding".}
proc nuraftParamsSetMaxAppendSize*(params: NuRaftParams,
    size: int32) {.importc: "nuraft_params_set_max_append_size".}

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
# Launcher
# ============================================

proc nuraftLauncherCreate*(): NuRaftLauncher {.importc: "nuraft_launcher_create".}
proc nuraftLauncherDestroy*(
    launcher: NuRaftLauncher) {.importc: "nuraft_launcher_destroy".}
proc nuraftLauncherInit*(launcher: NuRaftLauncher, sm: NuRaftSM,
    smgr: NuRaftSMgr, portNumber: int32, params: NuRaftParams,
    eventCb: RaftEventCb,
    eventCtx: pointer): bool {.importc: "nuraft_launcher_init".}
proc nuraftLauncherWaitInit*(launcher: NuRaftLauncher,
    timeoutMs: int32): bool {.importc: "nuraft_launcher_wait_init".}
proc nuraftLauncherGetServer*(
    launcher: NuRaftLauncher): NuRaftServer {.importc: "nuraft_launcher_get_server".}
proc nuraftLauncherShutdown*(launcher: NuRaftLauncher,
    timeoutSec: int32): bool {.importc: "nuraft_launcher_shutdown".}

# ============================================
# Raft Server
# ============================================

proc nuraftServerIsLeader*(
    server: NuRaftServer): bool {.importc: "nuraft_server_is_leader".}
proc nuraftServerGetLeader*(
    server: NuRaftServer): int32 {.importc: "nuraft_server_get_leader".}
proc nuraftServerGetId*(
    server: NuRaftServer): int32 {.importc: "nuraft_server_get_id".}
proc nuraftServerAppendEntry*(server: NuRaftServer, data: cstring,
    len: csize_t,
    outLogIdx: ptr uint64): int32 {.importc: "nuraft_server_append_entry".}
proc nuraftServerAddSrv*(server: NuRaftServer, srvId: int32,
    endpoint: cstring): int32 {.importc: "nuraft_server_add_srv".}
proc nuraftServerRemoveSrv*(server: NuRaftServer,
    srvId: int32): int32 {.importc: "nuraft_server_remove_srv".}
proc nuraftServerSetPriority*(server: NuRaftServer, srvId: int32,
    priority: int32): int32 {.importc: "nuraft_server_set_priority".}
proc nuraftServerYieldLeadership*(
    server: NuRaftServer) {.importc: "nuraft_server_yield_leadership".}
proc nuraftServerGetTerm*(
    server: NuRaftServer): uint64 {.importc: "nuraft_server_get_term".}
proc nuraftServerGetCommittedLogIdx*(
    server: NuRaftServer): uint64 {.importc: "nuraft_server_get_committed_log_idx".}
proc nuraftServerIsInitialized*(
    server: NuRaftServer): bool {.importc: "nuraft_server_is_initialized".}
