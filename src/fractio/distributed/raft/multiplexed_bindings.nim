# Nim Bindings for Multiplexed NuRaft RPC
#
# Provides bindings to the C++ multiplexed RPC implementation that
# allows all Raft groups to share a single TCP port.

import std/atomics
import fractio/distributed/raft/c_bindings

const wrapperPath = "thirdparty/NuRaft/wrapper/build"
const nuraftPath = "thirdparty/NuRaft/build"

{.passL: "-L" & wrapperPath & " -lnuraft_c_wrapper".}
{.passL: "-L" & nuraftPath & " -lnuraft".}
{.passL: "-lssl -lcrypto -lpthread -ldl -lstdc++".}
{.passL: "-Wl,-rpath," & wrapperPath & " -Wl,-rpath," & nuraftPath.}
{.passC: "-I" & "thirdparty/NuRaft/wrapper".}

# =============================================================================
# Opaque Types
# =============================================================================

type
  MultiplexedListener* = distinct pointer
    ## RPC listener for receiving messages

  MultiplexedClientFactory* = distinct pointer
    ## Factory for creating RPC clients

  MultiplexedTimer* = distinct pointer
    ## Timer scheduler for delayed tasks

# =============================================================================
# Callback Types
# =============================================================================

type
  MultiplexedSendCb* = proc(ctx: pointer,
                            groupIdBytes: cstring,
                            srcNodeId: int32,
                            dstNodeId: int32,
                            msgData: cstring,
                            msgLen: csize_t): int32 {.cdecl.}
    ## Callback to send a message to a remote endpoint.
    ## Returns 0 on success, non-zero on failure.

  MultiplexedSendResponseCb* = proc(ctx: pointer,
                                    groupIdBytes: cstring,
                                    srcNodeId: int32,
                                    dstNodeId: int32,
                                    msgData: cstring,
                                    msgLen: csize_t): int32 {.cdecl.}
    ## Callback to send a response message back through transport.

  MultiplexedScheduleTimerCb* = proc(ctx: pointer,
                                      groupIdHash: int32,
                                      timerType: int32,
                                      delayMs: int32) {.cdecl.}
    ## Callback to schedule a timer.

  MultiplexedCancelTimerCb* = proc(ctx: pointer,
                                    groupIdHash: int32,
                                    timerId: int32) {.cdecl.}
    ## Callback to cancel a timer.

# =============================================================================
# Nil Helpers
# =============================================================================

proc isNil*(p: MultiplexedListener): bool {.inline.} = pointer(p) == nil
proc isNil*(p: MultiplexedClientFactory): bool {.inline.} = pointer(p) == nil
proc isNil*(p: MultiplexedTimer): bool {.inline.} = pointer(p) == nil

# =============================================================================
# C API Functions
# =============================================================================

# Create a multiplexed context
proc nuraftMultiplexedCreate*(serverId: int32,
                               transportCtx: pointer,
                               sendCb: MultiplexedSendCb,
                               timerCtx: pointer,
                               scheduleCb: MultiplexedScheduleTimerCb,
                               cancelCb: MultiplexedCancelTimerCb): MultiplexedContext
  {.importc: "nuraft_multiplexed_create".}

# Destroy a multiplexed context
proc nuraftMultiplexedDestroy*(ctx: MultiplexedContext)
  {.importc: "nuraft_multiplexed_destroy".}

# Get the listener
proc nuraftMultiplexedGetListener*(ctx: MultiplexedContext): MultiplexedListener
  {.importc: "nuraft_multiplexed_get_listener".}

# Get the client factory
proc nuraftMultiplexedGetClientFactory*(
  ctx: MultiplexedContext): MultiplexedClientFactory
  {.importc: "nuraft_multiplexed_get_client_factory".}

# Get the timer scheduler
proc nuraftMultiplexedGetTimer*(ctx: MultiplexedContext): MultiplexedTimer
  {.importc: "nuraft_multiplexed_get_timer".}

# Set the response send callback on a listener
proc nuraftMultiplexedSetResponseCallback*(listener: MultiplexedListener,
                                            respCtx: pointer,
                                            respCb: MultiplexedSendResponseCb)
  {.importc: "nuraft_multiplexed_set_response_callback".}

# Set the GroupID on a listener
proc nuraftMultiplexedSetGroupId*(listener: MultiplexedListener,
                                   groupIdBytes: cstring)
  {.importc: "nuraft_multiplexed_set_group_id".}

# Set the source node ID on a listener
proc nuraftMultiplexedSetSrcNodeId*(listener: MultiplexedListener,
                                     srcNodeId: int32)
  {.importc: "nuraft_multiplexed_set_src_node_id".}

# Deliver a received message to the handler
proc nuraftMultiplexedDeliverMessage*(listener: MultiplexedListener,
                                       msgData: cstring,
                                       msgLen: csize_t)
  {.importc: "nuraft_multiplexed_deliver_message".}

# Shutdown the multiplexed context
proc nuraftMultiplexedShutdown*(ctx: MultiplexedContext)
  {.importc: "nuraft_multiplexed_shutdown".}

# Listen: set the message handler (raft_server) on the listener
proc nuraftMultiplexedListen*(listener: MultiplexedListener,
    server: NuRaftServer)
  {.importc: "nuraft_multiplexed_listen".}

# =============================================================================
# Higher-Level Nim API
# =============================================================================

type
  MultiplexedRpc* = ref object
    ## High-level Nim wrapper for multiplexed RPC
    context*: MultiplexedContext
    listener*: MultiplexedListener
    clientFactory*: MultiplexedClientFactory
    timer*: MultiplexedTimer
    serverId*: int32
    running*: Atomic[bool]

proc newMultiplexedRpc*(serverId: int32,
                         transportCtx: pointer,
                         sendCb: MultiplexedSendCb,
                         timerCtx: pointer,
                         scheduleCb: MultiplexedScheduleTimerCb,
                         cancelCb: MultiplexedCancelTimerCb): MultiplexedRpc =
  ## Create a new multiplexed RPC instance.
  new(result)
  result.serverId = serverId
  result.running.store(false)
  result.context = nuraftMultiplexedCreate(
    serverId, transportCtx, sendCb,
    timerCtx, scheduleCb, cancelCb
  )
  if not result.context.isNil:
    result.listener = nuraftMultiplexedGetListener(result.context)
    result.clientFactory = nuraftMultiplexedGetClientFactory(result.context)
    result.timer = nuraftMultiplexedGetTimer(result.context)
    result.running.store(true)

proc destroy*(rpc: MultiplexedRpc) =
  ## Destroy the multiplexed RPC instance.
  if rpc != nil and not rpc.context.isNil:
    nuraftMultiplexedShutdown(rpc.context)
    nuraftMultiplexedDestroy(rpc.context)
    rpc.context = nil.MultiplexedContext
    rpc.running.store(false)

proc isRunning*(rpc: MultiplexedRpc): bool =
  ## Check if the RPC is running.
  rpc != nil and rpc.running.load()

proc deliverMessage*(rpc: MultiplexedRpc, msgData: string) =
  ## Deliver a received message to the handler.
  if rpc != nil and not rpc.listener.isNil and msgData.len > 0:
    nuraftMultiplexedDeliverMessage(
      rpc.listener,
      cstring(msgData),
      csize_t(msgData.len)
    )

proc setResponseCallback*(rpc: MultiplexedRpc, respCtx: pointer,
                           respCb: MultiplexedSendResponseCb) =
  ## Set the response send callback on the listener.
  if rpc != nil and not rpc.listener.isNil:
    nuraftMultiplexedSetResponseCallback(rpc.listener, respCtx, respCb)

proc setGroupId*(rpc: MultiplexedRpc, groupIdBytes: cstring) =
  ## Set the GroupID bytes for this listener.
  if rpc != nil and not rpc.listener.isNil:
    nuraftMultiplexedSetGroupId(rpc.listener, groupIdBytes)

proc setSrcNodeId*(rpc: MultiplexedRpc, srcNodeId: int32) =
  ## Set the source node ID for this listener.
  if rpc != nil and not rpc.listener.isNil:
    nuraftMultiplexedSetSrcNodeId(rpc.listener, srcNodeId)
