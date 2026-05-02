# Nim Bindings for Multiplexed NuRaft RPC
#
# Provides convenience wrappers for the minimal C++ shim.

import fractio/distributed/raft/c_bindings

const wrapperPath = "src/fractio/distributed/raft/wrapper/build"

when not defined(macosx):
  {.passL: "-L" & wrapperPath & " -lnuraft_shim".}
{.passC: "-I" & "src/fractio/distributed/raft/wrapper".}

# =============================================================================
# Message Delivery
# =============================================================================

proc deliverMessage*(ctx: MultiplexedContext, server: NuRaftServer,
                     msgData: string) =
  ## Deliver a message to NuRaft for processing.
  ## Handles both requests (sends response via callback) and responses
  ## (matches to pending handler).
  if msgData.len == 0 or ctx.isNil or server.isNil:
    return
  nuraftMpDeliverMessage(ctx, server, cstring(msgData), csize_t(msgData.len))

# =============================================================================
# Listener Setup
# =============================================================================

proc setupListener*(ctx: MultiplexedContext, srcNodeId: int32,
                    responseCtx: pointer,
                        responseCb: SendCb): MultiplexedListener =
  ## Get the listener from the context and configure it for response handling.
  ## Returns the listener handle (caller should destroy when done).
  result = nuraftMpGetListener(ctx)
  if not result.isNil:
    nuraftMpListenerSetSrcNodeId(result, srcNodeId)
    nuraftMpListenerSetSendResponseCallback(result, responseCtx, responseCb)

proc destroyListener*(listener: MultiplexedListener) =
  ## Free the listener handle.
  if not listener.isNil:
    nuraftMpListenerDestroy(listener)
