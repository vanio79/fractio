# Raft Store Callbacks — Nim ↔ C callback bridge for WiscKey-backed persistence
#
# This module provides Nim procs that match the C function pointer signatures
# declared in nuraft_shim.h. These callbacks bridge between the C++ NuRaft
# log_store/state_mgr and the Nim RaftPersistentStore.
#
# The C++ callback_log_store and dynamic_state_mgr (with callbacks) call these
# Nim procs via function pointers. Each proc extracts the RaftPersistentStore
# from the opaque ctx pointer and delegates the operation.

import std/options
import fractio/distributed/raft/raft_persistent_store
import fractio/distributed/raft/c_bindings

# ============================================================================
# Log Store Callbacks
# ============================================================================

proc logAppendCb*(ctx: pointer, term: uint64, valType: int32,
    entryData: cstring, entryLen: csize_t): uint64 {.cdecl.} =
  ## Append a log entry. Called from C++ callback_log_store::append().
  ## NuRaft val_type values: app_log=1, conf=2, cluster_server=3, log_pack=4.
  ## Our LogValType enum matches these values directly.
  let store = cast[RaftPersistentStore](ctx)
  let valTypeEnum = LogValType(valType)
  let dataStr = if entryLen > 0 and not entryData.isNil:
    var s = newString(int(entryLen))
    copyMem(addr s[0], entryData, int(entryLen))
    s
  else:
    ""
  let entry = RaftLogEntry(term: term, valType: valTypeEnum, data: dataStr)
  store.appendEntry(entry)

proc logWriteAtCb*(ctx: pointer, index: uint64, term: uint64,
    valType: int32, entryData: cstring, entryLen: csize_t) {.cdecl.} =
  ## Write a log entry at the given index, truncating all after it.
  ## Called from C++ callback_log_store::write_at().
  ## NuRaft val_type values: app_log=1, conf=2, cluster_server=3, log_pack=4.
  ## Our LogValType enum matches these values directly.
  let store = cast[RaftPersistentStore](ctx)
  let valTypeEnum = LogValType(valType)
  let dataStr = if entryLen > 0 and not entryData.isNil:
    var s = newString(int(entryLen))
    copyMem(addr s[0], entryData, int(entryLen))
    s
  else:
    ""
  let entry = RaftLogEntry(term: term, valType: valTypeEnum, data: dataStr)
  store.writeAt(index, entry)

proc logGetCb*(ctx: pointer, index: uint64, outTerm: ptr uint64,
    outValType: ptr int32, outData: cstring,
        outCapacity: csize_t): csize_t {.cdecl.} =
  ## Get a log entry at the given index.
  ## Called from C++ callback_log_store::entry_at() and last_entry().
  let store = cast[RaftPersistentStore](ctx)
  let entryOpt = store.getEntry(index)
  if entryOpt.isNone:
    return 0.csize_t
  let entry = entryOpt.get()
  if not outTerm.isNil:
    outTerm[] = entry.term
  if not outValType.isNil:
    outValType[] = int32(ord(entry.valType))
  # Return the raw payload data length. The C++ side reconstructs a log_entry
  # from (term, val_type, data) where data is the payload bytes.
  # Since term and valType are already returned via outTerm/outValType,
  # we only need to copy the payload data.
  let payloadLen = min(csize_t(entry.data.len), outCapacity)
  if payloadLen > 0 and not outData.isNil:
    copyMem(outData, addr entry.data[0], int(payloadLen))
  result = csize_t(entry.data.len)

proc logTermAtCb*(ctx: pointer, index: uint64): uint64 {.cdecl.} =
  ## Get the term at the given index. Called from C++ callback_log_store::term_at().
  let store = cast[RaftPersistentStore](ctx)
  store.termAt(index)

proc logNextSlotCb*(ctx: pointer): uint64 {.cdecl.} =
  ## Get the next available log slot. Called from C++ callback_log_store::next_slot().
  let store = cast[RaftPersistentStore](ctx)
  store.nextSlot()

proc logStartIndexCb*(ctx: pointer): uint64 {.cdecl.} =
  ## Get the start index of the log. Called from C++ callback_log_store::start_index().
  let store = cast[RaftPersistentStore](ctx)
  store.startIndex()

proc logPackCb*(ctx: pointer, index: uint64, count: int32,
    outData: cstring, outCapacity: csize_t): csize_t {.cdecl.} =
  ## Pack log entries starting at index for count entries.
  ## Called from C++ callback_log_store::pack().
  let store = cast[RaftPersistentStore](ctx)
  let packed = store.packEntries(index, count)
  let copyLen = min(csize_t(packed.len), outCapacity)
  if copyLen > 0 and not outData.isNil:
    copyMem(outData, addr packed[0], int(copyLen))
  result = csize_t(packed.len)

proc logApplyPackCb*(ctx: pointer, index: uint64,
    packData: cstring, packLen: csize_t) {.cdecl.} =
  ## Apply packed log entries starting at index.
  ## Called from C++ callback_log_store::apply_pack().
  let store = cast[RaftPersistentStore](ctx)
  if packLen > 0 and not packData.isNil:
    let data = newString(int(packLen))
    copyMem(addr data[0], packData, int(packLen))
    store.applyPack(index, data)

proc logCompactCb*(ctx: pointer, lastLogIndex: uint64): int32 {.cdecl.} =
  ## Compact the log store. Called from C++ callback_log_store::compact().
  ## Returns 0 on success.
  let store = cast[RaftPersistentStore](ctx)
  if store.compact(lastLogIndex):
    0'i32
  else:
    -1'i32

proc logFlushCb*(ctx: pointer): int32 {.cdecl.} =
  ## Flush all pending writes. Called from C++ callback_log_store::flush().
  ## Returns 0 on success.
  let store = cast[RaftPersistentStore](ctx)
  if store.flush():
    0'i32
  else:
    -1'i32

# ============================================================================
# State Manager Callbacks
# ============================================================================

proc stateSaveCb*(ctx: pointer, term: uint64, votedFor: int32,
    configHwm: uint64) {.cdecl.} =
  ## Save Raft state (term, voted_for, config_hwm).
  ## Called from C++ dynamic_state_mgr::save_state() when callbacks are set.
  let store = cast[RaftPersistentStore](ctx)
  let state = RaftState(term: term, votedFor: votedFor,
      configLogIdxHwm: configHwm)
  store.saveState(state)

proc stateReadCb*(ctx: pointer, outTerm: ptr uint64, outVotedFor: ptr int32,
    outConfigHwm: ptr uint64): int32 {.cdecl.} =
  ## Load Raft state from persistent storage.
  ## Returns 1 if state was found, 0 if not found.
  let store = cast[RaftPersistentStore](ctx)
  let stateOpt = store.loadState()
  if stateOpt.isNone:
    return 0'i32
  let state = stateOpt.get()
  if not outTerm.isNil:
    outTerm[] = state.term
  if not outVotedFor.isNil:
    outVotedFor[] = state.votedFor
  if not outConfigHwm.isNil:
    outConfigHwm[] = state.configLogIdxHwm
  1'i32

proc configSaveCb*(ctx: pointer, configData: cstring,
    configLen: csize_t) {.cdecl.} =
  ## Save cluster config to persistent storage.
  ## Called from C++ dynamic_state_mgr::save_config() when callbacks are set.
  let store = cast[RaftPersistentStore](ctx)
  if configLen > 0 and not configData.isNil:
    let data = newString(int(configLen))
    copyMem(addr data[0], configData, int(configLen))
    store.saveClusterConfig(data)

proc configLoadCb*(ctx: pointer, outData: cstring,
    outCapacity: csize_t): csize_t {.cdecl.} =
  ## Load cluster config from persistent storage.
  ## Returns size of config data, or 0 if not found.
  let store = cast[RaftPersistentStore](ctx)
  let configOpt = store.loadClusterConfig()
  if configOpt.isNone:
    return 0.csize_t
  let configData = configOpt.get()
  let copyLen = min(csize_t(configData.len), outCapacity)
  if copyLen > 0 and not outData.isNil:
    copyMem(outData, addr configData[0], int(copyLen))
  csize_t(configData.len)

# ============================================================================
# Helper: Create a callback-based state manager using RaftPersistentStore
# ============================================================================

proc createCallbackSmgr*(store: RaftPersistentStore,
    myServerId: int32, myEndpoint: string,
    serverIds: seq[int32], endpoints: seq[string],
    catchingUp: bool): NuRaftSMgr =
  ## Create a NuRaft state manager backed by WiscKey via RaftPersistentStore.
  ## This replaces file-based persistence with callback-based persistence.
  ##
  ## `store` is NOT copied — the caller must keep it alive for the lifetime
  ## of the state manager.
  ##
  ## The same `store` pointer is used as both the log_store context and
  ## the state callback context, since both log and state operations are
  ## handled by RaftPersistentStore.
  var cServerIds = newSeq[int32](serverIds.len)
  var cEndpoints = newSeq[cstring](endpoints.len)
  for i in 0..<serverIds.len:
    cServerIds[i] = serverIds[i]
    cEndpoints[i] = cstring(endpoints[i])

  result = nuraftSmgrCreateWithCallbacks(
    myServerId = myServerId,
    myEndpoint = cstring(myEndpoint),
    numServers = int32(serverIds.len),
    serverIds = addr cServerIds[0],
    endpoints = addr cEndpoints[0],
    catchingUp = catchingUp,
    # Log store callbacks — all use the same store context
    logStoreCtx = cast[pointer](store),
    logAppendCb = logAppendCb,
    logWriteAtCb = logWriteAtCb,
    logGetCb = logGetCb,
    logTermAtCb = logTermAtCb,
    logNextSlotCb = logNextSlotCb,
    logStartIndexCb = logStartIndexCb,
    logPackCb = logPackCb,
    logApplyPackCb = logApplyPackCb,
    logCompactCb = logCompactCb,
    logFlushCb = logFlushCb,
    # State callbacks — also use the same store context
    stateCbCtx = cast[pointer](store),
    stateSaveCb = stateSaveCb,
    stateReadCb = stateReadCb,
    configSaveCb = configSaveCb,
    configLoadCb = configLoadCb
  )
