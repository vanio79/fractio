# Nim Bindings for NuRaft C Wrapper

import std/sequtils
import std/sets
import std/tables

# C function declarations
{.passL: "-Lthirdparty/NuRaft/wrapper -lnuraft_c_wrapper".}
{.passC: "-Ithirdparty/NuRaft/wrapper".}

# Buffer operations
type
  Buffer* = distinct pointer

proc bufferCreate*(size: int): Buffer {.importc: "nuraft_buffer_create", dynlib: "libnuraft_c_wrapper.so".}
proc bufferDestroy*(buf: Buffer) {.importc: "nuraft_buffer_destroy", dynlib: "libnuraft_c_wrapper.so".}
proc bufferData*(buf: Buffer): pointer {.importc: "nuraft_buffer_data", dynlib: "libnuraft_c_wrapper.so".}
proc bufferSize*(buf: Buffer): int {.importc: "nuraft_buffer_size", dynlib: "libnuraft_c_wrapper.so".}
proc bufferPos*(buf: Buffer): int {.importc: "nuraft_buffer_pos", dynlib: "libnuraft_c_wrapper.so".}
proc bufferSetPos*(buf: Buffer, pos: int) {.importc: "nuraft_buffer_set_pos", dynlib: "libnuraft_c_wrapper.so".}
proc bufferSkip*(buf: Buffer, count: int) {.importc: "nuraft_buffer_skip", dynlib: "libnuraft_c_wrapper.so".}
proc bufferPut*(buf: Buffer, data: pointer, len: int) {.importc: "nuraft_buffer_put", dynlib: "libnuraft_c_wrapper.so".}
proc bufferGetChar*(buf: Buffer): char {.importc: "nuraft_buffer_get_char", dynlib: "libnuraft_c_wrapper.so".}
proc bufferGetInt*(buf: Buffer): int32 {.importc: "nuraft_buffer_get_int", dynlib: "libnuraft_c_wrapper.so".}
proc bufferGetLong*(buf: Buffer): int64 {.importc: "nuraft_buffer_get_long", dynlib: "libnuraft_c_wrapper.so".}
proc bufferPutChar*(buf: Buffer, c: char) {.importc: "nuraft_buffer_put_char", dynlib: "libnuraft_c_wrapper.so".}
proc bufferPutInt*(buf: Buffer, i: int32) {.importc: "nuraft_buffer_put_int", dynlib: "libnuraft_c_wrapper.so".}
proc bufferPutLong*(buf: Buffer, l: int64) {.importc: "nuraft_buffer_put_long", dynlib: "libnuraft_c_wrapper.so".}

# Logger operations
type
  Logger* = distinct pointer

proc loggerCreate*(level: int32): Logger {.importc: "nuraft_logger_create", dynlib: "libnuraft_c_wrapper.so".}
proc loggerDestroy*(logger: Logger) {.importc: "nuraft_logger_destroy", dynlib: "libnuraft_c_wrapper.so".}

# Raft Parameters
type
  Params* = distinct pointer

proc paramsCreate*(): Params {.importc: "nuraft_params_create", dynlib: "libnuraft_c_wrapper.so".}
proc paramsDestroy*(params: Params) {.importc: "nuraft_params_destroy", dynlib: "libnuraft_c_wrapper.so".}
proc paramsSetElectionTimeout*(params: Params, ms: int32) {.importc: "nuraft_params_set_election_timeout", dynlib: "libnuraft_c_wrapper.so".}
proc paramsSetHeartbeatInterval*(params: Params, ms: int32) {.importc: "nuraft_params_set_heartbeat_interval", dynlib: "libnuraft_c_wrapper.so".}
proc paramsSetHeartbeatTimeout*(params: Params, ms: int32) {.importc: "nuraft_params_set_heartbeat_timeout", dynlib: "libnuraft_c_wrapper.so".}
proc paramsSetLogMaxSize*(params: Params, size: int) {.importc: "nuraft_params_set_log_max_size", dynlib: "libnuraft_c_wrapper.so".}
proc paramsSetSnapshotEnabled*(params: Params, enabled: bool) {.importc: "nuraft_params_set_snapshot_enabled", dynlib: "libnuraft_c_wrapper.so".}
proc paramsSetRpcFailureMax*(params: Params, max: int32) {.importc: "nuraft_params_set_rpc_failure_max", dynlib: "libnuraft_c_wrapper.so".}

# Log Store operations
type
  LogStore* = distinct pointer

proc logStoreCreate*(path: string): LogStore {.importc: "nuraft_log_store_create", dynlib: "libnuraft_c_wrapper.so".}
proc logStoreDestroy*(store: LogStore) {.importc: "nuraft_log_store_destroy", dynlib: "libnuraft_c_wrapper.so".}
proc logStoreNextSlot*(store: LogStore): int64 {.importc: "nuraft_log_store_next_slot", dynlib: "libnuraft_c_wrapper.so".}
proc logStoreStartIndex*(store: LogStore): int64 {.importc: "nuraft_log_store_start_index", dynlib: "libnuraft_c_wrapper.so".}
proc logStoreLastEntry*(store: LogStore): pointer {.importc: "nuraft_log_store_last_entry", dynlib: "libnuraft_c_wrapper.so".}
proc logStoreAppend*(store: LogStore, entry: pointer): int64 {.importc: "nuraft_log_store_append", dynlib: "libnuraft_c_wrapper.so".}
proc logStoreWriteAt*(store: LogStore, index: int64, entry: pointer) {.importc: "nuraft_log_store_write_at", dynlib: "libnuraft_c_wrapper.so".}
proc logStoreEntries*(store: LogStore, start: int64, end: int64): pointer {.importc: "nuraft_log_store_entries", dynlib: "libnuraft_c_wrapper.so".}
proc logStoreEntryAt*(store: LogStore, index: int64): pointer {.importc: "nuraft_log_store_entry_at", dynlib: "libnuraft_c_wrapper.so".}
proc logStoreTermAt*(store: LogStore, index: int64): int64 {.importc: "nuraft_log_store_term_at", dynlib: "libnuraft_c_wrapper.so".}
proc logStorePack*(store: LogStore, index: int64, cnt: int32): pointer {.importc: "nuraft_log_store_pack", dynlib: "libnuraft_c_wrapper.so".}
proc logStoreApplyPack*(store: LogStore, index: int64, pack: pointer) {.importc: "nuraft_log_store_apply_pack", dynlib: "libnuraft_c_wrapper.so".}
proc logStoreCompact*(store: LogStore, lastLogIndex: int64): bool {.importc: "nuraft_log_store_compact", dynlib: "libnuraft_c_wrapper.so".}
proc logStoreFlush*(store: LogStore): bool {.importc: "nuraft_log_store_flush", dynlib: "libnuraft_c_wrapper.so".}

# State Manager operations
type
  StateMgr* = distinct pointer

proc stateMgrCreate*(path: string, serverId: int32): StateMgr {.importc: "nuraft_state_mgr_create", dynlib: "libnuraft_c_wrapper.so".}
proc stateMgrDestroy*(mgr: StateMgr) {.importc: "nuraft_state_mgr_destroy", dynlib: "libnuraft_c_wrapper.so".}
proc stateMgrLoadConfig*(mgr: StateMgr): pointer {.importc: "nuraft_state_mgr_load_config", dynlib: "libnuraft_c_wrapper.so".}
proc stateMgrSaveConfig*(mgr: StateMgr, config: pointer) {.importc: "nuraft_state_mgr_save_config", dynlib: "libnuraft_c_wrapper.so".}
proc stateMgrSaveState*(mgr: StateMgr, state: pointer) {.importc: "nuraft_state_mgr_save_state", dynlib: "libnuraft_c_wrapper.so".}
proc stateMgrReadState*(mgr: StateMgr): pointer {.importc: "nuraft_state_mgr_read_state", dynlib: "libnuraft_c_wrapper.so".}
proc stateMgrLoadLogStore*(mgr: StateMgr): LogStore {.importc: "nuraft_state_mgr_load_log_store", dynlib: "libnuraft_c_wrapper.so".}
proc stateMgrServerId*(mgr: StateMgr): int32 {.importc: "nuraft_state_mgr_server_id", dynlib: "libnuraft_c_wrapper.so".}

# Log Entry operations
type
  LogEntry* = distinct pointer

proc logEntryCreate*(): LogEntry {.importc: "nuraft_log_entry_create", dynlib: "libnuraft_c_wrapper.so".}
proc logEntryDestroy*(entry: LogEntry) {.importc: "nuraft_log_entry_destroy", dynlib: "libnuraft_c_wrapper.so".}
proc logEntrySetTerm*(entry: LogEntry, term: int64) {.importc: "nuraft_log_entry_set_term", dynlib: "libnuraft_c_wrapper.so".}
proc logEntryGetTerm*(entry: LogEntry): int64 {.importc: "nuraft_log_entry_get_term", dynlib: "libnuraft_c_wrapper.so".}
proc logEntrySetValType*(entry: LogEntry, valType: int32) {.importc: "nuraft_log_entry_set_val_type", dynlib: "libnuraft_c_wrapper.so".}
proc logEntryGetValType*(entry: LogEntry): int32 {.importc: "nuraft_log_entry_get_val_type", dynlib: "libnuraft_c_wrapper.so".}
proc logEntrySetData*(entry: LogEntry, data: pointer) {.importc: "nuraft_log_entry_set_data", dynlib: "libnuraft_c_wrapper.so".}
proc logEntryGetData*(entry: LogEntry): pointer {.importc: "nuraft_log_entry_get_data", dynlib: "libnuraft_c_wrapper.so".}

# Raft Server operations
type
  RaftServer* = distinct pointer

proc raftServerCreate*(params: Params, stateMgr: StateMgr, stateMachine: pointer, logger: Logger): RaftServer {.importc: "nuraft_raft_server_create", dynlib: "libnuraft_c_wrapper.so".}
proc raftServerDestroy*(server: RaftServer) {.importc: "nuraft_raft_server_destroy", dynlib: "libnuraft_c_wrapper.so".}
proc raftServerInit*(server: RaftServer): bool {.importc: "nuraft_raft_server_init", dynlib: "libnuraft_c_wrapper.so".}
proc raftServerShutdown*(server: RaftServer) {.importc: "nuraft_raft_server_shutdown", dynlib: "libnuraft_c_wrapper.so".}
proc raftServerGetLeader*(server: RaftServer): int32 {.importc: "nuraft_raft_server_get_leader", dynlib: "libnuraft_c_wrapper.so".}
proc raftServerIsLeader*(server: RaftServer): bool {.importc: "nuraft_raft_server_is_leader", dynlib: "libnuraft_c_wrapper.so".}
proc raftServerCommit*(server: RaftServer, data: pointer, len: int): int64 {.importc: "nuraft_raft_server_commit", dynlib: "libnuraft_c_wrapper.so".}

# Helper procs for Nim compatibility
proc newBuffer*(size: int): Buffer =
  ## Create a new buffer
  result = bufferCreate(size)

proc freeBuffer*(buf: Buffer) =
  ## Free a buffer
  bufferDestroy(buf)

proc newLogger*(level: int32): Logger =
  ## Create a new logger
  result = loggerCreate(level)

proc freeLogger*(logger: Logger) =
  ## Free a logger
  loggerDestroy(logger)

proc newParams*(): Params =
  ## Create new raft parameters
  result = paramsCreate()

proc freeParams*(params: Params) =
  ## Free raft parameters
  paramsDestroy(params)

proc newLogStore*(path: string): LogStore =
  ## Create new log store
  result = logStoreCreate(path)

proc freeLogStore*(store: LogStore) =
  ## Free log store
  logStoreDestroy(store)

proc newStateMgr*(path: string, serverId: int32): StateMgr =
  ## Create new state manager
  result = stateMgrCreate(path, serverId)

proc freeStateMgr*(mgr: StateMgr) =
  ## Free state manager
  stateMgrDestroy(mgr)

proc newLogEntry*(): LogEntry =
  ## Create new log entry
  result = logEntryCreate()

proc freeLogEntry*(entry: LogEntry) =
  ## Free log entry
  logEntryDestroy(entry)

proc newRaftServer*(params: Params, stateMgr: StateMgr, stateMachine: pointer, logger: Logger): RaftServer =
  ## Create new raft server
  result = raftServerCreate(params, stateMgr, stateMachine, logger)

proc freeRaftServer*(server: RaftServer) =
  ## Free raft server
  raftServerDestroy(server)

# Utility procs
proc bufferToString*(buf: Buffer): string =
  ## Convert buffer to Nim string
  if buf == nil:
    return ""
  let size = bufferSize(buf)
  if size == 0:
    return ""
  
  let dataPtr = bufferData(buf)
  result = newString(size)
  if size > 0:
    copyMem(addr result[0], dataPtr, size)

proc stringToBuffer*(str: string): Buffer =
  ## Convert Nim string to buffer
  result = bufferCreate(str.len)
  bufferPut(result, addr str[0], str.len)

proc serializeLogEntry*(entry: LogEntry): string =
  ## Serialize log entry to string
  let term = logEntryGetTerm(entry)
  let valType = logEntryGetValType(entry)
  let dataPtr = logEntryGetData(entry)
  let dataSize = bufferSize(dataPtr)
  
  # Simplified serialization
  result = "{" \"term\": $term, \"type\": $valType, \"data\": \"" & $dataSize & "\"}"

proc deserializeLogEntry*(str: string): LogEntry =
  ## Deserialize string to log entry
  result = newLogEntry()
  # Simplified deserialization - in real implementation, parse the string
  logEntrySetTerm(result, 1)
  logEntrySetValType(result, 0)
  logEntrySetData(result, stringToBuffer(str))