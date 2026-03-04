# State Machine Interface and Implementations

import std/json
import std/tables
import std/strutils
import std/options
import std/sequtils

import fractio/distributed/raft/types
import fractio/utils/logging

type
  StateMachineImpl* = ref object of StateMachine
    ## Base implementation for state machines
    kvStore*: Table[string, string]
    lastIndex*: int64

  KVStateMachine* = ref object of StateMachineImpl
    ## Key-value state machine implementation

  RaftStateMachineError* = object of CatchableError
    ## State machine specific errors

proc newKVStateMachine*(): KVStateMachine =
  ## Create a new key-value state machine
  new(result)
  result.kvStore = initTable[string, string]()
  result.lastIndex = 0

method commit*(sm: KVStateMachine, logIdx: int64, data: string): string =
  ## Apply a committed log entry to the state machine
  var fields = initTable[string, string]()
  fields["logIdx"] = $logIdx
  fields["data"] = data
  debug("Applying committed log entry", fields)

  # Parse the data - expected format: "op:key:value" or "op:key"
  let parts = data.split(':')
  if parts.len < 2:
    raise newException(RaftStateMachineError, "Invalid log entry format")

  let op = parts[0]
  case op
  of "put":
    if parts.len != 3:
      raise newException(RaftStateMachineError, "Put operation requires key and value")
    let key = parts[1]
    let value = parts[2]
    sm.kvStore[key] = value
    result = "ok"
  of "delete":
    if parts.len != 2:
      raise newException(RaftStateMachineError, "Delete operation requires key")
    let key = parts[1]
    sm.kvStore.del(key)
    result = "ok"
  of "get":
    if parts.len != 2:
      raise newException(RaftStateMachineError, "Get operation requires key")
    let key = parts[1]
    result = sm.kvStore.getOrDefault(key, "")
  else:
    raise newException(RaftStateMachineError, "Unknown operation: " & op)

method rollback*(sm: KVStateMachine, logIdx: int64, data: string) =
  ## Rollback a log entry (no-op for KV store)
  var fields = initTable[string, string]()
  fields["logIdx"] = $logIdx
  debug("Rollback not implemented for KV store", fields)

method getLastAppliedIndex*(sm: KVStateMachine): int64 =
  ## Get the last applied log index
  return sm.lastIndex

proc get*(sm: KVStateMachine, key: string): Option[string] =
  ## Get a value from the KV store
  if sm.kvStore.hasKey(key):
    return some(sm.kvStore[key])
  else:
    return none(string)

proc put*(sm: KVStateMachine, key: string, value: string) =
  ## Put a value into the KV store
  sm.kvStore[key] = value

proc delete*(sm: KVStateMachine, key: string) =
  ## Delete a key from the KV store
  sm.kvStore.del(key)

proc getStats*(sm: KVStateMachine): Table[string, int] =
  ## Get statistics about the KV store
  result = initTable[string, int]()
  result["keys"] = sm.kvStore.len
  result["size"] = 0
  for value in sm.kvStore.values():
    result["size"] += value.len

proc clear*(sm: KVStateMachine) =
  ## Clear all entries from the KV store
  sm.kvStore.clear()

proc getKeys*(sm: KVStateMachine): seq[string] =
  ## Get all keys from the KV store
  result = sm.kvStore.keys().toSeq
