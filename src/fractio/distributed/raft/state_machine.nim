# State Machine Interface and Implementations

import std/tables

import fractio/distributed/raft/types
import fractio/utils/logging

type
  StateMachineImpl* = ref object of StateMachine
    ## Base implementation for state machines
    lastIndex*: int64

  KVStateMachine* = ref object of StateMachineImpl
    ## Key-value state machine — lightweight tracking object.
    ## All data is read/written through the WiscKey backend directly;
    ## the KVStateMachine only tracks the last applied log index.

  RaftStateMachineError* = object of CatchableError
    ## State machine specific errors

proc newKVStateMachine*(): KVStateMachine =
  ## Create a new key-value state machine
  new(result)
  result.lastIndex = 0

method commit*(sm: KVStateMachine, logIdx: int64, data: string): string =
  ## Apply a committed log entry to the state machine.
  ## Data is persisted by the WiscKey backend; this only tracks the index.
  sm.lastIndex = logIdx
  result = "ok"

method rollback*(sm: KVStateMachine, logIdx: int64, data: string) =
  ## Rollback a log entry (no-op for KV store)
  var fields = initTable[string, string]()
  fields["logIdx"] = $logIdx
  debug("Rollback not implemented for KV store", fields)

method getLastAppliedIndex*(sm: KVStateMachine): int64 =
  ## Get the last applied log index
  return sm.lastIndex
