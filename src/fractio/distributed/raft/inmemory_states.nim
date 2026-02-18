## In-memory implementation of RaftStateMachine for testing and as a placeholder.
## Provides a simple key-value store and cluster config manipulation.

import ./states, ./types, ../../core/errors, std/tables, std/atomics

type
  InMemoryStateMachine* {.gcsafe, acyclic.} = ref object of RaftStateMachine
    data*: Table[string, string] ## key-value store for client commands
    lock*: Atomic[uint32]        ## spinlock: 0 = unlocked, 1 = locked
    config*: seq[NodeId]         ## current cluster membership
    applyCount*: int             ## testing: number of times applyImpl was invoked

proc newInMemoryStateMachine*(initialPeers: seq[NodeId] = @[]): InMemoryStateMachine =
  ## Creates a new in-memory state machine.
  ## `initialPeers` sets the initial cluster configuration.
  result = InMemoryStateMachine(config: initialPeers)
  result.data = initTable[string, string]()
  result.applyCount = 0
  # Atomic[uint32] is zero-initialized; no explicit init needed.

method applyImpl*(self: InMemoryStateMachine, entry: RaftEntry): Result[(),
    FractioError] {.gcsafe, raises: [].} =
  ## Apply a committed entry to the in-memory state.
  ## Handles all command kinds:
  ## - rckNoop: nothing
  ## - rckAddNode/rckRemoveNode: modify config (data encodes uint64 nodeId)
  ## - rckReconfigure: replace config (data: count uint32 + nodeIds...)
  ## - rckClientCommand: deserialize ClientCommand and perform SET/DELETE.
  # Acquire spinlock
  while self.lock.exchange(1) == 1:
    discard
  try:
    inc self.applyCount
    case entry.command.kind
    of rckNoop:
      discard
    of rckAddNode:
      if entry.command.data.len != 8:
        return err[(), FractioError](storageError(
            "AddNode expects exactly 8 bytes (uint64 nodeId)", "inmemory"))
      var nid: uint64
      copyMem(addr nid, entry.command.data[0].addr, 8)
      if nid notin self.config:
        self.config.add(nid)
    of rckRemoveNode:
      if entry.command.data.len != 8:
        return err[(), FractioError](storageError(
            "RemoveNode expects exactly 8 bytes (uint64 nodeId)", "inmemory"))
      var nid: uint64
      copyMem(addr nid, entry.command.data[0].addr, 8)
      for i, existing in self.config:
        if existing == nid:
          self.config.delete(i)
          break
    of rckReconfigure:
      # Data format: count (uint32) followed by count * uint64 nodeIds, all in host byte order (for simplicity)
      if entry.command.data.len < 4:
        return err[(), FractioError](storageError(
            "Reconfigure data too short for count", "inmemory"))
      var count: uint32
      copyMem(addr count, entry.command.data[0].addr, 4)
      let expected = 4 + int(count) * 8
      if entry.command.data.len != expected:
        return err[(), FractioError](storageError(
            "Reconfigure data length mismatch", "inmemory"))
      var newConfig: seq[NodeId] = @[]
      for i in 0..<int(count):
        var nid: uint64
        copyMem(addr nid, entry.command.data[4 + i*8].addr, 8)
        newConfig.add(nid)
      self.config = newConfig
    of rckClientCommand:
      let cmdRes = deserialize(entry.command.data)
      if cmdRes.isErr:
        return err[(), FractioError](cmdRes.error)
      let cmd = cmdRes.value
      case cmd.op
      of cckSet:
        self.data[cmd.key] = cmd.value
      of cckDelete:
        if cmd.key in self.data:
          self.data.del(cmd.key)
    return ok[(), FractioError](())
  finally:
    self.lock.store(0)

proc getValue*(self: InMemoryStateMachine, key: string): string =
  ## Retrieve the current value for `key` (empty string if not present).
  while self.lock.exchange(1) == 1:
    discard
  try:
    result = self.data.getOrDefault(key, "")
  finally:
    self.lock.store(0)

proc hasKey*(self: InMemoryStateMachine, key: string): bool =
  while self.lock.exchange(1) == 1:
    discard
  try:
    result = key in self.data
  finally:
    self.lock.store(0)

proc getConfig*(self: InMemoryStateMachine): seq[NodeId] =
  while self.lock.exchange(1) == 1:
    discard
  try:
    result = self.config
  finally:
    self.lock.store(0)

proc resetApplyCount*(self: InMemoryStateMachine) {.gcsafe, raises: [].} =
  ## Reset the apply counter to zero. Useful for tests.
  while self.lock.exchange(1) == 1:
    discard
  try:
    self.applyCount = 0
  finally:
    self.lock.store(0)

# TODO: Properly deinitialize lock in finalizer. Currently disabled due to compiler signature issue.
# proc `=destroy`(self: InMemoryStateMachine) =
#   ## GC finalizer: deinitialize the lock to prevent resource leaks.
#   deinitLock(self.lock)
