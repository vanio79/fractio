# Recovery Test 5 - Step 2: Parent verifies crash data recovery
# Run after 05_crash_child.nim

import std/os
import std/options
import fractio/distributed/raft/types
import fractio/distributed/raft/node
import fractio/distributed/raft/state_machine

type TestStateMachine = ref object of StateMachine
  commits: seq[(int64, string)]
  rollbacks: seq[(int64, string)]
  lastAppliedIndex: int64

method commit(sm: TestStateMachine, logIdx: int64, data: string): string =
  sm.commits.add((logIdx, data))
  sm.lastAppliedIndex = logIdx
  return "OK"

method rollback(sm: TestStateMachine, logIdx: int64, data: string) =
  sm.rollbacks.add((logIdx, data))

method getLastAppliedIndex(sm: TestStateMachine): int64 =
  result = sm.lastAppliedIndex

let testPath = "tmp/raft_recovery_crash/"

let config = RaftConfig(
  serverId: 1,
  endpoint: "127.0.0.1:9000",
  electionTimeout: 1000,
  heartbeatInterval: 100,
  logStoragePath: testPath,
  snapshotEnabled: false,
  snapshotDistance: 1000,
  maxAppendSize: 100
)

var sm = TestStateMachine(commits: @[], rollbacks: @[], lastAppliedIndex: 0)
var raftNode = RaftNodeImpl(
  serverId: config.serverId,
  endpoint: config.endpoint,
  config: config,
  nodeState: RaftNodeState(role: SR_FOLLOWER, currentTerm: 0, votedFor: -1,
      leaderId: -1, commitIndex: 0, lastApplied: 0),
  logStore: nil, stateMachine: sm, initialized: false, isLeader: false,
      leaderId: -1, commitIndex: 0, lastApplied: 0
)

let success = raftNode.init(config, sm)
if not success:
  echo "FAIL: Failed to recover log store"
  quit(1)

let entry = raftNode.wsLogStore.getEntry(1)
if not entry.isSome or entry.get.data != "crash-data":
  echo "FAIL: crash-data not found or wrong data"
  quit(1)

raftNode.shutdown()
removeDir(testPath)
echo "OK: Node state after crash simulation verified"
