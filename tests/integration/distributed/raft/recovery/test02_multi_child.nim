# Recovery Test 2 - Step 1: Child creates multiple log entries
# Run before 02_multi_parent.nim

import std/os
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

let testPath = "tmp/raft_recovery_multi/"

if dirExists(testPath):
  removeDir(testPath)

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

discard raftNode.init(config, sm)
raftNode.becomeCandidate()
raftNode.becomeLeader()

let idx1 = raftNode.commit("entry1")
let idx2 = raftNode.commit("entry2")
let idx3 = raftNode.commit("entry3")

if idx1 != 1 or idx2 != 2 or idx3 != 3:
  echo "FAIL: Unexpected indices: ", idx1, ", ", idx2, ", ", idx3
  quit(1)

echo "Child: committed 3 entries"
raftNode.shutdown()
echo "OK: Multiple entries created"
