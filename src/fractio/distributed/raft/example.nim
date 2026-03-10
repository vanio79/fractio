# Example: Using Raft Node

import std/json

import fractio/distributed/raft/types
import fractio/distributed/raft/node
import fractio/distributed/raft/state_machine
import fractio/distributed/raft/cluster

type
  ExampleStateMachine* = ref object of StateMachine

proc commit*(sm: ExampleStateMachine, logIdx: int64, data: string): string =
  debug "Committing log entry", logIdx = $logIdx, data = data
  return "processed"

proc rollback*(sm: ExampleStateMachine, logIdx: int64, data: string) =
  debug "Rolling back log entry", logIdx = $logIdx

proc getLastAppliedIndex*(sm: ExampleStateMachine): int64 =
  return 0

proc exampleUsage*() =
  echo "=== Raft Example ==="

  # Create configuration
  let config = RaftConfig(
    serverId: 1,
    endpoint: "localhost:8080",
    electionTimeout: 150,
    heartbeatInterval: 50,
    logStoragePath: "raft_data/node1",
    snapshotEnabled: true,
    snapshotDistance: 1000
  )

  # Create state machine
  let stateMachine = newKVStateMachine()

  # Create Raft node
  let raftNode = RaftNodeImpl()
  if not raftNode.init(config, stateMachine):
    echo "Failed to initialize Raft node"
    return

  # Create cluster
  let cluster = newRaftCluster(config)
  discard cluster.addServer(1, "localhost:8080")
  discard cluster.addServer(2, "localhost:8081")
  discard cluster.addServer(3, "localhost:8082")

  echo "Cluster Info:"
  echo cluster.getClusterInfo()

  # Example operations
  echo "\n=== Example Operations ==="

  # Put operation
  let putData = "put:name:John Doe"
  let putIndex = raftNode.commit(putData)
  echo "Put operation committed at index: $#, Data: $#".format($putIndex, putData)

  # Get operation
  let getData = "get:name"
  let getIndex = raftNode.commit(getData)
  echo "Get operation committed at index: $#, Data: $#".format($getIndex, getData)

  # Delete operation
  let deleteData = "delete:name"
  let deleteIndex = raftNode.commit(deleteData)
  echo "Delete operation committed at index: $#, Data: $#".format($deleteIndex, deleteData)

  # Print state machine info
  echo "\n=== State Machine ==="
  echo "Last applied index: $#".format($stateMachine.lastIndex)

  # Cleanup
  raftNode.shutdown()
  echo "Raft node shutdown successfully"

when isMainModule:
  exampleUsage()
