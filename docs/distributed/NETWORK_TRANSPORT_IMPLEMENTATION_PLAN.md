# Network Transport Implementation Plan

## Overview

This document provides a detailed implementation plan for the TCP-based network transport layer. It is designed to be executed in phases, with each phase delivering working, testable functionality.

**Total Estimated Time:** 8-12 days

---

## Phase 1: Core Network Layer (Days 1-3)

### Goal
Create the foundation for TCP communication: message types, serialization, and basic transport.

### Tasks

#### 1.1 Create Network Types (`network/types.nim`)

**Time:** 2 hours

```nim
# src/fractio/distributed/network/types.nim

type
  # Message header for all protocol messages
  MessageHeader* = object
    messageType*: uint16
    messageId*: uint64
    sourceNodeId*: uint16
    targetNodeId*: uint16
    term*: uint64

  # Raft message types
  RaftMessageType* = enum
    rmtRequestVote = 1
    rmtRequestVoteResponse = 2
    rmtAppendEntries = 3
    rmtAppendEntriesResponse = 4
    rmtInstallSnapshot = 5
    rmtInstallSnapshotResponse = 6

  # Client message types
  ClientMessageType* = enum
    cmtBatchRequest = 100
    cmtBatchResponse = 101
    cmtScanRequest = 102
    cmtScanResponse = 103

  # Wire frame
  Frame* = object
    payloadLen*: uint32
    checksum*: uint32
    payload*: string
```

**Files to create:**
- `src/fractio/distributed/network/types.nim`

**Tests:**
- `tests/unit/distributed/network/test_types.nim`

---

#### 1.2 Create Serialization (`network/serialization.nim`)

**Time:** 4 hours

**Key functions:**
- `encodeHeader(header: MessageHeader): string`
- `decodeHeader(data: string): MessageHeader`
- `encodeFrame(payload: string): string`
- `decodeFrame(data: string): Frame`
- `computeChecksum(data: string): uint32`

**Files to create:**
- `src/fractio/distributed/network/serialization.nim`

**Tests:**
- `tests/unit/distributed/network/test_serialization.nim`

---

#### 1.3 Create TCP Transport (`network/tcp_transport.nim`)

**Time:** 6 hours

**Key components:**

```nim
type
  TCPTransport* = ref object
    nodeId*: NodeID
    bindAddr*: string
    port*: int
    serverSocket*: Socket
    connections*: Table[NodeID, Connection]
    running*: bool
    lock*: Lock

  Connection* = ref object
    nodeId*: NodeID
    socket*: Socket
    lastUsed*: Timestamp
    sendLock*: Lock

proc newTCPTransport*(nodeId: NodeID, bindAddr: string, port: int): TCPTransport
proc start*(t: TCPTransport)
proc stop*(t: TCPTransport)
proc connect*(t: TCPTransport, nodeId: NodeID, addr: string): Connection
proc send*(t: TCPTransport, nodeId: NodeID, data: string): string
proc broadcast*(t: TCPTransport, data: string)
```

**Files to create:**
- `src/fractio/distributed/network/tcp_transport.nim`

**Tests:**
- `tests/unit/distributed/network/test_tcp_transport.nim`

---

#### 1.4 Create Network Config (`network/config.nim`)

**Time:** 1 hour

**Files to create:**
- `src/fractio/distributed/network/config.nim`

---

### Phase 1 Deliverables

- [ ] `network/types.nim` with all message types
- [ ] `network/serialization.nim` with binary encoding
- [ ] `network/tcp_transport.nim` with basic TCP server/client
- [ ] `network/config.nim` with configuration types
- [ ] Unit tests passing

---

## Phase 2: Connection Management (Days 4-5)

### Goal
Implement connection pooling, health checking, and node management.

### Tasks

#### 2.1 Connection Pool (`network/connection_pool.nim`)

**Time:** 3 hours

**Key features:**
- Pool connections per node
- Idle connection timeout
- Max connections limit
- Thread-safe access

**Files to create:**
- `src/fractio/distributed/network/connection_pool.nim`

---

#### 2.2 Health Checker (`network/health_checker.nim`)

**Time:** 2 hours

**Key features:**
- Periodic ping messages
- Failure detection threshold
- Recovery detection

**Files to create:**
- `src/fractio/distributed/network/health_checker.nim`

---

#### 2.3 Connection Manager (`network/connection_manager.nim`)

**Time:** 4 hours

**Key features:**
- Coordinate multiple transports (Raft, Client, Admin)
- Node registry
- Health checking integration

**Files to create:**
- `src/fractio/distributed/network/connection_manager.nim`

---

### Phase 2 Deliverables

- [ ] `network/connection_pool.nim` working
- [ ] `network/health_checker.nim` working
- [ ] `network/connection_manager.nim` working
- [ ] Integration test: 2-node connection

---

## Phase 3: Raft Integration (Days 6-8)

### Goal
Integrate TCP transport with Raft implementation for actual consensus.

### Tasks

#### 3.1 Update Raft RPC (`raft/rpc.nim`)

**Time:** 4 hours

**Changes:**
- Implement `handleRequestVote`
- Implement `handleAppendEntries`
- Implement `handleInstallSnapshot`
- Register handlers with transport

**Files to update:**
- `src/fractio/distributed/raft/rpc.nim`

---

#### 3.2 Update Raft Node (`raft/node.nim`)

**Time:** 4 hours

**Changes:**
- Add `transport: TCPTransport` field
- Use transport for sending RPCs
- Handle incoming messages

**Files to update:**
- `src/fractio/distributed/raft/node.nim`

---

#### 3.3 Update Raft Cluster (`raft/cluster.nim`)

**Time:** 2 hours

**Changes:**
- Use network transport for cluster operations
- Implement node addition/removal over network

**Files to update:**
- `src/fractio/distributed/raft/cluster.nim`

---

#### 3.4 Raft Integration Test

**Time:** 3 hours

**Test scenarios:**
- 3-node leader election
- Log replication
- Leader failure and re-election

**Files to create:**
- `tests/integration/distributed/test_raft_network.nim`

---

### Phase 3 Deliverables

- [ ] Raft RPC handlers implemented
- [ ] Raft node using TCP transport
- [ ] 3-node Raft election working
- [ ] Log replication working

---

## Phase 4: Client Transport (Days 9-10)

### Goal
Enable client requests to be sent to remote nodes.

### Tasks

#### 4.1 Update DistSender (`sender.nim`)

**Time:** 3 hours

**Changes:**
- Use TCP transport for sending requests
- Route requests to correct nodes
- Handle responses

**Files to update:**
- `src/fractio/distributed/sender.nim`

---

#### 4.2 Client Request Handler

**Time:** 4 hours

**Implement handlers for:**
- BatchRequest
- ScanRequest
- Heartbeat

**Files to create:**
- `src/fractio/distributed/network/client_handler.nim`

---

#### 4.3 2PC Network Messages

**Time:** 3 hours

**Implement:**
- TxnPrepare/TxnPrepareResponse
- TxnCommit/TxnCommitResponse
- TxnRollback/TxnRollbackResponse

**Files to update:**
- `src/fractio/core/two_phase_commit.nim`

---

### Phase 4 Deliverables

- [ ] Client requests working over network
- [ ] 2PC messages working
- [ ] End-to-end KV operations

---

## Phase 5: Cluster Bootstrap (Days 11-12)

### Goal
Create utilities to start and manage a cluster.

### Tasks

#### 5.1 Cluster Bootstrap (`cluster_bootstrap.nim`)

**Time:** 4 hours

**Key functions:**
- `bootstrapCluster(config): seq[Node]`
- `startNode(config): Node`
- `createRange(nodes, numReplicas): RangeDescriptor`

**Files to create:**
- `src/fractio/distributed/cluster_bootstrap.nim`

---

#### 5.2 5-Node Cluster Test

**Time:** 3 hours

**Test scenarios:**
- Start 5 nodes
- Create range with 3 replicas
- Leader election
- KV operations
- Failover

**Files to create:**
- `tests/integration/distributed/test_5node_cluster.nim`

---

### Phase 5 Deliverables

- [ ] Cluster bootstrap working
- [ ] 5-node cluster test passing
- [ ] KV transactions working

---

## Testing Strategy

### Unit Tests

| Component | Test File | Coverage Target |
|-----------|-----------|-----------------|
| Types | `test_types.nim` | 100% |
| Serialization | `test_serialization.nim` | 100% |
| TCP Transport | `test_tcp_transport.nim` | 90% |
| Connection Pool | `test_connection_pool.nim` | 90% |
| Health Checker | `test_health_checker.nim` | 90% |

### Integration Tests

| Test | Description |
|------|-------------|
| `test_2node_connection.nim` | Basic TCP connection |
| `test_3node_raft.nim` | Raft consensus over network |
| `test_5node_cluster.nim` | Full cluster operations |
| `test_partition.nim` | Network partition handling |
| `test_transactions.nim` | Distributed transactions |

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Socket errors | Comprehensive error handling, retries |
| Deadlocks | Use timeouts, avoid blocking calls |
| Memory leaks | Careful resource management, destructors |
| Thread safety | Proper locking, use atomics where possible |
| Performance | Benchmark early, optimize hot paths |

---

## Rollout Plan

1. **Development:** Implement in phases, commit after each phase
2. **Testing:** Run full test suite after each phase
3. **Review:** Code review before merging
4. **Integration:** Merge to main after all phases complete
5. **Documentation:** Update README and examples

---

## Success Metrics

- [ ] All 569+ existing tests still pass
- [ ] New network tests pass
- [ ] 5-node cluster can be started
- [ ] Leader election works
- [ ] KV operations work across nodes
- [ ] Latency < 5ms for local writes
- [ ] No memory leaks
- [ ] Clean shutdown

