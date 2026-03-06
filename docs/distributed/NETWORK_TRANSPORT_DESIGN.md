# Network Transport Design for Distributed Fractio

## Executive Summary

This document describes the network transport layer for Fractio's distributed database system. The design uses **TCP for all network communication** (Raft consensus, client traffic, and transaction coordination), providing reliable delivery and simpler implementation.

## 1. Design Goals

### 1.1 Requirements

| Requirement | Priority | Notes |
|-------------|----------|-------|
| Raft consensus communication | High | Leader election, log replication |
| Client request routing | High | DistSender to range leaseholders |
| Cross-node transactions | High | 2PC coordinator-participant |
| Time synchronization | Medium | Already implemented via UDP |
| Reliable delivery | High | No lost messages |
| Ordered streams | Medium | Important for Raft logs |
| Fault tolerance | High | Handle network partitions |
| Simple implementation | High | Single protocol to maintain |

### 1.2 Protocol Selection Rationale

| Protocol | Use Case | Rationale |
|----------|----------|-----------|
| **TCP** | Raft RPC | Reliable delivery, ordered streams, simpler error handling, proven by etcd/TiKV/CockroachDB |
| **TCP** | Client traffic | Reliable delivery, larger payloads (scans), ordered streams |
| **TCP** | 2PC | Transaction coordination requires guaranteed delivery |
| **UDP** | SharedTimer | Already implemented, low-overhead time sync (keep as-is) |

### 1.3 Why TCP for Raft?

1. **Reliability**: Built-in retransmission, ordering, and flow control
2. **Simplicity**: Single protocol to implement, test, and debug
3. **Proven**: etcd, TiKV, CockroachDB, and others use TCP for Raft
4. **Connection pooling**: Reuse connections between nodes
5. **Backpressure**: TCP handles slow consumers automatically
6. **Security**: Easier to add TLS later

### 1.4 Port Allocation

```
Node Base Port: configurable (default 9000)

Per-Node Ports:
  - Raft TCP:      base + 0  (e.g., 9000) - Raft consensus
  - Client TCP:    base + 1  (e.g., 9001) - Client requests, 2PC
  - Admin TCP:     base + 2  (e.g., 9002) - Metrics, health, admin
  - SharedTimer:   base + 3  (e.g., 9003) - UDP time sync (existing)
```

---

## 2. Architecture Overview

### 2.1 Component Diagram

```
                                    +-------------------------------------+
                                    |              External Client        |
                                    +-----------------+-------------------+
                                                      | TCP (9001)
                                                      v
+---------------------------------------------------------------------------------+
|                              Fractio Node                                       |
|                                                                                 |
|  +------------+    +------------+    +------------+    +------------+         |
|  | DistSender |    |  2PC Coord |    | Raft Node  |    | SharedTimer|         |
|  |            |    |            |    |            |    |            |         |
|  +-----+------+    +-----+------+    +-----+------+    +-----+------+         |
|        |                 |                 |                 |                 |
|        v                 v                 v                 v                 |
|  +--------------------------------------------------------------------------+  |
|  |                        Network Transport Layer                           |  |
|  |                                                                          |  |
|  |   +-----------------+           +-----------------+                     |  |
|  |   | Raft TCP Server |           | Client TCP Srv  |                     |  |
|  |   |   (Port +0)     |           |   (Port +1)     |                     |  |
|  |   |                 |           |                 |                     |  |
|  |   | - RequestVote   |           | - BatchRequest  |                     |  |
|  |   | - AppendEntries |           | - ScanRequest   |                     |  |
|  |   | - Heartbeats    |           | - 2PC Protocol  |                     |  |
|  |   | - Snapshots     |           | - Admin API     |                     |  |
|  |   +-----------------+           +-----------------+                     |  |
|  +--------------------------------------------------------------------------+  |
|                                                                                 |
+---------------------------------------------------------------------------------+
         | TCP (9000)                    | TCP (9000)               | UDP (9003)
         v                               v                          v
    Other Nodes                     Other Nodes                 Other Nodes
```

### 2.2 Message Flow

#### Raft TCP Flow (Leader Election)

```
Node 1 (Candidate)                    Node 2 (Follower)                    Node 3 (Follower)
      |                                     |                                     |
      |==== TCP Connect ==================>|                                     |
      |==== RequestVote ==================>|                                     |
      |                                     |                                     |
      |==== TCP Connect ======================================================>|
      |==== RequestVote ======================================================>|
      |                                     |                                     |
      |<=== Vote Granted ==================|                                     |
      |<=== Vote Granted ======================================================|
      |                                     |                                     |
      |  (becomes leader)                   |                                     |
      |                                     |                                     |
      |==== Heartbeat (keepalive) =========>|                                     |
      |==== Heartbeat (keepalive) ============================================>|
      |                                     |                                     |
```

#### Raft Log Replication

```
Leader                                 Follower 1                        Follower 2
  |                                         |                                 |
  |==== TCP: AppendEntries ================>|                                 |
  |       (entries: [cmd1, cmd2])           |                                 |
  |                                         |                                 |
  |==== TCP: AppendEntries =================================================>|
  |       (entries: [cmd1, cmd2])           |                                 |
  |                                         |                                 |
  |<=== TCP: AppendEntriesResp =============|                                 |
  |       (success: true, matchIdx: 5)      |                                 |
  |                                         |                                 |
  |<=== TCP: AppendEntriesResp ==============================================|
  |       (success: true, matchIdx: 5)      |                                 |
  |                                         |                                 |
  |  (majority reached, commit)             |                                 |
  |                                         |                                 |
```

#### Client Write Transaction

```
Client                                Node 1 (Leader)                Node 2 (Follower)
  |                                         |                              |
  |==== TCP: BatchRequest (Put) ===========>|                              |
  |                                         |                              |
  |                                         |== TCP: AppendEntries ========>|
  |                                         |   (replicate to followers)   |
  |                                         |                              |
  |                                         |<== TCP: AppendEntriesResp ===|
  |                                         |                              |
  |<=== TCP: BatchResponse (success) =======|                              |
  |                                         |                              |
```

---

## 3. Protocol Specifications

### 3.1 Wire Protocol (All TCP)

All messages use a simple length-prefixed binary format:

```
+--------------------------------------------------------------------+
| Frame Header (8 bytes)                                             |
+--------------------------------------------------------------------+
| [0-3]   payloadLen (uint32, big-endian)                            |
| [4-7]   checksum (uint32, CRC32 of payload)                        |
+--------------------------------------------------------------------+
| Payload (variable)                                                 |
| - Serialized message using binary encoding                         |
+--------------------------------------------------------------------+
```

### 3.2 Message Types

```nim
type
  # Raft messages (port + 0)
  RaftMessageType* = enum
    rmtRequestVote = 1
    rmtRequestVoteResponse = 2
    rmtAppendEntries = 3
    rmtAppendEntriesResponse = 4
    rmtInstallSnapshot = 5
    rmtInstallSnapshotResponse = 6
    rmtTimeoutNow = 7              # For leadership transfer
    rmtReadIndex = 8               # For linearizable reads
    rmtReadIndexResponse = 9

  # Client messages (port + 1)
  ClientMessageType* = enum
    cmtBatchRequest = 100
    cmtBatchResponse = 101
    cmtScanRequest = 102
    cmtScanResponse = 103
    cmtTxnPrepare = 104
    cmtTxnPrepareResponse = 105
    cmtTxnCommit = 106
    cmtTxnCommitResponse = 107
    cmtTxnRollback = 108
    cmtTxnRollbackResponse = 109
    cmtHeartbeat = 110
    cmtHeartbeatResponse = 111

  # Admin messages (port + 2)
  AdminMessageType* = enum
    amtMetrics = 200
    amtHealth = 201
    amtConfig = 202
    amtConfigResponse = 203
```

### 3.3 Message Payloads

#### Raft Messages

```nim
type
  MessageHeader* = object
    messageType*: uint16
    messageId*: uint64        # For request/response correlation
    sourceNodeId*: uint16
    targetNodeId*: uint16
    term*: uint64

  RequestVote* = object
    header*: MessageHeader
    candidateId*: uint16
    lastLogIndex*: uint64
    lastLogTerm*: uint64

  RequestVoteResponse* = object
    header*: MessageHeader
    voteGranted*: bool
    term*: uint64

  AppendEntries* = object
    header*: MessageHeader
    leaderId*: uint16
    prevLogIndex*: uint64
    prevLogTerm*: uint64
    commitIndex*: uint64
    entries*: seq[LogEntry]

  AppendEntriesResponse* = object
    header*: MessageHeader
    success*: bool
    term*: uint64
    matchIndex*: uint64
    rejectHint*: uint64       # For fast log matching

  InstallSnapshot* = object
    header*: MessageHeader
    leaderId*: uint16
    lastIndex*: uint64
    lastTerm*: uint64
    offset*: uint64
    done*: bool
    data*: string

  InstallSnapshotResponse* = object
    header*: MessageHeader
    term*: uint64
    offset*: uint64
```

#### Client Messages

```nim
type
  BatchRequest* = object
    header*: MessageHeader
    requestId*: uint64
    rangeId*: uint32
    timestamp*: uint64        # HLC timestamp
    requests*: seq[Request]

  Request* = object
    case kind*: RequestKind
    of rkGet:
      getKey*: string
      getTimestamp*: uint64
    of rkPut:
      putKey*: string
      putValue*: string
    of rkDelete:
      deleteKey*: string
    of rkScan:
      scanStartKey*: string
      scanEndKey*: string
      scanLimit*: uint32

  BatchResponse* = object
    header*: MessageHeader
    requestId*: uint64
    success*: bool
    error*: string
    responses*: seq[Response]

  # 2PC Messages
  TxnPrepare* = object
    header*: MessageHeader
    txnId*: uint64
    participants*: seq[uint16]    # Node IDs
    timestamp*: uint64

  TxnPrepareResponse* = object
    header*: MessageHeader
    txnId*: uint64
    vote*: bool                   # true = commit, false = abort
    error*: string

  TxnCommit* = object
    header*: MessageHeader
    txnId*: uint64
    commitTimestamp*: uint64

  TxnCommitResponse* = object
    header*: MessageHeader
    txnId*: uint64
    success*: bool
    error*: string
```

---

## 4. Implementation Components

### 4.1 TCP Transport (New File)

**File:** `src/fractio/distributed/network/tcp_transport.nim`

```nim
type
  TCPTransport* = ref object
    nodeId*: NodeID
    bindAddr*: string
    port*: int
    serverSocket*: Socket
    connections*: Table[NodeID, Connection]    # Cached connections
    pendingRequests*: Table[uint64, PendingRequest]
    running*: bool
    acceptorThread*: Thread[void]
    logger*: Logger

  Connection* = ref object
    nodeId*: NodeID
    socket*: Socket
    lastUsed*: Timestamp
    sendLock*: Lock              # For thread-safe writes
    recvLock*: Lock

  PendingRequest* = object
    sentAt*: Timestamp
    responseType*: MessageType
    callback*: proc(response: Message)

  Message* = object
    header*: MessageHeader
    payload*: string

const
  TCP_FRAME_HEADER_SIZE = 8
  TCP_MAX_MESSAGE_SIZE = 16 * 1024 * 1024    # 16MB
  TCP_CONNECT_TIMEOUT_MS = 5000
  TCP_READ_TIMEOUT_MS = 30000
  TCP_WRITE_TIMEOUT_MS = 30000
  TCP_KEEPALIVE_IDLE_MS = 60000
  TCP_KEEPALIVE_INTERVAL_MS = 10000
```

#### Key Operations

```nim
proc newTCPTransport*(config: TCPTransportConfig): TCPTransport
proc start*(t: TCPTransport)
proc stop*(t: TCPTransport)

# Connection management
proc connect*(t: TCPTransport, nodeId: NodeID): Connection
proc disconnect*(t: TCPTransport, nodeId: NodeID)
proc getConnection*(t: TCPTransport, nodeId: NodeID): Connection

# Message sending
proc sendMessage*(t: TCPTransport, 
                  nodeId: NodeID, 
                  msg: Message): Future[Message]
proc sendAsync*(t: TCPTransport,
                nodeId: NodeID,
                msg: Message,
                callback: proc(response: Message))

# Message receiving (handled by receiver threads)
proc handleFrame*(t: TCPTransport, conn: Connection, frame: string)
```

### 4.2 Connection Manager (New File)

**File:** `src/fractio/distributed/network/connection_manager.nim`

```nim
type
  ConnectionManager* = ref object
    nodeId*: NodeID
    config*: NetworkConfig
    
    # Separate transports for different purposes
    raftTransport*: TCPTransport      # Port + 0
    clientTransport*: TCPTransport    # Port + 1
    adminTransport*: TCPTransport     # Port + 2
    
    # Node registry
    nodes*: Table[NodeID, NodeDescriptor]
    healthChecker*: HealthChecker
    
  NodeDescriptor* = object
    nodeId*: NodeID
    raftAddr*: string           # "host:port"
    clientAddr*: string
    adminAddr*: string
    lastSeen*: Timestamp
    isHealthy*: bool
    roundTripTime*: int64       # microseconds

  HealthChecker* = ref object
    checkIntervalMs*: int
    failureThreshold*: int
    recoveryThreshold*: int
    consecutiveFailures*: Table[NodeID, int]
    consecutiveSuccesses*: Table[NodeID, int]
```

#### Key Operations

```nim
proc newConnectionManager*(config: NetworkConfig): ConnectionManager
proc start*(m: ConnectionManager)
proc stop*(m: ConnectionManager)

# Node management
proc addNode*(m: ConnectionManager, desc: NodeDescriptor)
proc removeNode*(m: ConnectionManager, nodeId: NodeID)
proc getNode*(m: ConnectionManager, nodeId: NodeID): Option[NodeDescriptor]
proc getHealthyNodes*(m: ConnectionManager): seq[NodeID]

# Health checking
proc checkNodeHealth*(m: ConnectionManager, nodeId: NodeID): bool
proc markNodeUnhealthy*(m: ConnectionManager, nodeId: NodeID)
proc markNodeHealthy*(m: ConnectionManager, nodeId: NodeID)
```

### 4.3 Raft RPC Handler (Update Existing)

**File:** `src/fractio/distributed/raft/rpc.nim`

```nim
type
  RaftRPCHandler* = ref object
    node*: RaftNode
    transport*: TCPTransport

# Register handlers with transport
proc registerHandlers*(h: RaftRPCHandler, t: TCPTransport) =
  t.registerHandler(rmtRequestVote, proc(msg: Message): Message =
    result = h.handleRequestVote(msg))
  
  t.registerHandler(rmtAppendEntries, proc(msg: Message): Message =
    result = h.handleAppendEntries(msg))
  
  t.registerHandler(rmtInstallSnapshot, proc(msg: Message): Message =
    result = h.handleInstallSnapshot(msg))

proc handleRequestVote*(h: RaftRPCHandler, msg: Message): Message
proc handleAppendEntries*(h: RaftRPCHandler, msg: Message): Message
proc handleInstallSnapshot*(h: RaftRPCHandler, msg: Message): Message
```

### 4.4 Message Serialization (New File)

**File:** `src/fractio/distributed/network/serialization.nim`

```nim
# Binary serialization for efficiency
# Uses big-endian for network compatibility

proc encodeMessage*(msg: Message): string =
  ## Encode message to binary format
  ## Returns: length(4) + checksum(4) + header + payload
  
proc decodeMessage*(data: string): Message =
  ## Decode binary data to message
  ## Raises SerializationError on failure

proc encodeHeader*(header: MessageHeader): string
proc decodeHeader*(data: string, offset: int): MessageHeader

# Payload encoding (reusable)
proc encodeLogEntry*(entry: LogEntry): string
proc decodeLogEntry*(data: string, offset: int): (LogEntry, int)

proc encodeBatchRequest*(req: BatchRequest): string
proc decodeBatchRequest*(data: string): BatchRequest

proc computeChecksum*(data: string): uint32 =
  ## CRC32 checksum
```

---

## 5. Connection Lifecycle

### 5.1 Connection States

```
+----------+     connect()     +-----------+
|  Idle    | ----------------> | Connecting|
+----------+                   +-----------+
     ^                               |
     |                               | success
     | stop()                        v
     |                         +-----------+
     +------------------------ | Connected |
     |        disconnect()     +-----------+
     |                               |
     |                               | error/timeout
     |                               v
     |                         +-----------+
     +------------------------ |  Failed   |
              retry()          +-----------+
```

### 5.2 Connection Pooling

```nim
type
  ConnectionPool* = object
    connections*: Table[NodeID, seq[Connection]]   # Multiple connections per node
    maxConnectionsPerNode*: int                    # Default: 4
    idleTimeoutMs*: int                            # Default: 60000
    maxIdleConnections*: int                       # Default: 2

proc getConnection*(pool: ConnectionPool, nodeId: NodeID): Connection
proc returnConnection*(pool: ConnectionPool, conn: Connection)
proc pruneIdleConnections*(pool: ConnectionPool)
```

### 5.3 Backoff and Retry

```nim
type
  BackoffPolicy* = object
    initialDelayMs*: int         # Default: 100
    maxDelayMs*: int             # Default: 5000
    multiplier*: float           # Default: 1.5
    jitter*: bool                # Default: true

proc calculateBackoff*(policy: BackoffPolicy, attempt: int): int =
  ## Exponential backoff with optional jitter
  let delay = min(
    policy.initialDelayMs * pow(policy.multiplier, attempt.float),
    policy.maxDelayMs.float
  ).int
  
  if policy.jitter:
    result = delay + rand(delay div 2)
  else:
    result = delay
```

---

## 6. Thread Model

### 6.1 Thread Architecture

```
+-----------------------------------------------------------------+
| Main Thread                                                      |
| - Raft state machine                                             |
| - Transaction coordinator                                        |
| - Application logic                                              |
+-----------------------------------------------------------------+
         |                    |                    |
         v                    v                    v
+----------------+   +----------------+   +----------------+
| Raft Acceptor  |   | Client Acceptor|   | Admin Acceptor |
| Thread         |   | Thread         |   | Thread         |
| (port + 0)     |   | (port + 1)     |   | (port + 2)     |
+----------------+   +----------------+   +----------------+
         |                    |                    |
         v                    v                    v
+----------------+   +----------------+   +----------------+
| Raft Handler   |   | Client Handler |   | Admin Handler  |
| Thread Pool    |   | Thread Pool    |   | Thread Pool    |
| (N workers)    |   | (N workers)    |   | (N workers)    |
+----------------+   +----------------+   +----------------+
```

### 6.2 Thread Pool Configuration

```nim
type
  ThreadPoolConfig* = object
    numWorkers*: int              # Default: 4
    queueSize*: int               # Default: 1000
    threadName*: string

  ThreadPool* = object
    config*: ThreadPoolConfig
    queue*: Channel[Task]
    workers*: seq[Thread[void]]
    running*: bool

proc submit*(pool: ThreadPool, task: Task)
proc start*(pool: ThreadPool)
proc stop*(pool: ThreadPool)
```

---

## 7. Error Handling

### 7.1 Error Types

```nim
type
  NetworkError* = object of CatchableError
    code*: NetworkErrorCode

  NetworkErrorCode* = enum
    neConnectionRefused
    neConnectionReset
    neTimeout
    neInvalidMessage
    neChecksumMismatch
    neUnknownNode
    neNodeUnhealthy
    neBufferOverflow
    neSocketError

  SerializationError* = object of CatchableError
    code*: SerializationErrorCode

  SerializationErrorCode* = enum
    seInvalidFormat
    seTruncatedMessage
    seUnknownMessageType
    seChecksumMismatch
```

### 7.2 Error Recovery

| Error | Recovery Action |
|-------|-----------------|
| Connection refused | Backoff + retry, mark unhealthy after threshold |
| Connection reset | Reconnect immediately |
| Timeout | Cancel pending requests, reconnect |
| Invalid message | Close connection, reconnect |
| Checksum mismatch | Request retransmission |
| Node unhealthy | Route to other replicas, background health check |

---

## 8. Configuration

### 8.1 Network Configuration

**File:** `src/fractio/distributed/network/config.nim`

```nim
type
  NetworkConfig* = object
    nodeId*: NodeID
    basePort*: int
    bindAddress*: string          # Default: "0.0.0.0"
    
    # TCP settings
    tcpNoDelay*: bool             # Default: true
    tcpKeepAlive*: bool           # Default: true
    tcpSendBufferSize*: int       # Default: 4MB
    tcpRecvBufferSize*: int       # Default: 4MB
    tcpConnectTimeoutMs*: int     # Default: 5000
    tcpReadTimeoutMs*: int        # Default: 30000
    tcpWriteTimeoutMs*: int       # Default: 30000
    tcpMaxMessageSize*: int       # Default: 16MB
    
    # Connection pooling
    maxConnectionsPerNode*: int   # Default: 4
    idleTimeoutMs*: int           # Default: 60000
    
    # Health checking
    healthCheckIntervalMs*: int   # Default: 1000
    failureThreshold*: int        # Default: 3
    recoveryThreshold*: int       # Default: 2
    
    # Thread pools
    raftWorkers*: int             # Default: 4
    clientWorkers*: int           # Default: 8
    adminWorkers*: int            # Default: 2
    
    # Peers
    peers*: seq[PeerConfig]

  PeerConfig* = object
    nodeId*: NodeID
    host*: string
    basePort*: int

const
  DEFAULT_BASE_PORT* = 9000
  DEFAULT_CONNECT_TIMEOUT_MS* = 5000
  DEFAULT_READ_TIMEOUT_MS* = 30000
  DEFAULT_WRITE_TIMEOUT_MS* = 30000
  DEFAULT_MAX_MESSAGE_SIZE* = 16 * 1024 * 1024
```

---

## 9. Performance Targets

| Operation | Local | Same DC | Cross DC |
|-----------|-------|---------|----------|
| Raft vote | < 1ms | 1-5ms | 50-100ms |
| Raft heartbeat | < 1ms | 1-5ms | 50-100ms |
| AppendEntries (small) | < 1ms | 1-5ms | 50-100ms |
| AppendEntries (large) | 5-10ms | 10-50ms | 200-500ms |
| Client write (3 replicas) | 2-5ms | 5-20ms | 100-200ms |
| Client read (leaseholder) | < 1ms | 1-5ms | 50-100ms |
| Scan (1000 rows) | 5-10ms | 10-30ms | 100-200ms |

---

## 10. Testing Strategy

### 10.1 Unit Tests

| Test | Description |
|------|-------------|
| Message serialization | Verify binary encoding/decoding |
| Checksum validation | Test CRC32 error detection |
| Header parsing | Test message header extraction |
| Connection lifecycle | Test connect/disconnect/reconnect |
| Timeout handling | Test retry logic |
| Thread pool | Test task submission and execution |

### 10.2 Integration Tests

| Test | Description |
|------|-------------|
| 2-node connection | Basic TCP connection between nodes |
| 3-node Raft election | Leader election over TCP |
| 3-node log replication | AppendEntries over TCP |
| 5-node cluster | Full cluster operations |
| Network partition | Partition tolerance and recovery |
| Client transaction | End-to-end KV operations |
| 2PC commit | Distributed transaction commit |

### 10.3 Stress Tests

| Test | Description |
|------|-------------|
| Connection churn | Rapid connect/disconnect cycles |
| Message flood | High throughput message sending |
| Mixed workload | Read/write/scan combination |
| Long running | 24-hour stability test |

---

## 11. Security Considerations

### 11.1 Current (MVP)

- No authentication/authorization
- No encryption
- Trust all nodes in cluster
- Bind to localhost for development

### 11.2 Future Enhancements

| Feature | Priority | Description |
|---------|----------|-------------|
| TLS encryption | High | Encrypt all TCP traffic |
| Node authentication | High | Certificate-based node identity |
| Client authentication | Medium | Username/password or token auth |
| IP allowlisting | Medium | Restrict which IPs can connect |
| Rate limiting | Low | Prevent DoS attacks |

---

## 12. File Structure

```
src/fractio/distributed/
├── network/                          # NEW DIRECTORY
│   ├── config.nim                    # Network configuration types
│   ├── types.nim                     # Network message types
│   ├── serialization.nim             # Wire format encoding/decoding
│   ├── tcp_transport.nim             # TCP server/client implementation
│   ├── connection_pool.nim           # Connection pooling
│   ├── connection_manager.nim        # Multi-transport coordination
│   ├── health_checker.nim            # Node health monitoring
│   └── thread_pool.nim               # Worker thread pools
│
├── raft/
│   ├── types.nim                     # Existing Raft types
│   ├── node.nim                      # UPDATE: Add transport integration
│   ├── rpc.nim                       # UPDATE: Implement actual RPC handlers
│   └── cluster.nim                   # UPDATE: Use network transport
│
├── sender.nim                        # UPDATE: Use TCP transport
├── two_phase_commit.nim              # UPDATE: Network coordination
└── cluster_bootstrap.nim             # NEW: Cluster startup utilities
```

---

## 13. Implementation Plan

### Phase 1: Core Network Layer (2-3 days)

**Priority: Critical**

1. Create `network/types.nim` with message types
2. Create `network/serialization.nim` for binary encoding
3. Create `network/tcp_transport.nim` with basic TCP server/client
4. Add unit tests for serialization

**Deliverable:** Messages can be encoded, sent over TCP, and decoded

### Phase 2: Connection Management (1-2 days)

**Priority: High**

1. Create `network/connection_pool.nim` for connection reuse
2. Create `network/connection_manager.nim` for multi-transport
3. Create `network/health_checker.nim` for node monitoring
4. Add integration tests for connection lifecycle

**Deliverable:** Stable connections between nodes with health checking

### Phase 3: Raft Integration (2-3 days)

**Priority: Critical**

1. Update `raft/rpc.nim` to implement actual RPC handlers
2. Update `raft/node.nim` to use TCP transport
3. Update `raft/cluster.nim` for network-based operations
4. Add 3-node Raft election test

**Deliverable:** Working Raft consensus over TCP

### Phase 4: Client Transport (1-2 days)

**Priority: High**

1. Update `sender.nim` to use TCP transport
2. Implement BatchRequest/BatchResponse handling
3. Implement 2PC network messages
4. Add client transaction tests

**Deliverable:** Client requests can be sent to remote nodes

### Phase 5: Cluster Bootstrap (1-2 days)

**Priority: High**

1. Create `cluster_bootstrap.nim` for cluster startup
2. Implement node discovery and registration
3. Implement range creation with replicas
4. Add 5-node cluster test

**Deliverable:** Can start a 5-node cluster programmatically

### Phase 6: Integration Testing (2-3 days)

**Priority: High**

1. Create comprehensive integration tests
2. Test network partitions and recovery
3. Test failure scenarios
4. Performance benchmarking

**Deliverable:** Verified working 5-node cluster with KV transactions

---

## 14. Dependencies

### 14.1 Standard Library

```nim
import std/[net, nativesockets, asyncnet, asyncdispatch]
import std/[strutils, tables, deques, locks, threads, channels]
import std/[times, hashes, random, math]
import std/[endians, cpuinfo]
```

### 14.2 Internal Dependencies

```nim
import fractio/core/types
import fractio/core/errors
import fractio/distributed/raft/types
import fractio/distributed/range/types
import fractio/utils/logging
```

---

## 15. Success Criteria

A successful implementation will:

1. ✅ Start a 5-node cluster on localhost
2. ✅ Elect a leader via Raft over TCP
3. ✅ Replicate writes to 3 replicas
4. ✅ Handle network partitions gracefully
5. ✅ Support client KV operations (get/put/delete/scan)
6. ✅ Execute distributed transactions with 2PC
7. ✅ Pass all integration tests
8. ✅ Achieve < 5ms latency for local writes
