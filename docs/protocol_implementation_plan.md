# Fractio Protocol Implementation Plan

**Version:** 1.0
**Created:** 2025-03-07
**Status:** Planning

---

## 1. Executive Summary

This document outlines the implementation plan for Fractio's client/server TCP protocol.
The protocol supports key-value operations and distributed transactions, with a design that
easily extends to SQL, Graph, and Vector operations in the future.

### Key Deliverables

1. Binary wire protocol with framing and versioning
2. Connection lifecycle management (handshake, keepalive, close)
3. Key-Value API (Get, Put, Delete, Batch, Scan)
4. Transaction support (Begin, Commit, Rollback)
5. Error handling with structured error codes
6. Client library for Nim

### Timeline Estimate

| Phase  | Duration  | Dependencies |
|--------|-----------|--------------|
| Phase 1| 1-2 weeks | None         |
| Phase 2| 2-3 weeks | Phase 1      |
| Phase 3| 1-2 weeks | Phase 2      |
| Phase 4| 1 week    | Phase 1-3    |
| Phase 5| Ongoing   | Phase 4      |

---
## 2. Architecture

### 2.1 Directory Structure

```
src/fractio/protocol/
  types.nim           # Protocol constants, message types, ProtocolError, TxnState
  frame.nim           # Frame encoding/decoding with CRC16
  handshake.nim       # Connection handshake logic
  codec.nim           # Low-level binary encoding/decoding primitives
  router.nim          # Shard routing table, RaftMultiGroupKV, NOT_LEADER handling
  messages/
    core.nim          # Ping, Echo, Close messages
    kv.nim            # Get, Put, Delete, Batch, Scan; cross-shard scatter-gather
    txn.nim           # Transaction messages; 2PC coordinator for cross-shard txns
    admin.nim         # Server info, metrics, health
  server.nim          # Protocol server, ServerConfig, ClientConnection
  client.nim          # Client library, ClientConfig

tests/protocol/
  test_frame.nim
  test_codec.nim
  test_handshake.nim
  test_routing.nim
  test_kv.nim
  test_batch.nim
  test_batch_crossshard.nim
  test_scan.nim
  test_txn.nim
  test_txn_crossshard.nim
  test_txn_conflict.nim
  test_txn_timeout.nim
  test_txn_coordinator_failure.nim
  test_auth_password.nim
  test_auth_token.nim
  test_auth_tls.nim
  test_compression.nim
  test_admin.nim
  test_client_server.nim
  test_concurrent.nim
  test_stress.nim
```

### 2.2 Module Dependencies

```
                    +------------+
                    |   types    |
                    +------------+
                          |
         +----------------+----------------+
         |                |                |
    +---------+     +---------+     +---------+
    |  codec  |     |  frame  |     | handshake|
    +---------+     +---------+     +---------+
         |                |                |
         +----------------+----------------+
                          |
              +-----------+-----------+
              |                       |
         +---------+           +---------+
         | server  |           | client  |
         +---------+           +---------+
              |                       |
         +-----+-----+         +-----+-----+
         | messages  |         | messages  |
         +-----------+         +-----------+
```

---
## 3. Phase 1: Core Protocol

### 3.1 Objectives

- Define protocol types and constants
- Implement frame encoding/decoding with CRC16 checksums
- Implement connection handshake (greeting, client handshake, response)
- Implement core messages (Ping, Echo, Close)
- Implement error handling

### 3.2 Tasks

#### 3.2.1 Protocol Types (types.nim)

```nim
type
  # Protocol version
  ProtocolVersion* = distinct uint16

  # Message type categories
  MessageType* = enum
    # Core/Control (0x0000-0x00FF)
    mtPing = 0x0001
    mtEcho = 0x0002
    mtClose = 0x0003
    mtCancelStream = 0x0004

    # KV Operations (0x0100-0x01FF)
    mtGet = 0x0100
    mtPut = 0x0101
    mtDelete = 0x0102
    mtBatch = 0x0103
    mtScan = 0x0104

    # Transactions (0x0200-0x02FF)
    mtBeginTxn = 0x0200
    mtCommitTxn = 0x0201
    mtRollbackTxn = 0x0202
    mtTxnStatus = 0x0203

    # Future: SQL (0x0300-0x03FF)
    # Future: Graph (0x0400-0x04FF)
    # Future: Vector (0x0500-0x05FF)

    # Admin (0x0700-0x07FF)
    mtServerInfo = 0x0700
    mtMetrics = 0x0701
    mtHealth = 0x0702

  # Error kinds used throughout the protocol layer
  ProtocolErrorKind* = enum
    peInvalidFrame
    peChecksumMismatch
    peUnknownMessageType
    peVersionMismatch
    peAuthFailed
    peNotLeader
    peTimeout
    peInternal

  ProtocolError* = object
    kind*: ProtocolErrorKind
    msg*: string
    leaderAddr*: string   # populated for peNotLeader

  # Transaction state machine
  TxnState* = enum
    tsActive
    tsCommitted
    tsAborted
    tsTimedOut

  CommitResult* = object
    committed*: bool
    commitTimestamp*: uint64
    conflictKey*: string   # populated if committed=false due to conflict
```

#### 3.2.2 Frame Structure (frame.nim)

```nim
const
  FRAME_HEADER_SIZE = 12
  MAX_FRAME_SIZE = 16 * 1024 * 1024  # 16 MB

type
  FrameHeader* = object
    payloadLen*: uint32
    requestId*: uint32
    flags*: uint16
    checksum*: uint16

  Frame* = object
    header*: FrameHeader
    payload*: string

  FrameFlags* = enum
    ffCompressed = 0
    ffRequiresAck = 1
    ffIsResponse = 2
    ffIsError = 3
    ffEndOfStream = 4

proc encodeFrame*(payload: string, requestId: uint32, flags: uint16 = 0): string
proc decodeFrame*(data: string): Result[Frame, ProtocolError]
proc computeCRC16*(data: string): uint16
```

#### 3.2.3 Handshake (handshake.nim)

```nim
type
  ServerGreeting* = object
    magic*: string           # "FRC1"
    version*: ProtocolVersion
    features*: uint32
    authMethods*: seq[uint8]
    serverId*: uint16
    clusterId*: uint64

  ClientHandshake* = object
    version*: ProtocolVersion
    features*: uint32
    authType*: uint8
    authData*: string
    clientId*: string

  HandshakeResponse* = object
    status*: uint8
    features*: uint32
    serverName*: string
    errorMessage*: string

proc encodeGreeting*(g: ServerGreeting): string
proc decodeGreeting*(data: string): Result[ServerGreeting, ProtocolError]
proc encodeClientHandshake*(h: ClientHandshake): string
proc decodeClientHandshake*(data: string): Result[ClientHandshake, ProtocolError]
```

#### 3.2.4 Server Interface (server.nim)

```nim
type
  ServerConfig* = object
    host*: string
    port*: int
    maxConnections*: int          # default: 1024
    maxFrameBytes*: uint32        # default: 16 MB
    maxKeyBytes*: uint32          # default: 4 KB
    maxValueBytes*: uint32        # default: 64 MB
    idleTimeoutSecs*: int         # default: 30
    keepaliveIntervalSecs*: int   # default: 10
    tlsEnabled*: bool
    tlsCertFile*: string
    tlsKeyFile*: string
    tlsCaFile*: string
    authMethod*: AuthMethod

  MessageHandler* = proc(conn: ClientConnection, payload: string): Future[void] {.gcsafe, async.}

  ProtocolServer* = ref object
    config*: ServerConfig
    socket*: Socket
    running*: Atomic[bool]
    clients*: Table[uint32, ClientConnection]
    handlers*: Table[MessageType, MessageHandler]
    nextClientId*: Atomic[uint32]

  ClientConnection* = ref object
    id*: uint32
    socket*: Socket
    address*: string
    negotiatedFeatures*: uint32
    createdAt*: int64
    lastActivity*: int64

proc newProtocolServer*(config: ServerConfig): ProtocolServer
proc start*(server: ProtocolServer): Future[bool]
proc stop*(server: ProtocolServer): Future[void]
proc registerHandler*(server: ProtocolServer, msgType: MessageType, handler: MessageHandler)
proc broadcast*(server: ProtocolServer, msgType: MessageType, payload: string): Future[void]
```

#### 3.2.6 Binary Codec (codec.nim)

All on-wire encoding uses a simple **length-prefixed big-endian binary format** — no
external serialization library. Fields are packed in the order defined in the protocol spec.

```nim
# Primitive write helpers
proc writeUint8*(buf: var string, v: uint8)
proc writeUint16BE*(buf: var string, v: uint16)
proc writeUint32BE*(buf: var string, v: uint32)
proc writeUint64BE*(buf: var string, v: uint64)
proc writeBytes*(buf: var string, data: string)   # prefixed with uint32 length

# Primitive read helpers
proc readUint8*(buf: string, pos: var int): uint8
proc readUint16BE*(buf: string, pos: var int): uint16
proc readUint32BE*(buf: string, pos: var int): uint32
proc readUint64BE*(buf: string, pos: var int): uint64
proc readBytes*(buf: string, pos: var int): Result[string, ProtocolError]

# Validates pos does not exceed buf length; returns peInvalidFrame on overflow
proc checkBounds*(buf: string, pos: int, need: int): Result[void, ProtocolError]
```

#### 3.2.5 Client Interface (client.nim)

```nim
type
  ProtocolClient* = ref object
    config*: ClientConfig
    socket*: Socket
    connected*: Atomic[bool]
    nextRequestId*: Atomic[uint32]
    pendingRequests*: Table[uint32, Future[Frame]]

  ClientConfig* = object
    host*: string
    port*: int
    timeout*: int
    clientId*: string

proc newProtocolClient*(config: ClientConfig): ProtocolClient
proc connect*(client: ProtocolClient): Future[bool]
proc disconnect*(client: ProtocolClient): Future[void]
proc send*(client: ProtocolClient, msgType: MessageType, payload: string): Future[Frame]
proc ping*(client: ProtocolClient): Future[int64]  # Returns timestamp
```

### 3.3 Tests

- `test_frame.nim`: CRC16 computation, frame encoding/decoding
- `test_handshake.nim`: Greeting, client handshake, response encoding
- `test_core.nim`: Ping, Echo, Close message handling

### 3.4 Success Criteria

- [ ] Frame encoding/decoding with checksum verification
- [ ] Server accepts connections and performs handshake
- [ ] Client connects and completes handshake
- [ ] Ping request returns server timestamp
- [ ] Echo request returns same data
- [ ] Close gracefully terminates connection

---
## 4. Phase 2: KV Operations

### 4.1 Objectives

- Implement Get, Put, Delete operations
- Implement Batch operations
- Implement Scan with streaming responses
- Integrate with Raft multigroup KV for routing to correct shard leader

### 4.2 Tasks

#### 4.2.1 KV Message Types (messages/kv.nim)

```nim
type
  GetRequest* = object
    flags*: uint8
    txnId*: uint64
    readTimestamp*: uint64
    key*: string

  GetResponse* = object
    flags*: uint8
    timestamp*: uint64
    version*: uint64
    value*: string
    found*: bool

  PutRequest* = object
    flags*: uint8
    txnId*: uint64
    expectedVersion*: uint64
    key*: string
    value*: string

  PutResponse* = object
    status*: uint8
    timestamp*: uint64
    version*: uint64
    previousValue*: Option[string]

  DeleteRequest* = object
    flags*: uint8
    txnId*: uint64
    key*: string

  DeleteResponse* = object
    status*: uint8
    previousValue*: Option[string]

  BatchRequest* = object
    flags*: uint8
    txnId*: uint64
    operations*: seq[BatchOp]

  BatchOp* = object
    kind*: uint8  # 0=Get, 1=Put, 2=Delete
    data*: string

  ScanRequest* = object
    flags*: uint8
    txnId*: uint64
    readTimestamp*: uint64
    startKey*: Option[string]
    endKey*: Option[string]
    limit*: uint32
```

#### 4.2.2 Shard Router (router.nim)

The router maps keys to shard leaders and handles `NOT_LEADER` redirects.

```nim
type
  ShardRange* = object
    startKey*: string       # inclusive
    endKey*: string         # exclusive; empty = end of keyspace
    shardId*: uint32
    raftGroupId*: uint32

  LeaderInfo* = object
    nodeId*: uint32
    addr*: string           # "host:port"
    lastSeen*: int64        # monotonic timestamp

  RouterTable* = ref object
    shards*: seq[ShardRange]
    leaders*: Table[uint32, LeaderInfo]   # shardId -> leader
    mu*: Mutex

  RaftMultiGroupKV* = ref object
    router*: RouterTable
    localNodeId*: uint32
    raftGroups*: Table[uint32, RaftGroup]  # raftGroupId -> local group handle

# Returns the LeaderInfo for the shard owning `key`.
# Raises ProtocolError(peNotLeader) if this node is not the leader and redirect mode is on.
proc routeKey*(kv: RaftMultiGroupKV, key: string): Result[LeaderInfo, ProtocolError]

# Update routing table from gossip / Raft config change notifications
proc updateRoute*(kv: RaftMultiGroupKV, shard: ShardRange, leader: LeaderInfo)
```

#### 4.2.3 KV Handler Implementation

```nim
proc handleGet*(raftKV: RaftMultiGroupKV, req: GetRequest): Future[GetResponse]
proc handlePut*(raftKV: RaftMultiGroupKV, req: PutRequest): Future[PutResponse]
proc handleDelete*(raftKV: RaftMultiGroupKV, req: DeleteRequest): Future[DeleteResponse]
proc handleBatch*(raftKV: RaftMultiGroupKV, req: BatchRequest): Future[BatchResponse]
proc handleScan*(raftKV: RaftMultiGroupKV, conn: ClientConnection, req: ScanRequest): Future[void]
```

#### 4.2.4 Cross-Shard Batch

A `BatchRequest` whose operations span multiple shard ranges is handled via scatter-gather:

1. Group operations by shard using `routeKey`
2. Fan out sub-`BatchRequest`s to each shard leader (in parallel)
3. Collect results and merge in original operation order
4. If any sub-batch fails with `NOT_LEADER`, update routing table and retry that sub-batch

```nim
proc handleCrossShardBatch*(raftKV: RaftMultiGroupKV, req: BatchRequest): Future[BatchResponse]
```

### 4.3 Integration Points

- `router.nim` wraps existing `distributed/raft/` Raft group handles
- MVCC engine provides transaction-aware reads via read timestamps
- Consistent hashing (SHA-1) maps keys to `ShardRange` entries in `RouterTable`
- Leader routing table is updated via `ShardRoute` (0x0604) replication messages

### 4.4 Tests

- `test_kv.nim`: Get/Put/Delete operations
- `test_batch.nim`: Single-shard batch operations
- `test_batch_crossshard.nim`: Multi-shard batch scatter-gather
- `test_scan.nim`: Scan with streaming responses
- `test_routing.nim`: Key routing, `NOT_LEADER` redirect, routing table update

### 4.5 Success Criteria

- [ ] Get returns value or not-found
- [ ] Put stores value and returns timestamp/version
- [ ] Delete removes key
- [ ] Single-shard batch executes atomically
- [ ] Cross-shard batch scatter-gathers correctly
- [ ] Scan streams key-value pairs across multiple frames
- [ ] Requests route to correct Raft leader; `NOT_LEADER` triggers re-route

---
## 5. Phase 3: Transactions

### 5.1 Objectives

- Implement Begin/Commit/Rollback transactions
- Implement transaction status queries
- Integrate with MVCC engine and Raft leader for affected shard ranges
- Implement conflict detection

### 5.2 Tasks

#### 5.2.1 Transaction Message Types (messages/txn.nim)

```nim
type
  BeginTxnRequest* = object
    flags*: uint8
    timeout*: uint32

  BeginTxnResponse* = object
    txnId*: uint64
    readTimestamp*: uint64

  CommitTxnRequest* = object
    txnId*: uint64

  CommitTxnResponse* = object
    status*: uint8
    commitTimestamp*: uint64

  RollbackTxnRequest* = object
    txnId*: uint64

  RollbackTxnResponse* = object
    status*: uint8

  TxnStatusRequest* = object
    txnId*: uint64

  TxnStatusResponse* = object
    status*: uint8
    commitTimestamp*: uint64

const
  TxnActive*    = 0x00
  TxnCommitted* = 0x01
  TxnAborted*   = 0x02
  TxnNotFound*  = 0x03
```

#### 5.2.2 Transaction Manager Integration

```nim
type
  TransactionManager* = ref object
    activeTxns*: Table[uint64, Transaction]
    timestampProvider*: TimestampProvider
    mu*: Mutex

  Transaction* = object
    id*: uint64
    readTimestamp*: uint64
    writeSet*: HashSet[string]
    readSet*: HashSet[string]
    involvedShards*: HashSet[uint32]  # shard IDs touched by this transaction
    state*: TxnState
    createdAt*: int64
    timeout*: int64

proc beginTransaction*(mgr: TransactionManager, timeout: int32): Future[Transaction]
proc commitTransaction*(mgr: TransactionManager, txnId: uint64): Future[CommitResult]
proc rollbackTransaction*(mgr: TransactionManager, txnId: uint64): Future[void]
proc getTransactionStatus*(mgr: TransactionManager, txnId: uint64): TxnStatus
```

#### 5.2.3 Cross-Shard Transactions (2PC)

When a transaction touches keys in more than one shard range, the node that received
`Begin Transaction` acts as the **2PC coordinator**:

**Phase 1 — Prepare:**
1. Coordinator sends `Prepare(txnId, writeSet)` to each involved shard leader
2. Each shard leader validates the write set against its MVCC read/write conflict detection
3. Each shard leader responds `PREPARED` or `ABORT`

**Phase 2 — Commit or Abort:**
- If all shards respond `PREPARED`: coordinator sends `Commit(txnId, commitTimestamp)` to all
- If any shard responds `ABORT`: coordinator sends `Abort(txnId)` to all prepared shards

The coordinator writes a durable commit record to its own Raft log before Phase 2 to ensure
recovery in case of coordinator failure.

```nim
proc coordinateCrossShardCommit*(
  mgr: TransactionManager,
  txn: Transaction,
  raftKV: RaftMultiGroupKV
): Future[CommitResult]
```

### 5.3 Integration Points

- Single-shard transactions: committed directly via the owning shard's Raft leader
- Cross-shard transactions: 2PC coordinator role handled by `TransactionManager`
- MVCC engine from `fractio/storage/mvcc.nim` for snapshot reads and conflict detection
- Timestamp provider from `fractio/distributed/sharedtimer.nim` for commit timestamps
- Conflict detection based on read/write sets within each shard

### 5.4 Tests

- `test_txn.nim`: Begin/Commit/Rollback on a single shard
- `test_txn_crossshard.nim`: 2PC commit across multiple shards
- `test_txn_conflict.nim`: Concurrent transaction conflicts
- `test_txn_timeout.nim`: Transaction timeout handling
- `test_txn_coordinator_failure.nim`: Recovery when 2PC coordinator crashes

### 5.5 Success Criteria

- [ ] Begin creates new transaction with read timestamp
- [ ] Single-shard commit validates and commits via Raft leader
- [ ] Cross-shard commit uses 2PC and is durable across coordinator failure
- [ ] Rollback aborts transaction and notifies all involved shards
- [ ] Conflict detection prevents lost updates
- [ ] Transaction timeout aborts long-running transactions

---
## 6. Phase 4: Advanced Features

### 6.1 Objectives

- Implement authentication (password, token)
- Implement Snappy compression
- Implement TLS support
- Implement admin messages (server info, metrics, health)

### 6.2 Tasks

#### 6.2.1 Authentication

```nim
type
  AuthMethod* = enum
    amNone     = 0x00
    amPassword = 0x01
    amToken    = 0x02
    amTLS      = 0x03

  Authenticator* = ref object
    method*: AuthMethod
    users*: Table[string, string]  # username -> password hash

proc authenticate*(auth: Authenticator, authData: string): bool
```

#### 6.2.2 Compression

```nim
proc compressPayload*(payload: string): string
proc decompressPayload*(compressed: string): string
```

#### 6.2.3 Admin Messages

```nim
proc handleServerInfo*(server: ProtocolServer): ServerInfoResponse
proc handleMetrics*(server: ProtocolServer, flags: uint8): MetricsResponse
proc handleHealth*(server: ProtocolServer): HealthResponse
```

### 6.3 Tests

- `test_auth_password.nim`: Password authentication accept/reject
- `test_auth_token.nim`: Token authentication accept/reject
- `test_auth_tls.nim`: TLS client certificate validation
- `test_compression.nim`: Payload compression/decompression round-trip
- `test_admin.nim`: Server info, metrics, health check responses

### 6.4 Success Criteria

- [ ] Password authentication works
- [ ] Token authentication works
- [ ] TLS client certificate authentication works
- [ ] Large messages are compressed when flag is set
- [ ] Server info returns version, uptime, role
- [ ] Health check returns cluster status

---
## 7. Phase 5: Future Extensions

### 7.1 SQL Support (Planned)

- Prepare statement (0x0300)
- Execute statement (0x0301)
- Direct query (0x0302)
- Result set streaming

### 7.2 Graph Support (Planned)

- Traversal queries (0x0400)
- Pattern matching (0x0401)
- Shortest path (0x0402)

### 7.3 Vector Support (Planned)

- Vector insert (0x0500)
- Vector search (0x0501)
- ANN index integration

---
## 8. Testing Strategy

### 8.1 Unit Tests

Each module has corresponding unit tests:

```
tests/protocol/
  test_frame.nim                  # Frame encoding/decoding, CRC16
  test_codec.nim                  # Binary encoding primitives, bounds checking
  test_handshake.nim              # Server greeting, client handshake, response
  test_routing.nim                # Key-to-shard mapping, NOT_LEADER handling
  test_kv.nim                     # Get/Put/Delete single-shard
  test_batch.nim                  # Single-shard batch atomicity
  test_batch_crossshard.nim       # Multi-shard scatter-gather batch
  test_scan.nim                   # Scan streaming, cancel stream
  test_txn.nim                    # Begin/Commit/Rollback single-shard
  test_txn_crossshard.nim         # 2PC across multiple shards
  test_txn_conflict.nim           # Concurrent conflict detection
  test_txn_timeout.nim            # Transaction timeout abort
  test_txn_coordinator_failure.nim# 2PC recovery after coordinator crash
  test_auth_password.nim          # Password auth accept/reject
  test_auth_token.nim             # Token auth accept/reject
  test_auth_tls.nim               # TLS client cert validation
  test_compression.nim            # Snappy compress/decompress round-trip
  test_admin.nim                  # Server info, metrics, health check
```

### 8.2 Integration Tests

```
tests/protocol/
  test_client_server.nim  # Full client/server handshake + KV + txn
  test_concurrent.nim     # Concurrent operations, pipelining
  test_stress.nim         # Stress testing, connection limits
```

### 8.3 Concurrency Tests

- Multiple clients connecting simultaneously
- Concurrent read/write operations across shard ranges
- Transaction conflicts under load
- Leader failover during active requests

---
## 9. Client Library API

### 9.1 High-Level API (Nim)

```nim
type
  FractioClient* = ref object
    protocol*: ProtocolClient
    config*: ClientConfig

  Transaction* = ref object
    id*: uint64
    client*: FractioClient
    readTimestamp*: uint64

# Connection management
proc newFractioClient*(host: string, port: int): FractioClient
proc connect*(client: FractioClient): Future[void]
proc disconnect*(client: FractioClient): Future[void]
proc isConnected*(client: FractioClient): bool

# KV operations (routed to Raft leader for key range)
proc get*(client: FractioClient, key: string): Future[Option[string]]
proc put*(client: FractioClient, key, value: string): Future[void]
proc delete*(client: FractioClient, key: string): Future[bool]
proc scan*(client: FractioClient, startKey, endKey: string, limit: int): Future[seq[tuple[key, value: string]]]

# Batch operations
proc batch*(client: FractioClient, ops: seq[BatchOp]): Future[seq[BatchResult]]

# Transactions
proc beginTransaction*(client: FractioClient): Future[Transaction]
proc commit*(txn: Transaction): Future[void]
proc rollback*(txn: Transaction): Future[void]

# Transactional KV
proc get*(txn: Transaction, key: string): Future[Option[string]]
proc put*(txn: Transaction, key, value: string): Future[void]
proc delete*(txn: Transaction, key: string): Future[bool]
```

### 9.2 Usage Example

```nim
# Simple usage
let client = newFractioClient("localhost", 9000)
await client.connect()

# KV operations
await client.put("user:1", "Alice")
let value = await client.get("user:1")
echo value.get()  # "Alice"

# Transaction
let txn = await client.beginTransaction()
try:
  let balance = (await txn.get("balance")).get().parseInt
  await txn.put("balance", $(balance - 100))
  await txn.put("transferred", "100")
  await txn.commit()
except CatchableError:
  await txn.rollback()

await client.disconnect()
```

---
## 10. Risks and Mitigations

| Risk                           | Impact | Mitigation                                                    |
|--------------------------------|--------|---------------------------------------------------------------|
| Breaking existing network code | High   | New `protocol/` directory coexists with old `network/`        |
| Thread safety issues           | High   | Mutex on `RouterTable`, `TransactionManager`; `--threads:on` tests |
| Memory leaks in async code     | Medium | ORC GC; test with valgrind                                    |
| Protocol version conflicts     | Low    | Version negotiation at handshake; maintain backward compat    |
| Performance regression         | Medium | Benchmark each phase; optimize hot paths                      |
| Raft leader failover mid-request | High | Detect `NOT_LEADER`; retry with exponential backoff; update routing table |
| 2PC coordinator crash          | High   | Coordinator writes durable commit record before Phase 2; recovery on restart |
| Cross-shard batch partial failure | Medium | `Continue on error` flag; per-operation status in response  |
| Routing table staleness        | Medium | `ShardRoute` gossip (0x0604); TTL on leader cache entries     |

---
## 11. Migration Plan

### 11.1 From Current Network Layer

The current `distributed/network/` implementation can coexist with the new `protocol/` implementation during migration:

1. **Phase 1**: Implement new protocol in `protocol/` directory
2. **Phase 2**: Add compatibility layer for Raft messages
3. **Phase 3**: Migrate cluster bootstrap to use new protocol
4. **Phase 4**: Deprecate old network layer

### 11.2 Backward Compatibility

- Support both old and new clients during transition
- Protocol version negotiation allows gradual upgrade
- Admin messages can report protocol version

---

## 12. References

- Protocol Design: `docs/protocol_design.md`
- Raft Implementation: `src/fractio/distributed/raft/`
- MVCC Engine: `src/fractio/storage/mvcc.nim`
- Timestamp Provider: `src/fractio/distributed/sharedtimer.nim`

