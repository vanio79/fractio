# NuRaft + WiscKey Integration Design Document

## 1. Executive Summary

This document describes the design for integrating NuRaft (a C++ Raft consensus library) with WiscKey (LSM-Tree with value-log separation) to create a production-ready distributed consensus layer for Fractio.

**Goals:**
- Implement a fully functional Raft consensus implementation
- Store Raft log entries and server state in WiscKey for durability
- Provide a clean Nim API for creating and managing Raft clusters

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Fractio Application                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐         │
│  │   Nim API    │    │   Nim API    │    │   Nim API    │         │
│  │  (RaftNode)  │    │ (StateMach.) │    │  (Cluster)   │         │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘         │
│         │                    │                    │                  │
│         └────────────────────┼────────────────────┘                  │
│                              │                                        │
│  ┌───────────────────────────▼─────────────────────────────────┐   │
│  │              C++ NuRaft Core Library                         │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │   │
│  │  │   RaftServer │  │  Consensus  │  │   RPC Handler      │ │   │
│  │  │   (Leader)   │  │  (Protocol) │  │   (Messages)       │ │   │
│  │  └─────────────┘  └─────────────┘  └─────────────────────┘ │   │
│  └────────────────────────────┬────────────────────────────────┘   │
│                               │                                      │
│  ┌────────────────────────────▼────────────────────────────────┐   │
│  │              C Wrapper Layer (nuraft_c_wrapper)             │   │
│  │  - log_store adapter    - state_mgr adapter                │   │
│  │  - buffer helpers       - serialization/deserialization    │   │
│  └────────────────────────────┬────────────────────────────────┘   │
│                               │                                      │
│  ┌────────────────────────────▼────────────────────────────────┐   │
│  │              WiscKey Log Store (wisckey_log_store)         │   │
│  │  ┌──────────────────────────────────────────────────────┐  │   │
│  │  │ Key Schema:                                          │  │   │
│  │  │   "log:{index}"     → serialized log entry          │  │   │
│  │  │   "term:{index}"    → term for log entry            │  │   │
│  │  │   "start_idx"       → start index (for compaction) │  │   │
│  │  │   "config"          → cluster configuration          │  │   │
│  │  │   "srv_state"       → server state (term, voted_for)│  │   │
│  │  └──────────────────────────────────────────────────────┘  │   │
│  └────────────────────────────┬────────────────────────────────┘   │
│                               │                                      │
│  ┌────────────────────────────▼────────────────────────────────┐   │
│  │              WiscKey / LevelDB                             │   │
│  │   ┌─────────┐  ┌─────────┐  ┌─────────┐                   │   │
│  │   │  SST    │  │  MEM    │  │  VLOG   │                   │   │
│  │   │ Tables  │  │  Table  │  │ (Values)│                   │   │
│  │   └─────────┘  └─────────┘  └─────────┘                   │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

## 3. Key Components

### 3.1 C Wrapper Layer (`nuraft_c_wrapper`)

The C wrapper provides a C API that can be called from Nim. It wraps NuRaft's C++ interfaces.

**Files:**
- `thirdparty/NuRaft/wrapper/nuraft_c_wrapper.h` - Header declarations
- `thirdparty/NuRaft/wrapper/nuraft_c_wrapper.cxx` - Implementation
- `thirdparty/NuRaft/wrapper/CMakeLists.txt` - Build configuration

**Components:**

```cpp
// Buffer operations (existing)
void* nuraft_buffer_create(size_t size);
void nuraft_buffer_destroy(void* buf);
void* nuraft_buffer_data(void* buf);
size_t nuraft_buffer_size(void* buf);
void nuraft_buffer_put(void* buf, const void* data, size_t len);
int32_t nuraft_buffer_get_int(void* buf);
void nuraft_buffer_put_int(void* buf, int32_t i);
...

// Logger operations (existing)
void* nuraft_logger_create(int32_t level);
void nuraft_logger_destroy(void* logger);

// NEW: Log Store operations
void* nuraft_log_store_create(const char* path);
void nuraft_log_store_destroy(void* store);
ulong nuraft_log_store_next_slot(void* store);
ulong nuraft_log_store_start_index(void* store);
void* nuraft_log_store_last_entry(void* store);
ulong nuraft_log_store_append(void* store, void* entry);
void nuraft_log_store_write_at(void* store, ulong index, void* entry);
void* nuraft_log_store_entries(void* store, ulong start, ulong end);
void* nuraft_log_store_entry_at(void* store, ulong index);
ulong nuraft_log_store_term_at(void* store, ulong index);
void* nuraft_log_store_pack(void* store, ulong index, int32 cnt);
void nuraft_log_store_apply_pack(void* store, ulong index, void* pack);
bool nuraft_log_store_compact(void* store, ulong last_log_index);
bool nuraft_log_store_flush(void* store);

// NEW: State Manager operations
void* nuraft_state_mgr_create(const char* path, int32 server_id);
void nuraft_state_mgr_destroy(void* mgr);
void* nuraft_state_mgr_load_config(void* mgr);
void nuraft_state_mgr_save_config(void* mgr, void* config);
void nuraft_state_mgr_save_state(void* mgr, void* state);
void* nuraft_state_mgr_read_state(void* mgr);
void* nuraft_state_mgr_load_log_store(void* mgr);
int32 nuraft_state_mgr_server_id(void* mgr);

// NEW: Raft Server operations
void* nuraft_raft_server_create(void* params, void* state_mgr, 
                                void* state_machine, void* logger);
void nuraft_raft_server_destroy(void* server);
bool nuraft_raft_server_init(void* server);
void nuraft_raft_server_shutdown(void* server);
int nuraft_raft_server_get_leader(void* server);
bool nuraft_raft_server_is_leader(void* server);
ulong nuraft_raft_server_commit(void* server, void* data, size_t len);
```

### 3.2 WiscKey Log Store Implementation

The WiscKey-based log store persists Raft log entries to WiscKey.

**Key Schema:**
```
log:{index}       → Serialized log entry (term, value_type, data)
term:{index}      → Term value (stored separately for efficient term_at)
start_idx         → Start index (for log compaction tracking)
```

**Log Entry Serialization Format:**
```
[4 bytes: term]
[1 byte: value_type]
[4 bytes: data_length]
[n bytes: data]
```

**Implementation Strategy:**
- Use WiscKey for log storage (keys are small, values can be large)
- Implement batch writes for append operations
- Cache frequently accessed entries in memory
- Background compaction for log truncation

### 3.3 WiscKey State Manager Implementation

The state manager persists:
- **Cluster Configuration**: Servers, endpoints, their IDs
- **Server State**: Current term, voted_for, role
- **Log Store**: Reference to the log store instance

**Key Schema:**
```
config            → Serialized cluster configuration
srv_state         → Serialized server state (term, voted_for)
log_store_ptr    → Pointer to log store (for recovery)
```

## 4. Integration Points

### 4.1 NuRaft Interfaces to Implement

1. **log_store** - For Raft log persistence
   - `next_slot()` - Get next available log index
   - `start_index()` - Get first valid log index
   - `append(entry)` - Append a log entry
   - `log_entries(start, end)` - Get range of entries
   - `term_at(index)` - Get term at index
   - `pack(index, cnt)` - Pack entries for compaction
   - `apply_pack(index, pack)` - Apply packed entries
   - `compact(last_index)` - Compact log up to index
   - `flush()` - Ensure durability

2. **state_mgr** - For state persistence
   - `load_config()` / `save_config()` - Cluster configuration
   - `save_state()` / `read_state()` - Server state (term, voted_for)
   - `load_log_store()` - Get log store instance
   - `server_id()` - Get this server's ID

3. **state_machine** - For application state (provided by user)
   - `commit(log_idx, data)` - Apply committed log
   - `rollback(log_idx, data)` - Rollback uncommitted change

### 4.2 Nim API Design

```nim
# Main Raft Node
type
  RaftNode* = ref object
    ## High-level Raft node for managing consensus
    serverId*: int32
    endpoint*: string
    nuraftServer*: pointer        # NuRaft server instance
    stateMachine*: pointer        # User state machine
    stateMgr*: pointer           # State manager
    logStore*: pointer           # Log store
    initialized*: bool

# Configuration
type
  RaftConfig* = object
    ## Configuration for Raft node
    serverId*: int32
    endpoint*: string
    electionTimeout*: int        # ms
    heartbeatInterval*: int      # ms
    logStoragePath*: string      # WiscKey path
    snapshotEnabled*: bool
    snapshotDistance*: int       # Log distance between snapshots

# Cluster management
proc newRaftNode*(config: RaftConfig, stateMachine: StateMachine): RaftNode
proc init*(node: RaftNode): bool
proc shutdown*(node: RaftNode)
proc addServer*(node: RaftNode, serverId: int32, endpoint: string): bool
proc removeServer*(node: RaftNode, serverId: int32): bool
proc getLeader*(node: RaftNode): int32
proc isLeader*(node: RaftNode): bool
proc commit*(node: RaftNode, data: string): int64
proc getState*(node: RaftNode): ServerRole

# State Machine interface (user implements)
type
  StateMachine* = ref object of RootObj
    ## Base class for user-defined state machines
proc commit*(sm: StateMachine, logIdx: int64, data: string): string
proc rollback*(sm: StateMachine, logIdx: int64, data: string)
proc getLastAppliedIndex*(sm: StateMachine): int64

# Example: Key-Value State Machine
type
  KVStateMachine* = ref object of StateMachine
    ## Simple key-value state machine using WiscKey
    store*: WiscKeyBackend
    
proc commit*(sm: KVStateMachine, logIdx: int64, data: string): string =
  # Parse and apply key-value operation
  discard
```

## 5. Data Flow

### 5.1 Initialization

```
1. Create WiscKey storage backend
   └→ Open database at logStoragePath

2. Create WiscKeyLogStore (via C wrapper)
   └→ Initialize with WiscKey backend
   └→ Load start_index from storage

3. Create WiscKeyStateMgr (via C wrapper)
   └→ Load cluster config from WiscKey
   └→ Load server state from WiscKey
   └→ Return log store instance

4. Create State Machine
   └→ User-provided implementation

5. Create NuRaft Server
   └→ Initialize with state manager and state machine

6. Start Raft Server
   └→ Begin accepting requests
```

### 5.2 Log Append (Leader)

```
1. Client calls node.commit(data)
2. Nim converts data to buffer
3. C wrapper calls nuraft_server::commit()
4. NuRaft:
   a. Appends to local log store
   b. Replicates to followers
   c. Waits for quorum
5. On commit:
   a. NuRaft calls state_machine::commit()
   b. C wrapper invokes Nim callback
   c. Nim state machine applies change
6. Return log index to client
```

### 5.3 Recovery after Crash

```
1. Open WiscKey storage
2. Create WiscKeyLogStore
   └→ Loads start_index from "start_idx" key
3. Create WiscKeyStateMgr
   └→ Loads config from "config"
   └→ Loads state from "srv_state"
4. Create NuRaft Server with recovered state
5. Server resumes operation
```

## 6. Error Handling

### 6.1 Error Categories

1. **Storage Errors**: WiscKey read/write failures
   - Retry with exponential backoff
   - Return error to client if persistent

2. **Network Errors**: RPC failures between nodes
   - NuRaft handles internally (election, log replication)

3. **Consistency Errors**: Log inconsistency
   - NuRaft initiates recovery procedures

### 6.2 Failure Recovery

- **Log Corruption**: Rebuild from snapshots
- **Disk Full**: Return error, halt writes
- **Network Partition**: Election timeout triggers leader election

## 7. Testing Strategy

### 7.1 Unit Tests

- WiscKeyLogStore: All log store operations
- WiscKeyStateMgr: Config and state persistence
- Serialization: Log entry, config, state

### 7.2 Integration Tests

- Single node: Init, commit, shutdown, restart
- Three nodes: Leader election, replication
- Failure scenarios: Network partition, crash recovery

### 7.3 Concurrency Tests

- Parallel appends
- Concurrent reads/writes
- Thread safety of Nim bindings

## 8. Performance Considerations

### 8.1 Write Path

- Batch multiple log appends
- Use WiscKey async writes (default)
- Flush on quorum success

### 8.2 Read Path

- Cache recent log entries in memory
- Use bloom filters for term lookups

### 8.3 Compaction

- Background compaction to remove old log entries
- Periodic snapshots to reduce recovery time

## 9. File Structure

```
src/fractio/distributed/
├── raft/
│   ├── types.nim              # Core types (RaftNode, Config, etc.)
│   ├── node.nim              # RaftNode implementation
│   ├── state_machine.nim      # State machine interface
│   ├── cluster.nim           # Cluster management
│   ├── rpc.nim               # RPC handler
│   └── c_bindings.nim        # C wrapper Nim bindings
│
thirdparty/NuRaft/
├── wrapper/
│   ├── CMakeLists.txt
│   ├── nuraft_c_wrapper.h
│   ├── nuraft_c_wrapper.cxx
│   ├── wisckey_log_store.hxx
│   ├── wisckey_log_store.cxx
│   ├── wisckey_state_mgr.hxx
│   └── wisckey_state_mgr.cxx
│
tests/unit/distributed/raft/
├── test_node.nim
├── test_state_machine.nim
├── test_cluster.nim
└── test_replication.nim
```

## 10. Implementation Dependencies

- **WiscKey**: Already integrated in `src/fractio/storage/wisckey_backend.nim`
- **NuRaft**: Already cloned in `thirdparty/NuRaft/`
- **C++ Compiler**: GCC/Clang with C++17 support
- **CMake**: For building C wrapper

## 11. Milestones

1. **Phase 1**: C Wrapper Extensions
   - Extend wrapper with log store and state manager functions
   - Implement WiscKeyLogStore in C++
   - Implement WiscKeyStateMgr in C++

2. **Phase 2**: Nim Bindings
   - Create complete Nim bindings for C wrapper
   - Implement high-level Nim API

3. **Phase 3**: State Machine
   - Implement base state machine interface
   - Create example KV state machine

4. **Phase 4**: Testing
   - Unit tests for all components
   - Integration tests for cluster
   - Concurrency tests
