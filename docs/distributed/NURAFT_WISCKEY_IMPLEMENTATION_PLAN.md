# NuRaft + WiscKey Integration Implementation Plan

This document outlines the step-by-step implementation plan for integrating NuRaft with WiscKey.

---

## Phase 1: C++ Wrapper Extensions

### Step 1.1: Extend C Wrapper Header
**File**: `thirdparty/NuRaft/wrapper/nuraft_c_wrapper.h`

Add the following declarations:

```cpp
// Log Store
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

// State Manager
void* nuraft_state_mgr_create(const char* path, int32 server_id);
void nuraft_state_mgr_destroy(void* mgr);
void* nuraft_state_mgr_load_config(void* mgr);
void nuraft_state_mgr_save_config(void* mgr, void* config);
void nuraft_state_mgr_save_state(void* mgr, void* state);
void* nuraft_state_mgr_read_state(void* mgr);
void* nuraft_state_mgr_load_log_store(void* mgr);
int32 nuraft_state_mgr_server_id(void* mgr);

// Raft Server
void* nuraft_raft_server_create(void* params, void* state_mgr, 
                                void* state_machine, void* logger);
void nuraft_raft_server_destroy(void* server);
bool nuraft_raft_server_init(void* server);
void nuraft_raft_server_shutdown(void* server);
int nuraft_raft_server_get_leader(void* server);
bool nuraft_raft_server_is_leader(void* server);
ulong nuraft_raft_server_commit(void* server, void* data, size_t len);
```

**Status**: TODO  
**Estimated Effort**: 1 hour

---

### Step 1.2: Implement WiscKey Log Store
**File**: `thirdparty/NuRaft/wrapper/wisckey_log_store.hxx`  
**File**: `thirdparty/NuRaft/wrapper/wisckey_log_store.cxx`

Create a C++ class `wisckey_log_store` that implements `log_store` interface:

```cpp
#include <libnuraft/log_store.hxx>
#include <leveldb/db.h>

namespace nuraft {

class wisckey_log_store : public log_store {
public:
    wisckey_log_store(const std::string& path);
    ~wisckkey_log_store();

    // Required methods from log_store interface
    ulong next_slot() const override;
    ulong start_index() const override;
    ptr<log_entry> last_entry() const override;
    ulong append(ptr<log_entry>& entry) override;
    void write_at(ulong index, ptr<log_entry>& entry) override;
    ptr<std::vector<ptr<log_entry>>> log_entries(ulong start, ulong end) override;
    ptr<log_entry> entry_at(ulong index) override;
    ulong term_at(ulong index) override;
    ptr<buffer> pack(ulong index, int32 cnt) override;
    void apply_pack(ulong index, buffer& pack) override;
    bool compact(ulong last_log_index) override;
    bool flush() override;

private:
    std::string path_;
    leveldb::DB* db_;
    std::atomic<ulong> start_idx_;
    std::mutex lock_;
    
    // Helper methods
    std::string logKey(ulong index);
    std::string termKey(ulong index);
    ptr<log_entry> deserializeEntry(const std::string& data);
    std::string serializeEntry(const ptr<log_entry>& entry);
};

} // namespace nuraft
```

**Key Schema in WiscKey:**
- Key: `log:{index}` → Serialized log entry
- Key: `term:{index}` → Term value (8 bytes)
- Key: `start_idx` → Start index (8 bytes)

**Status**: TODO  
**Estimated Effort**: 4-6 hours

---

### Step 1.3: Implement WiscKey State Manager
**File**: `thirdparty/NuRaft/wrapper/wisckey_state_mgr.hxx`  
**File**: `thirdparty/NuRaft/wrapper/wisckey_state_mgr.cxx`

Create a C++ class `wisckey_state_mgr` that implements `state_mgr` interface:

```cpp
#include <libnuraft/state_mgr.hxx>
#include <libnuraft/log_store.hxx>

namespace nuraft {

class wisckey_state_mgr : public state_mgr {
public:
    wisckey_state_mgr(const std::string& path, int32 server_id);
    ~wisckey_state_mgr();

    // From state_mgr interface
    ptr<cluster_config> load_config() override;
    void save_config(const cluster_config& config) override;
    void save_state(const srv_state& state) override;
    ptr<srv_state> read_state() override;
    ptr<log_store> load_log_store() override;
    int32 server_id() override;
    void system_exit(int exit_code) override;

private:
    std::string path_;
    int32 server_id_;
    ptr<log_store> log_store_;
    // ... internal state
};

} // namespace nuraft
```

**Key Schema in WiscKey:**
- Key: `config` → Serialized cluster configuration
- Key: `srv_state` → Serialized server state

**Status**: TODO  
**Estimated Effort**: 3-4 hours

---

### Step 1.4: Implement Wrapper Functions
**File**: `thirdparty/NuRaft/wrapper/nuraft_c_wrapper.cxx`

Add implementations for all declared wrapper functions.

**Status**: TODO  
**Estimated Effort**: 2-3 hours

---

### Step 1.5: Build C Wrapper Library
**File**: `thirdparty/NuRaft/wrapper/CMakeLists.txt`

Create/Update CMake configuration:

```cmake
cmake_minimum_required(VERSION 3.10)
project(nuraft_c_wrapper)

set(CMAKE_CXX_STANDARD 17)

# Include paths
include_directories(${NURAFT_INCLUDE_DIR})
include_directories(${WISCKEY_INCLUDE_DIR})

# Sources
set(SOURCES
    nuraft_c_wrapper.cxx
    wisckey_log_store.cxx
    wisckey_state_mgr.cxx
)

# Create library
add_library(nuraft_c_wrapper SHARED ${SOURCES})

# Link libraries
target_link_libraries(nuraft_c_wrapper 
    nuraft
    leveldb
    pthread
    dl
)

# Install
install(TARGETS nuraft_c_wrapper LIBRARY DESTINATION lib)
```

Build the library:
```bash
cd thirdparty/NuRaft/wrapper
mkdir -p build && cd build
cmake .. -DNURAFT_INCLUDE_DIR=/usr/local/include -DWISCKEY_INCLUDE_DIR=/usr/local/include
make -j$(nproc)
sudo make install
```

**Status**: TODO  
**Estimated Effort**: 1 hour

---

## Phase 2: Nim Bindings

### Step 2.1: Create C Bindings Module
**File**: `src/fractio/distributed/raft/c_bindings.nim`

Create Nim bindings for all C wrapper functions:

```nim
# C function imports
proc c_nuraft_buffer_create*(size: csize_t): pointer {.
  importc: "nuraft_buffer_create", dynlib: "libnuraft_c_wrapper.so".}
proc c_nuraft_buffer_destroy*(buf: pointer) {.
  importc: "nuraft_buffer_destroy", dynlib: "libnuraft_c_wrapper.so".}
# ... etc for all functions

# Nim wrappers for type safety
type
  NuRaftBuffer* = ref object
    ptr*: pointer
    
proc newNuRaftBuffer*(size: int): NuRaftBuffer =
  new(result)
  result.ptr = c_nuraft_buffer_create(csize_t(size))
  
proc destroy*(buf: NuRaftBuffer) =
  c_nuraft_buffer_destroy(buf.ptr)
  buf.ptr = nil
```

**Status**: TODO  
**Estimated Effort**: 3-4 hours

---

### Step 2.2: Create Raft Types Module
**File**: `src/fractio/distributed/raft/types.nim`

Define core Nim types:

```nim
type
  RaftNode* = ref object
    ## High-level Raft node
    serverId*: int32
    endpoint*: string
    nuraftServer*: pointer
    stateMachine*: pointer
    stateMgr*: pointer
    logStore*: pointer
    initialized*: bool
    
  RaftConfig* = object
    serverId*: int32
    endpoint*: string
    electionTimeout*: int        # milliseconds
    heartbeatInterval*: int      # milliseconds
    logStoragePath*: string
    snapshotEnabled*: bool
    snapshotDistance*: int
    
  ServerRole* = enum
    srFollower
    srCandidate  
    srLeader
    srReserved
```

**Status**: TODO  
**Estimated Effort**: 1-2 hours

---

### Step 2.3: Create Raft Node Implementation
**File**: `src/fractio/distributed/raft/node.nim`

Implement high-level Raft node API:

```nim
proc newRaftNode*(config: RaftConfig, stateMachine: pointer): RaftNode =
  new(result)
  result.serverId = config.serverId
  result.endpoint = config.endpoint
  
  # Create state manager
  result.stateMgr = c_nuraft_state_mgr_create(
    config.logStoragePath.cstring, 
    config.serverId
  )
  
  # Create log store
  result.logStore = c_nuraft_log_store_create(config.logStoragePath.cstring)
  
  # Create Raft server
  let params = createRaftParams(config)
  result.nuraftServer = c_nuraft_raft_server_create(
    params, 
    result.stateMgr, 
    stateMachine,
    getLogger()
  )

proc init*(node: RaftNode): bool =
  result = c_nuraft_raft_server_init(node.nuraftServer)

proc shutdown*(node: RaftNode) =
  c_nuraft_raft_server_shutdown(node.nuraftServer)

proc commit*(node: RaftNode, data: string): int64 =
  let buf = newNuRaftBuffer(data.len + 4)
  buf.putInt32(int32(data.len))
  buf.put(data)
  result = c_nuraft_raft_server_commit(node.nuraftServer, buf.ptr, buf.size)

proc getLeader*(node: RaftNode): int32 =
  result = c_nuraft_raft_server_get_leader(node.nuraftServer)

proc isLeader*(node: RaftNode): bool =
  result = c_nuraft_raft_server_is_leader(node.nuraftServer) != 0
```

**Status**: TODO  
**Estimated Effort**: 4-6 hours

---

## Phase 3: State Machine Interface

### Step 3.1: Create State Machine Base Class
**File**: `src/fractio/distributed/raft/state_machine.nim`

```nim
type
  StateMachine* = ref object of RootObj
    ## Base class for user-defined state machines
    
  StateMachineCallbacks* = object
    ## Callbacks for state machine operations
    commitFn*: proc(sm: StateMachine, logIdx: int64, data: string): string
    rollbackFn*: proc(sm: StateMachine, logIdx: int64, data: string)
    getLastAppliedFn*: proc(sm: StateMachine): int64

# Example: Key-Value State Machine
type
  KVStateMachine* = ref object of StateMachine
    ## Simple key-value state machine
    backend*: WiscKeyBackend
    
proc commit*(sm: KVStateMachine, logIdx: int64, data: string): string =
  # Parse operation: [1 byte: op] [key] [value]
  # Apply to WiscKey
  discard

proc rollback*(sm: KVStateMachine, logIdx: int64, data: string) =
  # Undo operation
  discard
```

**Status**: TODO  
**Estimated Effort**: 2-3 hours

---

## Phase 4: Testing

### Step 4.1: Unit Tests for C Bindings
**File**: `tests/unit/distributed/raft/test_c_bindings.nim`

Test all C wrapper functions:
- Buffer create/destroy
- Log store operations
- State manager operations
- Raft server operations

**Status**: TODO  
**Estimated Effort**: 2-3 hours

---

### Step 4.2: Integration Tests
**File**: `tests/unit/distributed/raft/test_node_integration.nim`

Test complete scenarios:
1. Single node init/shutdown
2. Three node cluster
3. Leader election
4. Log replication
5. Crash recovery
6. Configuration changes

**Status**: TODO  
**Estimated Effort**: 4-6 hours

---

### Step 4.3: Concurrency Tests
**File**: `tests/unit/distributed/raft/test_concurrency.nim`

- Parallel commits
- Concurrent reads/writes
- Thread safety verification

**Status**: TODO  
**Estimated Effort**: 2-3 hours

---

## Phase 5: Documentation & Examples

### Step 5.1: Usage Examples
**File**: `docs/examples/raft_kv_server.nim`

Create example demonstrating:
- Starting a Raft cluster
- Using KV state machine
- Client requests

### Step 5.2: API Documentation
Update `docs/distributed/` with complete API documentation

---

## Implementation Checklist

| Step | Description | Effort | Status |
|------|-------------|--------|--------|
| 1.1 | Extend C wrapper header | 1h | TODO |
| 1.2 | Implement WiscKey log store | 4-6h | TODO |
| 1.3 | Implement WiscKey state manager | 3-4h | TODO |
| 1.4 | Implement wrapper functions | 2-3h | TODO |
| 1.5 | Build wrapper library | 1h | TODO |
| 2.1 | Create C bindings module | 3-4h | TODO |
| 2.2 | Create Raft types module | 1-2h | TODO |
| 2.3 | Create Raft node implementation | 4-6h | TODO |
| 3.1 | Create state machine interface | 2-3h | TODO |
| 4.1 | Unit tests for C bindings | 2-3h | TODO |
| 4.2 | Integration tests | 4-6h | TODO |
| 4.3 | Concurrency tests | 2-3h | TODO |
| 5.1 | Usage examples | 2h | TODO |
| 5.2 | API documentation | 2h | TODO |

**Total Estimated Effort**: 35-45 hours

---

## Notes

- All C++ code must be compiled with the same compiler flags as NuRaft
- WiscKey must be built as a shared library (`libleveldb.so`)
- Test with `LD_LIBRARY_PATH=/usr/local/lib` for development
- Consider memory management carefully - Nim GC vs manual C++ memory
