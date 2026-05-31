// Minimal NuRaft Shim - C++ Interface Implementations
//
// This shim provides minimal C++ implementations of NuRaft interfaces.
// All logic is delegated to Nim via callbacks.
//
// Kept minimal because:
// 1. NuRaft requires C++ virtual interface implementations
// 2. Nim cannot easily inherit from C++ classes
// 3. All business logic stays in Nim (debugging, logging, etc.)

#ifndef NURAFT_SHIM_H
#define NURAFT_SHIM_H

#include "libnuraft/nuraft.hxx"

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// =============================================================================
// Callback Types (called from C++ to Nim)
// =============================================================================

// State machine commit callback
typedef void (*nuraft_commit_cb)(void* ctx, uint64_t log_idx, const char* data, size_t len);

// Event callback (leader/follower changes)
typedef void (*nuraft_event_cb)(void* ctx, int event_type, int32_t leader_id, uint64_t term);

// Send message callback (called when NuRaft wants to send a message)
typedef int (*nuraft_send_cb)(void* ctx, const char* group_id_bytes, int32_t src_node_id,
                              int32_t dst_node_id, const char* msg_data, size_t msg_len);

// Schedule timer callback
typedef void (*nuraft_schedule_timer_cb)(void* ctx, int32_t timer_id, int32_t delay_ms);

// Cancel timer callback
typedef void (*nuraft_cancel_timer_cb)(void* ctx, int32_t timer_id);

// =============================================================================
// Log Store Callback Types (called from C++ to Nim for persistent log store)
// =============================================================================

// Append a log entry. Nim stores the serialized entry and returns the index.
// entry_data: serialized log entry [term:8][val_type:1][data_len:4][data:N]
// entry_len: length of entry_data
// Returns: the log index where the entry was stored
typedef uint64_t (*nuraft_log_append_cb)(void* ctx, uint64_t term, int32_t val_type,
                                         const char* entry_data, size_t entry_len);

// Write a log entry at the given index, truncating all entries after it.
typedef void (*nuraft_log_write_at_cb)(void* ctx, uint64_t index, uint64_t term,
                                        int32_t val_type, const char* entry_data, size_t entry_len);

// Get a log entry at the given index.
// Nim writes the serialized entry into out_data (up to out_capacity bytes).
// Returns: the actual size of the entry data, or 0 if not found.
typedef size_t (*nuraft_log_get_cb)(void* ctx, uint64_t index, uint64_t* out_term,
                                     int32_t* out_val_type, char* out_data, size_t out_capacity);

// Get the term at the given index. Returns 0 if not found.
typedef uint64_t (*nuraft_log_term_at_cb)(void* ctx, uint64_t index);

// Get the next available log slot (1-based).
typedef uint64_t (*nuraft_log_next_slot_cb)(void* ctx);

// Get the start index of the log.
typedef uint64_t (*nuraft_log_start_index_cb)(void* ctx);

// Pack log entries starting at index for count entries.
// Nim writes the packed data into out_data (up to out_capacity bytes).
// Returns: the actual size of the packed data, or 0 on error.
typedef size_t (*nuraft_log_pack_cb)(void* ctx, uint64_t index, int32_t count,
                                     char* out_data, size_t out_capacity);

// Apply packed log entries starting at index.
typedef void (*nuraft_log_apply_pack_cb)(void* ctx, uint64_t index,
                                          const char* pack_data, size_t pack_len);

// Compact the log store up to and including last_log_index.
typedef int32_t (*nuraft_log_compact_cb)(void* ctx, uint64_t last_log_index);

// Flush all pending writes to durable storage.
typedef int32_t (*nuraft_log_flush_cb)(void* ctx);

// =============================================================================
// State Manager Callback Types (called from C++ to Nim for persistent state)
// =============================================================================

// Save Raft state (term, voted_for, config_hwm) to persistent storage.
// state_data: serialized state [term:8][voted_for:4][padding:4][config_hwm:8]
typedef void (*nuraft_state_save_cb)(void* ctx, uint64_t term, int32_t voted_for,
                                      uint64_t config_hwm);

// Load Raft state from persistent storage.
// Nim writes the state values into the output parameters.
// Returns: 1 if state was found, 0 if not found.
typedef int32_t (*nuraft_state_read_cb)(void* ctx, uint64_t* out_term, int32_t* out_voted_for,
                                        uint64_t* out_config_hwm);

// Save cluster config to persistent storage.
typedef void (*nuraft_config_save_cb)(void* ctx, const char* config_data, size_t config_len);

// Load cluster config from persistent storage.
// Nim writes config data into out_data (up to out_capacity bytes).
// Returns: the actual size of the config data, or 0 if not found.
typedef size_t (*nuraft_config_load_cb)(void* ctx, char* out_data, size_t out_capacity);

// =============================================================================
// Process Request - Exposes NuRaft's internal process_req
// =============================================================================

// Process a raw request message through the raft_server.
// Used by Nim to handle incoming messages.
// Returns: pointer to serialized response buffer (caller must free with nuraft_free_buffer)
// out_len: length of response buffer
void* nuraft_process_req(void* server, const char* req_data, size_t req_len, size_t* out_len);

// Free a buffer returned by nuraft_process_req
void nuraft_free_buffer(void* buf);

// =============================================================================
// Raft Parameters
// =============================================================================

void* nuraft_params_create();
void nuraft_params_destroy(void* params);
void nuraft_params_set_election_timeout(void* params, int32_t lower_ms, int32_t upper_ms);
void nuraft_params_set_heartbeat_interval(void* params, int32_t ms);
void nuraft_params_set_return_method(void* params, int method);
void nuraft_params_set_snapshot_distance(void* params, int32_t distance);
void nuraft_params_set_reserved_log_items(void* params, int32_t count);
void nuraft_params_set_client_req_timeout(void* params, int32_t ms);
void nuraft_params_set_max_append_size(void* params, int32_t size);
void nuraft_params_set_leadership_transfer_min_wait_time(void* params, int32_t ms);
void nuraft_params_set_custom_election_quorum_size(void* params, int32_t size);
void nuraft_params_set_auto_adjust_quorum(void* params, int32_t enable);

// =============================================================================
// Limits (global settings)
// =============================================================================

void nuraft_limits_set_busy_connection_limit(int32_t limit);

// Purge expired RPC handlers from the global pending handlers registry.
// Should be called periodically (e.g., every 10 seconds) to prevent
// unbounded growth from lost responses.
void nuraft_purge_expired_handlers();

// =============================================================================
// State Machine
// =============================================================================

void* nuraft_sm_create(nuraft_commit_cb commit_cb, void* ctx);
void nuraft_sm_destroy(void* sm);
uint64_t nuraft_sm_last_commit_index(void* sm);

// =============================================================================
// State Manager
// =============================================================================

// Callback type for configuration changes (called when add_srv is committed)
typedef int32_t (*nuraft_config_change_cb)(void* ctx, int32_t server_id, const char* endpoint);

// Callback type for quorum updates (called when config changes)
typedef void (*nuraft_quorum_update_cb)(void* ctx, int32_t server_id, int32_t quorum_size);

// Create state manager with callback-based persistence (WiscKey-backed).
// The log store and state persistence callbacks delegate to Nim's RaftPersistentStore.
// No file I/O is used — all persistence goes through the Nim callbacks.
void* nuraft_smgr_create_with_callbacks(
    int32_t my_server_id,
    const char* my_endpoint,
    int32_t num_servers,
    const int32_t* server_ids,
    const char** endpoints,
    bool catching_up,
    // Log store callbacks
    void* log_store_ctx,
    nuraft_log_append_cb log_append_cb,
    nuraft_log_write_at_cb log_write_at_cb,
    nuraft_log_get_cb log_get_cb,
    nuraft_log_term_at_cb log_term_at_cb,
    nuraft_log_next_slot_cb log_next_slot_cb,
    nuraft_log_start_index_cb log_start_index_cb,
    nuraft_log_pack_cb log_pack_cb,
    nuraft_log_apply_pack_cb log_apply_pack_cb,
    nuraft_log_compact_cb log_compact_cb,
    nuraft_log_flush_cb log_flush_cb,
    // State callbacks
    void* state_cb_ctx,
    nuraft_state_save_cb state_save_cb,
    nuraft_state_read_cb state_read_cb,
    nuraft_config_save_cb config_save_cb,
    nuraft_config_load_cb config_load_cb);

void nuraft_smgr_destroy(void* smgr);
void nuraft_smgr_set_config_cb(void* smgr, void* ctx, nuraft_config_change_cb cb);
void nuraft_smgr_set_quorum_cb(void* smgr, void* ctx, nuraft_quorum_update_cb cb);
void nuraft_smgr_set_raft_server(void* smgr, void* server);

// =============================================================================
// Multiplexed Context (RPC + Timer)
// =============================================================================

void* nuraft_mp_context_create(
    int32_t server_id,
    void* transport_ctx,
    nuraft_send_cb send_cb,
    void* timer_ctx,
    nuraft_schedule_timer_cb schedule_cb,
    nuraft_cancel_timer_cb cancel_cb
);

void nuraft_mp_context_destroy(void* ctx);
void nuraft_mp_context_set_group_id(void* ctx, const char* group_id_bytes);

// =============================================================================
// Listener Helpers (for message delivery setup)
// =============================================================================

// Get the listener from a multiplexed context
// Returns a pointer that must be freed with nuraft_mp_listener_destroy
void* nuraft_mp_get_listener(void* mp_context);

// Set the source node ID for the listener (used in response routing)
void nuraft_mp_listener_set_src_node_id(void* listener_ptr, int32_t src_node_id);

// Set the callback for sending responses to incoming requests
void nuraft_mp_listener_set_send_response_callback(void* listener_ptr, void* ctx, nuraft_send_cb cb);

// Free a listener pointer returned by nuraft_mp_get_listener
void nuraft_mp_listener_destroy(void* listener_ptr);

// =============================================================================
// Message Delivery (callback-based)
// =============================================================================

// Deliver a message to NuRaft for processing.
// For requests: processes via raft_server and sends response via send_cb
// For responses: matches to pending handler, which may trigger further send_cb calls
void nuraft_mp_deliver_message(void* mp_context, void* server,
                               const void* msg_data, size_t msg_len);

// Invoke a scheduled timer by ID.
// Nim calls this when its timer fires to execute the delayed task.
// Returns: true if timer was found and executed, false otherwise
bool nuraft_mp_invoke_timer(void* mp_context, int32_t timer_id);

// =============================================================================
// Raft Server (with multiplexed context)
// =============================================================================

void* nuraft_server_create(
    void* mp_context,
    void* sm,
    void* smgr,
    void* params,
    nuraft_event_cb event_cb,
    void* event_ctx,
    int skip_initial_election
);

void nuraft_server_destroy(void* server);
void nuraft_server_shutdown(void* server);

bool nuraft_server_is_leader(void* server);
int32_t nuraft_server_get_leader(void* server);
int32_t nuraft_server_get_id(void* server);
uint64_t nuraft_server_get_term(void* server);
uint64_t nuraft_server_get_committed_log_idx(void* server);
uint64_t nuraft_server_get_last_log_idx(void* server);
bool nuraft_server_is_initialized(void* server);

int nuraft_server_append_entry(void* server, const void* data, size_t len, uint64_t* out_log_idx);
int nuraft_server_add_srv(void* server, int32_t srv_id, const char* endpoint);
int nuraft_server_remove_srv(void* server, int32_t srv_id);
int nuraft_server_set_priority(void* server, int32_t srv_id, int32_t priority);
void nuraft_server_yield_leadership(void* server, bool immediate, int32_t successor_id);
void nuraft_server_update_quorum(void* server, int32_t quorum_size);

// Peer info (for checking if preferred leader is online and caught-up)
typedef struct {
    uint64_t last_log_idx;
    uint64_t last_succ_resp_us;  // microseconds since last successful response
    int32_t  exists;
} nuraft_peer_info;

int32_t nuraft_server_get_peer_info(void* server, int32_t peer_id, nuraft_peer_info* out_info);

// Get the number of servers in the current cluster config (peers + self).
int32_t nuraft_server_get_server_count(void* server);

// Initialize the global NuRaft manager (shared thread pool for all Raft groups).
// This MUST be called before creating any raft_server instances.
// Without this, each raft_server creates 2 dedicated threads (commit + append),
// which means 4 groups × 2 threads × 8MB stack = 64MB of thread stacks alone.
// With this, all groups share 2 global threads (1 commit + 1 append = 16MB total).
// Parameters:
//   num_commit_threads: number of shared commit threads (default 1)
//   num_append_threads: number of shared append threads (default 1)
// Returns: 1 if initialized successfully, 0 if already initialized, -1 on error.
int32_t nuraft_global_mgr_init(int32_t num_commit_threads, int32_t num_append_threads);

// Shut down the global NuRaft manager and free resources.
// All raft_server instances MUST be destroyed before calling this.
void nuraft_global_mgr_shutdown();

#ifdef __cplusplus
}
#endif

#endif // NURAFT_SHIM_H