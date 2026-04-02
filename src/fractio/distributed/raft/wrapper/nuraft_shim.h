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

// =============================================================================
// State Machine
// =============================================================================

void* nuraft_sm_create(nuraft_commit_cb commit_cb, void* ctx);
void nuraft_sm_destroy(void* sm);
uint64_t nuraft_sm_last_commit_index(void* sm);

// =============================================================================
// State Manager
// =============================================================================

void* nuraft_smgr_create(int32_t my_server_id, const char* my_endpoint,
                         int32_t num_servers, const int32_t* server_ids, const char** endpoints);
void nuraft_smgr_destroy(void* smgr);

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
                               const char* msg_data, size_t msg_len);

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

int nuraft_server_append_entry(void* server, const char* data, size_t len, uint64_t* out_log_idx);
int nuraft_server_add_srv(void* server, int32_t srv_id, const char* endpoint);
int nuraft_server_remove_srv(void* server, int32_t srv_id);
int nuraft_server_set_priority(void* server, int32_t srv_id, int32_t priority);
void nuraft_server_yield_leadership(void* server, bool immediate, int32_t successor_id);

#ifdef __cplusplus
}
#endif

#endif // NURAFT_SHIM_H