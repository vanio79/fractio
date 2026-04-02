// Minimal NuRaft Shim Implementation
//
// This file provides minimal C++ implementations of NuRaft interfaces.
// All logic is delegated to Nim via callbacks.
//
// Key design:
// - C++ implements NuRaft virtual interfaces (state_machine, state_mgr, rpc_client, etc.)
// - Nim provides callbacks for all operations (send, timer, commit)
// - Message delivery uses process_req with response sent via Nim callback

#include "nuraft_shim.h"

#include "libnuraft/nuraft.hxx"
#include "libnuraft/raft_server_handler.hxx"
#include "in_memory_log_store.hxx"

#include <atomic>
#include <cstring>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>
#include <map>
#include <functional>

using namespace nuraft;

// =============================================================================
// Helper: Access NuRaft's protected process_req method
// =============================================================================

class raft_server_access : public raft_server_handler {
public:
    static ptr<resp_msg> call_process_req(raft_server* srv, req_msg& req) {
        return raft_server_handler::process_req(srv, req);
    }
};

// =============================================================================
// Global Pending Handlers Registry (for response correlation)
// =============================================================================
// Key: (group_id_hex, src_node_id, dst_node_id) -> rpc_handler

static std::mutex g_handlers_lock;
static std::map<std::tuple<std::string, int32_t, int32_t>, rpc_handler> g_pending_handlers;

// =============================================================================
// Message Serialization
// =============================================================================

static ptr<buffer> serialize_req_msg(ptr<req_msg>& req) {
    size_t total_size = 4 + 8 + 4 + 4 + 8 + 8 + 8 + 8 + 4;
    auto& entries = req->log_entries();
    total_size += entries.size() * (8 + 1 + 4);
    for (auto& entry : entries) {
        if (!entry->is_buf_null()) {
            total_size += entry->get_buf().size();
        }
    }

    ptr<buffer> buf = buffer::alloc(total_size);
    buffer_serializer bs(buf);

    bs.put_i32(static_cast<int32>(req->get_type()));
    bs.put_u64(req->get_term());
    bs.put_i32(req->get_src());
    bs.put_i32(req->get_dst());
    bs.put_u64(req->get_last_log_term());
    bs.put_u64(req->get_last_log_idx());
    bs.put_u64(req->get_commit_idx());
    bs.put_u64(req->get_extra_flags());

    bs.put_i32(static_cast<int32>(entries.size()));
    for (auto& entry : entries) {
        bs.put_u64(entry->get_term());
        bs.put_i8(static_cast<int8_t>(entry->get_val_type()));
        if (!entry->is_buf_null()) {
            buffer& entry_buf = entry->get_buf();
            bs.put_i32(static_cast<int32>(entry_buf.size()));
            bs.put_raw(entry_buf.data_begin(), entry_buf.size());
        } else {
            bs.put_i32(0);
        }
    }

    buf->pos(0);
    return buf;
}

static ptr<req_msg> deserialize_req_msg(buffer& buf) {
    buf.pos(0);
    buffer_serializer bs(buf);

    msg_type type = static_cast<msg_type>(bs.get_i32());
    ulong term = bs.get_u64();
    int32 src = bs.get_i32();
    int32 dst = bs.get_i32();
    ulong last_log_term = bs.get_u64();
    ulong last_log_idx = bs.get_u64();
    ulong commit_idx = bs.get_u64();
    uint64_t extra_flags = bs.get_u64();

    ptr<req_msg> req = cs_new<req_msg>(
        term, type, src, dst,
        last_log_term, last_log_idx, commit_idx
    );
    req->set_extra_flags(extra_flags);

    int32 num_entries = bs.get_i32();
    auto& entries = req->log_entries();
    entries.reserve(num_entries);

    for (int32 i = 0; i < num_entries; ++i) {
        ulong entry_term = bs.get_u64();
        log_val_type entry_type = static_cast<log_val_type>(bs.get_i8());
        int32 data_len = bs.get_i32();

        ptr<buffer> data_buf;
        if (data_len > 0) {
            data_buf = buffer::alloc(data_len);
            void* raw = bs.get_raw(data_len);
            std::memcpy(data_buf->data_begin(), raw, data_len);
            data_buf->pos(0);
        }

        ptr<log_entry> entry = cs_new<log_entry>(
            entry_term,
            data_buf,
            entry_type
        );
        entries.push_back(entry);
    }

    return req;
}

static ptr<buffer> serialize_resp_msg(ptr<resp_msg>& resp) {
    size_t total_size = 4 + 8 + 4 + 4 + 8 + 1 + 4 + 8 + 4;
    ptr<buffer> ctx = resp->get_ctx();
    if (ctx) {
        total_size += ctx->size();
    }

    ptr<buffer> buf = buffer::alloc(total_size);
    buffer_serializer bs(buf);

    bs.put_i32(static_cast<int32>(resp->get_type()));
    bs.put_u64(resp->get_term());
    bs.put_i32(resp->get_src());
    bs.put_i32(resp->get_dst());
    bs.put_u64(resp->get_next_idx());
    bs.put_i8(resp->get_accepted() ? 1 : 0);
    bs.put_i32(static_cast<int32>(resp->get_result_code()));
    bs.put_u64(resp->get_extra_flags());

    if (ctx) {
        bs.put_i32(static_cast<int32>(ctx->size()));
        ctx->pos(0);
        bs.put_raw(ctx->data_begin(), ctx->size());
    } else {
        bs.put_i32(0);
    }

    buf->pos(0);
    return buf;
}

static ptr<resp_msg> deserialize_resp_msg(buffer& buf) {
    buf.pos(0);
    buffer_serializer bs(buf);

    msg_type type = static_cast<msg_type>(bs.get_i32());
    ulong term = bs.get_u64();
    int32 src = bs.get_i32();
    int32 dst = bs.get_i32();
    ulong next_idx = bs.get_u64();
    bool accepted = (bs.get_i8() != 0);
    int32 result_code = bs.get_i32();
    uint64_t extra_flags = bs.get_u64();

    ptr<resp_msg> resp = cs_new<resp_msg>(term, type, src, dst, next_idx, accepted);
    resp->set_result_code(static_cast<cmd_result_code>(result_code));
    resp->set_extra_flags(extra_flags);

    int32 ctx_len = bs.get_i32();
    if (ctx_len > 0) {
        ptr<buffer> ctx_buf = buffer::alloc(ctx_len);
        void* raw = bs.get_raw(ctx_len);
        std::memcpy(ctx_buf->data_begin(), raw, ctx_len);
        ctx_buf->pos(0);
        resp->set_ctx(ctx_buf);
    }

    return resp;
}

// Helper to get message type from serialized buffer
static msg_type get_msg_type(buffer& buf) {
    buf.pos(0);
    int32 type_val = buffer_serializer(buf).get_i32();
    return static_cast<msg_type>(type_val);
}

// Helper to check if message is a response type
static bool is_response_type(msg_type type) {
    return (type == request_vote_response ||
            type == append_entries_response ||
            type == add_server_response ||
            type == remove_server_response ||
            type == sync_log_response ||
            type == join_cluster_response ||
            type == leave_cluster_response ||
            type == install_snapshot_response ||
            type == ping_response ||
            type == pre_vote_response ||
            type == other_response ||
            type == priority_change_response ||
            type == reconnect_response ||
            type == custom_notification_response);
}

// =============================================================================
// State Machine (Callback-based)
// =============================================================================

class callback_state_machine : public state_machine {
public:
    callback_state_machine(nuraft_commit_cb commit_cb, void* ctx)
        : commit_cb_(commit_cb), ctx_(ctx), last_committed_idx_(0) {}

    ~callback_state_machine() {}

    ptr<buffer> commit(const ulong log_idx, buffer& data) override {
        last_committed_idx_ = log_idx;
        if (commit_cb_) {
            const char* raw = reinterpret_cast<const char*>(data.data_begin());
            size_t len = data.size() - data.pos();
            commit_cb_(ctx_, log_idx, raw, len);
        }
        return nullptr;
    }

    void commit_config(const ulong log_idx, ptr<cluster_config>& new_conf) override {
        last_committed_idx_ = log_idx;
    }

    ptr<buffer> pre_commit(const ulong log_idx, buffer& data) override {
        return nullptr;
    }

    void rollback(const ulong log_idx, buffer& data) override {}

    int read_logical_snp_obj(snapshot& s, void*& user_snp_ctx, ulong obj_id,
                              ptr<buffer>& data_out, bool& is_last_obj) override {
        data_out = buffer::alloc(sizeof(int32));
        buffer_serializer bs(data_out);
        bs.put_i32(0);
        is_last_obj = true;
        return 0;
    }

    void save_logical_snp_obj(snapshot& s, ulong& obj_id, buffer& data,
                               bool is_first_obj, bool is_last_obj) override {
        obj_id++;
    }

    bool apply_snapshot(snapshot& s) override {
        std::lock_guard<std::mutex> l(snp_lock_);
        ptr<buffer> snp_buf = s.serialize();
        last_snapshot_ = snapshot::deserialize(*snp_buf);
        return true;
    }

    void free_user_snp_ctx(void*& user_snp_ctx) override {}

    ptr<snapshot> last_snapshot() override {
        std::lock_guard<std::mutex> l(snp_lock_);
        return last_snapshot_;
    }

    ulong last_commit_index() override {
        return last_committed_idx_;
    }

    void create_snapshot(snapshot& s, async_result<bool>::handler_type& when_done) override {
        {
            std::lock_guard<std::mutex> l(snp_lock_);
            ptr<buffer> snp_buf = s.serialize();
            last_snapshot_ = snapshot::deserialize(*snp_buf);
        }
        ptr<std::exception> except(nullptr);
        bool ret = true;
        when_done(ret, except);
    }

private:
    nuraft_commit_cb commit_cb_;
    void* ctx_;
    std::atomic<uint64_t> last_committed_idx_;
    ptr<snapshot> last_snapshot_;
    std::mutex snp_lock_;
};

// =============================================================================
// State Manager (In-memory with dynamic config)
// =============================================================================

class dynamic_state_mgr : public state_mgr {
public:
    dynamic_state_mgr(int32 my_id, const std::string& my_endpoint,
                      const std::vector<std::pair<int32, std::string>>& servers)
        : my_id_(my_id), my_endpoint_(my_endpoint),
          cur_log_store_(cs_new<inmem_log_store>()) {
        saved_config_ = cs_new<cluster_config>();
        for (auto& kv : servers) {
            auto sc = cs_new<srv_config>(kv.first, kv.second);
            saved_config_->get_servers().push_back(sc);
        }
    }

    ~dynamic_state_mgr() {}

    ptr<cluster_config> load_config() override {
        return saved_config_;
    }

    void save_config(const cluster_config& config) override {
        ptr<buffer> buf = config.serialize();
        saved_config_ = cluster_config::deserialize(*buf);
    }

    void save_state(const srv_state& state) override {
        ptr<buffer> buf = state.serialize();
        saved_state_ = srv_state::deserialize(*buf);
    }

    ptr<srv_state> read_state() override {
        return saved_state_;
    }

    ptr<log_store> load_log_store() override {
        return cur_log_store_;
    }

    int32 server_id() override {
        return my_id_;
    }

    void system_exit(const int exit_code) override {
        std::cerr << "NuRaft system_exit called with code " << exit_code << std::endl;
    }

private:
    int32 my_id_;
    std::string my_endpoint_;
    ptr<inmem_log_store> cur_log_store_;
    ptr<cluster_config> saved_config_;
    ptr<srv_state> saved_state_;
};

// =============================================================================
// Multiplexed RPC Components
// =============================================================================

// RPC Client - calls Nim send callback
class mp_rpc_client : public rpc_client {
public:
    mp_rpc_client(int32_t server_id, int32_t target_id, void* transport_ctx,
                  nuraft_send_cb send_cb, const char* group_id_bytes)
        : server_id_(server_id), target_id_(target_id),
          transport_ctx_(transport_ctx), send_cb_(send_cb),
          abandoned_(false) {
        if (group_id_bytes) {
            std::memcpy(group_id_bytes_, group_id_bytes, 16);
        }
    }

    ~mp_rpc_client() {}

    void send(ptr<req_msg>& req, rpc_handler& when_done, uint64_t timeout_ms = 0) override {
        if (abandoned_.load()) {
            ptr<resp_msg> resp = cs_new<resp_msg>(0, req->get_type(), target_id_, server_id_, 0, false);
            resp->set_result_code(cmd_result_code::NOT_LEADER);
            ptr<rpc_exception> no_err;
            when_done(resp, no_err);
            return;
        }

        // Serialize request
        ptr<buffer> buf = serialize_req_msg(req);
        const char* data = reinterpret_cast<const char*>(buf->data_begin());
        size_t len = buf->size();

        // Store handler for response correlation
        {
            std::lock_guard<std::mutex> lock(g_handlers_lock);
            std::string gid_hex;
            for (int i = 0; i < 16; i++) {
                char hex[3];
                sprintf(hex, "%02x", (unsigned char)group_id_bytes_[i]);
                gid_hex += hex;
            }
            // Key: (groupId, our_id, target_id)
            g_pending_handlers[std::make_tuple(gid_hex, server_id_, target_id_)] = when_done;
        }

        // Call Nim send callback
        int rc = send_cb_(transport_ctx_, group_id_bytes_, server_id_, target_id_, data, len);
        if (rc != 0) {
            // Send failed - invoke handler with error
            std::lock_guard<std::mutex> lock(g_handlers_lock);
            std::string gid_hex;
            for (int i = 0; i < 16; i++) {
                char hex[3];
                sprintf(hex, "%02x", (unsigned char)group_id_bytes_[i]);
                gid_hex += hex;
            }
            auto key = std::make_tuple(gid_hex, server_id_, target_id_);
            g_pending_handlers.erase(key);
            ptr<rpc_exception> err = cs_new<rpc_exception>("Send failed", req);
            ptr<resp_msg> null_resp;
            when_done(null_resp, err);
        }
    }

    uint64_t get_id() const override { return static_cast<uint64_t>(target_id_); }
    bool is_abandoned() const override { return abandoned_; }
    void mark_abandoned() { abandoned_.store(true); }

private:
    int32_t server_id_;
    int32_t target_id_;
    void* transport_ctx_;
    nuraft_send_cb send_cb_;
    char group_id_bytes_[16] = {0};
    std::atomic<bool> abandoned_;
};

// RPC Client Factory
class mp_rpc_client_factory : public rpc_client_factory {
public:
    mp_rpc_client_factory(int32_t server_id, void* transport_ctx, nuraft_send_cb send_cb)
        : server_id_(server_id), transport_ctx_(transport_ctx), send_cb_(send_cb) {}

    ~mp_rpc_client_factory() {}

    ptr<rpc_client> create_client(const std::string& endpoint) override {
        std::lock_guard<std::mutex> lock(clients_lock_);

        auto it = clients_.find(endpoint);
        if (it != clients_.end()) {
            return it->second;
        }

        // Parse server ID from endpoint (format: "serverId@host:port")
        int32_t target_id = 0;
        size_t at_pos = endpoint.find('@');
        if (at_pos != std::string::npos && at_pos > 0) {
            target_id = std::stoi(endpoint.substr(0, at_pos));
        } else {
            target_id = static_cast<int32_t>(std::hash<std::string>{}(endpoint));
        }

        auto client = cs_new<mp_rpc_client>(server_id_, target_id, transport_ctx_, send_cb_, group_id_bytes_);
        clients_[endpoint] = client;
        return client;
    }

    void set_group_id(const char* group_id_bytes) {
        if (group_id_bytes) {
            std::memcpy(group_id_bytes_, group_id_bytes, 16);
            // Update all existing clients
            std::lock_guard<std::mutex> lock(clients_lock_);
            for (auto& pair : clients_) {
                // Note: clients already have their own copy of group_id_bytes
            }
        }
    }

    void abandon_all() {
        std::lock_guard<std::mutex> lock(clients_lock_);
        for (auto& pair : clients_) {
            pair.second->mark_abandoned();
        }
    }

private:
    int32_t server_id_;
    void* transport_ctx_;
    nuraft_send_cb send_cb_;
    char group_id_bytes_[16] = {0};
    std::mutex clients_lock_;
    std::map<std::string, ptr<mp_rpc_client>> clients_;
};

// RPC Listener - stores handler and response callback
class mp_rpc_listener : public rpc_listener {
public:
    mp_rpc_listener() : stopped_(false), send_resp_cb_(nullptr), send_resp_ctx_(nullptr) {}
    ~mp_rpc_listener() { stop(); }

    void listen(ptr<msg_handler>& handler) override {
        handler_ = handler;
    }

    void stop() override { stopped_.store(true); }
    void shutdown() override { stop(); }

    ptr<msg_handler> get_handler() const { return handler_; }

    void set_group_id(const char* bytes) {
        if (bytes) std::memcpy(group_id_bytes_, bytes, 16);
    }
    const char* get_group_id() const { return group_id_bytes_; }

    void set_src_node_id(int32_t id) { src_node_id_ = id; }
    int32_t get_src_node_id() const { return src_node_id_; }

    void set_send_response_callback(void* ctx, nuraft_send_cb cb) {
        send_resp_ctx_ = ctx;
        send_resp_cb_ = cb;
    }

    bool has_send_response_callback() const { return send_resp_cb_ != nullptr; }
    nuraft_send_cb get_send_resp_cb() const { return send_resp_cb_; }
    void* get_send_resp_ctx() const { return send_resp_ctx_; }

private:
    ptr<msg_handler> handler_;
    std::atomic<bool> stopped_;
    char group_id_bytes_[16] = {0};
    int32_t src_node_id_ = 0;
    void* send_resp_ctx_;
    nuraft_send_cb send_resp_cb_;
};

// Timer - delegates to Nim callbacks
class mp_timer : public delayed_task_scheduler {
public:
    mp_timer(void* timer_ctx, nuraft_schedule_timer_cb schedule_cb, nuraft_cancel_timer_cb cancel_cb)
        : timer_ctx_(timer_ctx), schedule_cb_(schedule_cb), cancel_cb_(cancel_cb),
          next_timer_id_(1), stopped_(false) {}

    ~mp_timer() { stop(); }

    void schedule(ptr<delayed_task>& task, int32 milliseconds) override {
        if (stopped_.load()) return;

        int32_t timer_id = next_timer_id_.fetch_add(1);
        {
            std::lock_guard<std::mutex> lock(tasks_lock_);
            tasks_[timer_id] = task;
        }

        if (schedule_cb_) {
            schedule_cb_(timer_ctx_, timer_id, milliseconds);
        }
    }

    void cancel_impl(ptr<delayed_task>& task) override {
        std::lock_guard<std::mutex> lock(tasks_lock_);
        for (auto it = tasks_.begin(); it != tasks_.end(); ++it) {
            if (it->second == task) {
                if (cancel_cb_) {
                    cancel_cb_(timer_ctx_, it->first);
                }
                tasks_.erase(it);
                return;
            }
        }
    }

    bool invoke_timer(int32_t timer_id) {
        ptr<delayed_task> task;
        {
            std::lock_guard<std::mutex> lock(tasks_lock_);
            auto it = tasks_.find(timer_id);
            if (it == tasks_.end()) {
                std::cerr << "DEBUG invoke_timer: timer_id=" << timer_id << " NOT FOUND" << std::endl;
                return false;
            }
            task = it->second;
            tasks_.erase(it);
        }
        if (task) {
            std::cerr << "DEBUG invoke_timer: timer_id=" << timer_id << " EXECUTING task..." << std::endl;
            task->execute();
            std::cerr << "DEBUG invoke_timer: timer_id=" << timer_id << " EXECUTE DONE" << std::endl;
            return true;
        }
        return false;
    }

    void stop() {
        stopped_.store(true);
        std::lock_guard<std::mutex> lock(tasks_lock_);
        tasks_.clear();
    }

private:
    void* timer_ctx_;
    nuraft_schedule_timer_cb schedule_cb_;
    nuraft_cancel_timer_cb cancel_cb_;
    std::atomic<int32_t> next_timer_id_;
    std::atomic<bool> stopped_;
    std::mutex tasks_lock_;
    std::map<int32_t, ptr<delayed_task>> tasks_;
};

// =============================================================================
// Multiplexed Context (bundles all components)
// =============================================================================

struct mp_context_t {
    ptr<mp_rpc_client_factory> client_factory;
    ptr<mp_rpc_listener> listener;
    ptr<mp_timer> timer;
    int32_t server_id;
    char group_id_bytes[16] = {0};
};

// =============================================================================
// Raft Server Wrapper
// =============================================================================

struct server_wrapper {
    ptr<raft_server> server;
    ptr<context> raft_ctx;
    // Note: mp_ctx is NOT stored here - Nim owns it separately via rpcContext
    // Storing it here would cause double-free when server is destroyed
    ptr<callback_state_machine> sm;
    ptr<dynamic_state_mgr> smgr;
};

// =============================================================================
// C API Implementation
// =============================================================================

extern "C" {

// Process Request - direct API
void* nuraft_process_req(void* server, const char* req_data, size_t req_len, size_t* out_len) {
    if (!server || !req_data || req_len == 0 || !out_len) {
        return nullptr;
    }

    auto* wrapper = static_cast<server_wrapper*>(server);
    if (!wrapper || !wrapper->server) return nullptr;

    // Deserialize request
    ptr<buffer> req_buf = buffer::alloc(req_len);
    std::memcpy(req_buf->data_begin(), req_data, req_len);
    ptr<req_msg> req = deserialize_req_msg(*req_buf);

    // Process through NuRaft
    ptr<resp_msg> resp = raft_server_access::call_process_req(wrapper->server.get(), *req);

    if (!resp) {
        return nullptr;
    }

    // Serialize response
    ptr<buffer> resp_buf = serialize_resp_msg(resp);
    *out_len = resp_buf->size();

    void* result = std::malloc(*out_len);
    if (result) {
        std::memcpy(result, resp_buf->data_begin(), *out_len);
    }
    return result;
}

void nuraft_free_buffer(void* buf) {
    if (buf) {
        std::free(buf);
    }
}

// =============================================================================
// Parameters
// =============================================================================

void* nuraft_params_create() {
    auto* p = new raft_params();
    p->heart_beat_interval_ = 100;
    p->election_timeout_lower_bound_ = 200;
    p->election_timeout_upper_bound_ = 400;
    p->return_method_ = raft_params::blocking;
    p->snapshot_distance_ = 100;
    p->reserved_log_items_ = 10;
    p->max_append_size_ = 100;
    p->client_req_timeout_ = 5000;
    return p;
}

void nuraft_params_destroy(void* params) {
    delete static_cast<raft_params*>(params);
}

void nuraft_params_set_election_timeout(void* params, int32_t lower_ms, int32_t upper_ms) {
    if (!params) return;
    auto* p = static_cast<raft_params*>(params);
    p->election_timeout_lower_bound_ = lower_ms;
    p->election_timeout_upper_bound_ = upper_ms;
}

void nuraft_params_set_heartbeat_interval(void* params, int32_t ms) {
    if (!params) return;
    static_cast<raft_params*>(params)->heart_beat_interval_ = ms;
}

void nuraft_params_set_return_method(void* params, int method) {
    if (!params) return;
    static_cast<raft_params*>(params)->return_method_ =
        static_cast<raft_params::return_method_type>(method);
}

void nuraft_params_set_snapshot_distance(void* params, int32_t distance) {
    if (!params) return;
    static_cast<raft_params*>(params)->snapshot_distance_ = distance;
}

void nuraft_params_set_reserved_log_items(void* params, int32_t count) {
    if (!params) return;
    static_cast<raft_params*>(params)->reserved_log_items_ = count;
}

void nuraft_params_set_client_req_timeout(void* params, int32_t ms) {
    if (!params) return;
    static_cast<raft_params*>(params)->client_req_timeout_ = ms;
}

void nuraft_params_set_max_append_size(void* params, int32_t size) {
    if (!params) return;
    static_cast<raft_params*>(params)->max_append_size_ = size;
}

void nuraft_params_set_leadership_transfer_min_wait_time(void* params, int32_t ms) {
    if (!params) return;
    static_cast<raft_params*>(params)->leadership_transfer_min_wait_time_ = ms;
}

// =============================================================================
// State Machine
// =============================================================================

void* nuraft_sm_create(nuraft_commit_cb commit_cb, void* ctx) {
    auto sm = cs_new<callback_state_machine>(commit_cb, ctx);
    return new ptr<callback_state_machine>(sm);
}

void nuraft_sm_destroy(void* sm) {
    if (sm) {
        delete static_cast<ptr<callback_state_machine>*>(sm);
    }
}

uint64_t nuraft_sm_last_commit_index(void* sm) {
    if (!sm) return 0;
    auto& sp = *static_cast<ptr<callback_state_machine>*>(sm);
    return sp->last_commit_index();
}

// =============================================================================
// State Manager
// =============================================================================

void* nuraft_smgr_create(int32_t my_server_id, const char* my_endpoint,
                          int32_t num_servers, const int32_t* server_ids, const char** endpoints) {
    std::vector<std::pair<int32, std::string>> servers;
    for (int i = 0; i < num_servers; i++) {
        servers.push_back({server_ids[i], std::string(endpoints[i])});
    }
    auto smgr = cs_new<dynamic_state_mgr>(my_server_id, std::string(my_endpoint), servers);
    return new ptr<dynamic_state_mgr>(smgr);
}

void nuraft_smgr_destroy(void* smgr) {
    if (smgr) {
        delete static_cast<ptr<dynamic_state_mgr>*>(smgr);
    }
}

// =============================================================================
// Multiplexed Context
// =============================================================================

void* nuraft_mp_context_create(
    int32_t server_id,
    void* transport_ctx,
    nuraft_send_cb send_cb,
    void* timer_ctx_unused,  // Unused - we pass mp_context as timer ctx
    nuraft_schedule_timer_cb schedule_cb,
    nuraft_cancel_timer_cb cancel_cb
) {
    auto* ctx = new mp_context_t();
    ctx->server_id = server_id;
    ctx->client_factory = cs_new<mp_rpc_client_factory>(server_id, transport_ctx, send_cb);
    ctx->listener = cs_new<mp_rpc_listener>();
    // Pass the mp_context itself as timer_ctx so Nim can invoke timers
    ctx->timer = cs_new<mp_timer>(ctx, schedule_cb, cancel_cb);
    return ctx;
}

void nuraft_mp_context_destroy(void* ctx) {
    if (ctx) {
        auto* mp_ctx = static_cast<mp_context_t*>(ctx);
        std::cerr << "DEBUG nuraft_mp_context_destroy: ctx=" << ctx 
                  << " client_factory=" << mp_ctx->client_factory.get()
                  << " timer=" << mp_ctx->timer.get()
                  << " listener=" << mp_ctx->listener.get() << std::endl;
        try {
            if (mp_ctx->client_factory) {
                std::cerr << "DEBUG nuraft_mp_context_destroy: calling abandon_all" << std::endl;
                mp_ctx->client_factory->abandon_all();
            }
            if (mp_ctx->timer) {
                std::cerr << "DEBUG nuraft_mp_context_destroy: stopping timer" << std::endl;
                mp_ctx->timer->stop();
            }
            if (mp_ctx->listener) {
                std::cerr << "DEBUG nuraft_mp_context_destroy: stopping listener" << std::endl;
                mp_ctx->listener->stop();
            }
            std::cerr << "DEBUG nuraft_mp_context_destroy: deleting mp_ctx" << std::endl;
            delete mp_ctx;
            std::cerr << "DEBUG nuraft_mp_context_destroy: done" << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "DEBUG nuraft_mp_context_destroy: exception: " << e.what() << std::endl;
        } catch (...) {
            std::cerr << "DEBUG nuraft_mp_context_destroy: unknown exception" << std::endl;
        }
    }
}

void nuraft_mp_context_set_group_id(void* ctx, const char* group_id_bytes) {
    if (ctx && group_id_bytes) {
        auto* mp_ctx = static_cast<mp_context_t*>(ctx);
        std::memcpy(mp_ctx->group_id_bytes, group_id_bytes, 16);
        mp_ctx->client_factory->set_group_id(group_id_bytes);
        mp_ctx->listener->set_group_id(group_id_bytes);
    }
}

// =============================================================================
// Listener Helpers (for message delivery)
// =============================================================================

void* nuraft_mp_get_listener(void* mp_context) {
    if (!mp_context) return nullptr;
    auto* mp_ctx = static_cast<mp_context_t*>(mp_context);
    return new ptr<mp_rpc_listener>(mp_ctx->listener);
}

void nuraft_mp_listener_set_src_node_id(void* listener_ptr, int32_t src_node_id) {
    if (!listener_ptr) return;
    auto& listener_sp = *static_cast<ptr<mp_rpc_listener>*>(listener_ptr);
    listener_sp->set_src_node_id(src_node_id);
}

void nuraft_mp_listener_set_send_response_callback(void* listener_ptr, void* ctx, nuraft_send_cb cb) {
    if (!listener_ptr) return;
    auto& listener_sp = *static_cast<ptr<mp_rpc_listener>*>(listener_ptr);
    listener_sp->set_send_response_callback(ctx, cb);
}

void nuraft_mp_listener_destroy(void* listener_ptr) {
    if (listener_ptr) {
        delete static_cast<ptr<mp_rpc_listener>*>(listener_ptr);
    }
}

// =============================================================================
// Message Delivery
// =============================================================================

void nuraft_mp_deliver_message(void* mp_context, void* server,
                               const char* msg_data, size_t msg_len) {
    if (!mp_context || !msg_data || msg_len == 0) {
        std::cerr << "DEBUG deliver_message: invalid params" << std::endl;
        return;
    }

    auto* mp_ctx = static_cast<mp_context_t*>(mp_context);
    auto* wrapper = static_cast<server_wrapper*>(server);

    std::cerr << "DEBUG deliver_message: msg_len=" << msg_len 
              << " server_id=" << mp_ctx->server_id << std::endl;

    // Dump first 8 bytes of message for debugging
    std::cerr << "DEBUG deliver_message: first 8 bytes: ";
    for (size_t i = 0; i < 8 && i < msg_len; i++) {
        std::cerr << std::hex << (int)(unsigned char)msg_data[i] << " ";
    }
    std::cerr << std::dec << std::endl;

    // Deserialize message - allocate buffer and copy data
    // IMPORTANT: buffer::alloc creates buffer with given size, and pos=0
    ptr<buffer> msg_buf = buffer::alloc(msg_len);
    std::memcpy(msg_buf->data_begin(), msg_data, msg_len);
    msg_buf->pos(0);  // Reset position for reading

    std::cerr << "DEBUG deliver_message: buffer allocated, size=" << msg_buf->size() 
              << " pos=" << msg_buf->pos() << std::endl;

    msg_type type = get_msg_type(*msg_buf);
    std::cerr << "DEBUG deliver_message: msg_type=" << static_cast<int>(type) << std::endl;

    // Build group_id hex for handler lookup
    std::string gid_hex;
    for (int i = 0; i < 16; i++) {
        char hex[3];
        sprintf(hex, "%02x", (unsigned char)mp_ctx->group_id_bytes[i]);
        gid_hex += hex;
    }

    if (is_response_type(type)) {
        // It's a response - match to pending handler
        ptr<resp_msg> resp = deserialize_resp_msg(*msg_buf);

        // Key: (groupId_hex, our_server_id, responder_id)
        auto key = std::make_tuple(gid_hex, mp_ctx->server_id, resp->get_src());

        rpc_handler handler;
        {
            std::lock_guard<std::mutex> lock(g_handlers_lock);
            auto it = g_pending_handlers.find(key);
            if (it != g_pending_handlers.end()) {
                handler = it->second;
                g_pending_handlers.erase(it);
            }
        }

        if (handler) {
            ptr<rpc_exception> no_err;
            handler(resp, no_err);
        }
    } else {
        // It's a request - process and send response
        ptr<req_msg> req = deserialize_req_msg(*msg_buf);

        raft_server* srv = wrapper ? wrapper->server.get() : nullptr;
        if (!srv) return;

        ptr<resp_msg> resp = raft_server_access::call_process_req(srv, *req);

        if (resp) {
            // Send response via listener's callback
            auto& listener = mp_ctx->listener;
            if (listener && listener->has_send_response_callback()) {
                ptr<buffer> resp_buf = serialize_resp_msg(resp);
                const char* resp_data = reinterpret_cast<const char*>(resp_buf->data_begin());
                size_t resp_len = resp_buf->size();

                listener->get_send_resp_cb()(
                    listener->get_send_resp_ctx(),
                    listener->get_group_id(),
                    resp->get_src(),   // Our node ID
                    resp->get_dst(),   // Target node ID
                    resp_data,
                    resp_len
                );
            }
        }
    }
}

bool nuraft_mp_invoke_timer(void* mp_context, int32_t timer_id) {
    if (!mp_context) {
        std::cerr << "DEBUG invoke_timer: mp_context is null" << std::endl;
        return false;
    }

    auto* mp_ctx = static_cast<mp_context_t*>(mp_context);
    std::cerr << "DEBUG invoke_timer: timer_id=" << timer_id 
              << " mp_ctx=" << mp_context
              << " timer=" << mp_ctx->timer.get() << std::endl;
    if (mp_ctx->timer) {
        bool result = mp_ctx->timer->invoke_timer(timer_id);
        std::cerr << "DEBUG invoke_timer: result=" << result << std::endl;
        return result;
    }
    std::cerr << "DEBUG invoke_timer: timer is null!" << std::endl;
    return false;
}

// =============================================================================
// Raft Server
// =============================================================================

void* nuraft_server_create(
    void* mp_context,
    void* sm,
    void* smgr,
    void* params,
    nuraft_event_cb event_cb,
    void* event_ctx,
    int skip_initial_election
) {
    if (!mp_context || !sm || !smgr || !params) return nullptr;

    auto* mp_ctx = static_cast<mp_context_t*>(mp_context);
    auto& sm_sp = *static_cast<ptr<callback_state_machine>*>(sm);
    auto& smgr_sp = *static_cast<ptr<dynamic_state_mgr>*>(smgr);
    auto* rp = static_cast<raft_params*>(params);

    // Create context with custom RPC components
    context* ctx = new context(
        std::static_pointer_cast<state_mgr>(smgr_sp),
        std::static_pointer_cast<state_machine>(sm_sp),
        mp_ctx->listener,
        nullptr, // logger
        mp_ctx->client_factory,
        mp_ctx->timer,
        *rp
    );

    // Wire event callback
    if (event_cb) {
        ctx->set_cb_func([event_cb, event_ctx](cb_func::Type type, cb_func::Param* param) -> cb_func::ReturnCode {
            if (type == cb_func::BecomeLeader || type == cb_func::BecomeFollower) {
                event_cb(event_ctx, static_cast<int>(type), param->leaderId,
                         param->ctx ? *static_cast<uint64_t*>(param->ctx) : 0);
            }
            return cb_func::Ok;
        });
    }

    // Create raft_server
    raft_server::init_options init_opt;
    init_opt.start_server_in_constructor_ = false;

    auto server = cs_new<raft_server>(ctx, init_opt);
    if (!server) {
        delete ctx;
        return nullptr;
    }

    // Start server
    server->start_server(skip_initial_election != 0);

    // Wrap
    auto* wrapper = new server_wrapper();
    wrapper->server = server;
    wrapper->raft_ctx = nullptr; // server owns it
    // Note: mp_ctx is owned by Nim (rpcContext), not stored here
    wrapper->sm = sm_sp;
    wrapper->smgr = smgr_sp;

    return wrapper;
}

void nuraft_server_destroy(void* server) {
    if (!server) return;
    auto* wrapper = static_cast<server_wrapper*>(server);
    wrapper->server.reset();
    wrapper->raft_ctx.reset();
    // Note: mp_ctx is owned by Nim, destroyed separately via nuraftMpContextDestroy
    wrapper->sm.reset();
    wrapper->smgr.reset();
    delete wrapper;
}

void nuraft_server_shutdown(void* server) {
    if (!server) return;
    auto* wrapper = static_cast<server_wrapper*>(server);
    if (wrapper->server) {
        wrapper->server->shutdown();
    }
}

bool nuraft_server_is_leader(void* server) {
    if (!server) return false;
    auto* wrapper = static_cast<server_wrapper*>(server);
    return wrapper->server && wrapper->server->is_leader();
}

int32_t nuraft_server_get_leader(void* server) {
    if (!server) return -1;
    auto* wrapper = static_cast<server_wrapper*>(server);
    return wrapper->server ? wrapper->server->get_leader() : -1;
}

int32_t nuraft_server_get_id(void* server) {
    if (!server) return -1;
    auto* wrapper = static_cast<server_wrapper*>(server);
    return wrapper->server ? wrapper->server->get_id() : -1;
}

uint64_t nuraft_server_get_term(void* server) {
    if (!server) return 0;
    auto* wrapper = static_cast<server_wrapper*>(server);
    return wrapper->server ? wrapper->server->get_term() : 0;
}

uint64_t nuraft_server_get_committed_log_idx(void* server) {
    if (!server) return 0;
    auto* wrapper = static_cast<server_wrapper*>(server);
    return wrapper->server ? wrapper->server->get_committed_log_idx() : 0;
}

uint64_t nuraft_server_get_last_log_idx(void* server) {
    if (!server) return 0;
    auto* wrapper = static_cast<server_wrapper*>(server);
    return wrapper->server ? wrapper->server->get_last_log_idx() : 0;
}

bool nuraft_server_is_initialized(void* server) {
    if (!server) return false;
    auto* wrapper = static_cast<server_wrapper*>(server);
    return wrapper->server && wrapper->server->is_initialized();
}

int nuraft_server_append_entry(void* server, const char* data, size_t len, uint64_t* out_log_idx) {
    if (!server || !data || len == 0) return -1;
    auto* wrapper = static_cast<server_wrapper*>(server);
    if (!wrapper->server) return -1;

    ptr<buffer> log = buffer::alloc(len);
    log->put_raw(reinterpret_cast<const byte*>(data), len);
    log->pos(0);

    auto result = wrapper->server->append_entries({log});
    if (!result) return -1;

    if (result->get_accepted()) {
        if (out_log_idx) {
            auto buf = result->get();
            if (buf) {
                *out_log_idx = buf->get_ulong();
            } else {
                *out_log_idx = 0;
            }
        }
        return 0;
    }

    return static_cast<int>(result->get_result_code());
}

int nuraft_server_add_srv(void* server, int32_t srv_id, const char* endpoint) {
    if (!server || !endpoint) return -1;
    auto* wrapper = static_cast<server_wrapper*>(server);
    if (!wrapper->server) return -1;

    srv_config sc(srv_id, std::string(endpoint));
    auto result = wrapper->server->add_srv(sc);
    if (!result) return -1;

    return result->get_accepted() ? 0 : static_cast<int>(result->get_result_code());
}

int nuraft_server_remove_srv(void* server, int32_t srv_id) {
    if (!server) return -1;
    auto* wrapper = static_cast<server_wrapper*>(server);
    if (!wrapper->server) return -1;

    auto result = wrapper->server->remove_srv(srv_id);
    if (!result) return -1;

    return result->get_accepted() ? 0 : static_cast<int>(result->get_result_code());
}

int nuraft_server_set_priority(void* server, int32_t srv_id, int32_t priority) {
    if (!server) return -1;
    auto* wrapper = static_cast<server_wrapper*>(server);
    if (!wrapper->server) return -1;

    auto result = wrapper->server->set_priority(srv_id, priority);
    return (result == raft_server::PrioritySetResult::SET ||
            result == raft_server::PrioritySetResult::BROADCAST) ? 0 : -1;
}

void nuraft_server_yield_leadership(void* server, bool immediate, int32_t successor_id) {
    if (!server) return;
    auto* wrapper = static_cast<server_wrapper*>(server);
    if (wrapper->server) {
        wrapper->server->yield_leadership(immediate, successor_id);
    }
}

} // extern "C"