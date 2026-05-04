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
#include <fstream>
#include <sys/stat.h>

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
// State Manager (In-memory with dynamic config + persistent state)
// =============================================================================
// 
// State persistence: srv_state (term, voted_for) is saved to a file on disk.
// This ensures restarted nodes maintain their term and don't trigger
// unnecessary elections when rejoining a cluster.
//
// File format (binary, 16 bytes):
//   [term:8 bytes BE][voted_for:4 bytes BE][padding:4 bytes]
//
// The catching_up flag is NOT persisted - it's set fresh on each restart
// based on whether the node is joining as a new member.

class dynamic_state_mgr : public state_mgr {
public:
    // Constructor with optional state file path for persistence
    dynamic_state_mgr(int32 my_id, const std::string& my_endpoint,
                      const std::vector<std::pair<int32, std::string>>& servers,
                      bool catching_up = false,
                      const std::string& state_file_path = "")
        : my_id_(my_id), my_endpoint_(my_endpoint),
          cur_log_store_(cs_new<inmem_log_store>()),
          state_file_path_(state_file_path),
          config_change_cb_(nullptr), config_change_ctx_(nullptr),
          quorum_update_cb_(nullptr), quorum_update_ctx_(nullptr),
          raft_server_ptr_(nullptr) {
        saved_config_ = cs_new<cluster_config>();
        
        // Initialize state - load from file if path provided and file exists
        saved_state_ = cs_new<srv_state>();
        if (!state_file_path_.empty()) {
            load_state_from_file();
        }
        
        // Apply catching_up flag (not persisted, set fresh each restart)
        if (catching_up) {
            saved_state_->set_catching_up(true);
            saved_state_->allow_election_timer(false);
        }
        
        std::cerr << "[NuRaft] dynamic_state_mgr constructor: my_id=" << my_id_
                  << " servers=" << servers.size()
                  << " catching_up=" << catching_up
                  << " state_file=" << (state_file_path_.empty() ? "(none)" : state_file_path_)
                  << " loaded_term=" << saved_state_->get_term()
                  << " loaded_voted_for=" << saved_state_->get_voted_for()
                  << std::endl;
        for (auto& kv : servers) {
            std::cerr << "[NuRaft]   - server " << kv.first << " endpoint=" << kv.second << std::endl;
            auto sc = cs_new<srv_config>(kv.first, kv.second);
            saved_config_->get_servers().push_back(sc);
        }
    }

    ~dynamic_state_mgr() {}

    void set_raft_server(void* server_ptr) {
        raft_server_ptr_ = server_ptr;
    }

    void set_quorum_update_callback(void* ctx, nuraft_quorum_update_cb cb) {
        quorum_update_ctx_ = ctx;
        quorum_update_cb_ = cb;
    }

    ptr<cluster_config> load_config() override {
        std::cerr << "[NuRaft] load_config: my_id=" << my_id_
                  << " returning config with "
                  << saved_config_->get_servers().size() << " servers:" << std::endl;
        for (auto& srv : saved_config_->get_servers()) {
            std::cerr << "[NuRaft]   - server " << srv->get_id()
                      << " endpoint=" << srv->get_endpoint() << std::endl;
        }
        return saved_config_;
    }

    void save_config(const cluster_config& config) override {
        ptr<buffer> buf = config.serialize();
        saved_config_ = cluster_config::deserialize(*buf);
        
        // Check if the new config includes this server
        bool includes_self = false;
        for (auto& srv : saved_config_->get_servers()) {
            if (srv->get_id() == my_id_) {
                includes_self = true;
                break;
            }
        }
        
        size_t num_servers = saved_config_->get_servers().size();
        std::cerr << "[NuRaft] save_config: my_id=" << my_id_
                  << " new_config_servers=" << num_servers
                  << " includes_self=" << includes_self << std::endl;

        // Notify Nim about each server in the new config
        // This is critical for follower nodes to learn about new peers
        // when the leader adds them via add_srv
        if (config_change_cb_) {
            std::cerr << "[NuRaft] save_config: calling callback for "
                      << num_servers << " servers" << std::endl;
            for (auto& srv : saved_config_->get_servers()) {
                int32_t server_id = srv->get_id();
                const std::string& endpoint = srv->get_endpoint();
                std::cerr << "[NuRaft] save_config: server " << server_id
                          << " endpoint=" << endpoint << std::endl;
                // Call Nim callback to update peerInfo table
                config_change_cb_(config_change_ctx_, server_id, endpoint.c_str());
            }
        }
        
        // Update quorum based on new server count
        if (quorum_update_cb_ && num_servers > 0) {
            int32_t majority = (int32_t)(num_servers / 2) + 1;
            int32_t quorum_size = majority;
            std::cerr << "[NuRaft] save_config: calling quorum update callback, "
                      << "num_servers=" << num_servers
                      << " majority=" << majority
                      << " quorum_size=" << quorum_size
                      << " (quorum_for_election=" << (quorum_size - 1) << ")" << std::endl;
            quorum_update_cb_(quorum_update_ctx_, my_id_, quorum_size);
        }
    }

    void save_state(const srv_state& state) override {
        ptr<buffer> buf = state.serialize();
        saved_state_ = srv_state::deserialize(*buf);
        std::cerr << "[NuRaft] save_state: my_id=" << my_id_
                  << " term=" << saved_state_->get_term()
                  << " voted_for=" << saved_state_->get_voted_for()
                  << " catching_up=" << (saved_state_->is_catching_up() ? 1 : 0)
                  << " election_timer_allowed=" << (saved_state_->is_election_timer_allowed() ? 1 : 0)
                  << std::endl;
        
        // Persist state to file if path is set
        if (!state_file_path_.empty()) {
            save_state_to_file();
        }
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

    void set_config_change_callback(void* ctx, nuraft_config_change_cb cb) {
        config_change_ctx_ = ctx;
        config_change_cb_ = cb;
    }

private:
    int32 my_id_;
    std::string my_endpoint_;
    ptr<inmem_log_store> cur_log_store_;
    ptr<cluster_config> saved_config_;
    ptr<srv_state> saved_state_;
    std::string state_file_path_;  // Path for persistent state file
    nuraft_config_change_cb config_change_cb_;
    void* config_change_ctx_;
    nuraft_quorum_update_cb quorum_update_cb_;
    void* quorum_update_ctx_;
    void* raft_server_ptr_;
    
    // Load srv_state from file (term, voted_for)
    // File format: [term:8B BE][voted_for:4B BE][padding:4B] = 16 bytes
    void load_state_from_file() {
        std::ifstream file(state_file_path_, std::ios::binary);
        if (!file.is_open()) {
            std::cerr << "[NuRaft] load_state_from_file: file not found or cannot open: "
                      << state_file_path_ << ", using default state (term=0, voted_for=-1)"
                      << std::endl;
            saved_state_->set_term(0);
            saved_state_->set_voted_for(-1);
            return;
        }
        
        char buf[16];
        file.read(buf, 16);
        if (file.gcount() != 16) {
            std::cerr << "[NuRaft] load_state_from_file: incomplete file (read "
                      << file.gcount() << " bytes), using default state" << std::endl;
            saved_state_->set_term(0);
            saved_state_->set_voted_for(-1);
            return;
        }
        
        // Parse term (8 bytes big-endian)
        uint64_t term = 0;
        for (int i = 0; i < 8; i++) {
            term = (term << 8) | (uint8_t)buf[i];
        }
        
        // Parse voted_for (4 bytes big-endian)
        int32_t voted_for = 0;
        for (int i = 8; i < 12; i++) {
            voted_for = (voted_for << 8) | (int8_t)buf[i];
        }
        
        saved_state_->set_term(term);
        saved_state_->set_voted_for(voted_for);
        
        std::cerr << "[NuRaft] load_state_from_file: loaded from " << state_file_path_
                  << " term=" << term << " voted_for=" << voted_for << std::endl;
    }
    
    // Save srv_state to file (term, voted_for)
    void save_state_to_file() {
        // Ensure parent directory exists
        size_t last_slash = state_file_path_.find_last_of('/');
        if (last_slash != std::string::npos) {
            std::string dir = state_file_path_.substr(0, last_slash);
            mkdir(dir.c_str(), 0755);  // Ignore error if already exists
        }
        
        std::ofstream file(state_file_path_, std::ios::binary);
        if (!file.is_open()) {
            std::cerr << "[NuRaft] save_state_to_file: ERROR cannot open file: "
                      << state_file_path_ << std::endl;
            return;
        }
        
        char buf[16];
        
        // Write term (8 bytes big-endian)
        uint64_t term = saved_state_->get_term();
        for (int i = 7; i >= 0; i--) {
            buf[i] = (char)(term & 0xFF);
            term >>= 8;
        }
        
        // Write voted_for (4 bytes big-endian)
        int32_t voted_for = saved_state_->get_voted_for();
        for (int i = 11; i >= 8; i--) {
            buf[i] = (char)(voted_for & 0xFF);
            voted_for >>= 8;
        }
        
        // Padding (4 bytes)
        for (int i = 12; i < 16; i++) {
            buf[i] = 0;
        }
        
        file.write(buf, 16);
        file.flush();
        
        std::cerr << "[NuRaft] save_state_to_file: saved to " << state_file_path_
                  << " term=" << saved_state_->get_term()
                  << " voted_for=" << saved_state_->get_voted_for() << std::endl;
    }
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

    // Update group ID (called when factory's group ID changes)
    void set_group_id(const char* group_id_bytes) {
        if (group_id_bytes) {
            std::memcpy(group_id_bytes_, group_id_bytes, 16);
        }
    }

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
            // Update all existing clients with the new group_id
            std::lock_guard<std::mutex> lock(clients_lock_);
            for (auto& pair : clients_) {
                // Each client has its own copy of group_id_bytes_
                // We need to update it via the set_group_id method
                pair.second->set_group_id(group_id_bytes);
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

// Forward declare mp_timer for mp_context_t
class mp_timer;

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
// Timer - delegates to Nim callbacks
// =============================================================================

class mp_timer : public delayed_task_scheduler {
public:
    mp_timer(mp_context_t* parent_ctx, void* timer_ctx, nuraft_schedule_timer_cb schedule_cb, nuraft_cancel_timer_cb cancel_cb)
        : parent_ctx_(parent_ctx), timer_ctx_(timer_ctx), schedule_cb_(schedule_cb), cancel_cb_(cancel_cb),
          next_timer_id_(1), stopped_(false) {}

    ~mp_timer() { stop(); }

    void schedule(ptr<delayed_task>& task, int32 milliseconds) override {
        if (stopped_.load()) return;

        // CRITICAL: Reset the cancelled_ flag before scheduling.
        // NuRaft reuses the same task object (election_task_) when
        // restart_election_timer() is called. The cancel() method
        // sets cancelled_=true, and if we don't reset it here,
        // execute() will skip calling exec() even for the newly
        // scheduled timer. This is what asio_service::schedule() does.
        task->reset();

        int32_t timer_id = next_timer_id_.fetch_add(1);
        {
            std::lock_guard<std::mutex> lock(tasks_lock_);
            tasks_[timer_id] = task;
        }

        // Log timer type for debugging (include group_id)
        std::string type_str = "unknown";
        if (task) {
            int32 ttype = task->get_type();
            if (ttype == 1) type_str = "election";  // timer_task_type::election_timer
            else if (ttype == 2) type_str = "heartbeat";  // timer_task_type::heartbeat_timer
        }
        // Format group_id as hex string (last 4 bytes for brevity)
        std::string group_id_str = "unknown";
        if (parent_ctx_) {
            char hex_buf[9];
            for (int i = 12; i < 16; i++) {
                sprintf(hex_buf + (i-12)*2, "%02x", (unsigned char)parent_ctx_->group_id_bytes[i]);
            }
            group_id_str = std::string(hex_buf);
        }
        std::cerr << "[NuRaft] schedule_timer: server_id=" << (parent_ctx_ ? parent_ctx_->server_id : -1)
                  << " group_id=" << group_id_str
                  << " timer_id=" << timer_id << " type=" << type_str << " delay_ms=" << milliseconds << std::endl;

        if (schedule_cb_) {
            schedule_cb_(timer_ctx_, timer_id, milliseconds);
        }
    }

    void cancel_impl(ptr<delayed_task>& task) override {
        std::lock_guard<std::mutex> lock(tasks_lock_);
        for (auto it = tasks_.begin(); it != tasks_.end(); ++it) {
            if (it->second == task) {
                // Format group_id as hex string (last 4 bytes for brevity)
                std::string group_id_str = "unknown";
                if (parent_ctx_) {
                    char hex_buf[9];
                    for (int i = 12; i < 16; i++) {
                        sprintf(hex_buf + (i-12)*2, "%02x", (unsigned char)parent_ctx_->group_id_bytes[i]);
                    }
                    group_id_str = std::string(hex_buf);
                }
                std::cerr << "[NuRaft] cancel_timer: server_id=" << (parent_ctx_ ? parent_ctx_->server_id : -1)
                          << " group_id=" << group_id_str
                          << " timer_id=" << it->first << std::endl;
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
                // Format group_id as hex string (last 4 bytes for brevity)
                std::string group_id_str = "unknown";
                if (parent_ctx_) {
                    char hex_buf[9];
                    for (int i = 12; i < 16; i++) {
                        sprintf(hex_buf + (i-12)*2, "%02x", (unsigned char)parent_ctx_->group_id_bytes[i]);
                    }
                    group_id_str = std::string(hex_buf);
                }
                std::cerr << "[NuRaft] invoke_timer: server_id=" << (parent_ctx_ ? parent_ctx_->server_id : -1)
                          << " group_id=" << group_id_str
                          << " timer_id=" << timer_id
                          << " NOT FOUND (cancelled or expired)" << std::endl;
                return false;
            }
            task = it->second;
            tasks_.erase(it);
        }
        if (task) {
            int32 ttype = task->get_type();
            std::string type_str = "unknown";
            if (ttype == 1) type_str = "election";
            else if (ttype == 2) type_str = "heartbeat";
            
            // Format group_id as hex string (last 4 bytes for brevity)
            std::string group_id_str = "unknown";
            if (parent_ctx_) {
                char hex_buf[9];
                for (int i = 12; i < 16; i++) {
                    sprintf(hex_buf + (i-12)*2, "%02x", (unsigned char)parent_ctx_->group_id_bytes[i]);
                }
                group_id_str = std::string(hex_buf);
            }
            
            std::cerr << std::unitbuf;  // Force unbuffered
            
            // Count active timers BEFORE execution
            size_t count_before = 0;
            {
                std::lock_guard<std::mutex> lock(tasks_lock_);
                count_before = tasks_.size();
            }
            
            std::cerr << "[NuRaft shim] invoke_timer START: server_id=" << (parent_ctx_ ? parent_ctx_->server_id : -1)
                      << " group_id=" << group_id_str
                      << " ctx_ptr=" << (void*)parent_ctx_
                      << " timer_id=" << timer_id << " type=" << type_str 
                      << " active_before=" << count_before << std::endl;
            
            // Call execute() - internally checks cancelled_ flag
            task->execute();
            
            // Confirm that execute() was actually called
            std::cerr << "[NuRaft shim] invoke_timer EXECUTED: server_id=" << (parent_ctx_ ? parent_ctx_->server_id : -1)
                      << " group_id=" << group_id_str
                      << " timer_id=" << timer_id << std::endl;
            
            // Note: if task was cancelled, execute() returns silently without calling exec()
            // This would explain why HE_TIMEOUT doesn appear
            
            // Count active timers AFTER execution to see if new timer was scheduled
            size_t count_after = 0;
            {
                std::lock_guard<std::mutex> lock(tasks_lock_);
                count_after = tasks_.size();
            }
            
            std::cerr << "[NuRaft shim] invoke_timer END: server_id=" << (parent_ctx_ ? parent_ctx_->server_id : -1)
                      << " group_id=" << group_id_str
                      << " ctx_ptr=" << (void*)parent_ctx_
                      << " timer_id=" << timer_id << " type=" << type_str << " active_after=" << count_after << std::endl;
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
    mp_context_t* parent_ctx_;  // Pointer to parent context for server_id access
    void* timer_ctx_;
    nuraft_schedule_timer_cb schedule_cb_;
    nuraft_cancel_timer_cb cancel_cb_;
    std::atomic<int32_t> next_timer_id_;
    std::atomic<bool> stopped_;
    std::mutex tasks_lock_;
    std::map<int32_t, ptr<delayed_task>> tasks_;
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

void nuraft_params_set_custom_election_quorum_size(void* params, int32_t size) {
    if (!params) return;
    static_cast<raft_params*>(params)->custom_election_quorum_size_ = size;
    fprintf(stderr, "[NuRaft Shim] Set custom election quorum size to %d (actual quorum will be %d)\n",
            size, size > 0 ? size - 1 : 0);
}

// =============================================================================
// Limits (global settings)
// =============================================================================

void nuraft_limits_set_busy_connection_limit(int32_t limit) {
    // Get current limits, modify busy_connection_limit, and set back
    raft_server::limits current = raft_server::get_raft_limits();
    current.busy_connection_limit_ = limit;
    raft_server::set_raft_limits(current);
    fprintf(stderr, "[NuRaft Shim] Set busy_connection_limit to %d\n", limit);
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
    auto smgr = cs_new<dynamic_state_mgr>(my_server_id, std::string(my_endpoint), servers, false);
    return new ptr<dynamic_state_mgr>(smgr);
}

void* nuraft_smgr_create_with_catching_up(int32_t my_server_id, const char* my_endpoint,
                                           int32_t num_servers, const int32_t* server_ids,
                                           const char** endpoints, bool catching_up) {
    std::vector<std::pair<int32, std::string>> servers;
    for (int i = 0; i < num_servers; i++) {
        servers.push_back({server_ids[i], std::string(endpoints[i])});
    }
    auto smgr = cs_new<dynamic_state_mgr>(my_server_id, std::string(my_endpoint), servers, catching_up);
    return new ptr<dynamic_state_mgr>(smgr);
}

void* nuraft_smgr_create_with_persistence(int32_t my_server_id, const char* my_endpoint,
                                           int32_t num_servers, const int32_t* server_ids,
                                           const char** endpoints, bool catching_up,
                                           const char* state_file_path) {
    std::vector<std::pair<int32, std::string>> servers;
    for (int i = 0; i < num_servers; i++) {
        servers.push_back({server_ids[i], std::string(endpoints[i])});
    }
    std::string path_str = state_file_path ? std::string(state_file_path) : "";
    auto smgr = cs_new<dynamic_state_mgr>(my_server_id, std::string(my_endpoint),
                                          servers, catching_up, path_str);
    return new ptr<dynamic_state_mgr>(smgr);
}

void nuraft_smgr_destroy(void* smgr) {
    if (smgr) {
        delete static_cast<ptr<dynamic_state_mgr>*>(smgr);
    }
}

void nuraft_smgr_set_config_cb(void* smgr, void* ctx, nuraft_config_change_cb cb) {
    if (!smgr) return;
    auto& sp = *static_cast<ptr<dynamic_state_mgr>*>(smgr);
    sp->set_config_change_callback(ctx, cb);
}

void nuraft_smgr_set_quorum_cb(void* smgr, void* ctx, nuraft_quorum_update_cb cb) {
    if (!smgr) return;
    auto& sp = *static_cast<ptr<dynamic_state_mgr>*>(smgr);
    sp->set_quorum_update_callback(ctx, cb);
}

void nuraft_smgr_set_raft_server(void* smgr, void* server) {
    if (!smgr) return;
    auto& sp = *static_cast<ptr<dynamic_state_mgr>*>(smgr);
    sp->set_raft_server(server);
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
    ctx->timer = cs_new<mp_timer>(ctx, ctx, schedule_cb, cancel_cb);
    return ctx;
}

void nuraft_mp_context_destroy(void* ctx) {
    if (ctx) {
        auto* mp_ctx = static_cast<mp_context_t*>(ctx);
        try {
            if (mp_ctx->client_factory) {
                mp_ctx->client_factory->abandon_all();
            }
            if (mp_ctx->timer) {
                mp_ctx->timer->stop();
            }
            if (mp_ctx->listener) {
                mp_ctx->listener->stop();
            }
            delete mp_ctx;
        } catch (const std::exception& e) {
            std::cerr << "nuraft_mp_context_destroy: exception: " << e.what() << std::endl;
        } catch (...) {
            std::cerr << "nuraft_mp_context_destroy: unknown exception" << std::endl;
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
        std::cerr << "[shim] deliver_message: early return (null or zero len)" << std::endl;
        return;
    }

    auto* mp_ctx = static_cast<mp_context_t*>(mp_context);
    auto* wrapper = static_cast<server_wrapper*>(server);

    // Build group_id hex for logging
    std::string gid_hex;
    for (int i = 0; i < 16; i++) {
        char hex[3];
        sprintf(hex, "%02x", (unsigned char)mp_ctx->group_id_bytes[i]);
        gid_hex += hex;
    }
    std::string gid_short = gid_hex.substr(24, 8);  // Last 4 bytes for brevity

    // Deserialize message - allocate buffer and copy data
    // IMPORTANT: buffer::alloc creates buffer with given size, and pos=0
    ptr<buffer> msg_buf = buffer::alloc(msg_len);
    std::memcpy(msg_buf->data_begin(), msg_data, msg_len);
    msg_buf->pos(0);  // Reset position for reading

    msg_type type = get_msg_type(*msg_buf);

    std::cerr << "[shim] deliver_message: server_id=" << mp_ctx->server_id
              << " group=" << gid_short << " msg_type=" << static_cast<int>(type)
              << " len=" << msg_len << std::endl;

    if (is_response_type(type)) {
        // It's a response - match to pending handler
        ptr<resp_msg> resp = deserialize_resp_msg(*msg_buf);

        std::cerr << "[shim] deliver_message RESPONSE: src=" << resp->get_src()
                  << " dst=" << resp->get_dst() << " accepted=" << resp->get_accepted()
                  << " term=" << resp->get_term() << std::endl;

        // Key: (groupId_hex, our_server_id, responder_id)
        auto key = std::make_tuple(gid_hex, mp_ctx->server_id, resp->get_src());

        std::cerr << "[shim] deliver_message: lookup key=(" << gid_short << ", " << mp_ctx->server_id << ", " << resp->get_src() << ")" << std::endl;

        // Dump all pending handlers for debugging
        {
            std::lock_guard<std::mutex> lock(g_handlers_lock);
            std::cerr << "[shim] deliver_message: pending handlers count=" << g_pending_handlers.size() << std::endl;
            for (const auto& entry : g_pending_handlers) {
                const auto& k = entry.first;
                std::string k_gid = std::get<0>(k).substr(24, 8);
                std::cerr << "[shim]   handler: (" << k_gid << ", " << std::get<1>(k) << ", " << std::get<2>(k) << ")" << std::endl;
            }
        }

        rpc_handler handler;
        {
            std::lock_guard<std::mutex> lock(g_handlers_lock);
            auto it = g_pending_handlers.find(key);
            if (it != g_pending_handlers.end()) {
                handler = it->second;
                g_pending_handlers.erase(it);
                std::cerr << "[shim] deliver_message: FOUND handler for key" << std::endl;
            } else {
                std::cerr << "[shim] deliver_message: NO handler found for key" << std::endl;
            }
        }

        if (handler) {
            std::cerr << "[shim] deliver_message: invoking handler" << std::endl;
            ptr<rpc_exception> no_err;
            handler(resp, no_err);
            std::cerr << "[shim] deliver_message: handler completed" << std::endl;
        }
    } else {
        // It's a request - process and send response
        ptr<req_msg> req = deserialize_req_msg(*msg_buf);

        std::cerr << "[shim] deliver_message REQUEST: src=" << req->get_src()
                  << " dst=" << req->get_dst() << " term=" << req->get_term()
                  << " last_log_idx=" << req->get_last_log_idx() << std::endl;

        raft_server* srv = wrapper ? wrapper->server.get() : nullptr;
        if (!srv) {
            std::cerr << "[shim] deliver_message: server is null, returning" << std::endl;
            return;
        }

        std::cerr << "[shim] deliver_message: calling process_req for msg_type=" 
                  << static_cast<int>(req->get_type()) << std::endl;

        ptr<resp_msg> resp = raft_server_access::call_process_req(srv, *req);

        std::cerr << "[shim] deliver_message: process_req returned, resp=" 
                  << (resp ? "non-null" : "null") << std::endl;

        if (resp) {
            std::cerr << "[shim] deliver_message: got response, sending via listener" << std::endl;
            // Send response via listener's callback
            auto& listener = mp_ctx->listener;
            if (listener && listener->has_send_response_callback()) {
                ptr<buffer> resp_buf = serialize_resp_msg(resp);
                const char* resp_data = reinterpret_cast<const char*>(resp_buf->data_begin());
                size_t resp_len = resp_buf->size();

                std::cerr << "[shim] deliver_message: response src=" << resp->get_src()
                          << " dst=" << resp->get_dst() << " accepted=" << resp->get_accepted() << std::endl;

                listener->get_send_resp_cb()(
                    listener->get_send_resp_ctx(),
                    listener->get_group_id(),
                    resp->get_src(),   // Our node ID
                    resp->get_dst(),   // Target node ID
                    resp_data,
                    resp_len
                );
                std::cerr << "[shim] deliver_message: response sent" << std::endl;
            } else {
                std::cerr << "[shim] deliver_message: no listener or callback" << std::endl;
            }
        }
    }
}

bool nuraft_mp_invoke_timer(void* mp_context, int32_t timer_id) {
    if (!mp_context) {
        return false;
    }

    auto* mp_ctx = static_cast<mp_context_t*>(mp_context);
    if (mp_ctx->timer) {
        bool result = mp_ctx->timer->invoke_timer(timer_id);
        return result;
    }
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

    // Log params values to trace what's being passed to raft_server
    std::cerr << "[NuRaft shim] nuraft_server_create: params="
              << " election_timeout_lower=" << rp->election_timeout_lower_bound_
              << " election_timeout_upper=" << rp->election_timeout_upper_bound_
              << " heartbeat_interval=" << rp->heart_beat_interval_ << std::endl;

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
            std::cerr << "[NuRaft cb_func] type=" << static_cast<int>(type)
                      << " (BecomeLeader=" << static_cast<int>(cb_func::BecomeLeader)
                      << ", BecomeFollower=" << static_cast<int>(cb_func::BecomeFollower) << ")"
                      << " leaderId=" << param->leaderId << std::endl;
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

    std::cerr << "[NuRaft shim] add_srv: srv_id=" << srv_id
              << " endpoint=" << endpoint << std::endl;

    srv_config sc(srv_id, std::string(endpoint));
    auto result = wrapper->server->add_srv(sc);
    if (!result) {
        std::cerr << "[NuRaft shim] add_srv: result is NULL!" << std::endl;
        return -1;
    }

    int rc = result->get_accepted() ? 0 : static_cast<int>(result->get_result_code());
    std::cerr << "[NuRaft shim] add_srv: accepted=" << result->get_accepted()
              << " result_code=" << rc << std::endl;
    return rc;
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

void nuraft_server_update_quorum(void* server, int32_t quorum_size) {
    if (!server) return;
    auto* wrapper = static_cast<server_wrapper*>(server);
    if (!wrapper->server || !wrapper->raft_ctx) return;
    
    // Get current params and modify only the quorum size
    ptr<raft_params> current_params = wrapper->raft_ctx->get_params();
    raft_params new_params(*current_params);  // Copy existing params (preserves hb_interval!)
    new_params.custom_election_quorum_size_ = quorum_size;
    
    std::cerr << "[NuRaft Shim] update_quorum: server_id=" << wrapper->server->get_id()
              << " quorum_size=" << quorum_size
              << " (actual quorum=" << (quorum_size > 0 ? quorum_size - 1 : 0) << ")"
              << " hb_interval=" << new_params.heart_beat_interval_ << std::endl;
    
    wrapper->server->update_params(new_params);
}

} // extern "C"