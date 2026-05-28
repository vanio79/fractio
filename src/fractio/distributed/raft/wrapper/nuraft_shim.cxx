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

#include <atomic>
#include <cstring>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>
#include <map>
#include <deque>
#include <functional>
#include <fstream>
#include <sys/stat.h>

using namespace nuraft;

// =============================================================================
// Callback-based Log Store (delegates all operations to Nim via C callbacks)
// =============================================================================

class callback_log_store : public log_store {
public:
    callback_log_store(void* ctx,
                       nuraft_log_append_cb append_cb,
                       nuraft_log_write_at_cb write_at_cb,
                       nuraft_log_get_cb get_cb,
                       nuraft_log_term_at_cb term_at_cb,
                       nuraft_log_next_slot_cb next_slot_cb,
                       nuraft_log_start_index_cb start_index_cb,
                       nuraft_log_pack_cb pack_cb,
                       nuraft_log_apply_pack_cb apply_pack_cb,
                       nuraft_log_compact_cb compact_cb,
                       nuraft_log_flush_cb flush_cb)
        : ctx_(ctx),
          append_cb_(append_cb),
          write_at_cb_(write_at_cb),
          get_cb_(get_cb),
          term_at_cb_(term_at_cb),
          next_slot_cb_(next_slot_cb),
          start_index_cb_(start_index_cb),
          pack_cb_(pack_cb),
          apply_pack_cb_(apply_pack_cb),
          compact_cb_(compact_cb),
          flush_cb_(flush_cb) {}

    ~callback_log_store() {}

    ulong next_slot() const override {
        if (next_slot_cb_) {
            return next_slot_cb_(ctx_);
        }
        return 1;
    }

    ulong start_index() const override {
        if (start_index_cb_) {
            return start_index_cb_(ctx_);
        }
        return 1;
    }

    ptr<log_entry> last_entry() const override {
        // Get the entry at next_slot() - 1
        ulong ns = next_slot();
        if (ns <= 1) {
            // Empty log: return a dummy entry with term=0 and null buffer
            return cs_new<log_entry>(0, buffer::alloc(0));
        }
        ulong idx = ns - 1;
        // We need to call get_cb, but it's not const-qualified in the callback.
        // Since log_store declares last_entry() as const, we const_cast the ctx.
        // This is safe because the Nim callback is thread-safe.
        if (get_cb_) {
            uint64_t term = 0;
            int32_t val_type = 0;
            // Allocate a reasonable buffer for the entry data
            size_t buf_cap = 64 * 1024;  // 64KB should be enough for any single entry
            char* buf = new char[buf_cap];
            size_t actual_len = get_cb_(ctx_, idx, &term, &val_type, buf, buf_cap);
            ptr<buffer> data_buf;
            if (actual_len > 0 && actual_len <= buf_cap) {
                data_buf = buffer::alloc(actual_len);
                std::memcpy(data_buf->data_begin(), buf, actual_len);
                data_buf->pos(0);
            } else {
                data_buf = buffer::alloc(0);
            }
            delete[] buf;
            return cs_new<log_entry>(term, data_buf, static_cast<log_val_type>(val_type));
        }
        return cs_new<log_entry>(0, buffer::alloc(0));
    }

    ulong append(ptr<log_entry>& entry) override {
        if (!append_cb_ || !entry) {
            return next_slot();
        }
        // Serialize entry data
        size_t data_len = 0;
        const char* data_ptr = nullptr;
        if (!entry->is_buf_null()) {
            buffer& buf = entry->get_buf();
            data_len = buf.size() - buf.pos();
            data_ptr = reinterpret_cast<const char*>(buf.data_begin() + buf.pos());
        }
        return append_cb_(ctx_, entry->get_term(), static_cast<int32_t>(entry->get_val_type()),
                          data_ptr ? data_ptr : "", data_len);
    }

    void write_at(ulong index, ptr<log_entry>& entry) override {
        if (!write_at_cb_ || !entry) return;
        size_t data_len = 0;
        const char* data_ptr = nullptr;
        if (!entry->is_buf_null()) {
            buffer& buf = entry->get_buf();
            data_len = buf.size() - buf.pos();
            data_ptr = reinterpret_cast<const char*>(buf.data_begin() + buf.pos());
        }
        write_at_cb_(ctx_, index, entry->get_term(), static_cast<int32_t>(entry->get_val_type()),
                     data_ptr ? data_ptr : "", data_len);
    }

    void end_of_append_batch(ulong start, ulong cnt) override {
        // No-op: Nim handles batching internally via WiscKey write batches
    }

    ptr<std::vector<ptr<log_entry>>> log_entries(ulong start, ulong end) override {
        auto result = cs_new<std::vector<ptr<log_entry>>>();
        result->reserve(end - start);
        for (ulong idx = start; idx < end; idx++) {
            ptr<log_entry> entry = entry_at(idx);
            if (entry) {
                result->push_back(entry);
            }
        }
        return result;
    }

    ptr<log_entry> entry_at(ulong index) override {
        if (!get_cb_) {
            return cs_new<log_entry>(0, buffer::alloc(0));
        }
        uint64_t term = 0;
        int32_t val_type = 0;
        size_t buf_cap = 64 * 1024;
        char* buf = new char[buf_cap];
        size_t actual_len = get_cb_(ctx_, index, &term, &val_type, buf, buf_cap);

        ptr<buffer> data_buf;
        if (actual_len > 0 && actual_len <= buf_cap) {
            data_buf = buffer::alloc(actual_len);
            std::memcpy(data_buf->data_begin(), buf, actual_len);
            data_buf->pos(0);
        } else {
            data_buf = buffer::alloc(0);
        }
        delete[] buf;
        return cs_new<log_entry>(term, data_buf, static_cast<log_val_type>(val_type));
    }

    ulong term_at(ulong index) override {
        if (term_at_cb_) {
            return term_at_cb_(ctx_, index);
        }
        return 0;
    }

    ptr<buffer> pack(ulong index, int32 cnt) override {
        if (!pack_cb_) {
            return buffer::alloc(0);
        }
        // Safety check: prevent unreasonably large allocations.
        // cnt is bounded by NuRaft's max_append_size (default 100, we set it to 100).
        // If cnt is negative, zero, or absurdly large, return empty buffer.
        // Each entry is at most 64KB, so the max reasonable buffer is ~6.4MB.
        if (cnt <= 0 || cnt > 10000) {
            // Log to stderr so it's visible in crash logs
            fprintf(stderr, "[NuRaft Shim] FATAL: pack() called with unreasonable cnt=%d at index=%lu, returning empty buffer. This is a bug in NuRaft's internal snapshot/replication logic.\n", cnt, (unsigned long)index);
            fflush(stderr);
            return buffer::alloc(0);
        }
        size_t cap = static_cast<size_t>(cnt) * 64 * 1024 + 1024;
        char* buf = new char[cap];
        size_t actual_len = pack_cb_(ctx_, index, cnt, buf, cap);

        ptr<buffer> result;
        if (actual_len > 0 && actual_len <= cap) {
            result = buffer::alloc(actual_len);
            std::memcpy(result->data_begin(), buf, actual_len);
            result->pos(0);
        } else {
            result = buffer::alloc(0);
        }
        delete[] buf;
        return result;
    }

    void apply_pack(ulong index, buffer& pack) override {
        if (!apply_pack_cb_) return;
        pack.pos(0);
        const char* data = reinterpret_cast<const char*>(pack.data_begin() + pack.pos());
        size_t len = pack.size() - pack.pos();
        apply_pack_cb_(ctx_, index, data, len);
    }

    bool compact(ulong last_log_index) override {
        if (compact_cb_) {
            return compact_cb_(ctx_, last_log_index) == 0;
        }
        return true;
    }

    bool flush() override {
        if (flush_cb_) {
            return flush_cb_(ctx_) == 0;
        }
        return true;
    }

    ulong last_durable_index() override {
        // For now, treat all written entries as durable immediately
        return next_slot() > 0 ? next_slot() - 1 : 0;
    }

private:
    void* ctx_;
    nuraft_log_append_cb append_cb_;
    nuraft_log_write_at_cb write_at_cb_;
    nuraft_log_get_cb get_cb_;
    nuraft_log_term_at_cb term_at_cb_;
    nuraft_log_next_slot_cb next_slot_cb_;
    nuraft_log_start_index_cb start_index_cb_;
    nuraft_log_pack_cb pack_cb_;
    nuraft_log_apply_pack_cb apply_pack_cb_;
    nuraft_log_compact_cb compact_cb_;
    nuraft_log_flush_cb flush_cb_;
};

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
// Key: (group_id_hex, src_node_id, dst_node_id) -> deque of (handler, timestamp_ms)
// Handlers older than PENDING_HANDLER_TIMEOUT_MS are purged to prevent unbounded
// growth from lost responses (network failures, node crashes).

#include <chrono>

static const int64_t PENDING_HANDLER_TIMEOUT_MS = 30000; // 30 seconds

struct PendingHandler {
    rpc_handler handler;
    int64_t enqueue_time_ms; // monotonic clock millis since epoch
};

static std::mutex g_handlers_lock;
static std::map<std::tuple<std::string, int32_t, int32_t>, std::deque<PendingHandler>> g_pending_handlers;

static int64_t current_time_ms() {
    auto now = std::chrono::steady_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
    return ms.count();
}

// Purge expired handlers from the global registry. Called under g_handlers_lock.
// IMPORTANT: Do NOT invoke expired handlers — they may reference destroyed
// raft_server instances (use-after-free). Just remove them.
static void purgeExpiredHandlersLocked() {
    int64_t now = current_time_ms();
    for (auto it = g_pending_handlers.begin(); it != g_pending_handlers.end(); ) {
        auto& deque = it->second;
        while (!deque.empty() && (now - deque.front().enqueue_time_ms) > PENDING_HANDLER_TIMEOUT_MS) {
            // Drop the expired handler without invoking it.
            // Invoking would call raft_server internals which may be freed,
            // causing SIGSEGV (use-after-free on raft_server ptr).
            deque.pop_front();
        }
        if (deque.empty()) {
            it = g_pending_handlers.erase(it);
        } else {
            ++it;
        }
    }
}

static void clearPendingHandlersForGroupAndServer(const std::string& gid_hex, int32_t server_id) {
    std::lock_guard<std::mutex> lock(g_handlers_lock);
    for (auto it = g_pending_handlers.begin(); it != g_pending_handlers.end(); ) {
        if (std::get<0>(it->first) == gid_hex && std::get<1>(it->first) == server_id) {
            it = g_pending_handlers.erase(it);
        } else {
            ++it;
        }
    }
}

// Public API: purge expired handlers (called periodically from Nim timer)
extern "C" void nuraft_purge_expired_handlers() {
    std::lock_guard<std::mutex> lock(g_handlers_lock);
    purgeExpiredHandlersLocked();
}

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
        if (commit_cb_) {
            const char* raw = reinterpret_cast<const char*>(data.data_begin());
            size_t len = data.size() - data.pos();
            commit_cb_(ctx_, log_idx, raw, len);
        }
        // Update last_committed_idx_ AFTER the callback completes so that
        // callers polling for commit advancement (e.g. proposeAndWait) don't
        // observe the new index while the state machine is still being updated.
        last_committed_idx_ = log_idx;
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
// Callback-based State Manager (delegates persistence to Nim via C callbacks)
// =============================================================================
//
// Dedicated state_mgr that uses Nim callbacks (backed by WiscKey) for all
// persistence. No file I/O. Separated from dynamic_state_mgr which retains
// file-based persistence for backward compatibility.
//
// Handles:
// - Raft state (term, voted_for, config_hwm) via state_save_cb / state_read_cb
// - Cluster config via config_save_cb / config_load_cb
// - Log store via an externally-provided callback_log_store
// - Config change notifications to Nim (config_change_cb)
// - Quorum updates to Nim (quorum_update_cb)
// - High water mark to reject stale config changes during log replay

class callback_state_mgr : public state_mgr {
public:
    callback_state_mgr(int32 my_id, const std::string& my_endpoint,
                       const std::vector<std::pair<int32, std::string>>& servers,
                       bool catching_up,
                       ptr<callback_log_store> log_store,
                       nuraft_state_save_cb state_save_cb,
                       nuraft_state_read_cb state_read_cb,
                       nuraft_config_save_cb config_save_cb,
                       nuraft_config_load_cb config_load_cb,
                       void* cb_ctx)
        : my_id_(my_id), my_endpoint_(my_endpoint),
          cur_log_store_(log_store),
          state_save_cb_(state_save_cb),
          state_read_cb_(state_read_cb),
          config_save_cb_(config_save_cb),
          config_load_cb_(config_load_cb),
          cb_ctx_(cb_ctx),
          config_change_cb_(nullptr), config_change_ctx_(nullptr),
          quorum_update_cb_(nullptr), quorum_update_ctx_(nullptr),
          raft_server_ptr_(nullptr),
          config_log_idx_hwm_(0) {
        saved_config_ = cs_new<cluster_config>();
        saved_state_ = cs_new<srv_state>();

        // Load state from Nim callbacks
        if (state_read_cb_) {
            uint64_t term = 0;
            int32_t voted_for = -1;
            uint64_t config_hwm = 0;
            int32_t found = state_read_cb_(cb_ctx_, &term, &voted_for, &config_hwm);
            if (found) {
                saved_state_->set_term(term);
                saved_state_->set_voted_for(voted_for);
                config_log_idx_hwm_ = config_hwm;
            }
        }

        // Apply catching_up flag (not persisted, set fresh each restart)
        if (catching_up) {
            saved_state_->set_catching_up(true);
            saved_state_->allow_election_timer(false);
        }

        // Load config from Nim callbacks
        if (config_load_cb_) {
            size_t cap = 64 * 1024;  // 64KB buffer for config
            char* buf = new char[cap];
            size_t config_len = config_load_cb_(cb_ctx_, buf, cap);
            if (config_len > 0 && config_len <= cap) {
                ptr<buffer> config_buf = buffer::alloc(config_len);
                std::memcpy(config_buf->data_begin(), buf, config_len);
                config_buf->pos(0);
                saved_config_ = cluster_config::deserialize(*config_buf);
            }
            delete[] buf;
        }

        for (auto& kv : servers) {
            auto sc = cs_new<srv_config>(kv.first, kv.second);
            saved_config_->get_servers().push_back(sc);
        }
    }

    ~callback_state_mgr() {}

    void set_raft_server(void* server_ptr) {
        raft_server_ptr_ = server_ptr;
    }

    void set_quorum_update_callback(void* ctx, nuraft_quorum_update_cb cb) {
        quorum_update_ctx_ = ctx;
        quorum_update_cb_ = cb;
    }

    ptr<cluster_config> load_config() override {
        return saved_config_;
    }

    void save_config(const cluster_config& config) override {
        // High water mark: ignore config changes from old log entries.
        ulong new_log_idx = config.get_log_idx();
        if (new_log_idx > 0 && new_log_idx < config_log_idx_hwm_) {
            return;
        }
        if (new_log_idx > 0) {
            config_log_idx_hwm_ = new_log_idx;
            // Persist updated HWM via Nim callback
            if (state_save_cb_) {
                state_save_cb_(cb_ctx_, saved_state_->get_term(),
                               saved_state_->get_voted_for(), config_log_idx_hwm_);
            }
        }

        ptr<buffer> buf = config.serialize();
        saved_config_ = cluster_config::deserialize(*buf);

        // Persist the config itself via Nim callback
        if (config_save_cb_) {
            buf->pos(0);
            const char* config_data = reinterpret_cast<const char*>(buf->data_begin() + buf->pos());
            size_t config_len = buf->size() - buf->pos();
            config_save_cb_(cb_ctx_, config_data, config_len);
        }

        // Notify Nim about each server in the new config
        if (config_change_cb_) {
            for (auto& srv : saved_config_->get_servers()) {
                int32_t server_id = srv->get_id();
                const std::string& endpoint = srv->get_endpoint();
                config_change_cb_(config_change_ctx_, server_id, endpoint.c_str());
            }
        }

        // Update quorum based on new server count
        size_t num_servers = saved_config_->get_servers().size();
        if (quorum_update_cb_ && num_servers > 0) {
            int32_t majority = (int32_t)(num_servers / 2) + 1;
            quorum_update_cb_(quorum_update_ctx_, my_id_, majority);
        }
    }

    void save_state(const srv_state& state) override {
        ptr<buffer> buf = state.serialize();
        saved_state_ = srv_state::deserialize(*buf);

        if (state_save_cb_) {
            state_save_cb_(cb_ctx_, saved_state_->get_term(),
                           saved_state_->get_voted_for(), config_log_idx_hwm_);
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
        // No stderr output in production (per project rules)
        (void)exit_code;
    }

    void set_config_change_callback(void* ctx, nuraft_config_change_cb cb) {
        config_change_ctx_ = ctx;
        config_change_cb_ = cb;
    }

private:
    int32 my_id_;
    std::string my_endpoint_;
    ptr<callback_log_store> cur_log_store_;
    ptr<cluster_config> saved_config_;
    ptr<srv_state> saved_state_;
    nuraft_state_save_cb state_save_cb_;
    nuraft_state_read_cb state_read_cb_;
    nuraft_config_save_cb config_save_cb_;
    nuraft_config_load_cb config_load_cb_;
    void* cb_ctx_;
    nuraft_config_change_cb config_change_cb_;
    void* config_change_ctx_;
    nuraft_quorum_update_cb quorum_update_cb_;
    void* quorum_update_ctx_;
    void* raft_server_ptr_;
    ulong config_log_idx_hwm_;
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
            g_pending_handlers[std::make_tuple(gid_hex, server_id_, target_id_)].push_back({when_done, current_time_ms()});
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
            auto it = g_pending_handlers.find(key);
            if (it != g_pending_handlers.end() && !it->second.empty()) {
                it->second.pop_back();
                if (it->second.empty()) {
                    g_pending_handlers.erase(it);
                }
            }
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

        if (schedule_cb_) {
            schedule_cb_(timer_ctx_, timer_id, milliseconds);
        }
    }

    void cancel_impl(ptr<delayed_task>& task) override {
        std::lock_guard<std::mutex> lock(tasks_lock_);
        for (auto it = tasks_.begin(); it != tasks_.end(); ++it) {
            if (it->second == task) {
                // Format group_id as hex string (last 4 bytes for brevity)
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
                // Timer was cancelled or expired - this is normal, no need to log
                return false;
            }
            task = it->second;
            tasks_.erase(it);
        }
        if (task) {
            // Call execute() - internally checks cancelled_ flag
            task->execute();
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
// State Manager Handle (tagged wrapper for dynamic_state_mgr or callback_state_mgr)
// =============================================================================

struct smgr_handle {
    ptr<callback_state_mgr> smgr;

    smgr_handle() : smgr(nullptr) {}
};

// =============================================================================
// Raft Server Wrapper
// =============================================================================

struct server_wrapper {
    ptr<raft_server> server;
    context* raft_ctx;  // Non-owning pointer for quorum updates (server owns the context)
    // Note: mp_ctx is NOT stored here - Nim owns it separately via rpcContext
    // Storing it here would cause double-free when server is destroyed
    ptr<callback_state_machine> sm;
    smgr_handle* smgr;  // Tagged handle (dynamic or callback)
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

void nuraft_params_set_auto_adjust_quorum(void* params, int32_t enable) {
    if (!params) return;
    auto* p = static_cast<raft_params*>(params);
    p->auto_adjust_quorum_for_small_cluster_ = (enable != 0);
    fprintf(stderr, "[NuRaft Shim] Set auto_adjust_quorum_for_small_cluster to %s\n",
            enable ? "true" : "false");
    fflush(stderr);
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

// Create state manager with callback-based persistence (WiscKey-backed).
// The log_store and state persistence callbacks delegate to Nim's RaftPersistentStore.
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
    nuraft_config_load_cb config_load_cb) {
    std::vector<std::pair<int32, std::string>> servers;
    for (int i = 0; i < num_servers; i++) {
        servers.push_back({server_ids[i], std::string(endpoints[i])});
    }

    // Create the callback-based log store
    auto log_store = cs_new<callback_log_store>(
        log_store_ctx,
        log_append_cb,
        log_write_at_cb,
        log_get_cb,
        log_term_at_cb,
        log_next_slot_cb,
        log_start_index_cb,
        log_pack_cb,
        log_apply_pack_cb,
        log_compact_cb,
        log_flush_cb
    );

    // Create the callback-based state manager
    auto smgr = cs_new<callback_state_mgr>(
        my_server_id, std::string(my_endpoint), servers, catching_up,
        log_store,
        state_save_cb, state_read_cb, config_save_cb, config_load_cb,
        state_cb_ctx
    );
    auto* h = new smgr_handle();
    h->smgr = smgr;
    return h;
}

void nuraft_smgr_destroy(void* smgr) {
    if (smgr) {
        auto* h = static_cast<smgr_handle*>(smgr);
        delete h;
    }
}

void nuraft_smgr_set_config_cb(void* smgr, void* ctx, nuraft_config_change_cb cb) {
    if (!smgr) return;
    auto* h = static_cast<smgr_handle*>(smgr);
    h->smgr->set_config_change_callback(ctx, cb);
}

void nuraft_smgr_set_quorum_cb(void* smgr, void* ctx, nuraft_quorum_update_cb cb) {
    if (!smgr) return;
    auto* h = static_cast<smgr_handle*>(smgr);
    h->smgr->set_quorum_update_callback(ctx, cb);
}

void nuraft_smgr_set_raft_server(void* smgr, void* server) {
    if (!smgr) return;
    auto* h = static_cast<smgr_handle*>(smgr);
    h->smgr->set_raft_server(server);
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
            // Clear any pending RPC handlers for this group/server to prevent
            // stale handlers from interfering with future tests.
            std::string gid_hex;
            for (int i = 0; i < 16; i++) {
                char hex[3];
                sprintf(hex, "%02x", (unsigned char)mp_ctx->group_id_bytes[i]);
                gid_hex += hex;
            }
            clearPendingHandlersForGroupAndServer(gid_hex, mp_ctx->server_id);

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
        return;
    }

    auto* mp_ctx = static_cast<mp_context_t*>(mp_context);
    auto* wrapper = static_cast<server_wrapper*>(server);

    // Deserialize message - allocate buffer and copy data
    // IMPORTANT: buffer::alloc creates buffer with given size, and pos=0
    ptr<buffer> msg_buf = buffer::alloc(msg_len);
    std::memcpy(msg_buf->data_begin(), msg_data, msg_len);
    msg_buf->pos(0);  // Reset position for reading

    msg_type type = get_msg_type(*msg_buf);

    // std::cerr << "[shim] deliver_message: server_id=" << mp_ctx->server_id
    //           << " group=" << gid_short << " msg_type=" << static_cast<int>(type)
    //           << " len=" << msg_len << std::endl;

    if (is_response_type(type)) {
         // It's a response - match to pending handler
         ptr<resp_msg> resp = deserialize_resp_msg(*msg_buf);

         // Build group_id hex for handler lookup key
         std::string gid_hex_key;
         for (int i = 0; i < 16; i++) {
             char hex[3];
             sprintf(hex, "%02x", (unsigned char)mp_ctx->group_id_bytes[i]);
             gid_hex_key += hex;
         }

         // Key: (groupId_hex, our_server_id, responder_id)
         auto key = std::make_tuple(gid_hex_key, mp_ctx->server_id, resp->get_src());

         rpc_handler handler;
         {
             std::lock_guard<std::mutex> lock(g_handlers_lock);
             // Purge expired handlers periodically (every call is fine, the lock is held)
             purgeExpiredHandlersLocked();
             auto it = g_pending_handlers.find(key);
             if (it != g_pending_handlers.end() && !it->second.empty()) {
                 handler = it->second.front().handler;
                 it->second.pop_front();
                 if (it->second.empty()) {
                     g_pending_handlers.erase(it);
                 }
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
        if (!srv) {
            return;
        }

        // --- FRACTIO: longest-log-wins pre-vote gate ---
        // NuRaft's handle_prevote_req grants pre-vote whenever
        // `!hb_alive_`, with no log comparison. That allows a node
        // with FEWER entries to start an election, become leader,
        // and then crash into "peer last_log_idx too large".
        // We gate pre-vote here (in Fractio code, not NuRaft) so
        // that only the longest-log node can pass pre-vote.
        ptr<resp_msg> resp;
        if (req->get_type() == msg_type::pre_vote_request) {
            ulong my_last_log_idx = srv->get_last_log_idx();
            ulong req_last_log_idx = req->get_last_log_idx();
            if (my_last_log_idx > req_last_log_idx) {
                // We have more entries: deny pre-vote so the shorter-log
                // node cannot become leader.
                // next_idx == MAX with accepted==false signals a "live"
                // peer to handle_prevote_resp (counts toward live_ counter).
                resp = cs_new<resp_msg>(
                    req->get_term(),
                    msg_type::pre_vote_response,
                    mp_ctx->server_id,
                    req->get_src(),
                    std::numeric_limits<ulong>::max(),
                    false
                );
            }
        }

        if (!resp) {
            // Normal path: let NuRaft handle the request
            resp = raft_server_access::call_process_req(srv, *req);
        }

        if (resp) {
            // std::cerr << "[shim] deliver_message: got response, sending via listener" << std::endl;
            // Send response via listener's callback
            auto& listener = mp_ctx->listener;
            if (listener && listener->has_send_response_callback()) {
                ptr<buffer> resp_buf = serialize_resp_msg(resp);
                const char* resp_data = reinterpret_cast<const char*>(resp_buf->data_begin());
                size_t resp_len = resp_buf->size();

                // std::cerr << "[shim] deliver_message: response src=" << resp->get_src()
                //           << " dst=" << resp->get_dst() << " accepted=" << resp->get_accepted() << std::endl;

                listener->get_send_resp_cb()(
                    listener->get_send_resp_ctx(),
                    listener->get_group_id(),
                    resp->get_src(),   // Our node ID
                    resp->get_dst(),   // Target node ID
                    resp_data,
                    resp_len
                );
                // std::cerr << "[shim] deliver_message: response sent" << std::endl;
            } else {
                // std::cerr << "[shim] deliver_message: no listener or callback" << std::endl;
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
    auto* smgr_h = static_cast<smgr_handle*>(smgr);
    auto* rp = static_cast<raft_params*>(params);

    // Get the base state_mgr pointer from the handle
    ptr<state_mgr> smgr_ptr = std::static_pointer_cast<state_mgr>(smgr_h->smgr);

    // Create context with custom RPC components
    context* ctx = new context(
        smgr_ptr,
        std::static_pointer_cast<state_machine>(sm_sp),
        mp_ctx->listener,
        nullptr, // logger
        mp_ctx->client_factory,
        mp_ctx->timer,
        *rp
    );

    // Wire event callback
    // Only log BecomeLeader/BecomeFollower events — these are rare and critical.
    // Other event types (ProcessReq, AppendEntries, etc.) fire hundreds of times
    // per second and synchronous std::cerr writes block the calling thread,
    // preventing timely election processing.
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

    // Store context pointer before transferring ownership to raft_server.
    // raft_server takes ownership via unique_ptr, but we need a non-owning
    // reference for update_params (quorum updates).
    context* ctx_raw = ctx;

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
    wrapper->raft_ctx = ctx_raw; // Non-owning pointer for quorum updates
    // Note: mp_ctx is owned by Nim (rpcContext), not stored here
    wrapper->sm = sm_sp;
    wrapper->smgr = smgr_h;

    return wrapper;
}

void nuraft_server_destroy(void* server) {
    if (!server) return;
    auto* wrapper = static_cast<server_wrapper*>(server);
    wrapper->server.reset();
    wrapper->raft_ctx = nullptr; // Non-owning, just clear the pointer
    // Note: mp_ctx is owned by Nim, destroyed separately via nuraftMpContextDestroy
    wrapper->sm.reset();
    // smgr is a smgr_handle* — shared_ptrs inside are released when handle is deleted
    // via nuraftSmgrDestroy, not here. Just clear the pointer.
    wrapper->smgr = nullptr;
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

void nuraft_server_update_quorum(void* server, int32_t quorum_size) {
    if (!server) return;
    auto* wrapper = static_cast<server_wrapper*>(server);
    if (!wrapper->server) return;

    if (!wrapper->raft_ctx) return;

    // Get current params and modify quorum sizes
    ptr<raft_params> current_params = wrapper->raft_ctx->get_params();
    raft_params new_params(*current_params);  // Copy existing params (preserves hb_interval!)
    new_params.custom_election_quorum_size_ = quorum_size;
    new_params.custom_commit_quorum_size_ = quorum_size;

    wrapper->server->update_params(new_params);
}

int32_t nuraft_server_get_peer_info(void* server, int32_t peer_id, nuraft_peer_info* out_info) {
    if (!server || !out_info) return -1;
    auto* wrapper = static_cast<server_wrapper*>(server);
    if (!wrapper->server) return -1;

    auto peer = wrapper->server->get_peer_info(peer_id);
    if (peer.id_ < 0) {
        out_info->exists = 0;
        return -1;
    }
    out_info->exists = 1;
    out_info->last_log_idx = peer.last_log_idx_;
    out_info->last_succ_resp_us = peer.last_succ_resp_us_;
    return 0;
}

int32_t nuraft_server_get_server_count(void* server) {
    if (!server) return 0;
    auto* wrapper = static_cast<server_wrapper*>(server);
    if (!wrapper->server) return 0;
    auto config = wrapper->server->get_config();
    if (!config) return 0;
    return static_cast<int32_t>(config->get_servers().size());
}

// =============================================================================
// Global Manager: Shared thread pool for all Raft groups
// =============================================================================

int32_t nuraft_global_mgr_init(int32_t num_commit_threads, int32_t num_append_threads) {
    // Initialize the global NuRaft manager with a shared thread pool.
    // Without this, each raft_server creates 2 dedicated threads (bg_commit_thread_
    // and bg_append_thread_). With N groups, that's 2N threads × 8MB stack = 16N MB.
    // With the global manager, all groups share num_commit_threads + num_append_threads
    // threads total (default 2 threads), reducing stack to ~16MB regardless of group count.
    nuraft_global_config config;
    config.num_commit_threads_ = static_cast<size_t>(num_commit_threads);
    config.num_append_threads_ = static_cast<size_t>(num_append_threads);
    config.max_scheduling_unit_ms_ = 200;

    nuraft_global_mgr* mgr = nuraft_global_mgr::init(config);
    if (mgr) {
        return 1;  // Successfully initialized
    }
    return -1;  // Error
}

void nuraft_global_mgr_shutdown() {
    nuraft_global_mgr::shutdown();
}

} // extern "C"