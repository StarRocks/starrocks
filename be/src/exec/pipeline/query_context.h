// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <atomic>
#include <chrono>
#include <mutex>
#include <unordered_map>

#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "gen_cpp/InternalService_types.h" // for TQueryOptions
#include "gen_cpp/Types_types.h"           // for TUniqueId
#include "runtime/runtime_state.h"
#include "util/hash_util.hpp"

namespace starrocks {
namespace pipeline {

using std::chrono::seconds;
using std::chrono::milliseconds;
using std::chrono::steady_clock;
using std::chrono::duration_cast;
// The context for all fragment of one query in one BE
class QueryContext {
public:
    QueryContext();
    ~QueryContext();
    void set_exec_env(ExecEnv* exec_env) { _exec_env = exec_env; }
    void set_query_id(const TUniqueId& query_id) { _query_id = query_id; }
    TUniqueId query_id() { return _query_id; }
    void set_total_fragments(size_t total_fragments) { _total_fragments = total_fragments; }

    void increment_num_fragments() {
        _num_fragments.fetch_add(1);
        _num_active_fragments.fetch_add(1);
    }

    bool count_down_fragments() { return _num_active_fragments.fetch_sub(1) == 1; }

    bool has_no_active_instances() { return _num_active_fragments.load() == 0; }

    void set_expire_seconds(int expire_seconds) { _expire_seconds = seconds(expire_seconds); }
    inline int get_expire_seconds() { return _expire_seconds.count(); }
    // now time point pass by deadline point.
    bool is_expired() {
        auto now = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
        return now > _deadline;
    }

    bool is_dead() { return _num_active_fragments == 0 && _num_fragments == _total_fragments; }
    // add expired seconds to deadline
    void extend_lifetime() {
        _deadline = duration_cast<milliseconds>(steady_clock::now().time_since_epoch() + _expire_seconds).count();
    }
    FragmentContextManager* fragment_mgr();

    void cancel(const Status& status);

    void set_is_runtime_filter_coordinator(bool flag) { _is_runtime_filter_coordinator = flag; }

    ObjectPool* object_pool() { return &_object_pool; }
    void set_desc_tbl(DescriptorTbl* desc_tbl) {
        DCHECK(_desc_tbl == nullptr);
        _desc_tbl = desc_tbl;
    }

    DescriptorTbl* desc_tbl() {
        DCHECK(_desc_tbl != nullptr);
        return _desc_tbl;
    }
    int64_t compute_query_mem_limit(int64_t parent_mem_limit, int64_t per_instance_mem_limit, size_t pipeline_dop);
    size_t total_fragments() { return _total_fragments; }
    void init_mem_tracker(int64_t bytes_limit, MemTracker* parent);
    std::shared_ptr<MemTracker> mem_tracker() { return _mem_tracker; }

    // Record the number of rows read from the data source for big query checking
    void incr_cur_scan_rows_num(int64_t rows_num) { _cur_scan_rows_num += rows_num; }
    int64_t cur_scan_rows_num() const { return _cur_scan_rows_num; }
    void incr_cur_scan_bytes(int64_t scan_bytes) { _cur_scan_bytes += scan_bytes; }
    int64_t get_scan_bytes() const { return _cur_scan_bytes; }

private:
    ExecEnv* _exec_env = nullptr;
    TUniqueId _query_id;
    std::unique_ptr<FragmentContextManager> _fragment_mgr;
    size_t _total_fragments;
    std::atomic<size_t> _num_fragments;
    std::atomic<size_t> _num_active_fragments;
    int64_t _deadline;
    seconds _expire_seconds;
    bool _is_runtime_filter_coordinator = false;
    std::once_flag _init_mem_tracker_once;
    std::shared_ptr<RuntimeProfile> _profile;
    std::shared_ptr<MemTracker> _mem_tracker;
    ObjectPool _object_pool;
    DescriptorTbl* _desc_tbl = nullptr;

    std::atomic<int64_t> _cur_scan_rows_num = 0;
    std::atomic<int64_t> _cur_scan_bytes = 0;
};

class QueryContextManager {
public:
    QueryContextManager(size_t log2_num_slots);
    ~QueryContextManager();
    Status init();
    QueryContext* get_or_register(const TUniqueId& query_id);
    QueryContextPtr get(const TUniqueId& query_id);
    size_t size();
    void remove(const TUniqueId& query_id);
    // used for graceful exit
    void clear();

private:
    static void _clean_func(QueryContextManager* manager);
    void _clean_query_contexts();
    void _stop_clean_func() { _stop.store(true); }
    bool _is_stopped() { return _stop; }
    size_t _slot_idx(const TUniqueId& query_id);
    void _clean_slot_unlocked(size_t i);

private:
    const size_t _num_slots;
    const size_t _slot_mask;
    std::vector<std::shared_mutex> _mutexes;
    std::vector<std::unordered_map<TUniqueId, QueryContextPtr>> _context_maps;
    std::vector<std::unordered_map<TUniqueId, QueryContextPtr>> _second_chance_maps;

    std::atomic<bool> _stop{false};
    std::shared_ptr<std::thread> _clean_thread;

    inline static const char* _metric_name = "pip_query_ctx_cnt";
    std::unique_ptr<UIntGauge> _query_ctx_cnt;
};

} // namespace pipeline
} // namespace starrocks
