// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <atomic>
#include <chrono>
#include <mutex>
#include <unordered_map>

#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/stream_epoch_manager.h"
#include "exec/spill/query_spill_manager.h"
#include "gen_cpp/InternalService_types.h" // for TQueryOptions
#include "gen_cpp/Types_types.h"           // for TUniqueId
#include "gen_cpp/internal_service.pb.h"
#include "runtime/profile_report_worker.h"
#include "runtime/query_statistics.h"
#include "runtime/runtime_state.h"
#include "util/debug/query_trace.h"
#include "util/hash_util.hpp"
#include "util/spinlock.h"
#include "util/time.h"

namespace starrocks {

class StreamEpochManager;

namespace pipeline {

using std::chrono::seconds;
using std::chrono::milliseconds;
using std::chrono::steady_clock;
using std::chrono::duration_cast;

// The context for all fragment of one query in one BE
class QueryContext : public std::enable_shared_from_this<QueryContext> {
public:
    QueryContext();
    ~QueryContext() noexcept;
    void set_exec_env(ExecEnv* exec_env) { _exec_env = exec_env; }
    void set_query_id(const TUniqueId& query_id) { _query_id = query_id; }
    TUniqueId query_id() const { return _query_id; }
    int64_t lifetime() { return _lifetime_sw.elapsed_time(); }
    void set_total_fragments(size_t total_fragments) { _total_fragments = total_fragments; }

    void increment_num_fragments() {
        _num_fragments.fetch_add(1);
        _num_active_fragments.fetch_add(1);
    }

    void count_down_fragments();
    int num_active_fragments() const { return _num_active_fragments.load(); }
    bool has_no_active_instances() { return _num_active_fragments.load() == 0; }

    void set_delivery_expire_seconds(int expire_seconds) { _delivery_expire_seconds = seconds(expire_seconds); }
    void set_query_expire_seconds(int expire_seconds) { _query_expire_seconds = seconds(expire_seconds); }
    inline int get_query_expire_seconds() const { return _query_expire_seconds.count(); }
    // now time point pass by deadline point.
    bool is_delivery_expired() const {
        auto now = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
        return now > _delivery_deadline;
    }
    bool is_query_expired() const {
        auto now = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
        return now > _query_deadline;
    }

    bool is_dead() const { return _num_active_fragments == 0 && _num_fragments == _total_fragments; }
    // add expired seconds to deadline
    void extend_delivery_lifetime() {
        _delivery_deadline =
                duration_cast<milliseconds>(steady_clock::now().time_since_epoch() + _delivery_expire_seconds).count();
    }
    void extend_query_lifetime() {
        _query_deadline =
                duration_cast<milliseconds>(steady_clock::now().time_since_epoch() + _query_expire_seconds).count();
    }
    void set_enable_profile() { _enable_profile = true; }
    bool enable_profile() {
        if (_enable_profile) {
            return true;
        }
        if (_big_query_profile_threshold_ns <= 0) {
            return false;
        }
        return MonotonicNanos() - _query_begin_time > _big_query_profile_threshold_ns;
    }
    void set_big_query_profile_threshold(int64_t big_query_profile_threshold_s) {
        _big_query_profile_threshold_ns = 1'000'000'000L * big_query_profile_threshold_s;
    }
    void set_runtime_profile_report_interval(int64_t runtime_profile_report_interval_s) {
        _runtime_profile_report_interval_ns = 1'000'000'000L * runtime_profile_report_interval_s;
    }
    int64_t get_runtime_profile_report_interval_ns() { return _runtime_profile_report_interval_ns; }
    void set_profile_level(const TPipelineProfileLevel::type& profile_level) { _profile_level = profile_level; }
    const TPipelineProfileLevel::type& profile_level() { return _profile_level; }

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
    // If option_query_mem_limit > 0, use it directly.
    // Otherwise, use per_instance_mem_limit * num_fragments * pipeline_dop.
    int64_t compute_query_mem_limit(int64_t parent_mem_limit, int64_t per_instance_mem_limit, size_t pipeline_dop,
                                    int64_t option_query_mem_limit);
    size_t total_fragments() { return _total_fragments; }
    /// Initialize the mem_tracker of this query.
    /// Positive `big_query_mem_limit` and non-null `wg` indicate
    /// that there is a big query memory limit of this resource group.
    void init_mem_tracker(int64_t query_mem_limit, MemTracker* parent, int64_t big_query_mem_limit = -1,
                          workgroup::WorkGroup* wg = nullptr);
    std::shared_ptr<MemTracker> mem_tracker() { return _mem_tracker; }

    Status init_query_once(workgroup::WorkGroup* wg);
    /// Release the workgroup token only once to avoid double-free.
    /// This method should only be invoked while the QueryContext is still valid,
    /// to avoid double-free between the destruction and this method.
    void release_workgroup_token_once();

    // Some statistic about the query, including cpu, scan_rows, scan_bytes
    int64_t mem_cost_bytes() const { return _mem_tracker == nullptr ? 0 : _mem_tracker->peak_consumption(); }
    int64_t current_mem_usage_bytes() const { return _mem_tracker == nullptr ? 0 : _mem_tracker->consumption(); }
    void incr_cpu_cost(int64_t cost) {
        _total_cpu_cost_ns += cost;
        _delta_cpu_cost_ns += cost;
    }
    void incr_cur_scan_rows_num(int64_t rows_num) {
        _total_scan_rows_num += rows_num;
        _delta_scan_rows_num += rows_num;
    }
    void incr_cur_scan_bytes(int64_t scan_bytes) {
        _total_scan_bytes += scan_bytes;
        _delta_scan_bytes += scan_bytes;
    }

    void update_scan_stats(int64_t table_id, int64_t scan_rows_num, int64_t scan_bytes);

    int64_t cpu_cost() const { return _total_cpu_cost_ns; }
    int64_t cur_scan_rows_num() const { return _total_scan_rows_num; }
    int64_t get_scan_bytes() const { return _total_scan_bytes; }
    std::atomic_int64_t* mutable_total_spill_bytes() { return &_total_spill_bytes; }
    int64_t get_spill_bytes() { return _total_spill_bytes; }

    // Query start time, used to check how long the query has been running
    // To ensure that the minimum run time of the query will not be killed by the big query checking mechanism
    int64_t query_begin_time() const { return _query_begin_time; }
    void init_query_begin_time() { _query_begin_time = MonotonicNanos(); }

    void set_scan_limit(int64_t scan_limit) { _scan_limit = scan_limit; }
    int64_t get_scan_limit() const { return _scan_limit; }
    void set_query_trace(std::shared_ptr<starrocks::debug::QueryTrace> query_trace);

    starrocks::debug::QueryTrace* query_trace() { return _query_trace.get(); }

    std::shared_ptr<starrocks::debug::QueryTrace> shared_query_trace() { return _query_trace; }

    // Delta statistic since last retrieve
    std::shared_ptr<QueryStatistics> intermediate_query_statistic();
    // Merged statistic from all executor nodes
    std::shared_ptr<QueryStatistics> final_query_statistic();
    std::shared_ptr<QueryStatisticsRecvr> maintained_query_recv();
    bool is_final_sink() const { return _is_final_sink; }
    void set_final_sink() { _is_final_sink = true; }

    QueryContextPtr get_shared_ptr() { return shared_from_this(); }

    // STREAM MV
    StreamEpochManager* stream_epoch_manager() const { return _stream_epoch_manager.get(); }

    spill::QuerySpillManager* spill_manager() { return _spill_manager.get(); }

public:
    static constexpr int DEFAULT_EXPIRE_SECONDS = 300;

private:
    ExecEnv* _exec_env = nullptr;
    TUniqueId _query_id;
    MonotonicStopWatch _lifetime_sw;
    std::unique_ptr<FragmentContextManager> _fragment_mgr;
    size_t _total_fragments;
    std::atomic<size_t> _num_fragments;
    std::atomic<size_t> _num_active_fragments;
    int64_t _delivery_deadline = 0;
    int64_t _query_deadline = 0;
    seconds _delivery_expire_seconds = seconds(DEFAULT_EXPIRE_SECONDS);
    seconds _query_expire_seconds = seconds(DEFAULT_EXPIRE_SECONDS);
    bool _is_runtime_filter_coordinator = false;
    std::once_flag _init_mem_tracker_once;
    std::shared_ptr<RuntimeProfile> _profile;
    bool _enable_profile = false;
    int64_t _big_query_profile_threshold_ns = 0;
    int64_t _runtime_profile_report_interval_ns = std::numeric_limits<int64_t>::max();
    TPipelineProfileLevel::type _profile_level;
    std::shared_ptr<MemTracker> _mem_tracker;
    ObjectPool _object_pool;
    DescriptorTbl* _desc_tbl = nullptr;
    std::once_flag _query_trace_init_flag;
    std::shared_ptr<starrocks::debug::QueryTrace> _query_trace;

    std::once_flag _init_query_once;
    int64_t _query_begin_time = 0;
    std::atomic<int64_t> _total_cpu_cost_ns = 0;
    std::atomic<int64_t> _total_scan_rows_num = 0;
    std::atomic<int64_t> _total_scan_bytes = 0;
    std::atomic<int64_t> _total_spill_bytes = 0;
    std::atomic<int64_t> _delta_cpu_cost_ns = 0;
    std::atomic<int64_t> _delta_scan_rows_num = 0;
    std::atomic<int64_t> _delta_scan_bytes = 0;

    struct ScanStats {
        std::atomic<int64_t> total_scan_rows_num = 0;
        std::atomic<int64_t> total_scan_bytes = 0;
        std::atomic<int64_t> delta_scan_rows_num = 0;
        std::atomic<int64_t> delta_scan_bytes = 0;
    };
    // @TODO(silverbullet233):
    // our phmap's version is too old and it doesn't provide a thread-safe iteration interface,
    // we use spinlock + flat_hash_map here, after upgrading, we can change it to parallel_flat_hash_map
    SpinLock _scan_stats_lock;
    // table level scan stats
    phmap::flat_hash_map<int64_t, std::shared_ptr<ScanStats>> _scan_stats;

    bool _is_final_sink = false;
    std::shared_ptr<QueryStatisticsRecvr> _sub_plan_query_statistics_recvr; // For receive

    int64_t _scan_limit = 0;
    workgroup::RunningQueryTokenPtr _wg_running_query_token_ptr;
    std::atomic<workgroup::RunningQueryToken*> _wg_running_query_token_atomic_ptr = nullptr;

    // STREAM MV
    std::shared_ptr<StreamEpochManager> _stream_epoch_manager;

    std::unique_ptr<spill::QuerySpillManager> _spill_manager;
};

class QueryContextManager {
public:
    QueryContextManager(size_t log2_num_slots);
    ~QueryContextManager();
    Status init();
    QueryContext* get_or_register(const TUniqueId& query_id);
    QueryContextPtr get(const TUniqueId& query_id);
    size_t size();
    bool remove(const TUniqueId& query_id);
    // used for graceful exit
    void clear();

    void report_fragments(const std::vector<starrocks::PipeLineReportTaskKey>& non_pipeline_need_report_fragment_ids);

    void report_fragments_with_same_host(
            const std::vector<std::shared_ptr<FragmentContext>>& need_report_fragment_context,
            std::vector<bool>& reported, const TNetworkAddress& last_coord_addr,
            std::vector<TReportExecStatusParams>& report_exec_status_params_vector,
            std::vector<int32_t>& cur_batch_report_indexes);

    void collect_query_statistics(const PCollectQueryStatisticsRequest* request,
                                  PCollectQueryStatisticsResult* response);

private:
    static void _clean_func(QueryContextManager* manager);
    void _clean_query_contexts();
    void _stop_clean_func() { _stop.store(true); }
    bool _is_stopped() { return _stop; }
    size_t _slot_idx(const TUniqueId& query_id);
    void _clean_slot_unlocked(size_t i, std::vector<QueryContextPtr>& del);

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
