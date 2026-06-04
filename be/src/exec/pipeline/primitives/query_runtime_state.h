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
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/concurrency/spinlock.h"
#include "base/hash/hash.h"
#include "base/hash/hash_std.hpp"
#include "base/phmap/phmap.h"
#include "exec/pipeline/primitives/operator_exec_stats.h"
#include "gen_cpp/Types_types.h"

namespace starrocks {

class MemTracker;
class QueryStatistics;

namespace debug {
class QueryTrace;
} // namespace debug

namespace spill {
class QuerySpillManager;
} // namespace spill

namespace pipeline {

class QueryRuntimeState {
public:
    using SpillExpectedReservedBytesGetter = size_t (*)(const void*);

    static constexpr int DEFAULT_EXPIRE_SECONDS = 300;

    void set_query_id(const TUniqueId& query_id) { _query_id = query_id; }
    TUniqueId query_id() const { return _query_id; }

    void set_query_expire_seconds(int expire_seconds) { _query_expire_seconds = expire_seconds; }
    int get_query_expire_seconds() const { return _query_expire_seconds; }
    void set_query_deadline(int64_t query_deadline) { _query_deadline = query_deadline; }
    bool is_query_expired() const;

    void set_mem_tracker(std::shared_ptr<MemTracker> mem_tracker) { _mem_tracker = std::move(mem_tracker); }
    std::shared_ptr<MemTracker> mem_tracker() { return _mem_tracker; }
    const std::shared_ptr<MemTracker>& mem_tracker() const { return _mem_tracker; }

    void set_spill_manager(spill::QuerySpillManager* spill_manager) { _spill_manager = spill_manager; }
    spill::QuerySpillManager* spill_manager() { return _spill_manager; }
    void set_spill_expected_reserved_bytes_getter(const void* context, SpillExpectedReservedBytesGetter getter) {
        _spill_expected_reserved_bytes_context = context;
        _spill_expected_reserved_bytes_getter = getter;
    }
    size_t spill_expected_reserved_bytes() const;

    void set_query_trace(std::shared_ptr<starrocks::debug::QueryTrace> query_trace) {
        _query_trace = std::move(query_trace);
    }
    starrocks::debug::QueryTrace* query_trace() { return _query_trace.get(); }
    std::shared_ptr<starrocks::debug::QueryTrace> shared_query_trace() { return _query_trace; }

    void incr_cpu_cost(int64_t cost);
    int64_t cpu_cost() const { return _total_cpu_cost_ns; }
    void incr_cur_scan_rows_num(int64_t rows_num);
    int64_t cur_scan_rows_num() const { return _total_scan_rows_num; }
    void incr_cur_scan_bytes(int64_t scan_bytes);
    int64_t get_scan_bytes() const { return _total_scan_bytes; }

    void set_scan_limit(int64_t scan_limit) { _scan_limit = scan_limit; }
    int64_t get_scan_limit() const { return _scan_limit; }

    void init_node_exec_stats(const std::vector<int32_t>& exec_stats_node_ids);
    bool need_record_exec_stats(int32_t plan_node_id);
    void update_operator_exec_stats(const OperatorExecStatsSnapshot& snapshot);
    void update_push_rows_stats(int32_t plan_node_id, int64_t push_rows);
    void update_pull_rows_stats(int32_t plan_node_id, int64_t pull_rows);
    void update_pred_filter_stats(int32_t plan_node_id, int64_t pred_filter_rows);
    void update_index_filter_stats(int32_t plan_node_id, int64_t index_filter_rows);
    void update_rf_filter_stats(int32_t plan_node_id, int64_t rf_filter_rows);
    void force_set_pull_rows_stats(int32_t plan_node_id, int64_t pull_rows);

    void update_scan_stats(int64_t table_id, int64_t scan_rows_num, int64_t scan_bytes);
    void add_delta_stats(QueryStatistics* query_statistic);
    void add_cumulative_stats(QueryStatistics* query_statistic) const;

private:
    using milliseconds = std::chrono::milliseconds;
    using steady_clock = std::chrono::steady_clock;

    struct ScanStats {
        std::atomic<int64_t> total_scan_rows_num = 0;
        std::atomic<int64_t> total_scan_bytes = 0;
        std::atomic<int64_t> delta_scan_rows_num = 0;
        std::atomic<int64_t> delta_scan_bytes = 0;
    };

    struct NodeExecStats {
        std::atomic_int64_t push_rows = 0;
        std::atomic_int64_t pull_rows = 0;
        std::atomic_int64_t pred_filter_rows = 0;
        std::atomic_int64_t index_filter_rows = 0;
        std::atomic_int64_t rf_filter_rows = 0;
    };

    TUniqueId _query_id;
    int64_t _query_deadline = 0;
    int _query_expire_seconds = DEFAULT_EXPIRE_SECONDS;
    std::shared_ptr<MemTracker> _mem_tracker;
    spill::QuerySpillManager* _spill_manager = nullptr;
    const void* _spill_expected_reserved_bytes_context = nullptr;
    SpillExpectedReservedBytesGetter _spill_expected_reserved_bytes_getter = nullptr;
    std::shared_ptr<starrocks::debug::QueryTrace> _query_trace;

    std::atomic<int64_t> _total_cpu_cost_ns = 0;
    std::atomic<int64_t> _total_scan_rows_num = 0;
    std::atomic<int64_t> _total_scan_bytes = 0;
    std::atomic<int64_t> _delta_cpu_cost_ns = 0;
    std::atomic<int64_t> _delta_scan_rows_num = 0;
    std::atomic<int64_t> _delta_scan_bytes = 0;

    int64_t _scan_limit = 0;

    std::once_flag _node_exec_stats_init_flag;
    mutable SpinLock _scan_stats_lock;
    phmap::flat_hash_map<int64_t, std::shared_ptr<ScanStats>, StdHash<int64_t>> _scan_stats;
    std::unordered_map<int32_t, std::shared_ptr<NodeExecStats>> _node_exec_stats;
};

} // namespace pipeline
} // namespace starrocks
