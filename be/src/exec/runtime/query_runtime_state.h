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
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "base/concurrency/spinlock.h"
#include "base/hash/hash.h"
#include "base/hash/hash_std.hpp"
#include "base/phmap/phmap.h"
#include "base/time/time.h"
#include "exec/pipeline/primitives/operator_exec_stats.h"
#include "gen_cpp/InternalService_types.h" // for TPipelineProfileLevel, TTimeUnit
#include "gen_cpp/Types_types.h"           // for TUniqueId

namespace starrocks {
class GlobalLateMaterilizationContextMgr;
class MemTracker;
class ObjectPool;
class QueryStatistics;
namespace spill {
class QuerySpillManager;
} // namespace spill
} // namespace starrocks

namespace starrocks::pipeline {

struct ConnectorScanOperatorMemShareArbitrator;

class QueryRuntimeState {
public:
    QueryRuntimeState() = default;

    struct NodeExecStats {
        std::atomic_int64_t push_rows{0};
        std::atomic_int64_t pull_rows{0};
        std::atomic_int64_t pred_filter_rows{0};
        std::atomic_int64_t index_filter_rows{0};
        std::atomic_int64_t rf_filter_rows{0};
    };
    using NodeExecStatsMap = std::unordered_map<int32_t, std::shared_ptr<NodeExecStats>>;

    void set_query_id(const TUniqueId& query_id) { _query_id = query_id; }
    const TUniqueId& query_id() const { return _query_id; }
    void set_query_mem_tracker(MemTracker* query_mem_tracker) { _query_mem_tracker = query_mem_tracker; }
    MemTracker* query_mem_tracker() { return _query_mem_tracker; }
    const MemTracker* query_mem_tracker() const { return _query_mem_tracker; }
    void set_query_spill_manager(spill::QuerySpillManager* query_spill_manager) {
        _query_spill_manager = query_spill_manager;
    }
    spill::QuerySpillManager* query_spill_manager() { return _query_spill_manager; }
    const spill::QuerySpillManager* query_spill_manager() const { return _query_spill_manager; }
    void set_object_pool(ObjectPool* object_pool) { _object_pool = object_pool; }
    ObjectPool* object_pool() { return _object_pool; }
    void set_connector_scan_mem_tracker(MemTracker* connector_scan_mem_tracker) {
        _connector_scan_mem_tracker = connector_scan_mem_tracker;
    }
    MemTracker* connector_scan_mem_tracker() { return _connector_scan_mem_tracker; }
    void set_connector_scan_operator_mem_share_arbitrator(ConnectorScanOperatorMemShareArbitrator* arb) {
        _connector_scan_operator_mem_share_arbitrator = arb;
    }
    ConnectorScanOperatorMemShareArbitrator* connector_scan_operator_mem_share_arbitrator() const {
        return _connector_scan_operator_mem_share_arbitrator;
    }
    void set_global_late_materialization_ctx_mgr(GlobalLateMaterilizationContextMgr* mgr) {
        _global_late_materialization_ctx_mgr = mgr;
    }
    GlobalLateMaterilizationContextMgr* global_late_materialization_ctx_mgr() const {
        return _global_late_materialization_ctx_mgr;
    }
    void set_static_query_mem_limit(int64_t static_query_mem_limit) {
        _static_query_mem_limit = static_query_mem_limit;
    }
    int64_t static_query_mem_limit() const { return _static_query_mem_limit; }

    void set_enable_profile() { _enable_profile.store(true, std::memory_order_relaxed); }
    bool get_enable_profile_flag() const { return _enable_profile.load(std::memory_order_relaxed); }
    bool enable_profile() const {
        if (get_enable_profile_flag()) {
            return true;
        }
        if (_big_query_profile_threshold_ns <= 0) {
            return false;
        }
        return MonotonicNanos() - _query_begin_time > _big_query_profile_threshold_ns;
    }
    void set_big_query_profile_threshold(int64_t big_query_profile_threshold,
                                         TTimeUnit::type big_query_profile_threshold_unit);
    int64_t get_big_query_profile_threshold_ns() const { return _big_query_profile_threshold_ns; }
    void set_runtime_profile_report_interval(int64_t runtime_profile_report_interval_s) {
        _runtime_profile_report_interval_ns = 1'000'000'000L * runtime_profile_report_interval_s;
    }
    int64_t get_runtime_profile_report_interval_ns() const { return _runtime_profile_report_interval_ns; }
    void set_profile_level(const TPipelineProfileLevel::type& profile_level) { _profile_level = profile_level; }
    const TPipelineProfileLevel::type& profile_level() const { return _profile_level; }
    int64_t query_begin_time() const { return _query_begin_time; }
    void init_query_begin_time() { _query_begin_time = MonotonicNanos(); }

    static constexpr int DEFAULT_EXPIRE_SECONDS = 300;

    void set_delivery_expire_seconds(int expire_seconds) { _delivery_expire_seconds.store(expire_seconds); }
    void set_query_expire_seconds(int expire_seconds) { _query_expire_seconds.store(expire_seconds); }
    int get_query_expire_seconds() const { return static_cast<int>(_query_expire_seconds.load()); }

    bool is_delivery_expired() const { return _now_ms() > _delivery_deadline_ms.load(); }
    bool is_query_expired() const { return _now_ms() > _query_deadline_ms.load(); }

    void extend_delivery_lifetime() {
        _delivery_deadline_ms.store(_now_ms() + _delivery_expire_seconds.load() * 1000L);
    }
    void extend_query_lifetime() { _query_deadline_ms.store(_now_ms() + _query_expire_seconds.load() * 1000L); }

    void init_node_exec_stats(const std::vector<int32_t>& exec_stats_node_ids) {
        std::call_once(_node_exec_stats_init_flag, [this, &exec_stats_node_ids]() {
            for (int32_t node_id : exec_stats_node_ids) {
                _node_exec_stats[node_id] = std::make_shared<NodeExecStats>();
            }
        });
    }

    bool need_record_exec_stats(int32_t plan_node_id) const {
        auto it = _node_exec_stats.find(plan_node_id);
        return it != _node_exec_stats.end();
    }

    void update_operator_exec_stats(const OperatorExecStatsSnapshot& snapshot);

    void update_push_rows_stats(int32_t plan_node_id, int64_t push_rows) {
        if (auto* stats = _find_node_exec_stats(plan_node_id); stats != nullptr) {
            stats->push_rows += push_rows;
        }
    }

    void update_pull_rows_stats(int32_t plan_node_id, int64_t pull_rows) {
        if (auto* stats = _find_node_exec_stats(plan_node_id); stats != nullptr) {
            stats->pull_rows += pull_rows;
        }
    }

    void update_pred_filter_stats(int32_t plan_node_id, int64_t pred_filter_rows) {
        if (auto* stats = _find_node_exec_stats(plan_node_id); stats != nullptr) {
            stats->pred_filter_rows += pred_filter_rows;
        }
    }

    void update_index_filter_stats(int32_t plan_node_id, int64_t index_filter_rows) {
        if (auto* stats = _find_node_exec_stats(plan_node_id); stats != nullptr) {
            stats->index_filter_rows += index_filter_rows;
        }
    }

    void update_rf_filter_stats(int32_t plan_node_id, int64_t rf_filter_rows) {
        if (auto* stats = _find_node_exec_stats(plan_node_id); stats != nullptr) {
            stats->rf_filter_rows += rf_filter_rows;
        }
    }

    void force_set_pull_rows_stats(int32_t plan_node_id, int64_t pull_rows) {
        if (auto* stats = _find_node_exec_stats(plan_node_id); stats != nullptr) {
            stats->pull_rows.exchange(pull_rows);
        }
    }

    NodeExecStatsMap& node_exec_stats() { return _node_exec_stats; }
    const NodeExecStatsMap& node_exec_stats() const { return _node_exec_stats; }

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
    int64_t consume_delta_cpu_cost() { return _delta_cpu_cost_ns.exchange(0); }
    int64_t cur_scan_rows_num() const { return _total_scan_rows_num; }
    int64_t get_scan_bytes() const { return _total_scan_bytes; }
    void consume_delta_scan_stats(QueryStatistics* query_statistic);
    void add_total_scan_stats(QueryStatistics* query_statistic);

private:
    static int64_t _now_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now().time_since_epoch())
                .count();
    }

    NodeExecStats* _find_node_exec_stats(int32_t plan_node_id) {
        auto it = _node_exec_stats.find(plan_node_id);
        return it == _node_exec_stats.end() ? nullptr : it->second.get();
    }

    struct ScanStats {
        std::atomic<int64_t> total_scan_rows_num = 0;
        std::atomic<int64_t> total_scan_bytes = 0;
        std::atomic<int64_t> delta_scan_rows_num = 0;
        std::atomic<int64_t> delta_scan_bytes = 0;
    };

    TUniqueId _query_id;
    std::atomic<int64_t> _delivery_deadline_ms{0};
    std::atomic<int64_t> _query_deadline_ms{0};
    std::atomic<int64_t> _delivery_expire_seconds{DEFAULT_EXPIRE_SECONDS};
    std::atomic<int64_t> _query_expire_seconds{DEFAULT_EXPIRE_SECONDS};
    std::once_flag _node_exec_stats_init_flag;
    NodeExecStatsMap _node_exec_stats;
    std::atomic<int64_t> _total_cpu_cost_ns = 0;
    std::atomic<int64_t> _total_scan_rows_num = 0;
    std::atomic<int64_t> _total_scan_bytes = 0;
    std::atomic<int64_t> _delta_cpu_cost_ns = 0;
    std::atomic<int64_t> _delta_scan_rows_num = 0;
    std::atomic<int64_t> _delta_scan_bytes = 0;

    // @TODO(silverbullet233):
    // our phmap's version is too old and it doesn't provide a thread-safe iteration interface,
    // we use spinlock + flat_hash_map here, after upgrading, we can change it to parallel_flat_hash_map
    SpinLock _scan_stats_lock;
    // table level scan stats
    phmap::flat_hash_map<int64_t, std::shared_ptr<ScanStats>, StdHash<int64_t>> _scan_stats;

    // TODO: QueryContext still owns the query MemTracker lifecycle; migrate this reference when QueryContext
    // lifecycle is refactored.
    MemTracker* _query_mem_tracker = nullptr;
    // QueryContext owns the query spill manager lifecycle; QueryRuntimeState only exposes a non-owning reference.
    spill::QuerySpillManager* _query_spill_manager = nullptr;
    // QueryContext owns the lifecycle of all the services below; QueryRuntimeState only exposes non-owning
    // references, published by QueryContext at construction (_object_pool) or inside init_mem_tracker.
    ObjectPool* _object_pool = nullptr;
    MemTracker* _connector_scan_mem_tracker = nullptr;
    ConnectorScanOperatorMemShareArbitrator* _connector_scan_operator_mem_share_arbitrator = nullptr;
    GlobalLateMaterilizationContextMgr* _global_late_materialization_ctx_mgr = nullptr;
    int64_t _static_query_mem_limit = 0;
    std::atomic_bool _enable_profile = false;
    int64_t _big_query_profile_threshold_ns = 0;
    int64_t _runtime_profile_report_interval_ns = std::numeric_limits<int64_t>::max();
    TPipelineProfileLevel::type _profile_level = TPipelineProfileLevel::type::MERGE;
    int64_t _query_begin_time = 0;
};

} // namespace starrocks::pipeline
