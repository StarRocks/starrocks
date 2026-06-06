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
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "exec/pipeline/primitives/operator_exec_stats.h"
#include "gen_cpp/Types_types.h" // for TUniqueId

namespace starrocks::pipeline {

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

    TUniqueId _query_id;
    std::atomic<int64_t> _delivery_deadline_ms{0};
    std::atomic<int64_t> _query_deadline_ms{0};
    std::atomic<int64_t> _delivery_expire_seconds{DEFAULT_EXPIRE_SECONDS};
    std::atomic<int64_t> _query_expire_seconds{DEFAULT_EXPIRE_SECONDS};
    std::once_flag _node_exec_stats_init_flag;
    NodeExecStatsMap _node_exec_stats;
};

} // namespace starrocks::pipeline
