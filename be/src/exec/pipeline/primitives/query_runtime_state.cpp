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

#include "exec/pipeline/primitives/query_runtime_state.h"

#include "runtime/query_statistics.h"

namespace starrocks::pipeline {

bool QueryRuntimeState::is_query_expired() const {
    auto now = std::chrono::duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
    return now > _query_deadline;
}

size_t QueryRuntimeState::spill_expected_reserved_bytes() const {
    return _spill_expected_reserved_bytes_getter == nullptr
                   ? 0
                   : _spill_expected_reserved_bytes_getter(_spill_expected_reserved_bytes_context);
}

void QueryRuntimeState::incr_cpu_cost(int64_t cost) {
    _total_cpu_cost_ns += cost;
    _delta_cpu_cost_ns += cost;
}

void QueryRuntimeState::incr_cur_scan_rows_num(int64_t rows_num) {
    _total_scan_rows_num += rows_num;
    _delta_scan_rows_num += rows_num;
}

void QueryRuntimeState::incr_cur_scan_bytes(int64_t scan_bytes) {
    _total_scan_bytes += scan_bytes;
    _delta_scan_bytes += scan_bytes;
}

void QueryRuntimeState::init_node_exec_stats(const std::vector<int32_t>& exec_stats_node_ids) {
    std::call_once(_node_exec_stats_init_flag, [this, &exec_stats_node_ids]() {
        for (int32_t node_id : exec_stats_node_ids) {
            auto node_exec_stats = std::make_shared<NodeExecStats>();
            _node_exec_stats[node_id] = node_exec_stats;
        }
    });
}

bool QueryRuntimeState::need_record_exec_stats(int32_t plan_node_id) {
    auto it = _node_exec_stats.find(plan_node_id);
    return it != _node_exec_stats.end();
}

void QueryRuntimeState::update_operator_exec_stats(const OperatorExecStatsSnapshot& snapshot) {
    if (!snapshot.valid) {
        return;
    }
    if (snapshot.require_registered_plan_node && !need_record_exec_stats(snapshot.plan_node_id)) {
        return;
    }

    if (snapshot.update_push_rows) {
        update_push_rows_stats(snapshot.plan_node_id, snapshot.push_rows);
    }
    if (snapshot.update_pull_rows) {
        if (snapshot.force_set_pull_rows) {
            force_set_pull_rows_stats(snapshot.plan_node_id, snapshot.pull_rows);
        } else {
            update_pull_rows_stats(snapshot.plan_node_id, snapshot.pull_rows);
        }
    }
    if (snapshot.update_pred_filter_rows) {
        update_pred_filter_stats(snapshot.plan_node_id, snapshot.pred_filter_rows);
    }
    if (snapshot.update_rf_filter_rows) {
        update_rf_filter_stats(snapshot.plan_node_id, snapshot.rf_filter_rows);
    }
}

void QueryRuntimeState::update_push_rows_stats(int32_t plan_node_id, int64_t push_rows) {
    auto it = _node_exec_stats.find(plan_node_id);
    if (it != _node_exec_stats.end()) {
        it->second->push_rows += push_rows;
    }
}

void QueryRuntimeState::update_pull_rows_stats(int32_t plan_node_id, int64_t pull_rows) {
    auto it = _node_exec_stats.find(plan_node_id);
    if (it != _node_exec_stats.end()) {
        it->second->pull_rows += pull_rows;
    }
}

void QueryRuntimeState::update_pred_filter_stats(int32_t plan_node_id, int64_t pred_filter_rows) {
    auto it = _node_exec_stats.find(plan_node_id);
    if (it != _node_exec_stats.end()) {
        it->second->pred_filter_rows += pred_filter_rows;
    }
}

void QueryRuntimeState::update_index_filter_stats(int32_t plan_node_id, int64_t index_filter_rows) {
    auto it = _node_exec_stats.find(plan_node_id);
    if (it != _node_exec_stats.end()) {
        it->second->index_filter_rows += index_filter_rows;
    }
}

void QueryRuntimeState::update_rf_filter_stats(int32_t plan_node_id, int64_t rf_filter_rows) {
    auto it = _node_exec_stats.find(plan_node_id);
    if (it != _node_exec_stats.end()) {
        it->second->rf_filter_rows += rf_filter_rows;
    }
}

void QueryRuntimeState::force_set_pull_rows_stats(int32_t plan_node_id, int64_t pull_rows) {
    auto it = _node_exec_stats.find(plan_node_id);
    if (it != _node_exec_stats.end()) {
        it->second->pull_rows.exchange(pull_rows);
    }
}

void QueryRuntimeState::update_scan_stats(int64_t table_id, int64_t scan_rows_num, int64_t scan_bytes) {
    ScanStats* stats = nullptr;
    {
        std::lock_guard l(_scan_stats_lock);
        auto iter = _scan_stats.find(table_id);
        if (iter == _scan_stats.end()) {
            _scan_stats.insert({table_id, std::make_shared<ScanStats>()});
            iter = _scan_stats.find(table_id);
        }
        stats = iter->second.get();
    }

    stats->total_scan_rows_num += scan_rows_num;
    stats->delta_scan_rows_num += scan_rows_num;
    stats->total_scan_bytes += scan_bytes;
    stats->delta_scan_bytes += scan_bytes;
}

void QueryRuntimeState::add_delta_stats(QueryStatistics* query_statistic) {
    query_statistic->add_cpu_costs(_delta_cpu_cost_ns.exchange(0));
    {
        std::lock_guard l(_scan_stats_lock);
        for (const auto& [table_id, scan_stats] : _scan_stats) {
            QueryStatisticsItemPB stats_item;
            stats_item.set_table_id(table_id);
            stats_item.set_scan_rows(scan_stats->delta_scan_rows_num.exchange(0));
            stats_item.set_scan_bytes(scan_stats->delta_scan_bytes.exchange(0));
            query_statistic->add_stats_item(stats_item);
        }
    }
    for (const auto& [node_id, exec_stats] : _node_exec_stats) {
        query_statistic->add_exec_stats_item(
                node_id, exec_stats->push_rows.exchange(0), exec_stats->pull_rows.exchange(0),
                exec_stats->pred_filter_rows.exchange(0), exec_stats->index_filter_rows.exchange(0),
                exec_stats->rf_filter_rows.exchange(0));
    }
}

void QueryRuntimeState::add_cumulative_stats(QueryStatistics* query_statistic) const {
    {
        std::lock_guard l(_scan_stats_lock);
        for (const auto& [table_id, scan_stats] : _scan_stats) {
            QueryStatisticsItemPB stats_item;
            stats_item.set_table_id(table_id);
            stats_item.set_scan_rows(scan_stats->total_scan_rows_num);
            stats_item.set_scan_bytes(scan_stats->total_scan_bytes);
            query_statistic->add_stats_item(stats_item);
        }
    }

    for (const auto& [node_id, exec_stats] : _node_exec_stats) {
        query_statistic->add_exec_stats_item(node_id, exec_stats->push_rows, exec_stats->pull_rows,
                                             exec_stats->pred_filter_rows, exec_stats->index_filter_rows,
                                             exec_stats->rf_filter_rows);
    }
}

} // namespace starrocks::pipeline
