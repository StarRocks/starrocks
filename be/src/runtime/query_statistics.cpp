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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/query_statistics.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/query_statistics.h"

#include <algorithm>
#include <vector>

namespace starrocks {

namespace {

inline void atomic_max(std::atomic_int64_t* target, int64_t value) {
    int64_t prev = target->load(std::memory_order_relaxed);
    while (prev < value &&
           !target->compare_exchange_weak(prev, value, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
}

inline size_t thread_stripe_index() {
    static std::atomic<size_t> next_thread_id{0};
    thread_local const size_t tls_stripe =
            next_thread_id.fetch_add(1, std::memory_order_relaxed) & (QueryStatisticsRecvr::kNumStripes - 1);
    return tls_stripe;
}

} // namespace

void QueryStatistics::update_max_mem_costs(int64_t bytes) {
    atomic_max(&mem_cost_bytes, bytes);
}

void QueryStatistics::to_pb(PQueryStatistics* statistics) {
    DCHECK(statistics != nullptr);
    statistics->set_scan_rows(scan_rows.load(std::memory_order_relaxed));
    statistics->set_scan_bytes(scan_bytes.load(std::memory_order_relaxed));
    statistics->set_returned_rows(returned_rows);
    statistics->set_cpu_cost_ns(cpu_ns.load(std::memory_order_relaxed));
    statistics->set_mem_cost_bytes(mem_cost_bytes.load(std::memory_order_relaxed));
    statistics->set_spill_bytes(spill_bytes.load(std::memory_order_relaxed));
    statistics->set_read_local_cnt(read_local_cnt.load(std::memory_order_relaxed));
    statistics->set_read_remote_cnt(read_remote_cnt.load(std::memory_order_relaxed));
    statistics->set_transmitted_bytes(transmitted_bytes.load(std::memory_order_relaxed));
    {
        std::lock_guard l(_lock);
        if (_ai_statistics != nullptr && !_ai_statistics->empty()) {
            auto* ai = statistics->mutable_ai_statistics();
            ai->set_task_count(_ai_statistics->task_count);
            ai->set_request_count(_ai_statistics->request_count);
            ai->set_retry_count(_ai_statistics->retry_count);
            ai->set_timeout_count(_ai_statistics->timeout_count);
            ai->set_error_count(_ai_statistics->error_count);
            ai->set_http_time_ns(_ai_statistics->http_time_ns);
            ai->set_prompt_tokens(_ai_statistics->prompt_tokens);
            ai->set_completion_tokens(_ai_statistics->completion_tokens);
            ai->set_total_tokens(_ai_statistics->total_tokens);
            ai->set_prompt_usage_count(_ai_statistics->prompt_usage_count);
            ai->set_completion_usage_count(_ai_statistics->completion_usage_count);
            ai->set_total_usage_count(_ai_statistics->total_usage_count);
        } else {
            statistics->clear_ai_statistics();
        }
        for (const auto& [table_id, stats_item] : _stats_items) {
            if (stats_item.empty()) {
                continue;
            }
            auto* new_stats_item = statistics->add_stats_items();
            new_stats_item->set_table_id(table_id);
            new_stats_item->set_scan_rows(stats_item.scan_rows);
            new_stats_item->set_scan_bytes(stats_item.scan_bytes);
        }
        for (const auto& [node_id, exec_stats_item] : _exec_stats_items) {
            if (exec_stats_item.empty()) {
                continue;
            }
            auto* new_exec_stats_item = statistics->add_node_exec_stats_items();
            new_exec_stats_item->set_node_id(node_id);
            new_exec_stats_item->set_push_rows(exec_stats_item.push_rows);
            new_exec_stats_item->set_pull_rows(exec_stats_item.pull_rows);
            new_exec_stats_item->set_index_filter_rows(exec_stats_item.index_filter_rows);
            new_exec_stats_item->set_rf_filter_rows(exec_stats_item.rf_filter_rows);
            new_exec_stats_item->set_pred_filter_rows(exec_stats_item.pred_filter_rows);
        }
    }
}

void QueryStatistics::to_params(TAuditStatistics* params) {
    DCHECK(params != nullptr);
    params->__set_scan_rows(scan_rows.load(std::memory_order_relaxed));
    params->__set_scan_bytes(scan_bytes.load(std::memory_order_relaxed));
    params->__set_returned_rows(returned_rows);
    params->__set_cpu_cost_ns(cpu_ns.load(std::memory_order_relaxed));
    params->__set_mem_cost_bytes(mem_cost_bytes.load(std::memory_order_relaxed));
    params->__set_spill_bytes(spill_bytes.load(std::memory_order_relaxed));
    params->__set_read_local_cnt(read_local_cnt.load(std::memory_order_relaxed));
    params->__set_read_remote_cnt(read_remote_cnt.load(std::memory_order_relaxed));
    params->__set_transmitted_bytes(transmitted_bytes.load(std::memory_order_relaxed));
    {
        std::lock_guard l(_lock);
        if (_ai_statistics != nullptr && !_ai_statistics->empty()) {
            TAIExecutionStatistics ai;
            ai.__set_task_count(_ai_statistics->task_count);
            ai.__set_request_count(_ai_statistics->request_count);
            ai.__set_retry_count(_ai_statistics->retry_count);
            ai.__set_timeout_count(_ai_statistics->timeout_count);
            ai.__set_error_count(_ai_statistics->error_count);
            ai.__set_http_time_ns(_ai_statistics->http_time_ns);
            ai.__set_prompt_tokens(_ai_statistics->prompt_tokens);
            ai.__set_completion_tokens(_ai_statistics->completion_tokens);
            ai.__set_total_tokens(_ai_statistics->total_tokens);
            ai.__set_prompt_usage_count(_ai_statistics->prompt_usage_count);
            ai.__set_completion_usage_count(_ai_statistics->completion_usage_count);
            ai.__set_total_usage_count(_ai_statistics->total_usage_count);
            params->__set_ai_statistics(ai);
        } else {
            params->__isset.ai_statistics = false;
        }
        for (const auto& [table_id, stats_item] : _stats_items) {
            if (stats_item.empty()) {
                continue;
            }
            auto& new_stats_item = params->stats_items.emplace_back();
            new_stats_item.__set_table_id(table_id);
            new_stats_item.__set_scan_rows(stats_item.scan_rows);
            new_stats_item.__set_scan_bytes(stats_item.scan_bytes);
        }
    }
}

void QueryStatistics::clear() {
    scan_rows.store(0, std::memory_order_relaxed);
    scan_bytes.store(0, std::memory_order_relaxed);
    cpu_ns.store(0, std::memory_order_relaxed);
    mem_cost_bytes.store(0, std::memory_order_relaxed);
    returned_rows = 0;
    spill_bytes.store(0, std::memory_order_relaxed);
    read_local_cnt.store(0, std::memory_order_relaxed);
    read_remote_cnt.store(0, std::memory_order_relaxed);
    transmitted_bytes.store(0, std::memory_order_relaxed);
    {
        std::lock_guard l(_lock);
        _stats_items.clear();
        _exec_stats_items.clear();
        _has_map_updates = false;
        _ai_statistics.reset();
    }
}

void QueryStatistics::update_stats_item(int64_t table_id, int64_t scan_rows, int64_t scan_bytes) {
    if (table_id > 0 && (scan_rows > 0 || scan_bytes > 0)) {
        auto& item = _stats_items[table_id];
        item.scan_rows += scan_rows;
        item.scan_bytes += scan_bytes;
        _has_map_updates = true;
    }
}

void QueryStatistics::update_exec_stats_item(uint32_t node_id, int64_t push, int64_t pull, int64_t pred_filter,
                                             int64_t index_filter, int64_t rf_filter) {
    if (push == 0 && pull == 0 && pred_filter == 0 && index_filter == 0 && rf_filter == 0) {
        return;
    }
    auto& item = _exec_stats_items[node_id];
    item.push_rows += push;
    item.pull_rows += pull;
    item.pred_filter_rows += pred_filter;
    item.index_filter_rows += index_filter;
    item.rf_filter_rows += rf_filter;
    _has_map_updates = true;
}

void QueryStatistics::add_stats_item(QueryStatisticsItemPB& stats_item) {
    {
        std::lock_guard l(_lock);
        update_stats_item(stats_item.table_id(), stats_item.scan_rows(), stats_item.scan_bytes());
    }
    this->scan_rows.fetch_add(stats_item.scan_rows(), std::memory_order_relaxed);
    this->scan_bytes.fetch_add(stats_item.scan_bytes(), std::memory_order_relaxed);
}

void QueryStatistics::add_exec_stats_item(uint32_t node_id, int64_t push, int64_t pull, int64_t pred_filter,
                                          int64_t index_filter, int64_t rf_filter) {
    std::lock_guard l(_lock);
    update_exec_stats_item(node_id, push, pull, pred_filter, index_filter, rf_filter);
}

void QueryStatistics::add_scan_stats(int64_t scan_rows, int64_t scan_bytes) {
    this->scan_rows.fetch_add(scan_rows, std::memory_order_relaxed);
    this->scan_bytes.fetch_add(scan_bytes, std::memory_order_relaxed);
}

void QueryStatistics::add_ai_statistics(const AIExecutionStatistics& statistics) {
    if (statistics.empty()) {
        return;
    }
    std::lock_guard l(_lock);
    if (_ai_statistics == nullptr) {
        _ai_statistics = std::make_unique<AIExecutionStatistics>();
    }
    _ai_statistics->add(statistics);
}

void QueryStatistics::merge(int sender_id, QueryStatistics& other) {
    (void)sender_id;
    if (this == &other) {
        return;
    }

    if (const int64_t rows = other.scan_rows.exchange(0, std::memory_order_relaxed); rows != 0) {
        this->scan_rows.fetch_add(rows, std::memory_order_relaxed);
    }

    if (const int64_t bytes = other.scan_bytes.exchange(0, std::memory_order_relaxed); bytes != 0) {
        this->scan_bytes.fetch_add(bytes, std::memory_order_relaxed);
    }

    if (const int64_t cpu = other.cpu_ns.exchange(0, std::memory_order_relaxed); cpu != 0) {
        this->cpu_ns.fetch_add(cpu, std::memory_order_relaxed);
        DCHECK(this->cpu_ns.load(std::memory_order_relaxed) >= 0);
    }

    atomic_max(&this->mem_cost_bytes, other.mem_cost_bytes.load(std::memory_order_relaxed));

    if (const int64_t spill = other.spill_bytes.exchange(0, std::memory_order_relaxed); spill != 0) {
        this->spill_bytes.fetch_add(spill, std::memory_order_relaxed);
    }

    if (const int64_t local_cnt = other.read_local_cnt.exchange(0, std::memory_order_relaxed); local_cnt != 0) {
        this->read_local_cnt.fetch_add(local_cnt, std::memory_order_relaxed);
    }

    if (const int64_t remote_cnt = other.read_remote_cnt.exchange(0, std::memory_order_relaxed); remote_cnt != 0) {
        this->read_remote_cnt.fetch_add(remote_cnt, std::memory_order_relaxed);
    }

    if (const int64_t transmitted = other.transmitted_bytes.exchange(0, std::memory_order_relaxed); transmitted != 0) {
        this->transmitted_bytes.fetch_add(transmitted, std::memory_order_relaxed);
    }

    struct DrainedScanItem {
        int64_t table_id;
        ScanStats stats;
    };
    struct DrainedExecItem {
        uint32_t node_id;
        NodeExecStats stats;
    };
    std::vector<DrainedScanItem> drained_scan_items;
    std::vector<DrainedExecItem> drained_exec_items;
    std::unique_ptr<AIExecutionStatistics> other_ai_statistics;
    {
        std::lock_guard l(other._lock);
        if (other._has_map_updates) {
            if (!other._stats_items.empty()) {
                drained_scan_items.reserve(other._stats_items.size());
                for (auto& [table_id, stats_item] : other._stats_items) {
                    if (!stats_item.empty()) {
                        drained_scan_items.push_back({table_id, stats_item});
                        stats_item = ScanStats{};
                    }
                }
            }
            if (!other._exec_stats_items.empty()) {
                drained_exec_items.reserve(other._exec_stats_items.size());
                for (auto& [node_id, exec_stats_item] : other._exec_stats_items) {
                    if (!exec_stats_item.empty()) {
                        drained_exec_items.push_back({node_id, exec_stats_item});
                        exec_stats_item = NodeExecStats{};
                    }
                }
            }
            other._has_map_updates = false;
        }
        other_ai_statistics.swap(other._ai_statistics);
    }

    if (other_ai_statistics != nullptr || !drained_scan_items.empty() || !drained_exec_items.empty()) {
        std::lock_guard l(_lock);
        if (other_ai_statistics != nullptr) {
            if (_ai_statistics == nullptr) {
                _ai_statistics = std::move(other_ai_statistics);
            } else {
                _ai_statistics->add(*other_ai_statistics);
            }
        }
        for (const auto& item : drained_scan_items) {
            update_stats_item(item.table_id, item.stats.scan_rows, item.stats.scan_bytes);
        }
        for (const auto& item : drained_exec_items) {
            update_exec_stats_item(item.node_id, item.stats.push_rows, item.stats.pull_rows,
                                   item.stats.pred_filter_rows, item.stats.index_filter_rows,
                                   item.stats.rf_filter_rows);
        }
    }
}

void QueryStatistics::merge_pb(const PQueryStatistics& statistics) {
    if (statistics.has_ai_statistics()) {
        const auto& ai = statistics.ai_statistics();
        AIExecutionStatistics incoming;
        incoming.task_count = ai.task_count();
        incoming.request_count = ai.request_count();
        incoming.retry_count = ai.retry_count();
        incoming.timeout_count = ai.timeout_count();
        incoming.error_count = ai.error_count();
        incoming.http_time_ns = ai.http_time_ns();
        // Accept a reported token value only together with its valid coverage.
        // Otherwise separate partial messages could fabricate an observed value on merge.
        if (ai.has_prompt_tokens() && ai.prompt_tokens() >= 0 && ai.prompt_usage_count() > 0) {
            incoming.prompt_tokens = ai.prompt_tokens();
            incoming.prompt_usage_count = ai.prompt_usage_count();
        }
        if (ai.has_completion_tokens() && ai.completion_tokens() >= 0 && ai.completion_usage_count() > 0) {
            incoming.completion_tokens = ai.completion_tokens();
            incoming.completion_usage_count = ai.completion_usage_count();
        }
        if (ai.has_total_tokens() && ai.total_tokens() >= 0 && ai.total_usage_count() > 0) {
            incoming.total_tokens = ai.total_tokens();
            incoming.total_usage_count = ai.total_usage_count();
        }
        add_ai_statistics(incoming);
    }
    if (statistics.has_scan_rows()) {
        scan_rows.fetch_add(statistics.scan_rows(), std::memory_order_relaxed);
    }
    if (statistics.has_scan_bytes()) {
        scan_bytes.fetch_add(statistics.scan_bytes(), std::memory_order_relaxed);
    }
    if (statistics.has_cpu_cost_ns()) {
        cpu_ns.fetch_add(statistics.cpu_cost_ns(), std::memory_order_relaxed);
        DCHECK(cpu_ns.load(std::memory_order_relaxed) >= 0);
    }
    if (statistics.has_spill_bytes()) {
        spill_bytes.fetch_add(statistics.spill_bytes(), std::memory_order_relaxed);
    }
    if (statistics.has_read_local_cnt()) {
        read_local_cnt.fetch_add(statistics.read_local_cnt(), std::memory_order_relaxed);
    }
    if (statistics.has_read_remote_cnt()) {
        read_remote_cnt.fetch_add(statistics.read_remote_cnt(), std::memory_order_relaxed);
    }
    if (statistics.has_mem_cost_bytes()) {
        atomic_max(&mem_cost_bytes, statistics.mem_cost_bytes());
    }
    if (statistics.has_transmitted_bytes()) {
        transmitted_bytes.fetch_add(statistics.transmitted_bytes(), std::memory_order_relaxed);
    }
    if (statistics.stats_items_size() > 0 || statistics.node_exec_stats_items_size() > 0) {
        std::lock_guard l(_lock);
        for (int i = 0; i < statistics.stats_items_size(); ++i) {
            const auto& stats_item = statistics.stats_items(i);
            update_stats_item(stats_item.table_id(), stats_item.scan_rows(), stats_item.scan_bytes());
        }
        for (int i = 0; i < statistics.node_exec_stats_items_size(); ++i) {
            const auto& exec_stats_item = statistics.node_exec_stats_items(i);
            update_exec_stats_item(exec_stats_item.node_id(), exec_stats_item.push_rows(), exec_stats_item.pull_rows(),
                                   exec_stats_item.pred_filter_rows(), exec_stats_item.index_filter_rows(),
                                   exec_stats_item.rf_filter_rows());
        }
    }
}

void QueryStatisticsRecvr::insert(const PQueryStatistics& statistics, int sender_id) {
    (void)sender_id;
    if (statistics.has_mem_cost_bytes()) {
        atomic_max(&_max_mem_cost_bytes, statistics.mem_cost_bytes());
    }
    auto& stripe = _stripes[thread_stripe_index()];
    stripe.stats.merge_pb(statistics);
    stripe.dirty.store(true, std::memory_order_release);
}

void QueryStatisticsRecvr::aggregate(QueryStatistics* statistics) {
    DCHECK(statistics != nullptr);
    statistics->update_max_mem_costs(_max_mem_cost_bytes.load(std::memory_order_relaxed));
    for (size_t i = 0; i < kNumStripes; ++i) {
        if (_stripes[i].dirty.exchange(false, std::memory_order_acq_rel)) {
            statistics->merge(static_cast<int>(i), _stripes[i].stats);
        }
    }
}

} // namespace starrocks
