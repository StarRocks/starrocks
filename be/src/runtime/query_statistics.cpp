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

namespace starrocks {
namespace {

void per_scan_counters_to_pb(int32_t plan_node_id, const ScanAuditCounters& c, PerScanStatsItemPB* item) {
    item->set_plan_node_id(plan_node_id);
    if (c.mask & ScanAuditCounters::PHYSICAL_ROWS) item->set_physical_rows_read(c.physical_rows);
    if (c.mask & ScanAuditCounters::LOGICAL_ROWS) item->set_logical_rows_read(c.logical_rows);
    if (c.mask & ScanAuditCounters::OUTPUT_ROWS) item->set_output_rows(c.output_rows);
    if (c.mask & ScanAuditCounters::REMOTE_BYTES) item->set_remote_bytes(c.remote_bytes);
    if (c.mask & ScanAuditCounters::CACHE_BYTES) item->set_cache_bytes(c.cache_bytes);
}

void per_scan_counters_to_thrift(int32_t plan_node_id, const ScanAuditCounters& c, TAuditPerScanStatsItem* item) {
    item->__set_plan_node_id(plan_node_id);
    if (c.mask & ScanAuditCounters::PHYSICAL_ROWS) item->__set_physical_rows_read(c.physical_rows);
    if (c.mask & ScanAuditCounters::LOGICAL_ROWS) item->__set_logical_rows_read(c.logical_rows);
    if (c.mask & ScanAuditCounters::OUTPUT_ROWS) item->__set_output_rows(c.output_rows);
    if (c.mask & ScanAuditCounters::REMOTE_BYTES) item->__set_remote_bytes(c.remote_bytes);
    if (c.mask & ScanAuditCounters::CACHE_BYTES) item->__set_cache_bytes(c.cache_bytes);
}

ScanAuditCounters per_scan_counters_from_pb(const PerScanStatsItemPB& item) {
    ScanAuditCounters c;
    if (item.has_physical_rows_read()) {
        c.mask |= ScanAuditCounters::PHYSICAL_ROWS;
        c.physical_rows = item.physical_rows_read();
    }
    if (item.has_logical_rows_read()) {
        c.mask |= ScanAuditCounters::LOGICAL_ROWS;
        c.logical_rows = item.logical_rows_read();
    }
    if (item.has_output_rows()) {
        c.mask |= ScanAuditCounters::OUTPUT_ROWS;
        c.output_rows = item.output_rows();
    }
    if (item.has_remote_bytes()) {
        c.mask |= ScanAuditCounters::REMOTE_BYTES;
        c.remote_bytes = item.remote_bytes();
    }
    if (item.has_cache_bytes()) {
        c.mask |= ScanAuditCounters::CACHE_BYTES;
        c.cache_bytes = item.cache_bytes();
    }
    return c;
}

} // namespace
} // namespace starrocks

namespace starrocks {

void QueryStatistics::to_pb(PQueryStatistics* statistics) {
    DCHECK(statistics != nullptr);
    statistics->set_scan_rows(scan_rows);
    statistics->set_scan_bytes(scan_bytes);
    statistics->set_returned_rows(returned_rows);
    statistics->set_cpu_cost_ns(cpu_ns);
    statistics->set_mem_cost_bytes(mem_cost_bytes);
    statistics->set_spill_bytes(spill_bytes);
    statistics->set_read_local_cnt(read_local_cnt);
    statistics->set_read_remote_cnt(read_remote_cnt);
    statistics->set_transmitted_bytes(transmitted_bytes);
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
            auto new_stats_item = statistics->add_stats_items();
            new_stats_item->set_table_id(table_id);
            new_stats_item->set_scan_rows(stats_item->scan_rows);
            new_stats_item->set_scan_bytes(stats_item->scan_bytes);
        }
        for (const auto& [plan_node_id, counters] : _per_scan_stats_items) {
            per_scan_counters_to_pb(plan_node_id, counters, statistics->add_per_scan_stats_items());
        }
    }

    for (const auto& [node_id, exec_stats_item] : _exec_stats_items) {
        auto new_exec_stats_item = statistics->add_node_exec_stats_items();
        new_exec_stats_item->set_node_id(node_id);
        new_exec_stats_item->set_push_rows(exec_stats_item->push_rows);
        new_exec_stats_item->set_pull_rows(exec_stats_item->pull_rows);
        new_exec_stats_item->set_index_filter_rows(exec_stats_item->index_filter_rows);
        new_exec_stats_item->set_rf_filter_rows(exec_stats_item->rf_filter_rows);
        new_exec_stats_item->set_pred_filter_rows(exec_stats_item->pred_filter_rows);
    }
}

void QueryStatistics::to_params(TAuditStatistics* params) {
    DCHECK(params != nullptr);
    params->__set_scan_rows(scan_rows);
    params->__set_scan_bytes(scan_bytes);
    params->__set_returned_rows(returned_rows);
    params->__set_cpu_cost_ns(cpu_ns);
    params->__set_mem_cost_bytes(mem_cost_bytes);
    params->__set_spill_bytes(spill_bytes);
    params->__set_read_local_cnt(read_local_cnt);
    params->__set_read_remote_cnt(read_remote_cnt);
    params->__set_transmitted_bytes(transmitted_bytes);
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
            auto new_stats_item = params->stats_items.emplace_back();
            new_stats_item.__set_table_id(table_id);
            new_stats_item.__set_scan_rows(stats_item->scan_rows);
            new_stats_item.__set_scan_bytes(stats_item->scan_bytes);
        }
        if (!_per_scan_stats_items.empty()) {
            params->__isset.per_scan_stats_items = true;
            for (const auto& [plan_node_id, counters] : _per_scan_stats_items) {
                per_scan_counters_to_thrift(plan_node_id, counters, &params->per_scan_stats_items.emplace_back());
            }
        }
    }
}

void QueryStatistics::clear() {
    scan_rows = 0;
    scan_bytes = 0;
    cpu_ns = 0;
    mem_cost_bytes = 0;
    returned_rows = 0;
    spill_bytes = 0;
    read_local_cnt = 0;
    read_remote_cnt = 0;
    transmitted_bytes = 0;
    _stats_items.clear();
    _exec_stats_items.clear();
    {
        std::lock_guard l(_lock);
        _ai_statistics.reset();
        _per_scan_stats_items.clear();
    }
}

void QueryStatistics::update_stats_item(int64_t table_id, int64_t scan_rows, int64_t scan_bytes) {
    if (table_id > 0 && (scan_rows > 0 || scan_bytes > 0)) {
        auto iter = _stats_items.find(table_id);
        if (iter == _stats_items.end()) {
            _stats_items.insert({table_id, std::make_shared<ScanStats>(scan_rows, scan_bytes)});
        } else {
            iter->second->scan_rows += scan_rows;
            iter->second->scan_bytes += scan_bytes;
        }
    }
}

void QueryStatistics::update_per_scan_stats_item(int32_t plan_node_id, const ScanAuditCounters& counters) {
    _per_scan_stats_items[plan_node_id].add(counters);
}

void QueryStatistics::add_per_scan_stats(int32_t plan_node_id, const ScanAuditCounters& counters) {
    if (counters.empty()) {
        return;
    }
    std::lock_guard l(_lock);
    update_per_scan_stats_item(plan_node_id, counters);
}

void QueryStatistics::update_exec_stats_item(uint32_t node_id, int64_t push, int64_t pull, int64_t pred_filter,
                                             int64_t index_filter, int64_t rf_filter) {
    auto iter = _exec_stats_items.find(node_id);
    if (iter == _exec_stats_items.end()) {
        _exec_stats_items.insert(
                {node_id, std::make_shared<NodeExecStats>(push, pull, pred_filter, index_filter, rf_filter)});
    } else {
        iter->second->push_rows += push;
        iter->second->pull_rows += pull;
        iter->second->pred_filter_rows += pred_filter;
        iter->second->index_filter_rows += index_filter;
        iter->second->rf_filter_rows += rf_filter;
    }
}

void QueryStatistics::add_stats_item(QueryStatisticsItemPB& stats_item) {
    {
        std::lock_guard l(_lock);
        update_stats_item(stats_item.table_id(), stats_item.scan_rows(), stats_item.scan_bytes());
    }
    this->scan_rows += stats_item.scan_rows();
    this->scan_bytes += stats_item.scan_bytes();
}

void QueryStatistics::add_exec_stats_item(uint32_t node_id, int64_t push, int64_t pull, int64_t pred_filter,
                                          int64_t index_filter, int64_t rf_filter) {
    update_exec_stats_item(node_id, push, pull, pred_filter, index_filter, rf_filter);
}

void QueryStatistics::add_scan_stats(int64_t scan_rows, int64_t scan_bytes) {
    this->scan_rows += scan_rows;
    this->scan_bytes += scan_bytes;
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
    // Make the exchange action atomic
    int64_t scan_rows = other.scan_rows.load();
    if (other.scan_rows.compare_exchange_strong(scan_rows, 0)) {
        this->scan_rows += scan_rows;
    }

    int64_t scan_bytes = other.scan_bytes.load();
    if (other.scan_bytes.compare_exchange_strong(scan_bytes, 0)) {
        this->scan_bytes += scan_bytes;
    }

    int64_t cpu_ns = other.cpu_ns.load();
    if (other.cpu_ns.compare_exchange_strong(cpu_ns, 0)) {
        this->cpu_ns += cpu_ns;
        DCHECK(this->cpu_ns >= 0);
    }

    int64_t mem_cost_bytes = other.mem_cost_bytes.load();
    this->mem_cost_bytes = std::max<int64_t>(this->mem_cost_bytes, mem_cost_bytes);

    int64_t spill_bytes = other.spill_bytes.load();
    if (other.spill_bytes.compare_exchange_strong(spill_bytes, 0)) {
        this->spill_bytes += spill_bytes;
    }

    int64_t read_local_cnt = other.read_local_cnt.load();
    if (other.read_local_cnt.compare_exchange_strong(read_local_cnt, 0)) {
        this->read_local_cnt += read_local_cnt;
    }

    int64_t read_remote_cnt = other.read_remote_cnt.load();
    if (other.read_remote_cnt.compare_exchange_strong(read_remote_cnt, 0)) {
        this->read_remote_cnt += read_remote_cnt;
    }

    int64_t transmitted_bytes = other.transmitted_bytes.load();
    if (other.transmitted_bytes.compare_exchange_strong(transmitted_bytes, 0)) {
        this->transmitted_bytes += transmitted_bytes;
    }

    {
        std::unordered_map<int64_t, std::shared_ptr<ScanStats>> other_stats_item;
        std::unordered_map<uint32_t, std::shared_ptr<NodeExecStats>> other_exec_stats_items;
        std::unique_ptr<AIExecutionStatistics> other_ai_statistics;
        std::unordered_map<int32_t, ScanAuditCounters> other_per_scan_stats_items;
        {
            std::lock_guard l(other._lock);
            other_stats_item.swap(other._stats_items);
            other_per_scan_stats_items.swap(other._per_scan_stats_items);
            other_exec_stats_items.swap(other._exec_stats_items);
            other_ai_statistics.swap(other._ai_statistics);
        }
        std::lock_guard l(_lock);
        if (other_ai_statistics != nullptr) {
            if (_ai_statistics == nullptr) {
                _ai_statistics = std::move(other_ai_statistics);
            } else {
                _ai_statistics->add(*other_ai_statistics);
            }
        }
        for (const auto& [table_id, stats_item] : other_stats_item) {
            update_stats_item(table_id, stats_item->scan_rows, stats_item->scan_bytes);
        }
        for (const auto& [plan_node_id, counters] : other_per_scan_stats_items) {
            update_per_scan_stats_item(plan_node_id, counters);
        }
        for (const auto& [node_id, exec_stats_item] : other_exec_stats_items) {
            update_exec_stats_item(node_id, exec_stats_item->push_rows, exec_stats_item->pull_rows,
                                   exec_stats_item->pred_filter_rows, exec_stats_item->index_filter_rows,
                                   exec_stats_item->rf_filter_rows);
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
        scan_rows += statistics.scan_rows();
    }
    if (statistics.has_scan_bytes()) {
        scan_bytes += statistics.scan_bytes();
    }
    if (statistics.has_cpu_cost_ns()) {
        cpu_ns += statistics.cpu_cost_ns();
        DCHECK(cpu_ns >= 0);
    }
    if (statistics.has_spill_bytes()) {
        spill_bytes += statistics.spill_bytes();
    }
    if (statistics.has_read_local_cnt()) {
        read_local_cnt += statistics.read_local_cnt();
    }
    if (statistics.has_read_remote_cnt()) {
        read_remote_cnt += statistics.read_remote_cnt();
    }
    if (statistics.has_mem_cost_bytes()) {
        mem_cost_bytes = std::max<int64_t>(mem_cost_bytes, statistics.mem_cost_bytes());
    }
    if (statistics.has_transmitted_bytes()) {
        transmitted_bytes += statistics.transmitted_bytes();
    }
    {
        std::lock_guard l(_lock);
        for (int i = 0; i < statistics.stats_items_size(); ++i) {
            const auto& stats_item = statistics.stats_items(i);
            update_stats_item(stats_item.table_id(), stats_item.scan_rows(), stats_item.scan_bytes());
        }
        for (const auto& item : statistics.per_scan_stats_items()) {
            update_per_scan_stats_item(item.plan_node_id(), per_scan_counters_from_pb(item));
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
    QueryStatistics* query_statistics = nullptr;
    _query_statistics.lazy_emplace_l(
            sender_id, [&](QueryStatistics* const& existing) { query_statistics = existing; },
            [&](const auto& ctor) {
                query_statistics = new QueryStatistics;
                ctor(sender_id, query_statistics);
            });
    query_statistics->merge_pb(statistics);
}

void QueryStatisticsRecvr::aggregate(QueryStatistics* statistics) {
    _query_statistics.for_each([&statistics](const auto& pair) { statistics->merge(pair.first, *pair.second); });
}

QueryStatisticsRecvr::~QueryStatisticsRecvr() {
    _query_statistics.for_each_m([](auto& pair) { delete pair.second; });
    _query_statistics.clear();
}

} // namespace starrocks
