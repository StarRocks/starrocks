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

void QueryStatistics::to_pb(PQueryStatistics* statistics) {
    DCHECK(statistics != nullptr);
    statistics->set_scan_rows(scan_rows);
    statistics->set_scan_bytes(scan_bytes);
    statistics->set_returned_rows(returned_rows);
    statistics->set_cpu_cost_ns(cpu_ns);
    statistics->set_mem_cost_bytes(mem_cost_bytes);
    statistics->set_spill_bytes(spill_bytes);
    {
        std::lock_guard l(_lock);
        for (const auto& [table_id, stats_item] : _stats_items) {
            auto new_stats_item = statistics->add_stats_items();
            new_stats_item->set_table_id(table_id);
            new_stats_item->set_scan_rows(stats_item->scan_rows);
            new_stats_item->set_scan_bytes(stats_item->scan_bytes);
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
    {
        std::lock_guard l(_lock);
        for (const auto& [table_id, stats_item] : _stats_items) {
            auto new_stats_item = params->stats_items.emplace_back();
            new_stats_item.__set_table_id(table_id);
            new_stats_item.__set_scan_rows(stats_item->scan_rows);
            new_stats_item.__set_scan_bytes(stats_item->scan_bytes);
        }
    }
}

void QueryStatistics::clear() {
    scan_rows = 0;
    scan_bytes = 0;
    cpu_ns = 0;
    returned_rows = 0;
    spill_bytes = 0;
    _stats_items.clear();
    _exec_stats_items.clear();
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

    {
        std::unordered_map<int64_t, std::shared_ptr<ScanStats>> other_stats_item;
        std::unordered_map<uint32_t, std::shared_ptr<NodeExecStats>> other_exec_stats_items;
        {
            std::lock_guard l(other._lock);
            other_stats_item.swap(other._stats_items);
            other_exec_stats_items.swap(other._exec_stats_items);
        }
        std::lock_guard l(_lock);
        for (const auto& [table_id, stats_item] : other_stats_item) {
            update_stats_item(table_id, stats_item->scan_rows, stats_item->scan_bytes);
        }
        for (const auto& [node_id, exec_stats_item] : other_exec_stats_items) {
            update_exec_stats_item(node_id, exec_stats_item->push_rows, exec_stats_item->pull_rows,
                                   exec_stats_item->pred_filter_rows, exec_stats_item->index_filter_rows,
                                   exec_stats_item->rf_filter_rows);
        }
    }
}

void QueryStatistics::merge_pb(const PQueryStatistics& statistics) {
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
    if (statistics.has_mem_cost_bytes()) {
        mem_cost_bytes = std::max<int64_t>(mem_cost_bytes, statistics.mem_cost_bytes());
    }
    {
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
    std::lock_guard<SpinLock> l(_lock);
    QueryStatistics* query_statistics = nullptr;
    auto iter = _query_statistics.find(sender_id);
    if (iter == _query_statistics.end()) {
        query_statistics = new QueryStatistics;
        _query_statistics[sender_id] = query_statistics;
    } else {
        query_statistics = iter->second;
    }
    query_statistics->merge_pb(statistics);
}

void QueryStatisticsRecvr::aggregate(QueryStatistics* statistics) {
    std::lock_guard<SpinLock> l(_lock);
    for (auto& pair : _query_statistics) {
        statistics->merge(pair.first, *pair.second);
    }
}

QueryStatisticsRecvr::~QueryStatisticsRecvr() {
    // It is unnecessary to lock here, because the destructor will be
    // called alter DataStreamRecvr's close in ExchangeNode.
    for (auto& pair : _query_statistics) {
        delete pair.second;
    }
    _query_statistics.clear();
}

} // namespace starrocks
