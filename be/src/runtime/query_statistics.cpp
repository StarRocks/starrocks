// This file is made available under Elastic License 2.0.
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
<<<<<<< HEAD
    *statistics->mutable_stats_items() = {_stats_items.begin(), _stats_items.end()};
=======
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
>>>>>>> 4265212f40 ([BugFix] fix incorrect scan metrics in FE (#27779))
}

void QueryStatistics::clear() {
    scan_rows = 0;
    scan_bytes = 0;
    cpu_ns = 0;
    returned_rows = 0;
    _stats_items.clear();
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

void QueryStatistics::add_stats_item(QueryStatisticsItemPB& stats_item) {
    {
        std::lock_guard l(_lock);
        update_stats_item(stats_item.table_id(), stats_item.scan_rows(), stats_item.scan_bytes());
    }
    this->scan_rows += stats_item.scan_rows();
    this->scan_bytes += stats_item.scan_bytes();
}

void QueryStatistics::add_scan_stats(int64_t scan_rows, int64_t scan_bytes) {
    this->scan_rows += scan_rows;
    this->scan_bytes += scan_bytes;
}

void QueryStatistics::merge(int sender_id, QueryStatistics& other) {
    // Make the exchange action atomic
    int64_t rows = other.scan_rows.load();
    scan_rows += rows;
    other.scan_rows -= rows;

    int64_t bytes = other.scan_bytes.load();
    scan_bytes += bytes;
    other.scan_bytes -= bytes;

    int64_t cpu_ns = other.cpu_ns.load();
    cpu_ns += cpu_ns;
    other.cpu_ns -= cpu_ns;

    int64_t mem_bytes = other.mem_cost_bytes.load();
    mem_cost_bytes = std::max<int64_t>(mem_cost_bytes, mem_bytes);

    {
        std::unordered_map<int64_t, std::shared_ptr<ScanStats>> other_stats_item;
        {
            std::lock_guard l(other._lock);
            other_stats_item.swap(other._stats_items);
        }
        std::lock_guard l(_lock);
        for (const auto& [table_id, stats_item] : other_stats_item) {
            update_stats_item(table_id, stats_item->scan_rows, stats_item->scan_bytes);
        }
    }
}

void QueryStatistics::merge_pb(const PQueryStatistics& statistics) {
    scan_rows += statistics.scan_rows();
    scan_bytes += statistics.scan_bytes();
    cpu_ns += statistics.cpu_cost_ns();
    mem_cost_bytes = std::max<int64_t>(mem_cost_bytes, statistics.mem_cost_bytes());
    {
        std::lock_guard l(_lock);
        for (int i = 0; i < statistics.stats_items_size(); ++i) {
            const auto& stats_item = statistics.stats_items(i);
            update_stats_item(stats_item.table_id(), stats_item.scan_rows(), stats_item.scan_bytes());
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
