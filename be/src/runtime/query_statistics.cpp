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
    *statistics->mutable_stats_items() = {_stats_items.begin(), _stats_items.end()};
}

void QueryStatistics::clear() {
    scan_rows = 0;
    scan_bytes = 0;
    cpu_ns = 0;
    returned_rows = 0;
    spill_bytes = 0;
    _stats_items.clear();
}

void QueryStatistics::add_stats_item(QueryStatisticsItemPB& stats_item) {
    LOG(INFO) << "add stats item: " << stats_item.DebugString();
    this->_stats_items.emplace_back(stats_item);
    this->scan_rows += stats_item.scan_rows();
    this->scan_bytes += stats_item.scan_bytes();
}

void QueryStatistics::add_scan_stats(int64_t scan_rows, int64_t scan_bytes) {
    this->scan_rows += scan_rows;
    this->scan_bytes += scan_bytes;
}

void QueryStatistics::merge(int sender_id, QueryStatistics& other) {
    // Make the exchange action atomic
    int64_t scan_rows = other.scan_rows.load();
    this->scan_rows += scan_rows;
    other.scan_rows -= scan_rows;

    int64_t scan_bytes = other.scan_bytes.load();
    this->scan_bytes += scan_bytes;
    other.scan_bytes -= scan_bytes;

    int64_t cpu_ns = other.cpu_ns.load();
    this->cpu_ns += cpu_ns;
    other.cpu_ns -= cpu_ns;

    int64_t mem_cost_bytes = other.mem_cost_bytes.load();
    this->mem_cost_bytes = std::max<int64_t>(this->mem_cost_bytes, mem_cost_bytes);

    int64_t spill_bytes = other.spill_bytes.load();
    this->spill_bytes += spill_bytes;
    other.spill_bytes -= spill_bytes;

    _stats_items.insert(_stats_items.end(), other._stats_items.begin(), other._stats_items.end());
}

void QueryStatistics::merge_pb(const PQueryStatistics& statistics) {
    scan_rows += statistics.scan_rows();
    scan_bytes += statistics.scan_bytes();
    cpu_ns += statistics.cpu_cost_ns();
    spill_bytes += statistics.spill_bytes();
    mem_cost_bytes = std::max<int64_t>(mem_cost_bytes, statistics.mem_cost_bytes());
    _stats_items.insert(_stats_items.end(), statistics.stats_items().begin(), statistics.stats_items().end());
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
