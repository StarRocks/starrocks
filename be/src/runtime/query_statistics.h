// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/query_statistics.h

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

#pragma once

#include <mutex>

#include "gen_cpp/FrontendService.h"
#include "gen_cpp/data.pb.h"
#include "util/spinlock.h"

namespace starrocks {

class QueryStatisticsRecvr;

// This is responsible for collecting query statistics, usually it consists of
// two parts, one is current fragment or plan's statistics, the other is sub fragment
// or plan's statistics and QueryStatisticsRecvr is responsible for collecting it.
class QueryStatistics {
public:
    QueryStatistics() {}

    void set_returned_rows(int64_t num_rows) { this->returned_rows = num_rows; }

    void add_stats_item(QueryStatisticsItemPB& stats_item);
    void add_scan_stats(int64_t scan_rows, int64_t scan_bytes);
    void add_cpu_costs(int64_t cpu_ns) { this->cpu_ns += cpu_ns; }
    void add_mem_costs(int64_t bytes) { mem_cost_bytes += bytes; }

    void to_pb(PQueryStatistics* statistics);
    void to_params(TAuditStatistics* params);

    void merge(int sender_id, QueryStatistics& other);
    void merge_pb(const PQueryStatistics& statistics);

    int64_t get_scan_rows() const { return scan_rows; }
    int64_t get_mem_bytes() const { return mem_cost_bytes; }

    void clear();

private:
    void update_stats_item(int64_t table_id, int64_t scan_rows, int64_t scan_bytes);

    std::atomic_int64_t scan_rows{0};
    std::atomic_int64_t scan_bytes{0};
    std::atomic_int64_t cpu_ns{0};
    std::atomic_int64_t mem_cost_bytes{0};

    // number rows returned by query.
    // only set once by result sink when closing.
    int64_t returned_rows{0};
    struct ScanStats {
        ScanStats(int64_t rows, int64_t bytes) : scan_rows(rows), scan_bytes(bytes) {}
        int64_t scan_rows = 0;
        int64_t scan_bytes = 0;
    };
    SpinLock _lock;
    std::unordered_map<int64_t, std::shared_ptr<ScanStats>> _stats_items;
};

// It is used for collecting sub plan query statistics in DataStreamRecvr.
class QueryStatisticsRecvr {
public:
    ~QueryStatisticsRecvr();

    void insert(const PQueryStatistics& statistics, int sender_id);
    void aggregate(QueryStatistics* statistics);

private:
    std::map<int, QueryStatistics*> _query_statistics;
    SpinLock _lock;
};

} // namespace starrocks
