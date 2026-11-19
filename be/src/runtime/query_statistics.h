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

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "base/concurrency/spinlock.h"
#include "base/phmap/phmap.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/data.pb.h"
#include "platform/llm/ai_execution_statistics.h"

namespace starrocks {

class QueryStatisticsRecvr;

// This is responsible for collecting query statistics, usually it consists of
// two parts, one is current fragment or plan's statistics, the other is sub fragment
// or plan's statistics and QueryStatisticsRecvr is responsible for collecting it.
class QueryStatistics {
public:
    QueryStatistics() = default;

    void set_returned_rows(int64_t num_rows) { this->returned_rows = num_rows; }

    void add_stats_item(QueryStatisticsItemPB& stats_item);
    void add_exec_stats_item(uint32_t node_id, int64_t push, int64_t pull, int64_t pred_filter, int64_t index_filter,
                             int64_t rf_filter);
    void add_scan_stats(int64_t scan_rows, int64_t scan_bytes);
    void add_cpu_costs(int64_t cpu_ns) { this->cpu_ns.fetch_add(cpu_ns, std::memory_order_relaxed); }
    void add_mem_costs(int64_t bytes) { mem_cost_bytes.fetch_add(bytes, std::memory_order_relaxed); }
    void update_max_mem_costs(int64_t bytes);
    void add_spill_bytes(int64_t bytes) { spill_bytes.fetch_add(bytes, std::memory_order_relaxed); }
    void add_read_stats(int64_t local_cnt, int64_t remote_cnt) {
        read_local_cnt.fetch_add(local_cnt, std::memory_order_relaxed);
        read_remote_cnt.fetch_add(remote_cnt, std::memory_order_relaxed);
    }
    void add_transmitted_bytes(int64_t bytes) { transmitted_bytes.fetch_add(bytes, std::memory_order_relaxed); }
    void add_ai_statistics(const AIExecutionStatistics& statistics);

    void to_pb(PQueryStatistics* statistics);
    void to_params(TAuditStatistics* params);

    void merge(int sender_id, QueryStatistics& other);
    void merge_pb(const PQueryStatistics& statistics);

    int64_t get_scan_rows() const { return scan_rows.load(std::memory_order_relaxed); }
    int64_t get_mem_bytes() const { return mem_cost_bytes.load(std::memory_order_relaxed); }
    int64_t get_transmitted_bytes() const { return transmitted_bytes.load(std::memory_order_relaxed); }
    int64_t get_cpu_ns() const { return cpu_ns.load(std::memory_order_relaxed); }
    int64_t get_read_local_cnt() const { return read_local_cnt.load(std::memory_order_relaxed); }
    int64_t get_read_remote_cnt() const { return read_remote_cnt.load(std::memory_order_relaxed); }

    void clear();

private:
    void update_stats_item(int64_t table_id, int64_t scan_rows, int64_t scan_bytes);

    void update_exec_stats_item(uint32_t node_id, int64_t push, int64_t pull, int64_t pred_filter, int64_t index_filter,
                                int64_t rf_filter);

    std::atomic_int64_t scan_rows{0};
    std::atomic_int64_t scan_bytes{0};
    std::atomic_int64_t cpu_ns{0};
    std::atomic_int64_t mem_cost_bytes{0};
    std::atomic_int64_t spill_bytes{0};
    std::atomic_int64_t read_local_cnt{0};
    std::atomic_int64_t read_remote_cnt{0};
    std::atomic_int64_t transmitted_bytes{0};

    // number rows returned by query.
    // only set once by result sink when closing.
    int64_t returned_rows{0};
    struct ScanStats {
        ScanStats() = default;
        ScanStats(int64_t rows, int64_t bytes) : scan_rows(rows), scan_bytes(bytes) {}
        int64_t scan_rows = 0;
        int64_t scan_bytes = 0;

        bool empty() const { return scan_rows == 0 && scan_bytes == 0; }
    };

    struct NodeExecStats {
        int64_t push_rows = 0;
        int64_t pull_rows = 0;
        int64_t pred_filter_rows = 0;
        int64_t index_filter_rows = 0;
        int64_t rf_filter_rows = 0;
        bool active = false;

        NodeExecStats() = default;

        NodeExecStats(int64_t push, int64_t pull, int64_t pred_filter, int64_t index_filter, int64_t rf_filter)
                : push_rows(push),
                  pull_rows(pull),
                  pred_filter_rows(pred_filter),
                  index_filter_rows(index_filter),
                  rf_filter_rows(rf_filter),
                  active(true) {}
    };
    SpinLock _lock;
    bool _has_map_updates{false};
    // Protected by _lock; no payload is allocated for queries without AI tasks.
    std::unique_ptr<AIExecutionStatistics> _ai_statistics;
    phmap::flat_hash_map<int64_t, ScanStats> _stats_items;
    phmap::flat_hash_map<uint32_t, NodeExecStats> _exec_stats_items;
};

// It is used for collecting sub plan query statistics in DataStreamRecvr.
class QueryStatisticsRecvr {
public:
    static constexpr size_t kNumStripes = 16;

    QueryStatisticsRecvr() = default;
    ~QueryStatisticsRecvr() = default;

    void insert(const PQueryStatistics& statistics, int sender_id);
    void aggregate(QueryStatistics* statistics);

private:
    struct alignas(64) Stripe {
        QueryStatistics stats;
        std::atomic<bool> dirty{false};
    };

    std::array<Stripe, kNumStripes> _stripes;
    std::atomic_int64_t _max_mem_cost_bytes{0};
};

} // namespace starrocks
