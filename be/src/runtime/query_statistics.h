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

#ifndef STARROCKS_BE_EXEC_QUERY_STATISTICS_H
#define STARROCKS_BE_EXEC_QUERY_STATISTICS_H

#include <mutex>

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

    void merge(const QueryStatistics& other) {
        scan_rows += other.scan_rows;
        scan_bytes += other.scan_bytes;
        _stats_items.insert(_stats_items.end(), other._stats_items.begin(), other._stats_items.end());
    }

    void set_returned_rows(int64_t num_rows) { this->returned_rows = num_rows; }

    void add_stats_item(QueryStatisticsItemPB& stats_item) {
        this->_stats_items.emplace_back(stats_item);
        this->scan_rows += stats_item.scan_rows();
        this->scan_bytes += stats_item.scan_bytes();
    }

    void merge(QueryStatisticsRecvr* recvr);

    void clear() {
        scan_rows = 0;
        scan_bytes = 0;
        returned_rows = 0;
        _stats_items.clear();
    }

    void to_pb(PQueryStatistics* statistics) {
        DCHECK(statistics != nullptr);
        statistics->set_scan_rows(scan_rows);
        statistics->set_scan_bytes(scan_bytes);
        statistics->set_returned_rows(returned_rows);
        *statistics->mutable_stats_items() = {_stats_items.begin(), _stats_items.end()};
    }

    void merge_pb(const PQueryStatistics& statistics) {
        scan_rows += statistics.scan_rows();
        scan_bytes += statistics.scan_bytes();
        _stats_items.insert(_stats_items.end(), statistics.stats_items().begin(), statistics.stats_items().end());
    }

private:
    int64_t scan_rows{0};
    int64_t scan_bytes{0};
    // number rows returned by query.
    // only set once by result sink when closing.
    int64_t returned_rows{0};
    std::vector<QueryStatisticsItemPB> _stats_items;
};

// It is used for collecting sub plan query statistics in DataStreamRecvr.
class QueryStatisticsRecvr {
public:
    ~QueryStatisticsRecvr();

    void insert(const PQueryStatistics& statistics, int sender_id);

private:
    friend class QueryStatistics;

    void merge(QueryStatistics* statistics) {
        std::lock_guard<SpinLock> l(_lock);
        for (auto& pair : _query_statistics) {
            statistics->merge(*(pair.second));
        }
    }

    std::map<int, QueryStatistics*> _query_statistics;
    SpinLock _lock;
};

} // namespace starrocks

#endif
