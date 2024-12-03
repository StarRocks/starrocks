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

#include <memory>
#include <shared_mutex>

#include "util/metrics.h"
#include "util/phmap/phmap.h"

namespace starrocks {

struct TableMetrics {
    void install(MetricRegistry* registry, const std::string& table_id);

    METRIC_DEFINE_INT_COUNTER(scan_read_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(scan_read_rows, MetricUnit::ROWS);
    METRIC_DEFINE_INT_COUNTER(tablet_sink_load_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(tablet_sink_load_rows, MetricUnit::BYTES);
    int32_t ref_count = 0;
    // std::atomic<int> ref_count = 0;
};
using TableMetricsPtr = std::shared_ptr<TableMetrics>;

class TableMetricsManager {
public:
    TableMetricsManager(MetricRegistry* metrics) : _metrics(metrics) {}

    void register_table(uint64_t table_id) {
        bool is_created = false;
        TableMetricsPtr metrics_ptr;
        {
            std::unique_lock l(_mu);
            auto [iter, ret] = _metrics_map.try_emplace(table_id, std::make_shared<TableMetrics>());
            metrics_ptr = iter->second;
            metrics_ptr->ref_count++;
            is_created = ret;
        }
        if (is_created) {
            LOG(INFO) << "register table metrics: " << table_id;
            metrics_ptr->install(_metrics, std::to_string(table_id));
        }
        // register metrics?
    }
    void unregister_table(uint64_t table_id) {
        std::unique_lock l(_mu);
        DCHECK(_metrics_map.contains(table_id));
        auto metrics_ptr = _metrics_map.at(table_id);
        metrics_ptr->ref_count--;
        LOG(INFO) << "unregister table metrics: " << table_id << "ref: " << metrics_ptr->ref_count;
    }

    TableMetricsPtr get_table_metrics(uint64_t table_id) {
        std::shared_lock l(_mu);
        DCHECK(_metrics_map.contains(table_id));
        return _metrics_map.at(table_id);
    }

    void cleanup();

private:
    MetricRegistry* _metrics;
    std::shared_mutex _mu;
    phmap::flat_hash_map<uint64_t, TableMetricsPtr> _metrics_map;
    // used for cleanup
    int64_t _last_cleanup_ts = 0;
};

} // namespace starrocks