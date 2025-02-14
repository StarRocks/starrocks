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
    void uninstall(MetricRegistry* registry);

    METRIC_DEFINE_INT_COUNTER(scan_read_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(scan_read_rows, MetricUnit::ROWS);
    METRIC_DEFINE_INT_COUNTER(load_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(load_rows, MetricUnit::BYTES);
    int32_t ref_count = 0;
};
using TableMetricsPtr = std::shared_ptr<TableMetrics>;

// TableMetricsManager is used to manage all TableMetrics on BE and maintain the mapping from table_id to TableMetrics.
// The life cycle of TableMetrics is bound to Tablet.
// When the first tablet under the table is created, the TableMetrics is registered.
// When the last tablet under the table is released, the TableMetrics is unregistered.

// Considering that the interface for obtaining metrics is usually called periodically,
// in order to ensure data accuracy, we must ensure that the metrics can still be queried for a period of time
// after the last Tablet is released,
// so the real deletion will be done asynchronously by daemon thread.
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
            metrics_ptr->install(_metrics, std::to_string(table_id));
        }
    }
    void unregister_table(uint64_t table_id) {
        std::unique_lock l(_mu);
        DCHECK(_metrics_map.contains(table_id));
        auto metrics_ptr = _metrics_map.at(table_id);
        metrics_ptr->ref_count--;
    }

    TableMetricsPtr get_table_metrics(uint64_t table_id) {
        std::shared_lock l(_mu);
        auto iter = _metrics_map.find(table_id);
        if (iter != _metrics_map.end()) {
            return iter->second;
        }
        return _blackhole_metrics;
    }

    void cleanup();

private:
    MetricRegistry* _metrics;
    std::shared_mutex _mu;
    phmap::flat_hash_map<uint64_t, TableMetricsPtr> _metrics_map;
    // In some cases, we may not be able to obtain the metrics for the corresponding table id,
    // For example, when drop tablet and data load concurrently,
    // the Tablets may have been deleted before the load begins, and the table metrics may be cleared.
    // In such a scenario, we return blackhole metrics to ensure that subsequent processes can work well.
    TableMetricsPtr _blackhole_metrics = std::make_shared<TableMetrics>();
    // used for cleanup
    int64_t _last_cleanup_ts = 0;

    static const int64_t kCleanupIntervalSeconds = 300;
};

} // namespace starrocks