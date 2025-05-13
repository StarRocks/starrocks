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

#include "util/table_metrics.h"

#include <mutex>

#include "util/phmap/btree.h"
#include "util/time.h"

namespace starrocks {

void TableMetrics::install(MetricRegistry* registry, const std::string& table_id) {
#define REGISTER_TABLE_METRIC(name) \
    registry->register_metric("table_" #name, MetricLabels().add("table_id", table_id), &name)

    REGISTER_TABLE_METRIC(scan_read_bytes);
    REGISTER_TABLE_METRIC(scan_read_rows);
    REGISTER_TABLE_METRIC(load_bytes);
    REGISTER_TABLE_METRIC(load_rows);
}

void TableMetrics::uninstall(MetricRegistry* registry) {
#define UNREGISTER_TABLE_METRIC(name) registry->deregister_metric(&name)

    UNREGISTER_TABLE_METRIC(scan_read_bytes);
    UNREGISTER_TABLE_METRIC(scan_read_rows);
    UNREGISTER_TABLE_METRIC(load_bytes);
    UNREGISTER_TABLE_METRIC(load_rows);
}

void TableMetricsManager::cleanup() {
    int64_t current_second = MonotonicSeconds();
    if (current_second - _last_cleanup_ts <= kCleanupIntervalSeconds) {
        return;
    }

    std::vector<std::pair<uint64_t, TableMetricsPtr>> delete_metrics;
    _metrics_map.for_each([&] (const auto& pair) {
        if (pair.second->ref_count == 0) {
            delete_metrics.emplace_back(pair.first, pair.second);
        }
    });
    for (const auto& pair: delete_metrics) {
        _metrics_map.erase(pair.first);
        pair.second->uninstall(_metrics);
    }
    _last_cleanup_ts = MonotonicSeconds();
}

} // namespace starrocks