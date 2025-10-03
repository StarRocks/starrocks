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

#include "common/config.h"
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

bool TableMetricsManager::can_install_metrics() {
    int64_t old_num = 0;
    do {
        old_num = _installed_metrics_num.load();
        if (old_num + 1 > config::max_table_metrics_num) {
            return false;
        }
    } while (!_installed_metrics_num.compare_exchange_strong(old_num, old_num + 1));
    return true;
}

void TableMetricsManager::register_table(uint64_t table_id) {
    if (!config::enable_table_metrics) {
        return;
    }
    TableMetricsPtr metrics_ptr;

    bool is_created = _metrics_map.lazy_emplace_l(
            table_id,
            [&](TableMetricsPtr& value) {
                value->ref_count++;
                metrics_ptr = value;
            },
            [&](const auto& ctor) {
                bool should_install = can_install_metrics();
                metrics_ptr = std::make_shared<TableMetrics>(1, should_install);
                ctor(table_id, metrics_ptr);
            });

    if (is_created && metrics_ptr->installed) {
        metrics_ptr->install(&_metrics, std::to_string(table_id));
    }
}

void TableMetricsManager::unregister_table(uint64_t table_id) {
    if (!config::enable_table_metrics) {
        return;
    }
    _metrics_map.modify_if(table_id, [](TableMetricsPtr& metrics_ptr) { metrics_ptr->ref_count--; });
}

void TableMetricsManager::cleanup(bool force) {
    if (!config::enable_table_metrics) {
        return;
    }
#ifndef BE_TEST
    int64_t current_second = MonotonicSeconds();
    if (!force && current_second - _last_cleanup_ts <= kCleanupIntervalSeconds) {
        return;
    }
#endif
    // metrics should be deleted from _metrics_map
    std::vector<std::pair<uint64_t, TableMetricsPtr>> delete_metrics;
    _metrics_map.for_each([&](const auto& pair) {
        if (pair.second->ref_count == 0) {
            delete_metrics.emplace_back(pair.first, pair.second);
        }
    });
    int64_t uninstalled_num = 0;
    for (const auto& pair : delete_metrics) {
        _metrics_map.erase(pair.first);
        if (pair.second->installed) {
            pair.second->uninstall(&_metrics);
            uninstalled_num++;
        }
    }

    _installed_metrics_num -= uninstalled_num;
    _last_cleanup_ts = MonotonicSeconds();
}

} // namespace starrocks