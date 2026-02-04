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

#include <atomic>
#include <cstdint>
#include <memory>

#include "base/concurrency/bthread_shared_mutex.h"
#include "util/metrics.h"
#include "util/phmap/phmap.h"
#include "util/phmap/phmap_base.h"
#include "util/time.h"

namespace starrocks {

struct TableMetrics {
    TableMetrics(int32_t rc, bool ins) : ref_count(rc), installed(ins) {}

    void install(MetricRegistry* registry, const std::string& table_id);
    void uninstall(MetricRegistry* registry);

    METRIC_DEFINE_INT_COUNTER(scan_read_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(scan_read_rows, MetricUnit::ROWS);
    METRIC_DEFINE_INT_COUNTER(load_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(load_rows, MetricUnit::BYTES);
    int32_t ref_count = 0;
    bool installed = false;
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
    TableMetricsManager() : _last_cleanup_ts(MonotonicSeconds()) {}

    MetricRegistry* metric_registry() { return &_metrics; }

    void register_table(uint64_t table_id);
    void unregister_table(uint64_t table_id);

    TableMetricsPtr get_table_metrics(uint64_t table_id) {
        TableMetricsPtr ret = _blackhole_metrics;
        _metrics_map.if_contains(table_id, [&](TableMetricsPtr& metrics_ptr) { ret = metrics_ptr; });
        return ret;
    }

    void cleanup(bool force = false);

    size_t size() const { return _metrics_map.size(); }
    int64_t installed_metrics_num() const { return _installed_metrics_num.load(); }

private:
    bool can_install_metrics();

    MetricRegistry _metrics{"starrocks_be"};
    using MutexType = starrocks::bthreads::BThreadSharedMutex;
    using MetricsMap =
            phmap::parallel_flat_hash_map<uint64_t, TableMetricsPtr, phmap::Hash<uint64_t>, phmap::EqualTo<uint64_t>,
                                          phmap::Allocator<uint64_t>, 4, MutexType>;
    MetricsMap _metrics_map;
    // In some cases, we may not be able to obtain the metrics for the corresponding table id,
    // For example, when drop tablet and data load concurrently,
    // the Tablets may have been deleted before the load begins, and the table metrics may be cleared.
    // In such a scenario, we return blackhole metrics to ensure that subsequent processes can work well.
    TableMetricsPtr _blackhole_metrics = std::make_shared<TableMetrics>(0, false);
    // used for cleanup
    int64_t _last_cleanup_ts = 0;
    std::atomic_int64_t _installed_metrics_num = 0;

    static const int64_t kCleanupIntervalSeconds = 300;
};

} // namespace starrocks