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

#include "util/metrics/spill_metrics.h"

namespace starrocks {

namespace {

void register_labeled(MetricRegistry* registry, const char* storage_type, SpillMetrics::LabeledCounters* bucket) {
    MetricLabels labels;
    labels.add("storage_type", storage_type);

    bucket->trigger_total = std::make_unique<IntCounter>(MetricUnit::OPERATIONS);
    registry->register_metric("query_spill_trigger_total", labels, bucket->trigger_total.get());

    bucket->bytes_write_total = std::make_unique<IntCounter>(MetricUnit::BYTES);
    registry->register_metric("query_spill_bytes_write_total", labels, bucket->bytes_write_total.get());

    bucket->bytes_read_total = std::make_unique<IntCounter>(MetricUnit::BYTES);
    registry->register_metric("query_spill_bytes_read_total", labels, bucket->bytes_read_total.get());

    bucket->blocks_write_total = std::make_unique<IntCounter>(MetricUnit::OPERATIONS);
    registry->register_metric("query_spill_blocks_write_total", labels, bucket->blocks_write_total.get());

    bucket->blocks_read_total = std::make_unique<IntCounter>(MetricUnit::OPERATIONS);
    registry->register_metric("query_spill_blocks_read_total", labels, bucket->blocks_read_total.get());

    bucket->write_io_duration_ns_total = std::make_unique<IntCounter>(MetricUnit::NANOSECONDS);
    registry->register_metric("query_spill_write_io_duration_ns_total", labels,
                              bucket->write_io_duration_ns_total.get());

    bucket->read_io_duration_ns_total = std::make_unique<IntCounter>(MetricUnit::NANOSECONDS);
    registry->register_metric("query_spill_read_io_duration_ns_total", labels, bucket->read_io_duration_ns_total.get());
}

} // namespace

SpillMetrics* SpillMetrics::instance() {
    // Process-lifetime singleton: registered Metric objects keep back-pointers
    // to MetricRegistry, so avoid exit-time destruction after registry teardown.
    static auto* instance = new SpillMetrics();
    return instance;
}

void SpillMetrics::install(MetricRegistry* registry) {
    if (_registry != nullptr) {
        DCHECK_EQ(_registry, registry);
        return;
    }
    _registry = registry;
    _local_disk_bytes_used = std::make_unique<IntGauge>(MetricUnit::BYTES);
    registry->register_metric("spill_disk_bytes_used", MetricLabels().add("storage_type", "local"),
                              _local_disk_bytes_used.get());

    _remote_disk_bytes_used = std::make_unique<IntGauge>(MetricUnit::BYTES);
    registry->register_metric("spill_disk_bytes_used", MetricLabels().add("storage_type", "remote"),
                              _remote_disk_bytes_used.get());

    register_labeled(registry, "local", &_local);
    register_labeled(registry, "remote", &_remote);
}

} // namespace starrocks
