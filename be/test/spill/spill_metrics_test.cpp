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

#include "exec/spill/spill_metrics.h"

#include <gtest/gtest.h>

#include <string>

namespace starrocks {

namespace {

void assert_metric_value(MetricRegistry* registry, const std::string& name, const MetricLabels& labels,
                         const std::string& value) {
    auto* metric = registry->get_metric(name, labels);
    ASSERT_NE(nullptr, metric);
    ASSERT_EQ(value, metric->to_string());
}

} // namespace

TEST(SpillMetricsTest, InstallRegistersLocalAndRemoteMetrics) {
    MetricRegistry registry("test_registry");
    SpillMetrics metrics(&registry);
    metrics.install(&registry);

    metrics.local_disk_bytes_used()->set_value(1024);
    metrics.remote_disk_bytes_used()->set_value(2048);

    MetricLabels local_labels;
    local_labels.add("storage_type", "local");
    assert_metric_value(&registry, "spill_disk_bytes_used", local_labels, "1024");

    MetricLabels remote_labels;
    remote_labels.add("storage_type", "remote");
    assert_metric_value(&registry, "spill_disk_bytes_used", remote_labels, "2048");

    auto* local = metrics.get(false);
    ASSERT_NE(nullptr, local);
    local->trigger_total->increment(1);
    local->bytes_write_total->increment(2);
    local->bytes_read_total->increment(3);
    local->blocks_write_total->increment(4);
    local->blocks_read_total->increment(5);
    local->write_io_duration_ns_total->increment(6);
    local->read_io_duration_ns_total->increment(7);

    assert_metric_value(&registry, "query_spill_trigger_total", local_labels, "1");
    assert_metric_value(&registry, "query_spill_bytes_write_total", local_labels, "2");
    assert_metric_value(&registry, "query_spill_bytes_read_total", local_labels, "3");
    assert_metric_value(&registry, "query_spill_blocks_write_total", local_labels, "4");
    assert_metric_value(&registry, "query_spill_blocks_read_total", local_labels, "5");
    assert_metric_value(&registry, "query_spill_write_io_duration_ns_total", local_labels, "6");
    assert_metric_value(&registry, "query_spill_read_io_duration_ns_total", local_labels, "7");

    auto* remote = metrics.get(true);
    ASSERT_NE(nullptr, remote);
    remote->trigger_total->increment(8);
    assert_metric_value(&registry, "query_spill_trigger_total", remote_labels, "8");
}

} // namespace starrocks
