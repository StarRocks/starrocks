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

#include "storage/flat_json_metrics.h"

#include <gtest/gtest.h>

#include <string>

namespace starrocks {

namespace {

void assert_metric_value(MetricRegistry* registry, const std::string& name, const std::string& value) {
    auto* metric = registry->get_metric(name);
    ASSERT_NE(nullptr, metric);
    ASSERT_EQ(value, metric->to_string());
}

} // namespace

TEST(FlatJsonMetricsTest, InstallRegistersWriteAndAccessMetrics) {
    MetricRegistry registry("test_registry");
    FlatJsonMetrics metrics(&registry);

    metrics.flat_json_segment_write_total.increment(1);
    assert_metric_value(&registry, "flat_json_segment_write_total", "1");

    metrics.flat_json_write_rows_total.increment(2);
    assert_metric_value(&registry, "flat_json_write_rows_total", "2");

    metrics.flat_json_paths_discovered_total.increment(3);
    assert_metric_value(&registry, "flat_json_paths_discovered_total", "3");

    metrics.flat_json_paths_extracted_total.increment(4);
    assert_metric_value(&registry, "flat_json_paths_extracted_total", "4");

    metrics.flat_json_access_hit_total.increment(5);
    assert_metric_value(&registry, "flat_json_access_hit_total", "5");

    metrics.flat_json_access_miss_total.increment(6);
    assert_metric_value(&registry, "flat_json_access_miss_total", "6");
}

TEST(FlatJsonMetricsTest, InstallRegistersDurationAndCompactionMetrics) {
    MetricRegistry registry("test_registry");
    FlatJsonMetrics metrics(&registry);
    metrics.install(&registry);

    metrics.flat_json_cast_duration_ns_total.increment(7);
    assert_metric_value(&registry, "flat_json_cast_duration_ns_total", "7");

    metrics.flat_json_merge_duration_ns_total.increment(8);
    assert_metric_value(&registry, "flat_json_merge_duration_ns_total", "8");

    metrics.flat_json_flatten_duration_ns_total.increment(9);
    assert_metric_value(&registry, "flat_json_flatten_duration_ns_total", "9");

    metrics.flat_json_compaction_total.increment(10);
    assert_metric_value(&registry, "flat_json_compaction_total", "10");

    metrics.flat_json_compaction_schema_change_total.increment(11);
    assert_metric_value(&registry, "flat_json_compaction_schema_change_total", "11");
}

} // namespace starrocks
