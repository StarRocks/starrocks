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

#include "staros_integration/staros_worker_metrics.h"

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

TEST(StarOSWorkerMetricsTest, InstallRegistersFallbackMetrics) {
    MetricRegistry registry("test_registry");
    StarOSWorkerMetrics metrics(&registry);
    metrics.install(&registry);

    metrics.staros_shard_info_fallback_total.increment(3);
    assert_metric_value(&registry, "staros_shard_info_fallback_total", "3");

    metrics.staros_shard_info_fallback_failed_total.increment(4);
    assert_metric_value(&registry, "staros_shard_info_fallback_failed_total", "4");
}

} // namespace starrocks
