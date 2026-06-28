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

#include <gtest/gtest.h>

#include <string>

#include "exec/pipeline/primitives/pipeline_metrics.h"

namespace starrocks::pipeline {

namespace {

void assert_metric_value(MetricRegistry* registry, const std::string& name, const std::string& value) {
    auto* metric = registry->get_metric(name);
    ASSERT_NE(nullptr, metric);
    ASSERT_EQ(value, metric->to_string());
}

} // namespace

TEST(PipelineMetricsTest, RegisterAllMetricsIncludesDriverOverloaded) {
    MetricRegistry registry("test_registry");
    PipelineExecutorMetrics metrics;
    metrics.register_all_metrics(&registry);

    metrics.get_driver_executor_metrics()->driver_overloaded.increment(3);
    assert_metric_value(&registry, "pipe_driver_overloaded", "3");
}

TEST(PipelineMetricsTest, RegisterGaugeHooksBeforeInstall) {
    PipelineExecutorMetrics metrics;
    metrics.register_pipe_prepare_pool_queue_len_hook([] { return 4; });

    MetricRegistry registry("test_registry");
    metrics.register_all_metrics(&registry);
    registry.trigger_hook();

    assert_metric_value(&registry, "pipe_prepare_pool_queue_len", "4");
}

} // namespace starrocks::pipeline
