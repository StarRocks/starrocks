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

#include "agent/agent_metrics.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "common/thread/threadpool.h"

namespace starrocks {

namespace {

void assert_metric_value(MetricRegistry* registry, const std::string& name, const MetricLabels& labels,
                         const std::string& value) {
    auto* metric = registry->get_metric(name, labels);
    ASSERT_NE(nullptr, metric);
    ASSERT_EQ(value, metric->to_string());
}

void assert_metric_value(MetricRegistry* registry, const std::string& name, const std::string& value) {
    auto* metric = registry->get_metric(name);
    ASSERT_NE(nullptr, metric);
    ASSERT_EQ(value, metric->to_string());
}

} // namespace

TEST(AgentMetricsTest, InstallRegistersEngineRequestMetrics) {
    AgentMetrics metrics;
    MetricRegistry registry("test_registry");
    metrics.install(&registry);

    metrics.report_disk_requests_total.increment(3);
    assert_metric_value(&registry, "engine_requests_total",
                        MetricLabels().add("type", "report_disk").add("status", "total"), "3");

    metrics.report_task_requests_failed.increment(4);
    assert_metric_value(&registry, "engine_requests_total",
                        MetricLabels().add("type", "report_task").add("status", "failed"), "4");

    metrics.schema_change_requests_total.increment(5);
    assert_metric_value(&registry, "engine_requests_total",
                        MetricLabels().add("type", "schema_change").add("status", "total"), "5");

    metrics.finish_task_requests_failed.increment(6);
    assert_metric_value(&registry, "engine_requests_total",
                        MetricLabels().add("type", "finish_task").add("status", "failed"), "6");

    metrics.publish_task_request_total.increment(7);
    assert_metric_value(&registry, "engine_requests_total",
                        MetricLabels().add("type", "publish").add("status", "total"), "7");
}

TEST(AgentMetricsTest, InstallRegistersCloneMetrics) {
    AgentMetrics metrics;
    MetricRegistry registry("test_registry");
    metrics.install(&registry);

    metrics.clone_requests_failed.increment(8);
    assert_metric_value(&registry, "engine_requests_total", MetricLabels().add("type", "clone").add("status", "failed"),
                        "8");

    metrics.clone_task_inter_node_copy_bytes.increment(9);
    assert_metric_value(&registry, "clone_task_copy_bytes", MetricLabels().add("type", "INTER_NODE"), "9");

    metrics.clone_task_intra_node_copy_duration_ms.increment(10);
    assert_metric_value(&registry, "clone_task_copy_duration_ms", MetricLabels().add("type", "INTRA_NODE"), "10");
}

TEST(AgentMetricsTest, InstallRegistersDiskPathMetrics) {
    AgentMetrics metrics;
    MetricRegistry registry("test_registry");
    metrics.install(&registry);
    metrics.install_disk_path_metrics(&registry, {"path_a"});

    metrics.set_disk_metrics("path_a", 11, 12, 13, 1);
    assert_metric_value(&registry, "disks_total_capacity", MetricLabels().add("path", "path_a"), "11");
    assert_metric_value(&registry, "disks_avail_capacity", MetricLabels().add("path", "path_a"), "12");
    assert_metric_value(&registry, "disks_data_used_capacity", MetricLabels().add("path", "path_a"), "13");
    assert_metric_value(&registry, "disks_state", MetricLabels().add("path", "path_a"), "1");
}

TEST(AgentMetricsTest, RegisterThreadPoolMetrics) {
    std::unique_ptr<ThreadPool> threadpool;
    AgentMetrics metrics;
    MetricRegistry registry("test_registry");
    metrics.install(&registry);

    auto status = ThreadPoolBuilder("agent_metrics_test")
                          .set_min_threads(0)
                          .set_max_threads(3)
                          .set_max_queue_size(5)
                          .build(&threadpool);
    ASSERT_TRUE(status.ok()) << status;

    metrics.register_thread_pool_metrics("clone", threadpool.get());
    registry.trigger_hook();

    assert_metric_value(&registry, "clone_threadpool_size", "3");
    assert_metric_value(&registry, "clone_queue_count", "0");
}

TEST(AgentMetricsTest, RegisterThreadPoolMetricsBeforeInstall) {
    std::unique_ptr<ThreadPool> threadpool;
    AgentMetrics metrics;
    auto status = ThreadPoolBuilder("agent_metrics_test")
                          .set_min_threads(0)
                          .set_max_threads(3)
                          .set_max_queue_size(5)
                          .build(&threadpool);
    ASSERT_TRUE(status.ok()) << status;

    metrics.register_thread_pool_metrics("clone", threadpool.get());

    MetricRegistry registry("test_registry");
    metrics.install(&registry);
    registry.trigger_hook();

    assert_metric_value(&registry, "clone_threadpool_size", "3");
    assert_metric_value(&registry, "clone_queue_count", "0");
}

} // namespace starrocks
