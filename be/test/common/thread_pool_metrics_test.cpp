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

#include "common/metrics/thread_pool_metrics.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "common/system/cpu_info.h"
#include "common/thread/threadpool.h"

namespace starrocks {

namespace {

struct TestThreadPoolMetrics {
    METRICS_DEFINE_THREAD_POOL(test_pool);
};

class ThreadPoolMetricsTest : public ::testing::Test {
public:
    static void SetUpTestSuite() { CpuInfo::init(); }
};

std::unique_ptr<ThreadPool> make_thread_pool() {
    std::unique_ptr<ThreadPool> threadpool;
    auto status = ThreadPoolBuilder("common_metric_test")
                          .set_min_threads(0)
                          .set_max_threads(3)
                          .set_max_queue_size(5)
                          .build(&threadpool);
    EXPECT_TRUE(status.ok()) << status;
    return threadpool;
}

void assert_metric_value(MetricRegistry* registry, const std::string& name, const std::string& value) {
    auto* metric = registry->get_metric(name);
    ASSERT_NE(nullptr, metric);
    ASSERT_EQ(value, metric->to_string());
}

void assert_thread_pool_metrics(MetricRegistry* registry) {
    registry->trigger_hook();

    assert_metric_value(registry, "test_pool_threadpool_size", "3");
    assert_metric_value(registry, "test_pool_executed_tasks_total", "0");
    assert_metric_value(registry, "test_pool_pending_time_ns_total", "0");
    assert_metric_value(registry, "test_pool_execute_time_ns_total", "0");
    assert_metric_value(registry, "test_pool_queue_count", "0");
    assert_metric_value(registry, "test_pool_running_threads", "0");
    assert_metric_value(registry, "test_pool_active_threads", "0");
}

} // namespace

TEST_F(ThreadPoolMetricsTest, RegisterMetricsDirectly) {
    auto threadpool = make_thread_pool();
    ASSERT_NE(nullptr, threadpool);

    TestThreadPoolMetrics metrics;
    MetricRegistry registry("test_registry");
    ThreadPoolMetricsRegistrar registrar;

    registrar.register_metrics(&registry, "test_pool", threadpool.get(),
                               STARROCKS_THREAD_POOL_METRIC_GROUP(&metrics, test_pool));

    assert_thread_pool_metrics(&registry);
}

TEST_F(ThreadPoolMetricsTest, RegisterMetricsBeforeInstall) {
    auto threadpool = make_thread_pool();
    ASSERT_NE(nullptr, threadpool);

    TestThreadPoolMetrics metrics;
    ThreadPoolMetricsRegistrar registrar;

    registrar.register_metrics(nullptr, "test_pool", threadpool.get(),
                               STARROCKS_THREAD_POOL_METRIC_GROUP(&metrics, test_pool));

    MetricRegistry registry("test_registry");
    registrar.install(&registry);

    assert_thread_pool_metrics(&registry);
}

} // namespace starrocks
