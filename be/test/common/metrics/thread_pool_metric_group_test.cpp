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

#include "common/metrics/thread_pool_metric_group.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "common/system/cpu_info.h"
#include "common/thread/threadpool.h"

namespace starrocks {

namespace {

void assert_metric_value(MetricRegistry* registry, const std::string& name, const std::string& value) {
    auto* metric = registry->get_metric(name);
    ASSERT_NE(nullptr, metric);
    ASSERT_EQ(value, metric->to_string());
}

void assert_metric_missing(MetricRegistry* registry, const std::string& name) {
    ASSERT_EQ(nullptr, registry->get_metric(name));
}

void build_thread_pool(const std::string& name, int max_threads, std::unique_ptr<ThreadPool>* threadpool) {
    auto status = ThreadPoolBuilder(name)
                          .set_min_threads(0)
                          .set_max_threads(max_threads)
                          .set_max_queue_size(5)
                          .build(threadpool);
    ASSERT_TRUE(status.ok()) << status;
}

} // namespace

class ThreadPoolMetricGroupTest : public ::testing::Test {
public:
    static void SetUpTestSuite() { CpuInfo::init(); }
};

TEST_F(ThreadPoolMetricGroupTest, RegisterMetrics) {
    MetricRegistry registry("test_registry");
    std::unique_ptr<ThreadPool> threadpool;
    ThreadPoolMetricGroup group;

    build_thread_pool("tp_metric_test", 3, &threadpool);

    group.register_metrics(&registry, "sample", threadpool.get());
    registry.trigger_hook();

    assert_metric_value(&registry, "sample_threadpool_size", "3");
    assert_metric_value(&registry, "sample_executed_tasks_total", "0");
    assert_metric_value(&registry, "sample_pending_time_ns_total", "0");
    assert_metric_value(&registry, "sample_execute_time_ns_total", "0");
    assert_metric_value(&registry, "sample_queue_count", "0");
    assert_metric_value(&registry, "sample_running_threads", "0");
    assert_metric_value(&registry, "sample_active_threads", "0");
}

TEST_F(ThreadPoolMetricGroupTest, UnregisterMetrics) {
    MetricRegistry registry("test_registry");
    std::unique_ptr<ThreadPool> threadpool;
    ThreadPoolMetricGroup group;

    build_thread_pool("tp_metric_unregister_test", 3, &threadpool);
    group.register_metrics(&registry, "sample", threadpool.get());
    registry.trigger_hook();
    assert_metric_value(&registry, "sample_threadpool_size", "3");

    group.unregister_metrics();
    group.unregister_metrics();
    threadpool.reset();
    registry.trigger_hook();

    assert_metric_missing(&registry, "sample_threadpool_size");
    assert_metric_missing(&registry, "sample_executed_tasks_total");
    assert_metric_missing(&registry, "sample_pending_time_ns_total");
    assert_metric_missing(&registry, "sample_execute_time_ns_total");
    assert_metric_missing(&registry, "sample_queue_count");
    assert_metric_missing(&registry, "sample_running_threads");
    assert_metric_missing(&registry, "sample_active_threads");
}

TEST_F(ThreadPoolMetricGroupTest, DestructorUnregistersMetrics) {
    MetricRegistry registry("test_registry");
    std::unique_ptr<ThreadPool> threadpool;

    build_thread_pool("tp_metric_destructor_test", 3, &threadpool);
    {
        ThreadPoolMetricGroup group;
        group.register_metrics(&registry, "sample", threadpool.get());
        registry.trigger_hook();
        assert_metric_value(&registry, "sample_threadpool_size", "3");
    }

    threadpool.reset();
    registry.trigger_hook();
    assert_metric_missing(&registry, "sample_threadpool_size");
}

TEST_F(ThreadPoolMetricGroupTest, DestructorToleratesRegistryDestroyedFirst) {
    ThreadPoolMetricGroup group;

    {
        std::unique_ptr<ThreadPool> threadpool;
        MetricRegistry registry("test_registry");

        build_thread_pool("tp_metric_registry_destroyed_test", 3, &threadpool);
        group.register_metrics(&registry, "sample", threadpool.get());
        registry.trigger_hook();
        assert_metric_value(&registry, "sample_threadpool_size", "3");
    }
}

TEST_F(ThreadPoolMetricGroupTest, RegisterMetricsReplacesExistingRegistration) {
    MetricRegistry registry("test_registry");
    std::unique_ptr<ThreadPool> first_threadpool;
    std::unique_ptr<ThreadPool> second_threadpool;
    ThreadPoolMetricGroup group;

    build_thread_pool("tp_metric_reregister_first_test", 3, &first_threadpool);
    build_thread_pool("tp_metric_reregister_second_test", 7, &second_threadpool);

    group.register_metrics(&registry, "sample", first_threadpool.get());
    registry.trigger_hook();
    assert_metric_value(&registry, "sample_threadpool_size", "3");

    group.register_metrics(&registry, "sample", second_threadpool.get());
    first_threadpool.reset();
    registry.trigger_hook();
    assert_metric_value(&registry, "sample_threadpool_size", "7");
}

} // namespace starrocks
