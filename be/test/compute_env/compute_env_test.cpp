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

#include "compute_env/compute_env.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <string>
#include <utility>

#include "base/metrics.h"
#include "base/testutil/assert.h"
#include "common/config_exec_env_fwd.h"
#include "compute_env/load_path/base_load_path_mgr.h"
#include "compute_env/pipeline/driver_limiter.h"
#include "exec/pipeline/primitives/driver_executor.h"
#include "exec/pipeline/primitives/driver_queue.h"
#include "runtime/env/global_env.h"

namespace starrocks {

namespace {

void assert_metric_value(MetricRegistry* registry, const std::string& name, const std::string& value) {
    auto* metric = registry->get_metric(name);
    ASSERT_NE(nullptr, metric);
    ASSERT_EQ(value, metric->to_string());
}

class FakeDriverQueue final : public pipeline::DriverQueue {
public:
    FakeDriverQueue() : DriverQueue(nullptr) {}

    void close() override {}
    void put_back(const pipeline::DriverRawPtr driver) override {}
    void put_back(const std::vector<pipeline::DriverRawPtr>& drivers) override {}
    void put_back_from_executor(const pipeline::DriverRawPtr driver) override {}
    StatusOr<pipeline::DriverRawPtr> take(const bool block) override { return nullptr; }
    void cancel(pipeline::DriverRawPtr driver) override {}
    void update_statistics(const pipeline::DriverRawPtr driver) override {}
    size_t size() const override { return 0; }
    bool should_yield(const pipeline::DriverRawPtr driver, int64_t unaccounted_runtime_ns) const override {
        return false;
    }
};

class FakeDriverExecutor final : public pipeline::DriverExecutor {
public:
    explicit FakeDriverExecutor(std::string name) : DriverExecutor(std::move(name)) {}

    void submit(pipeline::DriverRawPtr driver) override {}
    void cancel(pipeline::DriverRawPtr driver) override {}
    void close() override {}
    void report_exec_state(pipeline::QueryContext* query_ctx, pipeline::FragmentContext* fragment_ctx,
                           const Status& status, bool done) override {}
    void report_audit_statistics(pipeline::QueryContext* query_ctx, pipeline::FragmentContext* fragment_ctx) override {}
    void report_audit_statistics_on_failure(pipeline::QueryContext* query_ctx,
                                            pipeline::FragmentContext* fragment_ctx) override {}
    void iterate_immutable_blocking_driver(const pipeline::ConstDriverConsumer& call) const override {}
    void bind_cpus(const CpuUtil::CpuIds& cpuids, const std::vector<CpuUtil::CpuIds>& borrowed_cpuids) override {}
};

workgroup::WorkGroupManager::DriverQueueFactory make_fake_driver_queue_factory() {
    return [](pipeline::DriverQueueMetrics*) { return std::make_unique<FakeDriverQueue>(); };
}

workgroup::DriverExecutorFactory make_fake_driver_executor_factory() {
    return [](const std::string& name, const CpuUtil::CpuIds& cpuids,
              const std::vector<CpuUtil::CpuIds>& borrowed_cpuids, uint32_t num_driver_threads,
              pipeline::PipelineExecutorMetrics* metrics, const workgroup::WorkGroupSchedulePolicy& schedule_policy)
                   -> StatusOr<std::unique_ptr<pipeline::DriverExecutor>> {
        (void)cpuids;
        (void)borrowed_cpuids;
        (void)num_driver_threads;
        (void)metrics;
        (void)schedule_policy;
        std::unique_ptr<pipeline::DriverExecutor> executor = std::make_unique<FakeDriverExecutor>(name);
        return executor;
    };
}

void init_compute_env_test_context() {
    static bool initialized = false;
    static auto* metrics = new MetricRegistry("compute_env_global_test");
    if (initialized) {
        return;
    }

    config::pipeline_exec_thread_pool_thread_num = 1;
    config::pipeline_max_num_drivers_per_exec_thread = 4;
    const auto spill_path = std::filesystem::absolute("./ut_dir/compute_env_spill");
    config::spill_local_storage_dir = spill_path.string() + ",medium:ssd";
    std::error_code ec;
    std::filesystem::create_directories(spill_path, ec);
    ASSERT_FALSE(ec) << ec.message();
    ASSERT_OK(GlobalEnv::GetInstance()->init_execution_thread_pools(metrics));
    initialized = true;
}

ComputeEnvOptions make_compute_env_options(MetricRegistry* metrics = nullptr) {
    ComputeEnvOptions options;
    options.global_env = GlobalEnv::GetInstance();
    options.metrics = metrics;
    options.as_cn = true;
    options.query_cache_capacity = 4 * 1024 * 1024;
    options.driver_queue_factory = make_fake_driver_queue_factory();
    options.driver_executor_factory = make_fake_driver_executor_factory();
    return options;
}

} // namespace

TEST(ComputeEnvTest, DriverLimiterLifecycle) {
    init_compute_env_test_context();
    ComputeEnv env;

    ASSERT_OK(env.init(make_compute_env_options()));
    ASSERT_NE(env.driver_limiter(), nullptr);
    ASSERT_NE(env.pipeline_timer(), nullptr);
    ASSERT_NE(env.stream_mgr(), nullptr);
    ASSERT_NE(env.result_mgr(), nullptr);
    ASSERT_NE(env.result_queue_mgr(), nullptr);
    ASSERT_NE(env.load_path_mgr(), nullptr);
    ASSERT_NE(env.workgroup_manager(), nullptr);
    ASSERT_NE(env.cache_mgr(), nullptr);
    ASSERT_NE(env.spill_dir_mgr(), nullptr);
    ASSERT_NE(env.global_spill_manager(), nullptr);

    auto token_or = env.driver_limiter()->try_acquire(3);
    ASSERT_TRUE(token_or.ok()) << token_or.status();
    auto token = std::move(token_or).value();
    EXPECT_EQ(env.driver_limiter()->num_total_drivers(), 3);

    auto overloaded_token_or = env.driver_limiter()->try_acquire(2);
    ASSERT_FALSE(overloaded_token_or.ok());
    EXPECT_EQ(overloaded_token_or.status().code(), TStatusCode::TOO_MANY_TASKS);
    EXPECT_EQ(env.driver_limiter()->num_total_drivers(), 3);

    token.reset();
    EXPECT_EQ(env.driver_limiter()->num_total_drivers(), 0);

    env.destroy();
    EXPECT_EQ(env.driver_limiter(), nullptr);
    EXPECT_EQ(env.pipeline_timer(), nullptr);
    EXPECT_EQ(env.stream_mgr(), nullptr);
    EXPECT_EQ(env.result_mgr(), nullptr);
    EXPECT_EQ(env.result_queue_mgr(), nullptr);
    EXPECT_EQ(env.load_path_mgr(), nullptr);
    EXPECT_EQ(env.workgroup_manager(), nullptr);
    EXPECT_EQ(env.cache_mgr(), nullptr);
    EXPECT_EQ(env.spill_dir_mgr(), nullptr);
    EXPECT_EQ(env.global_spill_manager(), nullptr);
}

TEST(ComputeEnvTest, DriverLimiterOwnsPipeDriversMetric) {
    MetricRegistry registry("test_registry");
    {
        pipeline::DriverLimiter driver_limiter(4);
        driver_limiter.init(&registry);

        auto token_or = driver_limiter.try_acquire(3);
        ASSERT_TRUE(token_or.ok()) << token_or.status();
        auto token = std::move(token_or).value();

        registry.trigger_hook();
        assert_metric_value(&registry, "pipe_drivers", "3");

        token.reset();
        registry.trigger_hook();
        assert_metric_value(&registry, "pipe_drivers", "0");

        auto next_token_or = driver_limiter.try_acquire(2);
        ASSERT_TRUE(next_token_or.ok()) << next_token_or.status();
        auto next_token = std::move(next_token_or).value();
        registry.trigger_hook();
        assert_metric_value(&registry, "pipe_drivers", "2");
    }
    registry.trigger_hook();
    ASSERT_EQ(nullptr, registry.get_metric("pipe_drivers"));
}

TEST(ComputeEnvTest, ComputeEnvInstallsDriverLimiterMetric) {
    init_compute_env_test_context();
    MetricRegistry registry("test_registry");
    ComputeEnv env;

    ASSERT_OK(env.init(make_compute_env_options(&registry)));
    auto token_or = env.driver_limiter()->try_acquire(2);
    ASSERT_TRUE(token_or.ok()) << token_or.status();
    auto token = std::move(token_or).value();

    registry.trigger_hook();
    assert_metric_value(&registry, "pipe_drivers", "2");

    token.reset();
    env.destroy();
}

TEST(ComputeEnvTest, LoadPathLifecycle) {
    init_compute_env_test_context();
    ComputeEnv env;
    EXPECT_EQ(env.load_path_mgr(), nullptr);

    ASSERT_OK(env.init(make_compute_env_options()));
    ASSERT_NE(env.load_path_mgr(), nullptr);

    std::string prefix;
    EXPECT_FALSE(env.load_path_mgr()->allocate_dir("db", "label", &prefix).ok());

    env.destroy();
    EXPECT_EQ(env.load_path_mgr(), nullptr);
}

} // namespace starrocks
