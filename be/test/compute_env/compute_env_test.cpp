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

#include <string>
#include <utility>

#include "base/metrics.h"
#include "base/testutil/assert.h"
#include "compute_env/load_path/base_load_path_mgr.h"
#include "compute_env/pipeline/driver_limiter.h"

namespace starrocks {

namespace {

void assert_metric_value(MetricRegistry* registry, const std::string& name, const std::string& value) {
    auto* metric = registry->get_metric(name);
    ASSERT_NE(nullptr, metric);
    ASSERT_EQ(value, metric->to_string());
}

} // namespace

TEST(ComputeEnvTest, DriverLimiterLifecycle) {
    ComputeEnv env;
    ComputeEnvOptions options;
    options.max_num_pipeline_drivers = 4;

    ASSERT_OK(env.init(options));
    ASSERT_NE(env.driver_limiter(), nullptr);
    ASSERT_NE(env.pipeline_timer(), nullptr);
    ASSERT_NE(env.stream_mgr(), nullptr);
    ASSERT_NE(env.result_mgr(), nullptr);
    ASSERT_NE(env.result_queue_mgr(), nullptr);

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
    MetricRegistry registry("test_registry");
    ComputeEnv env;
    ComputeEnvOptions options;
    options.max_num_pipeline_drivers = 4;
    options.metrics = &registry;

    ASSERT_OK(env.init(options));
    auto token_or = env.driver_limiter()->try_acquire(2);
    ASSERT_TRUE(token_or.ok()) << token_or.status();
    auto token = std::move(token_or).value();

    registry.trigger_hook();
    assert_metric_value(&registry, "pipe_drivers", "2");

    token.reset();
    env.destroy();
}

TEST(ComputeEnvTest, LoadPathLifecycle) {
    ComputeEnv env;
    EXPECT_EQ(env.load_path_mgr(), nullptr);

    ASSERT_OK(env.init_load_path({}, true));
    ASSERT_NE(env.load_path_mgr(), nullptr);

    std::string prefix;
    EXPECT_FALSE(env.load_path_mgr()->allocate_dir("db", "label", &prefix).ok());

    env.destroy_load_path();
    EXPECT_EQ(env.load_path_mgr(), nullptr);
}

} // namespace starrocks
