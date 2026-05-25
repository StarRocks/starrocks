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

#include "base/testutil/assert.h"
#include "compute_env/pipeline/driver_limiter.h"

namespace starrocks {

TEST(ComputeEnvTest, DriverLimiterLifecycle) {
    ComputeEnv env;
    ComputeEnvOptions options;
    options.max_num_pipeline_drivers = 4;

    ASSERT_OK(env.init(options));
    ASSERT_NE(env.driver_limiter(), nullptr);
    ASSERT_NE(env.pipeline_timer(), nullptr);

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
}

} // namespace starrocks
