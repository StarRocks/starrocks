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

#include "platform/platform_env.h"

#include <gtest/gtest.h>

#include "base/metrics.h"
#include "base/testutil/assert.h"

namespace starrocks {

TEST(PlatformEnvTest, OwnsThriftClientCacheAccessors) {
    auto* env = PlatformEnv::GetInstance();
    env->destroy();

    MetricRegistry metrics("platform_env_test");
    ASSERT_OK(env->init(&metrics));

    ASSERT_NE(env->backend_client_cache(), nullptr);
    ASSERT_NE(env->frontend_client_cache(), nullptr);
    ASSERT_NE(env->broker_client_cache(), nullptr);

    env->destroy();
    EXPECT_EQ(env->backend_client_cache(), nullptr);
    EXPECT_EQ(env->frontend_client_cache(), nullptr);
    EXPECT_EQ(env->broker_client_cache(), nullptr);
}

} // namespace starrocks
