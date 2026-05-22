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

#include "runtime/env/global_env.h"

namespace starrocks {

TEST(GlobalEnvTest, CalcQueryMemLimit) {
    ASSERT_EQ(GlobalEnv::calc_max_query_memory(-1, 80), -1);
    ASSERT_EQ(GlobalEnv::calc_max_query_memory(1000000000, -2), 900000000);
    ASSERT_EQ(GlobalEnv::calc_max_query_memory(1000000000, 102), 900000000);
    ASSERT_EQ(GlobalEnv::calc_max_query_memory(1000000000, 70), 700000000);
}

TEST(GlobalEnvTest, GetInstanceReturnsStableSingleton) {
    ASSERT_NE(GlobalEnv::GetInstance(), nullptr);
    ASSERT_EQ(GlobalEnv::GetInstance(), GlobalEnv::GetInstance());
}

} // namespace starrocks
