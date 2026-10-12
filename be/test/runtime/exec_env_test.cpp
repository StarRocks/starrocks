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

#include "runtime/exec_env.h"

#include <gtest/gtest.h>

#include <utility>

#include "common/config.h"
#include "testutil/scoped_updater.h"
#include "util/cpu_info.h"
#include "util/threadpool.h"

namespace starrocks {

TEST(GlobalEnvTest, calc_query_mem_limit) {
    ASSERT_EQ(GlobalEnv::calc_max_query_memory(-1, 80), -1);
    ASSERT_EQ(GlobalEnv::calc_max_query_memory(1000000000, -2), 900000000);
    ASSERT_EQ(GlobalEnv::calc_max_query_memory(1000000000, 102), 900000000);
    ASSERT_EQ(GlobalEnv::calc_max_query_memory(1000000000, 70), 700000000);
}

TEST(ExecEnvTest, PkIndexExecutionThreadPoolSizing) {
    CpuInfo::init();
    SCOPED_UPDATE(int32_t, config::pk_index_parallel_execution_threadpool_size, 8);

    for (const auto& [configured_threads, expected_threads] :
         {std::pair{0, CpuInfo::num_cores()}, std::pair{-1, CpuInfo::num_cores()}, std::pair{1, 1}, std::pair{7, 7}}) {
        SCOPED_TRACE(configured_threads);
        SCOPED_UPDATE(int32_t, config::pk_index_parallel_execution_threadpool_max_threads, configured_threads);

        ExecEnv env;
        // Exercise the production initializer; the ordinary BE_TEST pool stays fixed at one thread.
        auto st = env._init_pk_index_execution_thread_pool();
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_NE(env.pk_index_execution_thread_pool(), nullptr);
        EXPECT_EQ(env.pk_index_execution_thread_pool()->max_threads(), expected_threads);
        env.pk_index_execution_thread_pool()->shutdown();
    }
}

} // namespace starrocks
