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

#include "base/metrics.h"
#include "common/system/cpu_info.h"
#include "common/thread/threadpool.h"
#include "runtime/env/global_env.h"
#include "runtime/runtime_env_test_util.h"

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

TEST(GlobalEnvTest, OwnsExecutionThreadPools) {
    CpuInfo::init();
    runtime_env_test::set_small_thread_pool_configs();

    auto* env = GlobalEnv::GetInstance();
    env->destroy_thread_pools();

    MetricRegistry metrics("runtime_env_test");
    auto st = env->init_execution_thread_pools(&metrics);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_NE(env->thread_pool(), nullptr);
    ASSERT_NE(env->streaming_load_thread_pool(), nullptr);
    ASSERT_NE(env->load_rowset_thread_pool(), nullptr);
    ASSERT_NE(env->load_segment_thread_pool(), nullptr);
    ASSERT_NE(env->put_combined_txn_log_thread_pool(), nullptr);
    ASSERT_NE(env->jvm_call_pool(), nullptr);
    ASSERT_NE(env->pipeline_prepare_pool(), nullptr);
    ASSERT_NE(env->pipeline_sink_io_pool(), nullptr);
    ASSERT_NE(env->query_rpc_pool(), nullptr);
    ASSERT_NE(env->datacache_rpc_pool(), nullptr);
    ASSERT_NE(env->load_rpc_pool(), nullptr);
    ASSERT_NE(env->dictionary_cache_pool(), nullptr);
    ASSERT_NE(env->automatic_partition_pool(), nullptr);
    ASSERT_EQ(env->max_executor_threads(), 2);
    ASSERT_EQ(env->dictionary_cache_pool()->max_threads(), 1);

    env->shutdown_thread_pools();
    env->destroy_thread_pools();
    ASSERT_EQ(env->thread_pool(), nullptr);
    ASSERT_EQ(env->jvm_call_pool(), nullptr);
    ASSERT_EQ(env->load_rpc_pool(), nullptr);
}

TEST(GlobalEnvTest, OwnsLakeThreadPools) {
    CpuInfo::init();
    runtime_env_test::set_small_thread_pool_configs();

    auto* env = GlobalEnv::GetInstance();
    env->destroy_thread_pools();

    MetricRegistry metrics("runtime_env_lake_test");
    auto st = env->init_lake_thread_pools(&metrics);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_NE(env->lake_metadata_fetch_thread_pool(), nullptr);
    ASSERT_NE(env->lake_vector_index_build_thread_pool(), nullptr);
#ifdef BE_TEST
    ASSERT_NE(env->put_aggregate_metadata_thread_pool(), nullptr);
    ASSERT_NE(env->pk_index_execution_thread_pool(), nullptr);
    ASSERT_NE(env->pk_index_memtable_flush_thread_pool(), nullptr);
    ASSERT_NE(env->lake_partial_update_thread_pool(), nullptr);
    ASSERT_EQ(env->pk_index_execution_thread_pool()->max_threads(), 1);
#endif

    env->shutdown_thread_pools();
    env->destroy_thread_pools();
    ASSERT_EQ(env->lake_metadata_fetch_thread_pool(), nullptr);
}

} // namespace starrocks
