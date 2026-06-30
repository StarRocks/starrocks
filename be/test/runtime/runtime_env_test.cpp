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

#include "runtime/runtime_env.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <utility>

#include "base/metrics.h"
#include "base/testutil/scoped_updater.h"
#include "common/config_exec_env_fwd.h"
#include "common/config_path_fwd.h"
#include "common/system/cpu_info.h"
#include "common/system/mem_info.h"
#include "common/thread/threadpool.h"
#include "platform/platform_env.h"
#include "runtime/env/diagnose_daemon.h"
#include "runtime/runtime_env_test_util.h"

namespace starrocks {

TEST(RuntimeEnvTest, CalcQueryMemLimit) {
    ASSERT_EQ(RuntimeEnv::calc_max_query_memory(-1, 80), -1);
    ASSERT_EQ(RuntimeEnv::calc_max_query_memory(1000000000, -2), 900000000);
    ASSERT_EQ(RuntimeEnv::calc_max_query_memory(1000000000, 102), 900000000);
    ASSERT_EQ(RuntimeEnv::calc_max_query_memory(1000000000, 70), 700000000);
}

TEST(RuntimeEnvTest, GetInstanceReturnsStableSingleton) {
    ASSERT_NE(RuntimeEnv::GetInstance(), nullptr);
    ASSERT_EQ(RuntimeEnv::GetInstance(), RuntimeEnv::GetInstance());
}

TEST(RuntimeEnvTest, OwnsHeartbeatFlags) {
    auto* env = RuntimeEnv::GetInstance();
    auto* heartbeat_flags = env->heartbeat_flags();
    ASSERT_NE(heartbeat_flags, nullptr);
    ASSERT_EQ(heartbeat_flags, env->heartbeat_flags());
}

TEST(RuntimeEnvTest, OwnsDiagnoseDaemonLifecycle) {
    CpuInfo::init();
    MemInfo::init();

    auto* env = RuntimeEnv::GetInstance();
    auto* platform_env = PlatformEnv::GetInstance();
    env->stop();
    platform_env->destroy();
    platform_env->reset_store_paths_for_test();

    const auto small_file_dir = std::filesystem::absolute("./ut_dir/runtime_env_platform_global_order");
    std::error_code ec;
    std::filesystem::remove_all(small_file_dir, ec);

    SCOPED_UPDATE(std::string, config::mem_limit, "90%");
    SCOPED_UPDATE(std::string, config::small_file_dir, small_file_dir.string());

    MetricRegistry platform_metrics("runtime_env_platform_order_test");
    PlatformEnvOptions platform_options;
    platform_options.metrics = &platform_metrics;
    auto st = platform_env->init(std::move(platform_options));
    ASSERT_TRUE(st.ok()) << st;

    MetricRegistry metrics("runtime_env_diagnose_test");
    st = env->init(&metrics);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(env->diagnose_daemon(), nullptr);
    ASSERT_NE(env->diagnose_daemon()->thread_pool(), nullptr);

    env->stop();
    ASSERT_EQ(env->diagnose_daemon(), nullptr);
    ASSERT_NE(platform_env->brpc_stub_cache(), nullptr);
    ASSERT_NE(platform_env->small_file_mgr(), nullptr);

    platform_env->destroy();
    platform_env->reset_store_paths_for_test();
    std::filesystem::remove_all(small_file_dir, ec);
}

TEST(RuntimeEnvTest, OwnsExecutionThreadPools) {
    CpuInfo::init();
    runtime_env_test::set_small_thread_pool_configs();

    auto* env = RuntimeEnv::GetInstance();
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

TEST(RuntimeEnvTest, OwnsLakeThreadPools) {
    CpuInfo::init();
    runtime_env_test::set_small_thread_pool_configs();

    auto* env = RuntimeEnv::GetInstance();
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
