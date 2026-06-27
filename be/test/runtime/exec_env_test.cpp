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

#include <filesystem>
#include <utility>

#include "base/metrics.h"
#include "base/testutil/assert.h"
#include "common/config_storage_fwd.h"
#include "compute_env/compute_env.h"
#include "compute_env/profile_report_worker.h"
#include "exec/pipeline/driver_executor_factory.h"
#include "exec/pipeline/driver_queue_factory.h"
#include "platform/platform_env.h"
#include "runtime/env/global_env.h"
#include "runtime/runtime_filter_query_lifecycle.h"
#include "runtime/runtime_filter_sender.h"
#include "storage/storage_env.h"

namespace starrocks {

namespace {

class FakeRuntimeFilterServices : public RuntimeFilterSender, public RuntimeFilterQueryLifecycle {
public:
    void open_query(const TUniqueId&, const TQueryOptions&, const TRuntimeFilterParams&, bool) override {}
    void close_query(const TUniqueId&) override {}
    void send_part_runtime_filter(PTransmitRuntimeFilterParams&&, const std::vector<TNetworkAddress>&, int, int64_t,
                                  EventType) override {}
    void send_broadcast_runtime_filter(PTransmitRuntimeFilterParams&&, const std::vector<TRuntimeFilterDestination>&,
                                       int, int64_t) override {}
};

} // namespace

TEST(ExecEnvTest, refresh_service_contexts_keeps_context_views_in_sync) {
    ExecEnv env;
    auto* global_env = GlobalEnv::GetInstance();
    auto* platform_env = PlatformEnv::GetInstance();
    platform_env->destroy();

    static auto* metrics = new MetricRegistry("exec_env_test");
    ASSERT_OK(platform_env->init(PlatformEnvOptions{.metrics = metrics}));

    EXPECT_EQ(env.runtime_services().lookup_dispatcher_mgr, nullptr);
    EXPECT_EQ(env.runtime_services().load_path_mgr, nullptr);
    EXPECT_EQ(env.runtime_services().cache_mgr, nullptr);
    EXPECT_EQ(env.runtime_services().spill_dir_mgr, nullptr);
    EXPECT_EQ(env.runtime_services().global_spill_manager, nullptr);
    EXPECT_EQ(env.runtime_services().runtime_filter_sender, nullptr);
    EXPECT_EQ(env.runtime_services().runtime_filter_query_lifecycle, nullptr);

    ComputeEnvOptions compute_env_options;
    compute_env_options.max_num_pipeline_drivers = 2;
    ASSERT_OK(env.compute_env()->init(compute_env_options));
    ComputeEnvWorkGroupOptions workgroup_options;
    workgroup_options.max_executor_threads = 2;
    workgroup_options.metrics = metrics;
    workgroup_options.driver_queue_factory = pipeline::create_query_shared_driver_queue;
    workgroup_options.driver_executor_factory = pipeline::create_workgroup_driver_executor;
    ASSERT_OK(env.compute_env()->init_workgroup(workgroup_options));
    std::error_code ec;
    std::filesystem::create_directories(config::spill_local_storage_dir, ec);
    ASSERT_FALSE(ec) << ec.message();
    ASSERT_OK(env.compute_env()->init_spill({config::storage_root_path}, metrics));
    ASSERT_OK(env.compute_env()->init_load_path({}, true));
    ProfileReportWorkerOptions profile_report_worker_options;
    profile_report_worker_options.start_worker_thread = false;
    profile_report_worker_options.report_non_pipeline_fragments = [](const std::vector<TUniqueId>&) {
        return std::vector<TUniqueId>();
    };
    profile_report_worker_options.report_pipeline_fragments = [](const std::vector<PipeLineReportTaskKey>&) {
        return std::vector<PipeLineReportTaskKey>();
    };
    ASSERT_OK(env.compute_env()->init_profile_report_worker(std::move(profile_report_worker_options)));
    env._query_context_mgr = reinterpret_cast<pipeline::QueryContextManager*>(0x9);
    auto* agent_server = reinterpret_cast<AgentServer*>(0xa);
    FakeRuntimeFilterServices runtime_filter_services;

    env.set_runtime_filter_services(&runtime_filter_services, &runtime_filter_services);
    env.set_agent_server(agent_server);

    EXPECT_EQ(env.execution_services().thread_pool, global_env->thread_pool());
    EXPECT_EQ(env.execution_services().workgroup_manager, env.compute_env()->workgroup_manager());
    EXPECT_EQ(env.execution_services().driver_limiter, env.compute_env()->driver_limiter());
    EXPECT_EQ(env.execution_services().pipeline_timer, env.compute_env()->pipeline_timer());
    EXPECT_EQ(env.execution_services().max_executor_threads, global_env->max_executor_threads());
    EXPECT_EQ(env.stream_mgr(), env.compute_env()->stream_mgr());
    EXPECT_EQ(env.result_mgr(), env.compute_env()->result_mgr());
    EXPECT_EQ(env.result_queue_mgr(), env.compute_env()->result_queue_mgr());
    EXPECT_EQ(env.profile_report_worker(), env.compute_env()->profile_report_worker());

    EXPECT_EQ(env.rpc_services().backend_client_cache, platform_env->backend_client_cache());
    EXPECT_EQ(env.rpc_services().frontend_client_cache, platform_env->frontend_client_cache());
    EXPECT_EQ(env.rpc_services().broker_client_cache, platform_env->broker_client_cache());
    EXPECT_EQ(env.rpc_services().broker_mgr, platform_env->broker_mgr());
    EXPECT_EQ(env.rpc_services().brpc_stub_cache, platform_env->brpc_stub_cache());
    EXPECT_EQ(env.platform_services().store_path_registry, platform_env->store_path_registry());

    EXPECT_EQ(env.lake_services().lake_tablet_manager, StorageEnv::GetInstance()->lake_tablet_manager());
    EXPECT_EQ(env.lake_services().lake_update_manager, StorageEnv::GetInstance()->lake_update_manager());
    EXPECT_EQ(env.lake_services().lake_replication_txn_manager,
              StorageEnv::GetInstance()->lake_replication_txn_manager());
    EXPECT_EQ(env.lake_services().lake_vector_index_build_thread_pool,
              global_env->lake_vector_index_build_thread_pool());

    EXPECT_EQ(env.runtime_services().query_context_mgr, env._query_context_mgr);
    EXPECT_EQ(env.runtime_services().stream_mgr, env.compute_env()->stream_mgr());
    EXPECT_EQ(env.runtime_services().result_mgr, env.compute_env()->result_mgr());
    EXPECT_EQ(env.runtime_services().result_queue_mgr, env.compute_env()->result_queue_mgr());
    EXPECT_EQ(env.load_path_mgr(), env.compute_env()->load_path_mgr());
    EXPECT_EQ(env.runtime_services().load_path_mgr, env.compute_env()->load_path_mgr());
    EXPECT_EQ(env.load_stream_mgr(), env.compute_env()->load_stream_mgr());
    EXPECT_EQ(env.runtime_services().load_stream_mgr, env.compute_env()->load_stream_mgr());
    EXPECT_EQ(env.runtime_services().profile_report_worker, env.compute_env()->profile_report_worker());
    EXPECT_EQ(env.runtime_services().spill_dir_mgr, env.compute_env()->spill_dir_mgr());
    EXPECT_EQ(env.runtime_services().global_spill_manager, env.compute_env()->global_spill_manager());
    EXPECT_EQ(env.runtime_services().runtime_filter_sender,
              static_cast<RuntimeFilterSender*>(&runtime_filter_services));
    EXPECT_EQ(env.runtime_services().runtime_filter_query_lifecycle,
              static_cast<RuntimeFilterQueryLifecycle*>(&runtime_filter_services));

    EXPECT_EQ(env.agent_services().agent_server, agent_server);

    EXPECT_EQ(env.query_execution_services().execution, &env.execution_services());
    EXPECT_EQ(env.query_execution_services().rpc, &env.rpc_services());
    EXPECT_EQ(env.query_execution_services().lake, &env.lake_services());
    EXPECT_EQ(env.query_execution_services().runtime, &env.runtime_services());

    EXPECT_EQ(env.admin_services().execution, &env.execution_services());
    EXPECT_EQ(env.admin_services().rpc, &env.rpc_services());
    EXPECT_EQ(env.admin_services().lake, &env.lake_services());
    EXPECT_EQ(env.admin_services().runtime, &env.runtime_services());
    EXPECT_EQ(env.admin_services().agent, &env.agent_services());

    env.compute_env()->stop_workgroup();
    env.compute_env()->destroy();
}

} // namespace starrocks
