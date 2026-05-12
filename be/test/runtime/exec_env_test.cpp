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

namespace starrocks {

TEST(GlobalEnvTest, calc_query_mem_limit) {
    ASSERT_EQ(GlobalEnv::calc_max_query_memory(-1, 80), -1);
    ASSERT_EQ(GlobalEnv::calc_max_query_memory(1000000000, -2), 900000000);
    ASSERT_EQ(GlobalEnv::calc_max_query_memory(1000000000, 102), 900000000);
    ASSERT_EQ(GlobalEnv::calc_max_query_memory(1000000000, 70), 700000000);
}

TEST(ExecEnvTest, refresh_service_contexts_keeps_context_views_in_sync) {
    ExecEnv env;

    EXPECT_EQ(env.runtime_services().lookup_dispatcher_mgr, nullptr);
    EXPECT_EQ(env.runtime_services().cache_mgr, nullptr);

    env._thread_pool = reinterpret_cast<PriorityThreadPool*>(0x1);
    env._driver_limiter = reinterpret_cast<pipeline::DriverLimiter*>(0x2);
    env._pipeline_timer = reinterpret_cast<pipeline::PipelineTimer*>(0x3);
    env._max_executor_threads = 17;
    env._backend_client_cache = reinterpret_cast<ClientCache<BackendServiceClient>*>(0x4);
    env._broker_mgr = reinterpret_cast<BrokerMgr*>(0x5);
    env._brpc_stub_cache = reinterpret_cast<BrpcStubCache*>(0x6);
    env._lake_tablet_manager = reinterpret_cast<lake::TabletManager*>(0x7);
    env._fragment_mgr = reinterpret_cast<FragmentMgr*>(0x8);
    env._query_context_mgr = reinterpret_cast<pipeline::QueryContextManager*>(0x9);
    env._agent_server = reinterpret_cast<AgentServer*>(0xa);
    env._heartbeat_flags = reinterpret_cast<HeartbeatFlags*>(0xb);

    env._refresh_service_contexts();

    EXPECT_EQ(env.execution_services().thread_pool, env._thread_pool);
    EXPECT_EQ(env.execution_services().workgroup_manager, nullptr);
    EXPECT_EQ(env.execution_services().driver_limiter, env._driver_limiter);
    EXPECT_EQ(env.execution_services().pipeline_timer, env._pipeline_timer);
    EXPECT_EQ(env.execution_services().max_executor_threads, env._max_executor_threads);

    EXPECT_EQ(env.rpc_services().backend_client_cache, env._backend_client_cache);
    EXPECT_EQ(env.rpc_services().broker_mgr, env._broker_mgr);
    EXPECT_EQ(env.rpc_services().brpc_stub_cache, env._brpc_stub_cache);

    EXPECT_EQ(env.lake_services().lake_tablet_manager, env._lake_tablet_manager);

    EXPECT_EQ(env.runtime_services().fragment_mgr, env._fragment_mgr);
    EXPECT_EQ(env.runtime_services().query_context_mgr, env._query_context_mgr);

    EXPECT_EQ(env.agent_services().agent_server, env._agent_server);
    EXPECT_EQ(env.agent_services().heartbeat_flags, env._heartbeat_flags);

    EXPECT_EQ(env.query_execution_services().execution, &env.execution_services());
    EXPECT_EQ(env.query_execution_services().rpc, &env.rpc_services());
    EXPECT_EQ(env.query_execution_services().lake, &env.lake_services());
    EXPECT_EQ(env.query_execution_services().runtime, &env.runtime_services());

    EXPECT_EQ(env.admin_services().execution, &env.execution_services());
    EXPECT_EQ(env.admin_services().rpc, &env.rpc_services());
    EXPECT_EQ(env.admin_services().lake, &env.lake_services());
    EXPECT_EQ(env.admin_services().runtime, &env.runtime_services());
    EXPECT_EQ(env.admin_services().agent, &env.agent_services());
}

} // namespace starrocks
