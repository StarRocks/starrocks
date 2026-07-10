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

#include "agent/resource_group_usage_recorder.h"

#include "base/testutil/assert.h"
#include "common/system/cpu_info.h"
#include "compute_env/compute_env.h"
#include "compute_env/workgroup/work_group.h"
#include "exec/exec_env.h"
#include "exec/pipeline/driver_executor_factory.h"
#include "exec/pipeline/driver_queue_factory.h"
#include "gtest/gtest.h"

namespace starrocks {

TEST(ResourceGroupUsageRecorderTest, test_get_resource_group_usages) {
    const size_t num_cores = CpuInfo::num_cores();

    auto& exec_env = *ExecEnv::GetInstance();
    // Save original workgroup_manager to restore at end (otherwise subsequent tests fail)
    auto original_wg_manager = std::move(exec_env.compute_env()->_workgroup_manager);

    ComputeEnvOptions compute_env_options;
    compute_env_options.driver_queue_factory = pipeline::create_query_shared_driver_queue;
    compute_env_options.driver_executor_factory = pipeline::create_workgroup_driver_executor;
    ASSERT_OK(exec_env.compute_env()->_init_workgroup(compute_env_options, num_cores));
    auto default_wg = exec_env.workgroup_manager()->get_default_workgroup();

    ResourceGroupUsageRecorder recorder;
    ASSERT_TRUE(recorder.get_resource_group_usages().empty());

    default_wg->incr_cpu_runtime_ns(num_cores * 1000'000'000'000ull);
    const auto group_usages = recorder.get_resource_group_usages();
    ASSERT_EQ(group_usages.size(), 1);
    ASSERT_EQ(group_usages[0].group_id, default_wg->id());
    ASSERT_EQ(group_usages[0].cpu_core_used_permille, num_cores * 1000);
    ASSERT_EQ(group_usages[0].mem_pool, workgroup::WorkGroup::DEFAULT_MEM_POOL);
    ASSERT_EQ(group_usages[0].mem_limit_bytes, default_wg->mem_limit_bytes());
    ASSERT_EQ(group_usages[0].mem_pool_mem_limit_bytes, default_wg->mem_limit_bytes());

    // Restore original workgroup_manager
    exec_env.compute_env()->_stop_workgroup();
    exec_env.compute_env()->_workgroup_manager->destroy();
    exec_env.compute_env()->_workgroup_manager = std::move(original_wg_manager);
}

} // namespace starrocks
