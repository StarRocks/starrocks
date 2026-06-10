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

#include "common/runtime_profile.h"
#include "compute_env/workgroup/work_group_fwd.h"
#include "exec/runtime/fragment_runtime_state.h"

namespace starrocks::pipeline {

TEST(FragmentRuntimeStateTest, StoresFragmentInstanceId) {
    FragmentRuntimeState state;
    TUniqueId fragment_instance_id;
    fragment_instance_id.hi = 3;
    fragment_instance_id.lo = 4;

    state.set_fragment_instance_id(fragment_instance_id);

    EXPECT_EQ(3, state.fragment_instance_id().hi);
    EXPECT_EQ(4, state.fragment_instance_id().lo);
}

TEST(FragmentRuntimeStateTest, EnableCacheDefaultsFalseAndRoundTrips) {
    FragmentRuntimeState state;

    EXPECT_FALSE(state.enable_cache());

    state.set_enable_cache(true);
    EXPECT_TRUE(state.enable_cache());

    state.set_enable_cache(false);
    EXPECT_FALSE(state.enable_cache());
}

TEST(FragmentRuntimeStateTest, StoresReadOnlyFragmentRuntimeParameters) {
    FragmentRuntimeState state;

    TNetworkAddress fe_addr;
    fe_addr.hostname = "fe.example";
    fe_addr.port = 9030;
    state.set_fe_addr(fe_addr);

    PredicateTreeParams params;
    params.enable_or = true;
    params.enable_show_in_profile = true;
    params.max_pushdown_or_predicates = 17;
    state.set_pred_tree_params(params);

    state.set_enable_adaptive_dop(true);

    workgroup::WorkGroupPtr workgroup(reinterpret_cast<workgroup::WorkGroup*>(0x1234), [](workgroup::WorkGroup*) {});
    state.set_workgroup(workgroup);

    EXPECT_EQ("fe.example", state.fe_addr().hostname);
    EXPECT_EQ(9030, state.fe_addr().port);
    EXPECT_TRUE(state.pred_tree_params().enable_or);
    EXPECT_TRUE(state.pred_tree_params().enable_show_in_profile);
    EXPECT_EQ(17, state.pred_tree_params().max_pushdown_or_predicates);
    EXPECT_TRUE(state.enable_adaptive_dop());
    EXPECT_EQ(workgroup.get(), state.workgroup().get());
}

TEST(FragmentRuntimeStateTest, JitProfileCounterUpdateNoOpsWhenCountersAreNull) {
    FragmentRuntimeState state;

    EXPECT_NO_FATAL_FAILURE(state.update_jit_profile(10));
}

TEST(FragmentRuntimeStateTest, JitProfileCounterUpdateUsesInstalledCounters) {
    FragmentRuntimeState state;
    RuntimeProfile profile("jit");
    auto* jit_counter = ADD_COUNTER(&profile, "JITCounter", TUnit::UNIT);
    auto* jit_timer = ADD_TIMER(&profile, "JITTotalCostTime");

    state.set_jit_profile_counters(jit_counter, jit_timer);
    state.update_jit_profile(10);
    state.update_jit_profile(7);

    EXPECT_EQ(2, COUNTER_VALUE(jit_counter));
    EXPECT_EQ(17, COUNTER_VALUE(jit_timer));

    state.clear_jit_profile_counters();
    state.update_jit_profile(5);
    EXPECT_EQ(2, COUNTER_VALUE(jit_counter));
    EXPECT_EQ(17, COUNTER_VALUE(jit_timer));
}

#ifndef NDEBUG
TEST(FragmentRuntimeStateDeathTest, SealedFragmentRuntimeParametersRejectLateMutation) {
    FragmentRuntimeState state;
    state.seal_runtime_parameters();

    EXPECT_DEATH(state.set_enable_adaptive_dop(true), "");
}
#endif

TEST(FragmentRuntimeStateTest, RuntimeFilterHubAccessorIsStable) {
    FragmentRuntimeState state;

    auto* hub = state.runtime_filter_hub();

    ASSERT_NE(nullptr, hub);
    EXPECT_EQ(hub, state.runtime_filter_hub());

    const auto& const_state = state;
    EXPECT_EQ(hub, const_state.runtime_filter_hub());
}

TEST(FragmentRuntimeStateTest, FinalStatusDefaultsOk) {
    FragmentRuntimeState state;

    EXPECT_TRUE(state.final_status().ok());
}

} // namespace starrocks::pipeline
