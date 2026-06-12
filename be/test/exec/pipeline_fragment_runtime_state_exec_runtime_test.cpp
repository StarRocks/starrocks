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

#include <cstdint>

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

TEST(FragmentRuntimeStateTest, StoresFeAddress) {
    FragmentRuntimeState state;
    TNetworkAddress fe_addr;
    fe_addr.hostname = "127.0.0.1";
    fe_addr.port = 9030;

    state.set_fe_addr(fe_addr);

    EXPECT_EQ("127.0.0.1", state.fe_addr().hostname);
    EXPECT_EQ(9030, state.fe_addr().port);
}

TEST(FragmentRuntimeStateTest, StoresAdaptiveDopFlag) {
    FragmentRuntimeState state;

    EXPECT_FALSE(state.enable_adaptive_dop());

    state.set_enable_adaptive_dop(true);
    EXPECT_TRUE(state.enable_adaptive_dop());

    state.set_enable_adaptive_dop(false);
    EXPECT_FALSE(state.enable_adaptive_dop());
}

TEST(FragmentRuntimeStateTest, StoresPredicateTreeParams) {
    FragmentRuntimeState state;

    EXPECT_FALSE(state.pred_tree_params().enable_or);
    EXPECT_FALSE(state.pred_tree_params().enable_show_in_profile);
    EXPECT_EQ(PredicateTreeParams::DEFAULT_MAX_PUSHDOWN_OR_PREDICATES,
              state.pred_tree_params().max_pushdown_or_predicates);

    PredicateTreeParams params;
    params.enable_or = true;
    params.enable_show_in_profile = true;
    params.max_pushdown_or_predicates = 64;

    state.set_pred_tree_params(params);

    EXPECT_TRUE(state.pred_tree_params().enable_or);
    EXPECT_TRUE(state.pred_tree_params().enable_show_in_profile);
    EXPECT_EQ(64, state.pred_tree_params().max_pushdown_or_predicates);
}

TEST(FragmentRuntimeStateTest, StoresWorkgroupPointer) {
    FragmentRuntimeState state;

    EXPECT_EQ(nullptr, state.workgroup());

    auto* raw_workgroup = reinterpret_cast<workgroup::WorkGroup*>(static_cast<uintptr_t>(0x1234));
    workgroup::WorkGroupPtr workgroup(raw_workgroup, [](workgroup::WorkGroup*) {});

    state.set_workgroup(workgroup);

    EXPECT_EQ(raw_workgroup, state.workgroup().get());
}

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
