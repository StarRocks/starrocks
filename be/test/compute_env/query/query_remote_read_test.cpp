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

#include "compute_env/query/query_remote_read.h"

#include <gtest/gtest.h>

#include "compute_env/query/fragment_runtime_state.h"
#include "compute_env/workgroup/work_group.h"
#include "runtime/runtime_state.h"

namespace starrocks {

static TUniqueId make_id(int64_t hi, int64_t lo) {
    TUniqueId id;
    id.hi = hi;
    id.lo = lo;
    return id;
}

// Every fragment of a query reads in one flow, as the query's resource group with the cpu_weight and configuration
// version the query started with.
TEST(QueryRemoteReadTest, query_reads_as_its_resource_group) {
    const TUniqueId query_id = make_id(11, 22);
    auto wg = std::make_shared<workgroup::WorkGroup>("rg", 1001, /*version=*/7, /*cpu_weight=*/16, 0.5, 0, 0.9,
                                                     workgroup::WorkGroupType::WG_NORMAL,
                                                     workgroup::WorkGroup::DEFAULT_MEM_POOL);
    pipeline::FragmentRuntimeState fragment_state_1;
    pipeline::FragmentRuntimeState fragment_state_2;
    fragment_state_1.set_workgroup(wg);
    fragment_state_2.set_workgroup(wg);
    RuntimeState state_1(query_id, make_id(1, 1), TQueryOptions(), TQueryGlobals(), nullptr);
    RuntimeState state_2(query_id, make_id(1, 2), TQueryOptions(), TQueryGlobals(), nullptr);
    state_1.set_fragment_ctx(nullptr, &fragment_state_1);
    state_2.set_fragment_ctx(nullptr, &fragment_state_2);

    const auto ctx_1 = query_remote_read_context(&state_1);
    const auto ctx_2 = query_remote_read_context(&state_2);
    EXPECT_TRUE(ctx_1.is_set);
    EXPECT_EQ(RemoteReadContext::Workload::QUERY, ctx_1.workload);
    EXPECT_EQ(1001, ctx_1.group_id);
    EXPECT_EQ(16, ctx_1.group_weight);
    EXPECT_EQ(7, ctx_1.group_config_version);
    EXPECT_EQ(RemoteReadContext::flow_id_of(11, 22), ctx_1.flow_id);
    EXPECT_EQ(ctx_1.flow_id, ctx_2.flow_id);

    // Another query is another flow.
    RuntimeState other(make_id(33, 44), make_id(2, 1), TQueryOptions(), TQueryGlobals(), nullptr);
    other.set_fragment_ctx(nullptr, &fragment_state_1);
    EXPECT_NE(ctx_1.flow_id, query_remote_read_context(&other).flow_id);
}

// A query outside any resource group still reads as a query, with the default group and weight.
TEST(QueryRemoteReadTest, query_without_resource_group) {
    pipeline::FragmentRuntimeState fragment_state;
    RuntimeState state(make_id(5, 6), make_id(1, 1), TQueryOptions(), TQueryGlobals(), nullptr);
    state.set_fragment_ctx(nullptr, &fragment_state);
    auto ctx = query_remote_read_context(&state);
    EXPECT_TRUE(ctx.is_set);
    EXPECT_EQ(RemoteReadContext::Workload::QUERY, ctx.workload);
    EXPECT_EQ(0, ctx.group_id);
    EXPECT_EQ(1, ctx.group_weight);
    EXPECT_EQ(0, ctx.group_config_version);

    RuntimeState no_fragment(make_id(5, 6), make_id(1, 1), TQueryOptions(), TQueryGlobals(), nullptr);
    ctx = query_remote_read_context(&no_fragment);
    EXPECT_TRUE(ctx.is_set);
    EXPECT_EQ(0, ctx.group_id);
    EXPECT_EQ(RemoteReadContext::flow_id_of(5, 6), ctx.flow_id);
}

} // namespace starrocks
