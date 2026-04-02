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

#include "exec/runtime_filter/runtime_filter_descriptor.h"
#include "exec/runtime_filter/runtime_filter_helper.h"
#include "exec/runtime_filter/runtime_filter_probe.h"
#include "exprs/column_ref.h"
#include "runtime/runtime_filter.h"
#include "runtime/runtime_state.h"
#include "testutil/exprs_test_helper.h"

namespace starrocks {
namespace {

TRuntimeFilterLayout make_layout(int32_t filter_id) {
    TRuntimeFilterLayout layout;
    layout.__set_filter_id(filter_id);
    layout.__set_local_layout(TRuntimeFilterLayoutMode::SINGLETON);
    layout.__set_global_layout(TRuntimeFilterLayoutMode::GLOBAL_SHUFFLE_1L);
    layout.__set_pipeline_level_multi_partitioned(false);
    layout.__set_num_instances(1);
    layout.__set_num_drivers_per_instance(1);
    return layout;
}

TRuntimeFilterDescription make_build_desc() {
    TRuntimeFilterDescription desc;
    desc.__set_filter_id(7);
    desc.__set_expr_order(1);
    desc.__set_has_remote_targets(true);
    desc.__set_build_join_mode(TRuntimeFilterBuildJoinMode::BROADCAST);
    desc.__set_filter_type(TRuntimeFilterBuildType::JOIN_FILTER);
    desc.__set_build_expr(ExprsTestHelper::create_column_ref_t_expr<TYPE_INT>(2, true));
    desc.__set_layout(make_layout(7));
    desc.__set_plan_node_id_to_target_expr({{11, ExprsTestHelper::create_column_ref_t_expr<TYPE_INT>(3, true)}});
    return desc;
}

TRuntimeFilterDescription make_probe_desc(int32_t filter_id, TPlanNodeId node_id, TRuntimeFilterBuildType::type type) {
    TRuntimeFilterDescription desc;
    desc.__set_filter_id(filter_id);
    desc.__set_has_remote_targets(false);
    desc.__set_build_plan_node_id(5);
    desc.__set_build_join_mode(TRuntimeFilterBuildJoinMode::BROADCAST);
    desc.__set_filter_type(type);
    desc.__set_layout(make_layout(filter_id));
    desc.__set_plan_node_id_to_target_expr({{node_id, ExprsTestHelper::create_column_ref_t_expr<TYPE_INT>(9, true)}});
    return desc;
}

} // namespace

class RuntimeFilterExecCoreTest : public ::testing::Test {
protected:
    ObjectPool pool;
    RuntimeState runtime_state;
};

TEST_F(RuntimeFilterExecCoreTest, BuildDescriptorInitCapturesPlannerMetadata) {
    RuntimeFilterBuildDescriptor desc;
    ASSERT_OK(desc.init(&pool, make_build_desc(), &runtime_state));

    EXPECT_EQ(desc.filter_id(), 7);
    EXPECT_EQ(desc.build_expr_order(), 1);
    EXPECT_EQ(desc.type(), TRuntimeFilterBuildType::JOIN_FILTER);
    EXPECT_TRUE(desc.has_remote_targets());
    EXPECT_TRUE(desc.has_consumer());
    EXPECT_EQ(desc.join_mode(), TRuntimeFilterBuildJoinMode::BROADCAST);
    EXPECT_EQ(desc.layout().filter_id(), 7);
}

TEST_F(RuntimeFilterExecCoreTest, ProbeDescriptorInitDistinguishesJoinAndStreamFilters) {
    RuntimeFilterProbeDescriptor join_desc;
    ASSERT_OK(join_desc.init(&pool, make_probe_desc(17, 11, TRuntimeFilterBuildType::JOIN_FILTER), 11, &runtime_state));
    EXPECT_FALSE(join_desc.is_stream_build_filter());
    EXPECT_TRUE(join_desc.can_push_down_runtime_filter());

    auto topn_desc = make_probe_desc(19, 11, TRuntimeFilterBuildType::TOPN_FILTER);
    topn_desc.__set_plan_node_id_to_partition_by_exprs(
            {{11, std::vector<TExpr>{ExprsTestHelper::create_column_ref_t_expr<TYPE_INT>(10, true)}}});

    RuntimeFilterProbeDescriptor stream_desc;
    ASSERT_OK(stream_desc.init(&pool, topn_desc, 11, &runtime_state));
    EXPECT_TRUE(stream_desc.is_stream_build_filter());
    EXPECT_FALSE(stream_desc.can_push_down_runtime_filter());
    EXPECT_EQ(stream_desc.num_partition_by_exprs(), 1);
}

TEST_F(RuntimeFilterExecCoreTest, ProbeDescriptorExposesSlotRefAndAttachedFilter) {
    auto* probe_expr = pool.add(new ColumnRef(TypeDescriptor(TYPE_INT), 9));
    auto* probe_ctx = pool.add(new ExprContext(probe_expr));

    RuntimeFilterProbeDescriptor desc;
    ASSERT_OK(desc.init(23, probe_ctx));

    SlotId slot_id = -1;
    ASSERT_TRUE(desc.is_probe_slot_ref(&slot_id));
    EXPECT_EQ(slot_id, 9);

    auto* rf = pool.add(new ComposedRuntimeBloomFilter<TYPE_INT>());
    rf->insert(10);
    desc.set_runtime_filter(rf);
    EXPECT_EQ(desc.runtime_filter(-1), rf);
}

TEST_F(RuntimeFilterExecCoreTest, HelperCreatesMinMaxPredicateForNumericButNotString) {
    auto* numeric_filter = pool.add(new ComposedRuntimeBloomFilter<TYPE_INT>());
    numeric_filter->insert(10);
    numeric_filter->insert(20);

    Expr* min_max_predicate = nullptr;
    RuntimeFilterHelper::create_min_max_value_predicate(&pool, 1, TYPE_INT, numeric_filter, &min_max_predicate);
    ASSERT_NE(min_max_predicate, nullptr);

    auto* string_filter = pool.add(new ComposedRuntimeBloomFilter<TYPE_VARCHAR>());
    string_filter->insert(Slice("aa"));
    string_filter->insert(Slice("bb"));

    Expr* varchar_predicate = reinterpret_cast<Expr*>(0x1);
    RuntimeFilterHelper::create_min_max_value_predicate(&pool, 2, TYPE_VARCHAR, string_filter, &varchar_predicate);
    EXPECT_EQ(varchar_predicate, nullptr);
}

} // namespace starrocks
