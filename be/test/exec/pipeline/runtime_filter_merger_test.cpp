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

#include "column/column.h"
#include "exec/pipeline/runtime_filter_types.h"
#include "exec/runtime_filter/runtime_filter_descriptor.h"
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

// A singleton (non-multi-partitioned) build descriptor with a consumer, used to drive the merger.
TRuntimeFilterDescription make_merger_desc(int32_t filter_id, bool remote) {
    TRuntimeFilterDescription desc;
    desc.__set_filter_id(filter_id);
    desc.__set_expr_order(0);
    desc.__set_has_remote_targets(remote);
    desc.__set_build_join_mode(TRuntimeFilterBuildJoinMode::PARTITIONED);
    desc.__set_filter_type(TRuntimeFilterBuildType::JOIN_FILTER);
    desc.__set_build_expr(ExprsTestHelper::create_column_ref_t_expr<TYPE_INT>(2, true));
    desc.__set_layout(make_layout(filter_id));
    desc.__set_plan_node_id_to_target_expr({{11, ExprsTestHelper::create_column_ref_t_expr<TYPE_INT>(3, true)}});
    return desc;
}

// Drives a single-builder merge with the given build-side row count and NDV estimate, and returns the
// build descriptor after the merge. The bloom is sized/gated by NDV; passing empty Columns keeps the
// fill a no-op (the test only checks whether the filter is kept/emptied and how it is sized).
RuntimeFilterBuildDescriptor* run_single_merge(ObjectPool* pool, RuntimeState* state, bool remote, size_t ht_row_count,
                                               size_t ht_ndv) {
    pipeline::PartialRuntimeFilterMerger merger(pool, /*local_rf_limit=*/1024000,
                                                /*global_rf_limit=*/64 * 1024 * 1024,
                                                TFunctionVersion::type::RUNTIME_FILTER_SERIALIZE_VERSION_3,
                                                /*enable_join_runtime_bitset_filter=*/false);
    merger.incr_builder();

    auto* desc = pool->add(new RuntimeFilterBuildDescriptor());
    CHECK(desc->init(pool, make_merger_desc(7, remote), state).ok());
    pipeline::RuntimeMembershipFilters descs{desc};

    pipeline::OpTRuntimeBloomFilterBuildParams params;
    params.emplace_back(pipeline::RuntimeMembershipFilterBuildParam(
            /*multi_partitioned=*/false, /*eq_null=*/false, /*is_empty=*/false, Columns{},
            /*runtime_filter=*/nullptr, TypeDescriptor::from_logical_type(TYPE_INT)));

    auto merged = merger.add_partial_filters(0, ht_row_count, ht_ndv, pipeline::RuntimeInFilters{}, std::move(params),
                                             std::move(descs));
    CHECK(merged.ok());
    CHECK(merged.value()); // single builder -> this call performs the merge
    return desc;
}

} // namespace

class RuntimeFilterMergerTest : public ::testing::Test {
protected:
    ObjectPool pool;
    RuntimeState runtime_state;
};

// A remote (global) build with many rows but a low NDV stays under the byte cap, so the bloom is kept
// and sized by the NDV rather than the row count -- the case the row-count cap used to empty.
TEST_F(RuntimeFilterMergerTest, MergerGlobalBloomKeptAndSizedByLowNdv) {
    auto* desc = run_single_merge(&pool, &runtime_state, /*remote=*/true, /*ht_row_count=*/600000000,
                                  /*ht_ndv=*/10000000);
    ASSERT_NE(desc->runtime_filter(), nullptr);
    EXPECT_TRUE(desc->runtime_filter()->get_membership_filter()->can_use_bf());
    EXPECT_EQ(desc->runtime_filter()->get_membership_filter()->size(), 10000000u);
}

// A remote build whose NDV exceeds the global byte cap has its bloom emptied (min/max only).
TEST_F(RuntimeFilterMergerTest, MergerGlobalBloomEmptiedAboveNdvCap) {
    auto* desc = run_single_merge(&pool, &runtime_state, /*remote=*/true, /*ht_row_count=*/600000000,
                                  /*ht_ndv=*/100000000);
    ASSERT_NE(desc->runtime_filter(), nullptr);
    EXPECT_FALSE(desc->runtime_filter()->get_membership_filter()->can_use_bf());
}

// A local build whose NDV exceeds the local cap is skipped (no filter), even with many rows.
TEST_F(RuntimeFilterMergerTest, MergerLocalBloomSkippedAboveNdvCap) {
    auto* desc = run_single_merge(&pool, &runtime_state, /*remote=*/false, /*ht_row_count=*/600000000,
                                  /*ht_ndv=*/2000000);
    EXPECT_EQ(desc->runtime_filter(), nullptr);
}

// A local build with many rows but a low NDV stays under the local cap, so the filter is built.
TEST_F(RuntimeFilterMergerTest, MergerLocalBloomKeptBelowNdvCap) {
    auto* desc = run_single_merge(&pool, &runtime_state, /*remote=*/false, /*ht_row_count=*/600000000,
                                  /*ht_ndv=*/500000);
    ASSERT_NE(desc->runtime_filter(), nullptr);
    EXPECT_TRUE(desc->runtime_filter()->get_membership_filter()->can_use_bf());
}

} // namespace starrocks
