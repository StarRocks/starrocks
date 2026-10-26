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

#include "base/testutil/assert.h"
#include "column/fixed_length_column.h"
#include "exec/pipeline/runtime_filter_types.h"
#include "runtime/runtime_filter.h"
#include "runtime/runtime_state.h"
#include "testutil/exprs_test_helper.h"

namespace starrocks::pipeline {

namespace {

constexpr size_t kLocalRfLimit = 100;
constexpr size_t kGlobalRfLimit = 1 << 20;
constexpr int kFuncVersion = TFunctionVersion::RUNTIME_FILTER_SERIALIZE_VERSION_3;

TRuntimeFilterDescription make_build_desc(
        bool has_remote_targets, TRuntimeFilterBuildJoinMode::type join_mode = TRuntimeFilterBuildJoinMode::BROADCAST,
        bool multi_partitioned = false) {
    TRuntimeFilterLayout layout;
    layout.__set_filter_id(1);
    layout.__set_local_layout(TRuntimeFilterLayoutMode::SINGLETON);
    layout.__set_global_layout(TRuntimeFilterLayoutMode::SINGLETON);
    layout.__set_pipeline_level_multi_partitioned(multi_partitioned);
    layout.__set_num_instances(1);
    layout.__set_num_drivers_per_instance(1);

    TRuntimeFilterDescription desc;
    desc.__set_filter_id(1);
    desc.__set_expr_order(0);
    desc.__set_has_remote_targets(has_remote_targets);
    desc.__set_build_join_mode(join_mode);
    desc.__set_filter_type(TRuntimeFilterBuildType::JOIN_FILTER);
    desc.__set_build_expr(ExprsTestHelper::create_column_ref_t_expr<TYPE_INT>(1, false));
    desc.__set_layout(layout);
    desc.__set_plan_node_id_to_target_expr({{2, ExprsTestHelper::create_column_ref_t_expr<TYPE_INT>(3, false)}});
    return desc;
}

// A hash table key column: the dummy row at kHashJoinKeyColumnOffset followed by `num_rows` keys `i * step`.
ColumnPtr make_key_column(size_t num_rows, int32_t step) {
    auto column = Int32Column::create();
    column->append(0);
    for (size_t i = 1; i <= num_rows; ++i) {
        column->append(static_cast<int32_t>(i) * step);
    }
    return column;
}

} // namespace

class PartialRuntimeFilterMergerTest : public ::testing::Test {
protected:
    // Merges a single broadcast build of `num_rows` keys spaced by `step`, and returns the merged filter.
    RuntimeFilter* merge(bool has_remote_targets, size_t num_rows, int32_t step) {
        auto* desc = _pool.add(new RuntimeFilterBuildDescriptor());
        EXPECT_OK(desc->init(&_pool, make_build_desc(has_remote_targets), &_runtime_state));
        Columns columns{make_key_column(num_rows, step)};
        OpTRuntimeBloomFilterBuildParams params;
        params.emplace_back(RuntimeMembershipFilterBuildParam(false, false, false, std::move(columns), nullptr,
                                                              TypeDescriptor(TYPE_INT)));

        PartialRuntimeFilterMerger merger(&_pool, kLocalRfLimit, kGlobalRfLimit, kFuncVersion, true);
        merger.incr_builder();
        auto merged = merger.add_partial_filters(0, num_rows, {}, std::move(params), {desc});
        EXPECT_TRUE(merged.ok());
        EXPECT_TRUE(merged.value());
        return desc->runtime_filter();
    }

    RuntimeFilterBuildDescriptor* make_desc(
            TRuntimeFilterBuildJoinMode::type join_mode = TRuntimeFilterBuildJoinMode::BROADCAST,
            bool multi_partitioned = false, bool has_remote_targets = false) {
        auto* desc = _pool.add(new RuntimeFilterBuildDescriptor());
        EXPECT_OK(
                desc->init(&_pool, make_build_desc(has_remote_targets, join_mode, multi_partitioned), &_runtime_state));
        return desc;
    }

    ObjectPool _pool;
    RuntimeState _runtime_state;
};

// The local row limit bounds the bloom filter; a bitset filter over a small key range is built beyond it.
TEST_F(PartialRuntimeFilterMergerTest, LocalFilterOverRowLimitUsesBitset) {
    auto* rf = merge(false, 1000, 1);
    ASSERT_NE(nullptr, rf);
    ASSERT_EQ(RuntimeFilterSerializeType::BITSET_FILTER, rf->type());
}

TEST_F(PartialRuntimeFilterMergerTest, LocalFilterOverRowLimitWithWideKeyRangeIsSkipped) {
    auto* rf = merge(false, 1000, 1000000);
    ASSERT_EQ(nullptr, rf);
}

TEST_F(PartialRuntimeFilterMergerTest, LocalFilterWithinRowLimitWithWideKeyRangeUsesBloom) {
    auto* rf = merge(false, kLocalRfLimit, 1000000);
    ASSERT_NE(nullptr, rf);
    ASSERT_EQ(RuntimeFilterSerializeType::BLOOM_FILTER, rf->type());
}

TEST_F(PartialRuntimeFilterMergerTest, RemoteFilterOverLocalRowLimitIsUnchanged) {
    auto* bitset = merge(true, 1000, 1);
    ASSERT_NE(nullptr, bitset);
    ASSERT_EQ(RuntimeFilterSerializeType::BITSET_FILTER, bitset->type());

    auto* bloom = merge(true, 1000, 1000000);
    ASSERT_NE(nullptr, bloom);
    ASSERT_EQ(RuntimeFilterSerializeType::BLOOM_FILTER, bloom->type());
}

// HashJoiner keeps the build columns of a local filter over the row limit only when it will be a bitset filter.
TEST_F(PartialRuntimeFilterMergerTest, CanBuildLocalBitsetFilter) {
    const Columns narrow{make_key_column(1000, 1)};
    const Columns wide{make_key_column(1000, 1000000)};
    auto* broadcast = make_desc();
    ASSERT_TRUE(PartialRuntimeFilterMerger::can_build_local_bitset_filter(true, kFuncVersion, false, broadcast, narrow,
                                                                          1000, kGlobalRfLimit));
    // the key range makes a bitset larger than a bloom filter
    ASSERT_FALSE(PartialRuntimeFilterMerger::can_build_local_bitset_filter(true, kFuncVersion, false, broadcast, wide,
                                                                           1000, kGlobalRfLimit));
    // bitset filters disabled, or a version that can not serialize them
    ASSERT_FALSE(PartialRuntimeFilterMerger::can_build_local_bitset_filter(false, kFuncVersion, false, broadcast,
                                                                           narrow, 1000, kGlobalRfLimit));
    ASSERT_FALSE(PartialRuntimeFilterMerger::can_build_local_bitset_filter(
            true, TFunctionVersion::RUNTIME_FILTER_SERIALIZE_VERSION_2, false, broadcast, narrow, 1000,
            kGlobalRfLimit));
    // more build rows than the global limit: the merger does not build a bitset filter either
    ASSERT_FALSE(PartialRuntimeFilterMerger::can_build_local_bitset_filter(true, kFuncVersion, false, broadcast, narrow,
                                                                           1000, 999));
    // only a broadcast join that is not skew builds a bitset filter
    ASSERT_FALSE(PartialRuntimeFilterMerger::can_build_local_bitset_filter(true, kFuncVersion, true, broadcast, narrow,
                                                                           1000, kGlobalRfLimit));
    ASSERT_FALSE(PartialRuntimeFilterMerger::can_build_local_bitset_filter(
            true, kFuncVersion, false, make_desc(TRuntimeFilterBuildJoinMode::PARTITIONED), narrow, 1000,
            kGlobalRfLimit));
    // multi-partitioned and colocate filters are merged as bloom filters
    ASSERT_FALSE(PartialRuntimeFilterMerger::can_build_local_bitset_filter(
            true, kFuncVersion, false, make_desc(TRuntimeFilterBuildJoinMode::BROADCAST, true), narrow, 1000,
            kGlobalRfLimit));
    auto* colocate = make_desc();
    colocate->set_num_colocate_partition(2);
    ASSERT_FALSE(PartialRuntimeFilterMerger::can_build_local_bitset_filter(true, kFuncVersion, false, colocate, narrow,
                                                                           1000, kGlobalRfLimit));
}

// HashJoiner drops the build columns of a local filter over the row limit unless it will be a bitset filter.
TEST_F(PartialRuntimeFilterMergerTest, SkipsLocalFilterOverLimit) {
    const Columns narrow{make_key_column(1000, 1)};
    const Columns wide{make_key_column(1000, 1000000)};
    auto* local = make_desc();
    auto* remote = make_desc(TRuntimeFilterBuildJoinMode::BROADCAST, false, true);
    // within the limit, or remote: never skipped by the local limit
    ASSERT_FALSE(PartialRuntimeFilterMerger::skips_local_filter_over_limit(true, kFuncVersion, false, local, wide, 1000,
                                                                           kLocalRfLimit * 10, kGlobalRfLimit));
    ASSERT_FALSE(PartialRuntimeFilterMerger::skips_local_filter_over_limit(true, kFuncVersion, false, remote, wide,
                                                                           1000, kLocalRfLimit, kGlobalRfLimit));
    // over the limit: kept only as a bitset filter
    ASSERT_FALSE(PartialRuntimeFilterMerger::skips_local_filter_over_limit(true, kFuncVersion, false, local, narrow,
                                                                           1000, kLocalRfLimit, kGlobalRfLimit));
    ASSERT_TRUE(PartialRuntimeFilterMerger::skips_local_filter_over_limit(true, kFuncVersion, false, local, wide, 1000,
                                                                          kLocalRfLimit, kGlobalRfLimit));
    ASSERT_TRUE(PartialRuntimeFilterMerger::skips_local_filter_over_limit(false, kFuncVersion, false, local, narrow,
                                                                          1000, kLocalRfLimit, kGlobalRfLimit));
}

} // namespace starrocks::pipeline
