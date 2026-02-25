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
#include <vector>

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "runtime/bucket_aware_partition.h"
#include "runtime/runtime_filter.h"
#include "runtime/runtime_filter_layout.h"

namespace starrocks {
namespace {

ColumnPtr make_int_column(const std::vector<int32_t>& values) {
    auto column = Int32Column::create();
    for (int32_t value : values) {
        column->append(value);
    }
    return column;
}

void expect_filter_eq(const Filter& actual, const std::vector<uint8_t>& expected) {
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(actual[i], expected[i]);
    }
}

TEST(RuntimeFilterCoreTest, SimdBlockFilterInsertAndTest) {
    SimdBlockFilter bf;
    bf.init(100);

    for (int i = 1; i <= 200; i += 17) {
        bf.insert_hash(i);
    }

    for (int i = 1; i <= 200; i += 17) {
        EXPECT_TRUE(bf.test_hash(i));
        EXPECT_FALSE(bf.test_hash(i + 1));
    }
}

TEST(RuntimeFilterCoreTest, SimdBlockFilterSerializeDeserializeRoundTrip) {
    SimdBlockFilter bf0;
    bf0.init(100);
    for (int i = 1; i <= 200; i += 17) {
        bf0.insert_hash(i);
    }

    const size_t serialized_size = bf0.max_serialized_size();
    std::vector<uint8_t> buffer(serialized_size, 0);
    ASSERT_EQ(bf0.serialize(buffer.data()), serialized_size);

    SimdBlockFilter bf1;
    ASSERT_EQ(bf1.deserialize(buffer.data()), serialized_size);
    EXPECT_TRUE(bf0.check_equal(bf1));
}

TEST(RuntimeFilterCoreTest, SimdBlockFilterMerge) {
    SimdBlockFilter left;
    left.init(100);
    for (int i = 1; i <= 200; i += 17) {
        left.insert_hash(i);
    }

    SimdBlockFilter right;
    right.init(100);
    for (int i = 2; i <= 200; i += 17) {
        right.insert_hash(i);
    }

    SimdBlockFilter merged;
    merged.init(100);
    merged.merge(left);
    merged.merge(right);

    for (int i = 1; i <= 200; i += 17) {
        EXPECT_TRUE(merged.test_hash(i));
        EXPECT_TRUE(merged.test_hash(i + 1));
        EXPECT_FALSE(merged.test_hash(i + 2));
    }
}

TEST(RuntimeFilterCoreTest, MinMaxRangeAndNullableSemantics) {
    MinMaxRuntimeFilter<TYPE_INT> rf;
    rf.insert(10);
    rf.insert(20);

    auto col = make_int_column({5, 10, 15, 20, 25});
    RuntimeFilter::RunningContext ctx;
    ctx.use_merged_selection = false;
    rf.evaluate(col.get(), &ctx);
    expect_filter_eq(ctx.selection, {0, 1, 1, 1, 0});

    auto nullable_col = ColumnHelper::cast_to_nullable_column(col->clone());
    nullable_col->append_nulls(2);
    rf.evaluate(nullable_col.get(), &ctx);
    expect_filter_eq(ctx.selection, {0, 1, 1, 1, 0, 0, 0});

    rf.insert_null();
    rf.evaluate(nullable_col.get(), &ctx);
    expect_filter_eq(ctx.selection, {0, 1, 1, 1, 0, 1, 1});
}

TEST(RuntimeFilterCoreTest, RuntimeBloomFilterEvaluateConstAndNullableColumns) {
    TRuntimeBloomFilter<TYPE_INT> rf;
    rf.init(100);
    rf.insert(10);
    rf.insert(20);

    RuntimeFilter::RunningContext ctx;
    ctx.use_merged_selection = false;

    auto const_hit_col = ColumnHelper::create_const_column<TYPE_INT>(10, 8);
    ctx.selection.assign(const_hit_col->size(), 1);
    rf.evaluate(const_hit_col.get(), &ctx);
    expect_filter_eq(ctx.selection, {1, 1, 1, 1, 1, 1, 1, 1});

    auto const_miss_col = ColumnHelper::create_const_column<TYPE_INT>(11, 8);
    ctx.selection.assign(const_miss_col->size(), 1);
    rf.evaluate(const_miss_col.get(), &ctx);
    expect_filter_eq(ctx.selection, {0, 0, 0, 0, 0, 0, 0, 0});

    auto const_null_col = ColumnHelper::create_const_null_column(8);
    ctx.selection.assign(const_null_col->size(), 1);
    rf.evaluate(const_null_col.get(), &ctx);
    expect_filter_eq(ctx.selection, {0, 0, 0, 0, 0, 0, 0, 0});

    auto nullable_col = ColumnHelper::cast_to_nullable_column(make_int_column({10, 11, 20, 21}));
    nullable_col->append_nulls(2);
    ctx.selection.assign(nullable_col->size(), 1);
    rf.evaluate(nullable_col.get(), &ctx);
    expect_filter_eq(ctx.selection, {1, 0, 1, 0, 0, 0});

    rf.insert_null();
    ctx.selection.assign(const_null_col->size(), 1);
    rf.evaluate(const_null_col.get(), &ctx);
    expect_filter_eq(ctx.selection, {1, 1, 1, 1, 1, 1, 1, 1});

    ctx.selection.assign(nullable_col->size(), 1);
    rf.evaluate(nullable_col.get(), &ctx);
    expect_filter_eq(ctx.selection, {1, 0, 1, 0, 1, 1});
}

TEST(RuntimeFilterCoreTest, ComputePartitionIndexForSingletonLayout) {
    TRuntimeBloomFilter<TYPE_INT> rf;
    rf.set_join_mode(TRuntimeFilterBuildJoinMode::PARTITIONED);

    RuntimeFilterLayout layout;
    layout.init(1, {});

    auto col = make_int_column({1, 2, 3, 4, 5, 6, 7, 8});
    RuntimeFilter::RunningContext ctx;
    rf.compute_partition_index(layout, {col.get()}, &ctx);

    ASSERT_EQ(ctx.hash_values.size(), col->size());
    for (uint32_t hash_value : ctx.hash_values) {
        EXPECT_EQ(hash_value, 0U);
    }
}

TEST(RuntimeFilterCoreTest, ComputePartitionIndexForBucketAwareLayout) {
    std::vector<int32_t> bucketseq_to_instance = {2, 0, 1};

    TBucketProperty bucket_property;
    bucket_property.bucket_func = TBucketFunction::MURMUR3_X86_32;
    bucket_property.bucket_num = bucketseq_to_instance.size();

    TRuntimeFilterLayout t_layout;
    t_layout.__set_filter_id(1);
    t_layout.__set_local_layout(TRuntimeFilterLayoutMode::PIPELINE_BUCKET_LX);
    t_layout.__set_global_layout(TRuntimeFilterLayoutMode::GLOBAL_BUCKET_1L);
    t_layout.__set_pipeline_level_multi_partitioned(false);
    t_layout.__set_num_instances(1);
    t_layout.__set_num_drivers_per_instance(1);
    t_layout.__set_bucketseq_to_instance(bucketseq_to_instance);
    t_layout.__set_bucket_properties({bucket_property});

    RuntimeFilterLayout layout;
    layout.init(t_layout);

    auto col = make_int_column({3, 11, 19, 27, 35, 43, 51, 59});

    TRuntimeBloomFilter<TYPE_INT> rf;
    rf.set_global();
    rf.set_join_mode(TRuntimeFilterBuildJoinMode::COLOCATE);

    RuntimeFilter::RunningContext ctx;
    rf.compute_partition_index(layout, {col.get()}, &ctx);

    std::vector<uint32_t> expected_hash_values;
    std::vector<uint32_t> expected_round_hashes;
    std::vector<uint32_t> expected_bucket_ids;
    std::vector<uint32_t> expected_round_ids;
    BucketAwarePartitionCtx bctx(layout.bucket_properties(), expected_hash_values, expected_round_hashes,
                                 expected_bucket_ids, expected_round_ids);
    calc_hash_values_and_bucket_ids({col.get()}, bctx);

    ASSERT_EQ(ctx.bucket_ids, expected_bucket_ids);
    ASSERT_EQ(ctx.hash_values.size(), expected_bucket_ids.size());
    for (size_t i = 0; i < expected_bucket_ids.size(); ++i) {
        ASSERT_LT(expected_bucket_ids[i], bucketseq_to_instance.size());
        EXPECT_EQ(ctx.hash_values[i], static_cast<uint32_t>(bucketseq_to_instance[expected_bucket_ids[i]]));
    }
}

} // namespace
} // namespace starrocks
