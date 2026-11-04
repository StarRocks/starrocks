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

#include <memory>

#include "exec_primitive/pipeline/scan/dynamic_morsel_queue.h"
#include "exec_primitive/pipeline/scan/dynamic_morsel_queue_builder.h"
#include "exec_primitive/pipeline/scan/fixed_morsel_queue_builder.h"
#include "exec_primitive/pipeline/scan/scan_morsel.h"
#include "exec_primitive/pipeline/scan/ticketed_morsel_queue.h"

namespace starrocks::pipeline {

namespace {

Morsels make_morsels(int num_morsels) {
    Morsels morsels;
    morsels.reserve(num_morsels);
    for (int i = 0; i < num_morsels; ++i) {
        morsels.emplace_back(std::make_unique<ScanMorsel>(1, TScanRange()));
    }
    return morsels;
}

} // namespace

TEST(MorselPrimitiveTest, FixedBuilderBuildsFixedQueue) {
    auto builder = make_fixed_morsel_queue_builder(make_morsels(3));
    ASSERT_TRUE(builder->can_uniform_distribute());
    ASSERT_EQ(3, builder->num_original_morsels());
    ASSERT_EQ(3, builder->max_degree_of_parallelism());

    builder->set_has_more_scan_ranges(true);
    builder->set_has_more_from_split(true);

    auto queue_or = builder->build();
    ASSERT_TRUE(queue_or.ok()) << queue_or.status().to_string();
    auto queue = std::move(queue_or).value();
    ASSERT_EQ(MorselQueue::Type::FIXED, queue->type());
    ASSERT_EQ(3, queue->num_original_morsels());
    ASSERT_TRUE(queue->has_more_scan_ranges());
    ASSERT_TRUE(queue->has_more_from_split());
}

TEST(MorselPrimitiveTest, DynamicBuilderBuildsTicketedQueue) {
    auto builder = make_dynamic_morsel_queue_builder(make_morsels(2), true, 8);
    ASSERT_TRUE(builder->can_uniform_distribute());
    ASSERT_EQ(2, builder->num_original_morsels());
    ASSERT_EQ(8, builder->max_degree_of_parallelism());

    auto queue_or = builder->build();
    ASSERT_TRUE(queue_or.ok()) << queue_or.status().to_string();
    auto queue = std::move(queue_or).value();
    ASSERT_EQ(MorselQueue::Type::DYNAMIC, queue->type());
    ASSERT_EQ(8, queue->max_degree_of_parallelism());
    ASSERT_TRUE(queue->has_more_scan_ranges());
    auto* ticketed_queue = dynamic_cast<TicketedMorselQueue*>(queue.get());
    ASSERT_NE(nullptr, ticketed_queue);
    EXPECT_FALSE(ticketed_queue->should_attach_ticket_checker(false));
    EXPECT_TRUE(ticketed_queue->should_attach_ticket_checker(true));
}

TEST(MorselPrimitiveTest, ScanMorselHandlesEmptyScanRangeMarker) {
    TScanRangeParams marker;
    marker.__set_empty(true);
    marker.__set_has_more(true);

    Morsels morsels;
    bool has_more = false;
    ScanMorsel::build_scan_morsels(1, {marker}, true, &morsels, &has_more);
    ASSERT_TRUE(morsels.empty());
    ASSERT_TRUE(has_more);

    ScanMorsel::build_scan_morsels(1, {marker}, false, &morsels, &has_more);
    ASSERT_EQ(1, morsels.size());
    ASSERT_TRUE(has_more);
}

TEST(MorselPrimitiveTest, GroupedEnvelopeSurvivesCopiesAndKeepsSameTabletTasksDistinct) {
    TInternalScanRange internal;
    internal.__set_tablet_id(77);
    internal.__set_version("42");
    internal.__set_bucket_sequence(0);
    TGroupedRangeTask task;
    task.__set_group_id(0);
    TTuple cut;
    for (auto literal : {"5", "0", "0"}) {
        TScalarType scalar;
        scalar.__set_type(TPrimitiveType::BIGINT);
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        node.__set_scalar_type(scalar);
        TTypeDesc type;
        type.__set_types({node});
        type.__isset.types = true;
        TVariant value;
        value.__set_type(type);
        value.__set_variant_type(TVariantType::NORMAL_VALUE);
        value.__set_value(literal);
        cut.values.push_back(value);
    }
    TTabletRange clip;
    clip.__set_upper_bound(cut);
    clip.__set_upper_bound_included(false);
    task.__set_key_range(clip);
    task.__set_sort_key_unique_ids({100, 101, 102});
    TScanRangeParams first;
    first.scan_range.__set_internal_scan_range(internal);
    first.scan_range.__set_grouped_range_task(task);
    auto second = first;
    second.scan_range.grouped_range_task.__set_group_id(1);
    second.scan_range.grouped_range_task.key_range.__isset.upper_bound = false;
    second.scan_range.grouped_range_task.key_range.__set_lower_bound(cut);
    second.scan_range.grouped_range_task.key_range.__set_lower_bound_included(true);
    second.scan_range.internal_scan_range.__set_bucket_sequence(1);
    Morsels morsels;
    bool has_more = false;
    ScanMorsel::build_scan_morsels(9, {first, second}, true, &morsels, &has_more);
    ASSERT_EQ(2, morsels.size());
    first.scan_range.grouped_range_task.__set_group_id(99);
    for (int i = 0; i < 2; ++i) {
        auto* range = morsels[i]->get_scan_range();
        EXPECT_TRUE(range->__isset.grouped_range_task);
        EXPECT_EQ(i, range->grouped_range_task.group_id);
        EXPECT_EQ(i == 0, range->grouped_range_task.key_range.__isset.upper_bound);
        EXPECT_EQ(i == 1, range->grouped_range_task.key_range.__isset.lower_bound);
        const auto& bound = i == 0 ? range->grouped_range_task.key_range.upper_bound
                                   : range->grouped_range_task.key_range.lower_bound;
        EXPECT_EQ(cut, bound);
        EXPECT_EQ((std::vector<int32_t>{100, 101, 102}), range->grouped_range_task.sort_key_unique_ids);
        EXPECT_EQ(77, range->internal_scan_range.tablet_id);
        EXPECT_EQ("42", range->internal_scan_range.version);
        EXPECT_EQ(i, morsels[i]->owner_id());
    }
    EXPECT_FALSE(has_more);
}

TEST(MorselPrimitiveTest, GroupedPaddingHasNoSyntheticTabletAndTerminates) {
    TScanRangeParams marker;
    marker.__set_empty(true);
    Morsels morsels;
    bool has_more = true;
    ScanMorsel::build_scan_morsels(9, {marker}, true, &morsels, &has_more);
    EXPECT_TRUE(morsels.empty());
    EXPECT_FALSE(has_more);
    auto queue = make_fixed_morsel_queue_builder(std::move(morsels))->build();
    ASSERT_TRUE(queue.ok());
    EXPECT_TRUE(queue.value()->empty());
    EXPECT_FALSE(queue.value()->has_more());
    auto next = queue.value()->try_get();
    ASSERT_TRUE(next.ok());
    EXPECT_EQ(nullptr, next.value());
}

} // namespace starrocks::pipeline
