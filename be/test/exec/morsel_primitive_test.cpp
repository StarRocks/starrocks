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

#include "exec/pipeline/scan/dynamic_morsel_queue.h"
#include "exec/pipeline/scan/dynamic_morsel_queue_builder.h"
#include "exec/pipeline/scan/fixed_morsel_queue_builder.h"
#include "exec/pipeline/scan/scan_morsel.h"
#include "exec/pipeline/scan/ticketed_morsel_queue.h"

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
    ASSERT_NE(nullptr, dynamic_cast<TicketedMorselQueue*>(queue.get()));
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

} // namespace starrocks::pipeline
