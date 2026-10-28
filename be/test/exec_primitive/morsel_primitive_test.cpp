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

// Passing the ranges by std::move must build the same morsels as passing a copy, skip empty
// markers and carry has_more.
TEST(MorselPrimitiveTest, BuildScanMorselsMovesScanRanges) {
    auto make_ranges = [] {
        std::vector<TScanRangeParams> ranges(3);
        ranges[0].scan_range.__set_hdfs_scan_range(THdfsScanRange());
        ranges[0].scan_range.hdfs_scan_range.__set_full_path("a.parquet");
        ranges[1].__set_empty(true);
        ranges[1].__set_has_more(true);
        TInternalScanRange internal;
        internal.__set_tablet_id(42);
        internal.__set_version("7");
        ranges[2].scan_range.__set_internal_scan_range(internal);
        return ranges;
    };

    const std::vector<TScanRangeParams> copied_ranges = make_ranges();
    Morsels copied;
    bool copied_has_more = false;
    ScanMorsel::build_scan_morsels(1, copied_ranges, false, &copied, &copied_has_more);

    std::vector<TScanRangeParams> moved_ranges = make_ranges();
    Morsels moved;
    bool moved_has_more = false;
    ScanMorsel::build_scan_morsels(1, std::move(moved_ranges), false, &moved, &moved_has_more);

    ASSERT_EQ(2, moved.size());
    ASSERT_EQ(copied.size(), moved.size());
    ASSERT_TRUE(moved_has_more);
    ASSERT_EQ(copied_has_more, moved_has_more);
    for (size_t i = 0; i < moved.size(); i++) {
        EXPECT_EQ(*copied[i]->get_scan_range(), *moved[i]->get_scan_range());
        EXPECT_EQ(copied[i]->has_owner_id(), moved[i]->has_owner_id());
        EXPECT_EQ(copied[i]->owner_id(), moved[i]->owner_id());
        EXPECT_EQ(copied[i]->get_lane_owner_and_version(), moved[i]->get_lane_owner_and_version());
    }
    EXPECT_EQ("a.parquet", moved[0]->get_scan_range()->hdfs_scan_range.full_path);
    EXPECT_TRUE(moved[1]->has_owner_id());
    EXPECT_EQ(42, moved[1]->owner_id());
    // The const& overload leaves its input alone; the && one takes each scan range.
    EXPECT_EQ("a.parquet", copied_ranges[0].scan_range.hdfs_scan_range.full_path);
    EXPECT_FALSE(moved_ranges[0].scan_range.__isset.hdfs_scan_range);
}

// Thrift structs have no move constructor, so std::move alone would deep-copy: the morsel must
// take the scan range's members.
TEST(MorselPrimitiveTest, ScanMorselTakesScanRange) {
    TScanRange scan_range;
    scan_range.__set_hdfs_scan_range(THdfsScanRange());
    scan_range.hdfs_scan_range.__set_full_path("a.parquet");
    ScanMorsel morsel(1, std::move(scan_range));
    EXPECT_EQ("a.parquet", morsel.get_scan_range()->hdfs_scan_range.full_path);
    EXPECT_FALSE(scan_range.__isset.hdfs_scan_range);
    EXPECT_TRUE(scan_range.hdfs_scan_range.full_path.empty());
}

} // namespace starrocks::pipeline
