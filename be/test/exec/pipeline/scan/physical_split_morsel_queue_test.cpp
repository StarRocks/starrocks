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

#include "exec/pipeline/scan/morsel.h"
#include "gen_cpp/InternalService_types.h"
#include "storage/lake/tablet.h"

namespace starrocks::pipeline {

class PhysicalSplitMorselQueueTest : public ::testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

// Test case: PhysicalSplitMorselQueue crashes when tablet has no rowsets
// This test directly verifies the boundary checks in PhysicalSplitMorselQueue methods:
// _cur_rowset, _cur_segment, _is_last_split_of_current_morsel, _next_segment, _init_segment
TEST_F(PhysicalSplitMorselQueueTest, test_empty_rowset) {
    // Create scan range for the morsel
    TScanRange scan_range;
    TInternalScanRange internal_scan_range;
    internal_scan_range.tablet_id = 10001;
    internal_scan_range.version = "1";
    internal_scan_range.partition_id = 1;
    scan_range.__set_internal_scan_range(internal_scan_range);

    // Create morsels
    Morsels morsels;
    morsels.emplace_back(std::make_unique<ScanMorsel>(1, scan_range));

    // Create PhysicalSplitMorselQueue
    PhysicalSplitMorselQueue queue(std::move(morsels), 1, 1024);

    // Create a lake tablet for testing (nullptr TabletManager is allowed)
    auto tablet = std::make_shared<lake::Tablet>(nullptr, 10001);
    std::vector<BaseTabletSharedPtr> tablets;
    tablets.push_back(tablet);
    queue.set_tablets(tablets);

    // Set up tablet_rowsets with empty rowsets (key scenario for issue #70280)
    std::vector<std::vector<BaseRowsetSharedPtr>> tablet_rowsets;
    tablet_rowsets.emplace_back(); // Empty rowsets for the tablet
    queue.set_tablet_rowsets(tablet_rowsets);

    // Set a non-null tablet schema to avoid null dereference in _init_segment
    auto tablet_schema = std::make_shared<TabletSchema>();
    queue.set_tablet_schema(tablet_schema);

    // Call try_get, this should not crash with empty rowsets
    auto result = queue.try_get();
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(result.value(), nullptr);
    ASSERT_TRUE(queue.empty());
}

} // namespace starrocks::pipeline
