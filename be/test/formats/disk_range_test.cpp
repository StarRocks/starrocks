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

#include "formats/disk_range.hpp"

#include <common/config.h>
#include <gtest/gtest.h>

namespace starrocks {

TEST(DiskRangeTest, TestMergeEmptyDiskRanges) {
    std::vector<DiskRange> disk_ranges{};
    std::vector<DiskRange> merged_io_ranges{};
    DiskRangeHelper::merge_adjacent_disk_ranges(disk_ranges, config::io_coalesce_read_max_distance_size,
                                                config::io_coalesce_read_max_buffer_size, merged_io_ranges);
    EXPECT_EQ(0, merged_io_ranges.size());
}

TEST(DiskRangeTest, TestMergeTinyDiskRanges) {
    std::vector<DiskRange> disk_ranges{};
    constexpr int64_t KB = 1024;
    disk_ranges.emplace_back(0, 1 * KB);
    disk_ranges.emplace_back(10 * KB, 30 * KB);
    disk_ranges.emplace_back(800 * KB, 100 * KB);
    std::vector<DiskRange> merged_disk_ranges{};
    DiskRangeHelper::merge_adjacent_disk_ranges(disk_ranges, config::io_coalesce_read_max_distance_size,
                                                config::io_coalesce_read_max_buffer_size, merged_disk_ranges);
    EXPECT_EQ(1, merged_disk_ranges.size());
    EXPECT_EQ(0, merged_disk_ranges.at(0).offset());
    EXPECT_EQ(900 * KB, merged_disk_ranges.at(0).length());
}

// unordered DiskRange + DiskRange's length = 0 + duplicate DiskRange + Overlap DiskRange
TEST(DiskRangeTest, TestDiskRangesRobust) {
    std::vector<DiskRange> disk_ranges{};
    constexpr int64_t KB = 1024;
    // unordered
    disk_ranges.emplace_back(800 * KB, 100 * KB);
    // duplicate
    disk_ranges.emplace_back(0, 1 * KB);
    disk_ranges.emplace_back(0, 1 * KB);
    disk_ranges.emplace_back(0, 1 * KB);
    // overlap
    disk_ranges.emplace_back(10 * KB, 30 * KB);
    disk_ranges.emplace_back(15 * KB, 45 * KB);
    disk_ranges.emplace_back(850 * KB, 150 * KB);
    // length = 0 + duplicate
    disk_ranges.emplace_back(40 * KB, 0 * KB);
    disk_ranges.emplace_back(900 * KB, 0 * KB);
    disk_ranges.emplace_back(900 * KB, 0 * KB);
    disk_ranges.emplace_back(900 * KB, 0 * KB);
    std::vector<DiskRange> merged_disk_ranges{};
    DiskRangeHelper::merge_adjacent_disk_ranges(disk_ranges, config::io_coalesce_read_max_distance_size,
                                                config::io_coalesce_read_max_buffer_size, merged_disk_ranges);
    EXPECT_EQ(1, merged_disk_ranges.size());
    EXPECT_EQ(0, merged_disk_ranges.at(0).offset());
    EXPECT_EQ(1000 * KB, merged_disk_ranges.at(0).length());
}

TEST(DiskRangeTest, TestMergeBigDiskRanges) {
    std::vector<DiskRange> disk_ranges{};
    constexpr int64_t MB = 1024 * 1024;
    disk_ranges.emplace_back(0, 100 * MB);
    disk_ranges.emplace_back(200 * MB, 100 * MB);
    std::vector<DiskRange> merged_disk_ranges{};
    DiskRangeHelper::merge_adjacent_disk_ranges(disk_ranges, config::io_coalesce_read_max_distance_size,
                                                config::io_coalesce_read_max_buffer_size, merged_disk_ranges);
    EXPECT_EQ(2, merged_disk_ranges.size());
    EXPECT_EQ(0, merged_disk_ranges.at(0).offset());
    EXPECT_EQ(200 * MB, merged_disk_ranges.at(1).offset());
}

} // namespace starrocks