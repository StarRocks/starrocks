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

#include "formats/orc/utils.h"

#include <common/config.h>
#include <gtest/gtest.h>

namespace starrocks {

TEST(UtilsTest, TestMergeEmptyDiskRanges) {
    std::vector<DiskRange> disk_ranges{};
    std::vector<io::SharedBufferedInputStream::IORange> io_ranges{};
    DiskRangeHelper::mergeAdjacentDiskRanges(io_ranges, disk_ranges, config::io_coalesce_read_max_distance_size,
                                             config::io_coalesce_read_max_buffer_size);
    EXPECT_EQ(0, io_ranges.size());
}

TEST(UtilsTest, TestMergeTinyDiskRanges) {
    std::vector<DiskRange> disk_ranges{};
    constexpr int64_t KB = 1024;
    disk_ranges.emplace_back(0, 1 * KB);
    disk_ranges.emplace_back(10 * KB, 30 * KB);
    disk_ranges.emplace_back(800 * KB, 100 * KB);
    std::vector<io::SharedBufferedInputStream::IORange> io_ranges{};
    DiskRangeHelper::mergeAdjacentDiskRanges(io_ranges, disk_ranges, config::io_coalesce_read_max_distance_size,
                                             config::io_coalesce_read_max_buffer_size);
    EXPECT_EQ(1, io_ranges.size());
    EXPECT_EQ(0, io_ranges.at(0).offset);
    EXPECT_EQ(900 * KB, io_ranges.at(0).size);
}

TEST(UtilsTest, TestMergeBigDiskRanges) {
    std::vector<DiskRange> disk_ranges{};
    constexpr int64_t MB = 1024 * 1024;
    disk_ranges.emplace_back(0, 100 * MB);
    disk_ranges.emplace_back(200 * MB, 100 * MB);
    std::vector<io::SharedBufferedInputStream::IORange> io_ranges{};
    DiskRangeHelper::mergeAdjacentDiskRanges(io_ranges, disk_ranges, config::io_coalesce_read_max_distance_size,
                                             config::io_coalesce_read_max_buffer_size);
    EXPECT_EQ(2, io_ranges.size());
    EXPECT_EQ(0, io_ranges.at(0).offset);
    EXPECT_EQ(200 * MB, io_ranges.at(1).offset);
}

} // namespace starrocks