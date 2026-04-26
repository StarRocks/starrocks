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

#include "column/column_filter_range.h"

#include <gtest/gtest.h>

#include <vector>

#include "base/testutil/parallel_test.h"

namespace starrocks {

PARALLEL_TEST(ColumnFilterRangeTest, in_place_filter_int32) {
    std::vector<int32_t> values = {10, 11, 12, 13, 14, 15};
    Filter filter = {1, 0, 1, 0, 1, 0};

    size_t size = column_filter_range::filter_range<int32_t>(filter, values.data(), 0, values.size());
    ASSERT_EQ(3, size);
    EXPECT_EQ(10, values[0]);
    EXPECT_EQ(12, values[1]);
    EXPECT_EQ(14, values[2]);
}

PARALLEL_TEST(ColumnFilterRangeTest, filter_int32_to_separate_buffer) {
    const std::vector<int32_t> src = {1, 2, 3, 4, 5, 6};
    std::vector<int32_t> dst(src.size(), -1);
    Filter filter = {0, 1, 1, 0, 1, 0};

    size_t size = column_filter_range::filter_range<int32_t>(filter, dst.data(), src.data(), 0, src.size());
    ASSERT_EQ(3, size);
    EXPECT_EQ(2, dst[0]);
    EXPECT_EQ(3, dst[1]);
    EXPECT_EQ(5, dst[2]);
}

PARALLEL_TEST(ColumnFilterRangeTest, range_filter_double) {
    std::vector<double> values = {0.1, 0.2, 0.3, 0.4, 0.5, 0.6};
    Filter filter = {1, 0, 1, 1, 0, 1};

    size_t size = column_filter_range::filter_range<double>(filter, values.data(), 1, 5);
    ASSERT_EQ(3, size);
    EXPECT_DOUBLE_EQ(0.1, values[0]);
    EXPECT_DOUBLE_EQ(0.3, values[1]);
    EXPECT_DOUBLE_EQ(0.4, values[2]);
}

PARALLEL_TEST(ColumnFilterRangeTest, zero_and_one_masks) {
    std::vector<int32_t> values_zero = {7, 8, 9, 10};
    Filter zero_filter(values_zero.size(), 0);
    size_t zero_size =
            column_filter_range::filter_range<int32_t>(zero_filter, values_zero.data(), 0, values_zero.size());
    EXPECT_EQ(0, zero_size);

    std::vector<int32_t> values_one = {100, 200, 300, 400, 500};
    Filter one_filter(values_one.size(), 1);
    size_t one_size = column_filter_range::filter_range<int32_t>(one_filter, values_one.data(), 2, values_one.size());
    EXPECT_EQ(values_one.size(), one_size);
    EXPECT_EQ(100, values_one[0]);
    EXPECT_EQ(200, values_one[1]);
    EXPECT_EQ(300, values_one[2]);
    EXPECT_EQ(400, values_one[3]);
    EXPECT_EQ(500, values_one[4]);
}

} // namespace starrocks
