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

#include "storage/lake/sst_seek_range.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace starrocks::lake {

TEST(SstSeekRangeTest, test_has_overlap) {
    {
        // [10, 30) and [20, 40] -> overlap
        SstSeekRange range{"10", "30"};
        PersistentIndexSstableRangePB sst_range;
        sst_range.set_start_key("20");
        sst_range.set_end_key("40");
        ASSERT_TRUE(range.has_overlap(sst_range));
    }
    {
        // [10, 30) and [05, 20] -> overlap
        SstSeekRange range{"10", "30"};
        PersistentIndexSstableRangePB sst_range;
        sst_range.set_start_key("05");
        sst_range.set_end_key("20");
        ASSERT_TRUE(range.has_overlap(sst_range));
    }
    {
        // [10, 30) and [30, 40] -> no overlap (stop_key is exclusive)
        SstSeekRange range{"10", "30"};
        PersistentIndexSstableRangePB sst_range;
        sst_range.set_start_key("30");
        sst_range.set_end_key("40");
        ASSERT_FALSE(range.has_overlap(sst_range));
    }
    {
        // [10, 30) and [00, 05] -> no overlap
        SstSeekRange range{"10", "30"};
        PersistentIndexSstableRangePB sst_range;
        sst_range.set_start_key("00");
        sst_range.set_end_key("05");
        ASSERT_FALSE(range.has_overlap(sst_range));
    }
    {
        // [10, "") and [20, 40] -> overlap
        SstSeekRange range{"10", ""};
        PersistentIndexSstableRangePB sst_range;
        sst_range.set_start_key("20");
        sst_range.set_end_key("40");
        ASSERT_TRUE(range.has_overlap(sst_range));
    }
    {
        // ["", "") and [20, 40] -> overlap
        SstSeekRange range{"", ""};
        PersistentIndexSstableRangePB sst_range;
        sst_range.set_start_key("20");
        sst_range.set_end_key("40");
        ASSERT_TRUE(range.has_overlap(sst_range));
    }
}

TEST(SstSeekRangeTest, test_full_contains) {
    {
        // [10, 40) contains [20, 30] -> true
        SstSeekRange range{"10", "40"};
        PersistentIndexSstableRangePB sst_range;
        sst_range.set_start_key("20");
        sst_range.set_end_key("30");
        ASSERT_TRUE(range.full_contains(sst_range));
    }
    {
        // [10, 40) contains [10, 30] -> true
        SstSeekRange range{"10", "40"};
        PersistentIndexSstableRangePB sst_range;
        sst_range.set_start_key("10");
        sst_range.set_end_key("30");
        ASSERT_TRUE(range.full_contains(sst_range));
    }
    {
        // [10, 40) contains [20, 40] -> false (stop_key is exclusive)
        SstSeekRange range{"10", "40"};
        PersistentIndexSstableRangePB sst_range;
        sst_range.set_start_key("20");
        sst_range.set_end_key("40");
        ASSERT_FALSE(range.full_contains(sst_range));
    }
    {
        // [10, 40) contains [05, 30] -> false
        SstSeekRange range{"10", "40"};
        PersistentIndexSstableRangePB sst_range;
        sst_range.set_start_key("05");
        sst_range.set_end_key("30");
        ASSERT_FALSE(range.full_contains(sst_range));
    }
    {
        // [10, "") contains [20, 99] -> true
        SstSeekRange range{"10", ""};
        PersistentIndexSstableRangePB sst_range;
        sst_range.set_start_key("20");
        sst_range.set_end_key("99");
        ASSERT_TRUE(range.full_contains(sst_range));
    }
    {
        // ["", "") contains [00, 99] -> true
        SstSeekRange range{"", ""};
        PersistentIndexSstableRangePB sst_range;
        sst_range.set_start_key("00");
        sst_range.set_end_key("99");
        ASSERT_TRUE(range.full_contains(sst_range));
    }
}

TEST(SstSeekRangeTest, test_intersect) {
    {
        // [10, 40) & [20, 30) -> [20, 30)
        SstSeekRange r1{"10", "40"};
        SstSeekRange r2{"20", "30"};
        r1 &= r2;
        ASSERT_EQ("20", r1.seek_key);
        ASSERT_EQ("30", r1.stop_key);
    }
    {
        // [10, 40) & [05, 50) -> [10, 40)
        SstSeekRange r1{"10", "40"};
        SstSeekRange r2{"05", "50"};
        r1 &= r2;
        ASSERT_EQ("10", r1.seek_key);
        ASSERT_EQ("40", r1.stop_key);
    }
    {
        // [10, "") & ["", 30) -> [10, 30)
        SstSeekRange r1{"10", ""};
        SstSeekRange r2{"", "30"};
        r1 &= r2;
        ASSERT_EQ("10", r1.seek_key);
        ASSERT_EQ("30", r1.stop_key);
    }
}

} // namespace starrocks::lake
