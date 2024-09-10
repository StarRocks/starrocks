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

#include "storage/roaring2range.h"

#include <gtest/gtest.h>

namespace starrocks {

class Roaring2rangeTest : public testing::Test {};

TEST_F(Roaring2rangeTest, test_roaring2range) {
    Roaring roaring_1;
    auto ret_1 = roaring2range(roaring_1);
    ASSERT_EQ(ret_1.to_string(), "()");

    Roaring roaring_2;
    roaring_2.addRange(1, 10);
    auto ret_2 = roaring2range(roaring_2);
    ASSERT_EQ(ret_2.to_string(), "([1,10))");

    Roaring roaring_3;
    roaring_3.addRange(1, 10);
    roaring_3.addRange(15, 20);
    auto ret_3 = roaring2range(roaring_3);
    ASSERT_EQ(ret_3.to_string(), "([1,10), [15,20))");

    Roaring roaring_4;
    roaring_4.addRange(1, 300);
    roaring_4.addRange(400, 500);
    roaring_4.addRange(600, 1000);
    auto ret_4 = roaring2range(roaring_4);
    ASSERT_EQ(ret_4.to_string(), "([1,300), [400,500), [600,1000))");

    Roaring roaring_5;
    roaring_5.addRange(1, 257);
    auto ret_5 = roaring2range(roaring_5);
    ASSERT_EQ(ret_5.to_string(), "([1,257))");
}

TEST_F(Roaring2rangeTest, test_roaring2range_with_start) {
    Roaring roaring_1;
    auto ret_1 = roaring2range(roaring_1, 1, 10);
    ASSERT_EQ(ret_1.to_string(), "()");

    Roaring roaring_2;
    roaring_2.addRange(1, 10);
    auto ret_2_1 = roaring2range(roaring_2, 1, 20);
    ASSERT_EQ(ret_2_1.to_string(), "([1,10))");
    auto ret_2_2 = roaring2range(roaring_2, 1, 5);
    ASSERT_EQ(ret_2_2.to_string(), "([1,6))");
    auto ret_2_3 = roaring2range(roaring_2, 3, 5);
    ASSERT_EQ(ret_2_3.to_string(), "([3,8))");

    Roaring roaring_3;
    roaring_3.addRange(1, 10);
    roaring_3.addRange(15, 20);
    auto ret_3_1 = roaring2range(roaring_3, 1, 80);
    ASSERT_EQ(ret_3_1.to_string(), "([1,10), [15,20))");
    auto ret_3_2 = roaring2range(roaring_3, 1, 12);
    ASSERT_EQ(ret_3_2.to_string(), "([1,10), [15,18))");
    auto ret_3_3 = roaring2range(roaring_3, 16, 2);
    ASSERT_EQ(ret_3_3.to_string(), "([16,18))");

    Roaring roaring_4;
    roaring_4.addRange(1, 300);
    roaring_4.addRange(400, 500);
    roaring_4.addRange(600, 1000);
    auto ret_4_1 = roaring2range(roaring_4, 1, 2000);
    ASSERT_EQ(ret_4_1.to_string(), "([1,300), [400,500), [600,1000))");
    auto ret_4_2 = roaring2range(roaring_4, 450, 30);
    ASSERT_EQ(ret_4_2.to_string(), "([450,480))");
    auto ret_4_3 = roaring2range(roaring_4, 450, 200);
    ASSERT_EQ(ret_4_3.to_string(), "([450,500), [600,750))");
    auto ret_4_4 = roaring2range(roaring_4, 2000, 200);
    ASSERT_EQ(ret_4_4.to_string(), "()");

    Roaring roaring_5;
    roaring_5.addRange(1, 257);
    auto ret_5 = roaring2range(roaring_5, 1, 300);
    ASSERT_EQ(ret_5.to_string(), "([1,257))");
}

} // namespace starrocks