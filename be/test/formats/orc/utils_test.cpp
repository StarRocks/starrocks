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

#include <gtest/gtest.h>

namespace orc {

TEST(Int128CompareTest, Simple) {
    Int128 x = 123;
    EXPECT_EQ(Int128(123), x);
    EXPECT_EQ(true, x == 123);
    EXPECT_EQ(true, !(x == 124));
    EXPECT_EQ(true, !(x == -124));
    EXPECT_EQ(true, !(x == Int128(2, 123)));
    EXPECT_EQ(true, !(x != 123));
    EXPECT_EQ(true, x != -123);
    EXPECT_EQ(true, x != 124);
    EXPECT_EQ(true, x != Int128(-1, 123));
    x = Int128(0x123, 0x456);
    EXPECT_EQ(true, !(x < Int128(0x123, 0x455)));
    EXPECT_EQ(true, !(x < Int128(0x123, 0x456)));
    EXPECT_EQ(true, x < Int128(0x123, 0x457));
    EXPECT_EQ(true, !(x < Int128(0x122, 0x456)));
    EXPECT_EQ(true, x < Int128(0x124, 0x456));

    EXPECT_EQ(true, !(x <= Int128(0x123, 0x455)));
    EXPECT_EQ(true, x <= Int128(0x123, 0x456));
    EXPECT_EQ(true, x <= Int128(0x123, 0x457));
    EXPECT_EQ(true, !(x <= Int128(0x122, 0x456)));
    EXPECT_EQ(true, x <= Int128(0x124, 0x456));

    EXPECT_EQ(true, x > Int128(0x123, 0x455));
    EXPECT_EQ(true, !(x > Int128(0x123, 0x456)));
    EXPECT_EQ(true, !(x > Int128(0x123, 0x457)));
    EXPECT_EQ(true, x > Int128(0x122, 0x456));
    EXPECT_EQ(true, !(x > Int128(0x124, 0x456)));

    EXPECT_EQ(true, x >= Int128(0x123, 0x455));
    EXPECT_EQ(true, x >= Int128(0x123, 0x456));
    EXPECT_EQ(true, !(x >= Int128(0x123, 0x457)));
    EXPECT_EQ(true, x >= Int128(0x122, 0x456));
    EXPECT_EQ(true, !(x >= Int128(0x124, 0x456)));

    EXPECT_EQ(true, Int128(-3) < Int128(-2));
    EXPECT_EQ(true, Int128(-3) < Int128(0));
    EXPECT_EQ(true, Int128(-3) < Int128(3));
    EXPECT_EQ(true, Int128(0) < Int128(5));
    EXPECT_EQ(true, Int128::minimumValue() < 0);
    EXPECT_EQ(true, Int128(0) < Int128::maximumValue());
    EXPECT_EQ(true, Int128::minimumValue() < Int128::maximumValue());
}

TEST(Decimal, testDecimalComparison) {
    // same scales
    EXPECT_TRUE(compare(Decimal(Int128(99), 0), Decimal(Int128(100), 0)));
    EXPECT_TRUE(compare(Decimal(Int128(34543), 5), Decimal(Int128(4324324), 5)));
    EXPECT_TRUE(compare(Decimal(Int128(345345435432l), 15), Decimal(Int128(345344425435432l), 15)));
    EXPECT_TRUE(compare(Decimal(Int128(5), 20), Decimal(Int128(50), 20)));

    // different scales
    EXPECT_TRUE(compare(Decimal(Int128(10000), 4), Decimal(Int128(10000), 3)));
    EXPECT_FALSE(compare(Decimal(Int128(1111), 3), Decimal(Int128(111), 2)));
    EXPECT_TRUE(compare(Decimal(Int128(999999), 5), Decimal(Int128(9999999), 5)));
    EXPECT_FALSE(compare(Decimal(Int128(99), 0), Decimal(Int128(100), 1)));

    // same integral parts
    EXPECT_TRUE(compare(Decimal(Int128(99999), 0), Decimal(Int128(999999), 1)));
    EXPECT_TRUE(compare(Decimal(Int128(12345123), 3), Decimal(Int128(1234553432), 5)));

    // equal numbers
    EXPECT_FALSE(compare(Decimal(Int128(100000), 3), Decimal(Int128(100), 0)));
    EXPECT_FALSE(compare(Decimal(Int128(100), 0), Decimal(Int128(100000), 3)));
    EXPECT_FALSE(compare(Decimal(Int128(100000), 3), Decimal(Int128(100000), 3)));
    EXPECT_FALSE(compare(Decimal(Int128(100000), 3), Decimal(Int128(100000), 3)));
    EXPECT_FALSE(compare(Decimal(Int128(1), 10), Decimal(Int128(10), 11)));
    EXPECT_FALSE(compare(Decimal(Int128(10), 11), Decimal(Int128(1), 10)));

    // large scales (>18)
    EXPECT_TRUE(compare(Decimal(Int128(99), 35), Decimal(Int128(100), 35)));
    EXPECT_TRUE(compare(Decimal(Int128("12345678999999999999999999999999999998"), 29),
                        Decimal(Int128("123456789999999999999999999999999999999"), 30)));
    EXPECT_FALSE(compare(Decimal(Int128("123456789999999999999999999999999999900"), 30),
                         Decimal(Int128("12345678999999999999999999999999999990"), 29)));
    EXPECT_FALSE(compare(Decimal(Int128("12345678999999999999999999999999999990"), 29),
                         Decimal(Int128("123456789999999999999999999999999999900"), 30)));

    // fractional overflow
    EXPECT_TRUE(compare(Decimal(Int128::maximumValue(), 39),
                        Decimal(Int128("99999999999999999999999999999999999999"), 38)));

    // negative numbers
    EXPECT_TRUE(compare(Decimal(Int128(-99), 0), Decimal(Int128(100), 0)));
    EXPECT_TRUE(compare(Decimal(Int128(-4324324), 5), Decimal(Int128(-34543), 5)));
    EXPECT_TRUE(compare(Decimal(Int128(-345344425435432l), 15), Decimal(Int128(-345345435432l), 15)));
    EXPECT_TRUE(compare(Decimal(Int128(-50), 20), Decimal(Int128(-5), 20)));
    EXPECT_TRUE(compare(Decimal(Int128(-10000), 3), Decimal(Int128(-10000), 4)));
    EXPECT_TRUE(compare(Decimal(Int128(-1111), 3), Decimal(Int128(-111), 2)));
    EXPECT_TRUE(compare(Decimal(Int128(-9999999), 5), Decimal(Int128(-999999), 5)));
    EXPECT_TRUE(compare(Decimal(Int128(-99), 0), Decimal(Int128(-100), 1)));
    EXPECT_TRUE(compare(Decimal(Int128(-999999), 1), Decimal(Int128(-99999), 0)));
    EXPECT_TRUE(compare(Decimal(Int128(-1234553432), 5), Decimal(Int128(-12345123), 3)));
    EXPECT_FALSE(compare(Decimal(Int128(-100000), 3), Decimal(Int128(-100), 0)));
    EXPECT_FALSE(compare(Decimal(Int128(-100), 0), Decimal(Int128(-100000), 3)));
    EXPECT_FALSE(compare(Decimal(Int128(-100000), 3), Decimal(Int128(-100000), 3)));
    EXPECT_FALSE(compare(Decimal(Int128(-100000), 3), Decimal(Int128(-100000), 3)));
    EXPECT_FALSE(compare(Decimal(Int128(-1), 10), Decimal(Int128(-10), 11)));
    EXPECT_FALSE(compare(Decimal(Int128(-10), 11), Decimal(Int128(-1), 10)));
    EXPECT_TRUE(compare(Decimal(Int128(-100), 35), Decimal(Int128(-99), 35)));
    EXPECT_TRUE(compare(Decimal(Int128("-123456789999999999999999999999999999999"), 30),
                        Decimal(Int128("-12345678999999999999999999999999999998"), 29)));
    EXPECT_FALSE(compare(Decimal(Int128("-123456789999999999999999999999999999900"), 30),
                         Decimal(Int128("-12345678999999999999999999999999999990"), 29)));
    EXPECT_FALSE(compare(Decimal(Int128("-12345678999999999999999999999999999990"), 29),
                         Decimal(Int128("-123456789999999999999999999999999999900"), 30)));
    EXPECT_TRUE(compare(Decimal(Int128("-99999999999999999999999999999999999999"), 38),
                        Decimal(Int128::minimumValue(), 39)));

    // more tests
    EXPECT_TRUE(compare(Decimal(Int128("-99999999999999999999999999999999999999"), 38), Decimal(Int128(100), 0)));
    EXPECT_FALSE(compare(Decimal(Int128(0), 38), Decimal(Int128(Int128::minimumValue()), 37)));
    EXPECT_FALSE(compare(Decimal(Int128(Int128::maximumValue()), 38), Decimal(Int128("-1"), 37)));
}

} // namespace orc
