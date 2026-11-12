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

#include "geo/geo_exact_arithmetic.h"

#include <gtest/gtest.h>

#include <cmath>
#include <limits>
#include <memory>

#define BOOST_MATH_DISABLE_FLOAT128
#define BOOST_CSTDFLOAT_NO_LIBQUADMATH_SUPPORT
#include <boost/multiprecision/cpp_int.hpp>

namespace starrocks {
namespace {
using FixedInteger = boost::multiprecision::number<
        boost::multiprecision::cpp_int_backend<4224, 4224, boost::multiprecision::signed_magnitude,
                                               boost::multiprecision::unchecked, void>,
        boost::multiprecision::et_off>;
using CheckedInteger = boost::multiprecision::number<
        boost::multiprecision::cpp_int_backend<128, 16384, boost::multiprecision::signed_magnitude,
                                               boost::multiprecision::checked,
                                               std::allocator<boost::multiprecision::limb_type>>,
        boost::multiprecision::et_off>;

template <class Integer>
void check_scaling() {
    EXPECT_EQ(Integer(0), geo_exact::scaled_binary64<Integer>(0.0));
    EXPECT_EQ(Integer(0), geo_exact::scaled_binary64<Integer>(-0.0));
    EXPECT_EQ(Integer(1), geo_exact::scaled_binary64<Integer>(std::numeric_limits<double>::denorm_min()));
    EXPECT_EQ(Integer(-1), geo_exact::scaled_binary64<Integer>(-std::numeric_limits<double>::denorm_min()));
    EXPECT_EQ(Integer(1) << 52, geo_exact::scaled_binary64<Integer>(std::numeric_limits<double>::min()));
    EXPECT_EQ((Integer(1) << 52) - 1,
              geo_exact::scaled_binary64<Integer>(std::nextafter(std::numeric_limits<double>::min(), 0.0)));
    EXPECT_EQ(Integer(1) << 1074, geo_exact::scaled_binary64<Integer>(1.0));
    const Integer maximum = ((Integer(1) << 53) - 1) << 2045;
    EXPECT_EQ(maximum, geo_exact::scaled_binary64<Integer>(std::numeric_limits<double>::max()));
    EXPECT_EQ(-maximum, geo_exact::scaled_binary64<Integer>(-std::numeric_limits<double>::max()));
}

template <class Integer>
void check_orientation() {
    const Integer zero = 0, one = geo_exact::scaled_binary64<Integer>(1.0);
    const Integer next = geo_exact::scaled_binary64<Integer>(std::nextafter(1.0, 2.0));
    EXPECT_EQ(1, geo_exact::sign(geo_exact::orientation(zero, zero, one, zero, zero, one)));
    EXPECT_EQ(-1, geo_exact::sign(geo_exact::orientation(zero, zero, one, one, next, one)));
    EXPECT_EQ(1, geo_exact::sign(geo_exact::orientation(zero, zero, next, one, one, one)));
    EXPECT_EQ(0, geo_exact::sign(geo_exact::orientation(zero, zero, one, one, one, one)));
    const Integer maximum = geo_exact::scaled_binary64<Integer>(std::numeric_limits<double>::max());
    const Integer negative = -maximum;
    EXPECT_EQ(1, geo_exact::sign(geo_exact::orientation(negative, negative, maximum, negative, zero, maximum)));
    EXPECT_EQ(0, geo_exact::sign(zero));
    EXPECT_EQ(1, geo_exact::sign(Integer(1)));
    EXPECT_EQ(-1, geo_exact::sign(Integer(-1)));
}
} // namespace

TEST(GeoExactArithmeticTest, FiniteBinary64Scaling) {
    check_scaling<FixedInteger>();
    check_scaling<CheckedInteger>();
}

TEST(GeoExactArithmeticTest, ExactOrientationAndSign) {
    check_orientation<FixedInteger>();
    check_orientation<CheckedInteger>();
}
} // namespace starrocks
