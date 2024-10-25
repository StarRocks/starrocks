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

#include "util/numeric_types.h"

#include <gtest/gtest.h>

#include "column/datum.h"

namespace starrocks {

TEST(TestNumericTypes, test_check_signed_number_overflow_floating_to_integral) {
    EXPECT_FALSE((check_signed_number_overflow<double, int64_t>(9223372036854775000.0)));
    EXPECT_TRUE((check_signed_number_overflow<double, int64_t>(9223372036854775807.0)));
    EXPECT_TRUE((check_signed_number_overflow<double, int64_t>(9223372036854775807.3)));
    EXPECT_TRUE((check_signed_number_overflow<double, int64_t>(9223372036854775808.0)));
    EXPECT_TRUE((check_signed_number_overflow<double, int64_t>(9223372036854775809.0)));

    EXPECT_FALSE((check_signed_number_overflow<double, int64_t>(-9223372036854675807.0)));
    EXPECT_FALSE((check_signed_number_overflow<double, int64_t>(-9223372036854775807.0)));
    EXPECT_FALSE((check_signed_number_overflow<double, int64_t>(-9223372036854775808.0)));
    EXPECT_FALSE((check_signed_number_overflow<double, int64_t>(-9223372036854775809.0)));
    EXPECT_TRUE((check_signed_number_overflow<double, int64_t>(-9223372036854778808.0)));

    EXPECT_FALSE((check_signed_number_overflow<float, int32_t>(2147483000.0)));
    EXPECT_TRUE((check_signed_number_overflow<float, int32_t>(2147483647.0)));
    EXPECT_TRUE((check_signed_number_overflow<float, int32_t>(2147483647.3)));
    EXPECT_TRUE((check_signed_number_overflow<float, int32_t>(2147483648.0)));
    EXPECT_TRUE((check_signed_number_overflow<float, int32_t>(2147483649.0)));

    EXPECT_FALSE((check_signed_number_overflow<float, int32_t>(-2147473647.0)));
    EXPECT_FALSE((check_signed_number_overflow<float, int32_t>(-2147483647.0)));
    EXPECT_FALSE((check_signed_number_overflow<float, int32_t>(-2147483648.0)));
    EXPECT_FALSE((check_signed_number_overflow<float, int32_t>(-2147483649.0)));
    EXPECT_TRUE((check_signed_number_overflow<float, int32_t>(-2147494649.0)));

    EXPECT_FALSE((check_signed_number_overflow<float, int16_t>(32767)));
    EXPECT_TRUE((check_signed_number_overflow<float, int16_t>(32768)));
    EXPECT_TRUE((check_signed_number_overflow<float, int16_t>(32769)));
    EXPECT_FALSE((check_signed_number_overflow<float, int16_t>(-32765)));
    EXPECT_FALSE((check_signed_number_overflow<float, int16_t>(-32767)));
    EXPECT_FALSE((check_signed_number_overflow<float, int16_t>(-32768)));
    EXPECT_TRUE((check_signed_number_overflow<float, int16_t>(-32770)));

    EXPECT_FALSE((check_signed_number_overflow<float, int8_t>(127)));
    EXPECT_TRUE((check_signed_number_overflow<float, int8_t>(128)));
    EXPECT_TRUE((check_signed_number_overflow<float, int8_t>(129)));

    EXPECT_FALSE((check_signed_number_overflow<float, int8_t>(-127)));
    EXPECT_FALSE((check_signed_number_overflow<float, int8_t>(-128)));
    EXPECT_TRUE((check_signed_number_overflow<float, int8_t>(-129)));

    {
        double d_value = 9223372036854775000.0;
        for (int i = 0; i < 10000; i++) {
            d_value -= 10000.0;
            EXPECT_FALSE((check_signed_number_overflow<double, int64_t>(d_value)));
        }
    }

    {
        double d_value = 9223372036854775808.0;
        for (int i = 0; i < 10000; i++) {
            d_value += 10000.0;
            EXPECT_TRUE((check_signed_number_overflow<double, int64_t>(d_value)));
        }
    }

    {
        float d_value = 2147483000.0;
        for (int i = 0; i < 10000; i++) {
            d_value -= 1000.0;
            EXPECT_FALSE((check_signed_number_overflow<float, int32_t>(d_value)));
        }
    }

    {
        float d_value = 2147483649.0;
        for (int i = 0; i < 10000; i++) {
            d_value += 1000.0;
            EXPECT_TRUE((check_signed_number_overflow<float, int32_t>(d_value)));
        }
    }
}

TEST(TestNumericTypes, test_check_signed_number_overflow_widen_conversion) {
    // from int8_t
    EXPECT_FALSE((check_signed_number_overflow<int8_t, int16_t>(std::numeric_limits<int8_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int8_t, int16_t>(std::numeric_limits<int8_t>::min())));

    EXPECT_FALSE((check_signed_number_overflow<int8_t, int32_t>(std::numeric_limits<int8_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int8_t, int32_t>(std::numeric_limits<int8_t>::min())));

    EXPECT_FALSE((check_signed_number_overflow<int8_t, int64_t>(std::numeric_limits<int8_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int8_t, int64_t>(std::numeric_limits<int8_t>::min())));

    EXPECT_FALSE((check_signed_number_overflow<int8_t, int128_t>(std::numeric_limits<int8_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int8_t, int128_t>(std::numeric_limits<int8_t>::min())));

    EXPECT_FALSE((check_signed_number_overflow<int8_t, float>(std::numeric_limits<int8_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int8_t, float>(std::numeric_limits<int8_t>::min())));

    EXPECT_FALSE((check_signed_number_overflow<int8_t, double>(std::numeric_limits<int8_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int8_t, double>(std::numeric_limits<int8_t>::min())));

    // from int16_t
    EXPECT_FALSE((check_signed_number_overflow<int16_t, int32_t>(std::numeric_limits<int16_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int16_t, int32_t>(std::numeric_limits<int16_t>::min())));

    EXPECT_FALSE((check_signed_number_overflow<int16_t, int64_t>(std::numeric_limits<int16_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int16_t, int64_t>(std::numeric_limits<int16_t>::min())));

    EXPECT_FALSE((check_signed_number_overflow<int16_t, int128_t>(std::numeric_limits<int16_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int16_t, int128_t>(std::numeric_limits<int16_t>::min())));

    EXPECT_FALSE((check_signed_number_overflow<int16_t, float>(std::numeric_limits<int16_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int16_t, float>(std::numeric_limits<int16_t>::min())));

    EXPECT_FALSE((check_signed_number_overflow<int16_t, double>(std::numeric_limits<int16_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int16_t, double>(std::numeric_limits<int16_t>::min())));

    // from int32_t
    EXPECT_FALSE((check_signed_number_overflow<int32_t, int64_t>(std::numeric_limits<int32_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int32_t, int64_t>(std::numeric_limits<int32_t>::min())));

    EXPECT_FALSE((check_signed_number_overflow<int32_t, int128_t>(std::numeric_limits<int32_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int32_t, int128_t>(std::numeric_limits<int32_t>::min())));

    EXPECT_FALSE((check_signed_number_overflow<int32_t, float>(std::numeric_limits<int32_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int32_t, float>(std::numeric_limits<int32_t>::min())));

    EXPECT_FALSE((check_signed_number_overflow<int32_t, double>(std::numeric_limits<int32_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int32_t, double>(std::numeric_limits<int32_t>::min())));

    // from int64_t
    EXPECT_FALSE((check_signed_number_overflow<int64_t, int128_t>(std::numeric_limits<int64_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int64_t, int128_t>(std::numeric_limits<int64_t>::min())));

    EXPECT_FALSE((check_signed_number_overflow<int64_t, float>(std::numeric_limits<int64_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int64_t, float>(std::numeric_limits<int64_t>::min())));

    EXPECT_FALSE((check_signed_number_overflow<int64_t, double>(std::numeric_limits<int64_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int64_t, double>(std::numeric_limits<int64_t>::min())));

    // from int128_t
    EXPECT_FALSE((check_signed_number_overflow<int128_t, float>(std::numeric_limits<int128_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int128_t, float>(std::numeric_limits<int128_t>::min())));

    EXPECT_FALSE((check_signed_number_overflow<int128_t, double>(std::numeric_limits<int128_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int128_t, double>(std::numeric_limits<int128_t>::min())));

    // from float
    EXPECT_FALSE((check_signed_number_overflow<float, double>(std::numeric_limits<float>::lowest())));
    EXPECT_FALSE((check_signed_number_overflow<float, double>(std::numeric_limits<float>::min())));
}

TEST(TestNumericTypes, test_check_signed_number_overflow_integral_narrow_conversion) {
    // from int128_t
    EXPECT_FALSE((check_signed_number_overflow<int128_t, int8_t>(std::numeric_limits<int8_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int128_t, int8_t>(std::numeric_limits<int8_t>::min())));
    EXPECT_TRUE((check_signed_number_overflow<int128_t, int8_t>(
            static_cast<int128_t>(std::numeric_limits<int8_t>::max()) + 1)));
    EXPECT_TRUE((check_signed_number_overflow<int128_t, int8_t>(
            static_cast<int128_t>(std::numeric_limits<int8_t>::min()) - 1)));

    EXPECT_FALSE((check_signed_number_overflow<int128_t, int16_t>(std::numeric_limits<int16_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int128_t, int16_t>(std::numeric_limits<int16_t>::min())));
    EXPECT_TRUE((check_signed_number_overflow<int128_t, int16_t>(
            static_cast<int128_t>(std::numeric_limits<int16_t>::max()) + 1)));
    EXPECT_TRUE((check_signed_number_overflow<int128_t, int16_t>(
            static_cast<int128_t>(std::numeric_limits<int16_t>::min()) - 1)));

    EXPECT_FALSE((check_signed_number_overflow<int128_t, int32_t>(std::numeric_limits<int32_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int128_t, int32_t>(std::numeric_limits<int32_t>::min())));
    EXPECT_TRUE((check_signed_number_overflow<int128_t, int32_t>(
            static_cast<int128_t>(std::numeric_limits<int32_t>::max()) + 1)));
    EXPECT_TRUE((check_signed_number_overflow<int128_t, int32_t>(
            static_cast<int128_t>(std::numeric_limits<int32_t>::min()) - 1)));

    EXPECT_FALSE((check_signed_number_overflow<int128_t, int64_t>(std::numeric_limits<int64_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int128_t, int64_t>(std::numeric_limits<int64_t>::min())));
    EXPECT_TRUE((check_signed_number_overflow<int128_t, int64_t>(
            static_cast<int128_t>(std::numeric_limits<int64_t>::max()) + 1)));
    EXPECT_TRUE((check_signed_number_overflow<int128_t, int64_t>(
            static_cast<int128_t>(std::numeric_limits<int64_t>::min()) - 1)));

    // from int64_t
    EXPECT_FALSE((check_signed_number_overflow<int64_t, int8_t>(std::numeric_limits<int8_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int64_t, int8_t>(std::numeric_limits<int8_t>::min())));
    EXPECT_TRUE((check_signed_number_overflow<int64_t, int8_t>(
            static_cast<int64_t>(std::numeric_limits<int8_t>::max()) + 1)));
    EXPECT_TRUE((check_signed_number_overflow<int64_t, int8_t>(
            static_cast<int64_t>(std::numeric_limits<int8_t>::min()) - 1)));

    EXPECT_FALSE((check_signed_number_overflow<int64_t, int16_t>(std::numeric_limits<int16_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int64_t, int16_t>(std::numeric_limits<int16_t>::min())));
    EXPECT_TRUE((check_signed_number_overflow<int64_t, int16_t>(
            static_cast<int64_t>(std::numeric_limits<int16_t>::max()) + 1)));
    EXPECT_TRUE((check_signed_number_overflow<int64_t, int16_t>(
            static_cast<int64_t>(std::numeric_limits<int16_t>::min()) - 1)));

    EXPECT_FALSE((check_signed_number_overflow<int64_t, int32_t>(std::numeric_limits<int32_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int64_t, int32_t>(std::numeric_limits<int32_t>::min())));
    EXPECT_TRUE((check_signed_number_overflow<int64_t, int32_t>(
            static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1)));
    EXPECT_TRUE((check_signed_number_overflow<int64_t, int32_t>(
            static_cast<int64_t>(std::numeric_limits<int32_t>::min()) - 1)));

    // from int32_t
    EXPECT_FALSE((check_signed_number_overflow<int32_t, int8_t>(std::numeric_limits<int8_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int32_t, int8_t>(std::numeric_limits<int8_t>::min())));
    EXPECT_TRUE((check_signed_number_overflow<int32_t, int8_t>(
            static_cast<int32_t>(std::numeric_limits<int8_t>::max()) + 1)));
    EXPECT_TRUE((check_signed_number_overflow<int32_t, int8_t>(
            static_cast<int32_t>(std::numeric_limits<int8_t>::min()) - 1)));

    EXPECT_FALSE((check_signed_number_overflow<int32_t, int16_t>(std::numeric_limits<int16_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int32_t, int16_t>(std::numeric_limits<int16_t>::min())));
    EXPECT_TRUE((check_signed_number_overflow<int32_t, int16_t>(
            static_cast<int32_t>(std::numeric_limits<int16_t>::max()) + 1)));
    EXPECT_TRUE((check_signed_number_overflow<int32_t, int16_t>(
            static_cast<int32_t>(std::numeric_limits<int16_t>::min()) - 1)));

    // from int16_t
    EXPECT_FALSE((check_signed_number_overflow<int16_t, int8_t>(std::numeric_limits<int8_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<int16_t, int8_t>(std::numeric_limits<int8_t>::min())));
    EXPECT_TRUE((check_signed_number_overflow<int16_t, int8_t>(
            static_cast<int16_t>(std::numeric_limits<int8_t>::max()) + 1)));
    EXPECT_TRUE((check_signed_number_overflow<int16_t, int8_t>(
            static_cast<int16_t>(std::numeric_limits<int8_t>::min()) - 1)));

    // from double to float
    EXPECT_FALSE((check_signed_number_overflow<double, float>(std::numeric_limits<float>::max())));
    EXPECT_FALSE((check_signed_number_overflow<double, float>(std::numeric_limits<float>::lowest())));
    EXPECT_TRUE(
            (check_signed_number_overflow<double, float>(static_cast<double>(std::numeric_limits<float>::max()) * 2)));
    EXPECT_TRUE((check_signed_number_overflow<double, float>(static_cast<double>(std::numeric_limits<float>::lowest()) *
                                                             2)));
}

// JSON will use check_signed_number_overflow to check whether an uint64_t converted to a signed number type will overflow.
// Se checked_cast in formats/json/numeric_column.cpp for details.
TEST(TestNumericTypes, test_check_signed_number_overflow_from_uint64_t) {
    EXPECT_TRUE((check_signed_number_overflow<uint64_t, int8_t>(
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1)));
    EXPECT_TRUE((check_signed_number_overflow<uint64_t, int16_t>(
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1)));
    EXPECT_TRUE((check_signed_number_overflow<uint64_t, int32_t>(
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1)));
    EXPECT_TRUE((check_signed_number_overflow<uint64_t, int64_t>(
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1)));

    EXPECT_FALSE((check_signed_number_overflow<uint64_t, int128_t>(
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1)));
    EXPECT_FALSE((check_signed_number_overflow<uint64_t, int128_t>(std::numeric_limits<uint64_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<uint64_t, float>(std::numeric_limits<uint64_t>::max())));
    EXPECT_FALSE((check_signed_number_overflow<uint64_t, double>(std::numeric_limits<uint64_t>::max())));
}

} // namespace starrocks