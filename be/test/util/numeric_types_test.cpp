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

namespace starrocks {

TEST(TestNumericTypes, test_check_number_overflow) {
    EXPECT_FALSE((check_number_overflow<double, int64_t>(9223372036854775000.0)));
    EXPECT_TRUE((check_number_overflow<double, int64_t>(9223372036854775807.0)));
    EXPECT_TRUE((check_number_overflow<double, int64_t>(9223372036854775807.3)));
    EXPECT_TRUE((check_number_overflow<double, int64_t>(9223372036854775808.0)));
    EXPECT_TRUE((check_number_overflow<double, int64_t>(9223372036854775809.0)));

    EXPECT_FALSE((check_number_overflow<double, int64_t>(-9223372036854675807.0)));
    EXPECT_FALSE((check_number_overflow<double, int64_t>(-9223372036854775807.0)));
    EXPECT_FALSE((check_number_overflow<double, int64_t>(-9223372036854775808.0)));
    EXPECT_FALSE((check_number_overflow<double, int64_t>(-9223372036854775809.0)));
    EXPECT_TRUE((check_number_overflow<double, int64_t>(-9223372036854778808.0)));

    EXPECT_FALSE((check_number_overflow<float, int32_t>(2147483000.0)));
    EXPECT_TRUE((check_number_overflow<float, int32_t>(2147483647.0)));
    EXPECT_TRUE((check_number_overflow<float, int32_t>(2147483647.3)));
    EXPECT_TRUE((check_number_overflow<float, int32_t>(2147483648.0)));
    EXPECT_TRUE((check_number_overflow<float, int32_t>(2147483649.0)));

    EXPECT_FALSE((check_number_overflow<float, int32_t>(-2147473647.0)));
    EXPECT_FALSE((check_number_overflow<float, int32_t>(-2147483647.0)));
    EXPECT_FALSE((check_number_overflow<float, int32_t>(-2147483648.0)));
    EXPECT_FALSE((check_number_overflow<float, int32_t>(-2147483649.0)));
    EXPECT_TRUE((check_number_overflow<float, int32_t>(-2147494649.0)));

    EXPECT_FALSE((check_number_overflow<float, int16_t>(32767)));
    EXPECT_TRUE((check_number_overflow<float, int16_t>(32768)));
    EXPECT_TRUE((check_number_overflow<float, int16_t>(32769)));
    EXPECT_FALSE((check_number_overflow<float, int16_t>(-32765)));
    EXPECT_FALSE((check_number_overflow<float, int16_t>(-32767)));
    EXPECT_FALSE((check_number_overflow<float, int16_t>(-32768)));
    EXPECT_TRUE((check_number_overflow<float, int16_t>(-32770)));

    EXPECT_FALSE((check_number_overflow<float, int8_t>(127)));
    EXPECT_TRUE((check_number_overflow<float, int8_t>(128)));
    EXPECT_TRUE((check_number_overflow<float, int8_t>(129)));

    EXPECT_FALSE((check_number_overflow<float, int8_t>(-127)));
    EXPECT_FALSE((check_number_overflow<float, int8_t>(-128)));
    EXPECT_TRUE((check_number_overflow<float, int8_t>(-129)));

    {
        double d_value = 9223372036854775000.0;
        for (int i = 0; i < 10000; i++) {
            d_value -= 10000.0;
            EXPECT_FALSE((check_number_overflow<double, int64_t>(d_value)));
        }
    }

    {
        double d_value = 9223372036854775808.0;
        for (int i = 0; i < 10000; i++) {
            d_value += 10000.0;
            EXPECT_TRUE((check_number_overflow<double, int64_t>(d_value)));
        }
    }

    {
        float d_value = 2147483000.0;
        for (int i = 0; i < 10000; i++) {
            d_value -= 1000.0;
            EXPECT_FALSE((check_number_overflow<float, int32_t>(d_value)));
        }
    }

    {
        float d_value = 2147483649.0;
        for (int i = 0; i < 10000; i++) {
            d_value += 1000.0;
            EXPECT_TRUE((check_number_overflow<float, int32_t>(d_value)));
        }
    }
}

} // namespace starrocks