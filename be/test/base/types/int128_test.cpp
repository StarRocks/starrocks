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

#include "base/types/int128.h"

#include <gtest/gtest.h>

#include <limits>
#include <sstream>

namespace starrocks {

constexpr __int128 kMaxInt128 = ~((__int128)0x01 << 127);
constexpr __int128 kMinInt128 = ((__int128)0x01 << 127);

TEST(Int128Test, ToString) {
    EXPECT_EQ(LargeIntValue::to_string(static_cast<__int128>(std::numeric_limits<int64_t>::max())),
              "9223372036854775807");
    EXPECT_EQ(LargeIntValue::to_string(kMaxInt128), "170141183460469231731687303715884105727");
    EXPECT_EQ(LargeIntValue::to_string(kMinInt128), "-170141183460469231731687303715884105728");
}

TEST(Int128Test, StreamOutput) {
    {
        __int128 v1 = std::numeric_limits<int64_t>::max();
        std::stringstream ss;
        ss << v1;
        EXPECT_EQ(ss.str(), "9223372036854775807");
    }

    {
        std::stringstream ss;
        ss << kMaxInt128;
        EXPECT_EQ(ss.str(), "170141183460469231731687303715884105727");
    }

    {
        std::stringstream ss;
        ss << kMinInt128;
        EXPECT_EQ(ss.str(), "-170141183460469231731687303715884105728");
    }
}

} // namespace starrocks
