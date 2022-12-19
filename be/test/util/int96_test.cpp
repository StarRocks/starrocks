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

#include "util/int96.h"

#include <gtest/gtest.h>

#include <iostream>

namespace starrocks {
class Int96Test : public testing::Test {
public:
    Int96Test() = default;
    ~Int96Test() override = default;
};

TEST_F(Int96Test, Normal) {
    int96_t value;
    value.lo = 1;
    value.hi = 2;

    {
        int96_t check;
        check.lo = 0;
        check.hi = 2;
        ASSERT_TRUE(check < value);

        check.lo = 1;
        check.hi = 2;
        ASSERT_TRUE(check == value);
        ASSERT_FALSE(check != value);

        check.lo = 2;
        check.hi = 2;
        ASSERT_TRUE(check > value);

        check.lo = 1;
        check.hi = 3;
        ASSERT_TRUE(check > value);
    }

    std::stringstream ss;
    ss << value;
    ASSERT_STREQ("36893488147419103233", ss.str().c_str());
}

} // namespace starrocks
