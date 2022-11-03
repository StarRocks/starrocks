// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
