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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/runtime/int128_arithmetic_ops_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>

#include <iostream>
#include <random>
#include <string>

#include "runtime/int128_arithmetics_x86_64.h"
#include "util/logging.h"

typedef __int128 int128_t;
typedef unsigned __int128 uint128_t;
namespace starrocks {
class Int128ArithmeticOpsTest : public testing::Test {
    static inline int128_t gen_int128(int n) {
        std::random_device rd;
        std::mt19937 gen(rd());
        assert(0 <= n && n <= 128);

        int low_bits = 64;
        int high_bits = 64;
        if (n > 64) {
            low_bits = 64;
            high_bits = n - 64;
        } else {
            low_bits = n;
            high_bits = 0;
        }

        int64_t min_low_part = 0;
        int64_t max_low_part = 0;
        if (low_bits < 64) {
            max_low_part = (1l < low_bits) - 1;
            min_low_part = -max_low_part;
        } else {
            max_low_part = -1l ^ (1l << 63);
            min_low_part = 1l << 63;
        }

        int64_t min_high_part = 0;
        int64_t max_high_part = 0;
        if (high_bits < 64) {
            max_high_part = (1l < high_bits) - 1;
            min_high_part = -max_low_part;
        } else {
            max_high_part = -1l ^ (1l << 63);
            min_high_part = 1l << 63;
        }

        std::uniform_int_distribution<int64_t> low_rand(min_low_part, max_low_part);
        std::uniform_int_distribution<int64_t> high_rand(min_high_part, max_high_part);
        auto a = high_rand(gen);
        auto b = static_cast<uint64_t>(low_rand(gen));
        return (static_cast<int128_t>(a) << 64) ^ static_cast<int128_t>(b);
    }

    template <class F>
    static inline void compare(F&& f) {
        for (int i = 0; i <= 128; ++i) {
            for (int j = 0; j <= 128; ++j) {
                for (int k = 0; k < 10; ++k) {
                    auto x = gen_int128(i);
                    auto y = gen_int128(j);
                    f(x, y);
                }
            }
        }
    }
};
#if defined(__x86_64__) && defined(__GNUC__)
TEST_F(Int128ArithmeticOpsTest, compareMul) {
    compare([](int128_t x, int128_t y) {
        int128_t r0 = x * y;
        int128_t r1 = 0;
        multi3(x, y, r1);
        ASSERT_EQ(r0, r1);
    });
}

TEST_F(Int128ArithmeticOpsTest, compareDivAndMod) {
    compare([](int128_t x, int128_t y) {
        if (y != 0) {
            auto q0 = x / y;
            auto r0 = x % y;
            int128_t q1 = 0;
            uint128_t r1 = 0;
            divmodti3(x, y, q1, r1);
            ASSERT_EQ(q0, q1);
            ASSERT_EQ(r0, r1);
        }
    });
}

TEST_F(Int128ArithmeticOpsTest, testMulOverflow) {
    std::vector<std::tuple<int128_t, int128_t, int128_t, int>> data = {
            {0, 0, 0, 0},
            {-1, -1, 1, 0},
            {-1, 1, -1, 0},
            {1, -1, -1, 0},
            {0xffff'ffff'ffff'ffffl, 0xffff'ffff'ffff'ffffl,
             (static_cast<int128_t>(0xfffffffffffffffe) << 64) + 0000000000000001, 1},
            {0x8000'0000'0000'0000l, 0x8000'0000'0000'0000l, (static_cast<int128_t>(0x4000'0000'0000'0000l) << 64), 0},
            {0x8000'0000'0000'0000l, (static_cast<int128_t>(1) << 64),
             (static_cast<int128_t>(0x8000'0000'0000'0000l) << 64), 1},
            {0xffff'ffff'ffff'ffffl, (static_cast<int128_t>(1) << 64),
             (static_cast<int128_t>(0xffff'ffff'ffff'ffffl) << 64), 1},
            {(static_cast<int128_t>(0xffff'ffff'ffff'ffffl) << 64), (static_cast<int128_t>(1) << 63),
             (static_cast<int128_t>(0x8000'0000'0000'0000l) << 64), 1},
            {(static_cast<int128_t>(0xffff'ffff'ffff'ffffl) << 64),
             (static_cast<int128_t>(-1l) << 64) + 0x8000'0000'0000'0000l, (static_cast<int128_t>(1) << 127), 1},
            {(static_cast<int128_t>(0xffff'ffff'ffff'ffffl) << 64),
             (static_cast<int128_t>(-1l) << 64) + 0xc000'0000'0000'0000l, (static_cast<int128_t>(1) << 126), 0},
    };
    for (auto& d : data) {
        auto [x, y, expect_result, expect_overflow] = d;
        int128_t actual_result;
        int actual_overflow = multi3(x, y, actual_result);
        ASSERT_EQ(expect_result, actual_result);
        ASSERT_EQ(expect_overflow, actual_overflow);
    }
}

TEST_F(Int128ArithmeticOpsTest, test_i32xi32_produce_i64) {
    std::vector<int32_t> values{
            0,
            1,
            (int32_t)0xffff0000,
            -1,
            (int32_t)0xdeadbeef,
            99999'9999,
            -99999'9999,
            1 << 31,
            1 << 30,
            0x60606060,
            (int32_t)0xc0c0c0c0c,
            0x0c0c0c0c,
    };
    for (auto a : values) {
        for (auto b : values) {
            ASSERT_EQ(i32_x_i32_produce_i64(a, b), (int64_t)a * (int64_t)b);
        }
    }
}

TEST_F(Int128ArithmeticOpsTest, test_i64xi64_produce_i128) {
    std::vector<int64_t> values{
            0L,
            1L,
            (int64_t)0xffff0000ffff0000L,
            -1L,
            (int64_t)0xdeadbeefdeadbeefL,
            99999'9999'99999'9999L,
            -99999'9999'99999'9999L,
            1L << 63,
            1L << 62,
            0x6060'6060'6060'6060L,
            (int64_t)0xc0c0'c0c0'c0c0'c0c0L,
            0x0c0c'0c0c'0c0c'0c0cL,
    };
    for (auto a : values) {
        for (auto b : values) {
            ASSERT_EQ(i64_x_i64_produce_i128(a, b), (int128_t)a * (int128_t)b);
        }
    }
}

#endif // defined(__X86_64__) && defined(__GNUC__)
} // namespace starrocks
