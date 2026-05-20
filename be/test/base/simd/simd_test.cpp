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
//   https://github.com/apache/incubator-doris/blob/master/be/test/simd/simd_test.cpp

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

#include "base/simd/simd.h"

#include "gtest/gtest.h"

namespace starrocks {

class SIMDTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(SIMDTest, count_zeros) {
    EXPECT_EQ(0u, SIMD::count_zero(std::vector<int8_t>{}));
    EXPECT_EQ(3u, SIMD::count_zero(std::vector<int8_t>{0, 0, 0}));
    EXPECT_EQ(1u, SIMD::count_zero(std::vector<int8_t>{0, 1, 2}));
    EXPECT_EQ(1u, SIMD::count_zero(std::vector<int8_t>{-1, 0, 1}));

    // size greater than 64 will use SSE2 instructions.
    std::vector<int8_t> nums(100, 0);
    EXPECT_EQ(100u, SIMD::count_zero(nums));
    nums.emplace_back(1);
    EXPECT_EQ(100u, SIMD::count_zero(nums));
    nums.emplace_back(0);
    EXPECT_EQ(101u, SIMD::count_zero(nums));
    nums.emplace_back(-1);
    EXPECT_EQ(101u, SIMD::count_zero(nums));
}

TEST_F(SIMDTest, count_nonzero) {
    // size less than 64, count by loop.
    EXPECT_EQ(0u, SIMD::count_nonzero(std::vector<int8_t>{}));
    EXPECT_EQ(0u, SIMD::count_nonzero(std::vector<int8_t>{0, 0, 0}));
    EXPECT_EQ(3u, SIMD::count_nonzero(std::vector<int8_t>{1, 1, 1}));
    EXPECT_EQ(3u, SIMD::count_nonzero(std::vector<int8_t>{-1, 1, 2}));
    EXPECT_EQ(2u, SIMD::count_nonzero(std::vector<int8_t>{0, 1, 2}));

    // size greater than 64 will use SSE2 instructions.
    std::vector<uint8_t> numbers(100, 0);
    EXPECT_EQ(0u, SIMD::count_nonzero(numbers));

    for (int i = 1; i <= 10; i++) {
        numbers.emplace_back(1);
    }
    EXPECT_EQ(10u, SIMD::count_nonzero(numbers));

    for (int i = 1; i <= 10; i++) {
        numbers.emplace_back(i);
    }
    EXPECT_EQ(20u, SIMD::count_nonzero(numbers));

    for (int i = 1; i <= 10; i++) {
        numbers.emplace_back(0 - i);
    }
    EXPECT_EQ(30u, SIMD::count_nonzero(numbers));
}

TEST_F(SIMDTest, count_zeros_int32) {
    EXPECT_EQ(0u, SIMD::count_zero(std::vector<uint32_t>{}));
    EXPECT_EQ(3u, SIMD::count_zero(std::vector<uint32_t>{0, 0, 0}));
    EXPECT_EQ(1u, SIMD::count_zero(std::vector<uint32_t>{0, 1, 2}));
    EXPECT_EQ(1u, SIMD::count_zero(std::vector<uint32_t>{11, 0, 1}));

    // size greater than 64 will use SSE2 instructions.
    std::vector<uint32_t> nums(100, 0);
    EXPECT_EQ(100u, SIMD::count_zero(nums));
    nums.emplace_back(1);
    EXPECT_EQ(100u, SIMD::count_zero(nums));
    nums.emplace_back(0);
    EXPECT_EQ(101u, SIMD::count_zero(nums));
    nums.emplace_back(11);
    EXPECT_EQ(101u, SIMD::count_zero(nums));
}

TEST_F(SIMDTest, count_nonzero_int32) {
    // size less than 64, count by loop.
    EXPECT_EQ(0u, SIMD::count_nonzero(std::vector<uint32_t>{}));
    EXPECT_EQ(0u, SIMD::count_nonzero(std::vector<uint32_t>{0, 0, 0}));
    EXPECT_EQ(3u, SIMD::count_nonzero(std::vector<uint32_t>{1, 1, 1}));
    EXPECT_EQ(3u, SIMD::count_nonzero(std::vector<uint32_t>{11, 1, 2}));
    EXPECT_EQ(2u, SIMD::count_nonzero(std::vector<uint32_t>{0, 1, 2}));

    // size greater than 64 will use SSE2 instructions.
    std::vector<uint32_t> numbers(100, 0);
    EXPECT_EQ(0u, SIMD::count_nonzero(numbers));

    for (int i = 1; i <= 10; i++) {
        numbers.emplace_back(1);
    }
    EXPECT_EQ(10u, SIMD::count_nonzero(numbers));

    for (int i = 1; i <= 10; i++) {
        numbers.emplace_back(i);
    }
    EXPECT_EQ(20u, SIMD::count_nonzero(numbers));

    for (int i = 1; i <= 10; i++) {
        numbers.emplace_back(i + 100);
    }
    EXPECT_EQ(30u, SIMD::count_nonzero(numbers));
}

TEST_F(SIMDTest, contains_nonzero_bit) {
    std::vector<uint8_t> nums;
    for (int i = 0; i < 1000; i++) {
        nums.emplace_back(0);
    }
    EXPECT_FALSE(SIMD::contains_nonzero_bit(nums.data(), nums.size()));

    // non-zero in non-SIMD check tail part.
    nums.emplace_back(8);
    EXPECT_TRUE(SIMD::contains_nonzero_bit(nums.data(), nums.size()));

    // non-zero in SIMD check part.
    for (int i = 0; i < 1000; i++) {
        nums.emplace_back(0);
    }
    EXPECT_TRUE(SIMD::contains_nonzero_bit(nums.data(), nums.size()));
}

TEST_F(SIMDTest, all_zeros) {
    // Empty: vacuously all zeros (no non-zero element exists).
    EXPECT_TRUE(SIMD::all_zeros(std::vector<uint8_t>{}));
    EXPECT_TRUE(SIMD::all_zeros(std::vector<uint8_t>{0, 0, 0, 0}));
    EXPECT_FALSE(SIMD::all_zeros(std::vector<uint8_t>{0, 0, 1}));
    EXPECT_FALSE(SIMD::all_zeros(std::vector<uint8_t>{1, 0, 0}));

    // Large block all-zero, then flip various positions to make sure SIMD body
    // and scalar tail both reject.
    std::vector<uint8_t> nums(1024, 0);
    EXPECT_TRUE(SIMD::all_zeros(nums));

    nums[0] = 1;
    EXPECT_FALSE(SIMD::all_zeros(nums));
    nums[0] = 0;

    nums[512] = 1; // middle of SIMD body
    EXPECT_FALSE(SIMD::all_zeros(nums));
    nums[512] = 0;

    nums.back() = 1; // tail
    EXPECT_FALSE(SIMD::all_zeros(nums));
}

TEST_F(SIMDTest, all_ones) {
    // Contract: all_ones means "no zero byte present" (memchr-style), NOT
    // "every byte == 0xFF". Equivalent to count_zero == 0.
    EXPECT_TRUE(SIMD::all_ones(std::vector<uint8_t>{})); // vacuously true
    EXPECT_TRUE(SIMD::all_ones(std::vector<uint8_t>{1, 2, 3, 4}));
    EXPECT_TRUE(SIMD::all_ones(std::vector<uint8_t>{0xFF, 0xFF, 0xFF}));
    EXPECT_FALSE(SIMD::all_ones(std::vector<uint8_t>{1, 0, 1}));
    EXPECT_FALSE(SIMD::all_ones(std::vector<uint8_t>{0, 1, 1}));
    EXPECT_FALSE(SIMD::all_ones(std::vector<uint8_t>{1, 1, 0}));

    std::vector<uint8_t> nums(1024, 0xFF);
    EXPECT_TRUE(SIMD::all_ones(nums));
    nums[0] = 0;
    EXPECT_FALSE(SIMD::all_ones(nums));
    nums[0] = 0xFF;
    nums[512] = 0;
    EXPECT_FALSE(SIMD::all_ones(nums));
    nums[512] = 0xFF;
    nums.back() = 0;
    EXPECT_FALSE(SIMD::all_ones(nums));
}

} // namespace starrocks
