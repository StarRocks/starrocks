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

#include <algorithm>
#include <random>

#include "gtest/gtest.h"

namespace starrocks {

class SIMDTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(SIMDTest, count_zeros) {
    std::vector<uint8_t> empty_vec;
    EXPECT_EQ(0, SIMD::count_zero(empty_vec));
    EXPECT_EQ(0, SIMD::count_zero(static_cast<const uint8_t*>(nullptr), 0));
    EXPECT_EQ(0, SIMD::count_zero(static_cast<const int8_t*>(nullptr), 0));

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
    std::vector<uint32_t> empty_vec;
    EXPECT_EQ(0, SIMD::count_zero(empty_vec));
    EXPECT_EQ(0, SIMD::count_zero(static_cast<const uint32_t*>(nullptr), 0));
    EXPECT_EQ(0, SIMD::count_zero(static_cast<const int32_t*>(nullptr), 0));

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

TEST_F(SIMDTest, count_zeros_signed_int8) {
    EXPECT_EQ(0u, SIMD::count_zero(std::vector<int8_t>{}));
    EXPECT_EQ(3u, SIMD::count_zero(std::vector<int8_t>{0, 0, 0}));
    EXPECT_EQ(1u, SIMD::count_zero(std::vector<int8_t>{0, 1, -2}));
    EXPECT_EQ(1u, SIMD::count_zero(std::vector<int8_t>{-1, 0, 1}));

    std::vector<int8_t> nums(100, 0);
    EXPECT_EQ(100u, SIMD::count_zero(nums));
    EXPECT_EQ(100u, SIMD::count_zero(nums.data(), nums.size()));
    nums.emplace_back(1);
    EXPECT_EQ(100u, SIMD::count_zero(nums));
    EXPECT_EQ(1u, SIMD::count_nonzero(nums));
    nums.emplace_back(0);
    EXPECT_EQ(101u, SIMD::count_zero(nums));
    nums.emplace_back(-1);
    EXPECT_EQ(101u, SIMD::count_zero(nums));
    EXPECT_EQ(2u, SIMD::count_nonzero(nums));
}

TEST_F(SIMDTest, count_zeros_signed_int32) {
    EXPECT_EQ(0u, SIMD::count_zero(std::vector<int32_t>{}));
    EXPECT_EQ(3u, SIMD::count_zero(std::vector<int32_t>{0, 0, 0}));
    EXPECT_EQ(1u, SIMD::count_zero(std::vector<int32_t>{0, 1, -2}));
    EXPECT_EQ(1u, SIMD::count_zero(std::vector<int32_t>{-11, 0, 1}));

    std::vector<int32_t> nums(100, 0);
    EXPECT_EQ(100u, SIMD::count_zero(nums));
    EXPECT_EQ(100u, SIMD::count_zero(nums.data(), nums.size()));
    nums.emplace_back(1);
    EXPECT_EQ(100u, SIMD::count_zero(nums));
    EXPECT_EQ(1u, SIMD::count_nonzero(nums));
    nums.emplace_back(0);
    EXPECT_EQ(101u, SIMD::count_zero(nums));
    nums.emplace_back(-11);
    EXPECT_EQ(101u, SIMD::count_zero(nums));
    EXPECT_EQ(2u, SIMD::count_nonzero(nums));
}

TEST_F(SIMDTest, count_zeros_boundary_sizes) {
    const std::vector<size_t> boundary_sizes = {0,   1,   15,  16,  31,  32,  63,  64,  127,
                                                128, 255, 256, 257, 511, 512, 513, 1024};

    for (size_t sz : boundary_sizes) {
        // 1. All zeros
        std::vector<uint8_t> u8_zeros(sz, 0);
        std::vector<int8_t> i8_zeros(sz, 0);
        std::vector<uint32_t> u32_zeros(sz, 0);
        std::vector<int32_t> i32_zeros(sz, 0);

        EXPECT_EQ(sz, SIMD::count_zero(u8_zeros.data(), sz));
        EXPECT_EQ(sz, SIMD::count_zero(i8_zeros.data(), sz));
        EXPECT_EQ(sz, SIMD::count_zero(u32_zeros.data(), sz));
        EXPECT_EQ(sz, SIMD::count_zero(i32_zeros.data(), sz));

        EXPECT_EQ(0u, SIMD::count_nonzero(u8_zeros.data(), sz));
        EXPECT_EQ(0u, SIMD::count_nonzero(i8_zeros.data(), sz));
        EXPECT_EQ(0u, SIMD::count_nonzero(u32_zeros.data(), sz));
        EXPECT_EQ(0u, SIMD::count_nonzero(i32_zeros.data(), sz));

        // 2. All non-zeros
        std::vector<uint8_t> u8_ones(sz, 1);
        std::vector<int8_t> i8_ones(sz, -1);
        std::vector<uint32_t> u32_ones(sz, 42);
        std::vector<int32_t> i32_ones(sz, -42);

        EXPECT_EQ(0u, SIMD::count_zero(u8_ones.data(), sz));
        EXPECT_EQ(0u, SIMD::count_zero(i8_ones.data(), sz));
        EXPECT_EQ(0u, SIMD::count_zero(u32_ones.data(), sz));
        EXPECT_EQ(0u, SIMD::count_zero(i32_ones.data(), sz));

        EXPECT_EQ(sz, SIMD::count_nonzero(u8_ones.data(), sz));
        EXPECT_EQ(sz, SIMD::count_nonzero(i8_ones.data(), sz));
        EXPECT_EQ(sz, SIMD::count_nonzero(u32_ones.data(), sz));
        EXPECT_EQ(sz, SIMD::count_nonzero(i32_ones.data(), sz));

        // 3. Mixed pattern: zero every 3rd element
        std::vector<uint8_t> u8_mixed(sz);
        std::vector<int8_t> i8_mixed(sz);
        std::vector<uint32_t> u32_mixed(sz);
        std::vector<int32_t> i32_mixed(sz);

        for (size_t i = 0; i < sz; ++i) {
            bool is_zero = (i % 3 == 0);
            u8_mixed[i] = is_zero ? 0 : static_cast<uint8_t>((i % 127) + 1);
            i8_mixed[i] = is_zero ? 0 : static_cast<int8_t>((i % 120) + 1);
            u32_mixed[i] = is_zero ? 0 : static_cast<uint32_t>(i * 31 + 7);
            i32_mixed[i] = is_zero ? 0 : static_cast<int32_t>((i * 31) + 1);
        }

        const size_t expected_u8 = std::count(u8_mixed.begin(), u8_mixed.end(), static_cast<uint8_t>(0));
        const size_t expected_i8 = std::count(i8_mixed.begin(), i8_mixed.end(), static_cast<int8_t>(0));
        const size_t expected_u32 = std::count(u32_mixed.begin(), u32_mixed.end(), static_cast<uint32_t>(0));
        const size_t expected_i32 = std::count(i32_mixed.begin(), i32_mixed.end(), static_cast<int32_t>(0));

        EXPECT_EQ(expected_u8, SIMD::count_zero(u8_mixed.data(), sz));
        EXPECT_EQ(expected_i8, SIMD::count_zero(i8_mixed.data(), sz));
        EXPECT_EQ(expected_u32, SIMD::count_zero(u32_mixed.data(), sz));
        EXPECT_EQ(expected_i32, SIMD::count_zero(i32_mixed.data(), sz));

        EXPECT_EQ(sz - expected_u8, SIMD::count_nonzero(u8_mixed.data(), sz));
        EXPECT_EQ(sz - expected_i8, SIMD::count_nonzero(i8_mixed.data(), sz));
        EXPECT_EQ(sz - expected_u32, SIMD::count_nonzero(u32_mixed.data(), sz));
        EXPECT_EQ(sz - expected_i32, SIMD::count_nonzero(i32_mixed.data(), sz));
    }
}

TEST_F(SIMDTest, count_zeros_overflow_stress) {
    constexpr size_t N = 1024;
    std::vector<uint8_t> u8_zeros(N, 0);
    std::vector<int8_t> i8_zeros(N, 0);
    std::vector<uint32_t> u32_zeros(N, 0);
    std::vector<int32_t> i32_zeros(N, 0);

    // Verify 8-bit lane counters do not overflow with 1024 consecutive zeros.
    EXPECT_EQ(N, SIMD::count_zero(u8_zeros.data(), N));
    EXPECT_EQ(N, SIMD::count_zero(i8_zeros.data(), N));
    EXPECT_EQ(N, SIMD::count_zero(u32_zeros.data(), N));
    EXPECT_EQ(N, SIMD::count_zero(i32_zeros.data(), N));

    // Sparse non-zeros in large buffer.
    u8_zeros[100] = 5;
    u8_zeros[255] = 7;
    u8_zeros[256] = 9;
    u8_zeros[511] = 11;
    u8_zeros[512] = 13;
    u8_zeros[1023] = 15;
    EXPECT_EQ(N - 6, SIMD::count_zero(u8_zeros.data(), N));

    i8_zeros[100] = -5;
    i8_zeros[255] = -7;
    i8_zeros[256] = 9;
    i8_zeros[511] = -11;
    i8_zeros[512] = 13;
    i8_zeros[1023] = -15;
    EXPECT_EQ(N - 6, SIMD::count_zero(i8_zeros.data(), N));

    u32_zeros[100] = 1000;
    u32_zeros[255] = 2000;
    u32_zeros[256] = 3000;
    u32_zeros[511] = 4000;
    u32_zeros[512] = 5000;
    u32_zeros[1023] = 6000;
    EXPECT_EQ(N - 6, SIMD::count_zero(u32_zeros.data(), N));

    i32_zeros[100] = -1000;
    i32_zeros[255] = -2000;
    i32_zeros[256] = 3000;
    i32_zeros[511] = -4000;
    i32_zeros[512] = 5000;
    i32_zeros[1023] = -6000;
    EXPECT_EQ(N - 6, SIMD::count_zero(i32_zeros.data(), N));
}

TEST_F(SIMDTest, count_zeros_unaligned_pointers) {
    constexpr size_t total_elements = 1024 + 128;
    alignas(64) uint8_t u8_buf[total_elements];
    alignas(64) int8_t i8_buf[total_elements];
    alignas(64) uint32_t u32_buf[total_elements];
    alignas(64) int32_t i32_buf[total_elements];

    for (size_t i = 0; i < total_elements; ++i) {
        bool is_zero = (i % 3 == 0);
        u8_buf[i] = is_zero ? 0 : static_cast<uint8_t>((i % 127) + 1);
        i8_buf[i] = is_zero ? 0 : static_cast<int8_t>((i % 127) - 60);
        u32_buf[i] = is_zero ? 0 : static_cast<uint32_t>(i * 31 + 7);
        i32_buf[i] = is_zero ? 0 : static_cast<int32_t>((i * 31) - 1000);
    }

    const std::vector<size_t> test_sizes = {0, 1, 15, 16, 31, 32, 63, 64, 127, 128, 255, 256, 511, 512, 1024};

    for (size_t offset = 1; offset <= 63; ++offset) {
        for (size_t sz : test_sizes) {
            if (offset + sz > total_elements) {
                continue;
            }

            const uint8_t* u8_ptr = u8_buf + offset;
            const int8_t* i8_ptr = i8_buf + offset;
            const uint32_t* u32_ptr = u32_buf + offset;
            const int32_t* i32_ptr = i32_buf + offset;

            size_t expected_u8 = std::count(u8_ptr, u8_ptr + sz, static_cast<uint8_t>(0));
            size_t expected_i8 = std::count(i8_ptr, i8_ptr + sz, static_cast<int8_t>(0));
            size_t expected_u32 = std::count(u32_ptr, u32_ptr + sz, static_cast<uint32_t>(0));
            size_t expected_i32 = std::count(i32_ptr, i32_ptr + sz, static_cast<int32_t>(0));

            EXPECT_EQ(expected_u8, SIMD::count_zero(u8_ptr, sz));
            EXPECT_EQ(expected_i8, SIMD::count_zero(i8_ptr, sz));
            EXPECT_EQ(expected_u32, SIMD::count_zero(u32_ptr, sz));
            EXPECT_EQ(expected_i32, SIMD::count_zero(i32_ptr, sz));

            EXPECT_EQ(sz - expected_u8, SIMD::count_nonzero(u8_ptr, sz));
            EXPECT_EQ(sz - expected_i8, SIMD::count_nonzero(i8_ptr, sz));
            EXPECT_EQ(sz - expected_u32, SIMD::count_nonzero(u32_ptr, sz));
            EXPECT_EQ(sz - expected_i32, SIMD::count_nonzero(i32_ptr, sz));
        }
    }
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

TEST_F(SIMDTest, contain_zero) {
    // 1. Empty container and empty raw-pointer
    {
        std::vector<uint8_t> default_empty;
        ASSERT_EQ(nullptr, default_empty.data());
        EXPECT_FALSE(SIMD::contain_zero(default_empty));
        EXPECT_FALSE(SIMD::contain_zero(static_cast<const uint8_t*>(nullptr), 0));
        EXPECT_FALSE(SIMD::contain_zero(static_cast<const int8_t*>(nullptr), 0));
        EXPECT_TRUE(SIMD::all_ones(default_empty));
        EXPECT_TRUE(SIMD::all_ones(static_cast<const uint8_t*>(nullptr), 0));
        EXPECT_TRUE(SIMD::all_ones(static_cast<const int8_t*>(nullptr), 0));

        std::vector<uint8_t> u8_empty;
        EXPECT_FALSE(SIMD::contain_zero(u8_empty));
        EXPECT_FALSE(SIMD::contain_zero(u8_empty.data(), u8_empty.size()));
        EXPECT_EQ(SIMD::contain_zero(u8_empty), !SIMD::all_ones(u8_empty));
        EXPECT_EQ(SIMD::contain_zero(u8_empty), SIMD::count_zero(u8_empty) > 0);

        std::vector<int8_t> i8_empty;
        EXPECT_FALSE(SIMD::contain_zero(i8_empty));
        EXPECT_FALSE(SIMD::contain_zero(i8_empty.data(), i8_empty.size()));
        EXPECT_EQ(SIMD::contain_zero(i8_empty), !SIMD::all_ones(i8_empty));
        EXPECT_EQ(SIMD::contain_zero(i8_empty), SIMD::count_zero(i8_empty) > 0);
    }

    // 2. All non-zero / ones (returns false)
    {
        std::vector<uint8_t> u8_ones = {1, 2, 3, 4, 0xFF};
        EXPECT_FALSE(SIMD::contain_zero(u8_ones));
        EXPECT_FALSE(SIMD::contain_zero(u8_ones.data(), u8_ones.size()));
        EXPECT_EQ(SIMD::contain_zero(u8_ones), !SIMD::all_ones(u8_ones));
        EXPECT_EQ(SIMD::contain_zero(u8_ones), SIMD::count_zero(u8_ones) > 0);

        std::vector<int8_t> i8_ones = {1, 2, -1, -128, 127};
        EXPECT_FALSE(SIMD::contain_zero(i8_ones));
        EXPECT_FALSE(SIMD::contain_zero(i8_ones.data(), i8_ones.size()));
        EXPECT_EQ(SIMD::contain_zero(i8_ones), !SIMD::all_ones(i8_ones));
        EXPECT_EQ(SIMD::contain_zero(i8_ones), SIMD::count_zero(i8_ones) > 0);

        std::vector<uint8_t> u8_large_ones(1024, 0xAB);
        EXPECT_FALSE(SIMD::contain_zero(u8_large_ones));
        EXPECT_FALSE(SIMD::contain_zero(u8_large_ones.data(), u8_large_ones.size()));
        EXPECT_EQ(SIMD::contain_zero(u8_large_ones), !SIMD::all_ones(u8_large_ones));
        EXPECT_EQ(SIMD::contain_zero(u8_large_ones), SIMD::count_zero(u8_large_ones) > 0);

        std::vector<int8_t> i8_large_ones(1024, -5);
        EXPECT_FALSE(SIMD::contain_zero(i8_large_ones));
        EXPECT_FALSE(SIMD::contain_zero(i8_large_ones.data(), i8_large_ones.size()));
        EXPECT_EQ(SIMD::contain_zero(i8_large_ones), !SIMD::all_ones(i8_large_ones));
        EXPECT_EQ(SIMD::contain_zero(i8_large_ones), SIMD::count_zero(i8_large_ones) > 0);
    }

    // 3. All zeros (returns true)
    {
        std::vector<uint8_t> u8_zeros = {0, 0, 0, 0};
        EXPECT_TRUE(SIMD::contain_zero(u8_zeros));
        EXPECT_TRUE(SIMD::contain_zero(u8_zeros.data(), u8_zeros.size()));
        EXPECT_EQ(SIMD::contain_zero(u8_zeros), !SIMD::all_ones(u8_zeros));
        EXPECT_EQ(SIMD::contain_zero(u8_zeros), SIMD::count_zero(u8_zeros) > 0);

        std::vector<int8_t> i8_zeros = {0, 0, 0, 0};
        EXPECT_TRUE(SIMD::contain_zero(i8_zeros));
        EXPECT_TRUE(SIMD::contain_zero(i8_zeros.data(), i8_zeros.size()));
        EXPECT_EQ(SIMD::contain_zero(i8_zeros), !SIMD::all_ones(i8_zeros));
        EXPECT_EQ(SIMD::contain_zero(i8_zeros), SIMD::count_zero(i8_zeros) > 0);

        std::vector<uint8_t> u8_large_zeros(1024, 0);
        EXPECT_TRUE(SIMD::contain_zero(u8_large_zeros));
        EXPECT_TRUE(SIMD::contain_zero(u8_large_zeros.data(), u8_large_zeros.size()));
        EXPECT_EQ(SIMD::contain_zero(u8_large_zeros), !SIMD::all_ones(u8_large_zeros));
        EXPECT_EQ(SIMD::contain_zero(u8_large_zeros), SIMD::count_zero(u8_large_zeros) > 0);

        std::vector<int8_t> i8_large_zeros(1024, 0);
        EXPECT_TRUE(SIMD::contain_zero(i8_large_zeros));
        EXPECT_TRUE(SIMD::contain_zero(i8_large_zeros.data(), i8_large_zeros.size()));
        EXPECT_EQ(SIMD::contain_zero(i8_large_zeros), !SIMD::all_ones(i8_large_zeros));
        EXPECT_EQ(SIMD::contain_zero(i8_large_zeros), SIMD::count_zero(i8_large_zeros) > 0);
    }

    // 4. Zero at front
    {
        std::vector<uint8_t> u8_front = {0, 1, 2, 3};
        EXPECT_TRUE(SIMD::contain_zero(u8_front));
        EXPECT_TRUE(SIMD::contain_zero(u8_front.data(), u8_front.size()));
        EXPECT_EQ(SIMD::contain_zero(u8_front), !SIMD::all_ones(u8_front));
        EXPECT_EQ(SIMD::contain_zero(u8_front), SIMD::count_zero(u8_front) > 0);

        std::vector<int8_t> i8_front = {0, -1, 2, -3};
        EXPECT_TRUE(SIMD::contain_zero(i8_front));
        EXPECT_TRUE(SIMD::contain_zero(i8_front.data(), i8_front.size()));
        EXPECT_EQ(SIMD::contain_zero(i8_front), !SIMD::all_ones(i8_front));
        EXPECT_EQ(SIMD::contain_zero(i8_front), SIMD::count_zero(i8_front) > 0);

        std::vector<uint8_t> u8_large(1024, 1);
        u8_large[0] = 0;
        EXPECT_TRUE(SIMD::contain_zero(u8_large));
        EXPECT_TRUE(SIMD::contain_zero(u8_large.data(), u8_large.size()));
        EXPECT_EQ(SIMD::contain_zero(u8_large), !SIMD::all_ones(u8_large));
        EXPECT_EQ(SIMD::contain_zero(u8_large), SIMD::count_zero(u8_large) > 0);

        std::vector<int8_t> i8_large(1024, 1);
        i8_large[0] = 0;
        EXPECT_TRUE(SIMD::contain_zero(i8_large));
        EXPECT_TRUE(SIMD::contain_zero(i8_large.data(), i8_large.size()));
        EXPECT_EQ(SIMD::contain_zero(i8_large), !SIMD::all_ones(i8_large));
        EXPECT_EQ(SIMD::contain_zero(i8_large), SIMD::count_zero(i8_large) > 0);
    }

    // 5. Zero in middle
    {
        std::vector<uint8_t> u8_mid = {1, 2, 0, 3, 4};
        EXPECT_TRUE(SIMD::contain_zero(u8_mid));
        EXPECT_TRUE(SIMD::contain_zero(u8_mid.data(), u8_mid.size()));
        EXPECT_EQ(SIMD::contain_zero(u8_mid), !SIMD::all_ones(u8_mid));
        EXPECT_EQ(SIMD::contain_zero(u8_mid), SIMD::count_zero(u8_mid) > 0);

        std::vector<int8_t> i8_mid = {1, -2, 0, -3, 4};
        EXPECT_TRUE(SIMD::contain_zero(i8_mid));
        EXPECT_TRUE(SIMD::contain_zero(i8_mid.data(), i8_mid.size()));
        EXPECT_EQ(SIMD::contain_zero(i8_mid), !SIMD::all_ones(i8_mid));
        EXPECT_EQ(SIMD::contain_zero(i8_mid), SIMD::count_zero(i8_mid) > 0);

        std::vector<uint8_t> u8_large(1024, 1);
        u8_large[512] = 0;
        EXPECT_TRUE(SIMD::contain_zero(u8_large));
        EXPECT_TRUE(SIMD::contain_zero(u8_large.data(), u8_large.size()));
        EXPECT_EQ(SIMD::contain_zero(u8_large), !SIMD::all_ones(u8_large));
        EXPECT_EQ(SIMD::contain_zero(u8_large), SIMD::count_zero(u8_large) > 0);

        std::vector<int8_t> i8_large(1024, 1);
        i8_large[512] = 0;
        EXPECT_TRUE(SIMD::contain_zero(i8_large));
        EXPECT_TRUE(SIMD::contain_zero(i8_large.data(), i8_large.size()));
        EXPECT_EQ(SIMD::contain_zero(i8_large), !SIMD::all_ones(i8_large));
        EXPECT_EQ(SIMD::contain_zero(i8_large), SIMD::count_zero(i8_large) > 0);
    }

    // 6. Zero at end
    {
        std::vector<uint8_t> u8_end = {1, 2, 3, 0};
        EXPECT_TRUE(SIMD::contain_zero(u8_end));
        EXPECT_TRUE(SIMD::contain_zero(u8_end.data(), u8_end.size()));
        EXPECT_EQ(SIMD::contain_zero(u8_end), !SIMD::all_ones(u8_end));
        EXPECT_EQ(SIMD::contain_zero(u8_end), SIMD::count_zero(u8_end) > 0);

        std::vector<int8_t> i8_end = {1, -2, 3, 0};
        EXPECT_TRUE(SIMD::contain_zero(i8_end));
        EXPECT_TRUE(SIMD::contain_zero(i8_end.data(), i8_end.size()));
        EXPECT_EQ(SIMD::contain_zero(i8_end), !SIMD::all_ones(i8_end));
        EXPECT_EQ(SIMD::contain_zero(i8_end), SIMD::count_zero(i8_end) > 0);

        std::vector<uint8_t> u8_large(1024, 1);
        u8_large.back() = 0;
        EXPECT_TRUE(SIMD::contain_zero(u8_large));
        EXPECT_TRUE(SIMD::contain_zero(u8_large.data(), u8_large.size()));
        EXPECT_EQ(SIMD::contain_zero(u8_large), !SIMD::all_ones(u8_large));
        EXPECT_EQ(SIMD::contain_zero(u8_large), SIMD::count_zero(u8_large) > 0);

        std::vector<int8_t> i8_large(1024, 1);
        i8_large.back() = 0;
        EXPECT_TRUE(SIMD::contain_zero(i8_large));
        EXPECT_TRUE(SIMD::contain_zero(i8_large.data(), i8_large.size()));
        EXPECT_EQ(SIMD::contain_zero(i8_large), !SIMD::all_ones(i8_large));
        EXPECT_EQ(SIMD::contain_zero(i8_large), SIMD::count_zero(i8_large) > 0);
    }

    // 7. Equivalence sweep across sizes and zero positions
    {
        const std::vector<size_t> sizes = {1, 7, 15, 16, 31, 32, 63, 64, 127, 128, 255, 256, 512, 1024};
        for (size_t sz : sizes) {
            std::vector<uint8_t> u8_buf(sz, 1);
            std::vector<int8_t> i8_buf(sz, 1);

            // Initially no zeros
            EXPECT_EQ(SIMD::contain_zero(u8_buf.data(), sz), false);
            EXPECT_EQ(SIMD::contain_zero(i8_buf.data(), sz), false);
            EXPECT_EQ(SIMD::contain_zero(u8_buf.data(), sz), !SIMD::all_ones(u8_buf.data(), sz));
            EXPECT_EQ(SIMD::contain_zero(i8_buf.data(), sz), !SIMD::all_ones(i8_buf.data(), sz));
            EXPECT_EQ(SIMD::contain_zero(u8_buf.data(), sz), SIMD::count_zero(u8_buf.data(), sz) > 0);
            EXPECT_EQ(SIMD::contain_zero(i8_buf.data(), sz), SIMD::count_zero(i8_buf.data(), sz) > 0);

            // Place zero at various positions (start, quarter, half, 3/4, end)
            const std::vector<size_t> positions = {0, sz / 4, sz / 2, (3 * sz) / 4, sz - 1};
            for (size_t pos : positions) {
                u8_buf[pos] = 0;
                i8_buf[pos] = 0;

                EXPECT_EQ(SIMD::contain_zero(u8_buf.data(), sz), true);
                EXPECT_EQ(SIMD::contain_zero(i8_buf.data(), sz), true);
                EXPECT_EQ(SIMD::contain_zero(u8_buf), true);
                EXPECT_EQ(SIMD::contain_zero(i8_buf), true);

                EXPECT_EQ(SIMD::contain_zero(u8_buf.data(), sz), !SIMD::all_ones(u8_buf.data(), sz));
                EXPECT_EQ(SIMD::contain_zero(i8_buf.data(), sz), !SIMD::all_ones(i8_buf.data(), sz));
                EXPECT_EQ(SIMD::contain_zero(u8_buf.data(), sz), SIMD::count_zero(u8_buf.data(), sz) > 0);
                EXPECT_EQ(SIMD::contain_zero(i8_buf.data(), sz), SIMD::count_zero(i8_buf.data(), sz) > 0);

                u8_buf[pos] = 1;
                i8_buf[pos] = 1;
            }
        }
    }
}

TEST_F(SIMDTest, CountZero8BitTailAccumulatorBoundaries) {
    // Exhaustively test every size from 0 to 520, covering all combinations of 256B, 64B, 16B, and scalar tails
    for (size_t sz = 0; sz <= 520; ++sz) {
        std::vector<uint8_t> all_zero(sz, 0);
        EXPECT_EQ(sz, SIMD::count_zero(all_zero.data(), sz)) << "all_zero failed at sz=" << sz;

        std::vector<uint8_t> no_zero(sz, 1);
        EXPECT_EQ(0u, SIMD::count_zero(no_zero.data(), sz)) << "no_zero failed at sz=" << sz;

        std::vector<uint8_t> alternating(sz);
        size_t expected = 0;
        for (size_t i = 0; i < sz; ++i) {
            alternating[i] = (i % 3 == 0) ? 0 : static_cast<uint8_t>((i % 255) + 1);
            if (alternating[i] == 0) ++expected;
        }
        EXPECT_EQ(expected, SIMD::count_zero(alternating.data(), sz)) << "alternating failed at sz=" << sz;
    }
}

TEST_F(SIMDTest, streaming_agg_three_way_dispatch_equivalence) {
    auto pre_pr_count_zero_u8 = [](const uint8_t* data, size_t size) -> size_t {
        size_t count = 0;
        const uint8_t* end = data + size;
#if defined(__SSE2__) && defined(__POPCNT__)
        const __m128i zero16 = _mm_setzero_si128();
        const uint8_t* end64 = data + (size / 64 * 64);
        for (; data < end64; data += 64) {
            count += __builtin_popcountll(
                    static_cast<uint64_t>(_mm_movemask_epi8(
                            _mm_cmpeq_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i*>(data)), zero16))) |
                    (static_cast<uint64_t>(_mm_movemask_epi8(
                             _mm_cmpeq_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i*>(data + 16)), zero16)))
                     << 16u) |
                    (static_cast<uint64_t>(_mm_movemask_epi8(
                             _mm_cmpeq_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i*>(data + 32)), zero16)))
                     << 32u) |
                    (static_cast<uint64_t>(_mm_movemask_epi8(
                             _mm_cmpeq_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i*>(data + 48)), zero16)))
                     << 48u));
        }
#elif defined(__ARM_NEON) && defined(__aarch64__)
        const uint8_t* end16 = data + (size / 16 * 16);
        for (; data < end16; data += 16) {
            uint8x16_t vdata = vld1q_u8(data);
            uint8x16_t result = vceqq_u8(vdata, vdupq_n_u8(0));
            result = vandq_u8(result, vdupq_n_u8(1));
            count += vaddvq_u8(result);
        }
#endif
        for (; data < end; ++data) {
            count += (*data == 0);
        }
        return count;
    };

    auto fill_streaming_selection = [](std::vector<uint8_t>& sel, size_t n, int hit_pct, int dist_mode) {
        sel.resize(n);
        if (hit_pct == 0) {
            std::fill(sel.begin(), sel.end(), 1);
            return;
        }
        if (hit_pct == 100) {
            std::fill(sel.begin(), sel.end(), 0);
            return;
        }
        if (dist_mode == 0) {
            std::mt19937_64 rng(0x5E1EC700);
            std::uniform_int_distribution<int> d(0, 99);
            for (size_t i = 0; i < n; ++i) {
                sel[i] = (d(rng) < hit_pct) ? 0 : 1;
            }
        } else if (dist_mode == 1) {
            size_t k = (n * static_cast<size_t>(hit_pct)) / 100;
            std::fill(sel.begin(), sel.begin() + k, 0);
            std::fill(sel.begin() + k, sel.end(), 1);
        } else {
            size_t k = (n * static_cast<size_t>(hit_pct)) / 100;
            std::fill(sel.begin(), sel.begin() + (n - k), 1);
            std::fill(sel.begin() + (n - k), sel.end(), 0);
        }
    };

    constexpr size_t n = 4096;
    const std::vector<int> hit_pcts = {0, 1, 10, 25, 50, 75, 95, 100};
    const std::vector<int> dist_modes = {0, 1, 2};
    std::vector<uint8_t> sel;

    for (int dist_mode : dist_modes) {
        for (int hit_pct : hit_pcts) {
            fill_streaming_selection(sel, n, hit_pct, dist_mode);

            const size_t pre_pr_zero_count = pre_pr_count_zero_u8(sel.data(), n);
            const size_t future_zero_count = SIMD::count_zero(sel);
            EXPECT_EQ(pre_pr_zero_count, future_zero_count) << "dist_mode=" << dist_mode << " hit_pct=" << hit_pct;

            const int decision_pre_pr = (pre_pr_zero_count == 0) ? 0 : (pre_pr_zero_count == n ? 1 : 2);
            const int decision_current_pr = !SIMD::contain_zero(sel) ? 0 : (SIMD::all_zeros(sel) ? 1 : 2);
            const int decision_future = (future_zero_count == 0) ? 0 : (future_zero_count == n ? 1 : 2);

            EXPECT_EQ(decision_pre_pr, decision_current_pr) << "dist_mode=" << dist_mode << " hit_pct=" << hit_pct;
            EXPECT_EQ(decision_pre_pr, decision_future) << "dist_mode=" << dist_mode << " hit_pct=" << hit_pct;

            // Verify ADJUST-stage decision equivalence when Future reuses hit_count directly
            auto adjust_pre_pr = [&]() -> int {
                size_t hit_count = pre_pr_count_zero_u8(sel.data(), n);
                if (hit_count * 5 <= n) return 10;
                if (hit_count * 10 >= n * 9) return 11;
                size_t zc = pre_pr_count_zero_u8(sel.data(), n);
                return (zc == 0) ? 0 : (zc == n ? 1 : 2);
            };
            auto adjust_current_pr = [&]() -> int {
                size_t hit_count = SIMD::count_zero(sel);
                if (hit_count * 5 <= n) return 10;
                if (hit_count * 10 >= n * 9) return 11;
                return !SIMD::contain_zero(sel) ? 0 : (SIMD::all_zeros(sel) ? 1 : 2);
            };
            auto adjust_future = [&]() -> int {
                size_t hit_count = SIMD::count_zero(sel);
                if (hit_count * 5 <= n) return 10;
                if (hit_count * 10 >= n * 9) return 11;
                return (hit_count == 0) ? 0 : (hit_count == n ? 1 : 2);
            };

            EXPECT_EQ(adjust_pre_pr(), adjust_current_pr()) << "dist_mode=" << dist_mode << " hit_pct=" << hit_pct;
            EXPECT_EQ(adjust_pre_pr(), adjust_future()) << "dist_mode=" << dist_mode << " hit_pct=" << hit_pct;
        }
    }
}

} // namespace starrocks
