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

} // namespace starrocks
