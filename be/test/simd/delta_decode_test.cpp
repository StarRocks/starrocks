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

#include "simd/delta_decode.h"

#include <gtest/gtest.h>

#include <climits>
#include <limits>
#include <vector>

namespace starrocks {
class DeltaDecodeTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(DeltaDecodeTest, test_int32) {
    std::vector<int32_t> values(233);
    for (int i = 0; i < values.size(); i++) {
        values[i] = 1;
    }
    [[maybe_unused]] std::vector<int32_t> avx2_values(values);
    [[maybe_unused]] std::vector<int32_t> avx2x_values(values);
    [[maybe_unused]] std::vector<int32_t> avx512_values(values);

    {
        int32_t last_value = 10;
        delta_decode_chain_scalar_prefetch<int32_t>(values.data(), values.size(), 10, last_value);
        ASSERT_EQ(values.back(), last_value);
    }
#ifdef __AVX2__
    {
        int32_t last_value = 10;
        delta_decode_chain_int32_avx2(avx2_values.data(), avx2_values.size(), 10, last_value);
        ASSERT_EQ(avx2_values.back(), last_value);
        ASSERT_EQ(avx2_values, values);
    }
#endif

#if defined(__AVX512F__) && defined(__AVX512VL__)
    {
        int32_t last_value = 10;
        delta_decode_chain_int32_avx2x(avx2x_values.data(), avx2x_values.size(), 10, last_value);
        ASSERT_EQ(avx2x_values.back(), last_value);
        ASSERT_EQ(avx2x_values, values);
    }
#endif

#ifdef __AVX512F__
    {
        int32_t last_value = 10;
        delta_decode_chain_int32_avx512(avx512_values.data(), avx512_values.size(), 10, last_value);
        ASSERT_EQ(avx512_values.back(), last_value);
        ASSERT_EQ(avx512_values, values);
    }
#endif
}

TEST_F(DeltaDecodeTest, test_int64) {
    std::vector<int64_t> values(223);
    for (int i = 0; i < values.size(); i++) {
        values[i] = 1;
    }
    [[maybe_unused]] std::vector<int64_t> avx512_values(values);
    {
        int64_t last_value = 10;
        delta_decode_chain_scalar_prefetch<int64_t>(values.data(), values.size(), 10, last_value);
        ASSERT_EQ(values.back(), last_value);
    }
#ifdef __AVX512F__
    {
        int64_t last_value = 10;
        delta_decode_chain_int64_avx512(avx512_values.data(), avx512_values.size(), 10, last_value);
        ASSERT_EQ(avx512_values.back(), last_value);
        ASSERT_EQ(avx512_values, values);
    }
#endif
}

TEST_F(DeltaDecodeTest, test_int32_wrapping_overflow) {
    // Deliberately test values that wrap across INT32_MAX and INT32_MIN.
    // In standard C++, signed overflow is UB unless unsigned cast is used.
    // Under UBSan (-fsanitize=undefined), this verifies zero traps occur.
    const size_t sz = 137;
    std::vector<int32_t> values(sz);
    int32_t start_val = std::numeric_limits<int32_t>::max() - 50;
    int32_t min_delta = 100;
    for (size_t i = 0; i < sz; i++) {
        values[i] = static_cast<int32_t>(i * 1000);
    }
    // Force a signed underflow in the scalar tail (index 136) as well as the SIMD loop (index 0).
    values[sz - 1] = std::numeric_limits<int32_t>::min();

    std::vector<int32_t> expected(values);
    int32_t expected_last = start_val;
    delta_decode_chain_scalar_prefetch<int32_t>(expected.data(), expected.size(), min_delta, expected_last);
    ASSERT_EQ(expected.back(), expected_last);

    {
        std::vector<int32_t> v(values);
        int32_t last_val = start_val;
        delta_decode_chain_int32(v.data(), v.size(), min_delta, last_val);
        ASSERT_EQ(v, expected);
        ASSERT_EQ(v.back(), last_val);
    }

    {
        std::vector<int32_t> v(values);
        int32_t last_val = start_val;
        delta_decode_chain_scalar_prefetch<int32_t>(v.data(), v.size(), min_delta, last_val);
        ASSERT_EQ(v, expected);
        ASSERT_EQ(v.back(), last_val);
    }

#if defined(__AVX2__)
    {
        std::vector<int32_t> v(values);
        int32_t last_val = start_val;
        delta_decode_chain_int32_avx2(v.data(), v.size(), min_delta, last_val);
        ASSERT_EQ(v, expected);
        ASSERT_EQ(v.back(), last_val);
    }
#if !defined(ADDRESS_SANITIZER)
#if defined(__AVX512F__) && defined(__AVX512VL__)
    {
        std::vector<int32_t> v(values);
        int32_t last_val = start_val;
        delta_decode_chain_int32_avx2x(v.data(), v.size(), min_delta, last_val);
        ASSERT_EQ(v, expected);
        ASSERT_EQ(v.back(), last_val);
    }
#endif
#if defined(__AVX512F__)
    {
        std::vector<int32_t> v(values);
        int32_t last_val = start_val;
        delta_decode_chain_int32_avx512(v.data(), v.size(), min_delta, last_val);
        ASSERT_EQ(v, expected);
        ASSERT_EQ(v.back(), last_val);
    }
#endif
#endif
#endif
}

TEST_F(DeltaDecodeTest, test_int64_wrapping_overflow) {
    const size_t sz = 137;
    std::vector<int64_t> values(sz);
    int64_t start_val = std::numeric_limits<int64_t>::max() - 50;
    int64_t min_delta = 100;
    for (size_t i = 0; i < sz; i++) {
        values[i] = static_cast<int64_t>(i * 1000);
    }
    // Force a signed underflow in the scalar tail (index 136) as well as the SIMD loop (index 0).
    values[sz - 1] = std::numeric_limits<int64_t>::min();

    std::vector<int64_t> expected(values);
    int64_t expected_last = start_val;
    delta_decode_chain_scalar_prefetch<int64_t>(expected.data(), expected.size(), min_delta, expected_last);
    ASSERT_EQ(expected.back(), expected_last);

    {
        std::vector<int64_t> v(values);
        int64_t last_val = start_val;
        delta_decode_chain_int64(v.data(), v.size(), min_delta, last_val);
        ASSERT_EQ(v, expected);
        ASSERT_EQ(v.back(), last_val);
    }

    {
        std::vector<int64_t> v(values);
        int64_t last_val = start_val;
        delta_decode_chain_scalar_prefetch<int64_t>(v.data(), v.size(), min_delta, last_val);
        ASSERT_EQ(v, expected);
        ASSERT_EQ(v.back(), last_val);
    }

#if defined(__AVX512F__) && !defined(ADDRESS_SANITIZER)
    {
        std::vector<int64_t> v(values);
        int64_t last_val = start_val;
        delta_decode_chain_int64_avx512(v.data(), v.size(), min_delta, last_val);
        ASSERT_EQ(v, expected);
        ASSERT_EQ(v.back(), last_val);
    }
#endif
}

TEST_F(DeltaDecodeTest, test_scalar_various_sizes) {
    const std::vector<size_t> sizes = {0, 1, 2, 7, 8, 9, 15, 16, 23, 24, 31, 32, 64, 127};
    auto check = [&](auto initial_value) {
        using T = decltype(initial_value);
        for (size_t sz : sizes) {
            std::vector<T> actual(sz);
            std::vector<T> expected(sz);
            T expected_last = initial_value;
            for (size_t i = 0; i < sz; ++i) {
                actual[i] = static_cast<T>(i + 1);
                expected_last += static_cast<T>(i + 1) + 3;
                expected[i] = expected_last;
            }
            T actual_last = initial_value;
            delta_decode_chain_scalar_prefetch<T>(actual.data(), actual.size(), 3, actual_last);
            ASSERT_EQ(actual, expected);
            ASSERT_EQ(actual_last, expected_last);
        }
    };
    check(int32_t{5});
    check(int64_t{5});
}

TEST_F(DeltaDecodeTest, test_contains_negative_value) {
    const std::vector<size_t> sizes = {0, 1, 7, 8, 9, 15, 16, 23, 31, 32, 65, 128};
    for (size_t sz : sizes) {
        std::vector<int32_t> data(sz, 0);
        for (size_t i = 0; i < sz; ++i) {
            data[i] = (i % 2 == 0) ? 0 : ((i % 3 == 0) ? INT32_MAX : static_cast<int32_t>(i));
        }
        EXPECT_FALSE(contains_negative_value(data.data(), sz)) << "sz=" << sz;

        for (size_t pos = 0; pos < sz; ++pos) {
            int32_t saved = data[pos];
            data[pos] = -1;
            EXPECT_TRUE(contains_negative_value(data.data(), sz)) << "sz=" << sz << " pos=" << pos;
            data[pos] = INT32_MIN;
            EXPECT_TRUE(contains_negative_value(data.data(), sz)) << "sz=" << sz << " pos=" << pos;
            data[pos] = saved;
        }
    }
}

} // namespace starrocks
