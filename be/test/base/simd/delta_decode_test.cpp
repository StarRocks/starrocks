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

#include "base/simd/delta_decode.h"

#include <gtest/gtest.h>

#include <limits>
#include <vector>

namespace starrocks {
class DeltaDecodeTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

// The ISA-specific kernels now live in their own translation units and are always compiled (with
// per-file -m flags), so the guards below are *runtime* __builtin_cpu_supports checks rather than
// compile-time #ifdefs: this exercises the AVX-512 kernel on a capable CPU even when the binary was
// built with USE_AVX512=OFF, which is exactly the property the split is meant to provide. The
// AVX-512 kernels are skipped under ASan (known zmm-spill redzone trap), matching the dispatcher.

TEST_F(DeltaDecodeTest, test_int32) {
    std::vector<int32_t> values(233);
    for (size_t i = 0; i < values.size(); i++) {
        values[i] = 1;
    }

    // scalar reference
    std::vector<int32_t> expected(values);
    int32_t expected_last = 10;
    delta_decode_chain_scalar_prefetch<int32_t>(expected.data(), expected.size(), 10, expected_last);
    ASSERT_EQ(expected.back(), expected_last);

    // public dispatcher must match on any CPU
    {
        std::vector<int32_t> v(values);
        int32_t last_value = 10;
        delta_decode_chain_int32(v.data(), v.size(), 10, last_value);
        ASSERT_EQ(v, expected);
        ASSERT_EQ(v.back(), last_value);
    }

    // scalar entry point
    {
        std::vector<int32_t> v(values);
        int32_t last_value = 10;
        delta_decode_chain_int32_scalar(v.data(), v.size(), 10, last_value);
        ASSERT_EQ(v, expected);
        ASSERT_EQ(v.back(), last_value);
    }

#if defined(__x86_64__)
    if (__builtin_cpu_supports("avx2")) {
        std::vector<int32_t> v(values);
        int32_t last_value = 10;
        delta_decode_chain_int32_avx2(v.data(), v.size(), 10, last_value);
        ASSERT_EQ(v, expected);
        ASSERT_EQ(v.back(), last_value);
    }
#if !defined(ADDRESS_SANITIZER)
    if (__builtin_cpu_supports("avx512f") && __builtin_cpu_supports("avx512vl")) {
        std::vector<int32_t> v(values);
        int32_t last_value = 10;
        delta_decode_chain_int32_avx2x(v.data(), v.size(), 10, last_value);
        ASSERT_EQ(v, expected);
        ASSERT_EQ(v.back(), last_value);
    }
    if (__builtin_cpu_supports("avx512f")) {
        std::vector<int32_t> v(values);
        int32_t last_value = 10;
        delta_decode_chain_int32_avx512(v.data(), v.size(), 10, last_value);
        ASSERT_EQ(v, expected);
        ASSERT_EQ(v.back(), last_value);
    }
#endif
#endif
}

TEST_F(DeltaDecodeTest, test_int64) {
    std::vector<int64_t> values(223);
    for (size_t i = 0; i < values.size(); i++) {
        values[i] = 1;
    }

    // scalar reference
    std::vector<int64_t> expected(values);
    int64_t expected_last = 10;
    delta_decode_chain_scalar_prefetch<int64_t>(expected.data(), expected.size(), 10, expected_last);
    ASSERT_EQ(expected.back(), expected_last);

    // public dispatcher must match on any CPU
    {
        std::vector<int64_t> v(values);
        int64_t last_value = 10;
        delta_decode_chain_int64(v.data(), v.size(), 10, last_value);
        ASSERT_EQ(v, expected);
        ASSERT_EQ(v.back(), last_value);
    }

    // scalar entry point
    {
        std::vector<int64_t> v(values);
        int64_t last_value = 10;
        delta_decode_chain_int64_scalar(v.data(), v.size(), 10, last_value);
        ASSERT_EQ(v, expected);
        ASSERT_EQ(v.back(), last_value);
    }

#if defined(__x86_64__) && !defined(ADDRESS_SANITIZER)
    if (__builtin_cpu_supports("avx512f")) {
        std::vector<int64_t> v(values);
        int64_t last_value = 10;
        delta_decode_chain_int64_avx512(v.data(), v.size(), 10, last_value);
        ASSERT_EQ(v, expected);
        ASSERT_EQ(v.back(), last_value);
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
        delta_decode_chain_int32_scalar(v.data(), v.size(), min_delta, last_val);
        ASSERT_EQ(v, expected);
        ASSERT_EQ(v.back(), last_val);
    }

#if defined(__x86_64__)
    if (__builtin_cpu_supports("avx2")) {
        std::vector<int32_t> v(values);
        int32_t last_val = start_val;
        delta_decode_chain_int32_avx2(v.data(), v.size(), min_delta, last_val);
        ASSERT_EQ(v, expected);
        ASSERT_EQ(v.back(), last_val);
    }
#if !defined(ADDRESS_SANITIZER)
    if (__builtin_cpu_supports("avx512f") && __builtin_cpu_supports("avx512vl")) {
        std::vector<int32_t> v(values);
        int32_t last_val = start_val;
        delta_decode_chain_int32_avx2x(v.data(), v.size(), min_delta, last_val);
        ASSERT_EQ(v, expected);
        ASSERT_EQ(v.back(), last_val);
    }
    if (__builtin_cpu_supports("avx512f")) {
        std::vector<int32_t> v(values);
        int32_t last_val = start_val;
        delta_decode_chain_int32_avx512(v.data(), v.size(), min_delta, last_val);
        ASSERT_EQ(v, expected);
        ASSERT_EQ(v.back(), last_val);
    }
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
        delta_decode_chain_int64_scalar(v.data(), v.size(), min_delta, last_val);
        ASSERT_EQ(v, expected);
        ASSERT_EQ(v.back(), last_val);
    }

#if defined(__x86_64__) && !defined(ADDRESS_SANITIZER)
    if (__builtin_cpu_supports("avx512f")) {
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
    for (size_t sz : sizes) {
        // int32
        {
            std::vector<int32_t> values(sz);
            for (size_t i = 0; i < sz; i++) {
                values[i] = static_cast<int32_t>(i + 1);
            }
            std::vector<int32_t> expected(values);
            int32_t expected_last = 5;
            delta_decode_chain_scalar_prefetch<int32_t>(expected.data(), expected.size(), 3, expected_last);

            std::vector<int32_t> actual(values);
            int32_t actual_last = 5;
            delta_decode_chain_int32_scalar(actual.data(), actual.size(), 3, actual_last);

            ASSERT_EQ(actual, expected);
            ASSERT_EQ(actual_last, expected_last);
        }
        // int64
        {
            std::vector<int64_t> values(sz);
            for (size_t i = 0; i < sz; i++) {
                values[i] = static_cast<int64_t>(i + 1);
            }
            std::vector<int64_t> expected(values);
            int64_t expected_last = 5;
            delta_decode_chain_scalar_prefetch<int64_t>(expected.data(), expected.size(), 3, expected_last);

            std::vector<int64_t> actual(values);
            int64_t actual_last = 5;
            delta_decode_chain_int64_scalar(actual.data(), actual.size(), 3, actual_last);

            ASSERT_EQ(actual, expected);
            ASSERT_EQ(actual_last, expected_last);
        }
    }
}

TEST_F(DeltaDecodeTest, test_contains_negative_value) {
    const size_t sizes[] = {0, 1, 7, 8, 9, 15, 16, 23, 31, 32, 65, 128};
    for (size_t sz : sizes) {
        std::vector<int32_t> data(sz, 0);
        for (size_t i = 0; i < sz; ++i) {
            data[i] = static_cast<int32_t>(i + 1);
        }
        EXPECT_FALSE(contains_negative_value(data.data(), sz));

        if (sz > 0) {
            // First element negative
            data[0] = -1;
            EXPECT_TRUE(contains_negative_value(data.data(), sz));
            data[0] = 1;

            // Last element negative
            data[sz - 1] = -42;
            EXPECT_TRUE(contains_negative_value(data.data(), sz));
            data[sz - 1] = static_cast<int32_t>(sz);

            // Middle element negative
            size_t mid = sz / 2;
            data[mid] = -100;
            EXPECT_TRUE(contains_negative_value(data.data(), sz));
            data[mid] = static_cast<int32_t>(mid + 1);
        }
    }
}

} // namespace starrocks
