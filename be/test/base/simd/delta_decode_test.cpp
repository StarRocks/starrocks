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

#include <climits>
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

#if defined(__ARM_NEON) && defined(__aarch64__)
    {
        std::vector<int32_t> v(values);
        int32_t last_value = 10;
        delta_decode_chain_int32_neon(v.data(), v.size(), 10, last_value);
        ASSERT_EQ(v, expected);
        ASSERT_EQ(v.back(), last_value);
    }
#endif

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

#if defined(__ARM_NEON) && defined(__aarch64__)
    {
        std::vector<int64_t> v(values);
        int64_t last_value = 10;
        delta_decode_chain_int64_neon(v.data(), v.size(), 10, last_value);
        ASSERT_EQ(v, expected);
        ASSERT_EQ(v.back(), last_value);
    }
#endif

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

TEST_F(DeltaDecodeTest, test_boundary_sizes_and_negative_deltas) {
    const std::vector<int> sizes = {0, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 63, 64, 65};
    for (int n : sizes) {
        std::vector<int32_t> base32(n);
        std::vector<int64_t> base64(n);
        for (int i = 0; i < n; ++i) {
            base32[i] = (i * 7) % 13;
            base64[i] = (i * 11) % 19;
        }

        for (int32_t min_delta : {0, 5, -10}) {
            std::vector<int32_t> expected(base32);
            int32_t expected_last = -500;
            delta_decode_chain_scalar_prefetch<int32_t>(expected.data(), n, min_delta, expected_last);

            std::vector<int32_t> actual(base32);
            int32_t actual_last = -500;
            delta_decode_chain_int32(actual.data(), n, min_delta, actual_last);
            ASSERT_EQ(actual, expected) << "n=" << n << " min_delta=" << min_delta;
            ASSERT_EQ(actual_last, expected_last) << "n=" << n << " min_delta=" << min_delta;

#if defined(__ARM_NEON) && defined(__aarch64__)
            std::vector<int32_t> neon_actual(base32);
            int32_t neon_last = -500;
            delta_decode_chain_int32_neon(neon_actual.data(), n, min_delta, neon_last);
            ASSERT_EQ(neon_actual, expected) << "n=" << n << " min_delta=" << min_delta;
            ASSERT_EQ(neon_last, expected_last) << "n=" << n << " min_delta=" << min_delta;
#endif
        }

        for (int64_t min_delta : {int64_t{0}, int64_t{5}, int64_t{-10}}) {
            std::vector<int64_t> expected(base64);
            int64_t expected_last = -500;
            delta_decode_chain_scalar_prefetch<int64_t>(expected.data(), n, min_delta, expected_last);

            std::vector<int64_t> actual(base64);
            int64_t actual_last = -500;
            delta_decode_chain_int64(actual.data(), n, min_delta, actual_last);
            ASSERT_EQ(actual, expected) << "n=" << n << " min_delta=" << min_delta;
            ASSERT_EQ(actual_last, expected_last) << "n=" << n << " min_delta=" << min_delta;

#if defined(__ARM_NEON) && defined(__aarch64__)
            std::vector<int64_t> neon_actual(base64);
            int64_t neon_last = -500;
            delta_decode_chain_int64_neon(neon_actual.data(), n, min_delta, neon_last);
            ASSERT_EQ(neon_actual, expected) << "n=" << n << " min_delta=" << min_delta;
            ASSERT_EQ(neon_last, expected_last) << "n=" << n << " min_delta=" << min_delta;
#endif
        }
    }
}

TEST_F(DeltaDecodeTest, test_contains_negative_value) {
    const std::vector<size_t> sizes = {0, 1, 7, 8, 9, 15, 16, 23};
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
