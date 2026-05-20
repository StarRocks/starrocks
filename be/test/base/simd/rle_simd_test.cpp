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

#include "base/simd/rle_simd.h"

#include <algorithm>
#include <limits>
#include <random>
#include <vector>

#include "base/simd/simd_utils.h"
#include "base/testutil/parallel_test.h"
#include "gtest/gtest.h"

namespace starrocks {

// Sizes chosen to exercise every code path: empty, < SIMD width, one full vector,
// multiple vectors with unroll, vectors + tail, large enough to defeat unroll.
static const std::vector<int32_t> kRleTestSizes = {0, 1, 3, 7, 8, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 257};

// ----------------------------------------------------------------------------
// simd_fill_int16 / simd_fill_int32 / SIMDUtils::simd_fill<T>
// Contract: dst[i] = value for i in [0, count).
// ----------------------------------------------------------------------------

PARALLEL_TEST(RleSimdTest, simd_fill_int16) {
    for (int32_t n : kRleTestSizes) {
        std::vector<int16_t> dst(static_cast<size_t>(n) + 1, static_cast<int16_t>(-12345));
        dst.back() = 0x4242; // sentinel: must not be overwritten
        const int16_t value = static_cast<int16_t>(0x1234);

        simd_fill_int16(dst.data(), value, n);

        for (int32_t i = 0; i < n; ++i) {
            ASSERT_EQ(dst[i], value) << "n=" << n << " i=" << i;
        }
        ASSERT_EQ(dst.back(), static_cast<int16_t>(0x4242)) << "n=" << n << " sentinel overwritten";
    }
}

PARALLEL_TEST(RleSimdTest, simd_fill_int32) {
    for (int32_t n : kRleTestSizes) {
        std::vector<int32_t> dst(static_cast<size_t>(n) + 1, -1);
        dst.back() = 0xCAFEBABE;
        const int32_t value = 0x12345678;

        simd_fill_int32(dst.data(), value, n);

        for (int32_t i = 0; i < n; ++i) {
            ASSERT_EQ(dst[i], value) << "n=" << n << " i=" << i;
        }
        ASSERT_EQ(dst.back(), static_cast<int32_t>(0xCAFEBABE)) << "n=" << n;
    }
}

template <class T>
static void test_simdutils_fill(T value, T sentinel) {
    for (size_t n :
         {size_t{0}, size_t{1}, size_t{15}, size_t{16}, size_t{31}, size_t{32}, size_t{63}, size_t{64}, size_t{257}}) {
        std::vector<T> dst(n + 1, sentinel);
        SIMDUtils::simd_fill<T>(dst.data(), value, n);
        for (size_t i = 0; i < n; ++i) {
            ASSERT_EQ(dst[i], value) << "n=" << n << " i=" << i << " sizeof(T)=" << sizeof(T);
        }
        ASSERT_EQ(dst.back(), sentinel) << "n=" << n << " sentinel";
    }
}

PARALLEL_TEST(RleSimdTest, simdutils_simd_fill) {
    test_simdutils_fill<int8_t>(0x55, static_cast<int8_t>(-1));
    test_simdutils_fill<int16_t>(0x1234, static_cast<int16_t>(-1));
    test_simdutils_fill<int32_t>(0x12345678, -1);
    test_simdutils_fill<int64_t>(0x123456789ABCDEFLL, -1LL);
    test_simdutils_fill<uint8_t>(0xAB, 0x77);
}

// ----------------------------------------------------------------------------
// simd_minmax_int32
// Contract: out_min = *min_element([data, data+count)),
//           out_max = *max_element([data, data+count)).
// On empty input: out_min = INT32_MAX, out_max = INT32_MIN.
// ----------------------------------------------------------------------------

PARALLEL_TEST(RleSimdTest, simd_minmax_int32) {
    std::mt19937 rng(0xC0FFEE);
    std::uniform_int_distribution<int32_t> dist(std::numeric_limits<int32_t>::min(),
                                                std::numeric_limits<int32_t>::max());

    for (int32_t n : kRleTestSizes) {
        std::vector<int32_t> data(n);
        for (auto& v : data) v = dist(rng);

        int32_t got_min = 0, got_max = 0;
        simd_minmax_int32(data.data(), n, got_min, got_max);

        int32_t expected_min, expected_max;
        if (n == 0) {
            expected_min = std::numeric_limits<int32_t>::max();
            expected_max = std::numeric_limits<int32_t>::min();
        } else {
            expected_min = *std::min_element(data.begin(), data.end());
            expected_max = *std::max_element(data.begin(), data.end());
        }
        ASSERT_EQ(got_min, expected_min) << "n=" << n;
        ASSERT_EQ(got_max, expected_max) << "n=" << n;

        // Cross-check against the default scalar variant.
        int32_t def_min = 0, def_max = 0;
        simd_minmax_int32_default(data.data(), n, def_min, def_max);
        ASSERT_EQ(def_min, expected_min) << "n=" << n;
        ASSERT_EQ(def_max, expected_max) << "n=" << n;
    }

    // Edge: all identical, signed extremes, negatives.
    {
        std::vector<int32_t> data(40, -5);
        int32_t mn, mx;
        simd_minmax_int32(data.data(), data.size(), mn, mx);
        ASSERT_EQ(mn, -5);
        ASSERT_EQ(mx, -5);
    }
    {
        std::vector<int32_t> data = {std::numeric_limits<int32_t>::min(), 0, std::numeric_limits<int32_t>::max()};
        int32_t mn, mx;
        simd_minmax_int32(data.data(), data.size(), mn, mx);
        ASSERT_EQ(mn, std::numeric_limits<int32_t>::min());
        ASSERT_EQ(mx, std::numeric_limits<int32_t>::max());
    }
}

// ----------------------------------------------------------------------------
// simd_dict_gather_{int32,int64,float,double}
// Contract: dest[i] = dict[indices[i]].
// ----------------------------------------------------------------------------

template <class T>
static void test_dict_gather(void (*gather)(T*, const T*, const uint32_t*, int32_t)) {
    constexpr int32_t kDictSize = 137;
    std::mt19937 rng(0xDEADBEEF);
    std::uniform_int_distribution<uint32_t> idx_dist(0, kDictSize - 1);

    std::vector<T> dict(kDictSize);
    for (int32_t i = 0; i < kDictSize; ++i) {
        // Use values that exercise the full bit pattern (signed/unsigned aliasing
        // for float-via-int32 reinterpret).
        dict[i] = static_cast<T>(i * 31 - 1000);
    }

    for (int32_t n : kRleTestSizes) {
        std::vector<uint32_t> indices(n);
        for (auto& v : indices) v = idx_dist(rng);

        std::vector<T> dest(static_cast<size_t>(n) + 1, static_cast<T>(0xAA));
        const T sentinel = dest.back();

        gather(dest.data(), dict.data(), indices.data(), n);

        for (int32_t i = 0; i < n; ++i) {
            ASSERT_EQ(dest[i], dict[indices[i]]) << "n=" << n << " i=" << i;
        }
        ASSERT_EQ(dest.back(), sentinel) << "n=" << n;
    }
}

PARALLEL_TEST(RleSimdTest, simd_dict_gather_int32) {
    test_dict_gather<int32_t>(&simd_dict_gather_int32);
    // Default variant directly.
    int32_t dict[8] = {10, 20, 30, 40, 50, 60, 70, 80};
    uint32_t indices[5] = {7, 0, 3, 5, 2};
    int32_t dest[5] = {0};
    simd_dict_gather_int32_default(dest, dict, indices, 5);
    EXPECT_EQ(dest[0], 80);
    EXPECT_EQ(dest[1], 10);
    EXPECT_EQ(dest[2], 40);
    EXPECT_EQ(dest[3], 60);
    EXPECT_EQ(dest[4], 30);
}

PARALLEL_TEST(RleSimdTest, simd_dict_gather_int64) {
    test_dict_gather<int64_t>(&simd_dict_gather_int64);
    // Default variant directly.
    int64_t dict[8] = {10, 20, 30, 40, 50, 60, 70, 80};
    uint32_t indices[5] = {7, 0, 3, 5, 2};
    int64_t dest[5] = {0};
    simd_dict_gather_int64_default(dest, dict, indices, 5);
    EXPECT_EQ(dest[0], 80);
    EXPECT_EQ(dest[1], 10);
    EXPECT_EQ(dest[2], 40);
    EXPECT_EQ(dest[3], 60);
    EXPECT_EQ(dest[4], 30);
}

PARALLEL_TEST(RleSimdTest, simd_dict_gather_float) {
    constexpr int32_t kDictSize = 64;
    std::vector<float> dict(kDictSize);
    for (int32_t i = 0; i < kDictSize; ++i) dict[i] = static_cast<float>(i) * 0.5f - 1.25f;

    std::mt19937 rng(7);
    std::uniform_int_distribution<uint32_t> idx_dist(0, kDictSize - 1);
    for (int32_t n : kRleTestSizes) {
        std::vector<uint32_t> indices(n);
        for (auto& v : indices) v = idx_dist(rng);
        std::vector<float> dest(static_cast<size_t>(n) + 1, -7.5f);
        simd_dict_gather_float(dest.data(), dict.data(), indices.data(), n);
        for (int32_t i = 0; i < n; ++i) {
            ASSERT_EQ(dest[i], dict[indices[i]]) << "n=" << n << " i=" << i;
        }
        ASSERT_EQ(dest.back(), -7.5f) << "n=" << n;
    }
}

PARALLEL_TEST(RleSimdTest, simd_dict_gather_double) {
    constexpr int32_t kDictSize = 64;
    std::vector<double> dict(kDictSize);
    for (int32_t i = 0; i < kDictSize; ++i) dict[i] = static_cast<double>(i) * 0.25 - 3.14;

    std::mt19937 rng(13);
    std::uniform_int_distribution<uint32_t> idx_dist(0, kDictSize - 1);
    for (int32_t n : kRleTestSizes) {
        std::vector<uint32_t> indices(n);
        for (auto& v : indices) v = idx_dist(rng);
        std::vector<double> dest(static_cast<size_t>(n) + 1, -9.5);
        simd_dict_gather_double(dest.data(), dict.data(), indices.data(), n);
        for (int32_t i = 0; i < n; ++i) {
            ASSERT_EQ(dest[i], dict[indices[i]]) << "n=" << n << " i=" << i;
        }
        ASSERT_EQ(dest.back(), -9.5) << "n=" << n;
    }
}

// ----------------------------------------------------------------------------
// simd_widen_int{8,16}_to_int32
// Contract: dest[i] = static_cast<int32_t>(src[i]) (sign-extending).
// ----------------------------------------------------------------------------

PARALLEL_TEST(RleSimdTest, simd_widen_int8_to_int32) {
    std::mt19937 rng(0xABCD1234);
    std::uniform_int_distribution<int32_t> dist(std::numeric_limits<int8_t>::min(), std::numeric_limits<int8_t>::max());

    for (int32_t n : kRleTestSizes) {
        std::vector<int8_t> src(n);
        for (auto& v : src) v = static_cast<int8_t>(dist(rng));
        std::vector<int32_t> dest(static_cast<size_t>(n) + 1, static_cast<int32_t>(0xDEADBEEFu));

        simd_widen_int8_to_int32(dest.data(), src.data(), n);

        for (int32_t i = 0; i < n; ++i) {
            ASSERT_EQ(dest[i], static_cast<int32_t>(src[i])) << "n=" << n << " i=" << i;
        }
        ASSERT_EQ(dest.back(), static_cast<int32_t>(0xDEADBEEFu)) << "n=" << n;
    }
    // Sign-extension on the extreme negative explicitly.
    {
        int8_t src[8] = {-128, -1, 0, 1, 127, -64, 64, -2};
        int32_t dest[8] = {0};
        simd_widen_int8_to_int32(dest, src, 8);
        EXPECT_EQ(dest[0], -128);
        EXPECT_EQ(dest[1], -1);
        EXPECT_EQ(dest[4], 127);
        EXPECT_EQ(dest[5], -64);
    }
    // Default scalar variant directly.
    {
        int8_t src[8] = {-128, -1, 0, 1, 127, -64, 64, -2};
        int32_t dest[8] = {0};
        simd_widen_int8_to_int32_default(dest, src, 8);
        EXPECT_EQ(dest[0], -128);
        EXPECT_EQ(dest[4], 127);
        EXPECT_EQ(dest[5], -64);
        EXPECT_EQ(dest[7], -2);
    }
}

PARALLEL_TEST(RleSimdTest, simd_widen_int16_to_int32) {
    std::mt19937 rng(0x55AA55AA);
    std::uniform_int_distribution<int32_t> dist(std::numeric_limits<int16_t>::min(),
                                                std::numeric_limits<int16_t>::max());

    for (int32_t n : kRleTestSizes) {
        std::vector<int16_t> src(n);
        for (auto& v : src) v = static_cast<int16_t>(dist(rng));
        std::vector<int32_t> dest(static_cast<size_t>(n) + 1, static_cast<int32_t>(0xDEADBEEFu));

        simd_widen_int16_to_int32(dest.data(), src.data(), n);

        for (int32_t i = 0; i < n; ++i) {
            ASSERT_EQ(dest[i], static_cast<int32_t>(src[i])) << "n=" << n << " i=" << i;
        }
        ASSERT_EQ(dest.back(), static_cast<int32_t>(0xDEADBEEFu)) << "n=" << n;
    }
    {
        int16_t src[8] = {-32768, -1, 0, 1, 32767, -16000, 16000, -2};
        int32_t dest[8] = {0};
        simd_widen_int16_to_int32(dest, src, 8);
        EXPECT_EQ(dest[0], -32768);
        EXPECT_EQ(dest[4], 32767);
        EXPECT_EQ(dest[5], -16000);
    }
    // Default scalar variant directly.
    {
        int16_t src[8] = {-32768, -1, 0, 1, 32767, -16000, 16000, -2};
        int32_t dest[8] = {0};
        simd_widen_int16_to_int32_default(dest, src, 8);
        EXPECT_EQ(dest[0], -32768);
        EXPECT_EQ(dest[4], 32767);
        EXPECT_EQ(dest[5], -16000);
        EXPECT_EQ(dest[7], -2);
    }
}

} // namespace starrocks
