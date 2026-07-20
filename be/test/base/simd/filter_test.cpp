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

#include "base/simd/filter.h"

#include <gtest/gtest.h>

#include <random>
#include <vector>

#include "base/testutil/parallel_test.h"

namespace starrocks {

// Convenience wrapper so tests keep a typed call site.
template <typename T>
static size_t filter_range(T* dst, const T* src, const std::vector<uint8_t>& selector, size_t from, size_t to) {
    return SIMD::Filter::filter_range(dst, src, selector.data(), from, to);
}

PARALLEL_TEST(SimdFilterTest, in_place_filter_int32) {
    std::vector<int32_t> values = {10, 11, 12, 13, 14, 15};
    std::vector<uint8_t> selector = {1, 0, 1, 0, 1, 0};

    size_t size = filter_range(values.data(), values.data(), selector, 0, values.size());
    ASSERT_EQ(3, size);
    EXPECT_EQ(10, values[0]);
    EXPECT_EQ(12, values[1]);
    EXPECT_EQ(14, values[2]);
}

PARALLEL_TEST(SimdFilterTest, filter_int32_to_separate_buffer) {
    const std::vector<int32_t> src = {1, 2, 3, 4, 5, 6};
    std::vector<int32_t> dst(src.size(), -1);
    std::vector<uint8_t> selector = {0, 1, 1, 0, 1, 0};

    size_t size = filter_range(dst.data(), src.data(), selector, 0, src.size());
    ASSERT_EQ(3, size);
    EXPECT_EQ(2, dst[0]);
    EXPECT_EQ(3, dst[1]);
    EXPECT_EQ(5, dst[2]);
}

PARALLEL_TEST(SimdFilterTest, range_filter_double) {
    std::vector<double> values = {0.1, 0.2, 0.3, 0.4, 0.5, 0.6};
    std::vector<uint8_t> selector = {1, 0, 1, 1, 0, 1};

    // Only [1, 5) is compacted; values[0] is preserved.
    size_t size = filter_range(values.data(), values.data(), selector, 1, 5);
    ASSERT_EQ(3, size);
    EXPECT_DOUBLE_EQ(0.1, values[0]);
    EXPECT_DOUBLE_EQ(0.3, values[1]);
    EXPECT_DOUBLE_EQ(0.4, values[2]);
}

PARALLEL_TEST(SimdFilterTest, wide_element_width) {
    // 12-byte elements (e.g. int96 / decimal12): no vpcompress path, so this
    // exercises the width-specialised batch scan for a non-4/8-byte width.
    struct Wide {
        uint32_t a, b, c;
        bool operator==(const Wide& o) const { return a == o.a && b == o.b && c == o.c; }
    };
    std::vector<Wide> values = {{1, 1, 1}, {2, 2, 2}, {3, 3, 3}, {4, 4, 4}};
    std::vector<uint8_t> selector = {0, 1, 0, 1};
    size_t size = filter_range(values.data(), values.data(), selector, 0, values.size());
    ASSERT_EQ(2, size);
    EXPECT_TRUE((values[0] == Wide{2, 2, 2}));
    EXPECT_TRUE((values[1] == Wide{4, 4, 4}));
}

PARALLEL_TEST(SimdFilterTest, zero_and_one_masks) {
    std::vector<int32_t> values_zero = {7, 8, 9, 10};
    std::vector<uint8_t> zero_filter(values_zero.size(), 0);
    size_t zero_size = filter_range(values_zero.data(), values_zero.data(), zero_filter, 0, 4);
    EXPECT_EQ(0, zero_size);

    std::vector<int32_t> values_one = {100, 200, 300, 400, 500};
    std::vector<uint8_t> one_filter(values_one.size(), 1);
    size_t one_size = filter_range(values_one.data(), values_one.data(), one_filter, 2, 5);
    EXPECT_EQ(values_one.size(), one_size);
    EXPECT_EQ(100, values_one[0]);
    EXPECT_EQ(200, values_one[1]);
    EXPECT_EQ(300, values_one[2]);
    EXPECT_EQ(400, values_one[3]);
    EXPECT_EQ(500, values_one[4]);
}

// Exercise the vectorised batch path (>= 32 lanes) against a scalar reference
// across selectivities, for both 4- and 8-byte widths.
template <typename T>
static void check_against_reference(size_t n, int keep_permille, uint32_t seed) {
    std::mt19937 rng(seed);
    std::uniform_int_distribution<int> dist(0, 999);
    std::vector<T> src(n);
    std::vector<uint8_t> selector(n);
    for (size_t i = 0; i < n; ++i) {
        src[i] = static_cast<T>(i * 2654435761ull + 1);
        selector[i] = (dist(rng) < keep_permille) ? 1 : 0;
    }

    std::vector<T> expected;
    for (size_t i = 0; i < n; ++i) {
        if (selector[i]) expected.push_back(src[i]);
    }

    std::vector<T> got = src; // in-place
    size_t size = filter_range(got.data(), got.data(), selector, 0, n);
    ASSERT_EQ(expected.size(), size) << "n=" << n << " keep=" << keep_permille;
    for (size_t i = 0; i < size; ++i) {
        ASSERT_EQ(expected[i], got[i]) << "mismatch at " << i << " n=" << n << " keep=" << keep_permille;
    }
}

PARALLEL_TEST(SimdFilterTest, vectorized_path_matches_scalar) {
    for (size_t n : {32u, 33u, 63u, 64u, 100u, 4096u, 4097u}) {
        for (int keep : {0, 50, 250, 500, 750, 950, 1000}) {
            check_against_reference<int32_t>(n, keep, 12345);
            check_against_reference<int64_t>(n, keep, 67890);
        }
    }
}

} // namespace starrocks
