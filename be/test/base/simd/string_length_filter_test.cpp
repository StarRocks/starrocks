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

#include "base/simd/string_length_filter.h"

#include <cstdint>
#include <limits>
#include <random>
#include <vector>

#include "base/testutil/parallel_test.h"
#include "gtest/gtest.h"

namespace starrocks {

// Scalar reference: for each lane i in [0, width), set bit i iff
// (offsets[base + i + 1] - offsets[base + i]) satisfies the predicate.
static uint32_t length_eq_mask_scalar(const uint32_t* offsets, size_t base, uint32_t target_len) {
    uint32_t mask = 0;
    for (int i = 0; i < SIMD::kStringLenSimdWidth; ++i) {
        uint32_t len = offsets[base + i + 1] - offsets[base + i];
        if (len == target_len) mask |= (1u << i);
    }
    return mask;
}

static uint32_t length_in_range_mask_scalar(const uint32_t* offsets, size_t base, uint32_t min_len, uint32_t max_len) {
    uint32_t mask = 0;
    for (int i = 0; i < SIMD::kStringLenSimdWidth; ++i) {
        uint32_t len = offsets[base + i + 1] - offsets[base + i];
        if (len >= min_len && len <= max_len) mask |= (1u << i);
    }
    return mask;
}

// Build offsets[] of size N+1 from per-string lengths.
static std::vector<uint32_t> make_offsets(const std::vector<uint32_t>& lengths) {
    std::vector<uint32_t> off(lengths.size() + 1);
    off[0] = 0;
    for (size_t i = 0; i < lengths.size(); ++i) off[i + 1] = off[i] + lengths[i];
    return off;
}

PARALLEL_TEST(StringLengthFilterTest, length_eq_mask_known) {
    const int W = SIMD::kStringLenSimdWidth;
    // Construct a block of W strings with lengths {3, 5, 3, 7, ...} repeating,
    // then test target_len == 3.
    std::vector<uint32_t> lengths(W);
    for (int i = 0; i < W; ++i) lengths[i] = (i % 2 == 0) ? 3u : 5u;
    auto off = make_offsets(lengths);

    uint32_t got = SIMD::length_eq_mask(off.data(), 0, 3);
    uint32_t expected = length_eq_mask_scalar(off.data(), 0, 3);
    EXPECT_EQ(got & SIMD::kStringLenSimdMask, expected);

    // target_len == 5: bits where i is odd should be set.
    got = SIMD::length_eq_mask(off.data(), 0, 5);
    expected = length_eq_mask_scalar(off.data(), 0, 5);
    EXPECT_EQ(got & SIMD::kStringLenSimdMask, expected);

    // no match
    got = SIMD::length_eq_mask(off.data(), 0, 999);
    EXPECT_EQ(got & SIMD::kStringLenSimdMask, 0u);
}

PARALLEL_TEST(StringLengthFilterTest, length_eq_mask_random_oracle) {
    const int W = SIMD::kStringLenSimdWidth;
    std::mt19937 rng(0xBEEF);
    std::uniform_int_distribution<uint32_t> len_dist(0, 32);

    // Generate enough strings to cover multiple base offsets.
    const size_t kNumStrings = 256;
    std::vector<uint32_t> lengths(kNumStrings);
    for (auto& v : lengths) v = len_dist(rng);
    auto off = make_offsets(lengths);

    for (size_t base = 0; base + static_cast<size_t>(W) < kNumStrings; ++base) {
        for (uint32_t target : {0u, 1u, 5u, 17u, 32u, 100u}) {
            uint32_t got = SIMD::length_eq_mask(off.data(), base, target) & SIMD::kStringLenSimdMask;
            uint32_t expected = length_eq_mask_scalar(off.data(), base, target);
            ASSERT_EQ(got, expected) << "base=" << base << " target=" << target;
        }
    }
}

PARALLEL_TEST(StringLengthFilterTest, length_in_range_mask_known) {
    const int W = SIMD::kStringLenSimdWidth;
    std::vector<uint32_t> lengths(W);
    for (int i = 0; i < W; ++i) lengths[i] = static_cast<uint32_t>(i * 2); // 0, 2, 4, ...
    auto off = make_offsets(lengths);

    // Range [2, 4] catches indices {1, 2}.
    uint32_t got = SIMD::length_in_range_mask(off.data(), 0, 2, 4) & SIMD::kStringLenSimdMask;
    uint32_t expected = length_in_range_mask_scalar(off.data(), 0, 2, 4);
    EXPECT_EQ(got, expected);

    // Range matching all (0..len-1)
    got = SIMD::length_in_range_mask(off.data(), 0, 0, std::numeric_limits<uint32_t>::max()) & SIMD::kStringLenSimdMask;
    EXPECT_EQ(got, SIMD::kStringLenSimdMask);

    // Range matching none.
    got = SIMD::length_in_range_mask(off.data(), 0, 1000, 2000) & SIMD::kStringLenSimdMask;
    EXPECT_EQ(got, 0u);
}

PARALLEL_TEST(StringLengthFilterTest, length_in_range_mask_random_oracle) {
    const int W = SIMD::kStringLenSimdWidth;
    std::mt19937 rng(0xCAFE);
    std::uniform_int_distribution<uint32_t> len_dist(0, 100);

    const size_t kNumStrings = 256;
    std::vector<uint32_t> lengths(kNumStrings);
    for (auto& v : lengths) v = len_dist(rng);
    auto off = make_offsets(lengths);

    const std::vector<std::pair<uint32_t, uint32_t>> ranges = {
            {0, 0}, {0, 5}, {3, 17}, {50, 100}, {0, std::numeric_limits<uint32_t>::max()}, {200, 300},
    };

    for (size_t base = 0; base + static_cast<size_t>(W) < kNumStrings; ++base) {
        for (auto [lo, hi] : ranges) {
            uint32_t got = SIMD::length_in_range_mask(off.data(), base, lo, hi) & SIMD::kStringLenSimdMask;
            uint32_t expected = length_in_range_mask_scalar(off.data(), base, lo, hi);
            ASSERT_EQ(got, expected) << "base=" << base << " range=[" << lo << "," << hi << "]";
        }
    }
}

// Edge case: min_len == 0 and max_len == UINT32_MAX must not wrap (regression for
// the (min-1)/(max+1) trick the AVX2 path used to use).
PARALLEL_TEST(StringLengthFilterTest, length_in_range_extreme_bounds) {
    const int W = SIMD::kStringLenSimdWidth;
    std::vector<uint32_t> lengths(W);
    for (int i = 0; i < W; ++i) lengths[i] = static_cast<uint32_t>(i);
    auto off = make_offsets(lengths);

    // [0, UINT32_MAX] must match every lane.
    uint32_t got = SIMD::length_in_range_mask(off.data(), 0, 0, std::numeric_limits<uint32_t>::max()) &
                   SIMD::kStringLenSimdMask;
    EXPECT_EQ(got, SIMD::kStringLenSimdMask);

    // [UINT32_MAX, UINT32_MAX] must match nothing (no length equals UINT32_MAX here).
    got = SIMD::length_in_range_mask(off.data(), 0, std::numeric_limits<uint32_t>::max(),
                                     std::numeric_limits<uint32_t>::max()) &
          SIMD::kStringLenSimdMask;
    EXPECT_EQ(got, 0u);
}

} // namespace starrocks
