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
#include <string>
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

// Test all 256 possible match patterns across 8 elements to rigorously verify
// exact match bitmask generation on 8-lane SIMD (AVX2 and ARM64 NEON).
PARALLEL_TEST(StringLengthFilterTest, length_eq_mask_8_elements_patterns) {
    const uint32_t target_len = 16u;
    const uint32_t mismatch_base = 32u;

    // Test every possible 8-bit pattern (0 through 255)
    for (uint32_t pattern = 0; pattern < 256; ++pattern) {
        std::vector<uint32_t> lengths(8);
        for (int i = 0; i < 8; ++i) {
            lengths[i] = ((pattern & (1u << i)) != 0) ? target_len : (mismatch_base + static_cast<uint32_t>(i));
        }
        auto off = make_offsets(lengths);

        uint32_t got = SIMD::length_eq_mask(off.data(), 0, target_len) & SIMD::kStringLenSimdMask;
        uint32_t expected = length_eq_mask_scalar(off.data(), 0, target_len);
        ASSERT_EQ(got, expected) << "Failed on pattern=" << pattern;

        if constexpr (SIMD::kStringLenSimdWidth == 8) {
            ASSERT_EQ(got, pattern) << "Failed on pattern=" << pattern;
        }
    }

    // Verify with various base offsets in a larger offset buffer
    const size_t kTotalElements = 64;
    std::vector<uint32_t> buffer_lengths(kTotalElements, 5u);
    for (size_t base : {0UL, 1UL, 3UL, 7UL, 8UL, 13UL, 21UL, 32UL, 45UL, 55UL}) {
        if (base + static_cast<size_t>(SIMD::kStringLenSimdWidth) > kTotalElements) {
            continue;
        }
        for (int i = 0; i < SIMD::kStringLenSimdWidth; ++i) {
            buffer_lengths[base + i] = (i % 3 == 0) ? target_len : (mismatch_base + static_cast<uint32_t>(i));
        }
        auto off = make_offsets(buffer_lengths);

        uint32_t got = SIMD::length_eq_mask(off.data(), base, target_len) & SIMD::kStringLenSimdMask;
        uint32_t expected = length_eq_mask_scalar(off.data(), base, target_len);
        ASSERT_EQ(got, expected) << "Failed at base=" << base;
    }
}

// Test range match bitmask across 8 elements with various partition patterns and boundaries.
PARALLEL_TEST(StringLengthFilterTest, length_in_range_mask_8_elements_patterns) {
    // 8 distinct lengths: {10, 20, 30, 40, 50, 60, 70, 80}
    std::vector<uint32_t> lengths = {10, 20, 30, 40, 50, 60, 70, 80};
    auto off = make_offsets(lengths);

    struct TestCase {
        uint32_t min_len;
        uint32_t max_len;
        uint32_t expected_8lane_mask;
    };

    const std::vector<TestCase> test_cases = {
            {10, 80, 0xFFu}, // All 8 match
            {10, 40, 0x0Fu}, // First 4 match (indices 0, 1, 2, 3)
            {50, 80, 0xF0u}, // Last 4 match (indices 4, 5, 6, 7)
            {30, 60, 0x3Cu}, // Middle 4 match (indices 2, 3, 4, 5)
            {25, 65, 0x3Cu}, // In-between bounds matching indices 2, 3, 4, 5
            {10, 10, 0x01u}, // Single lane match at index 0
            {40, 40, 0x08u}, // Single lane match at index 3
            {80, 80, 0x80u}, // Single lane match at index 7
            {0, 9, 0x00u},   // None match below minimum
            {81, 100, 0x00u} // None match above maximum
    };

    for (const auto& tc : test_cases) {
        uint32_t got = SIMD::length_in_range_mask(off.data(), 0, tc.min_len, tc.max_len) & SIMD::kStringLenSimdMask;
        uint32_t expected = length_in_range_mask_scalar(off.data(), 0, tc.min_len, tc.max_len);
        EXPECT_EQ(got, expected) << "min=" << tc.min_len << " max=" << tc.max_len;

        if constexpr (SIMD::kStringLenSimdWidth == 8) {
            EXPECT_EQ(got, tc.expected_8lane_mask) << "min=" << tc.min_len << " max=" << tc.max_len;
        }
    }

    // Single-bit isolation test: verify each lane in [0, 7] can be exclusively selected
    for (int lane = 0; lane < 8; ++lane) {
        uint32_t target = lengths[lane];
        uint32_t got = SIMD::length_in_range_mask(off.data(), 0, target, target) & SIMD::kStringLenSimdMask;
        uint32_t expected = length_in_range_mask_scalar(off.data(), 0, target, target);
        EXPECT_EQ(got, expected) << "Isolated lane=" << lane;
        if constexpr (SIMD::kStringLenSimdWidth == 8) {
            EXPECT_EQ(got, (1u << lane)) << "Isolated lane=" << lane;
        }
    }
}

// Rigorously verify caller loops and tail peeling as implemented in array_functions.tpp
// across tail array sizes from 1 to 15.
PARALLEL_TEST(StringLengthFilterTest, caller_loop_tail_peeling_sizes_1_to_15) {
    // Helper replicating the caller pattern from array_functions.tpp (_find_string_in_array_simd)
    auto find_string_in_array_simd_simulated = [](const uint32_t* offsets_data, size_t array_offset, size_t array_size,
                                                  const std::vector<std::string>& strings,
                                                  const std::string& target) -> size_t {
        if (array_size == 0) return 0;
        const uint32_t target_len = static_cast<uint32_t>(target.size());
        constexpr int kSimdWidth = SIMD::kStringLenSimdWidth;
        size_t j = 0;

        for (; j + kSimdWidth <= array_size; j += kSimdWidth) {
            size_t base = array_offset + j;
            uint32_t mask = SIMD::length_eq_mask(offsets_data, base, target_len);
            if (mask == 0) {
                continue;
            }

            while (mask) {
                uint32_t bit = __builtin_ctz(mask);
                size_t elem_idx = base + bit;
                if (strings[elem_idx] == target) {
                    return j + bit + 1; // 1-based position
                }
                mask &= (mask - 1);
            }
        }

        for (; j < array_size; j++) {
            size_t elem_idx = array_offset + j;
            uint32_t elem_len = offsets_data[elem_idx + 1] - offsets_data[elem_idx];
            if (elem_len == target_len && strings[elem_idx] == target) {
                return j + 1; // 1-based position
            }
        }

        return 0;
    };

    // Scalar reference implementation
    auto find_string_scalar_ref = [](const std::vector<std::string>& strings, size_t array_offset, size_t array_size,
                                     const std::string& target) -> size_t {
        for (size_t i = 0; i < array_size; ++i) {
            if (strings[array_offset + i] == target) {
                return i + 1;
            }
        }
        return 0;
    };

    const std::string target = "match_me"; // length 8

    // Test across array sizes 1 through 15
    for (size_t array_size = 1; array_size <= 15; ++array_size) {
        // Test with different array offsets within a surrounding buffer
        for (size_t array_offset : {0UL, 1UL, 3UL, 7UL}) {
            const size_t total_elements = array_offset + array_size + 8;

            // Scenario 1: Target placed at every possible match position [0, array_size - 1]
            for (size_t match_pos = 0; match_pos < array_size; ++match_pos) {
                std::vector<std::string> strings(total_elements);
                std::vector<uint32_t> lengths(total_elements);

                for (size_t i = 0; i < total_elements; ++i) {
                    // Populate with distinct strings of various lengths
                    strings[i] = "dummy_" + std::to_string(i);
                    lengths[i] = static_cast<uint32_t>(strings[i].size());
                }

                // Place target at match position
                strings[array_offset + match_pos] = target;
                lengths[array_offset + match_pos] = static_cast<uint32_t>(target.size());

                auto offsets = make_offsets(lengths);
                size_t got = find_string_in_array_simd_simulated(offsets.data(), array_offset, array_size, strings,
                                                                 target);
                size_t expected = find_string_scalar_ref(strings, array_offset, array_size, target);

                ASSERT_EQ(got, match_pos + 1)
                        << "Failed at array_size=" << array_size << " offset=" << array_offset
                        << " match_pos=" << match_pos;
                ASSERT_EQ(got, expected);
            }

            // Scenario 2: Target not present, but strings of the same length exist (false positive length collisions)
            {
                std::vector<std::string> strings(total_elements);
                std::vector<uint32_t> lengths(total_elements);

                for (size_t i = 0; i < total_elements; ++i) {
                    // Same length as target (8 chars), but differing content
                    strings[i] = "other_" + std::to_string(i % 10) + "x";
                    lengths[i] = static_cast<uint32_t>(strings[i].size());
                }

                auto offsets = make_offsets(lengths);
                size_t got = find_string_in_array_simd_simulated(offsets.data(), array_offset, array_size, strings,
                                                                 target);
                ASSERT_EQ(got, 0UL) << "False positive match for array_size=" << array_size
                                    << " offset=" << array_offset;
            }

            // Scenario 3: Target not present and lengths differ
            {
                std::vector<std::string> strings(total_elements);
                std::vector<uint32_t> lengths(total_elements);

                for (size_t i = 0; i < total_elements; ++i) {
                    strings[i] = "x"; // length 1
                    lengths[i] = static_cast<uint32_t>(strings[i].size());
                }

                auto offsets = make_offsets(lengths);
                size_t got = find_string_in_array_simd_simulated(offsets.data(), array_offset, array_size, strings,
                                                                 target);
                ASSERT_EQ(got, 0UL) << "Expected 0 for array_size=" << array_size << " offset=" << array_offset;
            }
        }
    }
}

} // namespace starrocks
