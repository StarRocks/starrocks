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

#include "column/sorting/radix_sort.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <latch>
#include <limits>
#include <random>
#include <thread>
#include <vector>

#include "base/orlp/pdqsort.h"
#include "base/string/slice.h"
#include "base/testutil/sync_point.h"
#include "base/types/decimal12.h"
#include "base/types/int128.h"
#include "base/types/int256.h"
#include "base/types/int96.h"
#include "base/types/uint24.h"
#include "column/column_sorter_comparator.h"
#include "column/sorting/sort_permute.h"
#include "common/config_exec_flow_fwd.h"
#include "types/date_value.h"
#include "types/decimalv2_value.h"
#include "types/timestamp_value.h"

namespace starrocks {
namespace {

TEST(RadixSortTest, ConceptSatisfaction) {
    // 12 supported fixed-length types
    static_assert(RadixSortable<uint8_t>);
    static_assert(RadixSortable<uint16_t>);
    static_assert(RadixSortable<uint32_t>);
    static_assert(RadixSortable<uint64_t>);
    static_assert(RadixSortable<int8_t>);
    static_assert(RadixSortable<int16_t>);
    static_assert(RadixSortable<int32_t>);
    static_assert(RadixSortable<int64_t>);
    static_assert(RadixSortable<float>);
    static_assert(RadixSortable<double>);
    static_assert(RadixSortable<DateValue>);
    static_assert(RadixSortable<TimestampValue>);

    EXPECT_TRUE(RadixSortable<uint8_t>);
    EXPECT_TRUE(RadixSortable<uint16_t>);
    EXPECT_TRUE(RadixSortable<uint32_t>);
    EXPECT_TRUE(RadixSortable<uint64_t>);
    EXPECT_TRUE(RadixSortable<int8_t>);
    EXPECT_TRUE(RadixSortable<int16_t>);
    EXPECT_TRUE(RadixSortable<int32_t>);
    EXPECT_TRUE(RadixSortable<int64_t>);
    EXPECT_TRUE(RadixSortable<float>);
    EXPECT_TRUE(RadixSortable<double>);
    EXPECT_TRUE(RadixSortable<DateValue>);
    EXPECT_TRUE(RadixSortable<TimestampValue>);

    // Unsupported types must evaluate to false
    static_assert(!RadixSortable<int128_t>);
    static_assert(!RadixSortable<int256_t>);
    static_assert(!RadixSortable<DecimalV2Value>);
    static_assert(!RadixSortable<uint24_t>);
    static_assert(!RadixSortable<int96_t>);
    static_assert(!RadixSortable<decimal12_t>);
    static_assert(!RadixSortable<Slice>);

    EXPECT_FALSE(RadixSortable<int128_t>);
    EXPECT_FALSE(RadixSortable<int256_t>);
    EXPECT_FALSE(RadixSortable<DecimalV2Value>);
    EXPECT_FALSE(RadixSortable<uint24_t>);
    EXPECT_FALSE(RadixSortable<int96_t>);
    EXPECT_FALSE(RadixSortable<decimal12_t>);
    EXPECT_FALSE(RadixSortable<Slice>);
}

template <typename IntT>
void verify_integer_key_monotonicity(const std::vector<IntT>& sorted_values) {
    for (size_t i = 1; i < sorted_values.size(); ++i) {
        ASSERT_LT(sorted_values[i - 1], sorted_values[i]);
        auto k_prev_asc = RadixSortTraits<IntT>::to_radix_key(sorted_values[i - 1], true);
        auto k_curr_asc = RadixSortTraits<IntT>::to_radix_key(sorted_values[i], true);
        EXPECT_LT(k_prev_asc, k_curr_asc) << "ASC failure at index " << i;

        auto k_prev_desc = RadixSortTraits<IntT>::to_radix_key(sorted_values[i - 1], false);
        auto k_curr_desc = RadixSortTraits<IntT>::to_radix_key(sorted_values[i], false);
        EXPECT_GT(k_prev_desc, k_curr_desc) << "DESC failure at index " << i;
    }
}

TEST(RadixSortTest, IntegerTraitsMonotonicity) {
    verify_integer_key_monotonicity<int8_t>(
            {std::numeric_limits<int8_t>::min(), -100, -1, 0, 1, 100, std::numeric_limits<int8_t>::max()});
    verify_integer_key_monotonicity<int16_t>(
            {std::numeric_limits<int16_t>::min(), -1000, -1, 0, 1, 1000, std::numeric_limits<int16_t>::max()});
    verify_integer_key_monotonicity<int32_t>(
            {std::numeric_limits<int32_t>::min(), -1000, -1, 0, 1, 1000, std::numeric_limits<int32_t>::max()});
    verify_integer_key_monotonicity<int64_t>(
            {std::numeric_limits<int64_t>::min(), -1000, -1, 0, 1, 1000, std::numeric_limits<int64_t>::max()});

    verify_integer_key_monotonicity<uint8_t>({0, 1, 127, 128, 255});
    verify_integer_key_monotonicity<uint16_t>({0, 1, 1000, 32768, 65535});
    verify_integer_key_monotonicity<uint32_t>({0U, 1U, 1000U, 0x80000000U, 0xFFFFFFFFU});
    verify_integer_key_monotonicity<uint64_t>({0ULL, 1ULL, 1000ULL, 0x8000000000000000ULL, 0xFFFFFFFFFFFFFFFFULL});
}

template <typename FloatT, typename UnsignedT>
void verify_float_traits_monotonicity() {
    using Limits = std::numeric_limits<FloatT>;
    const FloatT neg_inf = -Limits::infinity();
    const FloatT neg_max = -Limits::max();
    const FloatT neg_one = static_cast<FloatT>(-1.0);
    const FloatT neg_min_norm = -Limits::min();
    const FloatT neg_denorm = -Limits::denorm_min();
    const FloatT neg_zero = -static_cast<FloatT>(0.0);
    const FloatT pos_zero = +static_cast<FloatT>(0.0);
    const FloatT pos_denorm = Limits::denorm_min();
    const FloatT pos_min_norm = Limits::min();
    const FloatT pos_one = static_cast<FloatT>(1.0);
    const FloatT pos_max = Limits::max();
    const FloatT pos_inf = Limits::infinity();
    const FloatT qnan = Limits::quiet_NaN();
    const FloatT snan = Limits::signaling_NaN();
    const FloatT neg_qnan = -Limits::quiet_NaN();

    FloatT custom_nan;
    UnsignedT custom_nan_bits =
            sizeof(FloatT) == 4 ? static_cast<UnsignedT>(0x7fc01234U) : static_cast<UnsignedT>(0xfff8000012345678ULL);
    std::memcpy(&custom_nan, &custom_nan_bits, sizeof(FloatT));

    for (bool asc : {true, false}) {
        // -0.0, +0.0, and all NaN bit patterns must map to the exact same key
        // to match SorterComparator<FloatingPoint>::compare (which maps NaN -> 0).
        auto k_zero = RadixSortTraits<FloatT>::to_radix_key(pos_zero, asc);
        EXPECT_EQ(k_zero, RadixSortTraits<FloatT>::to_radix_key(neg_zero, asc));
        EXPECT_EQ(k_zero, RadixSortTraits<FloatT>::to_radix_key(qnan, asc));
        EXPECT_EQ(k_zero, RadixSortTraits<FloatT>::to_radix_key(snan, asc));
        EXPECT_EQ(k_zero, RadixSortTraits<FloatT>::to_radix_key(neg_qnan, asc));
        EXPECT_EQ(k_zero, RadixSortTraits<FloatT>::to_radix_key(custom_nan, asc));

        // Strictly ordered chain:
        // -inf < -max < -1.0 < -min_normal < -denorm_min < +0.0 (== -0.0 == NaN) < +denorm_min < +min_normal < +1.0 < +max < +inf
        std::vector<FloatT> chain = {neg_inf,    neg_max,      neg_one, neg_min_norm, neg_denorm, pos_zero,
                                     pos_denorm, pos_min_norm, pos_one, pos_max,      pos_inf};

        for (size_t i = 1; i < chain.size(); ++i) {
            auto k_prev = RadixSortTraits<FloatT>::to_radix_key(chain[i - 1], asc);
            auto k_curr = RadixSortTraits<FloatT>::to_radix_key(chain[i], asc);
            if (asc) {
                EXPECT_LT(k_prev, k_curr) << "ASC float chain failure at index " << i;
            } else {
                EXPECT_GT(k_prev, k_curr) << "DESC float chain failure at index " << i;
            }
        }
    }
}

TEST(RadixSortTest, FloatingPointTraitsMonotonicityAndEdgeCases) {
    verify_float_traits_monotonicity<float, uint32_t>();
    verify_float_traits_monotonicity<double, uint64_t>();
}

TEST(RadixSortTest, DateAndTimestampTraitsMonotonicity) {
    std::vector<int32_t> julians = {std::numeric_limits<int32_t>::min(), -100000, -1, 0, 1, 2459000,
                                    std::numeric_limits<int32_t>::max()};
    for (size_t i = 1; i < julians.size(); ++i) {
        DateValue prev{julians[i - 1]};
        DateValue curr{julians[i]};
        EXPECT_LT(RadixSortTraits<DateValue>::to_radix_key(prev, true),
                  RadixSortTraits<DateValue>::to_radix_key(curr, true));
        EXPECT_GT(RadixSortTraits<DateValue>::to_radix_key(prev, false),
                  RadixSortTraits<DateValue>::to_radix_key(curr, false));
    }

    std::vector<int64_t> timestamps = {std::numeric_limits<int64_t>::min(), -1000000000LL, -1, 0, 1, 1000000000LL,
                                       std::numeric_limits<int64_t>::max()};
    for (size_t i = 1; i < timestamps.size(); ++i) {
        TimestampValue prev{timestamps[i - 1]};
        TimestampValue curr{timestamps[i]};
        EXPECT_LT(RadixSortTraits<TimestampValue>::to_radix_key(prev, true),
                  RadixSortTraits<TimestampValue>::to_radix_key(curr, true));
        EXPECT_GT(RadixSortTraits<TimestampValue>::to_radix_key(prev, false),
                  RadixSortTraits<TimestampValue>::to_radix_key(curr, false));
    }
}

template <RadixSortable T>
void verify_radix_sort_equivalence(const std::vector<T>& values, bool asc) {
    const size_t n = values.size();
    InlinePermutation<T> radix_items(n);
    InlinePermutation<T> radix_tie_items(n);
    InlinePermutation<T> ref_items(n);

    for (size_t i = 0; i < n; ++i) {
        radix_items[i] = InlinePermuteItem<T>{values[i], static_cast<uint32_t>(i)};
        radix_tie_items[i] = InlinePermuteItem<T>{values[i], static_cast<uint32_t>(i)};
        ref_items[i] = InlinePermuteItem<T>{values[i], static_cast<uint32_t>(i)};
    }

    // Reference sort using SorterComparator<T>::compare directly + stabilization by index_in_chunk for ties
    auto cmp_val = [&](const InlinePermuteItem<T>& a, const InlinePermuteItem<T>& b) -> int {
        return SorterComparator<T>::compare(a.inline_value, b.inline_value);
    };
    ::pdqsort(ref_items.begin(), ref_items.end(), [&](const InlinePermuteItem<T>& a, const InlinePermuteItem<T>& b) {
        int c = cmp_val(a, b);
        if (c != 0) {
            return asc ? (c < 0) : (c > 0);
        }
        return a.index_in_chunk < b.index_in_chunk;
    });

    Tie ref_tie(n, 1);
    bool ref_has_ties = false;
    if (n > 0) {
        ref_tie[0] = 0;
        for (size_t i = 1; i < n; ++i) {
            uint8_t eq = static_cast<uint8_t>(cmp_val(ref_items[i - 1], ref_items[i]) == 0);
            ref_tie[i] = eq;
            ref_has_ties |= (eq != 0);
        }
    }

    InlinePermutation<T> scratch(n);
    bool cancel = false;

    // 1. Test radix_sort_inlined
    radix_sort_inlined(cancel, radix_items.data(), scratch.data(), n, asc);
    for (size_t i = 0; i < n; ++i) {
        EXPECT_EQ(radix_items[i].index_in_chunk, ref_items[i].index_in_chunk)
                << "radix_sort_inlined mismatch at i=" << i << " (n=" << n << ", asc=" << asc << ")";
    }

    // 2. Test radix_sort_inlined_and_tie
    Tie actual_tie(n, 1);
    if (n > 0) {
        actual_tie[0] = 0;
    }
    bool actual_has_ties = false;
    radix_sort_inlined_and_tie(cancel, radix_tie_items.data(), scratch.data(), 0, n, asc, actual_tie, actual_has_ties);

    EXPECT_EQ(actual_has_ties, ref_has_ties) << "range_has_ties mismatch (n=" << n << ", asc=" << asc << ")";
    for (size_t i = 0; i < n; ++i) {
        EXPECT_EQ(radix_tie_items[i].index_in_chunk, ref_items[i].index_in_chunk)
                << "radix_sort_inlined_and_tie index mismatch at i=" << i << " (n=" << n << ", asc=" << asc << ")";
        EXPECT_EQ(actual_tie[i], ref_tie[i])
                << "radix_sort_inlined_and_tie tie mismatch at i=" << i << " (n=" << n << ", asc=" << asc << ")";
        if (i > 0 && actual_tie[i] == 1) {
            EXPECT_LT(radix_tie_items[i - 1].index_in_chunk, radix_tie_items[i].index_in_chunk)
                    << "Stability violated on tie at i=" << i;
        }
    }
}

template <RadixSortable T, typename GenFunc>
void test_type_distributions_and_sizes(GenFunc gen) {
    const std::vector<size_t> sizes = {0,  1,  2,  3,   5,   7,   15,  16,  17,  31,  32,   33,
                                       63, 64, 65, 255, 256, 257, 259, 260, 263, 511, 1024, 4096};
    for (size_t n : sizes) {
        std::vector<T> random_vals(n);
        for (size_t i = 0; i < n; ++i) {
            random_vals[i] = gen(i, n);
        }
        for (bool asc : {true, false}) {
            // Random with duplicates
            verify_radix_sort_equivalence(random_vals, asc);

            // Already sorted (exercises is_sorted fast-path)
            std::vector<T> sorted_vals = random_vals;
            std::sort(sorted_vals.begin(), sorted_vals.end(), [](const T& a, const T& b) {
                return RadixSortTraits<T>::to_radix_key(a, true) < RadixSortTraits<T>::to_radix_key(b, true);
            });
            verify_radix_sort_equivalence(sorted_vals, asc);

            // Reverse sorted
            std::vector<T> rev_vals = sorted_vals;
            std::reverse(rev_vals.begin(), rev_vals.end());
            verify_radix_sort_equivalence(rev_vals, asc);

            // All identical (exercises 0 active passes / uniform-byte pass skipping)
            if (n > 0) {
                std::vector<T> identical_vals(n, random_vals[0]);
                verify_radix_sort_equivalence(identical_vals, asc);
            }
        }
    }
}

TEST(RadixSortTest, All12SupportedTypesDistributionsAndSizes) {
    std::mt19937_64 rng(42);

    test_type_distributions_and_sizes<uint8_t>([&](size_t, size_t) { return static_cast<uint8_t>(rng() % 50); });
    test_type_distributions_and_sizes<int8_t>(
            [&](size_t, size_t) { return static_cast<int8_t>(static_cast<int>(rng() % 50) - 25); });
    test_type_distributions_and_sizes<uint16_t>([&](size_t, size_t) { return static_cast<uint16_t>(rng() % 200); });
    test_type_distributions_and_sizes<int16_t>(
            [&](size_t, size_t) { return static_cast<int16_t>(static_cast<int>(rng() % 200) - 100); });
    test_type_distributions_and_sizes<uint32_t>([&](size_t, size_t) { return static_cast<uint32_t>(rng() % 500); });
    test_type_distributions_and_sizes<int32_t>(
            [&](size_t, size_t) { return static_cast<int32_t>(static_cast<int64_t>(rng() % 500) - 250); });
    test_type_distributions_and_sizes<uint64_t>([&](size_t, size_t) { return static_cast<uint64_t>(rng() % 500); });
    test_type_distributions_and_sizes<int64_t>([&](size_t, size_t) { return static_cast<int64_t>(rng() % 500) - 250; });
    test_type_distributions_and_sizes<float>(
            [&](size_t, size_t) { return static_cast<float>(static_cast<int>(rng() % 200) - 100) * 0.5f; });
    test_type_distributions_and_sizes<double>(
            [&](size_t, size_t) { return static_cast<double>(static_cast<int>(rng() % 200) - 100) * 0.25; });
    test_type_distributions_and_sizes<DateValue>(
            [&](size_t, size_t) { return DateValue{static_cast<int32_t>(rng() % 300) - 150 + 2450000}; });
    test_type_distributions_and_sizes<TimestampValue>(
            [&](size_t, size_t) { return TimestampValue{static_cast<int64_t>(rng() % 300) - 150}; });
}

TEST(RadixSortTest, HighPrefixSharingPartialPassSkipping) {
    // Only 1 or 2 active byte passes out of 4 or 8
    const size_t n = 1024;
    std::mt19937_64 rng(1337);

    std::vector<uint32_t> vals32(n);
    std::vector<uint64_t> vals64(n);
    for (size_t i = 0; i < n; ++i) {
        // High 3 bytes constant (0x12345600), only LSB varies
        vals32[i] = 0x12345600U | static_cast<uint32_t>(rng() & 0xFFU);
        // High 6 bytes constant, only middle 2 bytes vary
        vals64[i] = 0xCAFEBABE00000000ULL | (static_cast<uint64_t>(rng() & 0xFFFFULL) << 8);
    }

    for (bool asc : {true, false}) {
        verify_radix_sort_equivalence(vals32, asc);
        verify_radix_sort_equivalence(vals64, asc);
    }
}

TEST(RadixSortTest, FloatingPointSpecialValuesSortingAndTies) {
    using LimitsF = std::numeric_limits<float>;
    float custom_nan1, custom_nan2;
    uint32_t nan1_bits = 0x7fc00001U;
    uint32_t nan2_bits = 0xffc00abcU;
    std::memcpy(&custom_nan1, &nan1_bits, sizeof(float));
    std::memcpy(&custom_nan2, &nan2_bits, sizeof(float));

    std::vector<float> base_specials = {
            LimitsF::quiet_NaN(),
            custom_nan1,
            custom_nan2,
            LimitsF::signaling_NaN(),
            LimitsF::infinity(),
            -LimitsF::infinity(),
            -0.0f,
            +0.0f,
            LimitsF::denorm_min(),
            -LimitsF::denorm_min(),
            LimitsF::min(),
            -LimitsF::min(),
            LimitsF::max(),
            -LimitsF::max(),
            1.0f,
            -1.0f,
    };

    std::vector<float> values;
    values.reserve(512);
    for (size_t i = 0; i < 512; ++i) {
        values.push_back(base_specials[i % base_specials.size()]);
    }

    for (bool asc : {true, false}) {
        verify_radix_sort_equivalence(values, asc);
    }
}

TEST(RadixSortTest, SubRangeIsolationAndPreExistingTiePreservation) {
    const size_t total = 500;
    const size_t first = 50;
    const size_t last = 350;
    const size_t sub_len = last - first;

    std::mt19937 rng(999);
    InlinePermutation<int32_t> items(total);
    for (size_t i = 0; i < total; ++i) {
        // Use small domain (0..15) so adjacent duplicates are frequent
        items[i] = InlinePermuteItem<int32_t>{static_cast<int32_t>(rng() % 16) - 8, static_cast<uint32_t>(i)};
    }
    const InlinePermutation<int32_t> original_items = items;

    Tie tie(total, 1);
    tie[0] = 0;
    tie[25] = 0;
    tie[first] = 0; // Sub-range start boundary
    tie[120] = 0;   // Pre-existing 0 boundary inside [first + 1, last)
    tie[240] = 0;   // Another pre-existing 0 boundary inside [first + 1, last)
    tie[last] = 0;  // Next sub-range start boundary
    tie[420] = 0;
    const Tie original_tie = tie;

    InlinePermutation<int32_t> scratch(sub_len);
    bool cancel = false;
    bool range_has_ties = false;

    radix_sort_inlined_and_tie(cancel, items.data(), scratch.data(), first, last, true, tie, range_has_ties);

    // 1. Verify items outside [first, last) are untouched
    for (size_t i = 0; i < first; ++i) {
        EXPECT_EQ(items[i].inline_value, original_items[i].inline_value);
        EXPECT_EQ(items[i].index_in_chunk, original_items[i].index_in_chunk);
    }
    for (size_t i = last; i < total; ++i) {
        EXPECT_EQ(items[i].inline_value, original_items[i].inline_value);
        EXPECT_EQ(items[i].index_in_chunk, original_items[i].index_in_chunk);
    }

    // 2. Verify tie[0 .. first] and tie[last .. total - 1] are untouched
    for (size_t i = 0; i <= first; ++i) {
        EXPECT_EQ(tie[i], original_tie[i]) << "tie modified before/at first at index " << i;
    }
    for (size_t i = last; i < total; ++i) {
        EXPECT_EQ(tie[i], original_tie[i]) << "tie modified at/after last at index " << i;
    }

    // 3. Verify pre-existing 0s at 120 and 240 were preserved, and tie semantics match
    EXPECT_EQ(tie[120], 0);
    EXPECT_EQ(tie[240], 0);

    bool expected_has_ties = false;
    for (size_t k = first + 1; k < last; ++k) {
        int32_t prev_val = original_items[items[k - 1].index_in_chunk].inline_value;
        int32_t curr_val = original_items[items[k].index_in_chunk].inline_value;
        ASSERT_LE(prev_val, curr_val);
        uint8_t expected_eq = (original_tie[k] != 0) & static_cast<uint8_t>(prev_val == curr_val);
        EXPECT_EQ(tie[k], expected_eq) << "Mismatch at k=" << k;
        expected_has_ties |= (expected_eq != 0);
    }
    EXPECT_EQ(range_has_ties, expected_has_ties);

    // Also test the is_sorted fast-path with pre-existing 0 boundaries in tie
    InlinePermutation<int64_t> sorted_items(total);
    for (size_t i = 0; i < total; ++i) {
        sorted_items[i] = InlinePermuteItem<int64_t>{static_cast<int64_t>(i / 4), static_cast<uint32_t>(i)};
    }
    Tie tie2 = original_tie;
    InlinePermutation<int64_t> scratch64(sub_len);
    bool range_has_ties2 = false;
    radix_sort_inlined_and_tie(cancel, sorted_items.data(), scratch64.data(), first, last, true, tie2, range_has_ties2);
    EXPECT_EQ(tie2[first], 0);
    EXPECT_EQ(tie2[120], 0);
    EXPECT_EQ(tie2[240], 0);
    EXPECT_EQ(tie2[last], 0);
    EXPECT_TRUE(range_has_ties2);
}

TEST(RadixSortTest, ExplicitSimdTargetKernelsMatchScalar) {
    static_assert(sizeof(detail::RadixPermuteItem<uint32_t>) == sizeof(InlinePermuteItem<int32_t>));
    static_assert(alignof(detail::RadixPermuteItem<uint32_t>) == alignof(InlinePermuteItem<int32_t>));
    static_assert(sizeof(detail::RadixPermuteItem<uint64_t>) == sizeof(InlinePermuteItem<int64_t>));
    static_assert(alignof(detail::RadixPermuteItem<uint64_t>) == alignof(InlinePermuteItem<int64_t>));

    const size_t n = 23;
    std::vector<detail::RadixPermuteItem<uint32_t>> items32(n);
    std::vector<detail::RadixPermuteItem<uint64_t>> items64(n);
    for (size_t i = 0; i < n; ++i) {
        uint32_t v32 = static_cast<uint32_t>((i / 2) * 17 + (i % 3 == 0 ? 0 : 1));
        items32[i] = detail::RadixPermuteItem<uint32_t>{v32, static_cast<uint32_t>(i)};
        items64[i] = detail::RadixPermuteItem<uint64_t>{static_cast<uint64_t>(v32) * 0x01010101ULL,
                                                        static_cast<uint32_t>(i)};
    }

    Tie base_tie(n, 1);
    for (size_t i = 0; i < n; ++i) {
        base_tie[i] = (i % 5 == 0) ? 0 : 1;
    }

    Tie tie32_scalar = base_tie;
    bool has_ties32_scalar = false;
    detail::detect_ties_scalar(items32.data(), 0, 1, n, tie32_scalar, has_ties32_scalar);

    Tie tie64_scalar = base_tie;
    bool has_ties64_scalar = false;
    detail::detect_ties_scalar(items64.data(), 0, 1, n, tie64_scalar, has_ties64_scalar);

#if defined(__GNUC__) && defined(__x86_64__)
    if (detail::cpu_supports_avx2()) {
        Tie tie32_avx2 = base_tie;
        bool has_ties32_avx2 = false;
        size_t j32_avx2 = detail::detect_ties_avx2_u32(items32.data(), 0, 1, n, tie32_avx2, has_ties32_avx2);
        detail::detect_ties_scalar(items32.data(), 0, j32_avx2, n, tie32_avx2, has_ties32_avx2);
        EXPECT_EQ(tie32_avx2, tie32_scalar);
        EXPECT_EQ(has_ties32_avx2, has_ties32_scalar);

        Tie tie64_avx2 = base_tie;
        bool has_ties64_avx2 = false;
        size_t j64_avx2 = detail::detect_ties_avx2_u64(items64.data(), 0, 1, n, tie64_avx2, has_ties64_avx2);
        detail::detect_ties_scalar(items64.data(), 0, j64_avx2, n, tie64_avx2, has_ties64_avx2);
        EXPECT_EQ(tie64_avx2, tie64_scalar);
        EXPECT_EQ(has_ties64_avx2, has_ties64_scalar);
    }

    if (detail::cpu_supports_avx512bw()) {
        Tie tie32_avx512 = base_tie;
        bool has_ties32_avx512 = false;
        size_t j32_avx512 = detail::detect_ties_avx512_u32(items32.data(), 0, 1, n, tie32_avx512, has_ties32_avx512);
        detail::detect_ties_scalar(items32.data(), 0, j32_avx512, n, tie32_avx512, has_ties32_avx512);
        EXPECT_EQ(tie32_avx512, tie32_scalar);
        EXPECT_EQ(has_ties32_avx512, has_ties32_scalar);

        Tie tie64_avx512 = base_tie;
        bool has_ties64_avx512 = false;
        size_t j64_avx512 = detail::detect_ties_avx512_u64(items64.data(), 0, 1, n, tie64_avx512, has_ties64_avx512);
        detail::detect_ties_scalar(items64.data(), 0, j64_avx512, n, tie64_avx512, has_ties64_avx512);
        EXPECT_EQ(tie64_avx512, tie64_scalar);
        EXPECT_EQ(has_ties64_avx512, has_ties64_scalar);
    }
#endif

    bool cancel = false;
    InlinePermutation<int32_t> single_item(1);
    single_item[0] = InlinePermuteItem<int32_t>{-7, 0};
    InlinePermutation<int32_t> scratch_vec;
    radix_sort_inlined(cancel, single_item.data(), scratch_vec, 1, true);
    EXPECT_EQ(scratch_vec.size(), 0U);

    Tie empty_tie(10, 1);
    radix_sort_inlined_and_tie(cancel, single_item.data(), scratch_vec, 5, 5, true, empty_tie);
    EXPECT_EQ(scratch_vec.size(), 0U);
}

TEST(RadixSortTest, CancellationExitsCleanly) {
    const size_t n = 1024;
    InlinePermutation<int32_t> items(n);
    for (size_t i = 0; i < n; ++i) {
        items[i] = InlinePermuteItem<int32_t>{static_cast<int32_t>(n - i), static_cast<uint32_t>(i)};
    }
    InlinePermutation<int32_t> scratch(n);
    Tie tie(n, 1);
    tie[0] = 0;
    bool cancel = true;
    bool range_has_ties = false;

    radix_sort_inlined(cancel, items.data(), scratch.data(), n, true);
    radix_sort_inlined_and_tie(cancel, items.data(), scratch.data(), 0, n, true, tie, range_has_ties);
    EXPECT_FALSE(range_has_ties);

    std::atomic<bool> atomic_cancel{true};
    radix_sort_inlined(atomic_cancel, items.data(), scratch.data(), n, true);
    radix_sort_inlined_and_tie(atomic_cancel, items.data(), scratch.data(), 0, n, true, tie, range_has_ties);
    radix_sort_inlined_and_tie(atomic_cancel, items.data(), scratch.data(), 0, n, true, tie);
    EXPECT_FALSE(range_has_ties);

    // True mid-sort concurrent cancellation across 8 scatter passes using deterministic SyncPoint barrier
    const size_t mid_n = 4096;
    InlinePermutation<uint64_t> work_items(mid_n);
    for (size_t i = 0; i < mid_n; ++i) {
        uint64_t val = static_cast<uint64_t>(i) * 6364136223846793005ULL + 1442695040888963407ULL;
        work_items[i] = InlinePermuteItem<uint64_t>{val, static_cast<uint32_t>(i)};
    }

    InlinePermutation<uint64_t> work_scratch;
    Tie work_tie(mid_n, 1);
    std::atomic<bool> mid_cancel{false};
    std::atomic<int> pass_count{0};
    std::latch in_flight_reached(1);
    std::latch cancel_stored(1);

    auto* sp = SyncPoint::GetInstance();
    sp->ClearAllCallBacks();
    sp->SetCallBack("radix_sort_core::after_scatter_pass", [&](void*) {
        if (pass_count.fetch_add(1, std::memory_order_relaxed) == 0) {
            in_flight_reached.count_down();
            cancel_stored.wait();
        }
    });
    sp->EnableProcessing();

    std::thread worker([&]() {
        radix_sort_inlined_and_tie(mid_cancel, work_items.data(), work_scratch, 0, mid_n, true, work_tie);
        if (pass_count.load(std::memory_order_relaxed) == 0) {
            in_flight_reached.count_down();
        }
    });

    in_flight_reached.wait();
    mid_cancel.store(true, std::memory_order_release);
    cancel_stored.count_down();
    worker.join();

    sp->DisableProcessing();
    sp->ClearAllCallBacks();

    // Pass 0 completed (allocating work_scratch), then Pass 1 observed mid_cancel == true and broke out,
    // and radix_sort_inlined_and_tie skipped detect_ties_in_range.
    EXPECT_EQ(pass_count.load(std::memory_order_relaxed), 1);
    EXPECT_EQ(work_scratch.size(), mid_n);
    EXPECT_EQ(work_tie[0], 1);
    EXPECT_EQ(work_tie[1], 1);
}

TEST(RadixSortTest, LazyScratchAllocationSkippedWhenNoScatterPassesRun) {
    const size_t n = 1024;
    std::atomic<bool> cancel{false};
    InlinePermutation<int64_t> scratch;
    ASSERT_EQ(scratch.capacity(), 0U);

    // 1. Already-sorted input
    {
        InlinePermutation<int64_t> items(n);
        for (size_t i = 0; i < n; ++i) {
            items[i] = InlinePermuteItem<int64_t>{static_cast<int64_t>(i), static_cast<uint32_t>(i)};
        }
        radix_sort_inlined(cancel, items.data(), scratch, n, true);
        EXPECT_EQ(scratch.size(), 0U);
        EXPECT_EQ(scratch.capacity(), 0U);

        for (size_t i = 0; i < n; ++i) {
            items[i] = InlinePermuteItem<int64_t>{static_cast<int64_t>(i), static_cast<uint32_t>(i)};
        }
        Tie tie(n, 1);
        tie[0] = 0;
        radix_sort_inlined_and_tie(cancel, items.data(), scratch, 0, n, true, tie);
        EXPECT_EQ(scratch.size(), 0U);
        EXPECT_EQ(scratch.capacity(), 0U);
    }

    // 2. Reverse-sorted input
    {
        InlinePermutation<int64_t> items(n);
        for (size_t i = 0; i < n; ++i) {
            items[i] = InlinePermuteItem<int64_t>{static_cast<int64_t>(1024 - i), static_cast<uint32_t>(i)};
        }
        radix_sort_inlined(cancel, items.data(), scratch, n, true);
        EXPECT_EQ(scratch.size(), 0U);
        EXPECT_EQ(scratch.capacity(), 0U);

        for (size_t i = 0; i < n; ++i) {
            items[i] = InlinePermuteItem<int64_t>{static_cast<int64_t>(1024 - i), static_cast<uint32_t>(i)};
        }
        Tie tie(n, 1);
        tie[0] = 0;
        radix_sort_inlined_and_tie(cancel, items.data(), scratch, 0, n, true, tie);
        EXPECT_EQ(scratch.size(), 0U);
        EXPECT_EQ(scratch.capacity(), 0U);
    }

    // 3. All-identical input
    {
        InlinePermutation<int64_t> items(n);
        for (size_t i = 0; i < n; ++i) {
            items[i] = InlinePermuteItem<int64_t>{42, static_cast<uint32_t>(i)};
        }
        radix_sort_inlined(cancel, items.data(), scratch, n, true);
        EXPECT_EQ(scratch.size(), 0U);
        EXPECT_EQ(scratch.capacity(), 0U);

        for (size_t i = 0; i < n; ++i) {
            items[i] = InlinePermuteItem<int64_t>{42, static_cast<uint32_t>(i)};
        }
        Tie tie(n, 1);
        tie[0] = 0;
        radix_sort_inlined_and_tie(cancel, items.data(), scratch, 0, n, true, tie);
        EXPECT_EQ(scratch.size(), 0U);
        EXPECT_EQ(scratch.capacity(), 0U);
    }

    // 4. Random unsorted input -> allocates scratch and sorts accurately
    {
        std::mt19937_64 rng(20261025);
        std::vector<int64_t> raw_vals(n);
        InlinePermutation<int64_t> unsorted_items(n);
        for (size_t i = 0; i < n; ++i) {
            raw_vals[i] = static_cast<int64_t>(rng() % 10000) - 5000;
            unsorted_items[i] = InlinePermuteItem<int64_t>{raw_vals[i], static_cast<uint32_t>(i)};
        }
        Tie tie(n, 1);
        tie[0] = 0;
        radix_sort_inlined_and_tie(cancel, unsorted_items.data(), scratch, 0, n, true, tie);
        EXPECT_GE(scratch.size(), n);
        for (size_t i = 1; i < n; ++i) {
            int64_t prev_v = raw_vals[unsorted_items[i - 1].index_in_chunk];
            int64_t curr_v = raw_vals[unsorted_items[i].index_in_chunk];
            EXPECT_LE(prev_v, curr_v);
            if (prev_v == curr_v) {
                EXPECT_EQ(tie[i], 1);
                EXPECT_LT(unsorted_items[i - 1].index_in_chunk, unsorted_items[i].index_in_chunk);
            } else {
                EXPECT_EQ(tie[i], 0);
            }
        }
    }
}

class RadixSortConfigEnv : public ::testing::Environment {
public:
    void SetUp() override {
        setenv("STARROCKS_HOME", ".", 0);
        setenv("UDF_RUNTIME_DIR", ".", 0);
        ASSERT_TRUE(config::init(nullptr));
    }
};

[[maybe_unused]] const ::testing::Environment* const kRadixSortConfigEnv =
        ::testing::AddGlobalTestEnvironment(new RadixSortConfigEnv);

TEST(RadixSortTest, ConfigThresholdDefaultAndMutable) {
    EXPECT_EQ(config::radix_sort_threshold, 256);
    const int32_t old_val = config::radix_sort_threshold;
    config::radix_sort_threshold = 512;
    EXPECT_EQ(config::radix_sort_threshold, 512);
    config::radix_sort_threshold = 0;
    EXPECT_EQ(config::radix_sort_threshold, 0);
    config::radix_sort_threshold = old_val;
    EXPECT_EQ(config::radix_sort_threshold, 256);
}

TEST(RadixSortTest, PublicEntryPointsOverwriteInlineValueWithRadixKeyAcrossAllPaths) {
    bool cancel = false;
    InlinePermutation<int32_t> scratch_i32;

    // 1. Single-element (n == 1) path overwrites inline_value with radix key and preserves index_in_chunk
    {
        InlinePermutation<int32_t> items = {{-42, 7U}};
        radix_sort_inlined(cancel, items.data(), scratch_i32, 1, true);
        EXPECT_EQ(items[0].index_in_chunk, 7U);
        uint32_t raw_bits = 0;
        std::memcpy(&raw_bits, &items[0].inline_value, sizeof(uint32_t));
        EXPECT_EQ(raw_bits, RadixSortTraits<int32_t>::to_radix_key(-42, true));
    }

    // 2. Already-sorted, reverse-sorted, and unsorted paths on float (NaN, -0.0, finite)
    for (bool asc : {true, false}) {
        std::vector<float> vals = {-10.5f, -0.0f, +0.0f, std::numeric_limits<float>::quiet_NaN(), 3.25f, 100.0f};
        if (!asc) {
            std::reverse(vals.begin(), vals.end());
        }
        const size_t n = vals.size();
        InlinePermutation<float> items(n);
        for (size_t i = 0; i < n; ++i) {
            items[i] = InlinePermuteItem<float>{vals[i], static_cast<uint32_t>(i)};
        }
        InlinePermutation<float> scratch_f32;
        Tie tie(n, 1);
        tie[0] = 0;
        radix_sort_inlined_and_tie(cancel, items.data(), scratch_f32, 0, n, asc, tie);

        for (size_t i = 0; i < n; ++i) {
            uint32_t stored_key = 0;
            std::memcpy(&stored_key, &items[i].inline_value, sizeof(uint32_t));
            uint32_t orig_idx = items[i].index_in_chunk;
            EXPECT_EQ(stored_key, RadixSortTraits<float>::to_radix_key(vals[orig_idx], asc));
        }
    }
}

TEST(RadixSortTest, SkewedNearlySortedAndSortedPrefixDistributionsEquivalence) {
    std::mt19937_64 rng(0x5EED1234ULL);
    for (size_t n : {size_t(1), size_t(7), size_t(64), size_t(256), size_t(1024)}) {
        // 1. Non-negative low-cardinality [0, 99] on int64_t (only 1 active scatter pass, 7 skipped)
        std::vector<int64_t> non_neg_low(n);
        for (size_t i = 0; i < n; ++i) {
            non_neg_low[i] = static_cast<int64_t>(rng() % 100);
        }

        // 2. Nearly-sorted (ascending + 1% swaps) on int32_t
        std::vector<int32_t> nearly_sorted(n);
        for (size_t i = 0; i < n; ++i) {
            nearly_sorted[i] = static_cast<int32_t>(static_cast<int64_t>(i) - static_cast<int64_t>(n / 2));
        }
        if (n > 1) {
            const size_t swaps = std::max<size_t>(1, n / 100);
            for (size_t s = 0; s < swaps; ++s) {
                std::swap(nearly_sorted[rng() % n], nearly_sorted[rng() % n]);
            }
        }

        // 3. 90% sorted prefix + 10% random tail on double
        std::vector<double> prefix_sorted(n);
        const size_t prefix = (n * 9) / 10;
        for (size_t i = 0; i < prefix; ++i) {
            prefix_sorted[i] = static_cast<double>(static_cast<int64_t>(i) - static_cast<int64_t>(n / 2));
        }
        for (size_t i = prefix; i < n; ++i) {
            prefix_sorted[i] = static_cast<double>(static_cast<int64_t>(rng() % 2000) - 1000) * 0.125;
        }

        // 4. Zipfian heavy-hitter skew (80% in 4 hot keys, 20% in [0, 1023]) on int64_t
        std::vector<int64_t> zipfian(n);
        for (size_t i = 0; i < n; ++i) {
            zipfian[i] =
                    ((rng() % 5) != 0) ? static_cast<int64_t>(rng() & 3ULL) : static_cast<int64_t>(rng() & 1023ULL);
        }

        for (bool asc : {true, false}) {
            verify_radix_sort_equivalence(non_neg_low, asc);
            verify_radix_sort_equivalence(nearly_sorted, asc);
            verify_radix_sort_equivalence(prefix_sorted, asc);
            verify_radix_sort_equivalence(zipfian, asc);
        }
    }
}

} // namespace
} // namespace starrocks
