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

#include <algorithm>
#include <cstring>
#include <functional>
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

PARALLEL_TEST(SimdFilterTest, non_normalized_selector) {
    // Any non-zero selector byte keeps the row, including bytes with the high bit
    // set (0x80..0xff) -- the vector paths must not treat those as dropped. Use
    // >= 32 elements so the AVX2/AVX-512 batch path (not just the tail) runs.
    constexpr size_t kN = 40;
    std::vector<int32_t> values(kN);
    std::vector<int32_t> expected;
    std::vector<uint8_t> selector(kN);
    for (size_t i = 0; i < kN; ++i) {
        values[i] = static_cast<int32_t>(i);
        // keep even indices, marked with assorted non-zero (incl. high-bit) bytes.
        selector[i] = (i % 2 == 0) ? static_cast<uint8_t>(0x80 | (i & 0x7f)) : 0;
        if (selector[i]) expected.push_back(values[i]);
    }
    size_t size = filter_range(values.data(), values.data(), selector, 0, kN);
    ASSERT_EQ(expected.size(), size);
    for (size_t i = 0; i < size; ++i) EXPECT_EQ(expected[i], values[i]);
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

struct Bytes16 {
    uint64_t lo;
    uint64_t hi;
};

template <typename T>
using FilterFn = std::function<size_t(T*, const T*, const uint8_t*, size_t, size_t)>;

template <typename T>
static T make_value(uint64_t i) {
    uint64_t bits[4] = {
            i * 0x9E3779B97F4A7C15ull + 1,
            (i ^ 0xBF58476D1CE4E5B9ull) * 0x94D049BB133111EBull + 3,
            (i + 7) * 0xD6E8FEB86659FD93ull + 5,
            (i ^ 0xA5A5A5A55A5A5A5Aull) + 7,
    };
    static_assert(sizeof(T) <= sizeof(bits));
    T v;
    std::memcpy(&v, bits, sizeof(T));
    return v;
}

template <typename T>
static void expect_matches_reference(const FilterFn<T>& fn, size_t n, size_t from, size_t to,
                                     const std::vector<uint8_t>& sel, bool in_place, const char* what) {
    std::vector<T> expected;
    for (size_t i = from; i < to; ++i) {
        if (sel[i]) expected.push_back(make_value<T>(i));
    }

    // Pass 1: Exact-size buffers (length == to) so ASAN redzones trap any 1-byte over-read/over-write past `to`.
    {
        std::vector<T> src_exact(to);
        for (size_t i = 0; i < to; ++i) src_exact[i] = make_value<T>(i);
        std::vector<uint8_t> sel_exact(sel.begin(), sel.begin() + to);
        std::vector<T> dst_exact = src_exact;
        if (!in_place) {
            for (auto& v : dst_exact) v = make_value<T>(0xDEAD0000ull);
        }
        const std::vector<T> ref_head = in_place ? src_exact : dst_exact;
        T* d = in_place ? src_exact.data() : dst_exact.data();
        std::vector<T>& out = in_place ? src_exact : dst_exact;
        const size_t got = fn(d, src_exact.data(), sel_exact.data(), from, to);
        ASSERT_EQ(from + expected.size(), got) << what << " exact n=" << n << " from=" << from << " to=" << to;
        for (size_t i = 0; i < expected.size(); ++i) {
            ASSERT_EQ(0, std::memcmp(&expected[i], &out[from + i], sizeof(T))) << what << " exact at " << i;
        }
        for (size_t i = 0; i < from; ++i) {
            ASSERT_EQ(0, std::memcmp(&ref_head[i], &out[i], sizeof(T))) << what << " exact head " << i;
        }
    }

    // Pass 2: Canary-padded buffers (length == n + 16) verifying [to, n + 16) is never mutated.
    {
        std::vector<T> src(n + 16);
        for (size_t i = 0; i < src.size(); ++i) src[i] = make_value<T>(i);
        std::vector<T> dst = src;
        if (!in_place) {
            for (auto& v : dst) v = make_value<T>(0xDEAD0000ull);
        }
        const std::vector<T> ref_outside = in_place ? src : dst;
        T* d = in_place ? src.data() : dst.data();
        std::vector<T>& out = in_place ? src : dst;
        const size_t got = fn(d, src.data(), sel.data(), from, to);
        ASSERT_EQ(from + expected.size(), got) << what << " padded n=" << n << " from=" << from << " to=" << to;
        for (size_t i = 0; i < expected.size(); ++i) {
            ASSERT_EQ(0, std::memcmp(&expected[i], &out[from + i], sizeof(T))) << what << " padded at " << i;
        }
        for (size_t i = 0; i < from; ++i) {
            ASSERT_EQ(0, std::memcmp(&ref_outside[i], &out[i], sizeof(T))) << what << " padded head " << i;
        }
        for (size_t i = to; i < out.size(); ++i) {
            ASSERT_EQ(0, std::memcmp(&ref_outside[i], &out[i], sizeof(T))) << what << " padded tail " << i;
        }
    }
}

template <typename T>
static FilterFn<T> public_api() {
    return [](T* d, const T* s, const uint8_t* sel, size_t f, size_t t) {
        return SIMD::Filter::detail::filter_range(d, s, sizeof(T), sel, f, t);
    };
}

PARALLEL_TEST(SimdFilterTest, neon_lut_and_bit_condensation_invariants) {
    // 1. Verify all 16 rows of 4-byte permutation LUT and popcount LUT.
    for (uint32_t m = 0; m < 16; ++m) {
        ASSERT_EQ(static_cast<uint8_t>(__builtin_popcount(m)), SIMD::Filter::detail::kPopcountLut4B[m]);
        size_t pos = 0;
        for (uint32_t lane = 0; lane < 4; ++lane) {
            if ((m >> lane) & 1u) {
                for (uint32_t b = 0; b < 4; ++b) {
                    ASSERT_EQ(static_cast<uint8_t>(lane * 4 + b), SIMD::Filter::detail::kPermuteLut4B[m][pos++]);
                }
            }
        }
        while (pos < 16) {
            ASSERT_EQ(0xFFu, SIMD::Filter::detail::kPermuteLut4B[m][pos++]);
        }
    }

    // 2. Verify all 4 rows of 8-byte permutation LUT and popcount LUT.
    for (uint32_t m = 0; m < 4; ++m) {
        ASSERT_EQ(static_cast<uint8_t>(__builtin_popcount(m)), SIMD::Filter::detail::kPopcountLut8B[m]);
        size_t pos = 0;
        for (uint32_t lane = 0; lane < 2; ++lane) {
            if ((m >> lane) & 1u) {
                for (uint32_t b = 0; b < 8; ++b) {
                    ASSERT_EQ(static_cast<uint8_t>(lane * 8 + b), SIMD::Filter::detail::kPermuteLut8B[m][pos++]);
                }
            }
        }
        while (pos < 16) {
            ASSERT_EQ(0xFFu, SIMD::Filter::detail::kPermuteLut8B[m][pos++]);
        }
    }

    // 3. Exhaustively verify bit-condensation across all 2^16 = 65,536 16-bit masks.
    for (uint32_t mask16 = 0; mask16 < (1u << 16); ++mask16) {
        uint64_t nibble_mask = 0;
        for (uint32_t bit = 0; bit < 16; ++bit) {
            if ((mask16 >> bit) & 1u) {
                nibble_mask |= (0xFull << (bit * 4));
            }
        }
        for (int k = 0; k < 4; ++k) {
            const uint32_t expected4 = (mask16 >> (k * 4)) & 0xFu;
            ASSERT_EQ(expected4, SIMD::Filter::detail::extract_neon_mask4(nibble_mask, k))
                    << "mask16=" << mask16 << " k=" << k;
        }
        for (int k = 0; k < 8; ++k) {
            const uint32_t expected2 = (mask16 >> (k * 2)) & 0x3u;
            ASSERT_EQ(expected2, SIMD::Filter::detail::extract_neon_mask2(nibble_mask, k))
                    << "mask16=" << mask16 << " k=" << k;
        }
    }
}

PARALLEL_TEST(SimdFilterTest, exact_batch_multiples_and_unaligned_ranges) {
    const size_t sizes[] = {0, 1, 15, 16, 17, 31, 32, 33, 48, 63, 64, 65, 1024};
    const size_t froms[] = {0, 1, 3, 7, 15, 16, 31};
    const uint8_t truthy_bytes[] = {1, 0x02, 0x7F, 0x80, 0xFF};

    for (size_t n : sizes) {
        for (size_t from : froms) {
            if (from > n) continue;
            for (int pattern = 0; pattern < 4; ++pattern) {
                std::vector<uint8_t> sel(n, 0);
                for (size_t i = 0; i < n; ++i) {
                    const uint8_t t = truthy_bytes[i % 5];
                    if (pattern == 0) {
                        sel[i] = 0;
                    } else if (pattern == 1) {
                        sel[i] = t;
                    } else if (pattern == 2) {
                        sel[i] = ((i & 1u) == 0) ? t : 0;
                    } else {
                        sel[i] = (((i * 7 + 3) % 5) < 3) ? t : 0;
                    }
                }
                for (bool in_place : {false, true}) {
                    expect_matches_reference<uint32_t>(public_api<uint32_t>(), n, from, n, sel, in_place, "u32_batch");
                    expect_matches_reference<uint64_t>(public_api<uint64_t>(), n, from, n, sel, in_place, "u64_batch");
                    expect_matches_reference<uint8_t>(public_api<uint8_t>(), n, from, n, sel, in_place, "u8_batch");
                    expect_matches_reference<uint16_t>(public_api<uint16_t>(), n, from, n, sel, in_place, "u16_batch");
                    expect_matches_reference<Bytes16>(public_api<Bytes16>(), n, from, n, sel, in_place, "b16_batch");
                    if (from > 0 && from < n) {
                        expect_matches_reference<uint32_t>(public_api<uint32_t>(), n, from, from, sel, in_place,
                                                           "u32_empty_mid_range");
                        expect_matches_reference<Bytes16>(public_api<Bytes16>(), n, from, from, sel, in_place,
                                                          "b16_empty_mid_range");
                    }
#if defined(__ARM_NEON) && defined(__aarch64__)
                    expect_matches_reference<uint32_t>(SIMD::Filter::detail::compress_neon_w4, n, from, n, sel,
                                                       in_place, "u32_batch_neon");
                    expect_matches_reference<uint64_t>(SIMD::Filter::detail::compress_neon_w8, n, from, n, sel,
                                                       in_place, "u64_batch_neon");
#endif
                }
            }
        }
    }
}

PARALLEL_TEST(SimdFilterTest, exhaustive_subbatch_patterns_and_float_bits) {
    // 1. All 16 4-bit patterns repeated across 16-row batches and tails, plus all 4 2-bit patterns.
    constexpr size_t kN = 37;
    for (uint32_t p4 = 0; p4 < 16; ++p4) {
        std::vector<uint8_t> sel(kN, 0);
        for (size_t i = 0; i < kN; ++i) {
            sel[i] = ((p4 >> (i & 3u)) & 1u) ? static_cast<uint8_t>(0x80u | (i & 0x7Fu)) : 0;
        }
        for (bool in_place : {false, true}) {
            expect_matches_reference<uint32_t>(public_api<uint32_t>(), kN, 0, kN, sel, in_place, "subbatch_u32");
            expect_matches_reference<float>(public_api<float>(), kN, 0, kN, sel, in_place, "subbatch_f32");
            expect_matches_reference<uint64_t>(public_api<uint64_t>(), kN, 0, kN, sel, in_place, "subbatch_u64");
            expect_matches_reference<double>(public_api<double>(), kN, 0, kN, sel, in_place, "subbatch_f64");
#if defined(__ARM_NEON) && defined(__aarch64__)
            expect_matches_reference<uint32_t>(SIMD::Filter::detail::compress_neon_w4, kN, 0, kN, sel, in_place,
                                               "subbatch_u32_neon");
            expect_matches_reference<uint64_t>(SIMD::Filter::detail::compress_neon_w8, kN, 0, kN, sel, in_place,
                                               "subbatch_u64_neon");
#endif
        }
    }

    // 2. Explicit IEEE-754 special values (-0.0, +0.0, +/-inf, distinct NaN payloads) verified via std::memcmp.
    {
        constexpr size_t kSpecialN = 32;
        std::vector<uint32_t> f32_bits(kSpecialN);
        std::vector<uint64_t> f64_bits(kSpecialN);
        for (size_t i = 0; i < kSpecialN; ++i) {
            switch (i % 6) {
            case 0:
                f32_bits[i] = 0x80000000u;           // -0.0f
                f64_bits[i] = 0x8000000000000000ull; // -0.0
                break;
            case 1:
                f32_bits[i] = 0x00000000u;           // +0.0f
                f64_bits[i] = 0x0000000000000000ull; // +0.0
                break;
            case 2:
                f32_bits[i] = 0x7F800000u; // +inf
                f64_bits[i] = 0x7FF0000000000000ull;
                break;
            case 3:
                f32_bits[i] = 0xFF800000u; // -inf
                f64_bits[i] = 0xFFF0000000000000ull;
                break;
            case 4:
                f32_bits[i] = 0x7FC01234u | static_cast<uint32_t>(i); // quiet NaN payload
                f64_bits[i] = 0x7FF8000012345678ull | static_cast<uint64_t>(i);
                break;
            default:
                f32_bits[i] = 0xFFCABCDEu ^ static_cast<uint32_t>(i * 17); // negative NaN payload
                f64_bits[i] = 0xFFF8ABCDE0123456ull ^ static_cast<uint64_t>(i * 17);
                break;
            }
        }
        std::vector<float> src_f32(kSpecialN), dst_f32(kSpecialN);
        std::vector<double> src_f64(kSpecialN), dst_f64(kSpecialN);
        std::memcpy(src_f32.data(), f32_bits.data(), kSpecialN * sizeof(float));
        std::memcpy(src_f64.data(), f64_bits.data(), kSpecialN * sizeof(double));

        std::vector<uint8_t> sel(kSpecialN);
        std::vector<uint32_t> exp_f32;
        std::vector<uint64_t> exp_f64;
        for (size_t i = 0; i < kSpecialN; ++i) {
            sel[i] = ((i * 5 + 1) % 3 != 0) ? 1 : 0;
            if (sel[i]) {
                exp_f32.push_back(f32_bits[i]);
                exp_f64.push_back(f64_bits[i]);
            }
        }

        size_t n32 = SIMD::Filter::filter_range(dst_f32.data(), src_f32.data(), sel.data(), 0, kSpecialN);
        size_t n64 = SIMD::Filter::filter_range(dst_f64.data(), src_f64.data(), sel.data(), 0, kSpecialN);
        ASSERT_EQ(exp_f32.size(), n32);
        ASSERT_EQ(exp_f64.size(), n64);
        ASSERT_EQ(0, std::memcmp(exp_f32.data(), dst_f32.data(), n32 * sizeof(float)));
        ASSERT_EQ(0, std::memcmp(exp_f64.data(), dst_f64.data(), n64 * sizeof(double)));
    }
}

PARALLEL_TEST(SimdFilterTest, sparse_four_byte_batches) {
    static_assert(SIMD::Filter::detail::kNeonCompressMinBitsW4 == 3);
    static_assert(SIMD::Filter::detail::kNeonCompressMinBitsW8 == 6);
    // Exercise 0, 1, 2 (CTZ sparse path), 3, 4 (vqtbl1q_u8 path), 15, 16 survivors per 16-row batch
    // across all 16 rotations, including non-zero `from` and unaligned `kN = 37` tail.
    const int counts[] = {0, 1, 2, 3, 4, 15, 16};
    const uint8_t truthy_bytes[] = {1, 0x02, 0x7F, 0x80, 0xFF};
    constexpr size_t kN = 37;
    for (int cnt : counts) {
        for (int rot = 0; rot < 16; ++rot) {
            std::vector<uint8_t> sel(kN, 0);
            for (int b = 0; b < cnt; ++b) {
                size_t pos0 = (rot + b * 3) & 15;
                size_t pos1 = 16 + ((rot + b * 5) & 15);
                while (sel[pos0]) pos0 = (pos0 + 1) & 15;
                while (sel[pos1]) pos1 = 16 + ((pos1 + 1) & 15);
                sel[pos0] = truthy_bytes[(rot + b) % 5];
                sel[pos1] = truthy_bytes[(rot + b + 2) % 5];
            }
            // Populate tail [32, 37)
            for (size_t t = 32; t < kN; ++t) {
                sel[t] = ((t + rot + cnt) & 1u) ? truthy_bytes[t % 5] : 0;
            }
            for (size_t from : {size_t(0), size_t(3)}) {
                for (bool in_place : {false, true}) {
                    expect_matches_reference<uint32_t>(public_api<uint32_t>(), kN, from, kN, sel, in_place,
                                                       "sparse_w4_u32_api");
                    expect_matches_reference<float>(public_api<float>(), kN, from, kN, sel, in_place,
                                                    "sparse_w4_f32_api");
#if defined(__ARM_NEON) && defined(__aarch64__)
                    expect_matches_reference<uint32_t>(SIMD::Filter::detail::compress_neon_w4, kN, from, kN, sel,
                                                       in_place, "sparse_w4_u32_neon");
#endif
                }
            }
        }
    }
}

PARALLEL_TEST(SimdFilterTest, sparse_eight_byte_batches) {
    // Exercise 0, 1, 2, 5, 6, 7, 15, 16 survivors per 16-row batch at every rotation (0..15).
    const int counts[] = {0, 1, 2, 5, 6, 7, 15, 16};
    constexpr size_t kN = 32;
    for (int cnt : counts) {
        for (int rot = 0; rot < 16; ++rot) {
            std::vector<uint8_t> sel(kN, 0);
            for (int b = 0; b < cnt; ++b) {
                size_t pos0 = (rot + b * 3) & 15;
                size_t pos1 = 16 + ((rot + b * 5) & 15);
                // Ensure exact popcount by falling back to linear scan if modular step collides
                while (sel[pos0]) pos0 = (pos0 + 1) & 15;
                while (sel[pos1]) pos1 = 16 + ((pos1 + 1) & 15);
                sel[pos0] = 1;
                sel[pos1] = 0x80;
            }
            for (bool in_place : {false, true}) {
                expect_matches_reference<uint64_t>(public_api<uint64_t>(), kN, 0, kN, sel, in_place, "sparse_w8_api");
#if defined(__ARM_NEON) && defined(__aarch64__)
                expect_matches_reference<uint64_t>(SIMD::Filter::detail::compress_neon_w8, kN, 0, kN, sel, in_place,
                                                   "sparse_w8_neon");
#endif
            }
        }
    }
}

PARALLEL_TEST(SimdFilterTest, randomized_differential_fuzz) {
    std::mt19937 rng(0xC0FFEE42u);
    constexpr int kIterations = 2000;
    for (int iter = 0; iter < kIterations; ++iter) {
        const size_t n = std::uniform_int_distribution<size_t>(0, 256)(rng);
        const size_t from = (n == 0) ? 0 : std::uniform_int_distribution<size_t>(0, n)(rng);
        const size_t to = (from == n) ? n : std::uniform_int_distribution<size_t>(from, n)(rng);
        const int keep_pct = std::uniform_int_distribution<int>(0, 100)(rng);
        const size_t cluster = (iter % 4 == 0) ? 16 : ((iter % 4 == 1) ? 4 : 1);
        const bool in_place = (iter & 1) != 0;

        std::vector<uint8_t> sel(n, 0);
        std::uniform_int_distribution<int> pct_dist(0, 99);
        std::uniform_int_distribution<int> byte_dist(1, 255);
        for (size_t i = 0; i < n; i += cluster) {
            const uint8_t keep = (pct_dist(rng) < keep_pct) ? static_cast<uint8_t>(byte_dist(rng)) : 0;
            for (size_t j = i; j < std::min(n, i + cluster); ++j) {
                sel[j] = keep;
            }
        }

        expect_matches_reference<uint32_t>(public_api<uint32_t>(), n, from, to, sel, in_place, "fuzz_u32");
        expect_matches_reference<uint64_t>(public_api<uint64_t>(), n, from, to, sel, in_place, "fuzz_u64");
#if defined(__ARM_NEON) && defined(__aarch64__)
        expect_matches_reference<uint32_t>(SIMD::Filter::detail::compress_neon_w4, n, from, to, sel, in_place,
                                           "fuzz_neon_w4");
        expect_matches_reference<uint64_t>(SIMD::Filter::detail::compress_neon_w8, n, from, to, sel, in_place,
                                           "fuzz_neon_w8");
#endif
    }
}

#if defined(__ARM_NEON) && defined(__aarch64__)
// Gate G2 note: 128-bit SVE2 svcompact_u32/u64 kernels were evaluated on Arm Neoverse-V2 (c4a) and pruned
// because 4-pipe NEON vqtbl1q_u8 LUT compaction (compress_neon_w4/w8) outperformed 1-pipe 128-bit COMPACT.
PARALLEL_TEST(SimdFilterTest, aarch64_explicit_neon_kernels) {
    const size_t sizes[] = {0, 1, 3, 4, 7, 15, 16, 17, 31, 32, 33, 64, 129};
    for (size_t n : sizes) {
        for (size_t from : {size_t(0), size_t(1), size_t(15), size_t(16)}) {
            if (from > n) continue;
            for (int mode = 0; mode < 3; ++mode) {
                std::vector<uint8_t> sel(n, 0);
                for (size_t i = 0; i < n; ++i) {
                    if (mode == 0) {
                        sel[i] = 0;
                    } else if (mode == 1) {
                        sel[i] = 0xFF;
                    } else {
                        sel[i] = ((i * 11 + 3) % 7 < 4) ? static_cast<uint8_t>(1 + (i & 0x7F)) : 0;
                    }
                }
                for (bool in_place : {false, true}) {
                    expect_matches_reference<uint32_t>(SIMD::Filter::detail::compress_neon_w4, n, from, n, sel,
                                                       in_place, "explicit_neon_w4");
                    expect_matches_reference<uint64_t>(SIMD::Filter::detail::compress_neon_w8, n, from, n, sel,
                                                       in_place, "explicit_neon_w8");
                }
            }
        }
    }
}
#endif

} // namespace starrocks
