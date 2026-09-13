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
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <limits>
#include <random>
#include <type_traits>
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

PARALLEL_TEST(SimdFilterTest, cross_platform_stream_compaction_sweep) {
    const std::vector<size_t> test_sizes = {7, 15, 16, 31, 32, 63, 64, 127, 128, 255, 256, 1024, 4096};
    const std::vector<int> selectivities = {0, 10, 25, 50, 75, 90, 100};

    // Test both 32-bit and 64-bit elements
    for (size_t n : test_sizes) {
        for (int sel_pct : selectivities) {
            // 1. Test int32_t out-of-place and in-place
            {
                std::vector<int32_t> src(n);
                std::vector<int32_t> dst(n, -1);
                std::vector<uint8_t> selector(n, 0);
                std::vector<int32_t> expected;
                expected.reserve(n);

                for (size_t i = 0; i < n; ++i) {
                    src[i] = static_cast<int32_t>(1000 + i);
                    if ((i * 37 + sel_pct) % 100 < sel_pct) {
                        selector[i] = static_cast<uint8_t>((i % 3 == 0) ? 0x80 : ((i % 5) + 1));
                        expected.push_back(src[i]);
                    }
                }

                // Out-of-place test
                size_t actual_count = SIMD::Filter::filter_range(dst.data(), src.data(), selector.data(), 0, n);
                ASSERT_EQ(expected.size(), actual_count) << "int32 out-of-place n=" << n << " sel=" << sel_pct;
                for (size_t i = 0; i < actual_count; ++i) {
                    ASSERT_EQ(expected[i], dst[i]) << "mismatch at " << i << " n=" << n;
                }

                // In-place test
                std::vector<int32_t> in_place_buf = src;
                size_t in_place_count =
                        SIMD::Filter::filter_range(in_place_buf.data(), in_place_buf.data(), selector.data(), 0, n);
                ASSERT_EQ(expected.size(), in_place_count) << "int32 in-place n=" << n << " sel=" << sel_pct;
                for (size_t i = 0; i < in_place_count; ++i) {
                    ASSERT_EQ(expected[i], in_place_buf[i]) << "in-place mismatch at " << i << " n=" << n;
                }
            }

            // 2. Test int64_t out-of-place and in-place
            {
                std::vector<int64_t> src(n);
                std::vector<int64_t> dst(n, -1);
                std::vector<uint8_t> selector(n, 0);
                std::vector<int64_t> expected;
                expected.reserve(n);

                for (size_t i = 0; i < n; ++i) {
                    src[i] = static_cast<int64_t>(100000000000LL + i);
                    if ((i * 37 + sel_pct) % 100 < sel_pct) {
                        selector[i] = static_cast<uint8_t>((i % 2 == 0) ? 1 : 255);
                        expected.push_back(src[i]);
                    }
                }

                size_t actual_count = SIMD::Filter::filter_range(dst.data(), src.data(), selector.data(), 0, n);
                ASSERT_EQ(expected.size(), actual_count) << "int64 out-of-place n=" << n << " sel=" << sel_pct;
                for (size_t i = 0; i < actual_count; ++i) {
                    ASSERT_EQ(expected[i], dst[i]) << "int64 mismatch at " << i << " n=" << n;
                }
            }

            // 3. Test sub-range [from, to) with non-zero offsets
            if (n >= 32) {
                size_t from = 5;
                size_t to = n - 7;
                std::vector<int32_t> src(n);
                std::vector<uint8_t> selector(n, 0);
                std::vector<int32_t> expected_in_place(n);

                for (size_t i = 0; i < n; ++i) {
                    src[i] = static_cast<int32_t>(5000 + i);
                    expected_in_place[i] = src[i];
                    if (i >= from && i < to && ((i * 37 + sel_pct) % 100 < sel_pct)) {
                        selector[i] = 1;
                    }
                }

                size_t write_idx = from;
                for (size_t i = from; i < to; ++i) {
                    if (selector[i]) {
                        expected_in_place[write_idx++] = src[i];
                    }
                }

                std::vector<int32_t> in_place_buf = src;
                size_t res_end =
                        SIMD::Filter::filter_range(in_place_buf.data(), in_place_buf.data(), selector.data(), from, to);
                ASSERT_EQ(write_idx, res_end);
                // Elements before 'from' must remain untouched
                for (size_t i = 0; i < from; ++i) {
                    ASSERT_EQ(src[i], in_place_buf[i]);
                }
                // Compacted elements in [from, res_end) must match
                for (size_t i = from; i < res_end; ++i) {
                    ASSERT_EQ(expected_in_place[i], in_place_buf[i]);
                }
            }
        }
    }
}

namespace {

struct Fixed3 {
    uint8_t v[3];
    bool operator==(const Fixed3& o) const { return std::memcmp(v, o.v, 3) == 0; }
};

struct Fixed12 {
    uint32_t v[3];
    bool operator==(const Fixed12& o) const { return v[0] == o.v[0] && v[1] == o.v[1] && v[2] == o.v[2]; }
};

struct Fixed16 {
    uint64_t v[2];
    bool operator==(const Fixed16& o) const { return v[0] == o.v[0] && v[1] == o.v[1]; }
};

struct Fixed32 {
    uint64_t v[4];
    bool operator==(const Fixed32& o) const { return std::memcmp(v, o.v, 32) == 0; }
};

struct Fixed48 {
    uint64_t v[6];
    bool operator==(const Fixed48& o) const { return std::memcmp(v, o.v, 48) == 0; }
};

static_assert(sizeof(Fixed3) == 3 && std::is_standard_layout<Fixed3>::value, "Fixed3 must be 3 bytes standard layout");
static_assert(sizeof(Fixed12) == 12 && std::is_standard_layout<Fixed12>::value,
              "Fixed12 must be 12 bytes standard layout");
static_assert(sizeof(Fixed16) == 16 && std::is_standard_layout<Fixed16>::value,
              "Fixed16 must be 16 bytes standard layout");
static_assert(sizeof(Fixed32) == 32 && std::is_standard_layout<Fixed32>::value,
              "Fixed32 must be 32 bytes standard layout");
static_assert(sizeof(Fixed48) == 48 && std::is_standard_layout<Fixed48>::value,
              "Fixed48 must be 48 bytes standard layout");

template <typename T, typename InitFn>
static void check_width_dispatch(size_t n, int sel_pct, InitFn&& init_fn) {
    std::vector<T> src(n);
    std::vector<T> dst(n);
    std::vector<uint8_t> selector(n);
    std::vector<T> expected;
    expected.reserve(n);

    for (size_t i = 0; i < n; ++i) {
        src[i] = init_fn(i);
        bool keep = ((i * 37 + sel_pct) % 100 < sel_pct);
        selector[i] = keep ? static_cast<uint8_t>((i % 5) + 1) : 0;
        if (keep) {
            expected.push_back(src[i]);
        }
    }

    // Out-of-place
    size_t out_count = filter_range(dst.data(), src.data(), selector, 0, n);
    ASSERT_EQ(expected.size(), out_count) << "width=" << sizeof(T) << " n=" << n << " sel=" << sel_pct;
    for (size_t i = 0; i < out_count; ++i) {
        ASSERT_EQ(expected[i], dst[i]) << "mismatch at " << i << " width=" << sizeof(T) << " n=" << n;
    }

    // In-place
    std::vector<T> in_place = src;
    size_t in_count = filter_range(in_place.data(), in_place.data(), selector, 0, n);
    ASSERT_EQ(expected.size(), in_count) << "in-place width=" << sizeof(T) << " n=" << n << " sel=" << sel_pct;
    for (size_t i = 0; i < in_count; ++i) {
        ASSERT_EQ(expected[i], in_place[i]) << "in-place mismatch at " << i << " width=" << sizeof(T) << " n=" << n;
    }
}

} // namespace

PARALLEL_TEST(SimdFilterTest, all_width_dispatch_matrix) {
    const std::vector<size_t> test_sizes = {15, 32, 64, 97};
    const std::vector<int> selectivities = {0, 33, 50, 100};

    for (size_t n : test_sizes) {
        for (int sel_pct : selectivities) {
            // Width 1
            check_width_dispatch<uint8_t>(n, sel_pct, [](size_t i) { return static_cast<uint8_t>(i & 0xff); });
            // Width 2
            check_width_dispatch<uint16_t>(n, sel_pct, [](size_t i) { return static_cast<uint16_t>(i * 17 + 1); });
            // Width 3 (Bytes<3>)
            check_width_dispatch<Fixed3>(n, sel_pct, [](size_t i) {
                return Fixed3{{static_cast<uint8_t>(i), static_cast<uint8_t>(i + 1), static_cast<uint8_t>(i + 2)}};
            });
            // Width 4
            check_width_dispatch<uint32_t>(n, sel_pct, [](size_t i) { return static_cast<uint32_t>(i * 10007 + 5); });
            // Width 8
            check_width_dispatch<uint64_t>(n, sel_pct,
                                           [](size_t i) { return static_cast<uint64_t>(i * 1000000007ULL + 11); });
            // Width 12 (Bytes<12>)
            check_width_dispatch<Fixed12>(n, sel_pct, [](size_t i) {
                return Fixed12{{static_cast<uint32_t>(i), static_cast<uint32_t>(i * 2), static_cast<uint32_t>(i * 3)}};
            });
            // Width 16 (Bytes<16>)
            check_width_dispatch<Fixed16>(n, sel_pct, [](size_t i) {
                return Fixed16{{static_cast<uint64_t>(i), static_cast<uint64_t>(i * 100)}};
            });
            // Width 32 (Bytes<32>)
            check_width_dispatch<Fixed32>(n, sel_pct, [](size_t i) { return Fixed32{{i, i + 1, i + 2, i + 3}}; });
            // Width 48 (scan_scalar_bytes fallback)
            check_width_dispatch<Fixed48>(n, sel_pct, [](size_t i) {
                return Fixed48{{i, i * 2, i * 3, i * 4, i * 5, i * 6}};
            });
        }
    }
}

PARALLEL_TEST(SimdFilterTest, batch_fast_paths_and_subgroups) {
    constexpr size_t kN = 128; // 4 full 32-lane batches or 8 full 16-lane batches
    std::vector<uint32_t> src(kN);
    for (size_t i = 0; i < kN; ++i) {
        src[i] = static_cast<uint32_t>(2000 + i);
    }

    // 1. All-dropped fast path (mask == 0 across batches)
    {
        std::vector<uint32_t> dst(kN, 0xdeadbeef);
        std::vector<uint8_t> all_zero(kN, 0);
        size_t count = filter_range(dst.data(), src.data(), all_zero, 0, kN);
        EXPECT_EQ(0, count);
        // Ensure dst was not overwritten
        for (size_t i = 0; i < kN; ++i) {
            EXPECT_EQ(0xdeadbeef, dst[i]);
        }

        // In-place validation
        std::vector<uint32_t> in_place = src;
        size_t in_count = filter_range(in_place.data(), in_place.data(), all_zero, 0, kN);
        EXPECT_EQ(0, in_count);
        for (size_t i = 0; i < kN; ++i) {
            ASSERT_EQ(src[i], in_place[i]);
        }
    }

    // 2. All-kept fast path (mask == 0xffffffff across batches)
    {
        std::vector<uint32_t> dst(kN, 0);
        std::vector<uint8_t> all_one(kN, 1);
        size_t count = filter_range(dst.data(), src.data(), all_one, 0, kN);
        EXPECT_EQ(kN, count);
        for (size_t i = 0; i < kN; ++i) {
            EXPECT_EQ(src[i], dst[i]);
        }

        // In-place validation
        std::vector<uint32_t> in_place = src;
        size_t in_count = filter_range(in_place.data(), in_place.data(), all_one, 0, kN);
        EXPECT_EQ(kN, in_count);
        for (size_t i = 0; i < kN; ++i) {
            EXPECT_EQ(src[i], in_place[i]);
        }
    }

    // 3. Subgroup mask patterns for 4-byte compaction:
    //    Batch 0 (0..31): group 0 all-drop (0), group 1 all-keep (8), group 2 alternating 01010101 (4), group 3 alternating 10101010 (4)
    //    Batch 1 (32..63): group 0 keep (8), group 1 drop (0), group 2 keep (8), group 3 drop (0)
    //    Batch 2 (64..95): sparse batch (only 3 elements kept total in 32 lanes, exercises popcount < 6)
    //    Batch 3 (96..127): dense batch (30 elements kept total, 2 dropped)
    {
        std::vector<uint8_t> selector(kN, 0);
        std::vector<uint32_t> expected;

        // Batch 0
        for (size_t i = 8; i < 16; ++i) selector[i] = 1;     // group 1: all keep
        for (size_t i = 16; i < 24; i += 2) selector[i] = 1; // group 2: even keep
        for (size_t i = 25; i < 32; i += 2) selector[i] = 1; // group 3: odd keep

        // Batch 1
        for (size_t i = 32; i < 40; ++i) selector[i] = 1; // group 0: all keep
        for (size_t i = 48; i < 56; ++i) selector[i] = 1; // group 2: all keep

        // Batch 2: sparse (3 lanes)
        selector[65] = 1;
        selector[75] = 1;
        selector[85] = 1;

        // Batch 3: dense (drop only 2 lanes)
        for (size_t i = 96; i < 128; ++i) selector[i] = 1;
        selector[100] = 0;
        selector[120] = 0;

        for (size_t i = 0; i < kN; ++i) {
            if (selector[i]) expected.push_back(src[i]);
        }

        std::vector<uint32_t> dst(kN, 0);
        size_t count = filter_range(dst.data(), src.data(), selector, 0, kN);
        ASSERT_EQ(expected.size(), count);
        for (size_t i = 0; i < count; ++i) {
            ASSERT_EQ(expected[i], dst[i]);
        }

        // In-place validation
        std::vector<uint32_t> in_place = src;
        size_t in_count = filter_range(in_place.data(), in_place.data(), selector, 0, kN);
        ASSERT_EQ(expected.size(), in_count);
        for (size_t i = 0; i < in_count; ++i) {
            ASSERT_EQ(expected[i], in_place[i]);
        }
    }

    // 4. Subgroup mask patterns for 8-byte compaction:
    //    Batch 0 (0..31): group 0 keep (4), group 1 drop (0), group 2 alternating 0101 (2), group 3 alternating 1010 (2)
    //    Batch 1 (32..63): groups 0..1 keep (8), groups 2..3 drop (0), groups 4..5 keep (8), groups 6..7 drop (0)
    //    Batch 2 (64..95): sparse batch (only 3 elements kept total in 32 lanes, exercises popcount < 6)
    //    Batch 3 (96..127): dense batch (30 elements kept total, 2 dropped)
    {
        std::vector<uint64_t> src8(kN);
        for (size_t i = 0; i < kN; ++i) {
            src8[i] = static_cast<uint64_t>(5000000000ULL + i);
        }

        std::vector<uint8_t> selector8(kN, 0);
        std::vector<uint64_t> expected8;

        // Batch 0
        for (size_t i = 0; i < 4; ++i) selector8[i] = 1;
        for (size_t i = 8; i < 12; i += 2) selector8[i] = 1;
        for (size_t i = 13; i < 16; i += 2) selector8[i] = 1;

        // Batch 1
        for (size_t i = 32; i < 40; ++i) selector8[i] = 1;
        for (size_t i = 48; i < 56; ++i) selector8[i] = 1;

        // Batch 2: sparse (3 lanes)
        selector8[65] = 1;
        selector8[75] = 1;
        selector8[85] = 1;

        // Batch 3: dense (drop only 2 lanes)
        for (size_t i = 96; i < 128; ++i) selector8[i] = 1;
        selector8[100] = 0;
        selector8[120] = 0;

        for (size_t i = 0; i < kN; ++i) {
            if (selector8[i]) expected8.push_back(src8[i]);
        }

        std::vector<uint64_t> dst8(kN, 0);
        size_t count8 = filter_range(dst8.data(), src8.data(), selector8, 0, kN);
        ASSERT_EQ(expected8.size(), count8);
        for (size_t i = 0; i < count8; ++i) {
            ASSERT_EQ(expected8[i], dst8[i]);
        }

        // In-place validation
        std::vector<uint64_t> in_place8 = src8;
        size_t in_count8 = filter_range(in_place8.data(), in_place8.data(), selector8, 0, kN);
        ASSERT_EQ(expected8.size(), in_count8);
        for (size_t i = 0; i < in_count8; ++i) {
            ASSERT_EQ(expected8[i], in_place8[i]);
        }
    }
}

PARALLEL_TEST(SimdFilterTest, unaligned_buffer_offsets) {
    constexpr size_t kN = 80;
    constexpr size_t kPad = 16;

    // 1. 4-byte elements (uint32_t)
    {
        std::vector<uint32_t> src_buf(kN + kPad);
        std::vector<uint32_t> dst_buf(kN + kPad);
        std::vector<uint8_t> sel_buf(kN + kPad);

        for (size_t i = 0; i < kN + kPad; ++i) {
            src_buf[i] = static_cast<uint32_t>(30000 + i);
        }

        // Offset selector by 0, 1, 2, 3 bytes to verify byte-unaligned vector loads
        for (size_t sel_shift : {0, 1, 2, 3}) {
            // Offset element arrays by 0, 1, 2, 3 elements to verify non-vector-aligned pointers
            for (size_t src_shift : {0, 1, 2, 3}) {
                const uint32_t* src = src_buf.data() + src_shift;
                uint8_t* sel = sel_buf.data() + sel_shift;

                std::vector<uint32_t> expected;
                for (size_t i = 0; i < kN; ++i) {
                    bool keep = ((i + sel_shift) % 3 != 0);
                    sel[i] = keep ? static_cast<uint8_t>(0x80 | (i & 0x7f)) : 0;
                    if (keep) {
                        expected.push_back(src[i]);
                    }
                }

                // Out-of-place validation
                for (size_t dst_shift : {0, 1, 2, 3}) {
                    SCOPED_TRACE(testing::Message() << "sel_shift=" << sel_shift << " src_shift=" << src_shift
                                                    << " dst_shift=" << dst_shift);
                    constexpr uint32_t kCanary = 0xdeadbeef;
                    std::fill(dst_buf.begin(), dst_buf.end(), kCanary);
                    uint32_t* dst = dst_buf.data() + dst_shift;
                    size_t count = SIMD::Filter::filter_range(dst, src, sel, 0, kN);
                    ASSERT_EQ(expected.size(), count)
                            << "shift sel=" << sel_shift << " src=" << src_shift << " dst=" << dst_shift;
                    for (size_t i = 0; i < count; ++i) {
                        ASSERT_EQ(expected[i], dst[i]) << "mismatch at " << i;
                    }
                    for (size_t i = 0; i < dst_shift; ++i) {
                        ASSERT_EQ(kCanary, dst_buf[i]) << "canary corrupted before dst_shift at " << i;
                    }
                    for (size_t i = dst_shift + count + 8; i < dst_buf.size(); ++i) {
                        ASSERT_EQ(kCanary, dst_buf[i]) << "canary corrupted after dst_shift + count + 8 at " << i;
                    }
                    for (size_t i = dst_shift + kN; i < dst_buf.size(); ++i) {
                        ASSERT_EQ(kCanary, dst_buf[i]) << "canary corrupted beyond dst_shift + kN at " << i;
                    }
                }

                // In-place validation
                {
                    std::vector<uint32_t> in_place_buf = src_buf;
                    uint32_t* in_place = in_place_buf.data() + src_shift;

                    size_t in_count = SIMD::Filter::filter_range(in_place, in_place, sel, 0, kN);
                    ASSERT_EQ(expected.size(), in_count) << "in-place shift sel=" << sel_shift << " src=" << src_shift;
                    for (size_t i = 0; i < in_count; ++i) {
                        ASSERT_EQ(expected[i], in_place[i]) << "in-place mismatch at " << i;
                    }
                    for (size_t i = 0; i < src_shift; ++i) {
                        ASSERT_EQ(src_buf[i], in_place_buf[i]) << "in-place prefix modified at " << i;
                    }
                }
            }
        }
    }

    // 2. 8-byte elements (uint64_t)
    {
        std::vector<uint64_t> src_buf(kN + kPad);
        std::vector<uint64_t> dst_buf(kN + kPad);
        std::vector<uint8_t> sel_buf(kN + kPad);

        for (size_t i = 0; i < kN + kPad; ++i) {
            src_buf[i] = static_cast<uint64_t>(70000000000ULL + i);
        }

        for (size_t sel_shift : {0, 1, 2, 3}) {
            for (size_t src_shift : {0, 1, 2, 3}) {
                const uint64_t* src = src_buf.data() + src_shift;
                uint8_t* sel = sel_buf.data() + sel_shift;

                std::vector<uint64_t> expected;
                for (size_t i = 0; i < kN; ++i) {
                    bool keep = ((i + sel_shift) % 3 != 0);
                    sel[i] = keep ? static_cast<uint8_t>(0x80 | (i & 0x7f)) : 0;
                    if (keep) {
                        expected.push_back(src[i]);
                    }
                }

                // Out-of-place validation
                for (size_t dst_shift : {0, 1, 2, 3}) {
                    SCOPED_TRACE(testing::Message() << "sel_shift=" << sel_shift << " src_shift=" << src_shift
                                                    << " dst_shift=" << dst_shift);
                    constexpr uint64_t kCanary = 0xdeadbeefdeadbeefULL;
                    std::fill(dst_buf.begin(), dst_buf.end(), kCanary);
                    uint64_t* dst = dst_buf.data() + dst_shift;
                    size_t count = SIMD::Filter::filter_range(dst, src, sel, 0, kN);
                    ASSERT_EQ(expected.size(), count)
                            << "uint64 shift sel=" << sel_shift << " src=" << src_shift << " dst=" << dst_shift;
                    for (size_t i = 0; i < count; ++i) {
                        ASSERT_EQ(expected[i], dst[i]) << "uint64 mismatch at " << i;
                    }
                    for (size_t i = 0; i < dst_shift; ++i) {
                        ASSERT_EQ(kCanary, dst_buf[i]) << "uint64 canary corrupted before dst_shift at " << i;
                    }
                    for (size_t i = dst_shift + count + 8; i < dst_buf.size(); ++i) {
                        ASSERT_EQ(kCanary, dst_buf[i])
                                << "uint64 canary corrupted after dst_shift + count + 8 at " << i;
                    }
                    for (size_t i = dst_shift + kN; i < dst_buf.size(); ++i) {
                        ASSERT_EQ(kCanary, dst_buf[i]) << "uint64 canary corrupted beyond dst_shift + kN at " << i;
                    }
                }

                // In-place validation
                {
                    std::vector<uint64_t> in_place_buf = src_buf;
                    uint64_t* in_place = in_place_buf.data() + src_shift;

                    size_t in_count = SIMD::Filter::filter_range(in_place, in_place, sel, 0, kN);
                    ASSERT_EQ(expected.size(), in_count)
                            << "uint64 in-place shift sel=" << sel_shift << " src=" << src_shift;
                    for (size_t i = 0; i < in_count; ++i) {
                        ASSERT_EQ(expected[i], in_place[i]) << "uint64 in-place mismatch at " << i;
                    }
                    for (size_t i = 0; i < src_shift; ++i) {
                        ASSERT_EQ(src_buf[i], in_place_buf[i]) << "uint64 in-place prefix modified at " << i;
                    }
                }
            }
        }
    }
}

PARALLEL_TEST(SimdFilterTest, degenerate_and_subrange_boundaries) {
    constexpr size_t kN = 100;
    std::vector<int32_t> src(kN);
    for (size_t i = 0; i < kN; ++i) {
        src[i] = static_cast<int32_t>(7000 + i);
    }

    // 1. Degenerate empty ranges (from == to)
    {
        std::vector<uint8_t> empty_sel(kN, 1);
        for (size_t empty_idx : std::initializer_list<size_t>{0, 1, 17, 32, kN - 1, kN}) {
            SCOPED_TRACE(testing::Message() << "empty_idx=" << empty_idx);
            // In-place validation
            std::vector<int32_t> in_place = src;
            size_t ret_in_place = filter_range(in_place.data(), in_place.data(), empty_sel, empty_idx, empty_idx);
            EXPECT_EQ(empty_idx, ret_in_place);
            for (size_t i = 0; i < kN; ++i) {
                EXPECT_EQ(src[i], in_place[i]);
            }

            // Out-of-place validation with canary buffer
            std::vector<int32_t> dst(kN, -1);
            size_t ret_out = filter_range(dst.data(), src.data(), empty_sel, empty_idx, empty_idx);
            EXPECT_EQ(empty_idx, ret_out);
            for (size_t i = 0; i < kN; ++i) {
                EXPECT_EQ(-1, dst[i]);
                EXPECT_EQ(src[i], static_cast<int32_t>(7000 + i));
            }
        }
    }

    // 2. Single-element ranges (to == from + 1)
    {
        std::vector<uint8_t> single_sel(kN, 1);
        for (size_t idx : std::initializer_list<size_t>{0, 15, 31, 32, 63, 64, kN - 1}) {
            SCOPED_TRACE(testing::Message() << "single_elem_idx=" << idx);
            // Dropped case (single_sel[idx] = 0)
            {
                single_sel[idx] = 0;
                // In-place validation
                std::vector<int32_t> in_place = src;
                size_t ret_drop_in = filter_range(in_place.data(), in_place.data(), single_sel, idx, idx + 1);
                EXPECT_EQ(idx, ret_drop_in);
                for (size_t i = 0; i < kN; ++i) {
                    EXPECT_EQ(src[i], in_place[i]);
                }

                // Out-of-place validation
                std::vector<int32_t> dst(kN, -1);
                size_t ret_drop_out = filter_range(dst.data(), src.data(), single_sel, idx, idx + 1);
                EXPECT_EQ(idx, ret_drop_out);
                for (size_t i = 0; i < kN; ++i) {
                    EXPECT_EQ(-1, dst[i]);
                }
            }

            // Kept case (single_sel[idx] = 1)
            {
                single_sel[idx] = 1;
                // In-place validation
                std::vector<int32_t> in_place = src;
                size_t ret_keep_in = filter_range(in_place.data(), in_place.data(), single_sel, idx, idx + 1);
                EXPECT_EQ(idx + 1, ret_keep_in);
                for (size_t i = 0; i < kN; ++i) {
                    EXPECT_EQ(src[i], in_place[i]);
                }

                // Out-of-place validation
                std::vector<int32_t> dst(kN, -1);
                size_t ret_keep_out = filter_range(dst.data(), src.data(), single_sel, idx, idx + 1);
                EXPECT_EQ(idx + 1, ret_keep_out);
                EXPECT_EQ(src[idx], dst[idx]);
                for (size_t i = 0; i < idx; ++i) {
                    EXPECT_EQ(-1, dst[i]);
                }
                for (size_t i = idx + 1; i < kN; ++i) {
                    EXPECT_EQ(-1, dst[i]);
                }
            }
        }
    }

    // 3. Asymmetric prime ranges: from = 13, to = 79 (span = 66 = 32 + 32 + 2)
    {
        size_t from = 13;
        size_t to = 79;
        std::vector<uint8_t> prime_sel(kN, 1);
        std::vector<int32_t> expected_kept;
        for (size_t i = from; i < to; ++i) {
            prime_sel[i] = (i % 2 == 0) ? 1 : 0;
            if (prime_sel[i]) {
                expected_kept.push_back(src[i]);
            }
        }

        // In-place validation
        {
            std::vector<int32_t> in_place = src;
            size_t res_end = filter_range(in_place.data(), in_place.data(), prime_sel, from, to);
            ASSERT_EQ(from + expected_kept.size(), res_end);

            // Prefix [0, from) must remain untouched
            for (size_t i = 0; i < from; ++i) {
                ASSERT_EQ(src[i], in_place[i]);
            }
            // Compacted range [from, res_end) must match expected
            for (size_t i = 0; i < expected_kept.size(); ++i) {
                ASSERT_EQ(expected_kept[i], in_place[from + i]);
            }
            // Suffix [to, kN) must remain untouched
            for (size_t i = to; i < kN; ++i) {
                ASSERT_EQ(src[i], in_place[i]);
            }
        }

        // Out-of-place validation
        {
            std::vector<int32_t> dst(kN, -1);
            size_t res_end = filter_range(dst.data(), src.data(), prime_sel, from, to);
            ASSERT_EQ(from + expected_kept.size(), res_end);

            // Prefix [0, from) must remain untouched (canary)
            for (size_t i = 0; i < from; ++i) {
                ASSERT_EQ(-1, dst[i]);
            }
            // Compacted range [from, res_end) must match expected
            for (size_t i = 0; i < expected_kept.size(); ++i) {
                ASSERT_EQ(expected_kept[i], dst[from + i]);
            }
            // Suffix [to, kN) must remain untouched (canary)
            for (size_t i = to; i < kN; ++i) {
                ASSERT_EQ(-1, dst[i]);
            }
            // Source array must remain untouched
            for (size_t i = 0; i < kN; ++i) {
                ASSERT_EQ(static_cast<int32_t>(7000 + i), src[i]);
            }
        }
    }
}

PARALLEL_TEST(SimdFilterTest, special_values_and_bitwise_fidelity) {
    // 1. Float special values (4-byte vector compaction)
    {
        SCOPED_TRACE("Float special values");
        std::vector<float> values = {
                std::numeric_limits<float>::quiet_NaN(),
                -0.0f,
                +0.0f,
                std::numeric_limits<float>::infinity(),
                -std::numeric_limits<float>::infinity(),
                std::numeric_limits<float>::denorm_min(),
                std::numeric_limits<float>::lowest(),
                std::numeric_limits<float>::max(),
        };
        values.reserve(48);
        // Duplicate to fill 32 lanes so vector path runs
        while (values.size() < 36) {
            values.insert(values.end(), values.begin(), values.begin() + 8);
        }

        for (int parity : {0, 1}) {
            SCOPED_TRACE(testing::Message() << "float parity=" << parity);
            std::vector<uint8_t> selector(values.size());
            std::vector<float> expected;
            for (size_t i = 0; i < values.size(); ++i) {
                selector[i] = (i % 2 == static_cast<size_t>(parity)) ? 1 : 0;
                if (selector[i]) {
                    expected.push_back(values[i]);
                }
            }

            // Out-of-place validation
            {
                SCOPED_TRACE("float out-of-place");
                std::vector<float> dst(values.size(), 0.0f);
                size_t count = filter_range(dst.data(), values.data(), selector, 0, values.size());
                ASSERT_EQ(expected.size(), count);
                for (size_t i = 0; i < count; ++i) {
                    uint32_t exp_bits = 0;
                    uint32_t act_bits = 0;
                    std::memcpy(&exp_bits, &expected[i], sizeof(float));
                    std::memcpy(&act_bits, &dst[i], sizeof(float));
                    ASSERT_EQ(exp_bits, act_bits) << "float out-of-place mismatch at " << i;
                }
            }

            // In-place validation
            {
                SCOPED_TRACE("float in-place");
                std::vector<float> in_place = values;
                size_t count = filter_range(in_place.data(), in_place.data(), selector, 0, values.size());
                ASSERT_EQ(expected.size(), count);
                for (size_t i = 0; i < count; ++i) {
                    uint32_t exp_bits = 0;
                    uint32_t act_bits = 0;
                    std::memcpy(&exp_bits, &expected[i], sizeof(float));
                    std::memcpy(&act_bits, &in_place[i], sizeof(float));
                    ASSERT_EQ(exp_bits, act_bits) << "float in-place mismatch at " << i;
                }
            }
        }
    }

    // 2. Double special values (8-byte vector compaction)
    {
        SCOPED_TRACE("Double special values");
        std::vector<double> values = {
                std::numeric_limits<double>::quiet_NaN(),
                -0.0,
                +0.0,
                std::numeric_limits<double>::infinity(),
                -std::numeric_limits<double>::infinity(),
                std::numeric_limits<double>::denorm_min(),
                std::numeric_limits<double>::lowest(),
                std::numeric_limits<double>::max(),
        };
        values.reserve(48);
        while (values.size() < 36) {
            values.insert(values.end(), values.begin(), values.begin() + 8);
        }

        for (int parity : {0, 1}) {
            SCOPED_TRACE(testing::Message() << "double parity=" << parity);
            std::vector<uint8_t> selector(values.size());
            std::vector<double> expected;
            for (size_t i = 0; i < values.size(); ++i) {
                selector[i] = (i % 2 == static_cast<size_t>(parity)) ? 1 : 0;
                if (selector[i]) {
                    expected.push_back(values[i]);
                }
            }

            // Out-of-place validation
            {
                SCOPED_TRACE("double out-of-place");
                std::vector<double> dst(values.size(), 0.0);
                size_t count = filter_range(dst.data(), values.data(), selector, 0, values.size());
                ASSERT_EQ(expected.size(), count);
                for (size_t i = 0; i < count; ++i) {
                    uint64_t exp_bits = 0;
                    uint64_t act_bits = 0;
                    std::memcpy(&exp_bits, &expected[i], sizeof(double));
                    std::memcpy(&act_bits, &dst[i], sizeof(double));
                    ASSERT_EQ(exp_bits, act_bits) << "double out-of-place mismatch at " << i;
                }
            }

            // In-place validation
            {
                SCOPED_TRACE("double in-place");
                std::vector<double> in_place = values;
                size_t count = filter_range(in_place.data(), in_place.data(), selector, 0, values.size());
                ASSERT_EQ(expected.size(), count);
                for (size_t i = 0; i < count; ++i) {
                    uint64_t exp_bits = 0;
                    uint64_t act_bits = 0;
                    std::memcpy(&exp_bits, &expected[i], sizeof(double));
                    std::memcpy(&act_bits, &in_place[i], sizeof(double));
                    ASSERT_EQ(exp_bits, act_bits) << "double in-place mismatch at " << i;
                }
            }
        }
    }

    // 3. Integer boundaries (8-byte vector compaction)
    {
        SCOPED_TRACE("Integer 64-bit boundaries");
        std::vector<int64_t> values = {
                std::numeric_limits<int64_t>::min(),
                std::numeric_limits<int64_t>::max(),
                -1LL,
                0LL,
                1LL,
                -9223372036854775807LL,
        };
        values.reserve(48);
        while (values.size() < 36) {
            values.insert(values.end(), values.begin(), values.begin() + 6);
        }

        for (int sel_pattern : {0, 1}) {
            SCOPED_TRACE(testing::Message() << "int64 pattern=" << sel_pattern);
            std::vector<uint8_t> selector(values.size());
            std::vector<int64_t> expected;
            for (size_t i = 0; i < values.size(); ++i) {
                selector[i] = (sel_pattern == 0) ? ((i % 3 != 0) ? 1 : 0) : ((i % 3 == 0) ? 1 : 0);
                if (selector[i]) {
                    expected.push_back(values[i]);
                }
            }

            // Out-of-place validation
            {
                SCOPED_TRACE("int64 out-of-place");
                std::vector<int64_t> dst(values.size(), 0);
                size_t count = filter_range(dst.data(), values.data(), selector, 0, values.size());
                ASSERT_EQ(expected.size(), count);
                for (size_t i = 0; i < count; ++i) {
                    ASSERT_EQ(expected[i], dst[i]) << "int64 out-of-place mismatch at " << i;
                }
            }

            // In-place validation
            {
                SCOPED_TRACE("int64 in-place");
                std::vector<int64_t> in_place = values;
                size_t count = filter_range(in_place.data(), in_place.data(), selector, 0, values.size());
                ASSERT_EQ(expected.size(), count);
                for (size_t i = 0; i < count; ++i) {
                    ASSERT_EQ(expected[i], in_place[i]) << "int64 in-place mismatch at " << i;
                }
            }
        }
    }
}

PARALLEL_TEST(SimdFilterTest, large_scale_bursty_stress) {
    constexpr size_t kN = 65536;
    std::vector<uint32_t> src(kN);
    std::vector<uint8_t> selector(kN, 0);
    std::vector<uint32_t> expected;
    expected.reserve(kN);

    // Create bursty runs:
    // run of 512 zeros, run of 1024 ones, run of 256 alternating, run of 128 sparse (1/32)
    size_t idx = 0;
    while (idx < kN) {
        // Run of zeros
        size_t zero_run = std::min<size_t>(512, kN - idx);
        for (size_t i = 0; i < zero_run; ++i) {
            src[idx + i] = static_cast<uint32_t>(idx + i);
            selector[idx + i] = 0;
        }
        idx += zero_run;
        if (idx >= kN) break;

        // Run of ones
        size_t one_run = std::min<size_t>(1024, kN - idx);
        for (size_t i = 0; i < one_run; ++i) {
            src[idx + i] = static_cast<uint32_t>(idx + i);
            selector[idx + i] = 1;
            expected.push_back(src[idx + i]);
        }
        idx += one_run;
        if (idx >= kN) break;

        // Alternating run
        size_t alt_run = std::min<size_t>(256, kN - idx);
        for (size_t i = 0; i < alt_run; ++i) {
            src[idx + i] = static_cast<uint32_t>(idx + i);
            selector[idx + i] = (i % 2 == 0) ? 1 : 0;
            if (selector[idx + i]) expected.push_back(src[idx + i]);
        }
        idx += alt_run;
        if (idx >= kN) break;

        // Sparse run (1 in 128 kept)
        size_t sparse_run = std::min<size_t>(128, kN - idx);
        for (size_t i = 0; i < sparse_run; ++i) {
            src[idx + i] = static_cast<uint32_t>(idx + i);
            selector[idx + i] = (i == 0) ? 1 : 0;
            if (selector[idx + i]) expected.push_back(src[idx + i]);
        }
        idx += sparse_run;
    }

    std::vector<uint32_t> dst(kN, 0);
    size_t count = filter_range(dst.data(), src.data(), selector, 0, kN);
    ASSERT_EQ(expected.size(), count);
    for (size_t i = 0; i < count; ++i) {
        ASSERT_EQ(expected[i], dst[i]);
    }

    // In-place validation
    std::vector<uint32_t> in_place = src;
    size_t in_count = filter_range(in_place.data(), in_place.data(), selector, 0, kN);
    ASSERT_EQ(expected.size(), in_count);
    for (size_t i = 0; i < in_count; ++i) {
        ASSERT_EQ(expected[i], in_place[i]);
    }
}

#if defined(__x86_64__)
TEST(SimdFilterTest, direct_avx2_kernels_test) {
    if (__builtin_cpu_supports("avx2")) {
        namespace detail = SIMD::Filter::detail;

        const size_t n = 64;
        std::vector<uint32_t> src(n);
        std::vector<uint32_t> dst(n, 0);
        std::vector<uint8_t> sel(n);
        for (size_t i = 0; i < n; ++i) {
            src[i] = static_cast<uint32_t>(i * 17 + 1);
            sel[i] = (i % 3 != 0) ? 1 : 0;
        }

        size_t c4 = detail::compress_avx2_w4(dst.data(), src.data(), sel.data(), 0, n);
        size_t expected_c = 0;
        for (size_t i = 0; i < n; ++i) {
            if (sel[i]) {
                ASSERT_EQ(src[i], dst[expected_c++]);
            }
        }
        ASSERT_EQ(expected_c, c4);

        std::vector<uint64_t> src8(n);
        std::vector<uint64_t> dst8(n, 0);
        for (size_t i = 0; i < n; ++i) {
            src8[i] = static_cast<uint64_t>(i * 31 + 7);
        }
        size_t c8 = detail::compress_avx2_w8(dst8.data(), src8.data(), sel.data(), 0, n);
        ASSERT_EQ(expected_c, c8);
        size_t expected_c8 = 0;
        for (size_t i = 0; i < n; ++i) {
            if (sel[i]) {
                ASSERT_EQ(src8[i], dst8[expected_c8++]);
            }
        }
        ASSERT_EQ(expected_c, expected_c8);

        // Comprehensive edge cases: all-ones, all-zeros, mixed groups, remainders, subranges
        for (size_t len : {0, 1, 7, 31, 32, 33, 64, 71, 100}) {
            for (int mode = 0; mode < 4; ++mode) {
                std::vector<uint32_t> s4(len);
                std::vector<uint32_t> d4(len, 0);
                std::vector<uint64_t> s8(len);
                std::vector<uint64_t> d8(len, 0);
                std::vector<uint8_t> selector(len);

                std::vector<uint32_t> exp4;
                std::vector<uint64_t> exp8;

                for (size_t i = 0; i < len; ++i) {
                    s4[i] = static_cast<uint32_t>(i * 101 + 3);
                    s8[i] = static_cast<uint64_t>(i * 10007 + 11);
                    if (mode == 0) {
                        selector[i] = 1; // all kept (tests mask == 0xffffffff)
                    } else if (mode == 1) {
                        selector[i] = 0; // all dropped (tests mask == 0)
                    } else if (mode == 2) {
                        selector[i] = (i < 32) ? 1 : ((i % 2 == 0) ? 1 : 0); // mix full batch + mixed batch
                    } else {
                        selector[i] = (i >= 8 && i < 16) ? 0 : ((i % 4 == 0) ? 1 : 0); // mix empty groups + sparse
                    }
                    if (selector[i]) {
                        exp4.push_back(s4[i]);
                        exp8.push_back(s8[i]);
                    }
                }

                size_t res4 = detail::compress_avx2_w4(d4.data(), s4.data(), selector.data(), 0, len);
                ASSERT_EQ(exp4.size(), res4);
                for (size_t i = 0; i < res4; ++i) {
                    ASSERT_EQ(exp4[i], d4[i]);
                }

                size_t res8 = detail::compress_avx2_w8(d8.data(), s8.data(), selector.data(), 0, len);
                ASSERT_EQ(exp8.size(), res8);
                for (size_t i = 0; i < res8; ++i) {
                    ASSERT_EQ(exp8[i], d8[i]);
                }
            }
        }

        // Non-zero 'from' subrange
        {
            const size_t len = 80;
            const size_t from = 13;
            const size_t to = 75;
            std::vector<uint32_t> s4(len);
            std::vector<uint32_t> d4(len, 0);
            std::vector<uint64_t> s8(len);
            std::vector<uint64_t> d8(len, 0);
            std::vector<uint8_t> selector(len);

            for (size_t i = 0; i < len; ++i) {
                s4[i] = static_cast<uint32_t>(i * 13 + 5);
                s8[i] = static_cast<uint64_t>(i * 1009 + 2);
                selector[i] = (i % 2 == 0) ? 1 : 0;
            }

            size_t expected_count = from;
            std::vector<uint32_t> exp4_sub;
            std::vector<uint64_t> exp8_sub;
            for (size_t i = from; i < to; ++i) {
                if (selector[i]) {
                    exp4_sub.push_back(s4[i]);
                    exp8_sub.push_back(s8[i]);
                    expected_count++;
                }
            }

            size_t res4 = detail::compress_avx2_w4(d4.data(), s4.data(), selector.data(), from, to);
            ASSERT_EQ(expected_count, res4);
            for (size_t i = 0; i < exp4_sub.size(); ++i) {
                ASSERT_EQ(exp4_sub[i], d4[from + i]);
            }

            size_t res8 = detail::compress_avx2_w8(d8.data(), s8.data(), selector.data(), from, to);
            ASSERT_EQ(expected_count, res8);
            for (size_t i = 0; i < exp8_sub.size(); ++i) {
                ASSERT_EQ(exp8_sub[i], d8[from + i]);
            }
        }
    }
}
#endif

} // namespace starrocks
