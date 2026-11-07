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

#include <cstring>
#include <iterator>
#include <random>
#include <vector>

#include "base/testutil/parallel_test.h"
#include "gutil/dynamic_annotations.h"

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

struct Elem3B {
    uint8_t bytes[3];
    bool operator==(const Elem3B& o) const { return std::memcmp(bytes, o.bytes, 3) == 0; }
};
static_assert(sizeof(Elem3B) == 3);

struct Elem12B {
    uint32_t a, b, c;
    bool operator==(const Elem12B& o) const { return a == o.a && b == o.b && c == o.c; }
};
static_assert(sizeof(Elem12B) == 12);

struct Elem16B {
    uint64_t a, b;
    bool operator==(const Elem16B& o) const { return a == o.a && b == o.b; }
};
static_assert(sizeof(Elem16B) == 16);

struct Elem32B {
    uint64_t a, b, c, d;
    bool operator==(const Elem32B& o) const { return a == o.a && b == o.b && c == o.c && d == o.d; }
};
static_assert(sizeof(Elem32B) == 32);

template <typename T, typename GenFn>
static void check_element_width_patterns(GenFn gen_fn) {
    const std::vector<size_t> sizes = {0, 1, 7, 16, 31, 32, 33, 64, 100};
    for (size_t n : sizes) {
        if (n == 0) {
            std::vector<T> src;
            std::vector<T> dst;
            std::vector<uint8_t> sel;
            size_t out = filter_range(dst.data(), src.data(), sel, 0, 0);
            EXPECT_EQ(0, out);
            continue;
        }
        std::vector<T> src(n);
        for (size_t i = 0; i < n; ++i) {
            src[i] = gen_fn(i);
        }

        for (int pattern = 0; pattern < 5; ++pattern) {
            std::vector<uint8_t> selector(n, 0);
            std::vector<T> expected;
            for (size_t i = 0; i < n; ++i) {
                bool keep = false;
                switch (pattern) {
                case 0:
                    keep = false;
                    break;
                case 1:
                    keep = true;
                    break;
                case 2:
                    keep = (i % 2 == 0);
                    break;
                case 3:
                    keep = (i % 7 == 0);
                    break;
                case 4:
                    keep = (i % 7 != 0);
                    break;
                }
                selector[i] = keep ? 1 : 0;
                if (keep) {
                    expected.push_back(src[i]);
                }
            }

            // Separate buffer
            std::vector<T> dst(n);
            size_t out_sz = filter_range(dst.data(), src.data(), selector, 0, n);
            ASSERT_EQ(expected.size(), out_sz);
            for (size_t i = 0; i < out_sz; ++i) {
                ASSERT_TRUE(expected[i] == dst[i]);
            }

            // In-place
            std::vector<T> in_place = src;
            size_t ip_sz = filter_range(in_place.data(), in_place.data(), selector, 0, n);
            ASSERT_EQ(expected.size(), ip_sz);
            for (size_t i = 0; i < ip_sz; ++i) {
                ASSERT_TRUE(expected[i] == in_place[i]);
            }
        }
    }
}

PARALLEL_TEST(SimdFilterTest, element_widths) {
    // 1B
    check_element_width_patterns<uint8_t>([](size_t i) -> uint8_t { return static_cast<uint8_t>(i * 17 + 3); });
    // 2B
    check_element_width_patterns<uint16_t>([](size_t i) -> uint16_t { return static_cast<uint16_t>(i * 1337 + 5); });
    // 3B
    check_element_width_patterns<Elem3B>([](size_t i) -> Elem3B {
        return Elem3B{static_cast<uint8_t>(i), static_cast<uint8_t>(i * 3), static_cast<uint8_t>(i ^ 0x5a)};
    });
    // 4B
    check_element_width_patterns<uint32_t>(
            [](size_t i) -> uint32_t { return static_cast<uint32_t>(i * 2654435761u + 7); });
    // 8B
    check_element_width_patterns<uint64_t>(
            [](size_t i) -> uint64_t { return static_cast<uint64_t>(i * 6364136223846793005ull + 11); });
    // 12B
    check_element_width_patterns<Elem12B>([](size_t i) -> Elem12B {
        return Elem12B{static_cast<uint32_t>(i), static_cast<uint32_t>(i * 2 + 1), static_cast<uint32_t>(i * 5 + 9)};
    });
    // 16B
    check_element_width_patterns<Elem16B>([](size_t i) -> Elem16B {
        return Elem16B{static_cast<uint64_t>(i * 2654435761ull), static_cast<uint64_t>(i ^ 0xdeadbeefcafebabeull)};
    });
    // 32B
    check_element_width_patterns<Elem32B>([](size_t i) -> Elem32B {
        return Elem32B{static_cast<uint64_t>(i), static_cast<uint64_t>(i + 10), static_cast<uint64_t>(i + 20),
                       static_cast<uint64_t>(i + 30)};
    });
}

PARALLEL_TEST(SimdFilterTest, subrange_offsets) {
    const std::vector<size_t> from_offsets = {0, 1, 15, 16, 31, 32};
    const std::vector<size_t> lengths = {0, 1, 2, 7, 15, 31, 32, 45, 64};

    auto run_for_type = [&](auto dummy) {
        using T = decltype(dummy);
        for (size_t from : from_offsets) {
            for (size_t len : lengths) {
                size_t to = from + len;
                size_t total_n = to + 16;
                std::vector<T> src(total_n);
                std::vector<T> dst(total_n);
                std::vector<uint8_t> selector(total_n);

                constexpr T kCanary = static_cast<T>(0xFEEDBEEF);
                for (size_t i = 0; i < total_n; ++i) {
                    src[i] = static_cast<T>(10000 + i);
                    dst[i] = kCanary;
                    selector[i] = ((i * 3 + 1) % 5 < 3) ? 1 : 0;
                }

                std::vector<T> expected;
                for (size_t i = from; i < to; ++i) {
                    if (selector[i]) {
                        expected.push_back(src[i]);
                    }
                }

                // Test separate buffer
                size_t new_end = filter_range(dst.data(), src.data(), selector, from, to);
                ASSERT_EQ(from + expected.size(), new_end) << "from=" << from << " to=" << to;
                for (size_t i = 0; i < from; ++i) {
                    ASSERT_EQ(kCanary, dst[i]) << "dst[" << i << "] modified before from=" << from;
                }
                for (size_t i = 0; i < expected.size(); ++i) {
                    ASSERT_EQ(expected[i], dst[from + i]) << "mismatch at " << i << " from=" << from << " to=" << to;
                }

                // Test in-place
                std::vector<T> in_place = src;
                size_t ip_end = filter_range(in_place.data(), in_place.data(), selector, from, to);
                ASSERT_EQ(from + expected.size(), ip_end) << "in-place from=" << from << " to=" << to;
                for (size_t i = 0; i < from; ++i) {
                    ASSERT_EQ(src[i], in_place[i]) << "in-place[" << i << "] modified before from=" << from;
                }
                for (size_t i = 0; i < expected.size(); ++i) {
                    ASSERT_EQ(expected[i], in_place[from + i]);
                }
            }
        }
    };

    run_for_type(uint32_t{0});
    run_for_type(uint64_t{0});
}

PARALLEL_TEST(SimdFilterTest, floating_point_special_values) {
    // 32-bit floats
    {
        const std::vector<uint32_t> float_bit_patterns = {
                0x00000000u, // +0.0f
                0x80000000u, // -0.0f
                0x7F800000u, // +inf
                0xFF800000u, // -inf
                0x7FC00001u, // quiet NaN payload 1
                0x7FC01234u, // quiet NaN payload 0x1234
                0xFFC0ABCDu, // negative quiet NaN payload 0xABCD
                0x7F800001u, // signaling NaN payload 1 (quiet bit 22 is 0)
                0x7FA05678u, // signaling NaN payload 0x205678 (quiet bit 22 is 0)
                0xFF800001u, // negative signaling NaN payload 1
                0x3F800000u, // 1.0f
                0xBF800000u, // -1.0f
                0x00000001u, // subnormal
        };

        const size_t n = 104; // multiple of patterns, >= 32 to trigger vector loops
        std::vector<float> src(n);
        std::vector<uint8_t> selector(n);
        std::vector<uint32_t> expected_bits;

        for (size_t i = 0; i < n; ++i) {
            uint32_t pattern = float_bit_patterns[i % float_bit_patterns.size()];
            std::memcpy(&src[i], &pattern, sizeof(float));
            selector[i] = ((i % 3 == 0) || (i % 7 == 0)) ? 1 : 0;
            if (selector[i]) {
                expected_bits.push_back(pattern);
            }
        }

        // Separate buffer
        std::vector<float> dst(n, 0.0f);
        size_t out_sz = filter_range(dst.data(), src.data(), selector, 0, n);
        ASSERT_EQ(expected_bits.size(), out_sz);
        for (size_t i = 0; i < out_sz; ++i) {
            uint32_t dst_bits;
            std::memcpy(&dst_bits, &dst[i], sizeof(float));
            ASSERT_EQ(expected_bits[i], dst_bits) << "Float bit mismatch at " << i << " expected 0x" << std::hex
                                                  << expected_bits[i] << " got 0x" << dst_bits;
        }

        // In-place
        std::vector<float> in_place = src;
        size_t ip_sz = filter_range(in_place.data(), in_place.data(), selector, 0, n);
        ASSERT_EQ(expected_bits.size(), ip_sz);
        for (size_t i = 0; i < ip_sz; ++i) {
            uint32_t ip_bits;
            std::memcpy(&ip_bits, &in_place[i], sizeof(float));
            ASSERT_EQ(expected_bits[i], ip_bits) << "In-place float bit mismatch at " << i;
        }
    }

    // 64-bit doubles
    {
        const std::vector<uint64_t> double_bit_patterns = {
                0x0000000000000000ull, // +0.0
                0x8000000000000000ull, // -0.0
                0x7FF0000000000000ull, // +inf
                0xFFF0000000000000ull, // -inf
                0x7FF8000000000001ull, // quiet NaN payload 1 (quiet bit 51 is 1)
                0x7FF8000012345678ull, // quiet NaN payload 0x12345678
                0xFFF80000AABBCCDDull, // negative quiet NaN payload 0xAABBCCDD
                0x7FF0000000000001ull, // signaling NaN payload 1 (quiet bit 51 is 0)
                0x7FF4000087654321ull, // signaling NaN payload 0x4000087654321 (quiet bit 51 is 0)
                0xFFF0000000000001ull, // negative signaling NaN payload 1
                0x3FF0000000000000ull, // 1.0
                0xBFF0000000000000ull, // -1.0
                0x0000000000000001ull, // subnormal
        };

        const size_t n = 104;
        std::vector<double> src(n);
        std::vector<uint8_t> selector(n);
        std::vector<uint64_t> expected_bits;

        for (size_t i = 0; i < n; ++i) {
            uint64_t pattern = double_bit_patterns[i % double_bit_patterns.size()];
            std::memcpy(&src[i], &pattern, sizeof(double));
            selector[i] = ((i % 3 == 0) || (i % 5 == 0)) ? 1 : 0;
            if (selector[i]) {
                expected_bits.push_back(pattern);
            }
        }

        // Separate buffer
        std::vector<double> dst(n, 0.0);
        size_t out_sz = filter_range(dst.data(), src.data(), selector, 0, n);
        ASSERT_EQ(expected_bits.size(), out_sz);
        for (size_t i = 0; i < out_sz; ++i) {
            uint64_t dst_bits;
            std::memcpy(&dst_bits, &dst[i], sizeof(double));
            ASSERT_EQ(expected_bits[i], dst_bits) << "Double bit mismatch at " << i << " expected 0x" << std::hex
                                                  << expected_bits[i] << " got 0x" << dst_bits;
        }

        // In-place
        std::vector<double> in_place = src;
        size_t ip_sz = filter_range(in_place.data(), in_place.data(), selector, 0, n);
        ASSERT_EQ(expected_bits.size(), ip_sz);
        for (size_t i = 0; i < ip_sz; ++i) {
            uint64_t ip_bits;
            std::memcpy(&ip_bits, &in_place[i], sizeof(double));
            ASSERT_EQ(expected_bits[i], ip_bits) << "In-place double bit mismatch at " << i;
        }
    }
}

PARALLEL_TEST(SimdFilterTest, non_normalized_selectors) {
    const std::vector<uint8_t> non_zero_bytes = {0x80, 0x7F, 0xFF, 0x01, 0xAA, 0x02, 0xFE};
    const std::vector<size_t> test_sizes = {1, 15, 31, 32, 33, 64, 100};

    auto run_for_type = [&](auto dummy) {
        using T = decltype(dummy);
        for (uint8_t nz_byte : non_zero_bytes) {
            for (size_t n : test_sizes) {
                std::vector<T> src(n);
                for (size_t i = 0; i < n; ++i) {
                    src[i] = static_cast<T>(i * 31 + 1);
                }

                // 1. All rows have nz_byte: 100% kept
                {
                    std::vector<uint8_t> selector(n, nz_byte);
                    std::vector<T> dst(n);
                    size_t out_sz = filter_range(dst.data(), src.data(), selector, 0, n);
                    ASSERT_EQ(n, out_sz) << "Byte 0x" << std::hex << static_cast<int>(nz_byte) << " n=" << n;
                    for (size_t i = 0; i < n; ++i) {
                        ASSERT_EQ(src[i], dst[i]);
                    }

                    std::vector<T> in_place = src;
                    size_t ip_sz = filter_range(in_place.data(), in_place.data(), selector, 0, n);
                    ASSERT_EQ(n, ip_sz);
                    for (size_t i = 0; i < n; ++i) {
                        ASSERT_EQ(src[i], in_place[i]);
                    }
                }

                // 2. Alternating nz_byte and 0x00: exactly even rows kept
                {
                    std::vector<uint8_t> selector(n);
                    std::vector<T> expected;
                    for (size_t i = 0; i < n; ++i) {
                        selector[i] = (i % 2 == 0) ? nz_byte : 0x00;
                        if (selector[i]) {
                            expected.push_back(src[i]);
                        }
                    }

                    std::vector<T> dst(n);
                    size_t out_sz = filter_range(dst.data(), src.data(), selector, 0, n);
                    ASSERT_EQ(expected.size(), out_sz);
                    for (size_t i = 0; i < out_sz; ++i) {
                        ASSERT_EQ(expected[i], dst[i]);
                    }

                    std::vector<T> in_place = src;
                    size_t ip_sz = filter_range(in_place.data(), in_place.data(), selector, 0, n);
                    ASSERT_EQ(expected.size(), ip_sz);
                    for (size_t i = 0; i < ip_sz; ++i) {
                        ASSERT_EQ(expected[i], in_place[i]);
                    }
                }
            }
        }

        // 3. Mixed selector with assorted non-canonical bytes (0x80, 0x7F, 0xFF, 0x01) and 0x00
        for (size_t n : test_sizes) {
            std::vector<T> src(n);
            std::vector<uint8_t> selector(n);
            std::vector<T> expected;
            const uint8_t palette[] = {0x80, 0x00, 0x7F, 0x00, 0xFF, 0x01, 0x00, 0x02};
            for (size_t i = 0; i < n; ++i) {
                src[i] = static_cast<T>(i * 17 + 5);
                selector[i] = palette[i % std::size(palette)];
                if (selector[i] != 0) {
                    expected.push_back(src[i]);
                }
            }

            std::vector<T> dst(n);
            size_t out_sz = filter_range(dst.data(), src.data(), selector, 0, n);
            ASSERT_EQ(expected.size(), out_sz);
            for (size_t i = 0; i < out_sz; ++i) {
                ASSERT_EQ(expected[i], dst[i]);
            }

            std::vector<T> in_place = src;
            size_t ip_sz = filter_range(in_place.data(), in_place.data(), selector, 0, n);
            ASSERT_EQ(expected.size(), ip_sz);
            for (size_t i = 0; i < ip_sz; ++i) {
                ASSERT_EQ(expected[i], in_place[i]);
            }
        }
    };

    run_for_type(uint32_t{0});
    run_for_type(uint64_t{0});
}

template <typename T>
static void check_exact_capacity_asan_for_type() {
    constexpr size_t kRedzone = 64; // guard elements past `to`
    const std::vector<size_t> from_offsets = {0, 1, 7, 15, 16, 31, 32};
    const std::vector<size_t> lengths = {0, 1, 2, 7, 8, 15, 16, 31, 32, 33, 63, 64, 65, 100, 128};

    for (size_t from : from_offsets) {
        for (size_t len : lengths) {
            size_t to = from + len;

            for (int selectivity_case : {0, 1, 2, 3, 4, 5}) {
                std::vector<T> src(to);
                std::vector<uint8_t> selector(to);

                for (size_t i = 0; i < to; ++i) {
                    src[i] = static_cast<T>(i * 101 + 7);
                    bool keep = false;
                    switch (selectivity_case) {
                    case 0:
                        keep = false; // 0%
                        break;
                    case 1:
                        keep = true; // 100%
                        break;
                    case 2:
                        keep = (i == from); // 1 item kept
                        break;
                    case 3:
                        keep = (i != from); // all except 1 kept
                        break;
                    case 4:
                        keep = (i % 2 == 0); // 50%
                        break;
                    case 5:
                        keep = ((i * 7 + 3) % 8 < 3); // ~37.5%
                        break;
                    }
                    selector[i] = keep ? 1 : 0;
                }

                std::vector<T> expected;
                for (size_t i = from; i < to; ++i) {
                    if (selector[i]) {
                        expected.push_back(src[i]);
                    }
                }

                // Allocate dst with capacity strictly `to`, plus `kRedzone` poison elements
                std::vector<T> dst_buf(to + kRedzone);
                constexpr uint8_t kCanaryByte = 0x5A;
                std::memset(dst_buf.data() + to, kCanaryByte, kRedzone * sizeof(T));

                // Poison memory region strictly past `to`
                ASAN_POISON_MEMORY_REGION(dst_buf.data() + to, kRedzone * sizeof(T));

                size_t out_sz = filter_range(dst_buf.data(), src.data(), selector, from, to);
                ASSERT_EQ(from + expected.size(), out_sz)
                        << "from=" << from << " to=" << to << " case=" << selectivity_case;

                for (size_t i = 0; i < expected.size(); ++i) {
                    ASSERT_EQ(expected[i], dst_buf[from + i]);
                }

                // Unpoison so we can verify canary bytes and deallocate safely
                ASAN_UNPOISON_MEMORY_REGION(dst_buf.data() + to, kRedzone * sizeof(T));

                const auto* redzone_bytes = reinterpret_cast<const uint8_t*>(dst_buf.data() + to);
                for (size_t b = 0; b < kRedzone * sizeof(T); ++b) {
                    ASSERT_EQ(kCanaryByte, redzone_bytes[b])
                            << "Redzone canary clobbered at byte offset " << b << " past to=" << to
                            << " for from=" << from << " case=" << selectivity_case;
                }

#if defined(__x86_64__)
                if (__builtin_cpu_supports("avx2")) {
                    std::fill(dst_buf.begin(), dst_buf.begin() + to, T{0});
                    std::memset(dst_buf.data() + to, kCanaryByte, kRedzone * sizeof(T));
                    ASAN_POISON_MEMORY_REGION(dst_buf.data() + to, kRedzone * sizeof(T));

                    size_t avx2_out_sz = SIMD::Filter::detail::filter_range_avx2(dst_buf.data(), src.data(), sizeof(T),
                                                                                 selector.data(), from, to);
                    ASSERT_EQ(from + expected.size(), avx2_out_sz)
                            << "AVX2 from=" << from << " to=" << to << " case=" << selectivity_case;

                    for (size_t i = 0; i < expected.size(); ++i) {
                        ASSERT_EQ(expected[i], dst_buf[from + i]);
                    }

                    ASAN_UNPOISON_MEMORY_REGION(dst_buf.data() + to, kRedzone * sizeof(T));

                    for (size_t b = 0; b < kRedzone * sizeof(T); ++b) {
                        ASSERT_EQ(kCanaryByte, redzone_bytes[b])
                                << "AVX2 redzone canary clobbered at byte offset " << b << " past to=" << to
                                << " for from=" << from << " case=" << selectivity_case;
                    }
                }
#endif
            }
        }
    }
}

PARALLEL_TEST(SimdFilterTest, exact_capacity_asan) {
    check_exact_capacity_asan_for_type<uint32_t>();
    check_exact_capacity_asan_for_type<uint64_t>();
    check_exact_capacity_asan_for_type<uint8_t>();
}

#if defined(__x86_64__)
PARALLEL_TEST(SimdFilterTest, x86_64_explicit_avx2_w4) {
    if (!__builtin_cpu_supports("avx2")) {
        GTEST_SKIP() << "AVX2 is not supported on this CPU";
    }
    const std::vector<size_t> sizes = {0, 1, 7, 16, 31, 32, 33, 64, 129};
    const std::vector<size_t> from_offsets = {0, 1, 7, 15, 16, 31, 32};
    const uint8_t non_canonical_palette[] = {0x80, 0x00, 0x7F, 0x00, 0xFF, 0x01, 0x00, 0x02};

    for (size_t from : from_offsets) {
        for (size_t len : sizes) {
            size_t to = from + len;
            if (to == from) {
                std::vector<uint32_t> dst(to + 8, 0xDeadBeef);
                std::vector<uint32_t> src(to + 8, 0x12345678);
                std::vector<uint8_t> selector(to + 8, 1);
                size_t out_sz =
                        SIMD::Filter::detail::compress_avx2_w4(dst.data(), src.data(), selector.data(), from, to);
                EXPECT_EQ(from, out_sz);
                continue;
            }

            for (int mode = 0; mode < 7; ++mode) {
                std::vector<uint32_t> src(to + 16);
                std::vector<uint8_t> selector(to + 16, 0);
                std::vector<uint32_t> expected;

                for (size_t i = 0; i < to + 16; ++i) {
                    src[i] = static_cast<uint32_t>(i * 101 + 37);
                }

                for (size_t i = 0; i < to; ++i) {
                    bool keep = false;
                    uint8_t sel_byte = 1;
                    switch (mode) {
                    case 0: // 0%
                        keep = false;
                        sel_byte = 0;
                        break;
                    case 1: // 100%
                        keep = true;
                        sel_byte = 1;
                        break;
                    case 2: // alternating
                        keep = (i % 2 == 0);
                        sel_byte = keep ? 1 : 0;
                        break;
                    case 3: // single survivor
                        keep = (i == from);
                        sel_byte = keep ? 1 : 0;
                        break;
                    case 4: // all except one
                        keep = (i != from);
                        sel_byte = keep ? 1 : 0;
                        break;
                    case 5: // pseudo-random / periodic (e.g. 3 of 8)
                        keep = ((i * 7 + 3) % 8 < 3);
                        sel_byte = keep ? 1 : 0;
                        break;
                    case 6: // non-canonical selector bytes
                        sel_byte = non_canonical_palette[i % std::size(non_canonical_palette)];
                        keep = (sel_byte != 0);
                        break;
                    }
                    selector[i] = sel_byte;
                    if (i >= from && keep) {
                        expected.push_back(src[i]);
                    }
                }

                // 1. Separate destination buffer
                {
                    std::vector<uint32_t> dst(to + 16, 0xBAADF00D);
                    size_t out_sz =
                            SIMD::Filter::detail::compress_avx2_w4(dst.data(), src.data(), selector.data(), from, to);
                    ASSERT_EQ(from + expected.size(), out_sz)
                            << "Separate dst: from=" << from << " to=" << to << " mode=" << mode;
                    for (size_t i = 0; i < from; ++i) {
                        ASSERT_EQ(0xBAADF00Du, dst[i])
                                << "Untouched prefix clobbered at i=" << i << " from=" << from << " to=" << to;
                    }
                    for (size_t i = 0; i < expected.size(); ++i) {
                        ASSERT_EQ(expected[i], dst[from + i])
                                << "Mismatch at i=" << i << " from=" << from << " to=" << to << " mode=" << mode;
                    }
                }

                // 2. In-place stream compaction (dst == src)
                {
                    std::vector<uint32_t> in_place = src;
                    size_t ip_sz = SIMD::Filter::detail::compress_avx2_w4(in_place.data(), in_place.data(),
                                                                          selector.data(), from, to);
                    ASSERT_EQ(from + expected.size(), ip_sz)
                            << "In-place: from=" << from << " to=" << to << " mode=" << mode;
                    for (size_t i = 0; i < from; ++i) {
                        ASSERT_EQ(src[i], in_place[i])
                                << "Untouched prefix in-place clobbered at i=" << i << " from=" << from << " to=" << to;
                    }
                    for (size_t i = 0; i < expected.size(); ++i) {
                        ASSERT_EQ(expected[i], in_place[from + i]) << "In-place mismatch at i=" << i << " from=" << from
                                                                   << " to=" << to << " mode=" << mode;
                    }
                }
            }
        }
    }
}

PARALLEL_TEST(SimdFilterTest, x86_64_explicit_avx2_w8) {
    if (!__builtin_cpu_supports("avx2")) {
        GTEST_SKIP() << "AVX2 is not supported on this CPU";
    }
    const std::vector<size_t> sizes = {0, 1, 7, 16, 31, 32, 33, 64, 129};
    const std::vector<size_t> from_offsets = {0, 1, 7, 15, 16, 31, 32};
    const uint8_t non_canonical_palette[] = {0x80, 0x00, 0x7F, 0x00, 0xFF, 0x01, 0x00, 0x02};

    for (size_t from : from_offsets) {
        for (size_t len : sizes) {
            size_t to = from + len;
            if (to == from) {
                std::vector<uint64_t> dst(to + 8, 0xDeadBeefCafeBabeULL);
                std::vector<uint64_t> src(to + 8, 0x123456789ABCDEF0ULL);
                std::vector<uint8_t> selector(to + 8, 1);
                size_t out_sz =
                        SIMD::Filter::detail::compress_avx2_w8(dst.data(), src.data(), selector.data(), from, to);
                EXPECT_EQ(from, out_sz);
                continue;
            }

            for (int mode = 0; mode < 7; ++mode) {
                std::vector<uint64_t> src(to + 16);
                std::vector<uint8_t> selector(to + 16, 0);
                std::vector<uint64_t> expected;

                for (size_t i = 0; i < to + 16; ++i) {
                    src[i] = static_cast<uint64_t>(i * 1000000007ULL + 0xDeadC0deULL);
                }

                for (size_t i = 0; i < to; ++i) {
                    bool keep = false;
                    uint8_t sel_byte = 1;
                    switch (mode) {
                    case 0: // 0%
                        keep = false;
                        sel_byte = 0;
                        break;
                    case 1: // 100%
                        keep = true;
                        sel_byte = 1;
                        break;
                    case 2: // alternating
                        keep = (i % 2 == 0);
                        sel_byte = keep ? 1 : 0;
                        break;
                    case 3: // single survivor
                        keep = (i == from);
                        sel_byte = keep ? 1 : 0;
                        break;
                    case 4: // all except one
                        keep = (i != from);
                        sel_byte = keep ? 1 : 0;
                        break;
                    case 5: // pseudo-random / periodic (e.g. 3 of 8)
                        keep = ((i * 7 + 3) % 8 < 3);
                        sel_byte = keep ? 1 : 0;
                        break;
                    case 6: // non-canonical selector bytes
                        sel_byte = non_canonical_palette[i % std::size(non_canonical_palette)];
                        keep = (sel_byte != 0);
                        break;
                    }
                    selector[i] = sel_byte;
                    if (i >= from && keep) {
                        expected.push_back(src[i]);
                    }
                }

                // 1. Separate destination buffer
                {
                    std::vector<uint64_t> dst(to + 16, 0xBAADF00DDEADBEEFULL);
                    size_t out_sz =
                            SIMD::Filter::detail::compress_avx2_w8(dst.data(), src.data(), selector.data(), from, to);
                    ASSERT_EQ(from + expected.size(), out_sz)
                            << "Separate dst: from=" << from << " to=" << to << " mode=" << mode;
                    for (size_t i = 0; i < from; ++i) {
                        ASSERT_EQ(0xBAADF00DDEADBEEFULL, dst[i])
                                << "Untouched prefix clobbered at i=" << i << " from=" << from << " to=" << to;
                    }
                    for (size_t i = 0; i < expected.size(); ++i) {
                        ASSERT_EQ(expected[i], dst[from + i])
                                << "Mismatch at i=" << i << " from=" << from << " to=" << to << " mode=" << mode;
                    }
                }

                // 2. In-place stream compaction (dst == src)
                {
                    std::vector<uint64_t> in_place = src;
                    size_t ip_sz = SIMD::Filter::detail::compress_avx2_w8(in_place.data(), in_place.data(),
                                                                          selector.data(), from, to);
                    ASSERT_EQ(from + expected.size(), ip_sz)
                            << "In-place: from=" << from << " to=" << to << " mode=" << mode;
                    for (size_t i = 0; i < from; ++i) {
                        ASSERT_EQ(src[i], in_place[i])
                                << "Untouched prefix in-place clobbered at i=" << i << " from=" << from << " to=" << to;
                    }
                    for (size_t i = 0; i < expected.size(); ++i) {
                        ASSERT_EQ(expected[i], in_place[from + i]) << "In-place mismatch at i=" << i << " from=" << from
                                                                   << " to=" << to << " mode=" << mode;
                    }
                }
            }
        }
    }
}

PARALLEL_TEST(SimdFilterTest, sparse_eight_byte_batches_avx2) {
    if (!__builtin_cpu_supports("avx2")) {
        GTEST_SKIP() << "AVX2 is not supported on this CPU";
    }
    const std::vector<size_t> popcounts = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 15, 16, 31, 32};
    const std::vector<size_t> test_lengths = {32, 35, 64};
    const std::vector<size_t> from_offsets = {0, 7};

    for (size_t from : from_offsets) {
        for (size_t len : test_lengths) {
            size_t to = from + len;
            for (size_t pop : popcounts) {
                for (size_t rot = 0; rot < 16; ++rot) {
                    for (int pattern_variant = 0; pattern_variant < 3; ++pattern_variant) {
                        std::vector<uint64_t> src(to + 16);
                        std::vector<uint8_t> selector(to + 16, 0);
                        std::vector<uint64_t> expected;

                        for (size_t i = 0; i < to + 16; ++i) {
                            src[i] = static_cast<uint64_t>(i * 6364136223846793005ULL + 1442695040888963407ULL);
                        }

                        for (size_t i = from; i < to; ++i) {
                            size_t batch_offset = (i - from) % 32;
                            bool keep = false;
                            if (pattern_variant == 0) {
                                // First 16 rows of each 32-row batch active with rotated pattern
                                if (batch_offset < 16) {
                                    keep = (((batch_offset + rot) % 16) < pop);
                                }
                            } else if (pattern_variant == 1) {
                                // Second 16 rows of each 32-row batch active with rotated pattern
                                if (batch_offset >= 16) {
                                    keep = ((((batch_offset - 16) + rot) % 16) < pop);
                                }
                            } else {
                                // Rotated pattern across a full 32-element window
                                keep = (((batch_offset + rot) % 32) < pop);
                            }

                            if (keep) {
                                selector[i] = 1;
                                expected.push_back(src[i]);
                            } else {
                                selector[i] = 0;
                            }
                        }

                        // 1. Separate destination buffer
                        {
                            std::vector<uint64_t> dst(to + 16, 0xCAFEBABECAFEBABEULL);
                            size_t out_sz = SIMD::Filter::detail::compress_avx2_w8(dst.data(), src.data(),
                                                                                   selector.data(), from, to);
                            ASSERT_EQ(from + expected.size(), out_sz)
                                    << "Sparse w8 separate: from=" << from << " len=" << len << " pop=" << pop
                                    << " rot=" << rot << " variant=" << pattern_variant;
                            for (size_t i = 0; i < from; ++i) {
                                ASSERT_EQ(0xCAFEBABECAFEBABEULL, dst[i])
                                        << "Prefix clobbered at i=" << i << " from=" << from;
                            }
                            for (size_t i = 0; i < expected.size(); ++i) {
                                ASSERT_EQ(expected[i], dst[from + i])
                                        << "Mismatch at i=" << i << " from=" << from << " pop=" << pop << " rot=" << rot
                                        << " variant=" << pattern_variant;
                            }
                        }

                        // 2. In-place stream compaction
                        {
                            std::vector<uint64_t> in_place = src;
                            size_t ip_sz = SIMD::Filter::detail::compress_avx2_w8(in_place.data(), in_place.data(),
                                                                                  selector.data(), from, to);
                            ASSERT_EQ(from + expected.size(), ip_sz)
                                    << "Sparse w8 in-place: from=" << from << " len=" << len << " pop=" << pop
                                    << " rot=" << rot << " variant=" << pattern_variant;
                            for (size_t i = 0; i < from; ++i) {
                                ASSERT_EQ(src[i], in_place[i])
                                        << "Prefix clobbered in-place at i=" << i << " from=" << from;
                            }
                            for (size_t i = 0; i < expected.size(); ++i) {
                                ASSERT_EQ(expected[i], in_place[from + i])
                                        << "In-place mismatch at i=" << i << " from=" << from << " pop=" << pop
                                        << " rot=" << rot << " variant=" << pattern_variant;
                            }
                        }

                        // 3. AVX2 dispatch entrypoint filter_range_avx2: separate destination buffer
                        {
                            std::vector<uint64_t> dst(to + 16, 0xCAFEBABECAFEBABEULL);
                            size_t out_sz = SIMD::Filter::detail::filter_range_avx2(
                                    dst.data(), src.data(), sizeof(uint64_t), selector.data(), from, to);
                            ASSERT_EQ(from + expected.size(), out_sz)
                                    << "filter_range_avx2(8B) separate: from=" << from << " len=" << len
                                    << " pop=" << pop << " rot=" << rot << " variant=" << pattern_variant;
                            for (size_t i = 0; i < from; ++i) {
                                ASSERT_EQ(0xCAFEBABECAFEBABEULL, dst[i])
                                        << "filter_range_avx2(8B) prefix clobbered at i=" << i << " from=" << from;
                            }
                            for (size_t i = 0; i < expected.size(); ++i) {
                                ASSERT_EQ(expected[i], dst[from + i])
                                        << "filter_range_avx2(8B) mismatch at i=" << i << " from=" << from
                                        << " pop=" << pop << " rot=" << rot << " variant=" << pattern_variant;
                            }
                        }

                        // 4. AVX2 dispatch entrypoint filter_range_avx2: in-place stream compaction
                        {
                            std::vector<uint64_t> in_place = src;
                            size_t ip_sz = SIMD::Filter::detail::filter_range_avx2(
                                    in_place.data(), in_place.data(), sizeof(uint64_t), selector.data(), from, to);
                            ASSERT_EQ(from + expected.size(), ip_sz)
                                    << "filter_range_avx2(8B) in-place: from=" << from << " len=" << len
                                    << " pop=" << pop << " rot=" << rot << " variant=" << pattern_variant;
                            for (size_t i = 0; i < from; ++i) {
                                ASSERT_EQ(src[i], in_place[i])
                                        << "filter_range_avx2(8B) prefix clobbered in-place at i=" << i
                                        << " from=" << from;
                            }
                            for (size_t i = 0; i < expected.size(); ++i) {
                                ASSERT_EQ(expected[i], in_place[from + i])
                                        << "filter_range_avx2(8B) in-place mismatch at i=" << i << " from=" << from
                                        << " pop=" << pop << " rot=" << rot << " variant=" << pattern_variant;
                            }
                        }
                    }
                }
            }
        }
    }
}

PARALLEL_TEST(SimdFilterTest, randomized_differential_fuzz_avx2) {
    if (!__builtin_cpu_supports("avx2")) {
        GTEST_SKIP() << "AVX2 is not supported on this CPU";
    }
    constexpr int kIterations = 2000;
    std::mt19937_64 rng(0x1337C0DE5EEDULL);
    std::uniform_int_distribution<size_t> dist_len(0, 256);
    std::uniform_int_distribution<size_t> dist_from(0, 64);
    std::uniform_int_distribution<int> dist_sel(0, 100);
    std::uniform_int_distribution<size_t> dist_cluster_idx(0, 2);
    const int clusters[] = {1, 4, 16};
    const uint8_t non_canonical_palette[] = {0x01, 0x02, 0x7F, 0x80, 0xAA, 0xFE, 0xFF};
    std::uniform_int_distribution<size_t> dist_palette(0, std::size(non_canonical_palette) - 1);
    std::uniform_int_distribution<int> dist_bool(0, 1);

    for (int iter = 0; iter < kIterations; ++iter) {
        size_t len = 0;
        size_t from = 0;
        size_t to = 0;

        if (iter % 2 == 0) {
            // Variant A: len in [0, 256], from in [0, 64]
            len = dist_len(rng);
            from = (iter % 8 == 0) ? 0 : dist_from(rng);
            to = from + len;
        } else {
            // Variant B: N in [0, 256], from and to in [0, N]
            size_t n = dist_len(rng);
            if (n == 0) {
                from = 0;
                to = 0;
            } else {
                from = std::uniform_int_distribution<size_t>(0, n)(rng);
                to = std::uniform_int_distribution<size_t>(from, n)(rng);
            }
            len = to - from;
        }

        int cluster = clusters[dist_cluster_idx(rng)];
        int selectivity_pct = dist_sel(rng);
        bool use_non_canonical = (dist_bool(rng) == 1);

        size_t buf_size = to + 16;
        std::vector<uint8_t> selector(buf_size, 0);
        for (size_t i = 0; i < buf_size; i += cluster) {
            bool keep = (std::uniform_int_distribution<int>(0, 100)(rng) <= selectivity_pct);
            uint8_t val = 0;
            if (keep) {
                val = use_non_canonical ? non_canonical_palette[dist_palette(rng)] : 1;
            }
            size_t chunk_len = std::min<size_t>(cluster, buf_size - i);
            std::fill_n(&selector[i], chunk_len, val);
        }

        // Test uint32_t (4-byte)
        {
            std::vector<uint32_t> src(buf_size);
            for (size_t i = 0; i < buf_size; ++i) {
                src[i] = static_cast<uint32_t>(rng());
            }
            std::vector<uint32_t> expected;
            for (size_t i = from; i < to; ++i) {
                if (selector[i]) {
                    expected.push_back(src[i]);
                }
            }

            // 1. Direct kernel compress_avx2_w4: out-of-place
            {
                std::vector<uint32_t> dst(buf_size, 0xBAADF00D);
                size_t out_sz =
                        SIMD::Filter::detail::compress_avx2_w4(dst.data(), src.data(), selector.data(), from, to);
                ASSERT_EQ(from + expected.size(), out_sz) << "compress_avx2_w4 out-of-place iter=" << iter;
                for (size_t i = 0; i < from; ++i) {
                    ASSERT_EQ(0xBAADF00Du, dst[i])
                            << "compress_avx2_w4 prefix clobbered at i=" << i << " iter=" << iter;
                }
                for (size_t i = 0; i < expected.size(); ++i) {
                    ASSERT_EQ(expected[i], dst[from + i]) << "compress_avx2_w4 mismatch at i=" << i << " iter=" << iter;
                }
            }

            // 2. Direct kernel compress_avx2_w4: in-place
            {
                std::vector<uint32_t> in_place = src;
                size_t ip_sz = SIMD::Filter::detail::compress_avx2_w4(in_place.data(), in_place.data(), selector.data(),
                                                                      from, to);
                ASSERT_EQ(from + expected.size(), ip_sz) << "compress_avx2_w4 in-place iter=" << iter;
                for (size_t i = 0; i < from; ++i) {
                    ASSERT_EQ(src[i], in_place[i])
                            << "compress_avx2_w4 in-place prefix clobbered at i=" << i << " iter=" << iter;
                }
                for (size_t i = 0; i < expected.size(); ++i) {
                    ASSERT_EQ(expected[i], in_place[from + i])
                            << "compress_avx2_w4 in-place mismatch at i=" << i << " iter=" << iter;
                }
            }

            // 3. Public API filter_range<uint32_t>: out-of-place
            {
                std::vector<uint32_t> dst(buf_size, 0xBAADF00D);
                size_t out_sz = SIMD::Filter::filter_range(dst.data(), src.data(), selector.data(), from, to);
                ASSERT_EQ(from + expected.size(), out_sz) << "filter_range<uint32_t> out-of-place iter=" << iter;
                for (size_t i = 0; i < from; ++i) {
                    ASSERT_EQ(0xBAADF00Du, dst[i])
                            << "filter_range<uint32_t> prefix clobbered at i=" << i << " iter=" << iter;
                }
                for (size_t i = 0; i < expected.size(); ++i) {
                    ASSERT_EQ(expected[i], dst[from + i])
                            << "filter_range<uint32_t> mismatch at i=" << i << " iter=" << iter;
                }
            }

            // 4. Public API filter_range<uint32_t>: in-place
            {
                std::vector<uint32_t> in_place = src;
                size_t ip_sz = SIMD::Filter::filter_range(in_place.data(), in_place.data(), selector.data(), from, to);
                ASSERT_EQ(from + expected.size(), ip_sz) << "filter_range<uint32_t> in-place iter=" << iter;
                for (size_t i = 0; i < from; ++i) {
                    ASSERT_EQ(src[i], in_place[i])
                            << "filter_range<uint32_t> in-place prefix clobbered at i=" << i << " iter=" << iter;
                }
                for (size_t i = 0; i < expected.size(); ++i) {
                    ASSERT_EQ(expected[i], in_place[from + i])
                            << "filter_range<uint32_t> in-place mismatch at i=" << i << " iter=" << iter;
                }
            }

            // 5. Direct AVX2 dispatch entrypoint filter_range_avx2 (4B): out-of-place
            {
                std::vector<uint32_t> dst(buf_size, 0xBAADF00D);
                size_t out_sz = SIMD::Filter::detail::filter_range_avx2(dst.data(), src.data(), sizeof(uint32_t),
                                                                        selector.data(), from, to);
                ASSERT_EQ(from + expected.size(), out_sz) << "filter_range_avx2(4B) out-of-place iter=" << iter;
                for (size_t i = 0; i < from; ++i) {
                    ASSERT_EQ(0xBAADF00Du, dst[i])
                            << "filter_range_avx2(4B) prefix clobbered at i=" << i << " iter=" << iter;
                }
                for (size_t i = 0; i < expected.size(); ++i) {
                    ASSERT_EQ(expected[i], dst[from + i])
                            << "filter_range_avx2(4B) mismatch at i=" << i << " iter=" << iter;
                }
            }

            // 6. Direct AVX2 dispatch entrypoint filter_range_avx2 (4B): in-place
            {
                std::vector<uint32_t> in_place = src;
                size_t ip_sz = SIMD::Filter::detail::filter_range_avx2(in_place.data(), in_place.data(),
                                                                       sizeof(uint32_t), selector.data(), from, to);
                ASSERT_EQ(from + expected.size(), ip_sz) << "filter_range_avx2(4B) in-place iter=" << iter;
                for (size_t i = 0; i < from; ++i) {
                    ASSERT_EQ(src[i], in_place[i])
                            << "filter_range_avx2(4B) in-place prefix clobbered at i=" << i << " iter=" << iter;
                }
                for (size_t i = 0; i < expected.size(); ++i) {
                    ASSERT_EQ(expected[i], in_place[from + i])
                            << "filter_range_avx2(4B) in-place mismatch at i=" << i << " iter=" << iter;
                }
            }
        }

        // Test uint64_t (8-byte)
        {
            std::vector<uint64_t> src(buf_size);
            for (size_t i = 0; i < buf_size; ++i) {
                src[i] = static_cast<uint64_t>(rng());
            }
            std::vector<uint64_t> expected;
            for (size_t i = from; i < to; ++i) {
                if (selector[i]) {
                    expected.push_back(src[i]);
                }
            }

            // 1. Direct kernel compress_avx2_w8: out-of-place
            {
                std::vector<uint64_t> dst(buf_size, 0xBAADF00DDEADBEEFULL);
                size_t out_sz =
                        SIMD::Filter::detail::compress_avx2_w8(dst.data(), src.data(), selector.data(), from, to);
                ASSERT_EQ(from + expected.size(), out_sz) << "compress_avx2_w8 out-of-place iter=" << iter;
                for (size_t i = 0; i < from; ++i) {
                    ASSERT_EQ(0xBAADF00DDEADBEEFULL, dst[i])
                            << "compress_avx2_w8 prefix clobbered at i=" << i << " iter=" << iter;
                }
                for (size_t i = 0; i < expected.size(); ++i) {
                    ASSERT_EQ(expected[i], dst[from + i]) << "compress_avx2_w8 mismatch at i=" << i << " iter=" << iter;
                }
            }

            // 2. Direct kernel compress_avx2_w8: in-place
            {
                std::vector<uint64_t> in_place = src;
                size_t ip_sz = SIMD::Filter::detail::compress_avx2_w8(in_place.data(), in_place.data(), selector.data(),
                                                                      from, to);
                ASSERT_EQ(from + expected.size(), ip_sz) << "compress_avx2_w8 in-place iter=" << iter;
                for (size_t i = 0; i < from; ++i) {
                    ASSERT_EQ(src[i], in_place[i])
                            << "compress_avx2_w8 in-place prefix clobbered at i=" << i << " iter=" << iter;
                }
                for (size_t i = 0; i < expected.size(); ++i) {
                    ASSERT_EQ(expected[i], in_place[from + i])
                            << "compress_avx2_w8 in-place mismatch at i=" << i << " iter=" << iter;
                }
            }

            // 3. Public API filter_range<uint64_t>: out-of-place
            {
                std::vector<uint64_t> dst(buf_size, 0xBAADF00DDEADBEEFULL);
                size_t out_sz = SIMD::Filter::filter_range(dst.data(), src.data(), selector.data(), from, to);
                ASSERT_EQ(from + expected.size(), out_sz) << "filter_range<uint64_t> out-of-place iter=" << iter;
                for (size_t i = 0; i < from; ++i) {
                    ASSERT_EQ(0xBAADF00DDEADBEEFULL, dst[i])
                            << "filter_range<uint64_t> prefix clobbered at i=" << i << " iter=" << iter;
                }
                for (size_t i = 0; i < expected.size(); ++i) {
                    ASSERT_EQ(expected[i], dst[from + i])
                            << "filter_range<uint64_t> mismatch at i=" << i << " iter=" << iter;
                }
            }

            // 4. Public API filter_range<uint64_t>: in-place
            {
                std::vector<uint64_t> in_place = src;
                size_t ip_sz = SIMD::Filter::filter_range(in_place.data(), in_place.data(), selector.data(), from, to);
                ASSERT_EQ(from + expected.size(), ip_sz) << "filter_range<uint64_t> in-place iter=" << iter;
                for (size_t i = 0; i < from; ++i) {
                    ASSERT_EQ(src[i], in_place[i])
                            << "filter_range<uint64_t> in-place prefix clobbered at i=" << i << " iter=" << iter;
                }
                for (size_t i = 0; i < expected.size(); ++i) {
                    ASSERT_EQ(expected[i], in_place[from + i])
                            << "filter_range<uint64_t> in-place mismatch at i=" << i << " iter=" << iter;
                }
            }

            // 5. Direct AVX2 dispatch entrypoint filter_range_avx2 (8B): out-of-place
            {
                std::vector<uint64_t> dst(buf_size, 0xBAADF00DDEADBEEFULL);
                size_t out_sz = SIMD::Filter::detail::filter_range_avx2(dst.data(), src.data(), sizeof(uint64_t),
                                                                        selector.data(), from, to);
                ASSERT_EQ(from + expected.size(), out_sz) << "filter_range_avx2(8B) out-of-place iter=" << iter;
                for (size_t i = 0; i < from; ++i) {
                    ASSERT_EQ(0xBAADF00DDEADBEEFULL, dst[i])
                            << "filter_range_avx2(8B) prefix clobbered at i=" << i << " iter=" << iter;
                }
                for (size_t i = 0; i < expected.size(); ++i) {
                    ASSERT_EQ(expected[i], dst[from + i])
                            << "filter_range_avx2(8B) mismatch at i=" << i << " iter=" << iter;
                }
            }

            // 6. Direct AVX2 dispatch entrypoint filter_range_avx2 (8B): in-place
            {
                std::vector<uint64_t> in_place = src;
                size_t ip_sz = SIMD::Filter::detail::filter_range_avx2(in_place.data(), in_place.data(),
                                                                       sizeof(uint64_t), selector.data(), from, to);
                ASSERT_EQ(from + expected.size(), ip_sz) << "filter_range_avx2(8B) in-place iter=" << iter;
                for (size_t i = 0; i < from; ++i) {
                    ASSERT_EQ(src[i], in_place[i])
                            << "filter_range_avx2(8B) in-place prefix clobbered at i=" << i << " iter=" << iter;
                }
                for (size_t i = 0; i < expected.size(); ++i) {
                    ASSERT_EQ(expected[i], in_place[from + i])
                            << "filter_range_avx2(8B) in-place mismatch at i=" << i << " iter=" << iter;
                }
            }
        }
    }
}

PARALLEL_TEST(SimdFilterTest, x86_64_avx2_dispatch_entrypoint) {
    if (!__builtin_cpu_supports("avx2")) {
        GTEST_SKIP() << "AVX2 is not supported on this CPU";
    }
    const std::vector<size_t> from_offsets = {0, 1, 7, 16, 32};
    const std::vector<size_t> lengths = {0, 1, 15, 31, 32, 33, 64, 129};
    const uint8_t non_canonical_palette[] = {0x80, 0x00, 0x7F, 0x00, 0xFF, 0x01, 0x00, 0x02};

    auto run_for_type = [&](auto gen_fn, auto canary_val) {
        using T = decltype(canary_val);
        for (size_t from : from_offsets) {
            for (size_t len : lengths) {
                size_t to = from + len;
                size_t buf_size = to + 16;
                for (int mode = 0; mode < 5; ++mode) {
                    std::vector<T> src(buf_size);
                    std::vector<uint8_t> selector(buf_size, 0);
                    std::vector<T> expected;

                    for (size_t i = 0; i < buf_size; ++i) {
                        src[i] = gen_fn(i);
                    }

                    for (size_t i = 0; i < to; ++i) {
                        uint8_t sel_byte = 0;
                        switch (mode) {
                        case 0:
                            sel_byte = 0;
                            break;
                        case 1:
                            sel_byte = 1;
                            break;
                        case 2:
                            sel_byte = (i % 2 == 0) ? 1 : 0;
                            break;
                        case 3:
                            sel_byte = ((i * 7 + 3) % 8 < 3) ? 1 : 0;
                            break;
                        case 4:
                            sel_byte = non_canonical_palette[i % std::size(non_canonical_palette)];
                            break;
                        }
                        selector[i] = sel_byte;
                        if (i >= from && sel_byte != 0) {
                            expected.push_back(src[i]);
                        }
                    }

                    // 1. Separate destination buffer
                    {
                        std::vector<T> dst(buf_size, canary_val);
                        size_t out_sz = SIMD::Filter::detail::filter_range_avx2(dst.data(), src.data(), sizeof(T),
                                                                                selector.data(), from, to);
                        ASSERT_EQ(from + expected.size(), out_sz)
                                << "elem_size=" << sizeof(T) << " from=" << from << " to=" << to << " mode=" << mode;
                        for (size_t i = 0; i < from; ++i) {
                            ASSERT_TRUE(dst[i] == canary_val)
                                    << "Untouched prefix clobbered at i=" << i << " from=" << from;
                        }
                        for (size_t i = 0; i < expected.size(); ++i) {
                            ASSERT_TRUE(dst[from + i] == expected[i])
                                    << "Mismatch at i=" << i << " elem_size=" << sizeof(T) << " from=" << from
                                    << " to=" << to << " mode=" << mode;
                        }
                    }

                    // 2. In-place compaction (dst == src)
                    {
                        std::vector<T> in_place = src;
                        size_t ip_sz = SIMD::Filter::detail::filter_range_avx2(in_place.data(), in_place.data(),
                                                                               sizeof(T), selector.data(), from, to);
                        ASSERT_EQ(from + expected.size(), ip_sz)
                                << "In-place elem_size=" << sizeof(T) << " from=" << from << " to=" << to
                                << " mode=" << mode;
                        for (size_t i = 0; i < from; ++i) {
                            ASSERT_TRUE(in_place[i] == src[i])
                                    << "In-place prefix clobbered at i=" << i << " from=" << from;
                        }
                        for (size_t i = 0; i < expected.size(); ++i) {
                            ASSERT_TRUE(in_place[from + i] == expected[i])
                                    << "In-place mismatch at i=" << i << " elem_size=" << sizeof(T) << " from=" << from
                                    << " to=" << to << " mode=" << mode;
                        }
                    }
                }
            }
        }
    };

    run_for_type([](size_t i) -> uint8_t { return static_cast<uint8_t>(i * 17 + 3); }, uint8_t{0xA5});
    run_for_type([](size_t i) -> uint16_t { return static_cast<uint16_t>(i * 1337 + 5); }, uint16_t{0xBEEF});
    run_for_type([](size_t i) -> uint32_t { return static_cast<uint32_t>(i * 2654435761u + 7); },
                 uint32_t{0xBAADF00Du});
    run_for_type([](size_t i) -> uint64_t { return static_cast<uint64_t>(i * 6364136223846793005ull + 11); },
                 uint64_t{0xBAADF00DDEADBEEFull});
    run_for_type(
            [](size_t i) -> Elem16B {
                return Elem16B{static_cast<uint64_t>(i * 2654435761ull),
                               static_cast<uint64_t>(i ^ 0xdeadbeefcafebabeull)};
            },
            Elem16B{0xCAFEBABE12345678ull, 0x87654321FEEDC0DEull});
}

#endif // __x86_64__

} // namespace starrocks
