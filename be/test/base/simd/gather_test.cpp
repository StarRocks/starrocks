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

#include "base/simd/gather.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <numeric>
#include <random>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace starrocks {

template <size_t N>
struct alignas(1) TestBytes {
    uint8_t data[N]{};
    bool operator==(const TestBytes& other) const { return std::memcmp(data, other.data, N) == 0; }
};

struct NonTrivialType {
    std::string text;
    int value{0};

    NonTrivialType() = default;
    NonTrivialType(std::string t, int v) : text(std::move(t)), value(v) {}
    bool operator==(const NonTrivialType& other) const { return value == other.value && text == other.text; }
};

template <typename DataType, typename IndexType>
static void reference_gather(DataType* dest, const DataType* src, const IndexType* indexes, size_t num_rows) {
    for (size_t i = 0; i < num_rows; ++i) {
        dest[i] = src[indexes[i]];
    }
}

template <typename DataType, typename IndexType, typename CondType>
static void reference_gather_filtered(DataType* dest, const DataType* src, const IndexType* indexes,
                                      const CondType* is_filtered, size_t num_rows) {
    for (size_t i = 0; i < num_rows; ++i) {
        dest[i] = (is_filtered[i] == 0) ? src[indexes[i]] : DataType{};
    }
}

template <typename TB, typename TC>
static void run_bucket_gather_test(size_t buckets, int num_rows, uint32_t seed) {
    std::vector<int16_t> a(buckets);
    for (size_t i = 0; i < buckets; ++i) {
        a[i] = static_cast<int16_t>((i * 7 + 3) & 0x7FFF);
    }

    std::vector<TC> c(num_rows);
    std::mt19937 rng(seed);
    std::uniform_int_distribution<size_t> dist(0, buckets - 1);
    for (int i = 0; i < num_rows; ++i) {
        c[i] = static_cast<TC>(dist(rng));
    }

    std::vector<TB> expected(num_rows);
    for (int i = 0; i < num_rows; ++i) {
        expected[i] = static_cast<TB>(a[c[i]]);
    }

    std::vector<TB> actual(num_rows, 0);
    SIMDGather::gather(actual.data(), a.data(), c.data(), buckets, num_rows);

    for (int i = 0; i < num_rows; ++i) {
        ASSERT_EQ(expected[i], actual[i])
                << "bucket gather mismatch at " << i << " buckets=" << buckets << " num_rows=" << num_rows;
    }
}

template <typename TB, typename TC>
static void test_bucket_gather_matrix() {
    const size_t in_process_buckets = 256;
    const size_t large_buckets = SIMDGather::max_process_size + 64;
    const std::vector<int> row_counts = {7, 35, 1024, 16384};

    for (int n : row_counts) {
        run_bucket_gather_test<TB, TC>(in_process_buckets, n, static_cast<uint32_t>(505 + n));
        run_bucket_gather_test<TB, TC>(large_buckets, n, static_cast<uint32_t>(606 + n));
    }
}

template <typename DataType, typename IndexType>
static void run_unfiltered_gather_test(size_t src_size, size_t num_rows, uint32_t seed) {
    std::vector<DataType> src(src_size);
    if constexpr (std::is_same_v<DataType, std::string>) {
        for (size_t i = 0; i < src_size; ++i) {
            src[i] = "str_val_" + std::to_string(i * 13 + 5);
        }
    } else if constexpr (std::is_same_v<DataType, NonTrivialType>) {
        for (size_t i = 0; i < src_size; ++i) {
            src[i] = NonTrivialType("item_" + std::to_string(i), static_cast<int>(i * 17));
        }
    } else if constexpr (std::is_same_v<DataType, __int128_t>) {
        for (size_t i = 0; i < src_size; ++i) {
            src[i] = static_cast<__int128_t>(i * 1000000007LL + 13);
        }
    } else if constexpr (std::is_arithmetic_v<DataType>) {
        for (size_t i = 0; i < src_size; ++i) {
            src[i] = static_cast<DataType>((i * 37 + 11) % 251);
        }
    } else {
        // Struct types
        for (size_t i = 0; i < src_size; ++i) {
            for (size_t b = 0; b < sizeof(DataType); ++b) {
                reinterpret_cast<uint8_t*>(&src[i])[b] = static_cast<uint8_t>((i * 7 + b * 13) & 0xFF);
            }
        }
    }

    std::vector<IndexType> indexes(num_rows);
    std::mt19937 rng(seed);
    std::uniform_int_distribution<size_t> dist(0, src_size - 1);
    for (size_t i = 0; i < num_rows; ++i) {
        indexes[i] = static_cast<IndexType>(dist(rng));
    }

    std::vector<DataType> expected(num_rows);
    reference_gather(expected.data(), src.data(), indexes.data(), num_rows);

    std::vector<DataType> actual(num_rows);
    SIMDGather::gather(actual.data(), src.data(), indexes.data(), num_rows);

    for (size_t i = 0; i < num_rows; ++i) {
        ASSERT_TRUE(expected[i] == actual[i]) << "mismatch at " << i << " num_rows=" << num_rows;
    }
}

template <typename DataType>
static void test_unfiltered_for_type() {
    constexpr size_t src_size = 32768;
    const std::vector<size_t> sizes = {5, 32, 16384};
    for (size_t n : sizes) {
        run_unfiltered_gather_test<DataType, uint32_t>(src_size, n, static_cast<uint32_t>(101 + n));
        run_unfiltered_gather_test<DataType, int32_t>(src_size, n, static_cast<uint32_t>(202 + n));
    }
}

template <typename DataType, typename IndexType, typename CondType = uint8_t>
static void run_filtered_gather_test(size_t src_size, size_t num_rows, int filter_mode, uint32_t seed) {
    // filter_mode: 0 = 0% filtered (all passing), 1 = 50% alternating, 2 = 100% filtered
    std::vector<DataType> src(src_size);
    if constexpr (std::is_same_v<DataType, std::string>) {
        for (size_t i = 0; i < src_size; ++i) {
            src[i] = "filtered_str_" + std::to_string(i * 19 + 7);
        }
    } else if constexpr (std::is_same_v<DataType, NonTrivialType>) {
        for (size_t i = 0; i < src_size; ++i) {
            src[i] = NonTrivialType("fitem_" + std::to_string(i), static_cast<int>(i * 23));
        }
    } else if constexpr (std::is_same_v<DataType, __int128_t>) {
        for (size_t i = 0; i < src_size; ++i) {
            src[i] = static_cast<__int128_t>(i * 1000000009LL + 17);
        }
    } else if constexpr (std::is_arithmetic_v<DataType>) {
        for (size_t i = 0; i < src_size; ++i) {
            src[i] = static_cast<DataType>((i * 41 + 13) % 251);
        }
    } else {
        // Struct types
        for (size_t i = 0; i < src_size; ++i) {
            for (size_t b = 0; b < sizeof(DataType); ++b) {
                reinterpret_cast<uint8_t*>(&src[i])[b] = static_cast<uint8_t>((i * 11 + b * 17 + 1) & 0xFF);
            }
        }
    }

    std::vector<IndexType> indexes(num_rows);
    std::vector<CondType> is_filtered(num_rows);
    std::mt19937 rng(seed);
    std::uniform_int_distribution<size_t> dist(0, src_size - 1);

    for (size_t i = 0; i < num_rows; ++i) {
        indexes[i] = static_cast<IndexType>(dist(rng));
        if (filter_mode == 0) {
            is_filtered[i] = static_cast<CondType>(0);
        } else if (filter_mode == 1) {
            is_filtered[i] = static_cast<CondType>(i % 2);
        } else {
            is_filtered[i] = static_cast<CondType>(1);
        }
    }

    std::vector<DataType> expected(num_rows);
    reference_gather_filtered(expected.data(), src.data(), indexes.data(), is_filtered.data(), num_rows);

    std::vector<DataType> actual(num_rows);
    SIMDGather::gather(actual.data(), src.data(), indexes.data(), is_filtered.data(), num_rows);

    for (size_t i = 0; i < num_rows; ++i) {
        ASSERT_TRUE(expected[i] == actual[i])
                << "filtered mismatch at " << i << " num_rows=" << num_rows << " filter_mode=" << filter_mode;
    }
}

template <typename DataType>
static void test_filtered_for_type() {
    constexpr size_t src_size = 32768;
    const std::vector<size_t> sizes = {5, 27, 16384};
    const std::vector<int> filter_modes = {0, 1, 2};
    for (size_t n : sizes) {
        for (int mode : filter_modes) {
            run_filtered_gather_test<DataType, uint32_t>(src_size, n, mode, static_cast<uint32_t>(303 + n + mode * 17));
            run_filtered_gather_test<DataType, int32_t>(src_size, n, mode, static_cast<uint32_t>(404 + n + mode * 17));
        }
    }
}

TEST(GatherTest, cross_platform_gather_sweep_int32) {
    const std::vector<size_t> test_sizes = {1,  2,   7,   8,   15,  16,   23,   31,   32,   63,
                                            64, 127, 128, 255, 256, 1024, 4096, 8192, 8200, 16384};
    constexpr size_t src_size = 32768;
    std::vector<int32_t> src(src_size);
    for (size_t i = 0; i < src_size; ++i) {
        src[i] = static_cast<int32_t>(i * 101 + 7);
    }

    std::mt19937 rng(42);
    for (size_t n : test_sizes) {
        // 1. Random scattered indices
        {
            std::vector<uint32_t> indexes(n);
            std::uniform_int_distribution<uint32_t> dist(0, src_size - 1);
            for (size_t i = 0; i < n; ++i) {
                indexes[i] = dist(rng);
            }

            std::vector<int32_t> expected(n, 0);
            reference_gather(expected.data(), src.data(), indexes.data(), n);

            std::vector<int32_t> actual(n, 0);
            SIMDGather::gather(actual.data(), src.data(), indexes.data(), n);

            for (size_t i = 0; i < n; ++i) {
                ASSERT_EQ(expected[i], actual[i]) << "mismatch at index " << i << " for size " << n;
            }
        }

        // 2. Strided indices
        {
            std::vector<uint32_t> indexes(n);
            for (size_t i = 0; i < n; ++i) {
                indexes[i] = static_cast<uint32_t>((i * 3) % src_size);
            }

            std::vector<int32_t> expected(n, 0);
            reference_gather(expected.data(), src.data(), indexes.data(), n);

            std::vector<int32_t> actual(n, 0);
            SIMDGather::gather(actual.data(), src.data(), indexes.data(), n);

            for (size_t i = 0; i < n; ++i) {
                ASSERT_EQ(expected[i], actual[i]) << "stride mismatch at index " << i << " for size " << n;
            }
        }
    }
}

TEST(GatherTest, cross_platform_gather_sweep_int64) {
    const std::vector<size_t> test_sizes = {1, 3, 7, 8, 15, 16, 31, 32, 64, 128, 256, 1024, 4096, 8192, 16384};
    constexpr size_t src_size = 32768;
    std::vector<int64_t> src(src_size);
    for (size_t i = 0; i < src_size; ++i) {
        src[i] = static_cast<int64_t>(100000000000LL + i * 997);
    }

    std::mt19937 rng(1337);
    for (size_t n : test_sizes) {
        std::vector<uint32_t> indexes(n);
        std::uniform_int_distribution<uint32_t> dist(0, src_size - 1);
        for (size_t i = 0; i < n; ++i) {
            indexes[i] = dist(rng);
        }

        std::vector<int64_t> expected(n, 0);
        reference_gather(expected.data(), src.data(), indexes.data(), n);

        std::vector<int64_t> actual(n, 0);
        SIMDGather::gather(actual.data(), src.data(), indexes.data(), n);

        for (size_t i = 0; i < n; ++i) {
            ASSERT_EQ(expected[i], actual[i]) << "int64 mismatch at index " << i << " for size " << n;
        }
    }
}

TEST(GatherTest, cross_platform_gather_filtered_sweep) {
    const std::vector<size_t> test_sizes = {8, 16, 32, 64, 128, 512, 2048, 8192, 16384};
    const std::vector<int> selectivities = {0, 10, 50, 90, 100};
    constexpr size_t src_size = 32768;

    std::vector<int32_t> src(src_size);
    for (size_t i = 0; i < src_size; ++i) {
        src[i] = static_cast<int32_t>(i + 500);
    }

    std::mt19937 rng(777);
    for (size_t n : test_sizes) {
        for (int sel_pct : selectivities) {
            std::vector<uint32_t> indexes(n);
            std::vector<uint8_t> is_filtered(n);
            std::uniform_int_distribution<uint32_t> dist(0, src_size - 1);

            for (size_t i = 0; i < n; ++i) {
                indexes[i] = dist(rng);
                is_filtered[i] = ((i * 31 + sel_pct) % 100 < static_cast<size_t>(sel_pct)) ? 0 : 1;
            }

            std::vector<int32_t> expected(n, -1);
            reference_gather_filtered(expected.data(), src.data(), indexes.data(), is_filtered.data(), n);

            std::vector<int32_t> actual(n, -1);
            SIMDGather::gather(actual.data(), src.data(), indexes.data(), is_filtered.data(), n);

            for (size_t i = 0; i < n; ++i) {
                ASSERT_EQ(expected[i], actual[i]) << "filtered mismatch at " << i << " n=" << n << " sel=" << sel_pct;
            }
        }
    }
}

TEST(GatherTest, gather_int16_buckets) {
    // 1. In-process size (< max_process_size), with num_rows = 1024
    {
        constexpr size_t buckets = 256;
        constexpr int num_rows = 1024;
        std::vector<int16_t> a(buckets);
        for (size_t i = 0; i < buckets; ++i) {
            a[i] = static_cast<int16_t>(i * 3);
        }

        std::vector<uint32_t> c(num_rows);
        for (int i = 0; i < num_rows; ++i) {
            c[i] = static_cast<uint32_t>(i % buckets);
        }

        std::vector<int32_t> b(num_rows, 0);
        SIMDGather::gather(b.data(), a.data(), c.data(), buckets, num_rows);

        for (int i = 0; i < num_rows; ++i) {
            ASSERT_EQ(static_cast<int32_t>(a[c[i]]), b[i]) << "bucket gather mismatch at " << i;
        }
    }

    // 2. Large array (>= 8192) exceeding max_process_size to exercise prefetch loop
    {
        const size_t buckets = SIMDGather::max_process_size + 64;
        const int num_rows = 16384;
        std::vector<int16_t> a(buckets);
        for (size_t i = 0; i < buckets; ++i) {
            a[i] = static_cast<int16_t>((i * 7) & 0x7FFF);
        }

        std::vector<uint32_t> c(num_rows);
        for (int i = 0; i < num_rows; ++i) {
            c[i] = static_cast<uint32_t>((i * 13) % buckets);
        }

        std::vector<int32_t> b(num_rows, 0);
        SIMDGather::gather(b.data(), a.data(), c.data(), buckets, num_rows);

        for (int i = 0; i < num_rows; ++i) {
            ASSERT_EQ(static_cast<int32_t>(a[c[i]]), b[i]) << "large bucket gather mismatch at " << i;
        }
    }

    // 3. Small array (< 8192) exceeding max_process_size with unrolled loop and remainder
    {
        const size_t buckets = SIMDGather::max_process_size + 64;
        const int num_rows = 35;
        std::vector<int16_t> a(buckets);
        for (size_t i = 0; i < buckets; ++i) {
            a[i] = static_cast<int16_t>(i * 5);
        }

        std::vector<uint32_t> c(num_rows);
        for (int i = 0; i < num_rows; ++i) {
            c[i] = static_cast<uint32_t>(i % buckets);
        }

        std::vector<int32_t> b(num_rows, 0);
        SIMDGather::gather(b.data(), a.data(), c.data(), buckets, num_rows);

        for (int i = 0; i < num_rows; ++i) {
            ASSERT_EQ(static_cast<int32_t>(a[c[i]]), b[i]) << "unrolled with remainder bucket gather mismatch at " << i;
        }
    }

    // 4. Scalar-only remainder (< 8) exceeding max_process_size to exercise direct scalar remainder loop
    {
        const size_t buckets = SIMDGather::max_process_size + 64;
        const int num_rows = 7;
        std::vector<int16_t> a(buckets);
        for (size_t i = 0; i < buckets; ++i) {
            a[i] = static_cast<int16_t>(i * 11);
        }

        std::vector<uint32_t> c(num_rows);
        for (int i = 0; i < num_rows; ++i) {
            c[i] = static_cast<uint32_t>(i % buckets);
        }

        std::vector<int32_t> b(num_rows, 0);
        SIMDGather::gather(b.data(), a.data(), c.data(), buckets, num_rows);

        for (int i = 0; i < num_rows; ++i) {
            ASSERT_EQ(static_cast<int32_t>(a[c[i]]), b[i]) << "scalar remainder bucket gather mismatch at " << i;
        }
    }

    // 5. Signed int32 index type
    {
        const size_t buckets = SIMDGather::max_process_size + 64;
        const int num_rows = 19;
        std::vector<int16_t> a(buckets);
        for (size_t i = 0; i < buckets; ++i) {
            a[i] = static_cast<int16_t>(i * 13);
        }

        std::vector<int32_t> c(num_rows);
        for (int i = 0; i < num_rows; ++i) {
            c[i] = static_cast<int32_t>(i % buckets);
        }

        std::vector<int32_t> b(num_rows, 0);
        SIMDGather::gather(b.data(), a.data(), c.data(), buckets, num_rows);

        for (int i = 0; i < num_rows; ++i) {
            ASSERT_EQ(static_cast<int32_t>(a[c[i]]), b[i]) << "signed index bucket gather mismatch at " << i;
        }
    }

    // 6. Unsigned uint32 destination type with uint32 index
    {
        constexpr size_t buckets = 256;
        constexpr int num_rows = 35;
        std::vector<int16_t> a(buckets);
        for (size_t i = 0; i < buckets; ++i) {
            a[i] = static_cast<int16_t>(i * 3 + 1);
        }

        std::vector<uint32_t> c(num_rows);
        for (int i = 0; i < num_rows; ++i) {
            c[i] = static_cast<uint32_t>(i % buckets);
        }

        std::vector<uint32_t> b(num_rows, 0);
        SIMDGather::gather(b.data(), a.data(), c.data(), buckets, num_rows);

        for (int i = 0; i < num_rows; ++i) {
            ASSERT_EQ(static_cast<uint32_t>(a[c[i]]), b[i]) << "unsigned dest bucket gather mismatch at " << i;
        }
    }

    // 7. Unsigned uint32 destination type with signed int32 index
    {
        const size_t buckets = SIMDGather::max_process_size + 64;
        constexpr int num_rows = 35;
        std::vector<int16_t> a(buckets);
        for (size_t i = 0; i < buckets; ++i) {
            a[i] = static_cast<int16_t>(i * 5 + 2);
        }

        std::vector<int32_t> c(num_rows);
        for (int i = 0; i < num_rows; ++i) {
            c[i] = static_cast<int32_t>(i % buckets);
        }

        std::vector<uint32_t> b(num_rows, 0);
        SIMDGather::gather(b.data(), a.data(), c.data(), buckets, num_rows);

        for (int i = 0; i < num_rows; ++i) {
            ASSERT_EQ(static_cast<uint32_t>(a[c[i]]), b[i])
                    << "unsigned dest signed index bucket gather mismatch at " << i;
        }
    }
}

TEST(GatherTest, bucket_gather_exhaustive_matrix) {
    // Tests all combinations of (signed/unsigned dest) x (signed/unsigned index)
    // across both in-process (< 512k) and large (>= 512k) bucket counts,
    // and across row counts < 8, [8, 8192), and >= 8192.
    test_bucket_gather_matrix<int32_t, uint32_t>();
    test_bucket_gather_matrix<int32_t, int32_t>();
    test_bucket_gather_matrix<uint32_t, uint32_t>();
    test_bucket_gather_matrix<uint32_t, int32_t>();
}

TEST(GatherTest, unfiltered_gather_element_widths) {
    // 1-byte elements
    test_unfiltered_for_type<uint8_t>();
    test_unfiltered_for_type<int8_t>();

    // 2-byte elements
    test_unfiltered_for_type<uint16_t>();
    test_unfiltered_for_type<int16_t>();

    // 4-byte elements
    test_unfiltered_for_type<uint32_t>();
    test_unfiltered_for_type<int32_t>();
    test_unfiltered_for_type<float>();

    // 8-byte elements
    test_unfiltered_for_type<uint64_t>();
    test_unfiltered_for_type<int64_t>();
    test_unfiltered_for_type<double>();

    // 12-byte elements
    test_unfiltered_for_type<TestBytes<12>>();

    // 16-byte elements
    test_unfiltered_for_type<TestBytes<16>>();
    test_unfiltered_for_type<__int128_t>();

    // 24-byte elements
    test_unfiltered_for_type<TestBytes<24>>();

    // 32-byte elements
    test_unfiltered_for_type<TestBytes<32>>();

    // Fallback unsupported sizes (non-power-of-2 / non-specialized)
    test_unfiltered_for_type<TestBytes<5>>();
    test_unfiltered_for_type<TestBytes<7>>();

    // Non-trivial type
    test_unfiltered_for_type<std::string>();
}

TEST(GatherTest, filtered_gather_element_widths) {
    // 1-byte elements
    test_filtered_for_type<uint8_t>();
    test_filtered_for_type<int8_t>();

    // 2-byte elements
    test_filtered_for_type<uint16_t>();
    test_filtered_for_type<int16_t>();

    // 4-byte elements
    test_filtered_for_type<uint32_t>();
    test_filtered_for_type<int32_t>();
    test_filtered_for_type<float>();

    // 8-byte elements
    test_filtered_for_type<uint64_t>();
    test_filtered_for_type<int64_t>();
    test_filtered_for_type<double>();

    // 12-byte elements
    test_filtered_for_type<TestBytes<12>>();

    // 16-byte elements
    test_filtered_for_type<TestBytes<16>>();
    test_filtered_for_type<__int128_t>();

    // 24-byte elements
    test_filtered_for_type<TestBytes<24>>();

    // 32-byte elements
    test_filtered_for_type<TestBytes<32>>();

    // Fallback unsupported size
    test_filtered_for_type<TestBytes<5>>();

    // Non-trivial type
    test_filtered_for_type<std::string>();
}

TEST(GatherTest, fallback_gather_sweeps) {
    // Exercises SIMDGather::gather fallback paths for non-trivially copyable types,
    // non-32-bit index types, and non-1-byte condition types.
    constexpr size_t src_size = 256;
    const std::vector<size_t> sizes = {5, 32, 1024};

    // 1. Non-trivially copyable unfiltered gather
    for (size_t n : sizes) {
        run_unfiltered_gather_test<NonTrivialType, uint32_t>(src_size, n, static_cast<uint32_t>(701 + n));
        run_unfiltered_gather_test<NonTrivialType, int32_t>(src_size, n, static_cast<uint32_t>(702 + n));
    }

    // 2. Non-trivially copyable filtered gather
    for (size_t n : sizes) {
        for (int mode : {0, 1, 2}) {
            run_filtered_gather_test<NonTrivialType, uint32_t>(src_size, n, mode,
                                                               static_cast<uint32_t>(703 + n + mode * 13));
        }
    }

    // 3. Non-32-bit index types (uint16_t, uint64_t)
    for (size_t n : {5UL, 32UL}) {
        run_unfiltered_gather_test<int32_t, uint16_t>(src_size, n, static_cast<uint32_t>(801 + n));
        run_unfiltered_gather_test<int32_t, uint64_t>(src_size, n, static_cast<uint32_t>(802 + n));
        run_filtered_gather_test<int32_t, uint16_t>(src_size, n, 1, static_cast<uint32_t>(803 + n));
        run_filtered_gather_test<int32_t, uint64_t>(src_size, n, 1, static_cast<uint32_t>(804 + n));
    }

    // 4. Non-1-byte condition type (int32_t cond)
    for (size_t n : {5UL, 32UL}) {
        run_filtered_gather_test<int32_t, uint32_t, int32_t>(src_size, n, 1, static_cast<uint32_t>(901 + n));
    }
}

TEST(GatherTest, filtered_gather_comprehensive_coverage) {
    constexpr size_t src_size = 32768;
    std::vector<int32_t> src(src_size);
    for (size_t i = 0; i < src_size; ++i) {
        src[i] = static_cast<int32_t>(i * 17 + 3);
    }

    // 1. num_rows = 16384 with alternating filter pattern (exercises lines 156-175 with both filtered and non-filtered entries)
    {
        constexpr size_t num_rows = 16384;
        std::vector<uint32_t> indexes(num_rows);
        std::vector<uint8_t> is_filtered(num_rows);
        for (size_t i = 0; i < num_rows; ++i) {
            indexes[i] = static_cast<uint32_t>((i * 19) % src_size);
            is_filtered[i] = static_cast<uint8_t>(i % 2);
        }

        std::vector<int32_t> expected(num_rows, -1);
        reference_gather_filtered(expected.data(), src.data(), indexes.data(), is_filtered.data(), num_rows);

        std::vector<int32_t> actual(num_rows, -1);
        SIMDGather::gather(actual.data(), src.data(), indexes.data(), is_filtered.data(), num_rows);

        for (size_t i = 0; i < num_rows; ++i) {
            ASSERT_EQ(expected[i], actual[i]) << "alternating filter mismatch at index " << i;
        }
    }

    // 2. num_rows = 16384 with 100% filtered entries (is_filtered == 1)
    {
        constexpr size_t num_rows = 16384;
        std::vector<uint32_t> indexes(num_rows);
        std::vector<uint8_t> is_filtered(num_rows, 1);
        for (size_t i = 0; i < num_rows; ++i) {
            indexes[i] = static_cast<uint32_t>((i * 23) % src_size);
        }

        std::vector<int32_t> expected(num_rows, -1);
        reference_gather_filtered(expected.data(), src.data(), indexes.data(), is_filtered.data(), num_rows);

        std::vector<int32_t> actual(num_rows, -1);
        SIMDGather::gather(actual.data(), src.data(), indexes.data(), is_filtered.data(), num_rows);

        for (size_t i = 0; i < num_rows; ++i) {
            ASSERT_EQ(expected[i], actual[i]) << "100% filtered mismatch at index " << i;
            ASSERT_EQ(0, actual[i]) << "expected 0 for filtered entry at index " << i;
        }
    }

    // 3. num_rows = 16384 with 0% filtered entries (is_filtered == 0)
    {
        constexpr size_t num_rows = 16384;
        std::vector<uint32_t> indexes(num_rows);
        std::vector<uint8_t> is_filtered(num_rows, 0);
        for (size_t i = 0; i < num_rows; ++i) {
            indexes[i] = static_cast<uint32_t>((i * 29) % src_size);
        }

        std::vector<int32_t> expected(num_rows, -1);
        reference_gather_filtered(expected.data(), src.data(), indexes.data(), is_filtered.data(), num_rows);

        std::vector<int32_t> actual(num_rows, -1);
        SIMDGather::gather(actual.data(), src.data(), indexes.data(), is_filtered.data(), num_rows);

        for (size_t i = 0; i < num_rows; ++i) {
            ASSERT_EQ(expected[i], actual[i]) << "0% filtered mismatch at index " << i;
        }
    }

    // 4. num_rows = 27 (exercises lines 178-186 unrolled loop and 189-190 scalar tail)
    {
        constexpr size_t num_rows = 27;
        std::vector<uint32_t> indexes(num_rows);
        std::vector<uint8_t> is_filtered(num_rows);
        for (size_t i = 0; i < num_rows; ++i) {
            indexes[i] = static_cast<uint32_t>((i * 31) % src_size);
            is_filtered[i] = static_cast<uint8_t>(i % 3 == 0 ? 1 : 0);
        }

        std::vector<int32_t> expected(num_rows, -1);
        reference_gather_filtered(expected.data(), src.data(), indexes.data(), is_filtered.data(), num_rows);

        std::vector<int32_t> actual(num_rows, -1);
        SIMDGather::gather(actual.data(), src.data(), indexes.data(), is_filtered.data(), num_rows);

        for (size_t i = 0; i < num_rows; ++i) {
            ASSERT_EQ(expected[i], actual[i]) << "tail filtered mismatch at index " << i;
        }
    }

    // 5. num_rows = 7 (exercises direct scalar tail < 8 for 5-arg filtered gather)
    {
        constexpr size_t num_rows = 7;
        std::vector<uint32_t> indexes(num_rows);
        std::vector<uint8_t> is_filtered(num_rows);
        for (size_t i = 0; i < num_rows; ++i) {
            indexes[i] = static_cast<uint32_t>((i * 37) % src_size);
            is_filtered[i] = static_cast<uint8_t>(i % 2);
        }

        std::vector<int32_t> expected(num_rows, -1);
        reference_gather_filtered(expected.data(), src.data(), indexes.data(), is_filtered.data(), num_rows);

        std::vector<int32_t> actual(num_rows, -1);
        SIMDGather::gather(actual.data(), src.data(), indexes.data(), is_filtered.data(), num_rows);

        for (size_t i = 0; i < num_rows; ++i) {
            ASSERT_EQ(expected[i], actual[i]) << "scalar-only filtered mismatch at index " << i;
        }
    }

    // 6. int64_t data type with 5-arg filtered gather
    {
        constexpr size_t num_rows = 27;
        std::vector<int64_t> src64(src_size);
        for (size_t i = 0; i < src_size; ++i) {
            src64[i] = static_cast<int64_t>(5000000000LL + i * 101);
        }
        std::vector<uint32_t> indexes(num_rows);
        std::vector<uint8_t> is_filtered(num_rows);
        for (size_t i = 0; i < num_rows; ++i) {
            indexes[i] = static_cast<uint32_t>((i * 41) % src_size);
            is_filtered[i] = static_cast<uint8_t>(i % 2);
        }

        std::vector<int64_t> expected(num_rows, -1);
        reference_gather_filtered(expected.data(), src64.data(), indexes.data(), is_filtered.data(), num_rows);

        std::vector<int64_t> actual(num_rows, -1);
        SIMDGather::gather(actual.data(), src64.data(), indexes.data(), is_filtered.data(), num_rows);

        for (size_t i = 0; i < num_rows; ++i) {
            ASSERT_EQ(expected[i], actual[i]) << "int64 filtered mismatch at index " << i;
        }
    }
}

TEST(GatherTest, non_trivial_string_gather) {
    const size_t n = 64;
    std::vector<std::string> src(n);
    for (size_t i = 0; i < n; ++i) {
        src[i] = "long_string_heap_allocated_payload_" + std::to_string(i * 997);
    }

    std::vector<uint32_t> indexes(n);
    std::vector<uint8_t> is_filtered(n, 0);
    for (size_t i = 0; i < n; ++i) {
        indexes[i] = (i * 17) % n;
        is_filtered[i] = (i % 2 == 0) ? 0 : 1;
    }

    // 1. Unfiltered gather with std::string
    std::vector<std::string> dest(n);
    SIMDGather::gather(dest.data(), src.data(), indexes.data(), n);
    for (size_t i = 0; i < n; ++i) {
        ASSERT_EQ(src[indexes[i]], dest[i]);
    }

    // 2. Filtered gather with std::string
    std::vector<std::string> dest_filtered(n);
    SIMDGather::gather(dest_filtered.data(), src.data(), indexes.data(), is_filtered.data(), n);
    for (size_t i = 0; i < n; ++i) {
        if (is_filtered[i] == 0) {
            ASSERT_EQ(src[indexes[i]], dest_filtered[i]);
        } else {
            ASSERT_EQ("", dest_filtered[i]);
        }
    }
}

#pragma pack(push, 1)
struct Packed4 {
    uint8_t a;
    uint8_t b;
    uint8_t c;
    uint8_t d;
    bool operator==(const Packed4& o) const { return a == o.a && b == o.b && c == o.c && d == o.d; }
};

struct Packed8 {
    uint8_t bytes[8];
    bool operator==(const Packed8& o) const { return std::memcmp(bytes, o.bytes, 8) == 0; }
};
#pragma pack(pop)

TEST(GatherTest, WeaklyAlignedStructs) {
    constexpr size_t N = 10000;
    std::vector<Packed4> src4(N);
    std::vector<Packed4> dst4(N);
    std::vector<Packed8> src8(N);
    std::vector<Packed8> dst8(N);
    std::vector<uint32_t> indexes(N);

    for (size_t i = 0; i < N; ++i) {
        src4[i] = {static_cast<uint8_t>(i), static_cast<uint8_t>(i + 1), static_cast<uint8_t>(i + 2),
                   static_cast<uint8_t>(i + 3)};
        for (int k = 0; k < 8; ++k) src8[i].bytes[k] = static_cast<uint8_t>(i + k);
        indexes[i] = (i * 37) % N;
    }

    SIMDGather::gather(dst4.data(), src4.data(), indexes.data(), N);
    for (size_t i = 0; i < N; ++i) {
        ASSERT_EQ(dst4[i], src4[indexes[i]]);
    }

    SIMDGather::gather(dst8.data(), src8.data(), indexes.data(), N);
    for (size_t i = 0; i < N; ++i) {
        ASSERT_EQ(dst8[i], src8[indexes[i]]);
    }
}

TEST(GatherTest, FloatDoubleAliasing) {
    constexpr size_t N = 10000;
    std::vector<float> src_f(N);
    std::vector<float> dst_f(N);
    std::vector<double> src_d(N);
    std::vector<double> dst_d(N);
    std::vector<uint8_t> filter(N);
    std::vector<uint32_t> indexes(N);

    for (size_t i = 0; i < N; ++i) {
        src_f[i] = static_cast<float>(i) * 1.5f;
        src_d[i] = static_cast<double>(i) * 3.14159;
        indexes[i] = (i * 17) % N;
        filter[i] = (i % 3 == 0) ? 1 : 0;
    }

    SIMDGather::gather(dst_f.data(), src_f.data(), indexes.data(), N);
    for (size_t i = 0; i < N; ++i) {
        ASSERT_FLOAT_EQ(dst_f[i], src_f[indexes[i]]);
    }

    SIMDGather::gather(dst_d.data(), src_d.data(), indexes.data(), filter.data(), N);
    for (size_t i = 0; i < N; ++i) {
        if (filter[i] == 0) {
            ASSERT_DOUBLE_EQ(dst_d[i], src_d[indexes[i]]);
        } else {
            ASSERT_DOUBLE_EQ(dst_d[i], 0.0);
        }
    }
}

struct MemberPtrHost {
    int x;
    int y;
};

struct CustomDefault {
    int val = 12345;
    bool operator==(const CustomDefault& o) const { return val == o.val; }
};

TEST(GatherTest, FilteredNonArithmeticTypedZero) {
    using MemberPtr = int MemberPtrHost::*;
    static_assert(std::is_trivially_copyable_v<MemberPtr>);
    static_assert(!std::is_arithmetic_v<MemberPtr>);

    constexpr size_t N = 1000;
    std::vector<MemberPtr> src(N);
    std::vector<MemberPtr> dst(N);
    std::vector<uint32_t> indexes(N);
    std::vector<uint8_t> filter(N);

    for (size_t i = 0; i < N; ++i) {
        src[i] = (i % 2 == 0) ? &MemberPtrHost::x : &MemberPtrHost::y;
        indexes[i] = (i * 17) % N;
        filter[i] = (i % 3 == 0) ? 1 : 0;
    }

    SIMDGather::gather(dst.data(), src.data(), indexes.data(), filter.data(), N);
    for (size_t i = 0; i < N; ++i) {
        if (filter[i] == 0) {
            ASSERT_EQ(dst[i], src[indexes[i]]);
        } else {
            ASSERT_EQ(dst[i], nullptr);
        }
    }
}

TEST(GatherTest, FilteredCustomDefault) {
    static_assert(std::is_trivially_copyable_v<CustomDefault>);
    static_assert(!std::is_arithmetic_v<CustomDefault>);

    constexpr size_t N = 1000;
    std::vector<CustomDefault> src(N);
    std::vector<CustomDefault> dst(N);
    std::vector<uint32_t> indexes(N);
    std::vector<uint8_t> filter(N);

    for (size_t i = 0; i < N; ++i) {
        src[i] = CustomDefault{static_cast<int>(i + 1)};
        indexes[i] = (i * 31) % N;
        filter[i] = (i % 4 == 0) ? 1 : 0;
    }

    SIMDGather::gather(dst.data(), src.data(), indexes.data(), filter.data(), N);
    for (size_t i = 0; i < N; ++i) {
        if (filter[i] == 0) {
            ASSERT_EQ(dst[i], src[indexes[i]]);
        } else {
            ASSERT_EQ(dst[i].val, 12345);
        }
    }
}

} // namespace starrocks
