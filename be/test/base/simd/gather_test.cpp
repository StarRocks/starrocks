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
#include <numeric>
#include <random>
#include <vector>

namespace starrocks {

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

TEST(GatherTest, cross_platform_gather_sweep_int32) {
    const std::vector<size_t> test_sizes = {1, 2, 7, 8, 15, 16, 23, 31, 32, 63, 64, 127, 128, 255, 256, 1024, 4096};
    constexpr size_t src_size = 16384;
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
    const std::vector<size_t> test_sizes = {1, 3, 7, 8, 15, 16, 31, 32, 64, 128, 256, 1024, 4096};
    constexpr size_t src_size = 16384;
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
    const std::vector<size_t> test_sizes = {8, 16, 32, 64, 128, 512, 2048};
    const std::vector<int> selectivities = {0, 10, 30, 50, 70, 90, 100};
    constexpr size_t src_size = 8192;

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
                is_filtered[i] = ((i * 31 + sel_pct) % 100 < sel_pct) ? 0 : 1;
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

} // namespace starrocks
