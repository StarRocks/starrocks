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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <random>
#include <string>
#include <vector>

#include "base/testutil/parallel_test.h"

namespace starrocks {

namespace {

constexpr size_t kPoolSize = 200003;
// Written past the end of every destination to catch out-of-bounds stores.
constexpr size_t kGuard = 64;

template <typename T>
T make_value(size_t i) {
    return static_cast<T>(i * 2654435761ULL + 7);
}

template <typename T>
std::vector<T> make_source() {
    std::vector<T> src(kPoolSize);
    for (size_t i = 0; i < kPoolSize; ++i) {
        src[i] = make_value<T>(i);
    }
    return src;
}

// The returned vector has exactly `n` elements so that ASAN flags any read past the end.
template <typename IndexType>
std::vector<IndexType> make_indexes(size_t n, uint32_t seed) {
    std::mt19937 rng(seed);
    std::uniform_int_distribution<uint32_t> dist(0, kPoolSize - 1);
    std::vector<IndexType> indexes(n);
    for (auto& index : indexes) {
        index = static_cast<IndexType>(dist(rng));
    }
    return indexes;
}

enum class FilterPattern {
    ModuloMix,
    Alternating,
    Random,
    AllZeros,
    AllOnes,
};

template <typename CondType = uint8_t>
std::vector<CondType> make_filter(size_t num_rows, FilterPattern pattern, uint32_t seed = 777) {
    std::vector<CondType> is_filtered(num_rows);
    switch (pattern) {
    case FilterPattern::ModuloMix:
        for (size_t i = 0; i < num_rows; ++i) {
            const size_t pick = (i * 7 + num_rows) % 5;
            is_filtered[i] = static_cast<CondType>(pick == 0 ? 1 : (pick == 1 ? 2 : 0));
        }
        break;
    case FilterPattern::Alternating:
        for (size_t i = 0; i < num_rows; ++i) {
            is_filtered[i] = static_cast<CondType>(i % 2);
        }
        break;
    case FilterPattern::Random: {
        std::mt19937 rng(seed + num_rows);
        std::bernoulli_distribution dist(0.5);
        for (size_t i = 0; i < num_rows; ++i) {
            is_filtered[i] = static_cast<CondType>(dist(rng) ? 1 : 0);
        }
        break;
    }
    case FilterPattern::AllZeros:
        std::fill(is_filtered.begin(), is_filtered.end(), static_cast<CondType>(0));
        break;
    case FilterPattern::AllOnes:
        std::fill(is_filtered.begin(), is_filtered.end(), static_cast<CondType>(1));
        break;
    }
    return is_filtered;
}

template <typename T, typename IndexType>
void check_gather(size_t num_rows) {
    const std::vector<T> src = make_source<T>();
    const std::vector<IndexType> indexes = make_indexes<IndexType>(num_rows, 12345 + num_rows);
    const T sentinel = make_value<T>(999983);

    std::vector<T> dest(num_rows + kGuard, sentinel);
    SIMDGather::gather(dest.data(), src.data(), indexes.data(), num_rows);

    for (size_t i = 0; i < num_rows; ++i) {
        ASSERT_EQ(src[indexes[i]], dest[i]) << "num_rows=" << num_rows << " i=" << i;
    }
    for (size_t i = num_rows; i < dest.size(); ++i) {
        ASSERT_EQ(sentinel, dest[i]) << "write past the end, num_rows=" << num_rows << " i=" << i;
    }
}

template <typename T, typename IndexType, typename CondType = uint8_t>
void check_filtered(size_t num_rows, FilterPattern pattern = FilterPattern::ModuloMix) {
    const std::vector<T> src = make_source<T>();
    const std::vector<IndexType> indexes = make_indexes<IndexType>(num_rows, 777 + num_rows);
    const std::vector<CondType> is_filtered = make_filter<CondType>(num_rows, pattern, 777 + num_rows);
    const T sentinel = make_value<T>(999983);

    std::vector<T> dest(num_rows + kGuard, sentinel);
    SIMDGather::gather(dest.data(), src.data(), indexes.data(), is_filtered.data(), num_rows);

    for (size_t i = 0; i < num_rows; ++i) {
        const T expected = is_filtered[i] == 0 ? src[indexes[i]] : T(0);
        ASSERT_EQ(expected, dest[i]) << "num_rows=" << num_rows << " i=" << i
                                     << " pattern=" << static_cast<int>(pattern);
    }
    for (size_t i = num_rows; i < dest.size(); ++i) {
        ASSERT_EQ(sentinel, dest[i]) << "write past the end, num_rows=" << num_rows << " i=" << i;
    }
}

template <typename TB, typename TC>
void check_int16_buckets(size_t buckets, size_t num_rows) {
    // One padding element: the AVX2 path loads 4 bytes per int16 slot, so the last slot is over-read by 2 bytes.
    // Values stay non-negative (dictionary codes): the AVX2 path zero-extends while the scalar tail sign-extends.
    constexpr size_t kSlots = 1000;
    std::vector<int16_t> a(kSlots + 1, 0);
    for (size_t i = 0; i < kSlots; ++i) {
        a[i] = static_cast<int16_t>((i * 131) % 32768);
    }
    std::mt19937 rng(4242 + num_rows);
    std::uniform_int_distribution<uint32_t> dist(0, kSlots - 1);
    std::vector<TC> c(num_rows);
    for (auto& v : c) {
        v = static_cast<TC>(dist(rng));
    }
    std::vector<TB> b(num_rows + kGuard, static_cast<TB>(-1));
    SIMDGather::gather(b.data(), a.data(), c.data(), buckets, static_cast<int>(num_rows));

    for (size_t i = 0; i < num_rows; ++i) {
        ASSERT_EQ(static_cast<TB>(a[c[i]]), b[i]) << "buckets=" << buckets << " num_rows=" << num_rows << " i=" << i;
    }
    for (size_t i = num_rows; i < b.size(); ++i) {
        ASSERT_EQ(static_cast<TB>(-1), b[i]) << "write past the end";
    }
}

// Row counts straddling the vector width, the 8192-row boundary and large batches.
std::vector<size_t> boundary_row_counts() {
    return {0,    1,    7,    8,    9,    15,   16,   17,    24,    25,    31,    32,    33,    100,    8185,
            8190, 8191, 8192, 8193, 8194, 8199, 8200, 8201,  8207,  8208,  8209,  8215,  8216,  8217,   8223,
            8224, 8225, 8231, 8232, 8233, 8256, 8257, 16384, 16391, 65535, 65536, 65537, 65550, 100001, 131072};
}

} // namespace

PARALLEL_TEST(GatherTest, boundary_rows) {
    for (size_t n : boundary_row_counts()) {
        check_gather<uint32_t, uint32_t>(n);
        check_gather<int32_t, uint32_t>(n);
        check_gather<int32_t, int32_t>(n);
        check_gather<int64_t, uint32_t>(n);
        check_gather<int64_t, int32_t>(n);
        check_gather<int16_t, uint32_t>(n);
        check_gather<int8_t, int32_t>(n);
    }
}

PARALLEL_TEST(GatherTest, filtered_variant_zero_default) {
    for (size_t n : boundary_row_counts()) {
        check_filtered<uint32_t, uint32_t, uint8_t>(n);
        check_filtered<int32_t, uint32_t>(n);
        check_filtered<int32_t, int32_t>(n);
        check_filtered<int64_t, uint32_t>(n);
        check_filtered<int64_t, int32_t>(n);
    }
}

PARALLEL_TEST(GatherTest, filtered_patterns_production_types) {
    const std::vector<size_t> test_sizes = {0,    1,     7,     8,     15,    16,    31,     32,    100,
                                            8192, 16384, 65535, 65536, 65537, 65550, 100001, 131072};
    const std::vector<FilterPattern> patterns = {
            FilterPattern::AllZeros, FilterPattern::AllOnes,   FilterPattern::Alternating,
            FilterPattern::Random,   FilterPattern::ModuloMix,
    };
    for (FilterPattern pattern : patterns) {
        for (size_t n : test_sizes) {
            check_filtered<uint32_t, uint32_t, uint8_t>(n, pattern);
        }
    }
}

PARALLEL_TEST(GatherTest, int16_buckets) {
    // `buckets` below max_process_size takes the AVX2 path where available; at or above it the scalar path.
    for (size_t buckets : {size_t(1000), size_t(SIMDGather::max_process_size)}) {
        for (size_t n : boundary_row_counts()) {
            check_int16_buckets<uint32_t, uint32_t>(buckets, n);
            check_int16_buckets<uint32_t, int32_t>(buckets, n);
            check_int16_buckets<int32_t, uint32_t>(buckets, n);
            check_int16_buckets<int32_t, int32_t>(buckets, n);
        }
    }
}

PARALLEL_TEST(GatherTest, string_gather) {
    std::vector<std::string> src(5000);
    for (size_t i = 0; i < src.size(); ++i) {
        // Mix of short (SSO) and heap-allocated strings.
        src[i] = std::string(i % 40, 'a' + (i % 26)) + std::to_string(i);
    }
    const std::vector<std::string> src_copy = src;

    for (size_t n : {size_t(0), size_t(1), size_t(8191), size_t(8192), size_t(8193), size_t(8199), size_t(20000)}) {
        std::mt19937 rng(99 + n);
        std::uniform_int_distribution<uint32_t> dist(0, src.size() - 1);
        std::vector<uint32_t> indexes(n);
        for (auto& index : indexes) {
            index = dist(rng);
        }
        std::vector<std::string> dest(n);
        SIMDGather::gather(dest.data(), src.data(), indexes.data(), n);
        for (size_t i = 0; i < n; ++i) {
            ASSERT_EQ(src_copy[indexes[i]], dest[i]) << "n=" << n << " i=" << i;
        }
    }
    // The source must be copied from, not moved from.
    ASSERT_EQ(src_copy, src);
}

} // namespace starrocks
