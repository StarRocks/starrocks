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
#include <type_traits>
#include <utility>
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

template <typename T, typename IndexType>
void check_caller_informed_gather(size_t num_rows, size_t pool_size) {
    const std::vector<T> src = make_source<T>();
    const std::vector<IndexType> indexes = make_indexes<IndexType>(num_rows, 12345 + num_rows);
    const T sentinel = make_value<T>(999983);

    std::vector<T> dest(num_rows + kGuard, sentinel);
    SIMDGather::gather(dest.data(), src.data(), indexes.data(), num_rows, SourcePoolSize{pool_size});

    for (size_t i = 0; i < num_rows; ++i) {
        ASSERT_EQ(src[indexes[i]], dest[i]) << "num_rows=" << num_rows << " pool_size=" << pool_size << " i=" << i;
    }
    for (size_t i = num_rows; i < dest.size(); ++i) {
        ASSERT_EQ(sentinel, dest[i]) << "write past the end, num_rows=" << num_rows << " pool_size=" << pool_size
                                     << " i=" << i;
    }
}

template <typename T, typename IndexType, typename CondType = uint8_t>
void check_caller_informed_filtered(size_t num_rows, size_t pool_size, FilterPattern pattern = FilterPattern::ModuloMix,
                                    bool poison_filtered_indices = false) {
    const std::vector<T> src = make_source<T>();
    std::vector<IndexType> indexes = make_indexes<IndexType>(num_rows, 777 + num_rows);
    const std::vector<CondType> is_filtered = make_filter<CondType>(num_rows, pattern, 777 + num_rows);
    const T sentinel = make_value<T>(999983);

    if (poison_filtered_indices) {
        for (size_t i = 0; i < num_rows; ++i) {
            if (is_filtered[i] != 0) {
                // Out-of-bounds index: accessing src[indexes[i]] would be an invalid read or segfault
                indexes[i] = static_cast<IndexType>(kPoolSize + 50000 + i);
            }
        }
    }

    std::vector<T> dest(num_rows + kGuard, sentinel);
    SIMDGather::gather(dest.data(), src.data(), indexes.data(), is_filtered.data(), num_rows,
                       SourcePoolSize{pool_size});

    for (size_t i = 0; i < num_rows; ++i) {
        const T expected = is_filtered[i] == 0 ? src[indexes[i]] : T(0);
        ASSERT_EQ(expected, dest[i]) << "num_rows=" << num_rows << " pool_size=" << pool_size << " i=" << i
                                     << " pattern=" << static_cast<int>(pattern);
    }
    for (size_t i = num_rows; i < dest.size(); ++i) {
        ASSERT_EQ(sentinel, dest[i]) << "write past the end, num_rows=" << num_rows << " pool_size=" << pool_size
                                     << " i=" << i;
    }
}

TEST(SIMDGatherTest, test_caller_informed_pool_size) {
    const std::vector<size_t> row_counts = {0,  1,   7,    8,    15,   16,   31,   32,  63,
                                            64, 100, 4088, 4095, 4096, 4097, 4104, 8192};
    const std::vector<size_t> pool_sizes = {0, 1024, 4 * 1024 * 1024, 16 * 1024 * 1024, 64 * 1024 * 1024};

    for (size_t pool_size : pool_sizes) {
        for (size_t n : row_counts) {
            check_caller_informed_gather<uint32_t, uint32_t>(n, pool_size);
            check_caller_informed_gather<int32_t, uint32_t>(n, pool_size);
            check_caller_informed_gather<int32_t, int32_t>(n, pool_size);
            check_caller_informed_gather<uint64_t, uint32_t>(n, pool_size);
            check_caller_informed_gather<int64_t, uint32_t>(n, pool_size);
            check_caller_informed_gather<int64_t, int32_t>(n, pool_size);
            check_caller_informed_gather<int16_t, uint32_t>(n, pool_size);
            check_caller_informed_gather<int8_t, int32_t>(n, pool_size);
        }
    }

    // String gather with caller-informed pool size
    std::vector<std::string> src(2000);
    for (size_t i = 0; i < src.size(); ++i) {
        src[i] = std::string(i % 30, 'a' + (i % 26)) + std::to_string(i);
    }
    const std::vector<std::string> src_copy = src;
    for (size_t pool_size : {size_t(0), size_t(4 * 1024 * 1024), size_t(64 * 1024 * 1024)}) {
        for (size_t n : {size_t(0), size_t(1), size_t(4095), size_t(4096), size_t(4097), size_t(8192)}) {
            std::mt19937 rng(42 + n);
            std::uniform_int_distribution<uint32_t> dist(0, src.size() - 1);
            std::vector<uint32_t> indexes(n);
            for (auto& index : indexes) {
                index = dist(rng);
            }
            std::vector<std::string> dest(n);
            SIMDGather::gather(dest.data(), src.data(), indexes.data(), n, SourcePoolSize{pool_size});
            for (size_t i = 0; i < n; ++i) {
                ASSERT_EQ(src_copy[indexes[i]], dest[i]) << "n=" << n << " i=" << i;
            }
        }
    }
    ASSERT_EQ(src_copy, src);
}

TEST(SIMDGatherTest, test_caller_informed_filtered_pool_size) {
    const std::vector<size_t> row_counts = {0,  1,   7,    8,    15,   16,   31,   32,  63,
                                            64, 100, 4088, 4095, 4096, 4097, 4104, 8192};
    const std::vector<size_t> pool_sizes = {0, 1024, 4 * 1024 * 1024, 16 * 1024 * 1024, 64 * 1024 * 1024};
    const std::vector<FilterPattern> patterns = {
            FilterPattern::AllZeros, FilterPattern::AllOnes,   FilterPattern::Alternating,
            FilterPattern::Random,   FilterPattern::ModuloMix,
    };

    // Standard filter patterns with small and large DRAM pools
    for (size_t pool_size : pool_sizes) {
        for (FilterPattern pattern : patterns) {
            for (size_t n : row_counts) {
                check_caller_informed_filtered<uint32_t, uint32_t, uint8_t>(n, pool_size, pattern, false);
                check_caller_informed_filtered<int32_t, uint32_t, uint8_t>(n, pool_size, pattern, false);
                check_caller_informed_filtered<int64_t, uint32_t, uint8_t>(n, pool_size, pattern, false);
                check_caller_informed_filtered<uint64_t, int32_t, uint8_t>(n, pool_size, pattern, false);
            }
        }
    }

    // Critical edge case: invalid filtered indices that should NOT be accessed
    // Filtered entries have indices pointing far out of bounds (would crash/segfault if accessed)
    for (size_t pool_size : {size_t(0), size_t(4 * 1024 * 1024), size_t(16 * 1024 * 1024), size_t(64 * 1024 * 1024)}) {
        for (FilterPattern pattern : {FilterPattern::Alternating, FilterPattern::ModuloMix, FilterPattern::AllOnes}) {
            for (size_t n : {size_t(1), size_t(8), size_t(16), size_t(4088), size_t(4095), size_t(4096), size_t(4097),
                             size_t(4104), size_t(8192)}) {
                check_caller_informed_filtered<uint32_t, uint32_t, uint8_t>(n, pool_size, pattern, true);
                check_caller_informed_filtered<int32_t, int32_t, uint8_t>(n, pool_size, pattern, true);
                check_caller_informed_filtered<uint64_t, uint32_t, uint8_t>(n, pool_size, pattern, true);
                check_caller_informed_filtered<int64_t, int32_t, uint8_t>(n, pool_size, pattern, true);
            }
        }
    }
}

namespace {
struct TestPOD {
    int64_t x{0};
    int64_t y{0};
    TestPOD() = default;
    TestPOD(int64_t x_, int64_t y_) : x(x_), y(y_) {}
    bool operator==(const TestPOD& o) const = default;
};

inline std::ostream& operator<<(std::ostream& os, const TestPOD& p) {
    return os << "{" << p.x << ", " << p.y << "}";
}

struct NonZeroDefaultIntWrapper {
    int32_t value{7};
    constexpr NonZeroDefaultIntWrapper() = default;
    constexpr explicit NonZeroDefaultIntWrapper(int v) : value(v) {}
    bool operator==(const NonZeroDefaultIntWrapper& o) const = default;
};
static_assert(std::is_trivially_copyable_v<NonZeroDefaultIntWrapper>);
static_assert(std::is_constructible_v<NonZeroDefaultIntWrapper, int>);

inline std::ostream& operator<<(std::ostream& os, const NonZeroDefaultIntWrapper& w) {
    return os << "Wrapper(" << w.value << ")";
}
} // namespace

static_assert(std::is_trivially_copyable_v<TestPOD>);
static_assert(!std::is_constructible_v<TestPOD, int>);

TEST(SIMDGatherTest, test_custom_trivially_copyable_struct) {
    constexpr size_t kPodPoolSize = 1000;
    std::vector<TestPOD> src(kPodPoolSize);
    for (size_t i = 0; i < kPodPoolSize; ++i) {
        src[i] = TestPOD{static_cast<int64_t>(i * 3 + 1), static_cast<int64_t>(i * 7 + 2)};
    }
    const std::vector<TestPOD> src_copy = src;

    const std::vector<size_t> row_counts = {0, 1, 7, 8, 15, 16, 31, 32, 63, 64, 100, 4096, 4104};
    const std::vector<FilterPattern> patterns = {
            FilterPattern::AllZeros, FilterPattern::AllOnes,   FilterPattern::Alternating,
            FilterPattern::Random,   FilterPattern::ModuloMix,
    };

    for (size_t n : row_counts) {
        std::mt19937 rng(12345 + n);
        std::uniform_int_distribution<uint32_t> dist(0, kPodPoolSize - 1);
        std::vector<uint32_t> indexes(n);
        for (auto& idx : indexes) {
            idx = dist(rng);
        }

        // 1. Basic gather (4-arg)
        {
            std::vector<TestPOD> dest(n + kGuard, TestPOD{-1, -1});
            SIMDGather::gather(dest.data(), src.data(), indexes.data(), n);
            for (size_t i = 0; i < n; ++i) {
                ASSERT_EQ(src[indexes[i]], dest[i]) << "Basic gather failed at n=" << n << " i=" << i;
            }
            for (size_t g = n; g < n + kGuard; ++g) {
                ASSERT_EQ((TestPOD{-1, -1}), dest[g]) << "Guard corrupted in basic gather at n=" << n << " g=" << g;
            }
        }

        // 2. Caller-informed gather (5-arg)
        for (size_t pool_size : {size_t(0), size_t(1000), size_t(16 * 1024 * 1024), size_t(64 * 1024 * 1024)}) {
            std::vector<TestPOD> dest(n + kGuard, TestPOD{-1, -1});
            SIMDGather::gather(dest.data(), src.data(), indexes.data(), n, SourcePoolSize{pool_size});
            for (size_t i = 0; i < n; ++i) {
                ASSERT_EQ(src[indexes[i]], dest[i])
                        << "Caller-informed gather failed at n=" << n << " pool=" << pool_size;
            }
            for (size_t g = n; g < n + kGuard; ++g) {
                ASSERT_EQ((TestPOD{-1, -1}), dest[g]) << "Guard corrupted at n=" << n << " g=" << g;
            }
        }

        // 3. Filtered gather (5-arg and 6-arg)
        for (FilterPattern pattern : patterns) {
            auto is_filtered = make_filter<uint8_t>(n, pattern, 42 + n);
            // 5-arg gather_filtered
            {
                std::vector<TestPOD> dest(n + kGuard, TestPOD{-1, -1});
                SIMDGather::gather(dest.data(), src.data(), indexes.data(), is_filtered.data(), n);
                for (size_t i = 0; i < n; ++i) {
                    TestPOD expected = (is_filtered[i] == 0) ? src[indexes[i]] : TestPOD{};
                    ASSERT_EQ(expected, dest[i]) << "Filtered gather failed at n=" << n << " i=" << i;
                }
                for (size_t g = n; g < n + kGuard; ++g) {
                    ASSERT_EQ((TestPOD{-1, -1}), dest[g]) << "Guard corrupted at n=" << n << " g=" << g;
                }
            }

            // 6-arg caller-informed gather_filtered
            for (size_t pool_size : {size_t(0), size_t(1000), size_t(16 * 1024 * 1024), size_t(64 * 1024 * 1024)}) {
                std::vector<TestPOD> dest(n + kGuard, TestPOD{-1, -1});
                SIMDGather::gather(dest.data(), src.data(), indexes.data(), is_filtered.data(), n,
                                   SourcePoolSize{pool_size});
                for (size_t i = 0; i < n; ++i) {
                    TestPOD expected = (is_filtered[i] == 0) ? src[indexes[i]] : TestPOD{};
                    ASSERT_EQ(expected, dest[i])
                            << "Caller-informed filtered gather failed at n=" << n << " pool=" << pool_size;
                }
                for (size_t g = n; g < n + kGuard; ++g) {
                    ASSERT_EQ((TestPOD{-1, -1}), dest[g]) << "Guard corrupted at n=" << n << " g=" << g;
                }
            }
        }
    }
    ASSERT_EQ(src_copy, src);
}

TEST(SIMDGatherTest, test_filtered_int_constructible_nonzero_default_wrapper) {
    constexpr size_t kWrapperPoolSize = 1024;
    std::vector<NonZeroDefaultIntWrapper> src(kWrapperPoolSize);
    for (size_t i = 0; i < kWrapperPoolSize; ++i) {
        src[i] = NonZeroDefaultIntWrapper(static_cast<int>(i * 5 + 11));
    }

    const std::vector<size_t> row_counts = {0, 1, 7, 8, 15, 16, 511, 512, 513, 520, 4096};
    const std::vector<FilterPattern> patterns = {
            FilterPattern::AllZeros,
            FilterPattern::AllOnes,
            FilterPattern::Alternating,
            FilterPattern::ModuloMix,
    };
    const std::vector<size_t> pool_sizes = {0, 4 * 1024 * 1024, 16 * 1024 * 1024};
    const NonZeroDefaultIntWrapper kSentinel(999983);

    for (size_t n : row_counts) {
        std::mt19937 rng(67890 + n);
        std::uniform_int_distribution<uint32_t> dist(0, kWrapperPoolSize - 1);
        std::vector<uint32_t> base_indexes(n);
        for (auto& idx : base_indexes) {
            idx = dist(rng);
        }

        for (FilterPattern pattern : patterns) {
            auto is_filtered = make_filter<uint8_t>(n, pattern, 77 + n);
            for (bool poison_filtered : {false, true}) {
                std::vector<uint32_t> indexes = base_indexes;
                if (poison_filtered) {
                    for (size_t i = 0; i < n; ++i) {
                        if (is_filtered[i] != 0) {
                            indexes[i] = 0x3FFFFFFFu;
                        }
                    }
                }

                // 5-arg filtered gather
                {
                    std::vector<NonZeroDefaultIntWrapper> dest(n + kGuard, kSentinel);
                    SIMDGather::gather(dest.data(), src.data(), indexes.data(), is_filtered.data(), n);
                    for (size_t i = 0; i < n; ++i) {
                        NonZeroDefaultIntWrapper expected =
                                (is_filtered[i] == 0) ? src[indexes[i]] : NonZeroDefaultIntWrapper(0);
                        ASSERT_EQ(expected, dest[i]) << "5-arg filtered gather failed at n=" << n << " i=" << i
                                                     << " poison=" << poison_filtered;
                    }
                    for (size_t g = n; g < n + kGuard; ++g) {
                        ASSERT_EQ(kSentinel, dest[g]) << "Guard corrupted at n=" << n << " g=" << g;
                    }
                }

                // 6-arg filtered gather with SourcePoolSize
                for (size_t pool_size : pool_sizes) {
                    std::vector<NonZeroDefaultIntWrapper> dest(n + kGuard, kSentinel);
                    SIMDGather::gather(dest.data(), src.data(), indexes.data(), is_filtered.data(), n,
                                       SourcePoolSize{pool_size});
                    for (size_t i = 0; i < n; ++i) {
                        NonZeroDefaultIntWrapper expected =
                                (is_filtered[i] == 0) ? src[indexes[i]] : NonZeroDefaultIntWrapper(0);
                        ASSERT_EQ(expected, dest[i])
                                << "6-arg filtered gather failed at n=" << n << " pool_size=" << pool_size << " i=" << i
                                << " poison=" << poison_filtered;
                    }
                    for (size_t g = n; g < n + kGuard; ++g) {
                        ASSERT_EQ(kSentinel, dest[g])
                                << "Guard corrupted at n=" << n << " pool_size=" << pool_size << " g=" << g;
                    }
                }
            }
        }
    }
}

namespace {

template <typename... Args>
using gather_call_t = decltype(SIMDGather::gather(std::declval<Args>()...));

template <typename Void, typename... Args>
struct IsGatherCallable : std::false_type {};

template <typename... Args>
struct IsGatherCallable<std::void_t<gather_call_t<Args...>>, Args...> : std::true_type {};

template <typename... Args>
constexpr bool is_gather_callable_v = IsGatherCallable<void, Args...>::value;

} // namespace

TEST(SIMDGatherTest, pool_size_hint_rejects_raw_size_t) {
    // A raw (size_t, size_t) tail must never be accepted: it would silently swap the
    // legacy (buckets, num_rows) arguments into the pool-size hint overload.
    static_assert(!is_gather_callable_v<int16_t*, const int16_t*, const uint32_t*, size_t, size_t>);
    static_assert(!is_gather_callable_v<int16_t*, const int16_t*, const uint32_t*, const uint8_t*, size_t, size_t>);

    // The strongly typed hint is accepted by both hint overloads.
    static_assert(is_gather_callable_v<int16_t*, const int16_t*, const uint32_t*, size_t, SourcePoolSize>);
    static_assert(
            is_gather_callable_v<int16_t*, const int16_t*, const uint32_t*, const uint8_t*, size_t, SourcePoolSize>);
    // The legacy overload is still selected for supported 4-byte destinations.
    static_assert(is_gather_callable_v<int32_t*, const int16_t*, const uint32_t*, size_t, int>);
    static_assert(!std::is_convertible_v<size_t, SourcePoolSize>);
    static_assert(SourcePoolSize{7}.elements == 7);
}

TEST(SIMDGatherTest, prefetch_constants_pinned) {
    static_assert(SIMDGather::kPrefetchMinRows == 512);
    static_assert(SIMDGather::kPrefetchPoolBytes == 16 * 1024 * 1024);
    static_assert(SIMDGather::kPrefetchLookahead == 8);
    static_assert(SIMDGather::kPrefetchLocalitySampleRows == 128);
    static_assert(SIMDGather::kPrefetchMinUniqueSampleBits == 72);
    EXPECT_EQ(512u, SIMDGather::kPrefetchMinRows);
    EXPECT_EQ(128u, SIMDGather::kPrefetchLocalitySampleRows);
    EXPECT_EQ(72, SIMDGather::kPrefetchMinUniqueSampleBits);
}

TEST(SIMDGatherTest, test_locality_gate_low_vs_high_cardinality) {
    constexpr size_t kRows = 4096;

    // Low cardinality: both cyclic and uniform random draws from a small palette must stay on gather_buffered.
    for (size_t unique_keys : {size_t(1), size_t(8), size_t(32), size_t(64)}) {
        std::vector<uint32_t> palette(unique_keys);
        for (size_t k = 0; k < unique_keys; ++k) {
            palette[k] = static_cast<uint32_t>(k * 1024);
        }

        // Deterministic cyclic
        std::vector<uint32_t> cyclic_indexes(kRows);
        for (size_t i = 0; i < kRows; ++i) {
            cyclic_indexes[i] = palette[i % unique_keys];
        }
        EXPECT_FALSE(SIMDGather::should_prefetch_indexes<uint32_t>(cyclic_indexes.data()))
                << "cyclic unique_keys=" << unique_keys;
        EXPECT_FALSE(SIMDGather::should_prefetch_indexes<int64_t>(cyclic_indexes.data()))
                << "cyclic unique_keys=" << unique_keys;

        // Uniform random draws from palette
        std::mt19937 rng(static_cast<uint32_t>(42 + unique_keys));
        std::uniform_int_distribution<size_t> dist(0, unique_keys - 1);
        std::vector<uint32_t> random_indexes(kRows);
        for (size_t i = 0; i < kRows; ++i) {
            random_indexes[i] = palette[dist(rng)];
        }
        EXPECT_FALSE(SIMDGather::should_prefetch_indexes<uint32_t>(random_indexes.data()))
                << "random unique_keys=" << unique_keys;
        EXPECT_FALSE(SIMDGather::should_prefetch_indexes<int64_t>(random_indexes.data()))
                << "random unique_keys=" << unique_keys;
    }

    // Sequential contiguous indices (touching 8 cache lines for uint32_t and 16 for int64_t in 128 rows)
    {
        std::vector<uint32_t> seq_indexes(kRows);
        for (size_t i = 0; i < kRows; ++i) {
            seq_indexes[i] = static_cast<uint32_t>(i);
        }
        EXPECT_FALSE(SIMDGather::should_prefetch_indexes<uint32_t>(seq_indexes.data()));
        EXPECT_FALSE(SIMDGather::should_prefetch_indexes<int64_t>(seq_indexes.data()));
    }

    // High cardinality: scattered cache-line-separated keys
    for (size_t unique_keys : {size_t(4096), size_t(16 * 1024 * 1024)}) {
        std::mt19937 rng(static_cast<uint32_t>(100 + unique_keys));
        std::uniform_int_distribution<size_t> dist(0, unique_keys - 1);
        std::vector<uint32_t> high_card_indexes(kRows);
        for (size_t i = 0; i < kRows; ++i) {
            const size_t k = dist(rng);
            high_card_indexes[i] = static_cast<uint32_t>(k * 37);
        }
        EXPECT_TRUE(SIMDGather::should_prefetch_indexes<uint32_t>(high_card_indexes.data()))
                << "high_card unique_keys=" << unique_keys;
        EXPECT_TRUE(SIMDGather::should_prefetch_indexes<int64_t>(high_card_indexes.data()))
                << "high_card unique_keys=" << unique_keys;
    }

    // End-to-end gather verification for both low-cardinality (64 keys) and high-cardinality (4096 keys)
    constexpr size_t kPoolElements = 4096 * 37 + 64;
    std::vector<int32_t> src32(kPoolElements);
    std::vector<int64_t> src64(kPoolElements);
    for (size_t i = 0; i < kPoolElements; ++i) {
        src32[i] = static_cast<int32_t>(i * 7 + 3);
        src64[i] = static_cast<int64_t>(i * 13 + 5);
    }

    for (size_t unique_keys : {size_t(64), size_t(4096)}) {
        for (size_t n : {size_t(511), size_t(512), size_t(513), size_t(520), size_t(4096)}) {
            std::mt19937 rng(static_cast<uint32_t>(200 + unique_keys + n));
            std::uniform_int_distribution<size_t> dist(0, unique_keys - 1);
            std::vector<uint32_t> indexes(n);
            for (size_t i = 0; i < n; ++i) {
                indexes[i] = static_cast<uint32_t>(dist(rng) * 37);
            }

            {
                const int32_t kSentinel32 = -123456789;
                std::vector<int32_t> dest32(n + kGuard, kSentinel32);
                SIMDGather::gather(dest32.data(), src32.data(), indexes.data(), n, SourcePoolSize{16 * 1024 * 1024});
                for (size_t i = 0; i < n; ++i) {
                    ASSERT_EQ(src32[indexes[i]], dest32[i])
                            << "int32 gather failed at unique_keys=" << unique_keys << " n=" << n << " i=" << i;
                }
                for (size_t g = n; g < n + kGuard; ++g) {
                    ASSERT_EQ(kSentinel32, dest32[g])
                            << "int32 guard corrupted at unique_keys=" << unique_keys << " n=" << n << " g=" << g;
                }
            }

            {
                const int64_t kSentinel64 = -123456789012345LL;
                std::vector<int64_t> dest64(n + kGuard, kSentinel64);
                SIMDGather::gather(dest64.data(), src64.data(), indexes.data(), n, SourcePoolSize{16 * 1024 * 1024});
                for (size_t i = 0; i < n; ++i) {
                    ASSERT_EQ(src64[indexes[i]], dest64[i])
                            << "int64 gather failed at unique_keys=" << unique_keys << " n=" << n << " i=" << i;
                }
                for (size_t g = n; g < n + kGuard; ++g) {
                    ASSERT_EQ(kSentinel64, dest64[g])
                            << "int64 guard corrupted at unique_keys=" << unique_keys << " n=" << n << " g=" << g;
                }
            }
        }
    }
}

// Row counts around the prefetch row gate and the unroll/lookahead tails, pool hints one below,
// at and one above the byte threshold.
template <typename T>
void check_prefetch_gate_boundaries() {
    const std::vector<size_t> row_counts = {0,   1,   7,   8,   15,  16,   17,   511, 512,
                                            513, 519, 520, 527, 528, 1000, 4095, 4096};
    const size_t threshold = SIMDGather::kPrefetchPoolBytes / sizeof(T);
    for (size_t pool : {threshold - 1, threshold, threshold + 1}) {
        for (size_t n : row_counts) {
            check_caller_informed_gather<T, uint32_t>(n, pool);
            check_caller_informed_gather<T, int32_t>(n, pool);
            for (FilterPattern pattern : {FilterPattern::Alternating, FilterPattern::ModuloMix, FilterPattern::AllOnes,
                                          FilterPattern::AllZeros}) {
                check_caller_informed_filtered<T, uint32_t, uint8_t>(n, pool, pattern, false);
                // Filtered rows carry out-of-range indexes: they must be neither dereferenced nor prefetched.
                check_caller_informed_filtered<T, int32_t, uint8_t>(n, pool, pattern, true);
            }
        }
    }
}

TEST(SIMDGatherTest, prefetch_gate_boundaries) {
    check_prefetch_gate_boundaries<int32_t>();
    check_prefetch_gate_boundaries<int64_t>();
    // Narrow types: the prefetch loop advances in steps of 8, so the buffered default loop then
    // starts at an index that is not a multiple of NUM_BATCH_VALUES (16 for int16, 32 for int8 under AVX2).
    check_prefetch_gate_boundaries<int16_t>();
    check_prefetch_gate_boundaries<int8_t>();
}

TEST(SIMDGatherTest, test_locality_gate_cheap_rejection_stages) {
    constexpr size_t kRows = SIMDGather::kPrefetchLocalitySampleRows;

    // Stage 0: 1-key (all identical indices) must reject immediately
    for (uint32_t val : {0u, 1u, 1024u, 4194304u}) {
        std::vector<uint32_t> uniform_indexes(kRows, val);
        EXPECT_FALSE(SIMDGather::should_prefetch_indexes<uint32_t>(uniform_indexes.data()));
        EXPECT_FALSE(SIMDGather::should_prefetch_indexes<int64_t>(uniform_indexes.data()));
    }

    // Stage 1: Low cardinality (2, 4, 8, 16 keys) must reject early in micro-sketch
    for (size_t unique_keys : {2, 4, 8, 16}) {
        std::vector<uint32_t> palette(unique_keys);
        for (size_t k = 0; k < unique_keys; ++k) {
            palette[k] = static_cast<uint32_t>(k * 2048);
        }
        std::mt19937 rng(static_cast<uint32_t>(54321 + unique_keys));
        std::uniform_int_distribution<size_t> dist(0, unique_keys - 1);
        std::vector<uint32_t> indexes(kRows);
        for (size_t i = 0; i < kRows; ++i) {
            indexes[i] = palette[dist(rng)];
        }
        EXPECT_FALSE(SIMDGather::should_prefetch_indexes<uint32_t>(indexes.data()));
        EXPECT_FALSE(SIMDGather::should_prefetch_indexes<int64_t>(indexes.data()));
    }

    // Stage 2: Medium cardinality (32, 64 keys) must reject at the 64-row checkpoint
    for (size_t unique_keys : {32, 64}) {
        std::vector<uint32_t> palette(unique_keys);
        for (size_t k = 0; k < unique_keys; ++k) {
            palette[k] = static_cast<uint32_t>(k * 4096);
        }
        std::mt19937 rng(static_cast<uint32_t>(98765 + unique_keys));
        std::uniform_int_distribution<size_t> dist(0, unique_keys - 1);
        std::vector<uint32_t> indexes(kRows);
        for (size_t i = 0; i < kRows; ++i) {
            indexes[i] = palette[dist(rng)];
        }
        EXPECT_FALSE(SIMDGather::should_prefetch_indexes<uint32_t>(indexes.data()));
        EXPECT_FALSE(SIMDGather::should_prefetch_indexes<int64_t>(indexes.data()));
    }

    // Stage 3: High cardinality (> 1024 unique lines) must pass to prefetch
    {
        std::mt19937 rng(13579);
        std::uniform_int_distribution<uint32_t> dist(0, 10000000u);
        std::vector<uint32_t> indexes(kRows);
        for (size_t i = 0; i < kRows; ++i) {
            indexes[i] = dist(rng) * 16;
        }
        EXPECT_TRUE(SIMDGather::should_prefetch_indexes<uint32_t>(indexes.data()));
        EXPECT_TRUE(SIMDGather::should_prefetch_indexes<int64_t>(indexes.data()));
    }
}

TEST(SIMDGatherTest, test_gather_helpers_direct_coverage) {
    // Direct coverage for gather_8_unrolled, gather_scalar, gather_filtered_scalar
    std::vector<int32_t> src(16);
    for (size_t i = 0; i < 16; ++i) {
        src[i] = static_cast<int32_t>(i * 10 + 1);
    }
    std::vector<uint32_t> indexes = {15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
    std::vector<int32_t> dest(16, 0);

    // Test gather_8_unrolled for trivially copyable type
    SIMDGather::gather_8_unrolled(dest.data(), src.data(), indexes.data(), 0);
    for (size_t i = 0; i < 8; ++i) {
        EXPECT_EQ(dest[i], src[indexes[i]]);
    }

    // Test gather_8_unrolled for non-trivially copyable type (std::string)
    std::vector<std::string> str_src(16);
    for (size_t i = 0; i < 16; ++i) {
        str_src[i] = "item_" + std::to_string(i);
    }
    std::vector<std::string> str_dest(16);
    SIMDGather::gather_8_unrolled(str_dest.data(), str_src.data(), indexes.data(), 0);
    for (size_t i = 0; i < 8; ++i) {
        EXPECT_EQ(str_dest[i], str_src[indexes[i]]);
    }

    // Test gather_scalar
    size_t offset = 8;
    SIMDGather::gather_scalar(dest.data(), src.data(), indexes.data(), 16, offset);
    EXPECT_EQ(offset, 16);
    for (size_t i = 8; i < 16; ++i) {
        EXPECT_EQ(dest[i], src[indexes[i]]);
    }

    // Test gather_filtered_scalar
    std::vector<uint8_t> is_filtered = {0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1};
    std::vector<int32_t> filtered_dest(16, 0);
    size_t filtered_offset = 0;
    SIMDGather::gather_filtered_scalar(filtered_dest.data(), src.data(), indexes.data(), is_filtered.data(), 16,
                                       filtered_offset);
    EXPECT_EQ(filtered_offset, 16);
    for (size_t i = 0; i < 16; ++i) {
        if (is_filtered[i] != 0) {
            EXPECT_EQ(filtered_dest[i], 0);
        } else {
            EXPECT_EQ(filtered_dest[i], src[indexes[i]]);
        }
    }
}

} // namespace starrocks
