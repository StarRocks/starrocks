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

#pragma once

#include <cstddef>
#include <cstdint>
#include <type_traits>

#ifdef __AVX2__
#include <emmintrin.h>
#include <immintrin.h>
#endif

#if defined(__x86_64__) || defined(_M_X64)
#include <xmmintrin.h>
#endif

// Read prefetch into all cache levels. Never faults, even for invalid addresses.
#ifndef STARROCKS_PREFETCH
#if defined(__x86_64__) || defined(_M_X64)
#define STARROCKS_PREFETCH(addr) _mm_prefetch(reinterpret_cast<const char*>(addr), _MM_HINT_T0)
#elif defined(__aarch64__) || defined(__arm__)
#define STARROCKS_PREFETCH(addr) __builtin_prefetch(static_cast<const void*>(addr), 0, 3)
#elif defined(__GNUC__) || defined(__clang__)
#define STARROCKS_PREFETCH(addr) __builtin_prefetch(static_cast<const void*>(addr), 0, 3)
#else
#define STARROCKS_PREFETCH(addr) ((void)0)
#endif
#endif

namespace starrocks {

struct SIMDGather {
    // https://johnysswlab.com/when-vectorization-hits-the-memory-wall-investigating-the-avx2-memory-gather-instruction
    // 512K
    static constexpr const int max_process_size = 512 * 1024;
    // b[i] = a[c[i]];
    // T was int32_t or uint32_t
    template <class TB, class TC>
    static void gather(TB* b, const int16_t* a, const TC* c, size_t buckets, int num_rows) {
        static_assert(sizeof(TB) == 4);
        static_assert(std::is_integral_v<TB>);
        static_assert(sizeof(TC) == 4);
        static_assert(std::is_integral_v<TC>);
        int i = 0;
#ifdef __AVX2__
        if (buckets < max_process_size) {
            // gather will collect data of size sizeof(int32)
            // we only need the lower 16 bits
            // eg:
            // a = [0x12 0x34 0x56 0x78 0x9a 0x...]
            // gather (a, [0,1,2,3], 2) will be:
            // [0x12 0x32 0x56 0x78] [0x56 0x78 0x9a..0.] [....]
            // use will use mask to get lower 16 bits
            // [0x12 0x32 0x00 0x00] [0x56 0x78 0x00 0x00] [....]
            __m256i mask = _mm256_set1_epi32(0xFFFF);
            for (; i + 8 <= num_rows; i += 8) {
                __m256i loaded = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(c));
                __m256i gathered = _mm256_i32gather_epi32((int32_t*)a, loaded, 2);
                gathered = _mm256_and_si256(gathered, mask);
                _mm256_storeu_si256(reinterpret_cast<__m256i*>(b), gathered);
                c += 8;
                b += 8;
            }
            _mm256_zeroupper();
        }
#endif
        for (; i < num_rows; i++) {
            *b = a[*c];
            b++;
            c++;
        }
    }

    static constexpr uint32_t simd_register_bitwidth() {
#ifdef __AVX2__
        return 256;
#elif defined(__ARM_NEON) && defined(__aarch64__)
        return 128;
#else
        return 128;
#endif
    }

    /// Helper: 8-element unrolled gather step with independent loads and stores
    template <typename DataType, typename IndexType>
    static inline void gather_8_unrolled(DataType* __restrict__ dest, const DataType* __restrict__ src,
                                         const IndexType* __restrict__ indexes, size_t i) {
        if constexpr (std::is_trivially_copyable_v<DataType>) {
            DataType v0 = src[indexes[i + 0]];
            DataType v1 = src[indexes[i + 1]];
            DataType v2 = src[indexes[i + 2]];
            DataType v3 = src[indexes[i + 3]];
            DataType v4 = src[indexes[i + 4]];
            DataType v5 = src[indexes[i + 5]];
            DataType v6 = src[indexes[i + 6]];
            DataType v7 = src[indexes[i + 7]];

            dest[i + 0] = v0;
            dest[i + 1] = v1;
            dest[i + 2] = v2;
            dest[i + 3] = v3;
            dest[i + 4] = v4;
            dest[i + 5] = v5;
            dest[i + 6] = v6;
            dest[i + 7] = v7;
        } else {
            dest[i + 0] = src[indexes[i + 0]];
            dest[i + 1] = src[indexes[i + 1]];
            dest[i + 2] = src[indexes[i + 2]];
            dest[i + 3] = src[indexes[i + 3]];
            dest[i + 4] = src[indexes[i + 4]];
            dest[i + 5] = src[indexes[i + 5]];
            dest[i + 6] = src[indexes[i + 6]];
            dest[i + 7] = src[indexes[i + 7]];
        }
    }

    /// Helper: 8-way unrolled loop for high-throughput default gather
    template <typename DataType, typename IndexType>
    static inline void gather_unrolled(DataType* __restrict__ dest, const DataType* __restrict__ src,
                                       const IndexType* __restrict__ indexes, size_t num_rows, size_t& i) {
        for (; i + 8 <= num_rows; i += 8) {
            gather_8_unrolled(dest, src, indexes, i);
        }
    }

    /// Helper: Scalar tail gather
    template <typename DataType, typename IndexType>
    static inline void gather_scalar(DataType* dest, const DataType* src, const IndexType* indexes, size_t num_rows,
                                     size_t& i) {
        for (; i < num_rows; ++i) {
            dest[i] = src[indexes[i]];
        }
    }

    /// Helper: 8-element filtered gather step with condition checks before dereferencing
    template <typename DataType, typename IndexType, typename CondType>
    static inline void gather_filtered_8_unrolled(DataType* __restrict__ dest, const DataType* __restrict__ src,
                                                  const IndexType* __restrict__ indexes,
                                                  const CondType* __restrict__ is_filtered, size_t i) {
        if constexpr (std::is_trivially_copyable_v<DataType>) {
            DataType v0 = (is_filtered[i + 0] == 0) ? src[indexes[i + 0]] : DataType{};
            DataType v1 = (is_filtered[i + 1] == 0) ? src[indexes[i + 1]] : DataType{};
            DataType v2 = (is_filtered[i + 2] == 0) ? src[indexes[i + 2]] : DataType{};
            DataType v3 = (is_filtered[i + 3] == 0) ? src[indexes[i + 3]] : DataType{};
            DataType v4 = (is_filtered[i + 4] == 0) ? src[indexes[i + 4]] : DataType{};
            DataType v5 = (is_filtered[i + 5] == 0) ? src[indexes[i + 5]] : DataType{};
            DataType v6 = (is_filtered[i + 6] == 0) ? src[indexes[i + 6]] : DataType{};
            DataType v7 = (is_filtered[i + 7] == 0) ? src[indexes[i + 7]] : DataType{};

            dest[i + 0] = v0;
            dest[i + 1] = v1;
            dest[i + 2] = v2;
            dest[i + 3] = v3;
            dest[i + 4] = v4;
            dest[i + 5] = v5;
            dest[i + 6] = v6;
            dest[i + 7] = v7;
        } else {
            if constexpr (std::is_constructible_v<DataType, int>) {
                dest[i + 0] = (is_filtered[i + 0] == 0) ? src[indexes[i + 0]] : DataType(0);
                dest[i + 1] = (is_filtered[i + 1] == 0) ? src[indexes[i + 1]] : DataType(0);
                dest[i + 2] = (is_filtered[i + 2] == 0) ? src[indexes[i + 2]] : DataType(0);
                dest[i + 3] = (is_filtered[i + 3] == 0) ? src[indexes[i + 3]] : DataType(0);
                dest[i + 4] = (is_filtered[i + 4] == 0) ? src[indexes[i + 4]] : DataType(0);
                dest[i + 5] = (is_filtered[i + 5] == 0) ? src[indexes[i + 5]] : DataType(0);
                dest[i + 6] = (is_filtered[i + 6] == 0) ? src[indexes[i + 6]] : DataType(0);
                dest[i + 7] = (is_filtered[i + 7] == 0) ? src[indexes[i + 7]] : DataType(0);
            } else {
                dest[i + 0] = (is_filtered[i + 0] == 0) ? src[indexes[i + 0]] : DataType{};
                dest[i + 1] = (is_filtered[i + 1] == 0) ? src[indexes[i + 1]] : DataType{};
                dest[i + 2] = (is_filtered[i + 2] == 0) ? src[indexes[i + 2]] : DataType{};
                dest[i + 3] = (is_filtered[i + 3] == 0) ? src[indexes[i + 3]] : DataType{};
                dest[i + 4] = (is_filtered[i + 4] == 0) ? src[indexes[i + 4]] : DataType{};
                dest[i + 5] = (is_filtered[i + 5] == 0) ? src[indexes[i + 5]] : DataType{};
                dest[i + 6] = (is_filtered[i + 6] == 0) ? src[indexes[i + 6]] : DataType{};
                dest[i + 7] = (is_filtered[i + 7] == 0) ? src[indexes[i + 7]] : DataType{};
            }
        }
    }

    /// Helper: 8-way unrolled loop for filtered gather
    template <typename DataType, typename IndexType, typename CondType>
    static inline void gather_filtered_unrolled(DataType* __restrict__ dest, const DataType* __restrict__ src,
                                                const IndexType* __restrict__ indexes,
                                                const CondType* __restrict__ is_filtered, size_t num_rows, size_t& i) {
        for (; i + 8 <= num_rows; i += 8) {
            gather_filtered_8_unrolled(dest, src, indexes, is_filtered, i);
        }
    }

    /// Helper: Scalar tail filtered gather
    template <typename DataType, typename IndexType, typename CondType>
    static inline void gather_filtered_scalar(DataType* dest, const DataType* src, const IndexType* indexes,
                                              const CondType* is_filtered, size_t num_rows, size_t& i) {
        for (; i < num_rows; ++i) {
            if (is_filtered[i] == 0) {
                dest[i] = src[indexes[i]];
            } else {
                if constexpr (std::is_constructible_v<DataType, int>) {
                    dest[i] = DataType(0);
                } else {
                    dest[i] = DataType{};
                }
            }
        }
    }

    /// dest[i] = src[indexes[i]]
    template <typename DataType, typename IndexType>
    static inline void gather(DataType* dest, const DataType* src, const IndexType* indexes, size_t num_rows) {
        static_assert(std::is_integral_v<IndexType>);
        size_t i = 0;
        gather_unrolled(dest, src, indexes, num_rows, i);
        gather_scalar(dest, src, indexes, num_rows, i);
    }

    /// dest[i] = is_filtered[i] == 0 ? src[indexes[i]] : 0
    template <typename DataType, typename IndexType, typename CondType>
    static inline void gather(DataType* dest, const DataType* src, const IndexType* indexes,
                              const CondType* is_filtered, size_t num_rows) {
        static_assert(std::is_integral_v<IndexType>);
        size_t i = 0;
        gather_filtered_unrolled(dest, src, indexes, is_filtered, num_rows, i);
        gather_filtered_scalar(dest, src, indexes, is_filtered, num_rows, i);
    }

    /// Caller-informed gather with DRAM prefetch heuristic.
    /// @param pool_size Element count of the source array (`src`).
    template <typename DataType, typename IndexType>
    static inline void gather(DataType* dest, const DataType* src, const IndexType* indexes, size_t num_rows,
                              size_t pool_size) {
        static_assert(std::is_integral_v<IndexType>);
        size_t i = 0;

#if defined(__x86_64__) || defined(_M_X64)
        constexpr size_t kDRAMByteThreshold = 16 * 1024 * 1024;
        constexpr size_t kBatchThreshold = 4096;
        constexpr size_t kPrefetchLookahead = 8;
        const bool is_large_pool = (pool_size >= kDRAMByteThreshold / sizeof(DataType));

        if (num_rows >= kBatchThreshold && is_large_pool) {
            for (size_t p = 0; p < kPrefetchLookahead; ++p) {
                STARROCKS_PREFETCH(src + indexes[p]);
            }
            for (; i + kPrefetchLookahead + 8 <= num_rows; i += 8) {
                STARROCKS_PREFETCH(src + indexes[i + kPrefetchLookahead + 0]);
                STARROCKS_PREFETCH(src + indexes[i + kPrefetchLookahead + 1]);
                STARROCKS_PREFETCH(src + indexes[i + kPrefetchLookahead + 2]);
                STARROCKS_PREFETCH(src + indexes[i + kPrefetchLookahead + 3]);
                STARROCKS_PREFETCH(src + indexes[i + kPrefetchLookahead + 4]);
                STARROCKS_PREFETCH(src + indexes[i + kPrefetchLookahead + 5]);
                STARROCKS_PREFETCH(src + indexes[i + kPrefetchLookahead + 6]);
                STARROCKS_PREFETCH(src + indexes[i + kPrefetchLookahead + 7]);

                gather_8_unrolled(dest, src, indexes, i);
            }
        }
#else
        (void)pool_size;
#endif

        gather_unrolled(dest, src, indexes, num_rows, i);
        gather_scalar(dest, src, indexes, num_rows, i);
    }

    /// Caller-informed filtered gather with DRAM prefetch heuristic.
    /// @param pool_size Element count of the source array (`src`).
    template <typename DataType, typename IndexType, typename CondType>
    static inline void gather(DataType* dest, const DataType* src, const IndexType* indexes,
                              const CondType* is_filtered, size_t num_rows, size_t pool_size) {
        static_assert(std::is_integral_v<IndexType>);
        size_t i = 0;

#if defined(__x86_64__) || defined(_M_X64)
        constexpr size_t kDRAMByteThreshold = 16 * 1024 * 1024;
        constexpr size_t kBatchThreshold = 4096;
        constexpr size_t kPrefetchLookahead = 8;
        const bool is_large_pool = (pool_size >= kDRAMByteThreshold / sizeof(DataType));

        if (num_rows >= kBatchThreshold && is_large_pool) {
            for (size_t p = 0; p < kPrefetchLookahead; ++p) {
                if (is_filtered[p] == 0) {
                    STARROCKS_PREFETCH(src + indexes[p]);
                }
            }
            for (; i + kPrefetchLookahead + 8 <= num_rows; i += 8) {
                if (is_filtered[i + kPrefetchLookahead + 0] == 0) {
                    STARROCKS_PREFETCH(src + indexes[i + kPrefetchLookahead + 0]);
                }
                if (is_filtered[i + kPrefetchLookahead + 1] == 0) {
                    STARROCKS_PREFETCH(src + indexes[i + kPrefetchLookahead + 1]);
                }
                if (is_filtered[i + kPrefetchLookahead + 2] == 0) {
                    STARROCKS_PREFETCH(src + indexes[i + kPrefetchLookahead + 2]);
                }
                if (is_filtered[i + kPrefetchLookahead + 3] == 0) {
                    STARROCKS_PREFETCH(src + indexes[i + kPrefetchLookahead + 3]);
                }
                if (is_filtered[i + kPrefetchLookahead + 4] == 0) {
                    STARROCKS_PREFETCH(src + indexes[i + kPrefetchLookahead + 4]);
                }
                if (is_filtered[i + kPrefetchLookahead + 5] == 0) {
                    STARROCKS_PREFETCH(src + indexes[i + kPrefetchLookahead + 5]);
                }
                if (is_filtered[i + kPrefetchLookahead + 6] == 0) {
                    STARROCKS_PREFETCH(src + indexes[i + kPrefetchLookahead + 6]);
                }
                if (is_filtered[i + kPrefetchLookahead + 7] == 0) {
                    STARROCKS_PREFETCH(src + indexes[i + kPrefetchLookahead + 7]);
                }

                gather_filtered_8_unrolled(dest, src, indexes, is_filtered, i);
            }
        }
#else
        (void)pool_size;
#endif

        gather_filtered_unrolled(dest, src, indexes, is_filtered, num_rows, i);
        gather_filtered_scalar(dest, src, indexes, is_filtered, num_rows, i);
    }
};

} // namespace starrocks
