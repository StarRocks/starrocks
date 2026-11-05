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

/// Strongly typed pool-size hint for the caller-informed SIMDGather overloads.
/// `elements` is the ELEMENT count of the source array (not bytes). Wrapping it in a
/// type prevents a raw `(size_t, size_t)` tail from silently binding to the hint
/// overloads when the legacy `(buckets, num_rows)` overload was intended.
struct SourcePoolSize {
    explicit constexpr SourcePoolSize(size_t n) : elements(n) {}
    size_t elements;
};

struct SIMDGather {
    // https://johnysswlab.com/when-vectorization-hits-the-memory-wall-investigating-the-avx2-memory-gather-instruction
    // 512K
    static constexpr const int max_process_size = 512 * 1024;
    // b[i] = a[c[i]];
    // T was int32_t or uint32_t
    // Constrained via SFINAE so unsupported types are rejected at overload resolution instead of
    // being accepted by a decltype-only check.
    // This is SFINAE rather than an in-body static_assert on purpose: a violating call (e.g. an
    // int16_t dest with raw size_t trailing arguments) must be rejected at overload resolution
    // instead of silently binding to a different overload.
    // Contract: b, a and c must not overlap (see the other overloads).
    template <class TB, class TC,
              std::enable_if_t<sizeof(TB) == 4 && std::is_integral_v<TB> && sizeof(TC) == 4 && std::is_integral_v<TC>,
                               int> = 0>
    static void gather(TB* b, const int16_t* a, const TC* c, size_t buckets, int num_rows) {
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

    /// Helper: 8-element unrolled gather step with independent loads and stores. Used only by the
    /// x86-64 prefetch loop bodies; the default path is the buffered loop.
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

    /// Prefetch tuning for the SourcePoolSize overloads (x86-64 only; ARM never prefetches).
    /// The 8-way unroll exists only in the x86-64 prefetch body. An AArch64 int64 unrolled default was
    /// NOT adopted: wins at small pools (e.g. about -10% at 256K-element pools) could not be confirmed on
    /// the available noisy ARM VM, and the 1M-element pool leaned toward a regression (+4.6..+11.5%).
    /// Clean-harness data (null control -2.5..+1.5%, 10 reps, rows 512..8192) on x86 Xeon Platinum 8581C
    /// (Emerald Rapids; VM reports 2 MiB L2 per core and shared 260 MiB L3; g++ 13.3, -O3 -mavx2) for
    /// CallerInformed vs default loop: pools below 16 MiB are gated off and equal default (measured -1.3..+0.4%);
    /// int32 wins -10.5..-11.4% at 16 MiB and -5.1..-9.6% at 256 MiB; int64 wins -9.7..-10.5% at 16 MiB,
    /// -4.9..-5.4% at 32 MiB, and no win at 512 MiB (-0.6..+0.1%). Gain is flat across 512..8192 rows.
    /// The <= 8 MiB loss figure (+4..+13%) is from an earlier layout-biased harness and was not re-measured.
    /// The 16 MiB gate is the conservative edge of the bracket (loss at 8 MiB, win at 16 MiB), so crossover is
    /// CPU/LLC-dependent on this hardware. A join stage microbenchmark (hash + gather, not a join) showed 5-10%
    /// faster lookup_init stage for int32 first arrays of 16-128 MiB (no change below 16 MiB); no end-to-end
    /// join speedup was measured.
    /// Minimum row count to start prefetching.
    static constexpr size_t kPrefetchMinRows = 512;
    /// Source-pool size threshold in BYTES (compared against `pool_size.elements * sizeof(DataType)`).
    static constexpr size_t kPrefetchPoolBytes = 16 * 1024 * 1024;
    /// How many rows ahead of the loads the prefetches are issued.
    static constexpr size_t kPrefetchLookahead = 8;

    /// Default gather loop on every target and element size: SIMD-width batches staged through a
    /// local buffer, continuing from index `i` (where the prefetch loop stopped, if it ran). Skips batching when NUM_BATCH_VALUES == 0 (e.g. int256_t on
    /// 128-bit NEON) to avoid an infinite loop; the scalar tail then handles every row.
    template <typename DataType, typename IndexType>
    static inline void gather_buffered(DataType* dest, const DataType* src, const IndexType* indexes, size_t num_rows,
                                       size_t& i) {
        static constexpr uint32_t SIMD_WIDTH = simd_register_bitwidth();
        static constexpr uint32_t NUM_BATCH_VALUES = SIMD_WIDTH / (8 * sizeof(DataType));

        // Skip batch processing when NUM_BATCH_VALUES = 0 to avoid infinite loop
        // This happens for large data types (e.g., int256_t) on small SIMD registers (e.g., ARM NEON 128-bit)
        if constexpr (NUM_BATCH_VALUES > 0) {
            DataType buffer[NUM_BATCH_VALUES];

            for (; i + NUM_BATCH_VALUES <= num_rows; i += NUM_BATCH_VALUES) {
                for (uint32_t j = 0; j < NUM_BATCH_VALUES; j++) {
                    buffer[j] = src[indexes[i + j]];
                }

                for (uint32_t j = 0; j < NUM_BATCH_VALUES; j++) {
                    dest[i + j] = buffer[j];
                }
            }
        }

        gather_scalar(dest, src, indexes, num_rows, i);
    }

    /// Default filtered gather loop on every target and element size, continuing from index `i`.
    /// Filtered rows write 0 and never dereference `src[indexes[i]]`.
    template <typename DataType, typename IndexType, typename CondType>
    static inline void gather_filtered_buffered(DataType* dest, const DataType* src, const IndexType* indexes,
                                                const CondType* is_filtered, size_t num_rows, size_t& i) {
        static constexpr uint32_t SIMD_WIDTH = simd_register_bitwidth();
        static constexpr uint32_t NUM_BATCH_VALUES = SIMD_WIDTH / (8 * sizeof(DataType));

        // Skip batch processing when NUM_BATCH_VALUES = 0 to avoid infinite loop
        // This happens for large data types (e.g., int256_t) on small SIMD registers (e.g., ARM NEON 128-bit)
        if constexpr (NUM_BATCH_VALUES > 0) {
            DataType buffer[NUM_BATCH_VALUES];

            for (; i + NUM_BATCH_VALUES <= num_rows; i += NUM_BATCH_VALUES) {
                for (uint32_t j = 0; j < NUM_BATCH_VALUES; j++) {
                    if (is_filtered[i + j] == 0) {
                        buffer[j] = src[indexes[i + j]];
                    } else if constexpr (std::is_constructible_v<DataType, int>) {
                        buffer[j] = DataType(0);
                    } else {
                        buffer[j] = DataType{};
                    }
                }

                for (uint32_t j = 0; j < NUM_BATCH_VALUES; j++) {
                    dest[i + j] = buffer[j];
                }
            }
        }

        gather_filtered_scalar(dest, src, indexes, is_filtered, num_rows, i);
    }

    /// dest[i] = src[indexes[i]]
    /// Contract: `dest`, `src` and `indexes` must not overlap: the helpers take `__restrict__`
    /// pointers, so overlapping ranges are undefined behaviour.
    template <typename DataType, typename IndexType>
    static inline void gather(DataType* dest, const DataType* src, const IndexType* indexes, size_t num_rows) {
        static_assert(std::is_integral_v<IndexType>);
        size_t i = 0;
        gather_buffered(dest, src, indexes, num_rows, i);
    }

    /// dest[i] = is_filtered[i] == 0 ? src[indexes[i]] : 0
    /// Contract: `dest`, `src` and `indexes` must not overlap: the helpers take `__restrict__`
    /// pointers, so overlapping ranges are undefined behaviour.
    template <typename DataType, typename IndexType, typename CondType>
    static inline void gather(DataType* dest, const DataType* src, const IndexType* indexes,
                              const CondType* is_filtered, size_t num_rows) {
        static_assert(std::is_integral_v<IndexType>);
        size_t i = 0;
        gather_filtered_buffered(dest, src, indexes, is_filtered, num_rows, i);
    }

    /// Caller-informed gather with DRAM prefetch heuristic.
    /// Contract: `dest`, `src` and `indexes` must not overlap: the helpers take `__restrict__`
    /// pointers, so overlapping ranges are undefined behaviour.
    /// `pool_size` is a PERFORMANCE hint only and never affects correctness. Prefetch is issued only
    /// for rows whose indexes the subsequent loads will read (filtered rows are skipped). Prefetch is
    /// x86-64 only (measured regression on AArch64); elsewhere this overload degenerates to the plain
    /// buffered default loop.
    /// @param pool_size Element count of the source array (`src`), wrapped in SourcePoolSize.
    template <typename DataType, typename IndexType>
    static inline void gather(DataType* dest, const DataType* src, const IndexType* indexes, size_t num_rows,
                              SourcePoolSize pool_size) {
        static_assert(std::is_integral_v<IndexType>);
        size_t i = 0;

#if defined(__x86_64__) || defined(_M_X64)
        const bool is_large_pool = (pool_size.elements >= kPrefetchPoolBytes / sizeof(DataType));

        if (num_rows >= kPrefetchMinRows && is_large_pool) {
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

        gather_buffered(dest, src, indexes, num_rows, i);
    }

    /// Caller-informed filtered gather with DRAM prefetch heuristic.
    /// Contract: `dest`, `src` and `indexes` must not overlap: the helpers take `__restrict__`
    /// pointers, so overlapping ranges are undefined behaviour.
    /// `pool_size` is a PERFORMANCE hint only and never affects correctness. Prefetch is issued only
    /// for rows whose indexes the subsequent loads will read (filtered rows are skipped). Prefetch is
    /// x86-64 only (measured regression on AArch64); elsewhere this overload degenerates to the plain
    /// buffered default loop.
    /// @param pool_size Element count of the source array (`src`), wrapped in SourcePoolSize.
    template <typename DataType, typename IndexType, typename CondType>
    static inline void gather(DataType* dest, const DataType* src, const IndexType* indexes,
                              const CondType* is_filtered, size_t num_rows, SourcePoolSize pool_size) {
        static_assert(std::is_integral_v<IndexType>);
        size_t i = 0;

#if defined(__x86_64__) || defined(_M_X64)
        const bool is_large_pool = (pool_size.elements >= kPrefetchPoolBytes / sizeof(DataType));

        if (num_rows >= kPrefetchMinRows && is_large_pool) {
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

        gather_filtered_buffered(dest, src, indexes, is_filtered, num_rows, i);
    }
};

} // namespace starrocks
