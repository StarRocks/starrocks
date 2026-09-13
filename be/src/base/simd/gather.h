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
#include <cstring>
#include <type_traits>

#ifdef __AVX2__
#include <emmintrin.h>
#include <immintrin.h>
#endif
#if defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_neon.h>
#endif

#if defined(__GNUC__) || defined(__clang__)
#define STARROCKS_PREFETCH(addr) __builtin_prefetch(static_cast<const void*>(addr), 0, 3)
#elif defined(__x86_64__) || defined(_M_X64)
#define STARROCKS_PREFETCH(addr) _mm_prefetch(reinterpret_cast<const char*>(addr), _MM_HINT_T0)
#else
#define STARROCKS_PREFETCH(addr) ((void)0)
#endif

namespace starrocks {

struct SIMDGather {
    static constexpr const int max_process_size = 512 * 1024;

    template <class TB, class TC>
    static void gather(TB* b, const int16_t* a, const TC* c, size_t buckets, int num_rows) {
        static_assert(sizeof(TB) == 4);
        static_assert(std::is_integral_v<TB>);
        static_assert(sizeof(TC) == 4);
        static_assert(std::is_integral_v<TC>);
        int i = 0;
#ifdef __AVX2__
        if (buckets < max_process_size) {
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
        constexpr int kPrefetchDist = 16;
        if (num_rows >= 8192) {
            for (; i + kPrefetchDist + 8 <= num_rows; i += 8) {
                STARROCKS_PREFETCH(&a[c[kPrefetchDist + 0]]);
                STARROCKS_PREFETCH(&a[c[kPrefetchDist + 2]]);
                STARROCKS_PREFETCH(&a[c[kPrefetchDist + 4]]);
                STARROCKS_PREFETCH(&a[c[kPrefetchDist + 6]]);

                b[0] = static_cast<TB>(a[c[0]]);
                b[1] = static_cast<TB>(a[c[1]]);
                b[2] = static_cast<TB>(a[c[2]]);
                b[3] = static_cast<TB>(a[c[3]]);
                b[4] = static_cast<TB>(a[c[4]]);
                b[5] = static_cast<TB>(a[c[5]]);
                b[6] = static_cast<TB>(a[c[6]]);
                b[7] = static_cast<TB>(a[c[7]]);
                b += 8;
                c += 8;
            }
        }
        for (; i + 8 <= num_rows; i += 8) {
            b[0] = static_cast<TB>(a[c[0]]);
            b[1] = static_cast<TB>(a[c[1]]);
            b[2] = static_cast<TB>(a[c[2]]);
            b[3] = static_cast<TB>(a[c[3]]);
            b[4] = static_cast<TB>(a[c[4]]);
            b[5] = static_cast<TB>(a[c[5]]);
            b[6] = static_cast<TB>(a[c[6]]);
            b[7] = static_cast<TB>(a[c[7]]);
            b += 8;
            c += 8;
        }
        for (; i < num_rows; i++) {
            *b = static_cast<TB>(a[*c]);
            b++;
            c++;
        }
    }

    /// dest[i] = src[indexes[i]]
    template <typename DataType, typename IndexType>
    static void gather(DataType* dest, const DataType* src, const IndexType* indexes, size_t num_rows) {
        static_assert(std::is_integral_v<IndexType>);

        size_t i = 0;
        constexpr size_t kUnroll = 8;
        constexpr size_t kPrefetchDistance = 16;

        // Large array path: prefetch ahead to mitigate DRAM/L3 latency
        if (num_rows >= 8192) {
            for (; i + kPrefetchDistance + kUnroll <= num_rows; i += kUnroll) {
                STARROCKS_PREFETCH(&src[indexes[i + kPrefetchDistance + 0]]);
                STARROCKS_PREFETCH(&src[indexes[i + kPrefetchDistance + 2]]);
                STARROCKS_PREFETCH(&src[indexes[i + kPrefetchDistance + 4]]);
                STARROCKS_PREFETCH(&src[indexes[i + kPrefetchDistance + 6]]);

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

        // L1/L2 fast path: unrolled vector copy without prefetch overhead
        for (; i + kUnroll <= num_rows; i += kUnroll) {
            dest[i + 0] = src[indexes[i + 0]];
            dest[i + 1] = src[indexes[i + 1]];
            dest[i + 2] = src[indexes[i + 2]];
            dest[i + 3] = src[indexes[i + 3]];
            dest[i + 4] = src[indexes[i + 4]];
            dest[i + 5] = src[indexes[i + 5]];
            dest[i + 6] = src[indexes[i + 6]];
            dest[i + 7] = src[indexes[i + 7]];
        }

        // Remainder loop
        for (; i < num_rows; ++i) {
            dest[i] = src[indexes[i]];
        }
    }

    /// dest[i] = is_filtered[i] == 0 ? src[indexes[i]] : 0
    template <typename DataType, typename IndexType, typename CondType>
    static void gather(DataType* dest, const DataType* src, const IndexType* indexes, const CondType* is_filtered,
                       size_t num_rows) {
        static_assert(std::is_integral_v<IndexType>);

        size_t i = 0;
        constexpr size_t kUnroll = 8;
        constexpr size_t kPrefetchDistance = 16;

        if (num_rows >= 8192) {
            for (; i + kPrefetchDistance + kUnroll <= num_rows; i += kUnroll) {
                for (size_t k = 0; k < kUnroll; k += 2) {
                    if (is_filtered[i + kPrefetchDistance + k] == 0) {
                        STARROCKS_PREFETCH(&src[indexes[i + kPrefetchDistance + k]]);
                    }
                }

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

        for (; i + kUnroll <= num_rows; i += kUnroll) {
            dest[i + 0] = (is_filtered[i + 0] == 0) ? src[indexes[i + 0]] : DataType{};
            dest[i + 1] = (is_filtered[i + 1] == 0) ? src[indexes[i + 1]] : DataType{};
            dest[i + 2] = (is_filtered[i + 2] == 0) ? src[indexes[i + 2]] : DataType{};
            dest[i + 3] = (is_filtered[i + 3] == 0) ? src[indexes[i + 3]] : DataType{};
            dest[i + 4] = (is_filtered[i + 4] == 0) ? src[indexes[i + 4]] : DataType{};
            dest[i + 5] = (is_filtered[i + 5] == 0) ? src[indexes[i + 5]] : DataType{};
            dest[i + 6] = (is_filtered[i + 6] == 0) ? src[indexes[i + 6]] : DataType{};
            dest[i + 7] = (is_filtered[i + 7] == 0) ? src[indexes[i + 7]] : DataType{};
        }

        for (; i < num_rows; ++i) {
            dest[i] = (is_filtered[i] == 0) ? src[indexes[i]] : DataType{};
        }
    }
};

} // namespace starrocks
