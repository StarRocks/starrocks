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
#include <string>
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

namespace detail {

void gather_int16_buckets(uint32_t* b, const int16_t* a, const uint32_t* c, size_t buckets, int num_rows);
void gather_int16_buckets(uint32_t* b, const int16_t* a, const int32_t* c, size_t buckets, int num_rows);
void gather_int16_buckets(int32_t* b, const int16_t* a, const uint32_t* c, size_t buckets, int num_rows);
void gather_int16_buckets(int32_t* b, const int16_t* a, const int32_t* c, size_t buckets, int num_rows);

void gather_bytes(void* dest, const void* src, size_t elem_size, const uint32_t* indexes, size_t num_rows);
void gather_bytes(void* dest, const void* src, size_t elem_size, const int32_t* indexes, size_t num_rows);

void gather_bytes_filtered(void* dest, const void* src, size_t elem_size, const void* zero_val, const uint32_t* indexes,
                           const uint8_t* is_filtered, size_t num_rows);
void gather_bytes_filtered(void* dest, const void* src, size_t elem_size, const void* zero_val, const int32_t* indexes,
                           const uint8_t* is_filtered, size_t num_rows);

void gather_generic_string(std::string* dest, const std::string* src, const uint32_t* indexes, size_t num_rows);
void gather_generic_string_filtered(std::string* dest, const std::string* src, const uint32_t* indexes,
                                    const uint8_t* is_filtered, size_t num_rows);

} // namespace detail

struct SIMDGather {
    static constexpr const int max_process_size = 512 * 1024;

    template <class TB, class TC>
    static void gather(TB* b, const int16_t* a, const TC* c, size_t buckets, int num_rows) {
        static_assert(sizeof(TB) == 4);
        static_assert(std::is_integral_v<TB>);
        static_assert(sizeof(TC) == 4);
        static_assert(std::is_integral_v<TC>);

        if constexpr (std::is_same_v<TB, uint32_t> && std::is_same_v<TC, uint32_t>) {
            detail::gather_int16_buckets(b, a, c, buckets, num_rows);
        } else if constexpr (std::is_same_v<TB, uint32_t> && std::is_same_v<TC, int32_t>) {
            detail::gather_int16_buckets(b, a, c, buckets, num_rows);
        } else if constexpr (std::is_same_v<TB, int32_t> && std::is_same_v<TC, uint32_t>) {
            detail::gather_int16_buckets(b, a, c, buckets, num_rows);
        } else if constexpr (std::is_same_v<TB, int32_t> && std::is_same_v<TC, int32_t>) {
            detail::gather_int16_buckets(b, a, c, buckets, num_rows);
        } else {
            int i = 0;
            for (; i < num_rows; i++) {
                *b = a[*c];
                b++;
                c++;
            }
        }
    }

    /// dest[i] = src[indexes[i]]
    template <typename DataType, typename IndexType>
    static void gather(DataType* dest, const DataType* src, const IndexType* indexes, size_t num_rows) {
        static_assert(std::is_integral_v<IndexType>);
        if constexpr (std::is_trivially_copyable_v<DataType> && sizeof(IndexType) == 4) {
            if constexpr (std::is_signed_v<IndexType>) {
                detail::gather_bytes(dest, src, sizeof(DataType), reinterpret_cast<const int32_t*>(indexes), num_rows);
            } else {
                detail::gather_bytes(dest, src, sizeof(DataType), reinterpret_cast<const uint32_t*>(indexes), num_rows);
            }
        } else if constexpr (std::is_same_v<DataType, std::string> && sizeof(IndexType) == 4) {
            detail::gather_generic_string(dest, src, reinterpret_cast<const uint32_t*>(indexes), num_rows);
        } else {
            for (size_t i = 0; i < num_rows; ++i) {
                dest[i] = src[indexes[i]];
            }
        }
    }

    /// dest[i] = is_filtered[i] == 0 ? src[indexes[i]] : DataType{}
    template <typename DataType, typename IndexType, typename CondType>
    static void gather(DataType* dest, const DataType* src, const IndexType* indexes, const CondType* is_filtered,
                       size_t num_rows) {
        static_assert(std::is_integral_v<IndexType>);
        if constexpr (std::is_trivially_copyable_v<DataType> && sizeof(IndexType) == 4 && sizeof(CondType) == 1) {
            const DataType zero{};
            if constexpr (std::is_signed_v<IndexType>) {
                detail::gather_bytes_filtered(dest, src, sizeof(DataType), &zero,
                                              reinterpret_cast<const int32_t*>(indexes),
                                              reinterpret_cast<const uint8_t*>(is_filtered), num_rows);
            } else {
                detail::gather_bytes_filtered(dest, src, sizeof(DataType), &zero,
                                              reinterpret_cast<const uint32_t*>(indexes),
                                              reinterpret_cast<const uint8_t*>(is_filtered), num_rows);
            }
        } else if constexpr (std::is_same_v<DataType, std::string> && sizeof(IndexType) == 4 && sizeof(CondType) == 1) {
            detail::gather_generic_string_filtered(dest, src, reinterpret_cast<const uint32_t*>(indexes),
                                                   reinterpret_cast<const uint8_t*>(is_filtered), num_rows);
        } else {
            for (size_t i = 0; i < num_rows; ++i) {
                dest[i] = (is_filtered[i] == 0) ? src[indexes[i]] : DataType{};
            }
        }
    }
};

} // namespace starrocks
